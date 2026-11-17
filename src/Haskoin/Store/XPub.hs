{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE TemplateHaskell #-}

-- |
-- Module      : Haskoin.Store.XPub
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Extended public keys (xpubs) answered from a cache and indexed in the
-- background: what the parts of that design share. @docs\/xpub-indexing.md@
-- explains the design; this module holds its vocabulary.
--
-- An xpub with many used addresses is slow to read from the database: each
-- address is derived and read, then its transactions or outputs. So no
-- request reads an xpub from the database itself. It reads the xpub from a
-- cache, and on a miss it hands the xpub to the indexer
-- ("Haskoin.Store.XPub.Indexer"), which reads it in the background and
-- writes it to the cache, while the request waits or is told to come back.
--
-- The cache is the fleet's, in Redis ("Haskoin.Store.Fleet.XPub"), shared
-- by every store, and it cannot be turned off: its size is all there is to
-- set. It gives the reads of a t'XPubReads', each of one version of an
-- entry. It keeps the xpubs that cost most to index again and were used
-- most recently (GreedyDual, 'greedyLua'), and counts that cost in the
-- units of the prices below: they rank entries, and charge nobody. A job
-- settles and tells its size through a t'Budget'.
module Haskoin.Store.XPub
  ( -- * Reading a cache
    XPubReads (..),
    EntryMoved (..),

    -- * Paying for a job
    Budget (..),
    noBudget,

    -- * Prices
    addressPrice,
    entryPrice,
    bodyPrice,
    indexCost,

    -- * Keeping up with the store
    inSync,
    txAddresses,

    -- * Eviction in Redis
    greedyLua,
  )
where

import Control.Exception (Exception)
import Data.ByteString (ByteString)
import Data.Either (rights)
import Data.FileEmbed (embedFile, makeRelativeToProject)
import Data.IntMap.Strict qualified as IntMap
import Data.List (nub)
import Data.Word (Word32)
import Haskoin
  ( Address,
    BlockNode (..),
    Network,
    Tx (..),
    TxHash,
    TxOut (..),
    headerHash,
    scriptToAddressBS,
  )
import Haskoin.Node (Chain, chainGetBest)
import Haskoin.Store.Common (Limits (..))
import Haskoin.Store.Data
  ( Prev (..),
    TxData (..),
    TxRef (..),
    XPubBal (..),
    XPubSpec,
    XPubUnspent,
  )
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader (DatabaseReader (..))

-- | What the web server reads from the cache of xpubs. Each
-- read is 'Nothing' when the cache does not have the xpub, and marks it as
-- used when it does.
data XPubReads = XPubReads
  { -- | balances of the addresses up to the gap
    balances :: XPubSpec -> IO (Maybe [XPubBal]),
    -- | a page of transactions, newest first, as the database gives it
    transactions :: XPubSpec -> Limits -> IO (Maybe [TxRef]),
    -- | number of transactions
    count :: XPubSpec -> IO (Maybe Word32),
    -- | a page of unspent outputs, newest first, given the balances, which
    -- the Redis cache needs for the paths of the addresses
    unspents :: XPubSpec -> [XPubBal] -> Limits -> IO (Maybe [XPubUnspent]),
    -- | bodies of transactions, with their spenders, in the order of the
    -- hashes; 'Nothing' for those the cache does not have
    bodies :: [TxHash] -> IO [Maybe TxData],
    -- | the bodies of transactions as the entries that the reads read have
    -- them, wherever the bodies come from: the confirmation of each
    -- transaction of an entry, and who spends each output of its wallet, at
    -- the version that the request read; the rest as the bodies have it
    -- (@docs\/xpub-indexing.md@, Exact reads)
    wallet :: [Maybe TxData] -> IO [Maybe TxData]
  }

-- | A request read an entry of a cache that changed before the request was
-- done: a write landed between two of its reads, or the entry went. The
-- reads of one request are of one version of each xpub, so the web server
-- answers it again from the start, before anything of it was sent
-- ('Haskoin.Store.Fleet.Member.fleetSession', @docs\/xpub-indexing.md@,
-- Exact reads).
data EntryMoved = EntryMoved
  deriving (Eq, Show)

instance Exception EntryMoved

-- | What a job of the indexer calls as it goes. 'settle' is called at the
-- safe points of a job, where it holds no iterator of the database: between
-- batches of addresses, between calls into the database. 'finish' is
-- called when the job is done, however it ends. Every job of the store gets
-- 'noBudget', in which only 'sized', set by the indexer, does anything:
-- the jobs are not paced. The seam is kept so that a job could be paced
-- or measured without changing the indexer.
data Budget = Budget
  { settle :: IO (),
    finish :: IO (),
    -- | the job tells how many transactions it has to read, once it knows:
    -- the indexer tells from it when the job should be done
    -- ('Haskoin.Store.XPub.Indexer.joinEstimate')
    sized :: Int -> IO ()
  }

-- | A budget that measures nothing and never waits.
noBudget :: Budget
noBudget = Budget {settle = return (), finish = return (), sized = \_ -> return ()}

-- | The cost of an address of an xpub: derived, 25, and its balance read
-- by key, 10. First guesses all, in the same arbitrary units.
addressPrice :: Int
addressPrice = 35

-- | The cost of an entry of a scan: a transaction of an address's history,
-- or one of its unspent outputs.
entryPrice :: Int
entryPrice = 2

-- | The cost of the body of a transaction: read by key, 10, and decoded, 5.
bodyPrice :: Int
bodyPrice = 15

-- | What it costs to index an xpub from nothing: its addresses,
-- its transactions and its unspent outputs, and the bodies of the
-- transactions if the cache keeps them. It is the cost that eviction weighs
-- ('greedyLua'): what the cache would pay to read the
-- xpub again.
--
-- It is worked out from what was read rather than from the time the job
-- took, which would count the load of the store with the size of the xpub.
indexCost ::
  -- | addresses
  Int ->
  -- | transactions
  Int ->
  -- | unspent outputs
  Int ->
  -- | bodies
  Int ->
  Int
indexCost addrs txs utxos bods =
  addressPrice * addrs + entryPrice * (txs + utxos) + bodyPrice * bods

-- | Whether the database has the best header of the node. A store that is
-- not in sync takes no job of the indexer: it would write what knows less
-- than the database of a store that is, and its upkeep would chase every
-- block that it imports while it catches up.
inSync :: DatabaseReader -> Chain -> IO Bool
inSync db chain =
  Q.getBestBlock db >>= \case
    Nothing -> return False
    Just bb -> do
      cb <- chainGetBest chain
      return (cb.height > 0 && headerHash cb.header == bb)

-- | Addresses that a transaction spends from or pays to: those whose cached
-- xpubs it changes.
txAddresses :: Network -> TxData -> [Address]
txAddresses net td =
  nub . rights $
    map (scriptToAddressBS net . (.script)) (IntMap.elems td.prevs)
      <> map (scriptToAddressBS net . (.script)) td.tx.outputs

-- | GreedyDual for the caches of xpubs in Redis, as Lua functions that the
-- scripts which evict or mark a use start with: @greedy_use@, which gives
-- an entry the level plus its cost as its priority, @greedy_evict@, which
-- evicts the lowest priorities past a number and raises the level to each,
-- and @greedy_migrate@, which gives the level to the entries of a sorted
-- set that held times of last use, so that they go first. The cache of
-- the fleet ("Haskoin.Store.Fleet.XPub") runs it in front of its scripts.
-- Between
-- equal priorities Redis goes by the id of the xpub. The file is
-- @src\/Haskoin\/Store\/XPub\/lua\/greedy.lua@; a script that uses it is
-- run with it in front, as @cat greedy.lua script.lua@ for @redis-cli@.
greedyLua :: ByteString
greedyLua = $(makeRelativeToProject "src/Haskoin/Store/XPub/lua/greedy.lua" >>= embedFile)
