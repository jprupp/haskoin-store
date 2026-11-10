-- |
-- Module      : Haskoin.Store.BlockStore.Pending
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Mempool transactions on their way into the database, as a pure value with
-- transitions.
--
-- A transaction that a peer announces goes through these states:
--
-- @
--   announced ──► requested ──► received ──► ready ──► taken for import
--                    │              │  (missing parents arrive)
--                    ▼              ▼
--                forgotten      dropped
--             (after 20 s)   (after an hour)
-- @
--
-- It is requested once, and not again while the request is fresh. Once
-- received, it waits until no parent is missing, and is then taken for
-- import together with other ready transactions, parents first.
--
-- = Why a pure value
--
-- The block store kept the queue and the recent requests in two variables,
-- changed by several functions, and forgot each request in a thread of its
-- own that slept for twenty seconds: one thread per transaction requested.
-- Here every change is a function, the clock is an argument, and requests
-- are forgotten by 'prune' on the clock tick of the block store, like the
-- transactions that waited too long.
module Haskoin.Store.BlockStore.Pending
  ( -- * State
    Pending,
    PendingTx (..),
    empty,
    isPending,
    queued,
    size,
    waiting,

    -- * Transitions
    requested,
    received,
    takeReady,
    imported,
    refresh,
    prune,

    -- * Timeouts
    requestTimeout,
    orphanTimeout,
  )
where

import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.Maybe (mapMaybe)
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime)
import Haskoin (Tx, TxHash, txHash)
import Haskoin.Store.Common (sortTxs)

-- | Transaction received from a peer and waiting to be imported.
data PendingTx = PendingTx
  { -- | when it was received
    time :: !UTCTime,
    -- | the transaction
    tx :: !Tx,
    -- | parent transactions that are missing; empty if it can be imported
    deps :: !(HashSet TxHash)
  }
  deriving stock (Show, Eq, Ord)

-- | Transactions requested from peers and received from them.
data Pending = Pending
  { -- | received, by hash
    txs :: !(HashMap TxHash PendingTx),
    -- | requested and not received yet, with when
    asked :: !(HashMap TxHash UTCTime)
  }
  deriving stock (Show, Eq)

-- | How long a request keeps the transaction from being requested again.
requestTimeout :: NominalDiffTime
requestTimeout = 20

-- | How long a transaction waits for missing parents.
orphanTimeout :: NominalDiffTime
orphanTimeout = 3600

-- | Nothing requested or received.
empty :: Pending
empty = Pending HashMap.empty HashMap.empty

-- | Whether a transaction is received or requested, so that it is not
-- requested again.
isPending :: TxHash -> Pending -> Bool
isPending h p = HashMap.member h p.txs || HashMap.member h p.asked

-- | How many transactions are received and not taken for import yet.
queued :: Pending -> Int
queued p = HashMap.size p.txs

-- | How many transactions are received or requested.
size :: Pending -> Int
size p = HashMap.size (HashMap.union (() <$ p.txs) (() <$ p.asked))

-- | Received transactions that have missing parents.
waiting :: Pending -> [PendingTx]
waiting p = filter (not . null . (.deps)) (HashMap.elems p.txs)

-- | Transactions were requested.
requested :: UTCTime -> [TxHash] -> Pending -> Pending
requested now hs p = p {asked = HashMap.union (HashMap.fromList [(h, now) | h <- hs]) p.asked}

-- | A transaction was received, with the parents that it spends and the
-- database does not have. It is no longer a request.
received :: UTCTime -> Tx -> HashSet TxHash -> Pending -> Pending
received now tx deps p =
  Pending
    { txs = HashMap.insert h (PendingTx now tx deps) p.txs,
      asked = HashMap.delete h p.asked
    }
  where
    h = txHash tx

-- | Take up to this many transactions with no missing parents, parents
-- before children.
takeReady :: Int -> Pending -> ([PendingTx], Pending)
takeReady n p = (taken, p {txs = foldr (HashMap.delete . txHash . (.tx)) p.txs taken})
  where
    ready = HashMap.filter (null . (.deps)) p.txs
    taken =
      take n $
        mapMaybe ((`HashMap.lookup` ready) . txHash . snd) (sortTxs (map (.tx) (HashMap.elems ready)))

-- | A transaction is in the database: transactions waiting for it no longer
-- miss it.
imported :: TxHash -> Pending -> Pending
imported h p = p {txs = HashMap.map (\t -> t {deps = HashSet.delete h t.deps}) p.txs}

-- | After a block, give each waiting transaction what the database now
-- says about it: 'Nothing' if it is in the database already, so it is
-- dropped, or the parents that it still misses.
refresh :: HashMap TxHash (Maybe (HashSet TxHash)) -> Pending -> Pending
refresh updates p = p {txs = HashMap.mapMaybeWithKey update p.txs}
  where
    update h t = case HashMap.lookup h updates of
      Nothing -> Just t
      Just Nothing -> Nothing
      Just (Just deps) -> Just t {deps = deps}

-- | Forget requests older than 'requestTimeout', and drop transactions
-- received longer than 'orphanTimeout' ago.
prune :: UTCTime -> Pending -> Pending
prune now p =
  Pending
    { txs = HashMap.filter (\t -> age t.time <= orphanTimeout) p.txs,
      asked = HashMap.filter (\t -> age t <= requestTimeout) p.asked
    }
  where
    age t = now `diffUTCTime` t
