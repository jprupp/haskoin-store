{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -Wno-ambiguous-fields #-}

-- |
-- Module      : Haskoin.Store.Logic.Layered
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Rules to import and revert blocks and transactions in the layered layout
-- of the database (@docs\/layered-database.md@), in which the mempool is
-- kept apart from the chain. These are the rules that the block store
-- imports with. They do what "Haskoin.Store.Logic" did for the layout of
-- version 20, with the same functions, the same results and the same
-- errors, which is how the block store moved from one to the other, and
-- how the tests compare the two.
--
-- = Two sets of rules
--
-- __The chain__ has one door. 'importBlock' and 'revertBlock' are the only
-- functions here that write to the column families of the chain, and what
-- they write depends on the block and on those column families alone. A
-- block is imported the same way whether or not the store had seen its
-- transactions before, as during the initial sync. It is not validated:
-- see "A mined block is not validated" below.
--
-- __The mempool__ is a set of transactions, each with its keys in the
-- mempool area ('Haskoin.Store.Database.Mempool.mempoolRecords'). A
-- transaction enters by having its keys
-- written and leaves by having them deleted; nothing else is read or
-- changed, and nothing is written to the chain. What is left of the rules
-- is the checks: that the outputs a transaction spends exist and are
-- unspent, in the chain or in the mempool, and what it replaces.
--
-- The two meet in three places. A block takes the transactions it confirms
-- out of the mempool, and deletes those that conflict with it. A reverted
-- block gives its transactions back to the mempool. And a mempool
-- transaction may spend outputs of the chain, which the chain goes on
-- listing as unspent: only the mempool area says otherwise.
--
-- = Same answers as version 20
--
-- For any sequence of transactions and of blocks that could be mined,
-- these rules leave the database that "Haskoin.Store.Logic" followed by
-- "Haskoin.Store.Database.Migrate" leaves, key for key, and each step gives
-- the same result. The tests check that for random sequences. There is one
-- difference by design. A block that spends an output of an unconfirmed
-- transaction is refused here, because that output is not in the chain;
-- version 20 imports it, since its indices do not tell the two apart.
--
-- = Importing a block in stages
--
-- 'importBlock' works in four stages, so that a full block takes a second
-- or two and not eighteen, as it did on a store of mainnet whose disk and
-- cores were mostly idle. What cost the time was thirty thousand reads of
-- the database, one after another from one thread, many of them of a key
-- that had been read already.
--
-- 1. __Prepare__ ('prepareBlock'), on all cores, from the block alone: the
--    hash of each transaction, the address of each output, the sizes. They
--    are carried along from there. The hash was worked out five or six
--    times for each transaction, and the block serialised three times for
--    its size and weight.
--
-- 2. __Read ahead__: every key that the rules will read is known from the
--    block, or from what those keys give. The outputs that the block
--    spends are read with multi-gets on all cores into the read cache of
--    the batch ("Haskoin.Store.Database.Writer"), but for those that the
--    block store remembers from when a mempool transaction came to spend
--    them ("Haskoin.Store.Database.Copy"). What the mempool has
--    for the block is not read from RocksDB: the database keeps a copy of
--    its mempool area in memory ("Haskoin.Store.Database.Copy"), and only
--    the bodies and the summaries are taken from it ahead.
--
-- 3. __Apply__: the rules, one transaction after another as before, now
--    without a read of the database between them.
--
-- 4. __Write__: the batch, as before.
--
-- Since database version 23 the balances of the addresses of a block and
-- the spenders of the outputs that it spends are not read at all: the
-- block writes how they change, as merge records, and RocksDB works out
-- the values when they are read ('addChainTx',
-- "Haskoin.Store.Database.Merge"). They were more than half of the read
-- ahead of a block on a store of mainnet.
--
-- = A mined block is not validated
--
-- A block that gets here was mined: it passed proof of work, and the node
-- that the store follows took it. The store takes it that its
-- transactions are valid: every output that the block spends is unspent
-- in the chain or made by a transaction of the block, and none is spent
-- twice. Checking that is the work of the node, and each check here was a
-- read of the database for every input of every block. So the import
-- reads what it needs to write the block, and nothing to check it:
--
-- * __No spender of the chain is read.__ Who spends an output in the
--   chain says whether it is spent, and the block is trusted about that.
--   A block writes its spenders as merge records and never reads a group
--   of them. Before, a block that met a mempool looked in the chain for
--   the spender of every output that it both made and spent, when the
--   mempool had not seen its transactions: thousands of keys that are not
--   there, each answered by a filter that a store with a small cache reads
--   from the disk. A block of mainnet took ten seconds over them.
--
-- * __An output that the block makes is not read from the database.__ The
--   rules find it in the changes of the batch, as each transaction is
--   applied after those that it spends ('readAheadBlock').
--
-- * __Only a transaction that the mempool does not have is looked up for
--   conflicts with the mempool__, and only the mempool is asked
--   ('importTxs', 'mempoolRivals').
--
-- * __An empty mempool is asked nothing.__ When the database has an empty
--   mempool and the batch has not touched it, there is no transaction to
--   make way.
--
-- * __Only a coinbase is looked up in the chain__, in case it is confirmed
--   already. That happened to two coinbases of 2010, and it is data, not
--   a check: the later one takes the place of the earlier. A transaction
--   with an input cannot be confirmed twice, since its input is spent.
--
-- What stays is what the store's own data needs: the unspent outputs that
-- the block spends, whose values, scripts and positions the balances and
-- the indexes are made from; the order in which transactions are taken
-- ('sortTxs', needed on Bitcoin Cash, whose blocks are in the order of the
-- hashes, and which decides which transaction of the block a replaced one
-- is said to conflict with); and all that the mempool does to give way to
-- a block. Reverting a block and the rules of the mempool check what they
-- always did: nothing vouches for an unconfirmed transaction.
--
-- A block that breaks the assumption is promised one thing: it is written
-- whole or not at all, since a batch that throws writes nothing
-- ("Haskoin.Store.Database.Writer"). It is still refused when the one
-- read that stays finds nothing: an output that does not exist, that the
-- chain has as spent, or that an earlier transaction of the block spent,
-- is not there to be read, and the block is an 'Orphan'. A transaction
-- that spends one output twice stops the batch too, when it comes to
-- spend what it just spent ('addChainTx'): a look in the changes of the
-- batch, in memory.
--
-- 'importBlockSequential' is the import as it was until version 1.15.1,
-- with every check that it had, kept as the reference: the tests give
-- both the same random histories of blocks that could be mined, and
-- compare every key of every column family.
--
-- What was not done: reading ahead for the blocks that come next while one
-- is written. The values of a block that has not been imported yet can be
-- changed by the block before it, so they cannot go into the cache of its
-- batch; only reading them to warm RocksDB's own cache would be safe.
module Haskoin.Store.Logic.Layered
  ( initBest,
    revertBlock,
    importBlock,
    importBlockSequential,
    newMempoolTx,
    deleteUnconfirmedTx,

    -- * What a block needs worked out once
    PreparedBlock (..),
    PreparedTx (..),
    prepareBlock,
  )
where

import Control.DeepSeq (NFData, force)
import Control.Exception (evaluate, throwIO)
import Control.Monad
  ( forM,
    forM_,
    guard,
    unless,
    void,
    when,
    zipWithM_,
  )
import Data.ByteString qualified as B
import Data.Default (def)
import Data.Either (rights)
import Data.Functor ((<&>))
import Data.HashMap.Strict qualified as M
import Data.HashSet qualified as H
import Data.IntMap.Strict qualified as I
import Data.List (nub)
import Data.Map.Strict qualified as Map
import Data.Maybe
  ( catMaybes,
    fromMaybe,
    isJust,
    isNothing,
    mapMaybe,
  )
import Data.Serialize (encode)
import Data.Set qualified as S
import Data.Text (Text)
import Data.Word (Word32, Word64)
import GHC.Generics (Generic)
import Haskoin
  ( Address,
    Block (..),
    BlockHash,
    BlockHeader (..),
    BlockNode (..),
    Network (..),
    OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    VarInt (..),
    blockHashToHex,
    computeSubsidy,
    doubleSHA256,
    eitherToMaybe,
    genesisBlock,
    genesisNode,
    headerHash,
    isGenesis,
    nullOutPoint,
    scriptToAddressBS,
    txHash,
    txHashToHex,
  )
import Haskoin.Node (LogLevel (..), logAt)
import Haskoin.Store.Common (Replaced (..), nub', replacedKept, sortTxs, sortTxsWith)
import Haskoin.Store.Data
import Haskoin.Store.Database.Mempool (MemRecords (..), mempoolRecordsOf)
import Haskoin.Store.Database.Types (BalVal (..), MemAddrKey (..), MemAddrVal (..), MemSummary (..))
import Haskoin.Store.Database.Writer
import Haskoin.Store.Logic (ImportException (..))

-------------------------------------------------------------------------------
-- Reading
-------------------------------------------------------------------------------

-- | Log a message with the logger of the writer.
-- What a peer sends and the store refuses (an orphan, a double spend, a
-- transaction without funds) is logged for debugging: it is the ordinary
-- traffic of a network, and nothing is wrong with the store. An error is
-- kept for what should not happen whatever peers do: a block that does not
-- fit the database.
logDebug, logError :: Writer -> Text -> IO ()
logDebug wr = logAt LevelDebug (writerLogger wr)
logError wr = logAt LevelError (writerLogger wr)

-- | Address of an output script, if it has one on the network.
scriptAddress :: Writer -> B.ByteString -> Maybe Address
scriptAddress wr = eitherToMaybe . scriptToAddressBS (writerNet wr)

-- | Data of a confirmed transaction, with the spenders of its outputs that
-- are in blocks. Version 20 kept the bodies of deleted transactions in the
-- same place; they do not count.
chainTx :: Writer -> TxHash -> IO (Maybe TxData)
chainTx wr h =
  getTxData wr h <&> \case
    Just td | not td.deleted -> Just td
    _ -> Nothing

-- | Data of a transaction that is in the mempool or in the chain, without
-- the mempool spenders of its outputs.
activeBody :: Writer -> TxHash -> IO (Maybe TxData)
activeBody wr h = getMempoolTx wr h >>= maybe (chainTx wr h) (return . Just)

-- | Data of a transaction that is in the mempool or in the chain, with every
-- spender of its outputs, in blocks and in the mempool.
activeTx :: Writer -> TxHash -> IO (Maybe TxData)
activeTx wr h = activeBody wr h >>= mapM spent
  where
    spent td = do
      ms <- getMempoolSpenders wr h (length td.tx.outputs)
      -- those of blocks are under keys of their own, and only a confirmed
      -- transaction can have any
      cs <- if confirmed td.block then getChainSpenders wr h (length td.tx.outputs) else return I.empty
      return (td :: TxData) {spenders = I.union cs ms}

-- | An output that a new mempool transaction may spend: one that the chain
-- has as unspent, or one of a mempool transaction, that no mempool
-- transaction spends.
spendable :: Writer -> OutPoint -> IO (Maybe Unspent)
spendable wr p =
  getMempoolSpender wr p >>= \case
    Just _ -> return Nothing
    Nothing ->
      getUnspent wr p >>= \case
        Just u -> return (Just u)
        Nothing -> (>>= output) <$> getMempoolTx wr p.hash
  where
    output td = unspentOutput wr td.block p <$> getTxOut p.index td.tx

-- | An output as an unspent output, given the position of its transaction.
unspentOutput :: Writer -> BlockRef -> OutPoint -> TxOut -> Unspent
unspentOutput wr br p o =
  Unspent
    { block = br,
      outpoint = p,
      value = o.value,
      script = o.script,
      address = scriptAddress wr o.script
    }

-- | Input that spends an output, in the mempool or in a block.
spenderOf :: Writer -> OutPoint -> IO (Maybe Spender)
spenderOf wr p = getMempoolSpender wr p >>= maybe (getChainSpender wr p) (return . Just)

-- | Data of the best block. Throws 'BestBlockNotFound' if there is none.
bestBlockData :: Writer -> IO BlockData
bestBlockData wr = do
  h <-
    getBestBlock wr >>= \case
      Nothing -> do
        logError wr "Best block unknown"
        throwIO BestBlockNotFound
      Just h -> return h
  getBlock wr h >>= \case
    Nothing -> do
      logError wr "Best block not found"
      throwIO BestBlockNotFound
    Just b -> return b

-------------------------------------------------------------------------------
-- Blocks
-------------------------------------------------------------------------------

-- | Import the genesis block if the database has no best block yet.
initBest :: Writer -> IO ()
initBest wr = do
  logDebug wr "Initializing best block"
  let net = writerNet wr
  m <- getBestBlock wr
  when (isNothing m) . void $ do
    logDebug wr "Importing Genesis block"
    importBlock wr (genesisBlock net) (genesisNode net)

-- | Check that a block builds on the best block, or that it is the genesis block
-- of an empty database.
checkNewBlock :: Writer -> Block -> BlockNode -> IO ()
checkNewBlock wr b n =
  getBestBlock wr >>= \case
    Nothing
      | isGenesis n -> return ()
      | otherwise -> do
          logError wr $ "Cannot import non-genesis block: " <> blockHashToHex (headerHash b.header)
          throwIO BestBlockNotFound
    Just h
      | b.header.prev == h -> return ()
      | otherwise -> do
          logError wr $ "Block does not build on head: " <> blockHashToHex (headerHash b.header)
          throwIO PrevBlockNotBest

-- | Import a block that builds on the best block, and make it the best block.
-- Returns its data and the data of its transactions.
--
-- The block goes into the chain's column families as it would into those of
-- a store with an empty mempool. Before that, the mempool gives way: the
-- transactions of the block that were in it leave it, and the mempool
-- transactions that spend an output that the block spends are deleted, with
-- their descendants.
--
-- It works in the stages that the head of this module describes, and notes
-- how long each took ('noteBlockTimes').
importBlock :: Writer -> Block -> BlockNode -> IO (BlockData, [TxData])
importBlock wr b n = do
  checkNewBlock wr b n
  (pb, preparing) <- stopwatch (prepareBlock (writerNet wr) b)
  ((quiet, kinds), reading) <- stopwatch (readAheadBlock wr pb)
  (done, applying) <- stopwatch (applyBlock wr b n pb quiet)
  noteBlockTimes wr preparing reading kinds applying
  return done

-- | A transaction of a block with what its import needs of it, worked out
-- once, from the transaction alone.
data PreparedTx = PreparedTx
  { tx :: !Tx,
    -- | its hash
    txid :: !TxHash,
    -- | the outputs that it spends, in the order of its inputs: none for a
    -- coinbase
    spends :: ![OutPoint],
    -- | the address of each of its outputs, for those that have one
    paid :: ![Maybe Address],
    -- | bytes that it takes in a block, with its witness
    size :: !Int,
    -- | the same without its witness
    base :: !Int
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (NFData)

-- | A block with what its import needs of it, worked out once.
data PreparedBlock = PreparedBlock
  { -- | its transactions, in its order
    txs :: ![PreparedTx],
    -- | the same, each after those that it spends, with its position in
    -- the block ('sortTxs')
    ordered :: ![(Word32, PreparedTx)],
    -- | bytes of the block
    size :: !Int,
    -- | weight of the block, as BIP 141 defines it
    weight :: !Int
  }
  deriving (Eq, Show)

-- | Work out what the import of a block needs of it, on all cores.
--
-- Each transaction is serialised once without its witness: the hash is
-- taken of those bytes, and their length is its part of the size of the
-- block. One with a witness is serialised once more for its length. The
-- size and the weight of the block are sums of those, with the header and
-- the count of transactions, instead of two more serialisations of the
-- whole block.
prepareBlock :: Network -> Block -> IO PreparedBlock
prepareBlock net b = do
  txs <- concat <$> onAllCores 2 16 (evaluate . force . map (prepareTx net)) b.txs
  let count = length txs
      overhead = B.length (encode b.header) + B.length (encode (VarInt (fromIntegral count)))
      base = overhead + sum (map (.base) txs)
      full = overhead + sum (map (.size) txs)
      ordered = sortTxsWith (.txid) (.tx) txs
  -- the sort is part of this stage
  _ <- evaluate (length ordered)
  return PreparedBlock {txs, ordered, size = full, weight = 3 * base + full}

-- | Work out what the import of a block needs of one of its transactions.
prepareTx :: Network -> Tx -> PreparedTx
prepareTx net tx =
  PreparedTx
    { tx,
      txid = TxHash (doubleSHA256 stripped),
      spends = prevOuts tx,
      paid = map (eitherToMaybe . scriptToAddressBS net . (.script)) tx.outputs,
      size = if null tx.witness then B.length stripped else B.length (encode tx),
      base = B.length stripped
    }
  where
    -- what 'txHash' hashes
    stripped = encode (tx :: Tx) {witness = []}

-- | Read ahead what the rules will read from the database for a block,
-- into the read cache of the batch, and say whether the mempool has
-- nothing to do with the block, so that the rules need not ask it
-- ('importTxs'): the database has an empty mempool, and the batch has not
-- touched it.
--
-- One kind of key is read from RocksDB, with multi-gets on all cores: the
-- outputs that the block spends and does not make, as unspent outputs of
-- the chain. Their values, scripts and positions are what the balances
-- and the indexes of the block are made from.
--
-- An output that a transaction of the block makes is not asked for. The
-- database does not have it, and the rules find it in the changes of the
-- batch, since each transaction is applied after those that it spends. On
-- Bitcoin mainnet in November 2026 that was 3,000 to 6,500 of the inputs
-- of a block.
--
-- Nor is an output read that the block store remembers: one that a
-- mempool transaction spends was read from the chain when the transaction
-- came, and is kept in memory until its mempool spender leaves
-- ('readAheadUnspents', "Haskoin.Store.Database.Copy"). At the tip of the
-- chain that is nearly every output that a block spends.
--
-- A key that is missing here costs a read when the rules ask for it, and
-- nothing else.
--
-- = The mempool area
--
-- Nothing of the mempool area is read from RocksDB: a batch answers
-- every read of it from the copy that the database keeps in memory
-- ("Haskoin.Store.Database.Copy"). Until then the mempool was most of the
-- read ahead of a block at the tip, some fifty thousand keys.
--
-- Two kinds of key are still taken from the copy ahead of the rules, on
-- all cores, unless the area is empty:
--
-- * the body of each transaction of the block that is in the mempool,
--   because a body has to be decoded, which is the work of a few
--   microseconds that the cores can share;
--
-- * the summary of each address of the block, because serialising the
--   key of an address costs more than the look itself.
--
-- The rest, the rules ask the copy for as they come to it: the mempool
-- spender of each output, the mempool spenders of the outputs of each
-- transaction that leaves the mempool, and whether a transaction is
-- listed. Each is a look in a tree, and reading them ahead into the
-- tables of the batch, also on all cores, was measured to gain nothing
-- (@docs\/block-import.md@).
--
-- = What is not read
--
-- The balances of the addresses of the block, and the groups of spenders
-- of the outputs that it spends, were read here until database version
-- 23, only to be changed and written back. They are changed without
-- being read now ('addChainTx'). On a store of mainnet they were more
-- than half of the read ahead.
--
-- Groups of spenders were still read after that to check the block: that
-- no output which it both makes and spends has a spender in the chain,
-- and, with a mempool, who spends in the chain an output that is not
-- there to be spent. A mined block is not checked (the head of this
-- module), so the import of a block reads no balance and no group of
-- spenders at all.
--
-- It also says how long each kind of key took to read, as t'ReadTimes'.
-- The balances and the spenders take no time, as none is read.
readAheadBlock :: Writer -> PreparedBlock -> IO (Bool, ReadTimes)
readAheadBlock wr pb
  | not (readsAhead wr) = return (False, noReads)
  | otherwise = do
      empty <- mempoolAreaEmpty wr
      (_, unspentReads) <- stopwatch (readAheadUnspents wr outside)
      (_, mempoolReads) <- stopwatch . unless empty $ do
        readAheadMempoolTxs wr hashes
        found <- catMaybes <$> mapM (getUnspent wr) outside
        readAheadMempoolSummaries wr (mapMaybe (.address) found <> concatMap (catMaybes . (.paid)) pb.txs)
      untouched <- mempoolUntouched wr
      return (empty && untouched, ReadTimes {unspentReads, spenderReads = 0, balanceReads = 0, mempoolReads})
  where
    hashes = map (.txid) pb.txs
    made = H.fromList hashes
    -- the outputs that the block spends and that no transaction of the
    -- block makes
    outside = filter (not . (`H.member` made) . (.hash)) (concatMap (.spends) pb.txs)

-- | The rules of the import of a block, given what was worked out of it
-- and whether the mempool has nothing to do with it ('readAheadBlock').
applyBlock :: Writer -> Block -> BlockNode -> PreparedBlock -> Bool -> IO (BlockData, [TxData])
applyBlock wr b n pb quiet = do
  let net = writerNet wr
      subsidy = computeSubsidy net n.height
  bs <- getBlocksAtHeight wr n.height
  setBlocksAtHeight wr (nub (headerHash n.header : bs)) n.height
  setBest wr (headerHash n.header)
  tds <- importTxs wr n quiet pb.ordered
  -- forget what a block replaced 'replacedKept' blocks ago
  when (n.height >= replacedKept) $
    getReplacedAt wr (n.height - replacedKept) >>= mapM_ (deleteReplaced wr)
  let bd =
        BlockData
          { height = n.height,
            main = True,
            work = n.work,
            header = n.header,
            size = fromIntegral pb.size,
            txs = map (.txid) pb.txs,
            weight = if net.segWit then fromIntegral pb.weight else 0,
            subsidy = subsidy,
            fee = sum $ map txDataFee tds,
            outputs = sum $ map (sum . map (.value) . (.outputs)) (drop 1 b.txs)
          }
  insertBlock wr bd
  logDebug wr $ "Finished importing block: " <> blockHashToHex (headerHash n.header)
  return (bd, tds)

-- | Import the transactions of a block, in dependency order, after deleting
-- the mempool transactions that conflict with them. What is kept of each
-- one deleted ('Replaced') names the transaction of the block that it
-- conflicts with, or that its unconfirmed ancestor conflicts with.
--
-- The flag says that no mempool transaction can conflict with the block
-- ('readAheadBlock'), and then none is looked for.
--
-- = Only a transaction that the mempool does not have can conflict
--
-- A transaction of the block that is in the mempool is not asked what it
-- conflicts with: it conflicts with nothing. The mempool has one spender
-- for an output at most, so the mempool spender of every output that such
-- a transaction spends is the transaction itself. That does not rest on
-- the block being valid, but on the rules of the mempool:
--
-- * the mempool spender of an output is the value of a key that is the
--   output (t'Haskoin.Store.Database.Types.MemSpenderKey'), and a
--   transaction writes one for each of its inputs, with itself as the
--   spender ('addMempoolTx', 'mempoolRecordsOf');
--
-- * a transaction enters the mempool only if none of the outputs that it
--   spends has a mempool spender, once those that it replaces have been
--   deleted with their keys ('newMempoolTx', 'spendable');
--
-- * a transaction that a reverted block gives back spends outputs that
--   the chain had as spent until then, which no mempool transaction could
--   spend ('revertBlock');
--
-- * a transaction leaves the mempool with the keys of its own inputs and
--   no others ('removeMempoolTxOf').
--
-- So the conflicts of a block are those of its transactions that the
-- mempool has not seen ('mempoolRivals'). At the tip of the chain that is
-- a few of them, where every input of every transaction was asked about.
importTxs :: Writer -> BlockNode -> Bool -> [(Word32, PreparedTx)] -> IO [TxData]
importTxs wr bn quiet txs = do
  unless quiet $ mapM_ (giveWay . snd) (reverse txs)
  mapM (uncurry action) txs
  where
    br i = BlockRef {height = bn.height, position = i}
    blockTime = fromIntegral bn.header.timestamp
    -- the mempool transactions that spend what a transaction of the block
    -- spends are deleted, with their descendants
    giveWay p =
      getMempoolTx wr p.txid >>= \case
        Just _ -> return ()
        Nothing ->
          mempoolRivals wr p
            >>= mapM_
              ( \gone -> do
                  deleteSingleTx wr (txHash gone)
                  insertReplaced wr (txHash gone) Replaced {by = p.txid, height = bn.height}
              )
    action i p =
      getMempoolTx wr p.txid >>= \case
        Just td -> do
          -- it leaves the mempool, and with it the time at which this
          -- store first saw it: in the chain its time is that of its
          -- block, like that of a transaction that was never in the
          -- mempool, so that the record is the same on every store
          removeMempoolTxOf wr (madeInMempool wr) p.txid td
          td' <- importChainTx wr (br i) blockTime p
          updateSpendersRBFOf wr p.txid p.tx
          return td'
        Nothing
          -- only a transaction without inputs can be in the chain already:
          -- the inputs of any other would be spent
          | null p.spends ->
              chainTx wr p.txid >>= \case
                Just td -> moveChainTx wr td (br i) blockTime
                Nothing -> importChainTx wr (br i) blockTime p
          | otherwise -> importChainTx wr (br i) blockTime p

-- | Import a transaction of a block into the chain, with the time of the
-- block. Throws 'Orphan' if an output that it spends is not unspent in the
-- chain. That is not a check of the block: the output is read for what the
-- transaction needs of it, and there is nothing to go on with when it is
-- not there.
--
-- = Why the time of the block
--
-- What the chain's column families hold is to follow from the chain and
-- from nothing else: the same bytes on every store that has the same
-- blocks, whatever each of them saw before. The time of a transaction was
-- the one thing in them that did not. A store kept the time at which it
-- first saw a transaction in its mempool when a block confirmed it, so two
-- stores had two times for it, a store that synced later had the time of
-- the block, and the stores of a fleet gave different answers about the
-- same confirmed transaction. The time of first sight is still what a
-- transaction has while it is in the mempool, where it is the store's own,
-- or the pool's in a fleet.
importChainTx :: Writer -> BlockRef -> Word64 -> PreparedTx -> IO TxData
importChainTx wr br time p = do
  us <- forM p.spends $ \o ->
    getUnspent wr o >>= \case
      Just u -> return u
      Nothing -> do
        logError wr $ "Attempted to import a tx missing UTXO: " <> txHashToHex p.txid
        throwIO Orphan
  let td = prepareTxData False br time p.tx us
  addChainTx wr p td us
  return td

-- | Give a confirmed transaction a new position in the chain: a block has a
-- transaction that is already confirmed. Only two coinbases of 2010 are, on
-- Bitcoin; the later one takes the place of the earlier, with the outputs
-- that are still unspent, as in the layout of version 20, and the time of
-- the later block.
moveChainTx :: Writer -> TxData -> BlockRef -> Word64 -> IO TxData
moveChainTx wr t new time = do
  forM_ (txDataAddresses wr t) $ \a -> do
    deleteAddrTx wr a TxRef {block = t.block, txid = h}
    insertAddrTx wr a TxRef {block = new, txid = h}
  forM_ (zip [0 ..] t.tx.outputs) $ \(i, _) ->
    getUnspent wr (OutPoint h i) >>= mapM_ move
  let td = (t :: TxData) {block = new, timestamp = time}
  insertTx wr td
  return td
  where
    h = txHash t.tx
    move u = do
      let u' = (u :: Unspent) {block = new}
      insertUnspent wr u'
      forM_ u.address $ \a -> deleteAddrUnspent wr a u >> insertAddrUnspent wr a u'

-- | Revert the best block: make its parent the best block, mark it as not in
-- the main chain, take its transactions out of the chain and give them to
-- the mempool at a time, the time of the revert, and delete its coinbase
-- with whatever spends it. Throws 'BlockNotBest' if the block is not the
-- best block.
--
-- The transactions go back with the time that they are put back, not that
-- of their block. The mempool deletes what is older than its timeout
-- (@--mempool-timeout@), by that time: with the time of the block, a
-- transaction of a block older than the timeout was deleted as soon as it
-- came back, while the nodes kept it. Stores of a fleet give it the same
-- time all the same, as the shared mempool keeps the time of the first
-- store that offers it.
--
-- What the block store remembers of the outputs that mempool transactions
-- spend is forgotten when the batch is written ('forgetSpentOutputs'):
-- the outputs of the block leave the chain while the mempool transactions
-- that spend them stay.
revertBlock :: Writer -> UnixTime -> BlockHash -> IO ()
revertBlock wr now bh = do
  bd <-
    bestBlockData wr >>= \b ->
      if headerHash b.header == bh
        then return b
        else do
          logError wr $ "Cannot revert non-head block: " <> blockHashToHex bh
          throwIO BlockNotBest
  -- nothing that is remembered of the outputs that mempool transactions
  -- spend outlives a block that leaves the chain
  forgetSpentOutputs wr
  tds <- mapM inChain bd.txs
  setBest wr bd.header.prev
  insertBlock wr bd {main = False}
  -- what the block replaced is not replaced any more
  getReplacedAt wr bd.height >>= mapM_ (deleteReplaced wr)
  case tds of
    [] -> return ()
    coinbase : rest -> do
      let returned = M.fromList [(txHash td.tx, td) | td <- rest]
          -- the transactions that go back to the mempool, each after the
          -- transactions that it spends
          ordered = filter ((`M.member` returned) . txHash) (map snd (sortTxs (map (.tx) tds)))
      -- out of the chain, each after the transactions that spend it
      forM_ (reverse ordered) (leave . txHash)
      -- into the mempool, at the time of the revert. The coinbase is still
      -- in the chain: a transaction that spends it has all its parents
      -- while its flag is worked out, as in the layout of version 20
      forM_ (mapMaybe ((`M.lookup` returned) . txHash) ordered) $ \td -> do
        rbf <- isRBF wr td.tx
        addMempoolTx wr now (td :: TxData) {rbf = rbf, timestamp = now}
        updateSpendersRBF wr td.tx
      -- a coinbase cannot be in the mempool: it is deleted, and so is
      -- everything that spends it
      spenders <- getMempoolSpenders wr (txHash coinbase.tx) (length coinbase.tx.outputs)
      forM_ (nub' (map (.txid) (I.elems spenders))) $ \h ->
        getChain wr True False h >>= mapM_ (deleteSingleTx wr . txHash)
      -- and forgotten: nothing is kept of a transaction that leaves the
      -- store unconfirmed
      leave (txHash coinbase.tx)
  where
    inChain h =
      chainTx wr h >>= \case
        Just td -> return td
        Nothing -> do
          logDebug wr $ "Tx not found: " <> txHashToHex h
          throwIO TxNotFound
    -- take a transaction that nothing in the chain spends out of the chain
    leave h = do
      td <- inChain h
      spent <- getChainSpenders wr h (length td.tx.outputs)
      unless (I.null spent) $ do
        logError wr $ "Tried to revert spent tx: " <> txHashToHex h
        throwIO TxSpent
      removeChainTx wr td

-------------------------------------------------------------------------------
-- The chain's column families
-------------------------------------------------------------------------------

-- | Data of a new transaction, from its replace-by-fee flag, position, time and
-- the outputs it spends.
prepareTxData :: Bool -> BlockRef -> Word64 -> Tx -> [Unspent] -> TxData
prepareTxData rbf br tt tx us =
  TxData
    { block = br,
      tx = tx,
      prevs = ps,
      deleted = False,
      rbf = rbf,
      timestamp = tt,
      spenders = I.empty
    }
  where
    mkprv Unspent {..} = Prev script value
    ps = I.fromList $ zip [0 ..] $ map mkprv us

-- | Change the balance of an address in the chain, by reading it and
-- writing it whole. An address whose balance goes back to nothing, as when
-- the only block that paid it is reverted, has none.
--
-- The import of a block does not go through here: it changes a balance
-- without reading it ('addChainTx'). This is for taking a block out, and
-- for the import that is kept as the reference.
modifyBalance :: Writer -> Address -> (Balance -> Balance) -> IO ()
modifyBalance wr a f = do
  b <- f . fromMaybe (zeroBalance a) <$> getBalance wr a
  if nullBalance b then deleteBalance wr a else setBalance wr b

-- | Subtract, or stop: no count or amount of the chain goes below zero.
less :: String -> Word64 -> Word64 -> Word64
less what x y
  | y > x = error $ "The " <> what <> " of an address would go below zero"
  | otherwise = x - y

-- | Add a confirmed transaction to every index of the chain, given what
-- was worked out of it and the outputs that it spends, which must be
-- unspent in the chain.
--
-- Its addresses are those worked out before: of its outputs, and of the
-- outputs that it spends, which came with them. Each is taken once, by
-- comparing them: hashing an address, as 'nub'' does, costs more than the
-- few comparisons that a transaction needs. Each output is asked for
-- again as it is spent, which the batch answers from memory: it is how a
-- transaction that spends an output twice is stopped. A mined block is
-- not validated, and this look is the one thing here that does nothing
-- else. It stays because it costs nothing that could be measured: the
-- benchmark took as long without it (@docs\/block-import.md@).
--
-- = Nothing is read to be changed
--
-- A balance is changed by its differences ('mergeBalance') and a spender
-- is added to its group ('mergeSpender') without reading either: the batch
-- writes them as merge records, and RocksDB works out the values when they
-- are read ("Haskoin.Store.Database.Merge"). Reading the balances and the
-- groups of spenders was most of the read ahead of a block on a store of
-- mainnet, and the import only read them to write them back.
--
-- Two things follow. A count or an amount of a balance is not checked
-- against going below zero here, as it is in 'removeChainTx', since that
-- takes the value: an output that is spent was unspent in the chain, so
-- its amount is in the balance. And a balance is never deleted here: the
-- count of transactions of every address that the transaction touches
-- goes up by one, so none of them comes to nothing. Taking a block out
-- reads each value and writes it whole, as 'importBlockSequential' does.
addChainTx :: Writer -> PreparedTx -> TxData -> [Unspent] -> IO ()
addChainTx wr p td us = do
  forM_ (S.toList (S.fromList (mapMaybe (.address) us <> catMaybes p.paid))) $ \a -> do
    insertAddrTx wr a TxRef {block = td.block, txid = h}
    mergeBalance wr a (def :: BalVal) {txs = 1}
  forM_ (zip3 [0 ..] td.tx.outputs p.paid) $ \(i, o, address) -> do
    let u = Unspent {block = td.block, outpoint = OutPoint h i, value = o.value, script = o.script, address}
    insertUnspent wr u
    forM_ address $ \a -> do
      insertAddrUnspent wr a u
      mergeBalance wr a (def :: BalVal) {confirmed = o.value, utxo = 1, received = o.value}
  zipWithM_ spend [0 ..] p.spends
  insertTxOf wr h td
  where
    h = p.txid
    spend ix o = do
      u <-
        getUnspent wr o >>= \case
          Just u -> return u
          Nothing -> error $ "Could not find UTXO to spend: " <> show o
      deleteUnspent wr o
      -- under the key of the output: the record of the transaction that
      -- made it is written once, when it is confirmed, and is not read
      -- here either
      mergeSpender wr o (Spender h ix)
      forM_ u.address $ \a -> do
        deleteAddrUnspent wr a u
        -- taken off by adding the two's complement
        mergeBalance wr a (def :: BalVal) {confirmed = negate u.value, utxo = negate 1}

-- | Take a confirmed transaction that nothing in the chain spends out of
-- every index of the chain, and make the outputs that it spent unspent
-- again.
removeChainTx :: Writer -> TxData -> IO ()
removeChainTx wr td = do
  forM_ (txDataAddresses wr td) $ \a -> do
    deleteAddrTx wr a TxRef {block = td.block, txid = h}
    modifyBalance wr a $ \b -> (b :: Balance) {txs = less "count of transactions" b.txs 1}
  forM_ (zip [0 ..] td.tx.outputs) $ \(i, o) -> do
    let p = OutPoint h i
        u = unspentOutput wr td.block p o
    deleteUnspent wr p
    forM_ u.address $ \a -> do
      deleteAddrUnspent wr a u
      modifyBalance wr a $ \b ->
        (b :: Balance)
          { confirmed = less "confirmed balance" b.confirmed o.value,
            utxo = less "count of unspent outputs" b.utxo 1,
            received = less "amount received" b.received o.value
          }
  forM_ (prevOuts td.tx) $ \p -> do
    parent <-
      chainTx wr p.hash >>= \case
        Just t -> return t
        Nothing -> error $ "Could not find tx data: " <> show p.hash
    let o = fromMaybe (error ("Could not find output: " <> show p)) (getTxOut p.index parent.tx)
        u = unspentOutput wr parent.block p o
    deleteSpender wr p
    insertUnspent wr u
    forM_ u.address $ \a -> do
      insertAddrUnspent wr a u
      modifyBalance wr a $ \b -> (b :: Balance) {confirmed = b.confirmed + o.value, utxo = b.utxo + 1}
  deleteTxData wr h
  where
    h = txHash td.tx

-------------------------------------------------------------------------------
-- The mempool area
-------------------------------------------------------------------------------

-- | Import a transaction into the mempool with the time it arrived. Returns
-- 'False' if it is already known.
--
-- Conflicting unconfirmed transactions are deleted together with their
-- descendants if the network has replace-by-fee; otherwise the transaction is
-- rejected with 'DoubleSpend'. Throws an 'ImportException' if the transaction is
-- invalid, for example if it spends unknown outputs.
newMempoolTx :: Writer -> Tx -> UnixTime -> IO Bool
newMempoolTx wr tx w =
  activeBody wr (txHash tx) >>= \case
    Just _ -> return False
    Nothing -> do
      -- Check everything before writing, so that a rejected transaction
      -- leaves the mempool as it was.
      replaced <- getConflicts wr True True tx
      checkNewTx wr (map txHash replaced) tx
      mapM_ (deleteSingleTx wr . txHash) replaced
      rbf <- isRBF wr tx
      us <- forM (prevOuts tx) $ \p ->
        spendable wr p >>= \case
          Just u -> return u
          Nothing -> do
            logDebug wr $ "Attempted to import a tx missing UTXO: " <> txHashToHex (txHash tx)
            throwIO Orphan
      addMempoolTx wr w (prepareTxData rbf (MemRef w) w tx us)
      return True

-- | Delete an unconfirmed transaction and its descendants. If the flag is set and
-- the network has no replace-by-fee, throw 'DoubleSpend' instead. Does nothing if
-- the transaction is not found.
deleteUnconfirmedTx :: Writer -> Bool -> TxHash -> IO ()
deleteUnconfirmedTx wr rbfcheck th =
  activeBody wr th >>= \case
    Just _ -> getChain wr True rbfcheck th >>= mapM_ (deleteSingleTx wr . txHash)
    Nothing -> logDebug wr $ "Not found or already deleted: " <> txHashToHex th

-- | Write the keys of a transaction to the mempool area, with the time it
-- entered the mempool. The body goes in at that mempool position and without
-- spenders, whatever it had.
addMempoolTx :: Writer -> UnixTime -> TxData -> IO ()
addMempoolTx wr time td = do
  insertMempoolTx wr r.body
  forM_ r.spends (uncurry (insertMempoolSpender wr))
  forM_ r.addresses (uncurry (insertMempoolAddr wr))
  adjustSummaries wr (inMempool wr . (.hash)) (+) r
  addToMempool wr r.txid r.time
  where
    r = mempoolRecordsOf (txHash td.tx) (writerNet wr) time td

-- | Delete the keys of a mempool transaction, given its body, from the
-- mempool area.
removeMempoolTx :: Writer -> TxData -> IO ()
removeMempoolTx wr td = removeMempoolTxOf wr (inMempool wr . (.hash)) (txHash td.tx) td

-- | 'removeMempoolTx' given the hash of the transaction, and how to tell
-- whether the transaction that made an output is in the mempool
-- ('adjustSummaries'): by asking the mempool, or for the import of a block
-- in stages by 'madeInMempool'.
removeMempoolTxOf :: Writer -> (OutPoint -> IO Bool) -> TxHash -> TxData -> IO ()
removeMempoolTxOf wr made h td = do
  deleteMempoolTx wr r.txid
  forM_ r.spends (deleteMempoolSpender wr . fst)
  forM_ r.addresses (deleteMempoolAddr wr . fst)
  adjustSummaries wr made (less "mempool summary") r
  deleteFromMempool wr r.txid
  where
    time = case td.block of
      MemRef t -> t
      BlockRef {} -> td.timestamp
    r = mempoolRecordsOf h (writerNet wr) time td

-- | Whether the transaction that made an output is in the mempool, for
-- an output that a transaction of a block spent while it was in the
-- mempool ('adjustSummaries'). The mempool is not asked when the chain has
-- the output as unspent, which the batch knows from memory: the block
-- read every output that it spends.
--
-- = Why that is the same answer
--
-- An output that is unspent in the chain was made by a confirmed
-- transaction, and a confirmed transaction is not in the mempool. Both
-- hold after every operation of these rules, and between the
-- transactions of a block:
--
-- * Unspent outputs of the chain are only written for confirmed
--   transactions: by 'addChainTx' for one that a block confirms, by
--   'moveChainTx' for a coinbase that is confirmed again, and by
--   'removeChainTx' for the outputs that a reverted transaction had
--   spent, whose transactions stay confirmed. A mempool transaction
--   writes to the mempool area and nowhere else ('addMempoolTx').
--
-- * A transaction is never in both. 'newMempoolTx' refuses one that is
--   confirmed. 'importTxs' takes a transaction out of the mempool before
--   it puts it in the chain, and 'revertBlock' takes each out of the
--   chain, with its unspent outputs, before it gives it to the mempool.
--
-- So it could only be wrong on a database that already had a transaction
-- both listed in the mempool and unspent in the chain, which no version
-- of these rules writes and the migration to this layout checks for
-- ('Haskoin.Store.Database.Migrate.migrateLayered'). The tests hold it to
-- 'importBlockSequential', which asks the mempool every time, over their
-- random histories.
--
-- It is not used where a transaction enters the mempool or is deleted
-- from it: there the batch has not read the outputs, and asking the chain
-- would be a read of RocksDB to save a look in memory.
madeInMempool :: Writer -> OutPoint -> IO Bool
madeInMempool wr p =
  getUnspent wr p >>= \case
    Just _ -> return False
    Nothing -> inMempool wr p.hash

-- | Add what a transaction does at each address it touches to the summary
-- of the address, or take it off again, given how to combine a number of
-- the summary with one of the transaction.
--
-- Everything is in the entries of the transaction but one thing: whether an
-- output that it spends was made by the mempool, and whether an output that
-- it makes is spent by the mempool, which is what 'both' counts. Each is a
-- read by key of what another transaction wrote. An output is counted in
-- 'both' while both of its transactions are in the mempool, so it is added
-- by the second of them to arrive and taken off by the first to leave,
-- whichever that is.
--
-- The second argument answers the first of those two questions for an
-- output: 'inMempool' of its transaction, or 'madeInMempool'.
adjustSummaries :: Writer -> (OutPoint -> IO Bool) -> (Word64 -> Word64 -> Word64) -> MemRecords -> IO ()
adjustSummaries wr madeByMempool op r = do
  deltas <- forM r.addresses $ \case
    (MemAddrTxKey a _ _, _) -> return (a, MemSummary 1 0 0 0 0 0 0)
    (MemAddrSpentKey a p, MemAddrSpentVal v) -> do
      made <- madeByMempool p
      return (a, if made then MemSummary 0 0 0 v 1 v 1 else MemSummary 0 0 0 v 1 0 0)
    (MemAddrOutKey a _ p, MemAddrOutVal v _) -> do
      taken <- isJust <$> getMempoolSpender wr p
      return (a, if taken then MemSummary 0 v 1 0 0 v 1 else MemSummary 0 v 1 0 0 0 0)
    (k, _) -> error ("Not an entry of a transaction: " <> show k)
  -- by comparing the addresses: a transaction has few, and hashing one
  -- costs more than the comparisons
  forM_ (Map.toList (Map.fromListWith plus deltas)) $ \(a, d) -> do
    old <-
      getMempoolAddr wr (MemAddrSumKey a) <&> \case
        Just (MemAddrSumVal s) -> s
        _ -> zero
    let new = combine op old d
    if new == zero
      then deleteMempoolAddr wr (MemAddrSumKey a)
      else insertMempoolAddr wr (MemAddrSumKey a) (MemAddrSumVal new)
  where
    zero = MemSummary 0 0 0 0 0 0 0
    plus = combine (+)
    combine f x y =
      MemSummary
        { txs = f x.txs y.txs,
          paid = f x.paid y.paid,
          paidCount = f x.paidCount y.paidCount,
          spent = f x.spent y.spent,
          spentCount = f x.spentCount y.spentCount,
          both = f x.both y.both,
          bothCount = f x.bothCount y.bothCount
        }

-- | Delete a mempool transaction that nothing spends. Nothing is kept of
-- it. Throws 'TxSpent' if something spends it.
--
-- Until version 22 its body was kept, marked as deleted, and the API
-- returned it with that mark. Clients that did not read the mark took a
-- replaced transaction for a payment, each store had its own set of them,
-- which a fleet cannot answer from, and nothing ever removed them. What
-- the store needs of them, not to fetch a transaction again that it just
-- deleted, it remembers for a while in memory
-- ("Haskoin.Store.BlockStore").
deleteSingleTx :: Writer -> TxHash -> IO ()
deleteSingleTx wr th =
  getMempoolTx wr th >>= \case
    Nothing -> do
      logDebug wr $ "Already deleted: " <> txHashToHex th
      throwIO TxNotFound
    Just td -> do
      spenders <- getMempoolSpenders wr th (length td.tx.outputs)
      if I.null spenders
        then do
          logDebug wr $ "Deleting tx: " <> txHashToHex th
          removeMempoolTx wr td
        else do
          logError wr $ "Tried to delete spent tx: " <> txHashToHex th
          throwIO TxSpent

-- | Check a transaction that will replace these ones. Outputs that they spend
-- count as unspent, and outputs that they create as missing.
checkNewTx :: Writer -> [TxHash] -> Tx -> IO ()
checkNewTx wr replaced tx = do
  when (unique_inputs < length tx.inputs) $ do
    logDebug wr $ "Transaction spends same output twice: " <> txHashToHex (txHash tx)
    throwIO DuplicatePrevOutput
  prevs <- mapM prev_output (prevOuts tx)
  when (any isNothing prevs) $ do
    logDebug wr $ "Orphan: " <> txHashToHex (txHash tx)
    throwIO Orphan
  when (isCoinbaseTx tx) $ do
    logDebug wr $ "Coinbase cannot be imported into mempool: " <> txHashToHex (txHash tx)
    throwIO UnexpectedCoinbase
  when (outputs > sum (map (.value) (catMaybes prevs))) $ do
    logDebug wr $ "Insufficient funds for tx: " <> txHashToHex (txHash tx)
    throwIO InsufficientFunds
  where
    prev_output op
      | op.hash `elem` replaced = return Nothing
      | otherwise = (>>= getTxOut op.index . (.tx)) <$> activeBody wr op.hash
    outputs = sum (map (.value) tx.outputs)
    unique_inputs = length (nub' (map (.outpoint) tx.inputs))

-- | Transactions that spend the same outputs as this one, with their
-- descendants, in an order in which they can be deleted. The first flag
-- refuses confirmed transactions, and the second refuses any on networks
-- without replace-by-fee.
--
-- It is for a transaction that nothing vouches for: one that arrives for
-- the mempool, and those of a block under 'importBlockSequential'. An
-- output that is not there to be spent is looked up in the chain too, so
-- that a transaction which spends what a block has spent is refused. The
-- import of a block in stages asks 'mempoolRivals' instead.
getConflicts :: Writer -> Bool -> Bool -> Tx -> IO [Tx]
getConflicts wr memonly rbfcheck tx = do
  let prevs = prevOuts tx
      h = txHash tx
  unspents <- mapM (spendable wr) prevs
  let spents = [p | (p, Nothing) <- zip prevs unspents]
  spndrs <- catMaybes <$> mapM (spenderOf wr) spents
  let txids = H.fromList $ filter (/= h) $ map (.txid) spndrs
  chains <- mapM (getChain wr memonly rbfcheck) (H.toList txids)
  return . reverse . map snd . sortTxs . M.elems $
    M.fromList [(txHash t, t) | t <- concat chains]

-- | The mempool transactions that a transaction of a block conflicts
-- with: those that spend an output that it spends, with their
-- descendants, in an order in which they can be deleted. It is for a
-- transaction that is not in the mempool itself ('importTxs').
--
-- Only the mempool is asked who spends each output, which is a look in
-- memory. 'getConflicts' also asks whether the output is there to be
-- spent, and who spends it in the chain if it is not. For a transaction
-- of a mined block the first is a read of an output that the block makes,
-- from a database that does not have it, and the second can only find
-- that the block is not valid, which is not checked (the head of this
-- module). What it finds for a block that could be mined is the same:
-- the mempool spenders of the outputs that the transaction spends.
mempoolRivals :: Writer -> PreparedTx -> IO [Tx]
mempoolRivals wr p = do
  takers <- catMaybes <$> mapM (getMempoolSpender wr) p.spends
  chains <- mapM (getChain wr True False) (nub' (map (.txid) takers))
  return . reverse . map snd . sortTxs . M.elems $
    M.fromList [(txHash t, t) | t <- concat chains]

-- | A transaction and all its descendants, in an order in which they can be
-- deleted: spenders first.
getChain :: Writer -> Bool -> Bool -> TxHash -> IO [Tx]
getChain wr memonly rbfcheck th' =
  sort_clean <$> go M.empty (H.singleton th')
  where
    sort_clean = reverse . map snd . sortTxs
    get_tx th =
      activeTx wr th >>= \case
        Nothing -> do
          logDebug wr $ "Transaction not found: " <> txHashToHex th
          return Nothing
        Just td
          | memonly && confirmed td.block -> do
              logDebug wr $ "Transaction already confirmed: " <> txHashToHex th
              throwIO TxConfirmed
          | rbfcheck ->
              if (writerNet wr).replaceByFee
                then return $ Just td
                else do
                  logDebug wr $ "Double-spending transaction: " <> txHashToHex th
                  throwIO DoubleSpend
          | otherwise -> return $ Just td
    go txm pdg = do
      let ths = filter (not . (`M.member` txm)) (H.toList pdg)
      tds <- catMaybes <$> mapM get_tx ths
      let txmn = M.fromList $ fmap (\d -> (txHash d.tx, d.tx)) tds
          spds = concatMap (map (.txid) . I.elems . (.spenders)) tds
          pdg' = H.fromList spds
          txm' = txmn <> txm
      if H.null pdg'
        then return $ M.elems txm'
        else go txm' pdg'

-- | Whether a mempool transaction can be replaced by fee: it signals BIP125
-- in one of its inputs, or it spends an unconfirmed transaction that can be
-- replaced. Always 'False' on networks without replace-by-fee.
isRBF :: Writer -> Tx -> IO Bool
isRBF wr tx
  | not (writerNet wr).replaceByFee = return False
  | any ((< 0xffffffff - 1) . (.sequence)) tx.inputs = return True
  | otherwise = ck (nub' $ map (.outpoint.hash) tx.inputs)
  where
    ck [] = return False
    ck (h : hs') =
      activeBody wr h >>= \case
        Nothing -> return False
        Just t
          | confirmed t.block -> ck hs'
          | t.rbf -> return True
          | otherwise -> ck hs'

-- | Mempool transactions inherit BIP125 signalling from unconfirmed parents
-- only, so their flag can change when a parent is confirmed or goes back to
-- the mempool. Correct the flags of the mempool transactions that spend the
-- outputs of a transaction, and of theirs in turn.
updateSpendersRBF :: Writer -> Tx -> IO ()
updateSpendersRBF wr tx = updateSpendersRBFOf wr (txHash tx) tx

-- | 'updateSpendersRBF' given the hash of the transaction.
updateSpendersRBFOf :: Writer -> TxHash -> Tx -> IO ()
updateSpendersRBFOf wr h tx = do
  spenders <- getMempoolSpenders wr h (length tx.outputs)
  forM_ (nub' (map (.txid) (I.elems spenders))) $ \spender ->
    getMempoolTx wr spender >>= mapM_ update
  where
    update s = do
      rbf <- isRBF wr s.tx
      when (rbf /= s.rbf) $ do
        insertMempoolTx wr (s :: TxData) {rbf = rbf}
        updateSpendersRBF wr s.tx

-------------------------------------------------------------------------------
-- The import of a block as it was, kept as the reference
-------------------------------------------------------------------------------

-- | 'importBlock' as it was until version 1.15.1: one transaction after
-- another, each read of the database made when the rules come to it, every
-- hash worked out where it is used, and every transaction looked up in the
-- chain. It leaves the same database as 'importBlock' and gives the same
-- result, for any block that could be mined.
--
-- It is the reference that the tests compare 'importBlock' with, and what
-- the benchmark measures it against (@haskoin-store-bench import@). Nothing
-- else should call it: it is several times slower. Run it through
-- 'withoutReadCache' to have the reads of the database as they were too.
importBlockSequential :: Writer -> Block -> BlockNode -> IO (BlockData, [TxData])
importBlockSequential wr b n = do
  checkNewBlock wr b n
  let net = writerNet wr
      subsidy = computeSubsidy net n.height
  bs <- getBlocksAtHeight wr n.height
  setBlocksAtHeight wr (nub (headerHash n.header : bs)) n.height
  setBest wr (headerHash n.header)
  tds <- importTxsSequential wr n b.txs
  -- forget what a block replaced 'replacedKept' blocks ago
  when (n.height >= replacedKept) $
    getReplacedAt wr (n.height - replacedKept) >>= mapM_ (deleteReplaced wr)
  let bd =
        BlockData
          { height = n.height,
            main = True,
            work = n.work,
            header = n.header,
            size = fromIntegral (B.length (encode b)),
            txs = map txHash b.txs,
            weight = if net.segWit then w else 0,
            subsidy = subsidy,
            fee = sum $ map txDataFee tds,
            outputs = ts_out_val
          }
  insertBlock wr bd
  logDebug wr $ "Finished importing block: " <> blockHashToHex (headerHash n.header)
  return (bd, tds)
  where
    ts_out_val =
      sum $ map (sum . map (.value) . (.outputs)) (drop 1 b.txs)
    w =
      let f t = (t :: Tx) {witness = []}
          b' = (b :: Block) {txs = map f b.txs}
          x = B.length (encode b)
          s = B.length (encode b')
       in fromIntegral $ s * 3 + x

-- | 'importTxs' for 'importBlockSequential'.
importTxsSequential :: Writer -> BlockNode -> [Tx] -> IO [TxData]
importTxsSequential wr bn txns = do
  mapM_ (freeOutputs . snd) (reverse txs)
  mapM (uncurry action) txs
  where
    txs = sortTxs txns
    br i = BlockRef {height = bn.height, position = i}
    blockTime = fromIntegral bn.header.timestamp
    freeOutputs tx =
      getConflicts wr True False tx
        >>= mapM_
          ( \gone -> do
              deleteSingleTx wr (txHash gone)
              insertReplaced wr (txHash gone) Replaced {by = txHash tx, height = bn.height}
          )
    action i tx =
      getMempoolTx wr (txHash tx) >>= \case
        Just td -> do
          -- it leaves the mempool, and with it the time at which this
          -- store first saw it: in the chain its time is that of its
          -- block, like that of a transaction that was never in the
          -- mempool, so that the record is the same on every store
          removeMempoolTx wr td
          td' <- importChainTxSequential wr (br i) blockTime tx
          updateSpendersRBF wr tx
          return td'
        Nothing ->
          chainTx wr (txHash tx) >>= \case
            Just td -> moveChainTx wr td (br i) blockTime
            Nothing -> importChainTxSequential wr (br i) blockTime tx

-- | 'importChainTx' for 'importBlockSequential'.
importChainTxSequential :: Writer -> BlockRef -> Word64 -> Tx -> IO TxData
importChainTxSequential wr br time tx = do
  us <- forM (prevOuts tx) $ \p ->
    getUnspent wr p >>= \case
      Just u -> return u
      Nothing -> do
        logError wr $ "Attempted to import a tx missing UTXO: " <> txHashToHex (txHash tx)
        throwIO Orphan
  let td = prepareTxData False br time tx us
  addChainTxSequential wr td
  return td

-- | 'addChainTx' for 'importBlockSequential'.
addChainTxSequential :: Writer -> TxData -> IO ()
addChainTxSequential wr td = do
  forM_ (txDataAddresses wr td) $ \a -> do
    insertAddrTx wr a TxRef {block = td.block, txid = h}
    modifyBalance wr a $ \b -> (b :: Balance) {txs = b.txs + 1}
  forM_ (zip [0 ..] td.tx.outputs) $ \(i, o) -> do
    let u = unspentOutput wr td.block (OutPoint h i) o
    insertUnspent wr u
    forM_ u.address $ \a -> do
      insertAddrUnspent wr a u
      modifyBalance wr a $ \b ->
        (b :: Balance)
          { confirmed = b.confirmed + o.value,
            utxo = b.utxo + 1,
            received = b.received + o.value
          }
  zipWithM_ spend [0 ..] (prevOuts td.tx)
  insertTx wr td
  where
    h = txHash td.tx
    spend ix p = do
      u <-
        getUnspent wr p >>= \case
          Just u -> return u
          Nothing -> error $ "Could not find UTXO to spend: " <> show p
      deleteUnspent wr p
      -- under the key of the output: the record of the transaction that
      -- made it is written once, when it is confirmed, and is not read
      -- here either
      insertSpender wr p (Spender h ix)
      forM_ u.address $ \a -> do
        deleteAddrUnspent wr a u
        modifyBalance wr a $ \b ->
          (b :: Balance)
            { confirmed = less "confirmed balance" b.confirmed u.value,
              utxo = less "count of unspent outputs" b.utxo 1
            }

-------------------------------------------------------------------------------
-- Transactions
-------------------------------------------------------------------------------

-- | Output of a transaction at an index, or 'Nothing' if out of range.
getTxOut :: Word32 -> Tx -> Maybe TxOut
getTxOut i tx = do
  guard (fromIntegral i < length tx.outputs)
  return $ tx.outputs !! fromIntegral i

-- | Addresses that a transaction is indexed under: those of its outputs and
-- of the outputs that it spends. An output script counts only when it has
-- an address on the network.
txDataAddresses :: Writer -> TxData -> [Address]
txDataAddresses wr t =
  nub' . rights $
    map (scriptToAddressBS net . (.script)) (I.elems t.prevs)
      <> map (scriptToAddressBS net . (.script)) t.tx.outputs
  where
    net = writerNet wr :: Network

-- | Outputs spent by a transaction, without the null outpoint of a coinbase.
prevOuts :: Tx -> [OutPoint]
prevOuts tx = filter (/= nullOutPoint) (map (.outpoint) tx.inputs)
