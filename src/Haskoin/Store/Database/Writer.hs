{-# LANGUAGE RecordWildCards #-}

-- |
-- Module      : Haskoin.Store.Database.Writer
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Buffered writes to the database.
--
-- A 'Writer' records every change in in-memory hash tables, and its reads look
-- in those tables first and fall back to the database. When the function given
-- to 'runWriter' returns, all the changes are written to RocksDB in a single
-- atomic batch. A block or a set of transactions is thus either fully imported
-- or not at all.
--
-- The writer is a plain handle passed to plain functions in 'IO' ('getTxData',
-- 'insertTx', ...), rather than a monad transformer with class instances: the
-- import logic says which writer it uses, and needs nothing but 'IO' to run.
-- = Why buffer writes
--
-- Importing a block touches thousands of keys, and many of them several times:
-- an output is created and spent in the same block, a balance changes with every
-- transaction of its address. Buffering collapses those into one final value per
-- key, and a single atomic batch makes the whole import all-or-nothing. A crash
-- or an invalid block leaves the database exactly as it was.
--
-- = Reading each value once
--
-- A batch also remembers what it read from the database (its /read cache/):
-- a read looks in the changes of the batch, then in what the batch read
-- before, and only then in the database. The database does not change while
-- a batch is open, as the block store is its only writer and writes one
-- batch at a time, so a value read once is good for the rest of the batch.
-- The changes are always looked at first: the cache can never hide one.
--
-- The cache is there so that the reads of a batch can be done ahead of the
-- rules that need them. Importing a block reads an unspent output for every
-- input, and what the mempool has of the block, thousands of keys, and the
-- rules ask for them one at a time from one thread. The functions under
-- "Reading ahead" read any number of keys with RocksDB's multi-get, split
-- over all cores, and put the answers in the cache, those that say "not
-- there" too. The rules then run as they are and find everything in
-- memory. A key that was not read ahead is read from the database when it
-- is asked for, and cached, so that reading ahead is never needed to be
-- right: it only moves the time of the reads.
--
-- The alternative was to hand the values read ahead to the rules as
-- arguments. That needs a second set of rules for blocks, which takes its
-- values from somewhere else than the rules for the mempool and for
-- reverting, and it must be told what to do when a value is missing. A
-- cache under the reads keeps one set of rules.
--
-- 'withoutReadCache' gives a writer that reads the database every time, as
-- every batch did until version 1.15.1: the tests compare the two.
--
-- = The mempool area, from a copy in memory
--
-- What a batch reads of the mempool area, the two column families of the
-- unconfirmed transactions, it does not read from RocksDB at all. The
-- database keeps a copy of the whole area in memory
-- ("Haskoin.Store.Database.Copy", 'mempoolCopy'), and every read of the
-- area through a batch is answered from it: 'getMempoolTx',
-- 'getMempoolSpender', 'getMempoolSpenders', 'inMempool', 'getMempoolAddr',
-- 'getMempool' and 'mempoolAreaEmpty'. The changes of the batch still come
-- first.
--
-- The copy is right because the block store is the only writer of the
-- area, one batch at a time: when a batch has been written, 'runWriterReport'
-- applies its changes to the copy too ('committing'). A block at the tip
-- read some fifty thousand keys of the area, at eight microseconds of
-- processor time each in RocksDB, though all of them were in memory.
--
-- Most of those reads are now made by the rules when they come to them, a
-- look in a tree for each. Two kinds are still made ahead, on all cores,
-- into the read cache: the bodies of the transactions, which have to be
-- decoded ('readAheadMempoolTxs'), and the summaries of the addresses
-- ('readAheadMempoolSummaries'). @docs\/block-import.md@ has what was
-- measured for each kind.
--
-- A writer of 'withoutReadCache' reads the area from RocksDB, and never
-- loads the copy: it is what the tests hold the copy to.
--
-- = The outputs that mempool transactions spend, remembered
--
-- A batch that puts a transaction in the mempool has read, from the chain,
-- the unspent outputs that the transaction spends. When it is written it
-- leaves them with the copy of the mempool area ('spentChanges',
-- t'Haskoin.Store.Database.Copy.SpentOutput'), and the batch of the block
-- that confirms the transaction takes them from there instead of reading
-- them from RocksDB a second time ('readAheadUnspents'): they were the
-- largest part of the read ahead of a block at the tip.
--
-- Only reading ahead looks there. A read of one output goes to the
-- database as before, and so does everything through 'withoutReadCache'.
-- "Haskoin.Store.Database.Copy" says what keeps an entry from ever being
-- other than what the chain has.
--
-- = Merges: changes to values that the batch has not read
--
-- The import of a block changes the balance of every address that it
-- touches and adds a spender to the group of every output that it spends.
-- It does not need to know either value, and reading them was most of the
-- reads of a block. So a batch can hold, for a balance or a group of
-- spenders, a change to a value that it has not read: the differences to
-- the five numbers of a balance ('mergeBalance'), the spenders to add to a
-- group ('mergeSpender'). They are written as merge records, one for each
-- key, which RocksDB combines with the value when it is read
-- ("Haskoin.Store.Database.Merge").
--
-- For each of those keys a batch holds /either/ a whole value, which it
-- writes as a put or a delete as before, /or/ pending merge data, never
-- both:
--
-- * a merge on a key that has a whole value in the batch is applied to
--   that value, in memory;
--
-- * a whole value written for a key that has pending merge data replaces
--   the data. Whoever worked that value out read the key through the
--   batch, and so saw the data applied;
--
-- * a read through the batch gives the value of the database with the
--   pending merge data applied: 'getBalance', 'getChainSpender',
--   'getChainSpenders'. It reads the database for it, like any read of a
--   key that the batch has not read: what the merge saves is the read of
--   a key that nobody asks for.
--
-- Only the import of a block merges
-- ('Haskoin.Store.Logic.Layered.importBlock'). Reverting a block, and the
-- import of version 1.15.1 that is kept as the reference, read each value
-- and write it whole.
--
-- = Two layouts
--
-- The writes of the first groups below are to the column families that the
-- store uses today, where the chain and the mempool share their indices.
-- The last group is for the layered layout of @docs\/layered-database.md@,
-- in which the mempool has an area of its own: bodies, spenders, address
-- entries and deleted transactions of the mempool. A batch can write to
-- both areas, and is still one atomic write. Only
-- "Haskoin.Store.Logic.Layered" uses that group, on a database opened with
-- 'Haskoin.Store.Database.Reader.withLayeredReader'; a batch that does not
-- use it writes nothing there.
module Haskoin.Store.Database.Writer
  ( -- * Batches
    Writer,
    MempoolChanges (..),
    runWriter,
    runWriterMempool,
    runWriterReport,
    BatchReport (..),
    writerNet,
    writerLogger,
    withoutReadCache,

    -- * Reading ahead
    readsAhead,
    readAheadUnspents,
    readAheadBalances,
    readAheadSpenders,
    readAheadMempoolTxs,
    readAheadMempoolSummaries,
    mempoolAreaEmpty,
    mempoolUntouched,
    forgetSpentOutputs,

    -- * Timing the import of a block
    BlockTimes (..),
    ReadTimes (..),
    noReads,
    noteBlockTimes,
    stopwatch,

    -- * Work on all cores
    onAllCores,

    -- * Reads, with the changes of the batch applied
    getBestBlock,
    getBlocksAtHeight,
    getBlock,
    getTxData,
    getSpender,
    getUnspent,
    getBalance,
    getMempool,

    -- * Writes
    setBest,
    insertBlock,
    setBlocksAtHeight,
    insertTx,
    insertTxOf,
    insertAddrTx,
    deleteAddrTx,
    insertAddrUnspent,
    deleteAddrUnspent,
    addToMempool,
    deleteFromMempool,
    inMempool,
    setBalance,
    mergeBalance,
    insertUnspent,
    deleteUnspent,
    deleteTxData,
    deleteBalance,

    -- * The mempool area of the layered layout
    getMempoolTx,
    getMempoolSpender,
    getMempoolSpenders,
    getChainSpender,
    getChainSpenders,
    insertSpender,
    mergeSpender,
    deleteSpender,
    insertMempoolTx,
    deleteMempoolTx,
    insertMempoolSpender,
    deleteMempoolSpender,
    insertMempoolAddr,
    deleteMempoolAddr,
    getMempoolAddr,

    -- * Transactions that blocks replaced
    insertReplaced,
    deleteReplaced,
    getReplacedAt,
  )
where

import Control.Concurrent (getNumCapabilities, myThreadId, threadCapability)
import Control.Concurrent.Async (wait, withAsyncOn)
import Control.DeepSeq (NFData, force)
import Control.Exception (evaluate, mask_)
import Control.Monad (filterM, forM, unless, when)
import Data.ByteString qualified as B
import Data.ByteString.Builder.Extra (Next (..), runBuilder)
import Data.ByteString.Internal (ByteString (BS))
import Data.Default (def)
import Data.Either (partitionEithers)
import Data.Functor ((<&>))
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.List (sortOn)
import Data.Maybe (catMaybes, fromMaybe, isJust, mapMaybe)
import Data.Ord (Down (..))
import Data.Serialize (Put, Serialize, decode, encode, put, runPut)
import Data.Serialize.Put (execPut, putByteString)
import Data.Word (Word32, Word8)
import Database.RocksDB
  ( BatchOp (..),
    ColumnFamily,
    DB,
    PackedOp (..),
    multiGetCF,
    packOps,
    packedHeaderSize,
    pokePackedHeader,
    writePacked,
  )
import Database.RocksDB.Query
  ( Key,
    KeyValue,
    deleteOp,
    deleteOpCF,
    insertOp,
    insertOpCF,
    retrieveCF,
    writeBatch,
  )
import Foreign.ForeignPtr (ForeignPtr, plusForeignPtr, withForeignPtr)
import Foreign.Ptr (plusPtr)
import GHC.Clock (getMonotonicTime)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import Haskoin
  ( Address,
    BlockHash,
    BlockHeight,
    Network,
    OutPoint (..),
    TxHash,
    headerHash,
    txHash,
  )
import Haskoin.Node (Logger)
import Haskoin.Store.Common (Replaced (..))
import Haskoin.Store.Data
import Haskoin.Store.Database.Copy
  ( Copy,
    CopyChange (..),
    Family (..),
    SpentChange (..),
    applyChanges,
    applySpent,
    firstKey,
    forgetCopy,
    prefixed,
    setCopy,
    shortKey,
    spentOutput,
    spentOutputs,
    spentUnspent,
    valueIn,
  )
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
import Haskoin.Store.Database.Table (Table, TableKey)
import Haskoin.Store.Database.Table qualified as T
import Haskoin.Store.Database.Types
import System.Metrics.StatsD (incrementCounter)

-- | A batch of writes: the database it reads from and writes to, the
-- changes made so far, and where to log. Make one with 'runWriter'.
data Writer = Writer
  { -- | database to read from and write to
    reader :: !DatabaseReader,
    -- | changes made so far, and values read
    memory :: !Memory,
    -- | log messages here
    logger :: !Logger,
    -- | whether a read looks in the values read before going to the
    -- database, and adds to them ('withoutReadCache')
    cached :: !Bool
  }

-- | The same batch, for reads that go to the database every time the batch
-- has not changed what they ask for, as every read did before the read
-- cache, and writes that land where those of the writer given do. Reading
-- ahead does nothing through it.
--
-- It is there for the tests, which run the rules through it to check that
-- the cache changes nothing, and for the benchmark, which measures what the
-- cache and reading ahead gain.
withoutReadCache :: Writer -> Writer
withoutReadCache w = w {cached = False}

-- | Whether reading ahead does anything for this writer: not through
-- 'withoutReadCache'.
readsAhead :: Writer -> Bool
readsAhead w = w.cached

-- | Network of the database of a batch.
writerNet :: Writer -> Network
writerNet w = w.reader.net

-- | Logger passed to 'runWriter'.
writerLogger :: Writer -> Logger
writerLogger w = w.logger

-- | Change to the best block: 'Nothing' if unchanged, @Just Nothing@ if deleted.
type BestRef = IORef (Maybe (Maybe BlockHash))

-- | Changed blocks: 'Nothing' for a deletion.
type BlockTable = Table BlockHash (Maybe BlockData)

-- | Changed lists of blocks at a height.
type HeightTable = Table BlockHeight [BlockHash]

-- | Changed transactions: 'Nothing' for a deletion.
type TxTable = Table TxHash (Maybe TxData)

-- | Changed unspent outputs: 'Nothing' for a deletion.
type UnspentTable = Table OutPoint (Maybe Unspent)

-- | Changed balances: 'Nothing' for a deletion.
type BalanceTable = Table Address (Maybe Balance)

-- | Differences to balances that the batch has not read, each the sum of
-- what the batch does to its address: what a merge record will hold.
type BalanceMergeTable = Table Address BalVal

-- | Changed address transactions: 'Nothing' for a deletion.
type AddrTxTable = Table (Address, TxRef) (Maybe ())

-- | Changed address unspent outputs: 'Nothing' for a deletion.
type AddrOutTable = Table (Address, BlockRef, OutPoint) (Maybe OutVal)

-- | Mempool changes in this batch: 'Nothing' for a deletion.
type MempoolTable = Table TxHash (Maybe UnixTime)

-- | Changed groups of spenders of outputs in the chain.
type SpenderTable = Table (TxHash, Word32) (IntMap Spender)

-- | Changed mempool spenders of outputs: 'Nothing' for a deletion.
type MemSpenderTable = Table OutPoint (Maybe Spender)

-- | Changed entries of the mempool by address: 'Nothing' for a deletion.
type MemAddrTable = Table MemAddrKey (Maybe MemAddrVal)

-- | Changed records of transactions that blocks replaced: 'Nothing' for a
-- deletion.
type ReplacedTable = Table TxHash (Maybe Replaced)

-- | Values that a batch read from the database, by key, with 'Nothing' for
-- a key that the database does not have. They are what the database held
-- when the batch started, and holds until the batch is written.
data ReadCache = ReadCache
  { -- | records of transactions of the chain
    txs :: !TxTable,
    -- | unspent outputs
    unspents :: !UnspentTable,
    -- | address balances
    balances :: !BalanceTable,
    -- | groups of spenders of outputs of the chain, empty for one that the
    -- database does not have
    spenders :: !SpenderTable,
    -- | bodies of mempool transactions, decoded from the copy of the
    -- mempool area
    memTxs :: !TxTable,
    -- | entries of the mempool by address, decoded from the copy of the
    -- mempool area
    memAddrs :: !MemAddrTable,
    -- | whether the database has no transaction in its mempool area, once
    -- 'mempoolAreaEmpty' has looked
    areaEmpty :: !(IORef (Maybe Bool))
  }

-- | Changes made in a batch, not yet written to the database, and what the
-- batch read.
data Memory = Memory
  { -- | network constants
    net :: !Network,
    -- | best block
    best :: !BestRef,
    -- | blocks
    blockTable :: !BlockTable,
    -- | blocks at each height
    heightTable :: !HeightTable,
    -- | transactions
    txTable :: !TxTable,
    -- | unspent outputs
    unspentTable :: !UnspentTable,
    -- | address balances that the batch has as whole values
    balanceTable :: !BalanceTable,
    -- | differences to the balances of addresses that are not in
    -- 'balanceTable': an address is in one of the two at most
    balanceMerges :: !BalanceMergeTable,
    -- | address transactions
    addressTable :: !AddrTxTable,
    -- | address unspent outputs
    outputTable :: !AddrOutTable,
    -- | mempool
    mempoolTable :: !MempoolTable,
    -- | bodies of mempool transactions, in the mempool area
    memTxTable :: !TxTable,
    -- | mempool spenders of outputs, in the mempool area
    memSpenderTable :: !MemSpenderTable,
    -- | entries of the mempool by address, in the mempool area
    memAddrTable :: !MemAddrTable,
    -- | inputs of blocks that spend outputs, by group of outputs
    -- as t'SpenderKey' says, each group as the batch leaves it: empty for one
    -- that has no spent output left
    spenderTable :: !SpenderTable,
    -- | spenders to add to the groups that are not in 'spenderTable', which
    -- the batch has not read: a group is in one of the two at most
    spenderMerges :: !SpenderTable,
    -- | what is kept of transactions that blocks replaced
    replacedTable :: !ReplacedTable,
    -- | values read from the database
    cache :: !ReadCache,
    -- | whether the batch reverted a block ('forgetSpentOutputs')
    reverted :: !(IORef Bool),
    -- | how long the stages of the imports of blocks took, if any block was
    -- imported
    times :: !(IORef (Maybe BlockTimes))
  }

-- | Set the best block.
setBest :: Writer -> BlockHash -> IO ()
setBest w h = writeIORef w.memory.best (Just (Just h))

-- | Add or replace the data of a block.
insertBlock :: Writer -> BlockData -> IO ()
insertBlock w b = T.insert w.memory.blockTable (headerHash b.header) (Just b)

-- | Set the hashes of the blocks at a height.
setBlocksAtHeight :: Writer -> [BlockHash] -> BlockHeight -> IO ()
setBlocksAtHeight w h g = T.insert w.memory.heightTable g h

-- | Add or replace the data of a transaction.
insertTx :: Writer -> TxData -> IO ()
insertTx w t = insertTxOf w (txHash t.tx) t

-- | 'insertTx' given the hash of the transaction, for a caller that has
-- it.
insertTxOf :: Writer -> TxHash -> TxData -> IO ()
insertTxOf w h t = T.insert w.memory.txTable h (Just t)

-- | Add a transaction to the index of an address.
insertAddrTx :: Writer -> Address -> TxRef -> IO ()
insertAddrTx w a t = T.insert w.memory.addressTable (a, t) (Just ())

-- | Remove a transaction from the index of an address.
deleteAddrTx :: Writer -> Address -> TxRef -> IO ()
deleteAddrTx w a t = T.insert w.memory.addressTable (a, t) Nothing

-- | Add an unspent output to the index of an address.
insertAddrUnspent :: Writer -> Address -> Unspent -> IO ()
insertAddrUnspent w a u =
  T.insert w.memory.outputTable (a, u.block, u.outpoint) (Just (OutVal u.value u.script))

-- | Remove an unspent output from the index of an address.
deleteAddrUnspent :: Writer -> Address -> Unspent -> IO ()
deleteAddrUnspent w a u =
  T.insert w.memory.outputTable (a, u.block, u.outpoint) Nothing

-- | Add a transaction to the mempool with the time it arrived.
addToMempool :: Writer -> TxHash -> UnixTime -> IO ()
addToMempool w x t = T.insert w.memory.mempoolTable x (Just t)

-- | Remove a transaction from the mempool.
deleteFromMempool :: Writer -> TxHash -> IO ()
deleteFromMempool w x = T.insert w.memory.mempoolTable x Nothing

-- | Set the balance of an address: the whole value, in place of whatever
-- the batch had for it. Differences that the batch held for the address
-- ('mergeBalance') are dropped: a caller that worked the balance out from
-- 'getBalance' has them in it.
setBalance :: Writer -> Balance -> IO ()
setBalance w b = do
  T.insert w.memory.balanceTable b.address (Just b)
  T.delete w.memory.balanceMerges b.address

-- | Add differences to the balance of an address without reading it: to
-- each of its five numbers, modulo 2^64, so that a number goes down by the
-- two's complement of what is taken off. A balance that is not there
-- counts from zero.
--
-- If the batch has the balance as a whole value, the differences are
-- applied to it. Otherwise they are added to those that the batch holds
-- for the address, and written as one merge record
-- ("Haskoin.Store.Database.Merge").
--
-- Nothing checks that a number does not go below zero, since that takes
-- the value, and the balance is never deleted, even if every number comes
-- to zero. It is for the import of a block, which only adds to the count
-- of transactions of an address. What takes a block out reads the balance
-- and writes it whole ('getBalance', 'setBalance', 'deleteBalance').
mergeBalance :: Writer -> Address -> BalVal -> IO ()
mergeBalance w a d =
  T.lookup w.memory.balanceTable a >>= \case
    Just whole ->
      let sum' = addBalVal (maybe def balanceToVal whole) d
       in T.insert w.memory.balanceTable a (Just (valToBalance a sum'))
    Nothing -> do
      pending <- T.lookup w.memory.balanceMerges a
      T.insert w.memory.balanceMerges a (maybe d (`addBalVal` d) pending)

-- | Add an unspent output.
insertUnspent :: Writer -> Unspent -> IO ()
insertUnspent w u = T.insert w.memory.unspentTable (fst (unspentToVal u)) (Just u)

-- | Remove an unspent output.
deleteUnspent :: Writer -> OutPoint -> IO ()
deleteUnspent w p = T.insert w.memory.unspentTable p Nothing

-- | Remove the data of a transaction.
deleteTxData :: Writer -> TxHash -> IO ()
deleteTxData w h = T.insert w.memory.txTable h Nothing

-- | Remove the balance of an address, as that of one never used, with any
-- differences that the batch held for it ('mergeBalance').
deleteBalance :: Writer -> Address -> IO ()
deleteBalance w a = do
  T.insert w.memory.balanceTable a Nothing
  T.delete w.memory.balanceMerges a

-- | Add or replace the body of a mempool transaction in the mempool area.
insertMempoolTx :: Writer -> TxData -> IO ()
insertMempoolTx w t = T.insert w.memory.memTxTable (txHash t.tx) (Just t)

-- | Remove the body of a mempool transaction from the mempool area.
deleteMempoolTx :: Writer -> TxHash -> IO ()
deleteMempoolTx w h = T.insert w.memory.memTxTable h Nothing

-- | Record the mempool input that spends an output.
insertMempoolSpender :: Writer -> OutPoint -> Spender -> IO ()
insertMempoolSpender w p s = T.insert w.memory.memSpenderTable p (Just s)

-- | Forget the mempool input that spends an output.
deleteMempoolSpender :: Writer -> OutPoint -> IO ()
deleteMempoolSpender w p = T.insert w.memory.memSpenderTable p Nothing

-- | Add an entry of the mempool by address.
insertMempoolAddr :: Writer -> MemAddrKey -> MemAddrVal -> IO ()
insertMempoolAddr w k v = T.insert w.memory.memAddrTable k (Just v)

-- | Remove an entry of the mempool by address.
deleteMempoolAddr :: Writer -> MemAddrKey -> IO ()
deleteMempoolAddr w k = T.insert w.memory.memAddrTable k Nothing

-- | An entry of the mempool by address, with the changes of the batch
-- applied: from the changes, or from what the batch read before, or from
-- the copy of the mempool area, which is then remembered decoded.
getMempoolAddr :: Writer -> MemAddrKey -> IO (Maybe MemAddrVal)
getMempoolAddr w k =
  T.lookup w.memory.memAddrTable k >>= \case
    Just x -> return x
    Nothing -> remembered w (.memAddrs) k (inArea w ByAddress k)

-- | Read a value from the changes of the batch, or from the database if the
-- batch did not change it. For what a batch reads once or twice: the best
-- block, the blocks at a height.
getLayered :: Writer -> (Memory -> IO (Maybe a)) -> (DatabaseReader -> IO a) -> IO a
getLayered w f g =
  f w.memory >>= \case
    Just x -> return x
    Nothing -> g w.reader

-- | Read the value of a key: from the changes of the batch, or else from
-- what the batch read before, or else from the database, which is then
-- remembered. The changes come first, so that nothing read can hide one.
getCached ::
  (TableKey k) =>
  Writer ->
  (Memory -> Table k v) ->
  (ReadCache -> Table k v) ->
  k ->
  (DatabaseReader -> IO v) ->
  IO v
getCached w changes cache k stored =
  T.lookup (changes w.memory) k >>= \case
    Just x -> return x
    Nothing -> remembered w cache k (stored w.reader)

-- | What the database has for a key: as read before in this batch, or
-- read now and remembered.
remembered :: (TableKey k) => Writer -> (ReadCache -> Table k v) -> k -> IO v -> IO v
remembered w cache k stored
  | not w.cached = stored
  | otherwise =
      T.lookup table k >>= \case
        Just x -> return x
        Nothing -> do
          x <- stored
          T.insert table k x
          return x
  where
    table = cache w.memory.cache

-- | A read of the transactions of the mempool area in the database, given
-- what it finds when there are none. Once 'mempoolAreaEmpty' has found the
-- area empty nothing is read, and no key is serialised to look it up.
areaRead :: Writer -> a -> IO a -> IO a
areaRead w nothing stored
  | not w.cached = stored
  | otherwise =
      readIORef w.memory.cache.areaEmpty >>= \case
        Just True -> return nothing
        _ -> stored

-- | What the mempool area of the database has for a key, whatever the
-- batch changed: from the copy of the area that the database keeps in
-- memory ('mempoolCopy'), which is loaded if it is not there, or from
-- RocksDB through 'withoutReadCache'.
inArea :: (KeyValue k v, Serialize k, Serialize v) => Writer -> Family -> k -> IO (Maybe v)
inArea w f k
  | w.cached = fmap decoded . valueIn f (shortKey k) <$> mempoolCopy w.reader
  | otherwise = retrieveCF db (familyCF db f) k
  where
    db = w.reader.db

-- | The column family of a part of the mempool area.
familyCF :: DB -> Family -> ColumnFamily
familyCF db General = mempoolCF db
familyCF db ByAddress = mempoolAddrCF db

-- | What the mempool area of the database has for the mempool inputs that
-- spend the outputs of a transaction, by output: the keys that start with
-- the transaction, in the copy or, through 'withoutReadCache', in RocksDB.
spendersInArea :: Writer -> TxHash -> IO (IntMap Spender)
spendersInArea w h
  | w.cached = spendersIn h <$> mempoolCopy w.reader
  | otherwise = M.getMempoolSpenders w.reader h

-- | The mempool inputs that spend the outputs of a transaction, by output,
-- as a copy of the mempool area has them.
spendersIn :: TxHash -> Copy -> IntMap Spender
spendersIn h c =
  IntMap.fromList
    [ (fromIntegral p.index, decoded v)
    | (k, v) <- prefixed General (shortKey (MemSpenderKeyS h)) c,
      MemSpenderKey p <- [decoded k]
    ]

-------------------------------------------------------------------------------
-- Reading ahead
-------------------------------------------------------------------------------

-- | Multi-gets run at once for each core: 32 on four cores.
--
-- A multi-get is a call into RocksDB that does not hold a core of the
-- Haskell runtime, and it reads its keys one after another. On a database
-- that is not in memory most of its time is spent waiting for the disk,
-- so how many reads the disk is given at once is how many multi-gets run
-- at once. The disk of the store that this was made for gave 5,800 random
-- reads a second one at a time and 31,000 a second 32 at a time.
--
-- When the files are in memory the reads are work for the cores, and more
-- multi-gets than cores only take turns: the benchmark took as long with
-- two for each core as with eight.
readsPerCore :: Int
readsPerCore = 8

-- | Fewest keys worth a multi-get of their own.
smallestRead :: Int
smallestRead = 32

-- | Read the values of many keys of a column family with RocksDB's
-- multi-get, the keys split over 'readsPerCore' calls a core that run at
-- the same time, and work out something from each value in the thread that
-- read it. One multi-get uses one core and reads one key at a time,
-- however many keys it is given: the time of a block went into such reads,
-- with three cores and the disk idle.
--
-- The results are in no particular order.
readMany ::
  (Serialize k, NFData v) =>
  DB ->
  ColumnFamily ->
  (a -> k) ->
  (a -> Maybe ByteString -> v) ->
  [a] ->
  IO [(a, v)]
readMany db cf key value items = concat <$> onAllCores readsPerCore smallestRead chunk items
  where
    chunk xs = do
      found <- multiGetCF db cf (map (encode . key) xs)
      forM (zip xs found) $ \(x, bytes) -> (x,) <$> evaluate (force (value x bytes))

-- | Fewest keys worth a thread of their own when they are looked up in the
-- copy of the mempool area, which takes a fraction of a microsecond each.
smallestLookup :: Int
smallestLookup = 256

-- | Look many keys up in the copy of the mempool area, on all cores, and
-- work out something from each value in the thread that found it: what
-- 'readMany' does with RocksDB. The copy does not change under the
-- threads, and they share it without a lock
-- ("Haskoin.Store.Database.Copy").
--
-- The results are in no particular order.
copyMany ::
  (Serialize k, NFData v) =>
  Writer ->
  Family ->
  (a -> k) ->
  (a -> Maybe ByteString -> v) ->
  [a] ->
  IO [(a, v)]
copyMany w f key value items = do
  c <- mempoolCopy w.reader
  let look x = (x,) <$> evaluate (force (value x (valueIn f (shortKey (key x)) c)))
  concat <$> onAllCores 1 smallestLookup (mapM look) items

-- | Split a list in pieces, as many for each core as the first number says
-- and none shorter than the second unless it is the only one, and work on
-- the pieces at the same time. The results are in the order of the pieces.
--
-- Each piece is given to a thread that is bound to a core, the pieces in
-- turn. Left to the scheduler, new threads start on the core of the thread
-- that made them and move to idle cores only as it gets to them, which for
-- work of a few milliseconds was most of the time: sixteen pieces of
-- serialising took 35 ms that way and 26 ms this way, against 80 ms in one
-- thread.
onAllCores :: Int -> Int -> ([a] -> IO b) -> [a] -> IO [b]
onAllCores perCore least work items = do
  cores <- getNumCapabilities
  let pieces = max 1 (cores * perCore)
      size = max least ((length items + pieces - 1) `div` pieces)
  case chunksOf size items of
    [] -> return []
    [one] -> pure <$> work one
    many -> go (zip [0 ..] many)
  where
    -- every thread is stopped if one of them fails
    go [] = return []
    go ((core, piece) : more) =
      withAsyncOn core (work piece) $ \running -> do
        others <- go more
        (: others) <$> wait running

-- | A list in pieces of a length, the last one shorter.
chunksOf :: Int -> [a] -> [[a]]
chunksOf _ [] = []
chunksOf n xs = let (a, b) = splitAt n xs in a : chunksOf n b

-- | A change of a batch that is not serialised yet.
data Change
  = -- | set a key of a column family to a value
    Set !ColumnFamily Put Put
  | -- | delete a key of a column family
    Drop !ColumnFamily Put
  | -- | add a merge record to a key of a column family, for its merge
    -- operator to combine with the value of the key
    Join !ColumnFamily Put Put
  | -- | an operation that is serialised already
    Ready !BatchOp

-- | The change that sets a key of a column family, with the types that
-- 'Database.RocksDB.Query.insertOpCF' asks for: the value must be of the
-- type that the key is declared to have ('KeyValue').
setOpCF :: (KeyValue k v, Serialize k, Serialize v) => ColumnFamily -> k -> v -> Change
setOpCF cf k v = Set cf (put k) (put v)
  where
    -- never looked at: it is what asks for the pair of types to be declared
    _declared = insertOpCF cf k v

-- | The change that deletes a key of a column family.
dropOpCF :: (Key k, Serialize k) => ColumnFamily -> k -> Change
dropOpCF cf k = Drop cf (put k)
  where
    _declared = deleteOpCF cf k

-- | The change that adds a merge record to a key of a column family. The
-- record is of the type of the values of the key: both merge operators of
-- the store take records in the format of a value
-- ("Haskoin.Store.Database.Merge").
joinOpCF :: (KeyValue k v, Serialize k, Serialize v) => ColumnFamily -> k -> v -> Change
joinOpCF cf k v = Join cf (put k) (put v)
  where
    _declared = insertOpCF cf k v

-- | The changes of a batch, serialised for RocksDB in one of two forms.
data Encoded
  = -- | a few operations, each with byte strings of its own
    Plain [BatchOp]
  | -- | chunks of packed records for 'writePacked', in the order of the
    -- changes, with the number of records and the bytes of their keys and
    -- values
    Packed [ByteString] !Int !Int

-- | Serialise the changes of a batch for RocksDB, on all cores, in the
-- order of the changes.
--
-- = Why not 'encode' for each key and value
--
-- A block changes a hundred thousand keys, nearly all of them shorter than
-- a hundred bytes. 'encode' takes a buffer of four kilobytes for each key
-- and each value, writes into it and copies the result out: a gigabyte of
-- buffers for a block. And every byte string that it returns is memory
-- that the collector may not move, which GHC hands out under a lock that
-- all cores share, so that doing it on four cores was barely faster than
-- on one: 82 ms against 127 for the batch of a block.
--
-- Here each core serialises its share of the changes into a few large
-- buffers, one after another ('arenaSize'). Nothing is taken for a single
-- key, and the cores do not wait for each other.
--
-- = Why packed records, and not an operation for each key
--
-- The buffers could hold just the keys and the values, each a slice of one
-- in a 'BatchOp', for RocksDB to be given the operations one foreign call
-- at a time. That was how it was done first, and it was measured. A thread
-- gives up its capability for such a call and has to get one back, and
-- the calls for the 65,000 keys of a block were 43 ms of the 102 that it
-- took to hand RocksDB the batch and have it written, with nothing else
-- running. Next to busy threads each call waits for a capability
-- (@docs\/block-import.md@).
--
-- So what is written into a buffer is the batch itself, in the form that
-- 'writePacked' of the RocksDB binding takes: for each change a header
-- with its column family, its lengths and what it does, then the key, then
-- the value. A buffer is handed over in one foreign call, a few thousand
-- changes at a time, and a change has no byte string, no 'BatchOp' and no
-- list cell of its own. RocksDB copies the bytes when it is given a
-- buffer, and the buffers go when the batch is written.
--
-- The header of a record comes before its key and holds the lengths of
-- the key and of the value, which are only known once both are
-- serialised. So room is left for it, the key and the value are serialised
-- after that room, and the header is filled in last ('packChange').
--
-- = Small batches
--
-- A batch of a few changes, such as that of a mempool transaction, is
-- serialised with 'encode' and given to RocksDB one operation at a time, as
-- before. A buffer of this size for each would be the waste that this is
-- meant to remove, and what a few dozen foreign calls cost is not worth a
-- second way to pack. It also keeps the old way in use, which the tests
-- compare the new one against: the same changes must leave the same
-- database whether they are written in one large batch or a few at a time.
encodeChanges :: (ColumnFamily -> Maybe Family) -> [Change] -> IO (Encoded, [CopyChange])
encodeChanges area changes
  | null (drop 255 changes) =
      let ops = map plain changes
       in return (Plain ops, mapMaybe (areaChange area) ops)
  | otherwise = do
      pieces <- onAllCores 2 512 inArenas changes
      return
        ( Packed
            (concat [chunks | (chunks, _, _, _) <- pieces])
            (sum [records | (_, records, _, _) <- pieces])
            (sum [size | (_, _, size, _) <- pieces]),
          concat [noted | (_, _, _, noted) <- pieces]
        )
  where
    inArenas piece = do
      arena <- newIORef =<< newArena
      mapM_ (packChange area arena) piece
      Arena buffer start used chunks records size noted <- readIORef arena
      return (reverse (sealed buffer start used chunks), records, size, reverse noted)

-- | What an operation of a batch changes in the mempool area, if it is on
-- one of its two column families, given how to tell them
-- ('mempoolFamily'). The byte strings are those of the operation.
areaChange :: (ColumnFamily -> Maybe Family) -> BatchOp -> Maybe CopyChange
areaChange area = \case
  PutCF cf k v -> area cf <&> \f -> CopySet f k v
  DelCF cf k -> area cf <&> \f -> CopyDrop f k
  MergeCF cf _ _ | isJust (area cf) -> error "A merge record for the mempool area, which has no merge operator"
  _ -> Nothing

-- | The operation of a change, with byte strings of its own.
plain :: Change -> BatchOp
plain = \case
  Set cf k v -> PutCF cf (runPut k) (runPut v)
  Drop cf k -> DelCF cf (runPut k)
  Join cf k v -> MergeCF cf (runPut k) (runPut v)
  Ready op -> op

-- | Bytes of the key of an operation, and of its value or merge record if
-- it has one.
opBytes :: BatchOp -> Int
opBytes = \case
  Put k v -> B.length k + B.length v
  PutCF _ k v -> B.length k + B.length v
  Del k -> B.length k
  DelCF _ k -> B.length k
  Merge k v -> B.length k + B.length v
  MergeCF _ k v -> B.length k + B.length v

-- | Where one core packs its share of the changes of a batch.
--
-- The fields are: the buffer that records are written into, one after
-- another; where in it the records start that are in no chunk yet; where
-- the next record goes; the chunks so far, the last one first; the
-- number of records in the chunks and in the buffer, with the bytes of
-- their keys and values; and the changes among them to the mempool area,
-- the last one first, for the copy of the area
-- ("Haskoin.Store.Database.Copy").
--
-- A chunk is a slice of a buffer, so the buffer lives as long as its
-- chunks, and a buffer can give more than one: see 'packChange'. The keys
-- and values of the changes to the mempool area are slices of it too, and
-- are let go when the copy has copied them.
data Arena = Arena !(ForeignPtr Word8) !Int !Int [ByteString] !Int !Int ![CopyChange]

-- | Bytes of the buffer of an t'Arena': room for a few thousand keys and
-- values, and for nearly any transaction. It is also the most that RocksDB
-- is handed in one foreign call, which then takes a fraction of a
-- millisecond.
arenaSize :: Int
arenaSize = 256 * 1024

newArena :: IO Arena
newArena = do
  buffer <- mallocPlainForeignPtrBytes arenaSize
  return (Arena buffer 0 0 [] 0 0 [])

-- | The chunks of an arena, the last one first, with the records of its
-- buffer that are in no chunk yet as one more.
sealed :: ForeignPtr Word8 -> Int -> Int -> [ByteString] -> [ByteString]
sealed buffer start used chunks
  | used > start = BS (buffer `plusForeignPtr` start) (used - start) : chunks
  | otherwise = chunks

-- | Whether a record could be written at the end of a buffer.
data Fit
  = -- | it is there, with so many bytes of key and so many of value
    Fits !Int !Int
  | -- | the buffer has no room for it
    NoRoom
  | -- | it brings a large byte string of its own, which is not for copying
    -- into a shared buffer
    Alone

-- | Pack a change as a record at the end of what an arena holds: room for
-- the header, the key, the value, and then the header, now that the
-- lengths are known.
--
-- A record that does not fit in what is left of the buffer starts a new
-- one. One that does not fit in an empty buffer either, or that brings a
-- large byte string of its own (a script of eight kilobytes or more), is
-- serialised by itself as 'encode' would, and is a chunk of its own.
-- The order of the changes is the order of the records, so the records
-- before it are made a chunk first, and those after it go on in the same
-- buffer.
--
-- A change to the mempool area, which the first argument tells from the
-- others ('mempoolFamily'), is also noted with where its key and its value
-- are, for the copy of the area.
packChange :: (ColumnFamily -> Maybe Family) -> IORef Arena -> Change -> IO ()
packChange area ref change = case change of
  Set cf k v -> record (Just cf) PackedPut k (Just v)
  Drop cf k -> record (Just cf) PackedDel k Nothing
  Join cf k v -> record (Just cf) PackedMerge k (Just v)
  Ready (PutCF cf k v) -> record (Just cf) PackedPut (putByteString k) (Just (putByteString v))
  Ready (DelCF cf k) -> record (Just cf) PackedDel (putByteString k) Nothing
  Ready (MergeCF cf k v) -> record (Just cf) PackedMerge (putByteString k) (Just (putByteString v))
  Ready (Put k v) -> record Nothing PackedPut (putByteString k) (Just (putByteString v))
  Ready (Del k) -> record Nothing PackedDel (putByteString k) Nothing
  Ready (Merge k v) -> record Nothing PackedMerge (putByteString k) (Just (putByteString v))
  where
    record cf op k v = do
      Arena buffer start used chunks records size noted <- readIORef ref
      let body = used + packedHeaderSize
          key = execPut k
          value = execPut <$> v
      fit <-
        if body > arenaSize
          then return NoRoom
          else withForeignPtr buffer $ \base -> do
            let header = base `plusPtr` used
                done klen vlen = do
                  pokePackedHeader header cf op klen vlen
                  return (Fits klen vlen)
            (klen, next) <- runBuilder key (base `plusPtr` body) (arenaSize - body)
            case (next, value) of
              (Done, Nothing) -> done klen 0
              (Done, Just builder) -> do
                let room = arenaSize - body - klen
                (vlen, next') <- runBuilder builder (base `plusPtr` (body + klen)) room
                case next' of
                  Done -> done klen vlen
                  More _ _ -> return NoRoom
                  Chunk _ _ -> return Alone
              (More _ _, _) -> return NoRoom
              (Chunk _ _, _) -> return Alone
      case fit of
        Fits klen vlen -> do
          let bytes = klen + vlen
              slice from len = BS (buffer `plusForeignPtr` from) len
              noted' = case cf >>= area of
                Nothing -> noted
                Just f -> case op of
                  PackedPut -> CopySet f (slice body klen) (slice (body + klen) vlen) : noted
                  PackedDel -> CopyDrop f (slice body klen) : noted
                  PackedMerge -> error "A merge record for the mempool area, which has no merge operator"
          writeIORef ref (Arena buffer start (body + bytes) chunks (records + 1) (size + bytes) noted')
        NoRoom | used > 0 -> do
          fresh <- mallocPlainForeignPtrBytes arenaSize
          writeIORef ref (Arena fresh 0 0 (sealed buffer start used chunks) records size noted)
          record cf op k v
        _ -> do
          let whole = plain change
              before = sealed buffer start used chunks
              noted' = maybe noted (: noted) (areaChange area whole)
          writeIORef ref $
            Arena buffer used used (packOps [whole] : before) (records + 1) (size + opBytes whole) noted'

-- | Decode a value read from the database.
decoded :: (Serialize v) => ByteString -> v
decoded = either error id . decode

-- | Read ahead: put what the database has for some keys in the read cache
-- of the batch, those that it does not have too, given how to read them
-- all. Keys that the batch has read already are not read again, and
-- nothing is read through 'withoutReadCache'.
readAhead ::
  (TableKey k) =>
  Writer ->
  (ReadCache -> Table k v) ->
  [k] ->
  ([k] -> IO [(k, v)]) ->
  IO ()
readAhead w cache keys reading = when w.cached $ do
  let table = cache w.memory.cache
  asked <- T.new
  -- each key once, and none that was read before
  let fresh k =
        T.lookup table k >>= \case
          Just _ -> return False
          Nothing ->
            T.lookup asked k >>= \case
              Just () -> return False
              Nothing -> T.insert asked k () >> return True
  new <- filterM fresh keys
  reading new >>= mapM_ (uncurry (T.insert table))

-- | Read unspent outputs ahead of the reads of the batch.
--
-- An output that a mempool transaction spends is not read from RocksDB if
-- the block store remembers it from the batch that put the transaction in
-- the mempool ('spentChanges'): it is taken from the table that is kept
-- with the copy of the mempool area, on all cores, as an entry is turned
-- back into an unspent output with its address. The others are read with
-- multi-gets as before. At the tip of the chain nearly every output that a
-- block spends is remembered, and they were the largest part of its read
-- ahead.
--
-- The table is only looked at if the database has a copy of its mempool
-- area already: with no copy there is nothing remembered.
readAheadUnspents :: Writer -> [OutPoint] -> IO ()
readAheadUnspents w ps = readAhead w (.unspents) ps $ \new -> do
  table <- maybe HashMap.empty spentOutputs <$> knownMempoolCopy w.reader
  (kept, rest) <-
    if HashMap.null table
      then return ([], new)
      else partitionEithers . concat <$> onAllCores 1 smallestLookup (mapM (look table)) new
  found <- readMany db (unspentCF db) UnspentKey (\p -> fmap (valToUnspent w.reader.net p . decoded)) rest
  Q.count w.reader $ \s -> incrementCounter s.dataUnspentCount (length (filter (isJust . snd) found))
  return (kept <> found)
  where
    db = w.reader.db
    look table p = case HashMap.lookup p table of
      Nothing -> return (Right p)
      Just s -> Left . (p,) . Just <$> evaluate (force (spentUnspent w.reader.net p s))

-- | Read balances ahead of the reads of the batch. The import of a block
-- does not need it any more, as it changes balances without reading them
-- ('mergeBalance'): it is for rules that read them.
readAheadBalances :: Writer -> [Address] -> IO ()
readAheadBalances w as = readAhead w (.balances) as $ \new -> do
  Q.count w.reader $ \s -> incrementCounter s.dataBalanceCount (length new)
  readMany db (balanceCF db) BalKey (\a -> fmap (valToBalance a . decoded)) new
  where
    db = w.reader.db

-- | Read ahead the spenders in the chain of the groups of outputs that
-- these outputs are in: what 'getChainSpender' and 'insertSpender' read
-- for each of them. The import of a block does not need it: it adds
-- spenders without reading them ('mergeSpender'), and does not ask who
-- spends an output in the chain, as a mined block is not validated
-- ('Haskoin.Store.Logic.Layered.importBlock'). It is for rules that read
-- them.
readAheadSpenders :: Writer -> [OutPoint] -> IO ()
readAheadSpenders w ps =
  readAhead w (.spenders) [(p.hash, p.index `div` spenderGroup) | p <- ps] $
    readMany db (spenderCF db) (uncurry SpenderKey) (\_ -> maybe IntMap.empty (group . decoded))
  where
    db = w.reader.db
    group (SpenderVal m) = m

-- | Read ahead the bodies that the mempool area has for these
-- transactions: looked up in the copy of the area and decoded, on all
-- cores. A body is a transaction, and decoding it is the one read of the
-- area that costs more than a look in a table: a block at the tip decodes
-- all of its transactions here, where the rules would do it one by one.
readAheadMempoolTxs :: Writer -> [TxHash] -> IO ()
readAheadMempoolTxs w hs =
  unlessEmpty w . readAhead w (.memTxs) hs $ copyMany w General TxKey (\_ -> fmap decoded)

-- | Read ahead the summaries of what the mempool does at these addresses:
-- looked up in the copy of the area and decoded, on all cores. The rules
-- ask for the summary of every address of every transaction that leaves
-- the mempool, and serialising the key of an address is most of what such
-- a read costs.
readAheadMempoolSummaries :: Writer -> [Address] -> IO ()
readAheadMempoolSummaries w as =
  unlessEmpty w . readAhead w (.memAddrs) (map MemAddrSumKey as) $
    copyMany w ByAddress id (\_ -> fmap decoded)

-- | Do something unless the mempool area of the database is known to be
-- empty.
unlessEmpty :: Writer -> IO () -> IO ()
unlessEmpty w action = readIORef w.memory.cache.areaEmpty >>= \e -> unless (e == Just True) action

-- | Whether the database has no transaction in its mempool area: no body,
-- no spender and no entry in the list of the mempool. It is 'False'
-- through 'withoutReadCache', whatever the database has.
--
-- Once this has found the area empty, the reads of the batch that ask it
-- for something ('getMempoolTx', 'getMempoolSpender', 'getMempoolSpenders',
-- 'inMempool') answer from the changes of the batch alone. A store that
-- syncs or catches up has an empty mempool, and its blocks would ask the
-- area about every transaction and every input all the same.
--
-- = Why look, and how
--
-- The alternatives were to count the transactions of the mempool somewhere,
-- which is one more thing to keep right through migrations and wipes, or
-- to take the list of the mempool for the whole area, which leans on the
-- rules having kept the two in step. The first key says what is there. The
-- transactions of the mempool have every key of the column family but one
-- kind: bodies of deleted transactions, which sort after all the others,
-- which version 21 wrote and which nothing reads. So the area is empty when
-- the column family has no key, or when its first key is one of those.
--
-- The first key is that of the copy of the area in memory
-- ('mempoolCopy'), which this loads if no batch has needed it before.
mempoolAreaEmpty :: Writer -> IO Bool
mempoolAreaEmpty w
  | not w.cached = return False
  | otherwise =
      readIORef ref >>= \case
        Just e -> return e
        Nothing -> do
          e <-
            firstKey General <$> mempoolCopy w.reader <&> \case
              Nothing -> True
              Just k -> B.take 1 k >= encode MemDeletedKeyS
          writeIORef ref (Just e)
          return e
  where
    ref = w.memory.cache.areaEmpty

-- | Say that the batch reverts a block: when it is written, nothing is
-- remembered any more of the outputs that mempool transactions spend
-- ("Haskoin.Store.Database.Copy").
--
-- A revert takes the outputs of a block out of the chain while the
-- mempool transactions that spend them stay, and puts back the outputs
-- that the block spent. The changes that the batch makes to unspent
-- outputs are applied to what is remembered in any case ('spentChanges'),
-- so this is not needed to be right. It is there so that being right
-- after a reorganisation, which is rare and which the tests reach less
-- often than a block, rests on nothing at all: the table is empty.
forgetSpentOutputs :: Writer -> IO ()
forgetSpentOutputs w = writeIORef w.memory.reverted True

-- | Whether the batch has not changed the mempool so far.
mempoolUntouched :: Writer -> IO Bool
mempoolUntouched w =
  and
    <$> sequence
      [ null <$> T.toList w.memory.mempoolTable,
        null <$> T.toList w.memory.memTxTable,
        null <$> T.toList w.memory.memSpenderTable,
        null <$> T.toList w.memory.memAddrTable
      ]

-------------------------------------------------------------------------------
-- Timing the import of a block
-------------------------------------------------------------------------------

-- | How long each stage of the import of a block took, in seconds. A batch
-- that imported several blocks has the sum for each stage.
data BlockTimes = BlockTimes
  { -- | working out, from the block alone, what the import needs of each
    -- transaction: its hash, the addresses of its outputs, its size
    prepare :: !Double,
    -- | reading ahead what the import reads from the database
    prefetch :: !Double,
    -- | how much of that each kind of key took
    reads :: !ReadTimes,
    -- | the rules: every change of the block, made in memory
    apply :: !Double,
    -- | encoding the changes and writing them to RocksDB in one batch
    write :: !Double
  }
  deriving (Eq, Show)

-- | How long the read ahead of a block took for each kind of key, in
-- seconds. The kinds are read one after another, each on all cores, with
-- multi-gets but for the mempool, so the four add up to nearly all of the
-- read ahead.
--
-- They are timed apart because they are not alike. An output that a block
-- spends was read when its transaction came into the mempool, and is
-- likely to be in RocksDB's cache still. The balances and the groups of
-- spenders of the chain were not read then, and are not read at all any
-- more: their two times are kept, as zero, for what reads them from a
-- report. Which kind a slow block waits for says what would make it
-- faster: @docs\/block-import-next.md@.
data ReadTimes = ReadTimes
  { -- | the outputs that the block spends, as unspent outputs of the chain
    unspentReads :: !Double,
    -- | groups of spenders in the chain: nothing, as the import of a block
    -- adds spenders without reading them since database version 23, and
    -- no longer reads any to check the block
    spenderReads :: !Double,
    -- | the balances of the addresses that the block pays or spends from:
    -- nothing since database version 23, as the import of a block changes
    -- them without reading them
    balanceReads :: !Double,
    -- | what is read ahead of the mempool area, from its copy in memory:
    -- the bodies of the transactions of the block that are in the mempool,
    -- which are decoded, and the summaries of its addresses. Nothing when
    -- the area is empty. The other reads of the area are made by the rules
    -- as they go, and are part of the time of the rules
    mempoolReads :: !Double
  }
  deriving (Eq, Show)

-- | No time spent reading: of an import that did not read ahead.
noReads :: ReadTimes
noReads = ReadTimes {unspentReads = 0, spenderReads = 0, balanceReads = 0, mempoolReads = 0}

-- | Note how long the first three stages of the import of a block took,
-- and each kind of key of the read ahead. 'runWriterReport' adds how long
-- the batch took to write.
noteBlockTimes :: Writer -> Double -> Double -> ReadTimes -> Double -> IO ()
noteBlockTimes w prepare prefetch kinds apply =
  modifyIORef' w.memory.times $ \case
    Nothing -> Just BlockTimes {prepare, prefetch, reads = kinds, apply, write = 0}
    Just t ->
      Just
        (t :: BlockTimes)
          { prepare = t.prepare + prepare,
            prefetch = t.prefetch + prefetch,
            reads =
              ReadTimes
                { unspentReads = t.reads.unspentReads + kinds.unspentReads,
                  spenderReads = t.reads.spenderReads + kinds.spenderReads,
                  balanceReads = t.reads.balanceReads + kinds.balanceReads,
                  mempoolReads = t.reads.mempoolReads + kinds.mempoolReads
                },
            apply = t.apply + apply
          }

-- | Run an action and say how many seconds it took.
stopwatch :: IO a -> IO (a, Double)
stopwatch action = do
  start <- getMonotonicTime
  x <- action
  end <- getMonotonicTime
  return (x, end - start)

-- | Run a batch of writes and commit all its changes to the database in one
-- atomic write. Reads within the batch see its own changes. If the function
-- throws, nothing is written.
runWriter ::
  Network ->
  DatabaseReader ->
  Logger ->
  (Writer -> IO a) ->
  IO a
runWriter net bdb logger f =
  fst <$> runWriterMempool net bdb logger f

-- | Transactions that a batch added to the mempool or removed from it,
-- compared to what the database held before the batch was written.
data MempoolChanges = MempoolChanges
  { -- | transactions that entered the mempool
    added :: ![TxHash],
    -- | transactions that left the mempool
    removed :: ![TxHash]
  }
  deriving (Eq, Show)

-- | Like 'runWriter', but also say how the batch changed the mempool, so
-- that callers do not have to read the whole mempool before and after.
runWriterMempool ::
  Network ->
  DatabaseReader ->
  Logger ->
  (Writer -> IO a) ->
  IO (a, MempoolChanges)
runWriterMempool net bdb logger f =
  fmap (.mempool) <$> runWriterReport net bdb logger f

-- | What a batch did, besides what its function returned.
data BatchReport = BatchReport
  { -- | how it changed the mempool
    mempool :: !MempoolChanges,
    -- | how long the stages of the import of a block took, if it imported
    -- one
    block :: !(Maybe BlockTimes),
    -- | keys that it set, deleted or wrote a merge record for
    operations :: !Int,
    -- | bytes of those keys, and of the values and merge records that it
    -- wrote: about what RocksDB appends to its write-ahead log for the
    -- batch
    bytes :: !Int
  }
  deriving (Eq, Show)

-- | Like 'runWriterMempool', but say everything about the batch, in a
-- t'BatchReport': for the metrics of the block store, and for the
-- benchmark.
--
-- The changes are still written in one batch, with one write to the
-- write-ahead log, however many there are: that is what makes a block all
-- or nothing, and a disk that is slow to sync is asked to once.
runWriterReport ::
  Network ->
  DatabaseReader ->
  Logger ->
  (Writer -> IO a) ->
  IO (a, BatchReport)
runWriterReport net bdb@DatabaseReader {db} logger f = do
  hm <- newMemory net
  let w = Writer {reader = bdb, memory = hm, logger, cached = True}
  x <- f w
  ((changes, operations, bytes), took) <- stopwatch $ do
    ops <- hashMapOps db hm
    (mops, changes) <- mempoolOps w
    (encoded, area) <- encodeChanges (mempoolFamily db) (ops <> mops)
    spent <- knownMempoolCopy bdb >>= maybe (return []) (spentChanges w)
    committing bdb area spent $ case encoded of
      Plain batch -> do
        writeBatch db batch
        return (changes, length batch, sum (map opBytes batch))
      Packed chunks records size -> do
        writePacked db chunks
        return (changes, records, size)
  times <- readIORef hm.times
  let block = (\t -> (t :: BlockTimes) {write = took}) <$> times
  return (x, BatchReport {mempool = changes, block, operations, bytes})

-- | Write a batch to the database, and bring the copy of the mempool area
-- up to date with it, given the changes of the batch to the area and to
-- what is remembered of the outputs that mempool transactions spend
-- ('spentChanges'): when the write is done, the copy with those changes
-- takes the place of the copy that the database had, in one step.
--
-- = Never a copy that is not the database
--
-- What must not happen is a copy that stays in use and is not what the
-- database has. So the order is: say that the copy is not known
-- ('forgetCopy'), write, and only then set the new copy. Whatever goes
-- wrong in between, a write that throws, of which RocksDB may have kept a
-- part, or the thread being killed, leaves the database without a copy,
-- and the next batch that needs one loads it from the column families.
-- That costs a scan, a third of a second for a full mempool, and is right
-- whatever was written.
--
-- The three steps are also masked, so that an asynchronous exception that
-- arrives while RocksDB writes is not delivered between the write and the
-- new copy, and a batch that was written does not cost a scan for
-- nothing. Masking alone would not do: the write itself can throw.
--
-- A batch whose function threw never gets here, and one that changes
-- nothing in the area or in what is remembered leaves the copy alone. If
-- the database has no copy,
-- because no batch has needed one yet, none is made: it is loaded when it
-- is asked for.
--
-- = While RocksDB writes
--
-- The new copy is worked out by another thread while RocksDB writes the
-- batch, which it does in one thread of its own and outside the Haskell
-- runtime. A block at the tip deletes sixty thousand keys from the copy.
-- Done after the write, that added 40 ms to the 170 that the write of
-- such a block takes in the benchmark.
--
-- The thread is bound to another core of the runtime than the one that
-- this runs on. Left on the same one it gained next to nothing, 202 ms
-- against 210: the thread that comes back from RocksDB wants that core
-- again. On the next core the write takes the 170 ms that it took
-- without a copy.
committing :: DatabaseReader -> [CopyChange] -> [SpentChange] -> IO a -> IO a
committing r area spent write
  | null area && null spent = write
  | otherwise =
      knownMempoolCopy r >>= \case
        Nothing -> write
        Just old -> mask_ $ do
          forgetCopy r.copy
          cores <- getNumCapabilities
          (here, _) <- threadCapability =<< myThreadId
          withAsyncOn ((here + 1) `mod` cores) (evaluate (applySpent spent (applyChanges area old))) $ \new -> do
            x <- write
            wait new >>= setCopy r.copy
            return x

-- | What a batch does, when it is written, to what the block store
-- remembers of the outputs that mempool transactions spend, given the
-- copy of the mempool area that holds it: for each of those outputs, a
-- t'Haskoin.Store.Database.Copy.SpentOutput'.
--
-- * A batch that reverted a block empties the table ('forgetSpentOutputs').
--
-- * An output whose mempool spender the batch took away is forgotten.
--
-- * An output that the batch gave a mempool spender is remembered, if the
--   batch knows the unspent output of the chain: as the batch leaves it,
--   if the batch changed it, or else as the batch read it, which is what
--   the database has until the batch is written. A new mempool
--   transaction read every output that it spends ('getUnspent'), so they
--   are all there. An output that the chain does not have as unspent, as
--   one made by another mempool transaction, is not remembered. One that
--   the batch has not read keeps what was remembered of it.
--
-- * An unspent output of the chain that the batch changed, and whose
--   mempool spender it left alone, has its entry changed with it or
--   dropped, if it has one. Without this, an entry could outlive what it
--   says: a coinbase that is confirmed a second time moves its unspent
--   outputs to the later block, and a mempool transaction that spends one
--   stays where it is.
--
-- So an entry is always what the database has for its output, whatever
-- the rules did in the batch: only batches write unspent outputs, and
-- every unspent output that a batch writes is looked up here.
--
-- The list is not worked out when the table is empty and the batch gives
-- no output a mempool spender, as while a store syncs.
spentChanges :: Writer -> Copy -> IO [SpentChange]
spentChanges w old = do
  reverted <- readIORef w.memory.reverted
  spenders <- T.toList w.memory.memSpenderTable
  case () of
    _
      | reverted -> return [ForgetAll | not empty]
      | empty && null spenders -> return []
      | otherwise -> do
          unspents <- if empty then return [] else T.toList w.memory.unspentTable
          changed <- concat <$> mapM (uncurry spender) spenders
          return ([Correct p (u >>= spentOutput) | (p, u) <- unspents] <> changed)
  where
    empty = HashMap.null (spentOutputs old)
    spender p Nothing = return [Forget p]
    spender p (Just _) =
      -- the unspent output of the chain as the batch leaves it, if the
      -- batch changed it or read it
      T.lookup w.memory.unspentTable p >>= maybe (T.lookup w.memory.cache.unspents p) (return . Just) <&> \case
        Just (Just u) | Just kept <- spentOutput u -> [Remember p kept]
        Just _ -> [Forget p]
        Nothing -> []

-- | Database operations for all changes of a batch except the mempool.
hashMapOps :: DB -> Memory -> IO [Change]
hashMapOps db Memory {..} =
  mconcat
    <$> sequence
      [ bestBlockOp best,
        blockHashOps db blockTable,
        blockHeightOps db heightTable,
        txOps db txTable,
        balOps db balanceTable,
        balMergeOps db balanceMerges,
        addrTxOps db addressTable,
        addrOutOps db outputTable,
        unspentOps db unspentTable,
        memTxOps db memTxTable,
        memSpenderOps db memSpenderTable,
        memAddrOps db memAddrTable,
        spenderOps db spenderTable,
        spenderMergeOps db spenderMerges,
        replacedOps db replacedTable
      ]

-- | Operation for the change of the best block, if any.
bestBlockOp :: BestRef -> IO [Change]
bestBlockOp r =
  readIORef r >>= \case
    Nothing -> return []
    Just Nothing -> return [Ready (deleteOp BestKey)]
    Just (Just b) -> return [Ready (insertOp BestKey b)]

-- | Operations for changed blocks.
blockHashOps :: DB -> BlockTable -> IO [Change]
blockHashOps db t = map (uncurry f) <$> (T.toList t)
  where
    f k (Just d) = setOpCF (blockCF db) (BlockKey k) d
    f k Nothing = dropOpCF (blockCF db) (BlockKey k)

-- | Operations for changed records of replaced transactions: the record,
-- and its entry at the height of its block. A record that is deleted, or
-- kept again at another height, takes its old entry with it.
replacedOps :: DB -> ReplacedTable -> IO [Change]
replacedOps db t = concat <$> (T.toList t >>= mapM f)
  where
    f (h, new) = do
      old <- retrieveCF db (blockCF db) (ReplacedKey h) :: IO (Maybe Replaced)
      let forget =
            [ dropOpCF (heightCF db) (ReplacedAtKey o.height h)
            | Just o <- [old],
              fmap (.height) new /= Just o.height
            ]
      return . (forget <>) $ case new of
        Just r -> [setOpCF (blockCF db) (ReplacedKey h) r, setOpCF (heightCF db) (ReplacedAtKey r.height h) ()]
        Nothing -> [dropOpCF (blockCF db) (ReplacedKey h)]

-- | Operations for changed block heights.
blockHeightOps :: DB -> HeightTable -> IO [Change]
blockHeightOps db t = map (uncurry f) <$> (T.toList t)
  where
    f = setOpCF (heightCF db) . HeightKey

-- | Operations for changed transactions.
txOps :: DB -> TxTable -> IO [Change]
txOps db t = map (uncurry f) <$> (T.toList t)
  where
    f k (Just t') = setOpCF (txCF db) (TxKey k) t'
    f k Nothing = dropOpCF (txCF db) (TxKey k)

-- | Operations for changed balances.
balOps :: DB -> BalanceTable -> IO [Change]
balOps db t = map (uncurry f) <$> (T.toList t)
  where
    f a (Just b) = setOpCF (balanceCF db) (BalKey a) (balanceToVal b)
    f a Nothing = dropOpCF (balanceCF db) (BalKey a)

-- | A merge record for each address whose balance the batch changes
-- without having read it: the sum of the differences.
balMergeOps :: DB -> BalanceMergeTable -> IO [Change]
balMergeOps db t = map (\(a, d) -> joinOpCF (balanceCF db) (BalKey a) d) <$> T.toList t

-- | Operations for changed address transactions.
addrTxOps :: DB -> AddrTxTable -> IO [Change]
addrTxOps db t = map (uncurry f) <$> (T.toList t)
  where
    f (a, t') (Just ()) = setOpCF (addrTxCF db) (AddrTxKey a t') ()
    f (a, t') Nothing = dropOpCF (addrTxCF db) (AddrTxKey a t')

-- | Operations for changed address unspent outputs.
addrOutOps :: DB -> AddrOutTable -> IO [Change]
addrOutOps db t = map (uncurry f) <$> (T.toList t)
  where
    f (a, b, p) (Just l) =
      setOpCF
        (addrOutCF db)
        (AddrOutKey a b p)
        l
    f (a, b, p) Nothing =
      dropOpCF
        (addrOutCF db)
        (AddrOutKey a b p)

-- | Operations for the mempool changes of a batch. Each changed entry is
-- compared to the database: its time is needed to update the index by
-- time, and the comparison says which transactions really entered or left
-- the mempool.
--
-- What the database has is taken from the copy of the mempool area if the
-- database has one, and read from RocksDB otherwise. The copy is not
-- loaded for it: a database whose batches all read RocksDB
-- ('withoutReadCache') stays without one.
mempoolOps :: Writer -> IO ([Change], MempoolChanges)
mempoolOps w = do
  changes <- T.toList w.memory.mempoolTable
  -- from the copy of the mempool area only if a batch has asked for it
  -- before: the batch of 'withoutReadCache' reads the database here too
  known <- knownMempoolCopy w.reader
  let before h = case known of
        Just c -> return (decoded <$> valueIn General (shortKey (MemTxKey h)) c)
        Nothing -> retrieveCF db cf (MemTxKey h)
  rows <- forM changes $ \(h, new) -> do
    old <- before h
    return (h, old, new)
  return
    ( concatMap ops rows,
      MempoolChanges
        { added = [h | (h, Nothing, Just _) <- rows],
          removed = [h | (h, Just _, Nothing) <- rows]
        }
    )
  where
    db = w.reader.db
    cf = mempoolCF db
    ops (_, Nothing, Nothing) = []
    ops (h, Nothing, Just n) = map Ready (mempoolInsertOps db h n)
    ops (h, Just o, Nothing) =
      [dropOpCF cf (MemTxKey h), dropOpCF cf (MemTimeKey o h)]
    ops (h, Just o, Just n)
      | o == n = []
      | otherwise = dropOpCF cf (MemTimeKey o h) : map Ready (mempoolInsertOps db h n)

-- | Operations for changed bodies of mempool transactions.
memTxOps :: DB -> TxTable -> IO [Change]
memTxOps db t = map (uncurry f) <$> T.toList t
  where
    f h (Just td) = setOpCF (mempoolCF db) (TxKey h) td
    f h Nothing = dropOpCF (mempoolCF db) (TxKey h)

-- | Operations for the changed groups of spenders of the chain.
spenderOps :: DB -> SpenderTable -> IO [Change]
spenderOps db t = map (uncurry f) <$> T.toList t
  where
    f (h, g) m
      | IntMap.null m = dropOpCF (spenderCF db) (SpenderKey h g)
      | otherwise = setOpCF (spenderCF db) (SpenderKey h g) (SpenderVal m)

-- | A merge record for each group of spenders that the batch adds to
-- without having read it: the spenders to add.
spenderMergeOps :: DB -> SpenderTable -> IO [Change]
spenderMergeOps db t =
  map (\((h, g), m) -> joinOpCF (spenderCF db) (SpenderKey h g) (SpenderVal m)) <$> T.toList t

-- | Operations for changed mempool spenders.
memSpenderOps :: DB -> MemSpenderTable -> IO [Change]
memSpenderOps db t = map (uncurry f) <$> T.toList t
  where
    f p (Just s) = setOpCF (mempoolCF db) (MemSpenderKey p) s
    f p Nothing = dropOpCF (mempoolCF db) (MemSpenderKey p)

-- | Operations for changed entries of the mempool by address. Their column
-- family is only looked up if there are any, as a database of the layout in
-- use today does not have it.
memAddrOps :: DB -> MemAddrTable -> IO [Change]
memAddrOps db t = map (uncurry f) <$> T.toList t
  where
    f k (Just v) = setOpCF (mempoolAddrCF db) k v
    f k Nothing = dropOpCF (mempoolAddrCF db) k

-- | Operations for changed unspent outputs.
unspentOps :: DB -> UnspentTable -> IO [Change]
unspentOps db t = map (uncurry f) <$> (T.toList t)
  where
    f p (Just u) =
      setOpCF (unspentCF db) (UnspentKey p) (snd (unspentToVal u))
    f p Nothing =
      dropOpCF (unspentCF db) (UnspentKey p)

-- | Best block, with the changes of the batch applied.
getBestBlock :: Writer -> IO (Maybe BlockHash)
getBestBlock w = getLayered w getBestBlockH Q.getBestBlock

-- | Blocks at a height, with the changes of the batch applied.
getBlocksAtHeight :: Writer -> BlockHeight -> IO [BlockHash]
getBlocksAtHeight w bh = getLayered w (getBlocksAtHeightH bh) (`Q.getBlocksAtHeight` bh)

-- | Data of a block, with the changes of the batch applied.
getBlock :: Writer -> BlockHash -> IO (Maybe BlockData)
getBlock w bh = getLayered w (getBlockH bh) (`Q.getBlock` bh)

-- | Data of a transaction, deleted ones included, with the changes of the
-- batch applied.
getTxData :: Writer -> TxHash -> IO (Maybe TxData)
getTxData w th = getCached w (.txTable) (.txs) th (`Q.getTxRecord` th)

-- | Spender of an output, with the changes of the batch applied.
getSpender :: Writer -> OutPoint -> IO (Maybe Spender)
getSpender w op =
  -- the rules of version 20 keep spenders in the record of the transaction
  -- that made the output, and this is theirs
  (>>= IntMap.lookup (fromIntegral op.index) . (.spenders)) <$> getTxData w op.hash

-- | Balance of an address, with the changes of the batch applied: the
-- whole value if the batch has one, and otherwise what the database has
-- with the differences that the batch holds for the address added to it
-- ('mergeBalance').
getBalance :: Writer -> Address -> IO (Maybe Balance)
getBalance w a =
  T.lookup w.memory.balanceTable a >>= \case
    Just whole -> return whole
    Nothing -> do
      stored <- remembered w (.balances) a (Q.getBalance w.reader a)
      T.lookup w.memory.balanceMerges a <&> \case
        Nothing -> stored
        Just d -> Just (valToBalance a (addBalVal (maybe def balanceToVal stored) d))

-- | Unspent output, with the changes of the batch applied.
getUnspent :: Writer -> OutPoint -> IO (Maybe Unspent)
getUnspent w op = getCached w (.unspentTable) (.unspents) op (`Q.getUnspent` op)

-- | Whether a transaction is in the mempool, with the changes of the batch
-- applied. It looks for the small key that lists the transaction, in the
-- copy of the mempool area, where 'getMempoolTx' decodes its whole body: a
-- rule that only needs to know asks this.
inMempool :: Writer -> TxHash -> IO Bool
inMempool w h =
  T.lookup w.memory.mempoolTable h >>= \case
    Just change -> return (isJust change)
    Nothing -> isJust <$> storedMempoolTime w h

-- | The time at which the database has a transaction entering the mempool,
-- if it lists it, whatever the batch changed: as the copy of the mempool
-- area has it.
storedMempoolTime :: Writer -> TxHash -> IO (Maybe UnixTime)
storedMempoolTime w h = areaRead w Nothing (inArea w General (MemTxKey h))

-- | Body of a transaction in the mempool area, without its spenders, with
-- the changes of the batch applied: from the changes, or as the batch
-- decoded it before, or decoded from the copy of the mempool area and
-- remembered.
getMempoolTx :: Writer -> TxHash -> IO (Maybe TxData)
getMempoolTx w h =
  T.lookup w.memory.memTxTable h >>= \case
    Just x -> return x
    Nothing -> areaRead w Nothing (remembered w (.memTxs) h (inArea w General (TxKey h)))

-- | Mempool input that spends an output, with the changes of the batch
-- applied: from the changes, or from the copy of the mempool area.
getMempoolSpender :: Writer -> OutPoint -> IO (Maybe Spender)
getMempoolSpender w p =
  T.lookup w.memory.memSpenderTable p >>= \case
    Just x -> return x
    Nothing -> areaRead w Nothing (inArea w General (MemSpenderKey p))

-- | Mempool inputs that spend the outputs of a transaction, by output
-- index, with the changes of the batch applied. The second argument is the
-- number of outputs of the transaction: what the database has for it is
-- the keys of the copy of the mempool area that start with the
-- transaction, and the changes of the batch are looked up for each output.
getMempoolSpenders :: Writer -> TxHash -> Int -> IO (IntMap Spender)
getMempoolSpenders w h outputs = do
  had <- areaRead w IntMap.empty (spendersInArea w h)
  changed <- catMaybes <$> mapM change [0 .. outputs - 1]
  return $ IntMap.mapMaybe id (IntMap.union (IntMap.fromList changed) (IntMap.map Just had))
  where
    change i = fmap (i,) <$> T.lookup w.memory.memSpenderTable (OutPoint h (fromIntegral i))

-- | The spenders of a group of outputs of a transaction, with the changes
-- of the batch applied: the whole group if the batch has one, and
-- otherwise what the database has with the spenders that the batch is to
-- add to it ('mergeSpender'), which win for an output that both have.
spenderGroupOf :: Writer -> TxHash -> Word32 -> IO (IntMap Spender)
spenderGroupOf w h g =
  T.lookup w.memory.spenderTable (h, g) >>= \case
    Just whole -> return whole
    Nothing -> do
      stored <-
        remembered w (.spenders) (h, g) $
          retrieveCF db (spenderCF db) (SpenderKey h g) <&> \case
            Just (SpenderVal m) -> m
            Nothing -> IntMap.empty
      maybe stored (`IntMap.union` stored) <$> T.lookup w.memory.spenderMerges (h, g)
  where
    db = w.reader.db

-- | Note that an input of a block spends an output, in the group of
-- outputs that it belongs to, by reading the group and writing it whole.
-- The record of the transaction that made the output is not read and not
-- written. Spenders that the batch was to add to the group ('mergeSpender')
-- are in the group that is read, and so in the one that is written.
insertSpender :: Writer -> OutPoint -> Spender -> IO ()
insertSpender w p s = do
  let g = p.index `div` spenderGroup
  m <- spenderGroupOf w p.hash g
  T.insert w.memory.spenderTable (p.hash, g) (IntMap.insert (fromIntegral p.index) s m)
  T.delete w.memory.spenderMerges (p.hash, g)

-- | Note that an input of a block spends an output, without reading the
-- group of outputs that it belongs to.
--
-- If the batch has the group as a whole value, the spender is put in it.
-- Otherwise it is added to those that the batch holds for the group, which
-- are written as one merge record: RocksDB joins it with the group when
-- the group is read ("Haskoin.Store.Database.Merge").
--
-- It is for the import of a block. 'insertSpender' does the same by
-- reading the group, as taking a spender out has to ('deleteSpender').
mergeSpender :: Writer -> OutPoint -> Spender -> IO ()
mergeSpender w p s =
  T.lookup w.memory.spenderTable key >>= \case
    Just whole -> T.insert w.memory.spenderTable key (IntMap.insert i s whole)
    Nothing -> do
      pending <- T.lookup w.memory.spenderMerges key
      T.insert w.memory.spenderMerges key (IntMap.insert i s (fromMaybe IntMap.empty pending))
  where
    key = (p.hash, p.index `div` spenderGroup)
    i = fromIntegral p.index

-- | Note that no block spends an output any more, by reading the group of
-- outputs that it belongs to and writing it whole, with the spenders that
-- the batch was to add to it ('mergeSpender').
deleteSpender :: Writer -> OutPoint -> IO ()
deleteSpender w p = do
  let g = p.index `div` spenderGroup
  m <- spenderGroupOf w p.hash g
  T.insert w.memory.spenderTable (p.hash, g) (IntMap.delete (fromIntegral p.index) m)
  T.delete w.memory.spenderMerges (p.hash, g)

-- | The input of a block that spends an output, with the changes of the
-- batch applied.
getChainSpender :: Writer -> OutPoint -> IO (Maybe Spender)
getChainSpender w p = IntMap.lookup (fromIntegral p.index) <$> spenderGroupOf w p.hash (p.index `div` spenderGroup)

-- | The inputs of blocks that spend the outputs of a transaction, by
-- output, with the changes of the batch applied, given the number of its
-- outputs: what the database has, read with one scan, with the groups
-- that the batch has whole in place of the stored ones, and the spenders
-- that the batch is to add to the other groups ('mergeSpender').
getChainSpenders :: Writer -> TxHash -> Int -> IO (IntMap Spender)
getChainSpenders w h outputs = do
  stored <- Q.getChainSpenders w.reader h
  let groups = [0 .. (fromIntegral (max 1 outputs) - 1) `div` spenderGroup]
  changed <- catMaybes <$> mapM (\g -> fmap (g,) <$> T.lookup w.memory.spenderTable (h, g)) groups
  added <- catMaybes <$> mapM (\g -> T.lookup w.memory.spenderMerges (h, g)) groups
  let replaced = [g | (g, _) <- changed]
      kept = IntMap.filterWithKey (\i _ -> (fromIntegral i `div` spenderGroup) `notElem` replaced) stored
  -- the first of the union wins: what the batch adds comes before what
  -- the database has, and no group is both whole and added to
  return (IntMap.unions (added <> (kept : map snd changed)))

-- | Keep what a block replaced.
insertReplaced :: Writer -> TxHash -> Replaced -> IO ()
insertReplaced w h r = T.insert w.memory.replacedTable h (Just r)

-- | Forget what was kept of a replaced transaction.
deleteReplaced :: Writer -> TxHash -> IO ()
deleteReplaced w h = T.insert w.memory.replacedTable h Nothing

-- | The transactions that the block at a height replaced, with the changes
-- of the batch applied.
getReplacedAt :: Writer -> BlockHeight -> IO [TxHash]
getReplacedAt w height = do
  stored <- Q.getReplacedAt w.reader height
  changes <- T.toList w.memory.replacedTable
  let changed = HashMap.fromList changes
      kept = filter (not . (`HashMap.member` changed)) stored
      added = [h | (h, Just r) <- changes, r.height == height]
  return (kept <> added)

-- | The mempool in the database with the changes of this batch applied,
-- newest first: the index by time of the copy of the mempool area, and the
-- changes.
getMempool :: Writer -> IO [(UnixTime, TxHash)]
getMempool w = do
  listed <- if w.cached then inCopy <$> mempoolCopy w.reader else Q.getMempool w.reader
  changes <- T.toList w.memory.mempoolTable
  let changed = HashMap.fromList changes
      unchanged = filter (not . (`HashMap.member` changed) . snd) listed
      added = [(t, h) | (h, Just t) <- changes]
  return $ sortOn Down (unchanged <> added)
  where
    -- the index by time, as the copy of the mempool area has it
    inCopy c = [(t, h) | (k, _) <- prefixed General (shortKey MemTimeKeyS) c, MemTimeKey t h <- [decoded k]]

-- | Empty set of changes.
newMemory ::
  Network ->
  IO Memory
newMemory net = do
  best <- newIORef Nothing
  blockTable <- T.new
  heightTable <- T.new
  txTable <- T.new
  unspentTable <- T.new
  balanceTable <- T.new
  balanceMerges <- T.new
  addressTable <- T.new
  outputTable <- T.new
  mempoolTable <- T.new
  memTxTable <- T.new
  memSpenderTable <- T.new
  memAddrTable <- T.new
  spenderTable <- T.new
  spenderMerges <- T.new
  replacedTable <- T.new
  cache <- newReadCache
  reverted <- newIORef False
  times <- newIORef Nothing
  return Memory {..}

-- | Nothing read yet.
newReadCache :: IO ReadCache
newReadCache = do
  txs <- T.new
  unspents <- T.new
  balances <- T.new
  spenders <- T.new
  memTxs <- T.new
  memAddrs <- T.new
  areaEmpty <- newIORef Nothing
  return ReadCache {..}

-- | Read the best block from the changes of the batch: 'Nothing' if unchanged, @Just Nothing@ if deleted.
getBestBlockH :: Memory -> IO (Maybe (Maybe BlockHash))
getBestBlockH = readIORef . (.best)

-- | Read the blocks at a height from the changes of the batch: 'Nothing' if unchanged, @Just Nothing@ if deleted.
getBlocksAtHeightH :: BlockHeight -> Memory -> IO (Maybe [BlockHash])
getBlocksAtHeightH h s = T.lookup s.heightTable h

-- | Read a block from the changes of the batch: 'Nothing' if unchanged, @Just Nothing@ if deleted.
getBlockH :: BlockHash -> Memory -> IO (Maybe (Maybe BlockData))
getBlockH h s = T.lookup s.blockTable h
