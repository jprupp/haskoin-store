{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NoFieldSelectors #-}

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
module Haskoin.Store.Database.Writer
  ( -- * Batches
    Writer,
    MempoolChanges (..),
    runWriter,
    runWriterMempool,
    writerNet,
    writerCtx,
    writerLogger,

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
    insertAddrTx,
    deleteAddrTx,
    insertAddrUnspent,
    deleteAddrUnspent,
    addToMempool,
    deleteFromMempool,
    setBalance,
    insertUnspent,
    deleteUnspent,
  )
where

import Control.Monad (forM)
import Data.HashMap.Strict qualified as HashMap
import Data.HashTable.IO qualified as H
import Data.IntMap.Strict qualified as IntMap
import Data.List (sortOn)
import Data.Ord (Down (..))
import Database.RocksDB (BatchOp, DB)
import Database.RocksDB.Query
  ( deleteOp,
    deleteOpCF,
    insertOp,
    insertOpCF,
    retrieveCF,
    writeBatch,
  )
import Haskoin
  ( Address,
    BlockHash,
    BlockHeight,
    Ctx,
    Network,
    OutPoint (..),
    TxHash,
    headerHash,
    txHash,
  )
import Haskoin.Node (Logger)
import Haskoin.Store.Data
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
import Haskoin.Store.Database.Types
import UnliftIO
  ( IORef,
    MonadIO,
    liftIO,
    newIORef,
    readIORef,
    writeIORef,
  )

-- | A batch of writes: the database it reads from and writes to, the
-- changes made so far, and where to log. Make one with 'runWriter'.
data Writer = Writer
  { -- | database to read from and write to
    reader :: !DatabaseReader,
    -- | changes made so far
    memory :: !Memory,
    -- | log messages here
    logger :: !Logger
  }

-- | Network of the database of a batch.
writerNet :: Writer -> Network
writerNet w = w.reader.net

-- | Secp256k1 context of the database of a batch.
writerCtx :: Writer -> Ctx
writerCtx w = w.reader.ctx

-- | Logger passed to 'runWriter'.
writerLogger :: Writer -> Logger
writerLogger w = w.logger

-- | Change to the best block: 'Nothing' if unchanged, @Just Nothing@ if deleted.
type BestRef = IORef (Maybe (Maybe BlockHash))

-- | Changed blocks: 'Nothing' for a deletion.
type BlockTable = H.BasicHashTable BlockHash (Maybe BlockData)

-- | Changed lists of blocks at a height.
type HeightTable = H.BasicHashTable BlockHeight [BlockHash]

-- | Changed transactions: 'Nothing' for a deletion.
type TxTable = H.BasicHashTable TxHash (Maybe TxData)

-- | Changed unspent outputs: 'Nothing' for a deletion.
type UnspentTable = H.BasicHashTable OutPoint (Maybe Unspent)

-- | Changed balances: 'Nothing' for a deletion.
type BalanceTable = H.BasicHashTable Address (Maybe Balance)

-- | Changed address transactions: 'Nothing' for a deletion.
type AddrTxTable = H.BasicHashTable (Address, TxRef) (Maybe ())

-- | Changed address unspent outputs: 'Nothing' for a deletion.
type AddrOutTable = H.BasicHashTable (Address, BlockRef, OutPoint) (Maybe OutVal)

-- | Mempool changes in this batch: 'Nothing' for a deletion.
type MempoolTable = H.BasicHashTable TxHash (Maybe UnixTime)

-- | Changes made in a batch, not yet written to the database.
data Memory = Memory
  { -- | network constants
    net :: !Network,
    -- | secp256k1 context
    ctx :: !Ctx,
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
    -- | address balances
    balanceTable :: !BalanceTable,
    -- | address transactions
    addressTable :: !AddrTxTable,
    -- | address unspent outputs
    outputTable :: !AddrOutTable,
    -- | mempool
    mempoolTable :: !MempoolTable
  }

-- | Set the best block.
setBest :: Writer -> BlockHash -> IO ()
setBest w h = writeIORef w.memory.best (Just (Just h))

-- | Add or replace the data of a block.
insertBlock :: Writer -> BlockData -> IO ()
insertBlock w b = H.insert w.memory.blockTable (headerHash b.header) (Just b)

-- | Set the hashes of the blocks at a height.
setBlocksAtHeight :: Writer -> [BlockHash] -> BlockHeight -> IO ()
setBlocksAtHeight w h g = H.insert w.memory.heightTable g h

-- | Add or replace the data of a transaction.
insertTx :: Writer -> TxData -> IO ()
insertTx w t = H.insert w.memory.txTable (txHash t.tx) (Just t)

-- | Add a transaction to the index of an address.
insertAddrTx :: Writer -> Address -> TxRef -> IO ()
insertAddrTx w a t = H.insert w.memory.addressTable (a, t) (Just ())

-- | Remove a transaction from the index of an address.
deleteAddrTx :: Writer -> Address -> TxRef -> IO ()
deleteAddrTx w a t = H.insert w.memory.addressTable (a, t) Nothing

-- | Add an unspent output to the index of an address.
insertAddrUnspent :: Writer -> Address -> Unspent -> IO ()
insertAddrUnspent w a u =
  H.insert w.memory.outputTable (a, u.block, u.outpoint) (Just (OutVal u.value u.script))

-- | Remove an unspent output from the index of an address.
deleteAddrUnspent :: Writer -> Address -> Unspent -> IO ()
deleteAddrUnspent w a u =
  H.insert w.memory.outputTable (a, u.block, u.outpoint) Nothing

-- | Add a transaction to the mempool with the time it arrived.
addToMempool :: Writer -> TxHash -> UnixTime -> IO ()
addToMempool w x t = H.insert w.memory.mempoolTable x (Just t)

-- | Remove a transaction from the mempool.
deleteFromMempool :: Writer -> TxHash -> IO ()
deleteFromMempool w x = H.insert w.memory.mempoolTable x Nothing

-- | Set the balance of an address.
setBalance :: Writer -> Balance -> IO ()
setBalance w b = H.insert w.memory.balanceTable b.address (Just b)

-- | Add an unspent output.
insertUnspent :: Writer -> Unspent -> IO ()
insertUnspent w u = H.insert w.memory.unspentTable (fst (unspentToVal u)) (Just u)

-- | Remove an unspent output.
deleteUnspent :: Writer -> OutPoint -> IO ()
deleteUnspent w p = H.insert w.memory.unspentTable p Nothing

-- | Read a value from the changes of the batch, or from the database if the
-- batch did not change it.
getLayered :: Writer -> (Memory -> IO (Maybe a)) -> (DatabaseReader -> IO a) -> IO a
getLayered w f g =
  f w.memory >>= \case
    Just x -> return x
    Nothing -> g w.reader

-- | Run a batch of writes and commit all its changes to the database in one
-- atomic write. Reads within the batch see its own changes. If the function
-- throws, nothing is written.
runWriter ::
  Network ->
  Ctx ->
  DatabaseReader ->
  Logger ->
  (Writer -> IO a) ->
  IO a
runWriter net ctx bdb logger f =
  fst <$> runWriterMempool net ctx bdb logger f

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
  Ctx ->
  DatabaseReader ->
  Logger ->
  (Writer -> IO a) ->
  IO (a, MempoolChanges)
runWriterMempool net ctx bdb@DatabaseReader {db} logger f = do
  hm <- newMemory net ctx
  x <- f Writer {reader = bdb, memory = hm, logger}
  ops <- hashMapOps db hm
  (mops, changes) <- mempoolOps db hm.mempoolTable
  liftIO $ writeBatch db (ops <> mops)
  return (x, changes)

-- | Database operations for all changes of a batch except the mempool.
hashMapOps :: (MonadIO m) => DB -> Memory -> m [BatchOp]
hashMapOps db Memory {..} =
  mconcat
    <$> sequence
      [ bestBlockOp best,
        blockHashOps db blockTable,
        blockHeightOps db heightTable,
        txOps db txTable,
        balOps db balanceTable,
        addrTxOps db addressTable,
        addrOutOps db outputTable,
        unspentOps db unspentTable
      ]

-- | Operation for the change of the best block, if any.
bestBlockOp :: (MonadIO m) => BestRef -> m [BatchOp]
bestBlockOp r =
  readIORef r >>= \case
    Nothing -> return []
    Just Nothing -> return [deleteOp BestKey]
    Just (Just b) -> return [insertOp BestKey b]

-- | Operations for changed blocks.
blockHashOps :: (MonadIO m) => DB -> BlockTable -> m [BatchOp]
blockHashOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f k (Just d) = insertOpCF (blockCF db) (BlockKey k) d
    f k Nothing = deleteOpCF (blockCF db) (BlockKey k)

-- | Operations for changed block heights.
blockHeightOps :: (MonadIO m) => DB -> HeightTable -> m [BatchOp]
blockHeightOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f = insertOpCF (heightCF db) . HeightKey

-- | Operations for changed transactions.
txOps :: (MonadIO m) => DB -> TxTable -> m [BatchOp]
txOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f k (Just t') = insertOpCF (txCF db) (TxKey k) t'
    f k Nothing = deleteOpCF (txCF db) (TxKey k)

-- | Operations for changed balances.
balOps :: (MonadIO m) => DB -> BalanceTable -> m [BatchOp]
balOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f a (Just b) = insertOpCF (balanceCF db) (BalKey a) (balanceToVal b)
    f a Nothing = deleteOpCF (balanceCF db) (BalKey a)

-- | Operations for changed address transactions.
addrTxOps :: (MonadIO m) => DB -> AddrTxTable -> m [BatchOp]
addrTxOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f (a, t') (Just ()) = insertOpCF (addrTxCF db) (AddrTxKey a t') ()
    f (a, t') Nothing = deleteOpCF (addrTxCF db) (AddrTxKey a t')

-- | Operations for changed address unspent outputs.
addrOutOps :: (MonadIO m) => DB -> AddrOutTable -> m [BatchOp]
addrOutOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f (a, b, p) (Just l) =
      insertOpCF
        (addrOutCF db)
        (AddrOutKey a b p)
        l
    f (a, b, p) Nothing =
      deleteOpCF
        (addrOutCF db)
        (AddrOutKey a b p)

-- | Operations for the mempool changes of a batch. Each changed entry is
-- compared to the database: its time is needed to update the index by
-- time, and the comparison says which transactions really entered or left
-- the mempool.
mempoolOps :: (MonadIO m) => DB -> MempoolTable -> m ([BatchOp], MempoolChanges)
mempoolOps db t = liftIO $ do
  changes <- H.toList t
  rows <- forM changes $ \(h, new) -> do
    old <- retrieveCF db cf (MemTxKey h)
    return (h, old, new)
  return
    ( concatMap ops rows,
      MempoolChanges
        { added = [h | (h, Nothing, Just _) <- rows],
          removed = [h | (h, Just _, Nothing) <- rows]
        }
    )
  where
    cf = mempoolCF db
    ops (_, Nothing, Nothing) = []
    ops (h, Nothing, Just n) = mempoolInsertOps db h n
    ops (h, Just o, Nothing) =
      [deleteOpCF cf (MemTxKey h), deleteOpCF cf (MemTimeKey o h)]
    ops (h, Just o, Just n)
      | o == n = []
      | otherwise = deleteOpCF cf (MemTimeKey o h) : mempoolInsertOps db h n

-- | Operations for changed unspent outputs.
unspentOps :: (MonadIO m) => DB -> UnspentTable -> m [BatchOp]
unspentOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f p (Just u) =
      insertOpCF (unspentCF db) (UnspentKey p) (snd (unspentToVal u))
    f p Nothing =
      deleteOpCF (unspentCF db) (UnspentKey p)

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
getTxData w th = getLayered w (getTxDataH th) (`Q.getTxData` th)

-- | Spender of an output, with the changes of the batch applied.
getSpender :: Writer -> OutPoint -> IO (Maybe Spender)
getSpender w op = getLayered w (getSpenderH op) (`Q.getSpender` op)

-- | Balance of an address, with the changes of the batch applied.
getBalance :: Writer -> Address -> IO (Maybe Balance)
getBalance w a = getLayered w (getBalanceH a) (`Q.getBalance` a)

-- | Unspent output, with the changes of the batch applied.
getUnspent :: Writer -> OutPoint -> IO (Maybe Unspent)
getUnspent w op = getLayered w (getUnspentH op) (`Q.getUnspent` op)

-- | The mempool in the database with the changes of this batch applied,
-- newest first.
getMempool :: Writer -> IO [(UnixTime, TxHash)]
getMempool w = do
  stored <- Q.getMempool w.reader
  changes <- H.toList w.memory.mempoolTable
  let changed = HashMap.fromList changes
      kept = filter (not . (`HashMap.member` changed) . snd) stored
      added = [(t, h) | (h, Just t) <- changes]
  return $ sortOn Down (kept <> added)

-- | Empty set of changes.
newMemory ::
  (MonadIO m) =>
  Network ->
  Ctx ->
  m Memory
newMemory net ctx = do
  best <- newIORef Nothing
  blockTable <- liftIO H.new
  heightTable <- liftIO H.new
  txTable <- liftIO H.new
  unspentTable <- liftIO H.new
  balanceTable <- liftIO H.new
  addressTable <- liftIO H.new
  outputTable <- liftIO H.new
  mempoolTable <- liftIO H.new
  return Memory {..}

-- | Read the best block from the changes of the batch: 'Nothing' if unchanged, @Just Nothing@ if deleted.
getBestBlockH :: Memory -> IO (Maybe (Maybe BlockHash))
getBestBlockH = readIORef . (.best)

-- | Read the blocks at a height from the changes of the batch: 'Nothing' if unchanged, @Just Nothing@ if deleted.
getBlocksAtHeightH :: BlockHeight -> Memory -> IO (Maybe [BlockHash])
getBlocksAtHeightH h s = H.lookup s.heightTable h

-- | Read a block from the changes of the batch: 'Nothing' if unchanged, @Just Nothing@ if deleted.
getBlockH :: BlockHash -> Memory -> IO (Maybe (Maybe BlockData))
getBlockH h s = H.lookup s.blockTable h

-- | Read a transaction from the changes of the batch: 'Nothing' if unchanged, @Just Nothing@ if deleted.
getTxDataH :: TxHash -> Memory -> IO (Maybe (Maybe TxData))
getTxDataH t s = H.lookup s.txTable t

-- | Read the spender of an output from the transaction changed in the batch:
-- 'Nothing' if the transaction did not change, @Just Nothing@ if the output is
-- not spent.
getSpenderH :: OutPoint -> Memory -> IO (Maybe (Maybe Spender))
getSpenderH op s = do
  fmap (f =<<) <$> getTxDataH op.hash s
  where
    f = IntMap.lookup (fromIntegral op.index) . (.spenders)

-- | Read the balance of an address from the changes of the batch: 'Nothing' if unchanged, @Just Nothing@ if deleted.
getBalanceH :: Address -> Memory -> IO (Maybe (Maybe Balance))
getBalanceH a s = H.lookup s.balanceTable a

-- | Read an unspent output from the changes of the batch: 'Nothing' if unchanged, @Just Nothing@ if deleted.
getUnspentH :: OutPoint -> Memory -> IO (Maybe (Maybe Unspent))
getUnspentH p Memory {..} = H.lookup unspentTable p
