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
-- 'WriterT' implements 'StoreWrite' by recording every change in in-memory hash
-- tables, and 'StoreReadBase' by reading those tables first and falling back to
-- the database. When the action given to 'runWriter' finishes, all the changes
-- are written to RocksDB in a single atomic batch. A block or a set of
-- transactions is thus either fully imported or not at all.
-- = Why buffer writes
--
-- Importing a block touches thousands of keys, and many of them several times:
-- an output is created and spent in the same block, a balance changes with every
-- transaction of its address. Buffering collapses those into one final value per
-- key, and a single atomic batch makes the whole import all-or-nothing. A crash
-- or an invalid block leaves the database exactly as it was.
module Haskoin.Store.Database.Writer
  ( WriterT,
    MempoolChanges (..),
    runWriter,
    runWriterMempool,
    writerLogger,
  )
where

import Control.Monad (forM)
import Control.Monad.Reader (ReaderT (..))
import Control.Monad.Reader qualified as R
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
import Haskoin.Store.Common
import Haskoin.Store.Data
-- the instances of the store classes for the database reader
import Haskoin.Store.Database.Query ()
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

-- | Environment of a batch of writes.
data Writer = Writer
  { -- | database to read from and write to
    reader :: !DatabaseReader,
    -- | changes made so far
    memory :: !Memory,
    -- | log messages here
    logger :: !Logger
  }

-- | Monad transformer for a batch of writes. Run it with 'runWriter'.
type WriterT = ReaderT Writer

-- | Logger passed to 'runWriter'.
writerLogger :: (Monad m) => WriterT m Logger
writerLogger = R.asks (.logger)

instance (MonadIO m) => StoreReadBase (WriterT m) where
  getNetwork = R.asks (.reader.net)
  getCtx = R.asks (.reader.ctx)
  getBestBlock = getBestBlockI
  getBlocksAtHeight = getBlocksAtHeightI
  getBlock = getBlockI
  getTxData = getTxDataI
  getSpender = getSpenderI
  getUnspent = getUnspentI
  getBalance = getBalanceI
  getMempool = getMempoolI

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

instance (MonadIO m) => StoreWrite (WriterT m) where
  setBest h =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ writeIORef best (Just (Just h))
  insertBlock b =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert blockTable (headerHash b.header) (Just b)
  setBlocksAtHeight h g =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert heightTable g h
  insertTx t =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert txTable (txHash t.tx) (Just t)
  insertAddrTx a t =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert addressTable (a, t) (Just ())
  deleteAddrTx a t =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert addressTable (a, t) Nothing
  insertAddrUnspent a u =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert outputTable k (Just v)
    where
      k = (a, u.block, u.outpoint)
      v = OutVal u.value u.script
  deleteAddrUnspent a u =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert outputTable k Nothing
    where
      k = (a, u.block, u.outpoint)
  addToMempool x t =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert mempoolTable x (Just t)
  deleteFromMempool x =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert mempoolTable x Nothing
  setBalance b =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert balanceTable b.address (Just b)
  insertUnspent u =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert unspentTable (fst (unspentToVal u)) (Just u)
  deleteUnspent p =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert unspentTable p Nothing

-- | Read a value from the changes of the batch, or from the database if the batch did not change it.
getLayered ::
  (MonadIO m) =>
  (Memory -> IO (Maybe a)) ->
  DatabaseReaderT m a ->
  WriterT m a
getLayered f g =
  ReaderT $ \Writer {reader = db, memory = s} ->
    liftIO (f s) >>= \case
      Just x -> return x
      Nothing -> runReaderT g db

-- | Run a batch of writes and commit all its changes to the database in one
-- atomic write. Reads within the batch see its own changes.
runWriter ::
  (MonadIO m) =>
  Network ->
  Ctx ->
  DatabaseReader ->
  Logger ->
  WriterT m a ->
  m a
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
  (MonadIO m) =>
  Network ->
  Ctx ->
  DatabaseReader ->
  Logger ->
  WriterT m a ->
  m (a, MempoolChanges)
runWriterMempool net ctx bdb@DatabaseReader {db} logger f = do
  hm <- newMemory net ctx
  x <- R.runReaderT f Writer {reader = bdb, memory = hm, logger}
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

-- | Read the best block, with the changes of the batch applied.
getBestBlockI :: (MonadIO m) => WriterT m (Maybe BlockHash)
getBestBlockI = getLayered getBestBlockH getBestBlock

-- | Read the blocks at a height, with the changes of the batch applied.
getBlocksAtHeightI :: (MonadIO m) => BlockHeight -> WriterT m [BlockHash]
getBlocksAtHeightI bh =
  getLayered (getBlocksAtHeightH bh) (getBlocksAtHeight bh)

-- | Read a block, with the changes of the batch applied.
getBlockI :: (MonadIO m) => BlockHash -> WriterT m (Maybe BlockData)
getBlockI bh = getLayered (getBlockH bh) (getBlock bh)

-- | Read a transaction, with the changes of the batch applied.
getTxDataI :: (MonadIO m) => TxHash -> WriterT m (Maybe TxData)
getTxDataI th = getLayered (getTxDataH th) (getTxData th)

-- | Read the spender of an output, with the changes of the batch applied.
getSpenderI :: (MonadIO m) => OutPoint -> WriterT m (Maybe Spender)
getSpenderI op = getLayered (getSpenderH op) (getSpender op)

-- | Read the balance of an address, with the changes of the batch applied.
getBalanceI :: (MonadIO m) => Address -> WriterT m (Maybe Balance)
getBalanceI a = getLayered (getBalanceH a) (getBalance a)

-- | Read an unspent output, with the changes of the batch applied.
getUnspentI :: (MonadIO m) => OutPoint -> WriterT m (Maybe Unspent)
getUnspentI op = getLayered (getUnspentH op) (getUnspent op)

-- | The mempool in the database with the changes of this batch applied,
-- newest first.
getMempoolI :: (MonadIO m) => WriterT m [(UnixTime, TxHash)]
getMempoolI =
  ReaderT $ \Writer {reader = db, memory = Memory {..}} -> do
    stored <- runReaderT getMempool db
    changes <- liftIO $ H.toList mempoolTable
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
