{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NoFieldSelectors #-}

module Haskoin.Store.Database.Writer (WriterT, runWriter) where

import Control.Monad.Reader (ReaderT (..))
import Control.Monad.Reader qualified as R
import Data.HashTable.IO qualified as H
import Data.IntMap.Strict qualified as IntMap
import Data.List (sortOn)
import Data.Ord (Down (..))
import Data.Tuple (swap)
import Database.RocksDB (BatchOp, DB)
import Database.RocksDB.Query
  ( deleteOp,
    deleteOpCF,
    insertOp,
    insertOpCF,
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
import Haskoin.Store.Common
import Haskoin.Store.Data
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

data Writer = Writer
  { reader :: !DatabaseReader,
    memory :: !Memory
  }

type WriterT = ReaderT Writer

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

type BestRef = IORef (Maybe (Maybe BlockHash))

type BlockTable = H.BasicHashTable BlockHash (Maybe BlockData)

type HeightTable = H.BasicHashTable BlockHeight [BlockHash]

type TxTable = H.BasicHashTable TxHash (Maybe TxData)

type UnspentTable = H.BasicHashTable OutPoint (Maybe Unspent)

type BalanceTable = H.BasicHashTable Address (Maybe Balance)

type AddrTxTable = H.BasicHashTable (Address, TxRef) (Maybe ())

type AddrOutTable = H.BasicHashTable (Address, BlockRef, OutPoint) (Maybe OutVal)

type MempoolTable = H.BasicHashTable TxHash UnixTime

data Memory = Memory
  { net :: !Network,
    ctx :: !Ctx,
    best :: !BestRef,
    blockTable :: !BlockTable,
    heightTable :: !HeightTable,
    txTable :: !TxTable,
    unspentTable :: !UnspentTable,
    balanceTable :: !BalanceTable,
    addressTable :: !AddrTxTable,
    outputTable :: !AddrOutTable,
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
      liftIO $ H.insert mempoolTable x t
  deleteFromMempool x =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.delete mempoolTable x
  setBalance b =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert balanceTable b.address (Just b)
  insertUnspent u =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert unspentTable (fst (unspentToVal u)) (Just u)
  deleteUnspent p =
    ReaderT $ \Writer {memory = Memory {..}} ->
      liftIO $ H.insert unspentTable p Nothing

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

runWriter ::
  (MonadIO m) =>
  Network ->
  Ctx ->
  DatabaseReader ->
  WriterT m a ->
  m a
runWriter net ctx bdb@DatabaseReader {db} f = do
  mempool <- runReaderT getMempool bdb
  hm <- newMemory net ctx mempool
  x <- R.runReaderT f Writer {reader = bdb, memory = hm}
  ops <- hashMapOps db hm
  liftIO $ writeBatch db ops
  return x

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
        mempoolOp mempoolTable,
        unspentOps db unspentTable
      ]

bestBlockOp :: (MonadIO m) => BestRef -> m [BatchOp]
bestBlockOp r =
  readIORef r >>= \case
    Nothing -> return []
    Just Nothing -> return [deleteOp BestKey]
    Just (Just b) -> return [insertOp BestKey b]

blockHashOps :: (MonadIO m) => DB -> BlockTable -> m [BatchOp]
blockHashOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f k (Just d) = insertOpCF (blockCF db) (BlockKey k) d
    f k Nothing = deleteOpCF (blockCF db) (BlockKey k)

blockHeightOps :: (MonadIO m) => DB -> HeightTable -> m [BatchOp]
blockHeightOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f = insertOpCF (heightCF db) . HeightKey

txOps :: (MonadIO m) => DB -> TxTable -> m [BatchOp]
txOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f k (Just t') = insertOpCF (txCF db) (TxKey k) t'
    f k Nothing = deleteOpCF (txCF db) (TxKey k)

balOps :: (MonadIO m) => DB -> BalanceTable -> m [BatchOp]
balOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f a (Just b) = insertOpCF (balanceCF db) (BalKey a) (balanceToVal b)
    f a Nothing = deleteOpCF (balanceCF db) (BalKey a)

addrTxOps :: (MonadIO m) => DB -> AddrTxTable -> m [BatchOp]
addrTxOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f (a, t') (Just ()) = insertOpCF (addrTxCF db) (AddrTxKey a t') ()
    f (a, t') Nothing = deleteOpCF (addrTxCF db) (AddrTxKey a t')

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

mempoolOp :: (MonadIO m) => MempoolTable -> m [BatchOp]
mempoolOp t =
  return . insertOp MemKey . sortOn Down . map swap <$> liftIO (H.toList t)

unspentOps :: (MonadIO m) => DB -> UnspentTable -> m [BatchOp]
unspentOps db t = map (uncurry f) <$> liftIO (H.toList t)
  where
    f p (Just u) =
      insertOpCF (unspentCF db) (UnspentKey p) (snd (unspentToVal u))
    f p Nothing =
      deleteOpCF (unspentCF db) (UnspentKey p)

getBestBlockI :: (MonadIO m) => WriterT m (Maybe BlockHash)
getBestBlockI = getLayered getBestBlockH getBestBlock

getBlocksAtHeightI :: (MonadIO m) => BlockHeight -> WriterT m [BlockHash]
getBlocksAtHeightI bh =
  getLayered (getBlocksAtHeightH bh) (getBlocksAtHeight bh)

getBlockI :: (MonadIO m) => BlockHash -> WriterT m (Maybe BlockData)
getBlockI bh = getLayered (getBlockH bh) (getBlock bh)

getTxDataI :: (MonadIO m) => TxHash -> WriterT m (Maybe TxData)
getTxDataI th = getLayered (getTxDataH th) (getTxData th)

getSpenderI :: (MonadIO m) => OutPoint -> WriterT m (Maybe Spender)
getSpenderI op = getLayered (getSpenderH op) (getSpender op)

getBalanceI :: (MonadIO m) => Address -> WriterT m (Maybe Balance)
getBalanceI a = getLayered (getBalanceH a) (getBalance a)

getUnspentI :: (MonadIO m) => OutPoint -> WriterT m (Maybe Unspent)
getUnspentI op = getLayered (getUnspentH op) (getUnspent op)

getMempoolI :: (MonadIO m) => WriterT m [(UnixTime, TxHash)]
getMempoolI =
  ReaderT $ \Writer {memory = Memory {..}} ->
    liftIO $ map swap <$> H.toList mempoolTable

newMemory ::
  (MonadIO m) =>
  Network ->
  Ctx ->
  [(UnixTime, TxHash)] ->
  m Memory
newMemory net ctx mempool = do
  best <- newIORef Nothing
  blockTable <- liftIO H.new
  heightTable <- liftIO H.new
  txTable <- liftIO H.new
  unspentTable <- liftIO H.new
  balanceTable <- liftIO H.new
  addressTable <- liftIO H.new
  outputTable <- liftIO H.new
  mempoolTable <- liftIO $ H.fromList (map swap mempool)
  return Memory {..}

getBestBlockH :: Memory -> IO (Maybe (Maybe BlockHash))
getBestBlockH = readIORef . (.best)

getBlocksAtHeightH :: BlockHeight -> Memory -> IO (Maybe [BlockHash])
getBlocksAtHeightH h s = H.lookup s.heightTable h

getBlockH :: BlockHash -> Memory -> IO (Maybe (Maybe BlockData))
getBlockH h s = H.lookup s.blockTable h

getTxDataH :: TxHash -> Memory -> IO (Maybe (Maybe TxData))
getTxDataH t s = H.lookup s.txTable t

getSpenderH :: OutPoint -> Memory -> IO (Maybe (Maybe Spender))
getSpenderH op s = do
  fmap (f =<<) <$> getTxDataH op.hash s
  where
    f = IntMap.lookup (fromIntegral op.index) . (.spenders)

getBalanceH :: Address -> Memory -> IO (Maybe (Maybe Balance))
getBalanceH a s = H.lookup s.balanceTable a

getUnspentH :: OutPoint -> Memory -> IO (Maybe (Maybe Unspent))
getUnspentH p Memory {..} = H.lookup unspentTable p
