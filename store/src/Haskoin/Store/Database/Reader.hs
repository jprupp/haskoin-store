{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NoFieldSelectors #-}

module Haskoin.Store.Database.Reader
  ( -- * RocksDB Database Access
    DatabaseReader (..),
    DatabaseReaderT,
    DataMetrics,
    withDB,
    createDataMetrics,
    withDatabaseReader,
    addrTxCF,
    addrOutCF,
    txCF,
    unspentCF,
    blockCF,
    heightCF,
    balanceCF,
  )
where

import Conduit
import Control.Monad (forM_)
import Control.Monad.Reader
import Control.Monad.Trans.Maybe (MaybeT (..), runMaybeT)
import Data.Bits ((.&.))
import Data.ByteString qualified as BS
import Data.Default (def)
import Data.IntMap.Strict qualified as IntMap
import Data.List (sortOn)
import Data.Maybe (fromMaybe)
import Data.Ord (Down (..))
import Data.Serialize (encode)
import Data.Word (Word32)
import Database.RocksDB hiding (withDB)
import Database.RocksDB.Query
import Haskoin
import Haskoin.Store.Common
import Haskoin.Store.Data
import Haskoin.Store.Database.Types
import System.Metrics.StatsD
import UnliftIO

type DatabaseReaderT = ReaderT DatabaseReader

data DatabaseReader = DatabaseReader
  { db :: !DB,
    maxGap :: !Word32,
    initGap :: !Word32,
    net :: !Network,
    metrics :: !(Maybe DataMetrics),
    ctx :: !Ctx
  }

data DataMetrics = DataMetrics
  { dataBestCount :: !StatCounter,
    dataBlockCount :: !StatCounter,
    dataTxCount :: !StatCounter,
    dataMempoolCount :: !StatCounter,
    dataBalanceCount :: !StatCounter,
    dataUnspentCount :: !StatCounter,
    dataAddrTxCount :: !StatCounter,
    dataXPubBals :: !StatCounter,
    dataXPubUnspents :: !StatCounter,
    dataXPubTxs :: !StatCounter,
    dataXPubTxCount :: !StatCounter,
    dataIters :: !StatGauge
  }

createDataMetrics :: Stats -> IO DataMetrics
createDataMetrics s = do
  dataBestCount <- newStatCounter s "db.best_block" n
  dataBlockCount <- newStatCounter s "db.blocks" n
  dataTxCount <- newStatCounter s "db.txs" n
  dataMempoolCount <- newStatCounter s "db.mempool" n
  dataBalanceCount <- newStatCounter s "db.balances" n
  dataUnspentCount <- newStatCounter s "db.unspents" n
  dataAddrTxCount <- newStatCounter s "db.address_txs" n
  dataXPubBals <- newStatCounter s "db.xpub_balances" n
  dataXPubUnspents <- newStatCounter s "db.xpub_unspents" n
  dataXPubTxs <- newStatCounter s "db.xpub_txs" n
  dataXPubTxCount <- newStatCounter s "db.xpub_tx_count" n
  dataIters <- newStatGauge s "db.iterators" 0
  return DataMetrics {..}
  where
    n = 10

withMetrics :: (MonadReader DatabaseReader m) => (DataMetrics -> m a) -> m ()
withMetrics go = asks (.metrics) >>= mapM_ go

dataVersion :: Word32
dataVersion = 18

withDB :: DatabaseReader -> DatabaseReaderT m a -> m a
withDB = flip runReaderT

withDatabaseReader ::
  (MonadUnliftIO m) =>
  Network ->
  Ctx ->
  Word32 ->
  Word32 ->
  FilePath ->
  Bool ->
  Maybe DataMetrics ->
  DatabaseReaderT m a ->
  m a
withDatabaseReader net ctx igap gap dir bloom stats m =
  withRunInIO $ \run ->
    withDBCF dir cfg (columnFamilyConfig bloom) $ \db -> do
      let bdb =
            DatabaseReader
              { db = db,
                maxGap = gap,
                net = net,
                initGap = igap,
                metrics = stats,
                ctx = ctx
              }
      initRocksDB bdb
      run $ runReaderT m bdb
  where
    cfg = def {createIfMissing = True}

columnFamilyConfig :: Bool -> [(String, Config)]
columnFamilyConfig bloom =
  [ ("addr-tx", def {prefixLength = Just 22, bloomFilter = bloom}),
    ("addr-out", def {prefixLength = Just 22, bloomFilter = bloom}),
    ("tx", def {prefixLength = Just 33, bloomFilter = bloom}),
    ("spender", def {prefixLength = Just 33, bloomFilter = bloom}), -- unused
    ("unspent", def {prefixLength = Just 37, bloomFilter = bloom}),
    ("block", def {prefixLength = Just 33, bloomFilter = bloom}),
    ("height", def {prefixLength = Nothing, bloomFilter = bloom}),
    ("balance", def {prefixLength = Just 22, bloomFilter = bloom})
  ]

addrTxCF :: DB -> ColumnFamily
addrTxCF db = columnFamilies db !! 0

addrOutCF :: DB -> ColumnFamily
addrOutCF db = columnFamilies db !! 1

txCF :: DB -> ColumnFamily
txCF db = columnFamilies db !! 2

unspentCF :: DB -> ColumnFamily
unspentCF db = columnFamilies db !! 4

blockCF :: DB -> ColumnFamily
blockCF db = columnFamilies db !! 5

heightCF :: DB -> ColumnFamily
heightCF db = columnFamilies db !! 6

balanceCF :: DB -> ColumnFamily
balanceCF db = columnFamilies db !! 7

initRocksDB :: DatabaseReader -> IO ()
initRocksDB DatabaseReader {db = db} =
  retrieve db VersionKey >>= \case
    Just v
      | v == dataVersion -> return ()
      | otherwise -> error "Incorrect RocksDB database version"
    Nothing -> setInitRocksDB db

setInitRocksDB :: DB -> IO ()
setInitRocksDB db = insert db VersionKey dataVersion

addressConduit ::
  (MonadIO m) =>
  Address ->
  Maybe Start ->
  Iterator ->
  ConduitT i TxRef (DatabaseReaderT m) ()
addressConduit a s it = do
  pipe .| mapC (uncurry f)
  where
    f (AddrTxKey _ t) () = t
    f _ _ = undefined
    pipe = case s of
      Nothing ->
        readerC $ \_ ->
          transPipe liftIO $
            matching it (AddrTxKeyA a)
      Just (AtBlock bh) ->
        readerC $ \_ ->
          transPipe liftIO $
            matchingSkip
              it
              (AddrTxKeyA a)
              (AddrTxKeyB a (BlockRef bh maxBound))
      Just (AtTx txh) ->
        lift (getTxData txh) >>= \case
          Just TxData {block = b@BlockRef {}} ->
            readerC $ \_ ->
              transPipe liftIO $
                matchingSkip
                  it
                  (AddrTxKeyA a)
                  (AddrTxKeyB a b)
          Just TxData {block = MemRef {}} ->
            let cond (AddrTxKey _ (TxRef MemRef {} th)) = th /= txh
                cond (AddrTxKey _ (TxRef BlockRef {} _)) = False
                cond _ = undefined
             in readerC $ \_ ->
                  transPipe liftIO $
                    matching it (AddrTxKeyA a)
                      .| (dropWhileC (cond . fst) >> mapC id)
          Nothing -> return ()

unspentConduit ::
  (MonadIO m) =>
  Ctx ->
  Address ->
  Maybe Start ->
  Iterator ->
  ConduitT i Unspent (DatabaseReaderT m) ()
unspentConduit ctx a s it =
  x .| mapC (uncurry (toUnspent ctx)) .| concatC
  where
    x = case s of
      Nothing ->
        readerC $ \_ -> transPipe liftIO $ matching it (AddrOutKeyA a)
      Just (AtBlock h) ->
        readerC $ \_ ->
          transPipe liftIO $
            matchingSkip
              it
              (AddrOutKeyA a)
              (AddrOutKeyB a (BlockRef h maxBound))
      Just (AtTx txh) ->
        lift (getTxData txh) >>= \case
          Just TxData {block = b@BlockRef {}} ->
            readerC $ \_ ->
              transPipe liftIO $
                matchingSkip it (AddrOutKeyA a) (AddrOutKeyB a b)
          Just TxData {block = MemRef {}} ->
            let cond (AddrOutKey _ MemRef {} p) = p.hash /= txh
                cond (AddrOutKey _ BlockRef {} _) = False
                cond _ = undefined
             in readerC $ \_ ->
                  transPipe liftIO $
                    matching it (AddrOutKeyA a)
                      .| (dropWhileC (cond . fst) >> mapC id)
          Nothing -> return ()

withIterCFStat ::
  (MonadUnliftIO m) =>
  DB ->
  ColumnFamily ->
  (Iterator -> DatabaseReaderT m a) ->
  DatabaseReaderT m a
withIterCFStat db cf f = do
  m <- asks (.metrics)
  bracket_ (open m) (close m) go
  where
    open (Just m) = incrementGauge m.dataIters 1
    open Nothing = return ()
    close (Just m) = decrementGauge m.dataIters 1
    close Nothing = return ()
    go = withRunInIO $ \run ->
      withIterCF db cf $ \it -> run (f it)

withManyIters ::
  (MonadUnliftIO m) =>
  DB ->
  ColumnFamily ->
  Int ->
  ([Iterator] -> DatabaseReaderT m a) ->
  DatabaseReaderT m a
withManyIters db cf i f = go [] i
  where
    go acc 0 = f acc
    go acc n = withIterCFStat db cf $ \it -> go (it : acc) (n - 1)

joinConduits ::
  (Monad m, Ord o) =>
  [ConduitT () o m ()] ->
  Limits ->
  m [o]
joinConduits cs l =
  runConduit $ joinDescStreams cs .| applyLimitsC l .| sinkList

instance (MonadIO m) => StoreReadBase (DatabaseReaderT m) where
  getCtx = asks (.ctx)
  getNetwork = asks (.net)

  getTxData th = do
    db <- asks (.db)
    liftIO (retrieveCF db (txCF db) (TxKey th)) >>= \case
      Nothing -> return Nothing
      Just t -> do
        withMetrics $ \s -> incrementCounter s.dataTxCount 1
        return (Just t)

  getSpender op = runMaybeT $ do
    td <- MaybeT $ getTxData op.hash
    let i = fromIntegral op.index
    MaybeT . return $ i `IntMap.lookup` td.spenders

  getUnspent p = do
    db <- asks (.db)
    ctx <- asks (.ctx)
    val <- liftIO (retrieveCF db (unspentCF db) (UnspentKey p))
    case fmap (valToUnspent ctx p) val of
      Nothing -> return Nothing
      Just u -> do
        withMetrics $ \s -> incrementCounter s.dataUnspentCount 1
        return (Just u)

  getBalance a = do
    db <- asks (.db)
    withMetrics $ \s -> incrementCounter s.dataBalanceCount 1
    fmap (valToBalance a) <$> liftIO (retrieveCF db (balanceCF db) (BalKey a))

  getMempool = do
    db <- asks (.db)
    withMetrics $ \s -> incrementCounter s.dataMempoolCount 1
    fromMaybe [] <$> liftIO (retrieve db MemKey)

  getBestBlock = do
    withMetrics $ \s -> incrementCounter s.dataBestCount 1
    asks (.db) >>= liftIO . (`retrieve` BestKey)

  getBlocksAtHeight h = do
    db <- asks (.db)
    liftIO (retrieveCF db (heightCF db) (HeightKey h)) >>= \case
      Nothing -> return []
      Just ls -> do
        withMetrics $ \s -> incrementCounter s.dataBlockCount (length ls)
        return ls

  getBlock h = do
    db <- asks (.db)
    liftIO (retrieveCF db (blockCF db) (BlockKey h)) >>= \case
      Nothing -> return Nothing
      Just b -> do
        withMetrics $ \s -> incrementCounter s.dataBlockCount 1
        return (Just b)

instance (MonadUnliftIO m) => StoreReadExtra (DatabaseReaderT m) where
  getAddressesTxs addrs limits = do
    db <- asks (.db)
    withManyIters db (addrTxCF db) (length addrs) $ \its -> do
      txs <- joinConduits (cs its) limits
      withMetrics $ \s -> incrementCounter s.dataAddrTxCount (length txs)
      return txs
    where
      cs = zipWith c addrs
      c a = addressConduit a limits.start

  getAddressesUnspents addrs limits = do
    db <- asks (.db)
    ctx <- asks (.ctx)
    withManyIters db (addrOutCF db) (length addrs) $ \its -> do
      uns <- joinConduits (cs ctx its) limits
      withMetrics $ \s -> incrementCounter s.dataUnspentCount (length uns)
      return uns
    where
      cs ctx = zipWith (c ctx) addrs
      c ctx a = unspentConduit ctx a limits.start

  getAddressUnspents a limits = do
    db <- asks (.db)
    ctx <- asks (.ctx)
    us <- withIterCFStat db (addrOutCF db) $ \it ->
      runConduit $
        unspentConduit ctx a limits.start it
          .| applyLimitsC limits
          .| sinkList
    withMetrics $ \s -> incrementCounter s.dataUnspentCount (length us)
    return us

  getAddressTxs a limits = do
    db <- asks (.db)
    txs <- withIterCFStat db (addrTxCF db) $ \it ->
      runConduit $
        addressConduit a limits.start it
          .| applyLimitsC limits
          .| sinkList
    withMetrics $ \s -> incrementCounter s.dataAddrTxCount (length txs)
    return txs

  getMaxGap = asks (.maxGap)

  getInitialGap = asks (.initGap)

  getNumTxData i = ReaderT $ \r -> do
    let (sk, w) = decodeTxKey i
    ls <- liftIO $ matchingAsListCF r.db (txCF r.db) (TxKeyS sk)
    let f t =
          let bs = encode $ txHash t.tx
              b = BS.head (BS.drop 6 bs)
              w' = b .&. 0xf8
           in w == w'
        txs = filter f $ map snd ls
    forM_ r.metrics $ \s -> incrementCounter s.dataTxCount (length txs)
    return txs

  getBalances as = do
    zipWith f as <$> mapM getBalance as
    where
      f a Nothing = zeroBalance a
      f _ (Just b) = b

  xPubBals xpub = do
    ctx <- asks (.ctx)
    igap <- asks (.initGap)
    gap <- asks (.maxGap)
    ext1 <- derive_until_gap gap 0 (take (fromIntegral igap) (aderiv ctx 0 0))
    if all nullxb ext1
      then do
        withMetrics $ \s -> incrementCounter s.dataXPubBals (length ext1)
        return ext1
      else do
        ext2 <- derive_until_gap gap 0 (aderiv ctx 0 igap)
        chg <- derive_until_gap gap 1 (aderiv ctx 1 0)
        let bals = ext1 <> ext2 <> chg
        withMetrics $ \s -> incrementCounter s.dataXPubBals (length bals)
        return bals
    where
      nullxb b = nullBalance b.balance
      aderiv ctx m =
        deriveAddresses
          (deriveFunction ctx xpub.deriv)
          (pubSubKey ctx xpub.key m)
      derive_until_gap gap m adrs =
        let xb b n = XPubBal {path = [m, n], balance = b}
            ig = fromIntegral gap
            test acc =
              let ns = takeWhile nullxb acc
               in length ns > ig
            go acc [] = return $ reverse acc
            go acc as
              | test acc =
                  let (ns, ys) = span nullxb acc
                      xs = take ig $ reverse ns
                   in return $ reverse ys <> xs
              | otherwise = do
                  let (as', as'') = splitAt ig as
                  bs <- getBalances (map snd as')
                  let xs = reverse $ zipWith xb bs (map fst as')
                  go (xs <> acc) as''
         in go [] adrs

  xPubUnspents _xspec xbals limits = do
    us <- concat <$> mapM h cs
    withMetrics $ \s -> incrementCounter s.dataXPubUnspents (length us)
    return . applyLimits limits $ sortOn Down us
    where
      l = deOffset limits
      cs = filter ((> 0) . (.balance.utxo)) xbals
      i b = getAddressUnspents b.balance.address l
      f b t = XPubUnspent {path = b.path, unspent = t}
      h b = map (f b) <$> i b

  xPubTxs _xspec xbals limits = do
    let as =
          map (.address) $
            filter (not . nullBalance) $
              map (.balance) xbals
    txs <- getAddressesTxs as limits
    withMetrics $ \s -> incrementCounter s.dataXPubTxs (length txs)
    return txs

  xPubTxCount xspec xbals = do
    withMetrics $ \s -> incrementCounter s.dataXPubTxCount 1
    fromIntegral . length <$> xPubTxs xspec xbals def
