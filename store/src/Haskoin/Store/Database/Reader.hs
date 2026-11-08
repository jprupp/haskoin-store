{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NoFieldSelectors #-}

-- |
-- Module      : Haskoin.Store.Database.Reader
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Queries against the RocksDB database of the store.
--
-- 'DatabaseReaderT' implements 'StoreReadBase' and 'StoreReadExtra' by reading
-- the indices directly from RocksDB. The database is opened with
-- 'withDatabaseReader', which also creates or migrates it.
--
-- Data is split across column families by kind; see 'addrTxCF' and the other
-- column family accessors. The keys stored in each are described in
-- "Haskoin.Store.Database.Types".
-- = Why column families
--
-- Each kind of record lives in its own column family. Their keys and access
-- patterns differ: balances are small point lookups, address transactions are
-- scans, transactions are large values. Separate column families let each have
-- its own key prefix length for filters and seeks, and keep scans over one kind
-- of data from wading through another.
--
-- = Why addresses of xpubs are cached
--
-- Deriving an address from an extended public key costs an elliptic curve
-- operation and two hashes, and an xpub query may need hundreds of them before it
-- reads a single balance. Addresses depend only on the xpub, so the derivations
-- of recently queried xpubs are kept and shared. The cache never goes stale.
module Haskoin.Store.Database.Reader
  ( -- * RocksDB Database Access
    DatabaseReader (..),
    DatabaseReaderT,
    DataMetrics,
    withDB,
    createDataMetrics,
    withDatabaseReader,
    memoryReport,
    addrTxCF,
    addrOutCF,
    txCF,
    unspentCF,
    blockCF,
    heightCF,
    balanceCF,
    mempoolCF,
    getMempoolBefore,
    mempoolInsertOps,
  )
where

import Conduit
import Control.Monad (forM, forM_, replicateM, (>=>))
import Control.Monad.Reader
import Control.Monad.Trans.Maybe (MaybeT (..), runMaybeT)
import Data.Bits ((.&.))
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.Default (def)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.IntMap.Strict qualified as IntMap
import Data.Functor ((<&>))
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe, isJust, mapMaybe)
import Data.Ord (Down (..))
import Data.Serialize (Serialize, decode, encode)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Word (Word32)
import Database.RocksDB hiding (withDB)
import Database.RocksDB.Query
import Haskoin
import Haskoin.Store.Common
import Haskoin.Store.Data
import Haskoin.Store.Database.Types
import System.Metrics.StatsD
import Text.Read (readMaybe)
import UnliftIO

-- | Monad transformer to query the database.
type DatabaseReaderT = ReaderT DatabaseReader

-- | Open database and its settings.
data DatabaseReader = DatabaseReader
  { -- | RocksDB handle, with the column families of the store
    db :: !DB,
    -- | consecutive unused addresses after which xpub derivation stops
    maxGap :: !Word32,
    -- | unused addresses to look at for an xpub with no transactions
    initGap :: !Word32,
    -- | network constants
    net :: !Network,
    -- | StatsD metrics, if enabled
    metrics :: !(Maybe DataMetrics),
    -- | secp256k1 context
    ctx :: !Ctx,
    -- | addresses derived from recently queried xpubs
    derived :: !(IORef DerivedCache)
  }

-- | Addresses of an xpub, external then change, in order of index. Each
-- list is infinite and lazy: it is derived as far as queries have needed,
-- and shared by later queries of the same xpub. Deriving an address costs
-- an elliptic curve operation and two hashes, which made up most of the
-- time of an xpub query.
data Derived = Derived ![(Word32, Address)] ![(Word32, Address)]

-- | Recently used xpubs and their addresses. When the newer generation is
-- full it becomes the older one, so the cache holds at most twice
-- 'derivedCacheSize' xpubs, and an xpub stays while it is in use.
data DerivedCache = DerivedCache
  { newer :: !(HashMap XPubSpec Derived),
    older :: !(HashMap XPubSpec Derived)
  }

-- | Number of xpubs in each generation of the cache of derived addresses.
derivedCacheSize :: Int
derivedCacheSize = 512

-- | Addresses of an xpub, from the cache or a new lazy derivation.
derivedAddrs :: (MonadIO m) => XPubSpec -> DatabaseReaderT m Derived
derivedAddrs xpub = do
  ref <- asks (.derived)
  ctx <- asks (.ctx)
  let fresh =
        let addrs m =
              deriveAddresses
                (deriveFunction ctx xpub.deriv)
                (pubSubKey ctx xpub.key m)
                0
         in Derived (addrs 0) (addrs 1)
  liftIO . atomicModifyIORef' ref $ \c ->
    case HashMap.lookup xpub c.newer of
      Just d -> (c, d)
      Nothing ->
        let d = fromMaybe fresh (HashMap.lookup xpub c.older)
            c'
              | HashMap.size c.newer >= derivedCacheSize =
                  DerivedCache {newer = HashMap.singleton xpub d, older = c.newer}
              | otherwise = c {newer = HashMap.insert xpub d c.newer}
         in (c', d)

-- | StatsD metrics of database reads. Counters count the items read.
data DataMetrics = DataMetrics
  { -- | best block reads (@db.best_block@)
    dataBestCount :: !StatCounter,
    -- | blocks read (@db.blocks@)
    dataBlockCount :: !StatCounter,
    -- | transactions read (@db.txs@)
    dataTxCount :: !StatCounter,
    -- | mempool entries read (@db.mempool@)
    dataMempoolCount :: !StatCounter,
    -- | balances read (@db.balances@)
    dataBalanceCount :: !StatCounter,
    -- | unspent outputs read (@db.unspents@)
    dataUnspentCount :: !StatCounter,
    -- | address transactions read (@db.address_txs@)
    dataAddrTxCount :: !StatCounter,
    -- | xpub balances read (@db.xpub_balances@)
    dataXPubBals :: !StatCounter,
    -- | xpub unspent outputs read (@db.xpub_unspents@)
    dataXPubUnspents :: !StatCounter,
    -- | xpub transactions read (@db.xpub_txs@)
    dataXPubTxs :: !StatCounter,
    -- | xpub transaction counts (@db.xpub_tx_count@)
    dataXPubTxCount :: !StatCounter,
    -- | open iterators (@db.iterators@)
    dataIters :: !StatGauge
  }

-- | Register the database metrics with a StatsD client. Samples are sent at a rate of one in ten.
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

-- | Run an action with the metrics, if they are enabled.
withMetrics :: (MonadReader DatabaseReader m) => (DataMetrics -> m a) -> m ()
withMetrics go = asks (.metrics) >>= mapM_ go

-- | Version of the database layout. Databases of versions 18 and 19 are
-- migrated when opened; other versions are refused.
--
-- Bumping the version is how a change to the database layout is made safe: an
-- old database is either migrated in place when it is opened or refused, never
-- read with the wrong layout.
dataVersion :: Word32
dataVersion = 20

-- | Run a query against a database.
withDB :: DatabaseReader -> DatabaseReaderT m a -> m a
withDB = flip runReaderT

-- | Open the database and run a query against it. Creates the database and its
-- column families if missing, and migrates it from older versions.
--
-- Arguments, in order: network, secp256k1 context, initial xpub gap, maximum
-- xpub gap, directory, whether to use Bloom filters, maximum open files, total
-- memtable size in bytes, write-ahead log size in bytes after which column
-- families are flushed, block cache size in bytes, and metrics. 'Nothing' leaves
-- a setting to RocksDB.
withDatabaseReader ::
  (MonadUnliftIO m) =>
  Network ->
  Ctx ->
  Word32 ->
  Word32 ->
  FilePath ->
  Bool ->
  Maybe Int ->
  Maybe Int ->
  Maybe Int ->
  Maybe Int ->
  Maybe DataMetrics ->
  DatabaseReaderT m a ->
  m a
withDatabaseReader net ctx igap gap dir bloom maxOpenFiles writeBuffers maxWal blockCache stats m =
  withRunInIO $ \run ->
    withDBCF dir cfg (columnFamilyConfig bloom (isJust blockCache)) $ \db -> do
      derived <- newIORef (DerivedCache HashMap.empty HashMap.empty)
      let bdb =
            DatabaseReader
              { db = db,
                maxGap = gap,
                net = net,
                initGap = igap,
                metrics = stats,
                ctx = ctx,
                derived = derived
              }
      initRocksDB bdb
      run $ runReaderT m bdb
  where
    cfg =
      def
        { createIfMissing = True,
          createMissingColumnFamilies = True,
          maxFiles = maxOpenFiles,
          dbWriteBufferSize = writeBuffers,
          maxTotalWalSize = maxWal,
          blockCacheSize = blockCache,
          cacheIndexAndFilterBlocks = isJust blockCache
        }

-- | Memory that RocksDB uses outside the GHC heap, per column family:
-- memtables and table readers (index and filter blocks of open files),
-- then the block cache shared by all of them. Values are in bytes.
memoryReport :: DB -> IO Text
memoryReport db = do
  rows <- forM cfs $ \(name, cf) -> do
    mem <- prop cf CurSizeAllMemTables
    readers <- prop cf EstimateTableReadersMem
    return (name, mem, readers)
  cache <- prop Nothing BlockCacheUsage
  let total f = sum (mapMaybe f rows)
      line (name, mem, readers) =
        T.justifyLeft 10 ' ' name
          <> T.justifyRight 16 ' ' (num mem)
          <> T.justifyRight 16 ' ' (num readers)
  return . T.unlines $
    ["** RocksDB memory (bytes) **", T.justifyLeft 10 ' ' "family" <> T.justifyRight 16 ' ' "memtables" <> T.justifyRight 16 ' ' "table readers"]
      <> map line rows
      <> [ line ("total", Just (total (\(_, m, _) -> m)), Just (total (\(_, _, r) -> r))),
           "block cache: " <> num cache,
           ""
         ]
  where
    cfs =
      ("default", Nothing)
        : zip (map (T.pack . fst) (columnFamilyConfig False False)) (map Just (columnFamilies db))
    prop cf p = do
      v <- maybe (getProperty db p) (\c -> getPropertyCF db c p) cf
      return (v >>= readMaybe . BS8.unpack)
    num :: Maybe Integer -> Text
    num = maybe "?" (T.pack . show)

-- | Column families, with Bloom filters or not, and with their index and
-- filter blocks in the block cache or not.
columnFamilyConfig :: Bool -> Bool -> [(String, Config)]
columnFamilyConfig bloom cached =
  [ ("addr-tx", c (Just 22)),
    ("addr-out", c (Just 22)),
    ("tx", c (Just 33)),
    ("spender", c (Just 33)), -- unused
    ("unspent", c (Just 37)),
    ("block", c (Just 33)),
    ("height", c Nothing),
    ("balance", c (Just 22)),
    ("mempool", c Nothing)
  ]
  where
    c p = def {prefixLength = p, bloomFilter = bloom, cacheIndexAndFilterBlocks = cached}

-- | Column family for transactions by address ('AddrTxKey').
addrTxCF :: DB -> ColumnFamily
addrTxCF db = columnFamilies db !! 0

-- | Column family for unspent outputs by address ('AddrOutKey').
addrOutCF :: DB -> ColumnFamily
addrOutCF db = columnFamilies db !! 1

-- | Column family for transactions ('TxKey').
txCF :: DB -> ColumnFamily
txCF db = columnFamilies db !! 2

-- | Column family for unspent outputs ('UnspentKey').
unspentCF :: DB -> ColumnFamily
unspentCF db = columnFamilies db !! 4

-- | Column family for blocks ('BlockKey').
blockCF :: DB -> ColumnFamily
blockCF db = columnFamilies db !! 5

-- | Column family for block hashes by height ('HeightKey').
heightCF :: DB -> ColumnFamily
heightCF db = columnFamilies db !! 6

-- | Column family for address balances ('BalKey').
balanceCF :: DB -> ColumnFamily
balanceCF db = columnFamilies db !! 7

-- | Column family for mempool transactions ('MemTxKey' and 'MemTimeKey').
mempoolCF :: DB -> ColumnFamily
mempoolCF db = columnFamilies db !! 8

-- | Set the version of a new database, migrate an old one, or call 'error' if the version is not supported.
initRocksDB :: DatabaseReader -> IO ()
initRocksDB DatabaseReader {db = db} =
  retrieve db VersionKey >>= \case
    Just v
      | v == dataVersion -> return ()
      | v == 19 -> migrateMempoolIndex db
      | v == 18 -> migrateMempool db
      | otherwise -> error "Incorrect RocksDB database version"
    Nothing -> setInitRocksDB db

-- | Version 18 kept the whole mempool as one list under 'MemKey'. Move each
-- transaction to its own key in the mempool column family, and index it by
-- time, in the same batch that sets the new version.
migrateMempool :: DB -> IO ()
migrateMempool db = do
  mempool <- fromMaybe [] <$> (retrieve db MemKey :: IO (Maybe [(UnixTime, TxHash)]))
  writeBatch db $
    concat [mempoolInsertOps db h t | (t, h) <- mempool]
      <> [deleteOp MemKey, insertOp VersionKey dataVersion]

-- | Version 19 kept one key per mempool transaction, but no index by time,
-- so every page of the mempool had to read and sort all of it. Build the
-- index in the same batch that sets the new version.
migrateMempoolIndex :: DB -> IO ()
migrateMempoolIndex db = do
  mempool <- matchingAsListCF db (mempoolCF db) MemTxKeyS
  writeBatch db $
    [insertOpCF (mempoolCF db) (MemTimeKey t h) () | (MemTxKey h, t) <- mempool]
      <> [insertOp VersionKey dataVersion]

-- | Operations that add a transaction to the mempool: its key and its entry
-- in the index by time.
mempoolInsertOps :: DB -> TxHash -> UnixTime -> [BatchOp]
mempoolInsertOps db h t =
  [ insertOpCF (mempoolCF db) (MemTxKey h) t,
    insertOpCF (mempoolCF db) (MemTimeKey t h) ()
  ]

-- | Mempool from the index by time, newest first. The index sorts last in
-- the mempool column family, so seek past its prefix and walk backwards
-- until a key is no longer in it.
mempoolNewestC :: Iterator -> ConduitT i (UnixTime, TxHash) IO ()
mempoolNewestC it = do
  liftIO $ do
    -- 0x0c is the first prefix after the 0x0b of 'MemTimeKey'
    iterSeek it (BS.singleton 0x0c)
    iterValid it >>= \case
      True -> iterPrev it
      False -> iterLast it
  go
  where
    go =
      liftIO (iterKey it) >>= \case
        Just k | Right (MemTimeKey t h) <- decode k -> do
          yield (t, h)
          liftIO (iterPrev it)
          go
        _ -> return ()

-- | Mempool transactions that entered it before the given time, oldest
-- first, at most as many as given. Reads only those entries of the index.
getMempoolBefore ::
  (MonadIO m) =>
  UnixTime ->
  Int ->
  DatabaseReaderT m [(UnixTime, TxHash)]
getMempoolBefore time n = do
  db <- asks (.db)
  liftIO . withIterCF db (mempoolCF db) $ \it ->
    runConduit $
      matching it MemTimeKeyS
        .| concatMapC entry
        .| takeWhileC ((< time) . fst)
        .| takeC n
        .| sinkList
  where
    entry (MemTimeKey t h, ()) = Just (t, h)
    entry (MemTimeKeyS, ()) = Nothing

-- | Set the version of a new database.
setInitRocksDB :: DB -> IO ()
setInitRocksDB db = insert db VersionKey dataVersion

-- | Stream the transactions of an address, newest first, from a starting point.
-- If the start is a transaction that is not found, nothing is streamed.
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
          Just TxData {block = b@MemRef {}} ->
            -- Mempool transactions that arrived in the same second are
            -- keyed by hash, so seek to the time and drop those before it.
            let start = txRefKey (TxRef b txh)
                before (AddrTxKey _ t, ()) = txRefKey t > start
                before _ = undefined
             in readerC $ \_ ->
                  transPipe liftIO $
                    matchingSkip it (AddrTxKeyA a) (AddrTxKeyB a b)
                      .| (dropWhileC before >> mapC id)
          Nothing -> return ()

-- | Stream the unspent outputs of an address, newest first, from a starting
-- point. If the start is a transaction that is not found, nothing is streamed.
unspentConduit ::
  (MonadIO m) =>
  Network ->
  Ctx ->
  Address ->
  Maybe Start ->
  Iterator ->
  ConduitT i Unspent (DatabaseReaderT m) ()
unspentConduit net ctx a s it =
  x .| mapC (uncurry (toUnspent net ctx)) .| concatC
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
          Just TxData {block = b@MemRef {}} ->
            -- The outputs of the start transaction come first among those
            -- of the same second whose hashes are not smaller.
            let start = (b, Down (encode (OutPoint txh 0)))
                before (AddrOutKey _ r p, _) = (r, Down (encode p)) > start
                before _ = undefined
             in readerC $ \_ ->
                  transPipe liftIO $
                    matchingSkip it (AddrOutKeyA a) (AddrOutKeyB a b)
                      .| (dropWhileC before >> mapC id)
          Nothing -> return ()

-- | Run an action with an iterator on a column family, counting it in the metrics.
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

-- | Run an action with this many iterators on a column family. They are
-- opened and closed in a single bracket rather than one bracket each: each
-- bracket leaves a frame on the stack, and every call into RocksDB through
-- an iterator walks the stack, so with one bracket per address an xpub of a
-- hundred addresses made each step of each iterator a hundred times slower.
withManyIters ::
  (MonadUnliftIO m) =>
  DB ->
  ColumnFamily ->
  Int ->
  ([Iterator] -> DatabaseReaderT m a) ->
  DatabaseReaderT m a
withManyIters db cf i f = do
  m <- asks (.metrics)
  bracket_ (gauge m i) (gauge m (negate i)) $
    withRunInIO $ \run ->
      bracket (newIORef []) (readIORef >=> mapM_ destroyIterator) $ \ref -> do
        its <- replicateM i . mask_ $ do
          it <- createIterator db (Just cf)
          modifyIORef' ref (it :)
          return it
        run (f its)
  where
    gauge (Just m) n = incrementGauge m.dataIters n
    gauge Nothing _ = return ()

-- | Values of many keys of a column family, read with one multi-get, in
-- the order of the keys.
retrieveManyCF ::
  (KeyValue key value, Serialize key, Serialize value) =>
  DB ->
  ColumnFamily ->
  [key] ->
  IO [Maybe value]
retrieveManyCF db cf keys =
  map (fmap (either error id . decode)) <$> multiGetCF db cf (map encode keys)

-- | Merge streams sorted newest first, drop duplicates, and apply the offset and
-- limit.
joinConduits ::
  (Monad m, Ord k) =>
  (o -> k) ->
  [ConduitT () o m ()] ->
  Limits ->
  m [o]
joinConduits key cs l =
  runConduit $ joinDescStreams key cs .| applyLimitsC l .| sinkList

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

  getManyTxData ths = do
    db <- asks (.db)
    tds <- liftIO (retrieveManyCF db (txCF db) (map TxKey ths))
    withMetrics $ \s -> incrementCounter s.dataTxCount (length (catMaybes tds))
    return tds

  getSpender op = runMaybeT $ do
    td <- MaybeT $ getTxData op.hash
    let i = fromIntegral op.index
    MaybeT . return $ i `IntMap.lookup` td.spenders

  getUnspent p = do
    db <- asks (.db)
    net <- asks (.net)
    ctx <- asks (.ctx)
    val <- liftIO (retrieveCF db (unspentCF db) (UnspentKey p))
    case fmap (valToUnspent net ctx p) val of
      Nothing -> return Nothing
      Just u -> do
        withMetrics $ \s -> incrementCounter s.dataUnspentCount 1
        return (Just u)

  getBalance a = do
    db <- asks (.db)
    withMetrics $ \s -> incrementCounter s.dataBalanceCount 1
    fmap (valToBalance a) <$> liftIO (retrieveCF db (balanceCF db) (BalKey a))

  getMempool = getMempoolPage def

  getMempoolPage limits = do
    db <- asks (.db)
    withMetrics $ \s -> incrementCounter s.dataMempoolCount 1
    liftIO . withIterCF db (mempoolCF db) $ \it ->
      runConduit $
        mempoolNewestC it
          .| applyLimitsC limits
          .| sinkList

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
      txs <- joinConduits txRefKey (cs its) limits
      withMetrics $ \s -> incrementCounter s.dataAddrTxCount (length txs)
      return txs
    where
      cs = zipWith c addrs
      c a = addressConduit a limits.start

  getAddressesUnspents addrs limits = do
    db <- asks (.db)
    net <- asks (.net)
    ctx <- asks (.ctx)
    withManyIters db (addrOutCF db) (length addrs) $ \its -> do
      uns <- joinConduits unspentKey (cs net ctx its) limits
      withMetrics $ \s -> incrementCounter s.dataUnspentCount (length uns)
      return uns
    where
      cs net ctx = zipWith (c net ctx) addrs
      c net ctx a = unspentConduit net ctx a limits.start

  getAddressUnspents a limits = do
    db <- asks (.db)
    net <- asks (.net)
    ctx <- asks (.ctx)
    us <- withIterCFStat db (addrOutCF db) $ \it ->
      runConduit $
        unspentConduit net ctx a limits.start it
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

  getBalances [] = return []
  getBalances as = do
    db <- asks (.db)
    withMetrics $ \s -> incrementCounter s.dataBalanceCount (length as)
    vals <- liftIO (retrieveManyCF db (balanceCF db) (map BalKey as))
    return (zipWith f as vals)
    where
      f a Nothing = zeroBalance a
      f a (Just v) = valToBalance a v

  xPubBals xpub = do
    igap <- asks (.initGap)
    gap <- asks (.maxGap)
    Derived ext chg <- derivedAddrs xpub
    bals <- scanXPubBals getBalances igap gap ext chg
    withMetrics $ \s -> incrementCounter s.dataXPubBals (length bals)
    return bals

  -- The first unspent outputs of each address, at most offset plus limit
  -- of them, sorted together. Read the addresses in step, so that each is
  -- read only as far as the page needs, rather than decoding the whole
  -- quota of every address to keep one page of it.
  xPubUnspents _xspec xbals limits = do
    db <- asks (.db)
    net <- asks (.net)
    ctx <- asks (.ctx)
    let quota = case (deOffset limits).limit of
          0 -> Nothing
          l -> Just l
        stream b it =
          unspentConduit net ctx b.balance.address limits.start it
            .| mapC (\u -> XPubUnspent {path = b.path, unspent = u})
    us <- withManyIters db (addrOutCF db) (length cs) $ \its ->
      runConduit $
        mergeDescGroups (.unspent.block) (unspentKey . (.unspent)) quota (zipWith stream cs its)
          .| applyLimitsC limits
          .| sinkList
    withMetrics $ \s -> incrementCounter s.dataXPubUnspents (length us)
    return us
    where
      cs = filter ((> 0) . (.balance.utxo)) xbals

  xPubTxs _xspec xbals limits = do
    let as =
          map (.address) $
            filter (not . nullBalance) $
              map (.balance) xbals
    txs <- getAddressesTxs as limits
    withMetrics $ \s -> incrementCounter s.dataXPubTxs (length txs)
    return txs

  -- Count the transactions of all the addresses, each once even when it
  -- touches several of them. The entries of an address are sorted, and the
  -- same transaction has the same key after the address prefix in every
  -- address, so merge the addresses by those raw key bytes and count the
  -- distinct ones, without decoding any key or reading any value.
  xPubTxCount _xspec xbals = do
    withMetrics $ \s -> incrementCounter s.dataXPubTxCount 1
    db <- asks (.db)
    let as =
          map (.address) $
            filter (not . nullBalance) $
              map (.balance) xbals
    withManyIters db (addrTxCF db) (length as) $ \its ->
      liftIO $ fromIntegral <$> countDistinctSuffixes (zip (map (encode . AddrTxKeyA) as) its)

-- | Number of distinct keys, after their prefix, in a set of iterators that
-- each walk the keys with one prefix, in order.
countDistinctSuffixes :: [(BS.ByteString, Iterator)] -> IO Int
countDistinctSuffixes streams = do
  heads <- mapM start streams
  go 0 (Map.fromListWith (<>) [(k, [x]) | (k, x) <- catMaybes heads])
  where
    start x@(pfx, it) = iterSeek it pfx >> current x
    current x@(pfx, it) =
      iterKey it <&> \case
        Just k | pfx `BS.isPrefixOf` k -> Just (BS.drop (BS.length pfx) k, x)
        _ -> Nothing
    advance x@(_, it) = iterNext it >> current x
    go !n m = case Map.minViewWithKey m of
      Nothing -> return n
      Just ((_, xs), m') -> do
        nexts <- catMaybes <$> mapM advance xs
        go (n + 1) (foldr (\(k, x) -> Map.insertWith (<>) k [x]) m' nexts)
