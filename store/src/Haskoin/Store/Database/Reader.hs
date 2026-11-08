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
    DataMetrics (..),
    Derived (..),
    derivedAddrs,
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
    mempoolInsertOps,
  )
where

import Conduit
import Control.Monad (forM)
import Control.Monad.Reader
import Data.ByteString.Char8 qualified as BS8
import Data.Default (def)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.Maybe (fromMaybe, isJust, mapMaybe)
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
derivedAddrs :: DatabaseReader -> XPubSpec -> IO Derived
derivedAddrs r xpub = do
  let ref = r.derived
      ctx = r.ctx
      fresh =
        let addrs m =
              deriveAddresses
                (deriveFunction ctx xpub.deriv)
                (pubSubKey ctx xpub.key m)
                0
         in Derived (addrs 0) (addrs 1)
  atomicModifyIORef' ref $ \c ->
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
  (DatabaseReader -> m a) ->
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
      run (m bdb)
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

-- | Set the version of a new database.
setInitRocksDB :: DB -> IO ()
setInitRocksDB db = insert db VersionKey dataVersion
