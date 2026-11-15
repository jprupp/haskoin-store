{-# LANGUAGE RecordWildCards #-}

-- |
-- Module      : Haskoin.Store.Database.Reader
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The RocksDB database of the store: opening it ('withDatabaseReader'), which
-- also creates or migrates it, its column families, its metrics, and the
-- memo of the addresses of extended public keys. The queries are in
-- "Haskoin.Store.Database.Query".
--
-- Two of the column families, the balances and the spenders, are opened
-- with a merge operator ("Haskoin.Store.Database.Merge"): what a reader
-- gets from them is always a value, which RocksDB may have worked out from
-- merge records on the way.
--
-- Data is split across column families by kind; see 'addrTxCF' and the other
-- column family accessors. The keys stored in each are described in
-- "Haskoin.Store.Database.Types".
--
-- = Why column families
--
-- Each kind of record lives in its own column family. Their keys and access
-- patterns differ: balances are small point lookups, address transactions are
-- scans, transactions are large values. Separate column families let each have
-- its own key prefix length for filters and seeks, and keep scans over one kind
-- of data from wading through another.
--
-- = The copy of the mempool area
--
-- An open database also has a place for a copy in memory of its mempool
-- area ("Haskoin.Store.Database.Copy"), which the batches of the block
-- store read instead of RocksDB ('mempoolCopy'). It belongs here because
-- it goes with the open database: one for each, loaded when a batch first
-- needs it, and gone when the database is closed. The queries of the API
-- do not use it.
--
-- The copy also holds what the block store remembers of the outputs that
-- mempool transactions spend. Both are what the database has now, between
-- two batches, and neither is true of the database as a view of it sees
-- it. So a reader of a snapshot ('snapshotReader'), which is what every
-- request of the API and every member of a fleet reads through, has a
-- place of its own for a copy, which nothing fills: the copy of the block
-- store cannot be reached from it.
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
    DataMetrics (..),
    Derived (..),
    derivedAddrs,
    createDataMetrics,
    withDatabaseReader,
    withLayeredReader,
    withReader,
    columnFamilyConfig,
    layeredFamilyConfig,
    mergeGuardFamily,
    initRocksDB,
    memoryReport,
    addrTxCF,
    addrOutCF,
    txCF,
    spenderCF,
    unspentCF,
    blockCF,
    heightCF,
    balanceCF,
    mempoolCF,
    mempoolAddrCF,
    mempoolInsertOps,

    -- * The copy of the mempool area
    mempoolCopy,
    knownMempoolCopy,
    forgetMempoolCopy,
    snapshotReader,
    mempoolFamily,
  )
where

import Control.Monad (forM, guard)
import Data.ByteString.Char8 qualified as BS8
import Data.Default (def)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.Maybe (fromMaybe, isJust, mapMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Word (Word32)
import Database.RocksDB hiding (withDB)
import Database.RocksDB.Query
import Haskoin
import Haskoin.Store.Common
import Haskoin.Store.Data
import Haskoin.Store.Database.Copy
  ( Copy,
    CopyRef,
    Family (..),
    forgetCopy,
    knownCopy,
    loadCopy,
    newCopyRef,
    setCopy,
  )
import Haskoin.Store.Database.Merge (balanceOperator, spenderOperator)
import Haskoin.Store.Database.Types
import System.Metrics.StatsD
import Text.Read (readMaybe)

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
    -- | addresses derived from recently queried xpubs
    derived :: !(IORef DerivedCache),
    -- | the copy in memory of the mempool area, once a batch of writes has
    -- needed it ('mempoolCopy'). Each open database has its own.
    copy :: !CopyRef
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
      fresh =
        let addrs m =
              deriveAddresses
                (deriveFunction xpub.deriv)
                (pubSubKey xpub.key m)
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

-- | Version of the layout that 'withDatabaseReader' opens, in which the
-- mempool is written into the indices of the chain. Databases of versions
-- 18 and 19 are migrated to it when opened that way; other versions are
-- refused.
--
-- The store runs on a later one,
-- 'Haskoin.Store.Database.Migrate.currentVersion', and opens its database
-- with 'withLayeredReader' and 'Haskoin.Store.Database.Migrate.upgradeDatabase'.
-- This version is kept for the tests, which hold the new layout to the
-- answers of this one, and as the way through for databases of 18 and 19.
--
-- Bumping the version is how a change to the database layout is made safe: an
-- old database is either migrated in place when it is opened or refused, never
-- read with the wrong layout.
dataVersion :: Word32
dataVersion = 20

-- | Open the database and run a query against it. Creates the database and its
-- column families if missing, and migrates it from older versions.
--
-- Arguments, in order: network, initial xpub gap, maximum xpub gap, directory,
-- whether to use Bloom filters, maximum open files, total memtable size in
-- bytes, write-ahead log size in bytes after which column families are
-- flushed, threads for flushes and compactions together, block cache size in
-- bytes, and metrics. 'Nothing' leaves a setting to RocksDB.
withDatabaseReader ::
  Network ->
  Word32 ->
  Word32 ->
  FilePath ->
  Bool ->
  Maybe Int ->
  Maybe Int ->
  Maybe Int ->
  Maybe Int ->
  Maybe Int ->
  Maybe DataMetrics ->
  (DatabaseReader -> IO a) ->
  IO a
withDatabaseReader = withReader columnFamilyConfig initRocksDB

-- | Open a database with the column families of the layered layout of
-- @docs\/layered-database.md@: those of 'withDatabaseReader' and two more,
-- one for what the mempool does at each address, and one that is empty
-- ('layeredFamilyConfig'). The arguments are those of 'withDatabaseReader'.
--
-- It neither checks nor sets the version of the database:
-- 'Haskoin.Store.Database.Migrate.upgradeDatabase' does, and has to be
-- called before anything reads the database. The store opens its database
-- with the two.
--
-- Opening a database this way adds the two column families to it. RocksDB
-- only opens a database when given all its column families, so a store
-- that does not know them can no longer open the database afterwards: not
-- one through 'withDatabaseReader', and not one older than database
-- version 23, which does not know the empty one and could not read the
-- merge records of the balances and the spenders. That is the point of no
-- return of an upgrade.
withLayeredReader ::
  Network ->
  Word32 ->
  Word32 ->
  FilePath ->
  Bool ->
  Maybe Int ->
  Maybe Int ->
  Maybe Int ->
  Maybe Int ->
  Maybe Int ->
  Maybe DataMetrics ->
  (DatabaseReader -> IO a) ->
  IO a
withLayeredReader = withReader layeredFamilyConfig (\_ -> return ())

-- | Open a database with these column families, given whether to use Bloom
-- filters and whether there is a shared block cache, and run a check on it
-- before anything else. The other arguments are those of
-- 'withDatabaseReader'.
--
-- The store opens its database with 'withLayeredReader'. This is for the
-- tests and the benchmarks, which open one with the column families of
-- 'layeredFamilyConfig' changed: without the one that older versions do
-- not have, as such a version left a database, or with RocksDB kept from
-- compacting. The families must be those of 'columnFamilyConfig' or of
-- 'layeredFamilyConfig' in their order, since the column families are
-- found by their place ('balanceCF' and the others).
withReader ::
  (Bool -> Bool -> [(String, Config)]) ->
  (DatabaseReader -> IO ()) ->
  Network ->
  Word32 ->
  Word32 ->
  FilePath ->
  Bool ->
  Maybe Int ->
  Maybe Int ->
  Maybe Int ->
  Maybe Int ->
  Maybe Int ->
  Maybe DataMetrics ->
  (DatabaseReader -> IO a) ->
  IO a
withReader families check net igap gap dir bloom maxOpenFiles writeBuffers maxWal jobs blockCache stats m =
  withDBCF dir cfg (families bloom (isJust blockCache)) $ \db -> do
    derived <- newIORef (DerivedCache HashMap.empty HashMap.empty)
    copy <- newCopyRef
    let bdb =
          DatabaseReader
            { db = db,
              maxGap = gap,
              net = net,
              initGap = igap,
              metrics = stats,
              derived = derived,
              copy = copy
            }
    check bdb
    m bdb
  where
    cfg =
      def
        { createIfMissing = True,
          createMissingColumnFamilies = True,
          maxFiles = maxOpenFiles,
          dbWriteBufferSize = writeBuffers,
          maxTotalWalSize = maxWal,
          maxBackgroundJobs = jobs,
          blockCacheSize = blockCache,
          cacheIndexAndFilterBlocks = isJust blockCache,
          partitionIndexAndFilters = isJust blockCache
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
    [ "** RocksDB memory (bytes) **",
      T.justifyLeft 10 ' ' "family"
        <> T.justifyRight 16 ' ' "memtables"
        <> T.justifyRight 16 ' ' "table readers"
    ]
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
--
-- = Why the indices by address never have filters
--
-- A filter lets RocksDB skip a table file that cannot have a key, or, for a
-- scan, a key prefix. That is safe for column families that are read by
-- key. It is not for the two that are scanned by address, as long as the
-- iterators of the binding have no bound: a scan asks RocksDB for the first
-- key at or after an address and stops when the key is of another address.
-- With filters, the files that hold the keys of the addresses that follow
-- are skipped, as they should be, but the keys that were deleted lately are
-- not in a file yet, and RocksDB steps over every one of them looking for a
-- key to give back, since it can no longer see that most still have
-- neighbours that are alive.
--
-- Measured with 5,000 mempool transactions just deleted: a scan of an
-- address that has nothing took 810 microseconds with filters on these
-- indices, and 13 without, in the layout of version 20. No filter was ever
-- built before rocksdb-haskell-jprupp 2.6.2, so it never happened to a
-- store, and filters are on by default now for everything else.
--
-- = The two that merge
--
-- The column families of the balances and of the spenders have a merge
-- operator each ("Haskoin.Store.Database.Merge"), since database version
-- 23: the import of a block writes merge records to them, and RocksDB
-- works out the values. They are set here, in the one place that every
-- way of opening the database goes through, because a database with merge
-- records must never be opened without them.
columnFamilyConfig :: Bool -> Bool -> [(String, Config)]
columnFamilyConfig bloom cached =
  [ ("addr-tx", scanned (Just 22)),
    ("addr-out", scanned (Just 22)),
    ("tx", c (Just 33)),
    ("spender", (c (Just 33)) {mergeOperator = Just spenderOperator}),
    ("unspent", c (Just 37)),
    ("block", c (Just 33)),
    ("height", c Nothing),
    ("balance", (c (Just 22)) {mergeOperator = Just balanceOperator}),
    ("mempool", c Nothing)
  ]
  where
    c p =
      def
        { prefixLength = p,
          bloomFilter = bloom,
          cacheIndexAndFilterBlocks = cached,
          partitionIndexAndFilters = cached
        }
    scanned p = (c p) {bloomFilter = False}

-- | Column families of the layered layout: those of 'columnFamilyConfig',
-- one for the entries of the mempool by address ('MemAddrKey'), and one
-- that is empty and keeps older versions out.
--
-- The two column families of the mempool always have Bloom filters, whatever
-- the setting for the others. Almost every lookup in them is for something
-- that is not there: a confirmed transaction, an output that no mempool
-- transaction spends, an address with nothing in the mempool. A filter
-- answers those without reading a block, and for column families this small
-- it takes a few megabytes; on the column families of the chain the same
-- filters would take gigabytes, which is why they are optional there.
--
-- The entries by address are in a column family of their own because their
-- filter is on the first 22 bytes of the key, the kind of key and the
-- address: a scan of an address with nothing in the mempool then stops at
-- the filter. The mempool column family is read by whole keys and scanned
-- across prefixes, for the index by time, so it cannot have a filter by
-- prefix.
--
-- That scan through a filter is what 'columnFamilyConfig' keeps out of the
-- indices of the chain, and it is as slow here after a block, which deletes
-- the keys of the transactions it confirms: 1.6 milliseconds for an address
-- with nothing, measured. It is one of the things that have to change before
-- this layout is used; @docs\/layered-database.md@ says how.
--
-- = The empty one
--
-- Since database version 23 the balances and the spenders have merge
-- records, which only RocksDB's merge operators can read. A store of an
-- older version does not set them, and RocksDB opens the database for it
-- all the same: it then drops, without a word, the merge records that are
-- still in the write-ahead log, which is the balances and the spenders of
-- the last blocks, and the store finds out about the version afterwards.
--
-- RocksDB does refuse to open a database when it is not given all of its
-- column families, and it does so before it reads the write-ahead log. So
-- the layout has a column family that those versions do not know of,
-- 'mergeGuardFamily', with nothing in it. They fail to open the database,
-- with an error that names it, and leave the database as it was.
layeredFamilyConfig :: Bool -> Bool -> [(String, Config)]
layeredFamilyConfig bloom cached =
  map mempool (columnFamilyConfig bloom cached)
    <> [("mempool-addr", c (Just 22)), (mergeGuardFamily, def)]
  where
    c p =
      def
        { prefixLength = p,
          bloomFilter = True,
          cacheIndexAndFilterBlocks = cached,
          partitionIndexAndFilters = cached
        }
    mempool (name, cfg)
      | name == "mempool" = (name, c Nothing)
      | otherwise = (name, cfg)

-- | The name of the column family that has nothing in it, and is there so
-- that a store older than database version 23 cannot open the database:
-- see 'layeredFamilyConfig'. The name is what the error of such a store
-- says is missing.
mergeGuardFamily :: String
mergeGuardFamily = "merge-operators"

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

-- | Column family for the inputs of blocks that spend outputs
-- as t'Haskoin.Store.Database.Types.SpenderKey' says.
spenderCF :: DB -> ColumnFamily
spenderCF db = columnFamilies db !! 3

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
-- In the layered layout it also has their bodies, under t'TxKey', and
-- their spenders, under t'MemSpenderKey'.
mempoolCF :: DB -> ColumnFamily
mempoolCF db = columnFamilies db !! 8

-- | Column family for what the mempool does at each address ('MemAddrKey').
-- Only a database opened with 'withLayeredReader' has it.
mempoolAddrCF :: DB -> ColumnFamily
mempoolAddrCF db = columnFamilies db !! 9

-- | The copy in memory of the mempool area of the database
-- ("Haskoin.Store.Database.Copy"): the one that the database has, or, the
-- first time and whenever that one was forgotten, a new one from a scan of
-- the two column families.
--
-- It is for the block store, through its batches of writes, and for
-- nothing else. The copy is what the database has only between two
-- batches and for the batch that is open: a request of the API that read
-- it could see a batch that RocksDB has and the copy has not yet, or the
-- other way around. Requests read RocksDB.
mempoolCopy :: DatabaseReader -> IO Copy
mempoolCopy r =
  knownCopy r.copy >>= \case
    Just c -> return c
    Nothing -> do
      c <- loadCopy r.db (mempoolCF r.db) (mempoolAddrCF r.db <$ guard layered)
      setCopy r.copy c
      return c
  where
    -- a database of the layout of version 20 has no entries by address
    layered = length (columnFamilies r.db) > 9

-- | The copy of the mempool area if the database has one that is known to
-- be right, without loading it: for what should use the copy only where a
-- batch has asked for it, and for the tests.
knownMempoolCopy :: DatabaseReader -> IO (Maybe Copy)
knownMempoolCopy r = knownCopy r.copy

-- | Say that the mempool area was written to by something else than a
-- batch of 'Haskoin.Store.Database.Writer.runWriterReport', so that the
-- copy is loaded again by the next batch that needs it. Whatever writes
-- to 'mempoolCF' or 'mempoolAddrCF' by itself has to call this afterwards:
-- a migration, a wipe of the mempool, a test. A batch does not need to: it
-- changes the copy as it is written.
forgetMempoolCopy :: DatabaseReader -> IO ()
forgetMempoolCopy r = forgetCopy r.copy

-- | A reader of a snapshot of the database of another reader: the same
-- in everything, but that it reads the snapshot, and that it does not
-- share the copy of the mempool area.
--
-- The copy, with what it remembers of the outputs that mempool
-- transactions spend, is what the database has now. A snapshot is the
-- database as it was. So the reader of a snapshot gets a place of its own
-- for a copy, with nothing in it. Nothing that reads through a view asks
-- for a copy; if something ever did, it would get one loaded from the
-- snapshot, which is right for it, and not the one of the block store.
snapshotReader :: DatabaseReader -> DB -> IO DatabaseReader
snapshotReader r snapshot = do
  own <- newCopyRef
  return r {db = snapshot, copy = own}

-- | Which column family of the mempool area a column family of this
-- database is, if it is one of the two.
mempoolFamily :: DB -> ColumnFamily -> Maybe Family
mempoolFamily db = which
  where
    which cf
      | cf == general = Just General
      | Just cf == byAddress = Just ByAddress
      | otherwise = Nothing
    families = columnFamilies db
    general = families !! 8
    byAddress = case drop 9 families of
      a : _ -> Just a
      [] -> Nothing

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
