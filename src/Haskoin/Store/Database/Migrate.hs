{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE MultiWayIf #-}

-- |
-- Module      : Haskoin.Store.Database.Migrate
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Migration of a database of version 20 to the layered layout, version 21,
-- of @docs\/layered-database.md@: the column families of the chain are left
-- with nothing from the mempool, and the mempool gets an area of its own
-- ("Haskoin.Store.Database.Mempool").
--
-- The store runs it when it opens a database ('upgradeDatabase'). It is
-- what makes the change of layout possible without syncing the chain again.
--
-- = What it does
--
-- In version 20 a mempool transaction is written into the indices of the
-- chain: its body, the entries of its addresses, its outputs as unspent, the
-- outputs it spends as spent, with itself as their spender in the bodies of
-- the transactions that created them, and all of it in the balances. The
-- migration takes each mempool transaction out of those indices, exactly as
-- deleting it does in version 20, and writes it to the mempool area instead.
-- When it is done, the chain's column families hold what they would hold if
-- the mempool had been wiped, and the mempool area holds the mempool.
--
-- It reads and writes only what the mempool touches, so it takes a time in
-- proportion to the mempool, not to the chain: about what wiping the
-- mempool takes in version 20.
--
-- = Why its own rules
--
-- Taking a transaction out of the indices of version 20 is what
-- "Haskoin.Store.Logic" does today when it deletes one. The migration does
-- not call that code: the rules of the store will change with the layout,
-- and a migration has to go on reading version 20 as it was. It carries the
-- few rules it needs, and the tests check them against "Haskoin.Store.Logic"
-- while both exist.
--
-- = Safe to interrupt
--
-- A mempool can hold hundreds of thousands of transactions, too many for one
-- write, so the migration works in batches. Each batch is one atomic write
-- that moves whole transactions, descendants before the transactions they
-- spend. After any batch the database is a valid mix: the transactions
-- already moved are in the mempool area, and the others are still in the
-- chain's indices, as a database of version 20 with a smaller mempool. The
-- migration can stop there and carry on later.
--
-- The first write sets the new version and a marker, t'MigrationKey', so
-- that a store of version 20 refuses the database from then on and a store
-- that knows the new layout carries the migration on. The last write
-- removes the marker. Before the first write everything is read once, so
-- that a database that the migration cannot handle is refused untouched.
--
-- = Version 22: the time of a confirmed transaction
--
-- Version 21 still had one thing in the chain's column families that did
-- not follow from the chain: a transaction that a store had seen in its
-- mempool kept the time of that when a block confirmed it. From version 22
-- a confirmed transaction has the time of its block
-- ('Haskoin.Store.Logic.Layered.importBlock'), and 'migrateTimes' gives
-- that time to the ones that are there already. It has to read every
-- transaction to find them, since nothing lists them, so it takes a time
-- in proportion to the chain: 'upgradeDatabase' says how far it is as it
-- goes. It changes nothing but that one number of each record, in small
-- writes, and does the same again if it is run again, so it can be
-- interrupted and started over.
module Haskoin.Store.Database.Migrate
  ( upgradeDatabase,
    currentVersion,
    migrateTimes,
    layeredVersion,
    Migration (..),
    MigrationError (..),
    MigrationOptions (..),
    defaultMigrationOptions,
    migrateLayered,
    migrateLayeredWith,
  )
where

import Control.Monad (forM_, unless, void, when)
import Data.Default (def)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.IntMap.Strict qualified as IntMap
import Data.Maybe (isJust, mapMaybe)
import Data.Serialize (decode, encode)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Word (Word32, Word64)
import Database.RocksDB (BatchOp, getCF, iterEntry, iterFirst, iterGetError, iterNext, withIterCF)
import Database.RocksDB.Query
  ( deleteOp,
    deleteOpCF,
    insertOp,
    insertOpCF,
    matchingAsListCF,
    retrieve,
    retrieveCF,
    writeBatch,
  )
import Haskoin
  ( Address,
    BlockHeader (..),
    OutPoint (..),
    Tx (..),
    TxHash,
    TxIn (..),
    TxOut (..),
    eitherToMaybe,
    nullOutPoint,
    scriptToAddressBS,
    txHash,
    txHashToHex,
  )
import Haskoin.Store.Common (nub')
import Haskoin.Store.Data
  ( BlockData (..),
    BlockRef (..),
    Prev (..),
    Spender (..),
    TxData (..),
    TxRef (..),
    UnixTime,
    confirmed,
  )
import Haskoin.Store.Database.Mempool (insertRecordOps, mempoolRecords, rebuildSummaries)
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    addrOutCF,
    addrTxCF,
    balanceCF,
    initRocksDB,
    mempoolCF,
    txCF,
    unspentCF,
  )
import Haskoin.Store.Database.Types
  ( AddrOutKey (..),
    AddrTxKey (..),
    BalKey (..),
    BalVal (..),
    MemTxKey (..),
    MigrationKey (..),
    OutVal (..),
    TxKey (..),
    UnspentKey (..),
    UnspentVal (..),
    VersionKey (..),
  )
import UnliftIO
  ( Exception,
    IORef,
    modifyIORef',
    newIORef,
    readIORef,
    throwIO,
    writeIORef,
  )

-- | Version of the layered layout as it was first written, with the time
-- at which a store first saw a transaction kept in the chain.
layeredVersion :: Word32
layeredVersion = 21

-- | Version that the store runs on: the layered layout, with the time of
-- its block for every confirmed transaction.
currentVersion :: Word32
currentVersion = 22

-- | Version that the migration starts from.
mixedVersion :: Word32
mixedVersion = 20

-- | What a migration did.
data Migration = Migration
  { -- | mempool transactions moved to the mempool area by this run
    moved :: !Int,
    -- | writes to the database that moved them
    batches :: !Int,
    -- | mempool transactions checked at the end, moved by this run or by
    -- an earlier one that was interrupted
    checked :: !Int,
    -- | confirmed transactions given the time of their block by this run
    retimed :: !Int
  }
  deriving (Eq, Show)

-- | Why a migration refused a database, or stopped.
data MigrationError
  = -- | the database is of a version that this migration does not start
    -- from, or has none
    UnknownVersion !(Maybe Word32)
  | -- | the database is not as its version says it should be; nothing was
    -- written for the transaction that showed it
    Inconsistent !Text
  deriving (Eq, Show, Exception)

-- | How to run a migration.
data MigrationOptions = MigrationOptions
  { -- | mempool transactions to move in each write
    chunk :: !Int,
    -- | called after each write with the number of transactions moved so
    -- far; the tests interrupt the migration from here
    afterBatch :: Int -> IO (),
    -- | where to say what is going on
    report :: Text -> IO ()
  }

-- | A thousand transactions in each write, the size of the batches that
-- wipe the mempool in version 20, and nothing said.
defaultMigrationOptions :: MigrationOptions
defaultMigrationOptions =
  MigrationOptions
    { chunk = 1000,
      afterBatch = \_ -> return (),
      report = \_ -> return ()
    }

-- | Bring a database opened with
-- 'Haskoin.Store.Database.Reader.withLayeredReader' to the layout that the
-- store runs on, whatever it was, telling a function what is being done:
-- a new database gets the current version, one of version 18 or 19 is
-- first brought to version 20 as it always was, one of version 20 is
-- migrated to the layered layout ('migrateLayered'), and one of version 21
-- gets the time of its block for every confirmed transaction
-- ('migrateTimes'), each from the start or after an earlier run that was
-- interrupted. Throws a 'MigrationError' for any other version.
--
-- This is the point of no return for a database of an older version: a
-- store older than the layout refuses it afterwards.
upgradeDatabase :: (Text -> IO ()) -> DatabaseReader -> IO Migration
upgradeDatabase report r = do
  version <- retrieve r.db VersionKey
  marker <- retrieve r.db MigrationKey :: IO (Maybe Word32)
  case version of
    Nothing -> do
      writeBatch r.db [insertOp VersionKey currentVersion]
      return nothing
    Just v
      | v == 18 || v == 19 -> do
          report $ "Upgrading the database from version " <> cs (show v) <> " to " <> cs (show mixedVersion)
          initRocksDB r
          layered >>= times
      | v == currentVersion && marker == Nothing -> return nothing
      -- the times were being set when the store last stopped
      | v == currentVersion && marker == Just layeredVersion -> times nothing
      | v == currentVersion -> throwIO (UnknownVersion version)
    Just (_ :: Word32) -> layered >>= times
  where
    nothing = Migration {moved = 0, batches = 0, checked = 0, retimed = 0}
    layered = migrateLayeredWith defaultMigrationOptions {report = report} r
    times done = do
      retimed <- migrateTimes report r
      return (done :: Migration) {retimed}

-- | Give every confirmed transaction of a database of version 21 the time
-- of its block, and make the database one of version 22; or carry that on
-- in a database where it was interrupted. Says how many transactions it
-- changed. Throws a 'MigrationError' for a database of another version,
-- and for one with a confirmed transaction at a height where the main
-- chain has no block.
--
-- Nothing lists the transactions that a store first saw in its mempool, so
-- every record is read: the whole of the largest column family, which on
-- a database of Bitcoin is hours. A function is told how far it is, every
-- million transactions. Records that have the right time already are not
-- written, so a store that synced from scratch and saw few transactions
-- in its mempool has few writes.
--
-- The first write sets the new version and the marker of a migration under
-- way; the last removes the marker. In between each write gives some
-- thousand transactions their times, and a database that is half done is a
-- valid one with both kinds of time in it. Starting again reads from the
-- beginning and skips what is done.
migrateTimes :: (Text -> IO ()) -> DatabaseReader -> IO Int
migrateTimes report r = do
  version <- retrieve db VersionKey
  marker <- retrieve db MigrationKey :: IO (Maybe Word32)
  if
    | version == Just layeredVersion && marker == Nothing -> do
        report "Setting the time of every confirmed transaction to that of its block: this reads them all"
        writeBatch db [insertOp VersionKey currentVersion, insertOp MigrationKey layeredVersion]
        run
    | version == Just currentVersion && marker == Just layeredVersion -> do
        report "Carrying on setting the times of confirmed transactions, from the first"
        run
    | version == Just currentVersion && marker == Nothing -> return 0
    | otherwise -> throwIO (UnknownVersion version)
  where
    db = r.db
    run = do
      times <- newIORef IntMap.empty
      pending <- newIORef []
      scanned <- newIORef (0 :: Int)
      changed <- newIORef (0 :: Int)
      let write = do
            ops <- readIORef pending
            unless (null ops) (writeBatch db ops)
            writeIORef pending []
          -- the time of the block of the main chain at a height
          timeAt :: Word32 -> IO Word64
          timeAt height =
            IntMap.lookup (fromIntegral height) <$> readIORef times >>= \case
              Just t -> return t
              Nothing -> do
                blocks <- Q.getBlocksAtHeight r height >>= mapM (Q.getBlock r)
                case [b | Just b <- blocks, b.main] of
                  [b] -> do
                    let t = fromIntegral b.header.timestamp
                    t <$ modifyIORef' times (IntMap.insert (fromIntegral height) t)
                  _ -> throwIO (Inconsistent ("No block of the main chain at height " <> cs (show height)))
          one (key, value) = case (decode key, decode value) of
            (Right (TxKey h), Right (td :: TxData)) -> do
              case td.block of
                BlockRef {height} | not td.deleted -> do
                  t <- timeAt height
                  when (td.timestamp /= t) $ do
                    modifyIORef' pending (insertOpCF (txCF db) (TxKey h) ((td :: TxData) {timestamp = t}) :)
                    modifyIORef' changed (+ 1)
                    n <- length <$> readIORef pending
                    when (n >= 2000) write
                _ -> return ()
              n <- readIORef scanned
              writeIORef scanned (n + 1)
              when ((n + 1) `mod` 1000000 == 0) $ do
                c <- readIORef changed
                report $ cs (show (n + 1)) <> " transactions read, " <> cs (show c) <> " given the time of their block"
            -- the short keys of the column family are not transactions
            _ -> return ()
          go it =
            iterEntry it >>= \case
              Nothing -> iterGetError it >>= mapM_ (throwIO . Inconsistent . cs . show)
              Just row -> one row >> iterNext it >> go it
      withIterCF db (txCF db) (\it -> iterFirst it >> go it)
      write
      writeBatch db [deleteOp MigrationKey]
      c <- readIORef changed
      n <- readIORef scanned
      report $ cs (show c) <> " of " <> cs (show n) <> " transactions given the time of their block"
      return c

-- | Migrate a database of version 20, opened with
-- 'Haskoin.Store.Database.Reader.withLayeredReader', to the layered layout,
-- or carry on a migration that was interrupted. Does nothing to a database
-- that is already there. Throws a 'MigrationError' otherwise.
migrateLayered :: DatabaseReader -> IO Migration
migrateLayered = migrateLayeredWith defaultMigrationOptions

-- | 'migrateLayered' with options.
migrateLayeredWith :: MigrationOptions -> DatabaseReader -> IO Migration
migrateLayeredWith opts r = do
  version <- retrieve db VersionKey
  marker <- retrieve db MigrationKey :: IO (Maybe Word32)
  if
    -- a database that is past this migration, whatever came after it
    | version == Just layeredVersion && marker == Nothing ->
        return Migration {moved = 0, batches = 0, checked = 0, retimed = 0}
    | version == Just currentVersion ->
        return Migration {moved = 0, batches = 0, checked = 0, retimed = 0}
    | version == Just layeredVersion && marker == Just mixedVersion -> do
        opts.report "Carrying on with the migration to the layered layout"
        run
    | version == Just mixedVersion -> do
        listed <- mempoolList
        mapM_ (preflight r) listed
        opts.report $ "Moving " <> cs (show (length listed)) <> " mempool transactions to their own area"
        -- from here on a store of version 20 refuses the database
        writeBatch db [insertOp VersionKey layeredVersion, insertOp MigrationKey mixedVersion]
        run
    | otherwise -> throwIO (UnknownVersion version)
  where
    db = r.db
    mempoolList = do
      rows <- matchingAsListCF db (mempoolCF db) MemTxKeyS
      return [h | (MemTxKey h, _ :: UnixTime) <- rows]
    run = do
      env <- newEnv opts r
      listed <- mempoolList
      mapM_ (moveTx env) listed
      flush env
      mapM_ (verify r) listed
      -- the summaries are worked out from the entries, all at once: doing it
      -- again after an interruption gives the same
      void (rebuildSummaries r)
      writeBatch db [deleteOp MigrationKey]
      moved <- readIORef env.total
      batches <- readIORef env.written
      opts.report $ "Moved " <> cs (show moved) <> " mempool transactions"
      return Migration {moved, batches, checked = length listed, retimed = 0}

-------------------------------------------------------------------------------
-- A batch
-------------------------------------------------------------------------------

-- | A migration under way, and the batch that it is building: what it will
-- write, with the last value of everything that is read again before the
-- write.
data Env = Env
  { reader :: !DatabaseReader,
    options :: !MigrationOptions,
    -- | bodies in the chain's column family that the batch changes;
    -- 'Nothing' for one that it removes
    bodies :: !(IORef (HashMap TxHash (Maybe TxData))),
    -- | balances in the chain's column family that the batch changes
    balances :: !(IORef (HashMap Address BalVal)),
    -- | everything else that the batch writes, newest first: each of these
    -- keys is written without being read, and in this order
    ops :: !(IORef [BatchOp]),
    -- | transactions that the batch moves
    moving :: !(IORef (HashSet TxHash)),
    -- | transactions moved by this run
    total :: !(IORef Int),
    -- | batches written by this run
    written :: !(IORef Int)
  }

-- | A migration that has moved nothing yet, with an empty batch.
newEnv :: MigrationOptions -> DatabaseReader -> IO Env
newEnv options reader = do
  bodies <- newIORef HashMap.empty
  balances <- newIORef HashMap.empty
  ops <- newIORef []
  moving <- newIORef HashSet.empty
  total <- newIORef 0
  written <- newIORef 0
  return Env {reader, options, bodies, balances, ops, moving, total, written}

-- | Body of a transaction in the chain's column family, as the batch leaves
-- it.
chainBody :: Env -> TxHash -> IO (Maybe TxData)
chainBody env h =
  HashMap.lookup h <$> readIORef env.bodies >>= \case
    Just changed -> return changed
    Nothing -> retrieveCF db (txCF db) (TxKey h)
  where
    db = env.reader.db

-- | Change the balance of an address in the chain's column family, or stop
-- if the change cannot be made.
change :: Env -> Address -> (BalVal -> Either Text BalVal) -> IO ()
change env a f = do
  current <-
    HashMap.lookup a <$> readIORef env.balances >>= \case
      Just b -> return b
      Nothing -> maybe def id <$> retrieveCF db (balanceCF db) (BalKey a)
  new <- either (throwIO . Inconsistent) return (f current)
  modifyIORef' env.balances (HashMap.insert a new)
  where
    db = env.reader.db

-- | Add an operation to the batch.
op :: Env -> BatchOp -> IO ()
op env o = modifyIORef' env.ops (o :)

-- | Write the batch, and start another.
flush :: Env -> IO ()
flush env = do
  count <- HashSet.size <$> readIORef env.moving
  unless (count == 0) $ do
    others <- reverse <$> readIORef env.ops
    bodies <- HashMap.toList <$> readIORef env.bodies
    balances <- HashMap.toList <$> readIORef env.balances
    writeBatch db $
      others
        <> [maybe (deleteOpCF (txCF db) (TxKey h)) (insertOpCF (txCF db) (TxKey h)) b | (h, b) <- bodies]
        <> [balanceOp a b | (a, b) <- balances]
    writeIORef env.ops []
    writeIORef env.bodies HashMap.empty
    writeIORef env.balances HashMap.empty
    writeIORef env.moving HashSet.empty
    modifyIORef' env.written (+ 1)
    readIORef env.total >>= env.options.afterBatch
  where
    db = env.reader.db
    -- an address that only the mempool used has no balance in the chain
    balanceOp a b
      | b == def = deleteOpCF (balanceCF db) (BalKey a)
      | otherwise = insertOpCF (balanceCF db) (BalKey a) b

-------------------------------------------------------------------------------
-- Moving a transaction
-------------------------------------------------------------------------------

-- | Whether a transaction is already in the mempool area.
isMoved :: Env -> TxHash -> IO Bool
isMoved env h = do
  now <- HashSet.member h <$> readIORef env.moving
  if now then return True else isJust <$> getCF db (mempoolCF db) (encode (TxKey h))
  where
    db = env.reader.db

-- | Move a mempool transaction, and first the mempool transactions that
-- spend its outputs: taking those out is what makes its outputs unspent
-- again, as they must be for its own turn.
moveTx :: Env -> TxHash -> IO ()
moveTx env h = do
  done <- isMoved env h
  unless done $ do
    td <- mempoolBody
    forM_ (nub' (map (.txid) (IntMap.elems td.spenders))) (moveTx env)
    -- read again: moving the spenders removed them from the body
    td' <- mempoolBody
    unless (IntMap.null td'.spenders) . stop $ "still has spenders after they were moved"
    time <- case td'.block of
      MemRef t -> return t
      BlockRef {} -> stop "is confirmed"
    takeOut env td'
    mapM_ (op env) (insertRecordOps db (mempoolRecords env.reader.net env.reader.ctx time td'))
    modifyIORef' env.moving (HashSet.insert h)
    modifyIORef' env.total (+ 1)
    count <- HashSet.size <$> readIORef env.moving
    when (count >= env.options.chunk) (flush env)
  where
    db = env.reader.db
    stop :: Text -> IO a
    stop why = throwIO . Inconsistent $ "Mempool transaction " <> txHashToHex h <> " " <> why
    mempoolBody =
      chainBody env h >>= \case
        Just td | not td.deleted, not (confirmed td.block) -> return td
        Just td | td.deleted -> stop "is marked as deleted"
        Just _ -> stop "is confirmed"
        Nothing -> stop "has no body"

-- | Take a mempool transaction that nothing spends out of the indices of
-- the chain, as deleting it does in version 20, except that its body is
-- removed and not kept as deleted: it is not deleted, it moves.
takeOut :: Env -> TxData -> IO ()
takeOut env td = do
  -- the entries of its addresses, and their counts of transactions
  forM_ touched $ \a -> do
    op env (deleteOpCF (addrTxCF db) (AddrTxKey a TxRef {block = td.block, txid = h}))
    change env a $ \b -> do
      n <- less "transactions" b.txs 1
      return (b :: BalVal) {txs = n}
  -- its outputs, all of them unspent by now
  forM_ (zip [0 ..] td.tx.outputs) $ \(i, o) -> do
    let p = OutPoint h i
    op env (deleteOpCF (unspentCF db) (UnspentKey p))
    forM_ (address o.script) $ \a -> do
      op env (deleteOpCF (addrOutCF db) (AddrOutKey a td.block p))
      change env a $ \b -> do
        u <- less "unconfirmed balance" b.unconfirmed o.value
        n <- less "unspent outputs" b.utxo 1
        rx <- less "amount received" b.received o.value
        return (b :: BalVal) {unconfirmed = u, utxo = n, received = rx}
  -- the outputs it spends are unspent again, as before it came
  forM_ [i.outpoint | i <- td.tx.inputs, i.outpoint /= nullOutPoint] $ \p -> do
    parent <-
      chainBody env p.hash >>= \case
        Just t | not t.deleted -> return t
        _ -> stop $ "spends an output of " <> txHashToHex p.hash <> ", which is not there"
    o <- case drop (fromIntegral p.index) parent.tx.outputs of
      o : _ -> return o
      [] -> stop $ "spends an output that " <> txHashToHex p.hash <> " does not have"
    modifyIORef' env.bodies . HashMap.insert p.hash . Just $
      (parent :: TxData) {spenders = IntMap.delete (fromIntegral p.index) parent.spenders}
    op env . insertOpCF (unspentCF db) (UnspentKey p) $
      UnspentVal {block = parent.block, value = o.value, script = o.script}
    forM_ (address o.script) $ \a -> do
      op env (insertOpCF (addrOutCF db) (AddrOutKey a parent.block p) (OutVal o.value o.script))
      change env a $ \b ->
        return $
          if confirmed parent.block
            then (b :: BalVal) {confirmed = b.confirmed + o.value, utxo = b.utxo + 1}
            else (b :: BalVal) {unconfirmed = b.unconfirmed + o.value, utxo = b.utxo + 1}
  -- its body leaves the chain's column family
  modifyIORef' env.bodies (HashMap.insert h Nothing)
  where
    db = env.reader.db
    h = txHash td.tx
    address = eitherToMaybe . scriptToAddressBS env.reader.net env.reader.ctx
    touched =
      nub' $
        mapMaybe (address . (.script)) (IntMap.elems td.prevs)
          <> mapMaybe (address . (.script)) td.tx.outputs
    stop :: Text -> IO a
    stop why = throwIO . Inconsistent $ "Mempool transaction " <> txHashToHex h <> " " <> why
    -- a balance never goes below zero in a database that is in order
    less :: Text -> Word64 -> Word64 -> Either Text Word64
    less what x y
      | y > x = Left $ "Taking out " <> txHashToHex h <> " would make the " <> what <> " of an address negative"
      | otherwise = Right (x - y)

-------------------------------------------------------------------------------
-- Checks
-------------------------------------------------------------------------------

-- | Before anything is written: a transaction that the mempool lists must
-- have the body of a mempool transaction.
preflight :: DatabaseReader -> TxHash -> IO ()
preflight r h =
  (retrieveCF r.db (txCF r.db) (TxKey h) :: IO (Maybe TxData)) >>= \case
    Just td | not td.deleted, not (confirmed td.block) -> return ()
    _ ->
      throwIO . Inconsistent $
        "The mempool lists " <> txHashToHex h <> ", which is not an unconfirmed transaction"

-- | After everything is moved: a mempool transaction is in the mempool
-- area and has left nothing in the chain's column families, and every
-- output it spends is there to be spent, in the mempool or unspent in the
-- chain.
verify :: DatabaseReader -> TxHash -> IO ()
verify r h = do
  td <-
    retrieveCF db (mempoolCF db) (TxKey h) >>= \case
      Just td -> return (td :: TxData)
      Nothing -> stop "is not in the mempool area"
  inChain <- isJust <$> getCF db (txCF db) (encode (TxKey h))
  when inChain $ stop "still has a body in the chain"
  forM_ (zip [0 ..] td.tx.outputs) $ \(i, _) -> do
    left <- isJust <$> getCF db (unspentCF db) (encode (UnspentKey (OutPoint h i)))
    when left $ stop "still has an unspent output in the chain"
  forM_ [i.outpoint | i <- td.tx.inputs, i.outpoint /= nullOutPoint] $ \p -> do
    inMempool <- isJust <$> getCF db (mempoolCF db) (encode (TxKey p.hash))
    unspent <- isJust <$> getCF db (unspentCF db) (encode (UnspentKey p))
    unless (inMempool || unspent) . void . stop $
      "spends an output of " <> txHashToHex p.hash <> " that is neither in the mempool nor unspent"
  where
    db = r.db
    stop :: Text -> IO a
    stop why = throwIO . Inconsistent $ "After the migration, mempool transaction " <> txHashToHex h <> " " <> why
