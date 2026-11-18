{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Haskoin.Store.Database.Stalls
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- RocksDB's background work, and the write stalls it causes when it falls
-- behind.
--
-- RocksDB writes new data to memtables, flushes them to table files, and
-- compacts the files into larger ones in background threads. When the
-- compactions fall behind, RocksDB protects reads by slowing every write,
-- and in the end by stopping them: past 64 GiB of compaction pending in a
-- column family it delays writes, past 256 GiB it stops them. It says so
-- only in its own @LOG@ file, so the store looked frozen while its own log
-- and @\/health@ said nothing. A fresh sync of Bitcoin Cash froze that way
-- at height 436,814, with 209 GB of compaction pending in the transactions
-- and RocksDB's default of two background jobs on a machine of 24 cores.
--
-- This module does two things about it:
--
-- * 'defaultBackgroundJobs' gives RocksDB a background job for each core,
--   within bounds, instead of RocksDB's two;
--
-- * 'watchStalls' reads RocksDB's properties every few seconds, logs when
--   writes start to be delayed or stopped and when they recover, keeps the
--   state for @\/health@, and sends it to StatsD.
--
-- The properties are polled rather than heard from an event listener of
-- RocksDB, which the C API of the binding does not have: a stall lasts
-- minutes to hours, so a few seconds late is soon enough, and each poll is
-- three reads of counters that RocksDB keeps anyway.
module Haskoin.Store.Database.Stalls
  ( -- * Background jobs
    defaultBackgroundJobs,

    -- * Write stalls
    Writes (..),
    Stall (..),
    noStall,
    readStall,
    stallNote,
    watchStalls,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (TVar, atomically, readTVarIO, writeTVar)
import Control.Monad (forM, forM_, forever)
import Data.ByteString.Char8 qualified as BS8
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Database.RocksDB (DB, Property (..), columnFamilies, getProperty, getPropertyCF)
import Haskoin.Node (LogLevel (..))
import System.Metrics.StatsD (Stats, newStatGauge, setGauge)
import Text.Read (readMaybe)

-- | Background jobs for RocksDB on a machine with this many cores: one a
-- core, at least 2 and at most 16.
--
-- RocksDB's own default is two jobs, one flush and one compaction, and a
-- single compaction thread does about 90 MiB a second. A store that syncs
-- writes faster than that, and its pending compactions grow until RocksDB
-- stops writes. One job a core lets the compactions of the different
-- column families and levels run side by side.
--
-- * __Why not more than 16:__ past that, the disk is the limit on any
--   machine the store runs on, and more threads make every read wait
--   longer for it.
-- * __Why one a core and not fewer:__ compaction threads wait for the disk
--   much of the time, and RocksDB runs only the compactions that are due,
--   so idle jobs cost nothing. A machine that serves traffic with all its
--   cores and has a slow disk can set fewer with @--background-jobs@.
-- * __Why at least 2:__ RocksDB splits the jobs into a quarter for flushes
--   and the rest for compactions, at least one of each.
defaultBackgroundJobs :: Int -> Int
defaultBackgroundJobs cores = max 2 (min 16 cores)

-- | Whether RocksDB lets writes through.
data Writes
  = -- | at full speed
    Flowing
  | -- | slowed down to let compactions catch up
    Delayed
  | -- | stopped until compactions or flushes catch up
    Stopped
  deriving (Eq, Ord, Show, Enum, Bounded)

-- | What RocksDB said at a poll.
data Stall = Stall
  { -- | whether writes go through
    writes :: !Writes,
    -- | bytes that RocksDB estimates it has to compact, summed over every
    -- column family (@rocksdb.estimate-pending-compaction-bytes@)
    pending :: !Integer,
    -- | bytes a second that writes are slowed to, 0 when they are not
    -- (@rocksdb.actual-delayed-write-rate@)
    rate :: !Integer
  }
  deriving (Eq, Show)

-- | The state before the first poll: writes flowing, nothing pending.
noStall :: Stall
noStall = Stall {writes = Flowing, pending = 0, rate = 0}

-- | Read the state of writes from RocksDB.
readStall :: DB -> IO Stall
readStall db = do
  pendings <- forM (Nothing : map Just (columnFamilies db)) $ \cf ->
    number <$> maybe (getProperty db pendingP) (\c -> getPropertyCF db c pendingP) cf
  stopped <- number <$> getProperty db (PropertyName "rocksdb.is-write-stopped")
  rate <- number <$> getProperty db (PropertyName "rocksdb.actual-delayed-write-rate")
  let writes
        | stopped /= 0 = Stopped
        | rate /= 0 = Delayed
        | otherwise = Flowing
  return Stall {writes, pending = sum pendings, rate}
  where
    pendingP = PropertyName "rocksdb.estimate-pending-compaction-bytes"
    number = fromMaybe 0 . (>>= readMaybe . BS8.unpack)

-- | What to log when the state of writes was the first and is now the
-- second: nothing while it stays the same, a warning when it gets worse,
-- and an info line when it gets better. A stall lasts long, and a line at
-- every poll would bury the rest of the log.
stallNote :: Writes -> Stall -> Maybe (LogLevel, Text)
stallNote before now
  | now.writes == before = Nothing
  | now.writes > before = Just (LevelWarn, worse)
  | otherwise = Just (LevelInfo, better)
  where
    worse = case now.writes of
      Stopped -> "RocksDB stopped writes" <> behind <> hint
      _ -> "RocksDB delays writes to " <> T.pack (show now.rate) <> " bytes/s" <> behind <> hint
    better = case now.writes of
      Flowing -> "RocksDB writes at full speed again" <> behind
      _ -> "RocksDB no longer stops writes, but delays them to " <> T.pack (show now.rate) <> " bytes/s" <> behind
    behind = ": " <> T.pack (show now.pending) <> " bytes of compaction pending"
    hint = "; more --background-jobs may help if the cores and disk have room"

-- | Poll RocksDB every five seconds, forever: run it in a thread of its own.
-- It keeps the latest state in the variable, logs the changes
-- ('stallNote'), and sends the gauges @db.pending_compaction_bytes@ and
-- @db.write_delayed@ (0 flowing, 1 delayed, 2 stopped) to StatsD.
watchStalls :: (LogLevel -> Text -> IO ()) -> Maybe Stats -> DB -> TVar Stall -> IO ()
watchStalls note stats db var = do
  gauges <- forM stats $ \s ->
    (,)
      <$> newStatGauge s "db.pending_compaction_bytes" 0
      <*> newStatGauge s "db.write_delayed" 0
  forever $ do
    before <- readTVarIO var
    now <- readStall db
    atomically (writeTVar var now)
    forM_ (stallNote before.writes now) (uncurry note)
    forM_ gauges $ \(pendingG, writesG) -> do
      setGauge pendingG (fromIntegral now.pending)
      setGauge writesG (fromEnum now.writes)
    threadDelay 5000000
