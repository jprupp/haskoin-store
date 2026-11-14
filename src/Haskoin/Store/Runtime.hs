{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Haskoin.Store.Runtime
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The garbage collector of GHC, as StatsD metrics.
--
-- Every collection of GHC stops every core, the small ones included, and a
-- busy store collects hundreds of times a second. While it is stopped, no
-- request moves: not even @\/health@, which only reads memory. So how long
-- the store spends stopped, and how the heap grows, say as much about how
-- it answers as the timings of its requests do. The statistics that GHC
-- keeps are read once a second and sent as metrics, under @rts.*@.
--
-- GHC keeps them only when the program runs with @+RTS -T@, which the
-- executable of the store turns on by default: it costs a few counters at
-- each collection. A program that uses the library without it gets a line
-- in the log that says so, and no metrics.
--
-- The statistics come as totals since the start, and are sent as what
-- changed in the second: counters, which StatsD sums and turns into rates.
-- The last collection's details would have given a pause time for each
-- collection; read once a second, they would have shown one collection in
-- hundreds, and not the longest. The mean pause of the second is sent
-- instead.
module Haskoin.Store.Runtime
  ( runtimeMetrics,
  )
where

import Control.Monad (forever, unless)
import GHC.Stats (GCDetails (..), RTSStats (..), getRTSStats, getRTSStatsEnabled)
import Haskoin.Node (Logger, logInfo)
import System.Metrics.StatsD
  ( Stats,
    StatCounter,
    StatGauge,
    incrementCounter,
    newStatCounter,
    newStatGauge,
    setGauge,
  )
import UnliftIO.Concurrent (threadDelay)

-- | The metrics of the collector.
data Metrics = Metrics
  { gcs :: !StatCounter,
    majorGcs :: !StatCounter,
    paused :: !StatCounter,
    pauseMean :: !StatGauge,
    pausePercent :: !StatGauge,
    gcCpu :: !StatCounter,
    syncs :: !StatCounter,
    marking :: !StatCounter,
    allocated :: !StatCounter,
    live :: !StatGauge,
    heap :: !StatGauge
  }

-- | Send the statistics of the collector to StatsD once a second, forever:
-- run it in a thread of its own. If the program does not keep them, it
-- says so in the log and returns.
runtimeMetrics :: Logger -> Stats -> IO ()
runtimeMetrics logger stats = do
  enabled <- getRTSStatsEnabled
  if not enabled
    then logInfo logger "No garbage collector metrics: run with +RTS -T for them"
    else do
      m <- metrics
      start <- getRTSStats
      let loop before = do
            threadDelay 1000000
            now <- getRTSStats
            report m before now
            loop now
      forever (loop start)
  where
    -- one sample a second each: sent whole, unless only aggregates are wanted
    counter k = newStatCounter stats ("rts." <> k) 1
    gauge k = newStatGauge stats ("rts." <> k) 0
    metrics =
      Metrics
        <$> counter "gcs"
        <*> counter "major_gcs"
        <*> counter "gc_pause_us"
        <*> gauge "gc_pause_mean_us"
        <*> gauge "gc_pause_percent"
        <*> counter "gc_cpu_us"
        <*> counter "nonmoving_sync_us"
        <*> counter "nonmoving_mark_us"
        <*> counter "allocated_bytes"
        <*> gauge "live_bytes"
        <*> gauge "heap_bytes"

-- | Send what changed between two readings of the statistics.
report :: Metrics -> RTSStats -> RTSStats -> IO ()
report m before now = do
  add m.gcs collections
  add m.majorGcs (delta (.major_gcs))
  add m.paused (micros (.gc_elapsed_ns))
  setGauge m.pauseMean (if collections > 0 then micros (.gc_elapsed_ns) `div` collections else 0)
  setGauge m.pausePercent (if wall > 0 then 100 * delta (.gc_elapsed_ns) `div` wall else 0)
  add m.gcCpu (micros (.gc_cpu_ns))
  add m.syncs (micros (.nonmoving_gc_sync_elapsed_ns))
  add m.marking (micros (.nonmoving_gc_elapsed_ns))
  add m.allocated (delta (.allocated_bytes))
  setGauge m.live (fromIntegral now.gc.gcdetails_live_bytes)
  setGauge m.heap (fromIntegral now.gc.gcdetails_mem_in_use_bytes)
  where
    delta :: (Integral a) => (RTSStats -> a) -> Int
    delta f = fromIntegral (f now) - fromIntegral (f before)
    micros :: (Integral a) => (RTSStats -> a) -> Int
    micros f = delta f `div` 1000
    collections = delta (.gcs)
    wall = delta (.elapsed_ns)
    -- a counter that did not move is not sent
    add c n = unless (n <= 0) (incrementCounter c n)
