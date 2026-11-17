-- |
-- Module      : Haskoin.Store.Web.Congestion
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- How late GHC's scheduler runs a thread that is ready, measured by probes
-- on each capability ('withProbes', 'delays'), and the controller that
-- follows it ('control'): a limit on how many long requests run at once
-- ("Haskoin.Store.Web.Admission"), cut by a quarter when the cores do not
-- keep up and raised while they have room, as TCP CUBIC does with its
-- window.
-- @docs/congestion-control.md@ explains the signal and the controller for a
-- reader new to congestion control.
--
-- No fixed number is right. A request that waits for RocksDB on the disk
-- gives its core back while it waits, in a safe foreign call, and the disk
-- of a production store answered 31,000 reads a second with 32 of them in
-- flight: such requests want many slots. A request that computes wants
-- about one a core, and more of them only make every thread, the health
-- check's included, wait longer for its turn. A cap of four a core was
-- wrong for both, and nobody retunes it as the requests change.
--
-- = The signal: how late a thread that the network woke runs
--
-- A probe on each capability waits for a bell, rung every 10 ms from
-- outside GHC, the way a connection waits for its socket, and notes how
-- late it ran ('withProbes'). GHC's manager of input and output wakes it,
-- and it goes to the back of its capability's queue of threads that are
-- ready, each of which runs for up to GHC's timeslice of 20 ms; when the
-- manager itself is in that queue rather than waiting in epoll, its turn
-- comes first. So the lateness is the length of that queue in time, as a
-- request that the network woke finds it. It rises only when the
-- capabilities have more to do than they can, whatever the requests cost;
-- a thread that waits for the disk in a safe foreign call is not in the
-- queue and does not raise it; and it is what a health check, or any short
-- request, waits before it runs.
--
-- = Without the pauses of the collector
--
-- A probe that is due while GHC's collector has stopped every thread wakes
-- only when the collection ends, late by as long as it lasted. Counted as
-- it is, that lateness says that the cores are behind when they are not:
-- the collection stops the idle cores as much as the busy ones, and fewer
-- slots would not make it shorter. So each wake is also counted
-- without the time that the collector took between the probe's sleep and
-- its wake, as GHC reports it ('getRTSStats', with @+RTS -T@), and
-- 'delays' gives both percentiles, with the time the collector took in
-- all. The limit follows the one without.
-- A collection that falls in the sleep and ends before the probe is due is
-- taken out too, which counts the wake as a little earlier than it was.
-- Without the statistics of GHC both are the same.
--
-- The latency of the requests themselves is not the signal, which rules out
-- controllers such as TCP Vegas, Netflix's Gradient2 or BBR, which compare
-- the latency of a request with the least seen. The long routes differ by a
-- factor of a thousand in cost, with the route and the parameters, so a
-- slower request says nothing about the server: it may just be a bigger
-- one.
--
-- = The control: cut once a round trip, grow as TCP CUBIC does
--
-- Every half second the controller takes the 90th percentile of the
-- probes' lateness without the collector's pauses, and the use of the
-- cores, smoothed over about two seconds ('cpuTime'), and moves the limit
-- ('control'):
--
-- * __Over the target__ (25 ms by default): the limit is cut by a quarter,
--   at most once a round trip, the time that a request holds its slot,
--   smoothed. A cut takes effect as requests end, and a second cut before
--   it has would answer one congestion twice, as TCP cuts its window once
--   a round trip.
--
-- * __Under the target, every slot used, and the cores used under 90%__:
--   the limit grows along the cubic curve of TCP CUBIC, concave back
--   toward the level that it was last cut from, flat near it, and convex
--   past it. It finds again at once the level that worked, and searches
--   carefully around it. The time on the curve runs only while the limit
--   may grow, so a limit held back for a while does not leap when it may
--   grow again.
--
-- The limit stays between a floor, two a core by default, and a ceiling.
-- A floor equal to the ceiling is a fixed cap.
--
-- The use of the cores is what keeps the limit from costing throughput, and
-- from growing when it is no use. An earlier controller added one slot a
-- step while the probes were under the target, and cut whenever they were
-- over it and the last cut had taken effect. On two cores of a production
-- store it answered a third fewer requests than no limit to sixteen
-- clients that asked one after the other: each cut left cores idle, and
-- one slot a step took seconds to fill them again. A server whose cores
-- are used at 90% gains nothing from more slots, and one whose cores have
-- room loses requests it could have answered while the limit climbs back.
--
-- The target cannot be much lower than GHC's timeslice: a thread that
-- wakes behind one other that computes waits up to 20 ms, which is normal
-- scheduling and not congestion. Measured with requests that compute, on
-- four cores (@bench/Admission.hs@), a target of 10 ms held the limit at
-- the floor, where the probes still read 17 ms; 50 ms let a light request
-- wait 83 ms at the 99th percentile, where 25 ms had it at 53.
--
-- = Not done here
--
-- * The requests for one thing by its key (@Short@, in
--   "Haskoin.Store.Web.Router") are not limited, and are answered before
--   the long ones. The same control could hold them too, with a limit of
--   their own.
--
-- * Linux reports how long tasks waited for the disk (pressure stall
--   information, @\/proc\/pressure\/io@). It would tell a disk that is the
--   limit from one with room, which the scheduler's delay cannot see, and
--   could be a second signal.
module Haskoin.Store.Web.Congestion
  ( -- * The controller
    Control (..),
    Observation (..),
    Window (..),
    window,
    control,
    cpuTime,

    -- * The probes
    Probe,
    withProbes,
    Delays (..),
    delays,
    lateness,
    weighted,
    percentile,
    snooze,
  )
where

import Control.Concurrent (getNumCapabilities, threadWaitRead)
import Control.Concurrent.Async (withAsyncOn)
import Control.Monad (forM, forever, replicateM, void, when)
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.Int (Int64)
import Data.List (sort, sortOn)
import Data.Maybe (listToMaybe)
import Foreign.C.Types (CInt (..), CUInt (..))
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Array (allocaArray, peekArray)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Conc (closeFdWith)
import GHC.Stats (RTSStats (..), getRTSStats, getRTSStatsEnabled)
import Haskoin.Store.Exception (bracket)
import System.CPUTime (getCPUTime)
import System.Posix.Types (Fd (..))

-- | The settings of the controller.
data Control = Control
  { -- | fewest slots
    floor :: !Int,
    -- | most slots
    ceiling :: !Int,
    -- | microseconds of lateness above which the limit comes down
    target :: !Int
  }
  deriving stock (Eq, Show)

-- | What the controller saw in a step.
data Observation = Observation
  { -- | the 90th percentile of the probes' lateness, without the pauses of
    -- the collector, in microseconds
    delay :: !Int,
    -- | the use of the cores, smoothed, in hundredths of all of them
    used :: !Int,
    -- | whether every slot was taken at some point, or a request waited
    -- or was refused
    saturated :: !Bool,
    -- | microseconds that a request holds its slot, smoothed: the round
    -- trip, once a cut has taken effect
    roundTrip :: !Int,
    -- | microseconds since the step before
    elapsed :: !Int
  }
  deriving stock (Eq, Show)

-- | The state of the controller between steps.
data Window = Window
  { -- | the limit: the level, rounded down, between the floor and the
    -- ceiling
    limit :: !Int,
    -- | the number of slots before rounding
    level :: !Double,
    -- | the level that the limit was last cut from, where the curve is
    -- flat
    peak :: !Double,
    -- | seconds on the curve, which run only while the limit may grow
    climbed :: !Double,
    -- | seconds from the start of the curve to its flat point
    knee :: !Double,
    -- | microseconds since the last cut
    sinceCut :: !Int
  }
  deriving stock (Eq, Show)

-- | The window at the start: at the floor, on the convex part of the curve.
window :: Control -> Window
window c = Window {limit = n, level = x, peak = x, climbed = 0, knee = 0, sinceCut = maxBound `div` 2}
  where
    n = lowest c
    x = fromIntegral n

-- | The floor, giving way to a ceiling below it.
lowest :: Control -> Int
lowest c = max 1 (min c.floor c.ceiling)

-- | What is kept of the level at a cut: three quarters.
backoff :: Double
backoff = 0.75

-- | How fast the curve rises away from its flat point, in slots a cubed
-- second, as in TCP CUBIC: back to the level of a cut from 16 slots in two
-- seconds, from 64 in three and a half.
cubic :: Double
cubic = 0.4

-- | Hundredths of the cores in use above which the limit does not grow.
busyCores :: Int
busyCores = 90

-- | The window after a step. Its limit is always between the floor and the
-- ceiling, the floor giving way to a ceiling below it.
control :: Control -> Observation -> Window -> Window
control c o w
  | o.delay > c.target && since >= o.roundTrip && w.limit > lowest c = cut
  | o.delay < c.target && o.saturated && o.used < busyCores = grow
  | otherwise = w {sinceCut = since}
  where
    since = min (maxBound `div` 2) (w.sinceCut + max 0 o.elapsed)
    -- a cut below the peak of the last one is a sign that the room is
    -- shrinking: the new peak is set lower still, as CUBIC does, so that
    -- the limit leaves room sooner
    cut =
      let top = if w.level < w.peak then w.level * (1 + backoff) / 2 else w.level
          low = max (fromIntegral (lowest c)) (w.level * backoff)
       in settle w {level = low, peak = top, climbed = 0, knee = cbrt ((top - low) / cubic), sinceCut = 0}
    grow =
      let t = w.climbed + fromIntegral (max 0 o.elapsed) / 1e6
          curve = cubic * (t - w.knee) ^ (3 :: Int) + w.peak
       in settle w {level = max w.level (min (fromIntegral c.ceiling) curve), climbed = t, sinceCut = since}
    settle x = x {limit = max (lowest c) (min c.ceiling (floor x.level))}
    cbrt x = if x <= 0 then 0 else x ** (1 / 3)

-- | Nanoseconds of the cores that the program has used so far, in all of
-- its threads, those of the foreign calls included.
cpuTime :: IO Int
cpuTime = (`div` 1000) . fromIntegral <$> getCPUTime

-- | The probe of a capability ('withProbes').
data Probe = Probe
  { wakes :: !(IORef Wakes),
    -- | nanoseconds that GHC's collector has stopped the program, so far
    collector :: !(IO Int)
  }

-- | What a probe noted since it was last asked.
data Wakes = Wakes
  { -- | when the probe last ran, in nanoseconds of the monotonic clock
    slept :: !Int,
    -- | the collector's time when the probe last ran, in nanoseconds
    paused :: !Int,
    -- | the time up to which the samples count, in nanoseconds: the last
    -- wake, or the last time that a late sleep was counted so far
    counted :: !Int,
    -- | each wake: microseconds late, microseconds late without the
    -- collector's pauses, and microseconds that it counts for
    samples :: ![(Int, Int, Int)]
  }

-- | Nanoseconds that GHC's collector has stopped the program so far, if
-- GHC keeps its statistics (@+RTS -T@); zero always otherwise.
collectorTime :: IO (IO Int)
collectorTime =
  getRTSStatsEnabled >>= \case
    True -> return (fromIntegral . (.gc_elapsed_ns) <$> getRTSStats)
    False -> return (return 0)

-- | Microseconds that a probe sleeps: a hundred samples a second from each
-- capability, and a thread that wakes that often costs nothing to notice.
nap :: Int
nap = 10000

-- | Nanoseconds of the monotonic clock.
nanos :: IO Int
nanos = fromIntegral <$> getMonotonicTimeNSec

-- | Sleep so many microseconds in the system, in a safe foreign call, which
-- gives the capability back while it sleeps.
foreign import ccall safe "usleep" c_usleep :: CUInt -> IO CInt

-- | Sleep so many microseconds, up to a second, with no timer of GHC's
-- timer manager ('withProbes' says why), and run again as soon as the
-- thread that runs on the capability ends its timeslice.
snooze :: Int -> IO ()
snooze us = void (c_usleep (fromIntegral (max 0 (min 999999 us))))

-- | The bell of the probes, in C (@cbits/haskoin_store_probe.c@).
data Bell

foreign import ccall unsafe "haskoin_probes_start"
  c_probesStart :: CInt -> CInt -> CInt -> Ptr CInt -> IO (Ptr Bell)

foreign import ccall safe "haskoin_probes_stop"
  c_probesStop :: Ptr Bell -> IO ()

foreign import ccall unsafe "haskoin_probe_take"
  c_probeTake :: Ptr Bell -> CInt -> Ptr Int64 -> Ptr Int64 -> IO CInt

foreign import ccall unsafe "close"
  c_close :: CInt -> IO CInt

-- | Run a probe on each capability while the action runs: a thread of the
-- capability that waits for a bell, rung every 10 ms by a thread of the
-- system, and notes how long after the bell it ran.
--
-- The probe waits as a connection waits for its socket: the bell writes
-- to an eventfd of the probe's own, which the probe waits for in
-- 'threadWaitRead'. GHC's manager of input and output for the capability
-- sees it in epoll and wakes the probe, which goes to the back of the
-- capability's queue of threads that are ready. So the wait counts what
-- a request that the network woke waits before it runs: the manager's
-- turn, when it is not already waiting in epoll but in the queue itself,
-- and the probe's own turn after it.
--
-- An earlier probe slept in a safe foreign call and woke a second thread
-- through an 'Control.Concurrent.MVar'. GHC lets a thread that returns
-- from a foreign call run first, so it counted the second thread's turn
-- and not the manager's: at overload it read what a connection waits once
-- the manager has woken it (40 ms at the 90th percentile, against 38 for
-- the connections), where this probe reads 86 ms with the manager's turn.
-- At moderate load the two read alike. The bell is outside GHC, as the
-- sleep was: every timer of a program goes through one thread of GHC, its
-- timer manager, which a busy capability can hold back, and a probe that
-- used one would measure that.
withProbes :: ([Probe] -> IO a) -> IO a
withProbes act = do
  n <- getNumCapabilities
  counted <- getRTSStatsEnabled
  t <- nanos
  gc <- collectorTime
  g <- gc
  ps <- replicateM n ((`Probe` gc) <$> newIORef (Wakes t g t []))
  bracket (start n counted) stop $ \(bell, fds) ->
    let listening [] = act ps
        listening ((i, p, fd) : rest) = withAsyncOn i (listen bell i p fd) $ \_ -> listening rest
     in listening (zip3 [0 ..] ps fds)
  where
    start n counted = allocaArray n $ \arr -> do
      bell <- c_probesStart (fromIntegral n) (fromIntegral nap) (if counted then 1 else 0) arr
      when (bell == nullPtr) $ ioError (userError "Haskoin.Store.Web.Congestion: no eventfd for the probes")
      fds <- map Fd <$> peekArray n arr
      return (bell, fds)
    -- the probes have stopped waiting by then; the eventfds are closed
    -- through GHC's manager, which may still have them
    stop (bell, fds) = do
      c_probesStop bell
      mapM_ (closeFdWith (\(Fd fd) -> void (c_close fd))) fds
    listen bell i p fd = alloca $ \wp -> alloca $ \gp -> forever $ do
      threadWaitRead fd
      t <- nanos
      g <- p.collector
      had <- c_probeTake bell (fromIntegral (i :: Int)) wp gp
      when (had /= 0) $ do
        due <- fromIntegral <$> peek wp
        before <- fromIntegral <$> peek gp
        atomicModifyIORef' p.wakes $ \w ->
          let sample =
                ( (t - due) `div` 1000,
                  (t - due - (g - before)) `div` 1000,
                  (t - w.counted) `div` 1000
                )
           in (Wakes t g t (sample : w.samples), ())

-- | Microseconds late, at the given time, of a probe that last ran at the
-- other; zero or less is not late.
overdue :: Int -> Int -> Int
overdue slept t = (t - slept) `div` 1000 - nap

-- | What the probes saw since they were last asked ('delays').
data Delays = Delays
  { -- | the 90th percentile of how late they woke, in microseconds
    raw :: !Int,
    -- | the same with the pauses of GHC's collector taken out of each wake:
    -- how late the scheduler alone made them
    scheduling :: !Int,
    -- | nanoseconds that the collector has stopped the program so far, as
    -- GHC counts them; zero when it keeps no statistics
    collected :: !Int
  }
  deriving stock (Eq, Show)

-- | The 90th percentile of how late the probes woke since the last call,
-- each wake counting for the time since the one before it ('delays'), with
-- the pauses of the collector counted as lateness.
lateness :: [Probe] -> IO Int
lateness ps = (.raw) <$> delays ps

-- | The 90th percentiles of how late the probes woke since the last call,
-- with the pauses of GHC's collector and without them, each wake counting
-- for the time since the one before it, and the collector's time so far.
--
-- Counting wakes would leave out the capabilities that matter most: a
-- probe on a capability with a long queue wakes seldom, and one on an idle
-- capability a hundred times a second. Weighed by time, every capability
-- counts alike, and the percentile is of the delay that a thread woken at
-- any moment, on any capability, would have. A probe that is still asleep
-- past its time counts with how late it is so far, for the time since it
-- was last counted: on capabilities so busy that no probe woke, the delay
-- would otherwise have no samples.
delays :: [Probe] -> IO Delays
delays ps = do
  t <- nanos
  g <- maybe (return 0) (.collector) (listToMaybe ps)
  xs <- forM ps $ \p ->
    atomicModifyIORef' p.wakes $ \w ->
      let late = overdue w.slept t
          sample = (late, late - (g - w.paused) `div` 1000, (t - w.counted) `div` 1000)
       in if late > 0
            then (w {counted = t, samples = []}, sample : w.samples)
            else (w {samples = []}, w.samples)
  let all' = concat xs
  return
    Delays
      { raw = weighted 90 [(x, w) | (x, _, w) <- all'],
        scheduling = weighted 90 [(x, w) | (_, x, w) <- all'],
        collected = g
      }

-- | The value that so many hundredths of the weight is at or under, each
-- value counting for its weight; zero for no weight.
weighted :: Int -> [(Int, Int)] -> Int
weighted p xs = go 0 (sortOn fst xs)
  where
    total = sum (map snd xs)
    go _ [] = 0
    go below ((x, w) : rest)
      | 100 * (below + w) >= p * total = x
      | otherwise = go (below + w) rest

-- | The value that so many hundredths of the values are at or under, by the
-- nearest rank; zero for no values.
percentile :: Int -> [Int] -> Int
percentile _ [] = 0
percentile p xs = sort xs !! max 0 (min (n - 1) (rank - 1))
  where
    n = length xs
    rank = (p * n + 99) `div` 100
