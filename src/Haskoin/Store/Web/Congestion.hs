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
-- keep up and raised while they do, as TCP does with its window.
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
-- = The control: additive increase, multiplicative decrease
--
-- Every half second the controller takes the 90th percentile of the
-- probes' lateness ('lateness') and moves the limit ('control'), as TCP and
-- LEDBAT move their window against a target delay:
--
-- * __Over the target__ (25 ms by default): the limit is cut by a quarter.
--   A cut takes effect as requests end, so there is no second cut while
--   more requests run than the limit allows: one congestion is answered
--   once, as TCP cuts its window once a round trip.
--
-- * __Under the target, and every slot used__: one more slot. The limit
--   grows only while it is what holds the requests back: a server that
--   has spare slots learns nothing from more of them, and an idle one must
--   not drift to the ceiling, to be found there by the next burst (TCP's
--   congestion window validation, RFC 7661).
--
-- The limit stays between a floor, the number of cores by default, and a
-- ceiling. A floor equal to the ceiling is a fixed cap.
--
-- The target cannot be much lower than GHC's timeslice: a thread that
-- wakes behind one other that computes waits up to 20 ms, which is normal
-- scheduling and not congestion. Measured with requests that compute, on
-- four cores (@bench/Admission.hs@), a target of 10 ms held the limit at
-- the floor, where the probes still read 17 ms, and answered 343 requests
-- a second; 25 ms held it at about six and answered 378; 50 ms held it at
-- about ten and answered 368, with a light request at 83 ms at the 99th
-- percentile, where 25 ms had it at 53.
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
    control,

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
  { -- | the 90th percentile of the probes' lateness, in microseconds
    delay :: !Int,
    -- | whether every slot was taken at some point, or a request waited
    -- or was refused
    saturated :: !Bool,
    -- | whether more requests run than the limit allows, which happens
    -- after a cut until enough of them end
    draining :: !Bool
  }
  deriving stock (Eq, Show)

-- | The limit after a step, from the limit before it. It is always between
-- the floor and the ceiling, the floor giving way to a ceiling below it.
control :: Control -> Observation -> Int -> Int
control c o n
  | o.delay > c.target && not o.draining = within (n * 3 `div` 4)
  | o.delay < c.target && o.saturated = within (n + 1)
  | otherwise = within n
  where
    within = max (min c.floor c.ceiling) . min c.ceiling

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
