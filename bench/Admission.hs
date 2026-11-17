-- | Admission control under load: no limit, a fixed one, and the one that
-- adapts, against requests that compute and requests that wait for a disk.
--
-- > haskoin-store-bench admission [MIX LIMIT [SECONDS [CLIENTS]]]
--
-- With no arguments it runs every MIX with every LIMIT, for 60 seconds each.
--
-- Each of CLIENTS (128) threads asks for long requests through
-- "Haskoin.Store.Web.Admission", one after the other, as the connections of
-- a load generator would; a client that is refused waits 100 ms before it
-- asks again, as if it had tried another server. The requests run in the
-- thread of their client, as a request of the web server runs in the thread
-- of its connection. A MIX is what they ask for:
--
-- * @cpu@: 10 ms of computing that allocates, with a map of 10,000 entries
--   held meanwhile, as a response being made holds its data.
--
-- * @disk@: twenty reads from a disk that takes 1 ms a read and does 32 at
--   once, 32,000 reads a second, like the disk of a production store; each
--   is followed by 50 µs of computing. A read waits for its turn at the
--   disk in Haskell, and then in a safe foreign call (@usleep@), which gives
--   the capability back, as a read of RocksDB does.
--
-- * @switch@: @disk@ for the first half of the run, then @cpu@.
--
-- A LIMIT is @off@, @fixed@, four slots a core as the cap of 1.16.0 had,
-- or @fixed=N@, or @adaptive@, between one and sixteen a core, or
-- @adaptive=FLOOR-CEILING[/TARGET]@. The target of the delay is 25 ms
-- unless given, in milliseconds; 64 requests may wait, for a second at
-- most.
--
-- Meanwhile a light request, which computes for 50 µs, is due every 10 ms,
-- on each capability in turn, and one of the threads that wait for them
-- there takes it, as the thread of a connection takes a health check. Its
-- latency is counted from when it was due, so a capability that runs late
-- counts for as many light requests as the others. Every capability has
-- threads of its own for them: GHC moves a thread to another capability
-- only when that one is idle, and sixteen threads that compute, started
-- from one, were found thirteen on one capability and one on each of the
-- others, so where a thread is started says much about how long it waits.
--
-- For the run, for each half of it, and for every five seconds, it prints
-- the long requests answered a second, their latency at the 50th and 99th
-- percentiles, counted from when they asked for a slot, the refusals a
-- second by why (line full, waited too long, CoDel), the latency of the
-- light requests, the limit, and the 90th percentile of the scheduler's
-- delay.
--
-- Run it as the production machine runs the store, on four cores:
--
-- > taskset -c 4-7 haskoin-store-bench admission +RTS -N4 -A64m -T
module Admission (admissionBench) where

import Control.Concurrent
  ( forkIOWithUnmask,
    forkOnWithUnmask,
    getNumCapabilities,
    killThread,
    newEmptyMVar,
    putMVar,
    takeMVar,
    threadDelay,
  )
import Control.Concurrent.Async (withAsync, withAsyncOn)
import Control.Concurrent.QSem (QSem, newQSem, signalQSem, waitQSem)
import Control.Concurrent.STM (atomically, check, newTVarIO, readTVar, writeTVar)
import Control.Exception (bracket, bracket_, evaluate)
import Control.Monad (forM_, forever, replicateM, replicateM_, void, when)
import Data.Bits ((.&.))
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.IntMap.Strict qualified as IntMap
import Data.List (intercalate)
import Foreign.C.Types (CInt (..), CUInt (..))
import GHC.Clock (getMonotonicTimeNSec)
import Haskoin.Store.Web.Admission
import Haskoin.Store.Web.Congestion (lateness, percentile, snooze, withProbes)
import System.Mem (performGC)
import Text.Printf (printf)

-- | Sleep so many microseconds in a safe foreign call, which lets go of the
-- capability while it sleeps.
foreign import ccall safe "usleep" c_usleep :: CUInt -> IO CInt

-- | What the long requests do.
data Mix = Cpu | Disk | Switch
  deriving (Eq, Show)

-- | How many long requests are answered at once.
data Limit = Off | Fixed !Int | Adaptive !Int !Int !Int

instance Show Limit where
  show Off = "off"
  show (Fixed n) = "fixed " <> show n
  show (Adaptive lo hi t) = "adaptive " <> show lo <> "-" <> show hi <> ", target " <> show t <> " ms"

-- | Everything a run records, newest first, in nanoseconds.
data Records = Records
  { -- | when a long request ended, and how long it took
    answered :: !(IORef [(Int, Int)]),
    -- | when a long request was refused, and why
    refused :: !(IORef [(Int, Refusal)]),
    -- | when a light request was due, and how long it took from then
    lights :: !(IORef [(Int, Int)]),
    -- | when a step of the controller was, its limit and the delay
    steps :: !(IORef [(Int, Int, Int)])
  }

admissionBench :: [String] -> IO ()
admissionBench args = do
  caps <- getNumCapabilities
  perMs <- calibrate
  printf "%d capabilities, %d steps of computing a millisecond\n" caps perMs
  let limits = [Off, Fixed (4 * caps), Adaptive caps (16 * caps) 25]
  case args of
    [] -> forM_ [Cpu, Disk, Switch] $ \m -> forM_ limits $ \l -> run perMs m l 60 128 >> performGC
    m : l : rest -> do
      let (secs, clients) = case rest of
            [] -> (60, 128)
            [s] -> (read s, 128)
            s : c : _ -> (read s, read c)
      run perMs (mixOf m) (limitOf caps l) secs clients
    _ -> error "usage: haskoin-store-bench admission [MIX LIMIT [SECONDS [CLIENTS]]]"
  where
    mixOf = \case
      "cpu" -> Cpu
      "disk" -> Disk
      "switch" -> Switch
      m -> error ("no such mix: " <> m)
    limitOf caps = \case
      "off" -> Off
      "fixed" -> Fixed (4 * caps)
      "adaptive" -> Adaptive caps (16 * caps) 25
      l
        | ("fixed=", n) <- splitAt 6 l -> Fixed (read n)
        | ("adaptive=", r) <- splitAt 9 l,
          (lo, '-' : r') <- break (== '-') r,
          (hi, t) <- break (== '/') r' ->
            Adaptive (read lo) (read hi) (if null t then 25 else read (drop 1 t))
        | otherwise -> error ("no such limit: " <> l)

-- | One run of so many seconds with so many clients.
run :: Int -> Mix -> Limit -> Int -> Int -> IO ()
run perMs mix limit secs clients = do
  a <- newAdmission settings
  disk <- newQSem 32
  rs <- Records <$> newIORef [] <*> newIORef [] <*> newIORef [] <*> newIORef []
  caps <- getNumCapabilities
  start <- nanos
  let half = start + secs * 500_000_000
      end = start + secs * 1_000_000_000
      kind t = if mix == Switch then (if t < half then Disk else Cpu) else mix
      client = forever $ do
        t0 <- nanos
        r <- admitted a (return . Left) (Right <$> request perMs disk (kind t0) t0)
        t1 <- nanos
        case r of
          Right () -> push rs.answered (t1, t1 - t0)
          -- a client is elsewhere, and its wait costs the server nothing:
          -- not even a timer of GHC's timer manager
          Left why -> push rs.refused (t1, why) >> void (c_usleep 100_000)
      -- on each capability in turn, a light request due every 10 ms, which
      -- one of the threads waiting there takes, as a health check is
      -- answered by the thread of its connection when it hears a request
      light i = do
        -- each thread has a place of its own to be told: a channel that
        -- they all read would wake them one after the other
        dues <- replicateM 128 newEmptyMVar
        let answer due = forever $ do
              t0 <- takeMVar due
              spin (perMs `div` 20)
              t1 <- nanos
              push rs.lights (t0, t1 - t0)
            tick k = do
              let t0 = start + (k * caps + i) * 10_000_000
              t <- nanos
              -- asleep in the system, as GHC's manager of input and output
              -- waits for the network, and with no timer of GHC
              when (t0 > t) (snooze ((t0 - t) `div` 1000))
              putMVar (dues !! (k `mod` length dues)) t0
              when (t0 < end) (tick (k + 1))
            -- unmasked, as 'bracket' masks what it starts, and a thread that
            -- computes with exceptions masked cannot be stopped
            begin d = forkOnWithUnmask i (\unmask -> unmask (answer d))
        bracket (mapM begin dues) (mapM_ killThread) (const (tick 0))
      lights = foldr (\i k -> withAsyncOn i (light i) (const k))
      record r = nanos >>= \t -> push rs.steps (t, r.limit, r.delay)
      -- with no limit there is no controller, but the delay is still worth
      -- seeing
      watch = case limit of
        Off -> withProbes $ \ps -> forever $ do
          threadDelay 500_000
          d <- lateness ps
          t <- nanos
          push rs.steps (t, 0, d)
        _ -> regulate a record
  -- every client is there before any of them asks: under load, a thread
  -- that GHC has yet to run may wait for seconds, and so would the clients
  -- that it was to start
  go <- newTVarIO False
  withAsync watch $ \_ ->
    flip lights [0 .. caps - 1] $
      let ready = atomically (readTVar go >>= check)
          begin = forkIOWithUnmask (\unmask -> unmask (ready >> client))
       in bracket (replicateM clients begin) (mapM_ killThread) $ \_ -> do
            atomically (writeTVar go True)
            threadDelay (secs * 1_000_000)
  printf "\n== %s, %s, %d s, %d clients\n" (show mix) (show limit) secs clients
  putStrLn header
  report rs "run" start end
  when (mix == Switch) $ do
    report rs "disk half" start half
    report rs "cpu half" half end
  putStrLn "every five seconds:"
  forM_ [0, 5 .. secs - 5] $ \s ->
    report rs (printf "%3d s" s) (start + s * 1_000_000_000) (start + (s + 5) * 1_000_000_000)
  where
    settings = case limit of
      Off -> base {ceiling = 0}
      Fixed n -> base {ceiling = n, floor = n}
      Adaptive lo hi t -> base {ceiling = hi, floor = lo, target = t * 1000}
    base = Settings {ceiling = 0, floor = 1, target = 25_000, room = 64, patience = 1_000_000}

-- | What a long request does.
request :: Int -> QSem -> Mix -> Int -> IO ()
request perMs disk mix salt = case mix of
  Disk -> replicateM_ 20 $ do
    bracket_ (waitQSem disk) (signalQSem disk) (void (c_usleep 1000))
    spin (perMs `div` 20)
  _ -> do
    -- the salt keeps GHC from making the map once for every request
    held <- evaluate (IntMap.fromList [(i, i + salt) | i <- [1 .. 10_000]])
    spin (10 * perMs)
    void (evaluate (IntMap.foldl' (+) 0 held))

-- | Compute for so many steps, allocating a number every sixteen, about
-- half a gigabyte a second: a loop that does not allocate never lets GHC
-- take its core, and one that allocates at every step spends its time in
-- the collector.
spin :: Int -> IO ()
spin steps = do
  r <- newIORef (0 :: Int)
  let go :: Int -> Int -> IO ()
      go 0 !acc = writeIORef r acc
      go n !acc
        | n .&. 15 == 0 = writeIORef r acc >> go (n - 1) (acc + n)
        | otherwise = go (n - 1) (acc + n)
  go steps 0

-- | Steps of 'spin' in a millisecond, the best of three, on a quiet process.
calibrate :: IO Int
calibrate = do
  spin 100_000_000
  ts <- mapM (const timed) [1 :: Int, 2, 3]
  return (round (200_000_000 / minimum ts :: Double))
  where
    timed = do
      t0 <- nanos
      spin 200_000_000
      t1 <- nanos
      return (fromIntegral (t1 - t0) / 1e6 :: Double)

-- | The columns of 'report'.
header :: String
header =
  intercalate
    "  "
    [ "window     ",
      "long/s",
      "p50 ms",
      "p99 ms",
      "refused/s full waited codel",
      "light p50 p95 p99 ms",
      "limit",
      "delay ms"
    ]

-- | Print a line of what happened from one time to another.
report :: Records -> String -> Int -> Int -> IO ()
report rs name from to = do
  done <- within fst <$> readIORef rs.answered
  no <- within fst <$> readIORef rs.refused
  ls <- within fst <$> readIORef rs.lights
  ss <- within (\(t, _, _) -> t) <$> readIORef rs.steps
  let per n = fromIntegral n / secs :: Double
      ms = (/ 1e6) . fromIntegral :: Int -> Double
      lat p = ms (percentile p (map snd done))
      light p = ms (percentile p (map snd ls))
      count why = length (filter ((== why) . snd) no)
      mean xs = if null xs then 0 else fromIntegral (sum xs) / fromIntegral (length xs) :: Double
  printf
    "%-11s  %6.0f  %6.1f  %6.1f  %9.1f %4.1f %6.1f %5.1f  %9.1f %3.0f %4.0f     %5.1f  %8.1f\n"
    name
    (per (length done))
    (lat 50)
    (lat 99)
    (per (length no))
    (per (count LineFull))
    (per (count WaitedTooLong))
    (per (count Overloaded))
    (light 50)
    (light 95)
    (light 99)
    (mean [l | (_, l, _) <- ss])
    (mean [d | (_, _, d) <- ss] / 1000)
  where
    secs = fromIntegral (to - from) / 1e9
    within :: (x -> Int) -> [x] -> [x]
    within f = filter (\x -> f x >= from && f x < to)

-- | Add to a record.
push :: IORef [a] -> a -> IO ()
push r x = atomicModifyIORef' r (\xs -> (x : xs, ()))

-- | Nanoseconds of the monotonic clock.
nanos :: IO Int
nanos = fromIntegral <$> getMonotonicTimeNSec
