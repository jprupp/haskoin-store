-- | The indexer of xpubs: its decisions, as a pure function, and its queue,
-- its waits and its lock, with jobs run one by one on a clock that the tests
-- move by hand, and once with its own threads on the clock of the system.
module Haskoin.Store.XPub.IndexerSpec (spec) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, cancel, forConcurrently, wait, withAsync)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar, takeMVar)
import Control.Monad (forM_, replicateM, void, when)
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef, writeIORef)
import Data.List (sort)
import GHC.Clock (getMonotonicTimeNSec)
import Haskoin.Store.XPub (Budget (..), noBudget)
import Haskoin.Store.XPub.Indexer
import Haskoin.Store.XPub.Lock (Lock (..), Taken (..), noLock)
import Test.Hspec

spec :: Spec
spec = do
  describe "the decision for a key that the cache does not have" $ do
    let s holder est room = Situation {expected = est, patience = 5, reach = 30, hasRoom = room, holder}
    it "takes nothing when another store holds the key, and retries from its lock" $ do
      decide (s (AnotherStore 7) 1 True) `shouldBe` RefuseNothing OtherStore 7
      decide (s (AnotherStore (-3)) 1 True) `shouldBe` RefuseNothing OtherStore 0
    it "joins the job of this store within the wait, whatever the room" $ do
      decide (s ThisStore 5 False) `shouldBe` Wait
      decide (s ThisStore 1 True) `shouldBe` Wait
    it "keeps the job of this store beyond the wait, and retries from the estimate" $ do
      decide (s ThisStore 6 True) `shouldBe` RefuseKeep 6
      decide (s ThisStore 60 False) `shouldBe` RefuseKeep 60
    it "takes nothing with a full queue, and retries at the horizon" $ do
      decide (s Nobody 1 False) `shouldBe` RefuseNothing QueueFull 30
      decide (s Nobody 60 False) `shouldBe` RefuseNothing QueueFull 30
    it "takes nothing beyond the horizon, and retries at the horizon" $
      decide (s Nobody 31 True) `shouldBe` RefuseNothing BeyondHorizon 30
    it "queues, locks and waits within the wait" $ do
      decide (s Nobody 5 True) `shouldBe` Wait
      decide (s Nobody 0 True) `shouldBe` Wait
    it "queues and locks, and retries from the estimate, within the horizon" $ do
      decide (s Nobody 6 True) `shouldBe` RefuseKeep 6
      decide (s Nobody 30 True) `shouldBe` RefuseKeep 30
    it "estimates a place in the queue from the median and the threads" $ do
      estimate 100 4 0 `shouldBe` 100
      estimate 100 4 4 `shouldBe` 200
      estimate 100 4 6 `shouldBe` 250
      estimate 100 1 3 `shouldBe` 400
    it "says Retry-After in whole seconds, at least one" $
      map retryAfter [0, 1, 999999, 1000000, 1000001, 4200000] `shouldBe` [1, 1, 1, 1, 2, 5]

  describe "the indexer" $ do
    it "runs one job for a key however many requests ask, and wakes them all when it is done" $ do
      t <- testbed settings
      answers <- withAsync (forConcurrently [1 .. 20] (\n -> request t.indexer 'x' [n])) $ \a -> do
        waiting t 20
        runNext t.indexer `shouldReturn` True
        runNext t.indexer `shouldReturn` False
        wait a
      -- each told that the cache has the key now, to read it there
      answers `shouldBe` replicate 20 Indexed
      -- the job with what each request brought
      map (fmap sort) <$> readIORef t.ran `shouldReturn` [('x', [1 .. 20])]
      readIORef t.locks `shouldReturn` ["take x", "release x"]
      s <- indexerStats t.indexer
      (s.counts.done, s.counts.answered, s.waiting) `shouldBe` (1, 20, 0)

    it "runs the jobs that requests wait for before upkeep, in the order they came" $ do
      t <- testbed settings
      upkeep t.indexer 'a' [1]
      upkeep t.indexer 'b' [2]
      upkeep t.indexer 'c' [3]
      r1 <- async (request t.indexer 'd' [4])
      waiting t 1
      -- a request for a key queued for upkeep moves it to the first part
      r2 <- async (request t.indexer 'c' [5])
      waiting t 2
      replicateM 5 (runNext t.indexer) `shouldReturn` [True, True, True, True, False]
      map fst <$> readIORef t.ran `shouldReturn` "dcab"
      lookup 'c' <$> readIORef t.ran `shouldReturn` Just [3, 5]
      mapM wait [r1, r2] `shouldReturn` [Indexed, Indexed]

    it "goes on with a job after the requests that wait for it are refused" $ do
      t <- testbed settings
      gate <- newEmptyMVar
      writeIORef t.during (readMVar gate)
      r <- async (request t.indexer 'x' [])
      waiting t 1
      job <- async (runNext t.indexer)
      waitFor (not . null <$> readIORef t.started)
      t.advance (settings.wait + 1)
      sweep t.indexer
      wait r >>= (`shouldSatisfy` refusedFor WaitedOut)
      putMVar gate ()
      wait job `shouldReturn` True
      readIORef t.ran `shouldReturn` [('x', [])]
      readIORef t.locks `shouldReturn` ["take x", "release x"]

    it "refuses a waiting request at a sweep once its time is up, by its own clock" $ do
      t <- testbed settings
      r <- async (request t.indexer 'x' [])
      waiting t 1
      -- nothing is refused before its time, however often the sweeper runs
      t.advance (settings.wait - 1)
      forM_ [1 .. 3 :: Int] $ \_ -> sweep t.indexer
      threadDelay 20000
      (.waiting) <$> indexerStats t.indexer `shouldReturn` 1
      t.advance 1
      sweep t.indexer
      a <- wait r
      a `shouldSatisfy` refusedFor WaitedOut
      -- the job has not run, so it is still where the request found it
      a `shouldBe` Refused WaitedOut settings.guess
      (.counts.refused.waitedOut) <$> indexerStats t.indexer `shouldReturn` 1

    it "refuses a waiting request in time with its own threads, which set no timer" $ do
      gate <- newEmptyMVar
      let runner = (plainRunner (\_ _ () -> readMVar gate) :: Runner Char ()) {ready = return True}
      withIndexer settings {wait = 300000, workers = 1} noLock runner $ \i -> do
        asked <- now
        a <- request i 'x' ()
        told <- now
        putMVar gate ()
        a `shouldSatisfy` refusedFor WaitedOut
        -- the sweeper runs ten times a second
        (told - asked) `shouldSatisfy` (\d -> d >= 300000 && d < 1500000)

    it "estimates from the median time of recent jobs and the place in the queue" $ do
      t <- testbed settings {workers = 2}
      -- each job takes as long as its payload says, on the hand clock
      writeIORef t.length' True
      forM_ (zip "abc" [100, 300, 200]) $ \(k, d) -> do
        void (prefetch t.indexer k [d])
        void (runNext t.indexer)
      (.median) <$> indexerStats t.indexer `shouldReturn` 200
      -- a request that does not wait is told the estimate: the median for
      -- the first in the queue, then a half more for each, two threads
      prefetch t.indexer 'd' [200] `shouldReturn` Refused TooLong 200
      prefetch t.indexer 'e' [200] `shouldReturn` Refused TooLong 300
      prefetch t.indexer 'f' [200] `shouldReturn` Refused TooLong 400
      -- and a request that joins one of them, its place
      prefetch t.indexer 'e' [] `shouldReturn` Refused TooLong 300
      -- the jobs of upkeep are not timed
      upkeep t.indexer 'g' [1000000]
      void (runNext t.indexer >> runNext t.indexer >> runNext t.indexer >> runNext t.indexer)
      (.median) <$> indexerStats t.indexer `shouldReturn` 200

    it "undoes the reservation of a request that is killed while it takes the lock" $ do
      t <- testbed settings
      entered <- newEmptyMVar
      gate <- newEmptyMVar
      writeIORef t.acquiring (putMVar entered () >> takeMVar gate)
      a <- async (request t.indexer 'x' [1])
      takeMVar entered
      cancel a
      writeIORef t.acquiring (return ())
      -- the lock, which it might have taken, is let go
      readIORef t.locks `shouldReturn` ["release x"]
      -- nothing of the request is left: upkeep for the key is queued and
      -- run, where a job left reserved would have taken it and never run
      upkeep t.indexer 'x' [2]
      runNext t.indexer `shouldReturn` True
      readIORef t.ran `shouldReturn` [('x', [2])]

    it "takes nothing for a key that another store holds, and gives the payload of upkeep back" $ do
      t <- testbed settings
      writeIORef t.other (Just 4000000)
      request t.indexer 'x' [1] `shouldReturn` Refused OtherStore 4000000
      upkeep t.indexer 'y' [2]
      -- the payloads of both come back, that of upkeep to be queued again
      readIORef t.blocked `shouldReturn` [('x', [1], 4000000), ('y', [2], 4000000)]
      runNext t.indexer `shouldReturn` False
      s <- indexerStats t.indexer
      (s.forRequests, s.forUpkeep, s.counts.refused.otherStore) `shouldBe` (0, 0, 1)

    it "joins upkeep to a queued job, and runs a key again when upkeep comes while it runs" $ do
      t <- testbed settings
      upkeep t.indexer 'x' [1]
      upkeep t.indexer 'x' [2]
      gate <- newEmptyMVar
      writeIORef t.during (readMVar gate)
      job <- async (runNext t.indexer)
      waitFor (not . null <$> readIORef t.started)
      upkeep t.indexer 'x' [3]
      upkeep t.indexer 'x' [4]
      putMVar gate ()
      wait job `shouldReturn` True
      writeIORef t.during (return ())
      runNext t.indexer `shouldReturn` True
      runNext t.indexer `shouldReturn` False
      readIORef t.ran `shouldReturn` [('x', [1, 2]), ('x', [3, 4])]
      -- the lock is kept from one run to the next
      readIORef t.locks `shouldReturn` ["take x", "release x"]

    it "answers a request while it lets go of a lock, and takes it anew for upkeep that came meanwhile" $ do
      t <- testbed settings
      gate <- newEmptyMVar
      writeIORef t.onRelease (readMVar gate)
      void (prefetch t.indexer 'x' [1])
      job <- async (runNext t.indexer)
      waitFor ((== ["take x", "release x"]) <$> readIORef t.locks)
      request t.indexer 'x' [2] `shouldReturn` Indexed
      upkeep t.indexer 'x' [3]
      writeIORef t.onRelease (return ())
      putMVar gate ()
      wait job `shouldReturn` True
      runNext t.indexer `shouldReturn` True
      readIORef t.ran `shouldReturn` [('x', [1]), ('x', [3])]
      readIORef t.locks `shouldReturn` ["take x", "release x", "take x", "release x"]

    it "takes nothing with a full queue, or beyond the horizon" $ do
      t <- testbed settings {room = 1}
      prefetch t.indexer 'a' [] `shouldReturn` Refused TooLong settings.guess
      request t.indexer 'b' [] `shouldReturn` Refused QueueFull settings.horizon
      u <- testbed settings {guess = settings.horizon + 1}
      request u.indexer 'b' [] `shouldReturn` Refused BeyondHorizon settings.horizon
      readIORef u.locks `shouldReturn` []

    it "counts prefetches apart from the refusals of requests" $ do
      t <- testbed settings {room = 1}
      -- a prefetch that queues a job, and one that finds it
      prefetch t.indexer 'a' [] `shouldReturn` Refused TooLong settings.guess
      prefetch t.indexer 'a' [] `shouldReturn` Refused TooLong settings.guess
      -- one that takes nothing, the queue being full
      prefetch t.indexer 'b' [] `shouldReturn` Refused QueueFull settings.horizon
      c <- (.counts) <$> indexerStats t.indexer
      (c.prefetched, c.declined, c.refused) `shouldBe` (2, 1, Refusals 0 0 0 0 0 0)
      -- a request is refused, and counted so
      request t.indexer 'b' [] `shouldReturn` Refused QueueFull settings.horizon
      d <- (.counts) <$> indexerStats t.indexer
      (d.prefetched, d.declined, d.refused.queueFull) `shouldBe` (2, 1, 1)

    it "takes no job while the store is not ready, and drops those it has" $ do
      t <- testbed settings
      upkeep t.indexer 'a' [1]
      writeIORef t.ready' False
      request t.indexer 'b' [2] `shouldReturn` Refused NotInSync settings.horizon
      upkeep t.indexer 'c' [3]
      runNext t.indexer `shouldReturn` True
      readIORef t.ran `shouldReturn` []
      sort <$> readIORef t.abandoned `shouldReturn` [('a', [1]), ('c', [3])]
      readIORef t.locks `shouldReturn` ["take a", "release a"]

    it "answers a request whose job failed, and goes on" $ do
      t <- testbed settings
      writeIORef t.during (fail "the database is gone")
      r <- async (request t.indexer 'x' [1])
      waiting t 1
      runNext t.indexer `shouldReturn` True
      wait r `shouldReturn` Failed
      readIORef t.failures `shouldReturn` [('x', [1])]
      readIORef t.locks `shouldReturn` ["take x", "release x"]

    it "tells a request that joins a running job what its size leaves of it, never more as it runs" $ do
      t <- testbed settings
      -- a job of 1,000 transactions that takes 100 ms: 100 microseconds each
      writeIORef t.length' True
      writeIORef t.size (Just 1000)
      void (prefetch t.indexer 'a' [100000])
      void (runNext t.indexer)
      -- a job of 50,000, which should take five seconds
      writeIORef t.length' False
      writeIORef t.size (Just 50000)
      gate <- newEmptyMVar
      writeIORef t.during (readMVar gate)
      void (prefetch t.indexer 'b' [])
      job <- async (runNext t.indexer)
      waitFor (elem 'b' <$> readIORef t.started)
      -- two seconds in, three are left
      t.advance 2000000
      prefetch t.indexer 'b' [] `shouldReturn` Refused TooLong 3000000
      -- six seconds in, it has run longer than its size says: the median
      t.advance 4000000
      prefetch t.indexer 'b' [] `shouldReturn` Refused TooLong 100000
      putMVar gate ()
      wait job `shouldReturn` True

    it "puts the estimate of a job in its lock when it starts, and when it tells its size" $ do
      t <- testbed settings {workers = 1}
      writeIORef t.size (Just 5000)
      upkeep t.indexer 'a' [1]
      void (runNext t.indexer)
      -- the median at the start, then 5,000 transactions at the first
      -- guess of the rate, 100 microseconds each
      readIORef t.renewed `shouldReturn` [[('a', 100000)], [('a', 500000)]]

    it "joins and waits for the job of this store when the door decided so, whatever its estimate" $ do
      t <- testbed settings {workers = 1}
      -- a job that tells it has 50,000 transactions: five seconds
      writeIORef t.size (Just 50000)
      upkeep t.indexer 'a' [5000000]
      gate <- newEmptyMVar
      writeIORef t.during (readMVar gate)
      job <- async (runNext t.indexer)
      waitFor ((== "a") <$> readIORef t.started)
      -- this store's own estimate is beyond the wait of a second
      request t.indexer 'a' [] `shouldReturn` Refused TooLong 5000000
      -- but the door read the estimate in the lock, and decided to wait
      waiter <- async (requestWithin t.indexer 1000000 'a' [])
      waitFor ((== 1) . (.waiting) <$> indexerStats t.indexer)
      putMVar gate ()
      void (wait job)
      wait waiter `shouldReturn` Indexed

    it "renews the locks of the jobs that it holds, with their estimates" $ do
      t <- testbed settings {workers = 1}
      upkeep t.indexer 'a' [1]
      void (prefetch t.indexer 'b' [2])
      renewHeld t.indexer
      readIORef t.renewed `shouldReturn` [[('a', 200000), ('b', 100000)]]
      sort <$> readIORef t.kept `shouldReturn` [('a', [1]), ('b', [2])]
  where
    settings = defaultSettings {workers = 1, wait = 1000000, horizon = 30000000, room = 10, guess = 100000}
    refusedFor why = \case
      Refused w _ -> w == why
      _ -> False
    now = (`div` 1000) . fromIntegral <$> getMonotonicTimeNSec :: IO Int

-- | An indexer of characters with lists of numbers as payloads, on a hand
-- clock, with no threads of its own, and what it did.
data Testbed = Testbed
  { indexer :: Indexer Char [Int],
    advance :: Int -> IO (),
    -- | the jobs run, in order, with their payloads
    ran :: IORef [(Char, [Int])],
    -- | jobs started
    started :: IORef [Char],
    -- | what a job does before it ends
    during :: IORef (IO ()),
    -- | whether a job moves the clock on by the sum of its payload
    length' :: IORef Bool,
    -- | the size that a job tells, if any
    size :: IORef (Maybe Int),
    -- | what the lock did
    locks :: IORef [String],
    -- | what letting go of a lock does after it is noted
    onRelease :: IORef (IO ()),
    -- | what taking a lock does before it answers
    acquiring :: IORef (IO ()),
    -- | when another store holds every lock, until when
    other :: IORef (Maybe Int),
    ready' :: IORef Bool,
    abandoned :: IORef [(Char, [Int])],
    blocked :: IORef [(Char, [Int], Int)],
    failures :: IORef [(Char, [Int])],
    renewed :: IORef [[(Char, Int)]],
    kept :: IORef [(Char, [Int])]
  }

testbed :: Settings -> IO Testbed
testbed s = do
  clock <- newIORef 0
  ran <- newIORef []
  started <- newIORef []
  during <- newIORef (return ())
  length' <- newIORef False
  size <- newIORef Nothing
  locks <- newIORef []
  onRelease <- newIORef (return ())
  acquiring <- newIORef (return ())
  other <- newIORef Nothing
  ready' <- newIORef True
  abandoned <- newIORef []
  blocked <- newIORef []
  failures <- newIORef []
  renewed <- newIORef []
  kept <- newIORef []
  let note r x = atomicModifyIORef' r (\xs -> (xs <> [x], ()))
      lock =
        Lock
          { acquire = \k _ ->
              (readIORef acquiring >>= id) >> readIORef other >>= \case
                Just t -> return (Held t)
                Nothing -> note locks ("take " <> [k]) >> return Took,
            renew = \ks -> note renewed (sort ks) >> return 0,
            release = \k -> note locks ("release " <> [k]) >> (readIORef onRelease >>= id),
            holder = \_ -> return Nothing,
            every = 10000000
          }
      runner =
        Runner
          { index = \b k p -> do
              readIORef size >>= mapM_ b.sized
              note started k
              readIORef during >>= id
              moves <- readIORef length'
              when moves $ modifyIORef' clock (+ sum p)
              note ran (k, p),
            budget = \_ -> return noBudget,
            ready = readIORef ready',
            abandon = \k p -> note abandoned (k, p),
            blocked = \k p t -> note blocked (k, p, t),
            failed = \k p _ -> note failures (k, p),
            renew = \jobs -> forM_ jobs (note kept)
          }
  i <- newIndexerWith (readIORef clock) s lock runner
  return
    Testbed
      { indexer = i,
        advance = \d -> modifyIORef' clock (+ d),
        ran,
        started,
        during,
        length',
        size,
        locks,
        onRelease,
        acquiring,
        other,
        ready',
        abandoned,
        blocked,
        failures,
        renewed,
        kept
      }

-- | Wait until so many requests wait for a job.
waiting :: Testbed -> Int -> IO ()
waiting t n = waitFor ((== n) . (.waiting) <$> indexerStats t.indexer)

-- | Wait for something, for up to five seconds.
waitFor :: IO Bool -> IO ()
waitFor check = go (5000 :: Int)
  where
    go k =
      check >>= \case
        True -> return ()
        False
          | k <= 0 -> expectationFailure "timed out"
          | otherwise -> threadDelay 1000 >> go (k - 1)
