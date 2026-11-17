{-# LANGUAGE OverloadedRecordDot #-}

-- | The limit on the requests answered at once: how many run, who waits, in
-- what order, and who is refused, and why. The clock is one that the tests
-- move by hand, so that CoDel and the limit of a wait are tested without
-- waiting, and without depending on how fast the machine is; the tests wait
-- only for the threads of the requests to get where they are going.
module Haskoin.Store.Web.AdmissionSpec (spec) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, cancel, forConcurrently, wait)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (ErrorCall (..), throwIO, try)
import Control.Monad (forM, replicateM)
import Data.IORef (atomicModifyIORef', modifyIORef', newIORef, readIORef)
import Haskoin.Store.Web.Admission
import Test.Hspec

spec :: Spec
spec = describe "the limit on the requests answered at once" $ do
  it "runs no more than its slots at once, and everything in the end" $ do
    (clock, _) <- handClock
    a <- newAdmissionWith clock (fixed 3 100)
    now <- newIORef (0 :: Int)
    most <- newIORef (0 :: Int)
    done <- forConcurrently [1 :: Int .. 40] $ \_ ->
      admitted a (const (return False)) $ do
        n <- atomicModifyIORef' now (\n -> (n + 1, n + 1))
        modifyIORef' most (max n)
        threadDelay 2000
        atomicModifyIORef' now (\m -> (m - 1, ()))
        return True
    done `shouldSatisfy` and
    readIORef most `shouldReturn` 3
    l <- load a
    (l.busy, l.waiting, l.refused) `shouldBe` (0, 0, Refusals 0 0 0)

  it "answers those that wait in the order that they came" $ do
    (clock, _) <- handClock
    a <- newAdmissionWith clock (fixed 1 10)
    gate <- newEmptyMVar
    order <- newIORef []
    first <- async (admitted a (const (return ())) (takeMVar gate))
    settle a 1 0
    rest <- forM [1 :: Int .. 5] $ \i -> do
      t <- async (admitted a (const (return ())) (modifyIORef' order (<> [i])))
      settle a 1 i
      return t
    putMVar gate ()
    mapM_ wait (first : rest)
    readIORef order `shouldReturn` [1 .. 5]

  it "refuses a request that finds the line full, without making it wait" $ do
    (clock, _) <- handClock
    a <- newAdmissionWith clock (fixed 1 2)
    gate <- newEmptyMVar
    held <- async (admitted a (return . Left) (Right "answered" <$ takeMVar gate))
    settle a 1 0
    queued <- replicateM 2 (async (admitted a (return . Left) (return (Right "answered"))))
    settle a 1 2
    admitted a (return . Left) (return (Right ("answered" :: String))) `shouldReturn` Left LineFull
    (.refused) <$> load a `shouldReturn` Refusals {full = 1, waited = 0, shed = 0}
    putMVar gate ()
    mapM wait (held : queued) `shouldReturn` replicate 3 (Right ("answered" :: String))

  it "refuses a request that has waited too long, and lets the next ones in" $ do
    (clock, move) <- handClock
    a <- newAdmissionWith clock (fixed 1 5) {patience = 1000000}
    gate <- newEmptyMVar
    held <- async (admitted a (return . Left) (Right () <$ takeMVar gate))
    settle a 1 0
    late <- async (admitted a (return . Left) (return (Right ())))
    settle a 1 1
    -- not long enough yet
    move 999 >> sweep a
    settle a 1 1
    move 1 >> sweep a
    wait late `shouldReturn` Left WaitedTooLong
    l <- load a
    (l.busy, l.waiting, l.refused) `shouldBe` (1, 0, Refusals 0 1 0)
    putMVar gate ()
    wait held `shouldReturn` Right ()
    admitted a (return . Left) (return (Right ())) `shouldReturn` Right ()

  it "gives the slot back when the request fails" $ do
    (clock, _) <- handClock
    a <- newAdmissionWith clock (fixed 1 0)
    r <- try (admitted a (const (return ())) (throwIO (ErrorCall "broken")))
    r `shouldBe` Left (ErrorCall "broken")
    admitted a (const (return False)) (return True) `shouldReturn` True

  it "takes a request that is stopped while it waits out of the line" $ do
    (clock, _) <- handClock
    a <- newAdmissionWith clock (fixed 1 5)
    gate <- newEmptyMVar
    held <- async (admitted a (const (return ())) (takeMVar gate))
    settle a 1 0
    stopped <- async (admitted a (const (return ())) (return ()))
    settle a 1 1
    behind <- async (admitted a (const (return False)) (return True))
    settle a 1 2
    cancel stopped
    settle a 1 1
    putMVar gate ()
    wait held
    -- the one behind it is not stuck behind a request that is gone
    wait behind `shouldReturn` True

  it "is no limit at all without a ceiling" $ do
    a <- newAdmission (fixed 0 0)
    rs <- forConcurrently [1 :: Int .. 20] $ \_ ->
      admitted a (const (return False)) (True <$ threadDelay 1000)
    rs `shouldSatisfy` and

  it "gives a slot that the limit gains to the first in line at once" $ do
    (clock, _) <- handClock
    a <- newAdmissionWith clock (fixed 3 10) {floor = 1}
    (.limit) <$> load a `shouldReturn` 1
    gate <- newEmptyMVar
    gates <- replicateM 3 newEmptyMVar
    held <- async (admitted a (const (return ())) (takeMVar gate))
    settle a 1 0
    waiting <- forM (zip [1 :: Int ..] gates) $ \(i, g) -> do
      t <- async (admitted a (const (return ())) (takeMVar g))
      settle a 1 i
      return t
    setLimit a 3
    settle a 3 1
    -- above the ceiling is the ceiling
    setLimit a 10
    (.limit) <$> load a `shouldReturn` 3
    mapM_ (`putMVar` ()) (gate : gates)
    mapM_ wait (held : waiting)

  it "lets the requests over a limit that was cut end before handing on a slot" $ do
    (clock, _) <- handClock
    a <- newAdmissionWith clock (fixed 3 10) {floor = 1}
    setLimit a 3
    gates <- replicateM 3 newEmptyMVar
    held <- forM (zip [1 ..] gates) $ \(i, g) -> do
      t <- async (admitted a (const (return ())) (takeMVar g))
      settle a i 0
      return t
    -- below the floor is the floor
    setLimit a 0
    (.limit) <$> load a `shouldReturn` 1
    behind <- async (admitted a (const (return False)) (return True))
    settle a 3 1
    let end (g, t) = putMVar g () >> wait t
    -- the slots of the first two come free, and the third one's goes on
    mapM_ (\(k, x) -> end x >> settle a (3 - k) 1) (zip [1 ..] (take 2 (zip gates held)))
    mapM_ end (drop 2 (zip gates held))
    wait behind `shouldReturn` True

  describe "CoDel" $ do
    it "refuses at once, and sheds those that waited, while the line stands" $ do
      (clock, move) <- handClock
      a <- newAdmissionWith clock (fixed 1 10)
      let ask g = async (admitted a (return . Left) (Right () <$ takeMVar g))
      gate1 <- newEmptyMVar
      gate2 <- newEmptyMVar
      gate3 <- newEmptyMVar
      -- the first interval: one request goes in at once, three wait
      first <- ask gate1
      settle a 1 0
      second <- ask gate2
      settle a 1 1
      thirds <- replicateM 2 (async (admitted a (return . Left) (return (Right ()))))
      settle a 1 3
      -- the first ends after the interval, which let a request in at once:
      -- the line worked, and the second has its slot after 110 ms
      move 110
      (putMVar gate1 () >> wait first) `shouldReturn` Right ()
      settle a 1 2
      (.overloaded) <$> load a `shouldReturn` False
      -- in the second interval the only one let in had waited 110 ms: the
      -- line stood, the two that waited 220 ms are refused, and the slot
      -- comes free
      move 110
      (putMVar gate2 () >> wait second) `shouldReturn` Right ()
      mapM wait thirds `shouldReturn` replicate 2 (Left Overloaded)
      l <- load a
      (l.busy, l.waiting, l.overloaded) `shouldBe` (0, 0, True)
      -- a free slot is taken; with none free, a request is refused at once
      third <- ask gate3
      settle a 1 0
      admitted a (return . Left) (return (Right ())) `shouldReturn` Left Overloaded
      -- the third went in at once: in the next interval the line works
      move 110
      (putMVar gate3 () >> wait third) `shouldReturn` Right ()
      (.overloaded) <$> load a `shouldReturn` False
      (.refused) <$> load a `shouldReturn` Refusals {full = 0, waited = 0, shed = 3}

    it "takes a line whose slots nobody leaves for one that stands" $ do
      (clock, move) <- handClock
      a <- newAdmissionWith clock (fixed 1 10)
      gate <- newEmptyMVar
      held <- async (admitted a (return . Left) (Right () <$ takeMVar gate))
      settle a 1 0
      waiting <- async (admitted a (return . Left) (return (Right ())))
      settle a 1 1
      -- the first interval let a request in at once
      move 150
      behind <- async (admitted a (return . Left) (return (Right ())))
      settle a 1 2
      (.overloaded) <$> load a `shouldReturn` False
      -- in the second nobody was let in, and the first in line had waited
      -- 260 ms by its end
      move 110
      admitted a (return . Left) (return (Right ())) `shouldReturn` Left Overloaded
      (.overloaded) <$> load a `shouldReturn` True
      putMVar gate ()
      wait held `shouldReturn` Right ()
      mapM wait [waiting, behind] `shouldReturn` replicate 2 (Left Overloaded)

    it "absorbs a burst that the slots clear within an interval" $ do
      (clock, move) <- handClock
      a <- newAdmissionWith clock (fixed 1 100)
      gates <- replicateM 20 newEmptyMVar
      ts <- forM (zip [0 :: Int ..] gates) $ \(i, g) -> do
        t <- async (admitted a (return . Left) (Right () <$ takeMVar g))
        settle a 1 i
        return t
      -- one ends every 4 ms: the last one waits 76 ms, and the line is
      -- empty again before the end of the interval
      mapM_ (\(g, t) -> move 4 >> putMVar g () >> wait t) (zip gates ts)
      (.refused) <$> load a `shouldReturn` Refusals 0 0 0

  it "lets a request step aside from its slot, and takes it back without the line" $ do
    (clock, _) <- handClock
    a <- newAdmissionWith clock (fixed 1 10)
    outside <- newEmptyMVar
    back <- newEmptyMVar
    gate <- newEmptyMVar
    stepping <- async $ admittedAside a (const (return False)) $ \out -> do
      aside out (putMVar outside () >> takeMVar back)
      takeMVar gate
      return True
    takeMVar outside
    -- its slot is free while it is aside
    settle a 0 0
    other <- async (admitted a (const (return False)) (True <$ takeMVar gate))
    settle a 1 0
    -- back in, past the limit
    putMVar back ()
    settle a 2 0
    putMVar gate ()
    putMVar gate ()
    mapM wait [stepping, other] `shouldReturn` [True, True]
    l <- load a
    (l.busy, l.waiting, l.refused) `shouldBe` (0, 0, Refusals 0 0 0)

-- | A fixed limit of so many slots, with room for so many in line, who may
-- wait ten seconds.
fixed :: Int -> Int -> Settings
fixed n room =
  Settings
    { ceiling = n,
      floor = n,
      target = 25000,
      room = room,
      patience = 10000000
    }

-- | A clock that moves only when told to, by so many milliseconds.
handClock :: IO (IO Int, Int -> IO ())
handClock = do
  v <- newIORef 0
  return (readIORef v, \ms -> atomicModifyIORef' v (\t -> (t + ms * 1000000, ())))

-- | Wait until so many requests run and so many wait.
settle :: Admission -> Int -> Int -> IO ()
settle a busy waiting = go (20000 :: Int)
  where
    go 0 = expectationFailure "the requests did not settle"
    go n = do
      l <- load a
      if l.busy == busy && l.waiting == waiting then return () else threadDelay 500 >> go (n - 1)
