{-# LANGUAGE OverloadedRecordDot #-}

-- | The controller of the limit on long requests, alone, and the probes of
-- the scheduler's delay that it follows.
module Haskoin.Store.Web.CongestionSpec (spec) where

import Control.Concurrent (getNumCapabilities, threadDelay)
import Control.Concurrent.Async (withAsyncOn)
import Control.Exception (evaluate)
import Control.Monad (forever, unless, when)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.IntMap.Strict qualified as IntMap
import GHC.Stats (getRTSStatsEnabled)
import Haskoin.Store.Web.Congestion
import System.Mem (performMajorGC)
import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

spec :: Spec
spec = do
  describe "the controller of the limit" $ do
    prop "keeps the limit between the floor and the ceiling" $
      forAll settings $ \c -> forAll (start c) $ \w ->
        forAll (listOf (observation c)) $ \os ->
          all (\x -> c.floor <= x.limit && x.limit <= c.ceiling) (steps c w os)

    prop "comes down to the floor while the delay stays over the target" $
      forAll settings $ \c -> forAll (start c) $ \w ->
        forAll (vectorOf 64 (congested c)) $ \os ->
          let ms = map (.limit) (steps c w os)
           in last ms == c.floor && and (zipWith (>=) ms (drop 1 ms))

    prop "cuts at most once a round trip" $
      forAll settings $ \c -> forAll (start c) $ \w -> forAll (chooseInt (0, 5000000)) $ \rtt ->
        forAll (listOf (congested c)) $ \os ->
          let os' = [o {roundTrip = rtt} | o <- os]
              ws = drop 1 (steps c w os')
              -- microseconds since the last cut, before each step
              since = scanl (\t (o, x) -> if x.sinceCut == 0 then 0 else t + o.elapsed) w.sinceCut (zip os' ws)
           in and [t + o.elapsed >= rtt | (t, o, x) <- zip3 since os' ws, x.sinceCut == 0]

    prop "never grows while it is not saturated, or while the cores are busy" $
      forAll settings $ \c -> forAll (start c) $ \w ->
        forAll (listOf (observation c)) $ \os -> forAll (chooseInt (90, 200)) $ \busy ->
          let idle = map (.limit) (steps c w [o {saturated = False} | o <- os])
              full = map (.limit) (steps c w [o {used = busy} | o <- os])
           in and (zipWith (>=) idle (drop 1 idle)) && and (zipWith (>=) full (drop 1 full))

    prop "never cuts while the cores have room" $
      forAll settings $ \c -> forAll (start c) $ \w ->
        forAll (listOf (congested c)) $ \os -> forAll (chooseInt (0, 89)) $ \idle ->
          let ms = map (.limit) (steps c w [o {used = idle} | o <- os])
           in and (zipWith (<=) ms (drop 1 ms))

    prop "is free to rise only while the cores have room, under the ceiling" $
      forAll settings $ \c -> forAll (start c) $ \w -> forAll (observation c) $ \o ->
        rising c o w == (o.used < 90 && w.limit < c.ceiling)

    prop "grows back to the level of the last cut, then past it to the ceiling" $
      forAll settings $ \c -> forAll (chooseInt (c.floor, c.ceiling)) $ \n ->
        let w = (window c) {limit = n, level = fromIntegral n, peak = fromIntegral n}
            cut = control c (Observation (c.target + 1) 95 True 0 500000) w
            climb = steps c cut (replicate 400 (calm c))
            -- seconds on the curve at each step
            times = [0.5 * fromIntegral i :: Double | i <- [0 :: Int ..]]
            early = [x.limit | (x, t) <- zip climb times, t < cut.knee]
            past = [x.limit | (x, t) <- zip climb times, t >= cut.knee + 0.5]
         in all (<= n) early
              && all (>= n) (take 1 past)
              && last (map (.limit) climb) == c.ceiling
              && and (zipWith (<=) (map (.limit) climb) (drop 1 (map (.limit) climb)))

    prop "never moves a fixed limit" $
      forAll settings $ \c0 -> forAll (listOf (observation c0)) $ \os ->
        let c = c0 {ceiling = c0.floor}
         in all ((== c.floor) . (.limit)) (steps c (window c) os)

  describe "the percentile" $ do
    it "is the value at its rank" $ do
      percentile 90 [1 .. 100] `shouldBe` 90
      percentile 90 [100, 99 .. 1] `shouldBe` 90
      percentile 90 [7] `shouldBe` 7
      percentile 50 [1, 2, 3, 4] `shouldBe` 2
    it "is zero for no values" $
      percentile 90 [] `shouldBe` 0
    it "counts each value for its weight, when weighted" $ do
      weighted 90 [(x, 1) | x <- [1 .. 100]] `shouldBe` 90
      -- thirty wakes on time on an idle capability, and one of a busy
      -- capability, 500 µs late, which covers more time than all of them
      weighted 90 ((500, 510) : replicate 30 (1, 10)) `shouldBe` 500
      percentile 90 (500 : replicate 30 1) `shouldBe` 1
      weighted 90 [] `shouldBe` 0

  describe "the probes of the scheduler's delay" $ do
    -- the target is 25 ms; the thresholds leave a factor of two either way
    it "stay well under the target on idle capabilities" $
      withProbes $ \ps -> do
        _ <- lateness ps
        threadDelay 1000000
        d <- lateness ps
        d `shouldSatisfy` (< 12500)

    it "rise well over the target with four busy threads on each capability" $ do
      caps <- getNumCapabilities
      fourOn [0 .. caps - 1] $ withProbes $ \ps -> do
        _ <- lateness ps
        threadDelay 1000000
        d <- lateness ps
        d `shouldSatisfy` (> 50000)

    it "rise well over the target with four busy threads on one capability" $
      fourOn [0] $
        withProbes $ \ps -> do
          _ <- lateness ps
          threadDelay 1000000
          d <- lateness ps
          d `shouldSatisfy` (> 50000)

    it "take the pauses of the collector out of the scheduler's delay" $ do
      counted <- getRTSStatsEnabled
      unless counted $ pendingWith "GHC keeps no statistics without +RTS -T"
      -- a map of a million entries, copied by every major collection,
      -- which nothing else runs between
      held <- evaluate (IntMap.fromList [(i, i) | i <- [1 .. 1_000_000 :: Int]])
      withProbes $ \ps -> do
        _ <- delays ps
        before <- delays ps
        let collect n = when (n > (0 :: Int)) (performMajorGC >> collect (n - 1))
        collect 20
        d <- delays ps
        IntMap.size held `shouldBe` 1_000_000
        d.collected `shouldSatisfy` (> before.collected)
        d.raw `shouldSatisfy` (> 5000)
        d.scheduling `shouldSatisfy` (< d.raw `div` 2)
  where
    steps c = scanl (flip (control c))

-- | Run an action with four threads that compute on each of these
-- capabilities.
fourOn :: [Int] -> IO a -> IO a
fourOn caps act = foldr (\i k -> busy i (busy i (busy i (busy i k)))) act caps
  where
    busy i k = withAsyncOn i spin (const k)

-- | A thread that computes, and allocates, so that GHC can take its core
-- from it at the end of its timeslice.
spin :: IO ()
spin = do
  r <- newIORef (0 :: Int)
  forever $ readIORef r >>= \x -> writeIORef r $! x + 1

-- | Settings of the controller.
settings :: Gen Control
settings = do
  lo <- chooseInt (1, 64)
  hi <- chooseInt (lo, 1024)
  target <- chooseInt (1, 100000)
  return Control {floor = lo, ceiling = hi, target = target}

-- | A window that some steps led to from the start.
start :: Control -> Gen Window
start c = foldl (flip (control c)) (window c) <$> listOf (observation c)

-- | Anything that a step may see.
observation :: Control -> Gen Observation
observation c =
  Observation
    <$> chooseInt (0, 2 * c.target)
    <*> chooseInt (0, 150)
    <*> arbitrary
    <*> chooseInt (0, 2000000)
    <*> chooseInt (0, 1000000)

-- | A step with a delay over the target and the cores busy, a round trip
-- after the last cut.
congested :: Control -> Gen Observation
congested c =
  Observation
    <$> chooseInt (c.target + 1, 4 * c.target)
    <*> chooseInt (90, 150)
    <*> arbitrary
    <*> pure 0
    <*> chooseInt (1, 1000000)

-- | A step of half a second with a delay under the target, every slot
-- taken, and cores with room.
calm :: Control -> Observation
calm c = Observation (c.target - 1) 50 True 0 500000
