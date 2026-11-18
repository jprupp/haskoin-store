{-# LANGUAGE OverloadedRecordDot #-}

module Haskoin.Store.Database.StallsSpec (spec) where

import Data.Maybe (isNothing)
import Haskoin (btcRegTest)
import Haskoin.Node (LogLevel (..))
import Haskoin.Store.Database.Reader (DatabaseReader (..))
import Haskoin.Store.Database.Stalls
import Haskoin.Store.Histories (openLayered)
import Haskoin.Store.Integration.Temp (withSystemTempDirectory)
import Test.Hspec

spec :: Spec
spec = do
  describe "the default number of background jobs" $ do
    it "is one a core between 2 and 16" $
      map defaultBackgroundJobs [1, 2, 4, 12, 16, 24] `shouldBe` [2, 2, 4, 12, 16, 16]
    it "is at least 2 even for a nonsense core count" $
      map defaultBackgroundJobs [-1, 0] `shouldBe` [2, 2]

  describe "the log of write stalls" $ do
    let at w = noStall {writes = w, pending = 70000000000, rate = if w == Delayed then 16777216 else 0}
        level before now = fst <$> stallNote before (at now)
    it "says nothing while the state stays the same" $
      mapM_ (\w -> stallNote w (at w) `shouldSatisfy` isNothing) [minBound .. maxBound]
    it "warns when writes get worse" $ do
      level Flowing Delayed `shouldBe` Just LevelWarn
      level Flowing Stopped `shouldBe` Just LevelWarn
      level Delayed Stopped `shouldBe` Just LevelWarn
    it "says so at info when they get better" $ do
      level Stopped Delayed `shouldBe` Just LevelInfo
      level Stopped Flowing `shouldBe` Just LevelInfo
      level Delayed Flowing `shouldBe` Just LevelInfo
    it "logs once for a run of polls in the same state" $ do
      let states = [Flowing, Flowing, Delayed, Delayed, Delayed, Stopped, Stopped, Delayed, Flowing, Flowing]
          notes = [l | (b, n) <- zip states (drop 1 states), Just (l, _) <- [stallNote b (at n)]]
      notes `shouldBe` [LevelWarn, LevelWarn, LevelInfo, LevelInfo]

  describe "reading the state from RocksDB" $
    it "finds writes flowing on a new database" $
      withSystemTempDirectory "haskoin-store-stalls-" $ \dir ->
        openLayered btcRegTest dir $ \db ->
          (.writes) <$> readStall db.db `shouldReturn` Flowing
