{-# LANGUAGE OverloadedRecordDot #-}

module Haskoin.Store.Database.StallsSpec (spec) where

import Data.Maybe (isNothing)
import Haskoin (btcRegTest)
import Haskoin.Node (LogLevel (..))
import Haskoin.Store.Data (DatabaseHealth (..), DatabaseWrites (..))
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
    let at w =
          noStall
            { writes = w,
              pendingCompaction = 70000000000,
              delayedWriteRate = if w == WritesDelayed then 16777216 else 0
            }
        level before now = fst <$> stallNote before (at now)
    it "says nothing while the state stays the same" $
      mapM_ (\w -> stallNote w (at w) `shouldSatisfy` isNothing) [minBound .. maxBound]
    it "warns when writes get worse" $ do
      level WritesFlowing WritesDelayed `shouldBe` Just LevelWarn
      level WritesFlowing WritesStopped `shouldBe` Just LevelWarn
      level WritesDelayed WritesStopped `shouldBe` Just LevelWarn
    it "says so at info when they get better" $ do
      level WritesStopped WritesDelayed `shouldBe` Just LevelInfo
      level WritesStopped WritesFlowing `shouldBe` Just LevelInfo
      level WritesDelayed WritesFlowing `shouldBe` Just LevelInfo
    it "logs once for a run of polls in the same state" $ do
      let states =
            [ WritesFlowing,
              WritesFlowing,
              WritesDelayed,
              WritesDelayed,
              WritesDelayed,
              WritesStopped,
              WritesStopped,
              WritesDelayed,
              WritesFlowing,
              WritesFlowing
            ]
          notes = [l | (b, n) <- zip states (drop 1 states), Just (l, _) <- [stallNote b (at n)]]
      notes `shouldBe` [LevelWarn, LevelWarn, LevelInfo, LevelInfo]

  describe "reading the state from RocksDB" $
    it "finds writes flowing on a new database, with no errors and nothing to compact" $
      withSystemTempDirectory "haskoin-store-stalls-" $ \dir ->
        openLayered btcRegTest dir $ \db -> do
          d <- readStall db.db
          (d.writes, d.errors, d.pendingCompaction, d.delayedWriteRate) `shouldBe` (WritesFlowing, 0, 0, 0)
