module Haskoin.Store.BlockStoreSpec (spec) where

import Haskoin.Store.BlockStore (needsMoreBlocks)
import Test.Hspec

spec :: Spec
spec =
  describe "needsMoreBlocks" $ do
    it "asks for more blocks once fewer than a quarter of the window remain" $ do
      -- 64 / 4 = 16
      needsMoreBlocks 64 16 `shouldBe` False
      needsMoreBlocks 64 15 `shouldBe` True
    it "asks for more blocks once none remain, even with a window below four" $ do
      needsMoreBlocks 1 1 `shouldBe` False
      needsMoreBlocks 1 0 `shouldBe` True
      needsMoreBlocks 3 0 `shouldBe` True
