{-# LANGUAGE OverloadedRecordDot #-}

module Haskoin.Store.CommonSpec (spec) where

import Conduit (runConduitPure, sinkList, yieldMany, (.|))
import Data.List (sortOn)
import Data.Ord (Down (..))
import Data.Word (Word32)
import Haskoin.Store.Common
  ( Limits (..),
    applyLimit,
    applyLimits,
    applyLimitsC,
    deOffset,
    mergeDescGroups,
  )
import Test.Hspec
import Test.QuickCheck

spec :: Spec
spec =
  describe "mergeDescGroups" $ do
    it "pages like sorting the first offset plus limit items of every stream" $
      property $ \(Streams ss) ->
        forAll (choose (0, 8)) $ \l ->
          forAll (choose (0, 8)) $ \o ->
            let limits = Limits l o Nothing
             in merged limits ss === reference limits ss
    it "stops reading once the page is full" $
      -- an undefined item after the first key of each stream would throw
      let ss = [[(9, 1), (8, 2)] <> undefined, [(9, 3), (7, 4)] <> undefined]
          limits = Limits 2 0 Nothing
       in merged limits ss `shouldBe` [(9, 3), (9, 1)]

-- | The paging of xpub unspent outputs before 'mergeDescGroups': the first
-- offset plus limit items of each stream, all sorted, then the page.
reference :: Limits -> [[(Int, Int)]] -> [(Int, Int)]
reference limits =
  applyLimits limits . sortOn Down . concatMap (applyLimit (deOffset limits).limit)

merged :: Limits -> [[(Int, Int)]] -> [(Int, Int)]
merged limits ss =
  runConduitPure $
    mergeDescGroups fst quota (map yieldMany ss)
      .| applyLimitsC limits
      .| sinkList
  where
    quota = case (deOffset limits).limit of
      0 -> Nothing
      q -> Just (q :: Word32)

-- | Streams of (key, item) whose keys never increase, with many equal keys
-- and the items of each key in any order, like the unspent outputs of the
-- addresses of an xpub: keyed by block, in key order within a block.
-- Items are unique across all streams, as outputs are.
newtype Streams = Streams [[(Int, Int)]]
  deriving (Show)

instance Arbitrary Streams where
  arbitrary = do
    n <- choose (0, 6)
    ss <- vectorOf n $ do
      keys <- sortOn Down <$> listOf (choose (0, 5))
      items <- shuffle [0 .. length keys - 1]
      return (zip keys items)
    -- number the items so that they are unique across streams
    return . Streams $
      zipWith (\i -> map (\(k, x) -> (k, x * 10 + i))) [0 ..] ss
