module Haskoin.Store.CacheSpec (spec) where

import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as C8
import Data.List (nub, sort)
import Haskoin (bch, btc, deriveXPubKey, makeXPrvKey, prepareContext)
import Haskoin.Store.Cache (blockRefScore, cachePrefix, fullReloadAfter, fullReloadEvery, scoreBlockRef)
import Haskoin.Store.Data (BlockRef (..), DeriveType (..), XPubSpec (..))
import Test.Hspec (Spec, describe, it, shouldBe, shouldSatisfy)
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck (Gen, choose, forAll, listOf, oneof)

spec :: Spec
spec = do
  describe "Score for block reference" $ do
    prop "sorts newest first" $
      forAll arbitraryBlockRefs $ \ts ->
        let scores = map blockRefScore (sort ts)
         in sort scores == reverse scores
    prop "respects identity" $
      forAll arbitraryBlockRef $ \b ->
        scoreBlockRef (blockRefScore b) == b
  describe "Keys" $
    it "start with a prefix of their own for each network" $ do
      cachePrefix btc `shouldBe` "hs2:btc:"
      cachePrefix bch `shouldBe` "hs2:bch:"
      -- no key of the old cache can start like a key of this one, nor the
      -- other way round: its keys are these names, or one of the one-letter
      -- prefixes followed by a serialized xpub or address
      let old = ["head", "max", "mempool", "lock", "xlock-", "b", "t", "u", "a", "i"]
          p = cachePrefix btc
      filter (\o -> o `BS.isPrefixOf` p || p `BS.isPrefixOf` o) old `shouldBe` []

  prepareContext $ \ctx ->
    describe "Scheduled full reloads" $
      it "come every five to ten minutes, at intervals spread over xpubs" $ do
        let seed i = C8.pack ("seed of the test xpub " <> show i)
            xpub i = XPubSpec (deriveXPubKey ctx (makeXPrvKey (seed i))) DeriveNormal
            after = map (fullReloadAfter . xpub) [1 .. 200 :: Int]
            half = fullReloadEvery `div` 2
        after `shouldSatisfy` all (\a -> a >= half && a < fullReloadEvery)
        -- each of the five minutes of the range has some of them
        sort (nub [(a - half) `div` 60000 | a <- after]) `shouldBe` [0 .. 4]

arbitraryBlockRefs :: Gen [BlockRef]
arbitraryBlockRefs = listOf arbitraryBlockRef

arbitraryBlockRef :: Gen BlockRef
arbitraryBlockRef = oneof [b, m]
  where
    b = do
      h <- choose (0, 0x07ffffff)
      p <- choose (0, 0x03ffffff)
      return BlockRef {height = h, position = p}
    m = do
      t <- choose (0, 0x001fffffffffffff)
      return MemRef {timestamp = t}
