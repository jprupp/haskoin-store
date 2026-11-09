{-# OPTIONS_GHC -Wno-orphans #-}

module Haskoin.Store.WebCommonSpec (spec) where

import Control.Monad (forM_)
import Data.ByteString qualified as B
import Data.Maybe (fromJust)
import Data.Proxy (Proxy (..))
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Word (Word64)
import Haskoin
import Haskoin.Store.Data (DeriveType (..))
import Haskoin.Store.Gen (arbitraryDeriveType)
import Haskoin.Store.WebCommon
import Haskoin.Util.Arbitrary
import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

spec :: Spec
spec = prepareContext $ \ctx -> do
  describe "Parameters" $ forM_ allNets $ \net -> describe net.name $ do
    roundTrip net ctx (arbitraryAddress net)
    roundTrip net ctx (listOf (arbitraryAddress net))
    roundTrip net ctx (arbitrary :: Gen StartParam)
    roundTrip net ctx (arbitrarySizedNatural :: Gen OffsetParam)
    roundTrip net ctx (arbitrarySizedNatural :: Gen LimitParam)
    roundTrip net ctx (arbitrarySizedNatural :: Gen HeightParam)
    roundTrip net ctx (arbitrary :: Gen HeightsParam)
    roundTrip net ctx (arbitrarySizedNatural :: Gen TimeParam)
    roundTrip net ctx (arbitraryXPubKey ctx)
    roundTrip net ctx (arbitraryDeriveType net)
    roundTrip net ctx (NoCache <$> arbitrary)
    roundTrip net ctx (NoTx <$> arbitrary)
    roundTrip net ctx arbitraryBlockHash
    roundTrip net ctx (listOf arbitraryBlockHash)
    roundTrip net ctx arbitraryTxHash
    roundTrip net ctx (listOf arbitraryTxHash)

  describe "Start of a page" $ do
    it "is a transaction hash, a height up to 1230768000, or a later time" $ do
      parse ["1230768000"] `shouldBe` Just (StartParamHeight 1230768000)
      parse ["1230768001"] `shouldBe` Just (StartParamTime 1230768001)
      parse [txHashToHex z] `shouldBe` Just (StartParamHash z.get)
    it "refuses anything else" $
      parse ["yesterday"] `shouldBe` Nothing

  describe "Derivation" $
    it "is only normal on networks without SegWit" $ do
      encodeParam bch ctx DeriveP2WPKH `shouldBe` Nothing
      encodeParam btc ctx DeriveP2WPKH `shouldBe` Just ["segwit"]

  describe "Paths" $ do
    let addr = fromJust (textToAddr btc "1111111111111111111114oLvT2")
        path :: (ApiResource a b) => a -> String
        path = cs . queryPath btc ctx
    it "of a block" $ do
      path (GetBlockBest (NoTx False)) `shouldBe` "/block/best"
      path (GetBlockHeight (HeightParam 5) (NoTx False)) `shouldBe` "/block/height/5"
    it "of a transaction" $
      path (GetTx z) `shouldBe` "/transaction/" <> cs (txHashToHex z)
    it "of an address" $
      path (GetAddrBalance addr) `shouldBe` "/address/1111111111111111111114oLvT2/balance"
    prop "of an xpub" $
      forAll (arbitraryXPubKey ctx) $ \x ->
        path (GetXPubBalances x DeriveNormal (NoCache False))
          `shouldBe` "/xpub/" <> cs (xPubExport btc ctx x) <> "/balances"
  where
    z = TxHash (sha256 B.empty)
    parse = parseParam btc undefined :: [Text] -> Maybe StartParam

-- | A parameter comes back from its encoding in a URL.
roundTrip :: forall a. (Eq a, Show a, Param a) => Network -> Ctx -> Gen a -> Spec
roundTrip net ctx g =
  prop (cs (proxyLabel (Proxy :: Proxy a))) . forAll g $ \p ->
    (encodeParam net ctx p >>= parseParam net ctx) `shouldBe` Just p

instance Arbitrary StartParam where
  arbitrary =
    oneof
      [ StartParamHash <$> arbitraryHash256,
        StartParamHeight . fromIntegral <$> (choose (0, 1230768000) :: Gen Word64),
        StartParamTime <$> choose (1230768001, maxBound)
      ]

instance Arbitrary HeightsParam where
  arbitrary = HeightsParam <$> listOf arbitrarySizedNatural
