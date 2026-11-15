{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot #-}

-- | The stored values read in part, held to the full decoders on random
-- values: a block whose transactions are decoded when used, and the bytes
-- of a stored transaction, which must be those that the raw answers sent
-- when they built the transaction and serialized it again.
module Haskoin.Store.Database.SlicesSpec (spec) where

import Data.ByteString qualified as B
import Data.Bytes.Put (runPutS)
import Data.Bytes.Serial (serialize)
import Data.IntMap.Strict qualified as IntMap
import Data.Serialize (encode)
import Haskoin
import Haskoin.Store.Data
import Haskoin.Store.Database.Slices (blockData, txBytes)
import Haskoin.Util.Arbitrary
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck

spec :: Spec
spec = do
  modifyMaxSuccess (const 500) $ describe "stored values read in part" $ do
    it "read a block as the full decoder does" $
      forAll arbitraryBlockData $
        \b -> blockData (encode b) `shouldBe` Right b
    it "refuse a block that has fewer bytes than its count of transactions" $
      forAll arbitraryBlockData $ \b ->
        let bytes = encode b
            cut = B.take (B.length bytes - 25) bytes
         in not (null b.txs) ==> either (const True) (const False) (blockData cut)
    it "find the transaction of a stored one, and whether it is deleted" $
      forAll arbitraryTxData $ \td ->
        txBytes (encode td) `shouldBe` Right (runPutS (serialize td.tx), td.deleted)
    it "give the bytes that building and serializing the transaction gave" $
      forAll arbitraryTxData $ \td ->
        fmap fst (txBytes (encode td))
          `shouldBe` Right (runPutS (serialize (transactionData (toTransaction btc td))))

arbitraryBlockData :: Gen BlockData
arbitraryBlockData =
  BlockData
    <$> arbitrary
    <*> arbitrary
    <*> oneof [choose (0, 2 ^ (31 :: Int)), choose (2 ^ (31 :: Int), 2 ^ (100 :: Int))]
    <*> arbitraryBlockHeader
    <*> arbitrary
    <*> arbitrary
    <*> oneof [listOf arbitraryTxHash, vectorOf 300 arbitraryTxHash]
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary

arbitraryTxData :: Gen TxData
arbitraryTxData =
  TxData
    <$> oneof [MemRef <$> arbitrary, BlockRef <$> arbitrary <*> arbitrary]
    <*> oneof [arbitraryTx btc, arbitraryWitnessTx btc, arbitraryLegacyTx btc]
    <*> (IntMap.fromList <$> listOf ((,) <$> choose (0, 1000) <*> (Prev <$> arbitraryBS <*> arbitrary)))
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> (IntMap.fromList <$> listOf ((,) <$> choose (0, 1000) <*> (Spender <$> arbitraryTxHash <*> arbitrary)))
