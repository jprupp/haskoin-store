module Haskoin.Store.WebSpec (spec) where

import Data.ByteString (ByteString)
import Data.Either (isLeft)
import Data.Maybe (fromJust)
import Data.Text.Encoding (decodeUtf8)
import Haskoin (Tx (..), bch, btc, decodeHex, txHash, txHashToHex)
import Haskoin.Store.Web (decodeTxBody)
import Test.Hspec

-- | A transaction written by hand, in the format without witness. It has
-- version 1, one input that spends output 0 of transaction aa..aa with an
-- empty script, one output of 1000 satoshi to the script OP_TRUE, and lock
-- time 0.
plainTxHex :: ByteString
plainTxHex =
  "01000000\
  \01aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
  \0000000000ffffffff\
  \01e8030000000000000151\
  \00000000"

-- | The same transaction in the SegWit format. The marker and flag bytes
-- 00 01 follow the version, and the input has one witness item, the byte 01.
segwitTxHex :: ByteString
segwitTxHex =
  "010000000001\
  \01aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
  \0000000000ffffffff\
  \01e8030000000000000151\
  \010101\
  \00000000"

spec :: Spec
spec =
  describe "decodeTxBody" $ do
    -- The id is the double SHA-256 of the bytes of plainTxHex, reversed. It
    -- was computed outside this library. The witness is not part of the id,
    -- so both formats of the transaction have this id.
    let txid = "deca6f028918fa8d8c0efc66f2015cd5a2b5d42c8352a7e07468983f988900c9"
        idOf = fmap (txHashToHex . txHash)
    it "reads a transaction given as hex" $
      idOf (decodeTxBody btc plainTxHex) `shouldBe` Right txid
    it "reads a transaction given as raw bytes" $ do
      let bytes = fromJust (decodeHex (decodeUtf8 plainTxHex))
      idOf (decodeTxBody btc bytes) `shouldBe` Right txid
    it "ignores white space around the hex" $
      idOf (decodeTxBody btc (" " <> plainTxHex <> "\r\n")) `shouldBe` Right txid
    it "refuses bytes after the transaction" $
      decodeTxBody btc (plainTxHex <> "00") `shouldSatisfy` isLeft
    it "reads the SegWit format on Bitcoin, with its witness" $ do
      let tx = decodeTxBody btc segwitTxHex
      idOf tx `shouldBe` Right txid
      fmap (.witness) tx `shouldBe` Right [["\x01"]]
    it "reads the format without witness on Bitcoin Cash" $
      idOf (decodeTxBody bch plainTxHex) `shouldBe` Right txid
    it "refuses the SegWit format on Bitcoin Cash, which has no SegWit" $
      decodeTxBody bch segwitTxHex `shouldSatisfy` isLeft
