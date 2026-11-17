module Haskoin.Store.LogicSpec (spec) where

import Control.Monad (forM)
import Data.ByteString qualified as B
import Data.Serialize (encode)
import Database.RocksDB (columnFamilies)
import Haskoin
  ( Block (..),
    Network (..),
    OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    btcRegTest,
    sha256,
    txHash,
  )
import Haskoin.Store.Common (sortTxs)
import Haskoin.Store.Data (BlockData (..))
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader (DatabaseReader (..))
import Haskoin.Store.Histories
import Haskoin.Store.Integration.Temp (withSystemTempDirectory)
import Haskoin.Store.Logic (PreparedBlock (..), PreparedTx (..), prepareBlock)
import Haskoin.Store.TestBlocks (coinbaseTx, script)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck (Gen, choose, forAll, ioProperty, property, vectorOf)

spec :: Spec
spec = describe "the rules of the chain" $ do
  it "take out with a revert exactly what the import of a block put in" $
    withSystemTempDirectory "haskoin-store-logic-" $ \dir ->
      openLayered btcRegTest dir $ \db -> do
        startChain db
        first <- mine db []
        coinbase <-
          Q.getBlock db first >>= \case
            Just bd | h : _ <- bd.txs -> return h
            _ -> fail "no coinbase"
        kept <- chainFamilies db
        let spend =
              Tx
                1
                [TxIn (OutPoint coinbase 0) "" maxBound]
                [TxOut 1000 script, TxOut 2000 ("\x00\x14" <> B.replicate 20 0x22)]
                []
                0
        second <- mine db [spend]
        Q.getTxData db (txHash spend) >>= (`shouldSatisfy` maybe False (const True))
        Q.getUnspent db (OutPoint coinbase 0) >>= (`shouldBe` Nothing) . fmap (const ())
        revert db
        Q.getBestBlock db `shouldReturn` Just first
        fmap (.main) <$> Q.getBlock db second `shouldReturn` Just False
        Q.getTxData db (txHash spend) >>= (`shouldBe` Nothing) . fmap (const ())
        chainFamilies db `shouldReturn` kept

  describe "prepareBlock" $
    modifyMaxSuccess (const 50) $
      it "gives the hash of each transaction, the order of sortTxs, and the size and weight of the block" $
        property . forAll payments $ \txs -> ioProperty $ do
          let block = Block btcRegTest.genesisHeader (coinbaseTx 1 : txs)
              stripped = (block :: Block) {txs = map (\tx -> (tx :: Tx) {witness = []}) block.txs}
          pb <- prepareBlock btcRegTest block
          map (.txid) pb.txs `shouldBe` map txHash block.txs
          map (.tx) pb.txs `shouldBe` block.txs
          [(i, p.tx) | (i, p) <- pb.ordered] `shouldBe` sortTxs block.txs
          pb.size `shouldBe` B.length (encode block)
          pb.weight `shouldBe` 3 * B.length (encode stripped) + B.length (encode block)
  where
    -- the column families of the chain whose keys a block adds and a revert
    -- takes out: addresses, outputs, transactions, unspent outputs,
    -- spenders and balances
    chainFamilies db = forM [0, 1, 2, 3, 4, 7] $ \i -> dump db (columnFamilies db.db !! i)

-- | Transactions that spend one another and some coins, some with a
-- witness, in a random order.
payments :: Gen [Tx]
payments = do
  n <- choose (0, 20)
  seeds <- vectorOf n (choose (0, 3 :: Int))
  let coin i = OutPoint (TxHash (sha256 (encode i))) 0
      -- one of kind 0 spends the one before it, and one of kind 2 has a
      -- witness
      tx prev (i, kind) =
        Tx
          1
          [TxIn (maybe (coin i) (\p -> if kind == 0 then OutPoint (txHash p) 0 else coin i) prev) "" maxBound]
          [TxOut (fromIntegral (1000 + i)) script]
          (if kind == 2 then [["\x01\x02"]] else [])
          0
      chained _ [] = []
      chained prev (x : xs) = let t = tx prev x in t : chained (Just t) xs
  return (reverse (chained Nothing (zip [0 :: Int ..] seeds)))
