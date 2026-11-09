{-# LANGUAGE RecordWildCards #-}

module Haskoin.Store.DataSpec (spec) where

import Control.Monad (forM_)
import Data.Aeson (Value)
import Data.Aeson qualified as A
import Data.ByteString qualified as B
import Data.ByteString.Lazy qualified as BL
import Data.Bytes.Put (runPutS)
import Data.Bytes.Serial (Serial, serialize)
import Data.Text (Text)
import Data.HashMap.Strict qualified as HashMap
import Data.IntMap qualified as IntMap
import Data.Maybe (fromJust)
import Haskoin
import Haskoin.Store.Data
import Haskoin.Store.Gen
import Haskoin.Store.Laws
import Haskoin.Util.Arbitrary
import Test.Hspec
import Test.QuickCheck

-- | Networks with different address formats: legacy and Bech32 on Bitcoin,
-- CashAddr on Bitcoin Cash, and the prefixes of their test networks.
nets :: [Network]
nets = allNets

spec :: Spec
spec = prepareContext $ \ctx -> do
  describe "Binary encoding" $ do
    binaryLaws (arbitrary :: Gen BlockRef)
    binaryLaws (arbitrary :: Gen TxRef)
    binaryLaws (arbitrary :: Gen BlockData)
    binaryLaws (arbitrary :: Gen Spender)
    binaryLaws (arbitrary :: Gen Prev)
    binaryLaws (arbitraryTxData ctx)
    binaryLaws (arbitrary :: Gen XPubSummary)
    binaryLaws (arbitrary :: Gen HealthCheck)
    binaryLaws (arbitrary :: Gen Event)
    binaryLaws (arbitrary :: Gen TxId)
    binaryLaws (arbitrary :: Gen PeerInfo)
    binaryLaws (arbitrary :: Gen (GenericResult BlockData))
    binaryLaws (arbitrary :: Gen (RawResult BlockData))
    binaryLaws (arbitrary :: Gen (RawResultList BlockData))
    forM_ nets $ \net -> describe net.name $ do
      binaryLaws (arbitraryDeriveType net)
      binaryLaws (arbitraryXPubSpec net ctx)
      binaryLaws (arbitraryBalance net)
      binaryLaws (arbitraryUnspent net)
      binaryLaws (arbitraryStoreInput net)
      binaryLaws (arbitraryStoreOutput net)
      binaryLaws (arbitraryTransaction net)
      binaryLaws (arbitraryXPubBal net)
      binaryLaws (arbitraryXPubUnspent net)

  describe "JSON" $ do
    jsonLaws (arbitrary :: Gen TxRef)
    jsonLaws (arbitrary :: Gen BlockRef)
    jsonLaws (arbitrary :: Gen Spender)
    jsonLaws (arbitrary :: Gen XPubSummary)
    jsonLaws (arbitrary :: Gen HealthCheck)
    jsonLaws (arbitrary :: Gen Event)
    jsonLaws (arbitrary :: Gen TxId)
    jsonLaws (arbitrary :: Gen PeerInfo)
    jsonLaws (arbitrary :: Gen (GenericResult XPubSummary))
    jsonLaws (arbitrary :: Gen (RawResult BlockData))
    jsonLaws (arbitrary :: Gen (RawResultList BlockData))
    jsonLaws (arbitrary :: Gen Except)
    jsonLaws (arbitrary :: Gen BinfoWallet)
    jsonLaws (arbitrary :: Gen BinfoSymbol)
    jsonLaws (arbitrary :: Gen BinfoBlockInfo)
    jsonLaws (arbitrary :: Gen BinfoInfo)
    jsonLaws (arbitrary :: Gen BinfoSpender)
    jsonLaws (arbitrary :: Gen BinfoRate)
    jsonLaws (arbitrary :: Gen BinfoTicker)
    jsonLaws (arbitrary :: Gen BinfoTxId)
    jsonLaws (arbitrary :: Gen BinfoShortBal)
    jsonLaws (arbitrary :: Gen BinfoHistory)
    jsonLaws (arbitrary :: Gen BinfoHeader)
    jsonLaws (arbitrary :: Gen BinfoBlockInfos)

  describe "JSON of a network" $ forM_ nets $ \net -> describe net.name $ do
    marshalLaws net (arbitraryBalance net)
    marshalLaws net (arbitraryStoreOutput net)
    marshalLaws net (arbitraryUnspent net)
    marshalLaws net (arbitraryXPubBal net)
    marshalLaws net (arbitraryXPubUnspent net)
    marshalLaws net (arbitraryStoreInput net)
    marshalLaws net (arbitraryBlockData net)
    marshalLaws net (arbitraryTransaction net)
    marshalLaws (net, ctx) (arbitraryBinfoMultiAddr net ctx)
    marshalLaws (net, ctx) (arbitraryBinfoBalance net ctx)
    marshalLaws (net, ctx) (arbitraryBinfoBlock net ctx)
    marshalLaws (net, ctx) (arbitraryBinfoTx net ctx)
    marshalLaws (net, ctx) (arbitraryBinfoTxInput net ctx)
    marshalLaws (net, ctx) (arbitraryBinfoTxOutput net ctx)
    marshalLaws (net, ctx) (arbitraryBinfoXPubPath net ctx)
    marshalLaws (net, ctx) (arbitraryBinfoUnspent net ctx)
    marshalLaws (net, ctx) (small (arbitraryBinfoBlock net ctx))
    marshalLaws (net, ctx) (arbitraryBinfoRawAddr net ctx)
    marshalLaws (net, ctx) (arbitraryBinfoMempool net ctx)

  -- Other programs read these formats: they must not change by accident.
  describe "Wire format" $ do
    let txid = TxHash (sha256 B.empty)
        txidHex = "55b852781b9995a44c939b64e441ae2724b96f99c8f4fb9a141cfc9842c4b0e3"
        txidRaw = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        burn = fromJust (textToAddr btc "1111111111111111111114oLvT2")
    wire (BlockRef 100 2) "{\"height\":100,\"position\":2}" "01ffffff9bfffffffd"
    wire (MemRef 1700000000) "{\"mempool\":1700000000}" "00ffffffff9aac0eff"
    wire (Spender txid 3) ("{\"txid\":\"" <> txidHex <> "\",\"input\":3}") (txidRaw <> "00000003")
    wire
      (TxRef (BlockRef 100 2) txid)
      ("{\"block\":{\"height\":100,\"position\":2},\"txid\":\"" <> txidHex <> "\"}")
      ("01ffffff9bfffffffd" <> txidRaw)
    it "Balance" $
      marshalValue btc (Balance burn 1 2 3 4 5)
        `shouldBe` json
          "{\"address\":\"1111111111111111111114oLvT2\",\"confirmed\":1,\
          \\"unconfirmed\":2,\"utxo\":3,\"txs\":4,\"received\":5}"
    it "XPubSummary" $
      A.toJSON XPubSummary {confirmed = 1, unconfirmed = 2, received = 3, utxo = 4, change = 5, external = 6}
        `shouldBe` json
          "{\"balance\":{\"confirmed\":1,\"unconfirmed\":2,\"received\":3,\"utxo\":4},\
          \\"indices\":{\"change\":5,\"external\":6}}"

  describe "Blockchain.info helpers" $ do
    it "numbers a transaction by the first bits of its hash" $ do
      let h = TxHash (sha256 B.empty)
      encodeBinfoTxId False h `shouldBe` BinfoTxIdHash h
      encodeBinfoTxId True h `shouldBe` BinfoTxIdIndex 8011147086339971
    it "writes values as signed big-endian hexadecimal" $
      map binfoHexValue [0, 1, 127, 128, 255, 256, 65535]
        `shouldBe` ["00", "01", "7f", "0080", "00ff", "0100", "00ffff"]
    it "reads addresses and xpubs separated by bars or commas" $
      forAll (vectorOf 3 (arbitraryAddress btc)) $ \as ->
        forAll (arbitraryXPubKey ctx) $ \x ->
          let txt = mconcat [t <> sep | (a, sep) <- zip as ["|", ",", "|"], Just t <- [addrToText btc a]] <> xPubExport btc ctx x
           in parseBinfoAddr btc ctx txt `shouldBe` Just (map BinfoAddr as <> [BinfoXpub x])
    it "refuses anything that is neither an address nor an xpub" $
      parseBinfoAddr btc ctx "nonsense" `shouldBe` Nothing
    it "reads nothing from nothing" $
      parseBinfoAddr btc ctx "" `shouldBe` Just []
    it "gives the next indices of an xpub and counts only payments to it" $
      forAll (arbitraryXPubSpec btc ctx) $ \x ->
        forAll (vectorOf 3 (arbitraryAddress btc)) $ \as -> case as of
          [a1, a2, a3] ->
            let bal a r c = (zeroBalance a) {received = r, confirmed = c, txs = 1}
                xbals =
                  [ XPubBal [0, 4] (bal a1 100 60),
                    XPubBal [0, 1] (bal a2 50 0),
                    XPubBal [1, 2] (bal a3 30 30)
                  ]
             in case toBinfoAddrs HashMap.empty (HashMap.singleton x xbals) (HashMap.singleton x 7) of
                  [BinfoXPubBalance {..}] ->
                    (external, change, received, balance, sent, txs) `shouldBe` (5, 3, 150, 90, 60, 7)
                  bs -> expectationFailure ("not the balance of one xpub: " <> show bs)
          _ -> discard

  describe "Balances" $ do
    it "are null only when nothing ever touched the address" $
      forAll (arbitraryBalance btc) $ \b ->
        nullBalance b
          === (b.confirmed == 0 && b.unconfirmed == 0 && b.utxo == 0 && b.txs == 0 && b.received == 0)
    it "start null" $
      forAll (arbitraryAddress btc) (nullBalance . zeroBalance)

  describe "Derivation types" $
    it "are named normal, compat and segwit" $ do
      map deriveTypeToText [DeriveNormal, DeriveP2SH, DeriveP2WPKH] `shouldBe` ["normal", "compat", "segwit"]
      map textToDeriveType ["normal", "compat", "segwit", "other"]
        `shouldBe` [Just DeriveNormal, Just DeriveP2SH, Just DeriveP2WPKH, Nothing]

  describe "Coin instance of Unspent" $
    it "reads the input kind from the output script" $
      property $
        forAll (arbitraryScriptOutput btc ctx) $ \so ->
          case encodeOutput ctx so of
            Left _ -> discard
            Right script ->
              let u =
                    Unspent
                      { block = MemRef 0,
                        outpoint = OutPoint (TxHash z) 0,
                        value = 1,
                        script = runPutS (serialize script),
                        address = Nothing
                      }
               in coinInputKind u `shouldBe` expectedKind so
  describe "toTransaction" $ do
    -- The JSON of the only output of a transaction that pays 1000 satoshi
    -- to a script given in hex.
    let outputJson net scriptHex =
          let script = fromJust (decodeHex scriptHex)
              tx = Tx 2 [TxIn (OutPoint (TxHash z) 0) "" maxBound] [TxOut 1000 script] [] 0
              td =
                TxData
                  { block = MemRef 0,
                    tx = tx,
                    prevs = IntMap.empty,
                    deleted = False,
                    rbf = False,
                    timestamp = 0,
                    spenders = IntMap.empty
                  }
           in map (marshalValue net) (toTransaction net ctx td).outputs
        -- Pay to witness public key hash, from the examples of BIP 173.
        p2wpkh = "0014751e76e8199196d454941c45d1b3a323f1433bd6"
        -- OP_HASH256, a push of 32 bytes, OP_EQUAL.
        hashLock = "aa20bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb87"
    it "computes the id, size, weight and witnesses of the transaction" $
      forAll (arbitraryTxData ctx) $ \td ->
        let t = toTransaction btc ctx td
            len :: Tx -> Int
            len = B.length . runPutS . serialize
            witless = let Tx {..} = td.tx in Tx {witness = [], ..}
            ws = take (length td.tx.inputs) (td.tx.witness <> repeat [])
         in (t.txid, t.size, t.weight, map (.witness) t.inputs)
              `shouldBe` ( txHash td.tx,
                           fromIntegral (len td.tx),
                           fromIntegral (len witless * 3 + len td.tx),
                           ws
                         )
    it "gives a SegWit output its address on Bitcoin" $
      outputJson btc p2wpkh
        `shouldBe` [ json
                       "{\"address\":\"bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4\",\
                       \\"pkscript\":\"0014751e76e8199196d454941c45d1b3a323f1433bd6\",\
                       \\"value\":1000,\"spent\":false,\"spender\":null}"
                   ]
    it "gives a SegWit-shaped output no address on Bitcoin Cash, which has no SegWit" $
      outputJson bch p2wpkh
        `shouldBe` [ json
                       "{\"address\":null,\
                       \\"pkscript\":\"0014751e76e8199196d454941c45d1b3a323f1433bd6\",\
                       \\"value\":1000,\"spent\":false,\"spender\":null}"
                   ]
    it "gives a 256-bit hash lock output no address on Bitcoin, which has no such address" $
      outputJson btc hashLock
        `shouldBe` [ json
                       "{\"address\":null,\
                       \\"pkscript\":\"aa20bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb87\",\
                       \\"value\":1000,\"spent\":false,\"spender\":null}"
                   ]
  where
    z = sha256 B.empty
    expectedKind = \case
      PayPK {} -> InputPK
      PayPKHash {} -> InputPKHash
      PayMulSig {keys, required} -> InputMulSig required (length keys)
      PayScriptHash {} -> InputNestedWitnessPKHash
      PayWitnessPKHash {} -> InputWitnessPKHash
      PayWitnessScriptHash {} -> InputWitnessMulSig 1 1
      PayWitness 1 bs | B.length bs == 32 -> InputTaproot
      _ -> InputPKHash


-- | A value has this JSON and these bytes.
wire :: (Show a, A.ToJSON a, Serial a) => a -> BL.ByteString -> Text -> Spec
wire x j h =
  it (takeWhile (/= ' ') (show x)) $ do
    A.toJSON x `shouldBe` json j
    encodeHex (runPutS (serialize x)) `shouldBe` h

-- | Read a JSON literal.
json :: BL.ByteString -> Value
json = fromJust . A.decode
