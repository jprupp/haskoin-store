module Haskoin.Store.WebSpec (spec) where

import Control.Concurrent (threadDelay)
import Data.ByteString (ByteString)
import Data.Either (isLeft)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Maybe (fromJust)
import Data.Text qualified as T
import Data.Text.Encoding (decodeUtf8)
import Haskoin (Tx (..), bch, btc, decodeHex, txHash, txHashToHex)
import Haskoin.Node (LogLevel (..), Logger (..))
import Haskoin.Store.Data
  ( BlockHealth (..),
    CountHealth (..),
    HealthCheck (..),
    MaxHealth (..),
    TimeHealth (..),
  )
import Haskoin.Store.Web (ailments, decodeTxBody, logIt)
import Network.HTTP.Types (status200, status404, status500, status503)
import Network.Wai (Request (..), defaultRequest, responseLBS)
import Network.Wai.Internal (ResponseReceived (..))
import System.Log.FastLogger (fromLogStr)
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

-- | What the log has after a request that is answered with a status, after
-- a number of milliseconds, by a server that warns of requests of a second.
logged :: Bool -> [T.Text] -> Int -> Int -> IO [T.Text]
logged access path status wait = do
  said <- newIORef []
  let keep line = modifyIORef' said (decodeUtf8 (fromLogStr line) :)
      logger = Logger {level = LevelDebug, source = "", push = keep}
      answer = case status of
        200 -> status200
        404 -> status404
        503 -> status503
        _ -> status500
      app _ respond = threadDelay (wait * 1000) >> respond (responseLBS answer [] "")
  _ <- logIt logger access 1 app defaultRequest {pathInfo = path} (\_ -> return ResponseReceived)
  reverse <$> readIORef said

-- | The level of each line of a log, as the line names it at its start.
levels :: [T.Text] -> [T.Text]
levels = map (T.takeWhile (`notElem` ['#', ']']) . T.drop 1)

-- | A health check that passes.
well :: HealthCheck
well =
  HealthCheck
    { blocks = BlockHealth {headers = 100, blocks = 100, max = 3},
      lastBlock = TimeHealth {age = 60, max = 14400},
      lastTx = TimeHealth {age = 5, max = 1800},
      pendingTxs = MaxHealth {count = 0, max = 10000},
      peers = CountHealth {count = 8, min = 1},
      network = "btc",
      version = "0",
      time = 0,
      redis = Nothing
    }

spec :: Spec
spec = do
  describe "log of requests" $ do
    it "has an error only for an error of the server" $ do
      levels <$> logged False ["block", "best"] 200 0 `shouldReturn` ["Debug"]
      levels <$> logged False ["address", "x"] 404 0 `shouldReturn` ["Debug"]
      levels <$> logged False ["health"] 503 0 `shouldReturn` ["Debug"]
      levels <$> logged False ["block", "best"] 500 0 `shouldReturn` ["Error"]
    it "warns of a request that took long, and not of the stream of events" $ do
      slow <- logged False ["xpub", "x"] 200 1100
      levels slow `shouldBe` ["Debug", "Warn"]
      last slow `shouldSatisfy` T.isInfixOf "Slow request, 1.1 seconds: "
      levels <$> logged False ["events"] 200 1100 `shouldReturn` ["Debug"]
    it "has every request as information when the access log is on" $ do
      lines' <- logged True ["block", "best"] 200 0
      levels lines' `shouldBe` ["Info"]
      -- the address of the client, the request, the status and the time
      last lines' `shouldSatisfy` T.isInfixOf "0.0.0.0:0 GET"
      last lines' `shouldSatisfy` T.isInfixOf ": 200 OK, "
      last lines' `shouldSatisfy` T.isSuffixOf " ms\n"
      levels <$> logged True ["block", "best"] 500 0 `shouldReturn` ["Error"]
  describe "ailments" $ do
    it "names what fails of a health check" $ do
      ailments well `shouldBe` ""
      let behind = well {blocks = BlockHealth {headers = 105, blocks = 100, max = 3}} :: HealthCheck
          alone = behind {peers = CountHealth {count = 0, min = 1}} :: HealthCheck
      ailments behind `shouldBe` "5 blocks behind the headers"
      ailments alone `shouldBe` "5 blocks behind the headers, 0 peers"
  describe "decodeTxBody" $ do
    -- The id is the double SHA-256 of the bytes of plainTxHex, reversed. It
    -- was computed outside this library. The witness is not part of the id,
    -- so both formats of the transaction have this id.
    let txid = "deca6f028918fa8d8c0efc66f2015cd5a2b5d42c8352a7e07468983f988900c9"
        idOf = fmap (txHashToHex . txHash)
    it "reads a transaction given as hex" $
      idOf (decodeTxBody btc plainTxHex) `shouldBe` Right txid
    it "refuses a transaction given as raw bytes" $ do
      let bytes = fromJust (decodeHex (decodeUtf8 plainTxHex))
      decodeTxBody btc bytes `shouldSatisfy` isLeft
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
