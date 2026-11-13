{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module Haskoin.Store.Web.PublishSpec (spec) where

import Control.Concurrent (forkIO, threadDelay)
import UnliftIO.Async (concurrently)
import Control.Concurrent.STM (atomically, modifyTVar', newTVarIO, readTVarIO)
import Control.Monad (replicateM_, void)
import Data.ByteString qualified as B
import Data.Serialize (encode)
import Data.Time.Clock (UTCTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Data.Word (Word32, Word64)
import Haskoin hiding (Coin (..))
import Haskoin.Store.Common (StoreEvent (..))
import Haskoin.Store.Web.Publish
import NQE (withPublisher)
import NQE qualified
import Network.Socket (SockAddr (..), tupleToHostAddress)
import Test.Hspec

-- | An output, told apart by a number.
output :: Word32 -> OutPoint
output n = OutPoint (TxHash (doubleSHA256 (encode n))) n

-- | A transaction that spends outputs and pays a value.
spending :: [OutPoint] -> Word64 -> Tx
spending ins v =
  Tx
    { version = 2,
      inputs = [TxIn {outpoint = o, script = "", sequence = maxBound} | o <- ins],
      outputs = [TxOut {value = v, script = B.replicate 25 1}],
      witness = [],
      locktime = 0
    }

limits :: Limits
limits =
  Limits {wait = 0.3, handover = 1, tries = 2, capacity = 3, memory = 600}

at :: Integer -> UTCTime
at = posixSecondsToUTCTime . fromInteger

node :: Word32 -> SockAddr
node n = SockAddrInet 8333 (tupleToHostAddress (10, 0, 0, fromIntegral n))

-- | A coin of a value that nothing is known of: not in a block, and with
-- a script that the store cannot judge the spending of.
held :: Word64 -> Coin
held v = Coin {output = TxOut v "", mined = Nothing, coinbase = False}

-- | The chain at block 1000.
--
-- The signatures are judged by the scripts of the test: an input script
-- of @forged@ is forged, one of @script@ cannot be told, and any other
-- verifies. The real judge is tested in haskoin-core, and against nodes
-- in the integration tests.
rulesOf :: Rules
rulesOf = Rules {net = btc, inputs = \tx _ -> map verdict tx.inputs, height = 1000, medianTime = 1600000000}
  where
    verdict i
      | i.script == "forged" = InputForged
      | i.script == "script" = InputUndecided
      | otherwise = InputVerified

-- | A desk to publish at: every output is a coin of 1000, the store has no
-- transaction, and the courier does what it is told for each node. It
-- counts the times it was sent.
desk :: Ctx -> (SockAddr -> Tx -> IO Delivery) -> (Publishing -> IO Int -> IO a) -> IO a
desk ctx courier go =
  withPublisher $ \events -> do
    sent <- newTVarIO (0 :: Int)
    here <- newTVarIO ([] :: [TxHash])
    outbox <- newTVarIO emptyOutbox
    let p =
          Publishing
            { limits = limits,
              outbox = outbox,
              rules = return (rulesOf),
              direct = \_ -> return Unavailable,
              holds = \h -> (\hs -> if elem h hs then Pending else Absent) <$> readTVarIO here,
              coin = \_ -> return (held 1000),
              present = \h -> elem h <$> readTVarIO here,
              events = events,
              mempool = True,
              nodes = return [node 1, node 2],
              courier = \sa tx -> atomically (modifyTVar' sent (+ 1)) >> courier sa tx,
              note = \_ _ -> return ()
            }
    go p (readTVarIO sent)

spec :: Spec
spec = prepareContext $ \ctx -> do
  let check = judge (rulesOf)
  describe "what the store can check" $ do
    let tx = spending [output 1, output 2] 1500
    it "lets through a transaction that spends what is there" $
      check tx [held 1000, held 500] `shouldBe` Right Nothing
    it "refuses an output it does not know, and one spent in a block" $ do
      check tx [held 1000, Missing] `shouldBe` Left (UnknownOutput (output 2))
      let by = (output 9).hash
      check tx [Spent by, held 1000] `shouldBe` Left (SpentOutput (output 1) by)
    it "refuses outputs worth more than the inputs" $
      check tx [held 1000, held 499] `shouldBe` Left Overspent
    it "refuses the same output twice, a coinbase, and nothing to spend or pay" $ do
      check (spending [output 1, output 1] 1) [held 1, held 1] `shouldBe` Left (DuplicateInput (output 1))
      check (spending [nullOutPoint] 1) [Missing] `shouldBe` Left IsCoinbase
      check (spending [] 1) [] `shouldBe` Left NoInputs
      check (spending [output 1] 1) {outputs = []} [held 1] `shouldBe` Left NoOutputs

    it "refuses a coinbase that is not a hundred blocks old" $ do
      let young h = Coin {output = TxOut 1000 "", mined = Just h, coinbase = True}
          one = spending [output 1] 900
      check one [young 902] `shouldBe` Left (Immature (output 1))
      check one [young 901] `shouldBe` Right Nothing
      check one [(young 902) {coinbase = False}] `shouldBe` Right Nothing
    it "refuses a lock time that has not passed, unless every input waives it" $ do
      let locked n = (spending [output 1] 900) {locktime = n, inputs = [TxIn (output 1) "" 0]} :: Tx
      check (locked 1000) [held 1000] `shouldBe` Right Nothing
      check (locked 1001) [held 1000] `shouldBe` Left (NotFinal 1001)
      check (locked 1599999999) [held 1000] `shouldBe` Right Nothing
      check (locked 1600000000) [held 1000] `shouldBe` Left (NotFinal 1600000000)
      check ((locked 1001) {inputs = [TxIn (output 1) "" maxBound]} :: Tx) [held 1000] `shouldBe` Right Nothing
    it "refuses an output worth less than it costs to spend, and not one that holds data" $ do
      let paying v script = (spending [output 1] 0) {outputs = [TxOut v script]} :: Tx
          keyHash = "\x76\xa9\x14" <> B.replicate 20 7 <> "\x88\xac"
          witnessKeyHash = "\x00\x14" <> B.replicate 20 7
      check (paying 545 keyHash) [held 1000] `shouldBe` Left (Dust 0)
      check (paying 546 keyHash) [held 1000] `shouldBe` Right Nothing
      check (paying 293 witnessKeyHash) [held 1000] `shouldBe` Left (Dust 0)
      check (paying 294 witnessKeyHash) [held 1000] `shouldBe` Right Nothing
      check (paying 0 "\x6a\x04data") [held 1000] `shouldBe` Right Nothing
    it "refuses a forged signature, and names the input it cannot judge" $ do
      let signedBy items = (spending [output 1, output 2] 900) {inputs = zipWith input [1 ..] items} :: Tx
          input n item = TxIn (output n) item maxBound
      check (signedBy ["good", "forged"]) [held 1000, held 1000] `shouldBe` Left (Forged 1)
      check (signedBy ["good", "script"]) [held 1000, held 1000] `shouldBe` Right (Just 1)
      check (signedBy ["good", "good"]) [held 1000, held 1000] `shouldBe` Right Nothing

  describe "outbox" $ do
    let k n = doubleSHA256 (encode (n :: Word32))
        come t n = enter limits (at t) (k n)
    it "gives one request the transaction, and has the others join" $ do
      let (first, box) = come 0 1 emptyOutbox
          (second, _) = come 1 1 box
      (first, second) `shouldBe` (Fresh, Joined)
    it "remembers a refusal for a while, and not a success" $ do
      let (_, box) = come 0 1 emptyOutbox
          refused = settle (at 1) (k 1) (Refused "no") box
          accepted = settle (at 1) (k 1) Accepted box
      fst (come 2 1 refused) `shouldBe` Known (Refused "no")
      fst (come 700 1 refused) `shouldBe` Fresh
      fst (come 2 1 accepted) `shouldBe` Fresh
    it "turns away what it has no room for" $ do
      let (_, one) = come 0 1 emptyOutbox
          (_, two) = come 0 2 one
          (_, three) = come 0 3 two
      fst (come 0 4 three) `shouldBe` Full
    it "keeps taking others after nodes closed the courier on some" $ do
      let (_, one) = come 0 1 emptyOutbox
          (_, two) = come 0 2 (settle (at 1) (k 1) Dropped one)
          box = settle (at 2) (k 2) Dropped two
      fst (come 3 3 box) `shouldBe` Fresh
      fst (come 3 1 box) `shouldBe` Known Dropped

  describe "publishing" $ do
    let tx = spending [output 1] 900
        echo events = void . forkIO $ threadDelay 50000 >> NQE.publish (StoreMempoolNew (txHash tx)) events
    it "answers when the transaction comes back, having sent it once" $
      desk ctx (\_ _ -> return Took) $ \p sent -> do
        echo p.events
        publish p tx `shouldReturn` Verdict Accepted
        sent `shouldReturn` 1
    it "sends nothing for what the store has in a block, or cannot be valid" $
      desk ctx (\_ _ -> return Took) $ \p sent -> do
        publish p {holds = \_ -> return Confirmed} tx `shouldReturn` Verdict Accepted
        publish p {coin = \_ -> return Missing} tx
          `shouldReturn` Invalid (UnknownOutput (output 1))
        sent `shouldReturn` 0
    it "sends again what the store has unconfirmed, each time, and waits for no echo" $
      desk ctx (\_ _ -> return Took) $ \p sent -> do
        let pending = p {holds = \_ -> return Pending}
        replicateM_ 2 $ publish pending tx `shouldReturn` Verdict Accepted
        sent `shouldReturn` 2
        -- checked first, like any other: a coinbase too young is not sent
        publish pending {coin = \_ -> return (Coin {output = TxOut 1000 "", mined = Just 950, coinbase = True})} tx
          `shouldReturn` Invalid (Immature (output 1))
        sent `shouldReturn` 2
    it "says why a node refuses what the store has unconfirmed" $
      desk ctx (\_ _ -> return (Rejected "missing inputs")) $ \p sent -> do
        publish p {holds = \_ -> return Pending} tx `shouldReturn` Verdict (Refused "missing inputs")
        sent `shouldReturn` 1
    it "gives the reason of a node, again without asking it again" $
      desk ctx (\_ _ -> return (Rejected "too cheap")) $ \p sent -> do
        replicateM_ 2 $
          publish p tx `shouldReturn` Verdict (Refused "too cheap")
        sent `shouldReturn` 1
    it "waits again, and sends nothing, for what a node took and did not announce" $
      desk ctx (\_ _ -> return Took) $ \p sent -> do
        publish p tx `shouldReturn` Verdict Silent
        echo p.events
        publish p tx `shouldReturn` Verdict Accepted
        sent `shouldReturn` 1
    it "goes to the next node when one cannot be reached, and tries anew when none can" $ do
      desk ctx (\sa _ -> return (if sa == node 1 then NoConnection else Rejected "no")) $ \p sent -> do
        publish p tx `shouldReturn` Verdict (Refused "no")
        sent `shouldReturn` 2
      desk ctx (\_ _ -> return NoConnection) $ \p sent -> do
        replicateM_ 2 $ publish p tx `shouldReturn` Verdict Unreachable
        sent `shouldReturn` 4
    it "does not send again what a node closed the courier on, and goes on publishing the rest" $
      desk ctx (\_ t -> return (if t == tx then Closed else Rejected "no")) $ \p sent -> do
        replicateM_ 2 $ publish p tx `shouldReturn` Verdict Dropped
        publish p (spending [output 2] 900) `shouldReturn` Verdict (Refused "no")
        sent `shouldReturn` 2
    it "refuses a transaction larger than nodes relay" $
      desk ctx (\_ _ -> return Took) $ \p sent -> do
        let long = (spending [output 1] 1) {outputs = [TxOut 1 (B.replicate 200000 1)]} :: Tx
        publish p long >>= \case
          Invalid (TooHeavy _) -> return ()
          other -> expectationFailure (show other)
        sent `shouldReturn` 0
    it "gives it to a node over RPC where it can, and to no peer then" $
      desk ctx (\_ _ -> return Took) $ \p sent -> do
        echo p.events
        publish p {direct = \_ -> return Sent} tx `shouldReturn` Verdict Accepted
        publish p {direct = \_ -> return (Declined "min relay fee not met")} (spending [output 2] 900)
          `shouldReturn` Verdict (Refused "min relay fee not met")
        sent `shouldReturn` 0
    it "sends to a peer only what it verified, and the rest over RPC only" $
      desk ctx (\_ _ -> return (Rejected "sent")) $ \p sent -> do
        let unjudged = (spending [output 1] 900) {inputs = [TxIn (output 1) "script" maxBound]} :: Tx
        -- no node to ask: it is not sent, and is not held against it
        replicateM_ 2 $ publish p unjudged `shouldReturn` Verdict (Unverified 0)
        sent `shouldReturn` 0
        -- a node that answers over RPC takes it
        publish p {direct = \_ -> return (Declined "no")} unjudged `shouldReturn` Verdict (Refused "no")
        -- one that was verified goes to a peer when RPC does not answer
        publish p tx `shouldReturn` Verdict (Refused "sent")
        sent `shouldReturn` 1
    it "sends a transaction once for two requests at the same time" $
      desk ctx (\_ _ -> threadDelay 100000 >> return (Rejected "no")) $ \p sent -> do
        concurrently (publish p tx) (publish p tx)
          `shouldReturn` (Verdict (Refused "no"), Verdict (Refused "no"))
        sent `shouldReturn` 1
    it "does not take the bytes of one transaction for those of another with its id" $ do
      -- same id, other witness: other bytes, so another slot
      let other = tx {witness = [["x"]]}
      txHash other `shouldBe` txHash tx
      desk ctx (\_ t -> return (if t == tx then Took else Rejected "bad witness")) $ \p _ -> do
        publish p other `shouldReturn` Verdict (Refused "bad witness")
        echo p.events
        publish p tx `shouldReturn` Verdict Accepted
