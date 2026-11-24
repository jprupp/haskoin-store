{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module Haskoin.Store.Web.PublishSpec (spec) where

import Control.Concurrent.STM (atomically, modifyTVar', newTVarIO, readTVarIO)
import Control.Monad (replicateM_)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Serialize (encode)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Data.Word (Word32, Word64, Word8)
import Haskoin hiding (Coin (..))
import Haskoin.Store.Web.Publish
import Network.Socket (SockAddr (..))
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

-- | A coin of a value that nothing is known of: not in a block, and with
-- a script that the store cannot judge the spending of.
held :: Word64 -> Coin
held v = Coin {output = TxOut v "", mined = Nothing, coinbase = False}

-- | An input script that pushes a word, which 'rulesOf' reads.
saying :: ByteString -> ByteString
saying word = encode (Script [opPushData word])

-- | The chain at block 1000.
--
-- The signatures are judged by the scripts of the test: an input script
-- that pushes @forged@ is forged, one that pushes @script@ cannot be told,
-- and any other verifies. The real judge is tested in haskoin-core, and
-- against nodes in the integration tests.
rulesOf :: Rules
rulesOf = Rules {net = btc, inputs = \tx _ -> map verdict tx.inputs, height = 1000, medianTime = 1600000000}
  where
    verdict i
      | i.script == saying "forged" = InputForged
      | i.script == saying "script" = InputUndecided
      | otherwise = InputVerified

-- | A peer, told apart by a number: a Unix socket by name, which nothing
-- opens, since the courier of these tests is a function.
node :: Int -> SockAddr
node n = SockAddrUnix ("peer-" <> show n)

-- | A desk to publish at: every output is a coin of 1000, the store has no
-- transaction, there is no node to ask over RPC, and there are as many
-- peers as said, each of which does with a transaction what it is told.
-- It counts what each peer was handed, and the transactions asked for
-- after the answer.
desk :: Int -> (Int -> Tx -> Delivery) -> (Publishing -> IO [Int] -> IO [TxHash] -> IO a) -> IO a
desk n does go = do
  sent <- newTVarIO (replicate n 0)
  asked <- newIORef []
  let count k = atomically (modifyTVar' sent (\cs' -> [if j == k then c + 1 else c | (j, c) <- zip [0 ..] cs']))
      p =
        Publishing
          { rules = return rulesOf,
            direct = \_ -> return Unavailable,
            holds = \_ -> return Absent,
            coin = \_ -> return (held 1000),
            peers = return [node k | k <- [0 .. n - 1]],
            courier = \sa tx -> case [k | k <- [0 .. n - 1], node k == sa] of
              k : _ -> count k >> return (does k tx)
              [] -> return NoConnection,
            fetch = \h -> modifyIORef' asked (h :),
            note = \_ _ -> return ()
          }
  go p (readTVarIO sent) (reverse <$> readIORef asked)

-- | Every peer takes what it is handed.
taking :: Int -> Tx -> Delivery
taking _ _ = Took

spec :: Spec
spec = do
  -- What the store checks of a transaction is tested in haskoin-core,
  -- where the checks are ("Haskoin.Transaction.JudgeSpec"): here is what
  -- the store does with the answer.
  describe "publishing" $ do
    let tx = spending [output 1] 900
    it "hands a transaction to every peer and answers at once, then asks for it" $
      desk 3 taking $ \p sent asked -> do
        publish p tx `shouldReturn` ToPeers 3
        sent `shouldReturn` [1, 1, 1]
        asked `shouldReturn` [txHash tx]
    it "sends again, each time, what the store has unconfirmed" $
      desk 2 taking $ \p sent _ -> do
        replicateM_ 2 $ publish p {holds = \_ -> return Pending} tx `shouldReturn` ToPeers 2
        sent `shouldReturn` [2, 2]
    it "sends nothing for what the store has in a block, or cannot be valid" $
      desk 2 taking $ \p sent asked -> do
        publish p {holds = \_ -> return Confirmed} tx `shouldReturn` InBlock
        publish p {coin = \_ -> return Missing} tx `shouldReturn` Invalid (UnknownOutput (output 1))
        let young = Coin {output = TxOut 1000 "", mined = Just 950, coinbase = True}
        publish p {holds = \_ -> return Pending, coin = \_ -> return young} tx
          `shouldReturn` Invalid (Immature (output 1))
        sent `shouldReturn` [0, 0]
        asked `shouldReturn` []
    it "refuses a transaction larger than nodes relay" $
      desk 1 taking $ \p sent _ -> do
        let long = (spending [output 1] 1) {outputs = [TxOut 1 (B.replicate 200000 1)]} :: Tx
        publish p long >>= \case
          Invalid (TooHeavy _) -> return ()
          other -> expectationFailure (show other)
        sent `shouldReturn` [0]
    it "gives it to a node over RPC where it can, and to no peer then" $
      desk 2 taking $ \p sent asked -> do
        publish p {direct = \_ -> return Sent} tx `shouldReturn` ToNode
        publish p {direct = \_ -> return (Declined "min relay fee not met")} tx
          `shouldReturn` Refused "min relay fee not met"
        sent `shouldReturn` [0, 0]
        asked `shouldReturn` [txHash tx]
    it "sends to the peers only what it verified, and the rest over RPC only" $
      desk 2 taking $ \p sent _ -> do
        let unjudged = (spending [output 1] 900) {inputs = [TxIn (output 1) (saying "script") maxBound]} :: Tx
        publish p unjudged `shouldReturn` Unverified 0
        sent `shouldReturn` [0, 0]
        publish p {direct = \_ -> return Sent} unjudged `shouldReturn` ToNode
        -- one that was verified goes to the peers when RPC does not answer
        publish p tx `shouldReturn` ToPeers 2
        sent `shouldReturn` [1, 1]
    it "says so when it has no peer, or reaches none" $ do
      desk 0 taking $ \p _ asked -> do
        publish p tx `shouldReturn` NoPeers
        asked `shouldReturn` []
      desk 2 (\_ _ -> NoConnection) $ \p sent _ -> do
        publish p tx `shouldReturn` NoPeers
        sent `shouldReturn` [1, 1]
    it "counts the peers that took it, and gives the reason when every one refused it" $ do
      desk 3 (\k _ -> if k == 1 then Rejected "too cheap" else NoAnswer) $ \p _ _ ->
        publish p tx `shouldReturn` ToPeers 2
      desk 2 (\_ _ -> Rejected "too cheap") $ \p _ asked -> do
        publish p tx `shouldReturn` Refused "too cheap"
        asked `shouldReturn` []
      desk 2 (\k _ -> if k == 0 then Closed else NoConnection) $ \p _ _ ->
        publish p tx >>= (`shouldSatisfy` \case Refused _ -> True; _ -> False)

  describe "asking for it back" $ do
    let at n = posixSecondsToUTCTime (fromInteger n)
        h n = txHash (spending [output n] 1)
    it "asks for what is due, and keeps it on the schedule for later" $ do
      let m = HashMap.fromList [(h 1, (at 10, [25])), (h 2, (at 20, []))]
          (ready, after) = due (at 10) m
      ready `shouldBe` [h 1]
      HashMap.lookup (h 1) after `shouldBe` Just (at 35, [])
      HashMap.lookup (h 2) after `shouldBe` Just (at 20, [])
    it "forgets a transaction after its last time" $ do
      let (ready, after) = due (at 30) (HashMap.fromList [(h 1, (at 30, []))])
      ready `shouldBe` [h 1]
      after `shouldBe` HashMap.empty
    it "moves a transaction on its schedule only once it was asked for" $ do
      let m = HashMap.fromList [(h 1, (at 10, [25])), (h 2, (at 10, []))]
          once = asked (at 10) (h 1) m
      -- the second, not asked for yet, is still due
      fst (due (at 10) once) `shouldBe` [h 2]
      HashMap.lookup (h 1) once `shouldBe` Just (at 35, [])
      -- at its last delay, it goes once asked for
      HashMap.lookup (h 2) (asked (at 10) (h 2) once) `shouldBe` Nothing
    it "asks past the two minutes in which nodes give back what they took" $
      sum askAgain `shouldSatisfy` (> 120)
