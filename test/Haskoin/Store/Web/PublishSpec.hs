{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module Haskoin.Store.Web.PublishSpec (spec) where

import Control.Concurrent.STM (atomically, modifyTVar', newTVarIO, readTVarIO)
import Control.Monad (replicateM_)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Data.Serialize (encode)
import Data.Word (Word32, Word64, Word8)
import Haskoin hiding (Coin (..))
import Haskoin.Store.Web.Publish
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

-- | A peer, told apart by a number.
node :: Int -> SockAddr
node n = SockAddrInet 8333 (tupleToHostAddress (10, 0, 0, fromIntegral n))

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

-- | The front of the script of an output that holds tokens: the category,
-- the flags, the commitment if there is one, and the amount if not zero.
withTokens :: ByteString -> Maybe (Word8, ByteString) -> Word64 -> ByteString
withTokens category nft amount =
  B.concat
    [ "\xef",
      category,
      B.singleton (nftFlags .|. (if amount > 0 then 0x10 else 0)),
      maybe "" (\(_, c) -> if B.null c then "" else B.singleton (fromIntegral (B.length c)) <> c) nft,
      if amount > 0 then compact amount else "",
      "\x76\xa9\x14" <> B.replicate 20 7 <> "\x88\xac"
    ]
  where
    nftFlags = case nft of
      Nothing -> 0
      Just (capability, c) -> 0x20 .|. capability .|. (if B.null c then 0 else 0x40)
    compact v
      | v < 0xfd = B.singleton (fromIntegral v)
      | otherwise = "\xfd" <> B.pack [fromIntegral v, fromIntegral (v `div` 256)]
    a .|. b = a + b

spec :: Spec
spec = do
  let check = judge rulesOf
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
          input n item = TxIn (output n) (saying item) maxBound
      check (signedBy ["good", "forged"]) [held 1000, held 1000] `shouldBe` Left (Forged 1)
      check (signedBy ["good", "script"]) [held 1000, held 1000] `shouldBe` Right (Just 1)
      check (signedBy ["good", "good"]) [held 1000, held 1000] `shouldBe` Right Nothing
    it "refuses a version that nodes do not take: 1 to 3 on Bitcoin, 1 and 2 on Bitcoin Cash" $ do
      let versioned v = (spending [output 1] 900) {version = v} :: Tx
          cash = judge rulesOf {net = bch}
      check (versioned 3) [held 1000] `shouldBe` Right Nothing
      check (versioned 0) [held 1000] `shouldBe` Left (BadVersion 0)
      check (versioned 4) [held 1000] `shouldBe` Left (BadVersion 4)
      cash (versioned 2) [held 1000] `shouldBe` Right Nothing
      cash (versioned 3) [held 1000] `shouldBe` Left (BadVersion 3)
    it "refuses an input script that does more than push, or pushes in a longer form than it needs" $ do
      let scripted ops = (spending [output 1] 900) {inputs = [TxIn (output 1) (encode (Script ops)) maxBound]} :: Tx
          signature = B.replicate 71 1
      check (scripted [opPushData signature, opPushData (B.replicate 33 2)]) [held 1000] `shouldBe` Right Nothing
      -- a push of 71 bytes takes one byte for its length, not two
      check (scripted [OP_PUSHDATA signature OPDATA1]) [held 1000] `shouldBe` Left (NotMinimal 0)
      -- the number 1 is OP_1, and the empty string OP_0
      check (scripted [OP_PUSHDATA "\x01" OPCODE]) [held 1000] `shouldBe` Left (NotMinimal 0)
      check (scripted [OP_0, OP_1, OP_1NEGATE]) [held 1000] `shouldBe` Right Nothing
      check (scripted [opPushData signature, OP_DUP]) [held 1000] `shouldBe` Left (NotPushOnly 0)
      -- bytes that are not a script at all
      check ((spending [output 1] 900) {inputs = [TxIn (output 1) "\x4c" maxBound]} :: Tx) [held 1000]
        `shouldBe` Left (NotPushOnly 0)

  describe "the tokens of Bitcoin Cash" $ do
    let category = B.replicate 32 9
        -- a category made by this transaction: the id of what its first
        -- input spends, whose index is 0
        newCategory = encode (OutPoint (TxHash (doubleSHA256 ("genesis" :: ByteString))) 0).hash
        from n = OutPoint (output n).hash n
        tx ins outs = (spending ins 0) {outputs = [TxOut 1000 s | s <- outs]} :: Tx
        coin s = TxOut 1000 s
        plain = "\x76\xa9\x14" <> B.replicate 20 7 <> "\x88\xac"
        fungible c n = withTokens c Nothing n
        nft c capability commitment = withTokens c (Just (capability, commitment)) 0
    it "lets tokens through as the inputs hold them" $ do
      tokenRules (tx [from 1] [fungible category 400, fungible category 600]) [coin (fungible category 1000)]
        `shouldBe` Right ()
      -- burnt: fewer out than in
      tokenRules (tx [from 1] [fungible category 10]) [coin (fungible category 1000)] `shouldBe` Right ()
      tokenRules (tx [from 1] [plain]) [coin plain] `shouldBe` Right ()
    it "refuses more fungible tokens out than in" $
      tokenRules (tx [from 1, from 2] [fungible category 2000]) [coin (fungible category 1000), coin plain]
        `shouldBe` Left (TokensUnbacked (concatMap (const "09") [1 .. 32 :: Int]))
    it "makes tokens of a new category at will, from an input that spends an output 0" $ do
      let genesisId = TxHash (doubleSHA256 ("genesis" :: ByteString))
          genesis = (tx [] [fungible newCategory 5000, nft newCategory 2 ""])
            {inputs = [TxIn (OutPoint genesisId 0) "" maxBound]} :: Tx
      tokenRules genesis [coin plain] `shouldBe` Right ()
      -- the same category from an input that spends an output 1 is not new
      let notNew = genesis {inputs = [TxIn (OutPoint genesisId 1) "" maxBound]} :: Tx
      tokenRules notNew [coin plain] `shouldSatisfy` either (const True) (const False)
    it "refuses tokens not written as tokens are" $
      tokenRules (tx [from 1] ["\xef\x01"]) [coin plain] `shouldBe` Left (BadTokens 0)
    it "keeps non-fungible tokens to what the inputs allow" $ do
      let keep = tx [from 1] [nft category 0 "a"]
      -- the same immutable token passes on; another commitment does not
      tokenRules keep [coin (nft category 0 "a")] `shouldBe` Right ()
      tokenRules keep [coin (nft category 0 "b")] `shouldSatisfy` either (const True) (const False)
      -- a mutable one becomes any one token, not two
      tokenRules keep [coin (nft category 1 "x")] `shouldBe` Right ()
      tokenRules (tx [from 1] [nft category 0 "a", nft category 0 "b"]) [coin (nft category 1 "x")]
        `shouldSatisfy` either (const True) (const False)
      -- a minting one makes any, and only a minting one makes one that mints
      tokenRules (tx [from 1] [nft category 0 "a", nft category 2 "b"]) [coin (nft category 2 "")]
        `shouldBe` Right ()
      tokenRules (tx [from 1] [nft category 2 "b"]) [coin (nft category 1 "")]
        `shouldSatisfy` either (const True) (const False)

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
    it "asks past the two minutes in which nodes give back what they took" $
      sum askAgain `shouldSatisfy` (> 120)
