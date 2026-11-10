module Haskoin.Store.Web.EventsSpec (spec) where

import Control.Concurrent.STM (atomically, newTVarIO, orElse, readTVarIO, stateTVar)
import Data.ByteString qualified as B
import Data.Bytes.Put (runPutS)
import Data.Bytes.Serial (serialize)
import Data.Foldable (toList)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet qualified as HashSet
import Data.Maybe (fromMaybe)
import Data.Word (Word32, Word8)
import Haskoin
import Haskoin.Store.Common (deriveFunction)
import Haskoin.Store.Data
import Haskoin.Store.Web.Events
import Test.Hspec

-- | An address that nothing else here has.
address :: Word8 -> Address
address n = PubKeyAddress (addressHash (B.replicate 33 n))

-- | A transaction that pays to addresses, told apart by a number.
paying :: Ctx -> Word32 -> [Address] -> Seen
paying ctx n to =
  Seen {tx, addresses = HashSet.fromList to, size = 1000}
  where
    tx = Tx {version = 1, inputs = [], outputs = map out to, witness = [], locktime = n}
    out a = TxOut {value = 1000, script = fromMaybe (error "No script") (addressToScriptBS ctx a)}

-- | A key, and its address at a place on a chain.
key :: Ctx -> DeriveType -> XPubSpec
key ctx deriv = XPubSpec {key = deriveXPubKey ctx (makeXPrvKey (B.replicate 32 7)), deriv}

keyAddress :: Ctx -> XPubSpec -> Word32 -> Word32 -> Address
keyAddress ctx x chain = deriveFunction ctx x.deriv (pubSubKey ctx x.key chain)

-- | A balance of a key's address that has been paid to.
used :: Ctx -> XPubSpec -> Word32 -> Word32 -> XPubBal
used ctx x chain i =
  XPubBal
    { path = [chain, i],
      balance =
        Balance
          { address = keyAddress ctx x chain i,
            confirmed = 1,
            unconfirmed = 0,
            utxo = 1,
            txs = 1,
            received = 1
          }
    }

spec :: Spec
spec = prepareContext $ \ctx -> do
  describe "log of events" $ do
    it "gives a reader what came after its place, and waits at the end" $ do
      events <- newEventLog
      let one = paying ctx 1 [address 1]
          h1 = txHash one.tx
          block = BlockHash (txHash (paying ctx 2 []).tx).get
      start <- atomically (eventsEnd events)
      atomically $ do
        appendEvent events (TxEntered h1 (Just one))
        appendEvent events (BlockArrived block)
        appendEvent events (TxLeft h1)
      Just (Just read') <- atomically ((Just <$> readEvents events start) `orElse` return Nothing)
      map (.event) (toList read') `shouldBe` [EventTx h1, EventBlock block, EventTx h1]
      -- a transaction that leaves is known as it was when it entered
      map (.seen) (toList read') `shouldBe` [Just one, Nothing, Just one]
      end <- atomically (eventsEnd events)
      waiting <- atomically ((Just <$> readEvents events end) `orElse` return Nothing)
      fmap (fmap length) waiting `shouldBe` Nothing
    it "takes up after the first event about a hash, and not after one it does not have" $ do
      events <- newEventLog
      let seen n = paying ctx n [address 1]
          hashes = map (txHash . (.tx) . seen) [1 .. 3]
          h1 = hashes !! 0
          h2 = hashes !! 1
      atomically $ do
        mapM_ (\n -> appendEvent events (TxEntered (txHash (seen n).tx) (Just (seen n)))) [1 .. 3]
        appendEvent events (TxLeft h1)
      Just from <- atomically (eventsAfter events Nothing (Just h1))
      Just later <- atomically (readEvents events from)
      map (.event) (toList later) `shouldBe` map EventTx [h2, hashes !! 2, h1]
      -- a hash is that of a block or of a transaction, as the caller reads it
      atomically (eventsAfter events (Just (BlockHash h1.get)) Nothing) `shouldReturn` Nothing
      atomically (eventsAfter events Nothing (Just (txHash (seen 9).tx))) `shouldReturn` Nothing
    it "forgets the oldest events, and tells a reader that fell off the start" $ do
      events <- newEventLog
      let seen n = paying ctx n [address 1]
          enter n = appendEvent events (TxEntered (txHash (seen n).tx) (Just (seen n)))
          extra = 5
      atomically (mapM_ enter [1 .. fromIntegral logLength + extra])
      atomically (readEvents events 0) `shouldReturn` Nothing
      Just kept <- atomically (readEvents events (fromIntegral extra))
      length kept `shouldBe` logLength
      atomically (eventsAfter events Nothing (Just (txHash (seen 1).tx))) `shouldReturn` Nothing
      -- the transaction of a forgotten event is not known when it leaves
      atomically (appendEvent events (TxLeft (txHash (seen 1).tx)))
      end <- atomically (eventsEnd events)
      Just left <- atomically (readEvents events (end - 1))
      map (.seen) (toList left) `shouldBe` [Nothing]
    it "gets shorter when its transactions are large" $ do
      events <- newEventLog
      let large :: Word32 -> Seen
          large n =
            let s = paying ctx n [address 1]
             in Seen {tx = s.tx, addresses = s.addresses, size = logWeight `div` 4}
      atomically (mapM_ (\n -> appendEvent events (TxEntered (txHash (large n).tx) (Just (large n)))) [1 .. 10])
      end <- atomically (eventsEnd events)
      atomically (readEvents events (end - 5)) `shouldReturn` Nothing
      Just kept <- atomically (readEvents events (end - 4))
      length kept `shouldBe` 4

  describe "choice of a client" $ do
    let gap = 3 :: Word32
        unused _ = return [] :: IO [XPubBal]
        told c var seen = atomically (stateTVar var (relevant c gap seen))
    it "is everything until the client chooses, and again when it says so" $ do
      var <- newTVarIO everything
      told ctx var (Just (paying ctx 1 [address 1])) `shouldReturn` True
      command ctx gap unused var (EventWatch [address 2]) `shouldReturn` Right ()
      told ctx var (Just (paying ctx 1 [address 1])) `shouldReturn` False
      told ctx var (Just (paying ctx 2 [address 1, address 2])) `shouldReturn` True
      -- what the server does not know is told
      told ctx var Nothing `shouldReturn` True
      command ctx gap unused var (EventUnwatch [address 2]) `shouldReturn` Right ()
      told ctx var (Just (paying ctx 2 [address 1, address 2])) `shouldReturn` False
      command ctx gap unused var EventAll `shouldReturn` Right ()
      readTVarIO var `shouldReturn` everything
      -- blocks alone, which are events without a transaction
      command ctx gap unused var EventNone `shouldReturn` Right ()
      told ctx var (Just (paying ctx 1 [address 1])) `shouldReturn` False
      told ctx var Nothing `shouldReturn` True
    it "watches a key up to its gap, which moves on with its payments" $ do
      var <- newTVarIO everything
      let x = key ctx DeriveNormal
          pay chain i = Just (paying ctx (chain * 1000 + i) [keyAddress ctx x chain i])
      -- used up to the second address of the external chain
      command ctx gap (\_ -> return [used ctx x 0 1]) var (EventWatchXPub x) `shouldReturn` Right ()
      w <- readTVarIO var
      HashMap.lookup x w.keys `shouldBe` Just (5, 3)
      HashMap.size w.derived `shouldBe` 8
      told ctx var (pay 0 5) `shouldReturn` False
      told ctx var (pay 1 3) `shouldReturn` False
      told ctx var (pay 0 4) `shouldReturn` True
      -- the payment to the fifth address moved the gap to the eighth
      told ctx var (pay 0 7) `shouldReturn` True
      told ctx var (pay 0 11) `shouldReturn` False
      told ctx var (pay 1 2) `shouldReturn` True
      told ctx var (pay 1 5) `shouldReturn` True
      -- the same key with another type of address is another key
      let y = key ctx DeriveP2WPKH
          other = Just (paying ctx 77 [keyAddress ctx y 0 0])
      told ctx var other `shouldReturn` False
      command ctx gap unused var (EventWatchXPub y) `shouldReturn` Right ()
      told ctx var other `shouldReturn` True
      command ctx gap unused var (EventUnwatchXPub x) `shouldReturn` Right ()
      told ctx var (pay 0 0) `shouldReturn` False
      told ctx var other `shouldReturn` True
    it "matches a Bloom filter as a node does, next to the addresses" $ do
      var <- newTVarIO everything
      let element = \case
            PubKeyAddress h -> runPutS (serialize h)
            _ -> error "Not the hash of a key"
          filter' = bloomInsert (bloomCreate 10 0.0000001 0 BloomUpdateAll) (element (address 1))
          one = paying ctx 1 [address 1]
          two = paying ctx 2 [address 2]
          three = paying ctx 3 [address 3]
      command ctx gap unused var (EventFilterAdd "x") `shouldReturn` Left "No filter to add to"
      command ctx gap unused var (EventFilterLoad filter') `shouldReturn` Right ()
      command ctx gap unused var (EventWatch [address 3]) `shouldReturn` Right ()
      told ctx var (Just one) `shouldReturn` True
      told ctx var (Just two) `shouldReturn` False
      told ctx var (Just three) `shouldReturn` True
      -- the server does not add to the filter by itself
      (.bloom) <$> readTVarIO var `shouldReturn` Just filter'
      -- by the hash of the transaction, and by what the client adds
      command ctx gap unused var (EventFilterAdd (runPutS (serialize (txHash two.tx)))) `shouldReturn` Right ()
      told ctx var (Just two) `shouldReturn` True
      command ctx gap unused var EventFilterClear `shouldReturn` Right ()
      told ctx var (Just one) `shouldReturn` False
      told ctx var (Just three) `shouldReturn` True
    it "refuses more addresses or keys than a client may name" $ do
      var <- newTVarIO everything
      let numbered n = PubKeyAddress (addressHash (runPutS (serialize n)))
          many = map numbered [1 .. fromIntegral maxWatchAddresses :: Word32]
      command ctx gap unused var (EventWatch many) `shouldReturn` Right ()
      command ctx gap unused var (EventWatch [address 1]) `shouldReturn` Left "Too many addresses"
      -- a refused command changes nothing
      HashSet.size . (.plain) <$> readTVarIO var `shouldReturn` maxWatchAddresses
