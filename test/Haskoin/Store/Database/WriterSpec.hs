module Haskoin.Store.Database.WriterSpec (spec) where

import Control.Monad (forM_)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Default (def)
import Data.IntMap.Strict qualified as IntMap
import Data.Maybe (fromMaybe)
import Data.Serialize (encode)
import Data.Word (Word32, Word64)
import Database.RocksDB (columnFamilies)
import Haskoin
  ( Address,
    OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    btcRegTest,
    eitherToMaybe,
    scriptToAddressBS,
    sha256,
    txHash,
  )
import Haskoin.Node (LogLevel (..), Logger (..))
import Haskoin.Store.Data
  ( Balance (..),
    BlockRef (..),
    Spender (..),
    TxData (..),
    TxRef (..),
    Unspent (..),
  )
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader (DatabaseReader (..))
import Haskoin.Store.Database.Types (BalVal (..))
import Haskoin.Store.Database.Writer
import Haskoin.Store.Histories (dump, openLayered)
import Haskoin.Store.Integration.Temp (withSystemTempDirectory)
import Test.Hspec

spec :: Spec
spec = describe "a batch of writes" $ do
  describe "its read cache" $ do
    it "never hides a change of the batch, whether the key was read ahead or not" $
      withDB $ \db -> do
        batch db $ \w -> insertUnspent w (coin 1) >> setBalance w (balanceOf 1 10)
        batch db $ \w -> do
          -- one that the database has, one that it has not
          readAheadUnspents w [(coin 1).outpoint, (coin 2).outpoint]
          readAheadBalances w [address 1, address 2]
          getUnspent w (coin 1).outpoint `shouldReturn` Just (coin 1)
          getUnspent w (coin 2).outpoint `shouldReturn` Nothing
          getBalance w (address 1) `shouldReturn` Just (balanceOf 1 10)
          getBalance w (address 2) `shouldReturn` Nothing
          deleteUnspent w (coin 1).outpoint
          insertUnspent w (coin 2)
          setBalance w (balanceOf 1 20)
          setBalance w (balanceOf 2 30)
          -- reading ahead again changes nothing either
          readAheadUnspents w [(coin 1).outpoint, (coin 2).outpoint]
          readAheadBalances w [address 1, address 2]
          getUnspent w (coin 1).outpoint `shouldReturn` Nothing
          getUnspent w (coin 2).outpoint `shouldReturn` Just (coin 2)
          getBalance w (address 1) `shouldReturn` Just (balanceOf 1 20)
          getBalance w (address 2) `shouldReturn` Just (balanceOf 2 30)
          -- a key that was not read ahead is read when asked for
          getUnspent w (coin 3).outpoint `shouldReturn` Nothing
          insertUnspent w (coin 3)
          getUnspent w (coin 3).outpoint `shouldReturn` Just (coin 3)
        Q.getUnspent db (coin 1).outpoint `shouldReturn` Nothing
        Q.getUnspent db (coin 2).outpoint `shouldReturn` Just (coin 2)
        Q.getUnspent db (coin 3).outpoint `shouldReturn` Just (coin 3)
        Q.getBalance db (address 1) `shouldReturn` Just (balanceOf 1 20)

    it "reads the groups of spenders ahead, and adds to them" $
      withDB $ \db -> do
        let made = TxHash (sha256 ("made" :: ByteString))
            spender i = Spender (TxHash (sha256 (encode i))) i
        batch db $ \w -> insertSpender w (OutPoint made 1) (spender 1)
        batch db $ \w -> do
          readAheadSpenders w [OutPoint made 2, OutPoint made 40]
          insertSpender w (OutPoint made 2) (spender 2)
          insertSpender w (OutPoint made 40) (spender 40)
          getChainSpender w (OutPoint made 1) `shouldReturn` Just (spender 1)
        Q.getChainSpenders db made
          `shouldReturn` IntMap.fromList [(1, spender 1), (2, spender 2), (40, spender 40)]

    it "gives the same answers without the cache" $
      withDB $ \db -> do
        batch db $ \w -> insertUnspent w (coin 1)
        batch db $ \w -> do
          let plain = withoutReadCache w
          readsAhead plain `shouldBe` False
          readAheadUnspents plain [(coin 1).outpoint]
          getUnspent plain (coin 1).outpoint `shouldReturn` Just (coin 1)
          -- a change made through one is seen through the other
          deleteUnspent plain (coin 1).outpoint
          getUnspent w (coin 1).outpoint `shouldReturn` Nothing

  describe "its merges: changes to values that it has not read" $ do
    it "adds differences to a balance that it has not read, and shows the sum to its own reads" $
      withDB $ \db -> do
        batch db $ \w -> setBalance w (balanceOf 1 10)
        batch db $ \w -> do
          mergeBalance w (address 1) (def :: BalVal) {confirmed = 5, txs = 1}
          mergeBalance w (address 1) (def :: BalVal) {confirmed = negate 3, utxo = 2}
          mergeBalance w (address 2) (def :: BalVal) {received = 7, txs = 1}
          -- the database is read for the value, and the differences added
          getBalance w (address 1) `shouldReturn` Just (numbers 1 12 3 2 10)
          getBalance w (address 2) `shouldReturn` Just (numbers 2 0 0 1 7)
          getBalance w (address 3) `shouldReturn` Nothing
          -- and the same through a writer that remembers no read
          getBalance (withoutReadCache w) (address 1) `shouldReturn` Just (numbers 1 12 3 2 10)
        Q.getBalance db (address 1) `shouldReturn` Just (numbers 1 12 3 2 10)
        Q.getBalance db (address 2) `shouldReturn` Just (numbers 2 0 0 1 7)
        Q.getBalance db (address 3) `shouldReturn` Nothing
        -- one batch after another, each without a read
        batch db $ \w -> mergeBalance w (address 2) (def :: BalVal) {received = 1}
        batch db $ \w -> mergeBalance w (address 2) (def :: BalVal) {received = 1}
        Q.getBalance db (address 2) `shouldReturn` Just (numbers 2 0 0 1 9)

    it "applies differences to a balance that it has whole, set or deleted" $
      withDB $ \db -> do
        batch db $ \w -> setBalance w (balanceOf 1 10) >> setBalance w (balanceOf 2 20)
        batch db $ \w -> do
          setBalance w (numbers 1 100 1 1 100)
          mergeBalance w (address 1) (def :: BalVal) {confirmed = 1}
          getBalance w (address 1) `shouldReturn` Just (numbers 1 101 1 1 100)
          deleteBalance w (address 2)
          mergeBalance w (address 2) (def :: BalVal) {confirmed = 4, txs = 1}
          getBalance w (address 2) `shouldReturn` Just (numbers 2 4 0 1 0)
        Q.getBalance db (address 1) `shouldReturn` Just (numbers 1 101 1 1 100)
        -- from zero, not from the 20 that the batch deleted
        Q.getBalance db (address 2) `shouldReturn` Just (numbers 2 4 0 1 0)

    it "lets a whole balance take the place of the differences, which whoever set it had read" $
      withDB $ \db -> do
        batch db $ \w -> setBalance w (balanceOf 1 10) >> setBalance w (balanceOf 2 20)
        batch db $ \w -> do
          mergeBalance w (address 1) (def :: BalVal) {confirmed = 5}
          mergeBalance w (address 2) (def :: BalVal) {confirmed = 5}
          -- as the revert of a block does: read, change, write whole
          Just b <- getBalance w (address 1)
          b.confirmed `shouldBe` 15
          setBalance w (b :: Balance) {confirmed = b.confirmed - 1}
          getBalance w (address 1) `shouldReturn` Just (numbers 1 14 1 1 10)
          deleteBalance w (address 2)
          getBalance w (address 2) `shouldReturn` Nothing
          -- and differences after that are applied to the whole value
          mergeBalance w (address 1) (def :: BalVal) {confirmed = 1}
          getBalance w (address 1) `shouldReturn` Just (numbers 1 15 1 1 10)
        Q.getBalance db (address 1) `shouldReturn` Just (numbers 1 15 1 1 10)
        Q.getBalance db (address 2) `shouldReturn` Nothing

    it "adds spenders to groups that it has not read, and shows them to each of its reads" $
      withDB $ \db -> do
        let made = TxHash (sha256 ("made" :: ByteString))
            other = TxHash (sha256 ("other" :: ByteString))
            spender i = Spender (TxHash (sha256 (encode i))) i
            out = OutPoint made
        batch db $ \w -> insertSpender w (out 1) (spender 1) >> insertSpender w (out 40) (spender 40)
        batch db $ \w -> do
          mergeSpender w (out 2) (spender 2)
          mergeSpender w (out 3) (spender 3)
          mergeSpender w (out 17) (spender 17)
          mergeSpender w (OutPoint other 0) (spender 9)
          -- one output at a time: what the database has, and what is added
          getChainSpender w (out 1) `shouldReturn` Just (spender 1)
          getChainSpender w (out 2) `shouldReturn` Just (spender 2)
          getChainSpender w (out 17) `shouldReturn` Just (spender 17)
          getChainSpender w (out 4) `shouldReturn` Nothing
          getChainSpender w (OutPoint other 0) `shouldReturn` Just (spender 9)
          getChainSpender (withoutReadCache w) (out 3) `shouldReturn` Just (spender 3)
          -- and all those of a transaction, over its three groups
          getChainSpenders w made 48
            `shouldReturn` IntMap.fromList [(i, spender (fromIntegral i)) | i <- [1, 2, 3, 17, 40]]
          getChainSpenders w other 1 `shouldReturn` IntMap.fromList [(0, spender 9)]
        Q.getChainSpenders db made
          `shouldReturn` IntMap.fromList [(i, spender (fromIntegral i)) | i <- [1, 2, 3, 17, 40]]
        Q.getChainSpender db (OutPoint other 0) `shouldReturn` Just (spender 9)
        -- the newer spender wins for an output that already has one
        batch db $ \w -> mergeSpender w (out 2) (spender 22)
        Q.getChainSpender db (out 2) `shouldReturn` Just (spender 22)

    it "puts a spender in a group that it has whole, and lets a whole group take the place of what it added" $
      withDB $ \db -> do
        let made = TxHash (sha256 ("made" :: ByteString))
            spender i = Spender (TxHash (sha256 (encode i))) i
            out = OutPoint made
            group is = IntMap.fromList [(i, spender (fromIntegral i)) | i <- is]
        batch db $ \w -> mapM_ (\i -> insertSpender w (out i) (spender i)) [1, 20, 35]
        batch db $ \w -> do
          -- added to, then read and written whole, as a revert does
          mergeSpender w (out 2) (spender 2)
          deleteSpender w (out 1)
          getChainSpenders w made 48 `shouldReturn` group [2, 20, 35]
          -- whole, then added to
          insertSpender w (out 3) (spender 3)
          mergeSpender w (out 4) (spender 4)
          getChainSpenders w made 48 `shouldReturn` group [2, 3, 4, 20, 35]
          -- a group that the batch empties is deleted, and what is added
          -- afterwards is all that it has
          deleteSpender w (out 20)
          getChainSpender w (out 20) `shouldReturn` Nothing
          mergeSpender w (out 21) (spender 21)
          getChainSpenders w made 48 `shouldReturn` group [2, 3, 4, 21, 35]
          -- added to, then a spender put in by reading the group: what
          -- is written whole has what the database had and what was added
          mergeSpender w (out 33) (spender 33)
          insertSpender w (out 34) (spender 34)
          getChainSpender w (out 33) `shouldReturn` Just (spender 33)
          getChainSpenders w made 48 `shouldReturn` group [2, 3, 4, 21, 33, 34, 35]
        Q.getChainSpenders db made `shouldReturn` group [2, 3, 4, 21, 33, 34, 35]

  it "writes a large batch, packed on all cores, as it writes the same changes a few at a time" $
    withDB $ \large ->
      withDB $ \small -> do
        -- Two thousand unspent outputs and balances and as many
        -- transactions, then a thousand transactions of three kilobytes,
        -- which fill the buffer of each core more than once. Among them,
        -- so that the records around them have to keep their place, one
        -- with a script too long to be copied into a shared buffer and one
        -- larger than such a buffer.
        let changes =
              concat
                [ [ \w -> insertUnspent w (coin i),
                    \w -> setBalance w (balanceOf i i),
                    \w -> insertAddrTx w (address i) (TxRef (BlockRef 7 i) (txHash (payment i))),
                    \w -> insertAddrUnspent w (address i) (coin i),
                    \w -> insertTx w (txData i (payment i))
                  ]
                | i <- [1 .. 2000]
                ]
                <> [\w -> insertTx w (txData 1 long), \w -> insertTx w (txData 2 big)]
                <> [\w -> insertTx w (txData i (wide i)) | i <- [1 .. 1000]]
                -- and merge records, for balances and for groups of
                -- spenders that have no value yet: two for each key, which
                -- one batch makes one record of and forty at a time two
                <> concat
                  [ [ \w -> mergeBalance w (address i) (def :: BalVal) {confirmed = fromIntegral i, txs = 1},
                      \w -> mergeSpender w (OutPoint (txHash (payment i)) (i `mod` 40)) (Spender (txHash (wide i)) i)
                    ]
                  | i <- [5001 .. 5600] <> [5001 .. 5600]
                  ]
        report <- snd <$> runWriterReport btcRegTest large quiet (\w -> mapM_ ($ w) changes)
        report.operations `shouldBe` 12202
        -- a few at a time, a batch is not packed: each operation is handed
        -- to RocksDB by itself, as all of them were before
        forM_ (pieces 40 changes) $ \some -> batch small (\w -> mapM_ ($ w) some)
        forM_ (zip (columnFamilies large.db) (columnFamilies small.db)) $ \(a, b) -> do
          written <- dump large a
          expected <- dump small b
          length written `shouldBe` length expected
          written `shouldBe` expected
        entries <- concat <$> mapM (dump large) (columnFamilies large.db)
        length entries `shouldBe` 12202
        -- every change set a key of its own, or merged into one that had
        -- no value, where the one merge record that the batch makes of
        -- the two is the value. So the bytes of the batch are those of the
        -- keys and values that the database now has
        report.bytes `shouldBe` sum [B.length k + B.length v | (k, v) <- entries]
        report.bytes `shouldSatisfy` (> 3600000)
        Q.getBalance large (address 5001) `shouldReturn` Just (numbers 5001 10002 0 2 0)

quiet :: Logger
quiet = Logger {level = LevelError, source = "", push = \_ -> return ()}

withDB :: (DatabaseReader -> IO a) -> IO a
withDB action = withSystemTempDirectory "haskoin-store-writer-" $ \dir -> openLayered btcRegTest dir action

batch :: DatabaseReader -> (Writer -> IO a) -> IO a
batch db = runWriter btcRegTest db quiet

address :: Word32 -> Address
address i = fromMaybe (error "no address") (eitherToMaybe (scriptToAddressBS btcRegTest (scriptOf i)))

scriptOf :: Word32 -> ByteString
scriptOf i = "\x76\xa9\x14" <> B.take 20 (encode (sha256 (encode i))) <> "\x88\xac"

-- | An unspent output of a number, paid to the address of that number.
coin :: Word32 -> Unspent
coin i =
  Unspent
    { block = BlockRef 7 i,
      outpoint = OutPoint (TxHash (sha256 (encode i))) (i `mod` 3),
      value = 1000 + fromIntegral i,
      script = scriptOf i,
      address = Just (address i)
    }

-- | A balance of an address by its numbers: confirmed, unspent outputs,
-- transactions and received.
numbers :: Word32 -> Word64 -> Word64 -> Word64 -> Word64 -> Balance
numbers i confirmed utxo txs received =
  Balance {address = address i, confirmed, unconfirmed = 0, utxo, txs, received}

balanceOf :: Word32 -> Word32 -> Balance
balanceOf i n =
  Balance
    { address = address i,
      confirmed = fromIntegral n,
      unconfirmed = 0,
      utxo = 1,
      txs = 1,
      received = fromIntegral n
    }

payment :: Word32 -> Tx
payment i = Tx 2 [TxIn (coin i).outpoint "" maxBound] [TxOut 900 (scriptOf (i + 1))] [] 0

-- | A transaction with an output script of twenty kilobytes.
long :: Tx
long = Tx 2 [TxIn (coin 1).outpoint "" maxBound] [TxOut 1 (B.replicate 20000 0x6a)] [] 0

-- | A transaction of three kilobytes with a number: short enough to be
-- copied into a shared buffer, and a few dozen of them fill one.
wide :: Word32 -> Tx
wide i = Tx 2 [TxIn (OutPoint (TxHash (sha256 (encode i))) 7) "" maxBound] [TxOut 1 (B.replicate 3000 0x6a)] [] 0

-- | A transaction of four hundred kilobytes, in outputs of a kilobyte.
big :: Tx
big = Tx 2 [TxIn (coin 2).outpoint "" maxBound] (replicate 400 (TxOut 1 (B.replicate 1000 0x6a))) [] 0

txData :: Word32 -> Tx -> TxData
txData i tx =
  TxData
    { block = BlockRef 7 i,
      tx = tx,
      prevs = mempty,
      deleted = False,
      rbf = False,
      timestamp = 1700000000,
      spenders = mempty
    }

pieces :: Int -> [a] -> [[a]]
pieces _ [] = []
pieces n xs = let (a, b) = splitAt n xs in a : pieces n b
