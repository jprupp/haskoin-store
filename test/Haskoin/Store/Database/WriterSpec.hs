module Haskoin.Store.Database.WriterSpec (spec) where

import Control.Exception (IOException, throwIO, try)
import Control.Monad (forM_)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Default (def)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.IntMap.Strict qualified as IntMap
import Data.Maybe (fromMaybe, isNothing)
import Data.Serialize (encode)
import Data.Word (Word32, Word64)
import Database.RocksDB (columnFamilies)
import Database.RocksDB.Query (insertCF)
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
    UnixTime,
    Unspent (..),
  )
import Haskoin.Store.Database.Copy (Family (..), copyEntries, copySize)
import Haskoin.Store.Database.Mempool (MemRecords (..), mempoolRecords, summaryEntries)
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader (DatabaseReader (..), forgetMempoolCopy, knownMempoolCopy, mempoolCF)
import Haskoin.Store.Database.Types
  ( BalVal (..),
    MemAddrKey (..),
    MemAddrVal (..),
    MemDeletedKey (..),
    MemSummary (..),
    MemTxKey (..),
  )
import Haskoin.Store.Database.Writer
import Haskoin.Store.Histories (copyIsArea, dump, openLayered, openOlder)
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

  describe "mempoolAreaEmpty" $ do
    it "is true of a database with no mempool transaction, and of what version 21 left of deleted ones" $
      withDB $ \db -> do
        batch db mempoolAreaEmpty `shouldReturn` True
        insertCF db.db (mempoolCF db.db) (MemDeletedKey (txHash big)) (txData 0 big)
        forgetMempoolCopy db
        batch db mempoolAreaEmpty `shouldReturn` True
        -- and then nothing is asked of the area
        batch db (\w -> mempoolAreaEmpty w >> getMempoolTx w (txHash big)) `shouldReturn` Nothing
    it "is false as soon as the mempool lists a transaction, and never true without the cache" $
      withDB $ \db -> do
        batch db (mempoolAreaEmpty . withoutReadCache) `shouldReturn` False
        insertCF db.db (mempoolCF db.db) (MemTxKey (txHash big)) (7 :: UnixTime)
        forgetMempoolCopy db
        batch db mempoolAreaEmpty `shouldReturn` False
        batch db (\w -> mempoolAreaEmpty w >> inMempool w (txHash big)) `shouldReturn` True
    it "does not hide what the batch itself puts in the mempool" $
      withDB $ \db ->
        batch db $ \w -> do
          mempoolAreaEmpty w `shouldReturn` True
          mempoolUntouched w `shouldReturn` True
          insertMempoolTx w (txData 0 big)
          addToMempool w (txHash big) 7
          mempoolUntouched w `shouldReturn` False
          fmap (.tx) <$> getMempoolTx w (txHash big) `shouldReturn` Just big
          inMempool w (txHash big) `shouldReturn` True

  describe "the copy of the mempool area" $ do
    it "answers every read of the area as the database does, also for what the batch has changed" $
      withDB $ \db -> do
        let known = map payment [1 .. 6] <> map child [1 .. 4]
        batch db $ \w -> do
          forM_ [1 .. 4] $ \i -> pool w (100 + fromIntegral i) (payment i)
          forM_ [1, 2] $ \i -> pool w (200 + fromIntegral i) (child i)
        batch db $ \w -> do
          agree w known
          -- what the database has
          fmap (.tx) <$> getMempoolTx w (txHash (payment 2)) `shouldReturn` Just (payment 2)
          getMempoolSpenders w (txHash (payment 2)) 1
            `shouldReturn` IntMap.fromList [(0, Spender (txHash (child 2)) 0)]
          inMempool w (txHash (child 2)) `shouldReturn` True
          inMempool w (txHash (child 3)) `shouldReturn` False
          getMempool w `shouldReturn` [(202, txHash (child 2)), (201, txHash (child 1))] <> listed [4, 3, 2, 1]
          -- changes: one leaves, two come, one summary is set and one is
          -- deleted
          unpool w 202 (child 2)
          pool w 105 (payment 5)
          pool w 203 (child 3)
          insertMempoolAddr w (MemAddrSumKey (address 2)) (MemAddrSumVal (MemSummary 9 8 7 6 5 4 3))
          deleteMempoolAddr w (MemAddrSumKey (address 5))
          -- and they come before what the copy has, as before what the
          -- database has
          agree w known
          getMempoolTx w (txHash (child 2)) `shouldReturn` Nothing
          fmap (.tx) <$> getMempoolTx w (txHash (payment 5)) `shouldReturn` Just (payment 5)
          getMempoolSpender w (OutPoint (txHash (payment 2)) 0) `shouldReturn` Nothing
          getMempoolSpenders w (txHash (payment 2)) 1 `shouldReturn` IntMap.empty
          getMempoolSpenders w (txHash (payment 3)) 1
            `shouldReturn` IntMap.fromList [(0, Spender (txHash (child 3)) 0)]
          inMempool w (txHash (child 2)) `shouldReturn` False
          inMempool w (txHash (child 3)) `shouldReturn` True
          getMempoolAddr w (MemAddrSumKey (address 2))
            `shouldReturn` Just (MemAddrSumVal (MemSummary 9 8 7 6 5 4 3))
          getMempoolAddr w (MemAddrSumKey (address 5)) `shouldReturn` Nothing
          getMempool w
            `shouldReturn` [(203, txHash (child 3)), (201, txHash (child 1))] <> listed [5, 4, 3, 2, 1]
        -- and once the batch is written, the copy has what it changed
        copyIsArea "after the changes" db
        batch db $ \w -> do
          agree w known
          getMempoolTx w (txHash (child 2)) `shouldReturn` Nothing
          inMempool w (txHash (child 3)) `shouldReturn` True
          getMempoolAddr w (MemAddrSumKey (address 5)) `shouldReturn` Nothing
          getMempool w
            `shouldReturn` [(203, txHash (child 3)), (201, txHash (child 1))] <> listed [5, 4, 3, 2, 1]

    it "is what a batch reads, not the database, until it is told that something else wrote to the area" $
      withDB $ \db -> do
        let h = txHash (payment 9)
        batch db (\w -> pool w 101 (payment 1) >> mempoolAreaEmpty w) `shouldReturn` True
        copyIsArea "after a batch" db
        -- behind the back of the batches
        insertCF db.db (mempoolCF db.db) (MemTxKey h) (7 :: UnixTime)
        batch db (`inMempool` h) `shouldReturn` False
        batch db ((`inMempool` h) . withoutReadCache) `shouldReturn` True
        forgetMempoolCopy db
        isNothing <$> knownMempoolCopy db `shouldReturn` True
        batch db (`inMempool` h) `shouldReturn` True
        copyIsArea "after it was loaded again" db

    it "is not loaded by a batch that reads the database every time" $
      withDB $ \db -> do
        batch db $ \w -> do
          let plain = withoutReadCache w
          pool plain 101 (payment 1)
          fmap (.tx) <$> getMempoolTx plain (txHash (payment 1)) `shouldReturn` Just (payment 1)
          inMempool plain (txHash (payment 2)) `shouldReturn` False
          getMempoolSpenders plain (txHash (payment 1)) 1 `shouldReturn` IntMap.empty
        batch db $ \w -> unpool (withoutReadCache w) 101 (payment 1)
        isNothing <$> knownMempoolCopy db `shouldReturn` True
        Q.getMempool db `shouldReturn` []

    it "is left as it was by a batch that throws" $
      withDB $ \db -> do
        batch db (\w -> pool w 101 (payment 1) >> mempoolAreaEmpty w) `shouldReturn` True
        Just earlier <- knownMempoolCopy db
        let failing w = do
              pool w 102 (payment 2)
              unpool w 101 (payment 1)
              fmap (.tx) <$> getMempoolTx w (txHash (payment 2)) `shouldReturn` Just (payment 2)
              throwIO (userError "not this one")
        batch db failing `shouldThrow` anyIOException
        Just later <- knownMempoolCopy db
        forM_ [General, ByAddress] $ \f -> copyEntries f later `shouldBe` copyEntries f earlier
        copyIsArea "after a batch that threw" db
        batch db (\w -> inMempool w (txHash (payment 2))) `shouldReturn` False

    it "is forgotten when the write of a batch fails, and loaded again by the next batch" $
      -- a database without merge operators, which refuses a batch that
      -- has a merge record
      withSystemTempDirectory "haskoin-store-writer-" $ \dir -> openOlder btcRegTest dir $ \db -> do
        batch db (\w -> pool w 101 (payment 1) >> mempoolAreaEmpty w) `shouldReturn` True
        copyIsArea "before the write that fails" db
        -- the merge record comes after the changes to the mempool area
        -- in the batch, and RocksDB 8.9 keeps what came before it
        let refused w = do
              pool w 102 (payment 2)
              unpool w 101 (payment 1)
              mergeSpender w (OutPoint (txHash (payment 7)) 0) (Spender (txHash (payment 8)) 0)
        batch db refused `shouldThrow` anyIOException
        -- so the copy is not trusted any more
        isNothing <$> knownMempoolCopy db `shouldReturn` True
        -- and the next batch reads the column families again, whatever
        -- RocksDB made of the batch. That batch may not be written, though
        -- it changes nothing: RocksDB 11 takes no write after such a
        -- failure until the database is opened again
        seen <- newIORef []
        let reading w = do
              agree w (map payment [1, 2])
              mapM (inMempool w . txHash . payment) [1, 2] >>= writeIORef seen
        _ <- try (batch db reading) :: IO (Either IOException ())
        listedNow <- map snd <$> Q.getMempool db
        readIORef seen `shouldReturn` map ((`elem` listedNow) . txHash . payment) [1, 2]
        copyIsArea "after the write that failed" db

    it "is loaded from a database that is opened with a mempool in it" $
      withSystemTempDirectory "haskoin-store-writer-" $ \dir -> do
        openLayered btcRegTest dir $ \db ->
          batch db $ \w -> forM_ [1 .. 5] (\i -> pool w (100 + fromIntegral i) (payment i))
        openLayered btcRegTest dir $ \db -> do
          isNothing <$> knownMempoolCopy db `shouldReturn` True
          batch db $ \w -> do
            mempoolAreaEmpty w `shouldReturn` False
            fmap (.tx) <$> getMempoolTx w (txHash (payment 3)) `shouldReturn` Just (payment 3)
            agree w (map payment [1 .. 6])
          copyIsArea "after it was opened" db
          Just copy <- knownMempoolCopy db
          fst (copySize copy) `shouldSatisfy` (>= 25)

    it "follows a large batch, whose changes are packed on all cores, with values of any size" $
      withDB $ \db -> do
        batch db mempoolAreaEmpty `shouldReturn` True
        -- a hundred payments to addresses, four hundred transactions of
        -- three kilobytes, one with a script too long to be copied into a
        -- shared buffer and one larger than such a buffer: the copy takes
        -- their bytes out of the buffers
        let many = map payment [11 .. 110] <> [wide i | i <- [1 .. 400]] <> [long, big]
        report <- snd <$> runWriterReport btcRegTest db quiet (\w -> forM_ many (pool w 300))
        report.operations `shouldSatisfy` (> 2300)
        copyIsArea "after a large batch" db
        batch db (\w -> agree w (take 5 many <> [wide 1, long, big]))
        -- and a large batch of deletes
        gone <- snd <$> runWriterReport btcRegTest db quiet (\w -> forM_ (drop 100 many) (unpool w 300))
        gone.operations `shouldSatisfy` (> 1200)
        copyIsArea "after a large batch of deletes" db
        length <$> Q.getMempool db `shouldReturn` 100

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

-- | Put a transaction in the mempool area at a time, as the rules do when
-- it enters the mempool, without their checks: its body, its spenders, its
-- entries by address with a summary for each address, and its place in
-- the list.
pool :: Writer -> UnixTime -> Tx -> IO ()
pool w time tx = do
  insertMempoolTx w r.body
  forM_ r.spends (uncurry (insertMempoolSpender w))
  forM_ r.addresses (uncurry (insertMempoolAddr w))
  forM_ (summaryEntries r.addresses) (uncurry (insertMempoolAddr w))
  addToMempool w r.txid r.time
  where
    r = mempoolRecords btcRegTest time (txData 0 tx)

-- | Take out what 'pool' put in.
unpool :: Writer -> UnixTime -> Tx -> IO ()
unpool w time tx = do
  deleteMempoolTx w r.txid
  forM_ r.spends (deleteMempoolSpender w . fst)
  forM_ r.addresses (deleteMempoolAddr w . fst)
  forM_ (summaryEntries r.addresses) (deleteMempoolAddr w . fst)
  deleteFromMempool w r.txid
  where
    r = mempoolRecords btcRegTest time (txData 0 tx)

-- | Every read of the mempool area gives the same through a batch, which
-- reads the copy of the area, and through the same batch without its read
-- cache, which reads the database: for these transactions, their outputs,
-- the outputs that they spend and their addresses, and for the list of
-- the mempool.
agree :: Writer -> [Tx] -> IO ()
agree w txs = do
  forM_ txs $ \tx -> do
    let h = txHash tx
        r = mempoolRecords btcRegTest 0 (txData 0 tx)
        outputs = [OutPoint h i | i <- [0 .. 2]]
    both (`getMempoolTx` h)
    both (`inMempool` h)
    both (\x -> getMempoolSpenders x h 3)
    forM_ (outputs <> map fst r.spends) $ \p -> both (`getMempoolSpender` p)
    forM_ (summaryEntries r.addresses) $ \(k, _) -> both (`getMempoolAddr` k)
    -- the entries themselves, at the times that the tests use
    forM_ r.addresses $ \case
      (MemAddrTxKey a _ _, _) -> forM_ times $ \t -> both (`getMempoolAddr` MemAddrTxKey a t h)
      (MemAddrOutKey a _ p, _) -> forM_ times $ \t -> both (`getMempoolAddr` MemAddrOutKey a t p)
      (k, _) -> both (`getMempoolAddr` k)
  both getMempool
  where
    times = [101 .. 105] <> [201 .. 203] <> [300]
    both :: (Eq a, Show a) => (Writer -> IO a) -> IO ()
    both f = do
      copied <- f w
      read' <- f (withoutReadCache w)
      copied `shouldBe` read'

-- | The entries of the list of the mempool of 'payment's put there at the
-- time of their number.
listed :: [Word32] -> [(UnixTime, TxHash)]
listed is = [(100 + fromIntegral i, txHash (payment i)) | i <- is]

-- | A transaction that spends the output of the 'payment' of a number.
child :: Word32 -> Tx
child i = Tx 2 [TxIn (OutPoint (txHash (payment i)) 0) "" maxBound] [TxOut 800 (scriptOf (i + 100))] [] 0

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
