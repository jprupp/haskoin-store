{-# LANGUAGE DeriveAnyClass #-}

module Haskoin.Store.Database.MigrateSpec (spec) where

import Control.Monad (foldM, forM_, void, when)
import Data.ByteString qualified as B
import Data.Either (rights)
import Data.List (sort)
import Data.Maybe (catMaybes)
import Data.Word (Word32)
import Database.RocksDB (BatchOp (..))
import Database.RocksDB.Query (insert, insertOp, retrieve, writeBatch)
import Haskoin
  ( Ctx,
    Network,
    OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    btcRegTest,
    prepareContext,
    scriptToAddressBS,
    sha256,
    txHash,
  )
import Haskoin.Store.Data (Balance (..), BlockRef (..), TxData (..))
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Migrate
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    mempoolAddrCF,
    mempoolCF,
    mempoolInsertOps,
  )
import Haskoin.Store.Database.Types (MigrationKey (..), VersionKey (..))
import Haskoin.Store.Histories
import Haskoin.Store.TestBlocks (script)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck (forAll, ioProperty, property, tabulate, vectorOf)
import UnliftIO (Exception, throwIO, try, withSystemTempDirectory)

spec :: Spec
spec = prepareContext $ \ctx -> describe "migration to the layered layout" $ do
  it "takes the mempool out of the chain's indices, keeping every answer" $
    withSystemTempDirectory "haskoin-store-migrate-" $ \dir -> do
      -- A coin of a block; a payment from it in the mempool, with change;
      -- and a transaction that spends that change, also in the mempool.
      (hist, old) <- mixedDatabase ctx dir $ \db hist0 -> do
        let payment = Tx 2 [TxIn (coinOf hist0) "" maxBound] [TxOut 3000 (p2pkh 1), TxOut 2000 script] [] 0
            child = Tx 2 [TxIn (OutPoint (txHash payment) 1) "" maxBound] [TxOut 1500 (p2pkh 2)] [] 0
        foldM (addTx db) hist0 [payment, child]
      (miner, one, two) <- case rights (map (scriptToAddressBS net ctx) [script, p2pkh 1, p2pkh 2]) of
        [a, b, c] -> return (a, b, c)
        _ -> fail "a script has no address"
      let numbers bs a = fmap (\b -> (b.confirmed, b.unconfirmed, b.utxo, b.txs, b.received)) (lookup a bs)
      -- the coin is spent in the mempool: it no longer counts as confirmed
      numbers old.balances miner `shouldBe` Just (0, 0, 0, 3, 5000002000)
      numbers old.balances one `shouldBe` Just (0, 3000, 1, 1, 3000)
      numbers old.balances two `shouldBe` Just (0, 1500, 1, 1, 1500)
      openLayered net ctx dir $ \db -> do
        migrateLayered db `shouldReturn` Migration {moved = 2, batches = 1, checked = 2}
        retrieve db.db VersionKey `shouldReturn` Just layeredVersion
        (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Nothing
        -- the chain has the coin again, whole and unspent
        chainBalance <- fmap (\b -> (b.confirmed, b.unconfirmed, b.utxo, b.txs, b.received)) <$> Q.getBalance db miner
        chainBalance `shouldBe` Just (5000000000, 0, 1, 1, 5000000000)
        Q.getBalance db one `shouldReturn` Nothing
        Q.getBalance db two `shouldReturn` Nothing
        -- and with the mempool on top, every answer is what it was
        layered db (ask hist) `shouldReturn` old
        -- a second run finds nothing to do
        migrateLayered db `shouldReturn` Migration {moved = 0, batches = 0, checked = 0}

  modifyMaxSuccess (const 50) $
    it "keeps every answer and leaves the chain as if the mempool had been wiped, for random histories" $
      property . forAll ((,) <$> vectorOf 45 anyNumber <*> vectorOf 12 anyNumber) $ \(seeds, last') -> ioProperty $ do
        withSystemTempDirectory "haskoin-store-migrate-" $ \dir -> do
          -- anything at first, then payments, so that the mempool has something
          (hist, old) <- mixedDatabase ctx dir $ \db hist0 -> foldM (step db) hist0 (seeds <> onlyPayments last')
          withSystemTempDirectory "haskoin-store-chain-" $ \dir' ->
            open20 net ctx dir' $ \fresh -> do
              -- the same blocks, in a database that never saw the mempool
              void (run fresh mixedRules.initBest) :: IO ()
              forM_ (reverse hist.chain) $ \(block, node) ->
                run fresh (\w -> mixedRules.importBlock w block node) >>= either (fail . show) (\_ -> return ())
              openLayered net ctx dir $ \db -> do
                done <- migrateLayeredWith defaultMigrationOptions {chunk = 3} db
                done.checked `shouldBe` length old.mempool
                done.moved `shouldBe` length old.mempool
                -- every answer is what it was
                layered db (ask hist) `shouldReturn` old
                -- the chain's column families are those of the fresh database,
                -- but for the times at which transactions were first seen
                migrated <- chainOf db
                expected <- chainOf fresh
                length expected.addressTxs `shouldSatisfy` (>= length hist.chain)
                migrated {firstSeen = []} `shouldBe` expected {firstSeen = []}
                -- the mempool area is the keys of its transactions, each worked
                -- out from that transaction alone: no more and no less
                moved <- catMaybes <$> mapM (M.getMempoolTx db . snd) old.mempool
                let records = [M.mempoolRecords net ctx time td | td <- moved, MemRef time <- [td.block]]
                length records `shouldBe` length old.mempool
                dump db (mempoolCF db.db) `shouldReturn` sort (concatMap (fst . M.recordEntries) records)
                dump db (mempoolAddrCF db.db) `shouldReturn` sort (M.addressArea records)
                -- without the mempool area, the answers are those of the chain
                void (M.wipeMempoolArea db)
                wiped <- layered db (ask hist)
                alone <- mixed fresh (ask hist)
                (wiped.balances, wiped.addressTxs, wiped.addressUnspents, wiped.unspents, wiped.mempool)
                  `shouldBe` (alone.balances, alone.addressTxs, alone.addressUnspents, alone.unspents, [])
          let inMempool = [td | (_, Just td) <- old.txs, not td.deleted, MemRef {} <- [td.block]]
              unconfirmed = map (txHash . (.tx)) inMempool
              chained = [() | td <- inMempool, i <- td.tx.inputs, i.outpoint.hash `elem` unconfirmed]
              bucket n
                | n == 0 = "none"
                | n <= 3 = "1 to 3"
                | n <= 9 = "4 to 9"
                | otherwise = "10 or more" :: String
          return
            . tabulate "mempool transactions" [bucket (length inMempool)]
            . tabulate "of them spending another" [bucket (length chained)]
            . tabulate "deleted transactions" [bucket (length [() | (_, Just td) <- old.txs, td.deleted])]
            $ tabulate "blocks" [bucket (length hist.chain)] True

  it "carries on after being interrupted, between any two writes" $
    forM_ [1 .. 6] $ \stopAt ->
      withSystemTempDirectory "haskoin-store-migrate-" $ \dir -> do
        (hist, old) <- mixedDatabase ctx dir $ \db hist0 -> foldM (addTx db) hist0 (payments (coinOf hist0))
        length old.mempool `shouldBe` 8
        -- one transaction in each write, and the plug pulled after a few
        let pull n = when (n == stopAt) (throwIO Interrupted)
            interrupted = defaultMigrationOptions {chunk = 1, afterBatch = pull}
        openLayered net ctx dir $ \db -> do
          try (migrateLayeredWith interrupted db) `shouldReturn` Left Interrupted
          -- a store of version 20 would refuse the database from here on
          retrieve db.db VersionKey `shouldReturn` Just layeredVersion
          (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Just 20
        openLayered net ctx dir $ \db -> do
          done <- migrateLayered db
          done.moved `shouldBe` length old.mempool - stopAt
          done.checked `shouldBe` length old.mempool
          (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Nothing
          layered db (ask hist) `shouldReturn` old

  it "refuses a database that it cannot handle, and leaves it untouched" $ do
    withSystemTempDirectory "haskoin-store-migrate-" $ \dir ->
      openLayered net ctx dir $ \db -> do
        migrateLayered db `shouldThrow` (== UnknownVersion Nothing)
        insert db.db VersionKey (19 :: Word32)
        migrateLayered db `shouldThrow` (== UnknownVersion (Just 19))
    withSystemTempDirectory "haskoin-store-migrate-" $ \dir -> do
      -- the mempool lists a transaction that has no body
      let lost = TxHash (sha256 ("lost" :: B.ByteString))
      _ <- mixedDatabase ctx dir $ \db hist0 -> do
        hist1 <- foldM (addTx db) hist0 (take 5 (payments (coinOf hist0)))
        writeBatch db.db (mempoolInsertOps db.db lost 7)
        return hist1
      openLayered net ctx dir $ \db -> do
        migrateLayered db `shouldThrow` \case
          Inconsistent _ -> True
          _ -> False
        retrieve db.db VersionKey `shouldReturn` Just (20 :: Word32)
        dump db (mempoolAddrCF db.db) `shouldReturn` []
        -- the mempool column family has its list and its index by time only
        written <- dump db (mempoolCF db.db)
        written `shouldSatisfy` all ((`elem` ["\x08", "\x0b"]) . B.take 1 . fst)

  it "brings a database of any version that the store knows to the layout it runs on" $ do
    -- a new database
    withSystemTempDirectory "haskoin-store-migrate-" $ \dir ->
      openLayered net ctx dir $ \db -> do
        upgradeDatabase (\_ -> return ()) db `shouldReturn` Migration {moved = 0, batches = 0, checked = 0}
        retrieve db.db VersionKey `shouldReturn` Just layeredVersion
    -- one of version 20 with a mempool, and then the same one again
    withSystemTempDirectory "haskoin-store-migrate-" $ \dir -> do
      (hist, old) <- mixedDatabase ctx dir $ \db hist0 ->
        foldM (addTx db) hist0 (take 5 (payments (coinOf hist0)))
      forM_ [5, 0] $ \moved ->
        openLayered net ctx dir $ \db -> do
          done <- upgradeDatabase (\_ -> return ()) db
          done.moved `shouldBe` moved
          retrieve db.db VersionKey `shouldReturn` Just layeredVersion
          layered db (ask hist) `shouldReturn` old
          M.wrongSummaries db `shouldReturn` []
    -- one of version 19, which had no index of the mempool by time
    withSystemTempDirectory "haskoin-store-migrate-" $ \dir -> do
      (hist, old) <- mixedDatabase ctx dir $ \db hist0 ->
        foldM (addTx db) hist0 (take 3 (payments (coinOf hist0)))
      openLayered net ctx dir $ \db -> do
        index <- dump db (mempoolCF db.db)
        writeBatch db.db $
          insertOp VersionKey (19 :: Word32)
            : [DelCF (mempoolCF db.db) k | (k, _) <- index, B.take 1 k == "\x0b"]
        done <- upgradeDatabase (\_ -> return ()) db
        done.moved `shouldBe` 3
        layered db (ask hist) `shouldReturn` old
    -- and one of a version that it does not know
    withSystemTempDirectory "haskoin-store-migrate-" $ \dir ->
      openLayered net ctx dir $ \db -> do
        insert db.db VersionKey (17 :: Word32)
        upgradeDatabase (\_ -> return ()) db `shouldThrow` (== UnknownVersion (Just 17))

net :: Network
net = btcRegTest

data Interrupted = Interrupted
  deriving (Eq, Show, Exception)

-- | Make a database of version 20 in a directory: the genesis block, a block
-- with a coin, and whatever the function does next. Gives the history, and
-- the answers of the database to every query about it.
mixedDatabase ::
  Ctx ->
  FilePath ->
  (DatabaseReader -> History -> IO History) ->
  IO (History, Answers)
mixedDatabase ctx dir go =
  open20 net ctx dir $ \db -> do
    hist <- start mixedRules db >>= go db
    answers <- mixed db (ask hist)
    return (hist, answers)

-- | Add a transaction to the mempool, by the rules of today's layout.
addTx :: DatabaseReader -> History -> Tx -> IO History
addTx db hist tx = record db hist op <$> apply mixedRules db op
  where
    op = Pay tx hist.clock
