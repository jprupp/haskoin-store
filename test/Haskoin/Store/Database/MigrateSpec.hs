{-# LANGUAGE DeriveAnyClass #-}

module Haskoin.Store.Database.MigrateSpec (spec) where

import Control.Monad (foldM, forM_, void, when)
import Data.ByteString qualified as B
import Data.Either (isRight, rights)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.IntMap.Strict qualified as IntMap
import Data.List (sort)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, listToMaybe)
import Data.Serialize (decode, encode)
import Data.Text qualified as T
import Data.Word (Word16, Word32, Word64)
import Database.RocksDB (BatchOp (..))
import Database.RocksDB.Query (insert, insertOp, retrieve, writeBatch)
import Haskoin
  ( BlockHeader (..),
    Ctx,
    Network,
    OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    btcRegTest,
    headerHash,
    prepareContext,
    scriptToAddressBS,
    sha256,
    txHash,
  )
import Haskoin.Store.Data (Balance (..), BlockData (..), BlockRef (..), Spender (..), TxData (..))
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Migrate
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered qualified as L
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    mempoolAddrCF,
    mempoolCF,
    mempoolInsertOps,
    spenderCF,
    txCF,
  )
import Haskoin.Store.Database.Types (MigrationKey (..), SpenderKey (..), SpenderVal (..), VersionKey (..))
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
        migrateLayered db `shouldReturn` Migration {moved = 2, batches = 1, checked = 2, rewritten = 0}
        retrieve db.db VersionKey `shouldReturn` Just layeredVersion
        (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Nothing
        -- the chain has the coin again, whole and unspent
        chainBalance <- fmap (\b -> (b.confirmed, b.unconfirmed, b.utxo, b.txs, b.received)) <$> Q.getBalance db miner
        chainBalance `shouldBe` Just (5000000000, 0, 1, 1, 5000000000)
        Q.getBalance db one `shouldReturn` Nothing
        Q.getBalance db two `shouldReturn` Nothing
        -- and with the mempool on top, every answer is what it was
        -- the queries read a database of the version that the store runs on
        settle db
        layered db (ask hist) `shouldReturn` old
        -- a second run finds nothing to do
        migrateLayered db `shouldReturn` Migration {moved = 0, batches = 0, checked = 0, rewritten = 0}

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
                -- the queries read a database of the version that the store runs on
                settle db
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
          -- the queries read a database of the version that the store runs on
          settle db
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
        upgradeDatabase (\_ -> return ()) db
          `shouldReturn` Migration {moved = 0, batches = 0, checked = 0, rewritten = 0}
        retrieve db.db VersionKey `shouldReturn` Just currentVersion
    -- one of version 20 with a mempool, and then the same one again
    withSystemTempDirectory "haskoin-store-migrate-" $ \dir -> do
      (hist, old) <- mixedDatabase ctx dir $ \db hist0 ->
        foldM (addTx db) hist0 (take 5 (payments (coinOf hist0)))
      forM_ [5, 0] $ \moved ->
        openLayered net ctx dir $ \db -> do
          done <- upgradeDatabase (\_ -> return ()) db
          done.moved `shouldBe` moved
          retrieve db.db VersionKey `shouldReturn` Just currentVersion
          (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Nothing
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

  describe "the time of a confirmed transaction" $ do
    it "is the time of its block, not of when the store first saw it" $
      withSystemTempDirectory "haskoin-store-times-" $ \dir ->
        openLayered net ctx dir $ \db -> do
          hist <- start layeredRules db
          let tx = Tx 2 [TxIn (coinOf hist) "" maxBound] [TxOut 4000 (p2pkh 1)] [] 0
              stored = do
                rows <- dump db (txCF db.db)
                return [td :: TxData | (_, v) <- rows, Right td <- [decode v], txHash td.tx == txHash tx]
          (.result) <$> apply layeredRules db (Pay tx 1000) `shouldReturn` Right "True"
          -- in the mempool it has the time at which it came
          fmap (.timestamp) <$> M.getMempoolTx db (txHash tx) `shouldReturn` Just 1000
          mined <- blockOn db [tx] >>= apply layeredRules db
          isRight mined.result `shouldBe` True
          best <- Q.getBestBlock db >>= maybe (fail "no best block") (Q.getBlock db)
          time <- maybe (fail "no block") (return . fromIntegral . (.header.timestamp)) best
          time `shouldNotBe` (1000 :: Word64)
          map (\td -> (td.block, td.timestamp)) <$> stored `shouldReturn` [(BlockRef 2 1, time)]
          wrongTimes db `shouldReturn` []
          -- and when the block is reverted it goes back to the mempool
          -- with the time of the revert, which the timeout of the mempool
          -- counts from
          reverted <- apply layeredRules db (Revert (headerHash (maybe (error "no block") (.header) best)) 7000)
          isRight reverted.result `shouldBe` True
          fmap (.timestamp) <$> M.getMempoolTx db (txHash tx) `shouldReturn` Just 7000
    modifyMaxSuccess (const 20) $
      it "is the time of its block, after any history" $
        -- whatever a store saw in its mempool before, and through blocks
        -- that are reverted and replaced
        property . forAll (vectorOf 60 anyNumber) $ \seeds -> ioProperty $
          withSystemTempDirectory "haskoin-store-times-" $ \dir ->
            openLayered net ctx dir $ \db -> withShadow net ctx $ \shadow -> do
              hist0 <- start layeredRules db
              void (foldM (walk shadow db) hist0 seeds)
              late <- wrongTimes db
              late `shouldBe` []
    modifyMaxSuccess (const 20) $
      it "is set by the migration to version 22, and nothing else is changed" $
        -- A database as the layered rules write it has the time of its
        -- block for every confirmed transaction. Some of them are given
        -- another time, as a store of version 21 that saw them in its
        -- mempool had, and the database is marked as of that version. The
        -- migration must give back the database as it was, byte for byte.
        property . forAll (vectorOf 40 anyNumber) $ \seeds -> ioProperty $
          withSystemTempDirectory "haskoin-store-times-" $ \dir ->
            openLayered net ctx dir $ \db -> withShadow net ctx $ \shadow -> do
              hist0 <- start layeredRules db
              void (foldM (walk shadow db) hist0 seeds)
              original <- dump db (txCF db.db)
              let confirmed = [(k, td :: TxData) | (k, v) <- original, Right td <- [decode v], inChain td]
                  -- every other one, and all of them when there are few
                  picked = [x | (i, x) <- zip [0 :: Int ..] confirmed, even i || length confirmed < 4]
              writeBatch db.db $
                insertOp VersionKey layeredVersion
                  : [PutCF (txCF db.db) k (encode (later td)) | (k, td) <- picked]
              wrongTimes db >>= (`shouldBe` length picked) . length
              done <- upgradeDatabase (\_ -> return ()) db
              done.rewritten `shouldBe` length picked
              retrieve db.db VersionKey `shouldReturn` Just currentVersion
              (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Nothing
              dump db (txCF db.db) `shouldReturn` original
              -- and again, it finds nothing to do
              upgradeDatabase (\_ -> return ()) db
                `shouldReturn` Migration {moved = 0, batches = 0, checked = 0, rewritten = 0}
    modifyMaxSuccess (const 20) $
      it "is in a record that is written once, and not when its outputs are spent" $
        -- After every step of a history, the record of each transaction
        -- that was confirmed before the step and still is, is the same
        -- bytes as before: blocks that spend its outputs, and blocks that
        -- are reverted, write keys of their own and leave it alone. No
        -- record has spenders in it.
        property . forAll (vectorOf 60 anyNumber) $ \seeds -> ioProperty $
          withSystemTempDirectory "haskoin-store-times-" $ \dir ->
            openLayered net ctx dir $ \db -> withShadow net ctx $ \shadow -> do
              hist0 <- start layeredRules db
              let records = do
                    rows <- dump db (txCF db.db)
                    return (Map.fromList [(k, v) | (k, v) <- rows, Right td <- [decode v], inChain td])
                  turn (hist, was) seed = do
                    hist' <- walk shadow db hist seed
                    now <- records
                    Map.filter id (Map.intersectionWith (/=) was now) `shouldBe` Map.empty
                    return (hist', now)
              first <- records
              (_, final) <- foldM turn (hist0, first) seeds
              [td.spenders | v <- Map.elems final, Right (td :: TxData) <- [decode v], not (IntMap.null td.spenders)]
                `shouldBe` []
    it "has the spenders of its outputs under a key for each sixteen of them" $
      withSystemTempDirectory "haskoin-store-times-" $ \dir ->
        openLayered net ctx dir $ \db -> do
          hist <- start layeredRules db
          -- forty outputs, of which three are spent, in three groups
          let wide = Tx 2 [TxIn (coinOf hist) "" maxBound] (replicate 40 (TxOut 1000 (p2pkh 1))) [] 0
              spend i = Tx 2 [TxIn (OutPoint (txHash wide) i) "" maxBound] [TxOut 500 (p2pkh 2)] [] 0
              spends = map spend [3, 17, 39]
              keysNow = do
                rows <- dump db (spenderCF db.db)
                return
                  [ (g, IntMap.keys m)
                  | (k, v) <- rows,
                    Right (SpenderKey h g) <- [decode k],
                    h == txHash wide,
                    Right (SpenderVal m) <- [decode v]
                  ]
              mine txs = do
                mined <- blockOn db txs >>= apply layeredRules db
                isRight mined.result `shouldBe` True
          mine [wide]
          written <- dump db (txCF db.db)
          keysNow `shouldReturn` []
          mine spends
          keysNow `shouldReturn` [(0, [3]), (1, [17]), (2, [39])]
          -- the record is as the first block wrote it
          now <- dump db (txCF db.db)
          [v | (k, v) <- now, k `elem` map fst written] `shouldBe` map snd written
          -- and the transaction is read with its spenders, each also by itself
          found <- L.withView db (`L.getTxData` txHash wide)
          let expected = IntMap.fromList [(fromIntegral i, Spender (txHash (spend i)) 0) | i <- [3, 17, 39]]
          fmap (.spenders) found `shouldBe` Just expected
          forM_ [3, 4, 17, 39] $ \i ->
            L.withView db (`L.getSpender` OutPoint (txHash wide) i)
              `shouldReturn` IntMap.lookup (fromIntegral i) expected
          -- a block that is reverted takes its spenders along, and a group
          -- with none left has no key
          best <- Q.getBestBlock db >>= maybe (fail "no best block") return
          reverted <- apply layeredRules db (Revert best 0)
          isRight reverted.result `shouldBe` True
          keysNow `shouldReturn` []
          Q.getChainSpenders db (txHash wide) `shouldReturn` IntMap.empty
          -- the transactions that spent them are back in the mempool, and
          -- the transaction is read with them as its spenders still
          fmap (.spenders) <$> L.withView db (`L.getTxData` txHash wide) `shouldReturn` Just expected
    modifyMaxSuccess (const 20) $
      it "has the spenders of its outputs moved to keys of their own by the migration" $
        -- A database as the layered rules write it has a key for each
        -- spent output. The spenders are put where version 21 had them,
        -- in the record of the transaction that made the output, the keys
        -- are deleted, and the database is marked as of that version. The
        -- migration must give back the records and the keys as they were.
        property . forAll (vectorOf 40 anyNumber) $ \seeds -> ioProperty $
          withSystemTempDirectory "haskoin-store-times-" $ \dir ->
            openLayered net ctx dir $ \db -> withShadow net ctx $ \shadow -> do
              hist0 <- start layeredRules db
              void (foldM (walk shadow db) hist0 seeds)
              records <- dump db (txCF db.db)
              keys <- dump db (spenderCF db.db)
              let spent =
                    [ (h, m :: IntMap.IntMap Spender)
                    | (k, v) <- keys,
                      Right (SpenderKey h _) <- [decode k],
                      Right (SpenderVal m) <- [decode v]
                    ]
                  inside td = IntMap.unions [m | (h, m) <- spent, h == txHash td.tx]
                  old (k, v) = case decode v of
                    Right td | inChain td -> (k, encode (embed td (inside td)))
                    _ -> (k, v)
              writeBatch db.db $
                insertOp VersionKey layeredVersion
                  : [DelCF (spenderCF db.db) k | (k, _) <- keys]
                    <> [PutCF (txCF db.db) k v | (k, v) <- map old records]
              done <- upgradeDatabase (\_ -> return ()) db
              -- every record that had a spender in it was written again
              done.rewritten `shouldBe` length [() | (k, v) <- records, old (k, v) /= (k, v)]
              dump db (txCF db.db) `shouldReturn` records
              dump db (spenderCF db.db) `shouldReturn` keys
    it "is set by a migration that carries on after it was interrupted" $
      withSystemTempDirectory "haskoin-store-times-" $ \dir ->
        openLayered net ctx dir $ \db -> withShadow net ctx $ \shadow -> do
          hist0 <- start layeredRules db
          -- payments, and then blocks, which confirm some of them
          void (foldM (walk shadow db) hist0 (onlyPayments [1 .. 6] <> [8 .. 11]))
          original <- dump db (txCF db.db)
          let confirmed = [(k, td :: TxData) | (k, v) <- original, Right td <- [decode v], inChain td]
          length confirmed `shouldSatisfy` (> 0)
          -- as a migration leaves it that stopped before its first change
          writeBatch db.db $
            [insertOp VersionKey currentVersion, insertOp MigrationKey layeredVersion]
              <> [PutCF (txCF db.db) k (encode ((td :: TxData) {timestamp = 7})) | (k, td) <- confirmed]
          said <- newIORef []
          done <- upgradeDatabase (\line -> modifyIORef' said (line :)) db
          done.rewritten `shouldBe` length confirmed
          dump db (txCF db.db) `shouldReturn` original
          (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Nothing
          readIORef said >>= (`shouldSatisfy` any ("Carrying on" `T.isPrefixOf`))
    it "is not guessed for a transaction whose block the database does not have" $
      withSystemTempDirectory "haskoin-store-times-" $ \dir ->
        openLayered net ctx dir $ \db -> do
          _ <- start layeredRules db
          rows <- dump db (txCF db.db)
          let confirmed = [(k, td :: TxData) | (k, v) <- rows, Right td <- [decode v], inChain td]
          (k, td) <- maybe (fail "no confirmed transaction") return (listToMaybe confirmed)
          writeBatch
            db.db
            [ insertOp VersionKey layeredVersion,
              PutCF (txCF db.db) k (encode ((td :: TxData) {block = BlockRef 5000 0}))
            ]
          upgradeDatabase (\_ -> return ()) db `shouldThrow` \case
            Inconsistent _ -> True
            _ -> False

net :: Network
net = btcRegTest

-- | Choose one operation of a history from a database of version 20 that
-- has gone through it ('withShadow'), and carry it out there and, under
-- the layered rules, in a database of the layered layout.
walk :: DatabaseReader -> DatabaseReader -> History -> Word16 -> IO History
walk shadow db hist seed =
  plan shadow hist seed >>= \case
    (hist', Nothing) -> return hist'
    (hist', Just op) -> do
      void (apply mixedRules shadow op)
      record db hist' op <$> apply layeredRules db op

-- | A transaction with a time that is not that of its block, and marked as
-- one that can be replaced, which a confirmed transaction cannot be.
later :: TxData -> TxData
later td = (td :: TxData) {timestamp = td.timestamp + 12345, rbf = True}

-- | Bring a database of version 21 to the version that the store runs on.
settle :: DatabaseReader -> IO ()
settle db = void (migrateRecords (\_ -> return ()) db)

-- | A transaction with spenders of its outputs in its record.
embed :: TxData -> IntMap.IntMap Spender -> TxData
embed td inside = (td :: TxData) {spenders = inside}

-- | Whether a transaction is in the chain.
inChain :: TxData -> Bool
inChain td = case td.block of
  BlockRef {} -> not td.deleted
  MemRef {} -> False

-- | The confirmed transactions of a database whose time is not that of
-- their block, each with both times.
wrongTimes :: DatabaseReader -> IO [(TxHash, Word64, Maybe Word64)]
wrongTimes db = do
  rows <- dump db (txCF db.db)
  fmap catMaybes . mapM check $ [td :: TxData | (_, v) <- rows, Right td <- [decode v], inChain td]
  where
    check td = do
      hashes <- Q.getBlocksAtHeight db td.block.height
      blocks <- catMaybes <$> mapM (Q.getBlock db) hashes
      let time = listToMaybe [fromIntegral b.header.timestamp | b <- blocks, b.main]
      return (if time == Just td.timestamp then Nothing else Just (txHash td.tx, td.timestamp, time))

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
