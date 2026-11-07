{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module Haskoin.Store.LogicSpec (spec) where

import Control.Monad (forM_, void)
import Control.Monad.Reader (ask, liftIO, runReaderT)
import Data.ByteString qualified as B
import Data.List (sort, sortOn)
import Data.Ord (Down (..))
import Data.Text qualified as T
import Data.Word (Word32, Word64)
import Database.RocksDB.Query (insert, insertCF, matchingAsListCF, retrieve)
import Haskoin
  ( Block (..),
    BlockHash,
    BlockHeader (..),
    BlockNode (..),
    Ctx,
    Network,
    OutPoint (..),
    Tx (..),
    TxHash (..),
    sha256,
    TxIn (..),
    TxOut (..),
    bchRegTest,
    btcRegTest,
    genesisNode,
    headerHash,
    prepareContext,
    scriptToAddressBS,
    txHash,
  )
import Haskoin.Node (LogLevel (..), Logger (..))
import Haskoin.Store.Common (Limits (..), StoreReadBase (..), StoreWrite (..), getActiveTxData, getDefaultBalance)
import Haskoin.Store.Data
  ( Balance (..),
    BlockData (..),
    BlockRef (..),
    Spender (..),
    TxData (..),
    Unspent (..),
  )
import Haskoin.Store.Database.Reader (DatabaseReader (..), DatabaseReaderT, getMempoolBefore, memoryReport, mempoolCF, withDatabaseReader)
import Haskoin.Store.Database.Types (MemKey (..), MemTimeKey (..), MemTxKey (..), VersionKey (..))
import Haskoin.Store.Database.Writer (MempoolChanges (..), runWriter, runWriterMempool)
import Haskoin.Store.Logic
  ( ImportException (..),
    ImportM,
    deleteUnconfirmedTx,
    importBlock,
    initBest,
    newMempoolTx,
    revertBlock,
  )
import Haskoin.Store.TestBlocks (coinbaseTx, nextBlock, script)
import System.Log.FastLogger (fromLogStr)
import Test.Hspec
import UnliftIO (catch, evaluate, try, withSystemTempDirectory)

spec :: Spec
spec = prepareContext $ \ctx -> do
  describe "initBest" $
    it "imports the genesis block once" $
      withDB btcRegTest ctx $ \db -> do
        importing db initBest `shouldReturn` Right ()
        importing db initBest `shouldReturn` Right ()
        reading db getBestBlock `shouldReturn` Just genesis

  describe "memoryReport" $
    it "lists the memory of every column family" $
      withCoin btcRegTest ctx $ \db _ -> do
        report <- memoryReport db.db
        forM_ ["default", "addr-tx", "unspent", "balance", "total", "block cache"] $ \name ->
          report `shouldSatisfy` T.isInfixOf name
        report `shouldNotSatisfy` T.isInfixOf "?"

  describe "data version 18" $
    it "moves the mempool into its column family when opened" $
      withSystemTempDirectory "haskoin-store-migrate-" $ \dir -> do
        let open :: DatabaseReaderT IO () -> IO ()
            open = withDatabaseReader btcRegTest ctx 20 32 dir False Nothing Nothing Nothing Nothing Nothing
            h1 = TxHash (sha256 ("one" :: B.ByteString))
            h2 = TxHash (sha256 ("two" :: B.ByteString))
            old = [(200, h2), (100, h1)] :: [(Word64, TxHash)]
        open $ ask >>= \r -> liftIO $ do
          insert r.db VersionKey (18 :: Word32)
          insert r.db MemKey old
        open $ ask >>= \r -> liftIO $ do
          reading r getMempool `shouldReturn` old
          reading r (getMempoolPage (Limits 1 1 Nothing)) `shouldReturn` drop 1 old
          (retrieve r.db MemKey :: IO (Maybe [(Word64, TxHash)])) `shouldReturn` Nothing
          retrieve r.db VersionKey `shouldReturn` Just (20 :: Word32)

  describe "data version 19" $
    it "indexes the mempool by time when opened" $
      withSystemTempDirectory "haskoin-store-migrate-" $ \dir -> do
        let open :: DatabaseReaderT IO () -> IO ()
            open = withDatabaseReader btcRegTest ctx 20 32 dir False Nothing Nothing Nothing Nothing Nothing
            old = sortOn Down (zip [300, 100, 200] (hashes 3))
        open $ ask >>= \r -> liftIO $ do
          insert r.db VersionKey (19 :: Word32)
          forM_ old $ \(t, h) -> insertCF r.db (mempoolCF r.db) (MemTxKey h) t
          (matchingAsListCF r.db (mempoolCF r.db) MemTimeKeyS :: IO [(MemTimeKey, ())])
            `shouldReturn` []
        open $ ask >>= \r -> liftIO $ do
          reading r getMempool `shouldReturn` old
          retrieve r.db VersionKey `shouldReturn` Just (20 :: Word32)

  describe "mempool index" $ do
    let entries = zip [5, 3, 5, 1, 4, 3, 2] (hashes 7)
        newest = sortOn Down entries
        fill db = importing db (forM_ entries (\(t, h) -> addToMempool h t))
    it "pages the mempool newest first" $
      withDB btcRegTest ctx $ \db -> do
        _ <- fill db
        reading db getMempool `shouldReturn` newest
        forM_ [(0, 0), (3, 0), (3, 2), (2, 5), (4, 7), (0, 3)] $ \(l, o) ->
          reading db (getMempoolPage (Limits l o Nothing))
            `shouldReturn` take' l (drop (fromIntegral o) newest)
    it "reads the oldest entries before a time" $
      withDB btcRegTest ctx $ \db -> do
        _ <- fill db
        let oldest = reverse newest
        reading db (getMempoolBefore 4 10) `shouldReturn` takeWhile ((< 4) . fst) oldest
        reading db (getMempoolBefore 4 2) `shouldReturn` take 2 oldest
        reading db (getMempoolBefore 1 10) `shouldReturn` []
    it "reports and indexes what a batch changes" $
      withDB btcRegTest ctx $ \db -> do
        let hs = hashes 3
            (h1, h2, h3) = (hs !! 0, hs !! 1, hs !! 2)
            batch :: ImportM () -> IO MempoolChanges
            batch = fmap snd . runWriterMempool db.net db.ctx db quietLogger
        c <- batch (addToMempool h1 10 >> addToMempool h2 20)
        (sort c.added, c.removed) `shouldBe` (sort [h1, h2], [])
        -- a new time moves the entry in the index, and is no change
        batch (addToMempool h1 30) `shouldReturn` MempoolChanges [] []
        reading db getMempool `shouldReturn` [(30, h1), (20, h2)]
        -- removing what is not there, or adding and removing in one batch,
        -- is no change either
        batch (deleteFromMempool h3) `shouldReturn` MempoolChanges [] []
        batch (addToMempool h3 40 >> deleteFromMempool h3) `shouldReturn` MempoolChanges [] []
        batch (deleteFromMempool h2) `shouldReturn` MempoolChanges [] [h2]
        reading db getMempool `shouldReturn` [(30, h1)]
        (matchingAsListCF db.db (mempoolCF db.db) MemTimeKeyS :: IO [(MemTimeKey, ())])
          `shouldReturn` [(MemTimeKey 30 h1, ())]

  describe "newMempoolTx" $ do
    it "imports a transaction and spends its input" $
      withCoin btcRegTest ctx $ \db coin -> do
        let tx = spend [coin] [1000, 2000]
        importing db (newMempoolTx tx 1700000000) `shouldReturn` Right True
        reading db getMempool `shouldReturn` [(1700000000, txHash tx)]
        reading db (getSpender coin) `shouldReturn` Just (Spender (txHash tx) 0)
        fmap (.value) <$> reading db (getUnspent (out tx 1)) `shouldReturn` Just 2000

    it "ignores a transaction it already has" $
      withCoin btcRegTest ctx $ \db coin -> do
        let tx = spend [coin] [1000]
        mapM (addTx db) [tx, tx] `shouldReturn` [Right True, Right False]
        _ <- mine db [tx]
        addTx db tx `shouldReturn` Right False
        fmap (.block) <$> txData db tx `shouldReturn` Just (BlockRef 2 1)

    it "rejects invalid transactions" $
      withCoin btcRegTest ctx $ \db coin -> do
        addTx db (spend [out (coinbaseTx 9) 0] [1]) `shouldReturn` Left Orphan
        addTx db (spend [coin, coin] [1]) `shouldReturn` Left DuplicatePrevOutput
        addTx db (spend [coin] [5000000001]) `shouldReturn` Left InsufficientFunds
        addTx db (coinbaseTx 9) `shouldReturn` Left UnexpectedCoinbase
        mempool db `shouldReturn` []

    it "flags BIP125 signalling transactions and their descendants" $
      withCoin btcRegTest ctx $ \db coin -> do
        let parent = spendRBF [coin] [1000]
            child = spend [out parent 0] [900]
        importing db (mapM (`newMempoolTx` 0) [parent, child])
          `shouldReturn` Right [True, True]
        mapM (rbfFlag db) [parent, child] `shouldReturn` [Just True, Just True]

    it "does not flag other transactions" $ do
      withCoin btcRegTest ctx $ \db coin -> do
        let parent = spend [coin] [1000]
            child = spend [out parent 0] [900]
        mapM (addTx db) [parent, child] `shouldReturn` [Right True, Right True]
        mapM (rbfFlag db) [parent, child] `shouldReturn` [Just False, Just False]
      withCoin bchRegTest ctx $ \db coin -> do
        let tx = spendRBF [coin] [1000]
        addTx db tx `shouldReturn` Right True
        rbfFlag db tx `shouldReturn` Just False

    it "replaces conflicting transactions and their descendants" $
      withCoin btcRegTest ctx $ \db coin -> do
        let original = spend [coin] [1000]
            child = spend [out original 0] [900]
            replacement = spend [coin] [2000]
        mapM (addTx db) [original, child, replacement]
          `shouldReturn` [Right True, Right True, Right True]
        mempool db `shouldReturn` [txHash replacement]
        reading db (getSpender coin) `shouldReturn` Just (Spender (txHash replacement) 0)
        fmap (.deleted) <$> reading db (getTxData (txHash original)) `shouldReturn` Just True

    it "replaces a transaction and its descendant when both conflict" $
      withCoin btcRegTest ctx $ \db coin1 -> do
        _ <- mine db []
        let coin2 = out (coinbaseTx 2) 0
            parent = spend [coin1] [1000]
            child = spend [out parent 0, coin2] [900]
            replacement = spend [coin1, coin2] [2000]
        mapM (addTx db) [parent, child, replacement]
          `shouldReturn` [Right True, Right True, Right True]
        mempool db `shouldReturn` [txHash replacement]

    it "leaves the mempool unchanged when it rejects a replacement" $
      withCoin btcRegTest ctx $ \db coin1 -> do
        _ <- mine db []
        let coin2 = out (coinbaseTx 2) 0
            original = spend [coin1] [1000]
        _ <- mine db [spend [coin2] [1000]]
        addTx db original `shouldReturn` Right True
        -- The block store imports mempool transactions in batches and skips
        -- the ones that are rejected.
        let tryAdd tx = newMempoolTx tx 0 `catch` skip
            skip :: ImportException -> ImportM Bool
            skip _ = return False
            unfunded = spend [coin1] [5000000001]
            confirmedConflict = spend [coin1, coin2] [2000]
            spendsReplaced = spend [coin1, out original 0] [900]
        importing db (mapM tryAdd [unfunded, confirmedConflict, spendsReplaced])
          `shouldReturn` Right [False, False, False]
        mempool db `shouldReturn` [txHash original]

    it "rejects double spends on networks without RBF" $
      withCoin bchRegTest ctx $ \db coin -> do
        let original = spendRBF [coin] [1000]
        mapM (addTx db) [original, spend [coin] [2000]]
          `shouldReturn` [Right True, Left DoubleSpend]
        mempool db `shouldReturn` [txHash original]

    it "rejects double spends of confirmed transactions" $
      withCoin btcRegTest ctx $ \db coin -> do
        _ <- mine db [spend [coin] [1000]]
        addTx db (spend [coin] [2000]) `shouldReturn` Left TxConfirmed

    it "commits nothing from a batch that fails" $
      withCoin btcRegTest ctx $ \db coin -> do
        let good = spend [coin] [1000]
            orphan = spend [out (coinbaseTx 9) 0] [1]
        importing db (newMempoolTx good 0 >> newMempoolTx orphan 0)
          `shouldReturn` Left Orphan
        mempool db `shouldReturn` []
        reading db (getSpender coin) `shouldReturn` Nothing

  describe "importBlock" $ do
    it "confirms mempool transactions" $
      withCoin btcRegTest ctx $ \db coin -> do
        let tx = spendRBF [coin] [1000]
        addTx db tx `shouldReturn` Right True
        _ <- mine db [tx]
        mempool db `shouldReturn` []
        fmap (\t -> (t.block, t.rbf)) <$> txData db tx `shouldReturn` Just (BlockRef 2 1, False)
        fmap (.block) <$> reading db (getUnspent (out tx 0)) `shouldReturn` Just (BlockRef 2 1)

    it "confirms a transaction whose outputs are spent in the mempool" $
      withCoin btcRegTest ctx $ \db coin -> do
        let parent = spend [coin] [1000]
            child = spend [out parent 0] [900]
        mapM (addTx db) [parent, child] `shouldReturn` [Right True, Right True]
        _ <- mine db [parent]
        mempool db `shouldReturn` [txHash child]
        fmap (.block) <$> txData db parent `shouldReturn` Just (BlockRef 2 1)
        reading db (getSpender (out parent 0)) `shouldReturn` Just (Spender (txHash child) 0)

    it "stops flagging a descendant once its signalling ancestor confirms" $
      withCoin btcRegTest ctx $ \db coin -> do
        let parent = spendRBF [coin] [1000]
            child = spend [out parent 0] [900]
        mapM (addTx db) [parent, child] `shouldReturn` [Right True, Right True]
        _ <- mine db [parent]
        rbfFlag db child `shouldReturn` Just False

    it "imports transactions in any order and records the block" $
      withCoin btcRegTest ctx $ \db coin -> do
        let parent = spend [coin] [4000000000]
            child = spend [out parent 0] [3999990000]
        block <- mine db [child, parent]
        Just bd <- reading db (getBlock (headerHash block.header))
        (bd.height, bd.main, bd.txs) `shouldBe` (2, True, map txHash block.txs)
        (bd.subsidy, bd.fee, bd.outputs) `shouldBe` (5000000000, 1000010000, 7999990000)
        fmap (.block) <$> txData db child `shouldReturn` Just (BlockRef 2 1)

    it "evicts conflicting mempool transactions and their descendants" $
      forM_ [btcRegTest, bchRegTest] $ \net ->
        withCoin net ctx $ \db coin -> do
          let original = spend [coin] [1000]
              child = spend [out original 0] [900]
          mapM (addTx db) [original, child] `shouldReturn` [Right True, Right True]
          _ <- mine db [spend [coin] [2000]]
          mempool db `shouldReturn` []
          mapM (txData db) [original, child] `shouldReturn` [Nothing, Nothing]

    it "rejects blocks that spend unknown outputs, changing nothing" $
      withCoin btcRegTest ctx $ \db _ -> do
        best <- reading db getBestBlock
        tryMine db [spend [out (coinbaseTx 9) 0] [1]] `shouldReturn` Left Orphan
        reading db getBestBlock `shouldReturn` best

    it "rejects blocks that do not build on the best block" $
      withDB btcRegTest ctx $ \db -> do
        let (block1, node1) = nextBlock btcRegTest 0 (genesisNode btcRegTest).header []
            (block2, node2) = nextBlock btcRegTest 1 node1.header []
        importing db (void (importBlock block1 node1)) `shouldReturn` Left BestBlockNotFound
        importing db initBest `shouldReturn` Right ()
        importing db (void (importBlock block2 node2)) `shouldReturn` Left PrevBlockNotBest

  describe "revertBlock" $ do
    it "returns the block's transactions to the mempool" $
      withCoin btcRegTest ctx $ \db coin -> do
        let tx = spendRBF [coin] [1000]
        block <- mine db [tx]
        let h = headerHash block.header
        importing db (revertBlock h) `shouldReturn` Right ()
        reading db getBestBlock `shouldReturn` Just block.header.prev
        fmap (.main) <$> reading db (getBlock h) `shouldReturn` Just False
        reading db getMempool `shouldReturn` [(1296689802, txHash tx)]
        rbfFlag db tx `shouldReturn` Just True
        txData db (coinbaseTx 2) `shouldReturn` Nothing

    it "flags mempool descendants of the signalling transactions it returns" $
      withCoin btcRegTest ctx $ \db coin -> do
        let parent = spendRBF [coin] [1000]
            child = spend [out parent 0] [900]
        block <- mine db [parent]
        addTx db child `shouldReturn` Right True
        rbfFlag db child `shouldReturn` Just False
        importing db (revertBlock (headerHash block.header)) `shouldReturn` Right ()
        rbfFlag db child `shouldReturn` Just True

    it "only reverts the best block" $
      withDB btcRegTest ctx $ \db -> do
        importing db (revertBlock genesis) `shouldReturn` Left BestBlockNotFound
        importing db initBest `shouldReturn` Right ()
        _ <- mine db []
        importing db (revertBlock genesis) `shouldReturn` Left BlockNotBest

  describe "deleteUnconfirmedTx" $ do
    it "deletes a transaction and its descendants" $
      withCoin btcRegTest ctx $ \db coin -> do
        let parent = spend [coin] [1000]
            child = spend [out parent 0] [900]
        mapM (addTx db) [parent, child] `shouldReturn` [Right True, Right True]
        importing db (deleteUnconfirmedTx False (txHash parent)) `shouldReturn` Right ()
        mempool db `shouldReturn` []
        fmap (.value) <$> reading db (getUnspent coin) `shouldReturn` Just 5000000000

    it "ignores unknown transactions" $
      withCoin btcRegTest ctx $ \db _ ->
        importing db (deleteUnconfirmedTx False (txHash (coinbaseTx 9)))
          `shouldReturn` Right ()

    it "refuses to delete confirmed transactions" $
      withCoin btcRegTest ctx $ \db _ ->
        importing db (deleteUnconfirmedTx False (txHash (coinbaseTx 1)))
          `shouldReturn` Left TxConfirmed

  describe "address balances" $
    it "follow transactions through the mempool, a block and its reversal" $
      withCoin btcRegTest ctx $ \db coin -> do
        a <- either fail return (scriptToAddressBS btcRegTest ctx script)
        let tx = spend [coin] [1000, 2000]
            -- confirmed, unconfirmed, outputs, transactions, received
            balance = do
              b <- reading db (getDefaultBalance a)
              return (b.confirmed, b.unconfirmed, b.utxo, b.txs, b.received)
        balance `shouldReturn` (5000000000, 0, 1, 1, 5000000000)
        addTx db tx `shouldReturn` Right True
        balance `shouldReturn` (0, 3000, 2, 2, 5000003000)
        block <- mine db [tx]
        balance `shouldReturn` (5000003000, 0, 3, 3, 10000003000)
        importing db (revertBlock (headerHash block.header)) `shouldReturn` Right ()
        balance `shouldReturn` (0, 3000, 2, 2, 5000003000)

  describe "a coin whose script has no address on the network" $
    it "is kept without an address" $
      withCoin btcRegTest ctx $ \db coin -> do
        -- OP_HASH256, a push of 32 bytes, OP_EQUAL. Bitcoin Cash has an
        -- address for this script. Bitcoin has none.
        let hashLock = "\xaa\x20" <> B.replicate 32 0xbb <> "\x87"
            tx = Tx 2 [TxIn coin "" maxBound] [TxOut 1000 hashLock] [] 0
        addTx db tx `shouldReturn` Right True
        coinAddress <- fmap (.address) <$> reading db (getUnspent (out tx 0))
        coinAddress `shouldBe` Just Nothing

-- | Run with an empty database in a temporary directory.
withDB :: Network -> Ctx -> (DatabaseReader -> IO a) -> IO a
withDB net ctx go =
  withSystemTempDirectory "haskoin-store-logic-" $ \dir ->
    withDatabaseReader net ctx 20 32 dir False Nothing Nothing Nothing Nothing Nothing (ask >>= liftIO . go)

-- | Run with the genesis block and one mined block, passing the output of
-- that block's coinbase, worth 5000000000.
withCoin :: Network -> Ctx -> (DatabaseReader -> OutPoint -> IO a) -> IO a
withCoin net ctx go =
  withDB net ctx $ \db -> do
    importing db initBest `shouldReturn` Right ()
    _ <- mine db []
    go db (out (coinbaseTx 1) 0)

importing :: DatabaseReader -> ImportM a -> IO (Either ImportException a)
importing db = try . runWriter db.net db.ctx db quietLogger

-- | Build every message, so that log lines are tested too, but print none.
quietLogger :: Logger
quietLogger =
  Logger
    { level = LevelDebug,
      source = "",
      push = void . evaluate . B.length . fromLogStr
    }

-- | Distinct transaction hashes.
hashes :: Int -> [TxHash]
hashes n = [TxHash (sha256 (B.singleton (fromIntegral i))) | i <- [1 .. n]]

-- | 'take' where a limit of 0 means no limit, as in 'Limits'.
take' :: Word32 -> [a] -> [a]
take' 0 = id
take' l = take (fromIntegral l)

reading :: DatabaseReader -> DatabaseReaderT IO a -> IO a
reading db f = runReaderT f db

addTx :: DatabaseReader -> Tx -> IO (Either ImportException Bool)
addTx db tx = importing db (newMempoolTx tx 0)

txData :: DatabaseReader -> Tx -> IO (Maybe TxData)
txData db = reading db . getActiveTxData . txHash

rbfFlag :: DatabaseReader -> Tx -> IO (Maybe Bool)
rbfFlag db tx = fmap (.rbf) <$> txData db tx

mempool :: DatabaseReader -> IO [TxHash]
mempool db = sort . map snd <$> reading db getMempool

-- | Mine a block with these transactions on top of the best block.
tryMine :: DatabaseReader -> [Tx] -> IO (Either ImportException Block)
tryMine db txs = do
  Just h <- reading db getBestBlock
  Just parent <- reading db (getBlock h)
  let (block, node) = nextBlock db.net parent.height parent.header txs
  fmap (const block) <$> importing db (importBlock block node)

mine :: DatabaseReader -> [Tx] -> IO Block
mine db txs = tryMine db txs >>= either (fail . ("Block rejected: " <>) . show) return

-- | Spend outputs into new ones of the given values.
spend :: [OutPoint] -> [Word64] -> Tx
spend = spendWith maxBound

-- | Like 'spend', but signal BIP125 replaceability.
spendRBF :: [OutPoint] -> [Word64] -> Tx
spendRBF = spendWith 0

spendWith :: Word32 -> [OutPoint] -> [Word64] -> Tx
spendWith s ops vs =
  Tx 2 [TxIn op "" s | op <- ops] [TxOut v script | v <- vs] [] 0

out :: Tx -> Word32 -> OutPoint
out tx = OutPoint (txHash tx)

-- | Bitcoin regtest genesis block.
genesis :: BlockHash
genesis = "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206"
