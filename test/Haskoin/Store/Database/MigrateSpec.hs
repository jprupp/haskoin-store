{-# LANGUAGE DeriveAnyClass #-}

module Haskoin.Store.Database.MigrateSpec (spec) where

import Control.Monad (foldM, forM_, void, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Default (def)
import Data.Either (rights)
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.List (sort)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Serialize (decode)
import Data.Word (Word16, Word32, Word8)
import Database.RocksDB (ColumnFamily, iterEntry, iterFirst, iterNext, withIterCF)
import Database.RocksDB.Query (insert, retrieve, writeBatch)
import Haskoin
  ( Address,
    Block (..),
    BlockNode (..),
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
import Haskoin.Node (LogLevel (..), Logger (..))
import Haskoin.Store.Common (sortTxs)
import Haskoin.Store.Data
  ( Balance (..),
    BlockData (..),
    BlockRef (..),
    Prev (..),
    Spender (..),
    TxData (..),
    TxRef (..),
    UnixTime,
    Unspent (..),
    zeroBalance,
  )
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Migrate
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    addrOutCF,
    addrTxCF,
    balanceCF,
    mempoolAddrCF,
    mempoolCF,
    mempoolInsertOps,
    txCF,
    unspentCF,
    withDatabaseReader,
    withLayeredReader,
  )
import Haskoin.Store.Database.Types (BalVal (..), MigrationKey (..), VersionKey (..))
import Haskoin.Store.Database.Writer (Writer, runWriter)
import Haskoin.Store.Logic
  ( ImportException,
    deleteUnconfirmedTx,
    importBlock,
    initBest,
    newMempoolTx,
    revertBlock,
  )
import Haskoin.Store.TestBlocks (nextBlock, script)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck (arbitrary, forAll, ioProperty, property, tabulate, vectorOf)
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
        hist1 <- addTx db hist0 payment
        addTx db hist1 child
      (miner, one, two) <- case rights (map (scriptToAddressBS net ctx) [script, p2pkh 1, p2pkh 2]) of
        [a, b, c] -> return (a, b, c)
        _ -> fail "a script has no address"
      let numbers bs a = fmap (\b -> (b.confirmed, b.unconfirmed, b.utxo, b.txs, b.received)) (lookup a bs)
      -- the coin is spent in the mempool: it no longer counts as confirmed
      numbers old.balances miner `shouldBe` Just (0, 0, 0, 3, 5000002000)
      numbers old.balances one `shouldBe` Just (0, 3000, 1, 1, 3000)
      numbers old.balances two `shouldBe` Just (0, 1500, 1, 1, 1500)
      openLayered ctx dir $ \db -> do
        migrateLayered db `shouldReturn` Migration {moved = 2, batches = 1, checked = 2}
        retrieve db.db VersionKey `shouldReturn` Just layeredVersion
        (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Nothing
        -- the chain has the coin again, whole and unspent
        chainBalance <- fmap (\b -> (b.confirmed, b.unconfirmed, b.utxo, b.txs, b.received)) <$> Q.getBalance db miner
        chainBalance `shouldBe` Just (5000000000, 0, 1, 1, 5000000000)
        Q.getBalance db one `shouldReturn` Nothing
        Q.getBalance db two `shouldReturn` Nothing
        -- and with the mempool on top, every answer is what it was
        ask ctx (layered db) db hist.made `shouldReturn` old
        -- a second run finds nothing to do
        migrateLayered db `shouldReturn` Migration {moved = 0, batches = 0, checked = 0}

  modifyMaxSuccess (const 50) $
    it "keeps every answer and leaves the chain as if the mempool had been wiped, for random histories" $
      property . forAll ((,) <$> vectorOf 40 arbitrary <*> vectorOf 12 arbitrary) $ \(seeds, last') -> ioProperty $ do
        withSystemTempDirectory "haskoin-store-migrate-" $ \dir -> do
          -- anything at first, then payments, so that the mempool has something
          let payments' = [p - p `mod` 10 + p `mod` 5 | p <- last']
          (hist, old) <- mixedDatabase ctx dir $ \db hist0 -> foldM (step ctx db) hist0 (seeds <> payments')
          withSystemTempDirectory "haskoin-store-chain-" $ \dir' ->
            open20 ctx dir' $ \fresh -> do
              -- the same blocks, in a database that never saw the mempool
              importing fresh initBest `shouldReturn` Right ()
              forM_ (reverse hist.chain) $ \(block, node) ->
                void (importing fresh (\w -> importBlock w block node)) :: IO ()
              openLayered ctx dir $ \db -> do
                done <- migrateLayeredWith defaultMigrationOptions {chunk = 3} db
                done.checked `shouldBe` length old.mempool
                done.moved `shouldBe` length old.mempool
                -- every answer is what it was
                ask ctx (layered db) db hist.made `shouldReturn` old
                -- the chain's column families are those of the fresh database
                migrated <- chainOf db
                expected <- chainOf fresh
                length expected.addressTxs `shouldSatisfy` (>= length hist.chain)
                migrated `shouldBe` expected
                -- the mempool area is the keys of its transactions, each worked
                -- out from that transaction alone: no more and no less
                moved <- catMaybes <$> mapM (M.getMempoolTx db . snd) old.mempool
                let records = [M.mempoolRecords net ctx time td | td <- moved, MemRef time <- [td.block]]
                length records `shouldBe` length old.mempool
                dump db (mempoolCF db.db) `shouldReturn` sort (concatMap (.general) records)
                dump db (mempoolAddrCF db.db) `shouldReturn` sort (concatMap (.byAddress) records)
                -- without the mempool area, the answers are those of the chain
                void (M.wipeMempoolArea db)
                wiped <- ask ctx (layered db) db hist.made
                alone <- ask ctx (mixed fresh) fresh hist.made
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
        openLayered ctx dir $ \db -> do
          try (migrateLayeredWith interrupted db) `shouldReturn` Left Interrupted
          -- a store of version 20 would refuse the database from here on
          retrieve db.db VersionKey `shouldReturn` Just layeredVersion
          (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Just 20
        openLayered ctx dir $ \db -> do
          done <- migrateLayered db
          done.moved `shouldBe` length old.mempool - stopAt
          done.checked `shouldBe` length old.mempool
          (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Nothing
          ask ctx (layered db) db hist.made `shouldReturn` old

  it "refuses a database that it cannot handle, and leaves it untouched" $ do
    withSystemTempDirectory "haskoin-store-migrate-" $ \dir ->
      openLayered ctx dir $ \db -> do
        migrateLayered db `shouldThrow` (== UnknownVersion Nothing)
        insert db.db VersionKey (19 :: Word32)
        migrateLayered db `shouldThrow` (== UnknownVersion (Just 19))
    withSystemTempDirectory "haskoin-store-migrate-" $ \dir -> do
      -- the mempool lists a transaction that has no body
      let lost = TxHash (sha256 ("lost" :: ByteString))
      _ <- mixedDatabase ctx dir $ \db hist0 -> do
        hist1 <- foldM (addTx db) hist0 (take 5 (payments (coinOf hist0)))
        writeBatch db.db (mempoolInsertOps db.db lost 7)
        return hist1
      openLayered ctx dir $ \db -> do
        migrateLayered db `shouldThrow` \case
          Inconsistent _ -> True
          _ -> False
        retrieve db.db VersionKey `shouldReturn` Just (20 :: Word32)
        dump db (mempoolAddrCF db.db) `shouldReturn` []
        -- the mempool column family has its list and its index by time only
        written <- dump db (mempoolCF db.db)
        written `shouldSatisfy` all ((`elem` ["\x08", "\x0b"]) . B.take 1 . fst)

-------------------------------------------------------------------------------
-- A database of version 20 with a history
-------------------------------------------------------------------------------

net :: Network
net = btcRegTest

-- | What happened to a database: its main chain, newest block first, every
-- transaction ever made for it, coinbases included, the outputs made that
-- have no address, which no index of addresses lists, and the time to give
-- the next mempool transaction.
data History = History
  { chain :: ![(Block, BlockNode)],
    made :: ![Tx],
    loose :: ![OutPoint],
    clock :: !UnixTime
  }

data Interrupted = Interrupted
  deriving (Eq, Show, Exception)

open20, openLayered :: Ctx -> FilePath -> (DatabaseReader -> IO a) -> IO a
open20 ctx dir = withDatabaseReader net ctx 20 32 dir False Nothing Nothing Nothing Nothing Nothing
openLayered ctx dir = withLayeredReader net ctx 20 32 dir False Nothing Nothing Nothing Nothing Nothing

importing :: DatabaseReader -> (Writer -> IO a) -> IO (Either ImportException a)
importing db = try . runWriter db.net db.ctx db quiet
  where
    quiet = Logger {level = LevelError, source = "", push = \_ -> return ()}

-- | Make a database of version 20 in a directory: the genesis block, a block
-- with a coin, and whatever the function does next. Gives the history, and
-- the answers of the database to every query about it.
mixedDatabase ::
  Ctx ->
  FilePath ->
  (DatabaseReader -> History -> IO History) ->
  IO (History, Answers)
mixedDatabase ctx dir go =
  open20 ctx dir $ \db -> do
    importing db initBest `shouldReturn` Right ()
    hist <- mine db History {chain = [], made = [], loose = [], clock = 1000} [] >>= go db
    answers <- ask ctx (mixed db) db hist.made
    return (hist, answers)

-- | The output of the coinbase of the best block.
coinOf :: History -> OutPoint
coinOf hist = case hist.chain of
  (Block {txs = coinbase : _}, _) : _ -> OutPoint (txHash coinbase) 0
  _ -> error "no block with a coinbase"

-- | Scripts that pay to five addresses: that of every coinbase, and four
-- more.
scripts :: [ByteString]
scripts = script : map p2pkh [1 .. 4]

p2pkh :: Word8 -> ByteString
p2pkh n = "\x76\xa9\x14" <> B.replicate 20 n <> "\x88\xac"

-- | A script that has no address on Bitcoin.
hashLock :: ByteString
hashLock = "\xaa\x20" <> B.replicate 32 0xbb <> "\x87"

addresses :: Ctx -> [Address]
addresses ctx = rights (map (scriptToAddressBS net ctx) scripts)

-- | Mine a block with these transactions on the best block.
mine :: DatabaseReader -> History -> [Tx] -> IO History
mine db hist txs = do
  Just best <- Q.getBestBlock db
  Just parent <- Q.getBlock db best
  let (block, node) = nextBlock net parent.height parent.header txs
  importing db (\w -> importBlock w block node) >>= \case
    Right _ -> return hist {chain = (block, node) : hist.chain, made = take 1 block.txs <> hist.made}
    Left e -> fail ("Block rejected: " <> show e)

-- | Add a transaction to the mempool, if the store takes it.
addTx :: DatabaseReader -> History -> Tx -> IO History
addTx db hist tx =
  importing db (\w -> newMempoolTx w tx hist.clock) >>= \case
    Right True -> return hist {made = tx : hist.made, loose = unlisted <> hist.loose}
    _ -> return hist
  where
    unlisted = [OutPoint (txHash tx) i | (i, o) <- zip [0 ..] tx.outputs, o.script == hashLock]

-- | Eight payments, each from the change of the one before, starting from
-- a coin: a chain in the mempool, of which the migration has to move the
-- last first.
payments :: OutPoint -> [Tx]
payments coin = take 8 (go coin 5000000000)
  where
    go from value =
      let left = value - 10000
          tx = Tx 2 [TxIn from "" maxBound] [TxOut left script, TxOut 9000 (p2pkh 3)] [] 0
       in tx : go (OutPoint (txHash tx) 0) left

-- | One thing that happens to a store, chosen by a number: a payment between
-- the addresses, from outputs that are unspent, some of them in the
-- mempool; a block with some of the mempool; a transaction that replaces
-- one in the mempool; the deletion of one; or the reversal of the best
-- block.
step :: Ctx -> DatabaseReader -> History -> Word16 -> IO History
step ctx db hist0 seed
  | kind <= 4 = pay
  | kind <= 6 = some
  | kind == 7 = replace
  | kind == 8 = delete
  | otherwise = revert
  where
    kind = seed `mod` 10
    s = fromIntegral seed `div` 10 :: Int
    -- mempool transactions get the same time now and then
    hist = hist0 {clock = hist0.clock + fromIntegral (seed `mod` 2)}
    rotate k xs = let k' = k `mod` max 1 (length xs) in drop k' xs <> take k' xs
    target j = (hashLock : scripts) !! ((s `div` 11 + j) `mod` 6)
    pay = do
      listed <- concat <$> mapM (\a -> Q.getAddressUnspents db a def) (addresses ctx)
      -- and those of the outputs without an address that are still unspent
      unspent <- (listed <>) . catMaybes <$> mapM (Q.getUnspent db) hist.loose
      let picked = take (1 + s `mod` 3) (rotate (s `div` 3) unspent)
          total = sum (map (.value) picked)
          count = 1 + (s `div` 7) `mod` 3
          share = (total - 500) `div` fromIntegral count
          -- some signal that they can be replaced
          signal = if even (s `div` 13) then maxBound else 0
          tx = Tx 2 [TxIn u.outpoint "" signal | u <- picked] [TxOut share (target j) | j <- [0 .. count - 1]] [] 0
      if null picked || total < 500 + 600 * fromIntegral count
        then return hist
        else addTx db hist tx
    pool = map snd <$> Q.getMempool db
    some = do
      tds <- catMaybes <$> (pool >>= Q.getManyTxData db)
      -- in an order in which they can be mined: a prefix leaves no orphans
      let ordered = map snd (sortTxs (map (.tx) tds))
      mine db hist (take (s `mod` (length ordered + 1)) ordered)
    replace =
      rotate s <$> pool >>= \case
        [] -> return hist
        h : _ -> do
          Just td <- Q.getTxData db h
          case (td.tx.inputs, IntMap.lookup 0 td.prevs) of
            (i : _, Just p)
              | p.value > 2000 ->
                  addTx db hist $
                    let value = p.value - 700 - fromIntegral (s `mod` 50)
                     in Tx 2 [TxIn i.outpoint "" maxBound] [TxOut value (target 0)] [] 0
            _ -> return hist
    delete =
      rotate s <$> pool >>= \case
        [] -> return hist
        h : _ -> do
          importing db (\w -> deleteUnconfirmedTx w False h) `shouldReturn` Right ()
          return hist
    revert = case hist.chain of
      (block, _) : rest@(_ : _) -> do
        importing db (`revertBlock` headerHash block.header) `shouldReturn` Right ()
        return hist {chain = rest}
      _ -> return hist

-------------------------------------------------------------------------------
-- Answers
-------------------------------------------------------------------------------

-- | The queries of a layout.
data Reads = Reads
  { txData :: TxHash -> IO (Maybe TxData),
    spender :: OutPoint -> IO (Maybe Spender),
    unspent :: OutPoint -> IO (Maybe Unspent),
    balance :: Address -> IO (Maybe Balance),
    addressTxs :: Address -> IO [TxRef],
    addressUnspents :: Address -> IO [Unspent]
  }

-- | Queries of version 20, where one set of indices has the chain and the
-- mempool mixed.
mixed :: DatabaseReader -> Reads
mixed db =
  Reads
    { txData = Q.getTxData db,
      spender = Q.getSpender db,
      unspent = Q.getUnspent db,
      balance = Q.getBalance db,
      addressTxs = \a -> Q.getAddressTxs db a def,
      addressUnspents = \a -> Q.getAddressUnspents db a def
    }

-- | Queries of the layered layout.
layered :: DatabaseReader -> Reads
layered db =
  Reads
    { txData = M.getTxData db,
      spender = M.getSpender db,
      unspent = M.getUnspent db,
      balance = M.getBalance db,
      addressTxs = M.getAddressTxs db,
      addressUnspents = M.getAddressUnspents db
    }

-- | Everything that the API could say about a history.
data Answers = Answers
  { balances :: ![(Address, Balance)],
    addressTxs :: ![(Address, [TxRef])],
    addressUnspents :: ![(Address, [Unspent])],
    txs :: ![(TxHash, Maybe TxData)],
    spenders :: ![(OutPoint, Maybe Spender)],
    unspents :: ![(OutPoint, Maybe Unspent)],
    mempool :: ![(UnixTime, TxHash)]
  }
  deriving (Eq, Show)

-- | Ask everything about the addresses and about every transaction made,
-- with each of its outputs and one that it does not have.
ask :: Ctx -> Reads -> DatabaseReader -> [Tx] -> IO Answers
ask ctx queries db made = do
  balances <- mapM (\a -> (a,) . fromMaybe (zeroBalance a) <$> queries.balance a) as
  addressTxs <- mapM (\a -> (a,) <$> queries.addressTxs a) as
  addressUnspents <- mapM (\a -> (a,) <$> queries.addressUnspents a) as
  txs <- mapM (\h -> (h,) <$> queries.txData h) hashes
  spenders <- mapM (\p -> (p,) <$> queries.spender p) outpoints
  unspents <- mapM (\p -> (p,) <$> queries.unspent p) outpoints
  mempool <- Q.getMempool db
  return Answers {balances, addressTxs, addressUnspents, txs, spenders, unspents, mempool}
  where
    as = addresses ctx
    hashes = map txHash made
    outpoints = [OutPoint (txHash tx) i | tx <- made, i <- [0 .. fromIntegral (length tx.outputs)]]

-------------------------------------------------------------------------------
-- The chain's column families
-------------------------------------------------------------------------------

-- | What the chain's column families hold, but for what does not come from
-- the chain: bodies that version 20 marked as deleted, the time a
-- transaction was first seen, and balances that version 20 left at zero.
data Chain = Chain
  { bodies :: ![(ByteString, (BlockRef, Tx, IntMap Prev, Bool, IntMap Spender))],
    addressTxs :: ![(ByteString, ByteString)],
    addressOutputs :: ![(ByteString, ByteString)],
    unspents :: ![(ByteString, ByteString)],
    balances :: ![(ByteString, BalVal)]
  }
  deriving (Eq, Show)

chainOf :: DatabaseReader -> IO Chain
chainOf db = do
  bodies <- dump db (txCF db.db) >>= mapM (\(k, v) -> (k,) <$> decoded v)
  addressTxs <- dump db (addrTxCF db.db)
  addressOutputs <- dump db (addrOutCF db.db)
  unspents <- dump db (unspentCF db.db)
  balances <- dump db (balanceCF db.db) >>= mapM (\(k, v) -> (k,) <$> decoded v)
  return
    Chain
      { bodies = [(k, (td.block, td.tx, td.prevs, td.rbf, td.spenders)) | (k, td :: TxData) <- bodies, not td.deleted],
        addressTxs,
        addressOutputs,
        unspents,
        balances = filter ((/= def) . snd) balances
      }
  where
    decoded v = either fail return (decode v)

-- | Every key and value of a column family.
dump :: DatabaseReader -> ColumnFamily -> IO [(ByteString, ByteString)]
dump db cf = withIterCF db.db cf $ \iter -> iterFirst iter >> go iter
  where
    go iter =
      iterEntry iter >>= \case
        Nothing -> return []
        Just e -> iterNext iter >> (e :) <$> go iter
