module Haskoin.Store.Database.Query.LayeredSpec (spec) where

import Control.Monad (foldM, forM_, void)
import Data.ByteString (ByteString)
import Data.Default (def)
import Data.List (sort)
import Haskoin
  ( Address,
    OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    bchRegTest,
    btcRegTest,
    prepareContext,
    scriptToAddressBS,
    sha256,
    txHash,
  )
import Haskoin.Store.Common (Limits (..), Start (..), StoreRead (..))
import Haskoin.Store.Data
  ( Balance (..),
    TxData (..),
    TxRef (..),
    Unspent (..),
    confirmed,
  )
import Haskoin.Store.Database.Migrate (migrateLayered)
import Haskoin.Store.Database.Query.Layered qualified as L
import Haskoin.Store.Database.Reader (DatabaseReader (..))
import Haskoin.Store.Histories
import Haskoin.Store.TestBlocks (coinbaseTx, script)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck (arbitrary, forAll, ioProperty, property, tabulate, vectorOf)
import Test.QuickCheck.Gen (unGen)
import Test.QuickCheck.Random (mkQCGen)
import UnliftIO (withSystemTempDirectory)

spec :: Spec
spec = prepareContext $ \ctx -> describe "queries of the layered layout" $ do
  forM_ [("Bitcoin", btcRegTest), ("Bitcoin Cash", bchRegTest)] $ \(name, net) ->
    modifyMaxSuccess (const 30) $
      it ("give the pages of today's queries on a migrated database, on " <> name) $
        property . forAll ((,,) <$> vectorOf 45 anyNumber <*> vectorOf 12 anyNumber <*> arbitrary) $
          \(seeds, last', seed) -> ioProperty $
            withSystemTempDirectory "haskoin-store-pages-" $ \dir -> do
              -- anything at first, then payments, so that the mempool has
              -- something; and questions about what came of it
              (asked, was, starts) <- open20 net ctx dir $ \db -> do
                hist <- start mixedRules db >>= \hist0 -> foldM (step db) hist0 (seeds <> onlyPayments last')
                let asked = unGen (questions net ctx hist 150) (mkQCGen seed) 30
                was <- replies mixed db asked
                starts <- mixed db (\queries -> mapM (from queries) asked)
                return (asked, was, starts)
              now <- openLayered net ctx dir $ \db -> do
                void (migrateLayered db)
                replies layered db asked
              length now `shouldBe` length was
              take 2 (differences (asked <> asked) was now) `shouldBe` []
              let content reply = if size reply == 0 then "nothing in it" else "something in it"
                  pages = [place <> ", " <> content reply | (Just place, reply) <- zip starts was]
              return (tabulate "pages that start at" pages True)

  it "cut pages from a list that is part in the mempool and part in the chain" $
    withSystemTempDirectory "haskoin-store-pages-" $ \dir ->
      openLayered btcRegTest ctx dir $ \db -> do
        hist <- start layeredRules db
        -- a payment from the coin of the first block, with change to the
        -- miner, and a payment from that change, in the same second
        let one = Tx 2 [TxIn (coinOf hist) "" maxBound] [TxOut 3000 (p2pkh 1), TxOut 2000 script] [] 0
            two = Tx 2 [TxIn (OutPoint (txHash one) 1) "" maxBound] [TxOut 1500 (p2pkh 2)] [] 0
            coinbase = txHash (coinbaseTx 1)
            -- those of one second come in the order of their hashes
            unconfirmed = sort [txHash one, txHash two]
            never = TxHash (sha256 ("never made" :: ByteString))
            page limits = L.withView db $ \v -> map (.txid) <$> L.getAddressTxs v miner limits
            outputs = L.withView db $ \v -> map (.outpoint) <$> L.getAddressUnspents v miner def
            amounts = L.withView db $ \v -> fmap numbers <$> L.getBalance v miner
            numbers b = (b.confirmed, b.unconfirmed, b.utxo, b.txs, b.received)
            miner :: Address
            miner = either error id (scriptToAddressBS btcRegTest ctx script)
        forM_ [one, two] $ \tx ->
          (.result) <$> apply layeredRules db (Pay tx 1000) `shouldReturn` Right "True"
        page def `shouldReturn` unconfirmed <> [coinbase]
        page def {limit = 1} `shouldReturn` take 1 unconfirmed
        page def {limit = 2, offset = 1} `shouldReturn` drop 1 unconfirmed <> [coinbase]
        page def {offset = 2} `shouldReturn` [coinbase]
        -- from a mempool transaction: it, and what is older
        page def {start = Just (AtTx (unconfirmed !! 1))} `shouldReturn` drop 1 unconfirmed <> [coinbase]
        page def {start = Just (AtTx (unconfirmed !! 1)), offset = 1} `shouldReturn` [coinbase]
        -- from a confirmed transaction: nothing of the mempool
        page def {start = Just (AtTx coinbase)} `shouldReturn` [coinbase]
        -- from a height: nothing of the mempool either, even above the best
        -- block, as today
        page def {start = Just (AtBlock 5)} `shouldReturn` [coinbase]
        page def {start = Just (AtBlock 0)} `shouldReturn` []
        page def {start = Just (AtTx never)} `shouldReturn` []
        -- the mempool spends the coin, and the change that it made
        outputs `shouldReturn` []
        amounts `shouldReturn` Just (0, 0, 0, 3, 5000002000)
        -- a block confirms the first payment and not the second
        block <- blockOn db [one]
        (.removed) <$> apply layeredRules db block `shouldReturn` [txHash one]
        let reward = txHash (coinbaseTx 2)
        page def `shouldReturn` [txHash two, txHash one, reward, coinbase]
        page def {start = Just (AtTx (txHash one))} `shouldReturn` [txHash one, reward, coinbase]
        page def {start = Just (AtBlock 2), offset = 1, limit = 1} `shouldReturn` [reward]
        -- the change is in the chain now, and the mempool still spends it
        outputs `shouldReturn` [OutPoint reward 0]
        amounts `shouldReturn` Just (5000000000, 0, 1, 4, 10000002000)

  it "answer from one moment, whatever is written in the meantime" $
    withSystemTempDirectory "haskoin-store-pages-" $ \dir ->
      openLayered btcRegTest ctx dir $ \db -> do
        hist0 <- start layeredRules db
        let chained = payments (coinOf hist0)
            pay hist tx = record db hist (Pay tx hist.clock) <$> apply layeredRules db (Pay tx hist.clock)
        hist <- foldM pay hist0 (take 4 chained)
        let asked = unGen (questions btcRegTest ctx hist 100) (mkQCGen 7) 30
            everything queries = (,) <$> ask hist queries <*> mapM (answer queries) asked
        (early, during) <- L.withView db $ \v -> do
          early <- everything (L.layeredRead v)
          -- a block confirms two of the payments, and two more arrive
          block <- blockOn db (take 2 chained)
          mined <- apply layeredRules db block
          length mined.removed `shouldBe` 2
          forM_ (take 2 (drop 4 chained)) $ \tx ->
            (.result) <$> apply layeredRules db (Pay tx 2000) `shouldReturn` Right "True"
          during <- everything (L.layeredRead v)
          return (early, during)
        during `shouldBe` early
        -- a view taken now sees all of it
        late <- layered db everything
        length (fst late).mempool `shouldBe` 4
        (fst late).mempool `shouldNotBe` (fst early).mempool
        (fst late).balances `shouldNotBe` (fst early).balances
        take 1 (differences asked (snd early) (snd late)) `shouldNotBe` []

-- | The answers of a database to some questions: asked with the gaps that
-- the tests open their databases with, which reach every address of the
-- wallet that is paid, and asked again with small gaps, under which the
-- later ones are found only when earlier ones are used.
replies ::
  (DatabaseReader -> (StoreRead -> IO [Reply]) -> IO [Reply]) ->
  DatabaseReader ->
  [Question] ->
  IO [Reply]
replies with db asked = concat <$> mapM under [(db.initGap, db.maxGap), (2, 3)]
  where
    under (initial, gap) =
      with (db :: DatabaseReader) {initGap = initial, maxGap = gap} $ \queries ->
        mapM (answer queries) asked

-- | Where the page of a question starts, for the questions that have a
-- page, given the queries of today's layout.
from :: StoreRead -> Question -> IO (Maybe String)
from queries question = case startOf question of
  Nothing -> return Nothing
  Just Nothing -> return (Just "the top")
  Just (Just (AtBlock _)) -> return (Just "a height")
  Just (Just (AtTx h)) -> Just . maybe "a transaction that is not known" known <$> queries.getTxData h
  where
    known td
      | td.deleted = "a deleted transaction"
      | confirmed td.block = "a confirmed transaction"
      | otherwise = "a mempool transaction"
