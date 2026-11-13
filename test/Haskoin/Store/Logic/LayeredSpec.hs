module Haskoin.Store.Logic.LayeredSpec (spec) where

import Control.Monad (foldM, forM_, replicateM_, void)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Either (isLeft, isRight)
import Data.List (sort, sortOn)
import Data.Maybe (isJust)
import Data.Serialize (decode)
import Haskoin
  ( Block (..),
    BlockHeight,
    BlockHash,
    Ctx,
    Network,
    OutPoint (..),
    Tx (..),
    TxHash,
    TxIn (..),
    TxOut (..),
    bchRegTest,
    btcRegTest,
    headerHash,
    prepareContext,
    txHash,
  )
import Haskoin.Store.Common (Replaced (..), Start (..), StoreRead (..), replacedKept)
import Haskoin.Store.Data (BlockData (..), TxData (..), Unspent (..))
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Migrate (upgradeDatabase)
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered qualified as L
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    blockCF,
    heightCF,
    mempoolAddrCF,
    mempoolCF,
    txCF,
  )
import Haskoin.Store.Histories
import Haskoin.Store.Logic (ImportException (..))
import Haskoin.Store.TestBlocks (coinbaseTx, script)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck (choose, forAll, ioProperty, property, tabulate, vectorOf)
import Test.QuickCheck.Gen (unGen)
import Test.QuickCheck.Random (mkQCGen)
import UnliftIO (withSystemTempDirectory)

spec :: Spec
spec = prepareContext $ \ctx -> describe "rules of the layered layout" $ do
  forM_ [("Bitcoin", btcRegTest), ("Bitcoin Cash, without replace-by-fee", bchRegTest)] $ \(name, net) ->
    modifyMaxSuccess (const 40) $
      it ("leave the database that today's rules and the migration leave, on " <> name) $
        -- the number says when the third store is upgraded, and which
        -- pages are asked for at the end
        property . forAll ((,) <$> vectorOf 70 anyNumber <*> choose (0, 1000000)) $ \(seeds, number) -> ioProperty $ do
          (outcomes, hist) <- sameHistory ctx net number [\db h -> plan db h seed | seed <- seeds]
          let count p = length (filter p outcomes)
              bucket n
                | n == 0 = "none"
                | n <= 3 = "1 to 3"
                | n <= 9 = "4 to 9"
                | otherwise = "10 or more" :: String
          return
            . tabulate "operations refused" [bucket (count (isLeft . (.result)))]
            . tabulate "operations that removed from the mempool" [bucket (count (not . null . (.removed)))]
            $ tabulate "blocks at the end" [bucket (length hist.chain)] True

  it "give a transaction that is confirmed twice the place of the later block, as today's" $ do
    -- the coinbase of the first block again, in the second; then its output
    -- is spent, the spend confirmed, and both blocks reverted
    let twice = coinbaseTx 1
        spend = Tx 2 [TxIn (OutPoint (txHash twice) 0) "" maxBound] [TxOut 4000 (p2pkh 1)] [] 0
        mine txs db h = (h,) . Just <$> blockOn db txs
        pay tx _ h = return (h, Just (Pay tx h.clock))
        revert _ h = return $ case h.chain of
          (b, _) : _ -> (h, Just (Revert (headerHash b.header) h.clock))
          [] -> (h, Nothing)
    -- the third store is upgraded with the spend in its mempool
    (outcomes, _) <- sameHistory ctx btcRegTest 2 [mine [twice], pay spend, mine [spend], revert, revert]
    map (either show (const "done") . (.result)) outcomes `shouldBe` replicate 5 "done"

  it "refuse a block that spends an output of the mempool, which today's rules import" $
    withSystemTempDirectory "haskoin-store-layered-" $ \dir ->
      openLayered btcRegTest ctx dir $ \db -> do
        hist <- start layeredRules db
        let parent = Tx 2 [TxIn (coinOf hist) "" maxBound] [TxOut 4000 script] [] 0
            child = Tx 2 [TxIn (OutPoint (txHash parent) 0) "" maxBound] [TxOut 3000 (p2pkh 1)] [] 0
        (.result) <$> apply layeredRules db (Pay parent 1000) `shouldReturn` Right "True"
        untouched <- everything db
        -- the parent is not in the block, so its output is not in the chain
        block <- blockOn db [child]
        (.result) <$> apply layeredRules db block `shouldReturn` Left Orphan
        everything db `shouldReturn` untouched

  it "keep what a block replaced, and forget it when the block is reverted" $
    withSystemTempDirectory "haskoin-store-layered-" $ \dir ->
      openLayered btcRegTest ctx dir $ \db -> do
        (gone, height) <- replacedByBlock db
        best <- Q.getBestBlock db >>= maybe (fail "no best block") return
        isRight . (.result) <$> apply layeredRules db (Revert best 2000) `shouldReturn` True
        mapM (Q.getReplaced db) gone `shouldReturn` [Nothing, Nothing]
        Q.getReplacedAt db height `shouldReturn` []

  it "forget what a block replaced 2016 blocks later" $
    withSystemTempDirectory "haskoin-store-layered-" $ \dir ->
      openLayered btcRegTest ctx dir $ \db -> do
        (gone, height) <- replacedByBlock db
        let mineEmpty = blockOn db [] >>= apply layeredRules db
        replicateM_ (fromIntegral replacedKept - 1) mineEmpty
        all isJust <$> mapM (Q.getReplaced db) gone `shouldReturn` True
        void mineEmpty
        mapM (Q.getReplaced db) gone `shouldReturn` [Nothing, Nothing]
        Q.getReplacedAt db height `shouldReturn` []

  it "leave the keys of a mempool transaction alone when a block confirms what it spends" $
    withSystemTempDirectory "haskoin-store-layered-" $ \dir ->
      openLayered btcRegTest ctx dir $ \db -> do
        hist <- start layeredRules db
        let parent = Tx 2 [TxIn (coinOf hist) "" maxBound] [TxOut 4000 script, TxOut 5000 (p2pkh 2)] [] 0
            child = Tx 2 [TxIn (OutPoint (txHash parent) 0) "" maxBound] [TxOut 3000 (p2pkh 1)] [] 0
            keysOf tx = do
              Just td <- M.getMempoolTx db (txHash tx)
              let r = M.mempoolRecords db.net db.ctx 1000 td
              return (fst (M.recordEntries r), M.addressArea [r])
        forM_ [parent, child] $ \tx ->
          (.result) <$> apply layeredRules db (Pay tx 1000) `shouldReturn` Right "True"
        (general, byAddress) <- keysOf child
        block <- blockOn db [parent]
        confirmed <- apply layeredRules db block
        confirmed.removed `shouldBe` [txHash parent]
        -- the child's keys are all that is left of the mempool, unchanged
        dump db (mempoolCF db.db) `shouldReturn` sortOn fst general
        dump db (mempoolAddrCF db.db) `shouldReturn` sortOn fst byAddress
        -- and the spent output of the parent, now in the chain, is still
        -- spent to anyone who asks
        L.withView db (\v -> L.getUnspent v (OutPoint (txHash parent) 0)) `shouldReturn` Nothing
        (fmap (.value) <$> Q.getUnspent db (OutPoint (txHash parent) 0)) `shouldReturn` Just 4000

-- | Give the same history to three stores, each with a database of its
-- own:
--
-- * one that runs the rules of today's layout;
--
-- * one that runs the rules of the layered layout;
--
-- * one that is upgraded on the way: it runs today's rules for the first
--   operations, as many as a number says, then its database is migrated,
--   and it runs the rules of the layered layout from there on.
--
-- Each operation is chosen from the first database and the history so far.
-- Every operation must have the same outcome in all of them, and leave the
-- same answers to every query. At the end all are asked for the same pages,
-- chosen from the number. Then the first database is migrated: it must equal
-- the second key for key, and the third in everything but the bodies of
-- deleted transactions, which each set of rules leaves in its own place.
-- Gives the outcomes and the history.
sameHistory ::
  Ctx ->
  Network ->
  Int ->
  [DatabaseReader -> History -> IO (History, Maybe Op)] ->
  IO ([Outcome], History)
sameHistory ctx net number choices =
  withSystemTempDirectory "haskoin-store-mixed-" $ \dirOld ->
    withSystemTempDirectory "haskoin-store-layered-" $ \dirNew ->
      withSystemTempDirectory "haskoin-store-upgraded-" $ \dirUp -> do
        -- the store to upgrade begins with a database of version 20
        open20 net ctx dirUp (\_ -> return ())
        openLayered net ctx dirNew $ \new ->
          openLayered net ctx dirUp $ \up -> do
            (outcomes, hist) <- open20 net ctx dirOld $ \old -> do
              hist0 <- start mixedRules old
              void (start layeredRules new)
              void (start mixedRules up)
              let (earlier, later) = splitAt (number `mod` (length choices + 1)) choices
                  today op _ _ _ = void (apply mixedRules up op)
                  upgraded op was hist answers = do
                    now <- apply layeredRules up op
                    (show op, now) `shouldBe` (show op, was)
                    M.wrongSummaries up `shouldReturn` []
                    layered up (ask hist) `shouldReturn` answers
              half <- foldM (next old new today) ([], hist0) earlier
              void (upgradeDatabase (\_ -> return ()) up)
              done@(_, hist) <- foldM (next old new upgraded) half later
              -- pages, of databases that the new rules built or carried on
              -- with, against today's
              let asked = unGen (questions net ctx hist 60) (mkQCGen number) 30
              was <- mixed old (\queries -> mapM (answer queries) asked)
              -- but for pages that start at a deleted transaction, which
              -- the layered layout does not know
              known <- mapM (startsKnown old) asked
              let both :: [x] -> [x]
                  both xs = [x | (x, True) <- zip xs known]
              forM_ [new, up] $ \db -> do
                now <- layered db (\queries -> mapM (answer queries) asked)
                take 2 (differences (both asked) (both was) (both now)) `shouldBe` []
              -- the queries of a fleet do not know deleted transactions, so
              -- they are not asked for pages that start at one
              let live queries q = case startOf q of
                    Just (Just (AtTx h)) -> isJust <$> queries.getTxData h
                    _ -> return True
              fair <- layered new (\queries -> mapM (live queries) asked)
              let kept :: [x] -> [x]
                  kept xs = [x | (x, True) <- zip xs fair]
              shared <- pooled new (\queries -> mapM (answer queries) (kept asked))
              take 2 (differences (kept asked) (kept was) shared) `shouldBe` []
              return done
            migrated <- openLayered net ctx dirOld $ \old -> upgradeDatabase (\_ -> return ()) old >> everything old
            mine <- everything new
            -- the same, but for the bodies of deleted transactions, which
            -- version 20 left in the chain: the layered rules keep none
            mine `shouldBe` migrated {inChain = [], deleted = []}
            -- the upgraded store has those that it deleted before the
            -- upgrade in the chain, as version 20 left them, even when the
            -- transaction came back later: the queries, compared after
            -- every operation, do not answer with those
            theirs <- everything up
            theirs {deleted = [], inChain = []} `shouldBe` migrated {deleted = [], inChain = []}
            return (reverse outcomes, hist)
  where
    next old new third (outcomes, hist) chosen =
      chosen old hist >>= \case
        (hist', Nothing) -> return (outcomes, hist')
        (hist', Just op) -> do
          was <- apply mixedRules old op
          now <- apply layeredRules new op
          (show op, now) `shouldBe` (show op, was)
          -- each summary is what the entries of its address add up to
          M.wrongSummaries new `shouldReturn` []
          -- and every answer is the same after it
          let hist'' = record old hist' op was
          answers <- mixed old (ask hist'')
          layered new (ask hist'') `shouldReturn` answers
          -- from the queries of a fleet as well, whose pool is here the
          -- store's own mempool
          pooled new (ask hist'') `shouldReturn` answers
          third op was hist'' answers :: IO ()
          return (was : outcomes, hist'')

-- | Everything in a database of the layered layout.
data Everything = Everything
  { best :: !(Maybe BlockHash),
    blocks :: ![(ByteString, ByteString)],
    heights :: ![(ByteString, ByteString)],
    chain :: !Chain,
    mempool :: ![(ByteString, ByteString)],
    mempoolByAddress :: ![(ByteString, ByteString)],
    -- | transactions that left the mempool unconfirmed, wherever their
    -- bodies are kept
    deleted :: ![(TxHash, TxData)],
    -- | those of them that are kept in the chain, as version 20 did
    inChain :: ![TxHash]
  }
  deriving (Eq, Show)

everything :: DatabaseReader -> IO Everything
everything db = do
  best <- Q.getBestBlock db
  -- what is kept of replaced transactions is the layered layout's own,
  -- and tested on its own
  blocks <- filter (not . ("\x11" `B.isPrefixOf`) . fst) <$> dump db (blockCF db.db)
  heights <- filter (not . ("\x12" `B.isPrefixOf`) . fst) <$> dump db (heightCF db.db)
  chain <- chainOf db
  area <- dump db (mempoolCF db.db)
  mempoolByAddress <- dump db (mempoolAddrCF db.db)
  bodies <- dump db (txCF db.db) >>= mapM (decoded . snd)
  let (gone, mempool) = span' ((== "\x0e") . B.take 1 . fst) area
      old = [td | td <- bodies, td.deleted]
  new <- mapM (decoded . snd) gone
  return
    Everything
      { best,
        blocks,
        heights,
        chain,
        mempool,
        mempoolByAddress,
        deleted = sortOn fst [(txHash td.tx, td) | td <- old <> new],
        inChain = map (txHash . (.tx)) old
      }
  where
    decoded :: ByteString -> IO TxData
    decoded = either fail return . decode
    span' p xs = (filter p xs, filter (not . p) xs)

-- | A payment and its child in the mempool, and a block whose transaction
-- spends what the payment spent: both are replaced. Gives their hashes and
-- the height of the block, after checking what is kept of them.
replacedByBlock :: DatabaseReader -> IO ([TxHash], BlockHeight)
replacedByBlock db = do
  hist <- start layeredRules db
  let paid = Tx 2 [TxIn (coinOf hist) "" maxBound] [TxOut 4000 (p2pkh 1)] [] 0
      child = Tx 2 [TxIn (OutPoint (txHash paid) 0) "" maxBound] [TxOut 3000 (p2pkh 2)] [] 0
      rival = Tx 2 [TxIn (coinOf hist) "" maxBound] [TxOut 4500 (p2pkh 3)] [] 0
      gone = map txHash [paid, child]
  forM_ [paid, child] $ \tx ->
    (.result) <$> apply layeredRules db (Pay tx 1000) `shouldReturn` Right "True"
  block <- blockOn db [rival]
  replaced <- apply layeredRules db block
  sort replaced.removed `shouldBe` sort gone
  best <- Q.getBestBlock db >>= maybe (fail "no best block") return
  height <- maybe (fail "no block") (return . (.height)) =<< Q.getBlock db best
  -- the payment, and its child, which spent what it paid
  let what = Replaced {by = txHash rival, height = height}
  mapM (Q.getReplaced db) gone `shouldReturn` [Just what, Just what]
  Q.getReplacedAt db height `shouldReturn` sort gone
  return (gone, height)
