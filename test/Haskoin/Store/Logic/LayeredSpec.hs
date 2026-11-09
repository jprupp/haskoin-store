module Haskoin.Store.Logic.LayeredSpec (spec) where

import Control.Monad (foldM, forM_, void)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Either (isLeft)
import Data.List (sortOn)
import Data.Serialize (decode)
import Haskoin
  ( Block (..),
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
import Haskoin.Store.Data (TxData (..), Unspent (..))
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Migrate (migrateLayered)
import Haskoin.Store.Database.Query qualified as Q
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
import Test.QuickCheck (forAll, ioProperty, property, tabulate, vectorOf)
import UnliftIO (withSystemTempDirectory)

spec :: Spec
spec = prepareContext $ \ctx -> describe "rules of the layered layout" $ do
  forM_ [("Bitcoin", btcRegTest), ("Bitcoin Cash, without replace-by-fee", bchRegTest)] $ \(name, net) ->
    modifyMaxSuccess (const 40) $
      it ("leave the database that today's rules and the migration leave, on " <> name) $
        property . forAll (vectorOf 70 anyNumber) $ \seeds -> ioProperty $ do
          (outcomes, hist) <- both ctx net [\db h -> plan db h seed | seed <- seeds]
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
          (b, _) : _ -> (h, Just (Revert (headerHash b.header)))
          [] -> (h, Nothing)
    (outcomes, _) <- both ctx btcRegTest [mine [twice], pay spend, mine [spend], revert, revert]
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

  it "leave the keys of a mempool transaction alone when a block confirms what it spends" $
    withSystemTempDirectory "haskoin-store-layered-" $ \dir ->
      openLayered btcRegTest ctx dir $ \db -> do
        hist <- start layeredRules db
        let parent = Tx 2 [TxIn (coinOf hist) "" maxBound] [TxOut 4000 script, TxOut 5000 (p2pkh 2)] [] 0
            child = Tx 2 [TxIn (OutPoint (txHash parent) 0) "" maxBound] [TxOut 3000 (p2pkh 1)] [] 0
            keysOf tx = do
              Just td <- M.getMempoolTx db (txHash tx)
              let r = M.mempoolRecords db.net db.ctx 1000 td
              return (M.recordEntries r)
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
        M.getUnspent db (OutPoint (txHash parent) 0) `shouldReturn` Nothing
        (fmap (.value) <$> Q.getUnspent db (OutPoint (txHash parent) 0)) `shouldReturn` Just 4000

-- | Give the same history to both sets of rules: to those of today's
-- layout in one database, and to those of the layered layout in another.
-- Each operation is chosen from the first database and the history so far.
-- Every operation must have the same outcome in both, and leave the same
-- answers to every query. At the end, migrate the first database and
-- compare it with the second, key for key. Gives the outcomes and the
-- history.
both :: Ctx -> Network -> [DatabaseReader -> History -> IO (History, Maybe Op)] -> IO ([Outcome], History)
both ctx net choices =
  withSystemTempDirectory "haskoin-store-mixed-" $ \dirOld ->
    withSystemTempDirectory "haskoin-store-layered-" $ \dirNew ->
      openLayered net ctx dirNew $ \new -> do
        (outcomes, hist) <- open20 net ctx dirOld $ \old -> do
          hist0 <- start mixedRules old
          void (start layeredRules new)
          foldM (next old new) ([], hist0) choices
        migrated <- openLayered net ctx dirOld $ \old -> migrateLayered old >> everything old
        mine <- everything new
        -- the same, but for where the bodies of deleted transactions are:
        -- the layered rules keep none of them in the chain
        mine `shouldBe` migrated {inChain = []}
        return (reverse outcomes, hist)
  where
    next old new (outcomes, hist) choose =
      choose old hist >>= \case
        (hist', Nothing) -> return (outcomes, hist')
        (hist', Just op) -> do
          was <- apply mixedRules old op
          now <- apply layeredRules new op
          (show op, now) `shouldBe` (show op, was)
          -- and every answer is the same after it
          let hist'' = record old hist' op was
          answers <- ask (mixed old) old hist''.made
          ask (layered new) new hist''.made `shouldReturn` answers
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
  blocks <- dump db (blockCF db.db)
  heights <- dump db (heightCF db.db)
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
