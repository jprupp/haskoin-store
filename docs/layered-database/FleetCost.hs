{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | What it costs a store to answer as a member of a fleet, for
-- docs/layered-database.md: the same questions asked of one database, from
-- its own mempool area and from the pool that a fleet shares in Redis.
--
-- It is not part of the package. Build the package first, write a database
-- with @QueryCost.hs make layered@, start a Redis that nothing else uses,
-- then, from the root of the repository:
--
-- > stack ghc -- -O1 -threaded -rtsopts -outputdir /tmp/fleet-cost \
-- >   -o /tmp/fleet-cost/run docs/layered-database/FleetCost.hs
-- > /tmp/fleet-cost/run /tmp/query-cost/b redis://127.0.0.1:6379
--
-- The store becomes the only member of a fleet under keys that start with
-- @cost:@, which are deleted first, and offers its whole mempool to the
-- pool, which is timed. Then each question is asked both ways, the answers
-- are checked to be the same, and the times are printed.
module Main (main) where

import Conduit (concatMapC, runConduit, sinkList, takeC, (.|))
import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate)
import Control.Monad (forM_, unless, when)
import Data.HashSet qualified as HashSet
import Data.List (sortOn)
import Data.List.NonEmpty (nonEmpty)
import Data.Ord (Down (..))
import Data.Serialize (encode)
import Data.Word (Word64)
import Database.Redis qualified as R
import Database.RocksDB (withIterCF)
import Database.RocksDB.Query (matching)
import GHC.Clock (getMonotonicTimeNSec)
import Haskoin
import Haskoin.Store.Common (Limits (..), StoreRead (..))
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered qualified as L
import Haskoin.Store.Database.Query.Pooled (pooledRead)
import Haskoin.Store.Database.Reader
import Haskoin.Store.Database.Types (BalKey (..), BalVal, MemAddrKey (..), MemAddrVal (..), MemSummary (..))
import Haskoin.Store.Fleet.Member
import System.Environment (getArgs)
import System.IO (BufferMode (..), hSetBuffering, stdout)
import Text.Printf (printf)
import UnliftIO (readTVarIO)

main :: IO ()
main = withContext $ \ctx -> do
  hSetBuffering stdout LineBuffering
  [dir, url] <- getArgs
  conn <- R.checkedConnect (either error id (R.parseConnectInfo url))
  old <- R.runRedis conn (R.keys "cost:*") >>= either (fail . show) return
  forM_ (nonEmpty old) $ \ks -> R.runRedis conn (R.del ks) >>= either (fail . show) (\_ -> return ())
  let mib = 1024 * 1024
      open = withLayeredReader btcRegTest ctx 20 32 dir False (Just 2000) (Just (256 * mib))
  open (Just (1024 * mib)) (Just (1024 * mib)) Nothing $ \db -> do
    m <- newMember MemberConfig {prefix = "cost:", name = "a", lease = 600000, depth = 6, expiry = 0} conn db
    Just best <- Q.getBestBlock db
    noteBest m best
    mempool <- Q.getMempool db
    -- the first round reports and moves the tip; the next ones offer
    t0 <- getMonotonicTimeNSec
    let rounds n = do
          memberTick m
          have <- HashSet.size <$> readTVarIO m.offered
          when (have < length mempool && n < (200 :: Int)) (rounds (n + 1))
    rounds 0
    t1 <- getMonotonicTimeNSec
    have <- HashSet.size <$> readTVarIO m.offered
    printf
      "%d of %d mempool transactions in the pool, %.0f microseconds for each\n"
      have
      (length mempool)
      (fromIntegral (t1 - t0) / 1000 / fromIntegral (max 1 have) :: Double)
    -- addresses by how much the mempool has for them
    sums <- withIterCF db.db (mempoolAddrCF db.db) $ \it ->
      runConduit $ matching it MemAddrKeyS .| concatMapC summary .| sinkList
    let touched = HashSet.fromList (map fst sums)
        busiest = case sortOn (Down . snd) sums of
          (a, _) : _ -> a
          [] -> error "nothing in the mempool"
        little = take 1000 [a | (a, n) <- sums, n <= 2]
    some <- withIterCF db.db (balanceCF db.db) $ \it ->
      runConduit $ matching it BalKeyS .| concatMapC balanced .| takeC 20000 .| sinkList
    let nothing = take 1000 (filter (not . (`HashSet.member` touched)) some)
        page = Limits {limit = 100, offset = 0, start = Nothing}
        own :: (StoreRead -> IO a) -> IO a
        own q = L.withView db (q . L.layeredRead)
        shared :: (StoreRead -> IO a) -> IO a
        -- as the web server does: from the tip that the member read at
        -- its last turn, which the first read of the pool checks
        shared q = do
          Just tip <- readTVarIO m.tip
          answeringAt m (encode tip) (\r pool confirm -> q (pooledRead r pool) <* confirm) >>= \case
            Just x -> return x
            Nothing -> fail "the store has no view of the fleet tip"
        ask :: (Eq a, NFData a) => String -> Int -> [StoreRead -> IO a] -> IO ()
        ask what times questions = do
          a <- mapM own questions
          b <- mapM shared questions
          unless (a == b) (fail ("different answers: " <> what))
          alone <- timed times (mapM own questions)
          fleet <- timed times (mapM shared questions)
          let each t = t / fromIntegral (length questions)
          printf "%-56s %10.1f %10.1f %7.1f\n" what (each alone) (each fleet) (fleet / alone)
    let heading = "microseconds for each" :: String
    printf "\n%-56s %10s %10s %7s\n" heading ("alone" :: String) ("fleet" :: String) ("ratio" :: String)
    ask "balance: address with nothing in the mempool" 3 [\q -> q.getBalance a | a <- nothing]
    ask "balance: address with a transaction or two in the mempool" 3 [\q -> q.getBalance a | a <- little]
    ask "balance: the busiest address" 200 [\q -> q.getBalance busiest]
    ask "balances of 100 addresses with nothing in the mempool" 20 [\q -> q.getBalances (take 100 nothing)]
    ask "address with nothing in the mempool: its transactions" 3 [\q -> q.getAddressTxs a page | a <- take 200 nothing]
    ask "address with a little in the mempool: its transactions" 3 [\q -> q.getAddressTxs a page | a <- take 200 little]
    ask "busiest address: first 100 transactions" 50 [\q -> q.getAddressTxs busiest page]
    ask "busiest address: first 100 unspent outputs" 50 [\q -> q.getAddressUnspents busiest page]
    let newest = map snd (take 1000 mempool)
    ask "transaction: in the mempool" 3 [\q -> q.getTxData h | h <- newest]
    ask "100 transactions: in the mempool" 20 [\q -> q.getManyTxData (take 100 newest)]
    ask "first 100 of the mempool" 50 [\q -> q.getMempoolPage page]
  where
    summary :: (MemAddrKey, MemAddrVal) -> Maybe (Address, Word64)
    summary = \case
      (MemAddrSumKey a, MemAddrSumVal s) -> Just (a, s.txs)
      _ -> Nothing
    balanced :: (BalKey, BalVal) -> Maybe Address
    balanced = \case
      (BalKey a, _) -> Just a
      _ -> Nothing

-- | The mean time of an action in microseconds, over so many runs after
-- one that does not count.
timed :: (NFData a) => Int -> IO a -> IO Double
timed times action = do
  _ <- action >>= evaluate . rnf
  t0 <- getMonotonicTimeNSec
  forM_ [1 .. times] $ \_ -> action >>= evaluate . rnf
  t1 <- getMonotonicTimeNSec
  return (fromIntegral (t1 - t0) / 1000 / fromIntegral times)
