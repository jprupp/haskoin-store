{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | How long the migration to the layered layout takes, for
-- docs/layered-database.md, against wiping the same mempool with the rules
-- of version 20.
--
-- It is not part of the package: it is the evidence for the numbers in the
-- design. Build the package first, then, from the root of the repository:
--
-- > stack ghc -- -O1 -threaded -outputdir /tmp/migrate-bench \
-- >   -o /tmp/migrate-bench/run docs/layered-database/MigrateBench.hs
-- > /tmp/migrate-bench/run make /tmp/migrate-bench/a 30000
-- > cp -r /tmp/migrate-bench/a /tmp/migrate-bench/b
-- > /tmp/migrate-bench/run wipe /tmp/migrate-bench/a
-- > /tmp/migrate-bench/run migrate /tmp/migrate-bench/b
--
-- @make@ writes a database of version 20 with that many mempool
-- transactions, @wipe@ deletes them all as @--wipe-mempool@ does, and
-- @migrate@ moves them to the mempool area.
module Main (main) where

import Control.Monad (forM_, unless, void)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.List (find)
import Data.Serialize (encode)
import Data.Time.Clock (diffUTCTime, getCurrentTime)
import Data.Word (Word32, Word64)
import Haskoin
import Haskoin.Node (LogLevel (..), Logger (..))
import Haskoin.Store.Data (BlockData (..))
import Haskoin.Store.Database.Migrate
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
import Haskoin.Store.Database.Writer (runWriter)
import Haskoin.Store.Logic
import System.Environment (getArgs)
import Text.Printf (printf)

net :: Network
net = btcRegTest

quiet :: Logger
quiet = Logger {level = LevelError, source = "", push = \_ -> return ()}

-- | A script paying to one of many addresses.
p2pkh :: Word32 -> ByteString
p2pkh n = "\x76\xa9\x14" <> B.take 20 (encode (sha256 (encode n))) <> "\x88\xac"

coinbase :: Tx
coinbase = Tx 1 [TxIn nullOutPoint (encode (1 :: Word32)) maxBound] [TxOut 5000000000 (p2pkh 0)] [] 0

-- | A tree of payments from a coin: each spends one output and makes two.
tree :: Int -> [Tx]
tree count = take count (go [(OutPoint (txHash coinbase) 0, 5000000000)] 1)
  where
    go [] _ = []
    go ((op, v) : rest) n =
      let half = (v - 400) `div` 2 :: Word64
          tx = Tx 2 [TxIn op "" maxBound] [TxOut half (p2pkh (2 * n)), TxOut half (p2pkh (2 * n + 1))] [] 0
          h = txHash tx
       in tx : go (rest <> [(OutPoint h 0, half), (OutPoint h 1, half)]) (n + 1)

chunks :: Int -> [a] -> [[a]]
chunks _ [] = []
chunks n xs = let (a, b) = splitAt n xs in a : chunks n b

timed :: String -> IO a -> IO a
timed what act = do
  t0 <- getCurrentTime
  x <- act
  t1 <- getCurrentTime
  printf "%s: %.2f seconds\n" what (realToFrac (diffUTCTime t1 t0) :: Double)
  return x

main :: IO ()
main = withContext $ \ctx ->
  getArgs >>= \case
    ["make", dir, count] ->
      withDatabaseReader net ctx 20 32 dir False Nothing Nothing Nothing Nothing Nothing $ \db -> do
        runWriter net ctx db quiet initBest
        Just best <- Q.getBestBlock db
        Just parent <- Q.getBlock db best
        let merkle = either error id (buildMerkleRoot [txHash coinbase])
            candidate = BlockHeader 0x20000000 best merkle (parent.header.timestamp + 600) parent.header.bits
        header <- maybe (fail "no nonce") return (find (isValidPOW net) (map candidate [0 ..]))
        let node = BlockNode header 1 0 best
        void (runWriter net ctx db quiet (\w -> importBlock w (Block header [coinbase]) node))
        timed ("adding " <> count <> " transactions to the mempool") $
          forM_ (zip [0 :: Word64 ..] (chunks 1000 (tree (read count)))) $ \(i, txs) ->
            runWriter net ctx db quiet (\w -> forM_ txs (\tx -> newMempoolTx w tx (1000 + i)))
        n <- length <$> Q.getMempool db
        printf "%d transactions in the mempool\n" n
    ["wipe", dir] ->
      withDatabaseReader net ctx 20 32 dir False Nothing Nothing Nothing Nothing Nothing $ \db -> do
        pool <- Q.getMempool db
        timed ("wiping " <> show (length pool) <> " transactions, as version 20 does") $
          forM_ (chunks 1000 pool) $ \txs ->
            runWriter net ctx db quiet (\w -> forM_ txs (\(_, h) -> deleteUnconfirmedTx w False h))
        left <- length <$> Q.getMempool db
        unless (left == 0) (fail "mempool not empty")
    ["migrate", dir] ->
      withLayeredReader net ctx 20 32 dir False Nothing Nothing Nothing Nothing Nothing $ \db -> do
        m <- timed "migrating" (migrateLayered db)
        print m
    _ -> fail "usage: make DIR COUNT | wipe DIR | migrate DIR"
