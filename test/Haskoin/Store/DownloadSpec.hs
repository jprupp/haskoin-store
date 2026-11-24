-- |
-- Module      : Haskoin.Store.DownloadSpec
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- How a store downloads the blocks of real nodes: Bitcoin Cash Node in
-- regtest, in Podman ("Haskoin.Store.Integration.Node"), reached through
-- the Unix sockets of its pod. The store has no Redis here: it syncs its
-- blocks, which is all these tests look at.
--
-- These tests replace the mock peers that served canned blocks to the store
-- in memory. A real node shows what a node does: how its headers arrive,
-- what it answers, how long it takes. A peer that hangs is a node stopped
-- with @SIGSTOP@ ('freezeNode').
--
-- Each test is a group at the top of the module, so that haskell-dev runs
-- each in a process of its own (@haskell-dev.toml@), with a pod of its own.
module Haskoin.Store.DownloadSpec (spec) where

import Conduit (ConduitT, await, yield, (.|))
import Control.Concurrent (threadDelay)
import Control.Exception (finally)
import Control.Monad (forM_, unless, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Foldable (toList)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Maybe (fromMaybe)
import Data.Serialize (decode, runGet)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock (NominalDiffTime)
import GHC.Stats (GCDetails (..), RTSStats (..), getRTSStats, getRTSStatsEnabled)
import Haskoin
import Haskoin.Node (Conduits (..), LogLevel (..), Logger (..), withConnection)
import Haskoin.Store
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Integration.Node hiding (nodeNetwork)
import Haskoin.Store.Integration.Node qualified as Node
import Haskoin.Store.Integration.Temp (withRamDir)
import Network.Socket (SockAddr)
import System.Mem (performMajorGC)
import System.Timeout (timeout)
import Test.Hspec
import Test.Podman (ContainerSpec (..), Pod, PodSpec (..), pod, podmanUsable, withPod)

spec :: Spec
spec = do
  usable <- runIO podmanUsable
  case usable of
    Left why ->
      describe "Downloads from regtest nodes in Podman" $
        it "check how the store downloads blocks" $
          pendingWith why
    Right () -> do
      describe "Downloads from Bitcoin Cash Node" $
        it "asks for blocks in windows of the block window, and keeps every transaction" windowTest
      describe "Downloads from two Bitcoin Cash Nodes, one that hangs" $
        it "asks the other node for the blocks that the frozen one does not deliver" stallTest
      describe "Downloads from Bitcoin Cash Node, memory" $
        it "keeps the heap flat while syncing" heapTest

kind :: NodeKind
kind = BitcoinCashNode

net :: Network
net = Node.nodeNetwork kind

-- | A store that syncs a chain of 102 blocks asks for them 64 at a time,
-- the block window, and the last 38; and it has every transaction of each
-- block, as the node has it.
windowTest :: IO ()
windowTest = withNodes 1 $ \_ nodes -> do
  n <- one nodes
  ensureWallet n
  miner <- getNewAddress n
  _ <- generateTo n 101 miner
  _ <- sendTo n miner 100000000
  tip <- lastOf <$> generateTo n 1 miner
  requests <- newIORef []
  withStoreOf [n] (counting requests) 10 $ \store -> do
    waitBlock store tip
    sizes <- reverse <$> readIORef requests
    sum sizes `shouldBe` 102
    take 1 sizes `shouldBe` [64]
    all (<= 64) sizes `shouldBe` True
    Just stored <- Q.getBlock store.db tip
    fromNode <- rpc n "getblock" [String (blockHashToHex tip)]
    let txids = [t | Just (Array ts) <- [field "tx" fromNode], String t <- toList ts]
    map txHashToHex stored.txs `shouldBe` txids
    length txids `shouldBe` 2
  where
    counting ref = \case
      MGetData (GetData ivs) -> atomicModifyIORef' ref (\xs -> (length ivs : xs, ()))
      _ -> return ()

-- | A store with two nodes, one of which hangs, has the blocks of the other
-- in a few seconds: it waits a second for a peer that delivers nothing
-- ('blockStall'), not the minute of its timeout. The store may ask either
-- node first, so what this checks is that a frozen node does not hold it
-- back.
stallTest :: IO ()
stallTest = withNodes 2 $ \p nodes -> case nodes of
  [n0, n1] -> do
    ensureWallet n0
    miner <- getNewAddress n0
    first <- lastOf <$> generateTo n0 101 miner
    waitUntil 60 "the second node to have the chain" $
      (== Just first) . hexToBlockHash <$> bestBlockHash n1
    withStoreOf [n0, n1] (\_ -> return ()) 1 $ \store -> do
      waitBlock store first
      freezeNode p 0 kind
      flip finally (thawNode p 0 kind) $ do
        ensureWallet n1
        other <- getNewAddress n1
        tip <- lastOf <$> generateTo n1 20 other
        got <- timeout 20000000 (waitBlock store tip)
        got `shouldBe` Just ()
  _ -> expectationFailure "Not two nodes"

-- | The heap of a store does not grow with the blocks it syncs. When
-- 'forever' went through a dictionary, each of six loops kept 33 to 107
-- bytes per message, a few hundred bytes per block in all: over the 5000
-- blocks measured, megabytes. A leak of 200 bytes per block fails the test.
heapTest :: IO ()
heapTest = do
  enabled <- getRTSStatsEnabled
  unless enabled $ pendingWith "run with +RTS -T to measure the heap"
  withNodes 1 $ \_ nodes -> do
    n <- one nodes
    ensureWallet n
    miner <- getNewAddress n
    -- in pieces, each within the time that an RPC call may take
    forM_ [1 .. 6 :: Int] $ \_ -> generateTo n 1000 miner
    tip <- hashOf =<< bestBlockHash n
    at1000 <- hashOf . textOf =<< rpc n "getblockhash" [Number 1000]
    withStoreOf [n] (\_ -> return ()) 10 $ \store -> do
      waitBlock store at1000
      early <- live
      waitBlock store tip
      late <- live
      let growth = fromIntegral late - fromIntegral early :: Integer
      putStrLn $ "heap growth over 5000 blocks: " <> show growth <> " bytes"
      growth `shouldSatisfy` (< 1024 * 1024)
  where
    live = do
      performMajorGC
      (.gc.gcdetails_live_bytes) <$> getRTSStats
    textOf = \case
      String t -> t
      v -> error ("Not a string: " <> show v)

-- | A pod of so many Bitcoin Cash Nodes, each after the first a peer of the
-- one before, with their RPC answering.
withNodes :: Int -> (Pod -> [Node] -> IO a) -> IO a
withNodes count go = do
  let spec' i =
        let c = nodeContainerAt i kind
         in if i == 0
              then c
              else c {command = c.command <> ["-addnode=127.0.0.1:" <> show (18444 + 10 * (i - 1))]}
      nodes = map spec' [0 .. count - 1]
  withPod (pod "download" nodes) {readyWithin = 120} $ \p -> do
    ns <- mapM (\i -> nodeAt i kind p) [0 .. count - 1]
    mapM_ waitRpc ns
    go p ns

-- | A store that syncs from these nodes and from nothing else, with no
-- Redis: it is given the messages that it sends to its peers to look at,
-- and the seconds that a peer may deliver nothing before another is asked
-- for its blocks ('blockStall').
withStoreOf :: [Node] -> (Message -> IO ()) -> NominalDiffTime -> (Store -> IO a) -> IO a
withStoreOf nodes seen stall go =
  withRamDir $ \dir ->
    withStore (config dir) go
  where
    config dir =
      StoreConfig
        { maxPeers = 8,
          initPeers = map (.peer) nodes,
          peerPermissions = mempty,
          foreignPeers = False,
          discover = False,
          db = dir,
          net = net,
          redis = Nothing,
          gap = 32,
          initGap = 20,
          redisMaxXPubs = 10000,
          xpubs = defaultXPubConfig,
          blockWindow = 64,
          mempoolTimeout = 14,
          peerTimeout = 60,
          blockStall = stall,
          blockPing = 5,
          maxPeerLife = 48 * 3600,
          connect = tapped seen,
          nodePeerRpc = Nothing,
          nodePeerRpcSocket = Nothing,
          nodePeerZmq = Nothing,
          stats = Nothing,
          bloom = True,
          maxOpenFiles = Just 256,
          writeBufferSize = Just (16 * 1024 * 1024),
          maxWalSize = Just (64 * 1024 * 1024),
          backgroundJobs = Nothing,
          blockCache = Just (16 * 1024 * 1024),
          logger = Logger {level = LevelError, source = "", push = \_ -> return ()}
        }

-- | 'withConnection', with every message that the store sends given to a
-- function on its way.
tapped :: (Message -> IO ()) -> SockAddr -> (Conduits -> IO ()) -> IO ()
tapped seen sa f =
  withConnection sa $ \c -> f c {outboundConduit = frames B.empty .| c.outboundConduit}
  where
    frames :: ByteString -> ConduitT ByteString ByteString IO ()
    frames buf =
      await >>= \case
        Nothing -> return ()
        Just bs -> yield bs >> liftIO (split (buf <> bs)) >>= frames
    -- the messages whole in the bytes so far, and the bytes left over
    split buf
      | B.length buf < 24 = return buf
      | otherwise = case decode (B.take 24 buf) of
          Left _ -> return B.empty
          Right (MessageHeader _ _ len _) -> do
            let total = 24 + fromIntegral len
            if B.length buf < total
              then return buf
              else do
                forM_ (runGet (getMessage net) (B.take total buf)) seen
                split (B.drop total buf)

-- | Wait until the store has a block as its best, or one after it.
waitBlock :: Store -> BlockHash -> IO ()
waitBlock store h =
  waitUntil 120 ("the store to have block " <> T.unpack (blockHashToHex h)) $ do
    best <- Q.getBestBlock store.db
    case best of
      Nothing -> return False
      Just b
        | b == h -> return True
        | otherwise ->
            Q.getBlock store.db h >>= \case
              Just bd -> return (bd.height > 0)
              Nothing -> return False

waitUntil :: Int -> String -> IO Bool -> IO ()
waitUntil secs what check = go (secs * 4)
  where
    go n = do
      ok <- check
      unless ok $ do
        when (n <= 0) $ expectationFailure ("Gave up waiting for " <> what)
        threadDelay 250000
        go (n - 1)

one :: [Node] -> IO Node
one = \case
  [n] -> return n
  ns -> fail ("Expected one node, got " <> show (length ns))

-- | The last of the blocks that an RPC call mined.
lastOf :: [Text] -> BlockHash
lastOf hs = case reverse hs of
  h : _ -> fromMaybe (error ("Not a block hash: " <> T.unpack h)) (hexToBlockHash h)
  [] -> error "No block mined"

hashOf :: Text -> IO BlockHash
hashOf t = maybe (fail ("Not a block hash: " <> T.unpack t)) return (hexToBlockHash t)
