-- |
-- Module      : Haskoin.Store.MempoolSpec
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- That the shared mempool of a store converges to the mempool of its node,
-- against a real node in Podman: the theorem of @docs\/mempool-ingestion.md@
-- as a test, in the situations in which stores were found not to converge.
--
-- On 21 November 2026 the btc store of a server fell 17,000 transactions
-- behind its node, and three fixes in a row each broke something else on
-- that server, because nothing reproduced the failures off it. Each test
-- here is one of them, on a regtest node whose mempool is filled the way
-- that node's was: with chains of 24 transactions, a root and its
-- descendants, all taken within seconds, each with an output of value 0
-- that holds data and one that pays, as the mints of Runes are.
--
-- The transactions spend outputs that anyone can spend, pay-to-script-hash
-- of @OP_TRUE@, so that the tests make thousands of them without a wallet
-- and without signing. Both nodes take them as standard.
--
-- A test is a group with a pod of its own, so that they run at the same
-- time, each in a process (@haskell-dev.toml@).
module Haskoin.Store.MempoolSpec (spec) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (forConcurrently_, link, withAsync)
import Control.Exception (throwIO, try)
import Control.Monad (forM_, unless, void, when)
import Data.Aeson (Value (..))
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.List (transpose)
import Data.Maybe (fromMaybe)
import Data.Serialize (encode)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word64)
import Database.Redis qualified as Redis
import Haskoin
  ( Network,
    OutPoint (..),
    Tx (..),
    TxIn (..),
    TxOut (..),
    addrToText,
    addressHash,
    encodeHex,
    hexToTxHash,
    scriptToAddressBS,
    txHash,
  )
import Haskoin.Node (Logger, Permissions)
import Haskoin.Store (Store, runWebSocket, withStore)
import Haskoin.Store.Integration.Node
import Haskoin.Store.Integration.Temp (withRamDir)
import Haskoin.Store.Integration.Valkey (redisContainer)
import Haskoin.Store.Integration.Valkey qualified as Valkey
import Haskoin.Store.IntegrationSpec
  ( Server,
    arr,
    getJSON,
    grantedBy,
    listening,
    storeConfig,
    webConfig,
    withTestLog,
  )
import Haskoin.Store.Startup (redisConnectInfo)
import Network.HTTP.Client (HttpException)
import System.IO (hPutStrLn, stderr)
import Test.Hspec
import Test.Podman (PodSpec (..), pod, podmanUsable, withPod)

spec :: Spec
spec = do
  usable <- runIO podmanUsable
  case usable of
    Left why -> describe "The mempool of a store and that of its node" $ it "are the same" (pendingWith why)
    Right () -> do
      describe "A store that starts from nothing" $
        it "has the mempool of its node once it has its blocks" (withBed BitcoinCore fromNothing)
      describe "A store that starts at the tip with an empty pool" $
        it "takes the whole mempool of its node, and stays healthy and connected" (withBed BitcoinCore atTheTip)
      describe "A store that was away while transactions came" $
        it "has them when it is back" (withBed BitcoinCore (away False))
      describe "A store that was away while transactions and a block came" $
        it "has what the block left in the mempool when it is back" (withBed BitcoinCore (away True))
      describe "A store whose node takes chains while it finds blocks" $
        it "has every transaction that no block confirmed" (withBed BitcoinCore busy)

-------------------------------------------------------------------------------
-- The tests
-------------------------------------------------------------------------------

-- | A store with no database and an empty pool, started on a node that has
-- 150 blocks and a mempool: it has the blocks to sync first.
fromNothing :: Bed -> IO ()
fromNothing bed = do
  roots <- fund bed 40
  submit bed (map (chainOf 24) roots)
  withMember bed $ \_ server -> converges bed server 90

-- | The experiment that was made on the server: a store whose database is
-- at the tip of its node, started with an empty pool on a node with a
-- large mempool. While it takes the mempool, its health never fails for
-- the transactions that it asked for, and it keeps its peer.
atTheTip :: Bed -> IO ()
atTheTip bed = withRamDir $ \dir -> do
  roots <- fund bed 200
  withMemberIn bed dir $ \_ server -> converges bed server 60
  submit bed (map (chainOf 24) roots)
  flush bed
  withMemberIn bed dir $ \_ server -> do
    worst <- newIORef (0 :: Integer, 1 :: Integer)
    let sample = do
          (_, h) <- getJSON server "/health"
          let count k = fromMaybe 0 (field k h >>= field "count" >>= number)
          modifyIORef' worst (\(p, n) -> (max p (count "pending-txs"), min n (count "peers")))
    convergesWith sample bed server 120
    (pending, peers) <- readIORef worst
    -- what the health check of a store allows, by default
    pending `shouldSatisfy` (<= 10000)
    peers `shouldBe` 1
  where
    number = \case
      Number n -> Just (round n)
      _ -> Nothing

-- | A store that ran, stopped, and starts again on its database and its
-- pool, after its node took transactions that it could not hear of, and
-- perhaps a block, which leaves the store behind its node when it starts.
away :: Bool -> Bed -> IO ()
away block bed = withRamDir $ \dir -> do
  roots <- fund bed 40
  let (before, after) = splitAt 10 roots
  withMemberIn bed dir $ \_ server -> do
    submit bed (map (chainOf 24) before)
    converges bed server 60
  -- half of each chain while the store is away, then a block, then the rest
  let chains = map (chainOf 24) after
  submit bed (map (take 12) chains)
  when block . void $ generateTo bed.node 1 bed.miner
  submit bed (map (drop 12) chains)
  withMemberIn bed dir $ \_ server -> converges bed server 60

-- | A store that runs while its node takes chains and finds blocks at the
-- same time, round after round: an announcement may reach the store while
-- it is a block behind.
busy :: Bed -> IO ()
busy bed = do
  roots <- fund bed 200
  withMember bed $ \_ server -> do
    converges bed server 60
    forM_ (pieces 10 roots) $ \some ->
      withAsync (submit bed (map (chainOf 24) some)) $ \sending -> do
        link sending
        -- two blocks while the chains go in
        forM_ [1 :: Int, 2] $ \_ -> threadDelay 150000 >> generateTo bed.node 1 bed.miner
        -- what the blocks left out goes in after them
        threadDelay 500000
    converges bed server 60
  where
    pieces _ [] = []
    pieces n xs = let (a, b) = splitAt n xs in a : pieces n b

-------------------------------------------------------------------------------
-- Convergence
-------------------------------------------------------------------------------

-- | Within so many seconds, the store has the best block of its node and
-- its mempool is that of the node, the same transactions and no other; and
-- it is still so two seconds later, the node having taken nothing since.
converges :: Bed -> Server -> Int -> IO ()
converges = convergesWith (return ())

-- | 'converges', with something to do at each look.
convergesWith :: IO () -> Bed -> Server -> Int -> IO ()
convergesWith each bed server seconds = do
  start <- getPOSIXTime
  let go = do
        each
        (node, ours, same) <- look
        now <- getPOSIXTime
        if same && node == ours
          then return ()
          else
            if now - start > fromIntegral seconds
              then do
                recent <- bed.logTail
                when (Set.size node /= Set.size ours || node /= ours) $
                  hPutStrLn stderr . unlines $
                    ["--- the last lines of the log of the store"] <> recent
                expectationFailure . unwords $
                  [ "After",
                    show seconds,
                    "seconds the node has",
                    show (Set.size node),
                    "transactions in its mempool and the store",
                    show (Set.size ours) <> ":",
                    show (Set.size (node `Set.difference` ours)),
                    "that the store lacks,",
                    show (Set.size (ours `Set.difference` node)),
                    "that only the store has; same best block:",
                    show same
                  ]
              else threadDelay 250000 >> go
  go
  -- and it stays: nothing was on its way out
  threadDelay 2000000
  (node, ours, same) <- look
  unless (same && node == ours) $
    expectationFailure "The store had the mempool of its node, and no longer has two seconds later"
  where
    look = do
      best <- bestBlockHash bed.node
      node <- Set.fromList <$> rawMempool bed.node
      (_, b) <- getJSON server "/block/best?notx=true"
      (_, m) <- getJSON server "/mempool?limit=1000000"
      let ours = Set.fromList [t | String t <- list m]
      return (node, ours, textField "hash" b == Just best)
    list = \case
      v@(Array _) -> arr v
      _ -> []

-------------------------------------------------------------------------------
-- Chains of transactions
-------------------------------------------------------------------------------

-- | The script that anyone can spend: @OP_TRUE@.
redeem :: ByteString
redeem = B.singleton 0x51

-- | An output script that pays to the hash of 'redeem'.
lock :: ByteString
lock = B.concat [B.pack [0xa9, 0x14], encode (addressHash redeem), B.singleton 0x87]

-- | An output script that holds data, of value 0: @OP_RETURN@ and twenty
-- bytes.
note :: ByteString
note = B.pack [0x6a, 0x14] <> B.replicate 20 0x2a

-- | A transaction that spends an output locked by 'lock'.
spend :: OutPoint -> [TxOut] -> Tx
spend op outs = Tx 2 [TxIn op (B.pack [0x01, 0x51]) maxBound] outs [] 0

-- | What each transaction of a chain pays its node.
fee :: Word64
fee = 5000

-- | A chain of so many transactions from an output and its value: each
-- has an output of value 0 that holds data and pays the rest to the next.
chainOf :: Int -> (OutPoint, Word64) -> [Tx]
chainOf 0 _ = []
chainOf n (op, value) =
  let tx = spend op [TxOut 0 note, TxOut (value - fee) lock]
   in tx : chainOf (n - 1) (OutPoint (txHash tx) 1, value - fee)

-- | So many confirmed outputs that anyone can spend, each with its value,
-- for chains to start from: the wallet of the node pays one output, a
-- transaction splits it, and a block confirms both.
fund :: Bed -> Int -> IO [(OutPoint, Word64)]
fund bed count = do
  let each = 1000000
      total = fromIntegral count * each + 100000
  address <- case scriptToAddressBS bed.net lock >>= maybe (Left "no text") Right . addrToText bed.net of
    Right a -> return a
    Left e -> fail ("No address for the script: " <> e)
  txid <- sendTo bed.node address total
  paid <- getRawTx bed.node txid
  let outs = maybe [] arr (field "vout" paid)
      mine =
        [ n
        | o <- outs,
          (field "scriptPubKey" o >>= textField "hex") == Just (encodeHex lock),
          Just (Number n) <- [field "n" o]
        ]
  index <- case mine of
    [n] -> return (round n)
    _ -> fail "The wallet did not pay the script once"
  hash <- maybe (fail "Not a transaction id") return (hexToTxHash txid)
  let split = spend (OutPoint hash index) (replicate count (TxOut each lock))
  send bed split
  void $ generateTo bed.node 1 bed.miner
  return [(OutPoint (txHash split) i, each) | i <- [0 .. fromIntegral count - 1]]

-- | Give the node a transaction. The socket of its RPC takes few
-- connections at once, and refuses one more with an error of the HTTP
-- client: such a call is made again, a few times.
send :: Bed -> Tx -> IO ()
send bed tx = go (5 :: Int)
  where
    go n =
      try (rpc bed.node "sendrawtransaction" [String (encodeHex (encode tx))]) >>= \case
        Right _ -> return ()
        Left (e :: HttpException)
          | n > 0 -> threadDelay 100000 >> go (n - 1)
          | otherwise -> throwIO e

-- | Give the node chains of transactions, each in its order, four chains
-- at a time.
submit :: Bed -> [[Tx]] -> IO ()
submit bed chains = forConcurrently_ (transpose (groups chains)) (mapM_ (mapM_ (send bed)))
  where
    groups [] = []
    groups xs = let (a, b) = splitAt 4 xs in a : groups b

-------------------------------------------------------------------------------
-- The bed of a test
-------------------------------------------------------------------------------

-- | A node that whitelists the store, with 150 blocks, and a Valkey, in a
-- pod: what a store needs, without the store.
data Bed = Bed
  { node :: !Node,
    net :: !Network,
    -- | address of the wallet of the node, to mine to
    miner :: !Text,
    redisURL :: !String,
    granted :: !Permissions,
    logger :: !Logger,
    -- | the last lines of the log of the stores of the bed
    logTail :: !(IO [String])
  }

withBed :: NodeKind -> (Bed -> IO ()) -> IO ()
withBed kind go =
  withPod (pod "mempool" [redisContainer, nodeContainer kind]) {readyWithin = 120} $ \p -> do
    node <- nodeOf kind p
    waitRpc node
    ensureWallet node
    miner <- getNewAddress node
    void $ generateTo node 150 miner
    withTestLog $ \logger logTail ->
      go
        Bed
          { node,
            net = nodeNetwork kind,
            miner,
            redisURL = Valkey.redisURL p,
            granted = grantedBy True kind,
            logger,
            logTail
          }

-- | Empty the Valkey of the bed, as a server whose Redis lost its data.
flush :: Bed -> IO ()
flush bed = do
  conn <- Redis.checkedConnect (either error id (redisConnectInfo bed.redisURL))
  void (Redis.runRedis conn Redis.flushall)
  Redis.disconnect conn

-- | A store of the bed with its web server, on a database in a folder,
-- while an action runs.
withMemberIn :: Bed -> FilePath -> (Store -> Server -> IO a) -> IO a
withMemberIn bed dir f =
  withStore (storeConfig bed.net bed.logger dir bed.redisURL bed.node.peer bed.granted) $ \store ->
    listening $ \(sock, server) ->
      withAsync (runWebSocket (webConfig bed.logger store) sock) $ \web ->
        link web >> f store server

-- | 'withMemberIn' a new folder.
withMember :: Bed -> (Store -> Server -> IO a) -> IO a
withMember bed f = withRamDir $ \dir -> withMemberIn bed dir f
