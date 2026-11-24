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
module Haskoin.Store.MempoolSpec
  ( spec,

    -- * For the specs of what a node does
    Bed (..),
    withBed,
    withBedOf,
    bare,
    chainOf,
    fund,
    send,
    submit,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (forConcurrently_, link, withAsync)
import Control.Exception (finally, throwIO, try)
import Control.Monad (forM_, unless, void, when)
import Data.Aeson (Value (..))
import Data.Aeson.KeyMap qualified as KeyMap
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.List (isInfixOf, transpose)
import Data.Maybe (fromMaybe)
import Data.Serialize (encode)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word32, Word64)
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
    txHashToHex,
  )
import Haskoin.Node (Logger, Permissions)
import Haskoin.Store (Store, StoreConfig (..), WebConfig (..), cachePrefix, runWebSocket, withStore)
import Haskoin.Store.Fleet.Redis (fleetKeys, poolWithdraw)
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
import System.Timeout (timeout)
import Test.Hspec
import Test.Podman (ContainerSpec (..), Pod, PodSpec (..), containerName, pod, podman, podmanUsable, withPod)

spec :: Spec
spec = do
  usable <- runIO podmanUsable
  case usable of
    Left why -> describe "The mempool of a store and that of its node" $ it "are the same" (pendingWith why)
    Right () -> do
      forM_ [BitcoinCore, BitcoinCashNode] $ \kind -> do
        -- a group for each test on each node, with a pod of its own
        let on what = describe (what <> ", on " <> name kind)
        on "A store that starts from nothing" $
          it "has the mempool of its node once it has its blocks" (withBed kind fromNothing)
        on "A store that starts at the tip with an empty pool" $
          it "takes the whole mempool of its node, and stays healthy and connected" (withBed kind atTheTip)
        on "A store that was away while transactions came" $
          it "has them when it is back" (withBed kind (away False))
        on "A store that was away while transactions and a block came" $
          it "has what the block left in the mempool when it is back" (withBed kind (away True))
        on "A store whose Valkey is stopped and started again under it" $
          it "has the mempool of its node again, without being started again" (withBed kind valkeyRestart)
        on "A store whose Valkey is emptied under it" $
          it "says so, and has the mempool of its node again within fifteen seconds" (withBed kind valkeyFlush)
        on "A store whose pool let transactions go that its node still has" $
          it "has them again when it next reads the mempool of its node" (withBed kind letGo)
        on "A store whose pool lost transactions that its node then takes children of" $
          it "asks its node for them, and has them and their children" (withBed kind bereft)
        on "A store whose node takes chains while it finds blocks" $
          it "has every transaction that no block confirmed" (withBed kind busy)
        on "A store whose only peer is its node through its RPC and its ZeroMQ socket" $
          it "has its blocks, its mempool, what the node takes next and its next block, with no other connection" $
            withBed kind (alone . \bed -> bed {viaRpc = True, viaZmq = True})
        on "A store whose only peer is its node through its RPC and its ZeroMQ socket, when the node stops" $
          it "has no peer and says so, and has its node again when the node goes on" $
            withBed kind (nodeStops . \bed -> bed {viaRpc = True, viaZmq = True})
        on "A store whose node never stops taking transactions" $
          it "answers from each new block within seconds of it" (withBed kind unceasing)
      describe "A store whose only peer is a node of somebody else, on Bitcoin Core" $
        it "takes of it what it can verify, and not what it cannot" (withBed BitcoinCore strangers)
      -- Bitcoin Cash Node names what it takes to a peer that it
      -- whitelists as fast as it takes it
      describe "A store whose node takes transactions faster than it names them, on Bitcoin Core" $
        it "has them within the time of a reading of the node's mempool, and not at the node's pace" $
          withBed BitcoinCore burst
      describe "A store that hears what its node publishes over ZeroMQ, on Bitcoin Core" $
        it "has what the node takes as fast as the node takes it, with no reading of its mempool" $
          withBed BitcoinCore (hears . \bed -> bed {viaRpc = True, viaZmq = True})
      -- Bitcoin Cash Node replaces nothing by fee
      describe "A store whose node replaces transactions by ones that pay more, on Bitcoin Core" $
        it "has the replacements, and nothing of what they replaced" (withBed BitcoinCore outbid)
      -- Bitcoin Cash Node gives its mempool to any peer, so only a store
      -- of Bitcoin Core can have no peer that is a source
      forM_ [False, True] $ \block ->
        describe ("A store that no peer gives a mempool to, with the RPC of its node" <> withBlock block) $
          it "has what came while it was away" $
            withBedOf (bare BitcoinCore) BitcoinCore (away block . rpcOnly)
      describe "A store with the RPC of its node, whose node takes transactions while the two are not connected" $
        it "has them once it is connected again" $
          withBedOf (bare BitcoinCore) BitcoinCore (cutOff . rpcOnly)
  where
    name BitcoinCore = "Bitcoin Core"
    name BitcoinCashNode = "Bitcoin Cash Node"
    withBlock block = if block then ", and a block while it is away" else ""
    -- the node grants the store nothing, and the store has its RPC
    rpcOnly bed = bed {granted = Set.empty, viaRpc = True}

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
    -- the most requests out, whether the peer was seen, and whether it
    -- was lost after that
    worst <- newIORef (0 :: Integer, False, False)
    let sample = do
          (_, h) <- getJSON server "/health"
          let count k = fromMaybe 0 (field k h >>= field "count" >>= number)
              there = count "peers" > 0
          modifyIORef' worst $ \(p, seen, lost) ->
            (max p (count "pending-txs"), seen || there, lost || (seen && not there))
    convergesWith sample bed server 120
    -- the store may have its node's mempool before its health is next
    -- checked, which is when it first says that it has a peer: look until
    -- it does, for longer than the time between two checks
    let awaited n = do
          (_, seen, _) <- readIORef worst
          unless (seen || n <= (0 :: Int)) (threadDelay 500000 >> sample >> awaited (n - 1))
    awaited 120
    (pending, seen, lost) <- readIORef worst
    -- what the health check of a store allows, by default
    pending `shouldSatisfy` (<= 10000)
    (seen, lost) `shouldBe` (True, False)
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

-- | A node that takes 2,500 transactions in a few seconds. Bitcoin Core
-- names about five hundred of them to a peer at once, and then seventy
-- every five seconds, to a peer that it whitelists too
-- (@namesAtItsPace@ in "Haskoin.Store.NodeAxiomsSpec"): at that pace the
-- store would have them all after two minutes and a half. It reads the
-- mempool of its node every thirty seconds while transactions come
-- (@told@ in "Haskoin.Store.Ingest"), so it has them all within the
-- time of a reading and of taking them in: here 31 seconds after the
-- first came, where it has 1,800 of them after a minute without.
--
-- A server was a minute and more behind its node, by thousands of
-- transactions, whenever the node took more than fourteen a second, and
-- read the node's mempool every ten minutes.
burst :: Bed -> IO ()
burst bed = do
  roots <- fund bed 2500
  withMember bed $ \_ server -> do
    converges bed server 60
    submit bed (map (chainOf 1) roots)
    converges bed server 60

-- | A store that is given no peer at all, and the RPC of its node with
-- the address of its ZeroMQ socket (@--node-rpc@, @--node-zmq@): the
-- node is its one peer, through those two ("Haskoin.Store.NodePeer"),
-- and there is no connection of the peer-to-peer protocol.
--
-- It starts from nothing on a node that has 150 blocks and a mempool.
-- It must have the blocks, which come over the RPC; the mempool, which
-- the node names when asked; what the node takes after that, which the
-- node publishes; and the block that the node finds then, which it
-- publishes too. Its health says that it has one peer.
alone :: Bed -> IO ()
alone bed = withRamDir $ \dir -> do
  roots <- fund bed 40
  let (before, after) = splitAt 20 roots
  submit bed (map (chainOf 4) before)
  withMemberOf (\c -> c {initPeers = []} :: StoreConfig) bed dir $ \_ server -> do
    converges bed server 120
    (_, health) <- getJSON server "/health"
    (field "peers" health >>= field "count") `shouldBe` Just (Number 1)
    submit bed (map (chainOf 4) after)
    converges bed server 30
    void (generateTo bed.node 1 bed.miner)
    converges bed server 30

-- | The same store when its node stops in its tracks, as a node that
-- hangs: the pipe to the peer is in memory and never breaks, and the
-- thread at its other end could answer for a node that is gone. It
-- answers a ping only once the node answered a call, so the store finds
-- its peer silent and lets it go, as it does of any peer: its health
-- says that it has none. When the node goes on, the store has it as a
-- peer again, with what the node took meanwhile.
nodeStops :: Bed -> IO ()
nodeStops bed = withRamDir $ \dir -> do
  roots <- fund bed 20
  let (before, after) = splitAt 10 roots
      quick c = c {initPeers = [], peerTimeout = 10} :: StoreConfig
  withMemberOf quick bed dir $ \_ server -> do
    submit bed (map (chainOf 4) before)
    converges bed server 120
    freezeNode bed.pod 0 bed.node.kind
    peers server 0 90 `finally` thawNode bed.pod 0 bed.node.kind
    submit bed (map (chainOf 4) after)
    peers server 1 90
    converges bed server 90
  where
    -- until the health of the store says that it has so many peers
    peers server n seconds = do
      (_, health) <- getJSON server "/health"
      unless ((field "peers" health >>= field "count") == Just (Number n)) $
        if seconds <= (0 :: Int)
          then expectationFailure ("The store does not come to have " <> show n <> " peers")
          else threadDelay 1000000 >> peers server n (seconds - 1)

-- | A store that has its node as a peer twice, over the peer-to-peer
-- protocol and through its RPC and its ZeroMQ socket (@--node-rpc@,
-- @--node-zmq@), while the node takes hundreds of transactions in a few
-- seconds, twice.
--
-- Bitcoin Core names to a peer about five hundred at once and then
-- fourteen a second (A8), so that a store that only listens to its peer
-- is behind for as long as a burst lasts, until it reads the mempool of
-- its node, every thirty seconds. Over ZeroMQ the node publishes the id
-- of each transaction as it takes it (F2), and the store fetches it over
-- the RPC: it has them all within five seconds of the last.
--
-- A reading of the mempool would bring them too, and one may fall in any
-- five seconds. So the test looks at two bursts, which are less than
-- thirty seconds apart: a reading cannot fall in both. In one of them at
-- least the store must have had everything with no reading, and in none
-- may it be late without one.
hears :: Bed -> IO ()
hears bed = withRamDir $ \dir -> do
  roots <- fund bed 1600
  -- two peers: the node over the peer-to-peer protocol, and the same node
  -- through its RPC and its ZeroMQ socket
  withMemberOf (\c -> c {maxPeers = 2} :: StoreConfig) bed dir $ \_ server -> do
    converges bed server 60
    both server 90
    let (first, second) = splitAt 800 roots
    rounds <- mapM (round' server) [first, second]
    -- a round that a reading fell in says nothing, and both cannot be
    filter fst rounds `shouldSatisfy` (not . null)
    [late | (True, late) <- rounds] `shouldSatisfy` all not
  where
    -- whether no reading fell in the round, and whether the store was
    -- then late
    round' server some = do
      before <- readings
      submit bed (map (chainOf 1) some)
      inTime <- timeout 5000000 (awaitSame server)
      after <- readings
      -- whatever happened, the store ends with the mempool of its node
      converges bed server 60
      return (after == before, inTime == Nothing)
    -- until the store has its two peers
    both server seconds = do
      (_, health) <- getJSON server "/health"
      unless ((field "peers" health >>= field "count") == Just (Number 2)) $
        if seconds <= (0 :: Int)
          then expectationFailure "The store does not come to have two peers"
          else threadDelay 1000000 >> both server (seconds - 1)
    awaitSame server = do
      node <- Set.fromList <$> rawMempool bed.node
      (_, m) <- getJSON server "/mempool?limit=1000000"
      let ours = Set.fromList [t | String t <- case m of v@(Array _) -> arr v; _ -> []]
      unless (node == ours) (threadDelay 200000 >> awaitSame server)
    -- how many times the store asked a source for its whole mempool
    readings = do
      said <- takeWhile (/= "--- at every level") <$> bed.logTail
      return (length (filter (\l -> "for its mempool" `isInfixOf` l || "Reading the mempool" `isInfixOf` l) said))

-- | A node that takes chains of transactions, and then, for the first of
-- each chain, another that spends the same output and pays more: it
-- replaces the first and drops what spent from it (replace-by-fee, which
-- a node of Bitcoin does for any unconfirmed transaction). The store has
-- to follow: a node is the judge of what is in its mempool, and a store
-- that kept what its node dropped, or turned away what its node took,
-- would answer what no node would. The replacements pay ten times the
-- fees of what they replace.
outbid :: Bed -> IO ()
outbid bed = do
  roots <- fund bed 8
  withMember bed $ \_ server -> do
    submit bed (map (chainOf 3) roots)
    converges bed server 60
    forM_ roots $ \(op, value) -> send bed (spend op [TxOut 0 note, TxOut (value - 30 * fee) lock])
    converges bed server 60

-- | A store that has the RPC of a node that whitelists nobody, and is its
-- peer: the node closes its connections, takes transactions, and takes
-- connections again. It names to a peer nothing that it took before the
-- peer connected (A1), and its RPC names nothing by itself, so the store
-- has them only by reading the RPC again once it is connected: a peer
-- that connects owes what its node took before.
--
-- A store read the RPC of its node when it started and connected to the
-- node a moment later. What the node took in between was in neither: one
-- run in many of another test here lost four transactions so, one for
-- each of the four that are sent at once, and ended with the 84 that
-- spent from them waiting.
cutOff :: Bed -> IO ()
cutOff bed = do
  roots <- fund bed 6
  withMember bed $ \_ server -> do
    converges bed server 60
    -- A store that starts on a new database starts its pool again once it
    -- has its blocks, and reads the RPC again for that, ten seconds after
    -- the first time. The node is cut off after that reading, so that
    -- nothing but the peer that connects again makes the store read.
    threadDelay 15000000
    void (rpc bed.node "setnetworkactive" [Bool False])
    -- the node closes its connections a moment after it is told to
    let alone = null . arr <$> rpc bed.node "getpeerinfo" []
        awaitAlone n = do
          gone <- alone
          unless gone $
            if n > (0 :: Int) then threadDelay 100000 >> awaitAlone (n - 1) else fail "The node kept its peer"
    awaitAlone 100
    -- transactions that nothing spends from: nothing asks for them
    submit bed (map (chainOf 1) roots)
    void (rpc bed.node "setnetworkactive" [Bool True])
    converges bed server 60

-- | A store that runs while its pool loses transactions, without the
-- store being told, and whose node then takes a transaction that spends
-- from each. The node named the lost ones once and will not name them
-- again, so nothing brings them back but the store asking for them when
-- the pool says that it lacks what a child spends from (@seek@ in
-- "Haskoin.Store.Ingest").
--
-- Without that the children waited, tried again after two seconds, four,
-- eight, and were given up after twenty minutes, when the store read the
-- mempool of its node again: a test ended with 84 transactions waiting
-- behind four.
bereft :: Bed -> IO ()
bereft bed = do
  roots <- fund bed 6
  let chains = map (chainOf 2) roots
  withMember bed $ \_ server -> do
    submit bed (map (take 1) chains)
    converges bed server 60
    conn <- Redis.checkedConnect (either error id (redisConnectInfo bed.redisURL))
    left <- poolWithdraw conn (fleetKeys (cachePrefix bed.net) 32) 32 [encode (txHash tx) | tx : _ <- chains]
    Redis.disconnect conn
    length left `shouldBe` 6
    submit bed (map (drop 1) chains)
    converges bed server 60

-- | A store that stops, whose pool then lets go of the first transaction
-- of each chain and of all that spends it, as when another transaction
-- took their place and was itself taken out later, and that starts
-- again: the node has them all, so the store has them all again.
--
-- The fleet remembers what its pool decided on, the transactions that it
-- let go among them, and a store did not fetch what the fleet remembered:
-- on a server, a transaction that the node held for hours stayed out of
-- the store through every reading of the node's mempool. Only what the
-- pool has counts as settled now.
letGo :: Bed -> IO ()
letGo bed = withRamDir $ \dir -> do
  roots <- fund bed 6
  let chains = map (chainOf 3) roots
  withMemberIn bed dir $ \_ server -> do
    submit bed chains
    converges bed server 60
  conn <- Redis.checkedConnect (either error id (redisConnectInfo bed.redisURL))
  left <- poolWithdraw conn (fleetKeys (cachePrefix bed.net) 32) 32 [encode (txHash tx) | tx : _ <- chains]
  Redis.disconnect conn
  length left `shouldBe` 18
  withMemberIn bed dir $ \_ server -> converges bed server 60

-- | A store whose Valkey is stopped and started again while it runs, as
-- when the package of Valkey is upgraded on a server. The Valkey of the
-- tests keeps nothing, so it comes back empty: every connection of the
-- store is broken at once, the pool is gone, and the store has to notice
-- both, and fill the pool again from its node, by itself. Half of the
-- transactions come before the restart and half after.
valkeyRestart :: Bed -> IO ()
valkeyRestart bed = do
  roots <- fund bed 40
  let (before, after) = splitAt 20 roots
  withMember bed $ \_ server -> do
    submit bed (map (chainOf 24) before)
    converges bed server 60
    void (podman ["restart", "--time", "1", containerName bed.pod redisContainer.name])
    submit bed (map (chainOf 24) after)
    converges bed server 120

-- | A store that was told that its peer is a node of somebody else
-- (@--foreign-peers@), and so has no node of its operator: it takes of
-- the peer only what it can verify.
--
-- The wallet of the node pays an output: a transaction that spends coins
-- locked to one key each, with their signatures, which the store verifies
-- and takes. A transaction then spends that output, which is locked by a
-- script, as all the chains of these tests are: the node runs the script
-- and takes it, and the store, which runs none, does not take it on the
-- word of a stranger. So the store ends with the first and without the
-- second, and says in its health that it holds one transaction.
--
-- The same store with the peer as a node of its operator takes both, as
-- every other test of this module has it.
strangers :: Bed -> IO ()
strangers bed = withRamDir $ \dir -> do
  let foreign' c = c {foreignPeers = True} :: StoreConfig
      address = case scriptToAddressBS bed.net lock >>= maybe (Left "no text") Right . addrToText bed.net of
        Right a -> a
        Left e -> error ("No address for the script: " <> e)
  (verified, scripted) <- withMemberOf foreign' bed dir $ \_ server -> do
    converges bed server 60
    -- from the wallet of the node, signed by its keys
    paid <- sendTo bed.node address 1000000
    paying <- getRawTx bed.node paid
    let index = case [n | o <- maybe [] arr (field "vout" paying), isLock o, Just (Number n) <- [field "n" o]] of
          [n] -> round n :: Word32
          _ -> error "The wallet did not pay the script once"
    hash <- maybe (fail "Not a transaction id") return (hexToTxHash paid)
    -- and what spends it, by a script
    let spending = spend (OutPoint hash index) [TxOut 990000 lock]
    send bed spending
    let spent = txHashToHex (txHash spending)
    -- the store has the first within seconds, and never the second
    has server paid 30
    threadDelay 5000000
    pool server `shouldReturn` Set.singleton paid
    Set.fromList <$> rawMempool bed.node `shouldReturn` Set.fromList [paid, spent]
    (_, health) <- getJSON server "/health"
    (field "mempool" health >>= field "held") `shouldBe` Just (Number 1)
    return (paid, spent)
  -- the same store, told nothing of the kind: the peer is a node of its
  -- operator, and what it took is taken
  withMemberIn bed dir $ \_ server -> do
    has server scripted 60
    pool server `shouldReturn` Set.fromList [verified, scripted]
  where
    isLock o = (field "scriptPubKey" o >>= textField "hex") == Just (encodeHex lock)
    pool server = do
      (_, m) <- getJSON server "/mempool?limit=1000000"
      return (Set.fromList [t | String t <- case m of v@(Array _) -> arr v; _ -> []])
    has server h seconds = do
      got <- pool server
      if Set.member h got
        then return ()
        else
          if seconds <= (0 :: Int)
            then expectationFailure ("The store does not have " <> T.unpack h)
            else threadDelay 1000000 >> has server h (seconds - 1)

-- | A store whose Valkey is emptied while it runs, as by @FLUSHALL@. Unlike
-- 'valkeyRestart', Valkey goes on answering and the store keeps its
-- connections: nothing fails, and only the epoch tells the store that the
-- pool is gone ('Haskoin.Store.Fleet.Member.noteEpoch'). The store must
-- say so, once, and have the mempool of its node again within fifteen
-- seconds. Then the node takes as much again, which the store must take
-- into the new pool.
--
-- A store reads the mempool of its node by the clock too, thirty seconds
-- after a reading in which the node named something new. That reading
-- brought the pool back, 26 seconds after it was emptied, in a store that
-- had been made unable to notice the loss, and so within any limit that
-- is safe for a slow machine. So the test waits for that reading before it
-- empties the pool: the next one is then thirty seconds away or more, the
-- node taking nothing meanwhile, and what brings the pool back in fifteen
-- is the store noticing.
valkeyFlush :: Bed -> IO ()
valkeyFlush bed = do
  roots <- fund bed 40
  let (before, after) = splitAt 20 roots
  withMember bed $ \_ server -> do
    submit bed (map (chainOf 24) before)
    converges bed server 60
    byTheClock 280
    flush bed
    converges bed server 15
    lost <- said "Redis lost its data"
    lost `shouldBe` 1
    submit bed (map (chainOf 24) after)
    converges bed server 60
  where
    -- how many lines of the log of the store, above the level of
    -- debugging, have these words
    said :: String -> IO Int
    said what = length . filter (what `isInfixOf`) . takeWhile (/= "--- at every level") <$> bed.logTail
    -- wait for the second reading of the mempool of the node, the first
    -- being that of when the node connected, for so many quarters of a
    -- second
    byTheClock :: Int -> IO ()
    byTheClock n = do
      readings <- said "for its mempool"
      unless (readings >= 2) $
        if n <= 0
          then expectationFailure "The store did not read the mempool of its node a second time in 70 seconds"
          else threadDelay 250000 >> byTheClock (n - 1)

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

-- | A block that confirms thousands of transactions of the pool comes
-- while the node goes on taking transactions without a pause, about sixty
-- a second, as the node of a busy network always does. The store has to
-- answer from the block within seconds, while they keep coming.
--
-- The other situations here let the node go quiet before they look, and so
-- passed on a store that could not move its tip while anything was
-- offered: on a server, such a store imported every block and answered
-- from an old one for as long as it ran, with its health saying that all
-- was well. Taking the block's transactions out of the pool is one
-- decision, carried out only if nothing that it read has changed, and
-- every transaction offered meanwhile changed something
-- ('Haskoin.Store.Fleet.Pool.Decide.floorHeld').
unceasing :: Bed -> IO ()
unceasing bed = do
  -- fewer in the first load than Bitcoin Core remembers having named to
  -- a peer, 3,500: asked for one that it named before those, it says
  -- that it has it not
  roots <- fund bed 160
  let (first, rest) = splitAt 100 roots
  withMember bed $ \_ server -> do
    submit bed (map (chainOf 24) first)
    converges bed server 180
    -- four chains at a time, the transactions of each in their order, with
    -- a pause after each: about sixty a second between them
    let slowly tx = send bed tx >> threadDelay 60000
        trickle = forConcurrently_ (lanes 4 (map (chainOf 24) rest)) (mapM_ slowly . concat)
    withAsync trickle $ \sending -> do
      link sending
      forM_ [1 :: Int, 2] $ \_ -> do
        threadDelay 3000000
        void (generateTo bed.node 1 bed.miner)
        best <- bestBlockHash bed.node
        follows server best 20
    converges bed server 180
  where
    lanes n xs = [[x | (i, x) <- zip [0 :: Int ..] xs, i `mod` n == l] | l <- [0 .. n - 1]]
    -- within so many seconds, the store answers from a block
    follows server best seconds = do
      start <- getPOSIXTime
      let go = do
            (_, b) <- getJSON server "/block/best?notx=true"
            now <- getPOSIXTime
            if textField "hash" b == Just best
              then return ()
              else
                if now - start > fromIntegral (seconds :: Int)
                  then
                    expectationFailure . unwords $
                      [ "After",
                        show seconds,
                        "seconds, with transactions still coming, the store answers from",
                        maybe "no block" T.unpack (textField "hash" b),
                        "and not from the best block of its node,",
                        T.unpack best
                      ]
                  else threadDelay 250000 >> go
      go

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
                (_, health) <- getJSON server "/health"
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
                    show same <> ".",
                    "The store says of its path into the pool:",
                    maybe "nothing" T.unpack (field "mempool" health >>= textField "text")
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
    field name = \case
      Object o -> KeyMap.lookup name o
      _ -> Nothing

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
  { -- | the pod, to stop and start its containers
    pod :: !Pod,
    node :: !Node,
    net :: !Network,
    -- | address of the wallet of the node, to mine to
    miner :: !Text,
    redisURL :: !String,
    granted :: !Permissions,
    -- | whether the stores of the bed are given the RPC of the node
    viaRpc :: !Bool,
    -- | and hears what the node publishes over ZeroMQ
    viaZmq :: !Bool,
    logger :: !Logger,
    -- | the last lines of the log of the stores of the bed
    logTail :: !(IO [String])
  }

withBed :: NodeKind -> (Bed -> IO ()) -> IO ()
withBed kind = withBedOf (nodeContainer kind) kind

-- | The container of a node that whitelists nobody: to its peers it is
-- what a node of somebody else is.
bare :: NodeKind -> ContainerSpec
bare kind = c {name = c.name <> "-bare", command = filter (/= "-whitelist=0.0.0.0/0") c.command}
  where
    c = nodeContainer kind

-- | 'withBed' with the container of the node given, for a node that is
-- started with other options.
withBedOf :: ContainerSpec -> NodeKind -> (Bed -> IO ()) -> IO ()
withBedOf nodeSpec kind go =
  withPod (pod "mempool" [redisContainer, nodeSpec]) {readyWithin = 120} $ \p -> do
    node <- nodeOf kind p
    waitRpc node
    ensureWallet node
    miner <- getNewAddress node
    void $ generateTo node 150 miner
    withTestLog $ \logger logTail ->
      go
        Bed
          { pod = p,
            node,
            net = nodeNetwork kind,
            miner,
            redisURL = Valkey.redisURL p,
            granted = grantedBy True kind,
            viaRpc = False,
            viaZmq = False,
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
withMemberIn = withMemberOf id

-- | 'withMemberIn', with the configuration of the store changed.
withMemberOf :: (StoreConfig -> StoreConfig) -> Bed -> FilePath -> (Store -> Server -> IO a) -> IO a
withMemberOf change bed dir f =
  withStore (change (heard (storeConfig bed.net bed.logger dir bed.redisURL bed.node.peer bed.granted))) $ \store ->
    listening $ \(sock, server) ->
      withAsync (runWebSocket (web store) sock) $ \running ->
        link running >> f store server
  where
    -- with the node as a peer through its RPC and its ZeroMQ socket
    heard c
      | bed.viaZmq =
          c
            { nodePeerRpc = Just bed.node.url,
              nodePeerRpcSocket = Just bed.node.rpcSocket,
              nodePeerZmq = Just bed.node.zmq
            }
      | otherwise = c
    web store
      | bed.viaRpc =
          (webConfig bed.logger store)
            { nodeRpc = Just bed.node.url,
              nodeRpcSocket = Just bed.node.rpcSocket,
              nodeZmq = if bed.viaZmq then Just bed.node.zmq else Nothing
            } ::
            WebConfig
      | otherwise = webConfig bed.logger store

-- | 'withMemberIn' a new folder.
withMember :: Bed -> (Store -> Server -> IO a) -> IO a
withMember bed f = withRamDir $ \dir -> withMemberIn bed dir f
