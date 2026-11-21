-- |
-- Module      : Haskoin.Store.NodeAxiomsSpec
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- What @docs\/mempool-ingestion.md@ assumes of a node, A1 to A5, checked
-- against Bitcoin Core and Bitcoin Cash Node in Podman.
--
-- The proofs and the model checker of the path into the pool
-- ("Haskoin.Store.Ingest") take these for granted: they are what the world
-- is assumed to do, and no proof can establish them. A test against the
-- real program can at least catch the day one of them stops being true,
-- with a new release of a node. One of them was false in the code for
-- months and nobody had written it down: "a peer announces again".
--
-- The tests speak to the node as a peer of their own, a few lines that
-- connect, shake hands and keep what the node sends, so that what is
-- checked is what the node does and not what the store makes of it.
module Haskoin.Store.NodeAxiomsSpec (spec) where

import Control.Exception (IOException, bracket, try)
import Control.Monad (forM_)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.List (elemIndex, sort)
import Data.Maybe (mapMaybe)
import Data.Serialize (runGet, runPut)
import Data.Set qualified as Set
import Data.Text qualified as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import Haskoin
  ( GetData (..),
    Inv (..),
    InvType (..),
    InvVector (..),
    Message (..),
    Network (..),
    NetworkAddress (..),
    NotFound (..),
    Ping (..),
    Pong (..),
    Tx,
    TxHash (..),
    VarString (..),
    Version (..),
    doubleSHA256,
    getMessage,
    putMessage,
    sockToHostAddress,
    txHash,
    txHashToHex,
  )
import Haskoin.Store.Ingest.Run (Feed (..))
import Haskoin.Store.Integration.Node
import Haskoin.Store.MempoolSpec (Bed (..), bare, chainOf, fund, send, submit, withBed, withBedOf)
import Haskoin.Store.NodeRpc (RpcFailure (..), nodeFeed, nodeSession)
import Network.Socket (SockAddr (..), Socket, close)
import Network.Socket.ByteString (recv, sendAll)
import System.Timeout (timeout)
import Test.Hspec
import Test.Podman (address, connectTo, podmanUsable)

spec :: Spec
spec = do
  usable <- runIO podmanUsable
  case usable of
    Left why -> describe "What is assumed of a node" $ it "is what the nodes do" (pendingWith why)
    Right () -> forM_ [BitcoinCore, BitcoinCashNode] $ \kind -> do
      let on what = describe (name kind <> " " <> what)
      on "names a transaction to a peer" $
        it "once, and never one that it took before the peer connected (A1)" (withBed kind namesOnce)
      on "asked for its mempool by a peer that it allows" $
        it "names all of it, each after those it spends from, and sends or says it has not (A2, A3, A5)" $
          withBed kind namesAll
      on "asked for its mempool by a peer that it gives no permission" $
        it "closes the connection, or answers if it serves Bloom filters (A2)" $
          withBedOf (bare kind) kind (noSource kind)
      on "asked for its mempool over its RPC" $
        it "gives all of it, the transactions asked for, and its word for one that it has not" $
          withBed kind overRpc
  where
    name BitcoinCore = "Bitcoin Core"
    name BitcoinCashNode = "Bitcoin Cash Node"

-- | A1. The node takes one transaction, a peer connects, and the node
-- takes another: the peer is told of the second once, and of the first
-- never.
namesOnce :: Bed -> IO ()
namesOnce bed = do
  roots <- fund bed 2
  case concatMap (chainOf 1) roots of
    [old, new] -> do
      send bed old
      withProbe bed $ \p -> do
        send bed new
        named <- namedIn <$> collect p 10
        filter (== txHash new) named `shouldBe` [txHash new]
        filter (== txHash old) named `shouldBe` []
    _ -> expectationFailure "Not two transactions"

-- | A2, A3 and A5. A peer that the node allows asks for its mempool: the
-- node names every transaction of it, those of a chain in their order;
-- asked for one of them, it sends it; asked for one that it never had, it
-- says so.
namesAll :: Bed -> IO ()
namesAll bed = do
  roots <- fund bed 3
  let chains = map (chainOf 5) roots
  submit bed chains
  withProbe bed $ \p -> do
    tell p MMempool
    named <- namedIn <$> collect p 6
    has <- rawMempool bed.node
    Set.fromList (map txHashToHex named) `shouldBe` Set.fromList has
    forM_ chains $ \chain -> do
      let places = mapMaybe ((`elemIndex` named) . txHash) chain
      length places `shouldBe` length chain
      places `shouldBe` sort places
    root <- case chains of
      (tx : _) : _ -> return (txHash tx)
      _ -> fail "No chain"
    let never = TxHash (doubleSHA256 ("never sent" :: ByteString))
    tell p (MGetData (GetData [InvVector InvTx root.get, InvVector InvTx never.get]))
    answers <- collect p 6
    [txHash tx | MTx tx <- answers] `shouldBe` [root]
    [TxHash h | MNotFound (NotFound ivs) <- answers, InvVector _ h <- ivs] `shouldBe` [never]

-- | A2, for a peer with no permission. Bitcoin Core closes the connection
-- of one that asks for its mempool; Bitcoin Cash Node serves Bloom
-- filters, and answers anyone.
noSource :: NodeKind -> Bed -> IO ()
noSource kind bed = do
  roots <- fund bed 1
  let txs = concatMap (chainOf 1) roots :: [Tx]
  mapM_ (send bed) txs
  withProbe bed $ \p -> do
    tell p MMempool
    case kind of
      BitcoinCore -> closes p 15 `shouldReturn` True
      BitcoinCashNode -> do
        named <- namedIn <$> collect p 8
        Set.fromList named `shouldBe` Set.fromList (map txHash txs)

-- | What the path into the pool assumes of the RPC of a node as a source
-- ('nodeFeed'), which is A2 and A3 for something that is no peer: it gives
-- the id of every transaction of the mempool; asked for transactions, it
-- gives each that it has and says of one that it never had that it has it
-- not. And a node that does not answer, or does not let the store in, is
-- an exception: it is never taken for an empty mempool, or for a
-- transaction that is gone.
overRpc :: Bed -> IO ()
overRpc bed = do
  roots <- fund bed 3
  let chains = map (chainOf 5) roots
  submit bed chains
  session <- nodeSession (Just bed.node.rpcSocket)
  let feed = nodeFeed session bed.node.url
  named <- feed.names
  has <- rawMempool bed.node
  length named `shouldBe` 15
  Set.fromList (map txHashToHex named) `shouldBe` Set.fromList has
  -- each after those it spends from, as a peer names them (A5)
  forM_ chains $ \chain -> do
    let places = mapMaybe ((`elemIndex` named) . txHash) chain
    places `shouldBe` sort places
  let mine = map txHash (concat chains)
      never = TxHash (doubleSHA256 ("never sent" :: ByteString))
      asked = take 2 mine <> [never] <> drop 13 mine
  found <- feed.bodies asked
  map (fmap txHash) found `shouldBe` map Just (take 2 mine) <> [Nothing] <> map Just (drop 13 mine)
  -- nobody listens
  deaf <- nodeSession (Just "/nowhere/rpc.sock")
  (nodeFeed deaf bed.node.url).names `shouldThrow` anyException
  (nodeFeed deaf bed.node.url).bodies asked `shouldThrow` anyException
  -- the node does not know the account
  let stranger = nodeFeed session (T.unpack (T.replace "test:test@" "test:other@" (T.pack bed.node.url)))
  bed.node.url `shouldContain` "test:test@"
  stranger.names `shouldThrow` \(RpcFailure _) -> True
  stranger.bodies asked `shouldThrow` \(RpcFailure _) -> True

-- | The transactions that messages name, in their order.
namedIn :: [Message] -> [TxHash]
namedIn msgs = [TxHash h | MInv (Inv ivs) <- msgs, InvVector t h <- ivs, t == InvTx || t == InvWitnessTx]

-------------------------------------------------------------------------------
-- A peer of the tests
-------------------------------------------------------------------------------

-- | A connection to a node, after the handshake, with the bytes read and
-- not yet made into a message.
data Probe = Probe
  { sock :: !Socket,
    net :: !Network,
    unread :: !(IORef ByteString)
  }

-- | What reading a message gave.
data Heard = Heard !Message | Quiet | Closed

-- | Connect to the node of a bed as a peer, shake hands, and run an action
-- on the connection. The peer says of itself what the network's own
-- constants say.
withProbe :: Bed -> (Probe -> IO a) -> IO a
withProbe bed f = bracket (connectTo (address bed.pod "p2p")) close $ \sock -> do
  unread <- newIORef B.empty
  let p = Probe {sock, net = bed.net, unread}
  now <- round <$> getPOSIXTime
  tell p (MVersion (version now))
  shake p False False
  f p
  where
    nobody = NetworkAddress 0 (sockToHostAddress (SockAddrInet 0 0))
    version now =
      Version
        { version = 70015,
          services = 0,
          timestamp = now,
          addrRecv = nobody,
          addrSend = nobody,
          nonce = 0x70726f6265,
          userAgent = VarString bed.net.userAgent,
          startHeight = 0,
          relay = True
        }
    shake _ True True = return ()
    shake p theirs ack =
      hear p 20000000 >>= \case
        Heard (MVersion _) -> tell p MVerAck >> shake p True ack
        Heard MVerAck -> shake p theirs True
        Heard _ -> shake p theirs ack
        _ -> fail "The node did not shake hands"

-- | Send a message.
tell :: Probe -> Message -> IO ()
tell p msg = sendAll p.sock (runPut (putMessage p.net msg))

-- | The next message of the node, within so many microseconds. A ping is
-- answered here, and is a message like another.
hear :: Probe -> Int -> IO Heard
hear p micros = do
  got <- timeout micros (try message)
  case got of
    Nothing -> return Quiet
    Just (Left (_ :: IOException)) -> return Closed
    Just (Right Nothing) -> return Closed
    Just (Right (Just msg)) -> do
      case msg of
        MPing (Ping n) -> tell p (MPong (Pong n))
        _ -> return ()
      return (Heard msg)
  where
    -- a header of 24 bytes, whose bytes 16 to 19 are the length of what
    -- follows
    message = do
      header <- exactly 24
      case header of
        Nothing -> return Nothing
        Just h -> do
          let size = sum [fromIntegral (B.index h (16 + i)) * 256 ^ i | i <- [0 .. 3 :: Int]]
          payload <- exactly size
          case payload of
            Nothing -> return Nothing
            Just body -> either fail (return . Just) (runGet (getMessage p.net) (h <> body))
    -- so many bytes, or nothing if the node closed the connection first;
    -- what was read stays for the next call if this one runs out of time
    exactly n = do
      have <- readIORef p.unread
      if B.length have >= n
        then writeIORef p.unread (B.drop n have) >> return (Just (B.take n have))
        else do
          more <- recv p.sock 65536
          if B.null more
            then return Nothing
            else writeIORef p.unread (have <> more) >> exactly n

-- | Every message of the node for so many seconds.
collect :: Probe -> Int -> IO [Message]
collect p seconds = do
  start <- getPOSIXTime
  let go heard = do
        now <- getPOSIXTime
        let left = fromIntegral seconds - realToFrac (now - start) :: Double
        if left <= 0
          then return (reverse heard)
          else
            hear p (round (left * 1000000)) >>= \case
              Heard msg -> go (msg : heard)
              _ -> return (reverse heard)
  go []

-- | Whether the node closes the connection within so many seconds.
closes :: Probe -> Int -> IO Bool
closes p seconds = do
  start <- getPOSIXTime
  let go = do
        now <- getPOSIXTime
        let left = fromIntegral seconds - realToFrac (now - start) :: Double
        if left <= 0
          then return False
          else
            hear p (round (left * 1000000)) >>= \case
              Closed -> return True
              Quiet -> return False
              Heard _ -> go
  go
