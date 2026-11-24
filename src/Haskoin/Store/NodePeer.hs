-- |
-- Module      : Haskoin.Store.NodePeer
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- A full node as a peer of the store, reached through its RPC and its
-- ZeroMQ socket and not through the peer-to-peer protocol (@--node-rpc@,
-- with @--node-zmq@).
--
-- = The problem
--
-- A store takes its blocks and its transactions from peers, over the
-- peer-to-peer protocol. That protocol is made for strangers: a node
-- tells a peer what it chooses to, at the pace that it chooses, and
-- closes the connection of a peer that asks for more than it grants. An
-- operator who runs the node has two better ways to it. Its RPC answers
-- anything, at once, to whoever has its password. Its ZeroMQ socket
-- publishes every transaction and every block as the node takes them.
-- Together they give all that a peer gives.
--
-- = What this is
--
-- A peer that is no connection to the network: the store is handed the two
-- ends of a pipe in memory, and at the other end a thread answers the
-- messages of the peer-to-peer protocol from the RPC of the node, and
-- announces what the node publishes.
--
-- > the store asks, as it asks any peer      answered from
-- > version                                  getblockcount
-- > getheaders                               getblockheader, getblockhash
-- > getdata of a block                       getblock
-- > getdata of a transaction                 getrawtransaction
-- > mempool                                  getrawmempool
-- > a transaction to relay                   sendrawtransaction
-- > (a new block, a new transaction)         ZeroMQ: hashblock, hashtx
--
-- So nothing else of the store knows the difference. The node counts as
-- a peer in the health of the store, it is a node of the operator, whose
-- word is taken ("Haskoin.Store.Ingest"), and it is a source: it says
-- that it serves Bloom filters, which is what tells the store that a peer
-- gives its mempool. A store can run on it alone, with no peer-to-peer
-- connection at all.
--
-- = Why a peer, and not another way to get blocks
--
-- The store could have learned to take headers and blocks from an RPC,
-- beside the way that it takes them from peers. That is a second path for
-- blocks, with its own windows, its own stalls and its own order, to get
-- right and to keep right. A peer costs a translation of some ten
-- messages, and everything that the store does with a peer, and every test
-- of it, applies as it is.
--
-- = When it is there
--
-- Both connections are the peer. If the RPC fails, or the ZeroMQ socket
-- closes, the peer is gone, and the store sees a peer that disconnected:
-- it connects again, as to any peer. A store whose node is down is a
-- store with no peer, and says so.
--
-- A ZeroMQ socket can also go quiet without closing, and a node drops what
-- a slow subscriber does not read. So the thread asks the node for its
-- best block every thirty seconds, and announces it if nothing did. When
-- messages may have been lost, it names the whole mempool of the node, as
-- an answer to @mempool@ does.
--
-- = Without the ZeroMQ socket
--
-- A node is meant to be given with both, and a store that has only the
-- RPC says so, in its log and in its health. It is a peer all the same,
-- and since the RPC announces nothing, the thread asks instead, every
-- five seconds ('pollEvery'):
--
-- * for the best block of the node, which it announces if it is new;
-- * for the ids of the mempool of the node, of which it names those that
--   were not there the last time, as a node names what it just took.
--
-- It asks for the mempool only while no other node of the operator is a
-- peer of the store ('alone'). Such a peer announces what the node takes,
-- and reading the mempool beside it would say the same twice. While every
-- other peer is a stranger, this reading is the only word of a node of
-- the operator that the store gets, and a transaction that only strangers
-- named waits for it ("Haskoin.Store.Ingest"): so it is asked for often.
--
-- A store without the socket is up to five seconds behind its node, and
-- asks it for the ids of its whole mempool each time: some four megabytes
-- of JSON for a mempool of sixty thousand transactions. That is the price
-- of not giving the socket.
--
-- = What it does not do
--
-- It serves no addresses of other nodes, no filters and no compact
-- blocks: the store asks a peer for none of them. Blocks come one RPC
-- call each, as hexadecimal in JSON, which is slower than the
-- peer-to-peer protocol for the first sync of a chain.
module Haskoin.Store.NodePeer
  ( NodePeer (..),
    nodePeerName,
    nodePeerAddress,
    nodePeerConnect,
    lacksZmq,
    pollEvery,
    fresh,
  )
where

import Conduit (awaitForever, liftIO, yield)
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (race_)
import Control.Concurrent.STM
  ( TBQueue,
    atomically,
    newTBQueueIO,
    newTVarIO,
    readTBQueue,
    readTVarIO,
    writeTBQueue,
    writeTVar,
  )
import Control.Exception (finally, throwIO)
import Control.Monad (forM, forM_, forever, unless, void, when)
import Data.Aeson qualified as A
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Bits ((.|.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Foldable (toList)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Maybe (catMaybes, isNothing, listToMaybe)
import Data.Serialize (decode, encode, putByteString, putWord32be, putWord32le, runGet, runPut)
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Haskoin
  ( BlockHash (..),
    BlockHeader,
    GetData (..),
    GetHeaders (..),
    Headers (..),
    Inv (..),
    InvType (..),
    InvVector (..),
    Message (..),
    Network (..),
    NetworkAddress (..),
    NotFound (..),
    Ping (..),
    Pong (..),
    TxHash (..),
    VarInt (..),
    VarString (..),
    Version (..),
    blockHashToHex,
    decodeHex,
    doubleSHA256,
    encodeHex,
    getMessage,
    hexToBlockHash,
    hexToTxHash,
    nodeBloom,
    nodeNetwork,
    nodeWitness,
    putMessage,
    sockToHostAddress,
    txHashToHex,
  )
import Haskoin.Node (Conduits (..), LogLevel (..), Logger, WithConnection, logAt)
import Haskoin.Store.NodeRpc (RpcFailure (..), nodeAnswers)
import Haskoin.Store.NodeZmq (Heard (..), ZmqAddress, hearNode)
import Network.Socket (SockAddr (..))
import Network.Wreq.Session (Session)
import System.Random (randomIO)

-- | What the peer needs: the network, the RPC of the node and its
-- ZeroMQ socket, if it was given.
data NodePeer = NodePeer
  { net :: !Network,
    -- | the HTTP client of the RPC ('Haskoin.Store.NodeRpc.nodeSession')
    session :: !Session,
    -- | the URL of the RPC, with its user and password
    url :: !String,
    -- | where the node publishes, or 'Nothing' for a node whose RPC is
    -- all that the store has: it is then asked every 'pollEvery'
    zmq :: !(Maybe ZmqAddress),
    -- | whether no other node of the operator is a peer of the store now:
    -- the mempool of a node without a ZeroMQ socket is read only then
    alone :: !(IO Bool),
    logger :: !Logger
  }

-- | The name of the peer among the peers of a store, as @--peer@ would
-- have it: a Unix socket whose path no socket has.
nodePeerName :: String
nodePeerName = "unix:@node-rpc"

-- | The address that 'nodePeerName' is: a store that is asked to connect
-- to it is given 'nodePeerConnect' in place of a connection.
nodePeerAddress :: SockAddr
nodePeerAddress = SockAddrUnix "@node-rpc"

-- | What happens at the other end of the pipe.
data Event
  = -- | bytes of the store, or the end of them
    Bytes !(Maybe ByteString)
  | Said !Heard
  | -- | time to ask the node for its best block and, without a ZeroMQ
    -- socket, for its mempool
    Poll

-- | How often a node without a ZeroMQ socket is asked for its best block
-- and for its mempool, in microseconds: five seconds. It is how far such
-- a store is behind its node.
pollEvery :: Int
pollEvery = 5000000

-- | Of the ids of a mempool, those that were not there the last time, in
-- the order that the node gave them: what a node that announces would
-- have named since.
fresh :: (Ord h) => Set h -> [h] -> [h]
fresh before = filter (`Set.notMember` before)

-- | A connection to the peer: a pipe in memory, served until the store
-- closes it or the node fails.
nodePeerConnect :: NodePeer -> WithConnection
nodePeerConnect cfg act = do
  toStore <- newTBQueueIO 256
  events <- newTBQueueIO 1024
  let conduits =
        Conduits
          { inboundConduit = source toStore,
            outboundConduit =
              awaitForever (liftIO . atomically . writeTBQueue events . Bytes . Just)
                >> liftIO (atomically (writeTBQueue events (Bytes Nothing)))
          }
      source q = liftIO (atomically (readTBQueue q)) >>= maybe (return ()) (\b -> yield b >> source q)
      -- whatever ends the serving, the store reads the end of the pipe
      served = serve cfg toStore events `finally` atomically (writeTBQueue toStore Nothing)
      -- without a socket there is nothing to hear, and no end of it
      hearing = case cfg.zmq of
        Just zmq -> hearNode zmq (atomically . writeTBQueue events . Said)
        Nothing -> forever (threadDelay maxBound)
      wait = if isNothing cfg.zmq then pollEvery else 30000000
      polling = forever (threadDelay wait >> atomically (writeTBQueue events Poll))
  race_ (act conduits) (race_ served (race_ hearing polling))

-- | Answer the store until it closes the pipe. Any failure of the RPC ends
-- it, and with it the peer.
serve :: NodePeer -> TBQueue (Maybe ByteString) -> TBQueue Event -> IO ()
serve cfg toStore events = do
  buffer <- newIORef B.empty
  announced <- newTVarIO Nothing
  -- the mempool of the node as it was last read
  named <- newIORef Set.empty
  let send m = atomically (writeTBQueue toStore (Just (runPut (putMessage cfg.net m))))
      -- a message whose payload is here already, as the node gave it
      sendRaw name payload = atomically (writeTBQueue toStore (Just (framed cfg.net name payload)))
      announce b = do
        before <- readTVarIO announced
        unless (before == Just b) $ do
          atomically (writeTVar announced (Just b))
          send (MInv (Inv [InvVector InvBlock b.get]))
      loop = do
        event <- atomically (readTBQueue events)
        case event of
          Bytes Nothing -> return ()
          Bytes (Just bytes) -> do
            before <- readIORef buffer
            let (messages, rest) = split cfg.net (before <> bytes)
            writeIORef buffer rest
            mapM_ (answer cfg send sendRaw whole) messages
            loop
          Said (HeardTxs hs) -> send (MInv (Inv [InvVector InvTx h.get | h <- hs])) >> loop
          Said (HeardBlock b) -> announce b >> loop
          -- something was not heard: all that the node has is named
          Said Missed -> whole >> best >>= announce >> loop
          Poll -> do
            best >>= announce
            quiet <- if isNothing cfg.zmq then cfg.alone else return False
            when quiet news
            loop
      -- every transaction of the mempool of the node
      whole = do
        hs <- mempool cfg
        writeIORef named (Set.fromList hs)
        naming hs
      -- those that the node took since its mempool was last read
      news = do
        hs <- mempool cfg
        before <- readIORef named
        writeIORef named (Set.fromList hs)
        naming (fresh before hs)
      -- as many to a message as a node names
      naming hs = forM_ (pieces hs) $ \some -> send (MInv (Inv [InvVector InvTx h.get | h <- some]))
      pieces hs
        | null hs = []
        | otherwise = take 50000 hs : pieces (drop 50000 hs)
      best =
        call cfg "getbestblockhash" [] >>= \case
          A.String t | Just b <- hexToBlockHash t -> return b
          other -> unexpected "getbestblockhash" other
  loop

-- | What the node answers to a message of the store.
answer :: NodePeer -> (Message -> IO ()) -> (ByteString -> ByteString -> IO ()) -> IO () -> Message -> IO ()
answer cfg send sendRaw whole = \case
  MVersion _ -> do
    height <-
      call cfg "getblockcount" [] >>= \case
        A.Number n -> return (round n)
        other -> unexpected "getblockcount" other
    now <- round <$> getPOSIXTime
    nonce <- randomIO
    -- a node that has the whole chain, with witnesses where there are
    -- any, and that gives its mempool when asked: what a node says when it
    -- serves Bloom filters
    let services = nodeNetwork .|. nodeBloom .|. (if cfg.net.segWit then nodeWitness else 0)
        here = NetworkAddress services (sockToHostAddress (SockAddrInet 0 0))
    send . MVersion $
      Version
        { version = 70016,
          services,
          timestamp = now,
          addrRecv = here,
          addrSend = here,
          nonce,
          userAgent = VarString peerLabel,
          startHeight = height,
          relay = True
        }
    send MVerAck
    case cfg.zmq of
      Just _ -> logAt LevelInfo cfg.logger "The node is a peer of the store through its RPC and its ZeroMQ socket"
      Nothing -> logAt LevelWarn cfg.logger lacksZmq
  -- A ping is answered once the node has answered a call: the store
  -- tells a peer that is there from one that is not by its pongs, and
  -- this thread would answer for a node that is gone.
  MPing (Ping n) -> call cfg "getblockcount" [] >> send (MPong (Pong n))
  MGetHeaders gh -> headersAfter cfg gh.locator >>= \hs -> send (MHeaders (Headers [(h, VarInt 0) | h <- hs]))
  MGetData (GetData ivs) -> do
    forM_ [BlockHash h | InvVector t h <- ivs, t `elem` [InvBlock, InvWitnessBlock]] $ \b ->
      call' cfg "getblock" [A.String (blockHashToHex b), A.Number 0] >>= \case
        Right (A.String hex) | Just bytes <- decodeHex hex -> sendRaw "block" bytes
        Left _ -> send (MNotFound (NotFound [InvVector InvBlock b.get]))
        Right other -> unexpected "getblock" other
    let wanted = [TxHash h | InvVector t h <- ivs, t `elem` [InvTx, InvWitnessTx]]
    answers <- nodeAnswers cfg.session cfg.url [("getrawtransaction", [A.String (txHashToHex h)]) | h <- wanted]
    lacking <- forM (zip wanted answers) $ \case
      (_, Right (A.String hex)) | Just bytes <- decodeHex hex -> sendRaw "tx" bytes >> return Nothing
      -- the node says that it has it not
      (h, Left _) -> return (Just h)
      (_, Right other) -> unexpected "getrawtransaction" other
    unless (null (catMaybes lacking)) $
      send (MNotFound (NotFound [InvVector InvTx h.get | h <- catMaybes lacking]))
  MMempool -> whole
  -- a transaction that the store relays for a client: the node judges it
  MTx tx -> void (call' cfg "sendrawtransaction" [A.String (encodeHex (encode tx))])
  -- the rest asks for nothing that the store waits for
  _ -> return ()

-- | What the peer calls itself in its version message, which never
-- leaves the store.
peerLabel :: ByteString
peerLabel = "/node-rpc/"

-- | What a store says, in its log and in its health, when it has the RPC
-- of its node and not its ZeroMQ socket.
lacksZmq :: Text
lacksZmq =
  "The store has the RPC of its node and not its ZeroMQ socket (--node-zmq): \
  \it asks the node every five seconds for what the socket would announce at once"

-- | The ids of the mempool of the node.
mempool :: NodePeer -> IO [TxHash]
mempool cfg =
  call cfg "getrawmempool" [] >>= \case
    A.Array a | Just hs <- mapM hash (toList a) -> return hs
    other -> unexpected "getrawmempool" other
  where
    hash = \case
      A.String t -> hexToTxHash t
      _ -> Nothing

-- | The headers of the chain of the node after the first block of a
-- locator that is in it, two thousand at most, as a node answers
-- @getheaders@. A locator ends with the first block of the chain, which
-- every node has.
headersAfter :: NodePeer -> [BlockHash] -> IO [BlockHeader]
headersAfter cfg locator = do
  known <- many [("getblockheader", [A.String (blockHashToHex b), A.Bool True]) | b <- locator]
  -- a block that is not in the best chain has no confirmations, or less
  -- than none
  let heights =
        [ round h :: Int
        | Right (A.Object o) <- known,
          Just (A.Number c) <- [KeyMap.lookup "confirmations" o],
          c > 0,
          Just (A.Number h) <- [KeyMap.lookup "height" o]
        ]
  top <-
    call cfg "getblockcount" [] >>= \case
      A.Number n -> return (round n :: Int)
      other -> unexpected "getblockcount" other
  case listToMaybe heights of
    Nothing -> return []
    Just from -> do
      hashes <- many [("getblockhash", [A.Number (fromIntegral h)]) | h <- [from + 1 .. min top (from + 2000)]]
      found <- many [("getblockheader", [h, A.Bool False]) | Right h <- hashes]
      -- the chain of the node may change between the calls: the headers
      -- that are there, up to the first that is not
      return (catMaybes (takeWhile (/= Nothing) (map header found)))
  where
    many = nodeAnswers cfg.session cfg.url
    header = \case
      Right (A.String hex) | Just bytes <- decodeHex hex, Right h <- decode bytes -> Just h
      _ -> Nothing

-- | One call of the RPC, and its result: an error of the node is a
-- failure.
call :: NodePeer -> Text -> [A.Value] -> IO A.Value
call cfg method params = call' cfg method params >>= either (unexpected method) return

-- | One call of the RPC: the error of the node, or its result.
call' :: NodePeer -> Text -> [A.Value] -> IO (Either A.Value A.Value)
call' cfg method params =
  nodeAnswers cfg.session cfg.url [(method, params)] >>= \case
    [r] -> return r
    _ -> throwIO (RpcFailure ("No answer to " <> show method))

unexpected :: (Show v) => Text -> v -> IO a
unexpected method v = throwIO (RpcFailure ("Unexpected answer to " <> show method <> ": " <> take 200 (show v)))

-- | The messages at the front of some bytes, and the bytes after them. A
-- message is 24 bytes of header, with the length of the rest in the
-- seventeenth to the twentieth. One that cannot be read is left out.
split :: Network -> ByteString -> ([Message], ByteString)
split net bytes
  | B.length bytes < 24 = ([], bytes)
  | B.length bytes < 24 + size = ([], bytes)
  | otherwise =
      let (one, rest) = B.splitAt (24 + size) bytes
          (more, left) = split net rest
       in (either (const more) (: more) (runGet (getMessage net) one), left)
  where
    size = B.foldr (\w acc -> acc * 256 + fromIntegral w) 0 (B.take 4 (B.drop 16 bytes))

-- | A message of the peer-to-peer protocol with a payload that is given
-- as bytes: the magic of the network, the name in twelve bytes, the
-- length, and the first four bytes of the double hash of the payload.
framed :: Network -> ByteString -> ByteString -> ByteString
framed net name payload = runPut $ do
  putWord32be net.magic
  putByteString (name <> B.replicate (12 - B.length name) 0)
  putWord32le (fromIntegral (B.length payload))
  putByteString (B.take 4 (encode (doubleSHA256 payload)))
  putByteString payload
