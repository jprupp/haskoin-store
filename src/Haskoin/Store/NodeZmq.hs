-- |
-- Module      : Haskoin.Store.NodeZmq
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- What a full node publishes over ZeroMQ, as a subscriber hears it
-- (@--node-zmq@).
--
-- = The problem
--
-- A store hears of a transaction when its node names it to a peer, and a
-- node names transactions to a peer at its own pace: Bitcoin Core about
-- fourteen a second after a first batch, to a peer that it whitelists too.
-- A store that has the RPC of its node reads the whole mempool every
-- thirty seconds while transactions come, and is up to that far behind in
-- a burst. The RPC itself announces nothing.
--
-- Both Bitcoin Core and Bitcoin Cash Node can publish what they take as
-- they take it (@-zmqpubhashtx@, @-zmqpubhashblock@): the id of every
-- transaction that enters the mempool, at once, and the hash of every
-- block that becomes the tip. With that and the RPC, a store hears of a
-- transaction and fetches it within the same second.
--
-- = What a node publishes
--
-- Each message has three parts: a topic, a body and a counter of the
-- messages of that topic, four bytes, least significant first.
--
-- > topic      body                              on
-- > hashtx     the id of a transaction           Bitcoin Core, Bitcoin Cash Node
-- > hashblock  the hash of a block               Bitcoin Core, Bitcoin Cash Node
-- > rawtx      a transaction                     both
-- > rawblock   a block                           both
-- > sequence   every change of mempool and tip   Bitcoin Core only
-- > hashds     a double-spend proof              Bitcoin Cash Node only
--
-- The ids and hashes are in the order in which people write them, the
-- reverse of their bytes in a transaction. A transaction is published
-- when it enters the mempool, and again when a block that has it becomes
-- the tip.
--
-- = What it does not give
--
-- The socket only publishes. Nothing can be asked of it, a subscriber
-- that connects late hears nothing of what came before, and a node drops
-- what a slow subscriber does not read, a thousand messages behind by
-- default. The counter says that messages were lost, and not which. So
-- this is never the only way that a store hears of transactions: when a
-- message was lost, or the connection made again, the store reads the
-- mempool of its node ('Heard', 'Missed').
--
-- Nothing is authenticated: whoever reaches the socket hears it, and
-- whoever answers on it is believed. It is for a node of the operator,
-- on the same machine or a network that the operator trusts.
--
-- = Why no library
--
-- ZeroMQ has a library in C, and a Haskell package that binds it. A
-- subscriber needs little of it: a greeting, one command, a subscription
-- for each topic, and frames to read (ZMTP 3.0,
-- <https://rfc.zeromq.org/spec/23/>). That is written here, on the
-- sockets that the store already uses, so that the store gains no library
-- of the system, and reaches a node through a Unix socket as it does
-- everywhere else.
module Haskoin.Store.NodeZmq
  ( -- * Where a node publishes
    ZmqAddress (..),
    parseZmqAddress,

    -- * Subscribing
    ZmqMessage (..),
    zmqSubscribe,

    -- * What the store hears
    Heard (..),
    hearNode,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (race_)
import Control.Concurrent.STM (atomically, flushTQueue, newTQueueIO, readTQueue, writeTQueue)
import Control.Exception (Exception, bracket, throwIO)
import Control.Monad (forM_, forever, unless, when)
import Data.Bits (shiftL, shiftR, testBit, (.&.), (.|.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Map.Strict qualified as Map
import Data.Serialize (decode)
import Data.Word (Word32, Word64)
import Haskoin (BlockHash, TxHash)
import Network.Socket
  ( AddrInfo (..),
    Family (AF_UNIX),
    SockAddr (SockAddrUnix),
    Socket,
    SocketType (Stream),
    close,
    connect,
    defaultHints,
    defaultProtocol,
    getAddrInfo,
    socket,
  )
import Network.Socket.ByteString (recv, sendAll)
import Text.Read (readMaybe)

-- | Where a node publishes: the address given to its @-zmqpub…@ options.
data ZmqAddress
  = -- | a host and a port (@tcp:\/\/127.0.0.1:28332@)
    ZmqTcp !String !Int
  | -- | a Unix socket (@ipc:\/\/\/run\/bitcoind\/zmq.sock@, or
    -- @unix:\/run\/bitcoind\/zmq.sock@ as Bitcoin Core also writes it)
    ZmqUnix !FilePath
  deriving (Eq, Show)

-- | Read an address as the nodes write it.
parseZmqAddress :: String -> Either String ZmqAddress
parseZmqAddress s
  | Just rest <- after "tcp://" = tcp rest
  | Just path <- after "ipc://", not (null path) = Right (ZmqUnix path)
  | Just path <- after "unix:", not (null path) = Right (ZmqUnix path)
  | otherwise = Left ("Not an address of ZeroMQ, such as tcp://127.0.0.1:28332 or ipc:///run/node/zmq.sock: " <> s)
  where
    after prefix
      | take (length prefix) s == prefix = Just (drop (length prefix) s)
      | otherwise = Nothing
    -- the port is after the last colon: an IPv6 host has colons, and is
    -- written in brackets
    tcp rest = case break (== ':') (reverse rest) of
      (port, ':' : host)
        | Just n <- readMaybe (reverse port), n > 0, n < (65536 :: Int), not (null host) ->
            Right (ZmqTcp (unbracket (reverse host)) n)
      _ -> Left ("No host and port in the address of ZeroMQ: " <> s)
    unbracket h = case h of
      '[' : more | not (null more), last more == ']' -> init more
      _ -> h

-- | A message of a node: its topic, its body, and how many messages of
-- that topic the node published before it.
data ZmqMessage = ZmqMessage
  { topic :: !ByteString,
    body :: !ByteString,
    number :: !Word32
  }
  deriving (Eq, Show)

-- | What went wrong with a connection to the socket of a node.
data ZmqError
  = -- | the other end closed the connection
    ZmqClosed
  | -- | what answered does not speak ZMTP 3 without encryption, or sent
    -- something that a publisher does not send
    ZmqProtocol !String
  deriving (Show)

instance Exception ZmqError

-- | Subscribe to topics of a node and give each message to an action,
-- after an action that is run once the subscription is made. It does not
-- return: it ends with an exception when the connection does.
--
-- What is spoken is ZMTP 3.0 with no security mechanism (@NULL@), which is
-- what a node speaks: a greeting of 64 bytes each way, the command
-- @READY@ each way, a subscription for each topic, and then frames. A
-- subscription is a prefix: a topic is given whole.
zmqSubscribe :: ZmqAddress -> [ByteString] -> IO () -> (ZmqMessage -> IO ()) -> IO a
zmqSubscribe address topics ready deliver = bracket (dial address) close $ \sock -> do
  sendAll sock greeting
  theirs <- exactly sock 64
  unless (B.take 1 theirs == "\xff" && B.index theirs 9 == 0x7f) $
    throwIO (ZmqProtocol "the greeting is not one of ZMTP")
  when (B.index theirs 10 < 3) $
    throwIO (ZmqProtocol ("ZMTP " <> show (B.index theirs 10) <> ", older than 3"))
  unless (B.takeWhile (/= 0) (B.take 20 (B.drop 12 theirs)) == "NULL") $
    throwIO (ZmqProtocol "the socket asks for a security mechanism")
  sendAll sock (frame True False (command "READY" [("Socket-Type", "SUB")]))
  answer <- readFrame sock
  case answer of
    (True, _, name) | B.take 6 name == "\x05READY" -> return ()
    (True, _, name) -> throwIO (ZmqProtocol ("the socket answered " <> C.unpack (B.take 40 name)))
    _ -> throwIO (ZmqProtocol "no READY from the socket")
  -- in ZMTP 3.0 a subscription is a message that starts with a byte 1
  forM_ topics $ \t -> sendAll sock (frame False False (B.cons 1 t))
  ready
  forever $ do
    parts <- readMessage sock
    case parts of
      [t, b, n] | B.length n == 4 -> deliver (ZmqMessage t b (littleEndian n))
      -- a publisher of another kind: no counter
      [t, b] -> deliver (ZmqMessage t b 0)
      _ -> return ()
  where
    -- the signature, version 3.0, the mechanism in twenty bytes, not a
    -- server, and thirty-one bytes of nothing
    greeting =
      B.concat
        [ "\xff\0\0\0\0\0\0\0\x01\x7f",
          "\x03\x00",
          "NULL" <> B.replicate 16 0,
          "\0",
          B.replicate 31 0
        ]
    littleEndian = B.foldr (\w acc -> acc `shiftL` 8 .|. fromIntegral w) 0

-- | A command: its name with its length before it, then properties, each
-- a name with its length in one byte and a value with its length in four.
command :: ByteString -> [(ByteString, ByteString)] -> ByteString
command name properties =
  B.concat (B.singleton (fromIntegral (B.length name)) : name : concatMap property properties)
  where
    property (k, v) = [B.singleton (fromIntegral (B.length k)), k, word32 (B.length v), v]
    word32 n = B.pack [fromIntegral (n `shiftR` s) | s <- [24, 16, 8, 0]]

-- | A frame: whether it is a command, whether more frames of the message
-- follow, and its body. The flags are one byte: 1 for more, 2 for a long
-- size, 4 for a command; the size is one byte, or eight when long.
frame :: Bool -> Bool -> ByteString -> ByteString
frame isCommand more payload
  | n < 256 = B.concat [B.singleton flags, B.singleton (fromIntegral n), payload]
  | otherwise = B.concat [B.singleton (flags .|. 2), word64 (fromIntegral n), payload]
  where
    n = B.length payload
    flags = (if isCommand then 4 else 0) .|. (if more then 1 else 0)
    word64 :: Word64 -> ByteString
    word64 w = B.pack [fromIntegral (w `shiftR` s) | s <- [56, 48, 40, 32, 24, 16, 8, 0]]

-- | The next frame: whether it is a command, whether more follow, and its
-- body.
readFrame :: Socket -> IO (Bool, Bool, ByteString)
readFrame sock = do
  flags <- B.head <$> exactly sock 1
  size <-
    if testBit flags 1
      then B.foldl' (\acc w -> acc `shiftL` 8 .|. fromIntegral w) (0 :: Word64) <$> exactly sock 8
      else fromIntegral . B.head <$> exactly sock 1
  when (size > maxFrame) $ throwIO (ZmqProtocol ("a frame of " <> show size <> " bytes"))
  payload <- exactly sock (fromIntegral size)
  return (testBit flags 2, flags .&. 1 == 1, payload)

-- | The largest frame taken: a block of Bitcoin, with room to spare. A
-- size past it is a connection that is not what it seems.
maxFrame :: Word64
maxFrame = 64000000

-- | The next message: its frames, with the commands between them left
-- out, such as a ping of a newer peer.
readMessage :: Socket -> IO [ByteString]
readMessage sock = go []
  where
    go parts = do
      (isCommand, more, payload) <- readFrame sock
      if isCommand
        then go parts
        else
          if more
            then go (payload : parts)
            else return (reverse (payload : parts))

-- | So many bytes of a socket, or 'ZmqClosed'.
exactly :: Socket -> Int -> IO ByteString
exactly sock = go []
  where
    go got 0 = return (B.concat (reverse got))
    go got n = do
      chunk <- recv sock (min n 65536)
      when (B.null chunk) (throwIO ZmqClosed)
      go (chunk : got) (n - B.length chunk)

-- | A connection to an address.
dial :: ZmqAddress -> IO Socket
dial = \case
  ZmqUnix path -> do
    sock <- socket AF_UNIX Stream defaultProtocol
    connect sock (SockAddrUnix path)
    return sock
  ZmqTcp host port -> do
    found <- getAddrInfo (Just defaultHints {addrSocketType = Stream}) (Just host) (Just (show port))
    case found of
      info : _ -> do
        sock <- socket info.addrFamily info.addrSocketType info.addrProtocol
        connect sock info.addrAddress
        return sock
      [] -> throwIO (ZmqProtocol ("no address for " <> host))

-- | What the store hears of its node.
data Heard
  = -- | the node took these transactions, or a block that became the tip
    -- has them
    HeardTxs ![TxHash]
  | -- | a block became the tip of the node
    HeardBlock !BlockHash
  | -- | something may not have been heard: the subscription was just
    -- made, or the counter of a topic skipped. Only a reading of the
    -- mempool of the node brings back what was lost.
    Missed
  deriving (Eq, Show)

-- | Subscribe to the transactions and blocks of a node (@hashtx@,
-- @hashblock@) and say what is heard. It does not return, and ends with an
-- exception when the connection does: the caller makes it again.
--
-- The ids of transactions are given together when they come together, at
-- most a thousand at once: a block that becomes the tip publishes every
-- transaction that it has, thousands in one go, and the path into the pool
-- asks the pool about a thousand ids in one question, or about one.
--
-- 'Missed' is said first, since what the node took before the
-- subscription was made was not heard, and again whenever the counter of a
-- topic is not the one after the last.
hearNode :: ZmqAddress -> (Heard -> IO ()) -> IO a
hearNode address tell = do
  queue <- newTQueueIO
  counters <- newIORef Map.empty
  let deliver m = do
        seen <- Map.lookup m.topic <$> readIORef counters
        modifyIORef' counters (Map.insert m.topic m.number)
        forM_ seen $ \before -> when (m.number /= before + 1) (atomically (writeTQueue queue Nothing))
        when (B.length m.body == 32) (atomically (writeTQueue queue (Just (m.topic, B.reverse m.body))))
      -- what waits, in the order in which it came: runs of transactions
      -- together, a block or a loss by itself
      batches = forever $ do
        first <- atomically (readTQueue queue)
        -- what comes within a moment comes with it
        threadDelay 20000
        rest <- atomically (flushTQueue queue)
        go [] (first : rest)
      go txs [] = flush txs
      go txs (item : items) = case item of
        Just ("hashtx", h)
          | Right t <- decode h ->
              if length txs >= 999 then flush (t : txs) >> go [] items else go (t : txs) items
        Just ("hashblock", h)
          | Right b <- decode h -> flush txs >> tell (HeardBlock b) >> go [] items
        Nothing -> flush txs >> tell Missed >> go [] items
        _ -> go txs items
      flush txs = unless (null txs) (tell (HeardTxs (reverse txs)))
      started = writeIORef counters Map.empty >> atomically (writeTQueue queue Nothing)
  race_ batches (zmqSubscribe address ["hashtx", "hashblock"] started deliver)
  throwIO ZmqClosed
