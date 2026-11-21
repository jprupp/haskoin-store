-- |
-- Module      : Haskoin.Store.NodeRpc
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The JSON-RPC of the store's node (@--node-rpc@), as far as the store
-- uses it: the HTTP client that reaches it, and the mempool of the node
-- read through it, as a source of the path into the shared mempool
-- ('nodeFeed').
--
-- A store needs nothing of a node but to be its peer, and the RPC is an
-- option. What it adds to the path into the pool is a source that does not
-- depend on what the node allows its peers: a node announces a transaction
-- to a peer once, so what a store misses comes back only by reading a
-- whole mempool again, and Bitcoin Core lets only a peer that it
-- whitelists do that. The RPC always answers its account.
--
-- The rule that this module keeps is to tell a node that __says__
-- something from a node that __does not answer__. A node that says that it
-- has not a transaction is believed, and the transaction is given up as
-- gone from its mempool. A node that does not answer, or whose answer
-- cannot be read, says nothing about any transaction: that is an
-- exception here, t'RpcFailure', never an empty mempool or a transaction
-- that is gone. The code before this one read a failed call as an empty
-- mempool.
module Haskoin.Store.NodeRpc
  ( nodeSession,
    unixSocketManager,
    nodeOptions,
    RpcFailure (..),
    nodeAnswers,
    nodeFeed,
  )
where

import Control.Exception (Exception, throwIO)
import Control.Lens ((&), (.~), (?~), (^.))
import Control.Monad (forM)
import Data.Aeson ((.=))
import Data.Aeson qualified as A
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Foldable (toList)
import Data.HashMap.Strict qualified as HashMap
import Data.Serialize (decode)
import Data.Text (Text)
import Data.Text qualified as T
import Haskoin (decodeHex, hexToTxHash, txHashToHex)
import Haskoin.Store.Exception (onException)
import Haskoin.Store.Ingest.Run (Feed (..))
import Network.HTTP.Client (ManagerSettings (..), defaultManagerSettings)
import Network.HTTP.Client.Internal (socketConnection)
import Network.Socket (Family (AF_UNIX), SockAddr (SockAddrUnix), SocketType (Stream))
import Network.Socket qualified as Socket
import Network.Wreq qualified as Wreq
import Network.Wreq.Session (Session)
import Network.Wreq.Session qualified as Wreq.Session

-- | An HTTP client for the RPC of the node: one whose every connection
-- goes to a Unix socket if one is given (@--node-rpc-socket@), or one that
-- goes where the URL says.
nodeSession :: Maybe FilePath -> IO Session
nodeSession = \case
  Nothing -> Wreq.Session.newAPISession
  Just path -> Wreq.Session.newSessionControl Nothing (unixSocketManager path)

-- | Settings of an HTTP client whose every connection goes to a Unix
-- socket, whatever host and port the URL names: for the RPC of a node that
-- is reached through a socket (@--node-rpc-socket@). Neither Bitcoin Core
-- nor Bitcoin Cash Node listens on one, so the socket is something that
-- joins it to the node, such as socat, or the bridge of a test in a
-- container. The URL still carries the user and the password of the RPC.
unixSocketManager :: FilePath -> ManagerSettings
unixSocketManager path =
  defaultManagerSettings
    { managerRawConnection = return $ \_ _ _ -> do
        s <- Socket.socket AF_UNIX Stream Socket.defaultProtocol
        Socket.connect s (SockAddrUnix path) `onException` Socket.close s
        socketConnection s 8192
    }

-- | The options of every request to the node's JSON-RPC: an answer of any
-- status is read, since the node says what went wrong in its body, and the
-- connection is closed after each answer. A connection kept open made
-- Bitcoin Cash Node wait, when it stopped, until the connection had been
-- idle for its @-rpcservertimeout@, 30 seconds by default. A new
-- connection for each request costs nothing that shows: the store calls
-- the RPC to publish a transaction, and to read the mempool in requests of
-- thousands of transactions each.
nodeOptions :: Wreq.Options
nodeOptions =
  Wreq.defaults
    & Wreq.checkResponse ?~ (\_ _ -> return ())
    & Wreq.header "Connection" .~ ["close"]

-- | The node did not answer a call of its RPC, or answered what cannot be
-- read, with what happened. It says nothing of what the node has.
newtype RpcFailure = RpcFailure String
  deriving (Show)

instance Exception RpcFailure

-- | What a node answers to calls of its JSON-RPC, made in one batch, in
-- the order of the calls: for each the result, or the error that the node
-- gave in its place. Throws when the node gives no answer that can be read
-- to any one of the calls, t'RpcFailure' or the exception of the HTTP
-- client: the answers are all there or none is.
nodeAnswers :: Session -> String -> [(Text, [A.Value])] -> IO [Either A.Value A.Value]
nodeAnswers _ _ [] = return []
nodeAnswers session url calls = do
  r <- Wreq.Session.postWith nodeOptions session url (A.toJSON (zipWith call [0 :: Int ..] calls))
  let status = r ^. Wreq.responseStatus . Wreq.statusCode
  answers <- case A.decode (r ^. Wreq.responseBody) of
    Just (A.Array a) ->
      return . HashMap.fromList $
        [ (i, answer o)
        | A.Object o <- toList a,
          Just (A.String i) <- [KeyMap.lookup "id" o]
        ]
    _ -> throwIO (RpcFailure ("The node answered HTTP " <> show status <> " and nothing that can be read"))
  forM [0 .. length calls - 1] $ \i ->
    case HashMap.lookup (T.pack (show i)) answers of
      Just (Just x) -> return x
      _ -> throwIO (RpcFailure ("The node gave no answer to call " <> show i <> " of " <> show (length calls)))
  where
    call i (method, params) =
      A.object
        [ "jsonrpc" .= ("1.0" :: Text),
          "id" .= T.pack (show i),
          "method" .= method,
          "params" .= params
        ]
    -- an error that is not null, or else a result, which may be null
    answer o = case (KeyMap.lookup "error" o, KeyMap.lookup "result" o) of
      (Just e, _) | e /= A.Null -> Just (Left e)
      (_, Just v) -> Just (Right v)
      _ -> Nothing

-- | The mempool of a node, read over its RPC, as a source of the path
-- into the shared mempool ('Haskoin.Store.Ingest.Run.ingestFeed'):
-- @getrawmempool@ for the ids, and @getrawtransaction@ in one batch for
-- the transactions that the path asks for.
--
-- A transaction is said to be gone only when the node says so, with the
-- error that both Bitcoin Core and Bitcoin Cash Node give for a
-- transaction that they have not (@RPC_INVALID_ADDRESS_OR_KEY@, -5).
-- Anything else that is not the transaction is an t'RpcFailure'.
--
-- How many transactions a batch has is not decided here: the path asks
-- for as many as the window that it keeps for this source, which follows
-- what the node delivers in time.
nodeFeed :: Session -> String -> Feed
nodeFeed session url = Feed {label = "the mempool of the node over its RPC", names, bodies}
  where
    names =
      nodeAnswers session url [("getrawmempool", [])] >>= \case
        [Right (A.Array a)] | Just hs <- mapM hash (toList a) -> return hs
        other -> throwIO (RpcFailure ("Unexpected answer to getrawmempool: " <> brief other))
    hash = \case
      A.String t -> hexToTxHash t
      _ -> Nothing
    bodies hs = do
      answers <- nodeAnswers session url [("getrawtransaction", [A.String (txHashToHex h)]) | h <- hs]
      forM answers $ \case
        Right (A.String hex)
          | Just bytes <- decodeHex hex, Right tx <- decode bytes -> return (Just tx)
        Left e | lacks e -> return Nothing
        other -> throwIO (RpcFailure ("Unexpected answer to getrawtransaction: " <> brief other))
    lacks = \case
      A.Object o -> KeyMap.lookup "code" o == Just (A.Number (-5))
      _ -> False
    brief :: (Show x) => x -> String
    brief = take 200 . show
