-- |
-- Module      : Haskoin.Store.Integration.Node
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Regtest nodes for the integration tests: Bitcoin Core for Bitcoin and
-- Bitcoin Cash Node for Bitcoin Cash, each in a container, driven through
-- JSON-RPC.
--
-- A regtest node gives the tests a chain they fully control: they mine blocks
-- on demand, send transactions from the wallet of the node, and sign
-- transactions that spend from the keys of the test xpubs. The store under
-- test connects to the node over the peer-to-peer protocol, as it would to
-- the real networks.
--
-- The node runs in a container of podman-test ("Test.Podman"), whose bridge
-- joins its RPC and peer-to-peer ports to Unix sockets of its pod: the
-- tests call the RPC through one ('Haskoin.Store.Web.unixSocketManager'),
-- and the store connects to the node as the peer @unix:PATH@. Neither node
-- listens on a socket itself.
--
-- Only RPC calls that both nodes have are used. Bitcoin Cash Node has no
-- @generateblock@, so the reorganisation test is built on coinbase maturity
-- instead (see "Haskoin.Store.IntegrationSpec").
module Haskoin.Store.Integration.Node
  ( NodeKind (..),
    Node (..),
    nodeContainer,
    strangerContainer,
    nodeOf,
    nodeNetwork,
    RpcError (..),
    rpc,
    waitRpc,
    ensureWallet,
    field,
    textField,
    btc,
    sats,
    getNewAddress,
    generateTo,
    sendTo,
    sendMany,
    getRawTx,
    bestBlockHash,
    rawMempool,
  )
where

import Control.Concurrent (threadDelay)
import Control.Exception (Exception, throwIO)
import Control.Lens ((&), (.~), (?~), (^.))
import Control.Monad (void)
import Data.Aeson (Value (..), eitherDecode, object, (.=))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Foldable (toList)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Word (Word64)
import Haskoin (Network, bchRegTest, btcRegTest)
import Haskoin.Store.Exception (tryAny)
import Haskoin.Store.Integration.Image (bchnBin, coreBin, testImage)
import Haskoin.Store.Web (unixSocketManager)
import Network.HTTP.Client (Manager, newManager)
import Network.Wreq qualified as Wreq
import Test.Podman (Address (..), ContainerSpec (..), Endpoint (..), Pod, address, container)

-- | Which node software.
data NodeKind = BitcoinCore | BitcoinCashNode
  deriving (Eq, Show)

-- | A node and how to reach it.
data Node = Node
  { kind :: !NodeKind,
    -- | URL of the JSON-RPC interface, with its user and password; its
    -- host is not used, since every request goes through 'rpcSocket'
    url :: !String,
    -- | the Unix socket of the JSON-RPC interface
    rpcSocket :: !FilePath,
    -- | an HTTP client whose connections go to 'rpcSocket'
    manager :: !Manager,
    -- | the peer of the store: the Unix socket of the peer-to-peer
    -- interface, as @unix:PATH@
    peer :: !String
  }

-- | The node of a pod that runs 'nodeContainer' or 'strangerContainer'.
nodeOf :: NodeKind -> Pod -> IO Node
nodeOf kind p = do
  rpcSocket <- socketOf "rpc"
  p2pSocket <- socketOf "p2p"
  manager <- newManager (unixSocketManager rpcSocket)
  return
    Node
      { kind,
        url = "http://test:test@localhost/",
        rpcSocket,
        manager,
        peer = "unix:" <> p2pSocket
      }
  where
    socketOf e = case address p e of
      UnixSocket path -> return path
      a -> fail ("The endpoint " <> e <> " of the node is not a socket: " <> show a)

-- | Network constants of the regtest chain of a node.
nodeNetwork :: NodeKind -> Network
nodeNetwork BitcoinCore = btcRegTest
nodeNetwork BitcoinCashNode = bchRegTest

-- | Container of a regtest node, from the test image
-- ("Haskoin.Store.Integration.Image"): its chain is in a tmpfs of 512 MiB,
-- it may use 1 GiB of memory, and its caches and mempool are kept small.
-- Its RPC and peer-to-peer ports are bridged to the sockets @rpc@ and
-- @p2p@ of its pod.
--
-- Every peer is whitelisted, so that the store may ask for the mempool and
-- gets transactions relayed without delay.
nodeContainer :: NodeKind -> ContainerSpec
nodeContainer kind =
  ( container
      ( case kind of
          BitcoinCore -> "bitcoin-core"
          BitcoinCashNode -> "bitcoin-cash-node"
      )
      testImage
  )
    { entrypoint = Just $ case kind of
        BitcoinCore -> coreBin
        BitcoinCashNode -> bchnBin,
      memory = Just "1g",
      tmpfs = [("/chain", "512m")],
      command =
        [ "-regtest",
          "-datadir=/chain",
          "-printtoconsole",
          "-server",
          "-rpcuser=test",
          "-rpcpassword=test",
          "-rpcbind=0.0.0.0",
          "-rpcallowip=0.0.0.0/0",
          "-bind=0.0.0.0",
          "-listen=1",
          "-whitelist=0.0.0.0/0",
          "-txindex=1",
          "-fallbackfee=0.0002",
          "-dbcache=16",
          "-maxmempool=16"
        ]
          <> case kind of
            -- releases of Bitcoin Cash Node stop serving RPC some months
            -- after they come out unless told not to
            BitcoinCashNode -> ["-expire=0"]
            BitcoinCore -> [],
      endpoints = [Bridged "rpc" 18443, Bridged "p2p" 18444]
    }

-- | The same node without the whitelist: it treats the store as it treats
-- any peer that connected to it. It announces transactions on its timer
-- and not at once, and may close the connections of a peer that sends it an
-- invalid transaction.
--
-- Bitcoin Core gives the store its mempool and nothing more (the permission
-- @mempool@), which the tests tell the store (@peerPermissions@): once the
-- pool fills, the store asks the peers whose node allows it for their
-- mempools, and a node that does not allow it closes the connection, with
-- what it had in its mempool then never announced again. Bitcoin Cash Node
-- gets no permission: it serves Bloom filters, and answers the request of
-- any peer.
strangerContainer :: NodeKind -> ContainerSpec
strangerContainer kind =
  spec
    { name = spec.name <> "-stranger",
      command = filter (/= "-whitelist=0.0.0.0/0") spec.command <> mempoolOnly
    }
  where
    spec = nodeContainer kind
    mempoolOnly = case kind of
      BitcoinCore -> ["-whitelist=mempool@0.0.0.0/0"]
      BitcoinCashNode -> []

-- | Error returned by a node.
data RpcError = RpcError !Text !Value
  deriving (Show)

instance Exception RpcError

-- | Call a method of the node and return its result.
rpc :: Node -> Text -> [Value] -> IO Value
rpc node method params = do
  let opts =
        Wreq.defaults
          & Wreq.auth ?~ Wreq.basicAuth "test" "test"
          & Wreq.checkResponse ?~ (\_ _ -> return ())
          & Wreq.manager .~ Right node.manager
      body =
        object
          [ "jsonrpc" .= ("1.0" :: Text),
            "id" .= ("test" :: Text),
            "method" .= method,
            "params" .= params
          ]
  r <- Wreq.postWith opts node.url body
  case eitherDecode (r ^. Wreq.responseBody) of
    Left e -> fail ("Bad answer to " <> T.unpack method <> ": " <> e)
    Right v -> case (field "error" v, field "result" v) of
      (Just Null, Just x) -> return x
      (Nothing, Just x) -> return x
      (Just e, _) -> throwIO (RpcError method e)
      _ -> fail ("No result in answer to " <> T.unpack method)

-- | Wait until the node answers RPC calls, for up to 90 seconds.
waitRpc :: Node -> IO ()
waitRpc node = go (360 :: Int)
  where
    go n =
      tryAny (rpc node "getblockchaininfo" []) >>= \case
        Right _ -> return ()
        Left e
          | n <= 0 -> fail ("The node did not start: " <> show e)
          | otherwise -> threadDelay 250000 >> go (n - 1)

-- | Make sure the node has exactly one wallet loaded, so that wallet calls
-- need no wallet name. Bitcoin Cash Node loads a default wallet; Bitcoin Core
-- has none until one is created.
ensureWallet :: Node -> IO ()
ensureWallet node =
  rpc node "listwallets" [] >>= \case
    Array ws | not (null ws) -> return ()
    _ -> void $ rpc node "createwallet" [String "test"]

-- | Field of a JSON object.
field :: Text -> Value -> Maybe Value
field k (Object o) = KeyMap.lookup (Key.fromText k) o
field _ _ = Nothing

-- | Text field of a JSON object.
textField :: Text -> Value -> Maybe Text
textField k v = case field k v of
  Just (String t) -> Just t
  _ -> Nothing

-- | Amount in satoshi as a JSON number of coins.
btc :: Word64 -> Value
btc s = Number (fromIntegral s / 100000000)

-- | JSON number of coins in satoshi.
sats :: Value -> Word64
sats (Number n) = round (n * 100000000)
sats v = error ("Not an amount: " <> show v)

asText :: Value -> IO Text
asText (String t) = return t
asText v = fail ("Expected text, got " <> show v)

-- | New address of the wallet of the node.
getNewAddress :: Node -> IO Text
getNewAddress node = rpc node "getnewaddress" [] >>= asText

-- | Mine blocks paying to an address. Returns their hashes.
generateTo :: Node -> Int -> Text -> IO [Text]
generateTo node n addr =
  rpc node "generatetoaddress" [Number (fromIntegral n), String addr] >>= \case
    Array hs -> mapM asText (toList hs)
    v -> fail ("Unexpected answer to generatetoaddress: " <> show v)

-- | Send from the wallet of the node to an address. Returns the transaction
-- hash.
sendTo :: Node -> Text -> Word64 -> IO Text
sendTo node addr amount =
  rpc node "sendtoaddress" [String addr, btc amount] >>= asText

-- | Send from the wallet of the node to several addresses in one
-- transaction. Returns the transaction hash.
sendMany :: Node -> [(Text, Word64)] -> IO Text
sendMany node outs =
  rpc node "sendmany" [String "", object [Key.fromText a .= btc v | (a, v) <- outs]]
    >>= asText

-- | Decoded transaction.
getRawTx :: Node -> Text -> IO Value
getRawTx node txid = rpc node "getrawtransaction" [String txid, Bool True]

-- | Hash of the best block.
bestBlockHash :: Node -> IO Text
bestBlockHash node = rpc node "getbestblockhash" [] >>= asText

-- | Hashes of the transactions in the mempool.
rawMempool :: Node -> IO [Text]
rawMempool node =
  rpc node "getrawmempool" [] >>= \case
    Array ts -> mapM asText (toList ts)
    v -> fail ("Unexpected answer to getrawmempool: " <> show v)
