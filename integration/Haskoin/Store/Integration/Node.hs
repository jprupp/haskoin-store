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
-- Only RPC calls that both nodes have are used. Bitcoin Cash Node has no
-- @generateblock@, so the reorganisation test is built on coinbase maturity
-- instead (see "Haskoin.Store.IntegrationSpec").
module Haskoin.Store.Integration.Node
  ( NodeKind (..),
    Node (..),
    nodeContainer,
    strangerContainer,
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

import Control.Lens ((&), (?~), (^.))
import Control.Monad (void)
import Data.Aeson (Value (..), eitherDecode, object, (.=))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Foldable (toList)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Word (Word64)
import Haskoin (Network, bchRegTest, btcRegTest)
import Haskoin.Store.Integration.Image (bchnBin, coreBin)
import Haskoin.Store.Integration.Podman (ContainerSpec (..))
import Network.Wreq qualified as Wreq
import UnliftIO (Exception, throwIO, tryAny)
import UnliftIO.Concurrent (threadDelay)

-- | Which node software.
data NodeKind = BitcoinCore | BitcoinCashNode
  deriving (Eq, Show)

-- | A node and how to reach it.
data Node = Node
  { kind :: !NodeKind,
    -- | URL of the JSON-RPC interface
    url :: !String,
    -- | port of the peer-to-peer interface on 127.0.0.1
    p2p :: !Int
  }

-- | Network constants of the regtest chain of a node.
nodeNetwork :: NodeKind -> Network
nodeNetwork BitcoinCore = btcRegTest
nodeNetwork BitcoinCashNode = bchRegTest

-- | Container of a regtest node, from the test image
-- ("Haskoin.Store.Integration.Image"): its chain is in a tmpfs of 512 MiB,
-- it may use 1 GiB of memory, and its caches and mempool are kept small.
--
-- Every peer is whitelisted, so that the store may ask for the mempool and
-- gets transactions relayed without delay.
nodeContainer :: NodeKind -> String -> ContainerSpec
nodeContainer kind image =
  ContainerSpec
    { name = case kind of
        BitcoinCore -> "bitcoin-core"
        BitcoinCashNode -> "bitcoin-cash-node",
      image = image,
      options =
        [ "--memory",
          "1g",
          "--tmpfs",
          "/chain:rw,size=512m,mode=1777",
          "--entrypoint",
          case kind of
            BitcoinCore -> coreBin
            BitcoinCashNode -> bchnBin
        ],
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
      ports = [18443, 18444]
    }

-- | The same node without the whitelist: it treats the store as it treats
-- any peer that connected to it. It announces transactions on its timer
-- and not at once, does not answer a request for its mempool, and may
-- close the connections of a peer that sends it an invalid transaction.
strangerContainer :: NodeKind -> String -> ContainerSpec
strangerContainer kind image =
  spec {name = spec.name <> "-stranger", command = filter (/= "-whitelist=0.0.0.0/0") spec.command}
  where
    spec = nodeContainer kind image

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
