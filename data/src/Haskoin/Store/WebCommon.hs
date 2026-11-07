{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoFieldSelectors #-}

-- |
-- Module      : Haskoin.Store.WebCommon
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Description of the REST API, shared by the server ("Haskoin.Store.Web") and
-- the client ("Haskoin.Store.WebClient").
--
-- Each endpoint is a type, such as 'GetBlock', with an 'ApiResource' instance
-- that gives its HTTP method, path, parameters and response type. Parameters
-- are types with a 'Param' instance, which knows how to encode them in a URL
-- and parse them back.
--
-- Endpoints that return lists take a limit, an offset and a start
-- ('LimitsParam'). Results are ordered from newest to oldest.
-- Describing each endpoint once, as a type, keeps the server and the client from
-- drifting apart: the path, parameters and response type that the server routes
-- are the same ones the client builds requests from.
module Haskoin.Store.WebCommon where

import Control.Applicative ((<|>))
import Control.Monad (guard)
import Data.Bytes.Serial
import Data.Default (Default, def)
import Data.Proxy (Proxy (..))
import Data.String (IsString (..))
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Text qualified as T
import Haskoin.Address
import Haskoin.Block
  ( Block,
    BlockHash,
    blockHashToHex,
    hexToBlockHash,
  )
import Haskoin.Crypto (Ctx, Hash256)
import Haskoin.Crypto.Keys
import Haskoin.Network.Data
import Haskoin.Store.Data qualified as Store
import Haskoin.Transaction
import Network.HTTP.Types (StdMethod (..))
import Numeric.Natural (Natural)
import Text.Read (readMaybe)
import Web.Scotty.Trans qualified as Scotty

-------------------
-- API Resources --
-------------------

-- | An endpoint of the API. The request type @a@ determines the response type
-- @b@.
class (Serial b) => ApiResource a b | a -> b where
  -- | HTTP method; 'GET' by default.
  resourceMethod :: Proxy a -> StdMethod
  resourceMethod _ = GET
  -- | Path of the endpoint, given the encoded values of the parameters that
  -- are part of the path.
  resourcePath :: Proxy a -> ([Text] -> Text)
  -- | Parameters of a request: those that go in the path, and those that go in
  -- the query string.
  queryParams :: a -> ([ParamBox], [ParamBox]) -- (resource, querystring)
  queryParams _ = ([], [])
  -- | Types of the parameters that go in the path, used to build the route
  -- pattern of the server.
  captureParams :: Proxy a -> [ProxyBox]
  captureParams _ = []
  -- | Body of the request, if any.
  resourceBody :: a -> Maybe PostBox
  resourceBody = const Nothing

-- | Request body of any serializable type.
data PostBox = forall s. (Serial s) => PostBox !s

-- | Parameter value of any type.
data ParamBox = forall p. (Eq p, Param p) => ParamBox !p

-- | Parameter type, without a value.
data ProxyBox = forall p. (Param p) => ProxyBox !(Proxy p)

--------------------
-- Resource Paths --
--------------------

-- Blocks
-- | Request for a block by hash: @GET \/block\/:block@.
data GetBlock = GetBlock !BlockHash !NoTx

-- | Request for several blocks by hash: @GET \/blocks?blocks=…@.
data GetBlocks = GetBlocks ![BlockHash] !NoTx

-- | Request for a block by hash, serialized as in the peer-to-peer protocol: @GET \/block\/:block\/raw@.
newtype GetBlockRaw = GetBlockRaw BlockHash

-- | Request for the best block: @GET \/block\/best@.
newtype GetBlockBest = GetBlockBest NoTx

-- | Request for the best block, serialized: @GET \/block\/best\/raw@.
data GetBlockBestRaw = GetBlockBestRaw

-- | Request for the best block and its ancestors, up to 100 blocks: @GET \/block\/latest@.
newtype GetBlockLatest = GetBlockLatest NoTx

-- | Request for the blocks at a height: @GET \/block\/height\/:height@.
data GetBlockHeight = GetBlockHeight !HeightParam !NoTx

-- | Request for the blocks at several heights: @GET \/block\/heights?heights=…@.
data GetBlockHeights = GetBlockHeights !HeightsParam !NoTx

-- | Request for the blocks at a height, serialized: @GET \/block\/height\/:height\/raw@.
newtype GetBlockHeightRaw = GetBlockHeightRaw HeightParam

-- | Request for the last block with a timestamp at or before a time: @GET \/block\/time\/:time@.
data GetBlockTime = GetBlockTime !TimeParam !NoTx

-- | Request for the last block with a timestamp at or before a time, serialized: @GET \/block\/time\/:time\/raw@.
newtype GetBlockTimeRaw = GetBlockTimeRaw TimeParam

-- | Request for the first block with a median time past at or after a time: @GET \/block\/mtp\/:time@.
data GetBlockMTP = GetBlockMTP !TimeParam !NoTx

-- | Request for the first block with a median time past at or after a time, serialized: @GET \/block\/mtp\/:time\/raw@.
newtype GetBlockMTPRaw = GetBlockMTPRaw TimeParam

-- Transactions
-- | Request for a transaction: @GET \/transaction\/:txid@.
newtype GetTx = GetTx TxHash

-- | Request for several transactions: @GET \/transactions?txids=…@.
newtype GetTxs = GetTxs [TxHash]

-- | Request for a transaction, serialized: @GET \/transaction\/:txid\/raw@.
newtype GetTxRaw = GetTxRaw TxHash

-- | Request for several transactions, serialized: @GET \/transactions\/raw?txids=…@.
newtype GetTxsRaw = GetTxsRaw [TxHash]

-- | Request for the transactions of a block: @GET \/transactions\/block\/:block@.
newtype GetTxsBlock = GetTxsBlock BlockHash

-- | Request for the transactions of a block, serialized: @GET \/transactions\/block\/:block\/raw@.
newtype GetTxsBlockRaw = GetTxsBlockRaw BlockHash

-- | Request for whether any ancestor of a transaction is a coinbase confirmed after a height, or null if it cannot be computed within limits: @GET \/transaction\/:txid\/after\/:height@.
data GetTxAfter = GetTxAfter !TxHash !HeightParam

-- | Request to broadcast a transaction: @POST \/transactions@.
newtype PostTx = PostTx Tx

-- | Request for hashes of mempool transactions, newest first: @GET \/mempool@.
data GetMempool = GetMempool !(Maybe LimitParam) !OffsetParam

-- | Request for stream of new blocks and mempool transactions: @GET \/events@.
data GetEvents = GetEvents

-- Address
-- | Request for transactions of an address: @GET \/address\/:address\/transactions@.
data GetAddrTxs = GetAddrTxs !Address !LimitsParam

-- | Request for transactions of several addresses: @GET \/address\/transactions?addresses=…@.
data GetAddrsTxs = GetAddrsTxs ![Address] !LimitsParam

-- | Request for full transactions of an address: @GET \/address\/:address\/transactions\/full@.
data GetAddrTxsFull = GetAddrTxsFull !Address !LimitsParam

-- | Request for full transactions of several addresses: @GET \/address\/transactions\/full?addresses=…@.
data GetAddrsTxsFull = GetAddrsTxsFull ![Address] !LimitsParam

-- | Request for balance of an address: @GET \/address\/:address\/balance@.
newtype GetAddrBalance = GetAddrBalance Address

-- | Request for balances of several addresses: @GET \/address\/balances?addresses=…@.
newtype GetAddrsBalance = GetAddrsBalance [Address]

-- | Request for unspent outputs of an address: @GET \/address\/:address\/unspent@.
data GetAddrUnspent = GetAddrUnspent !Address !LimitsParam

-- | Request for unspent outputs of several addresses: @GET \/address\/unspent?addresses=…@.
data GetAddrsUnspent = GetAddrsUnspent ![Address] !LimitsParam

-- XPubs
-- | Request for summary of an extended public key: @GET \/xpub\/:xpub@.
data GetXPub = GetXPub !XPubKey !Store.DeriveType !NoCache

-- | Request for transactions of an extended public key: @GET \/xpub\/:xpub\/transactions@.
data GetXPubTxs = GetXPubTxs !XPubKey !Store.DeriveType !LimitsParam !NoCache

-- | Request for full transactions of an extended public key: @GET \/xpub\/:xpub\/transactions\/full@.
data GetXPubTxsFull = GetXPubTxsFull !XPubKey !Store.DeriveType !LimitsParam !NoCache

-- | Request for balances of the used addresses of an extended public key: @GET \/xpub\/:xpub\/balances@.
data GetXPubBalances = GetXPubBalances !XPubKey !Store.DeriveType !NoCache

-- | Request for unspent outputs of an extended public key: @GET \/xpub\/:xpub\/unspent@.
data GetXPubUnspent = GetXPubUnspent !XPubKey !Store.DeriveType !LimitsParam !NoCache

-- | Request to remove an extended public key from the cache: @DELETE \/xpub\/:xpub@.
data DelCachedXPub = DelCachedXPub !XPubKey !Store.DeriveType

-- Network
-- | Request for connected peers: @GET \/peers@.
data GetPeers = GetPeers

-- | Request for health of the store: @GET \/health@.
data GetHealth = GetHealth

------------
-- Blocks --
------------

instance ApiResource GetBlock Store.BlockData where
  resourcePath _ = ("/block/" <:>)
  queryParams (GetBlock h t) = ([ParamBox h], noDefBox t)
  captureParams _ = [ProxyBox (Proxy :: Proxy BlockHash)]

instance ApiResource GetBlocks (Store.SerialList Store.BlockData) where
  resourcePath _ _ = "/blocks"
  queryParams (GetBlocks hs t) = ([], [ParamBox hs] <> noDefBox t)

instance ApiResource GetBlockRaw (Store.RawResult Block) where
  resourcePath _ = "/block/" <+> "/raw"
  queryParams (GetBlockRaw h) = ([ParamBox h], [])
  captureParams _ = [ProxyBox (Proxy :: Proxy BlockHash)]

instance ApiResource GetBlockBest Store.BlockData where
  resourcePath _ _ = "/block/best"
  queryParams (GetBlockBest t) = ([], noDefBox t)

instance ApiResource GetBlockBestRaw (Store.RawResult Block) where
  resourcePath _ _ = "/block/best/raw"

instance ApiResource GetBlockLatest (Store.SerialList Store.BlockData) where
  resourcePath _ _ = "/block/latest"
  queryParams (GetBlockLatest t) = ([], noDefBox t)

instance ApiResource GetBlockHeight (Store.SerialList Store.BlockData) where
  resourcePath _ = ("/block/height/" <:>)
  queryParams (GetBlockHeight h t) = ([ParamBox h], noDefBox t)
  captureParams _ = [ProxyBox (Proxy :: Proxy HeightParam)]

instance ApiResource GetBlockHeights (Store.SerialList Store.BlockData) where
  resourcePath _ _ = "/block/heights"
  queryParams (GetBlockHeights hs t) = ([], [ParamBox hs] <> noDefBox t)

instance ApiResource GetBlockHeightRaw (Store.RawResultList Block) where
  resourcePath _ = "/block/height/" <+> "/raw"
  queryParams (GetBlockHeightRaw h) = ([ParamBox h], [])
  captureParams _ = [ProxyBox (Proxy :: Proxy HeightParam)]

instance ApiResource GetBlockTime Store.BlockData where
  resourcePath _ = ("/block/time/" <:>)
  queryParams (GetBlockTime u t) = ([ParamBox u], noDefBox t)
  captureParams _ = [ProxyBox (Proxy :: Proxy TimeParam)]

instance ApiResource GetBlockTimeRaw (Store.RawResult Block) where
  resourcePath _ = "/block/time/" <+> "/raw"
  queryParams (GetBlockTimeRaw u) = ([ParamBox u], [])
  captureParams _ = [ProxyBox (Proxy :: Proxy TimeParam)]

instance ApiResource GetBlockMTP Store.BlockData where
  resourcePath _ = ("/block/mtp/" <:>)
  queryParams (GetBlockMTP u t) = ([ParamBox u], noDefBox t)
  captureParams _ = [ProxyBox (Proxy :: Proxy TimeParam)]

instance ApiResource GetBlockMTPRaw (Store.RawResult Block) where
  resourcePath _ = "/block/mtp/" <+> "/raw"
  queryParams (GetBlockMTPRaw u) = ([ParamBox u], [])
  captureParams _ = [ProxyBox (Proxy :: Proxy TimeParam)]

------------------
-- Transactions --
------------------

instance ApiResource GetTx Store.Transaction where
  resourcePath _ = ("/transaction/" <:>)
  queryParams (GetTx h) = ([ParamBox h], [])
  captureParams _ = [ProxyBox (Proxy :: Proxy TxHash)]

instance ApiResource GetTxs (Store.SerialList Store.Transaction) where
  resourcePath _ _ = "/transactions"
  queryParams (GetTxs hs) = ([], [ParamBox hs])

instance ApiResource GetTxRaw (Store.RawResult Tx) where
  resourcePath _ = "/transaction/" <+> "/raw"
  queryParams (GetTxRaw h) = ([ParamBox h], [])
  captureParams _ = [ProxyBox (Proxy :: Proxy TxHash)]

instance ApiResource GetTxsRaw (Store.RawResultList Tx) where
  resourcePath _ _ = "/transactions/raw"
  queryParams (GetTxsRaw hs) = ([], [ParamBox hs])

instance ApiResource GetTxsBlock (Store.SerialList Store.Transaction) where
  resourcePath _ = ("/transactions/block/" <:>)
  queryParams (GetTxsBlock h) = ([ParamBox h], [])
  captureParams _ = [ProxyBox (Proxy :: Proxy BlockHash)]

instance ApiResource GetTxsBlockRaw (Store.RawResultList Tx) where
  resourcePath _ = "/transactions/block/" <+> "/raw"
  queryParams (GetTxsBlockRaw h) = ([ParamBox h], [])
  captureParams _ = [ProxyBox (Proxy :: Proxy BlockHash)]

instance ApiResource GetTxAfter (Store.GenericResult (Maybe Bool)) where
  resourcePath _ = "/transaction/" <++> "/after/"
  queryParams (GetTxAfter h i) = ([ParamBox h, ParamBox i], [])
  captureParams _ =
    [ ProxyBox (Proxy :: Proxy TxHash),
      ProxyBox (Proxy :: Proxy HeightParam)
    ]

instance ApiResource PostTx Store.TxId where
  resourceMethod _ = POST
  resourcePath _ _ = "/transactions"
  resourceBody (PostTx tx) = Just $ PostBox tx

instance ApiResource GetMempool (Store.SerialList TxHash) where
  resourcePath _ _ = "/mempool"
  queryParams (GetMempool l o) = ([], noMaybeBox l <> noDefBox o)

instance ApiResource GetEvents (Store.SerialList Store.Event) where
  resourcePath _ _ = "/events"

-------------
-- Address --
-------------

instance ApiResource GetAddrTxs (Store.SerialList Store.TxRef) where
  resourcePath _ = "/address/" <+> "/transactions"
  queryParams (GetAddrTxs a (LimitsParam l o sM)) =
    ([ParamBox a], noMaybeBox l <> noDefBox o <> noMaybeBox sM)
  captureParams _ = [ProxyBox (Proxy :: Proxy Address)]

instance ApiResource GetAddrsTxs (Store.SerialList Store.TxRef) where
  resourcePath _ _ = "/address/transactions"
  queryParams (GetAddrsTxs as (LimitsParam l o sM)) =
    ([], [ParamBox as] <> noMaybeBox l <> noDefBox o <> noMaybeBox sM)

instance ApiResource GetAddrTxsFull (Store.SerialList Store.Transaction) where
  resourcePath _ = "/address/" <+> "/transactions/full"
  queryParams (GetAddrTxsFull a (LimitsParam l o sM)) =
    ([ParamBox a], noMaybeBox l <> noDefBox o <> noMaybeBox sM)
  captureParams _ = [ProxyBox (Proxy :: Proxy Address)]

instance ApiResource GetAddrsTxsFull (Store.SerialList Store.Transaction) where
  resourcePath _ _ = "/address/transactions/full"
  queryParams (GetAddrsTxsFull as (LimitsParam l o sM)) =
    ([], [ParamBox as] <> noMaybeBox l <> noDefBox o <> noMaybeBox sM)

instance ApiResource GetAddrBalance Store.Balance where
  resourcePath _ = "/address/" <+> "/balance"
  queryParams (GetAddrBalance a) = ([ParamBox a], [])
  captureParams _ = [ProxyBox (Proxy :: Proxy Address)]

instance ApiResource GetAddrsBalance (Store.SerialList Store.Balance) where
  resourcePath _ _ = "/address/balances"
  queryParams (GetAddrsBalance as) = ([], [ParamBox as])

instance ApiResource GetAddrUnspent (Store.SerialList Store.Unspent) where
  resourcePath _ = "/address/" <+> "/unspent"
  queryParams (GetAddrUnspent a (LimitsParam l o sM)) =
    ([ParamBox a], noMaybeBox l <> noDefBox o <> noMaybeBox sM)
  captureParams _ = [ProxyBox (Proxy :: Proxy Address)]

instance ApiResource GetAddrsUnspent (Store.SerialList Store.Unspent) where
  resourcePath _ _ = "/address/unspent"
  queryParams (GetAddrsUnspent as (LimitsParam l o sM)) =
    ([], [ParamBox as] <> noMaybeBox l <> noDefBox o <> noMaybeBox sM)

-----------
-- XPubs --
-----------

instance ApiResource GetXPub Store.XPubSummary where
  resourcePath _ = ("/xpub/" <:>)
  queryParams (GetXPub p d n) = ([ParamBox p], noDefBox d <> noDefBox n)
  captureParams _ = [ProxyBox (Proxy :: Proxy XPubKey)]

instance ApiResource GetXPubTxs (Store.SerialList Store.TxRef) where
  resourcePath _ = "/xpub/" <+> "/transactions"
  queryParams (GetXPubTxs p d (LimitsParam l o sM) n) =
    ( [ParamBox p],
      noDefBox d <> noMaybeBox l <> noDefBox o <> noMaybeBox sM <> noDefBox n
    )
  captureParams _ = [ProxyBox (Proxy :: Proxy XPubKey)]

instance ApiResource GetXPubTxsFull (Store.SerialList Store.Transaction) where
  resourcePath _ = "/xpub/" <+> "/transactions/full"
  queryParams (GetXPubTxsFull p d (LimitsParam l o sM) n) =
    ( [ParamBox p],
      noDefBox d <> noMaybeBox l <> noDefBox o <> noMaybeBox sM <> noDefBox n
    )
  captureParams _ = [ProxyBox (Proxy :: Proxy XPubKey)]

instance ApiResource GetXPubBalances (Store.SerialList Store.XPubBal) where
  resourcePath _ = "/xpub/" <+> "/balances"
  queryParams (GetXPubBalances p d n) = ([ParamBox p], noDefBox d <> noDefBox n)
  captureParams _ = [ProxyBox (Proxy :: Proxy XPubKey)]

instance ApiResource GetXPubUnspent (Store.SerialList Store.XPubUnspent) where
  resourcePath _ = "/xpub/" <+> "/unspent"
  queryParams (GetXPubUnspent p d (LimitsParam l o sM) n) =
    ( [ParamBox p],
      noDefBox d <> noMaybeBox l <> noDefBox o <> noMaybeBox sM <> noDefBox n
    )
  captureParams _ = [ProxyBox (Proxy :: Proxy XPubKey)]

instance ApiResource DelCachedXPub (Store.GenericResult Bool) where
  resourceMethod _ = DELETE
  resourcePath _ = ("/xpub/" <:>)
  queryParams (DelCachedXPub p d) = ([ParamBox p], noDefBox d)
  captureParams _ = [ProxyBox (Proxy :: Proxy XPubKey)]

-------------
-- Network --
-------------

instance ApiResource GetPeers (Store.SerialList Store.PeerInfo) where
  resourcePath _ _ = "/peers"

instance ApiResource GetHealth Store.HealthCheck where
  resourcePath _ _ = "/health"

-------------
-- Helpers --
-------------

-- | Path made of a prefix followed by one parameter.
(<:>) :: Text -> [Text] -> Text
(<:>) = (<+> "")

-- | Path made of a prefix, one parameter and a suffix.
(<+>) :: Text -> Text -> [Text] -> Text
a <+> b = fill 1 [a, b]

-- | Path made of a prefix, a parameter, an infix and another parameter.
(<++>) :: Text -> Text -> [Text] -> Text
a <++> b = fill 2 [a, b]

-- | Interleave path fragments with this many parameters. Calls 'error' if the number of parameters is wrong.
fill :: Int -> [Text] -> [Text] -> Text
fill i a b
  | length b /= i = error "Invalid query parameters"
  | otherwise = mconcat $ uncurry (<>) <$> zip a (b <> repeat "")

-- | Parameter to send, unless it has its default value.
noDefBox :: (Default p, Param p, Eq p) => p -> [ParamBox]
noDefBox p = [ParamBox p | p /= def]

-- | Parameter to send, if any.
noMaybeBox :: (Param p, Eq p) => Maybe p -> [ParamBox]
noMaybeBox (Just p) = [ParamBox p]
noMaybeBox _ = []

-- | Proxy for the type of a value.
asProxy :: a -> Proxy a
asProxy = const Proxy

-- | Path of a request, with its path parameters filled in.
queryPath :: (ApiResource a b) => Network -> Ctx -> a -> Text
queryPath net ctx a = f $ encParam <$> fst (queryParams a)
  where
    f = resourcePath $ asProxy a
    encParam (ParamBox p) =
      case encodeParam net ctx p of
        Just [res] -> res
        _ -> error "Invalid query param"

-- | Route pattern of an endpoint for the server, with a named capture for each path parameter.
capturePath :: (ApiResource a b) => Proxy a -> Scotty.RoutePattern
capturePath proxy =
  fromString $ cs $ f $ toLabel <$> captureParams proxy
  where
    f = resourcePath proxy
    toLabel (ProxyBox p) = ":" <> proxyLabel p

-- | Name of a parameter in URLs.
paramLabel :: (Param p) => p -> Text
paramLabel = proxyLabel . asProxy

-------------
-- Options --
-------------

-- | Type that can be a parameter of a request.
class Param a where
  -- | Name of the parameter in URLs.
  proxyLabel :: Proxy a -> Text
  -- | Encode a value as one or more strings. A list parameter is sent as
  -- comma-separated values.
  encodeParam :: Network -> Ctx -> a -> Maybe [Text]
  -- | Parse a value from one or more strings.
  parseParam :: Network -> Ctx -> [Text] -> Maybe a

instance Param Address where
  proxyLabel = const "address"
  encodeParam net ctx a = (: []) <$> addrToText net a
  parseParam net ctx [a] = textToAddr net a
  parseParam net ctx _ = Nothing

instance Param [Address] where
  proxyLabel = const "addresses"
  encodeParam net ctx = mapM (addrToText net)
  parseParam net ctx = mapM (textToAddr net)

-- | Where a page of results starts. In URLs it is a transaction hash, a block
-- height, or a Unix timestamp. Numbers up to 1230768000 are heights and larger
-- ones are timestamps.
data StartParam
  = -- | start at a transaction
    StartParamHash {hash :: Hash256}
  | -- | start at a block height
    StartParamHeight {height :: Natural}
  | -- | start at the last block at or before a time
    StartParamTime {time :: Store.UnixTime}
  deriving (Eq, Show)

instance Param StartParam where
  proxyLabel = const "height"
  encodeParam net ctx p =
    case p of
      StartParamHash h -> return [txHashToHex (TxHash h)]
      StartParamHeight h -> do
        guard $ h <= 1230768000
        return [cs $ show h]
      StartParamTime t -> do
        guard $ t > 1230768000
        return [cs $ show t]
  parseParam net ctx [s] =
    parseHash <|> parseHeight <|> parseUnix
    where
      parseHash = do
        guard (T.length s == 32 * 2)
        TxHash x <- hexToTxHash s
        return $ StartParamHash x
      parseHeight = do
        x <- readMaybe $ cs s
        guard $ x <= 1230768000
        return $ StartParamHeight x
      parseUnix = do
        x <- readMaybe $ cs s
        guard $ x > 1230768000
        return $ StartParamTime x
  parseParam net ctx _ = Nothing

-- | Number of results to skip (@offset@).
newtype OffsetParam = OffsetParam {get :: Natural}
  deriving (Eq, Show, Read, Enum, Ord, Num, Real, Integral)

instance Default OffsetParam where
  def = OffsetParam 0

instance Param OffsetParam where
  proxyLabel = const "offset"
  encodeParam net ctx (OffsetParam o) = Just [cs $ show o]
  parseParam net ctx [s] = OffsetParam <$> readMaybe (cs s)
  parseParam net ctx _ = Nothing

-- | Maximum number of results (@limit@).
newtype LimitParam = LimitParam {get :: Natural}
  deriving (Eq, Show, Read, Enum, Ord, Num, Real, Integral)

instance Param LimitParam where
  proxyLabel = const "limit"
  encodeParam net ctx (LimitParam l) = Just [cs $ show l]
  parseParam net ctx [s] = LimitParam <$> readMaybe (cs s)
  parseParam net ctx _ = Nothing

-- | Pagination of a list of results.
data LimitsParam = LimitsParam
  { -- | maximum number of results; @0@ means the maximum the server allows,
    -- and 'Nothing' the server default
    limit :: Maybe LimitParam,
    -- | number of results to skip after the start
    offset :: OffsetParam,
    -- | where to start, or from the newest result
    start :: Maybe StartParam
  }
  deriving (Eq, Show)

instance Default LimitsParam where
  def = LimitsParam Nothing def Nothing

-- | Block height (@height@).
newtype HeightParam = HeightParam {get :: Natural}
  deriving (Eq, Show, Read, Enum, Ord, Num, Real, Integral)

instance Param HeightParam where
  proxyLabel = const "height"
  encodeParam net ctx (HeightParam h) = Just [cs $ show h]
  parseParam net ctx [s] = HeightParam <$> readMaybe (cs s)
  parseParam net ctx _ = Nothing

-- | List of block heights (@heights@).
newtype HeightsParam = HeightsParam {get :: [Natural]}
  deriving (Eq, Show, Read)

instance Param HeightsParam where
  proxyLabel = const "heights"
  encodeParam net ctx (HeightsParam hs) = Just $ cs . show <$> hs
  parseParam net ctx xs = HeightsParam <$> mapM (readMaybe . cs) xs

-- | Unix timestamp in seconds (@time@).
newtype TimeParam = TimeParam {get :: Store.UnixTime}
  deriving (Eq, Show, Read, Enum, Ord, Num, Real, Integral)

instance Param TimeParam where
  proxyLabel = const "time"
  encodeParam net ctx (TimeParam t) = Just [cs $ show t]
  parseParam net ctx [s] = TimeParam <$> readMaybe (cs s)
  parseParam net ctx _ = Nothing

instance Param XPubKey where
  proxyLabel = const "xpub"
  encodeParam net ctx p = Just [xPubExport net ctx p]
  parseParam net ctx [s] = xPubImport net ctx s
  parseParam net ctx _ = Nothing

instance Param Store.DeriveType where
  proxyLabel = const "derive"
  encodeParam net ctx p = do
    guard (net.segWit || p == Store.DeriveNormal)
    Just [Store.deriveTypeToText p]
  parseParam net ctx d = do
    res <- case d of
      [x] -> Store.textToDeriveType x
      _ -> Nothing
    guard (net.segWit || res == Store.DeriveNormal)
    return res

-- | Do not use the cache to answer an xpub query (@nocache@).
newtype NoCache = NoCache {get :: Bool}
  deriving (Eq, Show, Read)

instance Default NoCache where
  def = NoCache False

instance Param NoCache where
  proxyLabel = const "nocache"
  encodeParam net ctx (NoCache True) = Just ["true"]
  encodeParam net ctx (NoCache False) = Just ["false"]
  parseParam net ctx = \case
    ["true"] -> Just $ NoCache True
    ["false"] -> Just $ NoCache False
    _ -> Nothing

-- | Leave out the transaction hashes of a block, except that of the coinbase
-- (@notx@).
newtype NoTx = NoTx {get :: Bool}
  deriving (Eq, Show, Read)

instance Default NoTx where
  def = NoTx False

instance Param NoTx where
  proxyLabel = const "notx"
  encodeParam net ctx (NoTx True) = Just ["true"]
  encodeParam net ctx (NoTx False) = Just ["false"]
  parseParam net ctx = \case
    ["true"] -> Just $ NoTx True
    ["false"] -> Just $ NoTx False
    _ -> Nothing

instance Param BlockHash where
  proxyLabel = const "block"
  encodeParam net ctx b = Just [blockHashToHex b]
  parseParam net ctx [s] = hexToBlockHash s
  parseParam net ctx _ = Nothing

instance Param [BlockHash] where
  proxyLabel = const "blocks"
  encodeParam net ctx bs = Just $ blockHashToHex <$> bs
  parseParam net ctx = mapM hexToBlockHash

instance Param TxHash where
  proxyLabel = const "txid"
  encodeParam net ctx t = Just [txHashToHex t]
  parseParam net ctx [s] = hexToTxHash s
  parseParam net ctx _ = Nothing

instance Param [TxHash] where
  proxyLabel = const "txids"
  encodeParam net ctx ts = Just $ txHashToHex <$> ts
  parseParam net ctx = mapM hexToTxHash
