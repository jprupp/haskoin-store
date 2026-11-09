{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -Wno-deprecations #-}

-- |
-- Module      : Haskoin.Store.Web
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- REST API server of Haskoin Store, built with Scotty.
--
-- 'runWeb' serves the endpoints described in "Haskoin.Store.WebCommon" and a
-- set of endpoints compatible with the Blockchain.info API under
-- @\/blockchain\/@. Responses are JSON by default, pretty-printed with
-- @?pretty=true@, or binary if the request has the header
-- @Accept: application\/octet-stream@. New blocks and mempool transactions are
-- streamed at @\/events@, as lines of JSON or over a WebSocket.
--
-- Limits on the size of responses are set with 'WebLimits'. Errors are
-- returned as JSON with an HTTP status that depends on the 'Except'.
-- = Design of the API
--
-- * __Two encodings of the same data.__ JSON is easy to use from anywhere. The
--   binary encoding is several times smaller and faster to produce and parse, and
--   is what the Haskell client uses.
--
-- * __Limits protect the server.__ A single request for every transaction of a
--   busy address could take minutes and gigabytes. 'WebLimits' caps list sizes
--   and offsets, and lower caps apply to full transactions, which are much larger
--   than references.
--
-- * __Health is measured, not assumed.__ The store reports itself unhealthy when
--   it lags behind the network, stops receiving blocks or transactions, has too
--   few peers or a backlog of transactions. Load balancers can route around an
--   instance that is still syncing or has lost its peers.
--
-- * __Slow event clients are dropped.__ Each @\/events@ client gets a bounded
--   queue. A client that falls too far behind is disconnected rather than allowed
--   to make the server buffer events without limit.
--
-- * __Compatibility endpoints ease migration.__ The @\/blockchain\/@ endpoints
--   follow the Blockchain.info API so that existing wallets can switch to a
--   self-hosted server with few changes.
--
-- * __Handlers take their state as an argument.__ Every handler runs in
--   Scotty's 'ActionT' over 'IO' and gets the 'WebState' as its first
--   argument, with the queries to answer with in 'query' ('webQuery': the
--   database, with the cache in front of the queries about xpubs).
--   @nocache=true@ is the same handler given a state whose queries read only
--   the database ('cacheOr'). A reader monad under 'ActionT' would hide the
--   state, and made every query a method of a class with an instance per
--   layer of the stack.
module Haskoin.Store.Web
  ( -- * Web
    WebConfig (..),
    Except (..),
    WebLimits (..),
    runWeb,
    runWebSocket,

    -- * Request parsing
    decodeTxBody,
  )
where

import Conduit
  ( ConduitT,
    await,
    concatMapC,
    concatMapMC,
    dropC,
    dropWhileC,
    headC,
    lengthC,
    mapC,
    runConduit,
    sinkList,
    takeC,
    takeWhileC,
    yield,
    (.|),
  )
import Control.Applicative ((<|>))
import Control.Lens ((.~), (^.))
import Control.Monad
  ( forM_,
    unless,
    void,
    when,
    (<=<),
  )
import Control.Monad.Trans (lift)
import Data.Aeson
  ( Encoding,
    ToJSON (..),
    Value,
  )
import Data.Aeson qualified as A
import Data.Aeson.Encode.Pretty
  ( Config (..),
    defConfig,
    encodePretty',
  )
import Data.Aeson.Encoding
  ( encodingToLazyByteString,
    list,
  )
import Data.ByteString.Base16 (decodeBase16, parseBase16)
import Data.ByteString.Builder (lazyByteString)
import Data.ByteString.Char8 qualified as C
import Data.ByteString.Lazy qualified as L
import Data.Bytes.Get
import Data.Bytes.Put
import Data.Bytes.Serial
import Data.Char (isSpace)
import Data.Default (Default (..))
import Data.Function ((&))
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.Int (Int64)
import Data.List (nub)
import Data.Maybe
  ( catMaybes,
    fromJust,
    fromMaybe,
    isJust,
    mapMaybe,
    maybeToList,
  )
import Data.Proxy (Proxy (..))
import Data.Serialize (decode)
import Data.String (fromString)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Text.Lazy qualified as TL
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word32, Word64)
import Database.RocksDB
  ( Property (..),
    getProperty,
  )
import Haskoin.Address
import Haskoin.Block qualified as H
import Haskoin.Crypto.Keys
import Haskoin.Network
import Haskoin.Node
  ( Chain,
    Logger,
    OnlinePeer (..),
    PeerMgr,
    chainGetAncestor,
    chainGetBest,
    getPeers,
    logDebug,
    logError,
    sendMessage,
    withSource,
  )
import Haskoin.Script
import Haskoin.Store.BlockStore
import Haskoin.Store.Cache
import Haskoin.Store.Common hiding
  ( blockAtOrAfter,
    blockAtOrAfterMTP,
    blockAtOrBefore,
    getActiveBlock,
    getActiveTxData,
    getDefaultBalance,
    getNumTransaction,
    getTransaction,
    getTransactions,
  )
import Haskoin.Store.Common qualified as C
import Haskoin.Store.Data
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
import Haskoin.Store.Manager
import Haskoin.Store.WebCommon
import Haskoin.Transaction
import Haskoin.Util
import NQE
  ( Publisher,
    mailboxFullSTM,
    newBoundedInbox,
    receiveSTM,
    sendSTM,
    subscribe,
    unsubscribe,
  )
import Network.HTTP.Types
  ( Status (..),
    status400,
    status404,
    status409,
    status413,
    status500,
    status503,
    statusIsClientError,
    statusIsServerError,
    statusIsSuccessful,
  )
import Network.Socket (Socket)
import Network.Wai
  ( Middleware,
    Request (..),
    Response,
    responseStatus,
  )
import Network.Wai.Handler.Warp
  ( defaultSettings,
    setHost,
    setPort,
  )
import Network.Wai.Handler.WebSockets (websocketsOr)
import Network.WebSockets
  ( acceptRequest,
    defaultConnectionOptions,
    pendingRequest,
    rejectRequestWith,
    requestPath,
    sendTextData,
  )
import Network.WebSockets qualified as WebSockets
import Network.WebSockets.Connection.PingPong
  ( PingPongOptions (..),
    defaultPingPongOptions,
    withPingPong,
  )
import Network.Wreq qualified as Wreq
import Network.Wreq.Session as Wreq (Session)
import Network.Wreq.Session qualified as Wreq.Session
import Numeric.Natural (Natural)
import System.Metrics.StatsD
import UnliftIO
  ( TVar,
    async,
    atomically,
    bracket,
    bracket_,
    checkSTM,
    handleAny,
    liftIO,
    newTVarIO,
    orElse,
    race_,
    readTVar,
    readTVarIO,
    withAsync,
    withRunInIO,
    writeTVar,
  )
import UnliftIO.Concurrent (threadDelay)
import Web.Scotty.Trans qualified as S

-- | Scotty application. Handlers get the 'WebState' as an argument.
type ScottyT = S.ScottyT IO

-- | Scotty request handler.
type ActionT = S.ActionT IO

-- | Limits of the API. A value of @0@ means no limit.
data WebLimits = WebLimits
  { -- | maximum number of items in a list response
    maxItemCount :: !Word32,
    -- | maximum number of full transactions in a list response
    maxFullItemCount :: !Word32,
    -- | maximum offset of a paged query
    maxOffset :: !Word32,
    -- | number of items in a list response when no limit is given
    defItemCount :: !Word32,
    -- | consecutive unused addresses after which xpub derivation stops
    xpubGap :: !Word32,
    -- | unused addresses to look at for an xpub with no transactions
    xpubGapInit :: !Word32,
    -- | seconds without a new transaction before the store is reported
    -- unhealthy
    txTimeout :: !Word64,
    -- | seconds without a new block before the store is reported unhealthy
    blockTimeout :: !Word64
  }
  deriving (Eq, Show)

-- | 200000 items, 5000 full transactions, offset up to 50000, 100 items by
-- default, xpub gaps of 32 and 20, transaction timeout of 30 minutes and
-- block timeout of four hours.
instance Default WebLimits where
  def =
    WebLimits
      { maxItemCount = 200000,
        maxFullItemCount = 5000,
        maxOffset = 50000,
        defItemCount = 100,
        xpubGap = 32,
        xpubGapInit = 20,
        txTimeout = 3600 `div` 2,
        blockTimeout = 4 * 3600
      }

-- | Configuration of the web server.
data WebConfig = WebConfig
  { -- | address to listen on, such as @*@ for all
    host :: !String,
    -- | port to listen on
    port :: !Int,
    -- | running store to serve
    store :: !Store,
    -- | blocks that the database may lag behind the headers before the store
    -- is reported unhealthy
    maxLaggingBlocks :: !Int,
    -- | transactions that may be waiting to be imported before the store is
    -- reported unhealthy
    maxPendingTxs :: !Int,
    -- | fewest peers before the store is reported unhealthy
    minPeers :: !Int,
    -- | limits of the API
    limits :: !WebLimits,
    -- | version reported by the health check
    version :: !String,
    -- | the mempool is not indexed, so do not report it in health checks
    noMempool :: !Bool,
    -- | StatsD client, if metrics are enabled
    stats :: !(Maybe Stats),
    -- | microseconds between updates of the price ticker
    tickerRefresh :: !Int,
    -- | URL of the price ticker service
    tickerURL :: !String,
    -- | URL of the historical price service
    priceHistoryURL :: !String,
    -- | disable the xpub endpoints
    noXPub :: !Bool,
    -- | disable the Blockchain.info compatible endpoints
    noBlockchainInfo :: !Bool,
    -- | seconds between health checks
    healthCheckInterval :: !Int,
    -- | log messages here
    logger :: !Logger
  }

-- | State shared by all request handlers.
data WebState = WebState
  { -- | configuration
    config :: !WebConfig,
    -- | latest prices by currency code
    ticker :: !(TVar (HashMap Text BinfoTicker)),
    -- | StatsD metrics, if enabled
    metrics :: !(Maybe WebMetrics),
    -- | HTTP session for price requests
    session :: !Wreq.Session,
    -- | result of the latest health check
    health :: !(TVar HealthCheck),
    -- | the queries to answer with, the cache included ('webQuery')
    query :: !StoreRead
  }

-- | StatsD metrics of the web server: a timing for each kind of request,
-- under @web.\<name\>@, error counters and a gauge of connected event
-- clients.
data WebMetrics = WebMetrics
  { -- Addresses
    addressTx :: !StatTiming,
    addressTxFull :: !StatTiming,
    addressBalance :: !StatTiming,
    addressUnspent :: !StatTiming,
    xpub :: !StatTiming,
    xpubDelete :: !StatTiming,
    xpubTxFull :: !StatTiming,
    xpubTx :: !StatTiming,
    xpubBalance :: !StatTiming,
    xpubUnspent :: !StatTiming,
    -- Transactions
    tx :: !StatTiming,
    txRaw :: !StatTiming,
    txAfter :: !StatTiming,
    txBlock :: !StatTiming,
    txBlockRaw :: !StatTiming,
    txPost :: !StatTiming,
    mempool :: !StatTiming,
    -- Blocks
    block :: !StatTiming,
    blockRaw :: !StatTiming,
    -- Blockchain
    binfoMultiaddr :: !StatTiming,
    binfoBalance :: !StatTiming,
    binfoAddressRaw :: !StatTiming,
    binfoUnspent :: !StatTiming,
    binfoTxRaw :: !StatTiming,
    binfoBlock :: !StatTiming,
    binfoBlockHeight :: !StatTiming,
    binfoBlockLatest :: !StatTiming,
    binfoBlockRaw :: !StatTiming,
    binfoMempool :: !StatTiming,
    binfoExportHistory :: !StatTiming,
    -- Blockchain /q endpoints
    binfoQaddresstohash :: !StatTiming,
    binfoQhashtoaddress :: !StatTiming,
    binfoQaddrpubkey :: !StatTiming,
    binfoQpubkeyaddr :: !StatTiming,
    binfoQhashpubkey :: !StatTiming,
    binfoQgetblockcount :: !StatTiming,
    binfoQlatesthash :: !StatTiming,
    binfoQbcperblock :: !StatTiming,
    binfoQtxtotalbtcoutput :: !StatTiming,
    binfoQtxtotalbtcinput :: !StatTiming,
    binfoQtxfee :: !StatTiming,
    binfoQtxresult :: !StatTiming,
    binfoQgetreceivedbyaddress :: !StatTiming,
    binfoQgetsentbyaddress :: !StatTiming,
    binfoQaddressbalance :: !StatTiming,
    binfoQaddressfirstseen :: !StatTiming,
    -- Errors
    serverErrors :: StatCounter,
    clientErrors :: StatCounter,
    -- Others
    health :: !StatTiming,
    peers :: !StatTiming,
    db :: !StatTiming,
    events :: !StatGauge
  }

-- | Register the web server metrics with a StatsD client.
createMetrics :: Stats -> IO WebMetrics
createMetrics s = liftIO $ do
  -- Addresses
  addressTx <- d "address_transactions"
  addressTxFull <- d "address_transactions_full"
  addressBalance <- d "address_balance"
  addressUnspent <- d "address_unspent"
  xpub <- d "xpub"
  xpubDelete <- d "xpub_delete"
  xpubTxFull <- d "xpub_transactions_full"
  xpubTx <- d "xpub_transactions"
  xpubBalance <- d "xpub_balances"
  xpubUnspent <- d "xpub_unspent"

  -- Transactions
  tx <- d "transaction"
  txRaw <- d "transaction_raw"
  txAfter <- d "transaction_after"
  txPost <- d "transaction_post"
  txBlock <- d "transactions_block"
  txBlockRaw <- d "transactions_block_raw"
  mempool <- d "mempool"

  -- Blocks
  block <- d "block"
  blockRaw <- d "block_raw"

  -- Blockchain
  binfoMultiaddr <- d "blockchain_multiaddr"
  binfoBalance <- d "blockchain_balance"
  binfoAddressRaw <- d "blockchain_rawaddr"
  binfoUnspent <- d "blockchain_unspent"
  binfoTxRaw <- d "blockchain_rawtx"
  binfoBlock <- d "blockchain_blocks"
  binfoBlockHeight <- d "blockchain_block_height"
  binfoBlockLatest <- d "blockchain_latestblock"
  binfoBlockRaw <- d "blockchain_rawblock"
  binfoMempool <- d "blockchain_mempool"
  binfoExportHistory <- d "blockchain_export_history"

  -- Blockchain /q endpoints
  binfoQaddresstohash <- d "blockchain_q_addresstohash"
  binfoQhashtoaddress <- d "blockchain_q_hashtoaddress"
  binfoQaddrpubkey <- d "blockckhain_q_addrpubkey"
  binfoQpubkeyaddr <- d "blockchain_q_pubkeyaddr"
  binfoQhashpubkey <- d "blockchain_q_hashpubkey"
  binfoQgetblockcount <- d "blockchain_q_getblockcount"
  binfoQlatesthash <- d "blockchain_q_latesthash"
  binfoQbcperblock <- d "blockchain_q_bcperblock"
  binfoQtxtotalbtcoutput <- d "blockchain_q_txtotalbtcoutput"
  binfoQtxtotalbtcinput <- d "blockchain_q_txtotalbtcinput"
  binfoQtxfee <- d "blockchain_q_txfee"
  binfoQtxresult <- d "blockchain_q_txresult"
  binfoQgetreceivedbyaddress <- d "blockchain_q_getreceivedbyaddress"
  binfoQgetsentbyaddress <- d "blockchain_q_getsentbyaddress"
  binfoQaddressbalance <- d "blockchain_q_addressbalance"
  binfoQaddressfirstseen <- d "blockchain_q_addressfirstseen"

  -- Errors
  serverErrors <- c "server_errors"
  clientErrors <- c "client_errors"

  -- Others
  health <- d "health"
  peers <- d "peers"
  db <- d "dbstats"

  events <- g "events_connected"
  return WebMetrics {..}
  where
    d x = newStatTiming s ("web." <> x) 10
    g x = newStatGauge s ("web." <> x) 0
    c x = newStatCounter s ("web." <> x) 10

-- | Increase a gauge while an action runs.
withGaugeIO :: StatGauge -> IO a -> IO a
withGaugeIO g = bracket_ (incrementGauge g 1) (decrementGauge g 1)

-- | Increase a gauge of the web metrics while a handler runs.
withGaugeIncrease ::
  WebState ->
  (WebMetrics -> StatGauge) ->
  ActionT a ->
  ActionT a
withGaugeIncrease st gf go =
  case st.metrics of
    Nothing -> go
    Just m -> withRunInIO $ \run -> withGaugeIO (gf m) (run go)

-- | Record how long a handler takes in a timing of the web metrics.
withMetrics ::
  WebState ->
  (WebMetrics -> StatTiming) ->
  ActionT a ->
  ActionT a
withMetrics st df go =
  case st.metrics of
    Nothing -> go
    Just m -> withRunInIO $ \run -> bracket time (stop m) (const (run go))
  where
    time = round . (* 1000) <$> getPOSIXTime
    stop m t1 = do
      t2 <- time
      addTiming (df m) (t2 - t1)

-- | Encoding of a response.
data SerialAs
  = -- | binary, for @Accept: application\/octet-stream@
    SerialAsBinary
  | -- | compact JSON
    SerialAsJSON
  | -- | indented JSON, for @?pretty=true@
    SerialAsPrettyJSON
  deriving (Eq, Show)

-------------------
-- Path Handlers --
-------------------

-- | Run the web server forever on the host and port of the configuration.
-- Also starts a thread that updates prices, unless the Blockchain.info
-- endpoints are disabled, and a thread that runs health checks.
runWeb :: WebConfig -> IO ()
runWeb config = serveWeb config (S.scottyOptsT opts)
  where
    opts = S.defaultOptions {S.settings = settings defaultSettings}
    settings = setPort config.port . setHost (fromString config.host)

-- | Like 'runWeb', on a socket that already listens; the host and port of the
-- configuration are ignored. Binding port 0 and passing the socket gives a
-- free port with no window in which another program can take it, which
-- finding a free port first and binding it later has.
runWebSocket :: WebConfig -> Socket -> IO ()
runWebSocket config sock = serveWeb config (S.scottySocketT S.defaultOptions sock)

-- | Run the web server with a function of Scotty that serves it.
serveWeb ::
  WebConfig ->
  ( (IO Response -> IO Response) ->
    ScottyT () ->
    IO ()
  ) ->
  IO ()
serveWeb config serve = do
  ticker <- newTVarIO HashMap.empty
  metrics <- mapM createMetrics config.stats
  session <- Wreq.Session.newAPISession
  health <- runHealthCheck >>= newTVarIO
  let query = webQuery config.store
      st = WebState {..}
  withAsync (priceUpdater session ticker) $ \_a1 ->
    withAsync (healthCheckLoop health) $ \_a2 ->
      serve id $ do
        S.middleware $ webSocketEvents st
        S.middleware (logIt config.logger)
        handlePaths st config
        S.notFound $ raise st ThingNotFound
  where
    priceUpdater session =
      unless config.noBlockchainInfo
        . price
          (withSource "Price" config.logger)
          config.store.net
          session
          config.tickerURL
          config.tickerRefresh
    runHealthCheck = healthCheck config
    healthCheckLoop v = loopForever $ do
      threadDelay (config.healthCheckInterval * 1000 * 1000)
      runHealthCheck >>= atomically . writeTVar v

-- | Historical prices of the coin of the network in a currency at several times. Returns an empty list on failure.
getRates ::
  Logger ->
  Network ->
  Wreq.Session ->
  String ->
  Text ->
  [Word64] ->
  IO [BinfoRate]
getRates logger net session url currency times = do
  handleAny err $ do
    r <-
      liftIO $
        Wreq.asJSON
          =<< Wreq.Session.postWith opts session url body
    return $ r ^. Wreq.responseBody
  where
    err _ = do
      liftIO $ logError logger "Could not get historic prices"
      return []
    body = toJSON times
    base =
      Wreq.defaults
        & Wreq.param "base" .~ [T.toUpper $ T.pack net.name]
    opts = base & Wreq.param "quote" .~ [currency]

-- | Update the prices of the coin of the network periodically, for BTC and BCH only.
price ::
  Logger ->
  Network ->
  Wreq.Session ->
  String ->
  Int ->
  TVar (HashMap Text BinfoTicker) ->
  IO ()
price logger net session url pget v = forM_ purl $ \u -> loopForever $ do
  let err e = liftIO . logError logger $ cs (show e)
  handleAny err $ do
    r <- liftIO $ Wreq.asJSON =<< Wreq.Session.get session u
    atomically . writeTVar v $ r ^. Wreq.responseBody
  threadDelay pget
  where
    purl = case code of
      Nothing -> Nothing
      Just x -> Just (url <> "?base=" <> x)
      where
        code
          | net == btc = Just "btc"
          | net == bch = Just "bch"
          | otherwise = Nothing

-- | Respond with an error and stop handling the request.
raise :: WebState -> Except -> ActionT a
raise st e = do
  pure st.metrics >>= mapM_ \m -> do
    liftIO $
      mapM_ (`incrementCounter` 1) $
        if
          | statusIsClientError (errStatus e) -> Just m.clientErrors
          | statusIsServerError (errStatus e) -> Just m.serverErrors
          | otherwise -> Nothing
  setHeaders
  S.status $ errStatus e
  S.json e
  S.finish

-- | HTTP status of an error.
errStatus :: Except -> Status
errStatus ThingNotFound = status404
errStatus BadRequest = status400
errStatus UserError {} = status400
errStatus StringError {} = status400
errStatus ServerError = status500
errStatus TxIndexConflict {} = status409
errStatus ServerTimeout = status500
errStatus RequestTooLarge = status413

-- | Register the handlers of every endpoint, except those disabled in the configuration.
handlePaths ::
  WebState ->
  WebConfig ->
  ScottyT ()
handlePaths st cfg = do
  -- Block Paths
  pathCompact
    (GetBlockBest <$> paramDef st)
    (scottyBlockBest st)
    (marshalEncoding net)
    (marshalValue net)
  pathCompact
    (GetBlockHeight <$> paramCapture st <*> paramDef st)
    (fmap SerialList . scottyBlockHeight st)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetBlockTime <$> paramCapture st <*> paramDef st)
    (scottyBlockTime st)
    (marshalEncoding net)
    (marshalValue net)
  pathCompact
    (GetBlockMTP <$> paramCapture st <*> paramDef st)
    (scottyBlockMTP st)
    (marshalEncoding net)
    (marshalValue net)
  pathCompact
    (GetBlock <$> paramCapture st <*> paramDef st)
    (scottyBlock st)
    (marshalEncoding net)
    (marshalValue net)
  pathCompact
    (GetTx <$> paramCapture st)
    (scottyTx st)
    (marshalEncoding net)
    (marshalValue net)
  pathCompact
    (GetTxRaw <$> paramCapture st)
    (scottyTxRaw st)
    toEncoding
    toJSON
  pathCompact
    (PostTx <$> parseTxBody st)
    (scottyPostTx st)
    toEncoding
    toJSON
  pathCompact
    (GetAddrTxs <$> paramCapture st <*> parseLimits st)
    (fmap SerialList . scottyAddrTxs st)
    toEncoding
    toJSON
  pathCompact
    (GetAddrBalance <$> paramCapture st)
    (scottyAddrBalance st)
    (marshalEncoding net)
    (marshalValue net)
  pathCompact
    (GetAddrUnspent <$> paramCapture st <*> parseLimits st)
    (fmap SerialList . scottyAddrUnspent st)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetPeers & return)
    (fmap SerialList . scottyPeers st)
    toEncoding
    toJSON
  pathCompact
    (GetHealth & return)
    (scottyHealth st)
    toEncoding
    toJSON
  pathCompact
    (GetMempool <$> paramOptional st <*> parseOffset st)
    (fmap SerialList . scottyMempool st)
    toEncoding
    toJSON
  pathCompact
    (GetBlocks <$> paramRequired st <*> paramDef st)
    (fmap SerialList . scottyBlocks st)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetBlockRaw <$> paramCapture st)
    (scottyBlockRaw st)
    toEncoding
    toJSON
  pathCompact
    (GetBlockBestRaw & return)
    (scottyBlockBestRaw st)
    toEncoding
    toJSON
  pathCompact
    (GetBlockLatest <$> paramDef st)
    (fmap SerialList . scottyBlockLatest st)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetBlockHeights <$> paramRequired st <*> paramDef st)
    (fmap SerialList . scottyBlockHeights st)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetBlockHeightRaw <$> paramCapture st)
    (scottyBlockHeightRaw st)
    toEncoding
    toJSON
  pathCompact
    (GetBlockTimeRaw <$> paramCapture st)
    (scottyBlockTimeRaw st)
    toEncoding
    toJSON
  pathCompact
    (GetBlockMTPRaw <$> paramCapture st)
    (scottyBlockMTPRaw st)
    toEncoding
    toJSON
  pathCompact
    (GetTxs <$> paramRequired st)
    (fmap SerialList . scottyTxs st)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetTxsRaw <$> paramRequired st)
    (scottyTxsRaw st)
    toEncoding
    toJSON
  pathCompact
    (GetTxsBlock <$> paramCapture st)
    (fmap SerialList . scottyTxsBlock st)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetTxsBlockRaw <$> paramCapture st)
    (scottyTxsBlockRaw st)
    toEncoding
    toJSON
  pathCompact
    (GetTxAfter <$> paramCapture st <*> paramCapture st)
    (scottyTxAfter st)
    toEncoding
    toJSON
  pathCompact
    (GetAddrsTxs <$> paramRequired st <*> parseLimits st)
    (fmap SerialList . scottyAddrsTxs st)
    toEncoding
    toJSON
  pathCompact
    (GetAddrTxsFull <$> paramCapture st <*> parseLimits st)
    (fmap SerialList . scottyAddrTxsFull st)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetAddrsTxsFull <$> paramRequired st <*> parseLimits st)
    (fmap SerialList . scottyAddrsTxsFull st)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetAddrsBalance <$> paramRequired st)
    (fmap SerialList . scottyAddrsBalance st)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetAddrsUnspent <$> paramRequired st <*> parseLimits st)
    (fmap SerialList . scottyAddrsUnspent st)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  S.get "/events" (scottyEvents st)
  S.get "/dbstats" (scottyDbStats st)
  unless cfg.noXPub $ do
    pathCompact
      (GetXPub <$> paramCapture st <*> paramDef st <*> paramDef st)
      (scottyXPub st)
      toEncoding
      toJSON
    pathCompact
      (GetXPubTxs <$> paramCapture st <*> paramDef st <*> parseLimits st <*> paramDef st)
      (fmap SerialList . scottyXPubTxs st)
      toEncoding
      toJSON
    pathCompact
      (GetXPubTxsFull <$> paramCapture st <*> paramDef st <*> parseLimits st <*> paramDef st)
      (fmap SerialList . scottyXPubTxsFull st)
      (list (marshalEncoding net) . (.get))
      (json_list (marshalValue net) . (.get))
    pathCompact
      (GetXPubBalances <$> paramCapture st <*> paramDef st <*> paramDef st)
      (fmap SerialList . scottyXPubBalances st)
      (list (marshalEncoding net) . (.get))
      (json_list (marshalValue net) . (.get))
    pathCompact
      (GetXPubUnspent <$> paramCapture st <*> paramDef st <*> parseLimits st <*> paramDef st)
      (fmap SerialList . scottyXPubUnspent st)
      (list (marshalEncoding net) . (.get))
      (json_list (marshalValue net) . (.get))
    pathCompact
      (DelCachedXPub <$> paramCapture st <*> paramDef st)
      (scottyDelXPub st)
      toEncoding
      toJSON
  unless cfg.noBlockchainInfo $ do
    S.post "/blockchain/multiaddr" (scottyMultiAddr st)
    S.get "/blockchain/multiaddr" (scottyMultiAddr st)
    S.get "/blockchain/balance" (scottyShortBal st)
    S.post "/blockchain/balance" (scottyShortBal st)
    S.get "/blockchain/rawaddr/:addr" (scottyRawAddr st)
    S.get "/blockchain/address/:addr" (scottyRawAddr st)
    S.get "/blockchain/xpub/:addr" (scottyRawAddr st)
    S.post "/blockchain/unspent" (scottyBinfoUnspent st)
    S.get "/blockchain/unspent" (scottyBinfoUnspent st)
    S.get "/blockchain/rawtx/:txid" (scottyBinfoTx st)
    S.get "/blockchain/rawblock/:block" (scottyBinfoBlock st)
    S.get "/blockchain/latestblock" (scottyBinfoLatest st)
    S.get "/blockchain/unconfirmed-transactions" (scottyBinfoMempool st)
    S.get "/blockchain/block-height/:height" (scottyBinfoBlockHeight st)
    S.get "/blockchain/blocks/:milliseconds" (scottyBinfoBlocksDay st)
    S.get "/blockchain/export-history" (scottyBinfoHistory st)
    S.post "/blockchain/export-history" (scottyBinfoHistory st)
    S.get "/blockchain/q/addresstohash/:addr" (scottyBinfoAddrToHash st)
    S.get "/blockchain/q/hashtoaddress/:hash" (scottyBinfoHashToAddr st)
    S.get "/blockchain/q/addrpubkey/:pubkey" (scottyBinfoAddrPubkey st)
    S.get "/blockchain/q/pubkeyaddr/:addr" (scottyBinfoPubKeyAddr st)
    S.get "/blockchain/q/hashpubkey/:pubkey" (scottyBinfoHashPubkey st)
    S.get "/blockchain/q/getblockcount" (scottyBinfoGetBlockCount st)
    S.get "/blockchain/q/latesthash" (scottyBinfoLatestHash st)
    S.get "/blockchain/q/bcperblock" (scottyBinfoSubsidy st)
    S.get "/blockchain/q/txtotalbtcoutput/:txid" (scottyBinfoTotalOut st)
    S.get "/blockchain/q/txtotalbtcinput/:txid" (scottyBinfoTotalInput st)
    S.get "/blockchain/q/txfee/:txid" (scottyBinfoTxFees st)
    S.get "/blockchain/q/txresult/:txid/:addr" (scottyBinfoTxResult st)
    S.get "/blockchain/q/getreceivedbyaddress/:addr" (scottyBinfoReceived st)
    S.get "/blockchain/q/getsentbyaddress/:addr" (scottyBinfoSent st)
    S.get "/blockchain/q/addressbalance/:addr" (scottyBinfoAddrBalance st)
    S.get "/blockchain/q/addressfirstseen/:addr" (scottyFirstSeen st)
  where
    json_list f = toJSONList . map f
    net = cfg.store.net

-- | Register a handler for an API resource, with compact JSON by default.
pathCompact ::
  (ApiResource a b) =>
  ActionT a ->
  (a -> ActionT b) ->
  (b -> Encoding) ->
  (b -> Value) ->
  ScottyT ()
pathCompact parser action encJson encValue =
  pathCommon parser action encJson encValue False

-- | Register a handler for an API resource: parse the request, run the handler,
-- and encode the response as JSON or binary, as the client asked.
pathCommon ::
  (ApiResource a b) =>
  ActionT a ->
  (a -> ActionT b) ->
  (b -> Encoding) ->
  (b -> Value) ->
  Bool ->
  ScottyT ()
pathCommon parser action encJson encValue pretty =
  S.addroute (resourceMethod proxy) (capturePath proxy) $ do
    setHeaders
    proto <- setupContentType pretty
    apiRes <- parser
    res <- action apiRes
    S.raw $ protoSerial proto encJson encValue res
  where
    toProxy :: ActionT a -> Proxy a
    toProxy = const Proxy
    proxy = toProxy parser

-- | Respond with JSON.
streamEncoding :: Encoding -> ActionT ()
streamEncoding e = do
  S.setHeader "Content-Type" "application/json; charset=utf-8"
  S.raw (encodingToLazyByteString e)

-- | Encode a response in binary, JSON or pretty JSON.
protoSerial ::
  (Serial a) =>
  SerialAs ->
  (a -> Encoding) ->
  (a -> Value) ->
  a ->
  L.ByteString
protoSerial SerialAsBinary _ _ = runPutL . serialize
protoSerial SerialAsJSON f _ = encodingToLazyByteString . f
protoSerial SerialAsPrettyJSON _ g =
  encodePretty' defConfig {confTrailingNewline = True} . g

-- | Allow requests from any origin.
setHeaders :: ActionT ()
setHeaders = S.setHeader "Access-Control-Allow-Origin" "*"

-- | Respond with JSON, pretty if the @pretty@ parameter or the default says so.
setupJSON :: Bool -> ActionT SerialAs
setupJSON pretty = do
  S.setHeader "Content-Type" "application/json"
  p <- param "pretty" `rescue` return pretty
  return $ if p then SerialAsPrettyJSON else SerialAsJSON

-- | Respond with binary.
setupBinary :: ActionT SerialAs
setupBinary = do
  S.setHeader "Content-Type" "application/octet-stream"
  return SerialAsBinary

-- | Choose the encoding of the response from the @Accept@ header of the request.
setupContentType :: Bool -> ActionT SerialAs
setupContentType pretty = do
  accept <- S.header "accept"
  maybe (setupJSON pretty) setType accept
  where
    setType "application/octet-stream" = setupBinary
    setType _ = setupJSON pretty

-- GET Block / GET Blocks --

-- | Handler for @GET \/block\/:block@: a block by hash.
scottyBlock ::
  WebState ->
  GetBlock ->
  ActionT BlockData
scottyBlock st (GetBlock h (NoTx noTx)) = withMetrics st (.block) $ do
  getBlock st h >>= \case
    Nothing ->
      raise st ThingNotFound
    Just b -> do
      return $ pruneTx noTx b

-- | Blocks by hash, without duplicates, leaving out unknown ones.
getBlocks ::
  WebState ->
  [H.BlockHash] ->
  Bool ->
  ActionT [BlockData]
getBlocks st hs notx =
  (pruneTx notx <$>) . catMaybes <$> mapM (getBlock st) (nub hs)

-- | Handler for @GET \/blocks?blocks=…@: several blocks by hash.
scottyBlocks ::
  WebState ->
  GetBlocks ->
  ActionT [BlockData]
scottyBlocks st (GetBlocks hs (NoTx notx)) =
  withMetrics st (.block) $
    getBlocks st hs notx

-- | Keep only the coinbase in the list of transactions of a block, if the flag is 'True'.
pruneTx :: Bool -> BlockData -> BlockData
pruneTx False b = b
pruneTx True BlockData {..} = BlockData {txs = take 1 txs, ..}

-- GET BlockRaw --

-- | Handler for @GET \/block\/:block\/raw@: a block by hash, serialized as in the peer-to-peer protocol.
scottyBlockRaw ::
  WebState ->
  GetBlockRaw ->
  ActionT (RawResult H.Block)
scottyBlockRaw st (GetBlockRaw h) =
  withMetrics st (.blockRaw) $
    RawResult <$> getRawBlock st h

-- | Block by hash with all its transactions. Responds with an error if not found.
getRawBlock ::
  WebState ->
  H.BlockHash ->
  ActionT H.Block
getRawBlock st h =
  getBlock st h >>= maybe (raise st ThingNotFound) return >>= lift . toRawBlock st.query

-- | Rebuild a block from its data and its transactions.
toRawBlock :: StoreRead -> BlockData -> IO H.Block
toRawBlock q b = do
  txs <- map transactionData . catMaybes <$> C.getTransactions q b.txs
  return H.Block {header = b.header, txs}

-- GET BlockBest / BlockBestRaw --

-- | Handler for @GET \/block\/best@: the best block.
scottyBlockBest ::
  WebState ->
  GetBlockBest ->
  ActionT BlockData
scottyBlockBest st (GetBlockBest (NoTx notx)) =
  withMetrics st (.block) $
    liftIO (bestBlock st.query) >>= maybe (raise st ThingNotFound) (return . pruneTx notx)

-- | Handler for @GET \/block\/best\/raw@: the best block, serialized.
scottyBlockBestRaw ::
  WebState ->
  GetBlockBestRaw ->
  ActionT (RawResult H.Block)
scottyBlockBestRaw st _ =
  withMetrics st (.blockRaw) $
    getBestBlock st >>= \case
      Nothing -> raise st ThingNotFound
      Just h -> RawResult <$> getRawBlock st h

-- GET BlockLatest --

-- | Handler for @GET \/block\/latest@: the best block and its ancestors, up to 100 blocks.
scottyBlockLatest ::
  WebState ->
  GetBlockLatest ->
  ActionT [BlockData]
scottyBlockLatest st (GetBlockLatest (NoTx noTx)) =
  withMetrics st (.block) $
    getBestBlock st >>= maybe (raise st ThingNotFound) (go [] <=< getBlock st)
  where
    go acc Nothing = return $ reverse acc
    go acc (Just b)
      | b.height <= 0 = return $ reverse acc
      | length acc == 99 = return . reverse $ pruneTx noTx b : acc
      | otherwise = go (pruneTx noTx b : acc) =<< getBlock st b.header.prev

-- GET BlockHeight / BlockHeights / BlockHeightRaw --

-- | Handler for @GET \/block\/height\/:height@: the blocks at a height.
scottyBlockHeight ::
  WebState ->
  GetBlockHeight ->
  ActionT [BlockData]
scottyBlockHeight st (GetBlockHeight h (NoTx notx)) =
  withMetrics st (.block) $
    getBlocksAtHeight st (fromIntegral h)
      >>= (\hs -> getBlocks st hs notx)

-- | Handler for @GET \/block\/heights?heights=…@: the blocks at several heights.
scottyBlockHeights ::
  WebState ->
  GetBlockHeights ->
  ActionT [BlockData]
scottyBlockHeights st (GetBlockHeights (HeightsParam heights) (NoTx notx)) =
  withMetrics st (.block) $
    mapM (getBlocksAtHeight st . fromIntegral) heights
      >>= (\hs -> getBlocks st hs notx) . concat

-- | Handler for @GET \/block\/height\/:height\/raw@: the blocks at a height, serialized.
scottyBlockHeightRaw ::
  WebState ->
  GetBlockHeightRaw ->
  ActionT (RawResultList H.Block)
scottyBlockHeightRaw st (GetBlockHeightRaw h) =
  withMetrics st (.blockRaw) $
    fmap RawResultList $
      mapM (getRawBlock st) =<< getBlocksAtHeight st (fromIntegral h)

-- GET BlockTime / BlockTimeRaw --

-- | Handler for @GET \/block\/time\/:time@: the last block with a timestamp at or before a time.
scottyBlockTime ::
  WebState ->
  GetBlockTime ->
  ActionT BlockData
scottyBlockTime st (GetBlockTime (TimeParam t) (NoTx notx)) =
  withMetrics st (.block) $ do
    let ch = st.config.store.chain
    blockAtOrBefore st ch t >>= \case
      Nothing -> raise st ThingNotFound
      Just b -> return $ pruneTx notx b

-- | Handler for @GET \/block\/mtp\/:time@: the first block with a median time past at or after a time.
scottyBlockMTP ::
  WebState ->
  GetBlockMTP ->
  ActionT BlockData
scottyBlockMTP st (GetBlockMTP (TimeParam t) (NoTx notx)) =
  withMetrics st (.block) $ do
    let ch = st.config.store.chain
    blockAtOrAfterMTP st ch t >>= \case
      Nothing -> raise st ThingNotFound
      Just b -> return $ pruneTx notx b

-- | Handler for @GET \/block\/time\/:time\/raw@: the last block with a timestamp at or before a time, serialized.
scottyBlockTimeRaw ::
  WebState ->
  GetBlockTimeRaw ->
  ActionT (RawResult H.Block)
scottyBlockTimeRaw st (GetBlockTimeRaw (TimeParam t)) =
  withMetrics st (.blockRaw) $ do
    let ch = st.config.store.chain
    blockAtOrBefore st ch t >>= \case
      Nothing -> raise st ThingNotFound
      Just b -> RawResult <$> lift (toRawBlock st.query b)

-- | Handler for @GET \/block\/mtp\/:time\/raw@: the first block with a median time past at or after a time, serialized.
scottyBlockMTPRaw ::
  WebState ->
  GetBlockMTPRaw ->
  ActionT (RawResult H.Block)
scottyBlockMTPRaw st (GetBlockMTPRaw (TimeParam t)) =
  withMetrics st (.blockRaw) $ do
    let ch = st.config.store.chain
    blockAtOrAfterMTP st ch t >>= \case
      Nothing -> raise st ThingNotFound
      Just b -> RawResult <$> lift (toRawBlock st.query b)

-- GET Transactions --

-- | Handler for @GET \/transaction\/:txid@: a transaction.
scottyTx :: WebState -> GetTx -> ActionT Transaction
scottyTx st (GetTx txid) =
  withMetrics st (.tx) $
    getTransaction st txid >>= maybe (raise st ThingNotFound) return

-- | Handler for @GET \/transactions?txids=…@: several transactions.
scottyTxs ::
  WebState ->
  GetTxs ->
  ActionT [Transaction]
scottyTxs st (GetTxs txids) =
  withMetrics st (.tx) $
    catMaybes <$> getTransactions st (nub txids)

-- | Handler for @GET \/transaction\/:txid\/raw@: a transaction, serialized.
scottyTxRaw ::
  WebState ->
  GetTxRaw ->
  ActionT (RawResult Tx)
scottyTxRaw st (GetTxRaw txid) =
  withMetrics st (.txRaw) $
    getTransaction st txid
      >>= \case
        Nothing -> raise st ThingNotFound
        Just tx -> return $ RawResult (transactionData tx)

-- | Handler for @GET \/transactions\/raw?txids=…@: several transactions, serialized.
scottyTxsRaw ::
  WebState ->
  GetTxsRaw ->
  ActionT (RawResultList Tx)
scottyTxsRaw st (GetTxsRaw txids) =
  withMetrics st (.txRaw) $
    RawResultList . map transactionData . catMaybes
      <$> getTransactions st (nub txids)

-- | Transactions of a block. Responds with an error if not found.
getTxsBlock ::
  WebState ->
  H.BlockHash ->
  ActionT [Transaction]
getTxsBlock st h =
  getBlock st h >>= \case
    Nothing -> raise st ThingNotFound
    Just b -> catMaybes <$> getTransactions st b.txs

-- | Handler for @GET \/transactions\/block\/:block@: the transactions of a block.
scottyTxsBlock ::
  WebState ->
  GetTxsBlock ->
  ActionT [Transaction]
scottyTxsBlock st (GetTxsBlock h) =
  withMetrics st (.txBlock) $ getTxsBlock st h

-- | Handler for @GET \/transactions\/block\/:block\/raw@: the transactions of a block, serialized.
scottyTxsBlockRaw ::
  WebState ->
  GetTxsBlockRaw ->
  ActionT (RawResultList Tx)
scottyTxsBlockRaw st (GetTxsBlockRaw h) =
  withMetrics st (.txBlockRaw) $
    RawResultList . map transactionData <$> getTxsBlock st h

-- GET TransactionAfterHeight --

-- | Handler for @GET \/transaction\/:txid\/after\/:height@: whether any ancestor of a transaction is a coinbase confirmed after a height.
scottyTxAfter ::
  WebState ->
  GetTxAfter ->
  ActionT (GenericResult (Maybe Bool))
scottyTxAfter st (GetTxAfter txid height) =
  withMetrics st (.txAfter) $
    GenericResult . fst <$> liftIO (cbAfterHeight st.query (fromIntegral height) txid)

-- | Check if any of the ancestors of this transaction is a coinbase after the
-- specified height. Returns 'Nothing' if answer cannot be computed before
-- hitting limits.
cbAfterHeight ::
  StoreRead ->
  H.BlockHeight ->
  TxHash ->
  IO (Maybe Bool, Int)
cbAfterHeight q height txid =
  inputs n HashSet.empty HashSet.empty [txid]
  where
    n = 10000
    inputs 0 _ _ [] = return (Nothing, 10000)
    inputs i is ns [] =
      let is' = HashSet.union is ns
          ns' = HashSet.empty
          ts = HashSet.toList (HashSet.difference ns is)
       in case ts of
            [] -> return (Just False, n - i)
            _ -> inputs i is' ns' ts
    inputs i is ns (t : ts) =
      C.getTransaction q t >>= \case
        Nothing -> return (Nothing, n - i)
        Just tx
          | height_check tx ->
              if cb_check tx
                then return (Just True, n - i + 1)
                else
                  let ns' = HashSet.union (ins tx) ns
                   in inputs (i - 1) is ns' ts
          | otherwise -> inputs (i - 1) is ns ts
    cb_check = any isCoinbase . (.inputs)
    ins = HashSet.fromList . map (.outpoint.hash) . (.inputs)
    height_check tx =
      case tx.block of
        BlockRef h _ -> h > height
        MemRef _ -> True

-- POST Transaction --

-- | Handler for @POST \/transactions@: broadcast a transaction to half of the peers and return its hash.
scottyPostTx :: WebState -> PostTx -> ActionT TxId
scottyPostTx st (PostTx tx) =
  withMetrics st (.txPost) $ do
    let cfg = st.config
    lift (publishTx cfg tx)
    return (TxId (txHash tx))

-- | Send transaction to all connected peers.
publishTx ::
  WebConfig ->
  Tx ->
  IO ()
publishTx cfg tx = do
  ps <- liftIO $ getPeers cfg.store.peerMgr
  let c = max 1 (length ps `div` 2)
  forM_ (take c ps) $ \p -> do
    liftIO $ sendMessage (MTx tx) p.mailbox
    void . async $ do
      threadDelay (5 * 1000 * 1000)
      let v = if cfg.store.net.segWit then InvWitnessTx else InvTx
          g = MGetData (GetData [InvVector v (txHash tx).get])
      liftIO $ sendMessage g p.mailbox

-- GET Mempool / Events --

-- | Handler for @GET \/mempool@: hashes of mempool transactions, newest first.
scottyMempool ::
  WebState ->
  GetMempool ->
  ActionT [TxHash]
scottyMempool st (GetMempool limitM (OffsetParam o)) =
  withMetrics st (.mempool) $ do
    WebLimits {..} <- pure st.config.limits
    let wl' = WebLimits {maxItemCount = 0, ..}
        l = Limits (validateLimit wl' False limitM) (fromIntegral o) Nothing
    map snd <$> getMempoolPage st l

-- | Middleware that serves @\/events@ over a WebSocket: each new block or mempool
-- transaction is sent as a JSON message. Clients that do not answer pings, or
-- that fall more than 10000 events behind, are disconnected.
webSocketEvents :: WebState -> Middleware
webSocketEvents s =
  websocketsOr defaultConnectionOptions (wrap . events)
  where
    pub = s.config.store.pub
    pingPong = defaultPingPongOptions {pingInterval = 30, pongTimeout = 60}
    gauge = (.events) <$> s.metrics
    wrap f = case gauge of
      Nothing -> f
      Just g -> withGaugeIO g f
    events pending = do
      let path = requestPath $ pendingRequest pending
      if path == "/events"
        then withEventSubscription pub $ \next -> do
          conn <- acceptRequest pending
          -- Drop clients that stop answering pings, and clients that fall
          -- so far behind that their event queue fills up.
          -- websockets only sees pongs while reading, hence the reader.
          withPingPong pingPong conn $ \c ->
            let send_events =
                  next >>= \case
                    Nothing ->
                      WebSockets.sendClose c ("Event queue full" :: Text)
                    Just event -> do
                      sendTextData c (A.encode event)
                      send_events
             in race_
                  (loopForever (WebSockets.receiveDataMessage c))
                  send_events
        else
          rejectRequestWith
            pending
            WebSockets.defaultRejectRequest
              { WebSockets.rejectBody =
                  L.toStrict $ A.encode ThingNotFound,
                WebSockets.rejectCode =
                  404,
                WebSockets.rejectMessage =
                  "Not Found",
                WebSockets.rejectHeaders =
                  [("Content-Type", "application/json")]
              }

-- | Handler for @GET \/events@: stream of new blocks and mempool transactions, one per line in JSON, until the client falls more than 10000 events behind.
scottyEvents :: WebState -> ActionT ()
scottyEvents st =
  withGaugeIncrease st (.events) $ do
    setHeaders
    proto <- setupContentType False
    let pub = st.config.store.pub
    S.stream $ \io flush' ->
      withEventSubscription pub $ \next ->
        -- ends the response when the client falls too far behind
        let send_events = do
              flush'
              next >>= \case
                Nothing -> return ()
                Just msg -> io (serial proto msg) >> send_events
         in send_events
  where
    serial proto e =
      lazyByteString $
        protoSerial proto toEncoding toJSON e <> newLine proto
    newLine SerialAsBinary = mempty
    newLine SerialAsJSON = "\n"
    newLine SerialAsPrettyJSON = mempty

-- | Most events queued for one /events client. A client that does not keep
-- up is disconnected once this many are waiting, instead of the queue
-- growing without bound.
eventQueueSize :: Natural
eventQueueSize = 10000

-- | Subscribe to the store events that /events clients see. The action gets
-- a blocking read that returns the next event, or 'Nothing' once the queue
-- has overflowed, after which the client must be disconnected. Other store
-- events are dropped by the publisher instead of being queued.
withEventSubscription ::
  Publisher StoreEvent -> (IO (Maybe Event) -> IO a) -> IO a
withEventSubscription pub f = do
  inbox <- newBoundedInbox eventQueueSize
  overflow <- newTVarIO False
  let listen e = forM_ (toEvent e) $ \ev ->
        mailboxFullSTM inbox >>= \case
          True -> writeTVar overflow True
          False -> sendSTM ev inbox
      next =
        atomically $
          (readTVar overflow >>= checkSTM >> return Nothing)
            `orElse` (Just <$> receiveSTM inbox)
  bracket (subscribe pub listen) (unsubscribe pub) $ \_ -> f next
  where
    toEvent = \case
      StoreBestBlock b -> Just (EventBlock b)
      StoreMempoolNew t -> Just (EventTx t)
      StoreMempoolDelete t -> Just (EventTx t)
      _ -> Nothing

-- GET Address Transactions --

-- | Handler for @GET \/address\/:address\/transactions@: transactions of an address.
scottyAddrTxs ::
  WebState ->
  GetAddrTxs ->
  ActionT [TxRef]
scottyAddrTxs st (GetAddrTxs addr pLimits) =
  withMetrics st (.addressTx) $
    getAddressTxs st addr =<< paramToLimits st False pLimits

-- | Handler for @GET \/address\/transactions?addresses=…@: transactions of several addresses.
scottyAddrsTxs ::
  WebState ->
  GetAddrsTxs ->
  ActionT [TxRef]
scottyAddrsTxs st (GetAddrsTxs addrs pLimits) =
  withMetrics st (.addressTx) $
    getAddressesTxs st addrs =<< paramToLimits st False pLimits

-- | Handler for @GET \/address\/:address\/transactions\/full@: full transactions of an address.
scottyAddrTxsFull ::
  WebState ->
  GetAddrTxsFull ->
  ActionT [Transaction]
scottyAddrTxsFull st (GetAddrTxsFull addr pLimits) =
  withMetrics st (.addressTxFull) $ do
    txs <- getAddressTxs st addr =<< paramToLimits st True pLimits
    catMaybes <$> getTransactions st (map (.txid) txs)

-- | Handler for @GET \/address\/transactions\/full?addresses=…@: full transactions of several addresses.
scottyAddrsTxsFull ::
  WebState ->
  GetAddrsTxsFull ->
  ActionT [Transaction]
scottyAddrsTxsFull st (GetAddrsTxsFull addrs pLimits) =
  withMetrics st (.addressTxFull) $ do
    txs <- getAddressesTxs st addrs =<< paramToLimits st True pLimits
    catMaybes <$> getTransactions st (map (.txid) txs)

-- | Handler for @GET \/address\/:address\/balance@: balance of an address.
scottyAddrBalance :: WebState -> GetAddrBalance -> ActionT Balance
scottyAddrBalance st (GetAddrBalance addr) =
  withMetrics st (.addressBalance) $ getDefaultBalance st addr

-- | Handler for @GET \/address\/balances?addresses=…@: balances of several addresses.
scottyAddrsBalance ::
  WebState ->
  GetAddrsBalance ->
  ActionT [Balance]
scottyAddrsBalance st (GetAddrsBalance addrs) =
  withMetrics st (.addressBalance) $ getBalances st addrs

-- | Handler for @GET \/address\/:address\/unspent@: unspent outputs of an address.
scottyAddrUnspent ::
  WebState ->
  GetAddrUnspent ->
  ActionT [Unspent]
scottyAddrUnspent st (GetAddrUnspent addr pLimits) =
  withMetrics st (.addressUnspent) $
    getAddressUnspents st addr =<< paramToLimits st False pLimits

-- | Handler for @GET \/address\/unspent?addresses=…@: unspent outputs of several addresses.
scottyAddrsUnspent ::
  WebState ->
  GetAddrsUnspent ->
  ActionT [Unspent]
scottyAddrsUnspent st (GetAddrsUnspent addrs pLimits) =
  withMetrics st (.addressUnspent) $
    getAddressesUnspents st addrs =<< paramToLimits st False pLimits

-- GET XPubs --

-- | Handler for @GET \/xpub\/:xpub@: summary of an extended public key.
scottyXPub ::
  WebState ->
  GetXPub ->
  ActionT XPubSummary
scottyXPub st (GetXPub xpub deriv (NoCache noCache)) =
  withMetrics st (.xpub) $
    let xs = XPubSpec xpub deriv
     in xPubSummary xs <$> xPubBals (cacheOr noCache st) xs

-- | Handler for @DELETE \/xpub\/:xpub@: remove an extended public key from the cache. Returns 'True' if it was cached.
scottyDelXPub ::
  WebState ->
  DelCachedXPub ->
  ActionT (GenericResult Bool)
scottyDelXPub st (DelCachedXPub xpub deriv) =
  withMetrics st (.xpubDelete) $ do
    let c = st.config.store.cache
    r <- liftIO $ maybe (return False) (`cacheDelete` XPubSpec xpub deriv) c
    return $ GenericResult r

-- | Transactions of an extended public key, optionally without the cache.
getXPubTxs ::
  WebState ->
  XPubKey ->
  DeriveType ->
  LimitsParam ->
  Bool ->
  ActionT [TxRef]
getXPubTxs st xpub deriv plimits nocache = do
  limits <- paramToLimits st False plimits
  let xs = XPubSpec xpub deriv
      st' = cacheOr nocache st
  xbals <- xPubBals st' xs
  xPubTxs st' xs xbals limits

-- | Handler for @GET \/xpub\/:xpub\/transactions@: transactions of an extended public key.
scottyXPubTxs ::
  WebState ->
  GetXPubTxs ->
  ActionT [TxRef]
scottyXPubTxs st (GetXPubTxs xpub deriv plimits (NoCache nocache)) =
  withMetrics st (.xpubTx) $
    getXPubTxs st xpub deriv plimits nocache

-- | Handler for @GET \/xpub\/:xpub\/transactions\/full@: full transactions of an extended public key.
scottyXPubTxsFull ::
  WebState ->
  GetXPubTxsFull ->
  ActionT [Transaction]
scottyXPubTxsFull st (GetXPubTxsFull xpub deriv plimits (NoCache nocache)) =
  withMetrics st (.xpubTxFull) $
    fmap catMaybes $
      getTransactions (cacheOr nocache st) . map (.txid)
        =<< getXPubTxs st xpub deriv plimits nocache

-- | Handler for @GET \/xpub\/:xpub\/balances@: balances of the used addresses of an extended public key.
scottyXPubBalances ::
  WebState ->
  GetXPubBalances ->
  ActionT [XPubBal]
scottyXPubBalances st (GetXPubBalances xpub deriv (NoCache noCache)) =
  withMetrics st (.xpubBalance) $
    xPubBals (cacheOr noCache st) (XPubSpec xpub deriv)

-- | Handler for @GET \/xpub\/:xpub\/unspent@: unspent outputs of an extended public key.
scottyXPubUnspent ::
  WebState ->
  GetXPubUnspent ->
  ActionT [XPubUnspent]
scottyXPubUnspent st (GetXPubUnspent xpub deriv pLimits (NoCache noCache)) =
  withMetrics st (.xpubUnspent) $ do
    limits <- paramToLimits st False pLimits
    let xspec = XPubSpec xpub deriv
        st' = cacheOr noCache st
    xbals <- xPubBals st' xspec
    xPubUnspents st' xspec xbals limits

---------------------------------------
-- Blockchain.info API Compatibility --
---------------------------------------

-- | Currency symbol of the coin of a network, in the Blockchain.info format.
netBinfoSymbol :: Network -> BinfoSymbol
netBinfoSymbol net
  | net == btc =
      BinfoSymbol
        { code = "BTC",
          symbol = "BTC",
          name = "Bitcoin",
          conversion = 100 * 1000 * 1000,
          after = True,
          local = False
        }
  | net == bch =
      BinfoSymbol
        { code = "BCH",
          symbol = "BCH",
          name = "Bitcoin Cash",
          conversion = 100 * 1000 * 1000,
          after = True,
          local = False
        }
  | otherwise =
      BinfoSymbol
        { code = "XTS",
          symbol = "¤",
          name = "Test",
          conversion = 100 * 1000 * 1000,
          after = False,
          local = False
        }

-- | Currency symbol of a fiat currency from its price ticker.
binfoTickerToSymbol :: Text -> BinfoTicker -> BinfoSymbol
binfoTickerToSymbol code t =
  BinfoSymbol
    { code,
      symbol = t.symbol,
      name,
      conversion = 100 * 1000 * 1000 / t.fifteen, -- sat/usd
      after = False,
      local = True
    }
  where
    name = case code of
      "EUR" -> "Euro"
      "USD" -> "U.S. dollar"
      "GBP" -> "British pound"
      x -> x

-- | Addresses and xpubs in a request parameter, separated by pipes or commas.
getBinfoAddrsParam ::
  WebState ->
  Text ->
  ActionT (HashSet BinfoAddr)
getBinfoAddrsParam st name = do
  let net = st.config.store.net
  let ctx = st.config.store.ctx
  p <- param name `rescue` return ""
  if T.null p
    then return HashSet.empty
    else case parseBinfoAddr net ctx p of
      Nothing -> raise st (UserError "invalid address")
      Just xs -> return $ HashSet.fromList xs

-- | Xpubs and addresses in the @active@, @activeP2SH@ and @activeBech32@
-- parameters. Xpubs in each parameter derive legacy, nested SegWit and native
-- SegWit addresses.
getBinfoActive ::
  WebState ->
  ActionT (HashSet XPubSpec, HashSet Address)
getBinfoActive st = do
  active <- getBinfoAddrsParam st "active"
  p2sh <- getBinfoAddrsParam st "activeP2SH"
  bech32 <- getBinfoAddrsParam st "activeBech32"
  let xspec d b = (`XPubSpec` d) <$> xpub b
      xspecs =
        HashSet.fromList $
          concat
            [ mapMaybe (xspec DeriveNormal) (HashSet.toList active),
              mapMaybe (xspec DeriveP2SH) (HashSet.toList p2sh),
              mapMaybe (xspec DeriveP2WPKH) (HashSet.toList bech32)
            ]
      addrs = HashSet.fromList $ mapMaybe addr $ HashSet.toList active
  return (xspecs, addrs)
  where
    addr (BinfoAddr a) = Just a
    addr (BinfoXpub _) = Nothing
    xpub (BinfoXpub x) = Just x
    xpub (BinfoAddr _) = Nothing

-- | Whether to identify transactions by short number, unless @txidindex@ is false.
getNumTxId :: ActionT Bool
getNumTxId = fmap not $ param "txidindex" `rescue` return False

-- | Height of the best header.
getChainHeight :: WebState -> ActionT H.BlockHeight
getChainHeight st =
  fmap (.height) $ liftIO . chainGetBest =<< pure st.config.store.chain

-- | Handler for @GET\/POST \/blockchain\/unspent@: unspent outputs of addresses and xpubs, as Blockchain.info.
scottyBinfoUnspent :: WebState -> ActionT ()
scottyBinfoUnspent st =
  withMetrics st (.binfoUnspent) $ do
    (xspecs, addrs) <- getBinfoActive st
    numtxid <- getNumTxId
    limit <- get_limit
    min_conf <- get_min_conf
    let net = st.config.store.net
    let ctx = st.config.store.ctx
    height <- getChainHeight st
    let mn u = min_conf > u.confirmations
    xbals <- lift $ getXBals st.query xspecs
    bus <-
      lift . runConduit $
        getBinfoUnspents st.query numtxid height xbals xspecs addrs
          .| (dropWhileC mn >> takeC limit .| sinkList)
    setHeaders
    streamEncoding (marshalEncoding (net, ctx) (BinfoUnspents bus))
  where
    get_limit = fmap (min 1000) $ param "limit" `rescue` return 250
    get_min_conf = param "confirmations" `rescue` return 0

-- | Stream the unspent outputs of xpubs and addresses, newest first, in the
-- Blockchain.info format.
getBinfoUnspents ::
  StoreRead ->
  Bool ->
  H.BlockHeight ->
  HashMap XPubSpec [XPubBal] ->
  HashSet XPubSpec ->
  HashSet Address ->
  ConduitT () BinfoUnspent IO ()
getBinfoUnspents q numtxid height xbals xspecs addrs = do
  cs' <- conduits
  joinDescStreams (unspentKey . fst) cs' .| mapC (uncurry binfo)
  where
    binfo u xp =
      let conf = case u.block of
            MemRef _ -> 0
            -- an output from the cache may be in a block that the chain of
            -- this store does not have yet: it is confirmed at least once
            BlockRef h _ -> if h > height then 1 else height - h + 1
       in BinfoUnspent
            { txid = u.outpoint.hash,
              index = u.outpoint.index,
              script = u.script,
              value = u.value,
              confirmations = fromIntegral conf,
              txidx = encodeBinfoTxId numtxid u.outpoint.hash,
              xpub = xp
            }
    conduits = (<>) <$> xconduits <*> pure acounduits
    xconduits = lift $ do
      let f x (XPubUnspent u p) =
            let path = toSoft (listToPath p)
                xp = BinfoXPubPath x.key <$> path
             in (u, xp)
          g x = do
            let h = q.xPubUnspents x (xBals x xbals)
                l = let Limits {..} = def in Limits {limit = 16, ..}
            return $ streamThings h Nothing l .| mapC (f x)
      mapM g (HashSet.toList xspecs)
    acounduits =
      let f u = (u, Nothing)
          h = q.getAddressUnspents
          l = let Limits {..} = def in Limits {limit = 16, ..}
          g a = streamThings (h a) Nothing l .| mapC f
       in map g (HashSet.toList addrs)

-- | Balances of the used addresses of each xpub.
getXBals ::
  StoreRead ->
  HashSet XPubSpec ->
  IO (HashMap XPubSpec [XPubBal])
getXBals q =
  fmap HashMap.fromList . mapM f . HashSet.toList
  where
    f x = (x,) . filter (not . nullBalance . (.balance)) <$> q.xPubBals x

-- | Balances of an xpub, or none if missing.
xBals :: XPubSpec -> HashMap XPubSpec [XPubBal] -> [XPubBal]
xBals = HashMap.findWithDefault []

-- | Stream the references to the transactions of xpubs and addresses, newest
-- first, without duplicates. Each xpub and address is read page by page, only
-- as far as the consumer needs, in pages of the given size: about as many as
-- the consumer is expected to take, as every page is a query of its own.
binfoTxRefs ::
  StoreRead ->
  Word32 ->
  HashMap XPubSpec [XPubBal] ->
  HashSet XPubSpec ->
  HashSet Address ->
  ConduitT () TxRef IO ()
binfoTxRefs q pageSize xbals sxspecs saddrs =
  joinDescStreams txRefKey $
    map xpub_c (HashSet.toList sxspecs) <> map addr_c (HashSet.toList saddrs)
  where
    l = Limits {limit = pageSize, offset = 0, start = Nothing}
    xpub_c x = streamThings (q.xPubTxs x (xBals x xbals)) (Just (.txid)) l
    addr_c a = streamThings (q.getAddressTxs a) (Just (.txid)) l

-- | Stream the transactions of xpubs and addresses, newest first, in the
-- Blockchain.info format, with the running balance of the given addresses after
-- each, keeping only those that pass the filter.
getBinfoTxs ::
  StoreRead ->
  Word32 -> -- page size of the references, as for 'binfoTxRefs'
  HashMap XPubSpec [XPubBal] -> -- xpub balances
  HashMap Address (Maybe BinfoXPubPath) -> -- address book
  HashSet XPubSpec -> -- show xpubs
  HashSet Address -> -- show addrs
  HashSet Address -> -- balance addresses
  BinfoFilter ->
  Bool -> -- numtxid
  Bool -> -- prune outputs
  Int64 -> -- starting balance
  ConduitT () BinfoTx IO ()
getBinfoTxs
  q
  pageSize
  xbals
  abook
  sxspecs
  saddrs
  baddrs
  bfilter
  numtxid
  prune
  bal =
    binfoTxRefs q pageSize xbals sxspecs saddrs .| bodies .| go bal
    where
      binfo_tx = toBinfoTx numtxid abook prune
      compute_bal_change t =
        let ins = map (.output) t.inputs
            out = t.outputs
            f b BinfoTxOutput {..} =
              let val = fromIntegral value
               in case address of
                    Nothing -> 0
                    Just a
                      | a `HashSet.member` baddrs ->
                          if b then val else negate val
                      | otherwise -> 0
         in sum $ map (f False) ins <> map (f True) out
      -- the bodies, read a page at a time, which is one round trip to Redis
      -- or one multi-get of the database instead of one per transaction
      bodies = do
        refs <- takeC (fromIntegral pageSize) .| sinkList
        unless (null refs) $ do
          lift (C.getTransactions q (map (.txid) refs)) >>= mapM_ yield
          bodies
      go b =
        await >>= \case
          Nothing -> return ()
          Just Nothing -> go b
          Just (Just x) -> do
            let a = binfo_tx b x
                b' = b - compute_bal_change a
                c = isJust a.blockHeight
                d = fst (fromJust a.balance)
                r = d + fromIntegral a.fee
            case bfilter of
              BinfoFilterAll ->
                yield a >> go b'
              BinfoFilterSent
                | 0 > r -> yield a >> go b'
                | otherwise -> go b'
              BinfoFilterReceived
                | r > 0 -> yield a >> go b'
                | otherwise -> go b'
              BinfoFilterMoved
                | r == 0 -> yield a >> go b'
                | otherwise -> go b'
              BinfoFilterConfirmed
                | c -> yield a >> go b'
                | otherwise -> go b'
              BinfoFilterMempool
                | c -> return ()
                | otherwise -> yield a >> go b'

-- | Page size to read the references of the transactions in, to show this
-- many after this offset: all of them in one page if they fit in 1000.
pageFor :: Int -> Int -> Word32
pageFor offset n = fromIntegral (max 16 (min 1000 (offset + n)))

-- | Whether to show addresses in CashAddr format (@cashaddr@).
getCashAddr :: ActionT Bool
getCashAddr = param "cashaddr" `rescue` return False

-- | Parse an address. Responds with not found if invalid.
getAddress :: WebState -> Text -> ActionT Address
getAddress st txt = do
  let net = st.config.store.net
  case textToAddr net txt of
    Nothing -> raise st ThingNotFound
    Just a -> return a

-- | Address or xpub in a path parameter. Passes to the next route if invalid.
getBinfoAddr :: WebState -> Text -> ActionT BinfoAddr
getBinfoAddr st param' = do
  txt <- S.captureParam param'
  let net = st.config.store.net
  let ctx = st.config.store.ctx
  let addr = BinfoAddr <$> textToAddr net txt
      xpub = BinfoXpub <$> xPubImport net ctx txt
  maybe S.next return (addr <|> xpub)

-- | Handler for @GET\/POST \/blockchain\/export-history@: daily history of the balance of addresses and xpubs with prices, as Blockchain.info.
scottyBinfoHistory :: WebState -> ActionT ()
scottyBinfoHistory st =
  withMetrics st (.binfoExportHistory) $ do
    (xspecs, addrs) <- getBinfoActive st
    (startM, endM) <- get_dates
    (code, price') <- getPrice st
    xbals <- liftIO (getXBals st.query xspecs)
    let xaddrs = HashSet.fromList $ concatMap (map get_addr) (HashMap.elems xbals)
        aaddrs = xaddrs <> addrs
        cur = price'.fifteen
        cs' = conduits (HashMap.toList xbals) addrs endM
    txs <-
      lift $
        runConduit $
          joinDescStreams txRefKey cs'
            .| takeWhileC (is_newer startM)
            .| concatMapMC get_transaction
            .| sinkList
    let times = map (\Transaction {..} -> timestamp) txs
    let net = st.config.store.net
    let url = st.config.priceHistoryURL
    let session = st.session
    let logger = st.config.logger
    rates <- map (.price) <$> lift (getRates logger net session url code times)
    let hs = zipWith (convert cur aaddrs) txs (rates <> repeat 0.0)
    setHeaders
    streamEncoding $ toEncoding hs
  where
    is_newer
      (Just BlockData {height = bh})
      TxRef {block = BlockRef {height = th}} =
        bh <= th
    is_newer Nothing TxRef {} = True
    is_newer _ _ = undefined
    get_addr = (.balance.address)
    get_transaction TxRef {txid = h} =
      C.getTransaction st.query h
    convert cur addrs tx rate =
      let ins = tx.inputs
          outs = tx.outputs
          fins = filter (input_addr addrs) ins
          fouts = filter (output_addr addrs) outs
          vin = fromIntegral . sum $ map (.value) fins
          vout = fromIntegral . sum $ map (.value) fouts
          fee = tx.fee
          v = vout - vin
          t = tx.timestamp
          h = txHash $ transactionData tx
       in toBinfoHistory v t rate cur fee h
    input_addr addrs' StoreInput {address = Just a} =
      a `HashSet.member` addrs'
    input_addr _ _ = False
    output_addr addrs' StoreOutput {address = Just a} =
      a `HashSet.member` addrs'
    output_addr _ _ = False
    get_dates = do
      BinfoDate start <- param "start"
      BinfoDate end' <- param "end"
      let end = end' + 24 * 60 * 60
      let ch = st.config.store.chain
      startM <- blockAtOrAfter st ch start
      endM <- blockAtOrBefore st ch end
      return (startM, endM)
    conduits xpubs addrs endM =
      map (uncurry (xpub_c endM)) xpubs
        <> map (addr_c endM) (HashSet.toList addrs)
    lim endM =
      Limits
        { limit = 16,
          offset = 0,
          start = AtBlock . (.height) <$> endM
        }
    addr_c endM a =
      streamThings (st.query.getAddressTxs a) (Just (.txid)) (lim endM)
    xpub_c endM x bs =
      streamThings (st.query.xPubTxs x bs) (Just (.txid)) (lim endM)

-- | Currency code in the @currency@ parameter, USD by default, and its latest price.
getPrice :: WebState -> ActionT (Text, BinfoTicker)
getPrice st = do
  code <- T.toUpper <$> param "currency" `rescue` return "USD"
  let ticker = st.ticker
  prices <- readTVarIO ticker
  case HashMap.lookup code prices of
    Nothing -> return (code, def)
    Just p -> return (code, p)

-- | Symbol of the currency in the @currency@ parameter.
getSymbol :: WebState -> ActionT BinfoSymbol
getSymbol st = uncurry binfoTickerToSymbol <$> getPrice st

-- | Handler for @GET \/blockchain\/blocks\/:milliseconds@: the blocks of the day before a time in milliseconds, as Blockchain.info.
scottyBinfoBlocksDay :: WebState -> ActionT ()
scottyBinfoBlocksDay st =
  withMetrics st (.binfoBlock) $ do
    t <- min h . (`div` 1000) <$> S.captureParam "milliseconds"
    let ch = st.config.store.chain
    m <- blockAtOrBefore st ch t
    bs <- go (d t) m
    streamEncoding $ toEncoding $ map toBinfoBlockInfo bs
  where
    h = fromIntegral (maxBound :: H.Timestamp)
    d = subtract (24 * 3600)
    go _ Nothing = return []
    go t (Just b)
      | b.header.timestamp <= fromIntegral t =
          return []
      | otherwise = do
          b' <- getBlock st b.header.prev
          (b :) <$> go t b'

-- | Handler for @GET\/POST \/blockchain\/multiaddr@: balances and transactions of addresses and xpubs, as Blockchain.info.
scottyMultiAddr :: WebState -> ActionT ()
scottyMultiAddr st =
  withMetrics st (.binfoMultiaddr) $ do
    (addrs', _, saddrs, sxpubs, xspecs) <- get_addrs
    numtxid <- getNumTxId
    cashaddr <- getCashAddr
    local' <- getSymbol st
    offset <- getBinfoOffset st
    n <- getBinfoCount st "n"
    prune <- get_prune
    fltr <- get_filter
    xbals <- liftIO (getXBals st.query xspecs)
    xtxns <- get_xpub_tx_count xbals xspecs
    let sxbals = only_show_xbals sxpubs xbals
        xabals = compute_xabals xbals
        addrs = addrs' `HashSet.difference` HashMap.keysSet xabals
    abals <- get_abals addrs
    let sxspecs = only_show_xspecs sxpubs xspecs
        sxabals = compute_xabals sxbals
        sabals = only_show_abals saddrs abals
        sallbals = sabals <> sxabals
        sbal = compute_bal sallbals
        allbals = abals <> xabals
        abook = compute_abook addrs xbals
        sxaddrs = compute_xaddrs sxbals
        salladdrs = saddrs <> sxaddrs
        bal = compute_bal allbals
        ibal = fromIntegral sbal
    ftxs <-
      lift . runConduit $
        getBinfoTxs
          st.query
          (pageFor offset n)
          xbals
          abook
          sxspecs
          saddrs
          salladdrs
          fltr
          numtxid
          prune
          ibal
          .| (dropC offset >> takeC n .| sinkList)
    -- counted without reading the transactions, which takes seconds for
    -- xpubs with tens of thousands of them: one xpub has its own count, from
    -- the cache when it is cached; for several xpubs and addresses, the
    -- database counts the distinct transactions of all their addresses
    ntx <- case (HashSet.toList sxspecs, HashSet.null saddrs) of
      ([x], True) -> return (HashMap.findWithDefault 0 x xtxns)
      _ -> fromIntegral <$> liftIO (Q.getAddressesTxCount st.config.store.db (HashSet.toList salladdrs))
    nfiltered <- case fltr of
      BinfoFilterAll -> return ntx
      _ ->
        lift . runConduit $
          getBinfoTxs
            st.query
            1000
            xbals
            abook
            sxspecs
            saddrs
            salladdrs
            fltr
            numtxid
            prune
            ibal
            .| lengthC
    let net = st.config.store.net
    let ctx = st.config.store.ctx
    best <- get_best_block
    peers <- get_peers
    let baddrs = toBinfoAddrs sabals sxbals xtxns
        abaddrs = toBinfoAddrs abals xbals xtxns
        recv = sum $ map (.received) abaddrs
        sent' = sum $ map (.sent) abaddrs
        wallet =
          BinfoWallet
            { balance = bal,
              txs = ntx,
              filtered = nfiltered,
              received = recv,
              sent = sent'
            }
        coin = netBinfoSymbol net
    let block =
          BinfoBlockInfo
            { hash = H.headerHash best.header,
              height = best.height,
              timestamp = best.header.timestamp,
              index = best.height
            }
    let info =
          BinfoInfo
            { connected = peers,
              conversion = 100 * 1000 * 1000,
              fiat = local',
              crypto = coin,
              head = block
            }
    let multiaddr =
          BinfoMultiAddr
            { addresses = baddrs,
              wallet = wallet,
              txs = ftxs,
              info = info,
              recommendFee = True,
              cashAddr = cashaddr
            }
    setHeaders
    streamEncoding $ marshalEncoding (net, ctx) multiaddr
  where
    get_xpub_tx_count xbals =
      fmap HashMap.fromList
        . mapM
          ( \x ->
              (x,)
                . fromIntegral
                <$> xPubTxCount st x (xBals x xbals)
          )
        . HashSet.toList
    get_filter = param "filter" `rescue` return BinfoFilterAll
    get_best_block =
      getBestBlock st >>= \case
        Nothing -> raise st ThingNotFound
        Just bh ->
          getBlock st bh >>= \case
            Nothing -> raise st ThingNotFound
            Just b -> return b
    get_prune =
      fmap not $
        param "noCompact"
          `rescue` param "no_compact"
          `rescue` return False
    only_show_xbals sxpubs =
      HashMap.filterWithKey (\k _ -> k.key `HashSet.member` sxpubs)
    only_show_xspecs sxpubs =
      HashSet.filter (\k -> k.key `HashSet.member` sxpubs)
    only_show_abals saddrs =
      HashMap.filterWithKey (\k _ -> k `HashSet.member` saddrs)
    addr (BinfoAddr a) = Just a
    addr (BinfoXpub _) = Nothing
    xpub (BinfoXpub x) = Just x
    xpub (BinfoAddr _) = Nothing
    get_addrs = do
      (xspecs, addrs) <- getBinfoActive st
      sh <- getBinfoAddrsParam st "onlyShow"
      let xpubs = HashSet.map (.key) xspecs
          actives =
            HashSet.map BinfoAddr addrs
              <> HashSet.map BinfoXpub xpubs
          sh' = if HashSet.null sh then actives else sh
          saddrs = HashSet.fromList $ mapMaybe addr $ HashSet.toList sh'
          sxpubs = HashSet.fromList $ mapMaybe xpub $ HashSet.toList sh'
      return (addrs, xpubs, saddrs, sxpubs, xspecs)
    get_abals =
      let f b = (b.address, b)
          g = HashMap.fromList . map f
       in fmap g . getBalances st . HashSet.toList
    get_peers = do
      ps <- lift $ getPeersInformation st.config.store.peerMgr
      return (fromIntegral (length ps))
    compute_xabals =
      let f b = (b.balance.address, b.balance)
       in HashMap.fromList . concatMap (map f) . HashMap.elems
    compute_bal =
      let f b = b.confirmed + b.unconfirmed
       in sum . map f . HashMap.elems
    compute_abook addrs xbals =
      let f xs xb =
            let a = xb.balance.address
                e = error "lions and tigers and bears"
                s = toSoft (listToPath xb.path)
             in (a, Just (BinfoXPubPath xs.key (fromMaybe e s)))
          amap =
            HashMap.map (const Nothing) $
              HashSet.toMap addrs
          xmap =
            HashMap.fromList
              . concatMap (uncurry (map . f))
              $ HashMap.toList xbals
       in amap <> xmap
    compute_xaddrs =
      let f = map (.balance.address)
       in HashSet.fromList . concatMap f . HashMap.elems

-- | Number of items requested in a parameter, within the limits.
getBinfoCount :: WebState -> Text -> ActionT Int
getBinfoCount st str = do
  let d = st.config.limits.defItemCount
  let x = st.config.limits.maxFullItemCount
  i <- min x <$> (param str `rescue` return d)
  return (fromIntegral i :: Int)

-- | Offset requested in the @offset@ parameter. Responds with an error if above the maximum.
getBinfoOffset ::
  WebState ->
  ActionT Int
getBinfoOffset st = do
  let x = st.config.limits.maxOffset
  o <- param "offset" `rescue` return 0
  when (o > x) $
    raise st $
      UserError $
        "offset exceeded: " <> show o <> " > " <> show x
  return (fromIntegral o :: Int)

-- | Handler for @GET \/blockchain\/rawaddr\/:addr@: balance and transactions of an address or xpub, as Blockchain.info; also at @\/blockchain\/address\/:addr@ and @\/blockchain\/xpub\/:addr@.
scottyRawAddr :: WebState -> ActionT ()
scottyRawAddr st =
  withMetrics st (.binfoAddressRaw) $
    getBinfoAddr st "addr" >>= \case
      BinfoAddr addr -> do_addr addr
      BinfoXpub xpub -> do_xpub xpub
  where
    do_xpub xpub = do
      numtxid <- getNumTxId
      derive <- param "derive" `rescue` return DeriveNormal
      let xspec = XPubSpec xpub derive
      n <- getBinfoCount st "limit"
      off <- getBinfoOffset st
      xbals <- liftIO (getXBals st.query (HashSet.singleton xspec))
      let net = st.config.store.net
      let summary = xPubSummary xspec (xBals xspec xbals)
          abook = compute_abook xpub (xBals xspec xbals)
          xspecs = HashSet.singleton xspec
          saddrs = HashSet.empty
          baddrs = HashMap.keysSet abook
          bfilter = BinfoFilterAll
          amnt = summary.confirmed + summary.unconfirmed
      txs <-
        lift $
          runConduit $
            getBinfoTxs
              st.query
              (pageFor off n)
              xbals
              abook
              xspecs
              saddrs
              baddrs
              bfilter
              numtxid
              False
              (fromIntegral amnt)
              .| (dropC off >> takeC n .| sinkList)
      -- n_tx is every transaction of the xpub, as for an address, not the
      -- number on this page
      ntx <- xPubTxCount st xspec (xBals xspec xbals)
      let ra =
            BinfoRawAddr
              { address = BinfoXpub xpub,
                balance = amnt,
                ntx = fromIntegral ntx,
                utxo = summary.utxo,
                received = summary.received,
                sent = fromIntegral summary.received - fromIntegral amnt,
                txs = txs
              }
      setHeaders
      let ctx = st.config.store.ctx
      streamEncoding $ marshalEncoding (net, ctx) ra
    compute_abook xpub xbals =
      let f xb =
            let a = xb.balance.address
                e = error "black hole swallows all your code"
                s = toSoft $ listToPath xb.path
                m = fromMaybe e s
             in (a, Just (BinfoXPubPath xpub m))
       in HashMap.fromList $ map f xbals
    do_addr addr = do
      numtxid <- getNumTxId
      n <- getBinfoCount st "limit"
      off <- getBinfoOffset st
      bal <- fromMaybe (zeroBalance addr) <$> getBalance st addr
      let net = st.config.store.net
      let abook = HashMap.singleton addr Nothing
          xspecs = HashSet.empty
          saddrs = HashSet.singleton addr
          bfilter = BinfoFilterAll
          balance = bal.confirmed + bal.unconfirmed
      txs <-
        lift . runConduit $
          getBinfoTxs
            st.query
            (pageFor off n)
            HashMap.empty
            abook
            xspecs
            saddrs
            saddrs
            bfilter
            numtxid
            False
            (fromIntegral balance)
            .| (dropC off >> takeC n .| sinkList)
      let ra =
            BinfoRawAddr
              { address = BinfoAddr addr,
                balance,
                ntx = bal.txs,
                utxo = bal.utxo,
                received = bal.received,
                sent = fromIntegral bal.received - fromIntegral balance,
                txs
              }
      setHeaders
      let ctx = st.config.store.ctx
      streamEncoding $ marshalEncoding (net, ctx) ra

-- | Handler for @GET \/blockchain\/q\/getreceivedbyaddress\/:addr@: total received by an address.
scottyBinfoReceived :: WebState -> ActionT ()
scottyBinfoReceived st =
  withMetrics st (.binfoQgetreceivedbyaddress) $ do
    a <- getAddress st =<< S.captureParam "addr"
    b <- fromMaybe (zeroBalance a) <$> getBalance st a
    setHeaders
    S.text $ cs $ show b.received

-- | Handler for @GET \/blockchain\/q\/getsentbyaddress\/:addr@: total sent by an address.
scottyBinfoSent :: WebState -> ActionT ()
scottyBinfoSent st =
  withMetrics st (.binfoQgetsentbyaddress) $ do
    a <- getAddress st =<< S.captureParam "addr"
    b <- fromMaybe (zeroBalance a) <$> getBalance st a
    setHeaders
    S.text $ cs $ show $ b.received - b.confirmed - b.unconfirmed

-- | Handler for @GET \/blockchain\/q\/addressbalance\/:addr@: balance of an address.
scottyBinfoAddrBalance :: WebState -> ActionT ()
scottyBinfoAddrBalance st =
  withMetrics st (.binfoQaddressbalance) $ do
    a <- getAddress st =<< S.captureParam "addr"
    b <- fromMaybe (zeroBalance a) <$> getBalance st a
    setHeaders
    S.text $ cs $ show $ b.confirmed + b.unconfirmed

-- | Handler for @GET \/blockchain\/q\/addressfirstseen\/:addr@: time of the first block with a transaction of an address.
scottyFirstSeen :: WebState -> ActionT ()
scottyFirstSeen st =
  withMetrics st (.binfoQaddressfirstseen) $ do
    a <- getAddress st =<< S.captureParam "addr"
    let ch = st.config.store.chain
    bb <- liftIO $ chainGetBest ch
    let top = bb.height
        bot = 0
    i <- go ch bb a bot top
    setHeaders
    S.text $ cs $ show i
  where
    go ch bb a bot top = do
      let mid = bot + (top - bot) `div` 2
          n = top - bot < 2
      x <- hasone a bot
      y <- hasone a mid
      z <- hasone a top
      if
        | x -> getblocktime ch bb bot
        | n -> getblocktime ch bb top
        | y -> go ch bb a bot mid
        | z -> go ch bb a mid top
        | otherwise -> return 0
    getblocktime ch bb h =
      liftIO (chainGetAncestor ch h bb) >>= \case
        Just b -> return b.header.timestamp
        Nothing -> do
          let logger = st.config.logger
          liftIO . logError logger $
            "Could not get ancestor at height "
              <> cs (show h)
              <> " for block "
              <> H.blockHashToHex (H.headerHash bb.header)
          error "Block ancestor retreival error"
    hasone a h = do
      let l = Limits 1 0 (Just (AtBlock h))
      not . null <$> getAddressTxs st a l

-- | Handler for @GET\/POST \/blockchain\/balance@: short balances of addresses and xpubs, as Blockchain.info.
scottyShortBal :: WebState -> ActionT ()
scottyShortBal st =
  withMetrics st (.binfoBalance) $ do
    (xspecs, addrs) <- getBinfoActive st
    cashaddr <- getCashAddr
    let net = st.config.store.net
    abals <- catMaybes <$> mapM (getabal net cashaddr) (HashSet.toList addrs)
    xbals <- mapM (getxbal net) (HashSet.toList xspecs)
    let res = HashMap.fromList (abals <> xbals)
    setHeaders
    streamEncoding $ toEncoding res
  where
    shorten bal =
      BinfoShortBal
        { final = bal.confirmed + bal.unconfirmed,
          ntx = bal.txs,
          received = bal.received
        }
    getabal net cashaddr a =
      let net' =
            if
              | cashaddr -> net
              | net == bch -> btc
              | net == bchTest -> btcTest
              | net == bchTest4 -> btcTest
              | otherwise -> net
       in case addrToText net' a of
            Nothing -> return Nothing
            Just a' ->
              getBalance st a >>= \case
                Nothing -> return $ Just (a', shorten (zeroBalance a))
                Just b -> return $ Just (a', shorten b)
    getxbal net xpub = do
      xbals <- xPubBals st xpub
      ntx <- fromIntegral <$> xPubTxCount st xpub xbals
      let val = sum $ map (.balance.confirmed) xbals
          zro = sum $ map (.balance.unconfirmed) xbals
          exs = [x | x@XPubBal {path = 0 : _} <- xbals]
          final = val + zro
          received = sum $ map (.balance.received) exs
          sbl = BinfoShortBal {final, ntx, received}
      let ctx = st.config.store.ctx
      return (xPubExport net ctx xpub.key, sbl)

-- | Whether to respond with hex (@format=hex@).
getBinfoHex :: ActionT Bool
getBinfoHex = (== ("hex" :: Text)) <$> param "format" `rescue` return "json"

-- | Handler for @GET \/blockchain\/block-height\/:height@: the blocks at a height, as Blockchain.info.
scottyBinfoBlockHeight :: WebState -> ActionT ()
scottyBinfoBlockHeight st =
  withMetrics st (.binfoBlockHeight) $ do
    numtxid <- getNumTxId
    height <- S.captureParam "height"
    bs <- fmap catMaybes $ getBlocksAtHeight st height >>= mapM (getBlock st)
    ns <- fmap catMaybes $ getBlocksAtHeight st (height + 1) >>= mapM (getBlock st)
    is <- mapM (get_binfo_blocks numtxid ns) bs
    setHeaders
    let net = st.config.store.net
    let ctx = st.config.store.ctx
    streamEncoding $ marshalEncoding (net, ctx) is
  where
    get_binfo_blocks numtxid ns b = do
      txs <- catMaybes <$> getTransactions st b.txs
      let h x = H.headerHash x.header
          nbs = [h n | n <- ns, n.header.prev == h b]
          bts = map (toBinfoTxSimple numtxid) txs
      return $ toBinfoBlock b bts nbs

-- | Handler for @GET \/blockchain\/latestblock@: the best block, as Blockchain.info.
scottyBinfoLatest :: WebState -> ActionT ()
scottyBinfoLatest st =
  withMetrics st (.binfoBlockLatest) $ do
    numtxid <- getNumTxId
    b <- liftIO (bestBlock st.query) >>= maybe (raise st ThingNotFound) return
    streamEncoding $
      toEncoding
        BinfoHeader
          { hash = H.headerHash b.header,
            timestamp = b.header.timestamp,
            index = b.height,
            height = b.height,
            txids = map (encodeBinfoTxId numtxid) b.txs
          }

-- | Handler for @GET \/blockchain\/rawblock\/:block@: a block with its transactions, as Blockchain.info.
scottyBinfoBlock :: WebState -> ActionT ()
scottyBinfoBlock st =
  withMetrics st (.binfoBlockRaw) $ do
    numtxid <- getNumTxId
    hex <- getBinfoHex
    S.captureParam "block" >>= \case
      BinfoBlockHash bh -> go numtxid hex bh
      BinfoBlockIndex i ->
        getBlocksAtHeight st i >>= \case
          [] -> raise st ThingNotFound
          bh : _ -> go numtxid hex bh
  where
    go numtxid hex bh = do
      b <- maybe (raise st ThingNotFound) return =<< getBlock st bh
      txs <- catMaybes <$> getTransactions st b.txs
      nhs <- fmap catMaybes $ getBlocksAtHeight st (b.height + 1) >>= mapM (getBlock st)
      let h x = H.headerHash x.header
          nxt = [h n | n <- nhs, n.header.prev == h b]
      if hex
        then do
          let x = H.Block b.header (map transactionData txs)
          setHeaders
          S.text . encodeHexLazy . runPutL $ serialize x
        else do
          let bts = map (toBinfoTxSimple numtxid) txs
              y = toBinfoBlock b bts nxt
          setHeaders
          let net = st.config.store.net
          let ctx = st.config.store.ctx
          streamEncoding $ marshalEncoding (net, ctx) y

-- | Transaction by hash or short number. Fails with 'TxIndexConflict' if a short number matches several.
getBinfoTx ::
  WebState ->
  BinfoTxId ->
  ActionT (Either Except Transaction)
getBinfoTx st txid = do
  tx <- case txid of
    BinfoTxIdHash h -> maybeToList <$> getTransaction st h
    BinfoTxIdIndex i -> getNumTransaction st i
  case tx of
    [t] -> return $ Right t
    [] -> return $ Left ThingNotFound
    ts ->
      let tids = map (txHash . transactionData) ts
       in return $ Left (TxIndexConflict tids)

-- | Handler for @GET \/blockchain\/rawtx\/:txid@: a transaction, as Blockchain.info.
scottyBinfoTx :: WebState -> ActionT ()
scottyBinfoTx st =
  withMetrics st (.binfoTxRaw) $ do
    numtxid <- getNumTxId
    hex <- getBinfoHex
    txid <- S.captureParam "txid"
    tx <-
      getBinfoTx st txid >>= \case
        Right t -> return t
        Left e -> raise st e
    if hex then hx tx else js numtxid tx
  where
    js numtxid t = do
      let net = st.config.store.net
      let ctx = st.config.store.ctx
      setHeaders
      streamEncoding $ marshalEncoding (net, ctx) $ toBinfoTxSimple numtxid t
    hx t = do
      setHeaders
      S.text . encodeHexLazy . runPutL . serialize $ transactionData t

-- | Handler for @GET \/blockchain\/q\/txtotalbtcoutput\/:txid@: total value of the outputs of a transaction.
scottyBinfoTotalOut :: WebState -> ActionT ()
scottyBinfoTotalOut st =
  withMetrics st (.binfoQtxtotalbtcoutput) $ do
    txid <- S.captureParam "txid"
    tx <-
      getBinfoTx st txid >>= \case
        Right t -> return t
        Left e -> raise st e
    S.text $ cs $ show $ sum $ map (.value) tx.outputs

-- | Handler for @GET \/blockchain\/q\/txfee\/:txid@: fee of a transaction.
scottyBinfoTxFees :: WebState -> ActionT ()
scottyBinfoTxFees st =
  withMetrics st (.binfoQtxfee) $ do
    txid <- S.captureParam "txid"
    tx <-
      getBinfoTx st txid >>= \case
        Right t -> return t
        Left e -> raise st e
    let i = sum $ map (.value) $ filter f tx.inputs
        o = sum $ map (.value) tx.outputs
    S.text . cs . show $ i - o
  where
    f StoreInput {} = True
    f StoreCoinbase {} = False

-- | Handler for @GET \/blockchain\/q\/txresult\/:txid\/:addr@: net amount that a transaction sends to an address.
scottyBinfoTxResult :: WebState -> ActionT ()
scottyBinfoTxResult st =
  withMetrics st (.binfoQtxresult) $ do
    txid <- S.captureParam "txid"
    addr <- getAddress st =<< S.captureParam "addr"
    tx <-
      getBinfoTx st txid >>= \case
        Right t -> return t
        Left e -> raise st e
    let i = toInteger $ sum $ map (.value) $ filter (f addr) tx.inputs
        o = toInteger $ sum $ map (.value) $ filter (g addr) tx.outputs
    S.text $ cs $ show $ o - i
  where
    f addr StoreInput {address = Just a} = a == addr
    f _ _ = False
    g addr StoreOutput {address = Just a} = a == addr
    g _ _ = False

-- | Handler for @GET \/blockchain\/q\/txtotalbtcinput\/:txid@: total value of the inputs of a transaction.
scottyBinfoTotalInput :: WebState -> ActionT ()
scottyBinfoTotalInput st =
  withMetrics st (.binfoQtxtotalbtcinput) $ do
    txid <- S.captureParam "txid"
    tx <-
      getBinfoTx st txid >>= \case
        Right t -> return t
        Left e -> raise st e
    S.text $ cs $ show $ sum $ map (.value) $ filter f $ tx.inputs
  where
    f StoreInput {} = True
    f StoreCoinbase {} = False

-- | Handler for @GET \/blockchain\/unconfirmed-transactions@: mempool transactions, as Blockchain.info.
scottyBinfoMempool :: WebState -> ActionT ()
scottyBinfoMempool st =
  withMetrics st (.binfoMempool) $ do
    numtxid <- getNumTxId
    offset <- getBinfoOffset st
    n <- getBinfoCount st "limit"
    -- a limit of 0 would mean no limit to getMempoolPage st
    txids <-
      if n <= 0
        then return []
        else
          map snd
            <$> getMempoolPage st (Limits (fromIntegral n) (fromIntegral offset) Nothing)
    txs <- catMaybes <$> getTransactions st txids
    let net = st.config.store.net
    setHeaders
    let mem = BinfoMempool $ map (toBinfoTxSimple numtxid) txs
    let ctx = st.config.store.ctx
    streamEncoding $ marshalEncoding (net, ctx) mem

-- | Handler for @GET \/blockchain\/q\/getblockcount@: height of the best header.
scottyBinfoGetBlockCount :: WebState -> ActionT ()
scottyBinfoGetBlockCount st =
  withMetrics st (.binfoQgetblockcount) $ do
    let ch = st.config.store.chain
    bn <- liftIO $ chainGetBest ch
    setHeaders
    S.text $ TL.pack $ show bn.height

-- | Handler for @GET \/blockchain\/q\/latesthash@: hash of the best header.
scottyBinfoLatestHash :: WebState -> ActionT ()
scottyBinfoLatestHash st =
  withMetrics st (.binfoQlatesthash) $ do
    let ch = st.config.store.chain
    bn <- liftIO $ chainGetBest ch
    setHeaders
    S.text $ TL.fromStrict $ H.blockHashToHex $ H.headerHash bn.header

-- | Handler for @GET \/blockchain\/q\/bcperblock@: block subsidy at the next height.
scottyBinfoSubsidy :: WebState -> ActionT ()
scottyBinfoSubsidy st =
  withMetrics st (.binfoQbcperblock) $ do
    let ch = st.config.store.chain
    let net = st.config.store.net
    bn <- liftIO $ chainGetBest ch
    setHeaders
    S.text $
      cs $
        show $
          (/ (100 * 1000 * 1000 :: Double)) $
            fromIntegral $
              H.computeSubsidy net (bn.height + 1)

-- | Handler for @GET \/blockchain\/q\/addresstohash\/:addr@: hash160 of an address in hex.
scottyBinfoAddrToHash :: WebState -> ActionT ()
scottyBinfoAddrToHash st =
  withMetrics st (.binfoQaddresstohash) $ do
    addr <- getAddress st =<< S.captureParam "addr"
    setHeaders
    if
      | Just h <- addressHash160 addr ->
          S.text $ encodeHexLazy $ runPutL $ serialize h
      | Just h <- addressHash256 addr ->
          S.text $ encodeHexLazy $ runPutL $ serialize h
      | otherwise ->
          return ()

-- | Handler for @GET \/blockchain\/q\/hashtoaddress\/:hash@: pay-to-public-key-hash address of a hash160.
scottyBinfoHashToAddr :: WebState -> ActionT ()
scottyBinfoHashToAddr st =
  withMetrics st (.binfoQhashtoaddress) $ do
    bs <- maybe S.next return . decodeHex =<< S.captureParam "hash"
    let net = st.config.store.net
    hash <- either (const S.next) return $ decode bs
    addr <- maybe S.next return $ addrToText net $ PubKeyAddress hash
    setHeaders
    S.text $ TL.fromStrict addr

-- | Handler for @GET \/blockchain\/q\/addrpubkey\/:pubkey@: address of a public key.
scottyBinfoAddrPubkey :: WebState -> ActionT ()
scottyBinfoAddrPubkey st =
  withMetrics st (.binfoQaddrpubkey) $ do
    hex <- S.captureParam "pubkey"
    let ctx = st.config.store.ctx
    pubkey <-
      maybe S.next (return . pubKeyAddr ctx) $
        eitherToMaybe . unmarshal ctx =<< decodeHex hex
    let net = st.config.store.net
    setHeaders
    case addrToText net pubkey of
      Nothing -> raise st ThingNotFound
      Just a -> S.text $ TL.fromStrict a

-- | Handler for @GET \/blockchain\/q\/pubkeyaddr\/:addr@: public key of an address, found in a transaction input that spends from it.
scottyBinfoPubKeyAddr :: WebState -> ActionT ()
scottyBinfoPubKeyAddr st =
  withMetrics st (.binfoQpubkeyaddr) $ do
    addr <- getAddress st =<< S.captureParam "addr"
    mi <- strm addr
    i <- case mi of
      Nothing -> raise st ThingNotFound
      Just i -> return i
    pk <- case extr addr i of
      Left e -> raise st $ UserError e
      Right t -> return t
    setHeaders
    S.text $ encodeHexLazy $ L.fromStrict pk
  where
    strm addr = do
      runConduit $ do
        let l = Limits {limit = 8, offset = 0, start = Nothing}
        streamThings (getAddressTxs st addr) (Just (.txid)) l
          .| concatMapMC (getTransaction st . (.txid))
          .| concatMapC (filter (inp addr) . (.inputs))
          .| headC
    inp addr StoreInput {address = Just a} = a == addr
    inp _ _ = False
    extr addr StoreInput {script, pkscript, witness} = do
      Script sig <- decode script
      Script pks <- decode pkscript
      case addr of
        PubKeyAddress {} ->
          case sig of
            [OP_PUSHDATA _ _, OP_PUSHDATA pub _] ->
              Right pub
            [OP_PUSHDATA _ _] ->
              case pks of
                [OP_PUSHDATA pub _, OP_CHECKSIG] ->
                  Right pub
                _ -> Left "Could not parse scriptPubKey"
            _ -> Left "Could not parse scriptSig"
        WitnessPubKeyAddress {} ->
          case witness of
            [_, pub] -> return pub
            _ -> Left "Could not parse scriptPubKey"
        _ -> Left "Address does not have public key"
    extr _ _ = Left "Incorrect input type"

-- | Handler for @GET \/blockchain\/q\/hashpubkey\/:pubkey@: hash160 of a public key in hex.
scottyBinfoHashPubkey :: WebState -> ActionT ()
scottyBinfoHashPubkey st =
  withMetrics st (.binfoQhashpubkey) $ do
    let ctx = st.config.store.ctx
    pkm <-
      fmap
        (eitherToMaybe . unmarshal ctx <=< decodeHex)
        (S.captureParam "pubkey")
    addr <- case pkm of
      Nothing -> raise st $ UserError "Could not decode public key"
      Just pk -> return $ pubKeyAddr ctx pk
    setHeaders
    if
      | Just h <- addressHash160 addr ->
          S.text $ encodeHexLazy $ runPutL $ serialize h
      | Just h <- addressHash256 addr ->
          S.text $ encodeHexLazy $ runPutL $ serialize h
      | otherwise ->
          return ()

-- GET Network Information --

-- | Handler for @GET \/peers@: connected peers.
scottyPeers ::
  WebState ->
  GetPeers ->
  ActionT [PeerInfo]
scottyPeers st _ =
  withMetrics st (.peers) $ do
    lift . getPeersInformation =<< pure st.config.store.peerMgr

-- | Obtain information about connected peers from peer manager process.
getPeersInformation ::
  PeerMgr -> IO [PeerInfo]
getPeersInformation mgr =
  mapMaybe toInfo <$> liftIO (getPeers mgr)
  where
    toInfo op = do
      ver <- op.version
      return
        PeerInfo
          { userAgent = ver.userAgent.get,
            address = show op.address,
            -- Signed, as the peer sent it. See 'PeerInfo'.
            version = ver.version,
            services = ver.services,
            relay = ver.relay
          }

-- | Handler for @GET \/health@: health of the store.
scottyHealth ::
  WebState ->
  GetHealth ->
  ActionT HealthCheck
scottyHealth st _ =
  withMetrics st (.health) $ do
    h <- pure st.health >>= readTVarIO
    unless (isOK h) $ S.status status503
    return h

-- | Heights of the best header and best block, and the maximum lag allowed.
blockHealthCheck :: WebConfig -> IO BlockHealth
blockHealthCheck cfg = do
  let ch = cfg.store.chain
      db = cfg.store.db
  headers <- (.height) <$> chainGetBest ch
  blocks <-
    Q.getBestBlock db >>= \case
      Nothing -> return 0
      Just h -> maybe 0 (.height) <$> Q.getBlock db h
  return
    BlockHealth
      { headers,
        blocks,
        max = fromIntegral cfg.maxLaggingBlocks
      }

-- | Age of the best header and the maximum allowed.
lastBlockHealthCheck :: Chain -> WebLimits -> IO TimeHealth
lastBlockHealthCheck ch WebLimits {blockTimeout} = do
  n <- round <$> liftIO getPOSIXTime
  t <- fromIntegral . (.header.timestamp) <$> liftIO (chainGetBest ch)
  return
    TimeHealth
      { age = n - t,
        max = fromIntegral blockTimeout
      }

-- | Age of the newest mempool transaction or best header, and the maximum allowed.
lastTxHealthCheck :: WebConfig -> IO TimeHealth
lastTxHealthCheck WebConfig {noMempool, store = store', limits} = do
  n <- round <$> liftIO getPOSIXTime
  b <- fromIntegral . (.header.timestamp) <$> liftIO (chainGetBest ch)
  t <-
    Q.getMempoolPage store'.db (Limits 1 0 Nothing) >>= \case
      t : _ ->
        let x = fromIntegral $ fst t
         in return $ max x b
      [] -> return b
  return
    TimeHealth
      { age = n - t,
        max = fromIntegral to
      }
  where
    ch = store'.chain
    to =
      if noMempool
        then limits.blockTimeout
        else limits.txTimeout

-- | Transactions waiting to be imported and the maximum allowed.
pendingTxsHealthCheck :: WebConfig -> IO MaxHealth
pendingTxsHealthCheck cfg = do
  n <- blockStorePendingTxs cfg.store.block
  return
    MaxHealth
      { max = fromIntegral cfg.maxPendingTxs,
        count = fromIntegral n
      }

-- | Online peers and the minimum required.
peerHealthCheck :: WebConfig -> IO CountHealth
peerHealthCheck cfg = do
  count <- fromIntegral . length <$> liftIO (getPeers cfg.store.peerMgr)
  return CountHealth {min = fromIntegral cfg.minPeers, count}

-- | Run every health check.
healthCheck :: WebConfig -> IO HealthCheck
healthCheck cfg = do
  blocks <- blockHealthCheck cfg
  lastBlock <- lastBlockHealthCheck cfg.store.chain cfg.limits
  lastTx <- lastTxHealthCheck cfg
  pendingTxs <- pendingTxsHealthCheck cfg
  peers <- peerHealthCheck cfg
  time <- round <$> liftIO getPOSIXTime
  return
    HealthCheck
      { network = cfg.store.net.name,
        version = cfg.version,
        ..
      }

-- | Handler for @GET \/dbstats@: RocksDB memory use and statistics, as text.
scottyDbStats :: WebState -> ActionT ()
scottyDbStats st =
  withMetrics st (.db) $ do
    setHeaders
    let db = st.config.store.db.db
    memory <- liftIO (memoryReport db)
    statsM <- liftIO (getProperty db Stats)
    S.text . cs $ memory <> maybe "Could not get stats" cs statsM

-----------------------
-- Parameter Parsing --
-----------------------

-- | Returns @Nothing@ if the parameter is not supplied. Raises an exception on
-- parse failure.
paramOptional :: forall a. (Param a) => WebState -> ActionT (Maybe a)
paramOptional st = do
  let net = st.config.store.net
  let ctx = st.config.store.ctx
  fmap Just (param label) `rescue` return Nothing >>= \case
    Nothing -> return Nothing -- Parameter was not supplied
    Just ts -> maybe (raise st err) (return . Just) (parseParam net ctx ts)
  where
    label = proxyLabel (Proxy :: Proxy a)
    err = UserError $ "Unable to parse param " <> T.unpack label

-- | Raises an exception if the parameter is not supplied
paramRequired :: forall a. (Param a) => WebState -> ActionT a
paramRequired st = maybe (raise st err) return =<< paramOptional st
  where
    label = T.unpack (proxyLabel (Proxy :: Proxy a))
    err = UserError $ "Param " <> label <> " not defined"

-- | Returns the default value of a parameter if it is not supplied. Raises an
-- exception on parse failure.
paramDef :: (Default a, Param a) => WebState -> ActionT a
paramDef st = fromMaybe def <$> paramOptional st

-- | Does not raise exceptions. Will call @Scotty.next@ if the parameter is
-- not supplied or if parsing fails.
paramCapture :: forall a. (Param a) => WebState -> ActionT a
paramCapture st = do
  let net = st.config.store.net
  let ctx = st.config.store.ctx
  p <- S.captureParam label `rescue` S.next
  maybe S.next return (parseParam net ctx p)
  where
    label = proxyLabel (Proxy :: Proxy a)

-- | Read the transaction that a request carries in its body.
parseTxBody :: WebState -> ActionT Tx
parseTxBody st = do
  let net = st.config.store.net
  b <- L.toStrict <$> S.body
  case decodeTxBody net b of
    Left _ -> raise st $ UserError "Failed to parse request body"
    Right tx -> return tx

-- | Read a transaction from a request body. The body is the transaction in
-- hex or as raw bytes. White space in the hex is ignored.
--
-- The body must hold one transaction and nothing else. Bytes after the
-- transaction are refused, as Bitcoin Core does for a raw transaction.
--
-- The transaction must be in the format that peers of the network read. A
-- network without SegWit, such as Bitcoin Cash, has the format without
-- witness only. Its nodes would not read the SegWit format, so the store
-- does not take it either.
decodeTxBody :: Network -> C.ByteString -> Either String Tx
decodeTxBody net body = fromHex <> fromBytes body
  where
    fromBytes = runGetS $ do
      tx <- getNetworkTx net
      done <- isEmpty
      unless done $ fail "Trailing bytes after transaction"
      return tx
    fromHex =
      case parseBase16 (C.filter (not . isSpace) body) of
        Right b16 -> fromBytes (decodeBase16 b16)
        Left e -> Left (cs e)

-- | Offset in the @offset@ parameter. Responds with an error if above the maximum.
parseOffset :: WebState -> ActionT OffsetParam
parseOffset st = do
  res@(OffsetParam o) <- paramDef st
  let limits = st.config.limits
  when (limits.maxOffset > 0 && fromIntegral o > limits.maxOffset) $
    raise st . UserError $
      "offset exceeded: " <> show o <> " > " <> show limits.maxOffset
  return res

-- | The best block, if there is one.
bestBlock :: StoreRead -> IO (Maybe BlockData)
bestBlock q = q.getBestBlock >>= maybe (return Nothing) q.getBlock

-- | Turn a start parameter into a position: a transaction, the block of a block
-- hash, a height, or the last block at or before a time.
parseStart ::
  WebState ->
  Maybe StartParam ->
  ActionT (Maybe Start)
parseStart _ Nothing = return Nothing
parseStart st (Just s) = liftIO $ case s of
  -- a transaction if there is one with that hash, else a block
  StartParamHash {hash = h} ->
    st.query.getTxData (TxHash h) >>= \case
      Just _ -> return (Just (AtTx (TxHash h)))
      Nothing -> fmap (AtBlock . (.height)) <$> st.query.getBlock (H.BlockHash h)
  StartParamHeight {height = h} -> return (Just (AtBlock (fromIntegral h)))
  StartParamTime {time = q} ->
    fmap (AtBlock . (.height)) <$> C.blockAtOrBefore st.query st.config.store.chain q

-- | Limit, offset and start in the parameters of a request.
parseLimits :: WebState -> ActionT LimitsParam
parseLimits st = LimitsParam <$> paramOptional st <*> parseOffset st <*> paramOptional st

-- | Pagination of a query from request parameters, within the limits for full or short items.
paramToLimits ::
  WebState ->
  Bool ->
  LimitsParam ->
  ActionT Limits
paramToLimits st full (LimitsParam limitM o startM) = do
  let wl = st.config.limits
  Limits (validateLimit wl full limitM) (fromIntegral o) <$> parseStart st startM

-- | Limit of a query: the requested one or the default, but no more than the maximum.
validateLimit :: WebLimits -> Bool -> Maybe LimitParam -> Word32
validateLimit wl full limitM =
  f m $ maybe d (fromIntegral . (.get)) limitM
  where
    m
      | full && wl.maxFullItemCount > 0 = wl.maxFullItemCount
      | otherwise = wl.maxItemCount
    d = wl.defItemCount
    f a 0 = a
    f 0 b = b
    f a b = min a b

-------------------------------------------------------------------------------
-- Reads in handlers
-------------------------------------------------------------------------------

-- $reads
-- The queries of the store as actions of a handler, with the state of the
-- web server: @getBlock st h@ is @liftIO (st.query.getBlock h)@.

getBestBlock :: WebState -> ActionT (Maybe H.BlockHash)
getBestBlock st = liftIO st.query.getBestBlock

getBlocksAtHeight :: WebState -> H.BlockHeight -> ActionT [H.BlockHash]
getBlocksAtHeight st = liftIO . st.query.getBlocksAtHeight

getBlock :: WebState -> H.BlockHash -> ActionT (Maybe BlockData)
getBlock st = liftIO . st.query.getBlock

getBalance :: WebState -> Address -> ActionT (Maybe Balance)
getBalance st = liftIO . st.query.getBalance

getMempoolPage :: WebState -> Limits -> ActionT [(UnixTime, TxHash)]
getMempoolPage st = liftIO . st.query.getMempoolPage

getAddressesTxs :: WebState -> [Address] -> Limits -> ActionT [TxRef]
getAddressesTxs st = \as -> liftIO . st.query.getAddressesTxs as

getAddressesUnspents :: WebState -> [Address] -> Limits -> ActionT [Unspent]
getAddressesUnspents st = \as -> liftIO . st.query.getAddressesUnspents as

getBalances :: WebState -> [Address] -> ActionT [Balance]
getBalances st = liftIO . st.query.getBalances

getAddressTxs :: WebState -> Address -> Limits -> ActionT [TxRef]
getAddressTxs st = \a -> liftIO . st.query.getAddressTxs a

getAddressUnspents :: WebState -> Address -> Limits -> ActionT [Unspent]
getAddressUnspents st = \a -> liftIO . st.query.getAddressUnspents a

xPubBals :: WebState -> XPubSpec -> ActionT [XPubBal]
xPubBals st = liftIO . st.query.xPubBals

xPubUnspents :: WebState -> XPubSpec -> [XPubBal] -> Limits -> ActionT [XPubUnspent]
xPubUnspents st = \x b -> liftIO . st.query.xPubUnspents x b

xPubTxs :: WebState -> XPubSpec -> [XPubBal] -> Limits -> ActionT [TxRef]
xPubTxs st = \x b -> liftIO . st.query.xPubTxs x b

xPubTxCount :: WebState -> XPubSpec -> [XPubBal] -> ActionT Word32
xPubTxCount st = \x -> liftIO . st.query.xPubTxCount x

getTransaction :: WebState -> TxHash -> ActionT (Maybe Transaction)
getTransaction st = liftIO . C.getTransaction st.query

getTransactions :: WebState -> [TxHash] -> ActionT [Maybe Transaction]
getTransactions st = liftIO . C.getTransactions st.query

getNumTransaction :: WebState -> Word64 -> ActionT [Transaction]
getNumTransaction st = liftIO . C.getNumTransaction st.query

getDefaultBalance :: WebState -> Address -> ActionT Balance
getDefaultBalance st = liftIO . C.getDefaultBalance st.query

blockAtOrAfter :: WebState -> Chain -> UnixTime -> ActionT (Maybe BlockData)
blockAtOrAfter st = \ch -> liftIO . C.blockAtOrAfter st.query ch

blockAtOrBefore :: WebState -> Chain -> UnixTime -> ActionT (Maybe BlockData)
blockAtOrBefore st = \ch -> liftIO . C.blockAtOrBefore st.query ch

blockAtOrAfterMTP :: WebState -> Chain -> UnixTime -> ActionT (Maybe BlockData)
blockAtOrAfterMTP st = \ch -> liftIO . C.blockAtOrAfterMTP st.query ch

---------------
-- Utilities --
---------------

-- | The queries that the web server answers with: those of the database,
-- with the Redis cache, if there is one, in front of the queries about xpubs
-- and of the bodies of transactions. An xpub that the cache does not have is
-- read from the database, and cached for next time if it is large enough.
webQuery :: Store -> StoreRead
webQuery s = case s.cache of
  Nothing -> base
  Just c ->
    (base :: StoreRead)
      { getTxData = fmap listToMaybe' . txData c . pure,
        getManyTxData = txData c,
        xPubBals = \x ->
          cacheXPubBals c x >>= \case
            Just bals -> return bals
            Nothing -> do
              bals <- base.xPubBals x
              cacheRequest c x bals
              return bals,
        xPubUnspents = \x bals l ->
          cacheXPubUnspents c x bals l >>= maybe (base.xPubUnspents x bals l) return,
        xPubTxs = \x bals l ->
          cacheXPubTxs c x l >>= maybe (base.xPubTxs x bals l) return,
        xPubTxCount = \x bals ->
          cacheXPubTxCount c x >>= maybe (base.xPubTxCount x bals) return
      }
  where
    base = Q.databaseRead s.db
    -- bodies from Redis when the cache has them, so that the transactions
    -- of a cached xpub are served in full even before this store has
    -- imported them, and from the database otherwise
    txData _ [] = return []
    txData c ths = do
      cached <- cacheTxData c ths
      let missing = [h | (h, Nothing) <- zip ths cached]
      fromDB <- HashMap.fromList . zip missing <$> Q.getManyTxData s.db missing
      return $
        zipWith
          (\h m -> maybe (fromMaybe Nothing (HashMap.lookup h fromDB)) Just m)
          ths
          cached

-- | The state of the web server, answering without the cache if the flag is
-- 'True', as @nocache=true@ asks.
cacheOr :: Bool -> WebState -> WebState
cacheOr False st = st
cacheOr True st = (st :: WebState) {query = Q.databaseRead st.config.store.db}

-- | The only element of a list of one optional value.
listToMaybe' :: [Maybe a] -> Maybe a
listToMaybe' [x] = x
listToMaybe' _ = Nothing

-- | Middleware that logs every request with its response status: errors at error level, others at debug level.
logIt :: Logger -> Middleware
logIt logger app req respond =
  app req $ \res -> do
    let s = responseStatus res
        msg = fmtReq req <> ": " <> fmtStatus s
    if statusIsSuccessful s
      then logDebug logger msg
      else logError logger msg
    respond res

-- | Describe a request for logs.
fmtReq :: Request -> Text
fmtReq req =
  let m = requestMethod req
      v = httpVersion req
      p = rawPathInfo req
      q = rawQueryString req
   in T.decodeUtf8 (m <> " " <> p <> q <> " " <> cs (show v))

-- | Describe a response status for logs.
fmtStatus :: Status -> Text
fmtStatus s = cs (show (statusCode s)) <> " " <> cs (statusMessage s)

-- | Run a handler, or another if the first one fails.
rescue ::
  ActionT a ->
  ActionT a ->
  ActionT a
x `rescue` y = x `S.catch` \(_ :: S.ScottyException) -> y

-- | Read a request parameter from the path, query string or form.
param ::
  (S.Parsable a) =>
  Text ->
  ActionT a
param t = S.queryParam t `rescue` S.formParam t
