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
-- REST API server of Haskoin Store, running directly on WAI and Warp.
--
-- 'runWeb' serves the endpoints described in "Haskoin.Store.WebCommon" and a
-- set of endpoints compatible with the Blockchain.info API under
-- @\/blockchain\/@. Responses are JSON.
-- New blocks and mempool transactions are streamed at @\/events@, as lines
-- of JSON or over a WebSocket.
--
-- Limits on the size of responses are set with 'WebLimits'. Errors are
-- returned as JSON with an HTTP status that depends on the 'Except'.
-- = Design of the API
--
-- * __One encoding.__ The API speaks JSON, which any language reads, and
--   which compression makes small. A second format would have to be kept
--   in step with the first for every change.
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
-- * __Handlers are plain functions.__ A handler takes the 'WebState' and
--   the request ('Req'), and returns a response in 'IO'. The state holds the
--   queries to answer with in 'query' ('webQuery': the database, with the
--   cache in front of the queries about xpubs); @nocache=true@ is the same
--   handler given a state whose queries read only the database ('cacheOr').
--   The state is made for each request, around one view of the database,
--   so that everything a request reads is of one moment; a store that is a
--   member of a fleet takes the view at the block that its fleet answers
--   from, with the mempool that the fleet shares on top, and has the
--   fleet's cache of xpubs in front of the queries about them
--   ("Haskoin.Store.Fleet.Member").
--   Routes, parameters and the passing of a request to the next route are
--   in "Haskoin.Store.Web.Router", a few dozen lines over WAI. A handler
--   stops early by throwing an 'Except', which becomes the response
--   ('guarded'). There is no web framework and no monad transformer: a
--   framework runs every handler in a stack of its own, and a reader monad
--   in it would hide the state.
module Haskoin.Store.Web
  ( -- * Web
    WebConfig (..),
    Except (..),
    WebLimits (..),
    runWeb,
    runWebSocket,

    -- * Request parsing
    decodeTxBody,

    -- * Logging
    logIt,
    ailments,
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
import Control.Lens ((&), (.~), (?~), (^.))
import Control.Monad
  ( filterM,
    forM_,
    forever,
    guard,
    join,
    unless,
    void,
    when,
    (<=<),
  )
import Control.Monad.Trans (lift)
import Data.Aeson
  ( Encoding,
    ToJSON (..),
    (.=),
  )
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Aeson qualified as A
import Data.Aeson.Encoding
  ( encodingToLazyByteString,
    fromEncoding,
    list,
    unsafeToEncoding,
  )
import Data.Aeson.Types (parseMaybe)
import Data.ByteString.Base16 (decodeBase16, parseBase16)
import Data.ByteString.Builder (Builder, lazyByteString)
import Data.ByteString.Char8 qualified as C
import Data.ByteString.Lazy qualified as L
import Data.Bytes.Get
import Data.Bytes.Put
import Data.Bytes.Serial
import Data.Char (isSpace)
import Data.Default (Default (..))
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.Int (Int64)
import Data.List qualified
import Data.List (nub)
import Data.Maybe
  ( catMaybes,
    fromJust,
    fromMaybe,
    isJust,
    listToMaybe,
    mapMaybe,
    maybeToList,
  )
import Data.Proxy (Proxy (..))
import Data.Serialize (Serialize, decode, encode)
import Data.String (fromString)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Text.Lazy qualified as TL
import Data.Text.Lazy.Encoding qualified as TL
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
    logAt,
    logDebug,
    logError,
    logInfo,
    logWarn,
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
import Haskoin.Store.Database.Query.Layered qualified as L
import Haskoin.Store.Database.Reader
import Haskoin.Store.Fleet.Member
  ( Member (..),
    TipMoved (..),
    answeringCached,
    memberAlive,
    memberEvents,
    memberTip,
  )
import Haskoin.Store.Fleet.Redis (FleetEvent (..))
import Haskoin.Store.Manager
import Haskoin.Store.Web.Docs (swaggerPage, swaggerYaml)
import Haskoin.Store.Web.Events
import Haskoin.Store.Web.Pieces (around, hexOf, listPieces, pieced, piecesResponse)
import Haskoin.Store.Web.Publish qualified as Publish
import Haskoin.Store.Web.Router hiding (header)
import Haskoin.Store.WebCommon
import Haskoin.Transaction
import Haskoin.Util
import NQE
  ( subscribe,
    unsubscribe,
  )
import Network.HTTP.Types
  ( ResponseHeaders,
    Status (..),
    hContentType,
    methodGet,
    methodPost,
    parseQuery,
    renderStdMethod,
    status200,
    status202,
    status203,
    status204,
    status400,
    status404,
    status409,
    status410,
    status413,
    status500,
    status503,
    statusIsClientError,
    statusIsServerError,
  )
import Network.Socket (Socket)
import Network.Wai
  ( Application,
    Middleware,
    Request (..),
    RequestBodyLength (..),
    Response,
    getRequestBodyChunk,
    mapResponseHeaders,
    modifyResponse,
    responseLBS,
    responseStatus,
    responseStream,
    setRequestBodyChunks,
  )
import Network.Wai.Handler.Warp
  ( defaultSettings,
    runSettings,
    runSettingsSocket,
    setHost,
    setPort,
  )
import Network.Wai.Handler.WarpTLS (runTLS, runTLSSocket, tlsSettings)
import Network.Wai.Handler.WebSockets (websocketsOr)
import Network.Wai.Middleware.Autohead (autohead)
import Network.Wai.Middleware.Gzip (gzip)
import Network.Wai.Middleware.Gzip qualified as Gzip
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
import Numeric (showFFloat)
import Numeric.Natural (Natural)
import System.Metrics.StatsD
import Text.Read (readMaybe)
import UnliftIO
  ( TVar,
    atomically,
    bracket,
    bracket_,
    catch,
    catchAny,
    checkSTM,
    Exception,
    fromException,
    handleAny,
    link,
    atomicModifyIORef',
    newIORef,
    newTQueueIO,
    newTVarIO,
    onException,
    race_,
    readIORef,
    readTQueue,
    readTVar,
    readTVarIO,
    registerDelay,
    stateTVar,
    throwIO,
    timeout,
    try,
    tryAny,
    withAsync,
    writeIORef,
    writeTQueue,
    writeTVar,
  )
import UnliftIO.Concurrent (threadDelay)

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
    blockTimeout :: !Word64,
    -- | seconds that a request may take before the log has a warning
    -- about it ('logIt')
    slowRequest :: !Word32
  }
  deriving (Eq, Show)

-- | 200000 items, 5000 full transactions, offset up to 50000, 100 items by
-- default, xpub gaps of 32 and 20, transaction timeout of 30 minutes,
-- block timeout of four hours, and a warning for requests of five seconds.
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
        blockTimeout = 4 * 3600,
        slowRequest = 5
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
    -- | seconds between health checks; there is one at each new block
    -- too
    healthCheckInterval :: !Int,
    -- | log every request, at the level of information ('logIt')
    accessLog :: !Bool,
    -- | URL of the JSON-RPC interface of a full node, with its user and
    -- password, to publish transactions through ('sendNode'); 'Nothing'
    -- to publish through peers only
    nodeRpc :: !(Maybe String),
    -- | a certificate and its private key, in files in PEM format, to
    -- serve HTTPS instead of HTTP; 'Nothing' for HTTP, which is the
    -- default
    tls :: !(Maybe (FilePath, FilePath)),
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
    -- | the queries to answer with, the cache included ('webQuery'): those
    -- of one view of the database, taken for the request
    query :: !StoreRead,
    -- | the same queries without the cache, on the same view
    direct :: !StoreRead,
    -- | whether Redis answered the last time it was asked; always 'True'
    -- for a store without a cache
    cacheUp :: !(TVar Bool),
    -- | the events that the clients of @\/events@ read ('feedEvents')
    eventLog :: !EventLog,
    -- | the transactions that were handed over and that the store asks
    -- for until it has them ("Haskoin.Store.Web.Publish")
    asking :: !Publish.Asking
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
createMetrics s = do
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
  IO a ->
  IO a
withGaugeIncrease st gf go =
  case st.metrics of
    Nothing -> go
    Just m -> withGaugeIO (gf m) go

-- | Record how long a handler takes in a timing of the web metrics.
withMetrics ::
  WebState ->
  (WebMetrics -> StatTiming) ->
  IO a ->
  IO a
withMetrics st df go =
  case st.metrics of
    Nothing -> go
    Just m -> bracket time (stop m) (const go)
  where
    time = round . (* 1000) <$> getPOSIXTime
    stop m t1 = do
      t2 <- time
      addTiming (df m) (t2 - t1)

-------------------
-- Path Handlers --
-------------------

-- | Run the web server forever on the host and port of the configuration.
-- Also starts a thread that updates prices, unless the Blockchain.info
-- endpoints are disabled, and a thread that runs health checks.
--
-- = HTTPS
--
-- With a certificate and a key in 'tls', the server speaks HTTPS on that
-- port, and nothing else: TLS 1.2 and 1.3, and HTTP\/2 for clients that
-- ask for it. It is off by default. A public service usually sits behind
-- a proxy that has the certificate, limits requests and spreads them over
-- servers; this is for a server by itself, and for links between machines
-- that should not be in the clear. The files are read when the server
-- starts, so a renewed certificate takes a restart.
runWeb :: WebConfig -> IO ()
runWeb config = serveWeb config $ case config.tls of
  Nothing -> runSettings settings
  Just (certificate, key) -> runTLS (tlsSettings certificate key) settings
  where
    settings = setPort config.port . setHost (fromString config.host) $ defaultSettings

-- | Like 'runWeb', on a socket that already listens; the host and port of the
-- configuration are ignored. Binding port 0 and passing the socket gives a
-- free port with no window in which another program can take it, which
-- finding a free port first and binding it later has.
runWebSocket :: WebConfig -> Socket -> IO ()
runWebSocket config sock = serveWeb config $ case config.tls of
  Nothing -> runSettingsSocket defaultSettings sock
  Just (certificate, key) -> runTLSSocket (tlsSettings certificate key) defaultSettings sock

-- | Run the web server with a function of Warp that serves an application.
-- The application is the router ("Haskoin.Store.Web.Router") with the
-- routes of 'handlePaths', behind the middleware for WebSocket events, for
-- logs and for the header that lets any origin make requests.
serveWeb :: WebConfig -> (Application -> IO ()) -> IO ()
serveWeb config serve = do
  ticker <- newTVarIO HashMap.empty
  metrics <- mapM createMetrics config.stats
  session <- Wreq.Session.newAPISession
  first <- runHealthCheck
  health <- newTVarIO first
  -- A member of a fleet takes no turn before its store is known to be
  -- well. A member is made ready to take turns, since a store without a
  -- web server has nothing else to say when; with one, a store that had
  -- just started took a turn before its first check, reported the blocks
  -- that it had then, and with nobody else there moved the fleet to them.
  forM_ config.store.fleet $ \member -> atomically (writeTVar member.healthy (isOK first))
  cacheUp <- cacheCheck >>= newTVarIO
  eventLog <- newEventLog
  asking <- Publish.newAsking
  let db = config.store.db
      stateOf q = WebState {query = webQuery config.store cacheUp q, direct = q, ..}
      routesOf s = map (guarded s) (concatMap withPost (handlePaths s config))
      -- outside of a request, each query has a view of its own; a request
      -- has one view for all that it reads, which is let go when its
      -- response is sent
      st = stateOf (L.standaloneRead db)
      local answer = L.withView db (answer . routesOf . stateOf . L.layeredRead)
      scoped answer = case config.store.fleet of
        Nothing -> local answer
        Just member ->
          readTVarIO cacheUp >>= \case
            True -> shared member (3 :: Int) False answer
            -- without Redis there is no fleet to answer with
            False -> local answer
      -- as a member of a fleet: from the view at the fleet tip, with the
      -- pool that the fleet shares on top. The tip is taken to be the one
      -- that the member read at its last turn, which saves asking Redis
      -- for it: the first read of the pool checks it, and a request that
      -- reads nothing from the pool checks it when its handler is done. If
      -- it is not the tip any more, the request is answered again, from the
      -- tip that Redis has.
      shared member tries fresh answer = do
        known <- if fresh then return Nothing else fmap encode <$> readTVarIO member.tip
        asked <- case known of
          Just tip -> return (Just (Just tip))
          Nothing -> timeout 2000000 (memberTip member) `catchAny` \_ -> return Nothing
        case asked of
          Nothing -> atomically (writeTVar cacheUp False) >> local answer
          -- no store has reported yet
          Just Nothing -> local answer
          Just (Just tip) -> do
            handled <- newIORef False
            let marked confirm r = r {handler = \req -> r.handler req <* confirm <* writeIORef handled True} :: Route
                -- extended public keys through the fleet's cache of
                -- them, unless a request asks for the database
                inFleet plain cached confirm =
                  answer (map (marked confirm) (routesOf WebState {query = cached, direct = plain, ..}))
            try (answeringCached member tip inFleet) >>= \case
              Right (Just sent) -> return sent
              Right Nothing
                -- a tip from the member's last turn of which there is no
                -- view may not be the tip any more
                | not fresh && tries > 0 -> shared member (tries - 1) True answer
                | otherwise -> behind answer
              Left TipMoved ->
                -- the tip moved while the handler was reading: answer
                -- again from the new one, unless the response is on its way
                readIORef handled >>= \case
                  True -> throwIO TipMoved
                  False
                    | tries > 0 -> shared member (tries - 1) True answer
                    | otherwise -> behind answer
      behind answer = answer [r {handler = behindResponse st} :: Route | r <- routes]
      -- the stream of events never ends, and reads nothing from the
      -- database: answered like the rest it would hold a view, and the
      -- data under it, for as long as its client listens
      app request
        | pathInfo request == ["events"] = routerScoped routes ($ routes) notFound request
        | otherwise = routerScoped routes scoped notFound request
      routes = routesOf st
      notFound _ NoRoute = errorResponse st ThingNotFound
      notFound _ (Unknown ks) =
        errorResponse st . UserError $
          "Unknown parameter: " <> T.unpack (T.intercalate ", " ks)
  saidWell <- newIORef Nothing
  saidRedis <- newIORef True
  pinged <- newIORef 0
  -- set when the store has a new best block, for the health check to run
  -- at once
  newBlock <- newTVarIO False
  withAsync (priceUpdater session ticker) $ \_a1 ->
    withAsync (healthCheckLoop health cacheUp saidWell saidRedis pinged newBlock) $ \_a2 ->
      withAsync (feedEvents st) $ \a3 ->
        withAsync (askingLoop st) $ \_a4 ->
          link a3 >> do
            let logged = logIt config.logger config.accessLog config.limits.slowRequest
            serve . webSocketEvents st . logged . anyOrigin . preflight . autohead . compressed . cappedBody st $ app
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
    cacheCheck = case (config.store.cache, config.store.fleet) of
      (Just cache, _) -> cacheAlive cache
      (_, Just member) -> memberAlive member
      _ -> return True
    -- The check is run every second by default, and at once when the
    -- store has a new best block, which is when its answer changes most:
    -- /health answers from the last result, and a load balancer or a
    -- fleet that acts on a result of half a minute ago sends requests to
    -- a server that fell behind. A check reads a handful of keys, so it
    -- costs little to run often; but a store that is catching up imports
    -- thousands of small blocks a second, and a check for each would be
    -- work for nothing, so there is a tenth of a second between two.
    healthCheckLoop v up saidWell saidRedis pinged newBlock =
      bracket (subscribe config.store.pub (heard newBlock)) (unsubscribe config.store.pub) $ \_ ->
        loopForever $ do
          threadDelay 100000
          due <- registerDelay (config.healthCheckInterval * 1000 * 1000)
          atomically $ do
            block <- readTVar newBlock
            time <- readTVar due
            checkSTM (block || time)
            writeTVar newBlock False
          h <- runHealthCheck
          atomically (writeTVar v h)
          tellHealth saidWell h
          -- a store whose checks fail stops taking its turns as a member
          -- of its fleet, so that it does not hold the others back
          forM_ config.store.fleet $ \member -> atomically (writeTVar member.healthy (isOK h))
          -- Redis is asked less often, every five seconds: it is the one
          -- part of the check that leaves the program, and waits up to
          -- two seconds for a Redis that is away. Requests find out by
          -- themselves that Redis is away; this is how the store finds
          -- out that it is back.
          now <- getPOSIXTime
          last' <- readIORef pinged
          when (now - last' >= 5) $ do
            writeIORef pinged now
            redis <- cacheCheck
            atomically (writeTVar up redis)
            tellRedis saidRedis redis
    heard newBlock = \case
      StoreBestBlock _ -> writeTVar newBlock True
      _ -> return ()
    -- A change of health is logged, and nothing while it stays as it was:
    -- what /health answers is otherwise only known to whoever asks it. A
    -- store is not healthy while it syncs, which is no cause for alarm,
    -- so only a store that was healthy gets a warning.
    health' = withSource "Health" config.logger
    tellHealth said h = do
      before <- readIORef said
      writeIORef said (Just (isOK h))
      case (before, isOK h) of
        (Just True, False) -> logWarn health' ("No longer healthy: " <> ailments h)
        (Nothing, False) -> logInfo health' ("Not healthy yet: " <> ailments h)
        (Just False, True) -> logInfo health' "Healthy again"
        (Nothing, True) -> logInfo health' "Healthy"
        _ -> return ()
    -- the same for the Redis that a store with a cache answers through;
    -- /health says 203 without it
    tellRedis said redis = do
      before <- readIORef said
      writeIORef said redis
      case (before, redis) of
        (True, False) -> logWarn health' "Redis does not answer: answering from the database"
        (False, True) -> logInfo health' "Redis answers again"
        _ -> return ()
    -- what a browser asks before a request from another site that is not
    -- of the plainest kind: which methods and headers it may use. Without
    -- an answer it does not send the request.
    preflight inner req respond
      | requestMethod req == "OPTIONS" =
          respond $
            responseLBS
              status204
              [ ("Allow", allowed),
                ("Access-Control-Allow-Methods", allowed),
                ("Access-Control-Allow-Headers", "Accept, Content-Type"),
                ("Access-Control-Max-Age", "86400")
              ]
              ""
      | otherwise = inner req respond
    allowed = "GET, HEAD, POST, DELETE, OPTIONS"
    -- answers are compressed for a client that says it reads gzip: JSON
    -- shrinks several times over. The events are a stream that has to
    -- leave as it is written, and are left alone.
    compressed inner req
      | pathInfo req == ["events"] = inner req
      | otherwise = gzip Gzip.defaultGzipSettings inner req
    -- any origin may ask, and whether the answer is compressed depends on
    -- what the client said it reads, which a cache between the client and
    -- the server has to know: it must not give gzip to a client that
    -- cannot read it.
    anyOrigin = modifyResponse (mapResponseHeaders varied)
    varied headers =
      [("Access-Control-Allow-Origin", "*"), ("Vary", "Accept-Encoding")]
        <> filter ((/= "Vary") . fst) headers

-- | Answer the errors of the handler of a route: an 'Except' with its
-- status and JSON, any other error as a server error, after logging it. A
-- request passed on to the next route goes on.
guarded :: WebState -> Route -> Route
guarded st r = r {handler = \req -> r.handler req `catchAny` answer req}
  where
    answer req e
      | Just Next <- fromException e = throwIO Next
      | Just TipMoved <- fromException e = throwIO TipMoved
      | Just x <- fromException e = errorResponse st x
      | Just g <- fromException e = goneResponse st g
      | otherwise = do
          logError st.config.logger $ fmtReq req.wai <> ": " <> cs (show e)
          errorResponse st ServerError

-- | A transaction that a block replaced ('Replaced'), asked for by its id:
-- answered 410, Gone, with the transaction that replaced it and the height
-- of its block, where one that never was is 404.
--
-- The errors of the API are those of haskoin-store-data, whose client
-- reads them. This one is not among them yet, so it is answered here, in
-- the same form: the client takes it for an error it does not know.
data Gone = Gone !TxHash !Replaced
  deriving (Show)

instance Exception Gone

-- | The answer to 'Gone', counted with the errors of clients.
goneResponse :: WebState -> Gone -> IO Response
goneResponse st (Gone txid r) = do
  forM_ st.metrics $ \m -> incrementCounter m.clientErrors 1
  return . responseLBS status410 [(hContentType, "application/json")] . A.encode $
    A.object
      [ "error" .= ("replaced" :: Text),
        "message"
          .= T.unwords
            [ "Transaction",
              txHashToHex txid,
              "was replaced by",
              txHashToHex r.by,
              "in the block at height",
              T.pack (show r.height)
            ],
        "replaced-by" .= r.by,
        "height" .= r.height
      ]

-- | Not found: 'Gone' if a block replaced the transaction, else 404.
notFoundTx :: WebState -> TxHash -> IO a
notFoundTx st txid = st.query.getReplaced txid >>= maybe (raise st ThingNotFound) (throwIO . Gone txid)

-- | What a store that is a member of a fleet answers when it has no view of
-- the block that the fleet answers from: 503, so that a load balancer sends
-- the request to another store. Any answer of its own could differ from
-- those of the other stores, which is what a fleet is there to prevent.
behindResponse :: WebState -> Req -> IO Response
behindResponse st _ = errorResponse st BehindFleet

-- | The response to an error, in JSON, counted in the metrics.
errorResponse :: WebState -> Except -> IO Response
errorResponse st e = do
  forM_ st.metrics $ \m ->
    mapM_ (`incrementCounter` 1) $
      if
        | statusIsClientError (errStatus e) -> Just m.clientErrors
        | statusIsServerError (errStatus e) -> Just m.serverErrors
        | otherwise -> Nothing
  return $ responseLBS (errStatus e) ((hContentType, "application/json") : again) (A.encode e)
  where
    -- a server behind its fleet catches up within a block or is taken out
    -- of rotation: a client or a load balancer that reads this header
    -- tries again in a second, and elsewhere meanwhile
    again = [("Retry-After", "1") | e == BehindFleet]

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
      Wreq.asJSON
        =<< Wreq.Session.postWith opts session url body
    return $ r ^. Wreq.responseBody
  where
    err _ = do
      logWarn logger "Could not get historic prices"
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
  let err e = logWarn logger $ "Could not get prices: " <> cs (show e)
  handleAny err $ do
    r <- Wreq.asJSON =<< Wreq.Session.get session u
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

-- | Read no more of the body of a transaction to publish than a
-- transaction can be long: twice 'Publish.maxTxSize' in hexadecimal, and
-- some room for white space. More is answered 413, by its length when the
-- client gave it, and else as soon as that much was read.
--
-- The router reads the body whole before it knows what to do with it.
-- Without this, anyone could have the server hold a body of any size.
-- Other requests with a body are forms of parameters, and are not capped
-- here.
cappedBody :: WebState -> Middleware
cappedBody st app request respond
  | requestMethod request == methodPost,
    filter (not . T.null) (pathInfo request) == ["transactions"] =
      case requestBodyLength request of
        KnownLength n | n > fromIntegral most -> refuse
        _ -> do
          left <- newIORef most
          let chunk = do
                bytes <- getRequestBodyChunk request
                now <- atomicModifyIORef' left (\l -> let l' = l - C.length bytes in (l', l'))
                when (now < 0) (throwIO RequestTooLarge)
                return bytes
          app (setRequestBodyChunks chunk request) respond `catch` \case
            RequestTooLarge -> refuse
            e -> throwIO e
  | otherwise = app request respond
  where
    most = maxPublishBody st.config.store.net
    refuse = errorResponse st RequestTooLarge >>= respond

-- | The most bytes of the body of @POST \/transactions@.
maxPublishBody :: Network -> Int
maxPublishBody net = 2 * Publish.maxTxSize net + 4096

-- | Stop handling the request, and answer with an error ('guarded').
raise :: WebState -> Except -> IO a
raise _ = throwIO

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
errStatus BehindFleet = status503

-- | A route, and with it the same route under @POST@ if it takes a list in
-- a parameter: addresses, blocks, heights or transactions.
--
-- A list in the query string has to fit in a URL, and proxies cut those
-- off at a few thousand characters: some hundred addresses. The router
-- reads the parameters of a form in the body as it reads those of the
-- query string, so under @POST@ the same parameters go in the body, with
-- @Content-Type: application\/x-www-form-urlencoded@, and have no such
-- limit. @\/transactions@ is left out: @POST@ there publishes a
-- transaction.
withPost :: Route -> [Route]
withPost r
  | r.method == methodGet && takesList && r.pattern /= [Left "transactions"] = [r, r {method = methodPost}]
  | otherwise = [r]
  where
    takesList = any (`elem` r.accepts) ["addresses", "blocks", "heights", "txids"]

-- | The parameters of @\/events@ that say what a client hears about from
-- the start ('eventsOpen').
eventParams :: [Text]
eventParams = ["quiet", "addresses", "xpub", "derive", "filter", "functions", "tweak", "flags"]

-- | The routes of every endpoint, except those disabled in the configuration.
handlePaths :: WebState -> WebConfig -> [Route]
handlePaths st cfg =
  [ marshalled ["notx"] (\r -> GetBlockBest <$> dflt r) (handleBlockBest st),
    marshalledList ["notx"] (\r -> GetBlockHeight <$> cap r <*> dflt r) (handleBlockHeight st),
    marshalled ["notx"] (\r -> GetBlockTime <$> cap r <*> dflt r) (handleBlockTime st),
    marshalled ["notx"] (\r -> GetBlockMTP <$> cap r <*> dflt r) (handleBlockMTP st),
    marshalled ["notx"] (\r -> GetBlock <$> cap r <*> dflt r) (handleBlock st),
    marshalled [] (\r -> GetTx <$> cap r) (handleTx st),
    plain [] (\r -> GetTxRaw <$> cap r) (handleTxRaw st),
    pathPosted [] (\r -> PostTx <$> parseTxBody st r) (handlePostTx st),
    plainList limits (\r -> GetAddrTxs <$> cap r <*> lim r) (handleAddrTxs st),
    marshalled [] (\r -> GetAddrBalance <$> cap r) (handleAddrBalance st),
    marshalledList limits (\r -> GetAddrUnspent <$> cap r <*> lim r) (handleAddrUnspent st),
    plainList [] (\_ -> return GetPeers) (handlePeers st),
    pathHealth st,
    plainList ["limit", "offset"] (\r -> GetMempool <$> opt r <*> off r) (handleMempool st),
    marshalledList ["blocks", "notx"] (\r -> GetBlocks <$> need r <*> dflt r) (handleBlocks st),
    pieces (.blockRaw) [] (\r -> GetBlockRaw <$> cap r) (handleBlockRaw st),
    pieces (.blockRaw) [] (\_ -> return GetBlockBestRaw) (handleBlockBestRaw st),
    marshalledList ["notx"] (\r -> GetBlockLatest <$> dflt r) (handleBlockLatest st),
    marshalledList ["heights", "notx"] (\r -> GetBlockHeights <$> need r <*> dflt r) (handleBlockHeights st),
    pieces (.blockRaw) [] (\r -> GetBlockHeightRaw <$> cap r) (handleBlockHeightRaw st),
    pieces (.blockRaw) [] (\r -> GetBlockTimeRaw <$> cap r) (handleBlockTimeRaw st),
    pieces (.blockRaw) [] (\r -> GetBlockMTPRaw <$> cap r) (handleBlockMTPRaw st),
    marshalledList ["txids"] (\r -> GetTxs <$> need r) (handleTxs st),
    plain ["txids"] (\r -> GetTxsRaw <$> need r) (handleTxsRaw st),
    marshalledPieces (.txBlock) [] (\r -> GetTxsBlock <$> cap r) (handleTxsBlock st),
    pieces (.txBlockRaw) [] (\r -> GetTxsBlockRaw <$> cap r) (handleTxsBlockRaw st),
    plain [] (\r -> GetTxAfter <$> cap r <*> cap r) (handleTxAfter st),
    plainList ("addresses" : limits) (\r -> GetAddrsTxs <$> need r <*> lim r) (handleAddrsTxs st),
    marshalledPieces (.addressTxFull) limits (\r -> GetAddrTxsFull <$> cap r <*> lim r) (handleAddrTxsFull st),
    marshalledPieces
      (.addressTxFull)
      ("addresses" : limits)
      (\r -> GetAddrsTxsFull <$> need r <*> lim r)
      (handleAddrsTxsFull st),
    marshalledList ["addresses"] (\r -> GetAddrsBalance <$> need r) (handleAddrsBalance st),
    marshalledList ("addresses" : limits) (\r -> GetAddrsUnspent <$> need r <*> lim r) (handleAddrsUnspent st),
    get "/events" ("after" : eventParams) (handleEvents st),
    get "/dbstats" [] (handleDbStats st),
    -- the reference of the API, as a page and as the file that it shows
    get "/" [] (\_ -> file "text/html; charset=utf-8" swaggerPage),
    get "/swagger.yaml" [] (\_ -> file "application/yaml" swaggerYaml)
  ]
    <> (if cfg.noXPub then [] else xpubRoutes)
    <> (if cfg.noBlockchainInfo then [] else binfoRoutes)
  where
    file kind bytes = return (responseLBS status200 [(hContentType, kind)] (L.fromStrict bytes))
    net = cfg.store.net
    -- the parameters that routes accept, beyond those of each route
    limits = ["limit", "offset", "height"]
    active = ["active", "activeP2SH", "activeBech32"]
    -- the Blockchain.info endpoints accept format=json ('jsonOnly')
    multiaddrParams =
      ["format", "onlyShow", "n", "offset", "noCompact", "no_compact", "currency", "cashaddr", "txidindex", "filter"]
        <> active
    rawaddrParams = ["format", "derive", "limit", "offset", "txidindex"]
    balanceParams = "format" : "cashaddr" : active
    unspentParams = "format" : "limit" : "confirmations" : "txidindex" : active
    historyParams = "format" : "start" : "end" : "currency" : active
    -- the parameters of a request
    cap, need :: (Param a) => Req -> IO a
    cap = paramCapture st
    need = paramRequired st
    dflt :: (Default a, Param a) => Req -> IO a
    dflt = paramDef st
    opt :: (Param a) => Req -> IO (Maybe a)
    opt = paramOptional st
    lim = parseLimits st
    off = parseOffset st
    -- routes by how they encode their responses: in the JSON of the
    -- network, as a list of those, or in plain JSON
    marshalled ::
      (ApiResource a b, MarshalJSON Network b) => [Text] -> (Req -> IO a) -> (a -> IO b) -> Route
    marshalled ps p h = pathCompact ps p h (marshalEncoding net)
    marshalledList ::
      (ApiResource a (SerialList b), MarshalJSON Network b) => [Text] -> (Req -> IO a) -> (a -> IO [b]) -> Route
    marshalledList ps p h =
      pathCompact ps p (fmap SerialList . h) (list (marshalEncoding net) . (.get))
    -- routes of what is answered while it is read: a list of items in
    -- the JSON of the network, or pieces that the handler made
    marshalledPieces ::
      (ApiResource a (SerialList b), MarshalJSON Network b) =>
      (WebMetrics -> StatTiming) ->
      [Text] ->
      (Req -> IO a) ->
      (a -> IO [IO [b]]) ->
      Route
    marshalledPieces df ps p h = pieces df ps p (listPieces (marshalEncoding net) <=< h)
    pieces ::
      (ApiResource a b) =>
      (WebMetrics -> StatTiming) ->
      [Text] ->
      (Req -> IO a) ->
      (a -> IO [IO Builder]) ->
      Route
    pieces = pathPieces st
    plain :: (ApiResource a b, ToJSON b) => [Text] -> (Req -> IO a) -> (a -> IO b) -> Route
    plain ps p h = pathCompact ps p h toEncoding
    plainList ::
      (ApiResource a (SerialList b), ToJSON b) => [Text] -> (Req -> IO a) -> (a -> IO [b]) -> Route
    plainList ps p h = plain ps p (fmap SerialList . h)
    xpubRoutes =
      [ plain ["derive", "nocache"] (\r -> GetXPub <$> cap r <*> dflt r <*> dflt r) (handleXPub st),
        plainList
          ("derive" : "nocache" : limits)
          (\r -> GetXPubTxs <$> cap r <*> dflt r <*> lim r <*> dflt r)
          (handleXPubTxs st),
        marshalledPieces
          (.xpubTxFull)
          ("derive" : "nocache" : limits)
          (\r -> GetXPubTxsFull <$> cap r <*> dflt r <*> lim r <*> dflt r)
          (handleXPubTxsFull st),
        marshalledList
          ["derive", "nocache"]
          (\r -> GetXPubBalances <$> cap r <*> dflt r <*> dflt r)
          (handleXPubBalances st),
        marshalledList
          ("derive" : "nocache" : limits)
          (\r -> GetXPubUnspent <$> cap r <*> dflt r <*> lim r <*> dflt r)
          (handleXPubUnspent st),
        plain ["derive"] (\r -> DelCachedXPub <$> cap r <*> dflt r) (handleDelXPub st)
      ]
    binfoRoutes =
      [ post "/blockchain/multiaddr" multiaddrParams (jsonOnly (handleMultiAddr st)),
        get "/blockchain/multiaddr" multiaddrParams (jsonOnly (handleMultiAddr st)),
        get "/blockchain/balance" balanceParams (jsonOnly (handleShortBal st)),
        post "/blockchain/balance" balanceParams (jsonOnly (handleShortBal st)),
        get "/blockchain/rawaddr/:addr" rawaddrParams (jsonOnly (handleRawAddr st)),
        get "/blockchain/address/:addr" rawaddrParams (jsonOnly (handleRawAddr st)),
        get "/blockchain/xpub/:addr" rawaddrParams (jsonOnly (handleRawAddr st)),
        post "/blockchain/unspent" unspentParams (jsonOnly (handleBinfoUnspent st)),
        get "/blockchain/unspent" unspentParams (jsonOnly (handleBinfoUnspent st)),
        get "/blockchain/rawtx/:txid" ["format", "txidindex"] (handleBinfoTx st),
        get "/blockchain/rawblock/:block" ["format", "txidindex"] (handleBinfoBlock st),
        get "/blockchain/latestblock" ["format", "txidindex"] (jsonOnly (handleBinfoLatest st)),
        get
          "/blockchain/unconfirmed-transactions"
          ["format", "limit", "offset", "txidindex"]
          (jsonOnly (handleBinfoMempool st)),
        get "/blockchain/block-height/:height" ["format", "txidindex"] (jsonOnly (handleBinfoBlockHeight st)),
        get "/blockchain/blocks/:milliseconds" ["format"] (jsonOnly (handleBinfoBlocksDay st)),
        get "/blockchain/export-history" historyParams (jsonOnly (handleBinfoHistory st)),
        post "/blockchain/export-history" historyParams (jsonOnly (handleBinfoHistory st)),
        get "/blockchain/q/addresstohash/:addr" [] (handleBinfoAddrToHash st),
        get "/blockchain/q/hashtoaddress/:hash" [] (handleBinfoHashToAddr st),
        get "/blockchain/q/addrpubkey/:pubkey" [] (handleBinfoAddrPubkey st),
        get "/blockchain/q/pubkeyaddr/:addr" [] (handleBinfoPubKeyAddr st),
        get "/blockchain/q/hashpubkey/:pubkey" [] (handleBinfoHashPubkey st),
        get "/blockchain/q/getblockcount" [] (handleBinfoGetBlockCount st),
        get "/blockchain/q/latesthash" [] (handleBinfoLatestHash st),
        get "/blockchain/q/bcperblock" [] (handleBinfoSubsidy st),
        get "/blockchain/q/txtotalbtcoutput/:txid" [] (handleBinfoTotalOut st),
        get "/blockchain/q/txtotalbtcinput/:txid" [] (handleBinfoTotalInput st),
        get "/blockchain/q/txfee/:txid" [] (handleBinfoTxFees st),
        get "/blockchain/q/txresult/:txid/:addr" [] (handleBinfoTxResult st),
        get "/blockchain/q/getreceivedbyaddress/:addr" [] (handleBinfoReceived st),
        get "/blockchain/q/getsentbyaddress/:addr" [] (handleBinfoSent st),
        get "/blockchain/q/addressbalance/:addr" [] (handleBinfoAddrBalance st),
        get "/blockchain/q/addressfirstseen/:addr" [] (handleFirstSeen st)
      ]

-- | A handler of the Blockchain.info endpoints that answer in JSON only. They
-- accept @format=json@, which clients written for Blockchain.info often
-- send, and refuse any other format rather than answering in JSON anyway.
jsonOnly :: (Req -> IO Response) -> Req -> IO Response
jsonOnly h req = do
  paramOr req json "format" ()
  h req
  where
    json "json" = Just ()
    json _ = Nothing

-- | The route of an API resource: parse the request, run the handler, and
-- encode the response as JSON.
pathCompact :: (ApiResource a b) => [Text] -> (Req -> IO a) -> (a -> IO b) -> (b -> Encoding) -> Route
pathCompact ps parser action encJson =
  route (renderStdMethod (resourceMethod proxy)) (capturePath proxy) ps $ \req -> do
    res <- action =<< parser req
    return $ responseLBS status200 jsonType (encodingToLazyByteString (encJson res))
  where
    toProxy :: (Req -> IO a) -> Proxy a
    toProxy = const Proxy
    proxy = toProxy parser

-- | The route of an API resource whose handler chooses the status of its
-- answer: @POST \/transactions@, which answers 202 for a transaction that
-- it handed over.
pathPosted :: (ApiResource a b) => [Text] -> (Req -> IO a) -> (a -> IO (Status, Encoding)) -> Route
pathPosted ps parser action =
  route (renderStdMethod (resourceMethod proxy)) (capturePath proxy) ps $ \req -> do
    (status, res) <- action =<< parser req
    return $ responseLBS status jsonType (encodingToLazyByteString res)
  where
    toProxy :: (Req -> IO a) -> Proxy a
    toProxy = const Proxy
    proxy = toProxy parser

-- | The route of an API resource that is answered while it is read, as
-- "Haskoin.Store.Web.Pieces" explains: the handler gives the bytes of the
-- response in pieces.
pathPieces ::
  (ApiResource a b) =>
  WebState ->
  (WebMetrics -> StatTiming) ->
  [Text] ->
  (Req -> IO a) ->
  (a -> IO [IO Builder]) ->
  Route
pathPieces st df ps parser action =
  route (renderStdMethod (resourceMethod proxy)) (capturePath proxy) ps $ \req ->
    inPieces st df jsonType (action =<< parser req)
  where
    toProxy :: (Req -> IO a) -> Proxy a
    toProxy = const Proxy
    proxy = toProxy parser

-- | A response in pieces, timed. The timing counts the whole response, the
-- time that the client takes to read it included.
inPieces :: WebState -> (WebMetrics -> StatTiming) -> ResponseHeaders -> IO [IO Builder] -> IO Response
inPieces st df headers pieces = do
  began <- now
  let done = forM_ st.metrics $ \m -> now >>= \ended -> addTiming (df m) (ended - began)
  (pieces >>= piecesResponse headers done) `onException` done
  where
    now = round . (* 1000) <$> getPOSIXTime :: IO Int

-- | The headers of a response in JSON.
jsonType :: ResponseHeaders
jsonType = [(hContentType, "application/json")]

-- | Respond with JSON.
streamEncoding :: Encoding -> IO Response
streamEncoding e =
  return $
    responseLBS status200 [(hContentType, "application/json; charset=utf-8")] (encodingToLazyByteString e)

-- | Respond with text.
textResponse :: TL.Text -> IO Response
textResponse t =
  return $
    responseLBS status200 [(hContentType, "text/plain; charset=utf-8")] (TL.encodeUtf8 t)

-- GET Block / GET Blocks --

-- | Handler for @GET \/block\/:block@: a block by hash.
handleBlock ::
  WebState ->
  GetBlock ->
  IO BlockData
handleBlock st (GetBlock h (NoTx noTx)) = withMetrics st (.block) $ do
  st.query.getBlock h >>= \case
    Nothing ->
      raise st ThingNotFound
    Just b -> do
      return $ pruneTx noTx b

-- | Blocks by hash, without duplicates, leaving out unknown ones.
getBlocks ::
  WebState ->
  [H.BlockHash] ->
  Bool ->
  IO [BlockData]
getBlocks st hs notx =
  (pruneTx notx <$>) . catMaybes <$> mapM (st.query.getBlock) (nub hs)

-- | Handler for @GET \/blocks?blocks=…@: several blocks by hash.
handleBlocks ::
  WebState ->
  GetBlocks ->
  IO [BlockData]
handleBlocks st (GetBlocks hs (NoTx notx)) =
  withMetrics st (.block) $
    getBlocks st hs notx

-- | Keep only the coinbase in the list of transactions of a block, if the flag is 'True'.
pruneTx :: Bool -> BlockData -> BlockData
pruneTx False b = b
pruneTx True BlockData {..} = BlockData {txs = take 1 txs, ..}

-- GET BlockRaw --

-- | Handler for @GET \/block\/:block\/raw@: a block by hash, serialized as in the peer-to-peer protocol.
handleBlockRaw :: WebState -> GetBlockRaw -> IO [IO Builder]
handleBlockRaw st (GetBlockRaw h) = rawResult <$> getRawBlock st h

-- | The pieces of a block by hash, serialized, in hexadecimal. Responds
-- with an error if not found.
getRawBlock :: WebState -> H.BlockHash -> IO [IO Builder]
getRawBlock st h =
  st.query.getBlock h >>= maybe (raise st ThingNotFound) (return . rawBlock st.query)

-- | The pieces of a block serialized as in the peer-to-peer protocol, in
-- hexadecimal: its header and the number of its transactions, then its
-- transactions as they are read. The first piece has transactions too, so
-- that a response does not begin before something of them was read.
rawBlock :: StoreRead -> BlockData -> [IO Builder]
rawBlock q b =
  around (hexOf b.header <> hexOf (VarInt (fromIntegral (length b.txs)))) mempty $
    pieced pieceSize (fmap (foldMap (hexOf . transactionData) . catMaybes) . C.getTransactions q) b.txs

-- | The pieces of the JSON of a 'RawResult', from those of the hexadecimal.
rawResult :: [IO Builder] -> [IO Builder]
rawResult = around "{\"result\":\"" "\"}"

-- | The pieces of the JSON of a 'RawResultList', from those of the
-- hexadecimal of each of its items.
rawResultList :: [[IO Builder]] -> [IO Builder]
rawResultList items =
  around "[" "]" (concat [around (comma <> "\"") "\"" item | (comma, item) <- zip ("" : repeat ",") items])

-- GET BlockBest / BlockBestRaw --

-- | Handler for @GET \/block\/best@: the best block.
handleBlockBest ::
  WebState ->
  GetBlockBest ->
  IO BlockData
handleBlockBest st (GetBlockBest (NoTx notx)) =
  withMetrics st (.block) $
    (bestBlock st.query) >>= maybe (raise st ThingNotFound) (return . pruneTx notx)

-- | Handler for @GET \/block\/best\/raw@: the best block, serialized.
handleBlockBestRaw :: WebState -> GetBlockBestRaw -> IO [IO Builder]
handleBlockBestRaw st _ =
  st.query.getBestBlock >>= \case
    Nothing -> raise st ThingNotFound
    Just h -> rawResult <$> getRawBlock st h

-- GET BlockLatest --

-- | Handler for @GET \/block\/latest@: the best block and its ancestors, up to 100 blocks.
handleBlockLatest ::
  WebState ->
  GetBlockLatest ->
  IO [BlockData]
handleBlockLatest st (GetBlockLatest (NoTx noTx)) =
  withMetrics st (.block) $
    st.query.getBestBlock >>= maybe (raise st ThingNotFound) (go [] <=< st.query.getBlock)
  where
    go acc Nothing = return $ reverse acc
    go acc (Just b)
      | b.height <= 0 = return $ reverse acc
      | length acc == 99 = return . reverse $ pruneTx noTx b : acc
      | otherwise = go (pruneTx noTx b : acc) =<< st.query.getBlock b.header.prev

-- GET BlockHeight / BlockHeights / BlockHeightRaw --

-- | Handler for @GET \/block\/height\/:height@: the blocks at a height.
handleBlockHeight ::
  WebState ->
  GetBlockHeight ->
  IO [BlockData]
handleBlockHeight st (GetBlockHeight h (NoTx notx)) =
  withMetrics st (.block) $
    st.query.getBlocksAtHeight (fromIntegral h)
      >>= (\hs -> getBlocks st hs notx)

-- | Handler for @GET \/block\/heights?heights=…@: the blocks at several heights.
handleBlockHeights ::
  WebState ->
  GetBlockHeights ->
  IO [BlockData]
handleBlockHeights st (GetBlockHeights (HeightsParam heights) (NoTx notx)) =
  withMetrics st (.block) $
    mapM (st.query.getBlocksAtHeight . fromIntegral) heights
      >>= (\hs -> getBlocks st hs notx) . concat

-- | Handler for @GET \/block\/height\/:height\/raw@: the blocks at a height, serialized.
handleBlockHeightRaw :: WebState -> GetBlockHeightRaw -> IO [IO Builder]
handleBlockHeightRaw st (GetBlockHeightRaw h) =
  fmap rawResultList $
    mapM (getRawBlock st) =<< st.query.getBlocksAtHeight (fromIntegral h)

-- GET BlockTime / BlockTimeRaw --

-- | Handler for @GET \/block\/time\/:time@: the last block with a timestamp at or before a time.
handleBlockTime ::
  WebState ->
  GetBlockTime ->
  IO BlockData
handleBlockTime st (GetBlockTime (TimeParam t) (NoTx notx)) =
  withMetrics st (.block) $ do
    let ch = st.config.store.chain
    C.blockAtOrBefore st.query ch t >>= \case
      Nothing -> raise st ThingNotFound
      Just b -> return $ pruneTx notx b

-- | Handler for @GET \/block\/mtp\/:time@: the first block with a median time past at or after a time.
handleBlockMTP ::
  WebState ->
  GetBlockMTP ->
  IO BlockData
handleBlockMTP st (GetBlockMTP (TimeParam t) (NoTx notx)) =
  withMetrics st (.block) $ do
    let ch = st.config.store.chain
    C.blockAtOrAfterMTP st.query ch t >>= \case
      Nothing -> raise st ThingNotFound
      Just b -> return $ pruneTx notx b

-- | Handler for @GET \/block\/time\/:time\/raw@: the last block with a timestamp at or before a time, serialized.
handleBlockTimeRaw :: WebState -> GetBlockTimeRaw -> IO [IO Builder]
handleBlockTimeRaw st (GetBlockTimeRaw (TimeParam t)) =
  C.blockAtOrBefore st.query st.config.store.chain t >>= \case
    Nothing -> raise st ThingNotFound
    Just b -> return (rawResult (rawBlock st.query b))

-- | Handler for @GET \/block\/mtp\/:time\/raw@: the first block with a median time
-- past at or after a time, serialized.
handleBlockMTPRaw :: WebState -> GetBlockMTPRaw -> IO [IO Builder]
handleBlockMTPRaw st (GetBlockMTPRaw (TimeParam t)) =
  C.blockAtOrAfterMTP st.query st.config.store.chain t >>= \case
    Nothing -> raise st ThingNotFound
    Just b -> return (rawResult (rawBlock st.query b))

-- GET Transactions --

-- | Handler for @GET \/transaction\/:txid@: a transaction.
handleTx :: WebState -> GetTx -> IO Transaction
handleTx st (GetTx txid) =
  withMetrics st (.tx) $
    C.getTransaction st.query txid >>= maybe (notFoundTx st txid) return

-- | Handler for @GET \/transactions?txids=…@: several transactions.
handleTxs ::
  WebState ->
  GetTxs ->
  IO [Transaction]
handleTxs st (GetTxs txids) =
  withMetrics st (.tx) $
    catMaybes <$> C.getTransactions st.query (nub txids)

-- | Handler for @GET \/transaction\/:txid\/raw@: a transaction, serialized.
handleTxRaw ::
  WebState ->
  GetTxRaw ->
  IO (RawResult Tx)
handleTxRaw st (GetTxRaw txid) =
  withMetrics st (.txRaw) $
    C.getTransaction st.query txid
      >>= \case
        Nothing -> notFoundTx st txid
        Just tx -> return $ RawResult (transactionData tx)

-- | Handler for @GET \/transactions\/raw?txids=…@: several transactions, serialized.
handleTxsRaw ::
  WebState ->
  GetTxsRaw ->
  IO (RawResultList Tx)
handleTxsRaw st (GetTxsRaw txids) =
  withMetrics st (.txRaw) $
    RawResultList . map transactionData . catMaybes
      <$> C.getTransactions st.query (nub txids)

-- | The hashes of the transactions of a block. Responds with an error if
-- not found.
getTxsBlock :: WebState -> H.BlockHash -> IO [TxHash]
getTxsBlock st h = st.query.getBlock h >>= maybe (raise st ThingNotFound) (return . (.txs))

-- | Handler for @GET \/transactions\/block\/:block@: the transactions of a block, in pieces.
handleTxsBlock :: WebState -> GetTxsBlock -> IO [IO [Transaction]]
handleTxsBlock st (GetTxsBlock h) = transactionPieces st.query <$> getTxsBlock st h

-- | Handler for @GET \/transactions\/block\/:block\/raw@: the transactions of a block, serialized, in pieces.
handleTxsBlockRaw :: WebState -> GetTxsBlockRaw -> IO [IO Builder]
handleTxsBlockRaw st (GetTxsBlockRaw h) =
  listPieces hex . transactionPieces st.query =<< getTxsBlock st h
  where
    hex t = unsafeToEncoding ("\"" <> hexOf (transactionData t) <> "\"")

-- GET TransactionAfterHeight --

-- | Handler for @GET \/transaction\/:txid\/after\/:height@: whether any ancestor
-- of a transaction is a coinbase confirmed after a height.
handleTxAfter ::
  WebState ->
  GetTxAfter ->
  IO (GenericResult (Maybe Bool))
handleTxAfter st (GetTxAfter txid height) =
  withMetrics st (.txAfter) $
    GenericResult . fst <$> (cbAfterHeight st.query (fromIntegral height) txid)

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

-- | Handler for @POST \/transactions@: check the transaction and hand it
-- over ("Haskoin.Store.Web.Publish"). The answer is 202 with its id and a
-- message once it was handed over, over the RPC of a node or to the peers,
-- and 200 with its id for one that the store has in a block. What the
-- store or a node refuses is 400 with the reason; no peer is 500.
handlePostTx :: WebState -> PostTx -> IO (Status, Encoding)
handlePostTx st (PostTx tx) =
  withMetrics st (.txPost) $
    Publish.publish (publishing st) tx >>= \case
      Publish.InBlock -> return (status200, toEncoding (TxId txid))
      Publish.ToNode -> handed "A full node took the transaction."
      Publish.ToPeers n ->
        handed . T.unwords $
          [ "Handed over to",
            T.pack (show n),
            if n == 1 then "peer." else "peers.",
            "The network decides whether to take it."
          ]
      Publish.Invalid why -> raise st (UserError (show why))
      Publish.Refused why ->
        raise st (UserError ("A node refused the transaction: " <> T.unpack why))
      Publish.Unverified i ->
        raise st . UserError $
          "This server cannot verify the signature of input "
            <> show i
            <> ", and sends to its peers only what it has verified"
      Publish.NoPeers -> raise st ServerError
  where
    txid = txHash tx
    handed message = return (status202, A.pairs ("txid" .= txid <> "message" .= (message :: Text)))

-- | The chain as the store has it, for the checks of a transaction to
-- publish: the height of the best block, and the median of the times of
-- the last eleven blocks.
chainRules :: WebState -> IO Publish.Rules
chainRules st = do
  best <- st.query.getBestBlock >>= maybe (return Nothing) st.query.getBlock
  times <- maybe (return []) (back (11 :: Int)) best
  return
    Publish.Rules
      { net = own.net,
        inputs = \tx -> judgeInputs own.net own.ctx tx . map Just,
        height = maybe 0 (.height) best,
        medianTime = case Data.List.sort times of
          [] -> 0
          sorted -> fromIntegral (sorted !! (length sorted `div` 2))
      }
  where
    own = st.config.store
    back n b
      | n <= 1 = return [b.header.timestamp]
      | otherwise =
          (b.header.timestamp :) <$> (st.query.getBlock b.header.prev >>= maybe (return []) (back (n - 1)))

-- | Give a transaction to a full node over its JSON-RPC interface
-- (@sendrawtransaction@).
--
-- The node runs every script and holds the transaction to its own rules
-- and its own mempool, says why it refuses, and does not close the
-- connections of the store over what it refuses, as it may over a
-- transaction that a peer sends it. A transaction that the node takes
-- comes back to the store as any other: the node announces it on the
-- connection that the store syncs from.
--
-- It is an option because the store needs nothing of a node but to be
-- its peer, and this needs an account on one. A node that does not
-- answer, or answers what cannot be read, is 'Publish.Unavailable': the
-- transaction then goes to the peers, if the store verified it. A
-- transaction that the node has already was sent.
sendNode :: WebState -> String -> Tx -> IO Publish.Direct
sendNode st url tx =
  tryAny (Wreq.Session.postWith opts st.session url body) >>= \case
    Left e -> unheard (cs (show e))
    Right r -> case A.decode (r ^. Wreq.responseBody) >>= answer of
      Nothing -> unheard ("HTTP " <> cs (show (r ^. Wreq.responseStatus . Wreq.statusCode)))
      Just Nothing -> return Publish.Sent
      Just (Just why)
        | "already" `T.isInfixOf` why -> return Publish.Sent
        | otherwise -> return (Publish.Declined why)
  where
    opts = Wreq.defaults & Wreq.checkResponse ?~ (\_ _ -> return ())
    body =
      A.object
        [ "jsonrpc" .= ("1.0" :: Text),
          "id" .= ("haskoin-store" :: Text),
          "method" .= ("sendrawtransaction" :: Text),
          "params" .= [encodeHex (runPutS (serialize tx))]
        ]
    -- the error of the answer if it has one, nothing if it has a result
    answer v = do
      A.Object top <- Just v
      case KeyMap.lookup "error" top of
        Just (A.Object e) | Just (A.String why) <- KeyMap.lookup "message" e -> Just (Just why)
        Just A.Null | KeyMap.member "result" top -> Just Nothing
        Nothing | KeyMap.member "result" top -> Just Nothing
        _ -> Nothing
    unheard why = do
      logWarn
        (withSource "Publish" st.config.logger)
        ("The node did not take a transaction over RPC, which goes to a peer if it is verified: " <> why)
      return Publish.Unavailable

-- | What "Haskoin.Store.Web.Publish" needs, for a request.
--
-- The peers are those of the store that take transactions. A transaction
-- that was handed over is asked for again after the answer
-- ('askingLoop'), unless the store keeps no mempool.
publishing :: WebState -> Publish.Publishing
publishing st =
  Publish.Publishing
    { rules = chainRules st,
      direct = maybe (\_ -> return Publish.Unavailable) (sendNode st) st.config.nodeRpc,
      holds = fmap heldOf . st.query.getTxData,
      coin = Publish.coinOf st.query,
      peers = map (.address) . filter (maybe False (.relay) . (.version)) <$> getPeers own.peerMgr,
      courier =
        Publish.deliver
          Publish.Courier
            { net = own.net,
              connect = own.connect,
              patience = 10,
              logger = withSource "Courier" st.config.logger
            },
      fetch = if st.config.noMempool then \_ -> return () else Publish.askFor st.asking,
      note = \level -> logAt level (withSource "Publish" st.config.logger)
    }
  where
    own = st.config.store
    heldOf = \case
      Just td
        | td.deleted -> Publish.Absent
        | confirmed td.block -> Publish.Confirmed
        | otherwise -> Publish.Pending
      Nothing -> Publish.Absent

-- | Ask for the transactions that were handed over until the store has
-- them ("Haskoin.Store.Web.Publish".'Publish.Asking'): as if each peer had
-- announced them, so that the block store asks one of them. A failure is
-- logged and the asking goes on.
askingLoop :: WebState -> IO ()
askingLoop st = forever $ do
  tryAny (Publish.askLoop st.asking present ask) >>= \case
    Left e -> logWarn (withSource "Publish" st.config.logger) ("Asking for transactions: " <> cs (show e))
    Right () -> return ()
  threadDelay 1000000
  where
    own = st.config.store
    present h = L.withView own.db $ \v -> maybe False (not . (.deleted)) <$> (L.layeredRead v).getTxData h
    ask h = getPeers own.peerMgr >>= mapM_ (\op -> blockStoreTxHash op.mailbox [h] own.block)

-- GET Mempool / Events --

-- | Handler for @GET \/mempool@: hashes of mempool transactions, newest first.
handleMempool ::
  WebState ->
  GetMempool ->
  IO [TxHash]
handleMempool st (GetMempool limitM (OffsetParam o)) =
  withMetrics st (.mempool) $ do
    WebLimits {..} <- pure st.config.limits
    let wl' = WebLimits {maxItemCount = 0, ..}
        l = Limits (validateLimit wl' False limitM) (fromIntegral o) Nothing
    map snd <$> st.query.getMempoolPage l

-- | Middleware that serves @\/events@ over a WebSocket: each new block and
-- each transaction that enters or leaves the mempool is a message, from the
-- log of events ("Haskoin.Store.Web.Events").
--
-- The client may send the commands of t'EventCommand' to choose the
-- transactions that it hears about. Each side speaks JSON in text frames;
-- a binary frame is a command that cannot be read. A command that the server cannot read
-- or cannot do closes the connection with the reason, as a node does with
-- an SPV client: there are no answers to wait for.
--
-- @?after=@ with the hash of the last event heard takes up from there
-- ('eventsStart'). Clients that do not answer pings, or that fall off the
-- start of the log, are disconnected.
webSocketEvents :: WebState -> Middleware
webSocketEvents s =
  websocketsOr defaultConnectionOptions (wrap . events)
  where
    pingPong = defaultPingPongOptions {pingInterval = 30, pongTimeout = 60}
    gauge = (.events) <$> s.metrics
    net = s.config.store.net
    ctx = s.config.store.ctx
    gap = s.query.maxGap
    wrap f = case gauge of
      Nothing -> f
      Just g -> withGaugeIO g f
    events pending = do
      let request = pendingRequest pending
          (path, query) = C.break (== '?') (requestPath request)
          asked = parseQuery query
      if path /= "/events"
        then turnAway pending ThingNotFound
        else
          eventsOpen s (\name -> T.decodeUtf8Lenient <$> join (lookup (T.encodeUtf8 name) asked)) >>= \case
            Left e -> turnAway pending e
            Right (start, watch) -> do
              conn <- acceptRequest pending
              -- websockets only sees pongs while reading, which the
              -- thread that hears the commands does
              withPingPong pingPong conn $ \c ->
                race_ (hear c watch) (tell c watch start) `catch` gone
    -- a client that leaves is not an error to report
    gone :: WebSockets.ConnectionException -> IO ()
    gone _ = return ()
    -- the commands of the client, until one cannot be done
    hear c watch = do
      message <- WebSockets.receiveDataMessage c
      let read' = case message of
            WebSockets.Text bytes _ -> A.decode bytes >>= parseMaybe (unmarshalValue (net, ctx))
            WebSockets.Binary _ -> Nothing
      case read' of
        Nothing -> refuse c "Could not read the command"
        Just cmd ->
          try (command ctx gap s.query.xPubBals watch cmd) >>= \case
            Right (Right ()) -> hear c watch
            Right (Left why) -> refuse c why
            Left (_ :: Except) -> refuse c "Could not do the command"
    -- 1008: the message is against what the server allows
    refuse c why = WebSockets.sendCloseCode c 1008 (T.pack why)
    -- the events from a place in the log on, those that the client chose
    tell c watch from =
      atomically (readEvents s.eventLog from) >>= \case
        Nothing -> WebSockets.sendClose c ("Event queue full" :: Text)
        Just entries -> do
          forM_ entries $ \e -> do
            wanted <- atomically (stateTVar watch (relevant ctx gap e.seen))
            when wanted $ sendTextData c (A.encode e.event)
          tell c watch (from + fromIntegral (length entries))
    turnAway pending e =
      rejectRequestWith
        pending
        WebSockets.defaultRejectRequest
          { WebSockets.rejectBody = L.toStrict (A.encode e),
            WebSockets.rejectCode = statusCode (errStatus e),
            WebSockets.rejectMessage = statusMessage (errStatus e),
            WebSockets.rejectHeaders = [("Content-Type", "application/json")]
          }

-- | Handler for @GET \/events@: stream of new blocks and of transactions
-- that enter or leave the mempool, one per line in JSON, until the client
-- falls off the start of the log.
-- @after@ takes up from an event ('eventsStart').
handleEvents :: WebState -> Req -> IO Response
handleEvents st req = do
  let asked name = lookup name req.params
  (start, watch) <- eventsOpen st asked >>= either (raise st) return
  return . responseStream status200 [(hContentType, "application/json")] $ \write flush' ->
    let send from = do
          flush'
          -- ends the response when the client falls too far behind
          atomically (readEvents st.eventLog from) >>= \case
            Nothing -> return ()
            Just entries -> do
              forM_ entries $ \e -> do
                wanted <- atomically (stateTVar watch (relevant own.ctx st.query.maxGap e.seen))
                when wanted (write (serial e.event))
              send (from + fromIntegral (length entries))
     in withGaugeIncrease st (.events) (send start)
  where
    serial e = fromEncoding (toEncoding e) <> "\n"
    own = st.config.store

-- | Where a client of the events starts reading the log, and what it hears
-- about from the start, as the parameters of its request say: 'eventsStart'
-- for @after@, and for the rest a choice made of the same commands that a
-- WebSocket client sends later.
--
-- A WebSocket client could only choose once it was connected, and until its
-- first command was read it was sent every transaction of a busy mempool.
-- With these the choice is there before the first event:
--
-- * @quiet=true@: no transactions, only blocks, until something is watched;
-- * @addresses@: a list of addresses to watch, separated by commas;
-- * @xpub@ and @derive@: an extended public key and the types of its
--   addresses, one or several separated by commas, legacy if left out;
-- * @filter@, @functions@, @tweak@, @flags@: a Bloom filter, in hex, with
--   its numbers.
--
-- Any of them but @quiet=false@ makes the client a filtered one. The HTTP
-- stream takes them too, and it is the only way that it has to choose.
-- The place in the log is taken first, so that nothing that happens while
-- a key is read from the database is left out.
eventsOpen :: WebState -> (Text -> Maybe Text) -> IO (Either Except (Word64, TVar Watch))
eventsOpen st asked =
  eventsStart st (asked "after") >>= \case
    Left e -> return (Left e)
    Right start -> case commands of
      Left why -> return (Left (UserError why))
      Right cmds -> do
        watch <- newTVarIO everything
        let run [] = return (Right (start, watch))
            run (cmd : rest) =
              command own.ctx st.query.maxGap st.query.xPubBals watch cmd >>= \case
                Left why -> return (Left (UserError why))
                Right () -> run rest
        run cmds
  where
    own = st.config.store
    commands = do
      quiet <- maybe (Right False) (need "quiet" . readBool) (asked "quiet")
      addresses <- mapM (need "addresses" . textToAddr own.net) (listed "addresses")
      derivs <- mapM (need "derive" . textToDeriveType) (listed "derive")
      keys <- mapM (need "xpub" . xPubImport own.net own.ctx) (maybe [] pure (asked "xpub"))
      filters <- case asked "filter" of
        Nothing -> Right []
        Just hex -> do
          let number name = maybe (Right 0) (need name . (readNum :: Text -> Maybe Word32)) (asked name)
          functions <- number "functions"
          tweak <- number "tweak"
          flags <- number "flags"
          let json =
                A.object
                  [ "type" A..= A.String "filterload",
                    "filter" A..= hex,
                    "functions" A..= functions,
                    "tweak" A..= tweak,
                    "flags" A..= flags
                  ]
          pure <$> need "filter" (parseMaybe (unmarshalValue (own.net, own.ctx)) json)
      when (null keys && not (null derivs)) (Left "Parameter derive needs xpub")
      let types = if null derivs then [DeriveNormal] else derivs
      return $
        [EventNone | quiet]
          <> [EventWatch addresses | not (null addresses)]
          <> [EventWatchXPub XPubSpec {key, deriv} | key <- keys, deriv <- types]
          <> filters
    listed name = maybe [] (filter (not . T.null) . T.splitOn ",") (asked name)
    need :: Text -> Maybe a -> Either String a
    need name = maybe (Left ("Could not read parameter " <> T.unpack name)) Right

-- | Where a client of the events starts reading the log: at the end, or
-- after the event that it says it heard last, named by the hash of its
-- block or transaction. 404 when the log has no event with that hash: the
-- client cannot know what it missed, and has to read what it follows again
-- and listen from the end.
eventsStart :: WebState -> Maybe Text -> IO (Either Except Word64)
eventsStart st = \case
  Nothing -> Right <$> atomically (eventsEnd st.eventLog)
  Just hex -> case (H.hexToBlockHash hex, hexToTxHash hex) of
    (Nothing, Nothing) -> return (Left (UserError "The parameter after is not a hash"))
    (block, tx) -> maybe (Left ThingNotFound) Right <$> atomically (eventsAfter st.eventLog block tx)

-- | Write what happens into the log of events for as long as the server
-- runs, each transaction that enters with what the database has of it
-- ('seenOf'), for the clients that choose what they hear about.
--
-- A store alone writes what it publishes. A member of a fleet writes what
-- the fleet tells ('memberEvents'), from the start of what Redis keeps, so
-- that the logs of all the servers of a fleet hold the same events and a
-- client can take up on any of them.
feedEvents :: WebState -> IO ()
feedEvents st = case own.fleet of
  Just member -> memberEvents member True (mapM_ happened . fleet)
  Nothing -> do
    queue <- newTQueueIO
    let listen = mapM_ (writeTQueue queue) . local
    bracket (subscribe own.pub listen) (unsubscribe own.pub) $ \_ ->
      forever (atomically (readTQueue queue) >>= happened)
  where
    own = st.config.store
    local = \case
      StoreBestBlock b -> Just (Left b)
      StoreMempoolNew t -> Just (Right (t, True))
      StoreMempoolDelete t -> Just (Right (t, False))
      _ -> Nothing
    fleet = \case
      FleetBlock h -> Left <$> hash h
      FleetEntered h -> Right . (,True) <$> hash h
      FleetLeft h -> Right . (,False) <$> hash h
    hash :: (Serialize x) => C.ByteString -> Maybe x
    hash = either (const Nothing) Just . decode
    happened = \case
      Left b -> atomically (appendEvent st.eventLog (BlockArrived b))
      Right (t, False) -> atomically (appendEvent st.eventLog (TxLeft t))
      Right (t, True) -> do
        -- a transaction that cannot be read is told to every client
        known <- st.direct.getTxData t `catchAny` \_ -> return Nothing
        atomically (appendEvent st.eventLog (TxEntered t (seenOf own.net own.ctx <$> known)))

-- GET Address Transactions --

-- | Handler for @GET \/address\/:address\/transactions@: transactions of an address.
handleAddrTxs ::
  WebState ->
  GetAddrTxs ->
  IO [TxRef]
handleAddrTxs st (GetAddrTxs addr pLimits) =
  withMetrics st (.addressTx) $
    st.query.getAddressTxs addr =<< paramToLimits st False pLimits

-- | Handler for @GET \/address\/transactions?addresses=…@: transactions of several addresses.
handleAddrsTxs ::
  WebState ->
  GetAddrsTxs ->
  IO [TxRef]
handleAddrsTxs st (GetAddrsTxs addrs pLimits) =
  withMetrics st (.addressTx) $
    st.query.getAddressesTxs addrs =<< paramToLimits st False pLimits

-- | Handler for @GET \/address\/:address\/transactions\/full@: full transactions of an address, in the pieces
-- of 'pathPieces'. Which transactions they are is read at once; their
-- bodies are read piece by piece.
handleAddrTxsFull ::
  WebState ->
  GetAddrTxsFull ->
  IO [IO [Transaction]]
handleAddrTxsFull st (GetAddrTxsFull addr pLimits) = do
  txs <- st.query.getAddressTxs addr =<< paramToLimits st True pLimits
  return (transactionPieces st.query (map (.txid) txs))

-- | Handler for @GET \/address\/transactions\/full?addresses=…@: full transactions of several addresses, in
-- the pieces of 'pathPieces'.
handleAddrsTxsFull ::
  WebState ->
  GetAddrsTxsFull ->
  IO [IO [Transaction]]
handleAddrsTxsFull st (GetAddrsTxsFull addrs pLimits) = do
  txs <- st.query.getAddressesTxs addrs =<< paramToLimits st True pLimits
  return (transactionPieces st.query (map (.txid) txs))

-- | Transactions by their hashes, in pieces of 'pieceSize' that are each
-- read with one multi-get. The hashes are those that the database gave,
-- of a block or of addresses, so the transactions are there; one that is
-- gone by the time its piece is read is left out, as it is when the list
-- is read at once.
transactionPieces :: StoreRead -> [TxHash] -> [IO [Transaction]]
transactionPieces q = pieced pieceSize (fmap catMaybes . C.getTransactions q)

-- | Transactions in a piece of a list answered piece by piece. With 20 the
-- first byte leaves a millisecond or two sooner and with 500 some ten
-- later; the time of the whole response is the same with all three.
pieceSize :: Int
pieceSize = 100

-- | Handler for @GET \/address\/:address\/balance@: balance of an address.
handleAddrBalance :: WebState -> GetAddrBalance -> IO Balance
handleAddrBalance st (GetAddrBalance addr) =
  withMetrics st (.addressBalance) $ C.getDefaultBalance st.query addr

-- | Handler for @GET \/address\/balances?addresses=…@: balances of several addresses.
handleAddrsBalance ::
  WebState ->
  GetAddrsBalance ->
  IO [Balance]
handleAddrsBalance st (GetAddrsBalance addrs) =
  withMetrics st (.addressBalance) $ st.query.getBalances addrs

-- | Handler for @GET \/address\/:address\/unspent@: unspent outputs of an address.
handleAddrUnspent ::
  WebState ->
  GetAddrUnspent ->
  IO [Unspent]
handleAddrUnspent st (GetAddrUnspent addr pLimits) =
  withMetrics st (.addressUnspent) $
    st.query.getAddressUnspents addr =<< paramToLimits st False pLimits

-- | Handler for @GET \/address\/unspent?addresses=…@: unspent outputs of several addresses.
handleAddrsUnspent ::
  WebState ->
  GetAddrsUnspent ->
  IO [Unspent]
handleAddrsUnspent st (GetAddrsUnspent addrs pLimits) =
  withMetrics st (.addressUnspent) $
    st.query.getAddressesUnspents addrs =<< paramToLimits st False pLimits

-- GET XPubs --

-- | Handler for @GET \/xpub\/:xpub@: summary of an extended public key.
handleXPub ::
  WebState ->
  GetXPub ->
  IO XPubSummary
handleXPub st (GetXPub xpub deriv (NoCache noCache)) =
  withMetrics st (.xpub) $
    let xs = XPubSpec xpub deriv
     in xPubSummary xs <$> xPubBalsChecked st noCache xs

-- | Handler for @DELETE \/xpub\/:xpub@: remove an extended public key from the cache. Returns 'True' if it was cached.
handleDelXPub ::
  WebState ->
  DelCachedXPub ->
  IO (GenericResult Bool)
handleDelXPub st (DelCachedXPub xpub deriv) =
  withMetrics st (.xpubDelete) $ do
    let c = st.config.store.cache
    r <- maybe (return False) (`cacheDelete` XPubSpec xpub deriv) c
    return $ GenericResult r

-- | Balances of the addresses of an extended public key ('xPubBals'), from
-- the cache unless told not to, refusing a wrong kind of address
-- ('checkDerive').
xPubBalsChecked :: WebState -> Bool -> XPubSpec -> IO [XPubBal]
xPubBalsChecked st noCache xs = do
  bals <- (cacheOr noCache st).query.xPubBals xs
  checkDerive st xs bals
  return bals

-- | Answer with an error if an extended public key has no transactions with
-- the kind of address asked for, but has some with another kind: the request
-- named the wrong kind.
--
-- An xpub does not record which kind of address its wallet derives, and the
-- same key gives different addresses for each kind, so a request with the
-- wrong @derive@ would otherwise get an empty wallet: a valid answer, and a
-- wrong one, that a wallet would show its user as a zero balance. An error
-- says what went wrong and how to fix it.
--
-- The check costs nothing for a wallet that has transactions. For one that
-- looks unused, it reads the balances of the first addresses of each other
-- kind, as many as the store reads to decide that an xpub is unused, in one
-- read per kind. A wallet unused with every kind is a new wallet, and gets its
-- empty answer. Networks without SegWit have one kind only, and no check.
checkDerive :: WebState -> XPubSpec -> [XPubBal] -> IO ()
checkDerive st xs bals =
  when (net.segWit && all (\b -> b.balance.txs == 0) bals) $ do
    used <- filterM usedAs others
    forM_ (take 1 used) $ \d ->
      raise st . UserError $
        unwords
          [ "This xpub has no transactions with derive=" <> name xs.deriv <> ",",
            "but has some with derive=" <> name d <> ";",
            "ask with derive=" <> name d <> "."
          ]
  where
    net = st.config.store.net
    ctx = st.config.store.ctx
    others = filter (/= xs.deriv) [DeriveNormal, DeriveP2SH, DeriveP2WPKH]
    name = T.unpack . deriveTypeToText
    usedAs d = do
      let external = pubSubKey ctx xs.key 0
          addrs =
            map snd . take (fromIntegral st.query.initialGap) $
              deriveAddresses (deriveFunction ctx d) external 0
      any (\b -> b.txs > 0) <$> st.query.getBalances addrs

-- | Transactions of an extended public key, optionally without the cache.
getXPubTxs ::
  WebState ->
  XPubKey ->
  DeriveType ->
  LimitsParam ->
  Bool ->
  IO [TxRef]
getXPubTxs st xpub deriv plimits nocache = do
  limits <- paramToLimits st False plimits
  let xs = XPubSpec xpub deriv
      st' = cacheOr nocache st
  xbals <- xPubBalsChecked st nocache xs
  st'.query.xPubTxs xs xbals limits

-- | Handler for @GET \/xpub\/:xpub\/transactions@: transactions of an extended public key.
handleXPubTxs ::
  WebState ->
  GetXPubTxs ->
  IO [TxRef]
handleXPubTxs st (GetXPubTxs xpub deriv plimits (NoCache nocache)) =
  withMetrics st (.xpubTx) $
    getXPubTxs st xpub deriv plimits nocache

-- | Handler for @GET \/xpub\/:xpub\/transactions\/full@: full transactions of an extended public key, in pieces.
handleXPubTxsFull :: WebState -> GetXPubTxsFull -> IO [IO [Transaction]]
handleXPubTxsFull st (GetXPubTxsFull xpub deriv plimits (NoCache nocache)) =
  transactionPieces (cacheOr nocache st).query . map (.txid)
    <$> getXPubTxs st xpub deriv plimits nocache

-- | Handler for @GET \/xpub\/:xpub\/balances@: balances of the used addresses of an extended public key.
handleXPubBalances ::
  WebState ->
  GetXPubBalances ->
  IO [XPubBal]
handleXPubBalances st (GetXPubBalances xpub deriv (NoCache noCache)) =
  withMetrics st (.xpubBalance) $
    xPubBalsChecked st noCache (XPubSpec xpub deriv)

-- | Handler for @GET \/xpub\/:xpub\/unspent@: unspent outputs of an extended public key.
handleXPubUnspent ::
  WebState ->
  GetXPubUnspent ->
  IO [XPubUnspent]
handleXPubUnspent st (GetXPubUnspent xpub deriv pLimits (NoCache noCache)) =
  withMetrics st (.xpubUnspent) $ do
    limits <- paramToLimits st False pLimits
    let xspec = XPubSpec xpub deriv
        st' = cacheOr noCache st
    xbals <- xPubBalsChecked st noCache xspec
    st'.query.xPubUnspents xspec xbals limits

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
  Req ->
  Text ->
  IO (HashSet BinfoAddr)
getBinfoAddrsParam st req name = do
  let net = st.config.store.net
  let ctx = st.config.store.ctx
  p <- fromMaybe "" <$> param req name
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
  Req ->
  IO (HashSet XPubSpec, HashSet Address)
getBinfoActive st req = do
  active <- getBinfoAddrsParam st req "active"
  p2sh <- getBinfoAddrsParam st req "activeP2SH"
  bech32 <- getBinfoAddrsParam st req "activeBech32"
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

-- | A kind of address to derive, as in the @derive@ parameter of the xpub
-- endpoints: SegWit kinds only on networks with SegWit.
readDerive :: Network -> Text -> Maybe DeriveType
readDerive net t = do
  d <- textToDeriveType t
  guard (net.segWit || d == DeriveNormal)
  return d

-- | Whether to identify transactions by short number, unless @txidindex@ is false.
getNumTxId :: Req -> IO Bool
getNumTxId req = not <$> paramOr req readBool "txidindex" False

-- | Height of the best header.
getChainHeight :: WebState -> IO H.BlockHeight
getChainHeight st =
  fmap (.height) $ chainGetBest =<< pure st.config.store.chain

-- | Handler for @GET\/POST \/blockchain\/unspent@: unspent outputs of addresses and xpubs, as Blockchain.info.
handleBinfoUnspent :: WebState -> Req -> IO Response
handleBinfoUnspent st req =
  withMetrics st (.binfoUnspent) $ do
    (xspecs, addrs) <- getBinfoActive st req
    numtxid <- getNumTxId req
    limit <- get_limit
    min_conf <- get_min_conf
    let net = st.config.store.net
    let ctx = st.config.store.ctx
    height <- getChainHeight st
    let mn u = min_conf > u.confirmations
    xbals <- getXBals st.query xspecs
    bus <-
      runConduit $
        getBinfoUnspents st.query numtxid height xbals xspecs addrs
          .| (dropWhileC mn >> takeC limit .| sinkList)
    streamEncoding (marshalEncoding (net, ctx) (BinfoUnspents bus))
  where
    get_limit = min 1000 . fromIntegral <$> paramOr req readNat "limit" 250
    get_min_conf = fromIntegral <$> paramOr req readNat "confirmations" 0

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
getCashAddr :: Req -> IO Bool
getCashAddr req = paramOr req readBool "cashaddr" False

-- | Parse an address. Responds with not found if invalid.
getAddress :: WebState -> Text -> IO Address
getAddress st txt = do
  let net = st.config.store.net
  case textToAddr net txt of
    Nothing -> raise st ThingNotFound
    Just a -> return a

-- | Address or xpub in a path parameter. Passes to the next route if invalid.
getBinfoAddr :: WebState -> Req -> Text -> IO BinfoAddr
getBinfoAddr st req param' = do
  txt <- capture req param'
  let net = st.config.store.net
  let ctx = st.config.store.ctx
  let addr = BinfoAddr <$> textToAddr net txt
      xpub = BinfoXpub <$> xPubImport net ctx txt
  maybe next return (addr <|> xpub)

-- | Handler for @GET\/POST \/blockchain\/export-history@: daily history of the
-- balance of addresses and xpubs with prices, as Blockchain.info.
handleBinfoHistory :: WebState -> Req -> IO Response
handleBinfoHistory st req =
  withMetrics st (.binfoExportHistory) $ do
    (xspecs, addrs) <- getBinfoActive st req
    (startM, endM) <- get_dates
    (code, price') <- getPrice st req
    xbals <- (getXBals st.query xspecs)
    let xaddrs = HashSet.fromList $ concatMap (map get_addr) (HashMap.elems xbals)
        aaddrs = xaddrs <> addrs
        cur = price'.fifteen
        cs' = conduits (HashMap.toList xbals) addrs endM
    txs <-
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
    rates <- map (.price) <$> (getRates logger net session url code times)
    let hs = zipWith (convert cur aaddrs) txs (rates <> repeat 0.0)
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
      BinfoDate start <- requireParam st req parseBinfoDate "start"
      BinfoDate end' <- requireParam st req parseBinfoDate "end"
      let end = end' + 24 * 60 * 60
      let ch = st.config.store.chain
      startM <- C.blockAtOrAfter st.query ch start
      endM <- C.blockAtOrBefore st.query ch end
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

-- | Currency code in the @currency@ parameter, USD by default, in any case,
-- and its latest price. A currency that the price service does not list is
-- refused. Until the server has prices, which it may never have if the
-- service cannot be reached, every currency has a price of zero, rather than
-- every request being refused.
getPrice :: WebState -> Req -> IO (Text, BinfoTicker)
getPrice st req = do
  prices <- readTVarIO st.ticker
  let known c
        | HashMap.null prices = Just (c, def)
        | otherwise = (c,) <$> HashMap.lookup c prices
  paramOr req (known . T.toUpper) "currency" ("USD", fromMaybe def (HashMap.lookup "USD" prices))

-- | Symbol of the currency in the @currency@ parameter.
getSymbol :: WebState -> Req -> IO BinfoSymbol
getSymbol st req = uncurry binfoTickerToSymbol <$> getPrice st req

-- | Handler for @GET \/blockchain\/blocks\/:milliseconds@: the blocks of the day
-- before a time in milliseconds, as Blockchain.info.
handleBinfoBlocksDay :: WebState -> Req -> IO Response
handleBinfoBlocksDay st req =
  withMetrics st (.binfoBlock) $ do
    t <- min h . (`div` 1000) <$> captureWith req readNum "milliseconds"
    let ch = st.config.store.chain
    m <- C.blockAtOrBefore st.query ch t
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
          b' <- st.query.getBlock b.header.prev
          (b :) <$> go t b'

-- | Handler for @GET\/POST \/blockchain\/multiaddr@: balances and transactions of
-- addresses and xpubs, as Blockchain.info.
handleMultiAddr :: WebState -> Req -> IO Response
handleMultiAddr st req =
  withMetrics st (.binfoMultiaddr) $ do
    (addrs', _, saddrs, sxpubs, xspecs) <- get_addrs
    numtxid <- getNumTxId req
    cashaddr <- getCashAddr req
    local' <- getSymbol st req
    offset <- getBinfoOffset st req
    n <- getBinfoCount st req "n"
    prune <- get_prune
    fltr <- get_filter
    xbals <- (getXBals st.query xspecs)
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
      runConduit $
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
      _ -> fromIntegral <$> st.direct.getAddressesTxCount (HashSet.toList salladdrs)
    nfiltered <- case fltr of
      BinfoFilterAll -> return ntx
      _ ->
        runConduit $
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
    streamEncoding $ marshalEncoding (net, ctx) multiaddr
  where
    get_xpub_tx_count xbals =
      fmap HashMap.fromList
        . mapM
          ( \x ->
              (x,)
                . fromIntegral
                <$> st.query.xPubTxCount x (xBals x xbals)
          )
        . HashSet.toList
    get_filter = paramOr req parseBinfoFilter "filter" BinfoFilterAll
    get_best_block =
      st.query.getBestBlock >>= \case
        Nothing -> raise st ThingNotFound
        Just bh ->
          st.query.getBlock bh >>= \case
            Nothing -> raise st ThingNotFound
            Just b -> return b
    get_prune = do
      noCompact <- paramOr req readBool "no_compact" False
      not <$> paramOr req readBool "noCompact" noCompact
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
      (xspecs, addrs) <- getBinfoActive st req
      sh <- getBinfoAddrsParam st req "onlyShow"
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
       in fmap g . st.query.getBalances . HashSet.toList
    get_peers = do
      ps <- getPeersInformation st.config.store.peerMgr
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
getBinfoCount :: WebState -> Req -> Text -> IO Int
getBinfoCount st req str = do
  let d = st.config.limits.defItemCount
  let x = st.config.limits.maxFullItemCount
  i <- fromIntegral <$> paramOr req readNat str (fromIntegral d)
  return (fromIntegral (if x == 0 then i else min x i) :: Int)

-- | Offset requested in the @offset@ parameter. Responds with an error if above the maximum.
getBinfoOffset ::
  WebState ->
  Req ->
  IO Int
getBinfoOffset st req = do
  let x = st.config.limits.maxOffset
  o <- fromIntegral <$> paramOr req readNat "offset" 0
  when (o > x) $
    raise st $
      UserError $
        "offset exceeded: " <> show o <> " > " <> show x
  return (fromIntegral o :: Int)

-- | Handler for @GET \/blockchain\/rawaddr\/:addr@: balance and transactions of an
-- address or xpub, as Blockchain.info; also at @\/blockchain\/address\/:addr@
-- and @\/blockchain\/xpub\/:addr@.
handleRawAddr :: WebState -> Req -> IO Response
handleRawAddr st req =
  withMetrics st (.binfoAddressRaw) $
    getBinfoAddr st req "addr" >>= \case
      BinfoAddr addr -> do_addr addr
      BinfoXpub xpub -> do_xpub xpub
  where
    do_xpub xpub = do
      numtxid <- getNumTxId req
      derive <- paramOr req (readDerive st.config.store.net) "derive" DeriveNormal
      let xspec = XPubSpec xpub derive
      n <- getBinfoCount st req "limit"
      off <- getBinfoOffset st req
      xbals <- (getXBals st.query (HashSet.singleton xspec))
      checkDerive st xspec (xBals xspec xbals)
      let net = st.config.store.net
      let summary = xPubSummary xspec (xBals xspec xbals)
          abook = compute_abook xpub (xBals xspec xbals)
          xspecs = HashSet.singleton xspec
          saddrs = HashSet.empty
          baddrs = HashMap.keysSet abook
          bfilter = BinfoFilterAll
          amnt = summary.confirmed + summary.unconfirmed
      txs <-
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
      ntx <- st.query.xPubTxCount xspec (xBals xspec xbals)
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
      numtxid <- getNumTxId req
      n <- getBinfoCount st req "limit"
      off <- getBinfoOffset st req
      bal <- fromMaybe (zeroBalance addr) <$> st.query.getBalance addr
      let net = st.config.store.net
      let abook = HashMap.singleton addr Nothing
          xspecs = HashSet.empty
          saddrs = HashSet.singleton addr
          bfilter = BinfoFilterAll
          balance = bal.confirmed + bal.unconfirmed
      txs <-
        runConduit $
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
      let ctx = st.config.store.ctx
      streamEncoding $ marshalEncoding (net, ctx) ra

-- | Handler for @GET \/blockchain\/q\/getreceivedbyaddress\/:addr@: total received by an address.
handleBinfoReceived :: WebState -> Req -> IO Response
handleBinfoReceived st req =
  withMetrics st (.binfoQgetreceivedbyaddress) $ do
    a <- getAddress st =<< capture req "addr"
    b <- fromMaybe (zeroBalance a) <$> st.query.getBalance a
    textResponse $ cs $ show b.received

-- | Handler for @GET \/blockchain\/q\/getsentbyaddress\/:addr@: total sent by an address.
handleBinfoSent :: WebState -> Req -> IO Response
handleBinfoSent st req =
  withMetrics st (.binfoQgetsentbyaddress) $ do
    a <- getAddress st =<< capture req "addr"
    b <- fromMaybe (zeroBalance a) <$> st.query.getBalance a
    textResponse $ cs $ show $ b.received - b.confirmed - b.unconfirmed

-- | Handler for @GET \/blockchain\/q\/addressbalance\/:addr@: balance of an address.
handleBinfoAddrBalance :: WebState -> Req -> IO Response
handleBinfoAddrBalance st req =
  withMetrics st (.binfoQaddressbalance) $ do
    a <- getAddress st =<< capture req "addr"
    b <- fromMaybe (zeroBalance a) <$> st.query.getBalance a
    textResponse $ cs $ show $ b.confirmed + b.unconfirmed

-- | Handler for @GET \/blockchain\/q\/addressfirstseen\/:addr@: time of the first
-- block with a transaction of an address.
handleFirstSeen :: WebState -> Req -> IO Response
handleFirstSeen st req =
  withMetrics st (.binfoQaddressfirstseen) $ do
    a <- getAddress st =<< capture req "addr"
    let ch = st.config.store.chain
    bb <- chainGetBest ch
    let top = bb.height
        bot = 0
    i <- go ch bb a bot top
    textResponse $ cs $ show i
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
      (chainGetAncestor ch h bb) >>= \case
        Just b -> return b.header.timestamp
        Nothing -> do
          let logger = st.config.logger
          logError logger $
            "Could not get ancestor at height "
              <> cs (show h)
              <> " for block "
              <> H.blockHashToHex (H.headerHash bb.header)
          error "Block ancestor retreival error"
    hasone a h = do
      let l = Limits 1 0 (Just (AtBlock h))
      not . null <$> st.query.getAddressTxs a l

-- | Handler for @GET\/POST \/blockchain\/balance@: short balances of addresses and xpubs, as Blockchain.info.
handleShortBal :: WebState -> Req -> IO Response
handleShortBal st req =
  withMetrics st (.binfoBalance) $ do
    (xspecs, addrs) <- getBinfoActive st req
    cashaddr <- getCashAddr req
    let net = st.config.store.net
    abals <- catMaybes <$> mapM (getabal net cashaddr) (HashSet.toList addrs)
    xbals <- mapM (getxbal net) (HashSet.toList xspecs)
    let res = HashMap.fromList (abals <> xbals)
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
              st.query.getBalance a >>= \case
                Nothing -> return $ Just (a', shorten (zeroBalance a))
                Just b -> return $ Just (a', shorten b)
    getxbal net xpub = do
      xbals <- st.query.xPubBals xpub
      ntx <- fromIntegral <$> st.query.xPubTxCount xpub xbals
      let val = sum $ map (.balance.confirmed) xbals
          zro = sum $ map (.balance.unconfirmed) xbals
          exs = [x | x@XPubBal {path = 0 : _} <- xbals]
          final = val + zro
          received = sum $ map (.balance.received) exs
          sbl = BinfoShortBal {final, ntx, received}
      let ctx = st.config.store.ctx
      return (xPubExport net ctx xpub.key, sbl)

-- | Whether to respond with hex (@format=hex@).
getBinfoHex :: Req -> IO Bool
getBinfoHex req = paramOr req format "format" False
  where
    format "hex" = Just True
    format "json" = Just False
    format _ = Nothing

-- | Handler for @GET \/blockchain\/block-height\/:height@: the blocks at a height, as Blockchain.info, in
-- pieces.
handleBinfoBlockHeight :: WebState -> Req -> IO Response
handleBinfoBlockHeight st req =
  inPieces st (.binfoBlockHeight) binfoType $ do
    numtxid <- getNumTxId req
    height <- captureWith req readNum "height"
    bs <- fmap catMaybes $ st.query.getBlocksAtHeight height >>= mapM (st.query.getBlock)
    blocks <- mapM (binfoBlockPieces st numtxid) bs
    return . around "{\"blocks\":[" "]}" $
      concat [around comma mempty block | (comma, block) <- zip ("" : repeat ",") blocks]

-- | The pieces of a block as Blockchain.info, with its transactions as
-- they are read.
--
-- The block is encoded without its transactions, which are the last thing
-- in it, and they are put where the empty list was. Should the encoding of
-- haskoin-store-data come to end another way, the block is read and
-- encoded whole, as it was before.
binfoBlockPieces :: WebState -> Bool -> BlockData -> IO [IO Builder]
binfoBlockPieces st numtxid b = do
  nhs <- fmap catMaybes $ st.query.getBlocksAtHeight (b.height + 1) >>= mapM (st.query.getBlock)
  let h x = H.headerHash x.header
      nxt = [h n | n <- nhs, n.header.prev == h b]
      own = (net, ctx)
      txs = transactionPieces st.query b.txs
      -- as 'toBinfoBlock' makes it, with the number of the transactions
      -- that are not in it yet
      bare =
        BinfoBlock
          { hash = h b,
            version = b.header.version,
            prev = b.header.prev,
            merkle = b.header.merkle,
            timestamp = b.header.timestamp,
            bits = b.header.bits,
            next = nxt,
            fee = b.fee,
            nonce = b.header.nonce,
            ntx = fromIntegral (length b.txs),
            size = b.size,
            index = b.height,
            main = b.main,
            height = b.height,
            weight = b.weight,
            txs = []
          }
      bytes = encodingToLazyByteString (marshalEncoding own bare)
      (before, after) = L.splitAt (L.length bytes - 3) bytes
  if after == "[]}"
    then
      around (lazyByteString before) "}"
        <$> listPieces (marshalEncoding own . toBinfoTxSimple numtxid) txs
    else do
      whole <- map (toBinfoTxSimple numtxid) . concat <$> sequence txs
      return [return (fromEncoding (marshalEncoding own (toBinfoBlock b whole nxt)))]
  where
    net = st.config.store.net
    ctx = st.config.store.ctx

-- | The headers of a response in JSON of the Blockchain.info endpoints.
binfoType :: ResponseHeaders
binfoType = [(hContentType, "application/json; charset=utf-8")]

-- | Handler for @GET \/blockchain\/latestblock@: the best block, as Blockchain.info.
handleBinfoLatest :: WebState -> Req -> IO Response
handleBinfoLatest st req =
  withMetrics st (.binfoBlockLatest) $ do
    numtxid <- getNumTxId req
    b <- (bestBlock st.query) >>= maybe (raise st ThingNotFound) return
    streamEncoding $
      toEncoding
        BinfoHeader
          { hash = H.headerHash b.header,
            timestamp = b.header.timestamp,
            index = b.height,
            height = b.height,
            txids = map (encodeBinfoTxId numtxid) b.txs
          }

-- | Handler for @GET \/blockchain\/rawblock\/:block@: a block with its transactions, as Blockchain.info, in
-- pieces.
handleBinfoBlock :: WebState -> Req -> IO Response
handleBinfoBlock st req = do
  numtxid <- getNumTxId req
  hex <- getBinfoHex req
  let kind = if hex then [(hContentType, "text/plain; charset=utf-8")] else binfoType
  inPieces st (.binfoBlockRaw) kind $ do
    bh <-
      captureWith req parseBinfoBlockId "block" >>= \case
        BinfoBlockHash bh -> return bh
        BinfoBlockIndex i -> st.query.getBlocksAtHeight i >>= maybe (raise st ThingNotFound) return . listToMaybe
    b <- maybe (raise st ThingNotFound) return =<< st.query.getBlock bh
    if hex then return (rawBlock st.query b) else binfoBlockPieces st numtxid b

-- | Transaction by hash or short number. Fails with 'TxIndexConflict' if a short number matches several.
getBinfoTx ::
  WebState ->
  BinfoTxId ->
  IO (Either Except Transaction)
getBinfoTx st txid = do
  tx <- case txid of
    BinfoTxIdHash h -> maybeToList <$> C.getTransaction st.query h
    BinfoTxIdIndex i -> C.getNumTransaction st.query i
  case tx of
    [t] -> return $ Right t
    [] -> return $ Left ThingNotFound
    ts ->
      let tids = map (txHash . transactionData) ts
       in return $ Left (TxIndexConflict tids)

-- | Handler for @GET \/blockchain\/rawtx\/:txid@: a transaction, as Blockchain.info.
handleBinfoTx :: WebState -> Req -> IO Response
handleBinfoTx st req =
  withMetrics st (.binfoTxRaw) $ do
    numtxid <- getNumTxId req
    hex <- getBinfoHex req
    txid <- captureWith req parseBinfoTxId "txid"
    tx <-
      getBinfoTx st txid >>= \case
        Right t -> return t
        Left e -> raise st e
    if hex then hx tx else js numtxid tx
  where
    js numtxid t = do
      let net = st.config.store.net
      let ctx = st.config.store.ctx
      streamEncoding $ marshalEncoding (net, ctx) $ toBinfoTxSimple numtxid t
    hx t = do
      textResponse . encodeHexLazy . runPutL . serialize $ transactionData t

-- | Handler for @GET \/blockchain\/q\/txtotalbtcoutput\/:txid@: total value of the outputs of a transaction.
handleBinfoTotalOut :: WebState -> Req -> IO Response
handleBinfoTotalOut st req =
  withMetrics st (.binfoQtxtotalbtcoutput) $ do
    txid <- captureWith req parseBinfoTxId "txid"
    tx <-
      getBinfoTx st txid >>= \case
        Right t -> return t
        Left e -> raise st e
    textResponse $ cs $ show $ sum $ map (.value) tx.outputs

-- | Handler for @GET \/blockchain\/q\/txfee\/:txid@: fee of a transaction.
handleBinfoTxFees :: WebState -> Req -> IO Response
handleBinfoTxFees st req =
  withMetrics st (.binfoQtxfee) $ do
    txid <- captureWith req parseBinfoTxId "txid"
    tx <-
      getBinfoTx st txid >>= \case
        Right t -> return t
        Left e -> raise st e
    let i = sum $ map (.value) $ filter f tx.inputs
        o = sum $ map (.value) tx.outputs
    textResponse . cs . show $ i - o
  where
    f StoreInput {} = True
    f StoreCoinbase {} = False

-- | Handler for @GET \/blockchain\/q\/txresult\/:txid\/:addr@: net amount that a transaction sends to an address.
handleBinfoTxResult :: WebState -> Req -> IO Response
handleBinfoTxResult st req =
  withMetrics st (.binfoQtxresult) $ do
    txid <- captureWith req parseBinfoTxId "txid"
    addr <- getAddress st =<< capture req "addr"
    tx <-
      getBinfoTx st txid >>= \case
        Right t -> return t
        Left e -> raise st e
    let i = toInteger $ sum $ map (.value) $ filter (f addr) tx.inputs
        o = toInteger $ sum $ map (.value) $ filter (g addr) tx.outputs
    textResponse $ cs $ show $ o - i
  where
    f addr StoreInput {address = Just a} = a == addr
    f _ _ = False
    g addr StoreOutput {address = Just a} = a == addr
    g _ _ = False

-- | Handler for @GET \/blockchain\/q\/txtotalbtcinput\/:txid@: total value of the inputs of a transaction.
handleBinfoTotalInput :: WebState -> Req -> IO Response
handleBinfoTotalInput st req =
  withMetrics st (.binfoQtxtotalbtcinput) $ do
    txid <- captureWith req parseBinfoTxId "txid"
    tx <-
      getBinfoTx st txid >>= \case
        Right t -> return t
        Left e -> raise st e
    textResponse $ cs $ show $ sum $ map (.value) $ filter f $ tx.inputs
  where
    f StoreInput {} = True
    f StoreCoinbase {} = False

-- | Handler for @GET \/blockchain\/unconfirmed-transactions@: mempool transactions, as Blockchain.info.
handleBinfoMempool :: WebState -> Req -> IO Response
handleBinfoMempool st req =
  withMetrics st (.binfoMempool) $ do
    numtxid <- getNumTxId req
    offset <- getBinfoOffset st req
    n <- getBinfoCount st req "limit"
    -- a limit of 0 would mean no limit to st.query.getMempoolPage
    txids <-
      if n <= 0
        then return []
        else
          map snd
            <$> st.query.getMempoolPage (Limits (fromIntegral n) (fromIntegral offset) Nothing)
    txs <- catMaybes <$> C.getTransactions st.query txids
    let net = st.config.store.net
    let mem = BinfoMempool $ map (toBinfoTxSimple numtxid) txs
    let ctx = st.config.store.ctx
    streamEncoding $ marshalEncoding (net, ctx) mem

-- | Handler for @GET \/blockchain\/q\/getblockcount@: height of the best header.
handleBinfoGetBlockCount :: WebState -> Req -> IO Response
handleBinfoGetBlockCount st _ =
  withMetrics st (.binfoQgetblockcount) $ do
    let ch = st.config.store.chain
    bn <- chainGetBest ch
    textResponse $ TL.pack $ show bn.height

-- | Handler for @GET \/blockchain\/q\/latesthash@: hash of the best header.
handleBinfoLatestHash :: WebState -> Req -> IO Response
handleBinfoLatestHash st _ =
  withMetrics st (.binfoQlatesthash) $ do
    let ch = st.config.store.chain
    bn <- chainGetBest ch
    textResponse $ TL.fromStrict $ H.blockHashToHex $ H.headerHash bn.header

-- | Handler for @GET \/blockchain\/q\/bcperblock@: block subsidy at the next height.
handleBinfoSubsidy :: WebState -> Req -> IO Response
handleBinfoSubsidy st _ =
  withMetrics st (.binfoQbcperblock) $ do
    let ch = st.config.store.chain
    let net = st.config.store.net
    bn <- chainGetBest ch
    textResponse $
      cs $
        show $
          (/ (100 * 1000 * 1000 :: Double)) $
            fromIntegral $
              H.computeSubsidy net (bn.height + 1)

-- | Handler for @GET \/blockchain\/q\/addresstohash\/:addr@: hash160 of an address in hex.
handleBinfoAddrToHash :: WebState -> Req -> IO Response
handleBinfoAddrToHash st req =
  withMetrics st (.binfoQaddresstohash) $ do
    addr <- getAddress st =<< capture req "addr"
    if
      | Just h <- addressHash160 addr ->
          textResponse $ encodeHexLazy $ runPutL $ serialize h
      | Just h <- addressHash256 addr ->
          textResponse $ encodeHexLazy $ runPutL $ serialize h
      | otherwise ->
          textResponse ""

-- | Handler for @GET \/blockchain\/q\/hashtoaddress\/:hash@: pay-to-public-key-hash address of a hash160.
handleBinfoHashToAddr :: WebState -> Req -> IO Response
handleBinfoHashToAddr st req =
  withMetrics st (.binfoQhashtoaddress) $ do
    bs <- maybe next return . decodeHex =<< capture req "hash"
    let net = st.config.store.net
    hash <- either (const next) return $ decode bs
    addr <- maybe next return $ addrToText net $ PubKeyAddress hash
    textResponse $ TL.fromStrict addr

-- | Handler for @GET \/blockchain\/q\/addrpubkey\/:pubkey@: address of a public key.
handleBinfoAddrPubkey :: WebState -> Req -> IO Response
handleBinfoAddrPubkey st req =
  withMetrics st (.binfoQaddrpubkey) $ do
    hex <- capture req "pubkey"
    let ctx = st.config.store.ctx
    pubkey <-
      maybe next (return . pubKeyAddr ctx) $
        eitherToMaybe . unmarshal ctx =<< decodeHex hex
    let net = st.config.store.net
    case addrToText net pubkey of
      Nothing -> raise st ThingNotFound
      Just a -> textResponse $ TL.fromStrict a

-- | Handler for @GET \/blockchain\/q\/pubkeyaddr\/:addr@: public key of an
-- address, found in a transaction input that spends from it.
handleBinfoPubKeyAddr :: WebState -> Req -> IO Response
handleBinfoPubKeyAddr st req =
  withMetrics st (.binfoQpubkeyaddr) $ do
    addr <- getAddress st =<< capture req "addr"
    mi <- strm addr
    i <- case mi of
      Nothing -> raise st ThingNotFound
      Just i -> return i
    pk <- case extr addr i of
      Left e -> raise st $ UserError e
      Right t -> return t
    textResponse $ encodeHexLazy $ L.fromStrict pk
  where
    strm addr = do
      runConduit $ do
        let l = Limits {limit = 8, offset = 0, start = Nothing}
        streamThings (st.query.getAddressTxs addr) (Just (.txid)) l
          .| concatMapMC (C.getTransaction st.query . (.txid))
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
handleBinfoHashPubkey :: WebState -> Req -> IO Response
handleBinfoHashPubkey st req =
  withMetrics st (.binfoQhashpubkey) $ do
    let ctx = st.config.store.ctx
    pkm <-
      fmap
        (eitherToMaybe . unmarshal ctx <=< decodeHex)
        (capture req "pubkey")
    addr <- case pkm of
      Nothing -> raise st $ UserError "Could not decode public key"
      Just pk -> return $ pubKeyAddr ctx pk
    if
      | Just h <- addressHash160 addr ->
          textResponse $ encodeHexLazy $ runPutL $ serialize h
      | Just h <- addressHash256 addr ->
          textResponse $ encodeHexLazy $ runPutL $ serialize h
      | otherwise ->
          textResponse ""

-- GET Network Information --

-- | Handler for @GET \/peers@: connected peers.
handlePeers ::
  WebState ->
  GetPeers ->
  IO [PeerInfo]
handlePeers st _ =
  withMetrics st (.peers) $ do
    getPeersInformation =<< pure st.config.store.peerMgr

-- | Obtain information about connected peers from peer manager process.
getPeersInformation ::
  PeerMgr -> IO [PeerInfo]
getPeersInformation mgr =
  mapMaybe toInfo <$> (getPeers mgr)
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

-- | The route of @GET \/health@. Its status says in one number what a load
-- balancer needs to know:
--
-- * 200: every check passes;
--
-- * 203: every check passes, but Redis does not answer, so the store
--   answers from its own database and mempool alone;
--
-- * 503: a check fails.
--
-- = Why 203 for a store without its Redis
--
-- Such a store is not broken: its answers are right for what it has seen. A
-- load balancer should go on sending it requests, which it does for any
-- status in the 200s, so the status cannot be an error. But the answers are
-- no longer those of the fleet: they do not come from the copy that the
-- stores share. That is what 203, Non-Authoritative Information, says of a
-- response, and a monitor that expects exactly 200 will notice it. 207 was
-- the other candidate, for a body with a status for each part; it promises
-- a format that this body does not have. The response also names what is
-- missing in a @Haskoin-Degraded@ header.
--
-- = The check of Redis in the body
--
-- The body of a store that has a cache has one more check than the others,
-- 'Haskoin.Store.Data.redis', which says whether Redis answers. It does not
-- count towards the @ok@ of the whole, which stays the answer to "is this
-- store fit to serve".
pathHealth :: WebState -> Route
pathHealth st =
  route (renderStdMethod (resourceMethod proxy)) (capturePath proxy) [] $ \_ -> do
    h <- handleHealth st GetHealth
    let (status, extra)
          | not (isOK h) = (status503, [("Retry-After", "5")])
          | h.redis == Just False = (status203, [("Haskoin-Degraded", "redis")])
          | otherwise = (status200, [])
    return $
      responseLBS status ((hContentType, "application/json") : extra) (encodingToLazyByteString (toEncoding h))
  where
    proxy = Proxy :: Proxy GetHealth

-- | Handler for @GET \/health@: health of the store.
handleHealth ::
  WebState ->
  GetHealth ->
  IO HealthCheck
handleHealth st _ =
  withMetrics st (.health) $ do
    h <- readTVarIO st.health
    up <- readTVarIO st.cacheUp
    -- a store without a cache has no Redis to be without
    let HealthCheck {blocks, lastBlock, lastTx, pendingTxs, peers, network, version, time} = h
    let shares = void st.config.store.cache <|> void st.config.store.fleet
    return HealthCheck {redis = up <$ shares, ..}

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
  n <- round <$> getPOSIXTime
  t <- fromIntegral . (.header.timestamp) <$> (chainGetBest ch)
  return
    TimeHealth
      { age = n - t,
        max = fromIntegral blockTimeout
      }

-- | Age of the newest mempool transaction or best header, and the maximum allowed.
lastTxHealthCheck :: WebConfig -> IO TimeHealth
lastTxHealthCheck WebConfig {noMempool, store = store', limits} = do
  n <- round <$> getPOSIXTime
  b <- fromIntegral . (.header.timestamp) <$> (chainGetBest ch)
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
  count <- fromIntegral . length <$> (getPeers cfg.store.peerMgr)
  return CountHealth {min = fromIntegral cfg.minPeers, count}

-- | Run every health check.
healthCheck :: WebConfig -> IO HealthCheck
healthCheck cfg = do
  blocks <- blockHealthCheck cfg
  lastBlock <- lastBlockHealthCheck cfg.store.chain cfg.limits
  lastTx <- lastTxHealthCheck cfg
  pendingTxs <- pendingTxsHealthCheck cfg
  peers <- peerHealthCheck cfg
  time <- round <$> getPOSIXTime
  return
    HealthCheck
      { network = cfg.store.net.name,
        version = cfg.version,
        -- filled in when the check is served, from what the last request
        -- or ping found
        redis = Nothing,
        ..
      }

-- | Handler for @GET \/dbstats@: RocksDB memory use and statistics, as text.
handleDbStats :: WebState -> Req -> IO Response
handleDbStats st _ =
  withMetrics st (.db) $ do
    let db = st.config.store.db.db
    memory <- (memoryReport db)
    statsM <- (getProperty db Stats)
    textResponse . cs $ memory <> maybe "Could not get stats" cs statsM

-----------------------
-- Parameter Parsing --
-----------------------

-- | Returns @Nothing@ if the parameter is not supplied. Raises an exception on
-- parse failure.
paramOptional :: forall a. (Param a) => WebState -> Req -> IO (Maybe a)
paramOptional st req = do
  let net = st.config.store.net
  let ctx = st.config.store.ctx
  param req label >>= \case
    Nothing -> return Nothing -- Parameter was not supplied
    Just t -> Just <$> readParam (parseParam net ctx . T.splitOn ",") label t
  where
    label = proxyLabel (Proxy :: Proxy a)

-- | Raises an exception if the parameter is not supplied
paramRequired :: forall a. (Param a) => WebState -> Req -> IO a
paramRequired st req = maybe (raise st err) return =<< paramOptional st req
  where
    label = T.unpack (proxyLabel (Proxy :: Proxy a))
    err = UserError $ "Missing parameter " <> label

-- | Returns the default value of a parameter if it is not supplied. Raises an
-- exception on parse failure.
paramDef :: (Default a, Param a) => WebState -> Req -> IO a
paramDef st req = fromMaybe def <$> paramOptional st req

-- | Does not raise exceptions. Passes the request to the next route ('next') if the parameter is
-- not supplied or if parsing fails.
paramCapture :: forall a. (Param a) => WebState -> Req -> IO a
paramCapture st req = do
  let net = st.config.store.net
  let ctx = st.config.store.ctx
  p <- capture req label
  maybe next return (parseParam net ctx (T.splitOn "," p))
  where
    label = proxyLabel (Proxy :: Proxy a)

-- | Read the transaction that a request carries in its body.
parseTxBody :: WebState -> Req -> IO Tx
parseTxBody st req = do
  let net = st.config.store.net
  case decodeTxBody net (L.toStrict req.body) of
    Left _ -> raise st $ UserError "Failed to parse request body"
    Right tx -> return tx

-- | Read a transaction from a request body. The body is the transaction in
-- hex. White space around and inside it is ignored.
--
-- The body must hold one transaction and nothing else. Bytes after the
-- transaction are refused, as Bitcoin Core does for a raw transaction.
--
-- The transaction must be in the format that peers of the network read. A
-- network without SegWit, such as Bitcoin Cash, has the format without
-- witness only. Its nodes would not read the SegWit format, so the store
-- does not take it either.
decodeTxBody :: Network -> C.ByteString -> Either String Tx
decodeTxBody net body =
  case parseBase16 (C.filter (not . isSpace) body) of
    Right b16 -> fromBytes (decodeBase16 b16)
    Left e -> Left (cs e)
  where
    fromBytes = runGetS $ do
      tx <- getNetworkTx net
      done <- isEmpty
      unless done $ fail "Trailing bytes after transaction"
      return tx

-- | Offset in the @offset@ parameter. Responds with an error if above the maximum.
parseOffset :: WebState -> Req -> IO OffsetParam
parseOffset st req = do
  res@(OffsetParam o) <- paramDef st req
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
  IO (Maybe Start)
parseStart _ Nothing = return Nothing
parseStart st (Just s) = case s of
  -- a transaction if there is one with that hash, else a block
  StartParamHash {hash = h} ->
    st.query.getTxData (TxHash h) >>= \case
      Just _ -> return (Just (AtTx (TxHash h)))
      Nothing -> fmap (AtBlock . (.height)) <$> st.query.getBlock (H.BlockHash h)
  StartParamHeight {height = h} -> return (Just (AtBlock (fromIntegral h)))
  StartParamTime {time = q} ->
    fmap (AtBlock . (.height)) <$> C.blockAtOrBefore st.query st.config.store.chain q

-- | Limit, offset and start in the parameters of a request.
parseLimits :: WebState -> Req -> IO LimitsParam
parseLimits st req = LimitsParam <$> paramOptional st req <*> parseOffset st req <*> paramOptional st req

-- | Pagination of a query from request parameters, within the limits for full or short items.
paramToLimits ::
  WebState ->
  Bool ->
  LimitsParam ->
  IO Limits
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

---------------
-- Utilities --
---------------

-- | The queries that the web server answers with: those of the database,
-- with the Redis cache, if there is one, in front of the queries about xpubs
-- and of the bodies of transactions. An xpub that the cache does not have is
-- read from the database, and cached for next time if it is large enough.
--
-- When Redis does not answer, every query is answered from the database, as
-- if the store had no cache. The variable says whether Redis answered the
-- last time: a query that fails clears it, so that the next ones do not wait
-- for Redis in turn, and the health check sets it again when Redis is back.
webQuery :: Store -> TVar Bool -> StoreRead -> StoreRead
webQuery s up base = case s.cache of
  Nothing -> base
  Just c ->
    (base :: StoreRead)
      { getTxData = fmap listToMaybe' . txData c . pure,
        getManyTxData = txData c,
        xPubBals = \x ->
          cached (cacheXPubBals c x) >>= \case
            Just bals -> return bals
            Nothing -> do
              bals <- base.xPubBals x
              _ <- cached (Just <$> cacheRequest c x bals)
              return bals,
        xPubUnspents = \x bals l ->
          cached (cacheXPubUnspents c x bals l) >>= maybe (base.xPubUnspents x bals l) return,
        xPubTxs = \x bals l ->
          cached (cacheXPubTxs c x l) >>= maybe (base.xPubTxs x bals l) return,
        xPubTxCount = \x bals ->
          cached (cacheXPubTxCount c x) >>= maybe (base.xPubTxCount x bals) return
      }
  where
    -- ask Redis, unless it is down, and take a failure, or no answer within
    -- two seconds, for "not cached"
    cached :: IO (Maybe a) -> IO (Maybe a)
    cached ask =
      readTVarIO up >>= \case
        False -> return Nothing
        True ->
          (timeout 2000000 ask `catchAny` \_ -> return Nothing) >>= \case
            Just answer -> return answer
            Nothing -> atomically (writeTVar up False) >> return Nothing
    -- bodies from Redis when the cache has them, so that the transactions
    -- of a cached xpub are served in full even before this store has
    -- imported them, and from the database otherwise
    txData _ [] = return []
    txData c ths = do
      found <- cached (Just <$> cacheTxData c ths)
      let fromCache = fromMaybe (map (const Nothing) ths) found
          missing = [h | (h, Nothing) <- zip ths fromCache]
      fromDB <- HashMap.fromList . zip missing <$> base.getManyTxData missing
      -- deleted ones are left out, wherever they come from
      return $
        zipWith
          (\h m -> notDeleted (maybe (fromMaybe Nothing (HashMap.lookup h fromDB)) Just m))
          ths
          fromCache

-- | The state of the web server, answering without the cache if the flag is
-- 'True', as @nocache=true@ asks.
cacheOr :: Bool -> WebState -> WebState
cacheOr False st = st
cacheOr True st = (st :: WebState) {query = st.direct}

-- | The only element of a list of one optional value.
listToMaybe' :: [Maybe a] -> Maybe a
listToMaybe' [x] = x
listToMaybe' _ = Nothing

-- | Middleware that logs every request with its response status. Only an
-- error of the server is logged as one: a request that the server could
-- not answer although nothing was wrong with it (500).
--
-- A request that the client got wrong (400, 404) is the client's error,
-- and a public server gets them all day. A 503 is the server saying that
-- it is not ready, which its health check and its fleet already report.
-- Both are logged for debugging, as successes are; logged as errors they
-- hid the ones that are.
--
-- A request that took a number of seconds or more to answer gets a
-- warning, with how long it took (0 for never). That is the one thing
-- about a single request that says something of the server: a query that
-- reads far more than it should, or a database that has become slow. The
-- time runs until the last byte of the response is handed to the
-- connection. A long answer is sent while it is read
-- ("Haskoin.Store.Web.Pieces"), so the time that its status took to make
-- says little: the reads that matter come after it. The price is that a
-- client on a slow line, or a proxy that reads slowly, can make a request
-- slow too. A response cut short, by the client leaving or by a failure,
-- is logged with what it took until then. The stream of events, whose
-- response never ends, is left out.
--
-- With the access log on, every request is logged as information instead
-- of for debugging, with the address of the client and the time it took.
-- It is off by default: a public server has a proxy in front that keeps
-- such a log, and knows the address of the client, where the server sees
-- that of the proxy unless the proxy passes it on in @X-Forwarded-For@.
-- It is for a server that is run without one.
logIt :: Logger -> Bool -> Word32 -> Middleware
logIt logger access slow app req respond = do
  start <- getPOSIXTime
  app req $ \res -> do
    sent <- respond res `onException` report start res True
    report start res False
    return sent
  where
    report start res cut = do
      took <- subtract start <$> getPOSIXTime
      let s = responseStatus res
          msg = fmtReq req <> ": " <> fmtStatus s <> if cut then ", cut short" else ""
          seconds = cs (showFFloat (Just 1) (realToFrac took :: Double) "")
          millis = cs (showFFloat (Just 1) (realToFrac took * 1000 :: Double) "")
          client = case lookup "X-Forwarded-For" (requestHeaders req) of
            Just given -> T.strip (T.takeWhile (/= ',') (T.decodeUtf8Lenient given))
            Nothing -> cs (show (remoteHost req))
      if
        | statusIsServerError s && s /= status503 -> logError logger msg
        | access -> logInfo logger (client <> " " <> msg <> ", " <> millis <> " ms")
        | otherwise -> logDebug logger msg
      when (slow > 0 && took >= fromIntegral slow && pathInfo req /= ["events"]) $
        logWarn logger ("Slow request, " <> seconds <> " seconds: " <> msg)

-- | What is wrong with a store whose health check fails, for its log.
ailments :: HealthCheck -> Text
ailments h =
  T.intercalate ", " . concat $
    [ [shown (h.blocks.headers - h.blocks.blocks) <> " blocks behind the headers" | not (isOK h.blocks)],
      [shown h.lastBlock.age <> " seconds since the last block" | not (isOK h.lastBlock)],
      [shown h.lastTx.age <> " seconds since the last transaction" | not (isOK h.lastTx)],
      [shown h.pendingTxs.count <> " transactions waiting to be imported" | not (isOK h.pendingTxs)],
      [shown h.peers.count <> " peers" | not (isOK h.peers)]
    ]
  where
    shown :: (Show a) => a -> Text
    shown = cs . show

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

-- | A parameter read with a parser, or the default if there is none. Answers
-- with an error if it does not parse ('readParam').
paramOr :: Req -> (Text -> Maybe a) -> Text -> a -> IO a
paramOr req f name d = param req name >>= maybe (return d) (readParam f name)

-- | A parameter read with a parser. Answers with an error if there is none or
-- it does not parse ('readParam').
requireParam :: WebState -> Req -> (Text -> Maybe a) -> Text -> IO a
requireParam st req f name =
  param req name >>= maybe (raise st (UserError ("Missing parameter " <> T.unpack name))) (readParam f name)

-- | The value of a parameter, read with a parser. Answers with an error that
-- names the parameter and the value if it does not parse: a value that the
-- server does not understand is a mistake of the client, and answering as if
-- it had not been given would hide the mistake.
readParam :: (Text -> Maybe a) -> Text -> Text -> IO a
readParam f name t =
  maybe (throwIO (invalidParam name t)) return (f t)

-- | The error for a value of a parameter that does not parse.
invalidParam :: Text -> Text -> Except
invalidParam name t =
  UserError . T.unpack $ "Invalid value for parameter " <> name <> ": " <> t

-- | A count, offset or number of confirmations: a whole number, not negative.
readNat :: Text -> Maybe Natural
readNat t = readMaybe (T.unpack t) >>= \(n :: Integer) -> if n < 0 then Nothing else Just (fromInteger n)

-- | @true@ or @false@, in any case.
readBool :: Text -> Maybe Bool
readBool t = case T.toLower t of
  "true" -> Just True
  "false" -> Just False
  _ -> Nothing

-- | A number in decimal.
readNum :: (Read a) => Text -> Maybe a
readNum = readMaybe . T.unpack
