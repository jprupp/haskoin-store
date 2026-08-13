{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE NoFieldSelectors #-}
{-# OPTIONS_GHC -Wno-deprecations #-}

module Haskoin.Store.Web
  ( -- * Web
    WebConfig (..),
    Except (..),
    WebLimits (..),
    runWeb,
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
    forever,
    unless,
    void,
    when,
    (<=<),
  )
import Control.Monad.Logger
  ( MonadLoggerIO,
    logDebugS,
    logErrorS,
  )
import Control.Monad.Reader
  ( MonadReader,
    ReaderT,
    asks,
    local,
    runReaderT,
  )
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Class (MonadTrans)
import Control.Monad.Trans.Control
  ( liftWith,
    restoreT,
  )
import Control.Monad.Trans.Maybe
  ( MaybeT (..),
    runMaybeT,
  )
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
import Data.Aeson.Text (encodeToLazyText)
import Data.Base16.Types (assertBase16)
import Data.ByteString.Base16 (decodeBase16, isBase16)
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
import Data.Text.Lazy (toStrict)
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
    OnlinePeer (..),
    PeerMgr,
    chainGetAncestor,
    chainGetBest,
    getPeers,
    sendMessage,
  )
import Haskoin.Script
import Haskoin.Store.BlockStore
import Haskoin.Store.Cache
import Haskoin.Store.Common
import Haskoin.Store.Data
import Haskoin.Store.Database.Reader
import Haskoin.Store.Manager
import Haskoin.Store.WebCommon
import Haskoin.Transaction
import Haskoin.Util
import NQE
  ( Inbox,
    receive,
    withSubscription,
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
import Network.Wai
  ( Middleware,
    Request (..),
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
import Network.Wreq qualified as Wreq
import Network.Wreq.Session as Wreq (Session)
import Network.Wreq.Session qualified as Wreq.Session
import System.Metrics.StatsD
import UnliftIO
  ( MonadIO,
    MonadUnliftIO,
    TVar,
    askRunInIO,
    async,
    atomically,
    bracket,
    bracket_,
    handleAny,
    liftIO,
    newTVarIO,
    readTVarIO,
    withAsync,
    writeTVar,
  )
import UnliftIO.Concurrent (threadDelay)
import Web.Scotty.Trans qualified as S

type ScottyT m = S.ScottyT (ReaderT WebState m)

type ActionT m = S.ActionT (ReaderT WebState m)

data WebLimits = WebLimits
  { maxItemCount :: !Word32,
    maxFullItemCount :: !Word32,
    maxOffset :: !Word32,
    defItemCount :: !Word32,
    xpubGap :: !Word32,
    xpubGapInit :: !Word32,
    txTimeout :: !Word64,
    blockTimeout :: !Word64
  }
  deriving (Eq, Show)

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

data WebConfig = WebConfig
  { host :: !String,
    port :: !Int,
    store :: !Store,
    maxLaggingBlocks :: !Int,
    maxPendingTxs :: !Int,
    minPeers :: !Int,
    limits :: !WebLimits,
    version :: !String,
    noMempool :: !Bool,
    stats :: !(Maybe Stats),
    tickerRefresh :: !Int,
    tickerURL :: !String,
    priceHistoryURL :: !String,
    noXPub :: !Bool,
    noBlockchainInfo :: !Bool,
    healthCheckInterval :: !Int
  }

data WebState = WebState
  { config :: !WebConfig,
    ticker :: !(TVar (HashMap Text BinfoTicker)),
    metrics :: !(Maybe WebMetrics),
    session :: !Wreq.Session,
    health :: !(TVar HealthCheck)
  }

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

createMetrics :: (MonadIO m) => Stats -> m WebMetrics
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

withGaugeIO :: (MonadUnliftIO m) => StatGauge -> m a -> m a
withGaugeIO g = bracket_ (incrementGauge g 1) (decrementGauge g 1)

withGaugeIncrease ::
  (MonadUnliftIO m) =>
  (WebMetrics -> StatGauge) ->
  ActionT m a ->
  ActionT m a
withGaugeIncrease gf go =
  askl (.metrics) >>= \case
    Nothing -> go
    Just m -> do
      s <- liftWith $ \run -> withGaugeIO (gf m) (run go)
      restoreT $ return s

withMetrics ::
  (MonadUnliftIO m) => (WebMetrics -> StatTiming) -> ActionT m a -> ActionT m a
withMetrics df go =
  askl (.metrics) >>= \case
    Nothing -> go
    Just m ->
      restoreT . return
        =<< liftWith (\run -> bracket time (stop m) (const (run go)))
  where
    time = round . (* 1000) <$> liftIO getPOSIXTime
    stop m t1 = do
      t2 <- time
      let ms = t2 - t1
      liftIO $ addTiming (df m) ms

data SerialAs = SerialAsBinary | SerialAsJSON | SerialAsPrettyJSON
  deriving (Eq, Show)

instance
  (MonadUnliftIO m) =>
  StoreReadBase (ReaderT WebState m)
  where
  getCtx = runInWebReader getCtx
  getNetwork = runInWebReader getNetwork
  getBestBlock = runInWebReader getBestBlock
  getBlocksAtHeight height = runInWebReader (getBlocksAtHeight height)
  getBlock bh = runInWebReader (getBlock bh)
  getTxData th = runInWebReader (getTxData th)
  getSpender op = runInWebReader (getSpender op)
  getUnspent op = runInWebReader (getUnspent op)
  getBalance a = runInWebReader (getBalance a)
  getMempool = runInWebReader getMempool

instance
  (MonadUnliftIO m, MonadLoggerIO m) =>
  StoreReadExtra (ReaderT WebState m)
  where
  getMaxGap = runInWebReader getMaxGap
  getInitialGap = runInWebReader getInitialGap
  getBalances as = runInWebReader (getBalances as)
  getAddressesTxs as = runInWebReader . getAddressesTxs as
  getAddressTxs a = runInWebReader . getAddressTxs a
  getAddressUnspents a = runInWebReader . getAddressUnspents a
  getAddressesUnspents as = runInWebReader . getAddressesUnspents as
  xPubBals = runInWebReader . xPubBals
  xPubUnspents xpub xbals = runInWebReader . xPubUnspents xpub xbals
  xPubTxs xpub xbals = runInWebReader . xPubTxs xpub xbals
  xPubTxCount xpub = runInWebReader . xPubTxCount xpub
  getNumTxData = runInWebReader . getNumTxData

instance (MonadUnliftIO m) => StoreReadBase (ActionT m) where
  getCtx = lift getCtx
  getNetwork = lift getNetwork
  getBestBlock = lift getBestBlock
  getBlocksAtHeight = lift . getBlocksAtHeight
  getBlock = lift . getBlock
  getTxData = lift . getTxData
  getSpender = lift . getSpender
  getUnspent = lift . getUnspent
  getBalance = lift . getBalance
  getMempool = lift getMempool

instance (MonadUnliftIO m, MonadLoggerIO m) => StoreReadExtra (ActionT m) where
  getBalances = lift . getBalances
  getAddressesTxs as = lift . getAddressesTxs as
  getAddressTxs a = lift . getAddressTxs a
  getAddressUnspents a = lift . getAddressUnspents a
  getAddressesUnspents as = lift . getAddressesUnspents as
  xPubBals = lift . xPubBals
  xPubUnspents xpub xbals = lift . xPubUnspents xpub xbals
  xPubTxs xpub xbals = lift . xPubTxs xpub xbals
  xPubTxCount xpub = lift . xPubTxCount xpub
  getMaxGap = lift getMaxGap
  getInitialGap = lift getInitialGap
  getNumTxData = lift . getNumTxData

-------------------
-- Path Handlers --
-------------------

runWeb :: (MonadUnliftIO m, MonadLoggerIO m) => WebConfig -> m ()
runWeb config = do
  ticker <- newTVarIO HashMap.empty
  metrics <- mapM createMetrics config.stats
  session <- liftIO Wreq.Session.newAPISession
  health <- runHealthCheck >>= newTVarIO
  let state = WebState {..}
  withAsync (priceUpdater session ticker) $ \_a1 ->
    withAsync (healthCheckLoop health) $ \_a2 -> do
      logger <- logIt
      runner <- askRunInIO
      S.scottyOptsT opts (runner . flip runReaderT state) $ do
        S.middleware $ webSocketEvents state
        S.middleware logger
        handlePaths config
        S.notFound $ raise ThingNotFound
  where
    priceUpdater session =
      unless config.noBlockchainInfo
        . price
          config.store.net
          session
          config.tickerURL
          config.tickerRefresh
    runHealthCheck = runReaderT (healthCheck config) config.store.db
    healthCheckLoop v = forever $ do
      threadDelay (config.healthCheckInterval * 1000 * 1000)
      runHealthCheck >>= atomically . writeTVar v
    opts = S.defaultOptions {S.settings = settings defaultSettings}
    settings = setPort config.port . setHost (fromString config.host)

getRates ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  Network ->
  Wreq.Session ->
  String ->
  Text ->
  [Word64] ->
  m [BinfoRate]
getRates net session url currency times = do
  handleAny err $ do
    r <-
      liftIO $
        Wreq.asJSON
          =<< Wreq.Session.postWith opts session url body
    return $ r ^. Wreq.responseBody
  where
    err _ = do
      $(logErrorS) "Web" "Could not get historic prices"
      return []
    body = toJSON times
    base =
      Wreq.defaults
        & Wreq.param "base" .~ [T.toUpper $ T.pack net.name]
    opts = base & Wreq.param "quote" .~ [currency]

price ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  Network ->
  Wreq.Session ->
  String ->
  Int ->
  TVar (HashMap Text BinfoTicker) ->
  m ()
price net session url pget v = forM_ purl $ \u -> forever $ do
  let err e = $(logErrorS) "Price" $ cs (show e)
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

raise :: (MonadIO m) => Except -> ActionT m a
raise e = do
  askl (.metrics) >>= mapM_ \m -> do
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

errStatus :: Except -> Status
errStatus ThingNotFound = status404
errStatus BadRequest = status400
errStatus UserError {} = status400
errStatus StringError {} = status400
errStatus ServerError = status500
errStatus TxIndexConflict {} = status409
errStatus ServerTimeout = status500
errStatus RequestTooLarge = status413

handlePaths ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  WebConfig ->
  S.ScottyT (ReaderT WebState m) ()
handlePaths cfg = do
  -- Block Paths
  pathCompact
    (GetBlockBest <$> paramDef)
    scottyBlockBest
    (marshalEncoding net)
    (marshalValue net)
  pathCompact
    (GetBlockHeight <$> paramCapture <*> paramDef)
    (fmap SerialList . scottyBlockHeight)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetBlockTime <$> paramCapture <*> paramDef)
    scottyBlockTime
    (marshalEncoding net)
    (marshalValue net)
  pathCompact
    (GetBlockMTP <$> paramCapture <*> paramDef)
    scottyBlockMTP
    (marshalEncoding net)
    (marshalValue net)
  pathCompact
    (GetBlock <$> paramCapture <*> paramDef)
    scottyBlock
    (marshalEncoding net)
    (marshalValue net)
  pathCompact
    (GetTx <$> paramCapture)
    scottyTx
    (marshalEncoding net)
    (marshalValue net)
  pathCompact
    (GetTxRaw <$> paramCapture)
    scottyTxRaw
    toEncoding
    toJSON
  pathCompact
    (PostTx <$> parseBody)
    scottyPostTx
    toEncoding
    toJSON
  pathCompact
    (GetAddrTxs <$> paramCapture <*> parseLimits)
    (fmap SerialList . scottyAddrTxs)
    toEncoding
    toJSON
  pathCompact
    (GetAddrBalance <$> paramCapture)
    scottyAddrBalance
    (marshalEncoding net)
    (marshalValue net)
  pathCompact
    (GetAddrUnspent <$> paramCapture <*> parseLimits)
    (fmap SerialList . scottyAddrUnspent)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetPeers & return)
    (fmap SerialList . scottyPeers)
    toEncoding
    toJSON
  pathCompact
    (GetHealth & return)
    scottyHealth
    toEncoding
    toJSON
  pathCompact
    (GetMempool <$> paramOptional <*> parseOffset)
    (fmap SerialList . scottyMempool)
    toEncoding
    toJSON
  pathCompact
    (GetBlocks <$> paramRequired <*> paramDef)
    (fmap SerialList . scottyBlocks)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetBlockRaw <$> paramCapture)
    scottyBlockRaw
    toEncoding
    toJSON
  pathCompact
    (GetBlockBestRaw & return)
    scottyBlockBestRaw
    toEncoding
    toJSON
  pathCompact
    (GetBlockLatest <$> paramDef)
    (fmap SerialList . scottyBlockLatest)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetBlockHeights <$> paramRequired <*> paramDef)
    (fmap SerialList . scottyBlockHeights)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetBlockHeightRaw <$> paramCapture)
    scottyBlockHeightRaw
    toEncoding
    toJSON
  pathCompact
    (GetBlockTimeRaw <$> paramCapture)
    scottyBlockTimeRaw
    toEncoding
    toJSON
  pathCompact
    (GetBlockMTPRaw <$> paramCapture)
    scottyBlockMTPRaw
    toEncoding
    toJSON
  pathCompact
    (GetTxs <$> paramRequired)
    (fmap SerialList . scottyTxs)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetTxsRaw <$> paramRequired)
    scottyTxsRaw
    toEncoding
    toJSON
  pathCompact
    (GetTxsBlock <$> paramCapture)
    (fmap SerialList . scottyTxsBlock)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetTxsBlockRaw <$> paramCapture)
    scottyTxsBlockRaw
    toEncoding
    toJSON
  pathCompact
    (GetTxAfter <$> paramCapture <*> paramCapture)
    scottyTxAfter
    toEncoding
    toJSON
  pathCompact
    (GetAddrsTxs <$> paramRequired <*> parseLimits)
    (fmap SerialList . scottyAddrsTxs)
    toEncoding
    toJSON
  pathCompact
    (GetAddrTxsFull <$> paramCapture <*> parseLimits)
    (fmap SerialList . scottyAddrTxsFull)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetAddrsTxsFull <$> paramRequired <*> parseLimits)
    (fmap SerialList . scottyAddrsTxsFull)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetAddrsBalance <$> paramRequired)
    (fmap SerialList . scottyAddrsBalance)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  pathCompact
    (GetAddrsUnspent <$> paramRequired <*> parseLimits)
    (fmap SerialList . scottyAddrsUnspent)
    (list (marshalEncoding net) . (.get))
    (json_list (marshalValue net) . (.get))
  S.get "/events" scottyEvents
  S.get "/dbstats" scottyDbStats
  unless cfg.noXPub $ do
    pathCompact
      (GetXPub <$> paramCapture <*> paramDef <*> paramDef)
      scottyXPub
      toEncoding
      toJSON
    pathCompact
      (GetXPubTxs <$> paramCapture <*> paramDef <*> parseLimits <*> paramDef)
      (fmap SerialList . scottyXPubTxs)
      toEncoding
      toJSON
    pathCompact
      (GetXPubTxsFull <$> paramCapture <*> paramDef <*> parseLimits <*> paramDef)
      (fmap SerialList . scottyXPubTxsFull)
      (list (marshalEncoding net) . (.get))
      (json_list (marshalValue net) . (.get))
    pathCompact
      (GetXPubBalances <$> paramCapture <*> paramDef <*> paramDef)
      (fmap SerialList . scottyXPubBalances)
      (list (marshalEncoding net) . (.get))
      (json_list (marshalValue net) . (.get))
    pathCompact
      (GetXPubUnspent <$> paramCapture <*> paramDef <*> parseLimits <*> paramDef)
      (fmap SerialList . scottyXPubUnspent)
      (list (marshalEncoding net) . (.get))
      (json_list (marshalValue net) . (.get))
    pathCompact
      (DelCachedXPub <$> paramCapture <*> paramDef)
      scottyDelXPub
      toEncoding
      toJSON
  unless cfg.noBlockchainInfo $ do
    S.post "/blockchain/multiaddr" scottyMultiAddr
    S.get "/blockchain/multiaddr" scottyMultiAddr
    S.get "/blockchain/balance" scottyShortBal
    S.post "/blockchain/balance" scottyShortBal
    S.get "/blockchain/rawaddr/:addr" scottyRawAddr
    S.get "/blockchain/address/:addr" scottyRawAddr
    S.get "/blockchain/xpub/:addr" scottyRawAddr
    S.post "/blockchain/unspent" scottyBinfoUnspent
    S.get "/blockchain/unspent" scottyBinfoUnspent
    S.get "/blockchain/rawtx/:txid" scottyBinfoTx
    S.get "/blockchain/rawblock/:block" scottyBinfoBlock
    S.get "/blockchain/latestblock" scottyBinfoLatest
    S.get "/blockchain/unconfirmed-transactions" scottyBinfoMempool
    S.get "/blockchain/block-height/:height" scottyBinfoBlockHeight
    S.get "/blockchain/blocks/:milliseconds" scottyBinfoBlocksDay
    S.get "/blockchain/export-history" scottyBinfoHistory
    S.post "/blockchain/export-history" scottyBinfoHistory
    S.get "/blockchain/q/addresstohash/:addr" scottyBinfoAddrToHash
    S.get "/blockchain/q/hashtoaddress/:hash" scottyBinfoHashToAddr
    S.get "/blockchain/q/addrpubkey/:pubkey" scottyBinfoAddrPubkey
    S.get "/blockchain/q/pubkeyaddr/:addr" scottyBinfoPubKeyAddr
    S.get "/blockchain/q/hashpubkey/:pubkey" scottyBinfoHashPubkey
    S.get "/blockchain/q/getblockcount" scottyBinfoGetBlockCount
    S.get "/blockchain/q/latesthash" scottyBinfoLatestHash
    S.get "/blockchain/q/bcperblock" scottyBinfoSubsidy
    S.get "/blockchain/q/txtotalbtcoutput/:txid" scottyBinfoTotalOut
    S.get "/blockchain/q/txtotalbtcinput/:txid" scottyBinfoTotalInput
    S.get "/blockchain/q/txfee/:txid" scottyBinfoTxFees
    S.get "/blockchain/q/txresult/:txid/:addr" scottyBinfoTxResult
    S.get "/blockchain/q/getreceivedbyaddress/:addr" scottyBinfoReceived
    S.get "/blockchain/q/getsentbyaddress/:addr" scottyBinfoSent
    S.get "/blockchain/q/addressbalance/:addr" scottyBinfoAddrBalance
    S.get "/blockchain/q/addressfirstseen/:addr" scottyFirstSeen
  where
    json_list f = toJSONList . map f
    net = cfg.store.net

pathCompact ::
  (ApiResource a b, MonadUnliftIO m) =>
  ActionT m a ->
  (a -> ActionT m b) ->
  (b -> Encoding) ->
  (b -> Value) ->
  S.ScottyT (ReaderT WebState m) ()
pathCompact parser action encJson encValue =
  pathCommon parser action encJson encValue False

pathCommon ::
  (ApiResource a b, MonadUnliftIO m) =>
  ActionT m a ->
  (a -> ActionT m b) ->
  (b -> Encoding) ->
  (b -> Value) ->
  Bool ->
  ScottyT m ()
pathCommon parser action encJson encValue pretty =
  S.addroute (resourceMethod proxy) (capturePath proxy) $ do
    setHeaders
    proto <- setupContentType pretty
    apiRes <- parser
    res <- action apiRes
    S.raw $ protoSerial proto encJson encValue res
  where
    toProxy :: ActionT m a -> Proxy a
    toProxy = const Proxy
    proxy = toProxy parser

streamEncoding :: (MonadIO m) => Encoding -> ActionT m ()
streamEncoding e = do
  S.setHeader "Content-Type" "application/json; charset=utf-8"
  S.raw (encodingToLazyByteString e)

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

setHeaders :: (MonadIO m) => S.ActionT m ()
setHeaders = S.setHeader "Access-Control-Allow-Origin" "*"

setupJSON :: (MonadUnliftIO m) => Bool -> S.ActionT m SerialAs
setupJSON pretty = do
  S.setHeader "Content-Type" "application/json"
  p <- param "pretty" `rescue` return pretty
  return $ if p then SerialAsPrettyJSON else SerialAsJSON

setupBinary :: (MonadIO m) => S.ActionT m SerialAs
setupBinary = do
  S.setHeader "Content-Type" "application/octet-stream"
  return SerialAsBinary

setupContentType :: (MonadUnliftIO m) => Bool -> S.ActionT m SerialAs
setupContentType pretty = do
  accept <- S.header "accept"
  maybe (setupJSON pretty) setType accept
  where
    setType "application/octet-stream" = setupBinary
    setType _ = setupJSON pretty

-- GET Block / GET Blocks --

scottyBlock ::
  (MonadUnliftIO m) => GetBlock -> ActionT m BlockData
scottyBlock (GetBlock h (NoTx noTx)) = withMetrics (.block) $ do
  getBlock h >>= \case
    Nothing ->
      raise ThingNotFound
    Just b -> do
      return $ pruneTx noTx b

getBlocks ::
  (MonadUnliftIO m) =>
  [H.BlockHash] ->
  Bool ->
  ActionT m [BlockData]
getBlocks hs notx =
  (pruneTx notx <$>) . catMaybes <$> mapM getBlock (nub hs)

scottyBlocks ::
  (MonadUnliftIO m) => GetBlocks -> ActionT m [BlockData]
scottyBlocks (GetBlocks hs (NoTx notx)) =
  withMetrics (.block) $
    getBlocks hs notx

pruneTx :: Bool -> BlockData -> BlockData
pruneTx False b = b
pruneTx True BlockData {..} = BlockData {txs = take 1 txs, ..}

-- GET BlockRaw --

scottyBlockRaw ::
  (MonadUnliftIO m) =>
  GetBlockRaw ->
  ActionT m (RawResult H.Block)
scottyBlockRaw (GetBlockRaw h) =
  withMetrics (.blockRaw) $
    RawResult <$> getRawBlock h

getRawBlock ::
  (MonadUnliftIO m) =>
  H.BlockHash ->
  ActionT m H.Block
getRawBlock h =
  getBlock h >>= maybe (raise ThingNotFound) return >>= lift . toRawBlock

toRawBlock :: (StoreReadBase m) => BlockData -> m H.Block
toRawBlock b = do
  txs <- map transactionData . catMaybes <$> mapM getTransaction b.txs
  return H.Block {header = b.header, txs}

-- GET BlockBest / BlockBestRaw --

scottyBlockBest ::
  (MonadUnliftIO m) => GetBlockBest -> ActionT m BlockData
scottyBlockBest (GetBlockBest (NoTx notx)) =
  withMetrics (.block) $
    maybe (raise ThingNotFound) (return . pruneTx notx) <=< runMaybeT $
      MaybeT getBestBlock >>= MaybeT . getBlock

scottyBlockBestRaw ::
  (MonadUnliftIO m) =>
  GetBlockBestRaw ->
  ActionT m (RawResult H.Block)
scottyBlockBestRaw _ =
  withMetrics (.blockRaw) $
    maybe (raise ThingNotFound) (return . RawResult) <=< runMaybeT $
      MaybeT getBestBlock >>= lift . getRawBlock

-- GET BlockLatest --

scottyBlockLatest ::
  (MonadUnliftIO m) =>
  GetBlockLatest ->
  ActionT m [BlockData]
scottyBlockLatest (GetBlockLatest (NoTx noTx)) =
  withMetrics (.block) $
    getBestBlock >>= maybe (raise ThingNotFound) (go [] <=< getBlock)
  where
    go acc Nothing = return $ reverse acc
    go acc (Just b)
      | b.height <= 0 = return $ reverse acc
      | length acc == 99 = return . reverse $ pruneTx noTx b : acc
      | otherwise = go (pruneTx noTx b : acc) =<< getBlock b.header.prev

-- GET BlockHeight / BlockHeights / BlockHeightRaw --

scottyBlockHeight ::
  (MonadUnliftIO m) => GetBlockHeight -> ActionT m [BlockData]
scottyBlockHeight (GetBlockHeight h (NoTx notx)) =
  withMetrics (.block) $
    getBlocksAtHeight (fromIntegral h)
      >>= (`getBlocks` notx)

scottyBlockHeights ::
  (MonadUnliftIO m) =>
  GetBlockHeights ->
  ActionT m [BlockData]
scottyBlockHeights (GetBlockHeights (HeightsParam heights) (NoTx notx)) =
  withMetrics (.block) $
    mapM (getBlocksAtHeight . fromIntegral) heights
      >>= (`getBlocks` notx) . concat

scottyBlockHeightRaw ::
  (MonadUnliftIO m) =>
  GetBlockHeightRaw ->
  ActionT m (RawResultList H.Block)
scottyBlockHeightRaw (GetBlockHeightRaw h) =
  withMetrics (.blockRaw) $
    fmap RawResultList $
      mapM getRawBlock =<< getBlocksAtHeight (fromIntegral h)

-- GET BlockTime / BlockTimeRaw --

scottyBlockTime ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  GetBlockTime ->
  ActionT m BlockData
scottyBlockTime (GetBlockTime (TimeParam t) (NoTx notx)) =
  withMetrics (.block) $ do
    ch <- askl (.config.store.chain)
    blockAtOrBefore ch t >>= \case
      Nothing -> raise ThingNotFound
      Just b -> return $ pruneTx notx b

scottyBlockMTP ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  GetBlockMTP ->
  ActionT m BlockData
scottyBlockMTP (GetBlockMTP (TimeParam t) (NoTx notx)) =
  withMetrics (.block) $ do
    ch <- askl (.config.store.chain)
    blockAtOrAfterMTP ch t >>= \case
      Nothing -> raise ThingNotFound
      Just b -> return $ pruneTx notx b

scottyBlockTimeRaw ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  GetBlockTimeRaw ->
  ActionT m (RawResult H.Block)
scottyBlockTimeRaw (GetBlockTimeRaw (TimeParam t)) =
  withMetrics (.blockRaw) $ do
    ch <- askl (.config.store.chain)
    blockAtOrBefore ch t >>= \case
      Nothing -> raise ThingNotFound
      Just b -> RawResult <$> lift (toRawBlock b)

scottyBlockMTPRaw ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  GetBlockMTPRaw ->
  ActionT m (RawResult H.Block)
scottyBlockMTPRaw (GetBlockMTPRaw (TimeParam t)) =
  withMetrics (.blockRaw) $ do
    ch <- askl (.config.store.chain)
    blockAtOrAfterMTP ch t >>= \case
      Nothing -> raise ThingNotFound
      Just b -> RawResult <$> lift (toRawBlock b)

-- GET Transactions --

scottyTx :: (MonadUnliftIO m) => GetTx -> ActionT m Transaction
scottyTx (GetTx txid) =
  withMetrics (.tx) $
    getTransaction txid >>= maybe (raise ThingNotFound) return

scottyTxs ::
  (MonadUnliftIO m) => GetTxs -> ActionT m [Transaction]
scottyTxs (GetTxs txids) =
  withMetrics (.tx) $
    catMaybes <$> mapM getTransaction (nub txids)

scottyTxRaw ::
  (MonadUnliftIO m) => GetTxRaw -> ActionT m (RawResult Tx)
scottyTxRaw (GetTxRaw txid) =
  withMetrics (.txRaw) $
    getTransaction txid
      >>= \case
        Nothing -> raise ThingNotFound
        Just tx -> return $ RawResult (transactionData tx)

scottyTxsRaw ::
  (MonadUnliftIO m) =>
  GetTxsRaw ->
  ActionT m (RawResultList Tx)
scottyTxsRaw (GetTxsRaw txids) =
  withMetrics (.txRaw) $
    RawResultList . map transactionData . catMaybes
      <$> mapM getTransaction (nub txids)

getTxsBlock ::
  (MonadUnliftIO m) =>
  H.BlockHash ->
  ActionT m [Transaction]
getTxsBlock h =
  getBlock h >>= \case
    Nothing -> raise ThingNotFound
    Just b -> catMaybes <$> mapM getTransaction b.txs

scottyTxsBlock ::
  (MonadUnliftIO m) =>
  GetTxsBlock ->
  ActionT m [Transaction]
scottyTxsBlock (GetTxsBlock h) =
  withMetrics (.txBlock) $ getTxsBlock h

scottyTxsBlockRaw ::
  (MonadUnliftIO m) =>
  GetTxsBlockRaw ->
  ActionT m (RawResultList Tx)
scottyTxsBlockRaw (GetTxsBlockRaw h) =
  withMetrics (.txBlockRaw) $
    RawResultList . map transactionData <$> getTxsBlock h

-- GET TransactionAfterHeight --

scottyTxAfter ::
  (MonadUnliftIO m) =>
  GetTxAfter ->
  ActionT m (GenericResult (Maybe Bool))
scottyTxAfter (GetTxAfter txid height) =
  withMetrics (.txAfter) $
    GenericResult . fst <$> cbAfterHeight (fromIntegral height) txid

-- | Check if any of the ancestors of this transaction is a coinbase after the
-- specified height. Returns 'Nothing' if answer cannot be computed before
-- hitting limits.
cbAfterHeight ::
  (StoreReadBase m) =>
  H.BlockHeight ->
  TxHash ->
  m (Maybe Bool, Int)
cbAfterHeight height txid =
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
      getTransaction t >>= \case
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

scottyPostTx :: (MonadUnliftIO m) => PostTx -> ActionT m TxId
scottyPostTx (PostTx tx) =
  withMetrics (.txPost) $ do
    cfg <- askl (.config)
    lift (publishTx cfg tx)
    return (TxId (txHash tx))

-- | Send transaction to all connected peers.
publishTx ::
  (MonadUnliftIO m, StoreReadBase m) =>
  WebConfig ->
  Tx ->
  m ()
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

scottyMempool ::
  (MonadUnliftIO m) => GetMempool -> ActionT m [TxHash]
scottyMempool (GetMempool limitM (OffsetParam o)) =
  withMetrics (.mempool) $ do
    WebLimits {..} <- askl (.config.limits)
    let wl' = WebLimits {maxItemCount = 0, ..}
        l = Limits (validateLimit wl' False limitM) (fromIntegral o) Nothing
    map snd . applyLimits l <$> getMempool

webSocketEvents :: WebState -> Middleware
webSocketEvents s =
  websocketsOr defaultConnectionOptions (wrap . events)
  where
    pub = s.config.store.pub
    gauge = (.events) <$> s.metrics
    wrap f = case gauge of
      Nothing -> f
      Just g -> withGaugeIO g f
    events pending = withSubscription pub $ \sub -> do
      let path = requestPath $ pendingRequest pending
      if path == "/events"
        then do
          conn <- acceptRequest pending
          forever $
            receiveEvent sub >>= \case
              Nothing -> return ()
              Just event -> sendTextData conn (A.encode event)
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

scottyEvents :: (MonadUnliftIO m) => ActionT m ()
scottyEvents =
  withGaugeIncrease (.events) $ do
    setHeaders
    proto <- setupContentType False
    pub <- askl (.config.store.pub)
    S.stream $ \io flush' ->
      withSubscription pub $ \sub ->
        forever $ do
          flush'
          receiveEvent sub >>= \case
            Nothing -> return ()
            Just msg -> io (serial proto msg)
  where
    serial proto e =
      lazyByteString $
        protoSerial proto toEncoding toJSON e <> newLine proto
    newLine SerialAsBinary = mempty
    newLine SerialAsJSON = "\n"
    newLine SerialAsPrettyJSON = mempty

receiveEvent :: Inbox StoreEvent -> IO (Maybe Event)
receiveEvent sub =
  go <$> receive sub
  where
    go = \case
      StoreBestBlock b -> Just (EventBlock b)
      StoreMempoolNew t -> Just (EventTx t)
      StoreMempoolDelete t -> Just (EventTx t)
      _ -> Nothing

-- GET Address Transactions --

scottyAddrTxs ::
  (MonadUnliftIO m, MonadLoggerIO m) => GetAddrTxs -> ActionT m [TxRef]
scottyAddrTxs (GetAddrTxs addr pLimits) =
  withMetrics (.addressTx) $
    getAddressTxs addr =<< paramToLimits False pLimits

scottyAddrsTxs ::
  (MonadUnliftIO m, MonadLoggerIO m) => GetAddrsTxs -> ActionT m [TxRef]
scottyAddrsTxs (GetAddrsTxs addrs pLimits) =
  withMetrics (.addressTx) $
    getAddressesTxs addrs =<< paramToLimits False pLimits

scottyAddrTxsFull ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  GetAddrTxsFull ->
  ActionT m [Transaction]
scottyAddrTxsFull (GetAddrTxsFull addr pLimits) =
  withMetrics (.addressTxFull) $ do
    txs <- getAddressTxs addr =<< paramToLimits True pLimits
    catMaybes <$> mapM (getTransaction . (.txid)) txs

scottyAddrsTxsFull ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  GetAddrsTxsFull ->
  ActionT m [Transaction]
scottyAddrsTxsFull (GetAddrsTxsFull addrs pLimits) =
  withMetrics (.addressTxFull) $ do
    txs <- getAddressesTxs addrs =<< paramToLimits True pLimits
    catMaybes <$> mapM (getTransaction . (.txid)) txs

scottyAddrBalance :: (MonadUnliftIO m) => GetAddrBalance -> ActionT m Balance
scottyAddrBalance (GetAddrBalance addr) =
  withMetrics (.addressBalance) $ getDefaultBalance addr

scottyAddrsBalance ::
  (MonadUnliftIO m, MonadLoggerIO m) => GetAddrsBalance -> ActionT m [Balance]
scottyAddrsBalance (GetAddrsBalance addrs) =
  withMetrics (.addressBalance) $ getBalances addrs

scottyAddrUnspent ::
  (MonadUnliftIO m, MonadLoggerIO m) => GetAddrUnspent -> ActionT m [Unspent]
scottyAddrUnspent (GetAddrUnspent addr pLimits) =
  withMetrics (.addressUnspent) $
    getAddressUnspents addr =<< paramToLimits False pLimits

scottyAddrsUnspent ::
  (MonadUnliftIO m, MonadLoggerIO m) => GetAddrsUnspent -> ActionT m [Unspent]
scottyAddrsUnspent (GetAddrsUnspent addrs pLimits) =
  withMetrics (.addressUnspent) $
    getAddressesUnspents addrs =<< paramToLimits False pLimits

-- GET XPubs --

scottyXPub ::
  (MonadUnliftIO m, MonadLoggerIO m) => GetXPub -> ActionT m XPubSummary
scottyXPub (GetXPub xpub deriv (NoCache noCache)) =
  withMetrics (.xpub) $
    let xs = XPubSpec xpub deriv
     in xPubSummary xs <$> lift (runNoCache noCache $ xPubBals xs)

scottyDelXPub ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  DelCachedXPub ->
  ActionT m (GenericResult Bool)
scottyDelXPub (DelCachedXPub xpub deriv) =
  withMetrics (.xpubDelete) $ do
    c <- askl (.config.store.cache)
    n <- lift (withCache c $ cacheDelXPubs [XPubSpec xpub deriv])
    return $ GenericResult (n > 0)

getXPubTxs ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  XPubKey ->
  DeriveType ->
  LimitsParam ->
  Bool ->
  ActionT m [TxRef]
getXPubTxs xpub deriv plimits nocache = do
  limits <- paramToLimits False plimits
  let xs = XPubSpec xpub deriv
  xbals <- xPubBals xs
  lift $ runNoCache nocache $ xPubTxs xs xbals limits

scottyXPubTxs ::
  (MonadUnliftIO m, MonadLoggerIO m) => GetXPubTxs -> ActionT m [TxRef]
scottyXPubTxs (GetXPubTxs xpub deriv plimits (NoCache nocache)) =
  withMetrics (.xpubTx) $
    getXPubTxs xpub deriv plimits nocache

scottyXPubTxsFull ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  GetXPubTxsFull ->
  ActionT m [Transaction]
scottyXPubTxsFull (GetXPubTxsFull xpub deriv plimits (NoCache nocache)) =
  withMetrics (.xpubTxFull) $
    fmap catMaybes $
      lift . runNoCache nocache . mapM (getTransaction . (.txid))
        =<< getXPubTxs xpub deriv plimits nocache

scottyXPubBalances ::
  (MonadUnliftIO m, MonadLoggerIO m) => GetXPubBalances -> ActionT m [XPubBal]
scottyXPubBalances (GetXPubBalances xpub deriv (NoCache noCache)) =
  withMetrics (.xpubBalance) $
    lift $
      runNoCache noCache $
        xPubBals (XPubSpec xpub deriv)

scottyXPubUnspent ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  GetXPubUnspent ->
  ActionT m [XPubUnspent]
scottyXPubUnspent (GetXPubUnspent xpub deriv pLimits (NoCache noCache)) =
  withMetrics (.xpubUnspent) $ do
    limits <- paramToLimits False pLimits
    let xspec = XPubSpec xpub deriv
    xbals <- xPubBals xspec
    lift $ runNoCache noCache $ xPubUnspents xspec xbals limits

---------------------------------------
-- Blockchain.info API Compatibility --
---------------------------------------

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

getBinfoAddrsParam ::
  (MonadUnliftIO m) =>
  Text ->
  ActionT m (HashSet BinfoAddr)
getBinfoAddrsParam name = do
  net <- askl (.config.store.net)
  ctx <- askl (.config.store.ctx)
  p <- param name `rescue` return ""
  if T.null p
    then return HashSet.empty
    else case parseBinfoAddr net ctx p of
      Nothing -> raise (UserError "invalid address")
      Just xs -> return $ HashSet.fromList xs

getBinfoActive ::
  (MonadUnliftIO m) =>
  ActionT m (HashSet XPubSpec, HashSet Address)
getBinfoActive = do
  active <- getBinfoAddrsParam "active"
  p2sh <- getBinfoAddrsParam "activeP2SH"
  bech32 <- getBinfoAddrsParam "activeBech32"
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

getNumTxId :: (MonadUnliftIO m) => ActionT m Bool
getNumTxId = fmap not $ param "txidindex" `rescue` return False

getChainHeight :: (MonadUnliftIO m) => ActionT m H.BlockHeight
getChainHeight =
  fmap (.height) $ liftIO . chainGetBest =<< askl (.config.store.chain)

scottyBinfoUnspent :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyBinfoUnspent =
  withMetrics (.binfoUnspent) $ do
    (xspecs, addrs) <- getBinfoActive
    numtxid <- getNumTxId
    limit <- get_limit
    min_conf <- get_min_conf
    net <- askl (.config.store.net)
    ctx <- askl (.config.store.ctx)
    height <- getChainHeight
    let mn u = min_conf > u.confirmations
    xbals <- lift $ getXBals xspecs
    bus <-
      lift . runConduit $
        getBinfoUnspents numtxid height xbals xspecs addrs
          .| (dropWhileC mn >> takeC limit .| sinkList)
    setHeaders
    streamEncoding (marshalEncoding (net, ctx) (BinfoUnspents bus))
  where
    get_limit = fmap (min 1000) $ param "limit" `rescue` return 250
    get_min_conf = param "confirmations" `rescue` return 0

getBinfoUnspents ::
  (StoreReadExtra m, MonadIO m) =>
  Bool ->
  H.BlockHeight ->
  HashMap XPubSpec [XPubBal] ->
  HashSet XPubSpec ->
  HashSet Address ->
  ConduitT () BinfoUnspent m ()
getBinfoUnspents numtxid height xbals xspecs addrs = do
  cs' <- conduits
  joinDescStreams cs' .| mapC (uncurry binfo)
  where
    binfo u xp =
      let conf = case u.block of
            MemRef _ -> 0
            BlockRef h _ -> height - h + 1
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
            let h = xPubUnspents x (xBals x xbals)
                l = let Limits {..} = def in Limits {limit = 16, ..}
            return $ streamThings h Nothing l .| mapC (f x)
      mapM g (HashSet.toList xspecs)
    acounduits =
      let f u = (u, Nothing)
          h = getAddressUnspents
          l = let Limits {..} = def in Limits {limit = 16, ..}
          g a = streamThings (h a) Nothing l .| mapC f
       in map g (HashSet.toList addrs)

getXBals ::
  (StoreReadExtra m) =>
  HashSet XPubSpec ->
  m (HashMap XPubSpec [XPubBal])
getXBals =
  fmap HashMap.fromList . mapM f . HashSet.toList
  where
    f x = (x,) . filter (not . nullBalance . (.balance)) <$> xPubBals x

xBals :: XPubSpec -> HashMap XPubSpec [XPubBal] -> [XPubBal]
xBals = HashMap.findWithDefault []

getBinfoTxs ::
  (StoreReadExtra m, MonadIO m) =>
  HashMap XPubSpec [XPubBal] -> -- xpub balances
  HashMap Address (Maybe BinfoXPubPath) -> -- address book
  HashSet XPubSpec -> -- show xpubs
  HashSet Address -> -- show addrs
  HashSet Address -> -- balance addresses
  BinfoFilter ->
  Bool -> -- numtxid
  Bool -> -- prune outputs
  Int64 -> -- starting balance
  ConduitT () BinfoTx m ()
getBinfoTxs
  xbals
  abook
  sxspecs
  saddrs
  baddrs
  bfilter
  numtxid
  prune
  bal = do
    cs' <- conduits
    joinDescStreams cs' .| go bal
    where
      sxspecs_ls = HashSet.toList sxspecs
      saddrs_ls = HashSet.toList saddrs
      conduits =
        (<>)
          <$> mapM xpub_c sxspecs_ls
          <*> pure (map addr_c saddrs_ls)
      xpub_c x = do
        let f = xPubTxs x (xBals x xbals)
            l = Limits {limit = 16, offset = 0, start = Nothing}
        lift . return $
          streamThings f (Just (.txid)) l
      addr_c a = do
        let l = Limits {limit = 16, offset = 0, start = Nothing}
        streamThings (getAddressTxs a) (Just (.txid)) l
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
      go b =
        await >>= \case
          Nothing -> return ()
          Just (TxRef _ t) ->
            lift (getTransaction t) >>= \case
              Nothing -> go b
              Just x -> do
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

getCashAddr :: (MonadUnliftIO m) => ActionT m Bool
getCashAddr = param "cashaddr" `rescue` return False

getAddress :: (MonadUnliftIO m) => Text -> ActionT m Address
getAddress txt = do
  net <- askl (.config.store.net)
  case textToAddr net txt of
    Nothing -> raise ThingNotFound
    Just a -> return a

getBinfoAddr :: (MonadUnliftIO m) => Text -> ActionT m BinfoAddr
getBinfoAddr param' = do
  txt <- S.captureParam param'
  net <- askl (.config.store.net)
  ctx <- askl (.config.store.ctx)
  let addr = BinfoAddr <$> textToAddr net txt
      xpub = BinfoXpub <$> xPubImport net ctx txt
  maybe S.next return (addr <|> xpub)

scottyBinfoHistory :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyBinfoHistory =
  withMetrics (.binfoExportHistory) $ do
    (xspecs, addrs) <- getBinfoActive
    (startM, endM) <- get_dates
    (code, price') <- getPrice
    xbals <- getXBals xspecs
    let xaddrs = HashSet.fromList $ concatMap (map get_addr) (HashMap.elems xbals)
        aaddrs = xaddrs <> addrs
        cur = price'.fifteen
        cs' = conduits (HashMap.toList xbals) addrs endM
    txs <-
      lift $
        runConduit $
          joinDescStreams cs'
            .| takeWhileC (is_newer startM)
            .| concatMapMC get_transaction
            .| sinkList
    let times = map (\Transaction {..} -> timestamp) txs
    net <- askl (.config.store.net)
    url <- askl (.config.priceHistoryURL)
    session <- askl (.session)
    rates <- map (.price) <$> lift (getRates net session url code times)
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
      getTransaction h
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
      ch <- askl (.config.store.chain)
      startM <- blockAtOrAfter ch start
      endM <- blockAtOrBefore ch end
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
      streamThings (getAddressTxs a) (Just (.txid)) (lim endM)
    xpub_c endM x bs =
      streamThings (xPubTxs x bs) (Just (.txid)) (lim endM)

getPrice :: (MonadUnliftIO m) => ActionT m (Text, BinfoTicker)
getPrice = do
  code <- T.toUpper <$> param "currency" `rescue` return "USD"
  ticker <- askl (.ticker)
  prices <- readTVarIO ticker
  case HashMap.lookup code prices of
    Nothing -> return (code, def)
    Just p -> return (code, p)

getSymbol :: (MonadUnliftIO m) => ActionT m BinfoSymbol
getSymbol = uncurry binfoTickerToSymbol <$> getPrice

scottyBinfoBlocksDay :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyBinfoBlocksDay =
  withMetrics (.binfoBlock) $ do
    t <- min h . (`div` 1000) <$> S.captureParam "milliseconds"
    ch <- askl (.config.store.chain)
    m <- blockAtOrBefore ch t
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
          b' <- getBlock b.header.prev
          (b :) <$> go t b'

scottyMultiAddr :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyMultiAddr =
  withMetrics (.binfoMultiaddr) $ do
    (addrs', _, saddrs, sxpubs, xspecs) <- get_addrs
    numtxid <- getNumTxId
    cashaddr <- getCashAddr
    local' <- getSymbol
    offset <- getBinfoOffset
    n <- getBinfoCount "n"
    prune <- get_prune
    fltr <- get_filter
    xbals <- getXBals xspecs
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
    net <- askl (.config.store.net)
    ctx <- askl (.config.store.ctx)
    best <- get_best_block
    peers <- get_peers
    let baddrs = toBinfoAddrs sabals sxbals xtxns
        abaddrs = toBinfoAddrs abals xbals xtxns
        recv = sum $ map (.received) abaddrs
        sent' = sum $ map (.sent) abaddrs
        txn = fromIntegral $ length ftxs
        wallet =
          BinfoWallet
            { balance = bal,
              txs = txn,
              filtered = txn,
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
                <$> xPubTxCount x (xBals x xbals)
          )
        . HashSet.toList
    get_filter = param "filter" `rescue` return BinfoFilterAll
    get_best_block =
      getBestBlock >>= \case
        Nothing -> raise ThingNotFound
        Just bh ->
          getBlock bh >>= \case
            Nothing -> raise ThingNotFound
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
      (xspecs, addrs) <- getBinfoActive
      sh <- getBinfoAddrsParam "onlyShow"
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
       in fmap g . getBalances . HashSet.toList
    get_peers = do
      ps <- lift $ getPeersInformation =<< asks (.config.store.peerMgr)
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

getBinfoCount :: (MonadUnliftIO m) => Text -> ActionT m Int
getBinfoCount str = do
  d <- askl (.config.limits.defItemCount)
  x <- askl (.config.limits.maxFullItemCount)
  i <- min x <$> (param str `rescue` return d)
  return (fromIntegral i :: Int)

getBinfoOffset ::
  (MonadUnliftIO m) =>
  ActionT m Int
getBinfoOffset = do
  x <- askl (.config.limits.maxOffset)
  o <- param "offset" `rescue` return 0
  when (o > x) $
    raise $
      UserError $
        "offset exceeded: " <> show o <> " > " <> show x
  return (fromIntegral o :: Int)

scottyRawAddr :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyRawAddr =
  withMetrics (.binfoAddressRaw) $
    getBinfoAddr "addr" >>= \case
      BinfoAddr addr -> do_addr addr
      BinfoXpub xpub -> do_xpub xpub
  where
    do_xpub xpub = do
      numtxid <- getNumTxId
      derive <- param "derive" `rescue` return DeriveNormal
      let xspec = XPubSpec xpub derive
      n <- getBinfoCount "limit"
      off <- getBinfoOffset
      xbals <- getXBals $ HashSet.singleton xspec
      net <- askl (.config.store.net)
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
      let ra =
            BinfoRawAddr
              { address = BinfoXpub xpub,
                balance = amnt,
                ntx = fromIntegral $ length txs,
                utxo = summary.utxo,
                received = summary.received,
                sent = fromIntegral summary.received - fromIntegral amnt,
                txs = txs
              }
      setHeaders
      ctx <- askl (.config.store.ctx)
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
      n <- getBinfoCount "limit"
      off <- getBinfoOffset
      bal <- fromMaybe (zeroBalance addr) <$> getBalance addr
      net <- askl (.config.store.net)
      let abook = HashMap.singleton addr Nothing
          xspecs = HashSet.empty
          saddrs = HashSet.singleton addr
          bfilter = BinfoFilterAll
          balance = bal.confirmed + bal.unconfirmed
      txs <-
        lift . runConduit $
          getBinfoTxs
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
      ctx <- askl (.config.store.ctx)
      streamEncoding $ marshalEncoding (net, ctx) ra

scottyBinfoReceived :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoReceived =
  withMetrics (.binfoQgetreceivedbyaddress) $ do
    a <- getAddress =<< S.captureParam "addr"
    b <- fromMaybe (zeroBalance a) <$> getBalance a
    setHeaders
    S.text $ cs $ show b.received

scottyBinfoSent :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoSent =
  withMetrics (.binfoQgetsentbyaddress) $ do
    a <- getAddress =<< S.captureParam "addr"
    b <- fromMaybe (zeroBalance a) <$> getBalance a
    setHeaders
    S.text $ cs $ show $ b.received - b.confirmed - b.unconfirmed

scottyBinfoAddrBalance :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoAddrBalance =
  withMetrics (.binfoQaddressbalance) $ do
    a <- getAddress =<< S.captureParam "addr"
    b <- fromMaybe (zeroBalance a) <$> getBalance a
    setHeaders
    S.text $ cs $ show $ b.confirmed + b.unconfirmed

scottyFirstSeen :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyFirstSeen =
  withMetrics (.binfoQaddressfirstseen) $ do
    a <- getAddress =<< S.captureParam "addr"
    ch <- askl (.config.store.chain)
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
          lift . $(logErrorS) "Web" $
            "Could not get ancestor at height "
              <> cs (show h)
              <> " for block "
              <> H.blockHashToHex (H.headerHash bb.header)
          error "Block ancestor retreival error"
    hasone a h = do
      let l = Limits 1 0 (Just (AtBlock h))
      not . null <$> getAddressTxs a l

scottyShortBal :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyShortBal =
  withMetrics (.binfoBalance) $ do
    (xspecs, addrs) <- getBinfoActive
    cashaddr <- getCashAddr
    net <- askl (.config.store.net)
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
              getBalance a >>= \case
                Nothing -> return $ Just (a', shorten (zeroBalance a))
                Just b -> return $ Just (a', shorten b)
    getxbal net xpub = do
      xbals <- xPubBals xpub
      ntx <- fromIntegral <$> xPubTxCount xpub xbals
      let val = sum $ map (.balance.confirmed) xbals
          zro = sum $ map (.balance.unconfirmed) xbals
          exs = [x | x@XPubBal {path = 0 : _} <- xbals]
          final = val + zro
          received = sum $ map (.balance.received) exs
          sbl = BinfoShortBal {final, ntx, received}
      ctx <- askl (.config.store.ctx)
      return (xPubExport net ctx xpub.key, sbl)

getBinfoHex :: (MonadUnliftIO m) => ActionT m Bool
getBinfoHex = (== ("hex" :: Text)) <$> param "format" `rescue` return "json"

scottyBinfoBlockHeight :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoBlockHeight =
  withMetrics (.binfoBlockHeight) $ do
    numtxid <- getNumTxId
    height <- S.captureParam "height"
    bs <- fmap catMaybes $ getBlocksAtHeight height >>= mapM getBlock
    ns <- fmap catMaybes $ getBlocksAtHeight (height + 1) >>= mapM getBlock
    is <- mapM (get_binfo_blocks numtxid ns) bs
    setHeaders
    net <- askl (.config.store.net)
    ctx <- askl (.config.store.ctx)
    streamEncoding $ marshalEncoding (net, ctx) is
  where
    get_binfo_blocks numtxid ns b = do
      txs <- catMaybes <$> mapM getTransaction b.txs
      let h x = H.headerHash x.header
          nbs = [h n | n <- ns, n.header.prev == h b]
          bts = map (toBinfoTxSimple numtxid) txs
      return $ toBinfoBlock b bts nbs

scottyBinfoLatest :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoLatest =
  withMetrics (.binfoBlockLatest) $ do
    numtxid <- getNumTxId
    mb <- runMaybeT $ MaybeT getBestBlock >>= MaybeT . getBlock
    b <- maybe (raise ThingNotFound) return mb
    streamEncoding $
      toEncoding
        BinfoHeader
          { hash = H.headerHash b.header,
            timestamp = b.header.timestamp,
            index = b.height,
            height = b.height,
            txids = map (encodeBinfoTxId numtxid) b.txs
          }

scottyBinfoBlock :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoBlock =
  withMetrics (.binfoBlockRaw) $ do
    numtxid <- getNumTxId
    hex <- getBinfoHex
    S.captureParam "block" >>= \case
      BinfoBlockHash bh -> go numtxid hex bh
      BinfoBlockIndex i ->
        getBlocksAtHeight i >>= \case
          [] -> raise ThingNotFound
          bh : _ -> go numtxid hex bh
  where
    go numtxid hex bh = do
      b <- maybe (raise ThingNotFound) return =<< getBlock bh
      txs <- catMaybes <$> mapM getTransaction b.txs
      nhs <- fmap catMaybes $ getBlocksAtHeight (b.height + 1) >>= mapM getBlock
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
          net <- askl (.config.store.net)
          ctx <- askl (.config.store.ctx)
          streamEncoding $ marshalEncoding (net, ctx) y

getBinfoTx ::
  (MonadLoggerIO m, MonadUnliftIO m) =>
  BinfoTxId ->
  ActionT m (Either Except Transaction)
getBinfoTx txid = do
  tx <- case txid of
    BinfoTxIdHash h -> maybeToList <$> getTransaction h
    BinfoTxIdIndex i -> getNumTransaction i
  case tx of
    [t] -> return $ Right t
    [] -> return $ Left ThingNotFound
    ts ->
      let tids = map (txHash . transactionData) ts
       in return $ Left (TxIndexConflict tids)

scottyBinfoTx :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyBinfoTx =
  withMetrics (.binfoTxRaw) $ do
    numtxid <- getNumTxId
    hex <- getBinfoHex
    txid <- S.captureParam "txid"
    tx <-
      getBinfoTx txid >>= \case
        Right t -> return t
        Left e -> raise e
    if hex then hx tx else js numtxid tx
  where
    js numtxid t = do
      net <- askl (.config.store.net)
      ctx <- askl (.config.store.ctx)
      setHeaders
      streamEncoding $ marshalEncoding (net, ctx) $ toBinfoTxSimple numtxid t
    hx t = do
      setHeaders
      S.text . encodeHexLazy . runPutL . serialize $ transactionData t

scottyBinfoTotalOut :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyBinfoTotalOut =
  withMetrics (.binfoQtxtotalbtcoutput) $ do
    txid <- S.captureParam "txid"
    tx <-
      getBinfoTx txid >>= \case
        Right t -> return t
        Left e -> raise e
    S.text $ cs $ show $ sum $ map (.value) tx.outputs

scottyBinfoTxFees :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyBinfoTxFees =
  withMetrics (.binfoQtxfee) $ do
    txid <- S.captureParam "txid"
    tx <-
      getBinfoTx txid >>= \case
        Right t -> return t
        Left e -> raise e
    let i = sum $ map (.value) $ filter f tx.inputs
        o = sum $ map (.value) tx.outputs
    S.text . cs . show $ i - o
  where
    f StoreInput {} = True
    f StoreCoinbase {} = False

scottyBinfoTxResult :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyBinfoTxResult =
  withMetrics (.binfoQtxresult) $ do
    txid <- S.captureParam "txid"
    addr <- getAddress =<< S.captureParam "addr"
    tx <-
      getBinfoTx txid >>= \case
        Right t -> return t
        Left e -> raise e
    let i = toInteger $ sum $ map (.value) $ filter (f addr) tx.inputs
        o = toInteger $ sum $ map (.value) $ filter (g addr) tx.outputs
    S.text $ cs $ show $ o - i
  where
    f addr StoreInput {address = Just a} = a == addr
    f _ _ = False
    g addr StoreOutput {address = Just a} = a == addr
    g _ _ = False

scottyBinfoTotalInput :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyBinfoTotalInput =
  withMetrics (.binfoQtxtotalbtcinput) $ do
    txid <- S.captureParam "txid"
    tx <-
      getBinfoTx txid >>= \case
        Right t -> return t
        Left e -> raise e
    S.text $ cs $ show $ sum $ map (.value) $ filter f $ tx.inputs
  where
    f StoreInput {} = True
    f StoreCoinbase {} = False

scottyBinfoMempool :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoMempool =
  withMetrics (.binfoMempool) $ do
    numtxid <- getNumTxId
    offset <- getBinfoOffset
    n <- getBinfoCount "limit"
    mempool <- getMempool
    let txids = map snd $ take n $ drop offset mempool
    txs <- catMaybes <$> mapM getTransaction txids
    net <- askl (.config.store.net)
    setHeaders
    let mem = BinfoMempool $ map (toBinfoTxSimple numtxid) txs
    ctx <- askl (.config.store.ctx)
    streamEncoding $ marshalEncoding (net, ctx) mem

scottyBinfoGetBlockCount :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoGetBlockCount =
  withMetrics (.binfoQgetblockcount) $ do
    ch <- askl (.config.store.chain)
    bn <- liftIO $ chainGetBest ch
    setHeaders
    S.text $ TL.pack $ show bn.height

scottyBinfoLatestHash :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoLatestHash =
  withMetrics (.binfoQlatesthash) $ do
    ch <- askl (.config.store.chain)
    bn <- liftIO $ chainGetBest ch
    setHeaders
    S.text $ TL.fromStrict $ H.blockHashToHex $ H.headerHash bn.header

scottyBinfoSubsidy :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoSubsidy =
  withMetrics (.binfoQbcperblock) $ do
    ch <- askl (.config.store.chain)
    net <- askl (.config.store.net)
    bn <- liftIO $ chainGetBest ch
    setHeaders
    S.text $
      cs $
        show $
          (/ (100 * 1000 * 1000 :: Double)) $
            fromIntegral $
              H.computeSubsidy net (bn.height + 1)

scottyBinfoAddrToHash :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoAddrToHash =
  withMetrics (.binfoQaddresstohash) $ do
    addr <- getAddress =<< S.captureParam "addr"
    setHeaders
    if
      | Just h <- addressHash160 addr ->
          S.text $ encodeHexLazy $ runPutL $ serialize h
      | Just h <- addressHash256 addr ->
          S.text $ encodeHexLazy $ runPutL $ serialize h
      | otherwise ->
          return ()

scottyBinfoHashToAddr :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoHashToAddr =
  withMetrics (.binfoQhashtoaddress) $ do
    bs <- maybe S.next return . decodeHex =<< S.captureParam "hash"
    net <- askl (.config.store.net)
    hash <- either (const S.next) return $ decode bs
    addr <- maybe S.next return $ addrToText net $ PubKeyAddress hash
    setHeaders
    S.text $ TL.fromStrict addr

scottyBinfoAddrPubkey :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoAddrPubkey =
  withMetrics (.binfoQaddrpubkey) $ do
    hex <- S.captureParam "pubkey"
    ctx <- askl (.config.store.ctx)
    pubkey <-
      maybe S.next (return . pubKeyAddr ctx) $
        eitherToMaybe . unmarshal ctx =<< decodeHex hex
    net <- askl (.config.store.net)
    setHeaders
    case addrToText net pubkey of
      Nothing -> raise ThingNotFound
      Just a -> S.text $ TL.fromStrict a

scottyBinfoPubKeyAddr :: (MonadUnliftIO m, MonadLoggerIO m) => ActionT m ()
scottyBinfoPubKeyAddr =
  withMetrics (.binfoQpubkeyaddr) $ do
    addr <- getAddress =<< S.captureParam "addr"
    mi <- strm addr
    i <- case mi of
      Nothing -> raise ThingNotFound
      Just i -> return i
    pk <- case extr addr i of
      Left e -> raise $ UserError e
      Right t -> return t
    setHeaders
    S.text $ encodeHexLazy $ L.fromStrict pk
  where
    strm addr = do
      runConduit $ do
        let l = Limits {limit = 8, offset = 0, start = Nothing}
        streamThings (getAddressTxs addr) (Just (.txid)) l
          .| concatMapMC (getTransaction . (.txid))
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

scottyBinfoHashPubkey :: (MonadUnliftIO m) => ActionT m ()
scottyBinfoHashPubkey =
  withMetrics (.binfoQhashpubkey) $ do
    ctx <- askl (.config.store.ctx)
    pkm <-
      fmap
        (eitherToMaybe . unmarshal ctx <=< decodeHex)
        (S.captureParam "pubkey")
    addr <- case pkm of
      Nothing -> raise $ UserError "Could not decode public key"
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

scottyPeers ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  GetPeers ->
  ActionT m [PeerInfo]
scottyPeers _ =
  withMetrics (.peers) $ do
    lift . getPeersInformation =<< askl (.config.store.peerMgr)

-- | Obtain information about connected peers from peer manager process.
getPeersInformation ::
  (MonadLoggerIO m) => PeerMgr -> m [PeerInfo]
getPeersInformation mgr =
  mapMaybe toInfo <$> liftIO (getPeers mgr)
  where
    toInfo op = do
      ver <- op.version
      return
        PeerInfo
          { userAgent = ver.userAgent.get,
            address = show op.address,
            version = ver.version,
            services = ver.services,
            relay = ver.relay
          }

scottyHealth ::
  (MonadUnliftIO m) => GetHealth -> ActionT m HealthCheck
scottyHealth _ =
  withMetrics (.health) $ do
    h <- askl (.health) >>= readTVarIO
    unless (isOK h) $ S.status status503
    return h

blockHealthCheck ::
  (MonadUnliftIO m, StoreReadBase m) =>
  WebConfig ->
  m BlockHealth
blockHealthCheck cfg = do
  let ch = cfg.store.chain
  headers <- (.height) <$> liftIO (chainGetBest ch)
  blocks <-
    maybe 0 (.height)
      <$> runMaybeT (MaybeT getBestBlock >>= MaybeT . getBlock)
  return
    BlockHealth
      { headers,
        blocks,
        max = fromIntegral cfg.maxLaggingBlocks
      }

lastBlockHealthCheck ::
  (MonadUnliftIO m, StoreReadBase m) =>
  Chain ->
  WebLimits ->
  m TimeHealth
lastBlockHealthCheck ch WebLimits {blockTimeout} = do
  n <- round <$> liftIO getPOSIXTime
  t <- fromIntegral . (.header.timestamp) <$> liftIO (chainGetBest ch)
  return
    TimeHealth
      { age = n - t,
        max = fromIntegral blockTimeout
      }

lastTxHealthCheck ::
  (MonadUnliftIO m, StoreReadBase m) =>
  WebConfig ->
  m TimeHealth
lastTxHealthCheck WebConfig {noMempool, store = store', limits} = do
  n <- round <$> liftIO getPOSIXTime
  b <- fromIntegral . (.header.timestamp) <$> liftIO (chainGetBest ch)
  t <-
    getMempool >>= \case
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

pendingTxsHealthCheck ::
  (MonadUnliftIO m, StoreReadBase m) =>
  WebConfig ->
  m MaxHealth
pendingTxsHealthCheck cfg = do
  n <- blockStorePendingTxs cfg.store.block
  return
    MaxHealth
      { max = fromIntegral cfg.maxPendingTxs,
        count = fromIntegral n
      }

peerHealthCheck ::
  (MonadUnliftIO m, StoreReadBase m) =>
  WebConfig ->
  m CountHealth
peerHealthCheck cfg = do
  count <- fromIntegral . length <$> liftIO (getPeers cfg.store.peerMgr)
  return CountHealth {min = fromIntegral cfg.minPeers, count}

healthCheck ::
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadBase m) =>
  WebConfig ->
  m HealthCheck
healthCheck cfg = do
  blocks <- blockHealthCheck cfg
  lastBlock <- lastBlockHealthCheck cfg.store.chain cfg.limits
  lastTx <- lastTxHealthCheck cfg
  pendingTxs <- pendingTxsHealthCheck cfg
  peers <- peerHealthCheck cfg
  time <- round <$> liftIO getPOSIXTime
  let check =
        HealthCheck
          { network = cfg.store.net.name,
            version = cfg.version,
            ..
          }
  unless (isOK check) $ do
    let t = toStrict $ encodeToLazyText check
    $(logErrorS) "Web" $ "Health check failed: " <> t
  return check

scottyDbStats :: (MonadUnliftIO m) => ActionT m ()
scottyDbStats =
  withMetrics (.db) $ do
    setHeaders
    db <- askl (.config.store.db.db)
    statsM <- liftIO (getProperty db Stats)
    S.text $ maybe "Could not get stats" cs statsM

-----------------------
-- Parameter Parsing --
-----------------------

-- | Returns @Nothing@ if the parameter is not supplied. Raises an exception on
-- parse failure.
paramOptional :: forall a m. (Param a, MonadUnliftIO m) => ActionT m (Maybe a)
paramOptional = do
  net <- askl (.config.store.net)
  ctx <- askl (.config.store.ctx)
  fmap Just (param label) `rescue` return Nothing >>= \case
    Nothing -> return Nothing -- Parameter was not supplied
    Just ts -> maybe (raise err) (return . Just) (parseParam net ctx ts)
  where
    label = proxyLabel (Proxy :: Proxy a)
    err = UserError $ "Unable to parse param " <> T.unpack label

-- | Raises an exception if the parameter is not supplied
paramRequired :: forall a m. (Param a, MonadUnliftIO m) => ActionT m a
paramRequired = maybe (raise err) return =<< paramOptional
  where
    label = T.unpack (proxyLabel (Proxy :: Proxy a))
    err = UserError $ "Param " <> label <> " not defined"

-- | Returns the default value of a parameter if it is not supplied. Raises an
-- exception on parse failure.
paramDef :: (Default a, Param a, MonadUnliftIO m) => ActionT m a
paramDef = fromMaybe def <$> paramOptional

-- | Does not raise exceptions. Will call @Scotty.next@ if the parameter is
-- not supplied or if parsing fails.
paramCapture :: forall a m. (Param a, MonadUnliftIO m) => ActionT m a
paramCapture = do
  net <- askl (.config.store.net)
  ctx <- askl (.config.store.ctx)
  p <- S.captureParam label `rescue` S.next
  maybe S.next return (parseParam net ctx p)
  where
    label = proxyLabel (Proxy :: Proxy a)

parseBody :: (MonadIO m, Serial a) => ActionT m a
parseBody = do
  b <- L.toStrict <$> S.body
  case hex b <> bin b of
    Left _ -> raise $ UserError "Failed to parse request body"
    Right x -> return x
  where
    bin = runGetS deserialize
    hex b =
      let ns = C.filter (not . isSpace) b
       in if isBase16 ns
            then bin . decodeBase16 $ assertBase16 ns
            else Left "Invalid hex input"

parseOffset :: (MonadUnliftIO m) => ActionT m OffsetParam
parseOffset = do
  res@(OffsetParam o) <- paramDef
  limits <- askl (.config.limits)
  when (limits.maxOffset > 0 && fromIntegral o > limits.maxOffset) $
    raise . UserError $
      "offset exceeded: " <> show o <> " > " <> show limits.maxOffset
  return res

parseStart ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  Maybe StartParam ->
  ActionT m (Maybe Start)
parseStart Nothing = return Nothing
parseStart (Just s) =
  runMaybeT $
    case s of
      StartParamHash {hash = h} -> start_tx h <|> start_block h
      StartParamHeight {height = h} -> start_height h
      StartParamTime {time = q} -> start_time q
  where
    start_height h = return $ AtBlock $ fromIntegral h
    start_block h = do
      b <- MaybeT $ getBlock (H.BlockHash h)
      return $ AtBlock b.height
    start_tx h = do
      _ <- MaybeT $ getTxData (TxHash h)
      return $ AtTx (TxHash h)
    start_time q = do
      ch <- lift $ askl (.config.store.chain)
      b <- MaybeT $ blockAtOrBefore ch q
      return $ AtBlock b.height

parseLimits :: (MonadUnliftIO m) => ActionT m LimitsParam
parseLimits = LimitsParam <$> paramOptional <*> parseOffset <*> paramOptional

paramToLimits ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  Bool ->
  LimitsParam ->
  ActionT m Limits
paramToLimits full (LimitsParam limitM o startM) = do
  wl <- askl (.config.limits)
  Limits (validateLimit wl full limitM) (fromIntegral o) <$> parseStart startM

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

runInWebReader ::
  (MonadIO m) =>
  CacheT (DatabaseReaderT m) a ->
  ReaderT WebState m a
runInWebReader f = do
  bdb <- asks (.config.store.db)
  mc <- asks (.config.store.cache)
  lift $ runReaderT (withCache mc f) bdb

runNoCache ::
  (MonadIO m) =>
  Bool ->
  ReaderT WebState m a ->
  ReaderT WebState m a
runNoCache False = id
runNoCache True = local $ \s ->
  s {config = s.config {store = s.config.store {cache = Nothing}}}

logIt ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  m Middleware
logIt = do
  runner <- askRunInIO
  return $ \app req respond ->
    app req $ \res -> do
      let s = responseStatus res
          msg = fmtReq req <> ": " <> fmtStatus s
      if statusIsSuccessful s
        then runner $ $(logDebugS) "Web" msg
        else runner $ $(logErrorS) "Web" msg
      respond res

fmtReq :: Request -> Text
fmtReq req =
  let m = requestMethod req
      v = httpVersion req
      p = rawPathInfo req
      q = rawQueryString req
   in T.decodeUtf8 (m <> " " <> p <> q <> " " <> cs (show v))

fmtStatus :: Status -> Text
fmtStatus s = cs (show (statusCode s)) <> " " <> cs (statusMessage s)

rescue ::
  (MonadUnliftIO m) =>
  S.ActionT m a ->
  S.ActionT m a ->
  S.ActionT m a
x `rescue` y = x `S.catch` \(_ :: S.ScottyException) -> y

param ::
  (MonadUnliftIO m, S.Parsable a) =>
  Text ->
  S.ActionT m a
param t = S.queryParam t `rescue` S.formParam t

askl :: (MonadTrans t, MonadReader r m) => (r -> a) -> t m a
askl f = lift (asks f)
