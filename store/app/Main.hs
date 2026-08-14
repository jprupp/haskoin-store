{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE NoFieldSelectors #-}

module Main (main) where

import Control.Monad (forever, void, when)
import Control.Monad.Cont (ContT (ContT), runContT)
import Control.Monad.Logger
  ( LogLevel (..),
    filterLogger,
    logInfoS,
    runStderrLoggingT,
  )
import Control.Monad.Trans (lift)
import Data.Char (toLower)
import Data.Default (Default (..))
import Data.List (intercalate)
import Data.Maybe (fromMaybe)
import Data.String.Conversions (cs)
import Data.Text qualified as T
import Haskoin
  ( Network (..),
    allNets,
    netByName,
    withContext,
  )
import Haskoin.Node (withConnection)
import Haskoin.Store
  ( Store (..),
    StoreConfig (..),
    WebConfig (..),
    WebLimits (..),
    runWeb,
    withStore,
  )
import NQE (receive, withSubscription)
import Options.Applicative
  ( Parser,
    auto,
    execParser,
    flag,
    fullDesc,
    header,
    help,
    helper,
    info,
    long,
    many,
    metavar,
    option,
    progDesc,
    short,
    showDefault,
    strOption,
    switch,
    value,
  )
import System.Exit (exitSuccess)
import System.FilePath ((</>))
import System.Metrics.StatsD
  ( StatConfig
      ( namespace,
        reportSamples,
        reportStats,
        statsdPort,
        statsdServer
      ),
    defStatConfig,
    withStats,
  )
import Text.Read (readMaybe)
import UnliftIO (MonadIO)
import UnliftIO.Directory
  ( createDirectoryIfMissing,
    getAppUserDataDirectory,
  )
import UnliftIO.Environment (lookupEnv)

haskoinStoreVersion :: String

#ifdef CURRENT_PACKAGE_VERSION
haskoinStoreVersion = CURRENT_PACKAGE_VERSION
#else
haskoinStoreVersion = "Unavailable"
#endif

data Config = Config
  { dir :: !FilePath,
    host :: !String,
    port :: !Int,
    net :: !String,
    discover :: !Bool,
    peers :: ![String],
    version :: !Bool,
    debug :: !Bool,
    maxPendingTxs :: !Int,
    maxLaggingBlocks :: !Int,
    minPeers :: !Int,
    webLimits :: !WebLimits,
    redis :: !Bool,
    redisURL :: !String,
    redisMinAddrs :: !Int,
    redisMaxKeys :: !Integer,
    redisSyncInterval :: !Int,
    noMempool :: !Bool,
    wipeMempool :: !Bool,
    syncMempool :: !Bool,
    peerTimeout :: !Int,
    maxPeerLife :: !Int,
    mempoolTimeout :: !Int,
    maxPeers :: !Int,
    statsd :: !Bool,
    statsdHost :: !String,
    statsdPort :: !Int,
    statsdPrefix :: !String,
    statsdNoEvents :: Bool,
    statsdAggregates :: Bool,
    tickerRefresh :: !Int,
    tickerURL :: !String,
    priceHistoryURL :: !String,
    noBlockchainInfo :: !Bool,
    noXPub :: !Bool,
    healthCheckInterval :: !Int,
    bloom :: !Bool,
    noWeb :: !Bool
  }

env :: (MonadIO m) => String -> a -> (String -> Maybe a) -> m a
env e d p = do
  ms <- lookupEnv e
  return $ fromMaybe d $ p =<< ms

defConfig :: (MonadIO m) => m Config
defConfig = do
  dir <-
    getDir
  host <-
    env "HOST" "*" pure
  port <-
    env "PORT" 3000 readMaybe
  net <-
    env "NET" "bch" pure
  discover <-
    env "DISCOVER" False parseBool
  peers <-
    env "PEER" [] (pure . words)
  debug <-
    env "DEBUG" False parseBool
  maxPendingTxs <-
    env "MAX_PENDING_TXS" 10000 readMaybe
  maxLaggingBlocks <-
    env "MAX_LAGGING_BLOCKS" 3 readMaybe
  minPeers <-
    env "MIN_PEERS" 1 readMaybe
  webLimits <-
    getWebLimits
  redis <-
    env "REDIS" False parseBool
  redisURL <-
    env "REDIS_URL" "" pure
  redisMinAddrs <-
    env "REDIS_MIN_ADDRS" 100 readMaybe
  redisMaxKeys <-
    env "REDIS_MAX_KEYS" 100000000 readMaybe
  redisSyncInterval <-
    env "REDIS_SYNC_INTERVAL" 30 readMaybe
  noMempool <-
    env "NO_MEMPOOL" False parseBool
  wipeMempool <-
    env "WIPE_MEMPOOL" False parseBool
  syncMempool <-
    env "SYNC_MEMPOOL" False parseBool
  mempoolTimeout <-
    env "MEMPOOL_TIMEOUT" 14 readMaybe
  peerTimeout <-
    env "PEER_TIMEOUT" 120 readMaybe
  maxPeerLife <-
    env "MAX_PEER_LIFE" (48 * 3600) readMaybe
  maxPeers <-
    env "MAX_PEERS" 20 readMaybe
  statsd <-
    env "STATSD" False parseBool
  statsdHost <-
    env "STATSD_HOST" "localhost" pure
  statsdPort <-
    env "STATSD_PORT" 8125 readMaybe
  statsdNoEvents <-
    env "STATSD_NO_EVENTS" False parseBool
  statsdAggregates <-
    env "STATSD_AGGREGATES" False parseBool
  statsdPrefix <-
    env "STATSD_PREFIX" "haskoin_store" pure
  tickerRefresh <-
    env "TICKER_REFRESH" (90 * 1000 * 1000) readMaybe
  tickerURL <-
    env "TICKER_URL" tickerString pure
  priceHistoryURL <-
    env "PRICE_HISTORY_URL" priceHistoryString pure
  noBlockchainInfo <-
    env "NO_BLOCKCHAIN_INFO" False parseBool
  noXPub <-
    env "NO_XPUB" False parseBool
  healthCheckInterval <-
    env "HEALTH_CHECK_INTERVAL" 30 readMaybe
  bloom <-
    env "BLOOM" False parseBool
  noWeb <-
    env "NO_WEB" False parseBool
  return Config {version = False, ..}
  where
    tickerString =
      "https://api.blockchain.info/ticker"
    priceHistoryString =
      "https://api.blockchain.info/price/index-series"
    getDir =
      getAppUserDataDirectory "haskoin-store" >>= \d ->
        env "DIR" d pure
    getWebLimits = do
      let d = def :: WebLimits
      maxItemCount <-
        env "MAX_ITEM_COUNT" d.maxItemCount readMaybe
      maxFullItemCount <-
        env "MAX_FULL_ITEM_COUNT" d.maxFullItemCount readMaybe
      maxOffset <-
        env "MAX_OFFSET" d.maxOffset readMaybe
      defItemCount <-
        env "DEF_ITEM_COUNT" d.defItemCount readMaybe
      xpubGap <-
        env "XPUB_GAP" d.xpubGap readMaybe
      xpubGapInit <-
        env "XPUB_GAP_INIT" d.xpubGapInit readMaybe
      blockTimeout <-
        env "BLOCK_TIMEOUT" d.blockTimeout readMaybe
      txTimeout <-
        env "TX_TIMEOUT" d.txTimeout readMaybe
      return WebLimits {..}

netNames :: String
netNames = intercalate "|" $ map (.name) allNets

parseBool :: String -> Maybe Bool
parseBool str = case map toLower str of
  "yes" -> Just True
  "true" -> Just True
  "on" -> Just True
  "1" -> Just True
  "no" -> Just False
  "false" -> Just False
  "off" -> Just False
  "0" -> Just False
  _ -> Nothing

config :: Config -> Parser Config
config c = do
  dir <-
    strOption $
      metavar "DIRECTORY"
        <> long "dir"
        <> short 'd'
        <> help "Data directory"
        <> showDefault
        <> value c.dir
  host <-
    strOption $
      metavar "ADDRESS"
        <> long "host"
        <> help "Network address to bind"
        <> showDefault
        <> value c.host
  port <-
    option auto $
      metavar "PORT"
        <> long "port"
        <> help "REST API listening port"
        <> showDefault
        <> value c.port
  net <-
    strOption $
      metavar netNames
        <> long "net"
        <> short 'n'
        <> help "Network to connect to"
        <> showDefault
        <> value c.net
  discover <-
    flag c.discover True $
      long "discover"
        <> help "Peer discovery"
  peers <-
    fmap (mappend c.peers) $
      many $
        option auto $
          metavar "HOSTNAME[:PORT]"
            <> long "peer"
            <> short 'p'
            <> help "Network peer (as many as required)"
  version <-
    switch $
      long "version"
        <> short 'v'
        <> help "Show version"
  debug <-
    flag c.debug True $
      long "debug"
        <> help "Show debug messages"
  maxPendingTxs <-
    option auto $
      metavar "COUNT"
        <> long "max-pending-txs"
        <> help "Maximum pending txs to fail health check"
        <> showDefault
        <> value c.maxPendingTxs
  maxLaggingBlocks <-
    option auto $
      metavar "COUNT"
        <> long "max-lagging-blocks"
        <> help "Maximum number of unindexed blocks"
        <> showDefault
        <> value c.maxLaggingBlocks
  minPeers <-
    option auto $
      metavar "COUNT"
        <> long "min-peers"
        <> help "Minimum number of connected peers for health check"
        <> showDefault
        <> value c.minPeers
  noWeb <-
    flag c.noWeb True $
      long "noweb"
        <> help "Disable web server"
  webLimits <- do
    maxItemCount <-
      option auto $
        metavar "COUNT"
          <> long "max-item-count"
          <> help "Hard limit for simple listings (0 = inf)"
          <> showDefault
          <> value c.webLimits.maxItemCount
    maxFullItemCount <-
      option auto $
        metavar "COUNT"
          <> long "max-full-item-count"
          <> help "Hard limit for full listings (0 = inf)"
          <> showDefault
          <> value c.webLimits.maxFullItemCount
    maxOffset <-
      option auto $
        metavar "OFFSET"
          <> long "max-offset"
          <> help "Hard limit for offsets (0 = inf)"
          <> showDefault
          <> value c.webLimits.maxOffset
    defItemCount <-
      option auto $
        metavar "COUNT"
          <> long "def-item-count"
          <> help "Soft default limit (0 = inf)"
          <> showDefault
          <> value c.webLimits.defItemCount
    xpubGap <-
      option auto $
        metavar "GAP"
          <> long "xpub-gap"
          <> help "Max gap for xpub queries"
          <> showDefault
          <> value c.webLimits.xpubGap
    xpubGapInit <-
      option auto $
        metavar "GAP"
          <> long "xpub-gap-init"
          <> help "Max gap for empty xpubs"
          <> showDefault
          <> value c.webLimits.xpubGapInit
    blockTimeout <-
      option auto $
        metavar "SECONDS"
          <> long "block-timeout"
          <> help "Block lag health timeout"
          <> showDefault
          <> value c.webLimits.blockTimeout
    txTimeout <-
      option auto $
        metavar "SECONDS"
          <> long "tx-timeout"
          <> help "Last tx received health timeout"
          <> showDefault
          <> value c.webLimits.txTimeout
    return WebLimits {..}
  redis <-
    flag c.redis True $
      long "redis"
        <> help "Redis cache for xpub data"
  redisURL <-
    strOption $
      metavar "URL"
        <> long "redis-url"
        <> help "URL for Redis cache"
        <> value c.redisURL
  redisMinAddrs <-
    option auto $
      metavar "GAP"
        <> long "redis-min-addrs"
        <> help "Minimum xpub address count to cache in Redis"
        <> showDefault
        <> value c.redisMinAddrs
  redisMaxKeys <-
    option auto $
      metavar "COUNT"
        <> long "redis-max-keys"
        <> help "Maximum number of keys in Redis"
        <> showDefault
        <> value c.redisMaxKeys
  redisSyncInterval <-
    option auto $
      metavar "SECONDS"
        <> long "redis-sync-interval"
        <> help "Sync mempool to Redis interval"
        <> showDefault
        <> value c.redisSyncInterval
  noMempool <-
    flag c.noMempool True $
      long "no-mempool"
        <> help "Do not index mempool transactions"
  wipeMempool <-
    flag c.wipeMempool True $
      long "wipe-mempool"
        <> help "Wipe indexed mempool at start"
  syncMempool <-
    flag c.syncMempool True $
      long "sync-mempool"
        <> help "Attempt to download peer mempools"
  mempoolTimeout <-
    option auto $
      metavar "DAYS"
        <> long "mempool-timeout"
        <> help "Cull old mempool transactions"
        <> showDefault
        <> value c.mempoolTimeout
  peerTimeout <-
    option auto $
      metavar "SECONDS"
        <> long "peer-timeout"
        <> help "Unresponsive peer timeout"
        <> showDefault
        <> value c.peerTimeout
  maxPeerLife <-
    option auto $
      metavar "SECONDS"
        <> long "max-peer-life"
        <> help "Maximum peer connection time"
        <> showDefault
        <> value c.maxPeerLife
  maxPeers <-
    option auto $
      metavar "COUNT"
        <> long "max-peers"
        <> help "Do not connect to more than this many peers"
        <> showDefault
        <> value c.maxPeers
  statsd <-
    flag c.statsd True $
      long "statsd"
        <> help "Enable statsd metrics"
  statsdHost <-
    strOption $
      metavar "HOSTNAME"
        <> long "statsd-host"
        <> help "Host to send statsd metrics"
        <> showDefault
        <> value c.statsdHost
  statsdPort <-
    option auto $
      metavar "PORT"
        <> long "statsd-port"
        <> help "Port to send statsd metrics"
        <> showDefault
        <> value c.statsdPort
  statsdPrefix <-
    strOption $
      metavar "PREFIX"
        <> long "statsd-prefix"
        <> help "Prefix for statsd metrics"
        <> showDefault
        <> value c.statsdPrefix
  statsdNoEvents <-
    flag c.statsdNoEvents True $
      long "statsd-no-events"
        <> help "Do not report individual events to statsd"
  statsdAggregates <-
    flag c.statsdAggregates True $
      long "statsd-aggregates"
        <> help "Send statistical aggregate reports to statsd every second"
  tickerRefresh <-
    option auto $
      metavar "MICROSECONDS"
        <> long "ticker-refresh"
        <> help "How often to retrieve price information"
        <> showDefault
        <> value c.tickerRefresh
  tickerURL <-
    strOption $
      metavar "URL"
        <> long "ticker-url"
        <> help "Blockchain.info price ticker URL"
        <> showDefault
        <> value c.tickerURL
  priceHistoryURL <-
    strOption $
      metavar "URL"
        <> long "price-history-url"
        <> help "Blockchain.info price history URL"
        <> showDefault
        <> value c.priceHistoryURL
  noBlockchainInfo <-
    flag c.noBlockchainInfo True $
      long "no-blockchain-info"
        <> help "Disable Blockchain.info-style API endpoints"
  noXPub <-
    flag c.noXPub True $
      long "no-xpub"
        <> help "Disable extended public key support"
  healthCheckInterval <-
    option auto $
      metavar "SECONDS"
        <> long "health-check-interval"
        <> help "Background check update interval"
        <> showDefault
        <> value c.healthCheckInterval
  bloom <-
    flag c.bloom True $
      long "bloom"
        <> help "RocksDB Bloom filters"
  pure Config {..}

networkReader :: String -> Either String Network
networkReader s =
  case netByName s of
    Just net -> Right net
    Nothing -> Left "Network name invalid"

main :: IO ()
main = do
  c <- execParser . opts =<< defConfig
  when c.version $ do
    putStrLn haskoinStoreVersion
    exitSuccess
  if null c.peers && not c.discover
    then run $ let Config {..} = c in Config {discover = True, ..}
    else run c
  where
    opts c =
      info (helper <*> config c) $
        fullDesc
          <> progDesc "Bitcoin (BCH & BTC) block chain index with HTTP API"
          <> Options.Applicative.header
            ("haskoin-store version " <> haskoinStoreVersion)

run :: Config -> IO ()
run cfg =
  withContext $ \ctx ->
    runStderrLoggingT $ filterLogger l $ flip runContT return $ do
      net <- either error return $ networkReader cfg.net
      stats <- ContT $ with_stats net
      let dir = cfg.dir </> net.name
      $(logInfoS) "haskoin-store" $
        "Creating working directory (if not found): " <> cs dir
      createDirectoryIfMissing True dir
      store <- ContT $ run_store net ctx stats dir
      if cfg.noWeb
        then lift . withSubscription store.pub $ \sub ->
          forever $ void (receive sub)
        else do
          lift $ run_web stats store
  where
    run_store net ctx stats dir =
      withStore
        StoreConfig
          { maxPeers = cfg.maxPeers,
            initPeers = cfg.peers,
            discover = cfg.discover,
            db = dir </> "db",
            net = net,
            ctx = ctx,
            redis = if cfg.redis then Just cfg.redisURL else Nothing,
            gap = cfg.webLimits.xpubGap,
            initGap = cfg.webLimits.xpubGapInit,
            redisMinAddrs = cfg.redisMinAddrs,
            redisMaxKeys = cfg.redisMaxKeys,
            wipeMempool = cfg.wipeMempool,
            noMempool = cfg.noMempool,
            syncMempool = cfg.syncMempool,
            mempoolTimeout = cfg.mempoolTimeout,
            peerTimeout = fromIntegral cfg.peerTimeout,
            maxPeerLife = fromIntegral cfg.maxPeerLife,
            connect = withConnection,
            stats = stats,
            redisSyncInterval = cfg.redisSyncInterval,
            bloom = cfg.bloom
          }
    run_web stats store =
      runWeb
        WebConfig
          { host = cfg.host,
            port = cfg.port,
            store = store,
            limits = cfg.webLimits,
            maxPendingTxs = cfg.maxPendingTxs,
            maxLaggingBlocks = cfg.maxLaggingBlocks,
            minPeers = cfg.minPeers,
            version = haskoinStoreVersion,
            noMempool = cfg.noMempool,
            stats = stats,
            tickerRefresh = cfg.tickerRefresh,
            tickerURL = cfg.tickerURL,
            priceHistoryURL = cfg.priceHistoryURL,
            noXPub = cfg.noXPub,
            noBlockchainInfo = cfg.noBlockchainInfo,
            healthCheckInterval = cfg.healthCheckInterval
          }
    with_stats net go
      | cfg.statsd = do
          $(logInfoS) "Main" $
            "Sending stats to "
              <> T.pack cfg.statsdHost
              <> ":"
              <> cs (show cfg.statsdPort)
              <> " with prefix: "
              <> T.pack cfg.statsdPrefix
          withStats
            defStatConfig
              { statsdServer = cfg.statsdHost,
                statsdPort = cfg.statsdPort,
                namespace = cfg.statsdPrefix <> "." <> net.name,
                reportSamples = not cfg.statsdNoEvents,
                reportStats = cfg.statsdAggregates
              }
            (go . Just)
      | otherwise = go Nothing
    l _ lvl
      | cfg.debug = True
      | otherwise = LevelInfo <= lvl
