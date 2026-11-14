{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE RecordWildCards #-}

module Main (main) where

import Control.Monad (when)
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
import Haskoin.Node
  ( LogLevel (..),
    logInfo,
    withConnection,
    withLogger,
    withSource,
  )
import Haskoin.Store
  ( Store (..),
    StoreConfig (..),
    WebConfig (..),
    WebLimits (..),
    runWeb,
    withStore,
  )
import Haskoin.Store.Common (loopForever)
import Haskoin.Store.Runtime (runtimeMetrics)
import NQE (receive, withSubscription)
import Options.Applicative
  ( Mod,
    Parser,
    auto,
    execParser,
    flag,
    fullDesc,
    header,
    help,
    helper,
    hidden,
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
import UnliftIO.Directory
  ( createDirectoryIfMissing,
    getAppUserDataDirectory,
  )
import UnliftIO.Async (withAsync)
import UnliftIO.Environment (lookupEnv)

-- | Version of the package, set at build time.
haskoinStoreVersion :: String

#ifdef CURRENT_PACKAGE_VERSION
haskoinStoreVersion = CURRENT_PACKAGE_VERSION
#else
haskoinStoreVersion = "Unavailable"
#endif

-- | Command line configuration. Each field has a command line option and an
-- environment variable that sets its default; run @haskoin-store --help@ for
-- the list.
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
    fleet :: !Bool,
    redisURL :: !String,
    redisMinAddrs :: !Int,
    redisMinTxs :: !Int,
    redisMaxXPubs :: !Int,
    noMempool :: !Bool,
    wipeMempool :: !Bool,
    syncMempool :: !Bool,
    blockWindow :: !Int,
    peerTimeout :: !Int,
    blockStall :: !Int,
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
    accessLog :: !Bool,
    nodeRpc :: !String,
    tlsCert :: !String,
    tlsKey :: !String,
    bloom :: !Bool,
    noWeb :: !Bool,
    maxOpenFiles :: !Int,
    writeBufferMB :: !Int,
    maxWalMB :: !Int,
    blockCacheMB :: !Int
  }

-- | Read an environment variable with a parser, or return the default if it is unset or invalid.
env :: String -> a -> (String -> Maybe a) -> IO a
env e d p = do
  ms <- lookupEnv e
  return $ fromMaybe d $ p =<< ms

-- | Default configuration, taken from environment variables where set.
defConfig :: IO Config
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
  fleet <-
    env "FLEET" False parseBool
  redisURL <-
    env "REDIS_URL" "" pure
  redisMinAddrs <-
    env "REDIS_MIN_ADDRS" 100 readMaybe
  redisMinTxs <-
    env "REDIS_MIN_TXS" 1000 readMaybe
  redisMaxXPubs <-
    env "REDIS_MAX_XPUBS" 10000 readMaybe
  noMempool <-
    env "NO_MEMPOOL" False parseBool
  wipeMempool <-
    env "WIPE_MEMPOOL" False parseBool
  syncMempool <-
    env "SYNC_MEMPOOL" False parseBool
  blockWindow <-
    env "BLOCK_WINDOW" 64 readMaybe
  mempoolTimeout <-
    env "MEMPOOL_TIMEOUT" 14 readMaybe
  peerTimeout <-
    env "PEER_TIMEOUT" 30 readMaybe
  blockStall <-
    env "BLOCK_STALL" 10 readMaybe
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
    env "HEALTH_CHECK_INTERVAL" 1 readMaybe
  accessLog <-
    env "ACCESS_LOG" False parseBool
  nodeRpc <-
    env "NODE_RPC" "" pure
  bloom <-
    env "BLOOM" True parseBool
  noWeb <-
    env "NO_WEB" False parseBool
  tlsCert <-
    env "TLS_CERT" "" pure
  tlsKey <-
    env "TLS_KEY" "" pure
  maxOpenFiles <-
    env "MAX_OPEN_FILES" 2000 readMaybe
  writeBufferMB <-
    env "WRITE_BUFFER_MB" 256 readMaybe
  maxWalMB <-
    env "MAX_WAL_MB" 1024 readMaybe
  blockCacheMB <-
    env "BLOCK_CACHE_MB" 1024 readMaybe
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
      slowRequest <-
        env "SLOW_REQUEST" d.slowRequest readMaybe
      return WebLimits {..}

-- | Names of the supported networks, separated by pipes.
netNames :: String
netNames = intercalate "|" $ map (.name) allNets

-- | Parse a boolean from @yes@, @true@, @on@, @1@ or their opposites, ignoring case.
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

-- | The help of an option, with the environment variable that sets its
-- default: every option has one, and someone who runs the store in a
-- container reads the help to find its name.
helpEnv :: String -> String -> Mod f a
helpEnv name text = help (text <> " [env: " <> name <> "]")

-- | Command line parser, with defaults from the given configuration.
config :: Config -> Parser Config
config c = do
  dir <-
    strOption $
      metavar "DIRECTORY"
        <> long "dir"
        <> short 'd'
        <> helpEnv "DIR" "Data directory"
        <> showDefault
        <> value c.dir
  host <-
    strOption $
      metavar "ADDRESS"
        <> long "host"
        <> helpEnv "HOST" "Network address to bind"
        <> showDefault
        <> value c.host
  port <-
    option auto $
      metavar "PORT"
        <> long "port"
        <> helpEnv "PORT" "REST API listening port"
        <> showDefault
        <> value c.port
  net <-
    strOption $
      metavar netNames
        <> long "net"
        <> short 'n'
        <> helpEnv "NET" "Network to connect to"
        <> showDefault
        <> value c.net
  discover <-
    flag c.discover True $
      long "discover"
        <> helpEnv "DISCOVER" "Peer discovery"
  peers <-
    fmap (mappend c.peers) $
      many $
        strOption $
          metavar "HOSTNAME[:PORT]"
            <> long "peer"
            <> short 'p'
            <> helpEnv "PEER" "Network peer (as many as required)"
  version <-
    switch $
      long "version"
        <> short 'v'
        <> help "Show version"
  debug <-
    flag c.debug True $
      long "debug"
        <> helpEnv "DEBUG" "Show debug messages"
  maxPendingTxs <-
    option auto $
      metavar "COUNT"
        <> long "max-pending-txs"
        <> helpEnv "MAX_PENDING_TXS" "Maximum pending txs to fail health check"
        <> showDefault
        <> value c.maxPendingTxs
  maxLaggingBlocks <-
    option auto $
      metavar "COUNT"
        <> long "max-lagging-blocks"
        <> helpEnv "MAX_LAGGING_BLOCKS" "Maximum number of unindexed blocks"
        <> showDefault
        <> value c.maxLaggingBlocks
  minPeers <-
    option auto $
      metavar "COUNT"
        <> long "min-peers"
        <> helpEnv "MIN_PEERS" "Minimum number of connected peers for health check"
        <> showDefault
        <> value c.minPeers
  noWeb <-
    flag c.noWeb True $
      long "noweb"
        <> helpEnv "NO_WEB" "Disable web server"
  webLimits <- do
    maxItemCount <-
      option auto $
        metavar "COUNT"
          <> long "max-item-count"
          <> helpEnv "MAX_ITEM_COUNT" "Hard limit for simple listings (0 = inf)"
          <> showDefault
          <> value c.webLimits.maxItemCount
    maxFullItemCount <-
      option auto $
        metavar "COUNT"
          <> long "max-full-item-count"
          <> helpEnv "MAX_FULL_ITEM_COUNT" "Hard limit for full listings (0 = inf)"
          <> showDefault
          <> value c.webLimits.maxFullItemCount
    maxOffset <-
      option auto $
        metavar "OFFSET"
          <> long "max-offset"
          <> helpEnv "MAX_OFFSET" "Hard limit for offsets (0 = inf)"
          <> showDefault
          <> value c.webLimits.maxOffset
    defItemCount <-
      option auto $
        metavar "COUNT"
          <> long "def-item-count"
          <> helpEnv "DEF_ITEM_COUNT" "Soft default limit (0 = inf)"
          <> showDefault
          <> value c.webLimits.defItemCount
    xpubGap <-
      option auto $
        metavar "GAP"
          <> long "xpub-gap"
          <> helpEnv "XPUB_GAP" "Max gap for xpub queries"
          <> showDefault
          <> value c.webLimits.xpubGap
    xpubGapInit <-
      option auto $
        metavar "GAP"
          <> long "xpub-gap-init"
          <> helpEnv "XPUB_GAP_INIT" "Max gap for empty xpubs"
          <> showDefault
          <> value c.webLimits.xpubGapInit
    blockTimeout <-
      option auto $
        metavar "SECONDS"
          <> long "block-timeout"
          <> helpEnv "BLOCK_TIMEOUT" "Block lag health timeout"
          <> showDefault
          <> value c.webLimits.blockTimeout
    txTimeout <-
      option auto $
        metavar "SECONDS"
          <> long "tx-timeout"
          <> helpEnv "TX_TIMEOUT" "Last tx received health timeout"
          <> showDefault
          <> value c.webLimits.txTimeout
    slowRequest <-
      option auto $
        metavar "SECONDS"
          <> long "slow-request"
          <> helpEnv "SLOW_REQUEST" "Warn in the log of requests that take this long (0 = never)"
          <> showDefault
          <> value c.webLimits.slowRequest
    return WebLimits {..}
  redis <-
    flag c.redis True $
      long "redis"
        <> helpEnv "REDIS" "Redis cache for xpub data"
  fleet <-
    flag c.fleet True $
      long "fleet"
        <> helpEnv
          "FLEET"
          ( unwords
              [ "Answer as one of the servers that share the Redis of --redis:",
                "all from the same block and the same mempool"
              ]
          )
  redisURL <-
    strOption $
      metavar "URL"
        <> long "redis-url"
        <> helpEnv "REDIS_URL" "URL for Redis cache"
        <> value c.redisURL
  redisMinAddrs <-
    option auto $
      metavar "COUNT"
        <> long "redis-min-addrs"
        <> helpEnv "REDIS_MIN_ADDRS" "Cache xpubs with at least this many used addresses"
        <> showDefault
        <> value c.redisMinAddrs
  redisMinTxs <-
    option auto $
      metavar "COUNT"
        <> long "redis-min-txs"
        <> helpEnv "REDIS_MIN_TXS" "Cache xpubs with at least this many transactions in their addresses"
        <> showDefault
        <> value c.redisMinTxs
  redisMaxXPubs <-
    option auto $
      metavar "COUNT"
        <> long "redis-max-xpubs"
        <> helpEnv "REDIS_MAX_XPUBS" "Evict the least recently used xpubs from Redis past this many"
        <> showDefault
        <> value c.redisMaxXPubs
  noMempool <-
    flag c.noMempool True $
      long "no-mempool"
        <> helpEnv "NO_MEMPOOL" "Do not index mempool transactions"
  wipeMempool <-
    flag c.wipeMempool True $
      long "wipe-mempool"
        <> helpEnv "WIPE_MEMPOOL" "Wipe indexed mempool at start"
  syncMempool <-
    flag c.syncMempool True $
      long "sync-mempool"
        <> helpEnv "SYNC_MEMPOOL" "Attempt to download peer mempools"
  blockWindow <-
    option auto $
      metavar "COUNT"
        <> long "block-window"
        <> helpEnv "BLOCK_WINDOW" "Blocks to request from the syncing peer at a time"
        <> showDefault
        <> value c.blockWindow
  mempoolTimeout <-
    option auto $
      metavar "DAYS"
        <> long "mempool-timeout"
        <> helpEnv "MEMPOOL_TIMEOUT" "Cull old mempool transactions"
        <> showDefault
        <> value c.mempoolTimeout
  peerTimeout <-
    option auto $
      metavar "SECONDS"
        <> long "peer-timeout"
        <> helpEnv "PEER_TIMEOUT" "Disconnect a peer that is silent, or owes blocks, for this long"
        <> showDefault
        <> value c.peerTimeout
  blockStall <-
    option auto $
      metavar "SECONDS"
        <> long "block-stall"
        <> helpEnv "BLOCK_STALL" "Ask another peer for blocks that the syncing peer has not delivered for this long"
        <> showDefault
        <> value c.blockStall
  maxPeerLife <-
    option auto $
      metavar "SECONDS"
        <> long "max-peer-life"
        <> helpEnv "MAX_PEER_LIFE" "Maximum peer connection time"
        <> showDefault
        <> value c.maxPeerLife
  maxPeers <-
    option auto $
      metavar "COUNT"
        <> long "max-peers"
        <> helpEnv "MAX_PEERS" "Do not connect to more than this many peers"
        <> showDefault
        <> value c.maxPeers
  statsd <-
    flag c.statsd True $
      long "statsd"
        <> helpEnv "STATSD" "Enable statsd metrics"
  statsdHost <-
    strOption $
      metavar "HOSTNAME"
        <> long "statsd-host"
        <> helpEnv "STATSD_HOST" "Host to send statsd metrics"
        <> showDefault
        <> value c.statsdHost
  statsdPort <-
    option auto $
      metavar "PORT"
        <> long "statsd-port"
        <> helpEnv "STATSD_PORT" "Port to send statsd metrics"
        <> showDefault
        <> value c.statsdPort
  statsdPrefix <-
    strOption $
      metavar "PREFIX"
        <> long "statsd-prefix"
        <> helpEnv "STATSD_PREFIX" "Prefix for statsd metrics"
        <> showDefault
        <> value c.statsdPrefix
  statsdNoEvents <-
    flag c.statsdNoEvents True $
      long "statsd-no-events"
        <> helpEnv "STATSD_NO_EVENTS" "Do not report individual events to statsd"
  statsdAggregates <-
    flag c.statsdAggregates True $
      long "statsd-aggregates"
        <> helpEnv "STATSD_AGGREGATES" "Send statistical aggregate reports to statsd every second"
  tickerRefresh <-
    option auto $
      metavar "MICROSECONDS"
        <> long "ticker-refresh"
        <> helpEnv "TICKER_REFRESH" "How often to retrieve price information"
        <> showDefault
        <> value c.tickerRefresh
  tickerURL <-
    strOption $
      metavar "URL"
        <> long "ticker-url"
        <> helpEnv "TICKER_URL" "Blockchain.info price ticker URL"
        <> showDefault
        <> value c.tickerURL
  priceHistoryURL <-
    strOption $
      metavar "URL"
        <> long "price-history-url"
        <> helpEnv "PRICE_HISTORY_URL" "Blockchain.info price history URL"
        <> showDefault
        <> value c.priceHistoryURL
  noBlockchainInfo <-
    flag c.noBlockchainInfo True $
      long "no-blockchain-info"
        <> helpEnv "NO_BLOCKCHAIN_INFO" "Disable Blockchain.info-style API endpoints"
  noXPub <-
    flag c.noXPub True $
      long "no-xpub"
        <> helpEnv "NO_XPUB" "Disable extended public key support"
  healthCheckInterval <-
    option auto $
      metavar "SECONDS"
        <> long "health-check-interval"
        <> helpEnv "HEALTH_CHECK_INTERVAL" "Seconds between health checks; there is one at each new block too"
        <> showDefault
        <> value c.healthCheckInterval
  accessLog <-
    flag c.accessLog True $
      long "access-log"
        <> helpEnv "ACCESS_LOG" "Log every request, with the address of the client and the time it took"
  nodeRpc <-
    strOption $
      metavar "URL"
        <> long "node-rpc"
        <> helpEnv
          "NODE_RPC"
          "Publish transactions over the JSON-RPC of this full node, and through the peers when it fails"
        <> value c.nodeRpc
  tlsCert <-
    strOption $
      metavar "FILE"
        <> long "tls-cert"
        <> helpEnv "TLS_CERT" "Serve HTTPS with this certificate chain, in PEM; needs --tls-key"
        <> value c.tlsCert
  tlsKey <-
    strOption $
      metavar "FILE"
        <> long "tls-key"
        <> helpEnv "TLS_KEY" "Private key of the certificate of --tls-cert, in PEM"
        <> value c.tlsKey
  noBloom <-
    switch $
      long "no-bloom"
        <> helpEnv "BLOOM=false" "No RocksDB Bloom filters on the data that is read by key, which are on by default"
  -- it was the way to turn them on, and commands that have it must still
  -- start
  _ <- switch (long "bloom" <> hidden)
  maxOpenFiles <-
    option auto $
      metavar "COUNT"
        <> long "max-open-files"
        <> helpEnv "MAX_OPEN_FILES" "RocksDB max_open_files; bounds index/filter memory (0 = unlimited)"
        <> showDefault
        <> value c.maxOpenFiles
  writeBufferMB <-
    option auto $
      metavar "MB"
        <> long "write-buffer-mb"
        <> helpEnv "WRITE_BUFFER_MB" "RocksDB memtable budget across all column families in MiB (0 = unlimited)"
        <> showDefault
        <> value c.writeBufferMB
  maxWalMB <-
    option auto $
      metavar "MB"
        <> long "max-wal-mb"
        <> helpEnv
          "MAX_WAL_MB"
          ( unwords
              [ "RocksDB write-ahead log size in MiB that forces flushes so old",
                "log files can be deleted (0 = RocksDB default)"
              ]
          )
        <> showDefault
        <> value c.maxWalMB
  blockCacheMB <-
    option auto $
      metavar "MB"
        <> long "block-cache-mb"
        <> helpEnv
          "BLOCK_CACHE_MB"
          ( unwords
              [ "RocksDB block cache in MiB, shared by all column families,",
                "holding index and filter blocks too (0 = RocksDB default of",
                "32 MiB per column family)"
              ]
          )
        <> showDefault
        <> value c.blockCacheMB
  pure Config {bloom = c.bloom && not noBloom, ..}

-- | Network by name.
networkReader :: String -> Either String Network
networkReader s =
  case netByName s of
    Just net -> Right net
    Nothing -> Left "Network name invalid"

-- | Parse the command line and run the store. If no peers are given and discovery
-- is off, discovery is turned on.
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

-- | Run the store with the web server, or without it if disabled, until killed.
-- Data goes in a directory named after the network inside the data directory.
run :: Config -> IO ()
run cfg =
  withContext $ \ctx ->
    withLogger level $ \logger -> do
      net <- either error return $ networkReader cfg.net
      with_stats logger net $ \stats -> do
        let dir = cfg.dir </> net.name
        logInfo (withSource "haskoin-store" logger) $
          "Creating working directory (if not found): " <> cs dir
        createDirectoryIfMissing True dir
        run_store logger net ctx stats dir $ \store ->
          if cfg.noWeb
            then withSubscription store.pub $ \sub ->
              loopForever (receive sub)
            else run_web logger stats store
  where
    run_store logger net ctx stats dir =
      withStore
        StoreConfig
          { maxPeers = cfg.maxPeers,
            initPeers = cfg.peers,
            discover = cfg.discover,
            db = dir </> "db",
            net = net,
            ctx = ctx,
            redis = if cfg.redis then Just cfg.redisURL else Nothing,
            fleet = cfg.fleet,
            gap = cfg.webLimits.xpubGap,
            initGap = cfg.webLimits.xpubGapInit,
            redisMinAddrs = cfg.redisMinAddrs,
            redisMinTxs = cfg.redisMinTxs,
            redisMaxXPubs = cfg.redisMaxXPubs,
            wipeMempool = cfg.wipeMempool,
            noMempool = cfg.noMempool,
            syncMempool = cfg.syncMempool,
            blockWindow = cfg.blockWindow,
            mempoolTimeout = cfg.mempoolTimeout,
            peerTimeout = fromIntegral cfg.peerTimeout,
            blockStall = fromIntegral cfg.blockStall,
            maxPeerLife = fromIntegral cfg.maxPeerLife,
            connect = withConnection,
            stats = stats,
            bloom = cfg.bloom,
            maxOpenFiles = if cfg.maxOpenFiles > 0 then Just cfg.maxOpenFiles else Nothing,
            writeBufferSize =
              if cfg.writeBufferMB > 0
                then Just (cfg.writeBufferMB * 1024 * 1024)
                else Nothing,
            maxWalSize =
              if cfg.maxWalMB > 0
                then Just (cfg.maxWalMB * 1024 * 1024)
                else Nothing,
            blockCache =
              if cfg.blockCacheMB > 0
                then Just (cfg.blockCacheMB * 1024 * 1024)
                else Nothing,
            logger = logger
          }
    run_web logger stats store =
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
            healthCheckInterval = cfg.healthCheckInterval,
            accessLog = cfg.accessLog,
            nodeRpc = if null cfg.nodeRpc then Nothing else Just cfg.nodeRpc,
            tls = case (cfg.tlsCert, cfg.tlsKey) of
              ("", "") -> Nothing
              ("", _) -> error "--tls-key needs --tls-cert"
              (_, "") -> error "--tls-cert needs --tls-key"
              (certificate, key) -> Just (certificate, key),
            logger = withSource "Web" logger
          }
    with_stats logger net go
      | cfg.statsd = do
          logInfo (withSource "Main" logger) $
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
            -- the metrics of the garbage collector, beside those of the store
            (\stats -> withAsync (runtimeMetrics (withSource "Runtime" logger) stats) (\_ -> go (Just stats)))
      | otherwise = go Nothing
    level
      | cfg.debug = LevelDebug
      | otherwise = LevelInfo
