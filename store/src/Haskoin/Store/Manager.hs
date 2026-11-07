{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoFieldSelectors #-}

-- |
-- Module      : Haskoin.Store.Manager
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Wires the components of a store together: the RocksDB database, the
-- peer-to-peer node, the block store that imports data, and the optional Redis
-- cache with its writer process. Node events are forwarded to the block store
-- and turned into 'StoreEvent's.
module Haskoin.Store.Manager
  ( StoreConfig (..),
    Store (..),
    withStore,
  )
where

import Control.Monad (unless)
import Control.Monad.Cont
  ( ContT (..),
    runContT,
  )
import Control.Monad.Reader (ReaderT (ReaderT), runReaderT)
import Control.Monad.Trans (lift)
import Data.Time.Clock (NominalDiffTime)
import Data.Word (Word32)
import Haskoin
  ( BlockHash (..),
    Ctx,
    Inv (..),
    InvType (..),
    InvVector (..),
    Message (..),
    MessageCommand (..),
    Network,
    NetworkAddress (..),
    NotFound (..),
    Pong (..),
    Reject (..),
    TxHash (..),
    VarString (..),
    sockToHostAddress,
  )
import Haskoin.Node
  ( Chain,
    ChainEvent (..),
    Logger,
    Node (..),
    NodeConfig (..),
    NodeEvent (..),
    PeerEvent (..),
    PeerMgr,
    WithConnection,
    withNode,
    withSource,
  )
import Haskoin.Store.BlockStore
  ( BlockStore,
    BlockStoreConfig (..),
    blockStoreBlockSTM,
    blockStoreHeadSTM,
    blockStoreNotFoundSTM,
    blockStorePeerConnectSTM,
    blockStorePeerDisconnectSTM,
    blockStoreTxHashSTM,
    blockStoreTxSTM,
    withBlockStore,
  )
import Haskoin.Store.Cache
  ( CacheConfig (..),
    CacheWriter,
    cacheNewBlock,
    cacheNewTx,
    cacheSyncMempool,
    cacheWriter,
    connectRedis,
    newCacheMetrics,
  )
import Haskoin.Store.Common
  ( StoreEvent (..),
    loopForever,
  )
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    DatabaseReaderT,
    createDataMetrics,
    withDatabaseReader,
  )
import NQE
  ( Inbox,
    Process (..),
    Publisher,
    publishSTM,
    receive,
    withProcess,
    withPublisher,
    withSubscription,
  )
import Network.Socket (SockAddr (..))
import System.Metrics.StatsD
import UnliftIO
  ( MonadIO,
    MonadUnliftIO,
    STM,
    atomically,
    liftIO,
    link,
    withAsync,
  )
import UnliftIO.Concurrent (threadDelay)

-- | Store mailboxes. Handles to the running components of a store, given by
-- 'withStore'.
data Store = Store
  { -- | peer manager of the node
    peerMgr :: !PeerMgr,
    -- | block header chain of the node
    chain :: !Chain,
    -- | process that imports blocks and transactions
    block :: !BlockStore,
    -- | database reader
    db :: !DatabaseReader,
    -- | cache configuration, if Redis is configured
    cache :: !(Maybe CacheConfig),
    -- | store events are published here
    pub :: !(Publisher StoreEvent),
    -- | network constants
    net :: !Network,
    -- | secp256k1 context
    ctx :: !Ctx
  }

-- | Configuration for a 'Store'.
data StoreConfig = StoreConfig
  { -- | max peers to connect to
    maxPeers :: !Int,
    -- | static set of peers to connect to
    initPeers :: ![String],
    -- | discover new peers
    discover :: !Bool,
    -- | RocksDB database path
    db :: !FilePath,
    -- | network constants
    net :: !Network,
    -- | Redis cache configuration
    redis :: !(Maybe String),
    -- | Secp256k1 context
    ctx :: !Ctx,
    -- | gap on extended public key with no transactions
    initGap :: !Word32,
    -- | gap for extended public keys
    gap :: !Word32,
    -- | cache xpubs with more than this many used addresses
    redisMinAddrs :: !Int,
    -- | maximum number of keys in Redis cache
    redisMaxKeys :: !Integer,
    -- | do not index new mempool transactions
    noMempool :: !Bool,
    -- | wipe mempool when starting
    wipeMempool :: !Bool,
    -- | sync mempool from peers
    syncMempool :: !Bool,
    -- | blocks to request from the syncing peer in one batch
    blockWindow :: !Int,
    -- | delete mempool transactions older than this number of days
    mempoolTimeout :: !Int,
    -- | disconnect peer if message not received for this many seconds
    peerTimeout :: !NominalDiffTime,
    -- | disconnect peer if it has been connected this long
    maxPeerLife :: !NominalDiffTime,
    -- | connect to peers using the function 'withConnection'
    connect :: !(SockAddr -> WithConnection),
    -- | StatsD
    stats :: !(Maybe Stats),
    -- | sync mempool against cache every this many seconds
    redisSyncInterval :: !Int,
    -- | database Bloom filters
    bloom :: !Bool,
    -- | RocksDB max_open_files ('Nothing' = unlimited: every SST file's index
    -- and filter blocks stay resident for the life of the process)
    maxOpenFiles :: !(Maybe Int),
    -- | bytes of RocksDB memtables across all column families ('Nothing' =
    -- unlimited: each column family keeps up to two 64 MiB memtables)
    writeBufferSize :: !(Maybe Int),
    -- | bytes of write-ahead log after which the column families holding the
    -- oldest log file are flushed ('Nothing' = RocksDB default)
    maxWalSize :: !(Maybe Int),
    -- | bytes of RocksDB block cache shared by all column families, which
    -- also holds their index and filter blocks ('Nothing' = RocksDB default:
    -- 32 MiB per column family, index and filter blocks outside it)
    blockCache :: !(Maybe Int),
    -- | log messages here
    logger :: !Logger
  }

-- | Run a store and pass its handles to the given action. Everything is stopped
-- when the action returns, and an exception in any component is re-thrown in
-- the calling thread.
--
-- The database is opened first, then the node, the cache (if 'redis' is set)
-- and the block store.
withStore ::
  (MonadUnliftIO m) =>
  StoreConfig ->
  (Store -> m a) ->
  m a
withStore cfg action =
  connectDB cfg $
    ReaderT $ \db -> flip runContT return $ do
      pub <- ContT withPublisher
      node_pub <- ContT withPublisher
      node_sub <- ContT $ withSubscription node_pub
      node <- ContT $ withNode $ nodeCfg cfg db node_pub
      cache_cfg <- ContT $ withCache cfg node.chain db pub
      block_store <- ContT $ withBlockStore $ blockStoreCfg cfg node pub db
      fwd <- ContT $ withAsync $ nodeForwarder block_store pub node_sub
      link fwd
      lift $
        action
          Store
            { peerMgr = node.peerMgr,
              chain = node.chain,
              block = block_store,
              db = db,
              cache = cache_cfg,
              pub = pub,
              net = cfg.net,
              ctx = cfg.ctx
            }

-- | Open the database described by the configuration and run an action with its reader.
connectDB ::
  (MonadUnliftIO m) =>
  StoreConfig ->
  DatabaseReaderT m a ->
  m a
connectDB cfg f = do
  stats <- liftIO $ mapM createDataMetrics cfg.stats
  withDatabaseReader
    cfg.net
    cfg.ctx
    cfg.initGap
    cfg.gap
    cfg.db
    cfg.bloom
    cfg.maxOpenFiles
    cfg.writeBufferSize
    cfg.maxWalSize
    cfg.blockCache
    stats
    f

-- | Configuration of the block store, from the store configuration.
blockStoreCfg ::
  StoreConfig ->
  Node ->
  Publisher StoreEvent ->
  DatabaseReader ->
  BlockStoreConfig
blockStoreCfg cfg node pub db =
  BlockStoreConfig
    { chain = node.chain,
      peerMgr = node.peerMgr,
      pub = pub,
      db = db,
      net = cfg.net,
      ctx = cfg.ctx,
      noMempool = cfg.noMempool,
      wipeMempool = cfg.wipeMempool,
      syncMempool = cfg.syncMempool,
      blockWindow = cfg.blockWindow,
      mempoolTimeout = cfg.mempoolTimeout,
      peerTimeout = cfg.peerTimeout,
      stats = cfg.stats,
      logger = withSource "BlockStore" cfg.logger
    }

-- | Configuration of the node, from the store configuration. The node keeps its
-- headers in the default column family of the store database.
nodeCfg ::
  StoreConfig ->
  DatabaseReader ->
  Publisher NodeEvent ->
  NodeConfig
nodeCfg cfg db pub =
  NodeConfig
    { maxPeers = cfg.maxPeers,
      db = db.db,
      cf = Nothing,
      peers = cfg.initPeers,
      discover = cfg.discover,
      pub = pub,
      address =
        NetworkAddress
          0
          (sockToHostAddress (SockAddrInet 0 0)),
      net = cfg.net,
      timeout = cfg.peerTimeout,
      maxPeerLife = cfg.maxPeerLife,
      connect = cfg.connect,
      logger = cfg.logger
    }

-- | If Redis is configured, connect to it, start the cache writer, and pass the
-- cache configuration to the action. Otherwise pass 'Nothing'.
withCache ::
  (MonadUnliftIO m) =>
  StoreConfig ->
  Chain ->
  DatabaseReader ->
  Publisher StoreEvent ->
  (Maybe CacheConfig -> m a) ->
  m a
withCache cfg chain db pub action =
  case cfg.redis of
    Nothing ->
      action Nothing
    Just redisurl -> do
      conn <- connectRedis redisurl
      metrics <- mapM (\s -> newCacheMetrics logger s conn db) cfg.stats
      withSubscription pub $ \evts ->
        let conf = c conn metrics
         in withProcess (f conf) $ \p ->
              cacheWriterProcesses
                cfg.redisSyncInterval
                evts
                (getProcessMailbox p)
                (action (Just conf))
  where
    f conf cwinbox = runReaderT (cacheWriter conf cwinbox) db
    logger = withSource "Cache" cfg.logger
    c conn metrics =
      CacheConfig
        { redis = conn,
          minAddrs = cfg.redisMinAddrs,
          chain = chain,
          maxKeys = cfg.redisMaxKeys,
          metrics = metrics,
          logger = logger
        }

-- | Run the loop that feeds store events to the cache writer while the action runs.
cacheWriterProcesses ::
  (MonadUnliftIO m) =>
  Int ->
  Inbox StoreEvent ->
  CacheWriter ->
  m a ->
  m a
cacheWriterProcesses interval evts cwm action =
  withAsync (cacheWriterEvents interval evts cwm) $ \a1 -> link a1 >> action

-- | Forward store events to the cache writer, and ask it to sync the mempool
-- every given number of seconds.
cacheWriterEvents :: (MonadUnliftIO m) => Int -> Inbox StoreEvent -> CacheWriter -> m ()
cacheWriterEvents interval evts cwm =
  withAsync mempool . const $
    loopForever $
      receive evts >>= \e ->
        e `cacheWriterDispatch` cwm
  where
    mempool = loopForever $ do
      threadDelay (interval * 1000 * 1000)
      cacheSyncMempool cwm

-- | Tell the cache writer about new blocks and mempool changes.
cacheWriterDispatch :: (MonadIO m) => StoreEvent -> CacheWriter -> m ()
cacheWriterDispatch (StoreBestBlock _) = cacheNewBlock
cacheWriterDispatch (StoreMempoolNew t) = cacheNewTx t
cacheWriterDispatch (StoreMempoolDelete t) = cacheNewTx t
cacheWriterDispatch _ = const (return ())

-- | Forward every node event to the block store and the store publisher, forever.
nodeForwarder ::
  (MonadIO m) =>
  BlockStore ->
  Publisher StoreEvent ->
  Inbox NodeEvent ->
  m ()
nodeForwarder b pub sub =
  loopForever $ receive sub >>= atomically . storeDispatch b pub

-- | Dispatcher of node events. Peer connections, blocks, transactions,
-- inventories and not-found messages go to the block store; peer events,
-- pongs, transaction announcements and rejections are published as store
-- events; new best headers go to the block store.
storeDispatch ::
  BlockStore ->
  Publisher StoreEvent ->
  NodeEvent ->
  STM ()
storeDispatch b pub (PeerEvent pe) =
  case pe of
    PeerConnected p -> do
      publishSTM (StorePeerConnected p) pub
      blockStorePeerConnectSTM p b
    PeerDisconnected p -> do
      publishSTM (StorePeerDisconnected p) pub
      blockStorePeerDisconnectSTM p b
    PeerMessage p msg ->
      case msg of
        MPong (Pong n) ->
          publishSTM (StorePeerPong p n) pub
        MBlock block ->
          blockStoreBlockSTM p block b
        MTx tx ->
          blockStoreTxSTM p tx b
        MNotFound (NotFound is) -> do
          let blocks =
                [ BlockHash h
                | InvVector t h <- is,
                  t == InvBlock || t == InvWitnessBlock
                ]
          unless (null blocks) $ blockStoreNotFoundSTM p blocks b
        MInv (Inv is) -> do
          let txs =
                [ TxHash h
                | InvVector t h <- is,
                  t == InvTx || t == InvWitnessTx
                ]
          publishSTM (StoreTxAnnounce p txs) pub
          unless (null txs) $ blockStoreTxHashSTM p txs b
        -- A reject that names no transaction hash cannot be attributed to a
        -- transaction and falls through to the catch-all below.
        MReject Reject {message = MCTx, extra = Just h, code, reason} ->
          publishSTM (StoreTxReject p (TxHash h) code reason.get) pub
        _ -> return ()
storeDispatch b _ (ChainEvent (ChainBestBlock bn)) =
  blockStoreHeadSTM bn b
storeDispatch _ _ (ChainEvent _) = return ()
