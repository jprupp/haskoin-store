{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE NoFieldSelectors #-}

module Haskoin.Store.Manager
  ( StoreConfig (..),
    Store (..),
    withStore,
  )
where

import Control.Monad (forever, unless)
import Control.Monad.Cont
  ( ContT (..),
    runContT,
  )
import Control.Monad.Logger (MonadLoggerIO)
import Control.Monad.Reader (ReaderT (ReaderT), runReaderT)
import Control.Monad.Trans (lift)
import Data.Serialize (decode)
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
    Node (..),
    NodeConfig (..),
    NodeEvent (..),
    PeerEvent (..),
    PeerMgr,
    WithConnection,
    withNode,
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
    withAsync
  )
import UnliftIO.Concurrent (threadDelay)

-- | Store mailboxes.
data Store = Store
  { peerMgr :: !PeerMgr,
    chain :: !Chain,
    block :: !BlockStore,
    db :: !DatabaseReader,
    cache :: !(Maybe CacheConfig),
    pub :: !(Publisher StoreEvent),
    net :: !Network,
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
    -- | disconnect peer if message not received for this many seconds
    mempoolTimeout :: !Int,
    -- | delete mempool transactions older than this number of days
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
    bloom :: !Bool
  }

withStore ::
  (MonadLoggerIO m, MonadUnliftIO m) =>
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
    stats
    f

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
      mempoolTimeout = cfg.mempoolTimeout,
      peerTimeout = cfg.peerTimeout,
      stats = cfg.stats
    }

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
      connect = cfg.connect
    }

withCache ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
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
      metrics <- mapM (\s -> newCacheMetrics s conn db) cfg.stats
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
    c conn metrics =
      CacheConfig
        { redis = conn,
          minAddrs = cfg.redisMinAddrs,
          chain = chain,
          maxKeys = cfg.redisMaxKeys,
          metrics = metrics
        }

cacheWriterProcesses ::
  (MonadUnliftIO m) =>
  Int ->
  Inbox StoreEvent ->
  CacheWriter ->
  m a ->
  m a
cacheWriterProcesses interval evts cwm action =
  withAsync (cacheWriterEvents interval evts cwm) $ \a1 -> link a1 >> action

cacheWriterEvents :: (MonadUnliftIO m) => Int -> Inbox StoreEvent -> CacheWriter -> m ()
cacheWriterEvents interval evts cwm =
  withAsync mempool . const $
    forever $
      receive evts >>= \e ->
        e `cacheWriterDispatch` cwm
  where
    mempool = forever $ do
      threadDelay (interval * 1000 * 1000)
      cacheSyncMempool cwm

cacheWriterDispatch :: (MonadIO m) => StoreEvent -> CacheWriter -> m ()
cacheWriterDispatch (StoreBestBlock _) = cacheNewBlock
cacheWriterDispatch (StoreMempoolNew t) = cacheNewTx t
cacheWriterDispatch (StoreMempoolDelete t) = cacheNewTx t
cacheWriterDispatch _ = const (return ())

nodeForwarder ::
  (MonadIO m) =>
  BlockStore ->
  Publisher StoreEvent ->
  Inbox NodeEvent ->
  m ()
nodeForwarder b pub sub =
  forever $ receive sub >>= atomically . storeDispatch b pub

-- | Dispatcher of node events.
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
        MReject Reject {message = MCTx, extra, code, reason} ->
          case decode extra of
            Left _ -> return ()
            Right th ->
              let reject =
                    StoreTxReject
                      p
                      th
                      code
                      reason.get
               in publishSTM reject pub
        _ -> return ()
storeDispatch b _ (ChainEvent (ChainBestBlock bn)) =
  blockStoreHeadSTM bn b
storeDispatch _ _ (ChainEvent _) = return ()
