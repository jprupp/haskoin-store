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
-- cache. Node events are forwarded to the block store and turned into
-- 'StoreEvent's, which the cache follows.
--
-- Keeping the wiring in one place means each component only knows about the
-- interfaces it uses: the node does not know about the block store, and the
-- block store does not know about the cache. 'withStore' starts them in dependency order
-- and stops them all together.
module Haskoin.Store.Manager
  ( StoreConfig (..),
    Store (..),
    withStore,
  )
where

import Control.Monad (unless, when)
import Data.ByteString.Char8 qualified as C
import Data.String.Conversions (cs)
import Data.Time.Clock (NominalDiffTime)
import Data.Word (Word32, Word64)
import Database.Redis qualified as Redis
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
    LogLevel (..),
    Logger,
    Node (..),
    NodeConfig (..),
    NodeEvent (..),
    PeerEvent (..),
    PeerMgr,
    WithConnection,
    logAt,
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
import Haskoin.Store.Cache (Cache, CacheConfig (..), cachePrefix, withCache)
import Haskoin.Store.Common
  ( StoreEvent (..),
    loopForever,
  )
import Haskoin.Store.Database.Migrate (Migration (..), upgradeDatabase)
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    createDataMetrics,
    withLayeredReader,
  )
import Haskoin.Store.Fleet.Member (Member, MemberConfig (..), memberLoop, newMember, noteBest)
import NQE
  ( Inbox,
    Publisher,
    publishSTM,
    receive,
    withPublisher,
    withSubscription,
  )
import Network.Socket (SockAddr (..))
import System.Metrics.StatsD
import System.Random (randomIO)
import UnliftIO
  ( STM,
    atomically,
    link,
    withAsync,
  )

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
    -- | cache of extended public keys, if Redis is configured and the store
    -- is not a member of a fleet
    cache :: !(Maybe Cache),
    -- | the store as a member of a fleet, if it is one
    fleet :: !(Maybe Member),
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
    -- | Redis URL for the cache of extended public keys ("Haskoin.Store.Cache");
    -- 'Nothing' for no cache, an empty URL for the local Redis
    redis :: !(Maybe String),
    -- | Secp256k1 context
    ctx :: !Ctx,
    -- | gap on extended public key with no transactions
    initGap :: !Word32,
    -- | gap for extended public keys
    gap :: !Word32,
    -- | cache xpubs with at least this many used addresses
    redisMinAddrs :: !Int,
    -- | or with at least this many transactions in their addresses
    redisMinTxs :: !Int,
    -- | evict the least recently used xpubs from the cache past this many
    redisMaxXPubs :: !Int,
    -- | do not index new mempool transactions
    noMempool :: !Bool,
    -- | wipe mempool when starting
    wipeMempool :: !Bool,
    -- | answer as one of a fleet of stores that share the Redis of 'redis':
    -- every store of the fleet then gives the same answer to the same
    -- request, from the block that they agree on and the mempool that they
    -- share ("Haskoin.Store.Fleet.Member"). The cache of extended public
    -- keys is not used then: its entries follow each store's own database,
    -- which is what a fleet does not answer from.
    fleet :: !Bool,
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
    -- | connect to peers using the function 'Haskoin.Node.withConnection'
    connect :: !(SockAddr -> WithConnection),
    -- | StatsD
    stats :: !(Maybe Stats),
    -- | Bloom filters on the column families that are read by key
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
-- The database is opened first, then the node, the block store and the cache
-- (if 'redis' is set).
withStore ::
  StoreConfig ->
  (Store -> IO a) ->
  IO a
withStore cfg action =
  connectDB cfg $ \db ->
    withPublisher $ \pub ->
      withPublisher $ \node_pub ->
        withSubscription node_pub $ \node_sub ->
          withNode (nodeCfg cfg db node_pub) $ \node -> do
            member <- storeMember cfg db
            withBlockStore (blockStoreCfg cfg node pub db member) $ \block_store ->
              withAsync (nodeForwarder block_store pub node_sub) $ \fwd -> do
                link fwd
                withStoreFleet cfg pub member . withStoreCache cfg db node.chain pub $ \cache ->
                  action $
                    Store
                      { peerMgr = node.peerMgr,
                        chain = node.chain,
                        block = block_store,
                        db = db,
                        cache = cache,
                        fleet = member,
                        pub = pub,
                        net = cfg.net,
                        ctx = cfg.ctx
                      }

-- | The store as a member of a fleet, if the configuration says that it is
-- one and has a Redis to share: under a name of its own, with a lease of
-- ten seconds, views of its last six blocks, and the store's limit on the
-- age of unconfirmed transactions.
storeMember :: StoreConfig -> DatabaseReader -> IO (Maybe Member)
storeMember cfg db = case (cfg.fleet, cfg.redis) of
  (True, Just url) -> do
    info <-
      if null url
        then return Redis.defaultConnectInfo
        else either fail return (Redis.parseConnectInfo url)
    conn <- Redis.checkedConnect info
    name <- C.pack . show <$> (randomIO :: IO Word64)
    Just
      <$> newMember
        MemberConfig
          { prefix = cachePrefix cfg.net,
            name,
            lease = 10000,
            depth = 6,
            expiry = fromIntegral cfg.mempoolTimeout * 24 * 3600
          }
        conn
        db
  _ -> return Nothing

-- | Take the store's turns as a member of its fleet while an action runs,
-- if it is one: every two seconds, and whenever the best block changes.
withStoreFleet :: StoreConfig -> Publisher StoreEvent -> Maybe Member -> IO a -> IO a
withStoreFleet _ _ Nothing action = action
withStoreFleet cfg pub (Just member) action =
  withSubscription pub $ \sub ->
    withAsync (memberLoop member (nextBlock sub) complain) $ \a -> link a >> action
  where
    nextBlock sub =
      receive sub >>= \case
        StoreBestBlock _ -> return ()
        _ -> nextBlock sub
    complain why = logAt LevelError (withSource "Fleet" cfg.logger) ("Could not take a turn: " <> cs why)

-- | Run the cache while an action runs, if Redis is configured.
withStoreCache ::
  StoreConfig ->
  DatabaseReader ->
  Chain ->
  Publisher StoreEvent ->
  (Maybe Cache -> IO a) ->
  IO a
withStoreCache cfg db chain pub action =
  case cfg.redis of
    Nothing -> action Nothing
    Just _ | cfg.fleet -> action Nothing
    Just url ->
      withCache
        CacheConfig
          { url = url,
            minAddrs = cfg.redisMinAddrs,
            minTxs = cfg.redisMinTxs,
            maxXPubs = cfg.redisMaxXPubs,
            stats = cfg.stats
          }
        db
        chain
        pub
        (withSource "Cache" cfg.logger)
        (action . Just)

-- | Open the database described by the configuration, bring it to the
-- layout that the store runs on ('upgradeDatabase'), and run an action with
-- its reader. A database of version 20 has its mempool moved to an area of
-- its own here, once, before anything reads it; older stores refuse the
-- database from then on.
connectDB ::
  StoreConfig ->
  (DatabaseReader -> IO a) ->
  IO a
connectDB cfg f = do
  stats <- mapM createDataMetrics cfg.stats
  withLayeredReader
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
    $ \db -> do
      done <- upgradeDatabase (logAt LevelInfo (withSource "Database" cfg.logger)) db
      when (done.moved > 0) $
        logAt LevelInfo (withSource "Database" cfg.logger) $
          "Moved " <> cs (show done.moved) <> " mempool transactions to their own area"
      f db

-- | Configuration of the block store, from the store configuration.
blockStoreCfg ::
  StoreConfig ->
  Node ->
  Publisher StoreEvent ->
  DatabaseReader ->
  Maybe Member ->
  BlockStoreConfig
blockStoreCfg cfg node pub db member =
  BlockStoreConfig
    { chain = node.chain,
      peerMgr = node.peerMgr,
      pub = pub,
      db = db,
      net = cfg.net,
      ctx = cfg.ctx,
      noMempool = cfg.noMempool,
      wipeMempool = cfg.wipeMempool,
      onBest = maybe (\_ -> return ()) noteBest member,
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

-- | Forward every node event to the block store and the store publisher, forever.
nodeForwarder ::
  BlockStore ->
  Publisher StoreEvent ->
  Inbox NodeEvent ->
  IO ()
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
