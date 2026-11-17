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
    cachePrefix,
    Store (..),
    withStore,
  )
where

import Control.Concurrent.Async (link, withAsync)
import Control.Concurrent.STM (STM, atomically)
import Control.Monad (forM_, unless, when)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as C
import Data.Serialize (Serialize, decode)
import Data.String.Conversions (cs)
import Data.Time.Clock (NominalDiffTime)
import Data.Word (Word32, Word64)
import Database.Redis qualified as Redis
import Haskoin
  ( BlockHash (..),
    Headers (..),
    Inv (..),
    InvType (..),
    InvVector (..),
    Message (..),
    MessageCommand (..),
    Network (..),
    NetworkAddress (..),
    NotFound (..),
    Pong (..),
    Reject (..),
    TxHash (..),
    VarString (..),
    headerHash,
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
    blockStoreAnnouncedSTM,
    blockStoreBlockSTM,
    blockStoreHeadSTM,
    blockStoreNotFoundSTM,
    blockStorePeerConnectSTM,
    blockStorePeerDisconnectSTM,
    blockStorePongSTM,
    blockStoreTxHashSTM,
    blockStoreTxSTM,
    withBlockStore,
  )
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
import Haskoin.Store.Fleet.Member
  ( Member (..),
    MemberConfig (..),
    memberCacheLoop,
    memberEvents,
    memberForget,
    memberLoop,
    memberReceived,
    newMember,
    noteBest,
  )
import Haskoin.Store.Fleet.Redis (FleetEvent (..))
import Haskoin.Store.XPub.Service (XPubConfig (..))
import NQE
  ( Inbox,
    Publisher,
    publish,
    publishSTM,
    receive,
    withPublisher,
    withSubscription,
  )
import Network.Socket (SockAddr (..))
import System.Metrics.StatsD
import System.Random (randomIO)

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
    -- | the store as a member of its fleet, with the door to the fleet's
    -- cache of xpubs that the web server answers them from; 'Nothing'
    -- without Redis, when the store is unhealthy and answers 503
    fleet :: !(Maybe Member),
    -- | what the store tells its clients: new best blocks and changes of
    -- the mempool. For a store alone they are its own, those that the
    -- block store publishes.
    -- For a member of a fleet they are those of the fleet, read from the
    -- Redis that it shares, so that every server tells the same.
    events :: !(Publisher StoreEvent),
    -- | store events are published here
    pub :: !(Publisher StoreEvent),
    -- | network constants
    net :: !Network,
    -- | how the store opens a connection to a node, for those it makes
    -- besides the ones of the peer manager
    -- ("Haskoin.Store.Web.Publish")
    connect :: !(SockAddr -> WithConnection)
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
    -- | Redis URL of the fleet: its tip, pool and cache of extended public
    -- keys ("Haskoin.Store.Fleet.Member"); an empty URL for the local Redis.
    -- A store always has one: 'Nothing' is for tests of the block store
    -- alone, which then syncs blocks but answers 503.
    redis :: !(Maybe String),
    -- | gap on extended public key with no transactions
    initGap :: !Word32,
    -- | gap for extended public keys
    gap :: !Word32,
    -- | most xpubs in the fleet's cache, at least 1, by GreedyDual: those
    -- that cost least to index again go first, of those not used for a
    -- while
    redisMaxXPubs :: !Int,
    -- | the indexers of xpubs ("Haskoin.Store.XPub.Service");
    -- 'Haskoin.Store.XPub.Service.defaultXPubConfig' for one thread a core
    xpubs :: !XPubConfig,
    -- | blocks to request from the syncing peer in one batch
    blockWindow :: !Int,
    -- | delete mempool transactions older than this number of days
    mempoolTimeout :: !Int,
    -- | disconnect peer if message not received for this many seconds
    peerTimeout :: !NominalDiffTime,
    -- | ask another peer for the blocks that the syncing peer has not
    -- delivered for this many seconds
    blockStall :: !NominalDiffTime,
    -- | a peer is asked for blocks if it answers a ping within this long,
    -- or if none does
    blockPing :: !NominalDiffTime,
    -- | disconnect peer if it has been connected this long
    maxPeerLife :: !NominalDiffTime,
    -- | connect to peers using the function 'Haskoin.Node.withConnection'
    connect :: !(SockAddr -> WithConnection),
    -- | StatsD
    stats :: !(Maybe Stats),
    -- | Bloom filters on the column families that are read by key
    bloom :: !Bool,
    -- | RocksDB max_open_files ('Nothing' = unlimited, which is what a
    -- store with a block cache wants: its index and filter blocks are in
    -- the cache, an open file costs a few kilobytes, and a file that is
    -- not open has to be opened for every lookup that lands in it. Without
    -- a block cache every open file keeps its index and filter in memory,
    -- and the limit is what bounds them)
    maxOpenFiles :: !(Maybe Int),
    -- | bytes of RocksDB memtables across all column families ('Nothing' =
    -- unlimited: each column family keeps up to two 64 MiB memtables)
    writeBufferSize :: !(Maybe Int),
    -- | bytes of write-ahead log after which the column families holding the
    -- oldest log file are flushed ('Nothing' = RocksDB default)
    maxWalSize :: !(Maybe Int),
    -- | threads that RocksDB may use at once for flushes and compactions
    -- together ('Nothing' = RocksDB default: two, one of each). More of
    -- them shorten a backlog of compactions where the disk and the cores
    -- have room, and take both from the requests where they have not
    backgroundJobs :: !(Maybe Int),
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
                withStoreFleet cfg pub member $
                  withFleetEvents cfg pub member $ \events ->
                    action $
                      Store
                        { peerMgr = node.peerMgr,
                          chain = node.chain,
                          block = block_store,
                          db = db,
                          fleet = member,
                          events = events,
                          pub = pub,
                          net = cfg.net,
                          connect = cfg.connect
                        }

-- | What every key of the fleet in Redis starts with.
cachePrefix :: Network -> ByteString
cachePrefix net = "hs2:" <> C.pack net.name <> ":"

-- | The store as a member of a fleet, if the configuration says that it is
-- one and has a Redis to share: under a name of its own, with a lease of
-- ten seconds, views of its last six blocks, and the store's limit on the
-- age of unconfirmed transactions.
storeMember :: StoreConfig -> DatabaseReader -> IO (Maybe Member)
storeMember cfg db = case cfg.redis of
  Just url -> do
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
            expiry = fromIntegral cfg.mempoolTimeout * 24 * 3600,
            cacheMax = max 1 cfg.redisMaxXPubs,
            indexer = cfg.xpubs.indexer,
            stats = cfg.stats,
            note = \level -> logAt level (withSource "Fleet" cfg.logger)
          }
        conn
        db
  -- without Redis the store syncs blocks, but is unhealthy and answers 503
  Nothing -> return Nothing

-- | Take the store's turns as a member of its fleet while an action runs,
-- if it is one: every two seconds, and whenever the best block changes.
-- Beside them, in a thread of its own, the member builds the entries of
-- the fleet's cache of extended public keys that requests asked for.
withStoreFleet :: StoreConfig -> Publisher StoreEvent -> Maybe Member -> IO a -> IO a
withStoreFleet _ _ Nothing action = action
withStoreFleet _ pub (Just member) action =
  withSubscription pub $ \sub ->
    withAsync (memberLoop member (nextBlock sub)) $ \a ->
      withAsync (memberCacheLoop member) $ \b -> link a >> link b >> action
  where
    -- until the next block, telling the member what the store deletes
    nextBlock sub =
      receive sub >>= \case
        StoreBestBlock _ -> return ()
        StoreMempoolDelete h -> memberForget member [h] >> nextBlock sub
        _ -> nextBlock sub

-- | Run an action with the events that the store tells its clients. A
-- store alone tells its own. A member of a fleet tells those of the
-- fleet, which a thread reads from Redis for as long as the action runs.
withFleetEvents ::
  StoreConfig ->
  Publisher StoreEvent ->
  Maybe Member ->
  (Publisher StoreEvent -> IO a) ->
  IO a
withFleetEvents _ pub Nothing action = action pub
withFleetEvents _ _ (Just member) action =
  withPublisher $ \shared ->
    withAsync (memberEvents member False (tell shared)) $ \a -> link a >> action shared
  where
    tell shared event = forM_ (told event) (`publish` shared)
    told = \case
      FleetBlock h -> StoreBestBlock <$> hash h
      FleetEntered h -> StoreMempoolNew <$> hash h
      FleetLeft h -> StoreMempoolDelete <$> hash h
    hash :: (Serialize x) => ByteString -> Maybe x
    hash = either (const Nothing) Just . decode

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
    cfg.initGap
    cfg.gap
    cfg.db
    cfg.bloom
    cfg.maxOpenFiles
    cfg.writeBufferSize
    cfg.maxWalSize
    cfg.backgroundJobs
    cfg.blockCache
    stats
    $ \db -> do
      done <- upgradeDatabase (logAt LevelInfo (withSource "Database" cfg.logger)) db
      when (done.moved > 0) $
        logAt LevelInfo (withSource "Database" cfg.logger) $
          "Moved " <> cs (show done.moved) <> " mempool transactions to their own area"
      when (done.rewritten > 0) $
        logAt LevelInfo (withSource "Database" cfg.logger) $
          "Wrote the records of " <> cs (show done.rewritten) <> " confirmed transactions again, to be written once"
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
      onBest = maybe (\_ -> return ()) noteBest member,
      onTx = maybe (\_ _ -> return ()) memberReceived member,
      blockWindow = cfg.blockWindow,
      mempoolTimeout = cfg.mempoolTimeout,
      peerTimeout = cfg.peerTimeout,
      blockStall = cfg.blockStall,
      blockPing = cfg.blockPing,
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
        MPong (Pong n) -> do
          publishSTM (StorePeerPong p n) pub
          blockStorePongSTM p n b
        -- the headers of a message are a chain: the last says what the
        -- peer has
        MHeaders (Headers hs)
          | (h, _) : _ <- reverse hs -> blockStoreAnnouncedSTM p [headerHash h] b
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
              blocks =
                [ BlockHash h
                | InvVector t h <- is,
                  t == InvBlock || t == InvWitnessBlock
                ]
          publishSTM (StoreTxAnnounce p txs) pub
          unless (null blocks) $ blockStoreAnnouncedSTM p blocks b
          unless (null txs) $ blockStoreTxHashSTM p txs b
        -- A reject that names no transaction hash cannot be attributed to a
        -- transaction and falls through to the catch-all below.
        MReject Reject {message = MCTx, extra = Just h, code, reason} ->
          publishSTM (StoreTxReject p (TxHash h) code reason.get) pub
        _ -> return ()
storeDispatch b _ (ChainEvent (ChainBestBlock bn)) =
  blockStoreHeadSTM bn b
storeDispatch _ _ (ChainEvent _) = return ()
