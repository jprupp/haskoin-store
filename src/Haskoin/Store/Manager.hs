-- |
-- Module      : Haskoin.Store.Manager
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Wires the components of a store together: the Redis of its fleet, the
-- RocksDB database, the peer-to-peer node, the block store that imports
-- data, and the store's membership of its fleet ("Haskoin.Store.Fleet.Member"),
-- which keeps the shared mempool and the cache of xpubs in Redis. Node
-- events are forwarded to the block store and turned into 'StoreEvent's.
--
-- Keeping the wiring in one place means each component only knows about the
-- interfaces it uses: the node does not know about the block store, and the
-- block store knows the member only through a few callbacks. 'withStore'
-- starts them in dependency order and stops them all together.
module Haskoin.Store.Manager
  ( StoreConfig (..),
    cachePrefix,
    Store (..),
    withStore,
  )
where

import Control.Concurrent.Async (link, withAsync)
import Control.Concurrent.STM (STM, TVar, atomically, newTVarIO, readTVar, retry)
import Control.Exception (throwIO)
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
    BlockNode (..),
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
    Judgement (..),
    LogLevel (..),
    Logger,
    Node (..),
    NodeConfig (..),
    NodeEvent (..),
    PeerEvent (..),
    PeerMgr,
    Permissions,
    WithConnection,
    chainGetBest,
    judgeMessage,
    logAt,
    withNode,
    withSource,
  )
import Haskoin.Node.Address (toSockAddr)
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
    withBlockStore,
  )
import Haskoin.Store.Common
  ( StoreEvent (..),
    loopForever,
  )
import Haskoin.Store.Data (DatabaseHealth)
import Haskoin.Store.Database.Migrate (Migration (..), upgradeDatabase)
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    createDataMetrics,
    withLayeredReader,
  )
import Haskoin.Store.Database.Stalls (noStall, watchStalls)
import Haskoin.Store.Fleet.Member
  ( Member (..),
    MemberConfig (..),
    memberCacheLoop,
    memberEvents,
    memberLoop,
    newMember,
    noteBest,
  )
import Haskoin.Store.Fleet.Redis (FleetEvent (..))
import Haskoin.Store.Ingest (defaultConfig)
import Haskoin.Store.Ingest.Run
  ( IngestConfig (..),
    Ingester,
    ingestEmptiedSTM,
    ingestLookSTM,
    ingestMissingSTM,
    ingestNamedSTM,
    ingestPeerDownSTM,
    ingestPeerUpSTM,
    ingestTxSTM,
    withIngest,
  )
import Haskoin.Store.NodePeer (NodePeer (..), nodePeerAddress, nodePeerConnect, nodePeerName)
import Haskoin.Store.NodeRpc (nodeSession)
import Haskoin.Store.NodeZmq (parseZmqAddress)
import Haskoin.Store.Startup (connectRedis, explainDatabase)
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
    -- | process that imports blocks
    block :: !BlockStore,
    -- | the path of transactions from the peers to the pool of the fleet
    -- ("Haskoin.Store.Ingest.Run")
    ingest :: !Ingester,
    -- | database reader
    db :: !DatabaseReader,
    -- | the store as a member of its fleet, with the door to the fleet's
    -- cache of xpubs that the web server answers them from; 'Nothing'
    -- without Redis, when the store is unhealthy and answers 503
    fleet :: !(Maybe Member),
    -- | what the store tells its clients: new best blocks and changes of
    -- the mempool, those of the fleet, read from the Redis that it
    -- shares, so that every server tells the same. Without a fleet, in
    -- tests of the block store alone, they are those that the block store
    -- publishes.
    events :: !(Publisher StoreEvent),
    -- | store events are published here
    pub :: !(Publisher StoreEvent),
    -- | network constants
    net :: !Network,
    -- | how the store opens a connection to a node, for those it makes
    -- besides the ones of the peer manager
    -- ("Haskoin.Store.Web.Publish")
    connect :: !(SockAddr -> WithConnection),
    -- | the state of the database: whether RocksDB lets writes through,
    -- and its work in the background, polled every five seconds
    -- ("Haskoin.Store.Database.Stalls"); @\/health@ reports it
    writes :: !(TVar DatabaseHealth)
  }

-- | Configuration for a 'Store'.
data StoreConfig = StoreConfig
  { -- | max peers to connect to
    maxPeers :: !Int,
    -- | static set of peers to connect to
    initPeers :: ![String],
    -- | the permissions that the nodes of 'initPeers' grant the store with
    -- @-whitelist@ or @-whitebind@, such as 'Haskoin.Node.PermMempool', which lets the
    -- store ask them for their mempools ('Haskoin.Node.NodeConfig')
    peerPermissions :: !Permissions,
    -- | whether the nodes of 'initPeers' are nodes of somebody else. They
    -- are taken for nodes of the operator otherwise, and the store takes
    -- what such a node took: a transaction that only strangers named is
    -- looked at first, and not taken if the store cannot verify it
    -- ("Haskoin.Store.Ingest").
    foreignPeers :: !Bool,
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
    -- | the longest that the syncing peer may deliver nothing before another
    -- peer is asked for its blocks; a quick peer is given less
    blockStall :: !NominalDiffTime,
    -- | a peer is asked for blocks if it answers a ping within this long,
    -- or if none does
    blockPing :: !NominalDiffTime,
    -- | disconnect peer if it has been connected this long
    maxPeerLife :: !NominalDiffTime,
    -- | connect to peers using the function 'Haskoin.Node.withConnection'
    connect :: !(SockAddr -> WithConnection),
    -- | the RPC of a full node of the operator, its Unix socket if it has
    -- one, and where that node publishes over ZeroMQ. With the RPC and
    -- the ZeroMQ socket both, the node is one more peer of the store,
    -- which needs no peer-to-peer connection ("Haskoin.Store.NodePeer").
    -- The web server has the RPC too, to publish through
    -- ('Haskoin.Store.Web.WebConfig').
    nodePeerRpc :: !(Maybe String),
    nodePeerRpcSocket :: !(Maybe FilePath),
    nodePeerZmq :: !(Maybe String),
    -- | StatsD
    stats :: !(Maybe Stats),
    -- | Bloom filters on the column families that are read by key
    bloom :: !Bool,
    -- | RocksDB max_open_files ('Nothing' = unlimited: a file that is not
    -- open has to be opened for every lookup that lands in it. Every open
    -- file keeps its index and filter in memory, outside the block cache,
    -- about 0.9% of its data, and the limit is what bounds them)
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
    -- | bytes of RocksDB block cache shared by all column families, for
    -- data blocks; index and filter blocks stay outside it, with their open
    -- tables ('Nothing' = RocksDB default: 32 MiB per column family)
    blockCache :: !(Maybe Int),
    -- | log messages here
    logger :: !Logger
  }

-- | Run a store and pass its handles to the given action. Everything is stopped
-- when the action returns, and an exception in any component is re-thrown in
-- the calling thread.
--
-- Redis is connected first (if 'redis' is set), and a store that cannot
-- reach it stops there with a 'StartupError' that says why. Then the
-- database is opened, with a thread that watches it for write stalls
-- (@withStalls@), then the node, the block store and the cache.
withStore ::
  StoreConfig ->
  (Store -> IO a) ->
  IO a
withStore cfg0 action = do
  cfg <- withNodePeer cfg0
  -- Redis first: a store cannot run without it, and finding out after the
  -- database opened and the node started only takes longer
  redis <- mapM connectRedis cfg.redis
  explainDatabase cfg.db (connectDB cfg) $ \db ->
    withStalls cfg db $ \writes ->
      withPublisher $ \pub ->
        withPublisher $ \node_pub ->
          withSubscription node_pub $ \node_sub ->
            withNode (nodeCfg cfg db node_pub) $ \node -> do
              member <- storeMember cfg redis db
              withIngest (ingestCfg cfg node db member) $ \ingest ->
                withBlockStore (blockStoreCfg cfg node pub db member ingest) $ \block_store ->
                  withAsync (nodeForwarder block_store ingest pub node_sub) $ \fwd -> do
                    link fwd
                    withStoreFleet member ingest $
                      withFleetEvents cfg pub member $ \events ->
                        action $
                          Store
                            { peerMgr = node.peerMgr,
                              chain = node.chain,
                              block = block_store,
                              ingest = ingest,
                              db = db,
                              fleet = member,
                              events = events,
                              pub = pub,
                              net = cfg.net,
                              connect = cfg.connect,
                              writes = writes
                            }

-- | The configuration of a store that has the RPC of a node and the
-- address of its ZeroMQ socket, with that node as one more of its peers
-- ("Haskoin.Store.NodePeer"): it is named among the peers that the store
-- was given, so that it is a node of the operator as they are, and a
-- connection to its address is a pipe in memory. Any other store is left
-- as it is.
--
-- An address of ZeroMQ that cannot be read stops the store, as any
-- option that makes no sense does.
withNodePeer :: StoreConfig -> IO StoreConfig
withNodePeer cfg = case (cfg.nodePeerRpc, cfg.nodePeerZmq) of
  (Just url, Just written) -> do
    zmq <- either (throwIO . userError) return (parseZmqAddress written)
    session <- nodeSession cfg.nodePeerRpcSocket
    let peer = NodePeer {net = cfg.net, session, url, zmq, logger = withSource "Node RPC" cfg.logger}
        connect sa
          | sa == nodePeerAddress = nodePeerConnect peer
          | otherwise = cfg.connect sa
    return (cfg {initPeers = cfg.initPeers <> [nodePeerName], connect} :: StoreConfig)
  _ -> return cfg

-- | Watch RocksDB for write stalls while an action runs
-- ("Haskoin.Store.Database.Stalls"), and give the action the latest state.
withStalls :: StoreConfig -> DatabaseReader -> (TVar DatabaseHealth -> IO a) -> IO a
withStalls cfg db action = do
  writes <- newTVarIO noStall
  withAsync (watchStalls note cfg.stats db.db writes) $ \a -> link a >> action writes
  where
    note level = logAt level (withSource "Database" cfg.logger)

-- | What every key of the fleet in Redis starts with. The number is that of
-- the way the fleet keeps its state: stores that keep it in another way
-- have another, and so are a fleet of their own on the same Redis, where
-- sharing keys would have each misread what the other wrote. It is 3 since
-- the rules of the shared mempool are decided in Haskell
-- ("Haskoin.Store.Fleet.Pool.Decide"), which keeps what a transaction does
-- at addresses in a form that the script before it does not read.
cachePrefix :: Network -> ByteString
cachePrefix net = "hs3:" <> C.pack net.name <> ":"

-- | The store as a member of its fleet, on the Redis that 'connectRedis'
-- connected and checked ('Nothing' only without one, in tests): under a
-- name of its own, with a lease of
-- ten seconds, views of its last six blocks, and the store's limit on the
-- age of unconfirmed transactions.
storeMember :: StoreConfig -> Maybe Redis.Connection -> DatabaseReader -> IO (Maybe Member)
storeMember cfg redis db = case redis of
  Just conn -> do
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
-- if it is one: every two seconds, whenever the best block changes, and
-- an offer as soon as a transaction comes ('memberLoop'). Beside them, in
-- a thread of its own, the member builds the entries of the fleet's cache
-- of extended public keys that requests asked for.
withStoreFleet :: Maybe Member -> Ingester -> IO a -> IO a
withStoreFleet Nothing _ action = action
withStoreFleet (Just member) ingest action =
  withAsync (memberLoop member) $ \a ->
    withAsync (memberCacheLoop member) $ \b ->
      withAsync (emptied 0) $ \c -> link a >> link b >> link c >> action
  where
    -- each time the member finds that the pool lost what it had, the path
    -- into the pool is told, and reads its sources again
    emptied seen = do
      n <- atomically $ do
        n <- readTVar member.refills
        when (n == seen) retry
        return n
      atomically (ingestEmptiedSTM ingest)
      emptied n

-- | Run an action with the events that the store tells its clients: those
-- of the fleet, which a thread reads from Redis for as long as the action
-- runs. Without a fleet, in tests, those that the block store publishes.
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
-- its reader. A database of version 20 has its mempool taken out here,
-- once, before anything reads it ("Haskoin.Store.Database.Migrate"); older
-- stores refuse the database from then on.
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
          "Took "
            <> cs (show done.moved)
            <> " mempool transactions out of the chain; the pool gets them again from the peers"
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
  Ingester ->
  BlockStoreConfig
blockStoreCfg cfg node pub db member ingest =
  BlockStoreConfig
    { chain = node.chain,
      peerMgr = node.peerMgr,
      pub = pub,
      db = db,
      net = cfg.net,
      -- the member takes its view of the block, and the path into the
      -- pool looks at whether the store is in sync now
      onBest = \h -> mapM_ (`noteBest` h) member >> atomically (ingestLookSTM ingest),
      blockWindow = cfg.blockWindow,
      peerTimeout = cfg.peerTimeout,
      blockStall = cfg.blockStall,
      blockPing = cfg.blockPing,
      stats = cfg.stats,
      logger = withSource "BlockStore" cfg.logger
    }

-- | What the path into the pool needs of the store ("Haskoin.Store.Ingest.Run"):
-- the member of the fleet, whether the best block of the database is the
-- best header of the node, which peers give their mempools when asked,
-- as the model of each peer's node says ('judgeMessage'), and which are
-- nodes of the operator: those that the store was given ('initPeers'),
-- unless it was told that they are not ('foreignPeers').
--
-- A peer is known by its address, as haskoin-node names a peer, and the
-- names of 'initPeers' are resolved here as haskoin-node resolves them
-- ('toSockAddr'), each time a peer connects. A name that resolves to
-- another address by then makes a node of the operator a stranger: what
-- the store cannot verify of it comes late, and nothing is taken that
-- should not be.
ingestCfg :: StoreConfig -> Node -> DatabaseReader -> Maybe Member -> IngestConfig
ingestCfg cfg node db member =
  IngestConfig
    { net = cfg.net,
      config = defaultConfig,
      member,
      inSync = do
        best <- Q.getBestBlock db
        top <- chainGetBest node.chain
        return (best == Just (headerHash top.header)),
      source = \p -> (== Allowed) <$> judgeMessage p MMempool,
      own = \p ->
        if cfg.foreignPeers
          then return False
          else do
            given <- concat <$> mapM (toSockAddr cfg.net) cfg.initPeers
            return (show p `elem` map show given),
      stats = cfg.stats,
      logger = withSource "Ingest" cfg.logger
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
      logger = cfg.logger,
      permissions = cfg.peerPermissions
    }

-- | Forward every node event to the block store and the store publisher, forever.
nodeForwarder ::
  BlockStore ->
  Ingester ->
  Publisher StoreEvent ->
  Inbox NodeEvent ->
  IO ()
nodeForwarder b ing pub sub =
  loopForever $ receive sub >>= atomically . storeDispatch b ing pub

-- | Dispatcher of node events. Peer connections, blocks, transactions,
-- inventories and not-found messages go to the block store; peer events,
-- pongs, transaction announcements and rejections are published as store
-- events; new best headers go to the block store.
storeDispatch ::
  BlockStore ->
  Ingester ->
  Publisher StoreEvent ->
  NodeEvent ->
  STM ()
storeDispatch b ing pub (PeerEvent pe) =
  case pe of
    PeerConnected p -> do
      publishSTM (StorePeerConnected p) pub
      blockStorePeerConnectSTM p b
      ingestPeerUpSTM p ing
    PeerDisconnected p -> do
      publishSTM (StorePeerDisconnected p) pub
      blockStorePeerDisconnectSTM p b
      ingestPeerDownSTM p ing
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
          ingestTxSTM p tx ing
        MNotFound (NotFound is) -> do
          let blocks =
                [ BlockHash h
                | InvVector t h <- is,
                  t == InvBlock || t == InvWitnessBlock
                ]
              txs =
                [ TxHash h
                | InvVector t h <- is,
                  t == InvTx || t == InvWitnessTx
                ]
          unless (null blocks) $ blockStoreNotFoundSTM p blocks b
          unless (null txs) $ ingestMissingSTM p txs ing
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
          unless (null txs) $ ingestNamedSTM p txs ing
        -- A reject that names no transaction hash cannot be attributed to a
        -- transaction and falls through to the catch-all below.
        MReject Reject {message = MCTx, extra = Just h, code, reason} ->
          publishSTM (StoreTxReject p (TxHash h) code reason.get) pub
        _ -> return ()
storeDispatch b ing _ (ChainEvent (ChainBestBlock bn)) = do
  blockStoreHeadSTM bn b
  -- a new best header: the store may be out of sync now
  ingestLookSTM ing
storeDispatch _ _ _ (ChainEvent _) = return ()
