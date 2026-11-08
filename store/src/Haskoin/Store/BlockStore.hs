{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE NoFieldSelectors #-}
{-# OPTIONS_GHC -Wno-ambiguous-fields #-}

-- |
-- Module      : Haskoin.Store.BlockStore
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The block store process downloads blocks and transactions from peers and
-- imports them into the database.
--
-- = Blocks
--
-- When the best header of the node is ahead of the best block in the database,
-- the block store picks one online peer as its syncing peer and asks it for
-- blocks in batches of 'blockWindow'. Each block is imported as soon as it
-- arrives. If the best block of the database is no longer in the main chain of
-- headers, blocks are reverted until it is. A syncing peer that sends a block
-- that was not requested, cannot find a block, or stays silent for longer than
-- 'peerTimeout' is disconnected.
--
-- = Mempool
--
-- Once the database is in sync, the block store asks each peer for its mempool
-- (if 'syncMempool' is set) and requests every transaction that peers
-- announce. Received transactions are queued and imported in batches on every
-- clock tick, in dependency order. Transactions that spend unknown outputs are
-- kept for up to an hour, waiting for their parents. Mempool transactions older
-- than 'mempoolTimeout' days are deleted.
--
-- = Feeding the block store
--
-- The block store does not listen to the node by itself.
-- "Haskoin.Store.Manager" forwards node events to it with the
-- @blockStore*@ functions of this module.
-- = Why it works this way
--
-- * __Blocks come from one peer at a time.__ Blocks must be imported in order,
--   so downloading from several peers at once would only buffer blocks that cannot
--   be imported yet. One syncing peer, kept busy with a sliding window of
--   requests, uses the network well without unbounded buffering.
--
-- * __The window refills early.__ The next batch is requested when fewer than a
--   quarter of the window are still outstanding, so the peer never sits idle
--   while the store imports, and the backlog never grows much beyond one window.
--
-- * __Mempool sync waits for the tip.__ Unconfirmed transactions only make sense
--   against the current chain, so they are requested once the database is in
--   sync.
--
-- * __Orphans get a grace period.__ A transaction can arrive before its parent.
--   It is held for up to an hour, waiting for the parent, instead of being
--   dropped and requested again.
--
-- * __Imports are batched.__ Mempool transactions are imported up to 2000 at a
--   time on each clock tick, which amortises the cost of a database write over
--   many transactions during bursts.
module Haskoin.Store.BlockStore
  ( -- * Block Store
    BlockStore,
    BlockStoreConfig (..),
    withBlockStore,
    blockStorePeerConnect,
    blockStorePeerConnectSTM,
    blockStorePeerDisconnect,
    blockStorePeerDisconnectSTM,
    blockStoreHead,
    blockStoreHeadSTM,
    blockStoreBlock,
    blockStoreBlockSTM,
    blockStoreNotFound,
    blockStoreNotFoundSTM,
    blockStoreTx,
    blockStoreTxSTM,
    blockStoreTxHash,
    blockStoreTxHashSTM,
    blockStorePendingTxs,
    blockStorePendingTxsSTM,
    needsMoreBlocks,
  )
where

import Control.Monad
  ( forM,
    forM_,
    mzero,
    unless,
    void,
    when,
  )
import Control.Monad.Reader
  ( MonadReader,
    ReaderT (..),
    ask,
    asks,
  )
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe (MaybeT (MaybeT), runMaybeT)
import Data.Bool (bool)
import Data.ByteString qualified as B
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.List (delete)
import Data.Maybe
  ( catMaybes,
    fromJust,
    fromMaybe,
    isJust,
    mapMaybe,
  )
import Data.Serialize (encode)
import Data.String (fromString)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock
  ( NominalDiffTime,
    UTCTime,
    diffUTCTime,
    getCurrentTime,
  )
import Data.Time.Clock.POSIX
  ( getPOSIXTime,
    posixSecondsToUTCTime,
    utcTimeToPOSIXSeconds,
  )
import Data.Time.Format
  ( defaultTimeLocale,
    formatTime,
  )
import Haskoin
  ( Block (..),
    BlockHash (..),
    BlockHeader (..),
    BlockHeight,
    BlockNode (..),
    Ctx,
    GetData (..),
    InvType (..),
    InvVector (..),
    Message (..),
    Network (..),
    OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    blockHashToHex,
    headerHash,
    txHash,
    txHashToHex,
  )
import Haskoin.Node
  ( Chain,
    LogLevel (..),
    Logger,
    OnlinePeer (..),
    Peer (..),
    PeerMgr,
    chainBlockMain,
    chainGetAncestor,
    chainGetBest,
    chainGetBlock,
    chainGetParents,
    getPeers,
    killPeer,
    logAt,
    sendMessage,
    setBusy,
    setFree,
  )
import Haskoin.Store.Common
import Haskoin.Store.Data
-- the instances of the store classes for the database reader
import Haskoin.Store.Database.Query (getMempoolBefore)
import Haskoin.Store.Database.Reader
import Haskoin.Store.Database.Writer
import Haskoin.Store.Logic
  ( ImportException (Orphan),
    ImportM,
    deleteUnconfirmedTx,
    importBlock,
    initBest,
    newMempoolTx,
    revertBlock,
  )
import NQE
  ( InChan,
    Listen,
    Mailbox,
    Publisher,
    inboxToMailbox,
    newInbox,
    publish,
    query,
    receive,
    send,
    sendSTM,
  )
import System.Metrics.StatsD
import System.Random (randomRIO)
import UnliftIO
  ( Exception,
    MonadIO,
    MonadUnliftIO,
    STM,
    TVar,
    async,
    atomically,
    catch,
    liftIO,
    link,
    modifyTVar,
    modifyTVar',
    newTVarIO,
    readTVar,
    readTVarIO,
    throwIO,
    try,
    withAsync,
    writeTVar,
  )
import UnliftIO.Concurrent (threadDelay)

-- | Messages handled by the block store process.
data BlockStoreMessage
  = -- | the node has a new best header
    BlockNewBest !BlockNode
  | -- | a peer connected
    BlockPeerConnect !Peer
  | -- | a peer disconnected
    BlockPeerDisconnect !Peer
  | -- | a peer sent a block
    BlockReceived !Peer !Block
  | -- | a peer could not find blocks
    BlockNotFound !Peer ![BlockHash]
  | -- | a peer sent a transaction
    TxRefReceived !Peer !Tx
  | -- | a peer announced transactions
    TxRefAvailable !Peer ![TxHash]
  | -- | clock tick for housekeeping; reply when done
    BlockPing !(Listen ())

-- | Fatal errors of the block store.
data BlockException
  = -- | the header of a block in the database is not known to the node
    BlockNotInChain !BlockHash
  | -- | the database has no best block
    Uninitialized
  | -- | the database is inconsistent
    CorruptDatabase
  | -- | the node has no header at this height below this block
    AncestorNotInChain !BlockHeight !BlockHash
  | -- | importing mempool transactions failed
    MempoolImportFailed
  deriving (Show, Eq, Ord, Exception)

-- | State of the sync against the syncing peer.
data Syncing = Syncing
  { -- | peer that blocks are downloaded from
    peer :: !Peer,
    -- | when it last delivered a block, or when syncing started
    time :: !UTCTime,
    -- | blocks requested and not yet imported, in chain order
    blocks :: ![BlockHash]
  }

-- | Transaction received from a peer and waiting to be imported.
data PendingTx = PendingTx
  { -- | when it was received
    time :: !UTCTime,
    -- | the transaction
    tx :: !Tx,
    -- | parent transactions that are missing; empty if it can be imported
    deps :: !(HashSet TxHash)
  }
  deriving (Show, Eq, Ord)

-- | Block store process state.
data BlockStore = BlockStore
  { -- | mailbox of the process
    mailbox :: !(Mailbox BlockStoreMessage),
    -- | configuration
    config :: !BlockStoreConfig,
    -- | syncing peer, if any
    peer :: !(TVar (Maybe Syncing)),
    -- | transactions received and waiting to be imported
    txs :: !(TVar (HashMap TxHash PendingTx)),
    -- | transactions requested from peers in the last twenty seconds
    requested :: !(TVar (HashSet TxHash)),
    -- | StatsD metrics, if enabled
    metrics :: !(Maybe StoreMetrics),
    -- | labels of the peers whose mempool was already requested
    syncedMempool :: !(TVar (HashSet Text))
  }

-- | StatsD metrics of the block store.
data StoreMetrics = StoreMetrics
  { -- | height of the best block (@store.blocks@)
    blocks :: !StatGauge,
    -- | height of the best header (@store.headers@)
    headers :: !StatGauge,
    -- | transactions waiting to be imported (@store.queued_txs@)
    queuedTxs :: !StatGauge,
    -- | online peers (@store.peers@)
    peers :: !StatGauge,
    -- | transactions in the mempool (@store.mempool@)
    mempool :: !StatGauge,
    -- | milliseconds to import a block (@store.block@)
    block :: !StatTiming
  }

-- | Register the block store metrics, if StatsD is configured, with their current values.
newStoreMetrics :: (MonadIO m) => BlockStoreConfig -> m (Maybe StoreMetrics)
newStoreMetrics cfg =
  forM cfg.stats $ \s -> liftIO $ do
    m <- withDB cfg.db getMempool
    b <-
      fmap (maybe 0 (.height)) $
        withDB cfg.db $
          runMaybeT $
            MaybeT getBestBlock >>= MaybeT . getBlock
    h <- chainGetBest cfg.chain
    p <- getPeers cfg.peerMgr
    blocks <- g s "blocks" (fromIntegral b)
    headers <- g s "headers" (fromIntegral h.height)
    queuedTxs <- g s "queued_txs" 0
    peers <- g s "peers" (length p)
    mempool <- g s "mempool" (length m)
    block <- t s "block" 0
    return StoreMetrics {..}
  where
    g s x = newStatGauge s ("store." <> x)
    t s x = newStatTiming s ("store." <> x)

-- | Update the gauge of the best block height.
setStoreHeight :: (MonadIO m) => BlockT m ()
setStoreHeight = void $ runMaybeT $ do
  m <- MaybeT (asks (.metrics))
  h <- MaybeT getBestBlock
  b <- MaybeT (getBlock h)
  setGauge m.blocks (fromIntegral b.height)

-- | Update the gauge of the best header height.
setHeadersHeight :: (MonadIO m) => BlockT m ()
setHeadersHeight = void $ runMaybeT $ do
  m <- MaybeT (asks (.metrics))
  n <- liftIO . chainGetBest =<< asks (.config.chain)
  setGauge m.headers (fromIntegral n.height)

-- | Update the gauge of transactions waiting to be imported.
setPendingTxs :: (MonadIO m) => BlockT m ()
setPendingTxs = void $ runMaybeT $ do
  m <- MaybeT (asks (.metrics))
  p <- readTVarIO =<< asks (.txs)
  setGauge m.queuedTxs (HashMap.size p)

-- | Update the gauge of online peers.
setPeersConnected :: (MonadIO m) => BlockT m ()
setPeersConnected = void $ runMaybeT $ do
  m <- MaybeT (asks (.metrics))
  p <- liftIO . getPeers =<< asks (.config.peerMgr)
  setGauge m.peers (length p)

-- | Record how long a successful block import took.
withBlockTiming :: (MonadIO m) => BlockT m (Either l r) -> BlockT m (Either l r)
withBlockTiming go = do
  asks (.metrics) >>= \case
    Just m -> do
      start <- round . (* 1000) <$> liftIO getPOSIXTime
      go >>= \case
        Right x -> do
          end <- round . (* 1000) <$> liftIO getPOSIXTime
          addTiming m.block (end - start)
          return (Right x)
        Left y -> return (Left y)
    Nothing -> go

-- | Configuration for a block store.
data BlockStoreConfig = BlockStoreConfig
  { -- | secp256k1 context
    ctx :: !Ctx,
    -- | peer manager from running node
    peerMgr :: !PeerMgr,
    -- | chain from a running node
    chain :: !Chain,
    -- | listener for store events
    pub :: !(Publisher StoreEvent),
    -- | RocksDB database handle
    db :: !DatabaseReader,
    -- | network constants
    net :: !Network,
    -- | do not index new mempool transactions
    noMempool :: !Bool,
    -- | wipe mempool at start
    wipeMempool :: !Bool,
    -- | sync mempool from peers
    syncMempool :: !Bool,
    -- | blocks to request from the syncing peer in one batch
    blockWindow :: !Int,
    -- | delete transactions older than this number of days from mempool
    mempoolTimeout :: !Int,
    -- | disconnect syncing peer if inactive for this long
    peerTimeout :: !NominalDiffTime,
    -- | StatsD client, if metrics are enabled
    stats :: !(Maybe Stats),
    -- | log messages here
    logger :: !Logger
  }

-- | Monad transformer of the block store process.
type BlockT m = ReaderT BlockStore m

-- | Log a message with the logger of the block store.
logDebug, logInfo, logWarn, logError ::
  (MonadIO m, MonadReader BlockStore m) => Text -> m ()
logDebug = blockLog LevelDebug
logInfo = blockLog LevelInfo
logWarn = blockLog LevelWarn
logError = blockLog LevelError

-- | Log a message at a level with the logger of the block store.
blockLog :: (MonadIO m, MonadReader BlockStore m) => LogLevel -> Text -> m ()
blockLog l msg = asks (.config.logger) >>= \lg -> logAt l lg msg

-- | Run an import against the database, returning the import error if it fails.
runImport ::
  (MonadIO m) =>
  Network ->
  Ctx ->
  ImportM a ->
  BlockT m (Either ImportException a)
runImport net ctx f = fmap fst <$> runImportMempool net ctx f

-- | Like 'runImport', but also return how the import changed the mempool.
-- The mempool gauge is kept up to date from those changes instead of
-- counting the whole mempool on every clock tick.
runImportMempool ::
  (MonadIO m) =>
  Network ->
  Ctx ->
  ImportM a ->
  BlockT m (Either ImportException (a, MempoolChanges))
runImportMempool net ctx f = do
  r <- ReaderT $ \r ->
    liftIO . try $ runWriterMempool net ctx r.config.db r.config.logger f
  forM_ r $ \(_, c) -> asks (.metrics) >>= mapM_ (\m -> do
    incrementGauge m.mempool (length c.added)
    decrementGauge m.mempool (length c.removed))
  return r

-- | Run an import and publish the transactions that it added to the
-- mempool or removed from it, then the new best block if there is one.
-- Transactions of the block are not published.
runImportNotify ::
  (MonadIO m) =>
  Maybe Block ->
  ImportM a ->
  BlockT m (Either ImportException a)
runImportNotify block f = do
  net <- getNetwork
  ctx <- getCtx
  runImportMempool net ctx f >>= \case
    Left e -> return (Left e)
    Right (x, c) -> do
      l <- asks (.config.pub)
      forM_ (filter notInBlock c.removed) $ \h ->
        publish (StoreMempoolDelete h) l
      forM_ (filter notInBlock c.added) $ \h ->
        publish (StoreMempoolNew h) l
      forM_ block $ \b ->
        publish (StoreBestBlock (headerHash b.header)) l
      return (Right x)
  where
    inBlock = maybe HashSet.empty (HashSet.fromList . map txHash . (.txs)) block
    notInBlock = not . (`HashSet.member` inBlock)

-- | Run a database query from the block store.
runRocksDB :: ReaderT DatabaseReader m a -> BlockT m a
runRocksDB f =
  ReaderT $ runReaderT f . (.config.db)

instance (MonadIO m) => StoreReadBase (BlockT m) where
  getCtx =
    asks (.config.ctx)
  getNetwork =
    runRocksDB getNetwork
  getBestBlock =
    runRocksDB getBestBlock
  getBlocksAtHeight =
    runRocksDB . getBlocksAtHeight
  getBlock =
    runRocksDB . getBlock
  getTxData =
    runRocksDB . getTxData
  getManyTxData =
    runRocksDB . getManyTxData
  getSpender =
    runRocksDB . getSpender
  getUnspent =
    runRocksDB . getUnspent
  getBalance =
    runRocksDB . getBalance
  getMempool =
    runRocksDB getMempool
  getMempoolPage =
    runRocksDB . getMempoolPage

instance (MonadUnliftIO m) => StoreReadExtra (BlockT m) where
  getMaxGap =
    runRocksDB getMaxGap
  getInitialGap =
    runRocksDB getInitialGap
  getAddressesTxs as =
    runRocksDB . getAddressesTxs as
  getAddressesUnspents as =
    runRocksDB . getAddressesUnspents as
  getAddressUnspents a =
    runRocksDB . getAddressUnspents a
  getAddressTxs a =
    runRocksDB . getAddressTxs a
  getNumTxData =
    runRocksDB . getNumTxData
  getBalances =
    runRocksDB . getBalances
  xPubBals =
    runRocksDB . xPubBals
  xPubUnspents x l =
    runRocksDB . xPubUnspents x l
  xPubTxs x l =
    runRocksDB . xPubTxs x l
  xPubTxCount x =
    runRocksDB . xPubTxCount x

-- | Run block store process.
withBlockStore ::
  (MonadUnliftIO m) =>
  BlockStoreConfig ->
  (BlockStore -> m a) ->
  m a
withBlockStore cfg action = do
  pb <- newTVarIO Nothing
  ts <- newTVarIO HashMap.empty
  rq <- newTVarIO HashSet.empty
  sy <- newTVarIO HashSet.empty
  inbox <- newInbox
  metrics <- newStoreMetrics cfg
  let r =
        BlockStore
          { mailbox = inboxToMailbox inbox,
            config = cfg,
            peer = pb,
            txs = ts,
            requested = rq,
            metrics = metrics,
            syncedMempool = sy
          }
  withAsync (runReaderT (go inbox) r) $ \a -> do
    link a
    action r
  where
    go inbox = do
      ini
      wipe
      run inbox
    del txs = do
      logAt LevelInfo cfg.logger $
        "Deleting " <> cs (show (length txs)) <> " transactions"
      forM_ txs $ \(_, th) -> deleteUnconfirmedTx False th
    wipe_it txs = do
      net <- getNetwork
      ctx <- getCtx
      let (txs1, txs2) = splitAt 1000 txs
      unless (null txs1) $
        runImport net ctx (del txs1) >>= \case
          Left e -> do
            logError $
              "Could not wipe mempool: " <> cs (show e)
            throwIO e
          Right () -> wipe_it txs2
    wipe
      | cfg.wipeMempool =
          getMempool >>= wipe_it
      | otherwise =
          return ()
    ini = do
      net <- getNetwork
      ctx <- getCtx
      runImport net ctx initBest >>= \case
        Left e -> do
          logError $
            "Could not initialize: " <> cs (show e)
          throwIO e
        Right () -> return ()
    run inbox =
      withAsync (pingMe (inboxToMailbox inbox)) $ \a ->
        link a >> runBlockStoreLoop inbox

-- | Handle messages forever.
runBlockStoreLoop ::
  (InChan mbox, MonadUnliftIO m) =>
  mbox BlockStoreMessage ->
  ReaderT BlockStore m b
runBlockStoreLoop inbox =
  loopForever $ do
    logDebug "Waiting for new event..."
    msg <- receive inbox
    ReaderT $ runReaderT $ processBlockStoreMessage msg

-- | Whether the best block in the database is the best header of the node. If
-- so, release the syncing peer.
isInSync :: (MonadIO m) => BlockT m Bool
isInSync =
  getBestBlock >>= \case
    Nothing -> do
      logError "Block database uninitialized"
      throwIO Uninitialized
    Just bb -> do
      cb <- asks (.config.chain) >>= liftIO . chainGetBest
      if headerHash cb.header == bb
        then clearSyncingState >> return True
        else return False

-- | Run an action only if the database is in sync.
guardSync :: (MonadIO m) => BlockT m () -> BlockT m ()
guardSync f = isInSync >>= \s -> when s f

-- | Run an action only if the mempool is indexed.
guardMempool :: (Monad m) => BlockT m () -> BlockT m ()
guardMempool f = asks (.config.noMempool) >>= \n -> unless n f

-- | Ask a peer for its mempool, once per peer, if in sync and mempool sync is enabled.
syncMempool :: (MonadUnliftIO m) => Peer -> BlockT m ()
syncMempool p =
  guardMempool . guardSync $ do
    s <- asks (.config.syncMempool)
    m <- asks (.syncedMempool)
    y <- not . HashSet.member p.label <$> readTVarIO m
    when (s && y) $ do
      logDebug $
        "Requesting mempool from peer: " <> p.label
      atomically . modifyTVar m $ HashSet.insert p.label
      liftIO $ MMempool `sendMessage` p

-- | Import a block from the syncing peer, then ask for more blocks or, if now in
-- sync, sync the mempool. Disconnects the peer if it is not the syncing peer, if
-- the block is unknown to the node, or if the block cannot be imported.
processBlock ::
  (MonadUnliftIO m) =>
  Peer ->
  Block ->
  BlockT m ()
processBlock peer block = void . runMaybeT $ do
  checkPeer peer >>= \case
    True -> return ()
    False -> do
      logError $
        "Non-syncing peer "
          <> peer.label
          <> " sent block "
          <> blockHashToHex blockhash
      killPeer peer
      mzero
  node <-
    getBlockNode blockhash >>= \case
      Just b -> return b
      Nothing -> do
        logError $
          "Peer "
            <> peer.label
            <> " sent unknown block "
            <> blockHashToHex blockhash
        killPeer peer
        mzero
  logDebug $
    "Processing block "
      <> blockText node Nothing
      <> " from peer "
      <> peer.label
  lift $ do
    ret <- withBlockTiming $ runImportNotify (Just block) (importBlock block node)
    case ret of
      Left e -> failure e
      Right _ -> success node
  where
    header = block.header
    blockhash = headerHash header
    hexhash = blockHashToHex blockhash
    success node = do
      logInfo $
        "Best block: " <> blockText node (Just block)
      removeSyncingBlock $ headerHash node.header
      touchPeer
      isInSync >>= \case
        False -> syncMe
        True -> do
          updateOrphans
          syncMempool peer
    failure e = do
      logError $
        "Error importing block "
          <> hexhash
          <> " from peer: "
          <> peer.label
          <> ": "
          <> cs (show e)
      killPeer peer

-- | Set the blocks requested from the syncing peer.
setSyncingBlocks ::
  (MonadReader BlockStore m, MonadIO m) =>
  [BlockHash] ->
  m ()
setSyncingBlocks hs =
  asks (.peer) >>= \box ->
    atomically $
      modifyTVar box $ \case
        Nothing -> Nothing
        Just x -> Just (x :: Syncing) {blocks = hs}

-- | Blocks requested from the syncing peer and not yet imported.
getSyncingBlocks :: (MonadReader BlockStore m, MonadIO m) => m [BlockHash]
getSyncingBlocks =
  asks (.peer) >>= readTVarIO >>= \case
    Nothing -> return []
    Just x -> return x.blocks

-- | Add blocks to those requested from the syncing peer.
addSyncingBlocks ::
  (MonadReader BlockStore m, MonadIO m) =>
  [BlockHash] ->
  m ()
addSyncingBlocks hs =
  asks (.peer) >>= \box ->
    atomically $
      modifyTVar box $ \case
        Nothing -> Nothing
        Just x -> Just (x :: Syncing) {blocks = x.blocks <> hs}

-- | Remove a block from those requested from the syncing peer.
removeSyncingBlock ::
  (MonadReader BlockStore m, MonadIO m) =>
  BlockHash ->
  m ()
removeSyncingBlock h = do
  box <- asks (.peer)
  atomically $
    modifyTVar box $ \case
      Nothing -> Nothing
      Just x -> Just (x :: Syncing) {blocks = delete h x.blocks}

-- | Whether a peer is the syncing peer.
checkPeer :: (MonadIO m, MonadReader BlockStore m) => Peer -> m Bool
checkPeer p = fmap (fromMaybe False) <$> runMaybeT $ do
  Syncing {peer} <- MaybeT getSyncingState
  return $ peer == p

-- | Header of a block from the node.
getBlockNode ::
  (MonadIO m, MonadReader BlockStore m) =>
  BlockHash ->
  m (Maybe BlockNode)
getBlockNode blockhash = do
  ch <- asks (.config.chain)
  liftIO $ chainGetBlock ch blockhash

-- | Disconnect a peer that could not find requested blocks.
processNoBlocks ::
  (MonadIO m) =>
  Peer ->
  [BlockHash] ->
  BlockT m ()
processNoBlocks p hs = do
  forM_ (zip [(1 :: Int) ..] hs) $ \(i, h) ->
    logError $
      "Block "
        <> cs (show i)
        <> "/"
        <> cs (show (length hs))
        <> " "
        <> blockHashToHex h
        <> " not found by peer: "
        <> p.label
  killPeer p

-- | Queue a transaction received from a peer for import.
processTx :: (MonadIO m) => Peer -> Tx -> BlockT m ()
processTx p tx = guardMempool $ do
  t <- liftIO getCurrentTime
  logDebug $
    "Received tx "
      <> txHashToHex (txHash tx)
      <> " by peer: "
      <> p.label
  addPendingTx $ PendingTx t tx HashSet.empty

-- | Drop queued transactions received more than an hour ago.
pruneOrphans :: (MonadIO m) => BlockT m ()
pruneOrphans = guardMempool $ do
  ts <- asks (.txs)
  now <- liftIO getCurrentTime
  -- keep pending transactions received within the last hour, drop older ones
  atomically . modifyTVar' ts . HashMap.filter $ \p ->
    now `diffUTCTime` p.time <= 3600

-- | Queue a transaction for import.
addPendingTx :: (MonadIO m) => PendingTx -> BlockT m ()
addPendingTx p = do
  ts <- asks (.txs)
  rq <- asks (.requested)
  atomically $ do
    modifyTVar ts $ HashMap.insert th p
    modifyTVar rq $ HashSet.delete th
  setPendingTxs
  where
    th = txHash p.tx

-- | Remember for twenty seconds that a transaction was requested, so that it is not requested again.
addRequestedTx :: (MonadIO m) => TxHash -> BlockT m ()
addRequestedTx th = do
  qbox <- asks (.requested)
  atomically $ modifyTVar qbox $ HashSet.insert th
  liftIO . void . async $ do
    threadDelay 20000000
    atomically $ modifyTVar qbox $ HashSet.delete th

-- | Whether a transaction is queued or was requested recently.
isPending :: (MonadIO m) => TxHash -> BlockT m Bool
isPending th = do
  tbox <- asks (.txs)
  qbox <- asks (.requested)
  atomically $ do
    ts <- readTVar tbox
    rs <- readTVar qbox
    return $
      th `HashMap.member` ts
        || th `HashSet.member` rs

-- | Take up to this many queued transactions that have no missing parents, in
-- dependency order, out of the queue.
pendingTxs :: (MonadIO m) => Int -> BlockT m [PendingTx]
pendingTxs i = do
  selected <-
    asks (.txs) >>= \box -> atomically $ do
      pending <- readTVar box
      let (selected, rest) = select pending
      writeTVar box rest
      return selected
  setPendingTxs
  return selected
  where
    select pend =
      let eligible = HashMap.filter (null . (.deps)) pend
          orphans = HashMap.difference pend eligible
          selected = take i $ sortit eligible
          remaining = HashMap.filter (`notElem` selected) eligible
       in (selected, remaining <> orphans)
    sortit m =
      let sorted = sortTxs $ map (.tx) $ HashMap.elems m
          txids = map (txHash . snd) sorted
       in mapMaybe (`HashMap.lookup` m) txids

-- | Mark a transaction as no longer missing for the queued transactions that spend it.
fulfillOrphans :: (MonadIO m) => BlockStore -> TxHash -> m ()
fulfillOrphans block_read th =
  atomically $ modifyTVar box (HashMap.map fulfill)
  where
    box = block_read.txs
    fulfill p = p {deps = HashSet.delete th p.deps}

-- | Drop queued transactions that are already in the database, and recompute the
-- missing parents of the others.
updateOrphans ::
  ( StoreReadBase m,
    MonadIO m,
    MonadReader BlockStore m
  ) =>
  m ()
updateOrphans = do
  box <- asks (.txs)
  pending <- readTVarIO box
  let orphans = HashMap.filter (not . null . (.deps)) pending
  updated <- forM orphans $ \p -> do
    let tx = p.tx
    exists (txHash tx) >>= \case
      True -> return Nothing
      False -> Just <$> fill_deps p
  let pruned = HashMap.map fromJust $ HashMap.filter isJust updated
  atomically $ writeTVar box pruned
  where
    exists th =
      getTxData th >>= \case
        Nothing -> return False
        Just TxData {deleted = True} -> return False
        Just TxData {deleted = False} -> return True
    prev_utxos tx = catMaybes <$> mapM (getUnspent . (.outpoint)) tx.inputs
    fulfill p unspent =
      let unspent_hash = unspent.outpoint.hash
          new_deps = HashSet.delete unspent_hash p.deps
       in p {deps = new_deps}
    fill_deps p = do
      let tx = p.tx
      unspents <- prev_utxos tx
      return $ foldl fulfill p unspents

-- | Queue a transaction that spends unknown outputs, recording which parents are missing.
newOrphanTx ::
  (MonadIO m) =>
  BlockStore ->
  UTCTime ->
  Tx ->
  WriterT m ()
newOrphanTx block_read time tx = do
  logAt LevelDebug block_read.config.logger $
    "Import tx "
      <> txHashToHex (txHash tx)
      <> ": Orphan"
  let box = block_read.txs
  unspents <- catMaybes <$> mapM getUnspent prevs
  let unspent_set = HashSet.fromList (map (.outpoint) unspents)
      missing_set = HashSet.difference prev_set unspent_set
      missing_txs = HashSet.map (.hash) missing_set
  atomically . modifyTVar box $
    HashMap.insert
      (txHash tx)
      PendingTx
        { time = time,
          tx = tx,
          deps = missing_txs
        }
  where
    prev_set = HashSet.fromList prevs
    prevs = map (.outpoint) tx.inputs

-- | Import a transaction into the mempool. Returns 'True' if imported. A
-- transaction that spends unknown outputs is queued as an orphan; other errors
-- are ignored.
importMempoolTx :: BlockStore -> UTCTime -> Tx -> ImportM Bool
importMempoolTx block_read time tx =
  catch new_mempool_tx handle_error
  where
    tx_hash = txHash tx
    handle_error :: ImportException -> ImportM Bool
    handle_error Orphan = do
      newOrphanTx block_read time tx
      return False
    handle_error _ = return False
    seconds = floor (utcTimeToPOSIXSeconds time)
    new_mempool_tx = do
      t <- newMempoolTx tx seconds
      logAt LevelInfo block_read.config.logger $
        bool "Already have" "Imported" t
          <> " tx "
          <> txHashToHex (txHash tx)
      when t $ fulfillOrphans block_read tx_hash
      return t

-- | Import up to 2000 queued transactions.
processMempool :: (MonadIO m) => BlockT m ()
processMempool = guardMempool $ do
  txs <- pendingTxs 2000
  block_read <- ask
  unless (null txs) $ import_txs block_read txs
  where
    run_import block_read p =
      importMempoolTx block_read p.time p.tx
    import_txs block_read txs =
      runImportNotify Nothing (mapM (run_import block_read) txs) >>= \case
        Left e -> report_error e
        Right _ -> return ()
    report_error e = do
      logError $
        "Error processing mempool: " <> cs (show e)
      throwIO e

-- | Delete up to 1000 mempool transactions older than the configured number of days.
pruneMempool :: (MonadIO m) => BlockT m ()
pruneMempool =
  guardMempool $ do
    days <- asks (.config.mempoolTimeout)
    when (days > 0) $ do
      time <- (floor . utcTimeToPOSIXSeconds) <$> liftIO getCurrentTime
      let thresh = time - (fromIntegral days * 24 * 60 * 60)
      -- runs on every clock tick: read only the expired entries, oldest
      -- first, from the index by time
      db <- asks (.config.db)
      txs <- map snd <$> liftIO (getMempoolBefore db thresh 1000)
      unless (null txs) $ do
        logInfo $
          "Deleting " <> cs (show (length txs)) <> " old transactions from mempool"
        runImportNotify Nothing (mapM_ (deleteUnconfirmedTx False) txs) >>= \case
          Left e ->
            logError $
              "Error pruning mempool: " <> cs (show e)
          Right () -> return ()

-- | Request from a peer the announced transactions that are neither queued,
-- recently requested nor in the database. Only while in sync.
processTxs ::
  (MonadIO m) =>
  Peer ->
  [TxHash] ->
  BlockT m ()
processTxs p hs = guardMempool . guardSync $ do
  logDebug $
    "Received inventory with "
      <> cs (show (length hs))
      <> " transactions from peer: "
      <> p.label
  xs <- catMaybes <$> zip_counter process_tx
  unless (null xs) $ go xs
  where
    len = length hs
    zip_counter = forM (zip [(1 :: Int) ..] hs) . uncurry
    process_tx i h =
      isPending h >>= \case
        True -> do
          logDebug $
            "Tx "
              <> cs (show i)
              <> "/"
              <> cs (show len)
              <> " "
              <> txHashToHex h
              <> ": "
              <> "Already Pending"
          return Nothing
        False ->
          getTxData h >>= \case
            Just _ -> do
              logDebug $
                "Tx "
                  <> cs (show i)
                  <> "/"
                  <> cs (show len)
                  <> " "
                  <> txHashToHex h
                  <> ": "
                  <> "Already Imported"
              return Nothing
            Nothing -> do
              logDebug $
                "Tx "
                  <> cs (show i)
                  <> "/"
                  <> cs (show len)
                  <> " "
                  <> txHashToHex h
                  <> ": "
                  <> "Requesting"
              return (Just h)
    go xs = do
      mapM_ addRequestedTx xs
      net <- asks (.config.net)
      let inv = if net.segWit then InvWitnessTx else InvTx
          vec = map (InvVector inv . (.get)) xs
          msg = MGetData (GetData vec)
      liftIO $ msg `sendMessage` p

-- | Record that the syncing peer just delivered something.
touchPeer ::
  ( MonadIO m,
    MonadReader BlockStore m
  ) =>
  m ()
touchPeer =
  getSyncingState >>= \case
    Nothing -> return ()
    Just _ -> do
      box <- asks (.peer)
      now <- liftIO getCurrentTime
      atomically $
        modifyTVar box $
          fmap $
            \x -> (x :: Syncing) {time = now}

-- | Disconnect the syncing peer if it has been silent for too long.
checkTime :: (MonadIO m) => BlockT m ()
checkTime =
  asks (.peer) >>= readTVarIO >>= \case
    Nothing -> return ()
    Just
      Syncing
        { time = t,
          peer = p
        } -> do
        now <- liftIO getCurrentTime
        peer_time_out <- asks (.config.peerTimeout)
        when (now `diffUTCTime` t > peer_time_out) $ do
          logError $
            "Syncing peer timeout: " <> p.label
          killPeer p

-- | Revert blocks until the best block in the database is in the main chain of headers.
revertToMainChain :: (MonadIO m) => BlockT m ()
revertToMainChain = do
  h <- headerHash . (.header) <$> getBest
  ch <- asks (.config.chain)
  net <- getNetwork
  ctx <- getCtx
  liftIO (chainBlockMain ch h) >>= \x -> unless x $ do
    logWarn $
      "Reverting best block: "
        <> blockHashToHex h
    runImport net ctx (revertBlock h) >>= \case
      Left e -> do
        logError $
          "Could not revert block "
            <> blockHashToHex h
            <> ": "
            <> cs (show e)
        throwIO e
      Right () -> setSyncingBlocks []
    revertToMainChain

-- | Header of the best block in the database.
getBest :: (MonadIO m) => BlockT m BlockNode
getBest = do
  bb <-
    getBestBlock >>= \case
      Just b -> return b
      Nothing -> do
        logError "No best block set"
        throwIO Uninitialized
  ch <- asks (.config.chain)
  liftIO (chainGetBlock ch bb) >>= \case
    Just x -> return x
    Nothing -> do
      logError $
        "Header not found for best block: "
          <> blockHashToHex bb
      throwIO (BlockNotInChain bb)

-- | Header of the last block requested from the syncing peer, or of the best block if none.
getSyncBest :: (MonadIO m) => BlockT m BlockNode
getSyncBest = do
  bb <-
    getSyncingBlocks >>= \case
      [] ->
        getBestBlock >>= \case
          Just b -> return b
          Nothing -> do
            logError "No best block set"
            throwIO Uninitialized
      hs -> return $ last hs
  ch <- asks (.config.chain)
  liftIO (chainGetBlock ch bb) >>= \case
    Just x -> return x
    Nothing -> do
      logError $
        "Header not found for block: "
          <> blockHashToHex bb
      throwIO (BlockNotInChain bb)

-- | The syncing peer, when it is time to ask it for the next batch of blocks.
shouldSync :: (MonadIO m) => BlockT m (Maybe Peer)
shouldSync =
  isInSync >>= \case
    True -> return Nothing
    False ->
      getSyncingState >>= \case
        Nothing -> return Nothing
        Just Syncing {peer = p, blocks = bs} -> do
          window <- blockWindow
          return $
            if needsMoreBlocks window (length bs)
              then Just p
              else Nothing

-- | Blocks to request from the syncing peer in one batch. Never below one,
-- or the store would never ask for any block.
blockWindow :: (Monad m) => BlockT m Int
blockWindow = asks (max 1 . (.config.blockWindow))

-- | Whether to ask for the next batch, given the window size and how many
-- requested blocks are still waiting to be imported. Asking once fewer than
-- a quarter of the window remain keeps the peer busy, while the queue of
-- downloaded blocks never grows much beyond one window. The threshold is at
-- least one, so that a window smaller than four still refills once empty.
needsMoreBlocks :: Int -> Int -> Bool
needsMoreBlocks window outstanding =
  outstanding < max 1 (window `div` 4)

-- | Ask the syncing peer for the next batch of blocks, if the window has room.
syncMe :: (MonadIO m) => BlockT m ()
syncMe = do
  revertToMainChain
  shouldSync >>= \case
    Nothing -> return ()
    Just p -> do
      bb <- getSyncBest
      bh <- getbh
      when (bb /= bh) $ do
        bns <- sel bb bh
        iv <- getiv bns
        logDebug $
          "Requesting "
            <> fromString (show (length iv))
            <> " blocks from peer: "
            <> p.label
        addSyncingBlocks $ map (headerHash . (.header)) bns
        liftIO $ MGetData (GetData iv) `sendMessage` p
  where
    getiv bns = do
      w <- asks (.config.net.segWit)
      let i = if w then InvWitnessBlock else InvBlock
          f = InvVector i . (.get) . headerHash . (.header)
      return $ map f bns
    getbh =
      liftIO . chainGetBest =<< asks (.config.chain)
    -- The blocks right after the last requested one, up to one window or
    -- the chain tip, whichever comes first.
    sel bb bh = do
      window <- blockWindow
      top <- findAncestor (min (bb.height + fromIntegral window) bh.height) bh
      ch <- asks (.config.chain)
      ps <- liftIO $ chainGetParents ch (bb.height + 1) top
      return $ ps <> [top]

-- | Ancestor of a header at a height. Throws 'AncestorNotInChain' if missing.
findAncestor ::
  (MonadIO m, MonadReader BlockStore m) =>
  BlockHeight ->
  BlockNode ->
  m BlockNode
findAncestor height target = do
  ch <- asks (.config.chain)
  liftIO (chainGetAncestor ch height target) >>= \case
    Just ancestor -> return ancestor
    Nothing -> do
      let h = headerHash target.header
      logError $
        "Could not find header for ancestor of block "
          <> blockHashToHex h
          <> " at height "
          <> cs (show target.height)
      throwIO $ AncestorNotInChain height h

-- | Forget a peer: release it if it was the syncing peer, and forget that its mempool was requested.
finishPeer ::
  (MonadIO m, MonadReader BlockStore m) =>
  Peer ->
  m ()
finishPeer p = do
  box <- asks (.peer)
  readTVarIO box >>= \case
    Just Syncing {peer = p'} | p == p' -> reset_it box
    _ -> return ()
  y <- asks (.syncedMempool)
  atomically $ modifyTVar y $ HashSet.delete p.label
  where
    reset_it box = do
      atomically $ writeTVar box Nothing
      logDebug $ "Releasing peer: " <> p.label
      liftIO $ setFree p

-- | Make a peer the syncing peer if there is none and the peer is not busy.
trySetPeer :: (MonadIO m) => Peer -> BlockT m Bool
trySetPeer p =
  getSyncingState >>= \case
    Just _ -> return False
    Nothing -> set_it
  where
    set_it =
      liftIO (setBusy p) >>= \case
        False -> return False
        True -> do
          logDebug $
            "Locked peer: " <> p.label
          box <- asks (.peer)
          now <- liftIO getCurrentTime
          atomically . writeTVar box $
            Just
              Syncing
                { peer = p,
                  time = now,
                  blocks = []
                }
          return True

-- | If not in sync and there is no syncing peer, pick the fastest free peer and start syncing.
trySyncing :: (MonadIO m) => BlockT m ()
trySyncing =
  isInSync >>= \case
    True -> return ()
    False ->
      getSyncingState >>= \case
        Just _ -> return ()
        Nothing -> online_peer
  where
    recurse [] = return ()
    recurse (p : ps) =
      trySetPeer p >>= \case
        False -> recurse ps
        True -> syncMe
    online_peer = do
      ops <- liftIO . getPeers =<< asks (.config.peerMgr)
      let ps = map (.mailbox) ops
      recurse ps

-- | Start syncing against a newly connected peer if needed, or request its mempool if in sync.
trySyncingPeer :: (MonadUnliftIO m) => Peer -> BlockT m ()
trySyncingPeer p =
  isInSync >>= \case
    True -> syncMempool p
    False ->
      trySetPeer p >>= \case
        False -> return ()
        True -> syncMe

-- | State of the sync against the syncing peer, if any.
getSyncingState ::
  (MonadIO m, MonadReader BlockStore m) => m (Maybe Syncing)
getSyncingState =
  readTVarIO =<< asks (.peer)

-- | Release the syncing peer, if any.
clearSyncingState ::
  (MonadIO m, MonadReader BlockStore m) => m ()
clearSyncingState =
  asks (.peer) >>= readTVarIO >>= \case
    Nothing -> return ()
    Just Syncing {peer = p} -> finishPeer p

-- | Handle a message sent to the block store.
processBlockStoreMessage ::
  (MonadUnliftIO m) =>
  BlockStoreMessage ->
  BlockT m ()
processBlockStoreMessage (BlockNewBest node) = do
  logDebug $
    "New best block mined at height "
      <> cs (show node.height)
      <> ": "
      <> blockHashToHex (headerHash node.header)
  trySyncing
processBlockStoreMessage (BlockPeerConnect p) = do
  logDebug $
    "New peer connected: " <> p.label
  trySyncingPeer p
processBlockStoreMessage (BlockPeerDisconnect p) = do
  logDebug $
    "Peer disconnected: " <> p.label
  finishPeer p
processBlockStoreMessage (BlockReceived p b) = do
  logDebug $
    "Received block: "
      <> blockHashToHex (headerHash b.header)
  processBlock p b
processBlockStoreMessage (BlockNotFound p bs) = do
  logDebug $
    "Blocks not found by peer "
      <> p.label
      <> ": "
      <> T.unwords (map blockHashToHex bs)
  processNoBlocks p bs
processBlockStoreMessage (TxRefReceived p tx) = do
  logDebug $
    "Transaction received from peer "
      <> p.label
      <> ": "
      <> txHashToHex (txHash tx)
  processTx p tx
processBlockStoreMessage (TxRefAvailable p ts) = do
  logDebug $
    "Transactions available from peer "
      <> p.label
      <> ": "
      <> T.unwords (map txHashToHex ts)
  processTxs p ts
processBlockStoreMessage (BlockPing r) = do
  logDebug "Internal clock event"
  setStoreHeight
  setHeadersHeight
  setPendingTxs
  setPeersConnected
  trySyncing
  processMempool
  pruneOrphans
  pruneMempool
  checkTime
  atomically (r ())

-- | Send a clock tick to the block store every 0.1 to 1 second, waiting for each to be handled.
pingMe :: (MonadIO m) => Mailbox BlockStoreMessage -> m ()
pingMe mbox =
  loopForever $ do
    BlockPing `query` mbox
    delay <- liftIO $ randomRIO (100 * 1000, 1000 * 1000)
    threadDelay delay

-- | Tell the block store that a peer connected.
blockStorePeerConnect :: (MonadIO m) => Peer -> BlockStore -> m ()
blockStorePeerConnect peer store =
  BlockPeerConnect peer `send` store.mailbox

-- | Tell the block store that a peer disconnected.
blockStorePeerDisconnect ::
  (MonadIO m) => Peer -> BlockStore -> m ()
blockStorePeerDisconnect peer store =
  BlockPeerDisconnect peer `send` store.mailbox

-- | Tell the block store that the node has a new best header.
blockStoreHead ::
  (MonadIO m) => BlockNode -> BlockStore -> m ()
blockStoreHead node store =
  BlockNewBest node `send` store.mailbox

-- | Give the block store a block received from a peer.
blockStoreBlock ::
  (MonadIO m) => Peer -> Block -> BlockStore -> m ()
blockStoreBlock peer block store =
  BlockReceived peer block `send` store.mailbox

-- | Tell the block store that a peer could not find blocks.
blockStoreNotFound ::
  (MonadIO m) => Peer -> [BlockHash] -> BlockStore -> m ()
blockStoreNotFound peer blocks store =
  BlockNotFound peer blocks `send` store.mailbox

-- | Give the block store a transaction received from a peer.
blockStoreTx ::
  (MonadIO m) => Peer -> Tx -> BlockStore -> m ()
blockStoreTx peer tx store =
  TxRefReceived peer tx `send` store.mailbox

-- | Tell the block store that a peer announced transactions.
blockStoreTxHash ::
  (MonadIO m) => Peer -> [TxHash] -> BlockStore -> m ()
blockStoreTxHash peer txhashes store =
  TxRefAvailable peer txhashes `send` store.mailbox

-- | 'STM' version of 'blockStorePeerConnect'.
blockStorePeerConnectSTM ::
  Peer -> BlockStore -> STM ()
blockStorePeerConnectSTM peer store =
  BlockPeerConnect peer `sendSTM` store.mailbox

-- | 'STM' version of 'blockStorePeerDisconnect'.
blockStorePeerDisconnectSTM ::
  Peer -> BlockStore -> STM ()
blockStorePeerDisconnectSTM peer store =
  BlockPeerDisconnect peer `sendSTM` store.mailbox

-- | 'STM' version of 'blockStoreHead'.
blockStoreHeadSTM ::
  BlockNode -> BlockStore -> STM ()
blockStoreHeadSTM node store =
  BlockNewBest node `sendSTM` store.mailbox

-- | 'STM' version of 'blockStoreBlock'.
blockStoreBlockSTM ::
  Peer -> Block -> BlockStore -> STM ()
blockStoreBlockSTM peer block store =
  BlockReceived peer block `sendSTM` store.mailbox

-- | 'STM' version of 'blockStoreNotFound'.
blockStoreNotFoundSTM ::
  Peer -> [BlockHash] -> BlockStore -> STM ()
blockStoreNotFoundSTM peer blocks store =
  BlockNotFound peer blocks `sendSTM` store.mailbox

-- | 'STM' version of 'blockStoreTx'.
blockStoreTxSTM ::
  Peer -> Tx -> BlockStore -> STM ()
blockStoreTxSTM peer tx store =
  TxRefReceived peer tx `sendSTM` store.mailbox

-- | 'STM' version of 'blockStoreTxHash'.
blockStoreTxHashSTM ::
  Peer -> [TxHash] -> BlockStore -> STM ()
blockStoreTxHashSTM peer txhashes store =
  TxRefAvailable peer txhashes `sendSTM` store.mailbox

-- | Number of transactions queued or requested by the block store.
blockStorePendingTxs ::
  (MonadIO m) => BlockStore -> m Int
blockStorePendingTxs =
  atomically . blockStorePendingTxsSTM

-- | 'STM' version of 'blockStorePendingTxs'.
blockStorePendingTxsSTM ::
  BlockStore -> STM Int
blockStorePendingTxsSTM BlockStore {..} = do
  x <- HashMap.keysSet <$> readTVar txs
  y <- readTVar requested
  return $ HashSet.size $ x `HashSet.union` y

-- | Describe a block for logs: height, time, hash and, if the block is given, size.
blockText :: BlockNode -> Maybe Block -> Text
blockText bn mblock = case mblock of
  Nothing ->
    height <> sep <> t <> sep <> hash
  Just block ->
    height <> sep <> t <> sep <> hash <> sep <> size block
  where
    height = cs $ show bn.height
    b = posixSecondsToUTCTime $ fromIntegral bn.header.timestamp
    t = cs $ formatTime defaultTimeLocale "%FT%T" b
    hash = blockHashToHex (headerHash bn.header)
    sep = " | "
    size = (<> " bytes") . cs . show . B.length . encode
