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

data BlockStoreMessage
  = BlockNewBest !BlockNode
  | BlockPeerConnect !Peer
  | BlockPeerDisconnect !Peer
  | BlockReceived !Peer !Block
  | BlockNotFound !Peer ![BlockHash]
  | TxRefReceived !Peer !Tx
  | TxRefAvailable !Peer ![TxHash]
  | BlockPing !(Listen ())

data BlockException
  = BlockNotInChain !BlockHash
  | Uninitialized
  | CorruptDatabase
  | AncestorNotInChain !BlockHeight !BlockHash
  | MempoolImportFailed
  deriving (Show, Eq, Ord, Exception)

data Syncing = Syncing
  { peer :: !Peer,
    time :: !UTCTime,
    blocks :: ![BlockHash]
  }

data PendingTx = PendingTx
  { time :: !UTCTime,
    tx :: !Tx,
    deps :: !(HashSet TxHash)
  }
  deriving (Show, Eq, Ord)

-- | Block store process state.
data BlockStore = BlockStore
  { mailbox :: !(Mailbox BlockStoreMessage),
    config :: !BlockStoreConfig,
    peer :: !(TVar (Maybe Syncing)),
    txs :: !(TVar (HashMap TxHash PendingTx)),
    requested :: !(TVar (HashSet TxHash)),
    metrics :: !(Maybe StoreMetrics),
    syncedMempool :: !(TVar (HashSet Text))
  }

data StoreMetrics = StoreMetrics
  { blocks :: !StatGauge,
    headers :: !StatGauge,
    queuedTxs :: !StatGauge,
    peers :: !StatGauge,
    mempool :: !StatGauge,
    block :: !StatTiming
  }

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

setStoreHeight :: (MonadIO m) => BlockT m ()
setStoreHeight = void $ runMaybeT $ do
  m <- MaybeT (asks (.metrics))
  h <- MaybeT getBestBlock
  b <- MaybeT (getBlock h)
  setGauge m.blocks (fromIntegral b.height)

setHeadersHeight :: (MonadIO m) => BlockT m ()
setHeadersHeight = void $ runMaybeT $ do
  m <- MaybeT (asks (.metrics))
  n <- liftIO . chainGetBest =<< asks (.config.chain)
  setGauge m.headers (fromIntegral n.height)

setPendingTxs :: (MonadIO m) => BlockT m ()
setPendingTxs = void $ runMaybeT $ do
  m <- MaybeT (asks (.metrics))
  p <- readTVarIO =<< asks (.txs)
  setGauge m.queuedTxs (HashMap.size p)

setPeersConnected :: (MonadIO m) => BlockT m ()
setPeersConnected = void $ runMaybeT $ do
  m <- MaybeT (asks (.metrics))
  p <- liftIO . getPeers =<< asks (.config.peerMgr)
  setGauge m.peers (length p)

setMempoolSize :: (MonadIO m) => BlockT m ()
setMempoolSize = void $ runMaybeT $ do
  m <- MaybeT (asks (.metrics))
  p <- lift getMempool
  setGauge m.mempool (length p)

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
  { ctx :: !Ctx,
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
    stats :: !(Maybe Stats),
    -- | log messages here
    logger :: !Logger
  }

type BlockT m = ReaderT BlockStore m

logDebug, logInfo, logWarn, logError ::
  (MonadIO m, MonadReader BlockStore m) => Text -> m ()
logDebug = blockLog LevelDebug
logInfo = blockLog LevelInfo
logWarn = blockLog LevelWarn
logError = blockLog LevelError

blockLog :: (MonadIO m, MonadReader BlockStore m) => LogLevel -> Text -> m ()
blockLog l msg = asks (.config.logger) >>= \lg -> logAt l lg msg

runImport ::
  (MonadIO m) =>
  Network ->
  Ctx ->
  ImportM a ->
  BlockT m (Either ImportException a)
runImport net ctx f =
  ReaderT $ \r ->
    liftIO . try $ runWriter net ctx r.config.db r.config.logger f

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
  getSpender =
    runRocksDB . getSpender
  getUnspent =
    runRocksDB . getUnspent
  getBalance =
    runRocksDB . getBalance
  getMempool =
    runRocksDB getMempool

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

runBlockStoreLoop ::
  (InChan mbox, MonadUnliftIO m) =>
  mbox BlockStoreMessage ->
  ReaderT BlockStore m b
runBlockStoreLoop inbox =
  loopForever $ do
    logDebug "Waiting for new event..."
    msg <- receive inbox
    ReaderT $ runReaderT $ processBlockStoreMessage msg

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

guardSync :: (MonadIO m) => BlockT m () -> BlockT m ()
guardSync f = isInSync >>= \s -> when s f

guardMempool :: (Monad m) => BlockT m () -> BlockT m ()
guardMempool f = asks (.config.noMempool) >>= \n -> unless n f

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
  net <- lift getNetwork
  ctx <- lift getCtx
  lift . notify (Just block) $ do
    ret <- withBlockTiming $ runImport net ctx (importBlock block node)
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

getSyncingBlocks :: (MonadReader BlockStore m, MonadIO m) => m [BlockHash]
getSyncingBlocks =
  asks (.peer) >>= readTVarIO >>= \case
    Nothing -> return []
    Just x -> return x.blocks

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

checkPeer :: (MonadIO m, MonadReader BlockStore m) => Peer -> m Bool
checkPeer p = fmap (fromMaybe False) <$> runMaybeT $ do
  Syncing {peer} <- MaybeT getSyncingState
  return $ peer == p

getBlockNode ::
  (MonadIO m, MonadReader BlockStore m) =>
  BlockHash ->
  m (Maybe BlockNode)
getBlockNode blockhash = do
  ch <- asks (.config.chain)
  liftIO $ chainGetBlock ch blockhash

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

processTx :: (MonadIO m) => Peer -> Tx -> BlockT m ()
processTx p tx = guardMempool $ do
  t <- liftIO getCurrentTime
  logDebug $
    "Received tx "
      <> txHashToHex (txHash tx)
      <> " by peer: "
      <> p.label
  addPendingTx $ PendingTx t tx HashSet.empty

pruneOrphans :: (MonadIO m) => BlockT m ()
pruneOrphans = guardMempool $ do
  ts <- asks (.txs)
  now <- liftIO getCurrentTime
  -- keep pending transactions received within the last hour, drop older ones
  atomically . modifyTVar' ts . HashMap.filter $ \p ->
    now `diffUTCTime` p.time <= 3600

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

addRequestedTx :: (MonadIO m) => TxHash -> BlockT m ()
addRequestedTx th = do
  qbox <- asks (.requested)
  atomically $ modifyTVar qbox $ HashSet.insert th
  liftIO . void . async $ do
    threadDelay 20000000
    atomically $ modifyTVar qbox $ HashSet.delete th

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

fulfillOrphans :: (MonadIO m) => BlockStore -> TxHash -> m ()
fulfillOrphans block_read th =
  atomically $ modifyTVar box (HashMap.map fulfill)
  where
    box = block_read.txs
    fulfill p = p {deps = HashSet.delete th p.deps}

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

notify :: (MonadIO m) => Maybe Block -> BlockT m a -> BlockT m a
notify block go = do
  old <- HashSet.union e . HashSet.fromList . map snd <$> getMempool
  x <- go
  new <- HashSet.union e . HashSet.fromList . map snd <$> getMempool
  l <- asks (.config.pub)
  forM_ (old `HashSet.difference` new) $ \h ->
    publish (StoreMempoolDelete h) l
  forM_ (new `HashSet.difference` old) $ \h ->
    publish (StoreMempoolNew h) l
  case block of
    Just b -> publish (StoreBestBlock (headerHash b.header)) l
    Nothing -> return ()
  return x
  where
    e = case block of
      Just b -> HashSet.fromList (map txHash b.txs)
      Nothing -> HashSet.empty

processMempool :: (MonadIO m) => BlockT m ()
processMempool = guardMempool . notify Nothing $ do
  txs <- pendingTxs 2000
  block_read <- ask
  unless (null txs) (import_txs block_read txs)
  where
    run_import block_read p =
      importMempoolTx block_read p.time p.tx
    import_txs block_read txs =
      let r = mapM (run_import block_read) txs
       in do
            net <- getNetwork
            ctx <- getCtx
            runImport net ctx r >>= \case
              Left e -> report_error e
              Right _ -> return ()
    report_error e = do
      logError $
        "Error processing mempool: " <> cs (show e)
      throwIO e

pruneMempool :: (MonadIO m) => BlockT m ()
pruneMempool =
  guardMempool . notify Nothing $ do
    days <- asks (.config.mempoolTimeout)
    when (days > 0) $ do
      mempool <- reverse <$> getMempool
      time <- (floor . utcTimeToPOSIXSeconds) <$> liftIO getCurrentTime
      let thresh = time - (fromIntegral days * 24 * 60 * 60)
          txs = take 1000 $ map snd $ takeWhile ((< thresh) . fst) mempool
      net <- getNetwork
      ctx <- getCtx
      when (length txs > 0) $ do
        logInfo $
          "Deleting " <> cs (show (length txs)) <> " old transactions from mempool"
        runImport net ctx (mapM_ (deleteUnconfirmedTx False) txs) >>= \case
          Left e ->
            logError $
              "Error pruning mempool: " <> cs (show e)
          Right () -> return ()

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

trySyncingPeer :: (MonadUnliftIO m) => Peer -> BlockT m ()
trySyncingPeer p =
  isInSync >>= \case
    True -> syncMempool p
    False ->
      trySetPeer p >>= \case
        False -> return ()
        True -> syncMe

getSyncingState ::
  (MonadIO m, MonadReader BlockStore m) => m (Maybe Syncing)
getSyncingState =
  readTVarIO =<< asks (.peer)

clearSyncingState ::
  (MonadIO m, MonadReader BlockStore m) => m ()
clearSyncingState =
  asks (.peer) >>= readTVarIO >>= \case
    Nothing -> return ()
    Just Syncing {peer = p} -> finishPeer p

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
  setMempoolSize
  trySyncing
  processMempool
  pruneOrphans
  pruneMempool
  checkTime
  atomically (r ())

pingMe :: (MonadIO m) => Mailbox BlockStoreMessage -> m ()
pingMe mbox =
  loopForever $ do
    BlockPing `query` mbox
    delay <- liftIO $ randomRIO (100 * 1000, 1000 * 1000)
    threadDelay delay

blockStorePeerConnect :: (MonadIO m) => Peer -> BlockStore -> m ()
blockStorePeerConnect peer store =
  BlockPeerConnect peer `send` store.mailbox

blockStorePeerDisconnect ::
  (MonadIO m) => Peer -> BlockStore -> m ()
blockStorePeerDisconnect peer store =
  BlockPeerDisconnect peer `send` store.mailbox

blockStoreHead ::
  (MonadIO m) => BlockNode -> BlockStore -> m ()
blockStoreHead node store =
  BlockNewBest node `send` store.mailbox

blockStoreBlock ::
  (MonadIO m) => Peer -> Block -> BlockStore -> m ()
blockStoreBlock peer block store =
  BlockReceived peer block `send` store.mailbox

blockStoreNotFound ::
  (MonadIO m) => Peer -> [BlockHash] -> BlockStore -> m ()
blockStoreNotFound peer blocks store =
  BlockNotFound peer blocks `send` store.mailbox

blockStoreTx ::
  (MonadIO m) => Peer -> Tx -> BlockStore -> m ()
blockStoreTx peer tx store =
  TxRefReceived peer tx `send` store.mailbox

blockStoreTxHash ::
  (MonadIO m) => Peer -> [TxHash] -> BlockStore -> m ()
blockStoreTxHash peer txhashes store =
  TxRefAvailable peer txhashes `send` store.mailbox

blockStorePeerConnectSTM ::
  Peer -> BlockStore -> STM ()
blockStorePeerConnectSTM peer store =
  BlockPeerConnect peer `sendSTM` store.mailbox

blockStorePeerDisconnectSTM ::
  Peer -> BlockStore -> STM ()
blockStorePeerDisconnectSTM peer store =
  BlockPeerDisconnect peer `sendSTM` store.mailbox

blockStoreHeadSTM ::
  BlockNode -> BlockStore -> STM ()
blockStoreHeadSTM node store =
  BlockNewBest node `sendSTM` store.mailbox

blockStoreBlockSTM ::
  Peer -> Block -> BlockStore -> STM ()
blockStoreBlockSTM peer block store =
  BlockReceived peer block `sendSTM` store.mailbox

blockStoreNotFoundSTM ::
  Peer -> [BlockHash] -> BlockStore -> STM ()
blockStoreNotFoundSTM peer blocks store =
  BlockNotFound peer blocks `sendSTM` store.mailbox

blockStoreTxSTM ::
  Peer -> Tx -> BlockStore -> STM ()
blockStoreTxSTM peer tx store =
  TxRefReceived peer tx `sendSTM` store.mailbox

blockStoreTxHashSTM ::
  Peer -> [TxHash] -> BlockStore -> STM ()
blockStoreTxHashSTM peer txhashes store =
  TxRefAvailable peer txhashes `sendSTM` store.mailbox

blockStorePendingTxs ::
  (MonadIO m) => BlockStore -> m Int
blockStorePendingTxs =
  atomically . blockStorePendingTxsSTM

blockStorePendingTxsSTM ::
  BlockStore -> STM Int
blockStorePendingTxsSTM BlockStore {..} = do
  x <- HashMap.keysSet <$> readTVar txs
  y <- readTVar requested
  return $ HashSet.size $ x `HashSet.union` y

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
