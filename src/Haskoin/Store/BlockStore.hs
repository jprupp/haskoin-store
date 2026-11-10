{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE RecordWildCards #-}
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
-- 'peerTimeout' is disconnected. The state of the download, and every rule
-- about it, is the pure state machine of "Haskoin.Store.BlockStore.Sync";
-- this module carries out what its transitions decide.
--
-- = Mempool
--
-- Once the database is in sync, the block store asks each peer for its mempool
-- (if 'syncMempool' is set) and requests every transaction that peers
-- announce. Received transactions are queued and imported in batches on every
-- clock tick, in dependency order. Transactions that spend unknown outputs are
-- kept for up to an hour, waiting for their parents. Mempool transactions older
-- than 'mempoolTimeout' days are deleted. The transactions on their way in,
-- requested or received, are the pure value of
-- "Haskoin.Store.BlockStore.Pending".
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
    unless,
    void,
    when,
    (>=>),
  )
import Data.Bool (bool)
import Data.ByteString qualified as B
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.Maybe
  ( catMaybes,
    isJust,
  )
import Data.Serialize (encode)
import Data.String (fromString)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock
  ( NominalDiffTime,
    UTCTime,
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
    Owner,
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
    newOwner,
    releasePeer,
    reservePeer,
    sendMessage,
  )
import Haskoin.Store.BlockStore.Pending (Pending, PendingTx (..))
import Haskoin.Store.BlockStore.Pending qualified as Pending
import Haskoin.Store.BlockStore.Sync
  ( Accept (..),
    Sync (..),
    needsMoreBlocks,
  )
import Haskoin.Store.BlockStore.Sync qualified as Sync
import Haskoin.Store.Common (StoreEvent (..), loopForever)
import Haskoin.Store.Data
import Haskoin.Store.Database.Mempool (wipeMempoolArea)
import Haskoin.Store.Database.Query (getMempoolBefore)
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered qualified as L
import Haskoin.Store.Database.Reader
import Haskoin.Store.Database.Writer (MempoolChanges (..), Writer, runWriterMempool)
import Haskoin.Store.Database.Writer qualified as W
import Haskoin.Store.Logic (ImportException (..))
import Haskoin.Store.Logic.Layered
  ( deleteUnconfirmedTx,
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
    STM,
    TVar,
    atomically,
    catch,
    link,
    modifyTVar,
    modifyTVar',
    newTVarIO,
    readTVar,
    readTVarIO,
    stateTVar,
    throwIO,
    try,
    withAsync,
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

-- | Block store process state.
data BlockStore = BlockStore
  { -- | mailbox of the process
    mailbox :: !(Mailbox BlockStoreMessage),
    -- | configuration
    config :: !BlockStoreConfig,
    -- | state of the block download ("Haskoin.Store.BlockStore.Sync")
    sync :: !(TVar (Sync Peer)),
    -- | transactions received and waiting to be imported
    -- ("Haskoin.Store.BlockStore.Pending")
    pending :: !(TVar Pending),
    -- | StatsD metrics, if enabled
    metrics :: !(Maybe StoreMetrics),
    -- | labels of the peers whose mempool was already requested
    syncedMempool :: !(TVar (HashSet Text)),
    -- | who reserves the syncing peer, so that no one else releases it
    owner :: !Owner
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
newStoreMetrics :: BlockStoreConfig -> IO (Maybe StoreMetrics)
newStoreMetrics cfg =
  forM cfg.stats $ \s -> do
    m <- Q.getMempool cfg.db
    b <-
      Q.getBestBlock cfg.db >>= \case
        Nothing -> return 0
        Just h -> maybe 0 (.height) <$> Q.getBlock cfg.db h
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
setStoreHeight :: BlockStore -> IO ()
setStoreHeight self =
  forM_ self.metrics $ \m ->
    getBestBlock self >>= mapM_ (getBlock self >=> mapM_ (setGauge m.blocks . fromIntegral . (.height)))

-- | Update the gauge of the best header height.
setHeadersHeight :: BlockStore -> IO ()
setHeadersHeight self =
  forM_ self.metrics $ \m -> do
    n <- chainGetBest self.config.chain
    setGauge m.headers (fromIntegral n.height)

-- | Update the gauge of transactions waiting to be imported.
setPendingTxs :: BlockStore -> IO ()
setPendingTxs self =
  forM_ self.metrics $ \m -> do
    p <- readTVarIO self.pending
    setGauge m.queuedTxs (Pending.queued p)

-- | Update the gauge of online peers.
setPeersConnected :: BlockStore -> IO ()
setPeersConnected self =
  forM_ self.metrics $ \m -> do
    p <- getPeers self.config.peerMgr
    setGauge m.peers (length p)

-- | Record how long a successful block import took.
withBlockTiming :: BlockStore -> IO (Either l r) -> IO (Either l r)
withBlockTiming self go = do
  case self.metrics of
    Just m -> do
      start <- round . (* 1000) <$> getPOSIXTime
      go >>= \case
        Right x -> do
          end <- round . (* 1000) <$> getPOSIXTime
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
    -- | what to do with the best block right after it changes, and when the
    -- store starts, before anything else is written to the database: a
    -- member of a fleet takes a view of the database for the block here
    -- ('Haskoin.Store.Fleet.Member.noteBest')
    onBest :: !(BlockHash -> IO ()),
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

-- | Log a message with the logger of the block store.
logDebug, logInfo, logWarn, logError :: BlockStore -> Text -> IO ()
logDebug self = logAt LevelDebug self.config.logger
logInfo self = logAt LevelInfo self.config.logger
logWarn self = logAt LevelWarn self.config.logger
logError self = logAt LevelError self.config.logger

-- | Run an import against the database, returning the import error if it fails.
runImport :: BlockStore -> Network -> Ctx -> (Writer -> IO a) -> IO (Either ImportException a)
runImport self net ctx f = fmap fst <$> runImportMempool self net ctx f

-- | Like 'runImport', but also return how the import changed the mempool.
-- The mempool gauge is kept up to date from those changes instead of
-- counting the whole mempool on every clock tick.
runImportMempool :: BlockStore -> Network -> Ctx -> (Writer -> IO a) -> IO (Either ImportException (a, MempoolChanges))
runImportMempool self net ctx f = do
  r <- try $ runWriterMempool net ctx self.config.db self.config.logger f
  forM_ r $ \(_, c) ->
    forM_
      self.metrics
      ( \m -> do
          incrementGauge m.mempool (length c.added)
          decrementGauge m.mempool (length c.removed)
      )
  return r

-- | Run an import and publish the transactions that it added to the
-- mempool or removed from it, then the new best block if there is one.
-- Transactions of the block are not published.
runImportNotify :: BlockStore -> Maybe Block -> (Writer -> IO a) -> IO (Either ImportException a)
runImportNotify self block f = do
  net <- getNetwork self
  ctx <- getCtx self
  runImportMempool self net ctx f >>= \case
    Left e -> return (Left e)
    Right (x, c) -> do
      let l = self.config.pub
      forM_ (filter notInBlock c.removed) $ \h ->
        publish (StoreMempoolDelete h) l
      forM_ (filter notInBlock c.added) $ \h ->
        publish (StoreMempoolNew h) l
      forM_ block $ \b -> do
        -- before the event, so that whoever hears of the block finds what
        -- was done for it
        self.config.onBest (headerHash b.header)
        publish (StoreBestBlock (headerHash b.header)) l
      return (Right x)
  where
    inBlock = maybe HashSet.empty (HashSet.fromList . map txHash . (.txs)) block
    notInBlock = not . (`HashSet.member` inBlock)

-- | Run a database query from the block store.
-- | Best block in the database.
getBestBlock :: BlockStore -> IO (Maybe BlockHash)
getBestBlock self = Q.getBestBlock self.config.db

-- | Data of a block in the database.
getBlock :: BlockStore -> BlockHash -> IO (Maybe BlockData)
getBlock self = Q.getBlock self.config.db

-- | Data of a transaction in the database: confirmed, in the mempool, or
-- deleted from it and marked so.
getTxData :: BlockStore -> TxHash -> IO (Maybe TxData)
getTxData self h = L.withView self.config.db (`L.getTxData` h)

-- | Unspent output in the database, of the chain or of the mempool.
getUnspent :: BlockStore -> OutPoint -> IO (Maybe Unspent)
getUnspent self p = L.withView self.config.db (`L.getUnspent` p)

-- | Network of the block store.
getNetwork :: BlockStore -> IO Network
getNetwork self = return self.config.net

-- | Secp256k1 context of the block store.
getCtx :: BlockStore -> IO Ctx
getCtx self = return self.config.ctx

-- | Run block store process.
withBlockStore ::
  BlockStoreConfig ->
  (BlockStore -> IO a) ->
  IO a
withBlockStore cfg action = do
  pb <- newTVarIO Idle
  ts <- newTVarIO Pending.empty
  sy <- newTVarIO HashSet.empty
  inbox <- newInbox
  metrics <- newStoreMetrics cfg
  owner <- (newOwner "block store")
  let r =
        BlockStore
          { mailbox = inboxToMailbox inbox,
            config = cfg,
            sync = pb,
            pending = ts,
            metrics = metrics,
            syncedMempool = sy,
            owner = owner
          }
  withAsync ((go r inbox)) $ \a -> do
    link a
    action r
  where
    go self inbox = do
      ini
      wipe
      run
      where
        -- the mempool is an area of its own, so wiping it is deleting the
        -- keys of that area: nothing of the chain has to be put right
        wipe = when cfg.wipeMempool $ do
          keys <- wipeMempoolArea cfg.db
          forM_ self.metrics $ \m -> setGauge m.mempool 0
          logAt LevelInfo cfg.logger $
            "Wiped the mempool: " <> cs (show keys) <> " keys deleted"
        ini = do
          net <- getNetwork self
          ctx <- getCtx self
          runImport self net ctx initBest >>= \case
            Left e -> do
              logError self $
                "Could not initialize: " <> cs (show e)
              throwIO e
            Right () -> getBestBlock self >>= mapM_ cfg.onBest
        run =
          withAsync (pingMe (inboxToMailbox inbox)) $ \a ->
            link a >> runBlockStoreLoop self inbox

-- | Handle messages forever.
runBlockStoreLoop :: (InChan mbox) => BlockStore -> mbox BlockStoreMessage -> IO b
runBlockStoreLoop self inbox =
  loopForever $ do
    logDebug self "Waiting for new event..."
    msg <- receive inbox
    processBlockStoreMessage self msg

-- | Whether the best block in the database is the best header of the node. If
-- so, release the syncing peer.
isInSync :: BlockStore -> IO Bool
isInSync self =
  getBestBlock self >>= \case
    Nothing -> do
      logError self "Block database uninitialized"
      throwIO Uninitialized
    Just bb -> do
      cb <- chainGetBest self.config.chain
      if headerHash cb.header == bb
        then clearSyncingState self >> return True
        else return False

-- | Run an action only if the database is in sync.
guardSync :: BlockStore -> IO () -> IO ()
guardSync self f = isInSync self >>= \s -> when s f

-- | Run an action only if the mempool is indexed.
guardMempool :: BlockStore -> IO () -> IO ()
guardMempool self f = unless self.config.noMempool f

-- | Ask a peer for its mempool, once per peer, if in sync and mempool sync is enabled.
syncMempool :: BlockStore -> Peer -> IO ()
syncMempool self p =
  guardMempool self . guardSync self $ do
    let s = self.config.syncMempool
    let m = self.syncedMempool
    y <- not . HashSet.member p.label <$> readTVarIO m
    when (s && y) $ do
      logDebug self $
        "Requesting mempool from peer: " <> p.label
      atomically . modifyTVar m $ HashSet.insert p.label
      MMempool `sendMessage` p

-- | Import a block from the syncing peer, then ask for more blocks or, if now in
-- sync, sync the mempool. Disconnects the peer if it is not the syncing peer, if
-- the block is unknown to the node, or if the block cannot be imported.
processBlock :: BlockStore -> Peer -> Block -> IO ()
processBlock self peer block =
  Sync.accept peer blockhash <$> readTVarIO self.sync >>= \case
    NotSyncingPeer -> refuse "Non-syncing peer"
    NotRequested -> refuse "Syncing peer"
    Requested ->
      getBlockNode self blockhash >>= \case
        Nothing -> do
          logError self $
            "Peer "
              <> peer.label
              <> " sent unknown block "
              <> blockHashToHex blockhash
          killPeer peer
        Just node -> do
          logDebug self $
            "Processing block "
              <> blockText node Nothing
              <> " from peer "
              <> peer.label
          ret <- withBlockTiming self $ runImportNotify self (Just block) (\wr -> importBlock wr block node)
          case ret of
            Left e -> failure e
            Right _ -> success node
  where
    header = block.header
    blockhash = headerHash header
    hexhash = blockHashToHex blockhash
    refuse who = do
      logError self $
        who
          <> " "
          <> peer.label
          <> " sent a block that was not requested: "
          <> hexhash
      killPeer peer
    success node = do
      logInfo self $
        "Best block: " <> blockText node (Just block)
      now <- getCurrentTime
      transition self $ \st -> ((), Sync.imported now blockhash st)
      isInSync self >>= \case
        False -> syncMe self
        True -> do
          updateOrphans self
          syncMempool self peer
    failure e = do
      logError self $
        "Error importing block "
          <> hexhash
          <> " from peer: "
          <> peer.label
          <> ": "
          <> cs (show e)
      killPeer peer

-- | Change the state of the block download with a transition of
-- "Haskoin.Store.BlockStore.Sync", atomically, and give its result.
transition :: BlockStore -> (Sync Peer -> (a, Sync Peer)) -> IO a
transition self = atomically . stateTVar self.sync

-- | Header of a block from the node.
getBlockNode :: BlockStore -> BlockHash -> IO (Maybe BlockNode)
getBlockNode self blockhash = do
  let ch = self.config.chain
  chainGetBlock ch blockhash

-- | Disconnect a peer that could not find requested blocks.
processNoBlocks :: BlockStore -> Peer -> [BlockHash] -> IO ()
processNoBlocks self p hs = do
  forM_ (zip [(1 :: Int) ..] hs) $ \(i, h) ->
    logError self $
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
processTx :: BlockStore -> Peer -> Tx -> IO ()
processTx self p tx = guardMempool self $ do
  t <- getCurrentTime
  logDebug self $
    "Received tx "
      <> txHashToHex (txHash tx)
      <> " by peer: "
      <> p.label
  changePending self (Pending.received t tx HashSet.empty)

-- | Forget old requests, and drop transactions that waited for their parents
-- for too long.
pruneOrphans :: BlockStore -> IO ()
pruneOrphans self = guardMempool self $ do
  now <- getCurrentTime
  changePending self (Pending.prune now)

-- | Change the mempool transactions on their way into the database.
changePending :: BlockStore -> (Pending -> Pending) -> IO ()
changePending self f = do
  atomically $ modifyTVar' self.pending f
  setPendingTxs self

-- | Whether a transaction is queued or was requested recently.
isPending :: BlockStore -> TxHash -> IO Bool
isPending self th = Pending.isPending th <$> readTVarIO self.pending

-- | Take up to this many queued transactions that have no missing parents, in
-- dependency order, out of the queue.
pendingTxs :: BlockStore -> Int -> IO [PendingTx]
pendingTxs self i = do
  selected <- atomically $ stateTVar self.pending (Pending.takeReady i)
  setPendingTxs self
  return selected

-- | Drop queued transactions that are already in the database, and recompute the
-- missing parents of the others.
updateOrphans :: BlockStore -> IO ()
updateOrphans self = do
  orphans <- Pending.waiting <$> readTVarIO self.pending
  updates <- forM orphans $ \p -> do
    let h = txHash p.tx
    exists h >>= \case
      True -> return (h, Nothing)
      False -> do
        found <- catMaybes <$> mapM (getUnspent self . (.outpoint)) p.tx.inputs
        let have = HashSet.fromList (map (.outpoint.hash) found)
        return (h, Just (HashSet.difference p.deps have))
  changePending self (Pending.refresh (HashMap.fromList updates))
  where
    exists th =
      getTxData self th >>= \case
        Nothing -> return False
        Just TxData {deleted = True} -> return False
        Just TxData {deleted = False} -> return True

-- | Queue a transaction that spends unknown outputs, recording which parents are missing.
newOrphanTx ::
  BlockStore ->
  UTCTime ->
  Tx ->
  Writer ->
  IO ()
newOrphanTx self time tx wr = do
  logAt LevelDebug self.config.logger $
    "Import tx "
      <> txHashToHex (txHash tx)
      <> ": Orphan"
  unspents <- catMaybes <$> mapM (W.getUnspent wr) prevs
  let have = HashSet.fromList (map (.outpoint) unspents)
      missing = HashSet.map (.hash) (HashSet.difference (HashSet.fromList prevs) have)
  atomically . modifyTVar' self.pending $ Pending.received time tx missing
  where
    prevs = map (.outpoint) tx.inputs

-- | Import a transaction into the mempool. Returns 'True' if imported. A
-- transaction that spends unknown outputs is queued as an orphan; other errors
-- are ignored.
importMempoolTx :: BlockStore -> UTCTime -> Tx -> Writer -> IO Bool
importMempoolTx block_read time tx wr =
  catch new_mempool_tx handle_error
  where
    tx_hash = txHash tx
    handle_error :: ImportException -> IO Bool
    handle_error Orphan = do
      newOrphanTx block_read time tx wr
      return False
    handle_error _ = return False
    seconds = floor (utcTimeToPOSIXSeconds time)
    new_mempool_tx = do
      t <- newMempoolTx wr tx seconds
      logAt LevelInfo block_read.config.logger $
        bool "Already have" "Imported" t
          <> " tx "
          <> txHashToHex (txHash tx)
      when t . atomically . modifyTVar' block_read.pending $ Pending.imported tx_hash
      return t

-- | Import up to 2000 queued transactions.
processMempool :: BlockStore -> IO ()
processMempool self = guardMempool self $ do
  txs <- pendingTxs self 2000
  let block_read = self
  unless (null txs) $ import_txs block_read txs
  where
    run_import block_read wr p =
      importMempoolTx block_read p.time p.tx wr
    import_txs block_read txs =
      runImportNotify self Nothing (\wr -> mapM (run_import block_read wr) txs) >>= \case
        Left e -> report_error e
        Right _ -> return ()
    report_error e = do
      logError self $
        "Error processing mempool: " <> cs (show e)
      throwIO e

-- | Delete up to 1000 mempool transactions older than the configured number of days.
pruneMempool :: BlockStore -> IO ()
pruneMempool self =
  guardMempool self $ do
    let days = self.config.mempoolTimeout
    when (days > 0) $ do
      time <- (floor . utcTimeToPOSIXSeconds) <$> getCurrentTime
      let thresh = time - (fromIntegral days * 24 * 60 * 60)
      -- runs on every clock tick: read only the expired entries, oldest
      -- first, from the index by time
      let db = self.config.db
      txs <- map snd <$> (getMempoolBefore db thresh 1000)
      unless (null txs) $ do
        logInfo self $
          "Deleting " <> cs (show (length txs)) <> " old transactions from mempool"
        runImportNotify self Nothing (\wr -> mapM_ (deleteUnconfirmedTx wr False) txs) >>= \case
          Left e ->
            logError self $
              "Error pruning mempool: " <> cs (show e)
          Right () -> return ()

-- | Request from a peer the announced transactions that are neither queued,
-- recently requested nor in the database. Only while in sync.
processTxs :: BlockStore -> Peer -> [TxHash] -> IO ()
processTxs self p hs = guardMempool self . guardSync self $ do
  logDebug self $
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
      isPending self h >>= \case
        True -> do
          logDebug self $
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
          getTxData self h >>= \case
            Just _ -> do
              logDebug self $
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
              logDebug self $
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
      now <- getCurrentTime
      changePending self (Pending.requested now xs)
      let net = self.config.net
      let inv = if net.segWit then InvWitnessTx else InvTx
          vec = map (InvVector inv . (.get)) xs
          msg = MGetData (GetData vec)
      msg `sendMessage` p

-- | Disconnect the syncing peer if it has been silent for too long.
checkTime :: BlockStore -> IO ()
checkTime self = do
  now <- getCurrentTime
  Sync.timedOut self.config.peerTimeout now <$> readTVarIO self.sync >>= \case
    Nothing -> return ()
    Just p -> do
      logError self $ "Syncing peer timeout: " <> p.label
      killPeer p

-- | Revert blocks until the best block in the database is in the main chain of headers.
revertToMainChain :: BlockStore -> IO ()
revertToMainChain self = do
  h <- headerHash . (.header) <$> getBest self
  let ch = self.config.chain
  net <- getNetwork self
  ctx <- getCtx self
  (chainBlockMain ch h) >>= \x -> unless x $ do
    logWarn self $
      "Reverting best block: "
        <> blockHashToHex h
    runImport self net ctx (`revertBlock` h) >>= \case
      Left e -> do
        logError self $
          "Could not revert block "
            <> blockHashToHex h
            <> ": "
            <> cs (show e)
        throwIO e
      Right () -> do
        getBestBlock self >>= mapM_ self.config.onBest
        transition self $ \st -> ((), Sync.forgetRequests st)
    revertToMainChain self

-- | Header of the best block in the database.
getBest :: BlockStore -> IO BlockNode
getBest self = do
  bb <-
    getBestBlock self >>= \case
      Just b -> return b
      Nothing -> do
        logError self "No best block set"
        throwIO Uninitialized
  let ch = self.config.chain
  (chainGetBlock ch bb) >>= \case
    Just x -> return x
    Nothing -> do
      logError self $
        "Header not found for best block: "
          <> blockHashToHex bb
      throwIO (BlockNotInChain bb)

-- | Header of the last block requested from the syncing peer, or of the best block if none.
getSyncBest :: BlockStore -> IO BlockNode
getSyncBest self = do
  bb <-
    Sync.lastRequested <$> readTVarIO self.sync >>= \case
      Just h -> return h
      Nothing ->
        getBestBlock self >>= \case
          Just b -> return b
          Nothing -> do
            logError self "No best block set"
            throwIO Uninitialized
  chainGetBlock self.config.chain bb >>= \case
    Just x -> return x
    Nothing -> do
      logError self $
        "Header not found for block: "
          <> blockHashToHex bb
      throwIO (BlockNotInChain bb)

-- | The syncing peer, when it is time to ask it for the next batch of blocks.
shouldSync :: BlockStore -> IO (Maybe Peer)
shouldSync self =
  isInSync self >>= \case
    True -> return Nothing
    False -> Sync.wantsMore (blockWindow self) <$> readTVarIO self.sync

-- | Blocks to request from the syncing peer in one batch. Never below one,
-- or the store would never ask for any block.
blockWindow :: BlockStore -> Int
blockWindow self = max 1 self.config.blockWindow

-- | Ask the syncing peer for the next batch of blocks, if the window has room.
syncMe :: BlockStore -> IO ()
syncMe self = do
  revertToMainChain self
  shouldSync self >>= \case
    Nothing -> return ()
    Just p -> do
      bb <- getSyncBest self
      bh <- chainGetBest self.config.chain
      when (bb /= bh) $ do
        bns <- sel bb bh
        logDebug self $
          "Requesting "
            <> fromString (show (length bns))
            <> " blocks from peer: "
            <> p.label
        transition self $ \st -> ((), Sync.request (map (headerHash . (.header)) bns) st)
        MGetData (GetData (map inv bns)) `sendMessage` p
  where
    inv = InvVector kind . (.get) . headerHash . (.header)
    kind = if self.config.net.segWit then InvWitnessBlock else InvBlock
    -- The blocks right after the last requested one, up to one window or
    -- the chain tip, whichever comes first.
    sel bb bh = do
      top <- findAncestor self (min (bb.height + fromIntegral (blockWindow self)) bh.height) bh
      ps <- chainGetParents self.config.chain (bb.height + 1) top
      return $ ps <> [top]

-- | Ancestor of a header at a height. Throws 'AncestorNotInChain' if missing.
findAncestor :: BlockStore -> BlockHeight -> BlockNode -> IO BlockNode
findAncestor self height target = do
  let ch = self.config.chain
  (chainGetAncestor ch height target) >>= \case
    Just ancestor -> return ancestor
    Nothing -> do
      let h = headerHash target.header
      logError self $
        "Could not find header for ancestor of block "
          <> blockHashToHex h
          <> " at height "
          <> cs (show target.height)
      throwIO $ AncestorNotInChain height h

-- | Forget a peer: release it if it was the syncing peer, and forget that its mempool was requested.
finishPeer :: BlockStore -> Peer -> IO ()
finishPeer self p = do
  released <- transition self (Sync.release p)
  when released $ do
    logDebug self $ "Releasing peer: " <> p.label
    void (releasePeer self.owner p)
  atomically $ modifyTVar self.syncedMempool $ HashSet.delete p.label

-- | Make a peer the syncing peer if there is none and the peer is not busy.
trySetPeer :: BlockStore -> Peer -> IO Bool
trySetPeer self p =
  isJust . Sync.syncingPeer <$> readTVarIO self.sync >>= \case
    True -> return False
    False ->
      reservePeer self.owner p >>= \case
        False -> return False
        True -> do
          now <- getCurrentTime
          started <- transition self $ \st -> case Sync.start now p st of
            Just st' -> (True, st')
            Nothing -> (False, st)
          if started
            then logDebug self ("Locked peer: " <> p.label)
            else void (releasePeer self.owner p)
          return started

-- | If not in sync and there is no syncing peer, pick the first free peer and start syncing.
trySyncing :: BlockStore -> IO ()
trySyncing self =
  isInSync self >>= \case
    True -> return ()
    False ->
      isJust . Sync.syncingPeer <$> readTVarIO self.sync >>= \case
        True -> return ()
        False -> getPeers self.config.peerMgr >>= recurse . map (.mailbox)
  where
    recurse [] = return ()
    recurse (p : ps) =
      trySetPeer self p >>= \case
        False -> recurse ps
        True -> syncMe self

-- | Start syncing against a newly connected peer if needed, or request its mempool if in sync.
trySyncingPeer :: BlockStore -> Peer -> IO ()
trySyncingPeer self p =
  isInSync self >>= \case
    True -> syncMempool self p
    False ->
      trySetPeer self p >>= \case
        False -> return ()
        True -> syncMe self

-- | Release the syncing peer, if any.
clearSyncingState :: BlockStore -> IO ()
clearSyncingState self =
  Sync.syncingPeer <$> readTVarIO self.sync >>= mapM_ (finishPeer self)

-- | Handle a message sent to the block store.
processBlockStoreMessage :: BlockStore -> BlockStoreMessage -> IO ()
processBlockStoreMessage self (BlockNewBest node) = do
  logDebug self $
    "New best block mined at height "
      <> cs (show node.height)
      <> ": "
      <> blockHashToHex (headerHash node.header)
  trySyncing self
processBlockStoreMessage self (BlockPeerConnect p) = do
  logDebug self $
    "New peer connected: " <> p.label
  trySyncingPeer self p
processBlockStoreMessage self (BlockPeerDisconnect p) = do
  logDebug self $
    "Peer disconnected: " <> p.label
  finishPeer self p
processBlockStoreMessage self (BlockReceived p b) = do
  logDebug self $
    "Received block: "
      <> blockHashToHex (headerHash b.header)
  processBlock self p b
processBlockStoreMessage self (BlockNotFound p bs) = do
  logDebug self $
    "Blocks not found by peer "
      <> p.label
      <> ": "
      <> T.unwords (map blockHashToHex bs)
  processNoBlocks self p bs
processBlockStoreMessage self (TxRefReceived p tx) = do
  logDebug self $
    "Transaction received from peer "
      <> p.label
      <> ": "
      <> txHashToHex (txHash tx)
  processTx self p tx
processBlockStoreMessage self (TxRefAvailable p ts) = do
  logDebug self $
    "Transactions available from peer "
      <> p.label
      <> ": "
      <> T.unwords (map txHashToHex ts)
  processTxs self p ts
processBlockStoreMessage self (BlockPing r) = do
  logDebug self "Internal clock event"
  setStoreHeight self
  setHeadersHeight self
  setPendingTxs self
  setPeersConnected self
  trySyncing self
  processMempool self
  pruneOrphans self
  pruneMempool self
  checkTime self
  atomically (r ())

-- | Send a clock tick to the block store every 0.1 to 1 second, waiting for each to be handled.
pingMe :: Mailbox BlockStoreMessage -> IO ()
pingMe mbox =
  loopForever $ do
    BlockPing `query` mbox
    delay <- randomRIO (100 * 1000, 1000 * 1000)
    threadDelay delay

-- | Tell the block store that a peer connected.
blockStorePeerConnect :: Peer -> BlockStore -> IO ()
blockStorePeerConnect peer store =
  BlockPeerConnect peer `send` store.mailbox

-- | Tell the block store that a peer disconnected.
blockStorePeerDisconnect ::
  Peer -> BlockStore -> IO ()
blockStorePeerDisconnect peer store =
  BlockPeerDisconnect peer `send` store.mailbox

-- | Tell the block store that the node has a new best header.
blockStoreHead ::
  BlockNode -> BlockStore -> IO ()
blockStoreHead node store =
  BlockNewBest node `send` store.mailbox

-- | Give the block store a block received from a peer.
blockStoreBlock ::
  Peer -> Block -> BlockStore -> IO ()
blockStoreBlock peer block store =
  BlockReceived peer block `send` store.mailbox

-- | Tell the block store that a peer could not find blocks.
blockStoreNotFound ::
  Peer -> [BlockHash] -> BlockStore -> IO ()
blockStoreNotFound peer blocks store =
  BlockNotFound peer blocks `send` store.mailbox

-- | Give the block store a transaction received from a peer.
blockStoreTx ::
  Peer -> Tx -> BlockStore -> IO ()
blockStoreTx peer tx store =
  TxRefReceived peer tx `send` store.mailbox

-- | Tell the block store that a peer announced transactions.
blockStoreTxHash ::
  Peer -> [TxHash] -> BlockStore -> IO ()
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
  BlockStore -> IO Int
blockStorePendingTxs =
  atomically . blockStorePendingTxsSTM

-- | 'STM' version of 'blockStorePendingTxs'.
blockStorePendingTxsSTM ::
  BlockStore -> STM Int
blockStorePendingTxsSTM store = Pending.size <$> readTVar store.pending

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
