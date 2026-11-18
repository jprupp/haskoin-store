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
-- blocks in batches of 'blockWindow'. Only a peer that has announced the next
-- block is picked ('Known'): the peers that have are pinged, and the first to
-- answer is the one, or a late one if none answers within 'blockPing'. Each
-- block is imported as soon as it arrives. If the best block of the database is no longer in the main chain of
-- headers, blocks are reverted until it is. A syncing peer that delivers
-- nothing for a few of its recent gaps between blocks, at least two seconds
-- and at most 'blockStall', is passed over: another is found the same way and
-- asked for the same blocks, and whichever delivers first is taken. A peer that sends a
-- block that it was not asked for, cannot find a block, or still owes blocks
-- after 'peerTimeout' is disconnected. The state of the download, and every rule
-- about it, is the pure state machine of "Haskoin.Store.BlockStore.Sync";
-- this module carries out what its transitions decide.
--
-- = Transactions
--
-- Once the database is in sync, the block store requests every transaction
-- that peers announce and that no store of the fleet has seen ('toFetch'):
-- what the fleet saw is a set in Redis, the same for every store and kept
-- across restarts, so a transaction is fetched about once for the fleet.
-- It asks peers for their whole mempools only to fill
-- the pool again after Redis lost it ('blockStoreRefill'): the pool outlives
-- the store, so a store that starts has nothing to ask for. A transaction
-- that arrives goes to 'onTx': the member of the fleet offers it to the
-- pool that the fleet shares in Redis, which is the mempool
-- ("Haskoin.Store.Fleet.Member"). The database holds the chain alone. The
-- transactions on their way in, requested, are the pure value of
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
    blockStoreAnnounced,
    blockStoreAnnouncedSTM,
    blockStorePong,
    blockStorePongSTM,
    blockStoreTx,
    blockStoreTxSTM,
    blockStoreTxHash,
    blockStoreTxHashSTM,
    blockStorePendingTxs,
    blockStorePendingTxsSTM,
    needsMoreBlocks,
    blockStoreRefill,
    toFetch,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (link, withAsync)
import Control.Concurrent.STM
  ( STM,
    TVar,
    atomically,
    modifyTVar,
    modifyTVar',
    newTVarIO,
    readTVar,
    readTVarIO,
    stateTVar,
    writeTVar,
  )
import Control.Exception (Exception, throwIO, try)
import Control.Monad
  ( filterM,
    forM,
    forM_,
    unless,
    void,
    when,
    (>=>),
  )
import Data.ByteString qualified as B
import Data.HashMap.Strict (HashMap)
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
import Data.Word (Word64)
import Haskoin
  ( Block (..),
    BlockHash (..),
    BlockHeader (..),
    BlockHeight,
    BlockNode (..),
    GetData (..),
    InvType (..),
    InvVector (..),
    Message (..),
    Network (..),
    OutPoint (..),
    Ping (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    Version (..),
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
    getOnlinePeer,
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
    Sync,
    needsMoreBlocks,
  )
import Haskoin.Store.BlockStore.Sync qualified as Sync
import Haskoin.Store.Common (StoreEvent (..), loopForever)
import Haskoin.Store.Data
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
import Haskoin.Store.Database.Writer
  ( BatchReport (..),
    BlockTimes (..),
    Writer,
    runWriterReport,
  )
import Haskoin.Store.Database.Writer qualified as W
import Haskoin.Store.Logic
  ( ImportException (..),
    importBlock,
    initBest,
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
import System.Random (randomIO, randomRIO)

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
  | -- | a peer announced blocks: the last header of those it sent, or the
    -- blocks of an inventory
    BlockAnnounced !Peer ![BlockHash]
  | -- | a peer answered a ping with a nonce
    BlockPong !Peer !Word64
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
    -- | whether to ask peers for their mempools: from the first refill of
    -- the pool on ('blockStoreRefill')
    refilling :: !(TVar Bool),
    -- | what each peer said that it has, by its label: a peer is asked
    -- only for blocks that it announced
    knows :: !(TVar (HashMap Text Known)),
    -- | who reserves the syncing peer, so that no one else releases it
    owner :: !Owner
  }

-- | The blocks that a peer said it has. A peer is asked for a block only
-- if it has announced it, or a block that comes after it: one that is
-- asked for a block that it does not have yet answers nothing, or that it
-- cannot find it, and the block arrives a stall later from another.
data Known = Known
  { -- | the height that it gave when it connected: it is taken to have
    -- the blocks of the main chain up to there
    height :: !BlockHeight,
    -- | the block with the most work of those it announced since, with
    -- the headers that it sent or in an inventory: it has that block and
    -- those before it
    tip :: !(Maybe BlockNode)
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
    -- | milliseconds to import a block (@store.block@)
    block :: !StatTiming,
    -- | milliseconds of each stage of the import of a block
    -- ('Haskoin.Store.Logic.importBlock'): working out what it
    -- needs of the block (@store.block_prepare@)
    blockPrepare :: !StatTiming,
    -- | reading ahead what the rules will read (@store.block_prefetch@)
    blockPrefetch :: !StatTiming,
    -- | of the read ahead, each kind of key
    -- ('Haskoin.Store.Database.Writer.ReadTimes'): the unspent outputs
    -- that the block spends (@store.block_read_unspents@)
    blockReadUnspents :: !StatTiming,
    -- | the groups of spenders of those outputs
    -- (@store.block_read_spenders@): zero, as a block reads none
    blockReadSpenders :: !StatTiming,
    -- | the balances of the addresses of the block
    -- (@store.block_read_balances@): zero, as a block reads none
    blockReadBalances :: !StatTiming,
    -- | the rules, in memory (@store.block_apply@)
    blockApply :: !StatTiming,
    -- | writing the batch to RocksDB (@store.block_write@)
    blockWrite :: !StatTiming,
    -- | the same times of the latest block, as gauges that keep their value
    -- until the next block (@store.last_block_ms@, @store.last_block_prepare_ms@,
    -- @store.last_block_prefetch_ms@, @store.last_block_apply_ms@ and
    -- @store.last_block_write_ms@): a timing has a value only in the second of
    -- its block, so a chart that averages seconds shows it smaller the more it
    -- is zoomed out; a gauge shows the time of the latest block at any zoom
    lastBlock :: !StatGauge,
    lastPrepare :: !StatGauge,
    lastPrefetch :: !StatGauge,
    lastApply :: !StatGauge,
    lastWrite :: !StatGauge
  }

-- | Register the block store metrics, if StatsD is configured, with their current values.
newStoreMetrics :: BlockStoreConfig -> IO (Maybe StoreMetrics)
newStoreMetrics cfg =
  forM cfg.stats $ \s -> do
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
    block <- t s "block"
    blockPrepare <- t s "block_prepare"
    blockPrefetch <- t s "block_prefetch"
    blockReadUnspents <- t s "block_read_unspents"
    blockReadSpenders <- t s "block_read_spenders"
    blockReadBalances <- t s "block_read_balances"
    blockApply <- t s "block_apply"
    blockWrite <- t s "block_write"
    lastBlock <- g s "last_block_ms" 0
    lastPrepare <- g s "last_block_prepare_ms" 0
    lastPrefetch <- g s "last_block_prefetch_ms" 0
    lastApply <- g s "last_block_apply_ms" 0
    lastWrite <- g s "last_block_write_ms" 0
    return StoreMetrics {..}
  where
    g s x = newStatGauge s ("store." <> x)
    -- every sample is sent: there is one a block. The timing of a block
    -- was made with a rate of 0 until version 1.15.1, which sends none,
    -- so it never reached the StatsD server
    t s x = newStatTiming s ("store." <> x) 1

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
          setGauge m.lastBlock (end - start)
          return (Right x)
        Left y -> return (Left y)
    Nothing -> go

-- | Configuration for a block store.
data BlockStoreConfig = BlockStoreConfig
  { -- | peer manager from running node
    peerMgr :: !PeerMgr,
    -- | chain from a running node
    chain :: !Chain,
    -- | listener for store events
    pub :: !(Publisher StoreEvent),
    -- | RocksDB database handle
    db :: !DatabaseReader,
    -- | network constants
    net :: !Network,
    -- | what to do with the best block right after it changes, and when the
    -- store starts, before anything else is written to the database: a
    -- member of a fleet takes a view of the database for the block here
    -- ('Haskoin.Store.Fleet.Member.noteBest')
    onBest :: !(BlockHash -> IO ()),
    -- | what to do with a transaction that a peer sent: the member of the
    -- fleet offers it to the pool, the mempool, which is in Redis alone
    -- ('Haskoin.Store.Fleet.Member.memberReceived')
    onTx :: !(UnixTime -> Tx -> IO ()),
    -- | of these transactions, those that the store does not fetch when a
    -- peer announces them: those that it holds to offer to the pool, and
    -- those that the pool has decided on, taken, refused as a conflict or
    -- let go unconfirmed. The database keeps
    -- nothing of the mempool, so without this every store would fetch each
    -- transaction from every peer that announces it
    -- ('Haskoin.Store.Fleet.Member.memberSeen')
    seen :: !([TxHash] -> IO (HashSet TxHash)),
    -- | blocks to request from the syncing peer in one batch
    blockWindow :: !Int,
    -- | delete transactions older than this number of days from mempool
    mempoolTimeout :: !Int,
    -- | disconnect syncing peer if inactive for this long
    peerTimeout :: !NominalDiffTime,
    -- | the longest that the syncing peer may deliver nothing before another
    -- peer is asked for its blocks; a quick peer is given less, a few of its
    -- recent gaps between blocks ("Haskoin.Store.BlockStore.Sync")
    blockStall :: !NominalDiffTime,
    -- | a peer is asked for blocks if it answers a ping within this long,
    -- or if none does ("Haskoin.Store.BlockStore.Sync")
    blockPing :: !NominalDiffTime,
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
runImport :: BlockStore -> Network -> (Writer -> IO a) -> IO (Either ImportException a)
runImport self net f = do
  r <- try $ runWriterReport net self.config.db self.config.logger f
  forM_ r $ \(_, report) ->
    forM_
      self.metrics
      ( \m ->
          -- only a batch that imported a block has these
          forM_ report.block $ \t -> do
            addTiming m.blockPrepare (milliseconds t.prepare)
            addTiming m.blockPrefetch (milliseconds t.prefetch)
            addTiming m.blockReadUnspents (milliseconds t.reads.unspentReads)
            addTiming m.blockReadSpenders (milliseconds t.reads.spenderReads)
            addTiming m.blockReadBalances (milliseconds t.reads.balanceReads)
            addTiming m.blockApply (milliseconds t.apply)
            addTiming m.blockWrite (milliseconds t.write)
            setGauge m.lastPrepare (milliseconds t.prepare)
            setGauge m.lastPrefetch (milliseconds t.prefetch)
            setGauge m.lastApply (milliseconds t.apply)
            setGauge m.lastWrite (milliseconds t.write)
      )
  return (fst <$> r)
  where
    milliseconds :: Double -> Int
    milliseconds = round . (* 1000)

-- | Run an import, then publish the new best block if there is one.
runImportNotify ::
  BlockStore -> Maybe BlockHash -> (Writer -> IO a) -> IO (Either ImportException a)
runImportNotify self block f = do
  net <- getNetwork self
  runImport self net f >>= \case
    Left e -> return (Left e)
    Right x -> do
      let l = self.config.pub
      forM_ block $ \h -> do
        -- before the event, so that whoever hears of the block finds what
        -- was done for it
        self.config.onBest h
        publish (StoreBestBlock h) l
      return (Right x)

-- | Run a database query from the block store.
-- | Best block in the database.
getBestBlock :: BlockStore -> IO (Maybe BlockHash)
getBestBlock self = Q.getBestBlock self.config.db

-- | Data of a block in the database.
getBlock :: BlockStore -> BlockHash -> IO (Maybe BlockData)
getBlock self = Q.getBlock self.config.db

-- | Data of a confirmed transaction in the database.
getTxData :: BlockStore -> TxHash -> IO (Maybe TxData)
getTxData self = Q.getTxData self.config.db

-- | Unspent output of the chain in the database.
getUnspent :: BlockStore -> OutPoint -> IO (Maybe Unspent)
getUnspent self = Q.getUnspent self.config.db

-- | Network of the block store.
getNetwork :: BlockStore -> IO Network
getNetwork self = return self.config.net

-- | Run block store process.
withBlockStore ::
  BlockStoreConfig ->
  (BlockStore -> IO a) ->
  IO a
withBlockStore cfg action = do
  pb <- newTVarIO . Sync.idle =<< getCurrentTime
  ts <- newTVarIO Pending.empty
  sy <- newTVarIO HashSet.empty
  refilling <- newTVarIO False
  inbox <- newInbox
  metrics <- newStoreMetrics cfg
  owner <- (newOwner "block store")
  knows <- newTVarIO HashMap.empty
  let r =
        BlockStore
          { mailbox = inboxToMailbox inbox,
            config = cfg,
            sync = pb,
            pending = ts,
            metrics = metrics,
            syncedMempool = sy,
            refilling = refilling,
            knows = knows,
            owner = owner
          }
  withAsync ((go r inbox)) $ \a -> do
    link a
    action r
  where
    go self inbox = do
      ini
      run
      where
        ini = do
          net <- getNetwork self
          runImport self net initBest >>= \case
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

-- | Ask a peer for its mempool, once per peer, if in sync and the pool is
-- being filled again ('blockStoreRefill').
syncMempool :: BlockStore -> Peer -> IO ()
syncMempool self p =
  guardSync self $ do
    s <- readTVarIO self.refilling
    let m = self.syncedMempool
    y <- not . HashSet.member p.label <$> readTVarIO m
    when (s && y) $ do
      logDebug self $
        "Requesting mempool from peer: " <> p.label
      atomically . modifyTVar m $ HashSet.insert p.label
      MMempool `sendMessage` p

-- | Ask every peer for its mempool again, and fetch every transaction that
-- it announces and the fleet has not seen: for when Redis lost its data,
-- the pool with it, and what the fleet had seen
-- ('Haskoin.Store.Fleet.Member.noteEpoch').
-- The transactions go to 'onTx', which offers them to the pool again.
blockStoreRefill :: BlockStore -> IO ()
blockStoreRefill self = do
  atomically $ do
    writeTVar self.syncedMempool HashSet.empty
    writeTVar self.refilling True
  getPeers self.config.peerMgr >>= mapM_ (syncMempool self . (.mailbox))

-- | Import a block from the syncing peer, then ask for more blocks or, if now in
-- sync, sync the mempool. Disconnects the peer if it is not the syncing peer, if
-- the block is unknown to the node, or if the block cannot be imported.
processBlock :: BlockStore -> Peer -> Block -> IO ()
processBlock self peer block =
  transition self (Sync.accept peer blockhash) >>= \case
    NotSyncingPeer -> refuse "Non-syncing peer"
    NotRequested -> refuse "Syncing peer"
    -- a peer that was passed over for being slow, and another was faster
    Late -> logDebug self $ "Block " <> hexhash <> " from peer " <> peer.label <> " came after another's"
    Requested ->
      getBlockNode self blockhash >>= \case
        Nothing -> do
          logWarn self $
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
          ret <-
            withBlockTiming self $
              runImportNotify self (Just blockhash) (\wr -> importBlock wr block node)
          case ret of
            Left e -> failure e
            Right _ -> success node
  where
    header = block.header
    blockhash = headerHash header
    hexhash = blockHashToHex blockhash
    refuse who = do
      logWarn self $
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
processTx self p tx = do
  t <- getCurrentTime
  logDebug self $
    "Received tx "
      <> txHashToHex (txHash tx)
      <> " by peer: "
      <> p.label
  changePending self (Pending.answered (txHash tx))
  self.config.onTx (fromIntegral (floor (utcTimeToPOSIXSeconds t) :: Integer)) tx

-- | Change the mempool transactions on their way into the database.
changePending :: BlockStore -> (Pending -> Pending) -> IO ()
changePending self f = do
  atomically $ modifyTVar' self.pending f
  setPendingTxs self

-- | Whether a transaction is queued or was requested recently.
isPending :: BlockStore -> TxHash -> IO Bool
isPending self th = Pending.isPending th <$> readTVarIO self.pending

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

-- | Of the transactions that a peer announced, those to fetch: given
-- whether one is queued or was requested lately, which of them the fleet
-- has seen ('seen' of t'BlockStoreConfig'), and whether one is in the
-- database. The fleet is asked once for all of them, since an inventory can
-- name thousands, and only for those that are not queued.
toFetch :: (TxHash -> IO Bool) -> ([TxHash] -> IO (HashSet TxHash)) -> (TxHash -> IO Bool) -> [TxHash] -> IO [TxHash]
toFetch pending seen stored hs = do
  fresh <- filterM (fmap not . pending) hs
  have <- if null fresh then return HashSet.empty else seen fresh
  filterM (fmap not . stored) (filter (not . (`HashSet.member` have)) fresh)

-- | Request from a peer the announced transactions that are neither queued,
-- recently requested, seen by the fleet nor in the database ('toFetch').
-- Only while in sync.
processTxs :: BlockStore -> Peer -> [TxHash] -> IO ()
processTxs self p hs = guardSync self $ do
  xs <- toFetch (isPending self) self.config.seen stored hs
  logDebug self $
    "Requesting "
      <> cs (show (length xs))
      <> " of "
      <> cs (show (length hs))
      <> " transactions announced by peer: "
      <> p.label
  unless (null xs) $ go xs
  where
    stored h = isJust <$> getTxData self h
    go xs = do
      now <- getCurrentTime
      changePending self (Pending.requested now xs)
      let net = self.config.net
      let inv = if net.segWit then InvWitnessTx else InvTx
          vec = map (InvVector inv . (.get)) xs
          msg = MGetData (GetData vec)
      msg `sendMessage` p

-- | Deal with peers that do not deliver: disconnect the syncing peer if it
-- has been silent for the timeout, else look for another peer to ask for its
-- blocks if it has been silent for the stall; and disconnect the peers passed
-- over before that still owe their blocks after the timeout. If peers were
-- pinged and none answered in time, ask one of them all the same.
checkTime :: BlockStore -> IO ()
checkTime self = do
  now <- getCurrentTime
  st <- readTVarIO self.sync
  let timeout = self.config.peerTimeout
  case Sync.timedOut timeout now st of
    Just p -> do
      logWarn self $ "Syncing peer timeout: " <> p.label
      killPeer p
    Nothing -> forM_ (Sync.stalled self.config.blockStall timeout now st) $ \slow ->
      search self (slow : Sync.passedPeers st)
  forM_ (Sync.expired timeout now st) $ \p -> do
    logWarn self $ "Peer did not deliver its blocks: " <> p.label
    killPeer p
  late <- Sync.overdue self.config.blockPing now <$> readTVarIO self.sync
  unless (null late) $ do
    logWarn self $
      "No peer answered a ping within "
        <> cs (show self.config.blockPing)
        <> ": asking one all the same"
    void (firstM (choose self now) late)
    transition self $ \s -> ((), Sync.settle s)
  where
    firstM _ [] = return Nothing
    firstM f (x : xs) = f x >>= \ok -> if ok then return (Just x) else firstM f xs

-- | Look for a peer to ask for blocks: ping those that have the block that
-- is needed, leaving out the ones given, fastest first. The first to
-- answer is asked ('processPong'). Peers that were pinged already in this
-- search are not pinged again, so this may be called at every tick, and
-- reaches the peers that announce the block while the search goes on.
search :: BlockStore -> [Peer] -> IO ()
search self asked =
  needed self >>= \case
    Nothing -> return ()
    Just node -> do
      -- the peers that still owe blocks from before come last
      slow <- Sync.passedPeers <$> readTVarIO self.sync
      peers <- filter (`notElem` asked) . map (.mailbox) <$> getPeers self.config.peerMgr
      able <- filterM (peerHas self node) (filter (`notElem` slow) peers <> filter (`elem` slow) peers)
      now <- getCurrentTime
      fresh <- randomIO
      transition self (Sync.ping now fresh able) >>= \case
        Nothing -> return ()
        Just (nonce, ps) -> forM_ ps $ \p -> do
          logDebug self $ "Pinging peer before asking it for blocks: " <> p.label
          MPing (Ping nonce) `sendMessage` p

-- | The block that a peer must have to be asked: the last one that the
-- syncing peer was asked for and has not delivered, since its blocks are
-- what the next peer is asked for; or, with nothing asked, the block after
-- the best one. A peer that has a block has those before it. Nothing if
-- there is no block to ask for.
needed :: BlockStore -> IO (Maybe BlockNode)
needed self =
  Sync.lastRequested <$> readTVarIO self.sync >>= \case
    Just h -> getBlockNode self h
    Nothing -> do
      bb <- getBest self
      bh <- chainGetBest self.config.chain
      if bb.height < bh.height
        then Just <$> findAncestor self (bb.height + 1) bh
        else return Nothing

-- | Whether a peer said that it has a block: it gave a height at least
-- that of the block when it connected, or it announced the block or one
-- that comes after it.
peerHas :: BlockStore -> BlockNode -> Peer -> IO Bool
peerHas self node p =
  HashMap.lookup p.label <$> readTVarIO self.knows >>= \case
    Nothing -> return False
    Just k
      | node.height <= k.height -> return True
      | Just t <- k.tip,
        node.height <= t.height ->
          (== Just node) <$> chainGetAncestor self.config.chain node.height t
      | otherwise -> return False

-- | Note the blocks that a peer announced, and look for a peer to ask if
-- blocks are needed: this one can now be asked for them. Blocks whose
-- headers the node does not have are ignored: a peer that announces one in
-- an inventory is asked for its header by the node, and announces it again
-- with that.
processAnnounced :: BlockStore -> Peer -> [BlockHash] -> IO ()
processAnnounced self p hs = do
  nodes <- catMaybes <$> mapM (getBlockNode self) hs
  forM_ nodes $ \n ->
    atomically . modifyTVar' self.knows $
      HashMap.insertWith (\_ k -> raise n k) p.label (Known 0 (Just n))
  unless (null nodes) (trySyncing self)
  where
    raise n k
      | maybe True ((< n.work) . (.work)) k.tip = Known k.height (Just n)
      | otherwise = k

-- | A peer answered a ping: if it was pinged to be asked for blocks and
-- answered in time, ask it.
processPong :: BlockStore -> Peer -> Word64 -> IO ()
processPong self p nonce = do
  now <- getCurrentTime
  chosen <- transition self (Sync.pong self.config.blockPing now p nonce)
  when chosen (void (choose self now p))

-- | Ask a peer that the search found for blocks: make it the syncing peer
-- if there is none, or pass the syncing peer over for it if that one is
-- still not delivering. Says whether the search is over; it is not if the
-- peer is busy with something else and cannot be locked.
choose :: BlockStore -> UTCTime -> Peer -> IO Bool
choose self now p = do
  st <- readTVarIO self.sync
  case Sync.syncingPeer st of
    Nothing ->
      trySetPeer self p >>= \case
        False -> return False
        True -> syncMe self >> return True
    Just _
      | isJust (Sync.stalled self.config.blockStall self.config.peerTimeout now st) -> passOver self now p
      -- the syncing peer delivered in the meantime
      | otherwise -> True <$ transition self (\s -> ((), Sync.settle s))

-- | Ask a peer for the blocks that the syncing peer is not delivering, and
-- make it the syncing peer. The one passed over stays connected, and its
-- blocks are still taken if they come first. Says whether it was done.
passOver :: BlockStore -> UTCTime -> Peer -> IO Bool
passOver self now p =
  reservePeer self.owner p >>= \case
    False -> return False
    True ->
      transition self (Sync.pass now p) >>= \case
        Nothing -> False <$ releasePeer self.owner p
        Just (old, blocks) -> do
          logWarn self $
            "Peer "
              <> old.label
              <> " is not delivering: asking "
              <> p.label
              <> " for "
              <> cs (show (length blocks))
              <> " blocks"
          void (releasePeer self.owner old)
          unless (null blocks) $ MGetData (GetData (map inv blocks)) `sendMessage` p
          return True
  where
    inv = InvVector kind . (.get)
    kind = if self.config.net.segWit then InvWitnessBlock else InvBlock

-- | Revert blocks until the best block in the database is in the main chain of headers.
revertToMainChain :: BlockStore -> IO ()
revertToMainChain self = do
  h <- headerHash . (.header) <$> getBest self
  let ch = self.config.chain
  net <- getNetwork self
  (chainBlockMain ch h) >>= \x -> unless x $ do
    logWarn self $
      "Reverting best block: "
        <> blockHashToHex h
    runImport self net (\wr -> revertBlock wr h) >>= \case
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
        bns <- sel bb bh >>= reach p
        idle <- null . Sync.outstanding <$> readTVarIO self.sync
        if null bns
          then when idle $ do
            -- it has delivered what it had, and has not announced what
            -- comes next: let it go, for a peer that has to be found
            logDebug self $ "Syncing peer has not announced the next block: " <> p.label
            released <- transition self (Sync.release p)
            when released (void (releasePeer self.owner p))
          else request p bns
  where
    request p bns = do
      logDebug self $
        "Requesting "
          <> fromString (show (length bns))
          <> " blocks from peer: "
          <> p.label
      transition self $ \st -> ((), Sync.request (map (headerHash . (.header)) bns) st)
      MGetData (GetData (map inv bns)) `sendMessage` p
    -- the blocks that the peer has announced: all of them if it has the
    -- last, since they are a chain, or else those up to the first that
    -- it has not
    reach _ [] = return []
    reach p bns =
      peerHas self (last bns) p >>= \case
        True -> return bns
        False -> takeWhileM (\n -> peerHas self n p) bns
    takeWhileM _ [] = return []
    takeWhileM f (x : xs) =
      f x >>= \case
        True -> (x :) <$> takeWhileM f xs
        False -> return []
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

-- | If not in sync and there is no syncing peer, look for one ('search').
trySyncing :: BlockStore -> IO ()
trySyncing self =
  isInSync self >>= \case
    True -> return ()
    False ->
      isJust . Sync.syncingPeer <$> readTVarIO self.sync >>= \case
        True -> return ()
        False -> revertToMainChain self >> search self []

-- | Note the height that a newly connected peer gave, then start syncing if
-- needed, or request its mempool if in sync.
trySyncingPeer :: BlockStore -> Peer -> IO ()
trySyncingPeer self p = do
  online <- getOnlinePeer self.config.peerMgr p
  let height = maybe 0 (fromIntegral . max 0 . (.startHeight)) (online >>= (.version))
  atomically . modifyTVar' self.knows $
    HashMap.insertWith (\_ k -> Known (max height k.height) k.tip) p.label (Known height Nothing)
  isInSync self >>= \case
    True -> syncMempool self p
    False -> trySyncing self

-- | Release the syncing peer, if any.
clearSyncingState :: BlockStore -> IO ()
clearSyncingState self = do
  transition self $ \s -> ((), Sync.settle s)
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
  atomically $ modifyTVar' self.knows $ HashMap.delete p.label
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
processBlockStoreMessage self (BlockAnnounced p bs) = processAnnounced self p bs
processBlockStoreMessage self (BlockPong p n) = processPong self p n
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
  -- a request that no peer answered, as a node does not for a transaction
  -- that it has not announced to the store, may be made again
  now <- getCurrentTime
  changePending self (Pending.prune now)
  setStoreHeight self
  setHeadersHeight self
  setPendingTxs self
  setPeersConnected self
  trySyncing self
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

-- | Tell the block store that a peer announced blocks.
blockStoreAnnounced :: Peer -> [BlockHash] -> BlockStore -> IO ()
blockStoreAnnounced peer blocks store = BlockAnnounced peer blocks `send` store.mailbox

-- | Tell the block store that a peer answered a ping.
blockStorePong :: Peer -> Word64 -> BlockStore -> IO ()
blockStorePong peer nonce store = BlockPong peer nonce `send` store.mailbox

-- | 'blockStoreAnnounced' in 'STM'.
blockStoreAnnouncedSTM :: Peer -> [BlockHash] -> BlockStore -> STM ()
blockStoreAnnouncedSTM peer blocks store = BlockAnnounced peer blocks `sendSTM` store.mailbox

-- | 'blockStorePong' in 'STM'.
blockStorePongSTM :: Peer -> Word64 -> BlockStore -> STM ()
blockStorePongSTM peer nonce store = BlockPong peer nonce `sendSTM` store.mailbox

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
