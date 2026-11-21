-- |
-- Module      : Haskoin.Store.Ingest.Run
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The runner of "Haskoin.Store.Ingest": the thread that gives the machine
-- its inputs and carries out its outputs. Every decision about a
-- transaction on its way to the pool is the machine's
-- (@docs\/mempool-ingestion.md@); this module decides nothing, and is the
-- part of the path that no proof covers, so it is kept to what a reader
-- can check by eye.
--
-- = What it does
--
-- * __It takes what happens__ from the thread that reads the events of the
--   node (the functions ending in @STM@), and from the clock, once a
--   second, in one queue and so in their order.
--
-- * __It names each connection__ with a number of its own, never used
--   again: what the pool answers about what a peer named is for that
--   connection, never for a later one to the same node (U1).
--
-- * __It carries out each output__: a request or a question to a peer, a
--   question to the pool ('memberSettled'), an offer ('memberOffer'), a
--   count of what left and why. An answer of the pool is an input, given
--   to the machine before the next thing that happened.
--
-- = What it does when it is behind
--
-- Its queue takes any number of the events that are few and must not be
-- lost: a peer that comes or goes, a pool that was emptied. Of the others,
-- what peers name and send, it takes 20,000 and drops the rest, and tells
-- the machine that something was lost, which makes it read its sources
-- again. Waiting instead would hold up the thread that reads the node's
-- events, and its blocks with it.
--
-- While Redis does not answer, an offer fails, and the machine asks for it
-- again at each tick. The runner makes one such attempt a tick, not one
-- for each transaction that is held.
module Haskoin.Store.Ingest.Run
  ( Ingester,
    IngestConfig (..),
    IngestState (..),
    withIngest,
    ingestState,

    -- * What happens
    ingestPeerUpSTM,
    ingestPeerDownSTM,
    ingestNamedSTM,
    ingestTxSTM,
    ingestMissingSTM,
    ingestLookSTM,
    ingestEmptiedSTM,
    ingestSubmit,
  )
where

import Control.Concurrent.Async (link, withAsync)
import Control.Concurrent.STM
  ( STM,
    TQueue,
    TVar,
    atomically,
    check,
    modifyTVar',
    newTQueueIO,
    newTVarIO,
    orElse,
    readTQueue,
    readTVar,
    registerDelay,
    swapTVar,
    writeTQueue,
    writeTVar,
  )
import Control.Monad (forM_, unless, when)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.List (find, nub)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Time.Clock.POSIX (getPOSIXTime)
import GHC.Clock (getMonotonicTimeNSec)
import Haskoin
  ( GetData (..),
    InvType (..),
    InvVector (..),
    Message (..),
    Network (..),
    OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    nullOutPoint,
    txHash,
  )
import Haskoin.Node (LogLevel (..), Logger, Peer, logAt, sendMessage)
import Haskoin.Store.Fleet.Member (Member, memberOffer, memberSettled)
import Haskoin.Store.Ingest
import System.Metrics.StatsD (StatCounter, StatGauge, Stats, incrementCounter, newStatCounter, newStatGauge, setGauge)
import Haskoin.Store.Exception (catchAny)

-- | What the runner needs of the store.
data IngestConfig = IngestConfig
  { net :: !Network,
    -- | the bounds and the waits of the machine
    config :: !Config,
    -- | the store as a member of its fleet, which asks the pool and offers
    -- to it; without one, in tests, nothing is ever settled
    member :: !(Maybe Member),
    -- | whether the store has the blocks of its nodes now
    inSync :: !(IO Bool),
    -- | whether a peer gives its whole mempool when asked, and so is a
    -- source: what the model of its node says
    -- ('Haskoin.Node.judgeMessage')
    source :: !(Peer -> IO Bool),
    stats :: !(Maybe Stats),
    logger :: !Logger
  }

-- | Something that happened, for the runner.
data Event
  = PeerCame !Peer
  | PeerLeft !Peer
  | Names !Peer ![TxHash]
  | Came !(Maybe Peer) !Tx
  | Lacks !Peer ![TxHash]
  | -- | the store may be in sync now, or no longer
    Look
  | PoolLost

-- | The way to tell the runner what happens, and to read what it knows.
data Ingester = Ingester
  { events :: !(TQueue Event),
    -- | how many of the events that may be dropped are in the queue
    waiting :: !(TVar Int),
    -- | how many were dropped since the machine was last told
    dropped :: !(TVar Int),
    state :: !(TVar IngestState)
  }

-- | What the path into the pool is at, for the health check of a store
-- and for whoever looks at it.
data IngestState = IngestState
  { -- | ids that the machine has a place for
    known :: !Int,
    -- | requests out to peers and not answered: what the health check
    -- counts as pending
    pending :: !Int,
    -- | transactions held, to offer or waiting for others
    held :: !Int,
    -- | whether something was given up that no source has been read for
    dirty :: !Bool,
    -- | whether a peer gives its whole mempool when asked. Without one,
    -- what the store misses stays missed until a block has it.
    sourced :: !Bool
  }
  deriving (Eq, Show)

-- | How many events of peers wait at most. Each is the ids that a peer
-- named in a message, or a transaction.
room :: Int
room = 20000

-- | What the path into the pool is at.
ingestState :: Ingester -> STM IngestState
ingestState ing = readTVar ing.state

always :: Event -> Ingester -> STM ()
always e ing = writeTQueue ing.events e

-- | An event that is dropped when @room@ of them wait already.
bulk :: Event -> Ingester -> STM ()
bulk e ing = do
  n <- readTVar ing.waiting
  if n >= room
    then modifyTVar' ing.dropped (+ 1)
    else writeTQueue ing.events e >> writeTVar ing.waiting (n + 1)

-- | A peer finished its handshake.
ingestPeerUpSTM :: Peer -> Ingester -> STM ()
ingestPeerUpSTM = always . PeerCame

-- | A peer is gone.
ingestPeerDownSTM :: Peer -> Ingester -> STM ()
ingestPeerDownSTM = always . PeerLeft

-- | A peer named transactions (@inv@).
ingestNamedSTM :: Peer -> [TxHash] -> Ingester -> STM ()
ingestNamedSTM p hs = bulk (Names p hs)

-- | A peer sent a transaction.
ingestTxSTM :: Peer -> Tx -> Ingester -> STM ()
ingestTxSTM p tx = bulk (Came (Just p) tx)

-- | A peer does not have transactions that it was asked for (@notfound@).
ingestMissingSTM :: Peer -> [TxHash] -> Ingester -> STM ()
ingestMissingSTM p hs = bulk (Lacks p hs)

-- | The store has a new best header or a new best block: it may be in
-- sync now, or no longer.
ingestLookSTM :: Ingester -> STM ()
ingestLookSTM = always Look

-- | The pool lost what it had: Redis lost its data, or the pool was
-- started again. The machine is told the same when the runner dropped
-- events of peers: in both cases something may be missing that only a
-- reading of the sources brings back.
ingestEmptiedSTM :: Ingester -> STM ()
ingestEmptiedSTM = always PoolLost

-- | A transaction that the store has from elsewhere than a peer, such as
-- the RPC of its node, to offer.
ingestSubmit :: Ingester -> Tx -> IO ()
ingestSubmit ing tx = atomically (bulk (Came Nothing tx) ing)

-- | What the runner keeps beside the machine.
data Run = Run
  { machine :: !(Ingest Int TxHash Tx),
    -- | the connections, by the numbers that name them
    peers :: !(Map Int Peer),
    -- | the number of the next connection
    next :: !Int
  }

-- | StatsD metrics of the path, under @ingest@.
data Metrics = Metrics
  { settled :: !StatCounter,
    gaveUp :: !(Map Reason StatCounter),
    droppedEvents :: !StatCounter,
    knownG :: !StatGauge,
    pendingG :: !StatGauge,
    heldG :: !StatGauge
  }

newMetrics :: Stats -> IO Metrics
newMetrics s = do
  settled <- newStatCounter s "ingest.settled" 10
  gaveUp <- Map.fromList <$> mapM reason [minBound ..]
  droppedEvents <- newStatCounter s "ingest.dropped" 10
  knownG <- newStatGauge s "ingest.ids" 0
  pendingG <- newStatGauge s "ingest.pending" 0
  heldG <- newStatGauge s "ingest.held" 0
  return Metrics {settled, gaveUp, droppedEvents, knownG, pendingG, heldG}
  where
    reason r = (r,) <$> newStatCounter s ("ingest.given_up." <> reasonName r) 10

-- | The name of a reason in a metric and in the log.
reasonName :: Reason -> String
reasonName = \case
  NodeHasNot -> "node_has_not"
  Unanswered -> "unanswered"
  NoRoom -> "no_room"
  PeerGone -> "peer_gone"
  WaitedTooLong -> "waited_too_long"
  Unjudged -> "unjudged"

-- | Run the path into the pool while an action runs, and give the action
-- the way to tell it what happens.
withIngest :: IngestConfig -> (Ingester -> IO a) -> IO a
withIngest cfg action = do
  events <- newTQueueIO
  waiting <- newTVarIO 0
  dropped <- newTVarIO 0
  state <- newTVarIO (IngestState 0 0 0 False False)
  metrics <- mapM newMetrics cfg.stats
  now <- milliseconds
  let ing = Ingester {events, waiting, dropped, state}
      run0 = Run {machine = start cfg.config now, peers = Map.empty, next = 0}
  withAsync (loop cfg ing metrics run0) $ \a -> link a >> action ing

-- | The clock of the machine.
milliseconds :: IO Tick
milliseconds = fromIntegral . (`div` 1000000) <$> getMonotonicTimeNSec

-- | The transactions that one spends from.
parentsOf :: Tx -> [TxHash]
parentsOf tx = nub [i.outpoint.hash | i <- tx.inputs, i.outpoint /= nullOutPoint]

loop :: IngestConfig -> Ingester -> Maybe Metrics -> Run -> IO a
loop cfg ing metrics run0 = milliseconds >>= \t -> go False t run0
  where
    -- with whether the machine was last told that the store is in sync,
    -- and when it was last told the time
    go wasSynced lastTick run = do
      before <- milliseconds
      timer <- registerDelay (1000 * max 0 (lastTick + 1000 - before))
      event <-
        atomically $
          (Just <$> readTQueue ing.events) `orElse` (readTVar timer >>= check >> return Nothing)
      lost <- atomically (swapTVar ing.dropped 0)
      when (lost > 0) $ do
        forM_ metrics $ \m -> incrementCounter m.droppedEvents lost
        say LevelWarn $
          "Dropped " <> shown lost <> " messages of peers that waited for the pool: the sources are read again"
      (run', inputs) <- inputsOf run event
      now <- milliseconds
      -- the time is told once a second, however many events come
      let tick = now - lastTick >= 1000
          looks = case event of
            Just Look -> True
            _ -> tick
      -- whether the store is in sync is looked at when it may have changed,
      -- and at every tick
      synced <- if looks then cfg.inSync else return wasSynced
      -- one attempt at the pool a pass while it fails
      failing <- newIORef False
      let all' =
            [Emptied | lost > 0]
              <> [Synced synced | synced /= wasSynced]
              <> inputs
              <> [Tick now | tick]
      machine <- drive run'.peers failing run'.machine all'
      publish machine
      go synced (if tick then now else lastTick) run' {machine}
    -- what an event is to the machine, with the peers named
    inputsOf run = \case
      Nothing -> return (run, [])
      Just e -> do
        when (droppable e) (atomically (modifyTVar' ing.waiting (subtract 1)))
        case e of
          PeerCame p -> do
            src <- cfg.source p
            unless src . say LevelInfo $
              "Peer " <> shown p <> " does not give its mempool when asked: what the store misses of it stays missed"
            return (run {peers = Map.insert run.next p run.peers, next = run.next + 1}, [PeerUp run.next src])
          PeerLeft p -> case keyOf run p of
            Just k -> return (run {peers = Map.delete k run.peers}, [PeerDown k])
            Nothing -> return (run, [])
          Names p hs -> return (run, [Named k hs | Just k <- [keyOf run p]])
          Came from tx -> return (run, [Body (from >>= keyOf run) (txHash tx) tx])
          Lacks p hs -> return (run, [Missing k hs | Just k <- [keyOf run p]])
          Look -> return (run, [])
          PoolLost -> return (run, [Emptied])
    droppable = \case
      Names {} -> True
      Came {} -> True
      Lacks {} -> True
      _ -> False
    keyOf run p = fst <$> find ((== p) . snd) (Map.toList run.peers)
    -- give the machine inputs, and carry out what it says; what the pool
    -- answers goes in after the inputs that are there already
    drive _ _ m [] = return m
    drive peers failing m (i : is) = do
      let (m', outs) = step parentsOf i m
      answers <- concat <$> mapM (perform peers failing) outs
      drive peers failing m' (is <> answers)
    perform peers failing = \case
      Get k hs -> do
        forM_ (Map.lookup k peers) $ \p ->
          MGetData (GetData [InvVector kind h.get | h <- hs]) `sendMessage` p
        return []
      Sweep k -> do
        forM_ (Map.lookup k peers) $ \p -> do
          say LevelInfo ("Asking peer " <> shown p <> " for its mempool")
          MMempool `sendMessage` p
        return []
      Check k hs -> case cfg.member of
        Nothing -> return [Unchecked k hs]
        Just member ->
          memberSettled member hs >>= \case
            Nothing -> return [Unchecked k hs]
            Just flags ->
              return [Checked k [h | (h, True) <- zip hs flags] [h | (h, False) <- zip hs flags]]
      Offer h tx -> do
        down <- readIORef failing
        case cfg.member of
          Just member | not down -> do
            time <- round <$> getPOSIXTime
            verdict <-
              memberOffer member time tx `catchAny` \e -> do
                writeIORef failing True
                say LevelDebug ("Could not offer " <> shown h <> " to the pool: " <> shown e)
                return Again
            return [Offered h verdict]
          _ -> return [Offered h Again]
      Gone h end -> do
        case end of
          Done -> forM_ metrics $ \m -> incrementCounter m.settled 1
          GaveUp why -> do
            forM_ metrics $ \m -> forM_ (Map.lookup why m.gaveUp) (`incrementCounter` 1)
            say LevelDebug ("Gave up " <> shown h <> ": " <> cs (reasonName why))
        return []
    kind = if cfg.net.segWit then InvWitnessTx else InvTx
    publish m = do
      let st =
            IngestState
              { known = Set.size (ids m),
                pending = Haskoin.Store.Ingest.pending m,
                held = holding m,
                dirty = isDirty m,
                sourced = hasSource m
              }
      atomically (writeTVar ing.state st)
      forM_ metrics $ \x -> do
        setGauge x.knownG st.known
        setGauge x.pendingG st.pending
        setGauge x.heldG st.held
    say level = logAt level cfg.logger
    shown :: (Show x) => x -> Text
    shown = cs . show
