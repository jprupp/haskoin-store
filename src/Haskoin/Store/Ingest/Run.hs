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
--   question to the pool ('memberSettled'), an offer ('memberOfferTimed'), a
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
--
-- = A source that is no peer
--
-- A node announces a transaction to a peer once, so what the store misses
-- comes back only from a /source/: something that gives a node's whole
-- mempool when asked. A peer is one when its node allows it, which Bitcoin
-- Core does only for a peer that it whitelists. The RPC of a node always
-- answers, so a store that has it has a source whatever its peers are: it
-- is given to the runner as a t'Feed', with 'ingestFeed'.
--
-- To the machine a feed is one more peer that is a source: it is told that
-- the feed came, asks it for its mempool and for transactions with the
-- same outputs, within the same window, and gets the same inputs back. So
-- nothing of the machine, of its proofs or of its model is particular to a
-- feed. A feed that fails is a peer that went, and one that answers again
-- is a new peer, with a new number (U1).
module Haskoin.Store.Ingest.Run
  ( Ingester,
    IngestConfig (..),
    IngestState (..),
    withIngest,
    ingestState,

    -- * A source that is no peer
    Feed (..),
    ingestFeed,

    -- * What happens
    ingestPeerUpSTM,
    ingestPeerDownSTM,
    ingestNamedSTM,
    ingestTxSTM,
    ingestMissingSTM,
    ingestLookSTM,
    ingestEmptiedSTM,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (link, withAsync)
import Control.Concurrent.STM
  ( STM,
    TQueue,
    TVar,
    atomically,
    check,
    modifyTVar',
    newTQueue,
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
import Control.Exception (evaluate)
import Control.Monad (forM_, unless, when)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.List (find, nub)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word64)
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
import Haskoin.Store.Exception (catchAny, onException, tryAny)
import Haskoin.Store.Fleet.Member (Member, OfferCost (..), memberOfferTimed, memberSettled)
import Haskoin.Store.Ingest
import System.Metrics.StatsD (StatCounter, StatGauge, Stats, incrementCounter, newStatCounter, newStatGauge, setGauge)

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
    -- | whether a peer is a node of the operator, which is taken at its
    -- word ("Haskoin.Store.Ingest"). A feed always is: the operator gave
    -- the store the RPC of that node.
    own :: !(Peer -> IO Bool),
    stats :: !(Maybe Stats),
    logger :: !Logger
  }

-- | A way to read the whole mempool of a node that is no peer of the
-- store, such as its RPC. Both actions throw when the node does not
-- answer, and only then: what they return is the node's word.
data Feed = Feed
  { -- | what it is, for the log, as in "Reading the mempool of …"
    label :: !Text,
    -- | the id of every transaction of the node's mempool; none for an
    -- empty mempool
    names :: !(IO [TxHash]),
    -- | the transactions asked for, in the order asked, with 'Nothing'
    -- for one that the node says it has not
    bodies :: !([TxHash] -> IO [Maybe Tx])
  }

-- | What the runner asks of a feed.
data Job
  = -- | every id of the mempool
    ReadAll
  | Fetch ![TxHash]

-- | What the machine knows as a peer: a connection to a node, or a feed
-- from when it is reached to when it fails, with its number among the
-- feeds, its label and the queue of what is asked of it.
data Link
  = Wire !Peer
  | Fed !Int !Text !(TQueue Job)
  deriving (Eq)

-- | Something that happened, for the runner.
data Event
  = PeerCame !Link
  | PeerLeft !Link
  | Names !Link ![TxHash]
  | Came !Link !Tx
  | Lacks !Link ![TxHash]
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
    state :: !(TVar IngestState),
    -- | the number of the next feed
    feeds :: !(TVar Int),
    logger :: !Logger
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
    -- | how many peers give their whole mempool when asked. With none,
    -- what the store misses stays missed until a block has it.
    sources :: !Int,
    -- | whether the pool is still filling
    -- ('Haskoin.Store.Ingest.isFilling')
    filling :: !Bool
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

-- | An event that waits until there is room for it: for a thread that
-- may wait, as that of a feed, which the reader of the node's events may
-- not.
patient :: Event -> Ingester -> STM ()
patient e ing = do
  n <- readTVar ing.waiting
  check (n < room)
  writeTQueue ing.events e
  writeTVar ing.waiting (n + 1)

-- | A peer finished its handshake.
ingestPeerUpSTM :: Peer -> Ingester -> STM ()
ingestPeerUpSTM = always . PeerCame . Wire

-- | A peer is gone.
ingestPeerDownSTM :: Peer -> Ingester -> STM ()
ingestPeerDownSTM = always . PeerLeft . Wire

-- | A peer named transactions (@inv@).
ingestNamedSTM :: Peer -> [TxHash] -> Ingester -> STM ()
ingestNamedSTM p hs = bulk (Names (Wire p) hs)

-- | A peer sent a transaction.
ingestTxSTM :: Peer -> Tx -> Ingester -> STM ()
ingestTxSTM p tx = bulk (Came (Wire p) tx)

-- | A peer does not have transactions that it was asked for (@notfound@).
ingestMissingSTM :: Peer -> [TxHash] -> Ingester -> STM ()
ingestMissingSTM p hs = bulk (Lacks (Wire p) hs)

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

-- | Give the path into the pool a source that is no peer, for as long as
-- this runs: it does not return.
--
-- The feed is told to the machine as a peer that gives its mempool. What
-- the machine then asks of that peer is asked of the feed, one thing at a
-- time, and what the feed answers goes to the machine as what a peer names
-- and sends. The machine asks within the window that it keeps for every
-- peer, which follows what the feed delivers, so nothing here says how
-- much a node is asked at once or how often.
--
-- When the feed throws, the machine is told that the peer went, which
-- makes it give up what it had asked of it, with a reason, and remember
-- that it has something to read again. The feed is tried again after a
-- second, then two, up to a minute, as a new peer each time (U1).
--
-- Its events wait for room in the queue of the runner and are never
-- dropped: this thread reads nothing else, so making it wait holds up
-- nothing.
ingestFeed :: Ingester -> Feed -> IO a
ingestFeed ing feed = connect 1 False
  where
    -- with the seconds to wait if it fails at once, and whether it failed
    -- the last time
    connect wait down = do
      (me, jobs) <- atomically $ do
        n <- readTVar ing.feeds
        writeTVar ing.feeds (n + 1)
        jobs <- newTQueue
        let me = Fed n feed.label jobs
        always (PeerCame me) ing
        return (me, jobs)
      let gone = atomically (always (PeerLeft me) ing)
      (why, still) <- serve me jobs down `onException` gone
      gone
      let pause = if still then wait else 1
      unless still . say LevelWarn $
        "Could not read " <> feed.label <> ", and will try again: " <> cs (show why)
      threadDelay (pause * 1000000)
      connect (min 60 (pause * 2)) True
    -- carry out what is asked until something fails: the failure, and
    -- whether nothing worked since the last one
    serve me jobs down = do
      job <- atomically (readTQueue jobs)
      tryAny (carry me job) >>= \case
        Left e -> return (e, down)
        Right () -> do
          when down (say LevelInfo ("Reading " <> feed.label <> " works again"))
          serve me jobs False
    carry me = \case
      ReadAll -> do
        hs <- feed.names
        forM_ (pieces hs) $ \some -> atomically (patient (Names me some) ing)
      Fetch hs -> do
        found <- feed.bodies hs
        forM_ found . mapM_ $ \tx -> atomically (patient (Came me tx) ing)
        let lacking = [h | (h, Nothing) <- zip hs found]
        unless (null lacking) (atomically (patient (Lacks me lacking) ing))
    -- as many ids to an event as a node names in a message
    pieces hs
      | null hs = []
      | otherwise = take 50000 hs : pieces (drop 50000 hs)
    say level = logAt level ing.logger

-- | What the runner keeps beside the machine.
data Run = Run
  { machine :: !(Ingest Int TxHash Tx),
    -- | the connections and the feeds, by the numbers that name them
    peers :: !(Map Int Link),
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
  IsForged -> "forged"
  NotVouched -> "not_vouched"

-- | Run the path into the pool while an action runs, and give the action
-- the way to tell it what happens.
withIngest :: IngestConfig -> (Ingester -> IO a) -> IO a
withIngest cfg action = do
  events <- newTQueueIO
  waiting <- newTVarIO 0
  dropped <- newTVarIO 0
  state <- newTVarIO (IngestState 0 0 0 False 0 False)
  metrics <- mapM newMetrics cfg.stats
  feeds <- newTVarIO 0
  now <- milliseconds
  let ing = Ingester {events, waiting, dropped, state, feeds, logger = cfg.logger}
      run0 = Run {machine = start cfg.config now, peers = Map.empty, next = 0}
  withAsync (loop cfg ing metrics run0) $ \a -> link a >> action ing

-- | The clock of the machine.
milliseconds :: IO Tick
milliseconds = fromIntegral . (`div` 1000000) <$> getMonotonicTimeNSec

-- | The transactions that one spends from.
parentsOf :: Tx -> [TxHash]
parentsOf tx = nub [i.outpoint.hash | i <- tx.inputs, i.outpoint /= nullOutPoint]

-- | Where the time of the runner went since it last said, in nanoseconds,
-- and what it did meanwhile.
data Costs = Costs
  { -- | events taken in
    passes :: !Int,
    offersMade :: !Int,
    checksMade :: !Int,
    -- | waiting for an event
    idle :: !Word64,
    -- | in the steps of the machine
    inMachine :: !Word64,
    inOffers :: !Word64,
    -- | asking the pool which of the ids that a peer named it has
    inChecks :: !Word64,
    -- | sending requests to peers
    inSends :: !Word64,
    -- | saying the state of the machine, for @\/health@ and the metrics
    inPublish :: !Word64,
    offerParts :: !OfferCost
  }

noCosts :: Costs
noCosts = Costs 0 0 0 0 0 0 0 0 0 mempty

loop :: IngestConfig -> Ingester -> Maybe Metrics -> Run -> IO a
loop cfg ing metrics run0 = do
  t <- milliseconds
  costs <- newIORef noCosts
  saidAt <- newIORef t
  loopWith costs saidAt cfg ing metrics run0 t

loopWith :: IORef Costs -> IORef Tick -> IngestConfig -> Ingester -> Maybe Metrics -> Run -> Tick -> IO a
loopWith costs saidAt cfg ing metrics run0 t0 = go False t0 run0
  where
    -- the time that an action takes, added to what the runner spent
    timed :: (Costs -> Word64 -> Costs) -> IO x -> IO x
    timed add act = do
      a <- getMonotonicTimeNSec
      r <- act
      b <- getMonotonicTimeNSec
      modifyIORef' costs (`add` (b - a))
      return r
    -- Say where the time went, every ten seconds in which the runner was
    -- busy for five or more: a store that is slow to fill its pool then
    -- says why, which nothing outside it can tell.
    account now m = do
      since <- readIORef saidAt
      when (now - since >= 10000) $ do
        c <- readIORef costs
        writeIORef costs noCosts
        writeIORef saidAt now
        let ms :: Word64 -> Text
            ms ns = shown (ns `div` 1000000)
            busy = c.inMachine + c.inOffers + c.inChecks + c.inSends + c.inPublish
            o = c.offerParts
        when (busy >= 5000000000) . say LevelInfo . T.unwords $
          [ "The path into the pool was busy for",
            ms busy,
            "of the last",
            shown (now - since) <> " ms:",
            shown c.passes,
            "events,",
            shown c.offersMade,
            "offers,",
            shown c.checksMade,
            "questions to the pool. In milliseconds: machine",
            ms c.inMachine <> ", offers",
            ms c.inOffers,
            "(whether a block has it",
            ms o.askedBlock <> ", outputs from the database",
            ms o.readDb <> ", outputs from the pool",
            ms o.readPool <> ", which are unspent",
            ms o.readUnspent <> ", the pool's decision",
            ms o.decided <> ", looking at what strangers sent",
            ms o.judged <> "), questions",
            ms c.inChecks <> ", sending",
            ms c.inSends <> ", saying its state",
            ms c.inPublish <> ", waiting",
            ms c.idle <> ". It knows",
            shown (idCount m),
            "ids and holds",
            shown (holding m) <> "."
          ]
    -- with whether the machine was last told that the store is in sync,
    -- and when it was last told the time
    go wasSynced lastTick run = do
      before <- milliseconds
      timer <- registerDelay (1000 * max 0 (lastTick + 1000 - before))
      event <-
        timed (\c d -> c {idle = c.idle + d, passes = c.passes + 1}) . atomically $
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
      -- what the machine gives up in this pass, by reason
      lostNow <- newIORef Map.empty
      let all' =
            [Emptied | lost > 0]
              <> [Synced synced | synced /= wasSynced]
              <> inputs
              <> [Tick now | tick]
      machine <- drive run'.peers failing lostNow run'.machine all'
      timed (\c d -> c {inPublish = c.inPublish + d}) (publish machine)
      -- A source is read every thirty seconds while transactions come
      -- (@told@ in "Haskoin.Store.Ingest"), and a reading goes through
      -- every transaction of a mempool. What that costs is said when it
      -- is a second or more, so that a store for which it is too much
      -- shows it.
      done <- milliseconds
      case event of
        Just (Names _ hs)
          | done - now >= 1000,
            length hs >= 1000 ->
              say LevelInfo . T.unwords $
                [ "Went through",
                  shown (length hs),
                  "transactions that a node named at once in",
                  shown (done - now),
                  "ms"
                ]
        _ -> return ()
      -- what was given up and that a node may still have is said, once a
      -- pass: it makes the store read its sources again, and without the
      -- reason nobody can tell why it did
      gaveUp <- Map.toList . Map.delete NodeHasNot <$> readIORef lostNow
      unless (null gaveUp) . say LevelInfo $
        "Gave up transactions on their way to the pool, and will read the sources again: "
          <> T.intercalate ", " [shown n <> " " <> cs (reasonName why) | (why, n) <- gaveUp]
      account done machine
      go synced (if tick then now else lastTick) run' {machine}
    -- what an event is to the machine, with the peers named
    inputsOf run = \case
      Nothing -> return (run, [])
      Just e -> do
        when (droppable e) (atomically (modifyTVar' ing.waiting (subtract 1)))
        case e of
          PeerCame p -> do
            (src, ours) <- case p of
              Wire peer -> do
                src <- cfg.source peer
                unless src . say LevelInfo . T.unwords $
                  [ "Peer " <> shown peer <> " does not give its mempool when asked:",
                    "what the store misses of it stays missed"
                  ]
                ours <- cfg.own peer
                -- said for each peer, so that an operator sees that the
                -- store knows its node: a node taken for a stranger
                -- shows late what the store cannot verify
                say LevelInfo $
                  if ours
                    then "Peer " <> shown peer <> " is a node of the operator: what it names is taken at its word"
                    else "Peer " <> shown peer <> " is a stranger: what only strangers name is verified first"
                return (src, ours)
              -- a feed is nothing but a source, of a node of the operator
              Fed {} -> return (True, True)
            return
              ( run {peers = Map.insert run.next p run.peers, next = run.next + 1},
                PeerUp run.next src : [Own run.next | ours]
              )
          PeerLeft p -> case keyOf run p of
            Just k -> return (run {peers = Map.delete k run.peers}, [PeerDown k])
            Nothing -> return (run, [])
          Names p hs -> return (run, [Named k hs | Just k <- [keyOf run p]])
          Came from tx -> return (run, [Body (keyOf run from) (txHash tx) tx])
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
    drive _ _ _ m [] = return m
    drive peers failing lostNow m (i : is) = do
      -- the step is made here, and what it says counted, so that its
      -- time is not taken for that of what is done with it
      (m', outs) <- timed (\c d -> c {inMachine = c.inMachine + d}) $ do
        let (next, said) = step parentsOf i m
        _ <- evaluate (length said)
        next' <- evaluate next
        return (next', said)
      answers <- concat <$> mapM (perform m' peers failing lostNow) outs
      drive peers failing lostNow m' (is <> answers)
    perform m peers failing lostNow = \case
      Get k hs -> timed (\c d -> c {inSends = c.inSends + d}) $ do
        forM_ (Map.lookup k peers) $ \case
          Wire p -> MGetData (GetData [InvVector kind h.get | h <- hs]) `sendMessage` p
          Fed _ _ jobs -> atomically (writeTQueue jobs (Fetch hs))
        return []
      Sweep k -> do
        forM_ (Map.lookup k peers) $ \case
          Wire p -> do
            say LevelInfo ("Asking peer " <> shown p <> " for its mempool")
            MMempool `sendMessage` p
          Fed _ label jobs -> do
            say LevelInfo ("Reading " <> label)
            atomically (writeTQueue jobs ReadAll)
        return []
      Check k hs -> case cfg.member of
        Nothing -> return [Unchecked k hs]
        Just member ->
          timed asked (memberSettled member hs) >>= \case
            Nothing -> return [Unchecked k hs]
            Just flags ->
              return [Checked k [h | (h, True) <- zip hs flags] [h | (h, False) <- zip hs flags]]
      Offer h tx -> do
        down <- readIORef failing
        case cfg.member of
          Just member | not down -> do
            time <- round <$> getPOSIXTime
            (verdict, parts) <-
              timed (\c d -> c {inOffers = c.inOffers + d, offersMade = c.offersMade + 1}) $
                memberOfferTimed member (vouchedFor h m) time tx `catchAny` \e -> do
                  writeIORef failing True
                  say LevelDebug ("Could not offer " <> shown h <> " to the pool: " <> shown e)
                  return (Again, mempty)
            modifyIORef' costs (\c -> c {offerParts = c.offerParts <> parts})
            return [Offered h verdict]
          _ -> return [Offered h Again]
      Gone h end -> do
        case end of
          Done -> forM_ metrics $ \m -> incrementCounter m.settled 1
          GaveUp why -> do
            modifyIORef' lostNow (Map.insertWith (+) why (1 :: Int))
            forM_ metrics $ \m -> forM_ (Map.lookup why m.gaveUp) (`incrementCounter` 1)
            say LevelDebug ("Gave up " <> shown h <> ": " <> cs (reasonName why))
        return []
    asked c d = c {inChecks = c.inChecks + d, checksMade = c.checksMade + 1}
    kind = if cfg.net.segWit then InvWitnessTx else InvTx
    publish m = do
      let st =
            IngestState
              { known = idCount m,
                pending = Haskoin.Store.Ingest.pending m,
                held = holding m,
                dirty = isDirty m,
                sources = sourceCount m,
                filling = isFilling m
              }
      atomically (writeTVar ing.state st)
      forM_ metrics $ \x -> do
        setGauge x.knownG st.known
        setGauge x.pendingG st.pending
        setGauge x.heldG st.held
    say level = logAt level cfg.logger
    shown :: (Show x) => x -> Text
    shown = cs . show
