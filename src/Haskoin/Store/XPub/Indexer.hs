{-# LANGUAGE MultiWayIf #-}

-- |
-- Module      : Haskoin.Store.XPub.Indexer
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The indexer of xpubs: threads that read xpubs from the database into a
-- cache in the background, so that no request reads an xpub itself
-- (@docs\/xpub-indexing.md@, The indexer).
--
-- A request for an xpub that the cache does not have hands it to the
-- indexer ('request'), and either waits for the job to be done, up to a
-- set time, or is told when to come back. The work goes on either way, so
-- the client that comes back finds the xpub in the cache. The queue can
-- also take upkeep, behind the jobs that requests wait for ('upkeep'); the
-- store queues none, as a move of the fleet tip updates the cache in Redis
-- itself ("Haskoin.Store.Fleet.Member").
--
-- = The queue
--
-- First in, first out, with no key in it twice, in two parts: the jobs that
-- a request asked for, then upkeep. A request for a key that is already
-- queued or running joins that job; one for a key that is queued for
-- upkeep moves it to the end of the first part. Upkeep for a key that is
-- queued adds to that job ('Semigroup' of the payload); for a key whose job
-- is running, it queues the key again for when the job is done, since that
-- job may have read the database before the change. A key never runs in two
-- threads at once, so two jobs of one xpub never race to write it.
--
-- = Deciding
--
-- When a request does not find its key in the queue, the store estimates
-- when a new job would be done ('estimate'): (place ÷ threads + 1) × the
-- median time of recent jobs. That is rough, since the size of an xpub is
-- not known until it is read, but it only has to tell soon from later.
-- 'decide' then picks one of three, a pure function of the estimate, the
-- wait, the horizon, the room in the queue and the lock:
--
-- * __wait__: queue and lock the key, or join its job, and wait for it;
-- * __refuse and keep the job__: queue and lock it, and answer 429 with
--   @Retry-After@ from the estimate;
-- * __refuse and take nothing__: beyond the horizon, with a full queue, or
--   with the key locked by another store of the fleet, which says when it
--   expects to be done.
--
-- The lock ("Haskoin.Store.XPub.Lock") is taken when the key is queued,
-- not when the job starts, renewed while the store holds the job, and let
-- go when the job is done. A store that loses the race for it takes
-- nothing.
--
-- = Waiting without timers
--
-- A request that waits sets no timer: it waits on a variable of its own,
-- which the job sets when it is done, and a sweeper thread, which sleeps in
-- the system ten times a second, refuses the requests whose time is up
-- ('sweep'). Every timer of a program, @threadDelay@, @registerDelay@ and
-- @timeout@ included, goes through one thread of GHC, its timer manager,
-- which was seen stuck for seconds under load ("Haskoin.Store.Web.Admission"
-- says how). Sleeping in a safe foreign call does not depend on it.
--
-- = What is a parameter
--
-- The job itself, a t'Runner': reading an xpub and writing it to Redis
-- ("Haskoin.Store.Fleet.XPub"); and
-- a t'Budget' per job, which it settles at its safe points and tells its
-- size through, from which the indexer estimates when it will be done.
module Haskoin.Store.XPub.Indexer
  ( -- * Settings
    Settings (..),
    defaultSettings,

    -- * The jobs
    Runner (..),
    plainRunner,

    -- * The indexer
    Indexer,
    newIndexer,
    newIndexerWith,
    withIndexer,
    runIndexer,

    -- * Asking for keys
    Answer (..),
    Refusal (..),
    request,
    requestWithin,
    prefetch,
    upkeep,
    retryAfter,

    -- * Deciding
    Holder (..),
    Situation (..),
    Decision (..),
    decide,
    estimate,
    median,

    -- * Watching
    IndexerStats (..),
    Counts (..),
    Refusals (..),
    indexerStats,
    indexerIdle,

    -- * Paying

    -- * Steps
    sweep,
    renewHeld,
    runNext,
    snooze,
  )
where

import Control.Concurrent (getNumCapabilities)
import Control.Concurrent.Async (link, replicateConcurrently_, withAsync)
import Control.Concurrent.STM
  ( STM,
    TVar,
    atomically,
    modifyTVar',
    newTVar,
    newTVarIO,
    readTVar,
    retry,
    writeTVar,
  )
import Control.Exception (SomeException, mask)
import Control.Monad (forM_, replicateM_, unless, void, when)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.Hashable (Hashable)
import Data.List (sort)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, maybeToList)
import Data.Sequence (Seq, ViewL (..), (|>))
import Data.Sequence qualified as Seq
import Foreign.C.Types (CInt (..), CUInt (..))
import GHC.Clock (getMonotonicTimeNSec)
import Haskoin.Store.Common (loopForever)
import Haskoin.Store.Exception (catchAny, finally, onException, tryAny)
import Haskoin.Store.XPub (Budget (..), noBudget)
import Haskoin.Store.XPub.Lock (Lock (..), Taken (..))

-------------------------------------------------------------------------------
-- Settings
-------------------------------------------------------------------------------

-- | Settings of the indexer. Times are in microseconds.
data Settings = Settings
  { -- | threads that run jobs; zero or less for one per core. They spend
    -- much of their time waiting for RocksDB, so one per core keeps the
    -- disk busy
    workers :: !Int,
    -- | how long a request may wait for its job
    wait :: !Int,
    -- | beyond this estimate, the store takes no new job
    horizon :: !Int,
    -- | most jobs that requests asked for in the queue; upkeep has no limit
    -- but the number of cached xpubs, since no key is queued twice
    room :: !Int,
    -- | the median time of a job before any has been timed
    guess :: !Int
  }
  deriving (Eq, Show)

-- | One thread per core, a wait of ten seconds, a horizon of thirty, room
-- for a thousand jobs, and a first guess of a tenth of a second a job.
defaultSettings :: Settings
defaultSettings =
  Settings
    { workers = 0,
      wait = 10000000,
      horizon = 30000000,
      room = 1000,
      guess = 100000
    }

-- | How many of the latest jobs that requests asked for make the median.
recentJobs :: Int
recentJobs = 31

-------------------------------------------------------------------------------
-- The jobs
-------------------------------------------------------------------------------

-- | What the indexer does with a key, and with the payload that came with
-- it: whatever the cache needs to know, such as the changes that upkeep is
-- for. Payloads of one key are joined with '<>'.
--
-- The fields are lazy, so that a cache can make a runner of itself before
-- it is built ('System.IO.fixIO').
data Runner k p = Runner
  { -- | read the key and write it to the cache, paying from the budget;
    -- it throws if it fails. The requests that wait read the cache once it
    -- is done, as every request does: nothing goes from the job to them
    index :: Budget -> k -> p -> IO (),
    -- | the budget of a job
    budget :: k -> IO Budget,
    -- | whether the store may take jobs: a store that is not in sync with
    -- its node takes none, since it would write what knows less than the
    -- others
    ready :: IO Bool,
    -- | a job that will not run, because the store is not ready: its
    -- payload is given back
    abandon :: k -> p -> IO (),
    -- | another store holds the lock of the key, and expects to be done in
    -- so many microseconds: the payload of the job that was not queued is
    -- given back
    blocked :: k -> p -> Int -> IO (),
    -- | a job threw, or its lock could not be taken
    failed :: k -> p -> SomeException -> IO (),
    -- | called every time the locks are renewed, with the jobs that this
    -- store holds, queued or running
    renew :: [(k, p)] -> IO ()
  }

-- | A runner that only runs the job: it is always ready, pays nothing, and
-- has nothing to give back.
plainRunner :: (Budget -> k -> p -> IO ()) -> Runner k p
plainRunner f =
  Runner
    { index = f,
      budget = \_ -> return noBudget,
      ready = return True,
      abandon = \_ _ -> return (),
      blocked = \_ _ _ -> return (),
      failed = \_ _ _ -> return (),
      renew = \_ -> return ()
    }

-- | Where a job is.
data Stage p
  = -- | a request or upkeep reserved it, and is taking its lock
    Pending
  | -- | in the queue
    Queued
  | -- | being run, since a time if known, with upkeep that came meanwhile,
    -- to run again once it is done
    Running !(Maybe Int) !(Maybe p)
  | -- | done, with what came of it, while its lock is let go, with upkeep
    -- that came meanwhile, to queue again once the lock is gone
    Finishing !Answer !(Maybe p)

-- | A job of the indexer.
data Job p = Job
  { payload :: !p,
    -- | whether a request asked for it: then it is in the first part
    asked :: !Bool,
    -- | the requests that wait for it
    waiters :: ![TVar Verdict],
    stage :: !(Stage p)
  }

-- | The queue: the keys of each part, in order, and every job, queued,
-- pending or running.
data Queue k p = Queue
  { first :: !(Seq k),
    second :: !(Seq k),
    jobs :: !(HashMap k (Job p))
  }

-- | What a waiting request was told, if anything yet.
data Verdict = Waiting | Answered !Answer

-------------------------------------------------------------------------------
-- Answers and decisions
-------------------------------------------------------------------------------

-- | What became of a request for a key.
data Answer
  = -- | the job wrote the key to the cache: the request reads it there
    Indexed
  | -- | refused, with how many microseconds to wait before asking again
    Refused !Refusal !Int
  | -- | the job failed, or its lock could not be taken
    Failed
  deriving (Eq, Show)

-- | Why a request was refused.
data Refusal
  = -- | another store of the fleet holds the lock of the key
    OtherStore
  | -- | the queue has no room for another job
    QueueFull
  | -- | a new job would be done later than the horizon
    BeyondHorizon
  | -- | the job would be done later than the request may wait; it goes on
    TooLong
  | -- | the request waited as long as it may; the job goes on
    WaitedOut
  | -- | the store is not in sync with its node, and takes no job
    NotInSync
  deriving (Eq, Show, Enum, Bounded)

-- | Seconds for @Retry-After@ from microseconds: rounded up, at least one.
retryAfter :: Int -> Int
retryAfter us = max 1 ((us + 999999) `div` 1000000)

-- | Who has the job of a key.
data Holder
  = -- | nobody
    Nobody
  | -- | this store, queued or running
    ThisStore
  | -- | another store of the fleet, which expects to be done in so many
    -- microseconds
    AnotherStore !Int
  deriving (Eq, Show)

-- | What a decision is made from. Times are in microseconds. The fields
-- are named apart from those of t'Settings', so that an update of either is
-- never ambiguous.
data Situation = Situation
  { -- | when the job would be done: a new one, or the one of this store
    expected :: !Int,
    -- | how long the request may wait: the wait of the settings, or none
    patience :: !Int,
    -- | the horizon: beyond this estimate, take no new job
    reach :: !Int,
    -- | whether the queue has room for a new job
    hasRoom :: !Bool,
    holder :: !Holder
  }
  deriving (Eq, Show)

-- | What to do with a request for a key that the cache does not have.
data Decision
  = -- | queue and lock a new job, or join the job of this store, and wait
    Wait
  | -- | queue and lock a new job, or keep the one of this store, and
    -- refuse with this @Retry-After@ in microseconds: the estimate
    RefuseKeep !Int
  | -- | take nothing, and refuse with this @Retry-After@: from the lock of
    -- another store, or the horizon
    RefuseNothing !Refusal !Int
  deriving (Eq, Show)

-- | The decision of @docs\/xpub-indexing.md@ (The door):
--
-- +------------------------+---------------------------+------------------------------------+
-- | Holder                 | Estimate                  | Decision                           |
-- +========================+===========================+====================================+
-- | another store          | any                       | take nothing, retry from its lock  |
-- +------------------------+---------------------------+------------------------------------+
-- | this store             | within the wait           | join and wait                      |
-- +------------------------+---------------------------+------------------------------------+
-- | this store             | beyond the wait           | keep the job, retry from estimate  |
-- +------------------------+---------------------------+------------------------------------+
-- | nobody, no room        | any                       | take nothing, retry at the horizon |
-- +------------------------+---------------------------+------------------------------------+
-- | nobody                 | beyond the horizon        | take nothing, retry at the horizon |
-- +------------------------+---------------------------+------------------------------------+
-- | nobody                 | within the wait           | queue, lock and wait               |
-- +------------------------+---------------------------+------------------------------------+
-- | nobody                 | within the horizon        | queue, lock, retry from estimate   |
-- +------------------------+---------------------------+------------------------------------+
--
-- A store that takes nothing tells its client to come back at the horizon:
-- by then this store, or another store of the fleet, may have room. In a
-- fleet the estimate of a job that a store holds is the one in its lock in
-- Redis, which every store reads, and the door decides from it before it
-- asks ('Haskoin.Store.XPub.Service.xpubAsk', 'requestWithin').
decide :: Situation -> Decision
decide s = case s.holder of
  AnotherStore t -> RefuseNothing OtherStore (max 0 t)
  ThisStore
    | s.expected <= s.patience -> Wait
    | otherwise -> RefuseKeep s.expected
  Nobody
    | not s.hasRoom -> RefuseNothing QueueFull s.reach
    | s.expected > s.reach -> RefuseNothing BeyondHorizon s.reach
    | s.expected <= s.patience -> Wait
    | otherwise -> RefuseKeep s.expected

-- | When a job at this place in the queue should be done, in the unit of
-- the median: the jobs ahead of it, shared between the threads, and then
-- its own, @(place ÷ threads + 1) × median@.
estimate :: Int -> Int -> Int -> Int
estimate med threads place = med + place * med `div` max 1 threads

-- | The median of some times, or the guess if there are none.
median :: Int -> Seq Int -> Int
median guess xs
  | Seq.null xs = guess
  | otherwise = sort (foldr (:) [] xs) !! (Seq.length xs `div` 2)

-------------------------------------------------------------------------------
-- The indexer
-------------------------------------------------------------------------------

-- | Requests refused since the start, by why.
data Refusals = Refusals
  { otherStore :: !Int,
    queueFull :: !Int,
    beyondHorizon :: !Int,
    tooLong :: !Int,
    waitedOut :: !Int,
    notInSync :: !Int
  }
  deriving (Eq, Show)

-- | What the indexer has done since the start.
data Counts = Counts
  { -- | jobs done
    done :: !Int,
    -- | jobs that threw
    errors :: !Int,
    -- | jobs dropped because the store was not ready
    dropped :: !Int,
    -- | requests answered after waiting
    answered :: !Int,
    -- | locks found taken by another store when they were renewed
    lost :: !Int,
    -- | requests refused, by why
    refused :: !Refusals,
    -- | prefetches that queued a job or found one: they are not refusals,
    -- since nobody waits for them
    prefetched :: !Int,
    -- | prefetches that took nothing: beyond the horizon, with a full queue,
    -- with the key locked by another store, or with the store not ready
    declined :: !Int
  }
  deriving (Eq, Show)

-- | The indexer of keys of type @k@, with payloads of type @p@.
data Indexer k p = Indexer
  { settings :: !Settings,
    -- | microseconds of a monotonic clock; the tests have one that they
    -- move by hand
    clock :: !(IO Int),
    lock :: !(Lock k),
    runner :: Runner k p,
    queue :: !(TVar (Queue k p)),
    -- | the requests that wait, by when their time is up and a serial
    -- number, with their keys
    deadlines :: !(TVar (Map (Int, Int) (k, TVar Verdict))),
    serial :: !(TVar Int),
    -- | microseconds of the latest jobs that requests asked for
    times :: !(TVar (Seq Int)),
    -- | the size of each running job, in transactions, that told it
    sizes :: !(TVar (HashMap k Int)),
    -- | microseconds per thousand transactions of the latest jobs that
    -- told their size
    rates :: !(TVar (Seq Int)),
    counts :: !(TVar Counts)
  }

-- | An indexer on the monotonic clock of the system. Nothing runs until
-- 'runIndexer' does, or use 'withIndexer'.
newIndexer :: Settings -> Lock k -> Runner k p -> IO (Indexer k p)
newIndexer = newIndexerWith ((`div` 1000) . fromIntegral <$> getMonotonicTimeNSec)

-- | An indexer on this clock, which gives microseconds.
newIndexerWith :: IO Int -> Settings -> Lock k -> Runner k p -> IO (Indexer k p)
newIndexerWith clock s lock runner = do
  n <- if s.workers > 0 then return s.workers else getNumCapabilities
  Indexer s {workers = n, room = max 0 s.room, wait = max 0 s.wait} clock lock runner
    <$> newTVarIO (Queue Seq.empty Seq.empty HashMap.empty)
    <*> newTVarIO Map.empty
    <*> newTVarIO 0
    <*> newTVarIO Seq.empty
    <*> newTVarIO HashMap.empty
    <*> newTVarIO Seq.empty
    <*> newTVarIO (Counts 0 0 0 0 0 (Refusals 0 0 0 0 0 0) 0 0)

-- | Run an indexer while an action runs: its threads, its sweeper and the
-- renewal of its locks.
withIndexer :: (Hashable k) => Settings -> Lock k -> Runner k p -> (Indexer k p -> IO a) -> IO a
withIndexer s lock runner action = do
  i <- newIndexer s lock runner
  withAsync (runIndexer i) $ \a -> link a >> action i

-- | Run the threads of an indexer, its sweeper and the renewal of its
-- locks, forever.
runIndexer :: (Hashable k) => Indexer k p -> IO ()
runIndexer i =
  withAsync sweeper $ \a1 ->
    withAsync renewer $ \a2 -> do
      link a1
      link a2
      replicateConcurrently_ i.settings.workers (loopForever (next i >>= run i))
  where
    sweeper = loopForever (snooze 100000 >> sweep i)
    renewer = loopForever (nap i.lock.every >> renewHeld i)

-- | Sleep so many microseconds, up to a second, in the system: a safe
-- foreign call, which gives the capability back while it sleeps, and sets
-- no timer of GHC ('Haskoin.Store.Web.Congestion.snooze' is the same).
snooze :: Int -> IO ()
snooze us = void (c_usleep (fromIntegral (max 0 (min 999999 us))))

foreign import ccall safe "usleep" c_usleep :: CUInt -> IO CInt

-- | Sleep so many microseconds, a tenth of a second at a time.
nap :: Int -> IO ()
nap us = replicateM_ (max 1 (us `div` 100000)) (snooze 100000)

-------------------------------------------------------------------------------
-- Asking
-------------------------------------------------------------------------------

-- | What a request does after it has looked at the queue.
data Step
  = -- | it has its answer
    Done !Answer
  | -- | it waits for a job
    Await !(Int, Int) !(TVar Verdict)
  | -- | it reserved a new job, and takes its lock, then waits, or answers
    Take !Decision !(Maybe ((Int, Int), TVar Verdict)) !Int

-- | Ask for a key that the cache does not have, as a request does: join
-- its job, or queue a new one, and wait for it, or be refused ('decide').
-- 'Indexed' means that the cache has the key now, and the request reads it
-- there, as every request does. Each refusal is counted by why, in a
-- t'Refusals'.
request :: (Hashable k, Semigroup p) => Indexer k p -> k -> p -> IO Answer
request i = asking False i i.settings.wait

-- | 'request', for a door that decided from the estimate in the lock of
-- the key, which every store of a fleet reads, that the request waits: it
-- joins a job of this store and waits, whatever this store's own estimate,
-- so that every store gives the same answer from the one estimate. It
-- waits at most so many microseconds, and no longer than the wait of the
-- settings, as for a request that waited for another store already and
-- takes the job over when that store let go of it. A new job is decided as
-- 'request' decides it.
requestWithin :: (Hashable k, Semigroup p) => Indexer k p -> Int -> k -> p -> IO Answer
requestWithin i = asking True i

-- | 'request' and 'requestWithin'.
asking :: (Hashable k, Semigroup p) => Bool -> Indexer k p -> Int -> k -> p -> IO Answer
asking decided i wait k p = do
  a <- ask decided i (max 0 (min wait i.settings.wait)) k p
  case a of
    Refused why _ -> atomically (refuse i why)
    _ -> return ()
  return a

-- | Ask for a key without waiting: queue it if a request could, and answer
-- at once. For a request that was answered from elsewhere and wants the key
-- in the cache for next time. Nobody waits for the answer, so it is not a
-- refusal when it says to come back: it counts as 'prefetched' if a job is
-- queued for the key, and as 'declined' if not.
prefetch :: (Hashable k, Semigroup p) => Indexer k p -> k -> p -> IO Answer
prefetch i k p = do
  a <- ask False i 0 k p
  atomically . modifyTVar' i.counts $ \c -> case a of
    Refused TooLong _ -> c {prefetched = c.prefetched + 1}
    Refused _ _ -> c {declined = c.declined + 1}
    _ -> c
  return a

-- | Ask for a key, waiting so many microseconds at most. It counts nothing:
-- 'request' and 'prefetch' count what they were told.
--
-- The reservation of a new job ('Pending') and the attempt to take its lock
-- run masked, and an exception that comes while the lock is being taken
-- undoes the reservation ('abandonReserved'): it answers the requests that
-- joined the job, and lets go of the lock if it was taken. Only waiting and
-- the call to Redis can be interrupted. A request killed between the two,
-- by its client going away or by a sibling of @mapConcurrently@ that
-- failed, once left the job 'Pending' for good, and every later request
-- for the key waited on it in vain.
ask :: (Hashable k, Semigroup p) => Bool -> Indexer k p -> Int -> k -> p -> IO Answer
ask decided i wait k p =
  i.runner.ready >>= \case
    False -> return (Refused NotInSync i.settings.horizon)
    True -> mask $ \restore -> do
      now <- i.clock
      step <- atomically $ do
        q <- readTVar i.queue
        med <- median i.settings.guess <$> readTVar i.times
        expected <- expectedOf i k
        case HashMap.lookup k q.jobs of
          Just Job {stage = Finishing a _} -> return (Done a)
          Just job -> do
            let est = joinEstimate i.settings.workers q med expected now k job
            case if decided && wait > 0 then Wait else decision est True ThisStore of
              Wait -> do
                (key, v) <- waiter i k (now + wait)
                joinJob i k p (Just v)
                return (Await key v)
              RefuseKeep e -> do
                joinJob i k p Nothing
                return (Done (Refused TooLong e))
              RefuseNothing why e -> return (Done (Refused why e))
          Nothing -> do
            let place = Seq.length q.first
                est = estimate med i.settings.workers place
            case decision est (place < i.settings.room) Nobody of
              RefuseNothing why e -> return (Done (Refused why e))
              d -> do
                w <- if d == Wait then Just <$> waiter i k (now + wait) else return Nothing
                let job = Job {payload = p, asked = True, waiters = maybeToList (snd <$> w), stage = Pending}
                writeTVar i.queue q {jobs = HashMap.insert k job q.jobs}
                return (Take d w est)
      case step of
        Done a -> return a
        Await key v -> restore (await i key v)
        Take d w est ->
          (restore (acquireFor i k est) `onException` abandonReserved i k) >>= \case
            Just a -> do
              forM_ w (unwait i . fst)
              return a
            Nothing -> case (d, w) of
              (Wait, Just (key, v)) -> restore (await i key v)
              (RefuseKeep e, _) -> return (Refused TooLong e)
              _ -> return Failed
  where
    -- a request that may not wait does not, even for a job due at once
    decision est room holder =
      case decide Situation {expected = est, patience = wait, reach = i.settings.horizon, hasRoom = room, holder} of
        Wait | wait <= 0 -> RefuseKeep est
        d -> d

-- | Queue a key for upkeep, behind the jobs that requests asked for, or
-- add the payload to its job. The payload is given back to the runner if
-- the store is not ready ('abandon'), if another store holds the lock
-- ('blocked'), or if the lock cannot be taken ('failed').
upkeep :: (Hashable k, Semigroup p) => Indexer k p -> k -> p -> IO ()
upkeep i k p =
  i.runner.ready >>= \case
    False -> do
      atomically (modifyTVar' i.counts (\c -> c {dropped = c.dropped + 1}))
      i.runner.abandon k p
    True -> do
      reserved <- atomically $ do
        q <- readTVar i.queue
        med <- median i.settings.guess <$> readTVar i.times
        case HashMap.lookup k q.jobs of
          Just job -> do
            let job' = case job.stage of
                  Running t again -> job {stage = Running t (Just (maybe p (<> p) again))}
                  Finishing a again -> job {stage = Finishing a (Just (maybe p (<> p) again))}
                  _ -> job {payload = job.payload <> p}
            writeTVar i.queue q {jobs = HashMap.insert k job' q.jobs}
            return Nothing
          Nothing -> do
            let job = Job {payload = p, asked = False, waiters = [], stage = Pending}
                place = Seq.length q.first + Seq.length q.second
            writeTVar i.queue q {jobs = HashMap.insert k job q.jobs}
            return (Just (estimate med i.settings.workers place))
      forM_ reserved (void . acquireFor i k)

-- | Take the lock of a reserved job and queue it; or, if another store
-- holds the lock or it cannot be taken, drop the reservation, answer the
-- requests that joined it, and give its payload back. 'Nothing' once the
-- job is queued.
acquireFor :: (Hashable k) => Indexer k p -> k -> Int -> IO (Maybe Answer)
acquireFor i k est =
  tryAny (i.lock.acquire k est) >>= \case
    Right Took -> do
      atomically . modifyTVar' i.queue $ \q -> case HashMap.lookup k q.jobs of
        Just job@Job {stage = Pending}
          | job.asked -> q {first = q.first |> k, jobs = HashMap.insert k job {stage = Queued} q.jobs}
          | otherwise -> q {second = q.second |> k, jobs = HashMap.insert k job {stage = Queued} q.jobs}
        _ -> q
      return Nothing
    Right (Held t) -> do
      let a = case decide (Situation 0 0 0 False (AnotherStore t)) of
            RefuseNothing why e -> Refused why e
            _ -> Refused OtherStore (max 0 t)
      gone <- atomically (unreserve i k a)
      forM_ gone $ \j -> i.runner.blocked k j.payload t `catchAny` \_ -> return ()
      return (Just a)
    Left e -> do
      gone <- atomically (unreserve i k Failed)
      forM_ gone $ \j -> i.runner.failed k j.payload e `catchAny` \_ -> return ()
      return (Just Failed)

-- | Drop the reservation of a key, if its job is still 'Pending', and
-- answer the requests that joined it.
unreserve :: (Hashable k) => Indexer k p -> k -> Answer -> STM (Maybe (Job p))
unreserve i k a = do
  q <- readTVar i.queue
  case HashMap.lookup k q.jobs of
    Just job@Job {stage = Pending} -> do
      writeTVar i.queue q {jobs = HashMap.delete k q.jobs}
      mapM_ (answer a) job.waiters
      return (Just job)
    _ -> return Nothing

-- | Undo the reservation of a key after an exception: drop the job if it is
-- still 'Pending', answering its requests 'Failed', and let go of the lock,
-- which the store may hold if the exception came after it was taken. A job
-- already queued is left: a worker runs it.
abandonReserved :: (Hashable k) => Indexer k p -> k -> IO ()
abandonReserved i k = do
  gone <- atomically (unreserve i k Failed)
  forM_ gone $ \_ -> i.lock.release k `catchAny` \_ -> return ()

-- | A request joins the job of a key: a queued job of upkeep moves to the
-- end of the first part, and takes the payload of the request; a running
-- job takes nothing but the waiter, since what the request adds would only
-- be read after the job is done.
joinJob :: (Hashable k, Semigroup p) => Indexer k p -> k -> p -> Maybe (TVar Verdict) -> STM ()
joinJob i k p v = do
  q <- readTVar i.queue
  forM_ (HashMap.lookup k q.jobs) $ \job -> do
    let ws = maybeToList v <> job.waiters
        joined = job {payload = job.payload <> p, asked = True, waiters = ws}
    case job.stage of
      Running {} -> writeTVar i.queue q {jobs = HashMap.insert k job {waiters = ws} q.jobs}
      Queued
        | not job.asked ->
            writeTVar
              i.queue
              q
                { first = q.first |> k,
                  second = Seq.filter (/= k) q.second,
                  jobs = HashMap.insert k joined q.jobs
                }
      _ -> writeTVar i.queue q {jobs = HashMap.insert k joined q.jobs}

-- | When the job of a key that a request joins should be done: a queued
-- one from its place, the place at the end of the first part for upkeep,
-- which the request moves there; a running one from what it has left.
--
-- What a running job has left is what its size says it takes, at the rate
-- of the latest jobs that told theirs, less what it has run; without a
-- size, the median less what it has run; and once it has run longer than
-- either, the median again. It never grows with the time that the job has
-- run: an estimate that did, as long again as the job had run, sent clients
-- back at about twice its length.
joinEstimate :: (Hashable k) => Int -> Queue k p -> Int -> Maybe Int -> Int -> k -> Job p -> Int
joinEstimate threads q med expected now k job = case job.stage of
  Running (Just t) _ ->
    let e = max 0 (now - t)
     in case expected of
          Just total | total > e -> total - e
          Nothing | med > e -> med - e
          _ -> med
  Running Nothing _ -> fromMaybe med expected
  Finishing {} -> 0
  Queued | job.asked -> estimate med threads (fromMaybe (Seq.length q.first) (Seq.elemIndexL k q.first))
  _ -> estimate med threads (Seq.length q.first)

-- | What the running job of a key is expected to take in all, from the
-- size that it told and the rate of the latest jobs that told theirs; or
-- 'Nothing' if it told none.
expectedOf :: (Hashable k) => Indexer k p -> k -> STM (Maybe Int)
expectedOf i k = ($ k) <$> expectations i

-- | 'expectedOf' for every key at once.
expectations :: (Hashable k) => Indexer k p -> STM (k -> Maybe Int)
expectations i = do
  sizes <- readTVar i.sizes
  rate <- median rateGuess <$> readTVar i.rates
  return $ \k -> (\n -> n * rate `div` 1000) <$> HashMap.lookup k sizes

-- | Microseconds per thousand transactions of a job, before any job has
-- told its size: 100 microseconds a transaction, about twice what the
-- Redis cache takes for a large wallet.
rateGuess :: Int
rateGuess = 100000

-- | A new waiter, whose time is up at the given time.
waiter :: Indexer k p -> k -> Int -> STM ((Int, Int), TVar Verdict)
waiter i k deadline = do
  n <- readTVar i.serial
  writeTVar i.serial (n + 1)
  v <- newTVar Waiting
  modifyTVar' i.deadlines (Map.insert (deadline, n) (k, v))
  return ((deadline, n), v)

-- | Forget a waiter.
unwait :: Indexer k p -> (Int, Int) -> IO ()
unwait i key = atomically (modifyTVar' i.deadlines (Map.delete key))

-- | Wait for a verdict.
await :: Indexer k p -> (Int, Int) -> TVar Verdict -> IO Answer
await i key v = do
  a <-
    atomically
      ( readTVar v >>= \case
          Waiting -> retry
          Answered a -> return a
      )
      `finally` unwait i key
  case a of
    Indexed -> atomically (modifyTVar' i.counts (\c -> c {answered = c.answered + 1}))
    _ -> return ()
  return a

-- | Tell a waiting request, unless it was told already.
answer :: Answer -> TVar Verdict -> STM ()
answer a v =
  readTVar v >>= \case
    Waiting -> writeTVar v (Answered a)
    _ -> return ()

-- | Count a refusal.
refuse :: Indexer k p -> Refusal -> STM ()
refuse i why = modifyTVar' i.counts $ \c ->
  let r = c.refused
   in c
        { refused = case why of
            OtherStore -> r {otherStore = r.otherStore + 1}
            QueueFull -> r {queueFull = r.queueFull + 1}
            BeyondHorizon -> r {beyondHorizon = r.beyondHorizon + 1}
            TooLong -> r {tooLong = r.tooLong + 1}
            WaitedOut -> r {waitedOut = r.waitedOut + 1}
            NotInSync -> r {notInSync = r.notInSync + 1}
        }

-------------------------------------------------------------------------------
-- Running
-------------------------------------------------------------------------------

-- | Take the next job: the first that a request asked for, or the first of
-- upkeep. It blocks while there is none.
next :: (Hashable k) => Indexer k p -> IO (k, Job p)
next i = atomically (readTVar i.queue >>= pick)
  where
    pick q = do
      (k, q') <- case (Seq.viewl q.first, Seq.viewl q.second) of
        (k :< rest, _) -> return (k, q {first = rest})
        (EmptyL, k :< rest) -> return (k, q {second = rest})
        (EmptyL, EmptyL) -> retry
      case HashMap.lookup k q'.jobs of
        Just job -> do
          writeTVar i.queue q' {jobs = HashMap.insert k job {stage = Running Nothing Nothing} q'.jobs}
          return (k, job)
        -- not possible: every key of the queue has a job
        Nothing -> pick q'

-- | Run the next job in this thread, if there is one: for tests, which run
-- jobs one by one without the threads of 'runIndexer'. Returns whether
-- there was one.
runNext :: (Hashable k) => Indexer k p -> IO Bool
runNext i = do
  q <- atomically (readTVar i.queue)
  if Seq.null q.first && Seq.null q.second
    then return False
    else next i >>= run i >> return True

-- | Run a job, then let go of its lock and tell the requests that wait.
run :: (Hashable k) => Indexer k p -> (k, Job p) -> IO ()
run i (k, job) =
  i.runner.ready >>= \case
    False -> do
      atomically (modifyTVar' i.counts (\c -> c {dropped = c.dropped + 1}))
      i.runner.abandon k job.payload `catchAny` \_ -> return ()
      finish i k (Refused NotInSync i.settings.horizon)
    True -> do
      t0 <- i.clock
      atomically . modifyTVar' i.queue $ \q ->
        q {jobs = HashMap.adjust (\j -> j {stage = started t0 j.stage}) k q.jobs}
      -- the estimate in the lock, which the other stores read, from now
      -- that the job runs, and again once it tells its size
      publish i k
      r <- tryAny $ do
        b <- i.runner.budget k
        let told n = do
              atomically (modifyTVar' i.sizes (HashMap.insert k (max 0 n)))
              publish i k
              b.sized n
        i.runner.index b {sized = told} k job.payload `finally` b.finish
      t1 <- i.clock
      size <- atomically $ do
        sz <- HashMap.lookup k <$> readTVar i.sizes
        modifyTVar' i.sizes (HashMap.delete k)
        return sz
      case r of
        Right () -> do
          atomically $ do
            modifyTVar' i.counts (\c -> c {done = c.done + 1})
            when job.asked $ modifyTVar' i.times (\ts -> Seq.take recentJobs ((t1 - t0) Seq.<| ts))
            forM_ size $ \n ->
              when (n > 0) $
                modifyTVar' i.rates (\rs -> Seq.take recentJobs (((t1 - t0) * 1000 `div` n) Seq.<| rs))
          finish i k Indexed
        Left e -> do
          atomically (modifyTVar' i.counts (\c -> c {errors = c.errors + 1}))
          i.runner.failed k job.payload e `catchAny` \_ -> return ()
          finish i k Failed
  where
    started t = \case
      Running _ again -> Running (Just t) again
      s -> s

-- | A job is done: tell the requests that wait, at once; then queue its key
-- again if upkeep came while it ran, keeping the lock, or let go of the
-- lock, then of the job.
--
-- The job stays, finishing, while its lock is let go, which is a call to
-- Redis: a request that comes meanwhile has its answer at once, and upkeep
-- that comes meanwhile is queued again once the lock is gone, taking it
-- anew. Without that, either would have found the lock of this store, and
-- taken it for that of another. The requests that waited do not wait for
-- that call: what they read was written before it.
finish :: (Hashable k) => Indexer k p -> k -> Answer -> IO ()
finish i k a = do
  kept <- atomically $ do
    q <- readTVar i.queue
    case HashMap.lookup k q.jobs of
      Nothing -> return (Just [])
      Just job -> case job.stage of
        Running _ (Just p) -> do
          let job' = Job {payload = p, asked = False, waiters = [], stage = Queued}
          writeTVar i.queue q {second = q.second |> k, jobs = HashMap.insert k job' q.jobs}
          return (Just job.waiters)
        _ -> do
          mapM_ (answer a) job.waiters
          writeTVar i.queue q {jobs = HashMap.insert k job {waiters = [], stage = Finishing a Nothing} q.jobs}
          return Nothing
  case kept of
    Just ws -> atomically (mapM_ (answer a) ws)
    Nothing -> do
      i.lock.release k `catchAny` \_ -> return ()
      again <- atomically $ do
        q <- readTVar i.queue
        med <- median i.settings.guess <$> readTVar i.times
        case HashMap.lookup k q.jobs of
          Just job -> do
            mapM_ (answer a) job.waiters
            case job.stage of
              Finishing _ (Just p) -> do
                let job' = Job {payload = p, asked = False, waiters = [], stage = Pending}
                    place = Seq.length q.first + Seq.length q.second
                writeTVar i.queue q {jobs = HashMap.insert k job' q.jobs}
                return (Just (estimate med i.settings.workers place))
              _ -> do
                writeTVar i.queue q {jobs = HashMap.delete k q.jobs}
                return Nothing
          Nothing -> return Nothing
      forM_ again (void . acquireFor i k)

-- | Refuse the requests whose time is up, with the estimate of their jobs:
-- what the sweeper does ten times a second. Each request counts its own
-- refusal ('request').
sweep :: (Hashable k) => Indexer k p -> IO ()
sweep i = do
  now <- i.clock
  atomically $ do
    ds <- readTVar i.deadlines
    let (due, rest) = Map.spanAntitone (\(t, _) -> t <= now) ds
    unless (Map.null due) $ do
      writeTVar i.deadlines rest
      q <- readTVar i.queue
      med <- median i.settings.guess <$> readTVar i.times
      forM_ due $ \(k, v) ->
        readTVar v >>= \case
          Waiting -> do
            expected <- expectedOf i k
            let e = maybe med (joinEstimate i.settings.workers q med expected now k) (HashMap.lookup k q.jobs)
            writeTVar v (Answered (Refused WaitedOut e))
          Answered _ -> return ()

-- | Put the estimate of the job of a key in its lock now, rather than at
-- the next renewal: when it starts to run, and when it tells its size. Every
-- store of a fleet decides from the estimate in the lock, and gives the
-- same @Retry-After@ from it.
publish :: (Hashable k) => Indexer k p -> k -> IO ()
publish i k = do
  now <- i.clock
  est <- atomically $ do
    q <- readTVar i.queue
    med <- median i.settings.guess <$> readTVar i.times
    expected <- expectedOf i k
    return (joinEstimate i.settings.workers q med expected now k <$> HashMap.lookup k q.jobs)
  forM_ est $ \e -> void (i.lock.renew [(k, e)]) `catchAny` \_ -> return ()

-- | Renew the locks of the jobs that this store holds, queued or running,
-- each with its estimate, and tell the runner which they are: what the
-- indexer does every so often ('every' of the lock).
renewHeld :: (Hashable k) => Indexer k p -> IO ()
renewHeld i = do
  now <- i.clock
  (held, payloads) <- atomically $ do
    q <- readTVar i.queue
    med <- median i.settings.guess <$> readTVar i.times
    expecting <- expectations i
    let threads = i.settings.workers
        places = HashMap.fromList (zip (foldr (:) [] (q.first <> q.second)) [0 :: Int ..])
        estimateOf k job = case job.stage of
          Running {} -> joinEstimate threads q med (expecting k) now k job
          _ -> estimate med threads (HashMap.findWithDefault 0 k places)
        mine = [(k, job) | (k, job) <- HashMap.toList q.jobs, holding job.stage]
    return
      ( [(k, estimateOf k job) | (k, job) <- mine],
        concat [(k, job.payload) : [(k, p) | Running _ (Just p) <- [job.stage]] | (k, job) <- mine]
      )
  n <- i.lock.renew held `catchAny` \_ -> return 0
  when (n > 0) $ atomically (modifyTVar' i.counts (\c -> c {lost = c.lost + n}))
  i.runner.renew payloads `catchAny` \_ -> return ()
  where
    -- a pending job has not taken its lock yet, and a finishing one is
    -- letting go of it
    holding = \case
      Pending -> False
      Finishing {} -> False
      _ -> True

-------------------------------------------------------------------------------
-- Watching
-------------------------------------------------------------------------------

-- | What the indexer holds and has done.
data IndexerStats = IndexerStats
  { -- | jobs in the first part of the queue, which requests asked for
    forRequests :: !Int,
    -- | jobs of upkeep in the queue
    forUpkeep :: !Int,
    -- | jobs running
    running :: !Int,
    -- | requests waiting for a job
    waiting :: !Int,
    -- | what it has done since the start
    counts :: !Counts,
    -- | the median time of the latest jobs that requests asked for, in
    -- microseconds
    median :: !Int,
    -- | threads that run jobs
    threads :: !Int
  }
  deriving (Eq, Show)

-- | Whether the indexer has nothing to do: no job queued, waiting for its
-- lock, running or letting go of its lock. For tests, and for a store that
-- wants to know that its upkeep has caught up.
indexerIdle :: Indexer k p -> IO Bool
indexerIdle i = HashMap.null . (.jobs) <$> atomically (readTVar i.queue)

-- | What the indexer holds and has done now.
indexerStats :: Indexer k p -> IO IndexerStats
indexerStats i = atomically $ do
  q <- readTVar i.queue
  ds <- readTVar i.deadlines
  ts <- readTVar i.times
  c <- readTVar i.counts
  return
    IndexerStats
      { forRequests = Seq.length q.first,
        forUpkeep = Seq.length q.second,
        running = length [() | Job {stage = Running {}} <- HashMap.elems q.jobs],
        waiting = Map.size ds,
        counts = c,
        median = median i.settings.guess ts,
        threads = i.settings.workers
      }
