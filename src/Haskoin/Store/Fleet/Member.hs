{-# LANGUAGE RecordWildCards #-}

-- |
-- Module      : Haskoin.Store.Fleet.Member
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- A store as a member of a fleet: what it does, with its own database and
-- the Redis that the fleet shares, so that every store of the fleet gives
-- the same answer to the same request. The rules are in
-- "Haskoin.Store.Fleet" and "Haskoin.Store.Fleet.Pool", Redis carries them
-- out through "Haskoin.Store.Fleet.Redis", and the reasons are in
-- @docs\/one-fleet.md@ and @docs\/fleet-mempool.md@.
--
-- = What a member does
--
-- * __It keeps a view of its database at each recent block__ ('noteBest'),
--   taken by the block store right after it imports or reverts a block,
--   and lets go of those that the rules no longer ask for.
--
-- * __It reports those blocks under a lease, and moves the fleet tip__
--   ('memberTick') to the newest block that every live store reported,
--   telling the pool what the blocks in between did ('changeBetween'):
--   which transactions they confirmed and which outputs they spent, and
--   across a reorganisation which transactions come back. Across more
--   blocks than a store goes through ('farthest'), it moves the tip and
--   starts the pool again ('wayBetween'), which every store then fills
--   again ('noteRestarts').
--
-- * __It offers a transaction to the shared pool__ ('memberOffer') when
--   the path into the pool says ("Haskoin.Store.Ingest.Run"), with whether
--   its inputs are unspent in the store's view at the fleet tip, so that
--   the pool only takes what is valid there. What the pool decided on is
--   settled for the fleet, in Redis, and no store fetches it when a peer
--   names it ('memberSettled').
--
-- * __It answers from the view at the fleet tip with the pool on top__
--   ('answering', 'redisPool'), or says that it is behind when it has no
--   view of the tip.
--
-- * __It keeps the fleet's cache of extended public keys__
--   ("Haskoin.Store.Fleet.XPub"): it answers the queries about a key from
--   it when the cache has the key ('answeringCached'), builds the entries
--   that requests missed ('memberCacheLoop'), with the lookahead of each
--   ('Haskoin.Store.Fleet.XPub.lookahead'), and brings the cache to the new
--   block in the same commit as the pool when it moves the tip
--   ('moveTip'), a reorganisation undone address by address.
--
-- = Why the member does not withdraw transactions
--
-- A transaction leaves the pool when it is replaced, when a block confirms
-- a conflicting one, and when it is too old. The first comes with the offer
-- of the replacement, which names what it replaces. The second comes when
-- the tip moves to that block. The third is done on the pool itself, by its
-- own times ('expirePool'), so that the stores do not have to agree on when
-- each of them first saw a transaction. A store that simply does not have a
-- transaction says nothing about it: another store has it.
module Haskoin.Store.Fleet.Member
  ( -- * A member
    Member (..),
    MemberConfig (..),
    newMember,
    noteBest,
    setHealthy,
    memberTick,
    memberLoop,
    memberAlive,
    memberEvents,
    noteEpoch,

    -- * Answering
    Standing (..),
    answering,
    memberTip,
    answeringAt,
    answeringCached,
    TipMoved (..),
    redisPool,

    -- * The cache of extended public keys
    CacheMetrics (..),
    memberCacheLoop,
    fleetSession,
    cacheGauges,

    -- * The parts of a tick
    report,
    moveTip,
    Between (..),
    blocksBetween,
    wayBetween,
    Way (..),
    farthest,
    changeBetween,
    memberSettled,
    memberOffer,
    memberPoolBody,
    memberNewest,
    expirePool,
    poolTx,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (link, withAsync)
import Control.Concurrent.STM
  ( STM,
    TVar,
    atomically,
    check,
    modifyTVar',
    newTVarIO,
    orElse,
    readTVar,
    readTVarIO,
    registerDelay,
    retry,
    stateTVar,
    writeTVar,
  )
import Control.Exception (Exception, handle, throwIO)
import Control.Monad (filterM, forM, forM_, join, unless, void, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.Either (lefts, rights)
import Data.Functor ((<&>))
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.IntMap.Strict qualified as IntMap
import Data.List (nub)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe, isJust, isNothing, mapMaybe)
import Data.Serialize (Serialize, decode, encode, get, getWord32be, putWord32be, runGet, runPut)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (decodeUtf8)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word64)
import Database.Redis qualified as R
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Stats (RTSStats (..), getRTSStats, getRTSStatsEnabled)
import Haskoin
  ( BlockHash,
    BlockHeader (..),
    Network (..),
    OutPoint (..),
    TokenData (..),
    Tx (..),
    TxHash,
    TxIn (..),
    TxOut (..),
    blockHashToHex,
    eitherToMaybe,
    headerHash,
    nullOutPoint,
    scriptToAddressBS,
    txHash,
  )
import Haskoin.Node (LogLevel (..))
import Haskoin.Store.Common (Limits (..), Replaced (..), StoreRead (..), loopForever)
import Haskoin.Store.Data
  ( BlockData (..),
    BlockRef (..),
    Prev (..),
    Spender (..),
    TxData (..),
    UnixTime,
    Unspent (..),
    XPubSpec,
    scriptTokens,
  )
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Pooled (From, PoolReads (..), pageStart, pooledRead)
import Haskoin.Store.Database.Reader (DatabaseReader (..), txCF, withSnapshotReader)
import Haskoin.Store.Database.Types (CategorySummary (..), MemSummary (..), PoolTokens (..), TxKey (..))
import Haskoin.Store.Database.Views (Views, heldViews, keepViews, newViews, takeView, withViewAt)
import Haskoin.Store.Exception (bracket, handleAny, tryAny)
import Haskoin.Store.Fleet qualified as F
import Haskoin.Store.Fleet.Redis
import Haskoin.Store.Fleet.XPub
import Haskoin.Store.Ingest qualified as I
import Haskoin.Store.Outage (Outage, attempt, newOutage)
import Haskoin.Store.XPub (EntryMoved (..), XPubReads (..))
import Haskoin.Store.XPub.Indexer
  ( Indexer,
    Runner (..),
    Settings (..),
    newIndexer,
    plainRunner,
    prefetch,
    requestWithin,
    runIndexer,
  )
import Haskoin.Store.XPub.Lock (Lock (..), defaultLockConfig, redisLock)
import Haskoin.Store.XPub.Service (Backend (..), Session (..), XPubs (..))
import System.IO (fixIO)
import System.Metrics.StatsD
  ( StatCounter,
    StatGauge,
    StatTiming,
    Stats,
    addTiming,
    incrementCounter,
    newStatCounter,
    newStatGauge,
    newStatTiming,
    setGauge,
  )
import System.Random (randomIO)
import System.Timeout (timeout)

-- | How a store takes part in a fleet.
data MemberConfig = MemberConfig
  { -- | what every key of the fleet in Redis starts with
    prefix :: !ByteString,
    -- | the name of this store, which no other store of the fleet has
    name :: !ByteString,
    -- | how long a report counts for, in milliseconds. A store that stops
    -- reporting holds the tip back for this long at most.
    lease :: !Word64,
    -- | how many recent blocks to keep views of and report. With more, the
    -- stores find a block in common sooner when they import several at
    -- once; each view keeps the files that it reads from being deleted.
    depth :: !Int,
    -- | seconds after which a transaction leaves the pool unconfirmed, or 0
    -- for never
    expiry :: !Word64,
    -- | how many extended public keys the fleet's cache of them may hold
    -- ("Haskoin.Store.Fleet.XPub"), evicted by GreedyDual: at least 1, as
    -- the cache cannot be turned off
    cacheMax :: !Int,
    -- | the settings of the indexer of xpubs: its threads, and how long a
    -- request waits for an xpub ("Haskoin.Store.XPub.Indexer")
    indexer :: !Settings,
    -- | StatsD client, to report what the cache of extended public keys
    -- does, as t'CacheMetrics' lists
    stats :: !(Maybe Stats),
    -- | where to say what a log should have: that the cache was emptied
    -- and why, as information, and each entry built and each move of the
    -- cache, for debugging
    note :: !(LogLevel -> Text -> IO ())
  }

-- | StatsD metrics of the fleet's cache of extended public keys, as this
-- store sees it, under @fleet.cache@.
data CacheMetrics = CacheMetrics
  { -- | requests about a key whose balances the cache gave
    -- (@fleet.cache.hits@)
    hits :: !StatCounter,
    -- | requests about a key that the database answered: the cache did
    -- not have the key, had it with other addresses, or was not at the
    -- fleet tip (@fleet.cache.misses@)
    misses :: !StatCounter,
    -- | entries that this store built or added addresses to
    -- (@fleet.cache.builds@)
    builds :: !StatCounter,
    -- | builds that this store gave up, because the tip moved or another
    -- store wrote the entry (@fleet.cache.abandoned@)
    abandoned :: !StatCounter,
    -- | entries that this store evicted, those of the lowest priority by
    -- GreedyDual (@fleet.cache.evictions@)
    evictions :: !StatCounter,
    -- | times that this store emptied the cache (@fleet.cache.emptied@)
    emptied :: !StatCounter,
    -- | milliseconds to build an entry (@fleet.cache.build@)
    buildTime :: !StatTiming,
    -- | entries in the cache, one for each extended public key, whichever
    -- store built them (@fleet.cache.xpubs@)
    xpubs :: !StatGauge,
    -- | addresses that those entries have between them
    -- (@fleet.cache.addresses@)
    addresses :: !StatGauge
  }

-- | Register the metrics of the cache.
newCacheMetrics :: Stats -> IO CacheMetrics
newCacheMetrics s = do
  hits <- c "hits"
  misses <- c "misses"
  builds <- c "builds"
  abandoned <- c "abandoned"
  evictions <- c "evictions"
  emptied <- c "emptied"
  buildTime <- newStatTiming s "fleet.cache.build" 10
  xpubs <- newStatGauge s "fleet.cache.xpubs" 0
  addresses <- newStatGauge s "fleet.cache.addresses" 0
  return CacheMetrics {..}
  where
    c x = newStatCounter s ("fleet.cache." <> x) 10

-- | Add to a counter of the cache, if metrics are reported.
tally :: Member -> (CacheMetrics -> StatCounter) -> Int -> IO ()
tally m which n = forM_ m.metrics (\x -> incrementCounter (which x) n)

-- | A store that is a member of a fleet.
data Member = Member
  { config :: !MemberConfig,
    redis :: !R.Connection,
    keys :: !FleetKeys,
    db :: !DatabaseReader,
    -- | the database as it was at each recent block
    views :: !(Views BlockHash),
    -- | the blocks that views were taken of, newest first
    recent :: !(TVar [BlockHash]),
    -- | the blocks of the last report that was taken
    reported :: !(TVar [BlockHash]),
    -- | the fleet tip, read after that report
    tip :: !(TVar (Maybe BlockHash)),
    -- | whether the store's own health checks pass. While they do not, the
    -- member does nothing, so that its lease runs out and a store that is
    -- stuck does not hold the fleet back.
    healthy :: !(TVar Bool),
    -- | whether the store was found unwell since the member's last whole
    -- turn, which it then takes as soon as the store is well ('setHealthy')
    owed :: !(TVar Bool),
    -- | whether the store has a new best block since the last whole turn
    -- ('noteBest')
    newBest :: !(TVar Bool),
    -- | the indexer that builds the entries of the xpubs that requests
    -- miss, one store of the fleet for each, holding its dibs in Redis
    indexer :: !(Indexer XPubSpec ()),
    lock :: !(Lock XPubSpec),
    -- | for each xpub that requests wait for, how many builds Redis
    -- announced while they waited and how many wait (@awaitBuilt@). A key
    -- is there only while a request waits for it, so the map holds no more
    -- keys than there are waiting requests
    built :: !(TVar (HashMap ByteString (Int, Int))),
    -- | the door to the cache, for the web server
    door :: !XPubs,
    -- | the metrics of the cache, if the configuration has a StatsD client
    metrics :: !(Maybe CacheMetrics),
    -- | whether the last thing that the member tried with Redis failed,
    -- so that an outage is logged once, by whichever loop meets it
    outage :: !Outage,
    -- | the epoch of Redis at the last turn ('noteEpoch')
    epoch :: !(TVar (Maybe ByteString)),
    -- | the count of restarts of the pool at the last turn ('noteRestarts')
    restarts :: !(TVar (Maybe Integer)),
    -- | how many times this store found the pool emptied, because Redis
    -- lost its data or the pool started again: the web server fills the
    -- pool again each time ('Haskoin.Store.Web')
    refills :: !(TVar Int),
    -- | how many moves of the tip in a row this store tried and did not
    -- make: what it warns of ('moveTip')
    refused :: !(TVar Int),
    -- | the last tip that Redis was seen to have, and when, by the clock of
    -- 'getMonotonicTimeNSec': a request that read nothing of the pool
    -- takes it for the tip for 'seenFor', instead of asking Redis
    -- ('answeringWith')
    seen :: !(IORef (ByteString, Word64))
  }

-- | For how long, in nanoseconds, a tip that Redis was seen to have is
-- taken to be the tip still, by a request that reads nothing of the pool.
-- Asking Redis is a round trip for every such request, and a request a
-- few milliseconds earlier would have had that tip anyway. It never mixes
-- tips: a read of the pool checks the tip in Redis in the same step.
seenFor :: Word64
seenFor = 5000000

-- | A member with no views yet, on a connection to the Redis of the fleet.
newMember :: MemberConfig -> R.Connection -> DatabaseReader -> IO Member
newMember config redis db = do
  views <- newViews db
  recent <- newTVarIO []
  reported <- newTVarIO []
  tip <- newTVarIO Nothing
  healthy <- newTVarIO True
  owed <- newTVarIO False
  newBest <- newTVarIO False
  metrics <- mapM newCacheMetrics config.stats
  outage <- newOutage
  built <- newTVarIO HashMap.empty
  epoch <- newTVarIO Nothing
  restarts <- newTVarIO Nothing
  refills <- newTVarIO 0
  refused <- newTVarIO 0
  seen <- newIORef ("", 0)
  lock <- redisLock (defaultLockConfig (config.prefix <> "fleet:") config.name) redis xpubId
  fixIO $ \m -> do
    let runner = (plainRunner (\_ x () -> buildFor m x)) {ready = isJust <$> readTVarIO m.tip}
    indexer <- newIndexer config.indexer lock runner
    let door =
          XPubs
            { backend =
                Backend
                  { ask = \x w -> requestWithin indexer w x (),
                    await = awaitBuilt m,
                    eta = \x -> fmap fst <$> lock.holder x,
                    delete = \x -> True <$ cacheDrop redis (fleetKeys config.prefix 32) x
                  },
              patience = config.indexer.wait
            }
    return Member {keys = fleetKeys config.prefix 32, ..}

-- | Take a view of the database for its best block. The block store calls
-- this right after it imports or reverts a block, and when it starts,
-- before it writes anything else: the view is then of that block and of
-- nothing more.
--
-- The block joins the recent ones before its view is taken: @prune@ runs on
-- the member's thread too, and one that ran between the two would let go
-- of a view whose block it did not yet count as recent.
noteBest :: Member -> BlockHash -> IO ()
noteBest m block = do
  atomically . modifyTVar' m.recent $ \blocks -> take m.config.depth (block : filter (/= block) blocks)
  takeView m.views block
  prune m
  -- last, so that the turn that this wakes finds the view
  atomically (writeTVar m.newBest True)

-- | Say whether the store's own health checks pass. A store found unwell
-- owes a whole turn, which its member takes as soon as it is well again
-- ('Haskoin.Store.Fleet.turn'), instead of waiting up to two seconds for
-- its timer.
setHealthy :: Member -> Bool -> STM ()
setHealthy m well = do
  writeTVar m.healthy well
  unless well (writeTVar m.owed True)

-- | Let go of the views that the rules do not ask for: all but those of
-- the recent blocks, of the last report, and of the tip read after it. The
-- three are read in the transaction that lets the views go ('keepViews'),
-- so that the block store's thread and the member's, which both prune,
-- each decide on what is there at that moment.
prune :: Member -> IO ()
prune m = keepViews m.views $ do
  recent <- readTVar m.recent
  reported <- readTVar m.reported
  tip <- readTVar m.tip
  return (nub (F.keep (recent <> reported) tip))

-- | Whether Redis answers a ping within two seconds.
memberAlive :: Member -> IO Bool
memberAlive m =
  handleAny (\_ -> return False) $
    timeout 2000000 (R.runRedis m.redis R.ping) <&> \case
      Just (Right _) -> True
      _ -> False

-- | One round of what a member does: report, move the tip if the stores
-- agree on another block, and let go of old views. Call it every second
-- or two, and when the best block changes. A store whose own health checks fail must not
-- call it: its lease then runs out, and the fleet goes on without it.
memberTick :: Member -> IO ()
memberTick m = do
  noteEpoch m
  noteRestarts m
  -- only a store whose report was taken, which is live, moves the tip: one
  -- whose report was refused has no view of the tip, and moving from a tip
  -- that it does not know would empty the pool that another store knows
  taken <- report m
  moved <- if taken then moveTip m else return False
  -- the tip is read after the report, as the rule of 'F.keep' asks
  tip <- fmap decoded <$> fleetTip m.redis m.keys
  atomically (writeTVar m.tip (fromMaybe Nothing tip))
  prune m
  cacheGauges m
  unless moved (expirePool m)
  where
    decoded = either (const Nothing) Just . decode

-- | Find out whether Redis lost its data since this store's last turn.
-- Redis keeps an epoch, a random number that the first store to find it
-- missing sets. A store that finds it missing or changed forgets its view
-- of the fleet tip, so that it is
-- unhealthy until a tip is set again, which its next turn does, since
-- every live store reports. It counts the loss in 'refills': the path
-- into the pool then reads the mempools of its sources again, and the web
-- server that of the node over RPC if it has one.
-- Entries of the cache of extended public keys are built again on demand.
--
-- Without an epoch, a loss would show only in the count of restarts of
-- the pool going back, which says nothing when it was 0 before.
noteEpoch :: Member -> IO ()
noteEpoch m = do
  mine <- C.pack . show <$> (randomIO :: IO Word64)
  now <- redis' (R.setnx key mine >> R.get key)
  lost <- atomically $ do
    before <- readTVar m.epoch
    writeTVar m.epoch now
    let lost = isJust before && before /= now
    when lost $ do
      writeTVar m.tip Nothing
      -- the count of restarts went with the rest: not a second refill
      writeTVar m.restarts Nothing
      modifyTVar' m.refills (+ 1)
    return lost
  when lost . m.config.note LevelWarn $
    "Redis lost its data: the pool fills again from the peers, and xpub entries are built again on demand"
  where
    key = m.keys.prefix <> "fleet:epoch"
    redis' :: R.Redis (Either R.Reply a) -> IO a
    redis' f = R.runRedis m.redis f >>= either (fail . ("Redis error: " <>) . show) return

-- | Find out whether the pool was emptied since this store's last turn
-- ('fleetRestarts'), and if so count a refill, as 'noteEpoch' does for a
-- loss of Redis. The pool forgot with it what the fleet had seen, so what
-- the peers announce from then on is fetched again; the refill brings
-- back what they announced before. The first turn only notes the count.
noteRestarts :: Member -> IO ()
noteRestarts m = do
  now <- fleetRestarts m.redis m.keys
  restarted <- atomically $ do
    before <- readTVar m.restarts
    writeTVar m.restarts (Just now)
    let restarted = maybe False (/= now) before
    when restarted $ modifyTVar' m.refills (+ 1)
    return restarted
  when restarted . m.config.note LevelInfo $
    "The pool started again: it fills again from the node and the peers"

-- | Take the member's turns ('memberTick'), for as long as the store runs,
-- when the rule of 'Haskoin.Store.Fleet.turn' says: every two seconds, at
-- each new best block and when the store is well again, only while the
-- store's health checks pass. A turn that fails, as when Redis is away, is
-- tried again at the next, and logged as an outage is ('attempt').
--
-- The member waits in one transaction for what wakes it, and clears in
-- the same transaction what the turn will see to: a block or a health
-- that comes during the turn wakes it again after it, and none is lost
-- between the read and the clearing. Before, the member waited for a
-- block with a timer around it, and read the health after: a store that
-- the block made well took no turn for up to two seconds.
--
-- Transactions are not offered here: "Haskoin.Store.Ingest.Run" offers
-- each as it comes ('memberOffer').
memberLoop :: Member -> IO a
memberLoop m = registerDelay 2000000 >>= go
  where
    go timer = do
      atomically $ do
        wake <-
          F.Wake
            <$> readTVar m.healthy
            <*> readTVar m.owed
            <*> readTVar m.newBest
            <*> readTVar timer
        unless (F.turn wake) retry
        writeTVar m.owed False
        writeTVar m.newBest False
      void (attempt m.outage m.config.note "take a turn in the fleet" (memberTick m))
      registerDelay 2000000 >>= go

-- | Hear what happens to the fleet, for as long as the store runs: each
-- move of the tip and each transaction that enters or leaves the pool is
-- told to a function, in the order in which Redis has them. Every store of
-- the fleet hears the same, which is what its clients are told
-- ('FleetEvent').
--
-- The telling starts from now, or from the oldest event that Redis keeps:
-- the second is for a listener that wants to hold the same history as the
-- other stores whenever it started, as the log of events of the web server
-- does. When Redis cannot be read, that is logged as an outage is
-- ('attempt'), and listening goes on a second later from the last event
-- heard, so nothing is left out unless Redis dropped it meanwhile.
memberEvents :: Member -> Bool -> (FleetEvent -> IO ()) -> IO a
memberEvents m whole tell = newIORef (if whole then Just "0-0" else Nothing) >>= go
  where
    go at = do
      heard <- attempt m.outage m.config.note "read the events of the fleet" (listen at)
      unless heard (void (timeout 1000000 (atomically retry) :: IO (Maybe ())))
      go at
    -- what there is after the place, waiting a second for it
    listen at = do
      from <- readIORef at >>= maybe (fleetEventsEnd m.redis m.keys) return
      writeIORef at (Just from)
      events <- fleetEventsAfter m.redis m.keys from 1000
      mapM_ (tell . snd) events
      writeIORef at (Just (foldl (\_ (place, _) -> place) from events))

-- | Report the blocks of the main chain that the store has views of, newest
-- first.
report :: Member -> IO Bool
report m = do
  held <- heldViews m.views
  recent <- readTVarIO m.recent
  blocks <- filterM main (filter (`elem` held) recent)
  taken <- fleetReport m.redis m.keys Nothing m.config.lease m.config.name (map encode blocks) (map encode held)
  atomically (writeTVar m.reported (if taken then blocks else []))
  unless taken . m.config.note LevelDebug $
    "Report not taken: this store has no view of the fleet tip, and another store is live"
  return taken
  where
    main h = maybe False (.main) <$> Q.getBlock m.db h

-- | Move the fleet tip to the block that the live stores agree on, if it is
-- another one, with what the blocks in between did to the pool and to the
-- cache of extended public keys, in one commit. Says whether it moved.
--
-- The move begins under a token ('cacheBegin'), which has builds wait;
-- what the blocks did at the addresses that entries have, a reorganisation
-- undone address by address, is written in pieces ('tipMoves',
-- 'cachePieces'); then 'fleetCommit' moves the pool, the tip and the cache
-- together. A move that is refused is aborted, and the entries that it
-- wrote are dropped ('cacheAbort').
--
-- From the beginning of a move to its end the store says every two
-- seconds that it is at it ('cacheBeat'), which keeps the floor: whatever
-- else writes to the pool or to the cache gives way meanwhile
-- ('Haskoin.Store.Fleet.Pool.Decide.floorHeld'). The move of a full block
-- takes seconds to decide, and more than ten on a slow machine. When the
-- floor lasted ten seconds from the last piece, such a move lost it before
-- it was carried out, and the tip of a server stayed where it was while
-- its database went on. A move that is not over in ten minutes has gone
-- wrong: the store stops saying so then, and the floor is free ten
-- seconds later, as it is ten seconds after a store that stopped.
--
-- When this store cannot say what the
-- blocks did, the pool is emptied, and the cache with it: Redis allows that
-- to a store that is alone, and to any store when the way is too long for
-- all of them (v'TooFar').
--
-- When the block that the stores agree on is no longer on this store's main
-- chain, because the store moved its chain after it reported the block, the
-- tip stays until the next turn (v'Unsettled'). Treating that as a way it
-- does not know emptied the pool of a store that was alone in the middle of
-- a reorganisation, and lost the transactions of the abandoned blocks that
-- no node had any more.
moveTip :: Member -> IO Bool
moveTip m = do
  seen <- fleetTip m.redis m.keys
  leases <- fleetLive m.redis m.keys
  recents <- redis' (R.hgetall m.keys.recent)
  now <- redis' R.time <&> \(s, us) -> fromIntegral (s * 1000 + us `div` 1000)
  let reports =
        Map.fromList
          [ (store, F.Report {recent = chunks blocks, expires})
          | (store, expires) <- leases,
            Just blocks <- [lookup store recents]
          ]
      fleet = F.Fleet {tip = seen, reports}
  case F.agreed now fleet of
    Just target | Just target /= seen -> do
      token <- C.pack . show <$> (randomIO :: IO Word64)
      cacheBegin m.redis m.keys token (fromMaybe "" seen) >>= \case
        Moving -> do
          m.config.note LevelDebug "Fleet tip not moved: another store is moving it"
          return False
        BeginRaced -> return False
        Begun -> withAsync (beats token (300 :: Int)) (\_ -> moveWith seen token target)
    agreed -> do
      when (isNothing agreed) . m.config.note LevelDebug $
        "The live stores have no block in common: " <> shown (Map.size reports) <> " reports"
      return False
  where
    -- the store says that it is at its move, so many times at most, and
    -- stops when the move is no longer its own
    beats token n = when (n > 0) $ do
      threadDelay 2000000
      still <- cacheBeat m.redis m.keys token
      when still (beats token (n - 1))
    -- the move under a token of the cache: what the blocks did at the
    -- addresses of the entries is written in pieces, and the commit moves
    -- the pool, the tip and the cache together
    moveWith seen token target = do
      began <- getMonotonicTimeNSec
      collected <- collector
      tried <- newIORef []
      cacheAt <- newIORef Nothing
      way <- case (traverse decode seen, decode target) of
        (Right from, Right to) -> wayBetween m.views m.db from to
        _ -> return NoWay
      found <- getMonotonicTimeNSec
      let between = case way of
            Way b -> Just b
            _ -> Nothing
          path = maybe [] (map encode . (.path)) between
      -- what the blocks did at the addresses of the entries, read at the
      -- new tip: without a view of it, this store does not move the tip
      written <- case (way, decode target) of
        (Way b, Right to) ->
          withViewAt m.views to (\r -> tipMoves m.redis m.keys r b.abandoned b.added) >>= \case
            Just moves -> do
              getMonotonicTimeNSec >>= writeIORef cacheAt . Just
              cachePieces m.redis m.keys token (fromMaybe "" seen) moves
            Nothing -> return False
        (Way _, _) -> return False
        -- the store's chain moved since its report: not this turn
        (Unsettled, _) -> return False
        -- what the abandoned blocks held cannot be read: not this turn
        (Unreadable, _) -> return False
        _ -> return True
      pieced <- getMonotonicTimeNSec
      moved <-
        if not written
          then return Raced
          else case way of
            TooFar -> do
              m.config.note LevelWarn $
                "The fleet tip is more than "
                  <> shown farthest
                  <> " blocks from "
                  <> blockName target
                  <> ": starting the shared mempool again there"
              fleetRestart m.redis m.keys 32 Nothing seen target
            _ ->
              fleetCommitTelling
                (\a -> modifyIORef' tried (a :))
                m.redis
                m.keys
                32
                Nothing
                m.config.name
                seen
                target
                ((.pool) <$> between)
                path
                token
      ended <- getMonotonicTimeNSec
      attempts <- reverse <$> readIORef tried
      collecting <- collector
      readAt <- fromMaybe pieced <$> readIORef cacheAt
      let ms a b = shown ((b - a) `div` 1000000)
          millis n = shown (n `div` 1000000)
          took = ms began ended <> " ms"
          -- what Redis answered otherwise when the move was to be carried
          -- out, if it was decided and then refused
          refusal = case mapMaybe (.changed) attempts of
            [] -> ""
            qs -> ", refused for the answer to " <> T.pack (show (last qs))
          -- Where the time went. Transactions wait for as long as a move
          -- takes, and what would shorten it depends on which part is
          -- long: reading the blocks from the database, the cache, the
          -- decision, which is gone through once for each time that Redis
          -- is asked, or Redis.
          parts =
            T.intercalate ", " $
              [ "over " <> shown (maybe 0 (length . (.pool.confirmed)) between) <> " transactions",
                "the blocks read in " <> ms began found <> " ms",
                "the cache read in " <> ms found readAt <> " and written in " <> ms readAt pieced <> " ms"
              ]
                <> [ mconcat
                       [ "decided in ",
                         millis a.planning,
                         " ms, ",
                         millis a.waiting,
                         " of them in ",
                         shown a.passes,
                         " calls to Redis, with ",
                         shown a.answers,
                         " answers and ",
                         shown a.written,
                         " writes, and ",
                         maybe "carried out" (const "refused") a.changed,
                         " in ",
                         millis a.carrying,
                         " ms"
                       ]
                   | a <- attempts
                   ]
                -- every thread waits while the collector runs, this one
                -- too: a store with a large heap may spend most of a move
                -- there, whatever the move does
                <> [ mconcat
                       [ "the collector ran for ",
                         shown ((after.gc_elapsed_ns - before.gc_elapsed_ns) `div` 1000000),
                         " ms meanwhile, in ",
                         shown (after.gcs - before.gcs),
                         " collections, ",
                         shown (after.major_gcs - before.major_gcs),
                         " of the whole heap"
                       ]
                   | (Just before, Just after) <- [(collected, collecting)]
                   ]
      m.config.note LevelDebug . mconcat $
        [ "Fleet tip from ",
          maybe "none" blockName seen,
          " to ",
          blockName target,
          case moved of
            MovedTo _ -> ": moved"
            Stays -> ": stays"
            Raced -> ": not moved, another store did it first or has to",
          " in ",
          took,
          " (",
          parts,
          ")",
          refusal,
          case way of
            Way _ -> ""
            NoWay -> " (this store does not know the way)"
            Unsettled -> " (the chain of this store moved since its report)"
            Unreadable -> " (this store cannot read what the abandoned blocks held)"
            TooFar -> " (the way is too long)"
        ]
      case moved of
        MovedTo _ -> do
          let restarted = case way of
                Way _ -> False
                _ -> True
          when restarted $ wasEmptied m target "the shared mempool started again"
          -- transactions wait for as long as a move takes, so a slow
          -- one is said: half of the ten seconds that the floor lasts
          -- without a word is slow
          when (ended - began >= 5000000000) . m.config.note LevelInfo $
            "The fleet tip moved to " <> blockName target <> " in " <> took <> ": " <> parts
          atomically (writeTVar m.refused 0)
          return True
        _ -> do
          -- A move that is refused once is nothing: another store made
          -- it, or the store was not ready. Refused turn after turn, the
          -- store answers from an old block while its database goes on,
          -- and its health, which looks at the database, says that all
          -- is well. That happened, in silence, so it is said.
          n <- atomically (stateTVar m.refused (\x -> (x + 1, x + 1)))
          when (n == 5 || n `mod` 150 == 0) . m.config.note LevelWarn . mconcat $
            [ "The fleet tip has not moved in ",
              shown n,
              " turns of this store, from ",
              maybe "none" blockName seen,
              " to ",
              blockName target,
              ": the store answers from the block that it is at. The last took ",
              took,
              " (",
              parts,
              ")",
              refusal
            ]
          False <$ cacheAbort m.redis m.keys token
    -- what the collector has done so far, if the store keeps count
    -- (@+RTS -T@)
    collector =
      getRTSStatsEnabled >>= \case
        True -> Just <$> getRTSStats
        False -> return Nothing
    redis' :: R.Redis (Either R.Reply a) -> IO a
    redis' f = R.runRedis m.redis f >>= either (fail . ("Redis error: " <>) . show) return
    chunks bytes
      | B.null bytes = []
      | otherwise = B.take 32 bytes : chunks (B.drop 32 bytes)

-- | The most blocks that 'changeBetween' goes through on either side of a
-- fork. Beyond that, a store that is alone starts the pool again, which
-- costs the stores an offer of their mempools; going through a thousand
-- blocks would cost a read of every transaction in them.
farthest :: Int
farthest = 50

-- | What the blocks between two tips did, as far as the pool is concerned,
-- read from the database as it is now: the transactions of the blocks that
-- lead from the first tip, or from the genesis block, to where the main
-- chain forked from it, which come back or are gone, and those of the
-- blocks of the main chain from there to the second tip, which are
-- confirmed, with the outputs that they spend.
--
-- Without a first tip there was no pool, and the blocks did nothing to it.
-- 'Nothing' if the database does not have one of the tips, if the second
-- is not on its main chain, or if the way is longer than 'farthest'
-- blocks on either side.
changeBetween :: Views BlockHash -> DatabaseReader -> Maybe BlockHash -> BlockHash -> IO (Maybe PoolChange)
changeBetween views db from to = fmap (.pool) <$> blocksBetween views db from to

-- | What the blocks between two tips did.
data Between = Between
  { -- | what they did to the pool
    pool :: !PoolChange,
    -- | the transactions of the blocks of the main chain from where it
    -- left the way to the first tip, up to the second, in their order
    added :: ![TxData],
    -- | whether blocks of the way to the first tip were abandoned
    reorganised :: !Bool,
    -- | the transactions of those abandoned blocks, in their order, as the
    -- view of the database at the first tip has them
    abandoned :: ![TxData],
    -- | those blocks of the main chain, oldest first, the second tip last:
    -- what the fleet tells its clients that the tip moved through
    path :: ![BlockHash]
  }

-- | 'changeBetween', with the transactions that the new blocks confirm and
-- those of the abandoned blocks, which is what the cache of extended public
-- keys is brought to the new tip with ('moveTip').
blocksBetween :: Views BlockHash -> DatabaseReader -> Maybe BlockHash -> BlockHash -> IO (Maybe Between)
blocksBetween views db from to =
  wayBetween views db from to <&> \case
    Way between -> Just between
    _ -> Nothing

-- | What a store can say of the way between two tips.
data Way
  = -- | what the blocks between them did
    Way !Between
  | -- | nothing: it does not have the first tip, or the blocks between
    -- do not connect in its database. Another store may know; a store that
    -- is the one live store empties the pool.
    NoWay
  | -- | not now: it has the first tip, and a reorganisation abandoned
    -- blocks whose transactions it cannot read, having no view of the
    -- database at that tip. The tip waits: another store may read them,
    -- and once the way is longer than 'farthest' it is v'TooFar' for every
    -- store. Taken for v'NoWay' once, it emptied the pool of a store that
    -- was alone, where the documentation said that it waits.
    Unreadable
  | -- | not yet: the second tip is not on its main chain as the database
    -- has it now. The store reported the tip, and its chain has moved
    -- since, as when it reverts blocks in a reorganisation; its next report
    -- says where it is. The tip waits for the next turn.
    Unsettled
  | -- | it has both, and the way is longer than 'farthest' blocks on one
    -- side of the fork. That is so for every store.
    TooFar

-- | 'blocksBetween', telling a way that is too long from one that the
-- store does not know. The fleet treats them differently: a tip that no
-- store will ever say the way from has to be left behind
-- ('fleetRestart'), and one that another store may know has to wait.
--
-- The transactions of the blocks that a reorganisation abandoned are no
-- longer in the database: reverting a block takes them out
-- ("Haskoin.Store.Logic"). They are read from the store's view of the
-- database at the first tip, which it keeps ('noteBest'). Without that
-- view the store cannot say what they held, and the way is v'Unreadable':
-- it does not move the tip, which keeps the cache of extended public keys
-- whole, since a reorganisation never empties it. Another store may know.
wayBetween :: Views BlockHash -> DatabaseReader -> Maybe BlockHash -> BlockHash -> IO Way
wayBetween views db from to = withSnapshotReader db $ \r -> do
  let -- the blocks from one that is not on the main chain back to it
      back :: Int -> BlockHash -> IO (Either Way ([BlockData], BlockData))
      back n h
        | n > farthest = return (Left TooFar)
        | otherwise =
            Q.getBlock r h >>= \case
              Nothing -> return (Left NoWay)
              Just bd
                | bd.main -> return (Right ([], bd))
                | otherwise -> fmap (\(bs, fork) -> (bd : bs, fork)) <$> back (n + 1) bd.header.prev
      -- the blocks of the main chain from a block down to above a height
      down :: Int -> BlockData -> BlockData -> IO (Either Way [BlockData])
      down n fork bd
        | n > farthest = return (Left TooFar)
        | bd.height <= fork.height = return (if bd.header == fork.header then Right [] else Left NoWay)
        | otherwise =
            Q.getBlock r bd.header.prev >>= \case
              Nothing -> return (Left NoWay)
              Just parent -> fmap (bd :) <$> down (n + 1) fork parent
  target <- Q.getBlock r to
  left <- case from of
    Just h -> back 0 h
    -- without a tip there was no pool, and nothing for the blocks to do
    Nothing -> return (maybe (Left NoWay) (Right . ([],)) target)
  case (target, left) of
    (_, Left none) -> return none
    (Just bd, Right (abandoned, fork))
      | bd.main ->
          down 0 fork bd >>= \case
            Left none -> return none
            Right added ->
              maybe (return Unreadable) (wayWith r bd abandoned added) =<< abandonedBodies (reverse abandoned)
    -- the store reported the second tip, and has moved on since
    _ -> return Unsettled
  where
    wayWith r bd abandoned added old = do
      new <- concat <$> mapM (confirmedBy r) (reverse added)
      -- what the abandoned blocks give back enters the pool now, not at
      -- the time of its block, which the pool would expire at once
      now <- round <$> getPOSIXTime
      -- those of the abandoned blocks that this store has as
      -- replaced: every store had them confirmed, and says the same
      let named h rep = encode h <> encode rep.by <> runPut (putWord32be rep.height)
      lostOnes <- fmap catMaybes . forM [h | (h, Nothing) <- old] $ \h ->
        fmap (named h) <$> Q.getReplaced r h
      let pool =
            PoolChange
              { returned = [poolTx r.net (td :: TxData) {block = MemRef now} | (_, Just td) <- old, not (coinbase td)],
                gone = [encode h | (h, found) <- old, maybe True coinbase found],
                confirmed = map (encode . txHash . (.tx)) new,
                -- with what spends each output and where, for what
                -- the blocks replaced
                spent =
                  [ encode i.outpoint <> encode (txHash td.tx) <> runPut (putWord32be h)
                  | td <- new,
                    BlockRef {height = h} <- [td.block],
                    i <- td.tx.inputs,
                    i.outpoint /= nullOutPoint
                  ],
                height = Just bd.height,
                lost = lostOnes
              }
      let path = map (headerHash . (.header)) (reverse added)
      let held = [td | (_, Just td) <- old]
      return (Way Between {pool, added = new, reorganised = not (null abandoned), abandoned = held, path})
    -- the transactions of the abandoned blocks, each with what the view at
    -- the first tip has of it; 'Nothing' without that view
    abandonedBodies [] = return (Just [])
    abandonedBodies bds = do
      let hs = concatMap (.txs) bds
      case from of
        Nothing -> return Nothing
        Just tip -> withViewAt views tip (\old -> zip hs . map (>>= live) <$> bodiesIn old hs)
    bodiesIn old hs = Q.retrieveManyCF old (txCF old.db) (map TxKey hs)
    live td = if td.deleted then Nothing else Just td
    confirmedBy :: DatabaseReader -> BlockData -> IO [TxData]
    confirmedBy r bd = catMaybes <$> Q.retrieveManyCF r (txCF r.db) (map TxKey bd.txs)
    coinbase td = any ((== nullOutPoint) . (.outpoint)) td.tx.inputs

-- | A transaction as the pool keeps it: its body without spenders, at the
-- time that it entered the pool, and what it does at each address: the
-- transaction touches it, spends an output of it, or pays to it. It is a
-- function of the transaction and that time alone.
poolTx :: Network -> TxData -> PoolTx
poolTx net td =
  PoolTx
    { txid = encode h,
      time,
      inputs = [encode op | op <- spent],
      body = encode (td :: TxData) {block = MemRef time, deleted = False, spenders = IntMap.empty},
      entries =
        [PoolTouches (encode a) | a <- touched]
          <> [ PoolSpends (encode a) (encode i.outpoint) p.value
             | (k, i) <- zip [0 :: Int ..] td.tx.inputs,
               i.outpoint /= nullOutPoint,
               Just p <- [IntMap.lookup k td.prevs],
               Just a <- [address p.script]
             ]
          <> [ PoolPays (encode a) (encode (OutPoint h i)) o.value o.script
             | (i, o) <- zip [0 ..] td.tx.outputs,
               Just a <- [address o.script]
             ]
          -- the outputs that hold tokens, made and used up (CashTokens)
          <> [ PoolMakes (tokenOut (OutPoint h i) o.script t) {coins = Just (o.value, o.script)}
             | (i, o) <- zip [0 ..] td.tx.outputs,
               Just t <- [scriptTokens net o.script]
             ]
          <> [ PoolUses (tokenOut i.outpoint p.script t)
             | (k, i) <- zip [0 :: Int ..] td.tx.inputs,
               i.outpoint /= nullOutPoint,
               Just p <- [IntMap.lookup k td.prevs],
               Just t <- [scriptTokens net p.script]
             ]
    }
  where
    h = txHash td.tx
    -- never 0, which in an entry is the chain's layer
    -- ("Haskoin.Store.Fleet.XPub".'score'); what a reorganisation gives
    -- back comes with the time it is given back ('wayBetween')
    time = case td.block of
      MemRef t -> max 1 t
      BlockRef {} -> max 1 td.timestamp
    spent = [i.outpoint | i <- td.tx.inputs, i.outpoint /= nullOutPoint]
    address = eitherToMaybe . scriptToAddressBS net
    tokenOut op s t =
      PoolTokenOut
        { address = encode <$> address s,
          output = encode op,
          category = encode t.category,
          amount = t.amount,
          nft = isJust t.nft,
          coins = Nothing
        }
    touched =
      nub $
        mapMaybe (address . (.script)) (IntMap.elems td.prevs)
          <> mapMaybe (address . (.script)) td.tx.outputs

-- | Of these transactions, which are settled: the pool has them
-- ('fleetSettled'). 'Nothing' if Redis does not answer in five seconds.
--
-- It is what "Haskoin.Store.Ingest" asks before it gives a place to what a
-- peer names ('Haskoin.Store.Ingest.Check'): a node names its whole
-- mempool when asked, and nearly all of it is in the pool already. The
-- ids go to Redis a thousand at a time, so that no call is long enough to
-- hold up the others.
--
-- A block of the store may confirm a transaction that the pool never had.
-- That is not looked up here, one read of the database for each of tens
-- of thousands of ids: 'memberOffer' finds it for the few that are
-- fetched.
memberSettled :: Member -> [TxHash] -> IO (Maybe [Bool])
memberSettled _ [] = return (Just [])
memberSettled m hs =
  handleAny (\_ -> return Nothing) . timeout 5000000 $
    concat <$> mapM (fleetSettled m.redis m.keys . map encode) (thousands hs)
  where
    thousands [] = []
    thousands xs = let (a, b) = splitAt 1000 xs in a : thousands b

-- | Offer a transaction to the pool, with the time at which the store has
-- it, and say what became of it, as "Haskoin.Store.Ingest" needs to know
-- ('Haskoin.Store.Ingest.Offer'):
--
-- * 'Haskoin.Store.Ingest.Settled': the pool took it or had it, or a block of the store's view
--   at the fleet tip confirms it;
--
-- * 'Haskoin.Store.Ingest.Orphan' with the transactions that it spends from and that neither
--   the tip nor the pool has;
--
-- * 'Haskoin.Store.Ingest.Again': there is no answer now. The store has no view of the fleet
--   tip, the tip moved between the reading and the offer, or the
--   transaction conflicts with the pool twice in a row.
--
-- The store says of each output that the transaction spends whether it is
-- unspent in its view at the fleet tip, so that the pool only takes what
-- is valid there. A transaction that conflicts with transactions of the
-- pool takes their place: a peer relayed it, so its node took it in place
-- of them.
--
-- One transaction to a call, and no list of those that wait: which to
-- offer and when is for the machine to say. The member held them once and
-- offered them oldest first by the second, so that a chain that a node
-- took in one second was offered child first and took as many rounds as
-- it had transactions.
memberOffer :: Member -> UnixTime -> Tx -> IO (I.Verdict TxHash)
memberOffer m time tx =
  readTVarIO m.tip >>= \case
    Nothing -> return I.Again
    Just tip -> fmap (fromMaybe I.Again) . withViewAt m.views tip $ \at ->
      Q.getTxData at h >>= \case
        Just td | not td.deleted -> return I.Settled
        _ -> do
          let pool = redisPool m.db.net m.redis m.keys (encode tip) (return ())
          found <- forM (zip [0 :: Int ..] tx.inputs) (spent at pool)
          case lefts found of
            [] -> do
              unspent <- forM [i.outpoint | i <- tx.inputs, i.outpoint /= nullOutPoint] $ \p ->
                isJust <$> Q.getUnspent at p
              let td =
                    TxData
                      { block = MemRef time,
                        tx,
                        prevs = IntMap.fromList (catMaybes (rights found)),
                        deleted = False,
                        rbf = False,
                        timestamp = time,
                        spenders = IntMap.empty
                      }
                  offer = poolOffer m.redis m.keys 32 (Just (encode tip)) (poolTx m.db.net td) unspent
                  answered second = \case
                    Taken -> return I.Settled
                    Known -> return I.Settled
                    Orphan outs -> return (I.Orphan (nub [(op :: OutPoint).hash | Right op <- map decode outs]))
                    Late -> return I.Again
                    Conflict rivals
                      | not second -> offer rivals >>= answered True
                      | otherwise -> return I.Again
              offer [] >>= answered False
            missing -> return (I.Orphan (nub missing))
  where
    h = txHash tx
    -- the output that an input spends, from the view at the tip or from
    -- the pool, or the transaction that should have it
    spent at pool (n, i)
      | i.outpoint == nullOutPoint = return (Right Nothing)
      | otherwise =
          Q.getUnspent at i.outpoint >>= \case
            Just u -> return (Right (Just (n, Prev u.script u.value)))
            Nothing ->
              pool.bodies [i.outpoint.hash] >>= \case
                [(Just parent, _)]
                  | fromIntegral i.outpoint.index < length parent.tx.outputs ->
                      let o = parent.tx.outputs !! fromIntegral i.outpoint.index
                       in return (Right (Just (n, Prev o.script o.value)))
                _ -> return (Left i.outpoint.hash)

-- | The body of a transaction of the pool, for the filters of @/events@.
memberPoolBody :: Member -> TxHash -> IO (Maybe TxData)
memberPoolBody m h =
  readTVarIO m.tip >>= \case
    Nothing -> return Nothing
    Just tip ->
      (redisPool m.db.net m.redis m.keys (encode tip) (return ())).bodies [h] <&> \case
        [(td, _)] -> td
        _ -> Nothing

-- | The time at which the newest transaction of the pool entered it, if the
-- pool has any and the member can read it at the tip that it knows: what
-- tells the health check that transactions still come in. 'Nothing' too if
-- Redis does not answer within two seconds: the health check, which asks
-- this, must not hang with a connection that does.
memberNewest :: Member -> IO (Maybe UnixTime)
memberNewest m =
  readTVarIO m.tip >>= \case
    Nothing -> return Nothing
    Just tip ->
      handleAny (\_ -> return Nothing) . fmap join . timeout 2000000 $
        (redisPool m.db.net m.redis m.keys (encode tip) (return ())).page 0 1 <&> \case
          (t, _) : _ -> Just t
          [] -> Nothing

-- | Take out of the pool the transactions that entered a mempool longer
-- ago than the configuration allows, with what spends them.
expirePool :: Member -> IO ()
expirePool m = when (m.config.expiry > 0) $ do
  (now, _) <- R.runRedis m.redis R.time >>= either (fail . show) return
  -- chosen and withdrawn in one call ('poolExpire')
  void (poolExpire m.redis m.keys 32 (fromIntegral now - fromIntegral m.config.expiry) 1000)

-- | Where a store stands when a request comes.
data Standing a
  = -- | the request was answered from the view at the fleet tip
    Answered !a
  | -- | there is no fleet tip yet: the store answers by itself
    Alone
  | -- | the store has no view of the fleet tip, so any answer of its own
    -- would differ from those of the other stores
    Behind

-- | What a read of the pool throws when the fleet tip is no longer the
-- block that the request answers from. The request is then answered again,
-- from the new tip.
data TipMoved = TipMoved
  deriving (Show)

instance Exception TipMoved

-- | What a build that did not put its entry in the cache throws, with why:
-- the job of the indexer fails, and the requests that wait for it are told
-- so ('buildFor').
newtype NotBuilt = NotBuilt String
  deriving (Show)

instance Exception NotBuilt

-- | Answer a request as a member of a fleet: read the fleet tip, and run
-- an action with the database as it was at that block and the pool as it is
-- at that tip. The action must do all its reading before it returns.
answering :: Member -> (DatabaseReader -> PoolReads -> IO a) -> IO (Standing a)
answering m action =
  memberTip m >>= \case
    Nothing -> return Alone
    Just tip -> maybe Behind Answered <$> answeringAt m tip (\r pool _ -> action r pool)

-- | The fleet tip as Redis has it now, if there is one.
memberTip :: Member -> IO (Maybe ByteString)
memberTip m = fleetTip m.redis m.keys

-- | Run an action with the database as it was at a block that is taken to
-- be the fleet tip, and the pool as it is at that tip, or give 'Nothing' if
-- the store has no view of that block. The action must do all its reading
-- before it returns.
--
-- The block need not be the tip any more: it can be the one that the
-- member read at its last turn, which saves a request the round trip of
-- asking. Every read of the pool checks it and throws v'TipMoved' if the tip
-- is another one. The action is also given a check to run when it has what
-- it needs: it asks Redis for the tip if no read of the pool has done so,
-- and no request saw that tip in Redis in the last 'seenFor', and throws
-- the same. An answer that passed either is one that every
-- store of the fleet gave at that moment.
answeringAt :: Member -> ByteString -> (DatabaseReader -> PoolReads -> IO () -> IO a) -> IO (Maybe a)
answeringAt m tip action = answeringWith m tip (\r pool _ confirm -> action r pool confirm)

-- | 'answeringAt', for an action that reads Redis by other ways than the
-- pool too: it is also given what to run after each such read that found
-- the tip where it should be.
answeringWith :: Member -> ByteString -> (DatabaseReader -> PoolReads -> IO () -> IO () -> IO a) -> IO (Maybe a)
answeringWith m tip action = case decode tip of
  Left _ -> return Nothing
  Right block -> withViewAt m.views block $ \r -> do
    checked <- newIORef False
    let saw = getMonotonicTimeNSec >>= \t -> writeIORef m.seen (tip, t)
        confirm =
          readIORef checked >>= \case
            True -> return ()
            False -> do
              (last', at) <- readIORef m.seen
              t <- getMonotonicTimeNSec
              if last' == tip && t - at < seenFor
                then writeIORef checked True
                else do
                  now <- memberTip m
                  if now == Just tip then saw >> writeIORef checked True else throwIO TipMoved
        found = saw >> writeIORef checked True
    action r (redisPool m.db.net m.redis m.keys tip found) found confirm

-- | 'answeringAt' with the queries ready: those of
-- "Haskoin.Store.Database.Query.Pooled" for the view and the pool, and
-- the same with the cache of extended public keys in front
-- ('fleetSession'), which is what requests are answered with unless they
-- ask not to be.
answeringCached :: Member -> ByteString -> (StoreRead -> Session -> IO () -> IO a) -> IO (Maybe a)
answeringCached m tip action = answeringWith m tip $ \r pool found confirm -> do
  sess <- fleetSession m tip r pool found
  action (pooledRead r pool) sess confirm

-------------------------------------------------------------------------------
-- The cache of extended public keys
-------------------------------------------------------------------------------

-- | The reads of the fleet's cache of xpubs for one request, at the tip
-- that it answers from, with its view and the pool: the balances of a key
-- if the cache has it with the addresses that it has now, and its other
-- lists from the entry with as many addresses. A read that finds the tip
-- moved throws v'TipMoved'; a miss is 'Nothing', and the door asks.
fleetSession :: Member -> ByteString -> DatabaseReader -> PoolReads -> IO () -> IO Session
fleetSession m tip r pool found = do
  seen <- newIORef HashMap.empty
  let hit :: Cached a -> IO (Maybe a)
      hit = \case
        Hit x -> Just x <$ found
        Miss -> return Nothing
        Moved -> throwIO TipMoved
      entryOf x = HashMap.lookup (xpubId x) <$> readIORef seen
      -- a list of the version of the entry that the request read, or the
      -- request is answered again
      same :: Entry -> Maybe (ByteString, b) -> IO (Maybe b)
      same e = \case
        Just (v, a) | v == e.version -> return (Just a)
        Just _ -> throwIO EntryMoved
        Nothing -> return Nothing
      paged :: XPubSpec -> Limits -> (Entry -> From -> IO (Cached (ByteString, [a]))) -> IO (Maybe [a])
      paged x limits cached =
        entryOf x >>= \case
          Nothing -> throwIO EntryMoved
          Just e ->
            pageStart r pool limits.start >>= \case
              Nothing -> return (Just [])
              Just from -> cached e from >>= hit >>= same e
  return
    Session
      { reads =
          XPubReads
            { balances = \x ->
                cachedEntry m.redis m.keys tip r.initGap r.maxGap True x >>= hit >>= \case
                  Just e -> do
                    -- read again, as after a miss: of the version read first
                    entryOf x >>= mapM_ (\old -> when (old.version /= e.version) (throwIO EntryMoved))
                    tally m (.hits) 1
                    -- payments used up most of the lookahead: the indexer
                    -- extends it, and the entry answers meanwhile
                    when e.short (void (prefetch m.indexer x ()))
                    Just e.balances <$ modifyIORef' seen (HashMap.insert (xpubId x) e)
                  Nothing -> Nothing <$ tally m (.misses) 1,
              transactions = \x limits ->
                paged x limits $ \e from -> cachedTxs m.redis m.keys tip x e.sizes from limits,
              unspents = \x _ limits -> paged x limits $ \e from -> cachedUnspents m.redis m.keys tip x e from limits,
              count = \x ->
                entryOf x >>= \case
                  Nothing -> throwIO EntryMoved
                  Just e -> cachedCount m.redis m.keys tip x e.sizes >>= hit >>= same e,
              bodies = return . map (const Nothing),
              wallet = return
            },
        settle = return ()
      }

-- | The time in milliseconds, by which a build is timed.
milliseconds :: IO Word64
milliseconds = round . (* 1000) <$> getPOSIXTime

-- | Run the indexer of the fleet's cache of xpubs, and hear the builds that
-- Redis announces, for as long as the store runs. The indexer is apart from
-- the turns of the member because an entry of a wallet with a long history
-- takes seconds to read, and a member that does not report for that long
-- loses its lease.
memberCacheLoop :: Member -> IO a
memberCacheLoop m = withAsync hear $ \a -> link a >> runIndexer m.indexer >> loopForever (return ())
  where
    -- a connection that ends is made again, a second later after the first
    -- failure and up to five seconds after many: Redis that is down once
    -- made this a loop that never slept. A build announced while the
    -- connection was down is lost; the requests that wait for it find out
    -- when their wait runs out, and look again, as for any build that
    -- another store did not announce. Waking them all at each reconnection,
    -- as this once did, told them of builds that never happened
    hear = go (0 :: Int)
      where
        go failures = do
          ctl <- R.newPubSubController [(m.config.prefix <> "fleet:built", heard)] []
          r <- tryAny (R.pubSubForever m.redis ctl (return ()))
          let failures' = either (const (failures + 1)) (const 0) r
          threadDelay (1000000 * min 5 (max 1 failures'))
          go failures'
    -- only a key that a request waits for is counted
    heard i = atomically (modifyTVar' m.built (HashMap.adjust (\(n, w) -> (n + 1, w)) i))

-- | Wait at most so many microseconds for Redis to announce a build of the
-- entry of a key; whether it did. The request counts itself among those
-- that wait for the key while it waits, and the last to go takes the key
-- out of the map ('built'), so that the map never grows past the requests
-- that wait.
awaitBuilt :: Member -> XPubSpec -> Int -> IO Bool
awaitBuilt m x us = bracket enter leave $ \n0 -> do
  timer <- registerDelay (max 1 us)
  atomically $
    (count >>= check . (> n0) >> return True)
      `orElse` (readTVar timer >>= check >> return False)
  where
    k = xpubId x
    count = maybe 0 fst . HashMap.lookup k <$> readTVar m.built
    enter = atomically $ do
      n0 <- count
      modifyTVar' m.built (HashMap.insertWith (\_ (n, w) -> (n, w + 1)) k (n0, 1))
      return n0
    leave _ =
      atomically . modifyTVar' m.built $
        HashMap.update (\(n, w) -> if w <= 1 then Nothing else Just (n, w - 1)) k

-- | Build the entry of a key at the fleet tip, the job of the indexer, and
-- announce it; then evict past the size of the cache.
--
-- A build waits while a move of the tip is under way, a few milliseconds
-- at a time, and is then made at the new tip, which it reads from Redis:
-- the member's own may be a turn old.
buildFor :: Member -> XPubSpec -> IO ()
buildFor m spec = do
  now <- milliseconds
  building (200 :: Int) >>= \case
    Nothing -> return ()
    Just built -> do
      let name = decodeUtf8 (xpubId spec)
          -- announced only once it is in the cache: a request woken by the
          -- announcement reads the entry, and one woken for a build that was
          -- abandoned found nothing and answered 503
          announce = void (R.runRedis m.redis (R.publish (m.config.prefix <> "fleet:built") (xpubId spec)))
      took <- subtract now <$> milliseconds
      case built of
        Just (Built n) -> do
          announce
          when (n > 0) $ do
            tally m (.builds) 1
            forM_ m.metrics (\x -> addTiming x.buildTime (fromIntegral took))
            m.config.note LevelDebug $
              "Cached xpub " <> name <> ": " <> shown n <> " addresses added in " <> shown took <> " ms"
            gone <- cacheEvict m.redis m.keys m.config.cacheMax
            when (gone > 0) $ tally m (.evictions) gone
        -- not built: the job fails, and the requests that wait for it are
        -- told so, where they were told that it was done
        Just Abandoned -> tally m (.abandoned) 1 >> throwIO (NotBuilt "abandoned at a move of the tip")
        Just other -> throwIO (NotBuilt (show other))
        Nothing -> return ()
  where
    building tries =
      memberTip m >>= \case
        Nothing -> return Nothing
        Just tip -> do
          built <-
            handle (\TipMoved -> return (Just Abandoned)) $
              answeringAt m tip (\r pool _ -> cacheBuild m.redis m.keys tip r pool spec)
          case built of
            Just Busy | tries > 0 -> threadDelay 25000 >> building (tries - 1)
            Just Abandoned | tries > 197 -> building (tries - 1)
            _ -> return (Just built)

-- | Count and say that this store emptied the cache, at a block and for a
-- reason. It is information and not a warning: nothing is wrong with what
-- the fleet answers, and entries are built again on demand. It happens
-- only when the pool starts again, or when a store cannot say what the
-- abandoned blocks of a reorganisation held.
wasEmptied :: Member -> ByteString -> Text -> IO ()
wasEmptied m block why = do
  tally m (.emptied) 1
  m.config.note LevelInfo ("Emptied the xpub cache at block " <> blockName block <> ": " <> why)

-- | A block as a log names it.
blockName :: ByteString -> Text
blockName bytes = either (const "?") blockHashToHex (decode bytes :: Either String BlockHash)

shown :: (Show a) => a -> Text
shown = T.pack . show

-- | Tell StatsD how large the cache of extended public keys is, at every
-- turn: it is one small read, and the numbers are the fleet's, the same
-- from every store. The cache cannot lag behind the tip: it moves with it
-- ('moveTip').
cacheGauges :: Member -> IO ()
cacheGauges m =
  forM_ m.metrics $ \x -> do
    (entries, held) <- cacheSize m.redis m.keys
    setGauge x.xpubs entries
    setGauge x.addresses held

-- | The shared pool of a fleet as it is at a tip, for the queries of
-- "Haskoin.Store.Database.Query.Pooled". Every read throws v'TipMoved' if
-- the tip is another one by then, and runs an action if it is not, by which
-- a caller knows that the tip was checked.
redisPool :: Network -> R.Connection -> FleetKeys -> ByteString -> IO () -> PoolReads
redisPool net conn keys tip checked =
  PoolReads
    { summaries = \as -> case as of
        [] -> return []
        _ -> map summary <$> ask "sums" (map encode as),
      addressTxs = \a start n -> do
        rows <- ask "txs" (encode a : from start <> [num n])
        return [(number time, h) | [member, time] <- groups 2 rows, Right h <- [decode member]],
      addressOuts = \a start n -> do
        rows <- ask "outs" (encode a : from start <> [num n])
        let paid time p v s = Unspent {block = MemRef time, outpoint = p, value = v, script = s, address = Just a}
        return
          [ (paid (number time) p (number v) s, f == "1")
          | [member, time, kept, f] <- groups 4 rows,
            Right p <- [decode member],
            let (v, s) = B.drop 1 <$> C.break (== ' ') kept
          ],
      spentOutputs = \a ps -> case ps of
        [] -> return []
        _ -> map (== '1') . C.unpack . B.concat <$> ask "spent" [encode a, B.concat (map encode ps)],
      bodies = \hs -> case hs of
        [] -> return []
        _ -> transactions <$> ask "bodies" [B.concat (map encode hs)],
      spenders = \ps -> case ps of
        [] -> return []
        _ -> do
          rows <- ask "spenders" [B.concat (map encode ps)]
          return [spender t i | [t, i] <- groups 2 rows],
      page = \offset limit -> do
        rows <- ask "page" [num offset, num limit]
        return [(number time, h) | [member, time] <- groups 2 rows, Right h <- [decode member]],
      startingWith = \start -> rights . map decode <$> ask "ids" [B.drop 1 (encode (TxKeyS start))],
      replaced = \h ->
        ask "replaced" [encode h] >>= \case
          [value] | Right (by, height) <- runGet ((,) <$> get <*> getWord32be) value -> return (Just Replaced {..})
          _ -> return Nothing,
      addressTokens = \a -> do
        rows <- ask "tokens" [encode a]
        return [(c, t) | [m, row] <- groups 2 rows, Right c <- [decode m], Just t <- [tokens row]],
      categoryHolders = \c -> do
        rows <- ask "holders" [encode c]
        return [(a, t) | [m, row] <- groups 2 rows, Right a <- [decode m], Just t <- [tokens row]],
      categorySummary = \c ->
        ask "csum" [encode c] <&> \case
          [row] | [txs, free, spent] <- map number (C.words row) -> Just CategorySummary {..}
          _ -> Nothing,
      categoryTxs = \c start n -> do
        rows <- ask "ctxs" (encode c : from start <> [num n])
        return [(number time, h) | [member, time] <- groups 2 rows, Right h <- [decode member]],
      categoryOuts = \c start n -> do
        rows <- ask "couts" (encode c : from start <> [num n])
        let made time p v s =
              Unspent
                { block = MemRef time,
                  outpoint = p,
                  value = v,
                  script = s,
                  address = eitherToMaybe (scriptToAddressBS net s)
                }
        return
          [ (made (number time) p (number v) s, f == "1")
          | [member, time, kept, f] <- groups 4 rows,
            Right p <- [decode member],
            let (v, s) = B.drop 1 <$> C.break (== ' ') kept
          ]
    }
  where
    ask what args = poolRead conn keys 32 (Just tip) what args >>= maybe (throwIO TipMoved) (<$ checked)
    num :: (Show a) => a -> ByteString
    num = C.pack . show
    number :: (Num a) => ByteString -> a
    number = maybe 0 (fromInteger . fst) . C.readInteger
    from :: (Serialize k) => Maybe (UnixTime, k) -> [ByteString]
    from = maybe ["", ""] (\(time, k) -> [num time, encode k])
    groups n rows
      | null rows = []
      | otherwise = take n rows : groups n (drop n rows)
    -- for each transaction: its body, how many of its outputs the pool
    -- spends, and those with what spends them
    transactions (bytes : count : rest) =
      let (spent, more) = splitAt (3 * number count) rest
          inputs =
            IntMap.fromList
              [ (fromIntegral p.index, s)
              | [o, t, i] <- groups 3 spent,
                Right p <- [outpoint o],
                Just s <- [spender t i]
              ]
       in (body bytes, inputs) : transactions more
    transactions _ = []
    outpoint :: ByteString -> Either String OutPoint
    outpoint = decode
    -- the sums of the tokens of a category at an address, each amount
    -- in two parts, the digits above the last nine and the last nine,
    -- joined again modulo 2^64 as the chain's sums are
    tokens row = case map number (C.words row) :: [Integer] of
      [ txs,
        freeHi,
        freeLo,
        freeNfts,
        freeOutputs,
        spentHi,
        spentLo,
        spentNfts,
        spentOutputs,
        receivedHi,
        receivedLo
        ] ->
          Just
            PoolTokens
              { txs = fromInteger txs,
                free = joined freeHi freeLo,
                freeNfts = fromInteger freeNfts,
                freeOutputs = fromInteger freeOutputs,
                spent = joined spentHi spentLo,
                spentNfts = fromInteger spentNfts,
                spentOutputs = fromInteger spentOutputs,
                received = joined receivedHi receivedLo
              }
      _ -> Nothing
    joined hi lo = fromInteger (hi * 1000000000 + lo)
    summary row = case map number (C.words row) of
      [txs, paid, paidCount, spent, spentCount, both, bothCount] -> Just MemSummary {..}
      _ -> Nothing
    -- Whether a transaction of the pool can be replaced by fee is a fact
    -- of its network: every one can on Bitcoin, whose nodes take a
    -- replacement of any unconfirmed transaction since Bitcoin Core 28,
    -- whatever the transaction says of itself, and none can on Bitcoin
    -- Cash, which has no such rule. The pool worked it out once, from
    -- what each transaction signalled and what it spent from (BIP 125),
    -- and kept that right as parents were confirmed and came back.
    body bytes
      | B.null bytes = Nothing
      | otherwise = either (const Nothing) (\td -> Just (td :: TxData) {rbf = net.replaceByFee}) (decode bytes)
    spender t i
      | B.null t = Nothing
      | otherwise = either (const Nothing) (\h -> Just Spender {txid = h, index = number i}) (decode t)
