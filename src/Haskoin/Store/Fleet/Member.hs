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
-- * __It offers to the shared pool what its peers send it__ ('memberReceived',
--   'offerMempool'), each
--   transaction with whether its inputs are unspent in the store's view at
--   the fleet tip, so that the pool only takes what is valid there. What
--   the pool decided on is seen by the fleet, in Redis, and no store
--   fetches it again when a peer announces it ('memberSeen').
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
    offerMempool,
    memberReceived,
    memberSeen,
    mostSeen,
    memberPoolBody,
    memberNewest,
    expirePool,
    poolTx,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (link, withAsync)
import Control.Concurrent.STM
  ( TVar,
    atomically,
    check,
    modifyTVar',
    newTVarIO,
    orElse,
    readTVar,
    readTVarIO,
    registerDelay,
    retry,
    writeTVar,
  )
import Control.Exception (Exception, handle, throwIO)
import Control.Monad (filterM, forM, forM_, unless, void, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.Either (rights)
import Data.Functor ((<&>))
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.IntMap.Strict qualified as IntMap
import Data.List (nub, partition, sortOn)
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
import Haskoin
  ( BlockHash,
    BlockHeader (..),
    Network (..),
    OutPoint (..),
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
  )
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Pooled (From, PoolReads (..), pageStart, pooledRead)
import Haskoin.Store.Database.Reader (DatabaseReader (..), txCF, withSnapshotReader)
import Haskoin.Store.Database.Types (MemSummary (..), TxKey (..))
import Haskoin.Store.Database.Views (Views, heldViews, keepViews, newViews, takeView, withViewAt)
import Haskoin.Store.Exception (handleAny, tryAny)
import Haskoin.Store.Fleet qualified as F
import Haskoin.Store.Fleet.Redis
import Haskoin.Store.Fleet.XPub
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
    -- | the transactions that peers sent and that are still to offer: new,
    -- refused as orphans, or with no time to offer them yet, with the times
    -- at which the store heard of them; they are offered at the next turn.
    -- What the pool has decided on is in the fleet's seen set instead
    -- ('memberSeen'), which Redis keeps for every store.
    waiting :: !(TVar (HashMap TxHash (UnixTime, Tx))),
    -- | whether the store's own health checks pass. While they do not, the
    -- member does nothing, so that its lease runs out and a store that is
    -- stuck does not hold the fleet back.
    healthy :: !(TVar Bool),
    -- | the indexer that builds the entries of the xpubs that requests
    -- miss, one store of the fleet for each, holding its dibs in Redis
    indexer :: !(Indexer XPubSpec ()),
    lock :: !(Lock XPubSpec),
    -- | how many builds Redis announced, by xpub, for those that wait
    built :: !(TVar (HashMap ByteString Int)),
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
  waiting <- newTVarIO HashMap.empty
  healthy <- newTVarIO True
  metrics <- mapM newCacheMetrics config.stats
  outage <- newOutage
  built <- newTVarIO HashMap.empty
  epoch <- newTVarIO Nothing
  restarts <- newTVarIO Nothing
  refills <- newTVarIO 0
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
noteBest :: Member -> BlockHash -> IO ()
noteBest m block = do
  takeView m.views block
  atomically . modifyTVar' m.recent $ \blocks -> take m.config.depth (block : filter (/= block) blocks)
  prune m

-- | Let go of the views that the rules do not ask for: all but those of
-- the recent blocks, of the last report, and of the tip read after it.
prune :: Member -> IO ()
prune m = do
  recent <- readTVarIO m.recent
  reported <- readTVarIO m.reported
  tip <- readTVarIO m.tip
  keepViews m.views (nub (F.keep (recent <> reported) tip))

-- | Whether Redis answers a ping within two seconds.
memberAlive :: Member -> IO Bool
memberAlive m =
  handleAny (\_ -> return False) $
    timeout 2000000 (R.runRedis m.redis R.ping) <&> \case
      Just (Right _) -> True
      _ -> False

-- | One round of what a member does: report, move the tip if the stores
-- agree on another block, offer what peers sent that it has not offered
-- yet, let go of old views, and trim what the fleet has seen
-- ('fleetTrimSeen'). Call it every second or two, and when
-- the best block changes. A store whose own health checks fail must not
-- call it: its lease then runs out, and the fleet goes on without it.
memberTick :: Member -> IO ()
memberTick m = do
  noteEpoch m
  noteRestarts m
  report m
  moved <- moveTip m
  -- the tip is read after the report, as the rule of 'F.keep' asks
  tip <- fmap decoded <$> fleetTip m.redis m.keys
  atomically (writeTVar m.tip (fromMaybe Nothing tip))
  prune m
  cacheGauges m
  unless moved (expirePool m)
  offerMempool m
  void (fleetTrimSeen m.redis m.keys mostSeen)
  where
    decoded = either (const Nothing) Just . decode

-- | Find out whether Redis lost its data since this store's last turn.
-- Redis keeps an epoch, a random number that the first store to find it
-- missing sets. A store that finds it missing or changed forgets its view
-- of the fleet tip, so that it is
-- unhealthy until a tip is set again, which its next turn does, since
-- every live store reports. It counts the loss in 'refills', and the web
-- server then asks the peers, and the node over RPC if it has one, for
-- their mempools, which fill the pool again through 'memberReceived'.
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

-- | Take a turn as a member every two seconds, and at once when an action
-- that waits for the next best block returns, for as long as the store's
-- health checks pass. A turn that fails, as when Redis is away, is tried
-- again at the next, and logged as an outage is ('attempt').
memberLoop :: Member -> IO () -> IO a
memberLoop m nextBlock = go
  where
    go = do
      void (timeout 2000000 nextBlock)
      well <- readTVarIO m.healthy
      when well (void (attempt m.outage m.config.note "take a turn in the fleet" (memberTick m)))
      go

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
report :: Member -> IO ()
report m = do
  held <- heldViews m.views
  recent <- readTVarIO m.recent
  blocks <- filterM main (filter (`elem` held) recent)
  taken <- fleetReport m.redis m.keys Nothing m.config.lease m.config.name (map encode blocks) (map encode held)
  atomically (writeTVar m.reported (if taken then blocks else []))
  unless taken . m.config.note LevelDebug $
    "Report not taken: this store has no view of the fleet tip, and another store is live"
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
-- wrote are dropped ('cacheAbort'). When this store cannot say what the
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
        Begun -> moveWith seen token target
    agreed -> do
      when (isNothing agreed) . m.config.note LevelDebug $
        "The live stores have no block in common: " <> shown (Map.size reports) <> " reports"
      return False
  where
    -- the move under a token of the cache: what the blocks did at the
    -- addresses of the entries is written in pieces, and the commit moves
    -- the pool, the tip and the cache together
    moveWith seen token target = do
      way <- case (traverse decode seen, decode target) of
        (Right from, Right to) -> wayBetween m.views m.db from to
        _ -> return NoWay
      let between = case way of
            Way b -> Just b
            _ -> Nothing
          path = maybe [] (map encode . (.path)) between
      -- what the blocks did at the addresses of the entries, read at the
      -- new tip: without a view of it, this store does not move the tip
      written <- case (way, decode target) of
        (Way b, Right to) ->
          withViewAt m.views to (\r -> tipMoves m.redis m.keys r b.abandoned b.added) >>= \case
            Just moves -> cachePieces m.redis m.keys token (fromMaybe "" seen) moves
            Nothing -> return False
        (Way _, _) -> return False
        -- the store's chain moved since its report: not this turn
        (Unsettled, _) -> return False
        _ -> return True
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
            _ -> fleetCommit m.redis m.keys 32 Nothing seen target ((.pool) <$> between) path token
      m.config.note LevelDebug . mconcat $
        [ "Fleet tip from ",
          maybe "none" blockName seen,
          " to ",
          blockName target,
          case moved of
            MovedTo _ -> ": moved"
            Stays -> ": stays"
            Raced -> ": not moved, another store did it first or has to",
          case way of
            Way _ -> ""
            NoWay -> " (this store does not know the way)"
            Unsettled -> " (the chain of this store moved since its report)"
            TooFar -> " (the way is too long)"
        ]
      case moved of
        MovedTo _ -> do
          let restarted = case way of
                Way _ -> False
                _ -> True
          when restarted $ wasEmptied m target "the shared mempool started again"
          return True
        _ -> False <$ cacheAbort m.redis m.keys token
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
  | -- | nothing: it does not have the first tip, or no view of the
    -- database at it. Another store may know.
    NoWay
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
-- view the store cannot say what they held, and the way is v'NoWay': it
-- does not move the tip, which keeps the cache of extended public keys
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
            Right added -> maybe (return NoWay) (wayWith r bd abandoned added) =<< abandonedBodies (reverse abandoned)
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
      signals = net.replaceByFee && any ((< 0xffffffff - 1) . (.sequence)) td.tx.inputs,
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
    touched =
      nub $
        mapMaybe (address . (.script)) (IntMap.elems td.prevs)
          <> mapMaybe (address . (.script)) td.tx.outputs

-- | Offer to the pool the transactions that peers sent this store and that
-- it has not offered yet ('memberReceived'), oldest first, so that each
-- comes after those that it spends from. A store without a view of the
-- fleet tip offers nothing: it cannot say what is unspent there.
--
-- A transaction whose inputs neither the tip nor the pool has, an orphan
-- for now, waits for the next round, as does everything after an offer
-- that the pool turned away because the tip had moved. An orphan is let go
-- 'orphanLife' after the store heard of it: its parent is not coming. One that conflicts
-- with transactions of the pool takes their place: a peer relayed it, so
-- its node took it in place of them.
--
-- A round offers 'mostOffers' transactions at most and leaves the rest
-- waiting, so that a store with many to offer still reports in time to
-- keep its lease.
offerMempool :: Member -> IO ()
offerMempool m =
  readTVarIO m.tip >>= \case
    Nothing -> return ()
    Just tip -> void . withViewAt m.views tip $ \at -> do
      now <- round <$> getPOSIXTime
      held <- readTVarIO m.waiting
      let todo = take mostOffers (sortOn (fst . snd) (HashMap.toList held))
          pool = redisPool m.redis m.keys (encode tip) (return ())
      go now at pool tip todo
  where
    go _ _ _ _ [] = return ()
    go now at pool tip ((h, (time, tx)) : rest) =
      txDataAt at pool time tx >>= \case
        -- an input that neither the tip nor the pool has: an orphan for now
        Nothing -> orphan now h time >> go now at pool tip rest
        Just td -> do
          unspent <- forM [i.outpoint | i <- tx.inputs, i.outpoint /= nullOutPoint] $ \p ->
            isJust <$> Q.getUnspent at p
          let offer = poolOffer m.redis m.keys 32 (Just (encode tip)) (poolTx m.db.net td) unspent
              done = atomically (modifyTVar' m.waiting (HashMap.delete h))
          offer [] >>= \case
            Taken -> done >> go now at pool tip rest
            Known -> done >> go now at pool tip rest
            Orphan _ -> orphan now h time >> go now at pool tip rest
            Late -> return ()
            -- a peer relayed it, so its node took it in place of what it
            -- conflicts with: it replaces them in the pool too
            Conflict rivals -> offer rivals >> done >> go now at pool tip rest
    orphan now h time =
      when (now > time + orphanLife) $
        atomically (modifyTVar' m.waiting (HashMap.delete h))

-- | A transaction that the store heard from a peer, to offer to the pool at
-- the next turn, with the time at which it was heard. There is no mempool
-- of the store's own: the pool in Redis is the mempool.
memberReceived :: Member -> UnixTime -> Tx -> IO ()
memberReceived m time tx =
  atomically $ modifyTVar' m.waiting (HashMap.insertWith (\_ old -> old) (txHash tx) (time, tx))

-- | Of these transactions, those that the block store need not fetch when
-- a peer announces them: those that this store holds to offer, and those
-- that the fleet has seen ('fleetTrimSeen'), asked of Redis in one round
-- trip. The fleet sees a transaction only once the pool has decided on it,
-- so what this store received and has not offered yet is known here alone.
-- Without an answer from Redis within a second, only what the store holds;
-- the store then fetches the rest, and finds out at the offer what the pool
-- has.
memberSeen :: Member -> [TxHash] -> IO (HashSet TxHash)
memberSeen m hs = do
  held <- readTVarIO m.waiting
  let (mine, others) = partition (`HashMap.member` held) hs
  fleet <-
    if null others
      then return HashSet.empty
      else
        handleAny (\_ -> return HashSet.empty) $
          timeout 1000000 (fleetSeen m.redis m.keys (map encode others)) <&> \case
            Just flags -> HashSet.fromList [h | (h, True) <- zip others flags]
            Nothing -> HashSet.empty
  return (HashSet.union (HashSet.fromList mine) fleet)

-- | How many transactions the fleet remembers having seen, the newest
-- ('fleetTrimSeen'), trimmed at each turn of a member. A peer announces a transaction within seconds of having
-- it, so a hundred thousand covers far more than the time in which the
-- peers of the fleet announce one, at any rate that Bitcoin carries.
mostSeen :: Int
mostSeen = 100000

-- | The body of a transaction of the pool, for the filters of @/events@.
memberPoolBody :: Member -> TxHash -> IO (Maybe TxData)
memberPoolBody m h =
  readTVarIO m.tip >>= \case
    Nothing -> return Nothing
    Just tip ->
      (redisPool m.redis m.keys (encode tip) (return ())).bodies [h] <&> \case
        [(td, _)] -> td
        _ -> Nothing

-- | The time at which the newest transaction of the pool entered it, if the
-- pool has any and the member can read it at the tip that it knows: what
-- tells the health check that transactions still come in.
memberNewest :: Member -> IO (Maybe UnixTime)
memberNewest m =
  readTVarIO m.tip >>= \case
    Nothing -> return Nothing
    Just tip ->
      handleAny (\_ -> return Nothing) $
        (redisPool m.redis m.keys (encode tip) (return ())).page 0 1 <&> \case
          (t, _) : _ -> Just t
          [] -> Nothing

-- | A transaction with the outputs that it spends, from the view at the tip
-- or from the pool; 'Nothing' if one of them is in neither.
txDataAt :: DatabaseReader -> PoolReads -> UnixTime -> Tx -> IO (Maybe TxData)
txDataAt at pool time tx = do
  prevs <- forM (zip [0 :: Int ..] tx.inputs) $ \(n, i) ->
    if i.outpoint == nullOutPoint
      then return (Just Nothing)
      else
        Q.getUnspent at i.outpoint >>= \case
          Just u -> return (Just (Just (n, Prev u.script u.value)))
          Nothing ->
            pool.bodies [i.outpoint.hash] >>= \case
              [(Just parent, _)]
                | fromIntegral i.outpoint.index < length parent.tx.outputs ->
                    let o = parent.tx.outputs !! fromIntegral i.outpoint.index
                     in return (Just (Just (n, Prev o.script o.value)))
              _ -> return Nothing
  return $ do
    ps <- sequence prevs
    return
      TxData
        { block = MemRef time,
          tx,
          prevs = IntMap.fromList (catMaybes ps),
          deleted = False,
          rbf = False,
          timestamp = time,
          spenders = IntMap.empty
        }

-- | How long, in seconds, a transaction waits for its parent before it is
-- let go: twenty minutes, as Bitcoin Core keeps its orphans. A parent comes
-- within seconds when it comes at all, in a block or from a peer; one that
-- has not come by then was replaced or never reached the network. Kept for
-- ever, such orphans would pile up, and once they were 'mostOffers', the
-- oldest first, a round would offer nothing else.
orphanLife :: UnixTime
orphanLife = 1200

-- | The most transactions that one round offers. An offer takes a tenth of
-- a millisecond or two, so this is well under the two seconds of a round.
mostOffers :: Int
mostOffers = 2000

-- | Take out of the pool the transactions that entered a mempool longer
-- ago than the configuration allows, with what spends them.
expirePool :: Member -> IO ()
expirePool m = when (m.config.expiry > 0) $ do
  (now, _) <- R.runRedis m.redis R.time >>= either (fail . show) return
  let before = fromIntegral now - fromIntegral m.config.expiry :: Double
  old <-
    R.runRedis m.redis (R.zrangebyscoreLimit (m.keys.prefix <> "pool:time") 0 before 0 1000)
      >>= either (fail . show) return
  unless (null old) (void (poolWithdraw m.redis m.keys 32 old))

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
    action r (redisPool m.redis m.keys tip found) found confirm

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
    hear = loopForever $ do
      ctl <- R.newPubSubController [(m.config.prefix <> "fleet:built", heard)] []
      _ <- tryAny (R.pubSubForever m.redis ctl (return ()))
      atomically (modifyTVar' m.built (HashMap.map (+ 1)))
    heard i = atomically (modifyTVar' m.built (HashMap.insertWith (+) i 1))

-- | Wait at most so many microseconds for Redis to announce a build of the
-- entry of a key; whether it did.
awaitBuilt :: Member -> XPubSpec -> Int -> IO Bool
awaitBuilt m x us = do
  n0 <- HashMap.lookupDefault 0 (xpubId x) <$> readTVarIO m.built
  timer <- registerDelay (max 1 us)
  atomically $
    (HashMap.lookupDefault 0 (xpubId x) <$> readTVar m.built >>= check . (> n0) >> return True)
      `orElse` (readTVar timer >>= check >> return False)

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
      took <- subtract now <$> milliseconds
      _ <- R.runRedis m.redis (R.publish (m.config.prefix <> "fleet:built") (xpubId spec))
      case built of
        Just (Built n) | n > 0 -> do
          tally m (.builds) 1
          forM_ m.metrics (\x -> addTiming x.buildTime (fromIntegral took))
          m.config.note LevelDebug $
            "Cached xpub " <> name <> ": " <> shown n <> " addresses added in " <> shown took <> " ms"
          gone <- cacheEvict m.redis m.keys m.config.cacheMax
          when (gone > 0) $ tally m (.evictions) gone
        Just Abandoned -> tally m (.abandoned) 1
        _ -> return ()
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
redisPool :: R.Connection -> FleetKeys -> ByteString -> IO () -> PoolReads
redisPool conn keys tip checked =
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
          _ -> return Nothing
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
    -- for each transaction: its body, whether it can be replaced by fee,
    -- how many of its outputs the pool spends, and those with what spends
    -- them
    transactions (bytes : flag : count : rest) =
      let (spent, more) = splitAt (3 * number count) rest
          inputs =
            IntMap.fromList
              [ (fromIntegral p.index, s)
              | [o, t, i] <- groups 3 spent,
                Right p <- [outpoint o],
                Just s <- [spender t i]
              ]
       in (body bytes (flag == "1"), inputs) : transactions more
    transactions _ = []
    outpoint :: ByteString -> Either String OutPoint
    outpoint = decode
    summary row = case map number (C.words row) of
      [txs, paid, paidCount, spent, spentCount, both, bothCount] -> Just MemSummary {..}
      _ -> Nothing
    -- whether a transaction can be replaced by fee is what the pool says
    -- now, not what the store that offered it found then
    body bytes can
      | B.null bytes = Nothing
      | otherwise = either (const Nothing) (\td -> Just (td :: TxData) {rbf = can}) (decode bytes)
    spender t i
      | B.null t = Nothing
      | otherwise = either (const Nothing) (\h -> Just Spender {txid = h, index = number i}) (decode t)
