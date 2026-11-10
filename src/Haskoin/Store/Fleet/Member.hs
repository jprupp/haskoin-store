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
-- out through "Haskoin.Store.Fleet.Redis", and the reasons are in "A fleet
-- that answers as one" in @docs\/layered-database.md@.
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
--   across a reorganisation which transactions come back.
--
-- * __It offers its mempool to the shared pool__ ('offerMempool'), each
--   transaction with whether its inputs are unspent in the store's view at
--   the fleet tip, so that the pool only takes what is valid there.
--
-- * __It answers from the view at the fleet tip with the pool on top__
--   ('answering', 'redisPool'), or says that it is behind when it has no
--   view of the tip.
--
-- * __It keeps the fleet's cache of extended public keys__
--   ("Haskoin.Store.Fleet.XPub"): it answers the queries about a key from
--   it when the cache has the key ('answeringCached'), builds the entries
--   that requests missed ('memberCacheLoop'), brings the cache to the new
--   block when it moves the tip, and empties a cache that was left behind
--   ('syncCache').
--
-- = Why the member does not withdraw what it deletes
--
-- A store deletes a transaction from its own mempool when it replaces it,
-- when a block confirms a conflicting one, and when it is too old. The
-- first reaches the pool with the offer of the replacement, which names
-- what it replaces. The second reaches it when the tip moves to that block.
-- The third is done on the pool itself, by its own times ('expirePool'), so
-- that the stores do not have to agree on when each of them first saw a
-- transaction. A store that simply does not have a transaction says
-- nothing about it: another store has it.
module Haskoin.Store.Fleet.Member
  ( -- * A member
    Member (..),
    MemberConfig (..),
    newMember,
    noteBest,
    memberForget,
    memberTick,
    memberLoop,
    memberAlive,
    memberEvents,

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
    wantCached,
    syncCache,

    -- * The parts of a tick
    report,
    moveTip,
    Between (..),
    blocksBetween,
    changeBetween,
    offerMempool,
    expirePool,
    poolTx,
  )
where

import Control.Exception (Exception, throwIO)
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
import Data.IntMap.Strict qualified as IntMap
import Data.List (nub, sortOn)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe, isJust, mapMaybe)
import Data.Serialize (Serialize, decode, encode)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (decodeUtf8)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word64)
import Database.Redis qualified as R
import Haskoin
  ( BlockHash,
    BlockHeader (..),
    Ctx,
    Network (..),
    OutPoint (..),
    Tx (..),
    TxHash,
    TxIn (..),
    blockHashToHex,
    headerHash,
    nullOutPoint,
    txHash,
  )
import Haskoin.Node (LogLevel (..))
import Haskoin.Store.Common (Limits (..), StoreRead (..))
import Haskoin.Store.Data
  ( Balance (..),
    BlockData (..),
    BlockRef (..),
    Spender (..),
    TxData (..),
    UnixTime,
    Unspent (..),
    XPubBal (..),
    XPubSpec,
    nullBalance,
  )
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered qualified as L
import Haskoin.Store.Database.Query.Pooled (PoolReads (..), pageStart, pooledRead)
import Haskoin.Store.Database.Reader (DatabaseReader (..), txCF)
import Haskoin.Store.Database.Types
  ( MemAddrKey (..),
    MemAddrVal (..),
    MemSummary (..),
    TxKey (..),
  )
import Haskoin.Store.Database.Views (Views, heldViews, keepViews, newViews, takeView, withViewAt)
import Haskoin.Store.Fleet qualified as F
import Haskoin.Store.Fleet.Redis
import Haskoin.Store.Fleet.XPub
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
import UnliftIO
  ( TVar,
    atomically,
    handle,
    handleAny,
    modifyIORef',
    modifyTVar',
    newIORef,
    newTVarIO,
    readIORef,
    readTVar,
    readTVarIO,
    retrySTM,
    stateTVar,
    timeout,
    writeIORef,
    writeTVar,
  )

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
    -- ("Haskoin.Store.Fleet.XPub"), the least recently used going first, or
    -- 0 not to use the cache
    cacheMax :: !Int,
    -- | cache a key that has at least this many used addresses
    cacheMinAddrs :: !Int,
    -- | or at least this many transactions in its addresses
    cacheMinTxs :: !Int,
    -- | StatsD client, to report what the cache of extended public keys
    -- does, as t'CacheMetrics' lists
    stats :: !(Maybe Stats),
    -- | where to say what a log should have: that the cache was emptied
    -- and why, as information, and each entry built and each move of the
    -- cache, for debugging
    note :: !(LogLevel -> Text -> IO ())
  }

-- | StatsD metrics of the fleet's cache of extended public keys, as this
-- store sees it. They have the names of those of "Haskoin.Store.Cache"
-- under @fleet@, so that a dashboard reads both alike.
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
    -- | entries that this store deleted for being the least recently used
    -- (@fleet.cache.evictions@)
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
    -- | the transactions of the store's mempool that the pool has
    offered :: !(TVar (HashSet TxHash)),
    -- | those that it refused, or that there was no time to offer, with
    -- the times at which they entered the mempool: they are offered again
    waiting :: !(TVar (HashMap TxHash UnixTime)),
    -- | how many times the mempool was offered, and at which tip last
    rounds :: !(TVar Int),
    offeredAt :: !(TVar (Maybe BlockHash)),
    -- | whether the best block changed since the mempool was last offered
    changed :: !(TVar Bool),
    -- | whether the store's own health checks pass. While they do not, the
    -- member does nothing, so that its lease runs out and a store that is
    -- stuck does not hold the fleet back.
    healthy :: !(TVar Bool),
    -- | the extended public keys that requests asked about and the cache
    -- did not have, by their names in Redis: 'memberCacheLoop' builds
    -- their entries
    wanted :: !(TVar (HashMap ByteString XPubSpec)),
    -- | the fleet tip at which the cache was found at another block, at
    -- the last turn
    lagging :: !(TVar (Maybe ByteString)),
    -- | the metrics of the cache, if the configuration has a StatsD client
    metrics :: !(Maybe CacheMetrics),
    -- | transactions that the store deleted from its mempool since it
    -- started, up to a hundred thousand. The database keeps nothing of
    -- them, and the rule for conflicts in the pool asks about them
    -- ('offerMempool').
    gone :: !(TVar (HashSet TxHash))
  }

-- | A member with no views yet, on a connection to the Redis of the fleet.
newMember :: MemberConfig -> R.Connection -> DatabaseReader -> IO Member
newMember config redis db = do
  views <- newViews db
  recent <- newTVarIO []
  reported <- newTVarIO []
  tip <- newTVarIO Nothing
  offered <- newTVarIO HashSet.empty
  waiting <- newTVarIO HashMap.empty
  rounds <- newTVarIO 0
  offeredAt <- newTVarIO Nothing
  changed <- newTVarIO True
  healthy <- newTVarIO True
  wanted <- newTVarIO HashMap.empty
  lagging <- newTVarIO Nothing
  metrics <- mapM newCacheMetrics config.stats
  gone <- newTVarIO HashSet.empty
  return Member {keys = fleetKeys config.prefix 32, ..}

-- | Take a view of the database for its best block. The block store calls
-- this right after it imports or reverts a block, and when it starts,
-- before it writes anything else: the view is then of that block and of
-- nothing more.
noteBest :: Member -> BlockHash -> IO ()
noteBest m block = do
  takeView m.views block
  atomically $ do
    modifyTVar' m.recent $ \blocks -> take m.config.depth (block : filter (/= block) blocks)
    writeTVar m.changed True
  prune m

-- | Note that transactions left the store's mempool unconfirmed. The store
-- calls this when it deletes them. The member forgets that it offered them:
-- one that comes back, as a transaction does that is sent again after it
-- was replaced, is then offered again, and takes the place of what replaced
-- it if this store's rules say so.
memberForget :: Member -> [TxHash] -> IO ()
memberForget m hs = atomically $ do
  modifyTVar' m.offered (\have -> foldr HashSet.delete have hs)
  modifyTVar' m.waiting (\held -> foldr HashMap.delete held hs)
  modifyTVar' m.gone $ \seen ->
    let more = foldr HashSet.insert seen hs
     in if HashSet.size more > 100000 then HashSet.fromList hs else more

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
-- agree on another block, offer what the pool does not have of the store's
-- mempool, and let go of old views. Call it every second or two, and when
-- the best block changes. A store whose own health checks fail must not
-- call it: its lease then runs out, and the fleet goes on without it.
memberTick :: Member -> IO ()
memberTick m = do
  report m
  moved <- moveTip m
  -- the tip is read after the report, as the rule of 'F.keep' asks
  tip <- fmap decoded <$> fleetTip m.redis m.keys
  atomically (writeTVar m.tip (fromMaybe Nothing tip))
  prune m
  syncCache m
  unless moved (expirePool m)
  offerMempool m
  where
    decoded = either (const Nothing) Just . decode

-- | Take a turn as a member every two seconds, and at once when an action
-- that waits for the next best block returns, for as long as the store's
-- health checks pass. A turn that fails, as when Redis is away, is told to
-- a function and tried again at the next.
memberLoop :: Member -> IO () -> (String -> IO ()) -> IO a
memberLoop m nextBlock complain = go
  where
    go = do
      void (timeout 2000000 nextBlock)
      well <- readTVarIO m.healthy
      when well (handleAny (complain . show) (memberTick m))
      go

-- | Hear what happens to the fleet, for as long as the store runs: each
-- move of the tip and each transaction that enters or leaves the pool is
-- told to a function, in the order in which Redis has them, from the
-- moment that this starts. Every store of the fleet hears the same, which
-- is what its clients are told ('FleetEvent'). When Redis cannot be read
-- a function is told why, and listening starts again from what is newest
-- then: the events in between are not told.
memberEvents :: Member -> (FleetEvent -> IO ()) -> (String -> IO ()) -> IO a
memberEvents m tell complain = start
  where
    start = do
      handleAny (complain . show) (fleetEventsEnd m.redis m.keys >>= listen)
      void (timeout 1000000 (atomically retrySTM) :: IO (Maybe ()))
      start
    listen from = do
      events <- fleetEventsAfter m.redis m.keys from 1000
      mapM_ (tell . snd) events
      listen (foldl (\_ (at, _) -> at) from events)

-- | Report the blocks of the main chain that the store has views of, newest
-- first.
report :: Member -> IO ()
report m = do
  held <- heldViews m.views
  recent <- readTVarIO m.recent
  blocks <- filterM main (filter (`elem` held) recent)
  taken <- fleetReport m.redis m.keys Nothing m.config.lease m.config.name (map encode blocks) (map encode held)
  atomically (writeTVar m.reported (if taken then blocks else []))
  where
    main h = maybe False (.main) <$> Q.getBlock m.db h

-- | Move the fleet tip to the block that the live stores agree on, if it is
-- another one, with what the blocks in between did to the pool. Says
-- whether it moved. When this store cannot say what they did, the pool is
-- emptied, which Redis allows only to a store that is alone.
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
      between <- case (traverse decode seen, decode target) of
        (Right from, Right to) -> blocksBetween m.db from to
        _ -> return Nothing
      let path = maybe [] (map encode . (.path)) between
      fleetAdvance m.redis m.keys 32 Nothing seen target ((.pool) <$> between) path >>= \case
        MovedTo _ -> advanceCache m seen target between >> return True
        _ -> return False
    _ -> return False
  where
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
changeBetween :: DatabaseReader -> Maybe BlockHash -> BlockHash -> IO (Maybe PoolChange)
changeBetween db from to = fmap (.pool) <$> blocksBetween db from to

-- | What the blocks between two tips did.
data Between = Between
  { -- | what they did to the pool
    pool :: !PoolChange,
    -- | the transactions of the blocks of the main chain from where it
    -- left the way to the first tip, up to the second, in their order
    added :: ![TxData],
    -- | whether blocks of the way to the first tip were abandoned
    reorganised :: !Bool,
    -- | those blocks of the main chain, oldest first, the second tip last:
    -- what the fleet tells its clients that the tip moved through
    path :: ![BlockHash]
  }

-- | 'changeBetween', with the transactions that the new blocks confirm,
-- which is what the cache of extended public keys is brought to the new
-- tip with ('advanceCache').
blocksBetween :: DatabaseReader -> Maybe BlockHash -> BlockHash -> IO (Maybe Between)
blocksBetween db from to = L.withView db $ \view -> do
  let r = L.viewReader view
      -- the blocks from one that is not on the main chain back to it
      back :: Int -> BlockHash -> IO (Maybe ([BlockData], BlockData))
      back n h
        | n > farthest = return Nothing
        | otherwise =
            Q.getBlock r h >>= \case
              Nothing -> return Nothing
              Just bd
                | bd.main -> return (Just ([], bd))
                | otherwise -> fmap (\(bs, fork) -> (bd : bs, fork)) <$> back (n + 1) bd.header.prev
      -- the blocks of the main chain from a block down to above a height
      down :: Int -> BlockData -> BlockData -> IO (Maybe [BlockData])
      down n fork bd
        | n > farthest = return Nothing
        | bd.height <= fork.height = return (if bd.header == fork.header then Just [] else Nothing)
        | otherwise =
            Q.getBlock r bd.header.prev >>= \case
              Nothing -> return Nothing
              Just parent -> fmap (bd :) <$> down (n + 1) fork parent
  target <- Q.getBlock r to
  left <- case from of
    Just h -> back 0 h
    -- without a tip there was no pool, and nothing for the blocks to do
    Nothing -> return (fmap ([],) target)
  case (target, left) of
    (Just bd, Just (abandoned, fork))
      | bd.main ->
          down 0 fork bd >>= \case
            Nothing -> return Nothing
            Just added -> do
              old <- concat <$> mapM (bodies view) (reverse abandoned)
              new <- concat <$> mapM (confirmedBy r) (reverse added)
              let pool =
                    PoolChange
                      { returned = [poolTx r.net r.ctx td | (_, Just td) <- old, not (coinbase td)],
                        gone = [encode h | (h, found) <- old, maybe True coinbase found],
                        confirmed = map (encode . txHash . (.tx)) new,
                        spent = [encode i.outpoint | td <- new, i <- td.tx.inputs, i.outpoint /= nullOutPoint]
                      }
              let path = map (headerHash . (.header)) (reverse added)
              return (Just Between {pool, added = new, reorganised = not (null abandoned), path})
    _ -> return Nothing
  where
    -- the transactions of an abandoned block, each with what the store has
    -- of it now, if it still has it and did not delete it
    bodies view bd = do
      found <- L.getManyTxData view bd.txs
      return [(h, found' >>= live) | (h, found') <- zip bd.txs found]
    live td = if td.deleted then Nothing else Just td
    confirmedBy :: DatabaseReader -> BlockData -> IO [TxData]
    confirmedBy r bd = catMaybes <$> Q.retrieveManyCF r.db (txCF r.db) (map TxKey bd.txs)
    coinbase td = any ((== nullOutPoint) . (.outpoint)) td.tx.inputs

-- | A transaction as the pool keeps it: its body without spenders, and
-- what it does at each address, as "Haskoin.Store.Database.Mempool" works
-- them out for the store's own mempool area.
poolTx :: Network -> Ctx -> TxData -> PoolTx
poolTx net ctx td =
  PoolTx
    { txid = encode records.txid,
      time,
      inputs = [encode op | (op, _) <- records.spends],
      body = encode records.body,
      signals = net.replaceByFee && any ((< 0xffffffff - 1) . (.sequence)) td.tx.inputs,
      entries = mapMaybe entry records.addresses
    }
  where
    time = case td.block of
      MemRef t -> t
      BlockRef {} -> 0
    records = M.mempoolRecords net ctx time td
    entry = \case
      (MemAddrTxKey a _ _, _) -> Just (PoolTouches (encode a))
      (MemAddrSpentKey a p, MemAddrSpentVal v) -> Just (PoolSpends (encode a) (encode p) v)
      (MemAddrOutKey a _ p, MemAddrOutVal v s) -> Just (PoolPays (encode a) (encode p) v s)
      _ -> Nothing

-- | Offer to the pool the transactions of the store's mempool that it does
-- not have, oldest first, so that each comes after those that it spends
-- from. A store without a view of the fleet tip offers nothing: it cannot
-- say what is unspent there.
--
-- A transaction that the pool refuses is offered again at the next round:
-- its parents may not be there yet, or the tip may have moved. One that
-- conflicts with transactions of the pool takes their place when this
-- store deleted every one of them by its own rules, as it does when it
-- replaces a transaction, and otherwise stays out: the first of two that
-- spend the same output stays.
--
-- = Reading no more of the mempool than is new
--
-- A mempool has tens of thousands of transactions and a round comes every
-- two seconds, so a round does not read the whole list. It reads the list
-- from the newest, a page at a time, until a page has nothing that it does
-- not know, and offers what it found with what is waiting from earlier
-- rounds. The whole list is read when the tip has moved or the store's best
-- block has changed, since a block or a reorganisation changes the mempool
-- anywhere, and every 'wholeEvery' rounds, for whatever the short way
-- missed. What the store deletes in between it tells the member
-- ('memberForget'). A round offers 'mostOffers'
-- transactions at most and leaves the rest waiting, so that a store with a
-- large mempool to offer still reports in time to keep its lease.
offerMempool :: Member -> IO ()
offerMempool m =
  readTVarIO m.tip >>= \case
    Nothing -> return ()
    Just tip -> void . withViewAt m.views tip $ \at -> do
      before <- readTVarIO m.offeredAt
      (n, dirty) <- atomically $ do
        writeTVar m.offeredAt (Just tip)
        (,) <$> stateTVar m.rounds (\k -> (k, k + 1)) <*> stateTVar m.changed (,False)
      todo <- if dirty || before /= Just tip || n `mod` wholeEvery == 0 then everything else newest
      let (now, later) = splitAt mostOffers (sortOn fst todo)
      atomically (modifyTVar' m.waiting (HashMap.union (HashMap.fromList [(h, time) | (time, h) <- later])))
      go at tip now
  where
    -- all of the mempool that the pool does not have, forgetting what has
    -- left the mempool
    everything = do
      mempool <- Q.getMempool m.db
      have <- readTVarIO m.offered
      let kept = HashSet.intersection have (HashSet.fromList (map snd mempool))
      atomically (writeTVar m.offered kept >> writeTVar m.waiting HashMap.empty)
      return [(time, h) | (time, h) <- mempool, not (HashSet.member h kept)]
    -- the newest of the mempool that this member has not seen, and what is
    -- waiting
    newest = do
      have <- readTVarIO m.offered
      held <- readTVarIO m.waiting
      let known h = HashSet.member h have || HashMap.member h held
          scan offset = do
            page <- Q.getMempoolPage m.db Limits {limit = pageSize, offset, start = Nothing}
            let fresh = filter (not . known . snd) page
            if null fresh || length page < fromIntegral pageSize
              then return fresh
              else (fresh <>) <$> scan (offset + pageSize)
      found <- scan 0
      return (found <> [(time, h) | (h, time) <- HashMap.toList held])
    pageSize = 256
    go _ _ [] = return ()
    go at tip ((time, h) : rest) =
      M.getMempoolTx m.db h >>= \case
        Nothing -> forget h >> go at tip rest
        Just td -> do
          let tx = poolTx m.db.net m.db.ctx td
          unspent <- forM [i.outpoint | i <- td.tx.inputs, i.outpoint /= nullOutPoint] $ \p ->
            isJust <$> Q.getUnspent at p
          let offer = poolOffer m.redis m.keys 32 (Just (encode tip)) tx unspent
              done = atomically $ do
                modifyTVar' m.offered (HashSet.insert h)
                modifyTVar' m.waiting (HashMap.delete h)
              wait = atomically (modifyTVar' m.waiting (HashMap.insert h time))
          offer [] >>= \case
            Taken -> done >> go at tip rest
            Known -> done >> go at tip rest
            Orphan _ -> wait >> go at tip rest
            -- the tip moved: the next round reads the whole mempool
            Late -> return ()
            Conflict rivals -> do
              mine <- mapM deletedHere rivals
              if and mine
                then
                  offer rivals >>= \case
                    Taken -> done
                    _ -> wait
                else wait
              go at tip rest
    forget h = atomically (modifyTVar' m.waiting (HashMap.delete h))
    deletedHere rival = case decode rival of
      Left _ -> return False
      -- it deleted it, and has not taken it back since. A store that was
      -- started again does not remember, and leaves the conflict as it
      -- is: the first of the two stays in the pool.
      Right h -> do
        deleted <- HashSet.member h <$> readTVarIO m.gone
        back <- isJust <$> M.getMempoolTx m.db h
        return (deleted && not back)

-- | How many rounds of offers go by between two that read the whole
-- mempool: five minutes of them.
wholeEvery :: Int
wholeEvery = 150

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
-- and throws the same. An answer that passed either is one that every
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
    let confirm =
          readIORef checked >>= \case
            True -> return ()
            False -> do
              now <- memberTip m
              if now == Just tip then writeIORef checked True else throwIO TipMoved
        found = writeIORef checked True
    action r (redisPool m.redis m.keys tip found) found confirm

-- | 'answeringAt' with the queries ready: those of
-- "Haskoin.Store.Database.Query.Pooled" for the view and the pool, and
-- the same with the cache of extended public keys in front
-- ('cachedRead'), which is what requests are answered with unless they
-- ask not to be.
answeringCached :: Member -> ByteString -> (StoreRead -> StoreRead -> IO () -> IO a) -> IO (Maybe a)
answeringCached m tip action = answeringWith m tip $ \r pool found confirm -> do
  cached <- cachedRead m tip r pool found
  action (pooledRead r pool) cached confirm

-------------------------------------------------------------------------------
-- The cache of extended public keys
-------------------------------------------------------------------------------

-- | The queries of a request with the fleet's cache of extended public
-- keys in front of those about them: given the tip that the request
-- answers from, the view at that block, the pool, and what to run after a
-- read that found the tip where it should be.
--
-- The balances of a key come from the cache if it has the key with the
-- addresses that it has now. The other three queries about a key are given
-- its balances by the caller, and are answered from the cache when this
-- same request got them there: the cache is then asked for an entry with
-- as many addresses, so that an entry that changed in between is not
-- used. Whatever the cache does not answer the database does, and a key
-- that it did not have is put down to be built ('wantCached'). A read that
-- finds the tip moved throws v'TipMoved', like those of the pool.
cachedRead :: Member -> ByteString -> DatabaseReader -> PoolReads -> IO () -> IO StoreRead
cachedRead m tip r pool found
  | m.config.cacheMax <= 0 = return base
  | otherwise = do
      seen <- newIORef HashMap.empty
      let hit :: Cached a -> IO (Maybe a)
          hit = \case
            Hit x -> Just x <$ found
            Miss -> return Nothing
            Moved -> throwIO TipMoved
          entryOf x = HashMap.lookup (xpubId x) <$> readIORef seen
          -- from the cache if this request has the entry of the key, and
          -- from where the page starts if it has a start
          paged :: XPubSpec -> Limits -> IO [a] -> (Entry -> L.From -> IO (Cached [a])) -> IO [a]
          paged x limits direct cached =
            entryOf x >>= \case
              Nothing -> direct
              Just e ->
                pageStart r pool limits.start >>= \case
                  Nothing -> return []
                  Just from -> cached e from >>= hit >>= maybe direct return
      return
        base
          { xPubBals = \x -> do
              now <- milliseconds
              cachedEntry m.redis m.keys tip r.initGap r.maxGap now x >>= hit >>= \case
                Just e -> do
                  tally m (.hits) 1
                  e.balances <$ modifyIORef' seen (HashMap.insert (xpubId x) e)
                Nothing -> do
                  tally m (.misses) 1
                  bals <- base.xPubBals x
                  wantCached m x bals
                  return bals,
            xPubTxs = \x bals limits ->
              paged x limits (base.xPubTxs x bals limits) $ \e from ->
                cachedTxs m.redis m.keys tip x e.sizes from limits,
            xPubUnspents = \x bals limits ->
              paged x limits (base.xPubUnspents x bals limits) $ \e from ->
                cachedUnspents m.redis m.keys tip x e from limits,
            xPubTxCount = \x bals ->
              entryOf x >>= \case
                Nothing -> base.xPubTxCount x bals
                Just e ->
                  cachedCount m.redis m.keys tip x e.sizes >>= hit >>= maybe (base.xPubTxCount x bals) return
          }
  where
    base = pooledRead r pool

-- | The time in milliseconds, by which the cache orders the keys by use.
milliseconds :: IO Word64
milliseconds = round . (* 1000) <$> getPOSIXTime

-- | Put an extended public key down to be cached, given its balances, if
-- it is worth it: a key with few addresses and few transactions is read
-- from the database as fast as from Redis. The thresholds are those of the
-- configuration, and count as "Haskoin.Store.Cache" counts them.
wantCached :: Member -> XPubSpec -> [XPubBal] -> IO ()
wantCached m x bals =
  when (m.config.cacheMax > 0 && (length used >= m.config.cacheMinAddrs || txs >= m.config.cacheMinTxs)) $
    atomically . modifyTVar' m.wanted $ \w ->
      if HashMap.size w < 1000 then HashMap.insert (xpubId x) x w else w
  where
    used = filter (not . nullBalance . (.balance)) bals
    txs = fromIntegral (sum (map (.balance.txs) used)) :: Int

-- | Build the entries of the keys that requests asked about and the cache
-- did not have, one at a time, for as long as the store runs. A build that
-- fails is told to a function. The least recently used entries are deleted
-- when there are more than the configuration allows.
--
-- This is apart from the turns of the member because an entry of a wallet
-- with a long history takes seconds to read from the database, and a
-- member that does not report for that long loses its lease.
memberCacheLoop :: Member -> (String -> IO ()) -> IO a
memberCacheLoop m complain = go
  where
    go = do
      spec <- atomically $ do
        w <- readTVar m.wanted
        case HashMap.toList w of
          [] -> retrySTM
          (k, x) : _ -> x <$ writeTVar m.wanted (HashMap.delete k w)
      handleAny (complain . show) (build spec)
      go
    build spec =
      readTVarIO m.tip >>= \case
        Nothing -> return ()
        Just block -> do
          now <- milliseconds
          let tip = encode block
              name = decodeUtf8 (xpubId spec)
          built <-
            handle (\TipMoved -> return (Just Abandoned)) $
              answeringAt m tip (\r pool _ -> cacheBuild m.redis m.keys tip r pool now spec)
          took <- subtract now <$> milliseconds
          case built of
            Just (Built n) | n > 0 -> do
              tally m (.builds) 1
              forM_ m.metrics (\x -> addTiming x.buildTime (fromIntegral took))
              m.config.note LevelDebug $
                "Cached xpub " <> name <> ": " <> shown n <> " addresses added in " <> shown took <> " ms"
              gone <- cacheEvict m.redis m.keys m.config.cacheMax
              when (gone > 0) $ do
                tally m (.evictions) gone
                m.config.note LevelDebug ("Evicted " <> shown gone <> " xpubs from the cache")
            Just Abandoned -> do
              tally m (.abandoned) 1
              m.config.note LevelDebug ("Gave up caching xpub " <> name <> ": the fleet or the entry moved on")
            _ -> return ()

-- | Bring the cache of extended public keys to the block that this member
-- just moved the fleet tip to, with what the blocks in between did at the
-- addresses that its entries have. The cache is emptied instead when the
-- member cannot say what they did, when the chain was reorganised, which
-- is rare and would need what the abandoned blocks did undone, or when
-- the member has no view of the block.
advanceCache :: Member -> Maybe ByteString -> ByteString -> Maybe Between -> IO ()
advanceCache m seen target between = when (m.config.cacheMax > 0) $ do
  changes <- case (between, decode target) of
    (Just b, Right to) | not b.reorganised -> withViewAt m.views to $ \r -> do
      let touched = HashMap.toList (blockChanges r.net r.ctx b.added)
      flags <- cacheTracked m.redis m.keys (map fst touched)
      forM [x | (x, True) <- zip touched flags] $ \(a, change) -> do
        bal <- Q.getBalance r a
        return (a, bal, change)
    _ -> return Nothing
  let why = case (between, changes) of
        (Nothing, _) -> "this store cannot say what the blocks since the last tip did"
        (Just b, _) | b.reorganised -> "the chain was reorganised"
        (_, Nothing) -> "this store has no view of the block"
        _ -> "it was at another block than the last tip"
  cacheAdvance m.redis m.keys seen target changes >>= \case
    CacheEmptied -> wasEmptied m target why
    CacheMoved ->
      m.config.note LevelDebug $
        "The xpub cache is at block "
          <> blockName target
          <> ": "
          <> shown (maybe 0 length changes)
          <> " addresses changed"
    _ -> return ()

-- | Count and say that this store emptied the cache, at a block and for a
-- reason. It is information and not a warning: nothing is wrong with what
-- the fleet answers, which comes from the database until entries are
-- built again. Emptied at every block, though, the cache is of no use,
-- and the log is where that shows.
wasEmptied :: Member -> ByteString -> Text -> IO ()
wasEmptied m block why = do
  tally m (.emptied) 1
  m.config.note LevelInfo ("Emptied the xpub cache at block " <> blockName block <> ": " <> why)

-- | A block as a log names it.
blockName :: ByteString -> Text
blockName bytes = either (const "?") blockHashToHex (decode bytes :: Either String BlockHash)

shown :: (Show a) => a -> Text
shown = T.pack . show

-- | See that the cache of extended public keys does not stay at another
-- block than the fleet tip. It is there for a moment after every move of
-- the tip, until the store that moved it has brought the cache along, and
-- for good if that store stopped before it did. So a member that finds
-- the cache behind the same tip at two turns in a row empties it, and it
-- starts again at the tip. Every turn of a member runs this.
syncCache :: Member -> IO ()
syncCache m = when (m.config.cacheMax > 0) $ do
  -- how large the cache is, at every turn: it is one small read, and the
  -- number is the fleet's, the same from every store
  forM_ m.metrics $ \x -> do
    (entries, held) <- cacheSize m.redis m.keys
    setGauge x.xpubs entries
    setGauge x.addresses held
  tip <- fmap encode <$> readTVarIO m.tip
  at <- cacheTip m.redis m.keys
  before <- readTVarIO m.lagging
  case tip of
    Just t | at /= Just t -> do
      when (before == Just t) $
        cacheAdvance m.redis m.keys Nothing t Nothing >>= \case
          CacheEmptied -> wasEmptied m t "no store brought it to the fleet tip"
          _ -> return ()
      atomically (writeTVar m.lagging (Just t))
    _ -> atomically (writeTVar m.lagging Nothing)

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
      startingWith = \start -> rights . map decode <$> ask "ids" [B.drop 1 (encode (TxKeyS start))]
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
