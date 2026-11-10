{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -Wno-ambiguous-fields #-}

-- |
-- Module      : Haskoin.Store.Cache
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Redis cache of extended public keys (xpubs), shared by several stores.
--
-- An xpub with many used addresses is slow to answer from the database: every
-- address has to be derived and read. Wallets of exchanges and payment
-- processors are like that, and are asked about all the time. The cache keeps
-- everything that the web server needs to answer about such an xpub in Redis:
-- the balances of its addresses up to the gap, its transactions, its unspent
-- outputs, and the bodies of its transactions with who spends their outputs.
--
-- = One source of truth
--
-- The data of a cached xpub always comes from the database, read with the
-- same functions that answer uncached queries
-- ("Haskoin.Store.Database.Query"), and is never computed from what the
-- cache guesses a change did, as the cache that this one replaced did, and
-- drifted. Whenever a block or a mempool transaction touches one of its
-- addresses, or a reorganisation changes one of its transactions, the store
-- that saw it reads from the database what that change affects, the
-- transactions and the outputs they create and spend, and writes the
-- differences in one atomic step ('cacheRefresh'). It reads the whole xpub
-- instead when it caches it, and whenever it cannot be sure that the entry
-- has every earlier change: checks against the balances of every address of
-- the xpub, and a full reload every ten minutes, keep a missed change from
-- staying. Reloads of one xpub are coalesced, so a burst of payments costs at
-- most one reload in progress and one waiting.
--
-- Everything a response needs is in Redis, including the bodies of the
-- transactions: a store that has not yet seen a transaction of a cached xpub
-- still serves it in full. Bodies are kept once per transaction and shared by
-- the xpubs that refer to them; the last xpub to let go of one deletes it.
-- Bodies are stored without the spenders of their outputs, which change
-- without touching the xpub (an output paying someone else is spent by a
-- transaction that has nothing to do with the xpub). The spenders are in a
-- separate hash per transaction, kept up to date by every store as it imports
-- transactions, and filled in when a body is read.
--
-- = Several stores, one Redis
--
-- Stores index the same chain at slightly different times, and see mempool
-- transactions in different orders. Three rules keep them from getting in
-- each other's way:
--
-- * __Each change is applied once.__ A store claims each change it sees, per
--   cached xpub that it touches (@ev:\<event\>:\<id\>@). The first store to
--   claim it reloads the xpub and marks the claim done; the others skip it. A
--   store that is catching up finds its events done and does no work, and a
--   store whose database does not have the best header ignores events
--   altogether.
--
-- * __No store writes what knows less than Redis.__ A store does not replace
--   an entry stamped with a higher block than its own best block, nor one
--   with a transaction that its database has never seen; it gives the change
--   up for a store that knows more. So stores never undo each other's work.
--
-- * __No entry refers to what Redis does not have.__ An entry is written in
--   one Lua script together with the bodies it needs, and the script writes
--   nothing if a body would be missing. A store that is behind can serve a
--   cached xpub in full, transactions it has not imported included. Bodies
--   that Redis already has are rewritten just before, in small scripts, so
--   that a large block does not hold Redis for long; a body may so be a
--   moment newer than an entry that has it, never older.
--
-- The first two rules are the pure decisions of
-- "Haskoin.Store.Cache.Protocol", where a model test checks them with several
-- stores catching up in random interleavings.
--
-- A lock per xpub keeps two stores from reloading it at the same time, and
-- every change of several keys of an entry is a Lua script, so readers never
-- see half of one. Stores that answer from Redis serve the same data; stores whose
-- databases disagree can still give different answers for xpubs that are not
-- cached.
--
-- = Keys
--
-- Every key starts with @hs2:\<network\>:@, so this cache can share a Redis with
-- the cache of older versions of the store, whose keys have no such prefix,
-- and with stores of other networks.
--
-- * @x:\<id\>:meta@: hash with the serialized xpub (@spec@), the height of
--   the best block it was read at (@height@), the format version (@ver@,
--   now 3), the generation of the entry (@gen@), which every reload
--   increments, and the time of its last full reload in milliseconds
--   (@full@).
--   @\<id\>@ is the hexadecimal of the first 16 bytes of the SHA-256 of the
--   serialized xpub.
-- * @x:\<id\>:bal@: balances of the addresses up to the gap, serialized.
-- * @x:\<id\>:txs@: sorted set of transaction hashes, scored by 'blockRefScore'.
-- * @x:\<id\>:utxo@: sorted set of serialized unspent outputs, scored the same.
-- * @x:\<id\>:uop@: hash of serialized outpoint to its member of @utxo@, so
--   that an output can be removed by its outpoint alone, though its member
--   changes with its block. Entries of version 2 have none; their next full
--   reload builds it.
-- * @x:\<id\>:addrs@: set of the addresses of the balances.
-- * @a:\<address\>@: set of the ids of the cached xpubs with this address.
-- * @tx:\<txid\>@: transaction body, serialized 'TxData' without spenders.
-- * @sp:\<txid\>@: hash of output index to spender.
-- * @txref:\<txid\>@: set of the ids of the cached xpubs with this transaction.
-- * @lru@: sorted set of ids by time of last use, for eviction.
-- * @lock:\<id\>@: lock held while an xpub is reloaded.
-- * @ev:\<event\>:\<id\>@: claim of a change of a cached xpub: @busy@ while a
--   store applies it, for two minutes at most, then @done@ for an hour.
module Haskoin.Store.Cache
  ( -- * Running the cache
    Cache,
    CacheConfig (..),
    withCache,
    cacheAlive,

    -- * Reading
    cacheXPubBals,
    cacheXPubTxs,
    cacheXPubTxCount,
    cacheXPubUnspents,
    cacheTxData,
    cacheIsCached,
    CacheCounters (..),
    cacheCounters,

    -- * Changing
    cacheRequest,
    Reload (..),
    cacheRefresh,
    cacheDelete,
    cacheEvict,

    -- * Internals

    -- | Exported for tests.
    cachePrefix,
    xpubId,
    blockRefScore,
    scoreBlockRef,
    fullReloadEvery,
    fullReloadAfter,
  )
where

import Control.Monad (forM, forM_, join, unless, void, when)
import Data.Bits (shift, (.&.), (.|.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as C8
import Data.Either (rights)
import Data.FileEmbed (embedFile, makeRelativeToProject)
import Data.Functor ((<&>))
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.IntMap.Strict qualified as IntMap
import Data.List (nub)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Maybe (catMaybes, fromMaybe, isJust, isNothing, maybeToList)
import Data.Serialize (decode, encode)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word32, Word64)
import Database.Redis (Connection, Redis, Reply)
import Database.Redis qualified as R
import Haskoin
  ( Address,
    BlockHash,
    BlockHeader (..),
    BlockNode (..),
    Network (..),
    OutPoint (..),
    Tx (..),
    TxHash,
    TxIn (..),
    TxOut (..),
    encodeHex,
    headerHash,
    scriptToAddressBS,
    sha256,
    txHash,
  )
import Haskoin.Node (Chain, LogLevel (..), Logger, chainGetBest, logAt)
import Haskoin.Store.Cache.Protocol
  ( Claim (..),
    OnClaim (..),
    OnReload (..),
    Reload (..),
    afterClaim,
    afterReload,
    mayReplace,
  )
import Haskoin.Store.Common (Limits (..), Start (..), StoreEvent (..), loopForever)
import Haskoin.Store.Data
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered qualified as L
import Haskoin.Store.Database.Reader (DatabaseReader (..))
import NQE (Inbox, Publisher, receive, withSubscription)
import System.Metrics.StatsD
  ( StatCounter,
    StatTiming,
    Stats,
    addTiming,
    incrementCounter,
    newStatCounter,
    newStatTiming,
  )
import System.Random (randomIO)
import Text.Read (readMaybe)
import UnliftIO
  ( IORef,
    TVar,
    atomicModifyIORef',
    atomically,
    checkSTM,
    handleAny,
    link,
    modifyTVar',
    newIORef,
    newTVarIO,
    pooledForConcurrentlyN_,
    readIORef,
    readTVar,
    timeout,
    tryAny,
    withAsync,
    writeIORef,
    writeTVar,
  )
import UnliftIO.Concurrent (forkIO, threadDelay)

-- | Settings of the cache.
data CacheConfig = CacheConfig
  { -- | Redis URL, such as @redis:\/\/localhost:6379@; empty for the default
    url :: !String,
    -- | cache xpubs with at least this many used addresses
    minAddrs :: !Int,
    -- | or with at least this many transactions in their addresses
    minTxs :: !Int,
    -- | evict the least recently used xpubs past this many
    maxXPubs :: !Int,
    -- | StatsD client, to report the metrics of the cache
    stats :: !(Maybe Stats)
  }

-- | StatsD metrics of the cache.
data CacheMetrics = CacheMetrics
  { -- | xpubs answered from the cache (@cache.hits@)
    hits :: !StatCounter,
    -- | xpubs asked about that were not cached (@cache.misses@)
    misses :: !StatCounter,
    -- | xpubs read again from the database and written (@cache.reloads@)
    reloads :: !StatCounter,
    -- | reloads refused because the cache had newer data (@cache.stale@)
    stale :: !StatCounter,
    -- | xpubs evicted (@cache.evictions@)
    evictions :: !StatCounter,
    -- | reloads that read the whole xpub rather than what changed
    -- (@cache.full_reloads@)
    fullReloads :: !StatCounter,
    -- | milliseconds to reload an xpub (@cache.reload@)
    reloadTime :: !StatTiming
  }

-- | Register the metrics of the cache.
newCacheMetrics :: Stats -> IO CacheMetrics
newCacheMetrics s = do
  hits <- c "hits"
  misses <- c "misses"
  reloads <- c "reloads"
  stale <- c "stale"
  evictions <- c "evictions"
  fullReloads <- c "full_reloads"
  reloadTime <- newStatTiming s "cache.reload" 10
  return CacheMetrics {..}
  where
    c x = newStatCounter s ("cache." <> x) 10

-- | Read the database through a view of it as it is now: the mempool is
-- kept apart from the chain and laid over it when read, and the two have to
-- be read as they were at one moment
-- ("Haskoin.Store.Database.Query.Layered"). Transactions that left the
-- mempool unconfirmed are among what these reads give, marked as deleted:
-- the cache has to hear of them to take them out.
viewed :: Cache -> (L.View -> IO a) -> IO a
viewed c = L.withView c.db

-- | A running cache.
data Cache = Cache
  { redis :: !Connection,
    prefix :: !ByteString,
    db :: !DatabaseReader,
    chain :: !Chain,
    minAddrs :: !Int,
    minTxs :: !Int,
    maxXPubs :: !Int,
    logger :: !Logger,
    metrics :: !(Maybe CacheMetrics),
    -- | what this store did to the cache since it started
    counters :: !(IORef CacheCounters),
    -- | work for the worker, newest first
    jobs :: !(TVar [Job]),
    -- | when this store last marked each xpub as used, by id, in
    -- milliseconds; see 'touchEvery'
    touched :: !(IORef (HashMap ByteString Integer)),
    -- | transactions that this store deleted since it started, as far as
    -- the cache heard of them, up to a hundred thousand. The database
    -- keeps nothing of a deleted transaction, so without this one that
    -- Redis has and the database does not could as well be one that this
    -- store has not seen yet, which an entry must not lose.
    lost :: !(IORef (HashSet TxHash))
  }

-- | What a store did to the cache since it started.
data CacheCounters = CacheCounters
  { -- | xpubs read again from the database and written
    reloads :: !Int,
    -- | reloads given up because Redis knew more
    behind :: !Int,
    -- | reloads that read the whole xpub rather than what changed
    fullReloads :: !Int
  }
  deriving (Eq, Show)

-- | What this store did to the cache since it started.
cacheCounters :: Cache -> IO CacheCounters
cacheCounters c = readIORef c.counters

-- | Work for the worker of the cache.
data Job
  = -- | an event of the store, which may change cached xpubs
    JobEvent !StoreEvent
  | -- | cache an xpub that was asked about
    JobCache !XPubSpec
  | -- | try again to claim changes of an xpub that another store was busy
    -- with: the transactions that changed it, and the claims
    JobClaim !XPubSpec !(HashSet TxHash) ![ByteString]
  | -- | reload an xpub for changes whose claims this store holds
    JobHeld !XPubSpec !(HashSet TxHash) ![ByteString]

-- | Connect to Redis and run the process that keeps the cache up to date with
-- the events of the store, while an action runs.
withCache ::
  CacheConfig ->
  DatabaseReader ->
  Chain ->
  Publisher StoreEvent ->
  Logger ->
  (Cache -> IO a) ->
  IO a
withCache cfg db chain pub logger action = do
  info <-
    if null cfg.url
      then return R.defaultConnectInfo
      else either fail return (R.parseConnectInfo cfg.url)
  conn <- R.checkedConnect info
  jobs <- newTVarIO []
  counters <- newIORef (CacheCounters 0 0 0)
  touched <- newIORef HashMap.empty
  lost <- newIORef HashSet.empty
  metrics <- mapM newCacheMetrics cfg.stats
  let cache =
        Cache
          { redis = conn,
            prefix = cachePrefix db.net,
            db = db,
            chain = chain,
            minAddrs = cfg.minAddrs,
            minTxs = cfg.minTxs,
            maxXPubs = cfg.maxXPubs,
            logger = logger,
            metrics = metrics,
            counters = counters,
            jobs = jobs,
            touched = touched,
            lost = lost
          }
  withSubscription pub $ \events ->
    withAsync (listener cache events) $ \a1 ->
      withAsync (worker cache) $ \a2 -> do
        link a1
        link a2
        action cache

-- | Prefix of every key of the cache for a network.
cachePrefix :: Network -> ByteString
cachePrefix net = "hs2:" <> C8.pack net.name <> ":"

-- | Identifier of an xpub in the keys.
xpubId :: XPubSpec -> ByteString
xpubId = cs . encodeHex . BS.take 16 . encode . sha256 . encode

xkey :: Cache -> XPubSpec -> ByteString -> ByteString
xkey c x suffix = c.prefix <> "x:" <> xpubId x <> ":" <> suffix

txKey :: Cache -> TxHash -> ByteString
txKey c h = c.prefix <> "tx:" <> encode h

spKey :: Cache -> TxHash -> ByteString
spKey c h = c.prefix <> "sp:" <> encode h

addrKey :: Cache -> Address -> ByteString
addrKey c a = c.prefix <> "a:" <> encode a

lruKey :: Cache -> ByteString
lruKey c = c.prefix <> "lru"

-- | Count something in the metrics, if they are enabled.
count :: Cache -> (CacheMetrics -> StatCounter) -> Int -> IO ()
count c f n = mapM_ (\m -> incrementCounter (f m) n) c.metrics

log' :: Cache -> LogLevel -> Text -> IO ()
log' c l = logAt l c.logger

-- | Whether Redis answers a ping within two seconds. The health check of
-- the web server asks, so that a store whose Redis is gone says that it is
-- answering from its own database alone.
cacheAlive :: Cache -> IO Bool
cacheAlive c =
  handleAny (\_ -> return False) $
    timeout 2000000 (R.runRedis c.redis R.ping) <&> \case
      Just (Right _) -> True
      _ -> False

-- | Run Redis commands, throwing if Redis answers with an error.
run :: Cache -> Redis (Either Reply a) -> IO a
run c f =
  R.runRedis c.redis f >>= \case
    Right x -> return x
    Left e -> fail ("Redis error: " <> show e)

-------------------------------------------------------------------------------
-- Scores
-------------------------------------------------------------------------------

-- | Score of a position in a Redis sorted set, such that sorting by score
-- ascending sorts from newest to oldest. Mempool transactions get negative
-- scores from their time, so they come first; confirmed ones get positive
-- scores that decrease with height and position. Height and position are
-- packed into the 53 bits that a double holds exactly.
--
-- Between equal scores Redis orders members by their bytes, which for
-- transaction hashes and unspent outputs is the order that the database uses
-- ('Haskoin.Store.Common.txRefKey', 'Haskoin.Store.Common.unspentKey'), so a
-- cached page is the page the database gives.
blockRefScore :: BlockRef -> Double
blockRefScore BlockRef {height = h, position = p} =
  fromIntegral (0x001fffffffffffff - (h' .|. p'))
  where
    h' = (fromIntegral h .&. 0x07ffffff) `shift` 26 :: Word64
    p' = (fromIntegral p .&. 0x03ffffff) :: Word64
blockRefScore MemRef {timestamp = t} = negate t'
  where
    t' = fromIntegral (t .&. 0x001fffffffffffff)

-- | Inverse of 'blockRefScore'.
scoreBlockRef :: Double -> BlockRef
scoreBlockRef s
  | s < 0 = MemRef {timestamp = n}
  | otherwise = BlockRef {height = h, position = p}
  where
    n = truncate (abs s) :: Word64
    m = 0x001fffffffffffff - n
    h = fromIntegral (m `shift` (-26))
    p = fromIntegral (m .&. 0x03ffffff)

-------------------------------------------------------------------------------
-- Reading
-------------------------------------------------------------------------------

-- | Balances of a cached xpub, or 'Nothing' if it is not cached. Marks the
-- xpub as used. An entry that cannot be decoded is deleted and counts as not
-- cached.
cacheXPubBals :: Cache -> XPubSpec -> IO (Maybe [XPubBal])
cacheXPubBals c x = do
  touch <- touchArgs c x
  r <- run c $ do
    b <- R.get (xkey c x "bal")
    -- marking an xpub that is not cached is harmless: eviction removes it
    forM_ touch $ \now -> R.zadd (lruKey c) [(fromIntegral now, xpubId x)]
    return b
  case r of
    Nothing -> count c (.misses) 1 >> return Nothing
    Just bs -> case decode bs of
      Right bals -> count c (.hits) 1 >> return (Just bals)
      Left _ -> do
        count c (.misses) 1
        log' c LevelWarn "Cached xpub balances cannot be decoded; deleting them"
        void (cacheDelete c x)
        return Nothing

-- | Whether an xpub is cached.
cacheIsCached :: Cache -> XPubSpec -> IO Bool
cacheIsCached c x = run c (R.exists (xkey c x "meta"))

-- | Number of transactions of a cached xpub.
cacheXPubTxCount :: Cache -> XPubSpec -> IO (Maybe Word32)
cacheXPubTxCount c x = do
  touch <- touchArgs c x
  -- sent together, in one round trip
  (cached, n) <- run c $ do
    e <- R.exists (xkey c x "meta")
    z <- R.zcard (xkey c x "txs")
    forM_ touch $ \now -> R.zadd (lruKey c) [(fromIntegral now, xpubId x)]
    return ((,) <$> e <*> z)
  return (if cached then Just (fromIntegral n) else Nothing)

-- | Page of the transactions of a cached xpub, newest first, as the database
-- would give it.
cacheXPubTxs :: Cache -> XPubSpec -> Limits -> IO (Maybe [TxRef])
cacheXPubTxs c x limits = do
  xs <- page c x "txs" limits (\th -> (0, encode th))
  return $ xs <&> \ys -> [TxRef {txid = t, block = scoreBlockRef sc} | (bs, sc) <- ys, Right t <- [decode bs]]

-- | Page of the unspent outputs of a cached xpub, newest first, given its
-- balances, which give the paths of the addresses.
cacheXPubUnspents :: Cache -> XPubSpec -> [XPubBal] -> Limits -> IO (Maybe [XPubUnspent])
cacheXPubUnspents c x bals limits = do
  -- members are serialized unspent outputs: their block position, which is
  -- nine bytes long, then their output point
  xs <- page c x "utxo" limits (\th -> (9, encode (OutPoint th 0)))
  let paths = HashMap.fromList [(b.balance.address, b.path) | b <- bals]
  return $
    xs <&> \ys ->
      [ XPubUnspent {unspent = u, path = p}
      | (bs, _) <- ys,
        Right u <- [decode bs],
        Just a <- [u.address],
        Just p <- [HashMap.lookup a paths]
      ]

-- | How often a store marks an xpub as used, in milliseconds. Marking it on
-- every read would make every read a write; eviction only needs to know
-- which xpubs were used recently.
touchEvery :: Integer
touchEvery = 10000

-- | The time to mark an xpub as used with, if this store has not marked it
-- in the last 'touchEvery' milliseconds.
touchArgs :: Cache -> XPubSpec -> IO (Maybe Integer)
touchArgs c x = do
  now <- nowMs
  atomicModifyIORef' c.touched $ \m ->
    case HashMap.lookup (xpubId x) m of
      Just t | now - t < touchEvery -> (m, Nothing)
      _ ->
        -- forget everything now and then, so that the map stays small
        let m' = if HashMap.size m > 100000 then HashMap.empty else m
         in (HashMap.insert (xpubId x) now m', Just now)

-- | A page of a sorted set of a cached xpub, with the scores of its members,
-- or 'Nothing' if the xpub is not cached. One round trip to Redis: the
-- script finds where the page starts, as the database would, and marks the
-- xpub as used if it is time to.
--
-- A page that starts at a transaction starts at its position, which is the
-- score of the transaction in the sorted set of transactions of the xpub.
-- Members of the same score whose bytes, after a number of bytes, sort
-- before the bytes that the function gives for the transaction are skipped,
-- as the database skips them. A start transaction that is not in the xpub is
-- looked up in Redis or in the database, in a second round trip; a page
-- with a start that is found nowhere is empty.
page ::
  Cache ->
  XPubSpec ->
  ByteString ->
  Limits ->
  (TxHash -> (Int, ByteString)) ->
  IO (Maybe [(ByteString, Double)])
page c x set limits tieBytes = do
  touch <- touchArgs c x
  let common =
        [ xkey c x "meta",
          xkey c x set,
          xkey c x "txs",
          lruKey c,
          maybe "" num touch,
          xpubId x,
          num limits.offset,
          num limits.limit
        ]
      call extra = run c (R.eval pageScript [] (common <> extra))
  r <- case limits.start of
    Nothing -> call ["top"]
    Just (AtBlock h) -> call ["score", num' (blockRefScore (BlockRef h maxBound))]
    Just (AtTx th) -> do
      let (skip, bytes) = tieBytes th
      call ["tx", encode th, num skip, bytes] >>= \case
        ["nostart"] ->
          startOf th >>= \case
            Nothing -> return ["ok"]
            Just ref -> call ["at", num' (blockRefScore ref), num skip, bytes]
        y -> return y
  case r of
    "ok" : rest -> return (Just (pairs rest))
    _ -> return Nothing
  where
    pairs (m : sc : rest) | Just d <- readMaybe (C8.unpack sc) = (m, d) : pairs rest
    pairs _ = []
    startOf th =
      cacheTxData c [th] >>= \case
        [Just td] -> return (Just td.block)
        _ -> fmap (.block) <$> viewed c (\v -> L.getTxData v th)

-- | Bodies of transactions that Redis has, in the order of the hashes, with
-- the spenders of their outputs filled in. 'Nothing' for those it does not
-- have.
cacheTxData :: Cache -> [TxHash] -> IO [Maybe TxData]
cacheTxData _ [] = return []
cacheTxData c hs = do
  bodies <- run c (R.mget (NonEmpty.fromList (map (txKey c) hs)))
  spenders <- run c $ do
    rs <- forM (zip hs bodies) $ \(h, b) ->
      if isJust b then fmap Just <$> R.hgetall (spKey c h) else return (pure Nothing)
    return (sequence rs)
  return (zipWith fill bodies spenders)
  where
    fill (Just b) sp
      | Right td <- decode b =
          let ss =
                IntMap.fromList
                  [ (fromIntegral i, s)
                  | (k, v) <- fromMaybe [] sp,
                    Right (i :: Word32) <- [decode k],
                    Right s <- [decode v]
                  ]
           in Just (td :: TxData) {spenders = ss}
    fill _ _ = Nothing

-------------------------------------------------------------------------------
-- Changing
-------------------------------------------------------------------------------

-- | Cache an xpub that was just read from the database, if it has enough
-- used addresses, or enough transactions: a wallet that reuses a few
-- addresses for many payments is as slow to read from the database as one
-- with many addresses. Transactions are counted per address, so one that
-- touches two addresses counts twice, which is close enough to decide. The
-- xpub is loaded in the background; the caller keeps the answer it has.
cacheRequest :: Cache -> XPubSpec -> [XPubBal] -> IO ()
cacheRequest c x bals =
  when (length used >= c.minAddrs || sum (map (.balance.txs) used) >= fromIntegral c.minTxs) $
    push c (JobCache x)
  where
    used = filter (not . nullBalance . (.balance)) bals

-- | Give the worker something to do.
push :: Cache -> Job -> IO ()
push c j = atomically (modifyTVar' c.jobs (j :))

-- | Give the worker something to do in two seconds.
later :: Cache -> Job -> IO ()
later c j = void . forkIO $ threadDelay 2000000 >> push c j

-- | Remove an xpub from the cache. Returns whether it was cached.
cacheDelete :: Cache -> XPubSpec -> IO Bool
cacheDelete c x = do
  r <- run c (R.eval deleteScript [] [c.prefix, xpubId x])
  return (r == (1 :: Integer))

-- | Evict the least recently used xpubs until at most the given number are
-- cached. Returns how many were evicted. The cache does this by itself after
-- caching an xpub, down to its configured maximum.
cacheEvict :: Cache -> Int -> IO Int
cacheEvict c n = do
  evicted <- fromInteger <$> run c (R.eval evictScript [] [c.prefix, num n])
  count c (.evictions) evicted
  return evicted

-- | Bring the entry of an xpub in Redis up to date with the database, unless
-- another store holds its lock or Redis knows more than this store (see
-- 'Reload'). The transactions given are the ones that changed it: their
-- bodies are written again even if Redis has them.
--
-- A store never writes data that knows less than what is in Redis, so stores
-- that see the chain and the mempool at slightly different times do not undo
-- each other's work. Every body that the entry refers to is in Redis before
-- the entry is: the changes and the missing bodies are written in one Lua
-- script, which writes nothing if a body is missing, so a store that is
-- behind never finds a cached transaction that it cannot serve. Only the
-- differences are written, and the script writes nothing if the entry
-- changed since it was read (its generation), so Redis is held for the size
-- of the change, not of the xpub. Bodies that Redis already has, such as
-- those of the transactions that a block confirms, are rewritten first, in
-- small scripts ('rewriteBodies'), so that the confirmation of a large block
-- does not hold Redis in one long script.
--
-- = Reading only what changed
--
-- Working out the differences can take one of two paths.
--
-- * __Incremental__ ('incrementalDiff'): only the transactions that changed
--   are read, with the outputs that they create and spend at addresses of
--   the xpub. The stress test (@stress\/results@) showed that reading the
--   whole xpub from both the database and Redis was almost all the time of a
--   reload, growing with the size of the xpub to seconds at 100,000
--   transactions; the changes of one block or transaction are a few reads,
--   whatever the size of the xpub.
--
-- * __Full__: the whole xpub is read from the database and from Redis, and
--   compared. This is what a first caching does, and the fallback whenever
--   the incremental path cannot be sure of its answer.
--
-- The incremental path relies on every change of the xpub having been
-- applied to the entry, which is what the claims of events are for, but a
-- store can miss events: while it catches up, after a restart, or after an
-- error. So it is only taken when the entry shows no sign of having missed
-- one, and it checks its own result:
--
-- * the entry is in the current format, stamped with the best block of this
--   store or the one before it (a larger gap means blocks it may have
--   missed), and was fully reloaded less than 'fullReloadAfter' ago;
--
-- * the balances of every address of the xpub are read from the database,
--   which is cheap, a read per address; an address that no changed
--   transaction touches must have the balance that Redis has. Under load it
--   often does not, because the database is already ahead of the events
--   being applied, so the newer transactions of those addresses are added to
--   the changes first ('catchUp'), and only a balance that they do not
--   explain means a change was missed;
--
-- * no address with transactions enters or leaves the gap, as its history
--   would have to be read whole.
--
-- Otherwise the full path runs. A missed change that leaves every balance
-- as it was, such as the confirmation of a transaction that only spends from
-- the xpub, is the one thing the checks cannot see, and the regular full
-- reload bounds how long it can last.
cacheRefresh :: Cache -> XPubSpec -> HashSet TxHash -> IO Reload
cacheRefresh c x changed =
  fromMaybe Locked <$> withLock c x (timed (go (3 :: Int) []))
  where
    go tries extra = do
      m <- readMeta c x
      height <- bestHeight c
      now <- nowMs
      if not (mayReplace m.stamp height False)
        then behind
        else do
          bals <- viewed c (\v -> L.xPubBals v x)
          fast <-
            if incremental m height now
              then incrementalDiff c x changed m bals
              else return Fallback
          diff <- case fast of
            Apply d -> return (Just d)
            KnowsMore -> return Nothing
            Fallback -> fullDiff c x changed m height bals now
          case diff of
            Nothing -> behind
            Just d -> commit tries extra m height now bals d
    commit tries extra m height now bals d = do
      bodies <- catMaybes <$> viewed c (\v -> L.getManyTxData v (nub (extra <> d.wanted))) >>= rewriteBodies c
      let args =
            [c.prefix, xpubId x, num height, num now, encode x, encode bals, m.gen]
              <> [if d.full then num now else ""]
              <> counted d.addrAdd
              <> counted d.addrDel
              <> counted2 [(t, num' sc) | (t, sc) <- d.txAdd]
              <> counted d.txDel
              <> counted3 [(op, u, num' sc) | (op, u, sc) <- d.utxoAdd]
              <> counted2 d.utxoDel
              <> bodyArgs bodies
      run c (R.eval commitScript [] args) >>= \case
        ["ok"] -> do
          void (cacheEvict c c.maxXPubs)
          count c (.reloads) 1
          when d.full $ count c (.fullReloads) 1
          atomicModifyIORef' c.counters $ \k ->
            ( k {reloads = k.reloads + 1, fullReloads = k.fullReloads + fromEnum d.full} :: CacheCounters,
              ()
            )
          return Reloaded
        ("missing" : missing)
          | tries > 0 -> go (tries - 1) (rights (map decode missing))
        ["changed"]
          | tries > 0 -> go (tries - 1) extra
        _ -> behind
    -- whether the entry may be brought up to date incrementally
    incremental m height now =
      m.ver == "3"
        && not (HashSet.null changed)
        && maybe False (\s -> toInteger height - s <= 1) m.stamp
        && maybe False (\f -> now - f < fullReloadAfter x) m.full
    behind = do
      count c (.stale) 1
      atomicModifyIORef' c.counters (\k -> (k {behind = k.behind + 1} :: CacheCounters, ()))
      return Behind
    timed f = do
      start <- nowMs
      r <- f
      end <- nowMs
      forM_ c.metrics $ \m -> addTiming m.reloadTime (fromInteger (end - start))
      return r

-- | Longest time, in milliseconds, between full reloads of an entry: ten
-- minutes; each xpub has its own interval up to this ('fullReloadAfter'). It
-- bounds how long a change that the checks of the incremental path cannot
-- see stays out of the cache. A full reload of the largest xpubs of the
-- stress test takes a few seconds and holds the xpub in memory while it
-- runs, so it is kept rare.
fullReloadEvery :: Integer
fullReloadEvery = 10 * 60 * 1000

-- | Time, in milliseconds, after its last full reload when an entry is read
-- whole again: between half of 'fullReloadEvery' and all of it, the same for
-- an xpub on every store, and different from one xpub to the next.
--
-- With one interval for every entry, entries cached together are reloaded
-- whole together, every time. In the full stress test the four xpubs did so
-- every ten minutes, and the heap of the server went from under 100 MiB to
-- 785 MiB for a few seconds, holding all four at once. Each xpub gets its own
-- interval instead, so such entries drift apart from their first scheduled
-- full reload. It comes from the hash of the xpub, not from a random number,
-- so that it needs no state and every store sharing Redis agrees on it.
fullReloadAfter :: XPubSpec -> Integer
fullReloadAfter x = half + spread `mod` half
  where
    half = fullReloadEvery `div` 2
    spread = BS.foldl' (\a w -> a * 256 + toInteger w) 0 (BS.take 4 (encode (sha256 (encode x))))

-- | What to write to bring an entry up to date. Members of the sets are
-- serialized: addresses, transaction hashes, and unspent outputs, which go
-- with their outpoints.
data Diff = Diff
  { addrAdd :: ![ByteString],
    addrDel :: ![ByteString],
    -- | transactions to add or move, with their scores
    txAdd :: ![(ByteString, Double)],
    txDel :: ![ByteString],
    -- | unspent outputs to add, with their outpoints and scores
    utxoAdd :: ![(ByteString, ByteString, Double)],
    -- | outpoints of unspent outputs to remove, with the member if known; the
    -- script finds it by its outpoint otherwise
    utxoDel :: ![(ByteString, ByteString)],
    -- | transactions whose bodies to write
    wanted :: ![TxHash],
    -- | whether the whole xpub was read, which the entry records
    full :: !Bool
  }

-- | The outcome of trying the incremental path.
data Incremental
  = -- | these are the differences
    Apply !Diff
  | -- | Redis has a changed transaction that this store has never seen
    KnowsMore
  | -- | the incremental path cannot be sure; read the whole xpub
    Fallback

-- | The differences of an entry from the transactions that changed and the
-- balances of the xpub, without reading its history; see 'cacheRefresh'.
--
-- When the balances show that the database knows of changes beyond the ones
-- given, the newest transactions of the addresses whose balances moved are
-- added to them ('catchUp'), and the differences are worked out again; only
-- if that does not account for every balance does it fall back.
incrementalDiff :: Cache -> XPubSpec -> HashSet TxHash -> Meta -> [XPubBal] -> IO Incremental
incrementalDiff c x changed m bals =
  case (decode m.bal :: Either String [XPubBal], m.stamp) of
    (Right old, Just stamp) ->
      incrementalFrom c x old bals changed >>= \case
        Missed missed ->
          catchUp c x stamp missed >>= \case
            Just extra
              | not (extra `HashSet.isSubsetOf` changed) ->
                  incrementalFrom c x old bals (changed <> extra) <&> \case
                    Missed _ -> Fallback
                    Settled r -> r
            _ -> return Fallback
        Settled r -> return r
    _ -> return Fallback

-- | The outcome of working out the differences from a set of changes.
data Attempt
  = -- | the balances of these addresses moved, but no change touches them
    Missed ![Address]
  | Settled !Incremental

-- | The differences of an entry from its old balances, its new ones, and the
-- transactions that changed it.
incrementalFrom :: Cache -> XPubSpec -> [XPubBal] -> [XPubBal] -> HashSet TxHash -> IO Attempt
incrementalFrom c x old bals changed = do
  let hashes = HashSet.toList changed
  stored <- viewed c (\v -> L.getManyTxData v hashes)
  -- those that the database does not have because this store deleted
  -- them count as known, and as deleted
  gone <- readIORef c.lost
  dropped <- lostTxs c [h | (h, Nothing) <- zip hashes stored, HashSet.member h gone]
  let found = HashMap.fromList [(txHash td.tx, td) | td <- dropped]
      tds = [maybe (HashMap.lookup h found) Just td | (h, td) <- zip hashes stored]
  let newMap = HashMap.fromList [(b.balance.address, b.balance) | b <- bals]
      oldMap = HashMap.fromList [(b.balance.address, b.balance) | b <- old]
      entering = [b | (a, b) <- HashMap.toList newMap, not (HashMap.member a oldMap)]
      leaving = [b | (a, b) <- HashMap.toList oldMap, not (HashMap.member a newMap)]
      known = catMaybes tds
      unknown = [h | (h, Nothing) <- zip hashes tds]
      ours a = HashMap.member a newMap
      touched = HashSet.fromList [a | td <- known, a <- txAddresses c td, ours a]
      missed =
        [ a
        | (a, b) <- HashMap.toList newMap,
          Just o <- [HashMap.lookup a oldMap],
          o /= b,
          not (HashSet.member a touched)
        ]
  if
    | any (\b -> b.txs > 0) (entering <> leaving) -> return (Settled Fallback)
    | not (null missed) -> return (Missed missed)
    | otherwise -> do
        inRedis <- run c (sequence <$> mapM (R.zscore (xkey c x "txs") . encode) unknown)
        if any isJust inRedis
          then return (Settled KnowsMore)
          else do
            let active td = not td.deleted && any ours (txAddresses c td)
                ops = nub (concatMap (outpointsAt c ours) known)
            us <- viewed c (\v -> mapM (L.getUnspent v) ops)
            return . Settled . Apply $
              Diff
                { addrAdd = [encode b.address | b <- entering],
                  addrDel = [encode b.address | b <- leaving],
                  txAdd = [(encode (txHash td.tx), blockRefScore td.block) | td <- known, active td],
                  txDel = [encode (txHash td.tx) | td <- known, not (active td)],
                  utxoAdd = [(encode op, encode u, blockRefScore u.block) | (op, Just u) <- zip ops us],
                  utxoDel = [(encode op, "") | (op, Nothing) <- zip ops us],
                  wanted = [txHash td.tx | td <- known, active td],
                  full = False
                }

-- | Changes of an entry that the database has and that it was not told of:
-- what a store finds when its database runs ahead of the events it is
-- applying, as it does under load, when the balances it reads after a batch
-- already include the transactions of the next. 'Nothing' if there are more
-- than 'catchUpMost', when reading the whole xpub is as cheap.
--
-- The entry is stamped with a block that this store had whole, so what can
-- have changed since is:
--
-- * transactions in the mempool or in a later block, which are first in the
--   index of an address, newest first, and are read from it for each address
--   whose balance moved, down to the stamp;
--
-- * transactions in the mempool when the entry was written that have been
--   confirmed or dropped since, found from the mempool transactions of the
--   entry, which are few.
--
-- A reorganisation that replaces the block of the stamp is not covered, and
-- the balances catch it: if the transactions found do not account for every
-- balance, the full path runs.
catchUp :: Cache -> XPubSpec -> Integer -> [Address] -> IO (Maybe (HashSet TxHash))
catchUp c x stamp addrs = do
  newer <- sequence <$> mapM addressNewer addrs
  pool <- run c (R.zrangebyscoreLimit (xkey c x "txs") (-1 / 0) (-0.5) 0 (toInteger catchUpMost + 1))
  let poolHashes = rights (map decode pool)
  tds <- viewed c (\v -> L.getManyTxData v poolHashes)
  let settled = [h | (h, td) <- zip poolHashes tds, maybe True left td]
      found = HashSet.fromList (settled <> concat (fromMaybe [] newer))
  return $
    if isNothing newer || length pool > catchUpMost || HashSet.size found > catchUpMost
      then Nothing
      else Just found
  where
    -- a mempool transaction of the entry that is no longer in the mempool
    left td = td.deleted || not (isMem td.block)
    isMem MemRef {} = True
    isMem BlockRef {} = False
    after t = case t.block of
      MemRef {} -> True
      BlockRef {height = h} -> toInteger h > stamp
    chunk = 100 :: Word32
    addressNewer a = loop 0 []
      where
        loop off acc = do
          ts <- viewed c (\v -> L.getAddressTxs v a Limits {limit = chunk, offset = off, start = Nothing})
          let (new, rest) = span after ts
              acc' = acc <> map (.txid) new
          if
            | length acc' > catchUpMost -> return Nothing
            | not (null rest) || length ts < fromIntegral chunk -> return (Just acc')
            | otherwise -> loop (off + chunk) acc'

-- | Most transactions that 'catchUp' adds to the changes of a reload. Each
-- costs a few reads; a full reload of the xpubs of the stress test costs
-- tens of thousands.
catchUpMost :: Int
catchUpMost = 1000

-- | The differences of an entry from the whole xpub, read from the database
-- and from Redis. 'Nothing' if Redis has a transaction of the xpub that this
-- store has never seen.
fullDiff :: Cache -> XPubSpec -> HashSet TxHash -> Meta -> Word32 -> [XPubBal] -> Integer -> IO (Maybe Diff)
fullDiff c x changed m height bals _ = do
  st <- readStored c x
  txs <- viewed c (\v -> L.xPubTxs v bals def')
  let newTxs = [(encode t.txid, blockRefScore t.block) | t <- txs]
      newTxSet = HashSet.fromList (map fst newTxs)
      txDel = [t | t <- HashMap.keys st.txs, not (HashSet.member t newTxSet)]
  -- a transaction that Redis has and this store has never seen. The store
  -- keeps nothing of a transaction that it deleted, so one that it does
  -- not have is taken for deleted, and not for unseen, if the cache heard
  -- of its deletion, or if the entry has it as unconfirmed for more than
  -- ten minutes: a store that is in step with the others has seen a
  -- transaction of the mempool by then, and one that is not does not get
  -- here.
  let asked = [(h, sc) | t <- txDel, Right h <- [decode t], Just sc <- [HashMap.lookup t st.txs]]
  known <- viewed c (\v -> L.getManyTxData v (map fst asked))
  gone <- readIORef c.lost
  now <- (`div` 1000) <$> nowMs
  let stale sc = sc < 0 && fromIntegral now - negate sc > 600
      unseen = [h | ((h, sc), Nothing) <- zip asked known, not (HashSet.member h gone), not (stale sc)]
  if not (mayReplace m.stamp height (not (null unseen)))
    then return Nothing
    else do
      utxos <- viewed c (\v -> L.xPubUnspents v bals def')
      let txAdd = [(t, sc) | (t, sc) <- newTxs, HashMap.lookup t st.txs /= Just sc]
          newUs = [(encode u.unspent.outpoint, encode u.unspent, blockRefScore u.unspent.block) | u <- utxos]
          newUSet = HashSet.fromList [u | (_, u, _) <- newUs]
          -- an entry of an older format has no index of outpoints yet, so
          -- every output is written, to build it
          utxoAdd
            | m.ver == "3" = [n | n@(_, u, _) <- newUs, not (HashSet.member u st.utxos)]
            | otherwise = newUs
          utxoDel =
            [ (either (const "") (encode . (.outpoint)) (decode u :: Either String Unspent), u)
            | u <- HashSet.toList st.utxos,
              not (HashSet.member u newUSet)
            ]
          newAs = HashSet.fromList [encode b.balance.address | b <- bals]
      return . Just $
        Diff
          { addrAdd = [a | a <- HashSet.toList newAs, not (HashSet.member a st.addrs)],
            addrDel = [a | a <- HashSet.toList st.addrs, not (HashSet.member a newAs)],
            txAdd,
            txDel,
            utxoAdd,
            utxoDel,
            -- bodies of the transactions that are new or moved, and of those
            -- that changed; the others are in Redis, kept there by this entry
            wanted =
              nub $
                [h | (t, _) <- txAdd, Right h <- [decode t]]
                  <> [t.txid | t <- txs, t.txid `HashSet.member` changed],
            full = True
          }
  where
    def' = Limits {limit = 0, offset = 0, start = Nothing}

-- | Addresses that a transaction spends from or pays to.
txAddresses :: Cache -> TxData -> [Address]
txAddresses c td =
  nub . rights $
    map (scriptToAddressBS c.db.net c.db.ctx . (.script)) (IntMap.elems td.prevs)
      <> map (scriptToAddressBS c.db.net c.db.ctx . (.script)) td.tx.outputs

-- | Outpoints whose unspent output a transaction may change at addresses that
-- pass a test: those it creates, and those it spends.
outpointsAt :: Cache -> (Address -> Bool) -> TxData -> [OutPoint]
outpointsAt c ours td =
  [OutPoint (txHash td.tx) i | (i, o) <- zip [0 ..] td.tx.outputs, at o.script]
    <> [ inp.outpoint
       | (i, inp) <- zip [0 :: Int ..] td.tx.inputs,
         Just prev <- [IntMap.lookup i td.prevs],
         at prev.script
       ]
  where
    at s = either (const False) ours (scriptToAddressBS c.db.net c.db.ctx s)

-- | What a reload needs to know of an entry first: the height it is stamped
-- with, its generation, which every commit increments, the version of its
-- format, the time of its last full reload, and its balances, serialized.
data Meta = Meta
  { stamp :: !(Maybe Integer),
    gen :: !ByteString,
    ver :: !ByteString,
    full :: !(Maybe Integer),
    bal :: !ByteString
  }

-- | Read what a reload needs to know of an entry, in one round trip.
readMeta :: Cache -> XPubSpec -> IO Meta
readMeta c x = do
  (fs, b) <- run c $ do
    fs <- R.hmget (xkey c x "meta") ("height" NonEmpty.:| ["gen", "ver", "full"])
    b <- R.get (xkey c x "bal")
    return ((,) <$> fs <*> b)
  let field k = join (lookup k (zip [0 :: Int ..] fs))
      number = (readMaybe . C8.unpack =<<)
  return
    Meta
      { stamp = number (field 0),
        gen = fromMaybe "" (field 1),
        ver = fromMaybe "" (field 2),
        full = number (field 3),
        bal = fromMaybe "" b
      }

-- | The sets of an entry, as a full reload compares them with the database.
data Stored = Stored
  { txs :: !(HashMap ByteString Double),
    utxos :: !(HashSet ByteString),
    addrs :: !(HashSet ByteString)
  }

-- | Read the sets of an entry. The sorted sets are read in chunks, so that
-- no command holds Redis for long however large the xpub. The chunks are not
-- read atomically, but nothing changes the entry while this store holds its
-- lock except deleting or evicting it, which changes its generation, and the
-- commit checks the generation.
readStored :: Cache -> XPubSpec -> IO Stored
readStored c x = do
  as <- run c (R.smembers (xkey c x "addrs"))
  ts <- chunks (\i j -> R.zrangeWithscores (xkey c x "txs") i j)
  us <- chunks (\i j -> R.zrange (xkey c x "utxo") i j)
  return
    Stored
      { txs = HashMap.fromList ts,
        utxos = HashSet.fromList us,
        addrs = HashSet.fromList as
      }
  where
    size = 5000 :: Integer
    chunks :: (Integer -> Integer -> Redis (Either Reply [a])) -> IO [a]
    chunks f = loop 0
      where
        loop i = do
          xs <- run c (f i (i + size - 1))
          if toInteger (length xs) < size then return xs else (xs <>) <$> loop (i + size)

-- | Rewrite the bodies that Redis already has, in scripts of 'bodyChunk'
-- bodies each, and give back the others, for the commit to write.
--
-- A block confirms many transactions of a busy xpub at once, and each needs
-- its body written again, with its block and spenders. In one script, with
-- the scores and outputs of the entry, that held Redis for 10 to 30 ms at
-- the confirmation of each block in the stress test, while every other
-- client waited. Bodies are shared by every entry that has the transaction,
-- and are written whole from the database, so writing them before the entry
-- is safe: a body is never older than the entry that refers to it, which is
-- what matters (see 'cacheRefresh'), and other entries already see bodies
-- written by commits of other xpubs before their own. Only bodies that are
-- there are rewritten, so that a commit that then fails leaves nothing
-- behind; and if one goes between these scripts and the commit, the commit
-- reports it missing and is retried with it.
rewriteBodies :: Cache -> [TxData] -> IO [TxData]
rewriteBodies c tds =
  fmap concat . forM (chunks tds) $ \ch -> do
    absent <- HashSet.fromList <$> run c (R.eval bodiesScript [] (c.prefix : bodyArgs ch))
    return [td | td <- ch, encode (txHash td.tx) `HashSet.member` absent]
  where
    chunks [] = []
    chunks xs = let (a, b) = splitAt bodyChunk xs in a : chunks b

-- | Bodies rewritten by one script of 'rewriteBodies': each is a few
-- commands, so a chunk holds Redis for a millisecond or two.
bodyChunk :: Int
bodyChunk = 200

-- | Arguments for bodies and their spenders: the number of bodies, the hash
-- and body of each, the number of spenders, and the transaction, output index
-- and spender of each.
bodyArgs :: [TxData] -> [ByteString]
bodyArgs tds =
  counted2 [(encode (txHash td.tx), encode (td :: TxData) {spenders = IntMap.empty}) | td <- tds]
    <> counted3
      [ (encode (txHash td.tx), encode (fromIntegral i :: Word32), encode s)
      | td <- tds,
        (i, s) <- IntMap.toList td.spenders
      ]

counted :: [ByteString] -> [ByteString]
counted xs = num (length xs) : xs

counted2 :: [(ByteString, ByteString)] -> [ByteString]
counted2 xs = num (length xs) : concat [[a, b] | (a, b) <- xs]

counted3 :: [(ByteString, ByteString, ByteString)] -> [ByteString]
counted3 xs = num (length xs) : concat [[a, b, d] | (a, b, d) <- xs]

num :: (Integral a) => a -> ByteString
num = C8.pack . show . toInteger

num' :: Double -> ByteString
num' = C8.pack . show

nowMs :: IO Integer
nowMs = round . (* 1000) <$> getPOSIXTime

-- | Height of the best block of the database, 0 if none.
bestHeight :: Cache -> IO Word32
bestHeight c =
  Q.getBestBlock c.db >>= \case
    Nothing -> return 0
    Just h -> maybe 0 (.height) <$> Q.getBlock c.db h

-- | Whether the database has the best header of the node.
inSync :: Cache -> IO Bool
inSync c =
  Q.getBestBlock c.db >>= \case
    Nothing -> return False
    Just bb -> do
      cb <- chainGetBest c.chain
      return (cb.height > 0 && headerHash cb.header == bb)

-- | Run an action holding the lock of an xpub. 'Nothing' if another store
-- holds it. The lock expires after two minutes in case its holder dies.
withLock :: Cache -> XPubSpec -> IO a -> IO (Maybe a)
withLock c x f = do
  token <- C8.pack . show <$> (randomIO :: IO Word64)
  let key = c.prefix <> "lock:" <> xpubId x
      opts =
        R.SetOpts
          { R.setSeconds = Nothing,
            R.setMilliseconds = Just 120000,
            R.setUnixSeconds = Nothing,
            R.setUnixMilliseconds = Nothing,
            R.setCondition = Just R.Nx,
            R.setKeepTTL = False
          }
  R.runRedis c.redis (R.setOpts key token opts) >>= \case
    Right R.Ok -> do
      r <- f
      void (run c (R.eval unlockScript [key] [token] :: Redis (Either Reply Integer)))
      return (Just r)
    _ -> return Nothing

-------------------------------------------------------------------------------
-- Keeping up with the store
-------------------------------------------------------------------------------

-- | Do the work of the cache, forever.
--
-- A store that is not in sync ignores events and requests: the stores that
-- are ahead have already done or will do the work, and this one would only
-- find that Redis knows more. Once in sync, each event is claimed per cached
-- xpub that it changes ('claim'): the first store to claim a change reloads
-- the xpub and marks the claim done, and the others skip it, so a change is
-- applied once however many stores share the Redis, and stores that catch up
-- find their events done. A change that a store could not apply because
-- Redis knew more is released for another store to take; one that another
-- store is busy with is looked at again later.
worker :: Cache -> IO ()
worker c = do
  lastBest <- newIORef Nothing
  loopForever $ do
    js <- atomically $ do
      xs <- readTVar c.jobs
      checkSTM (not (null xs))
      writeTVar c.jobs []
      return (reverse xs)
    synced <- inSync c
    if synced
      then handleAny (\e -> log' c LevelError ("Cache: " <> cs (show e))) (work c lastBest js)
      else releaseClaims c (concat [ks | JobHeld _ _ ks <- js])

-- | Do a batch of jobs.
work :: Cache -> IORef (Maybe BlockHash) -> [Job] -> IO ()
work c lastBest js = do
  fromEvents <- concat <$> mapM (eventChanges c lastBest) [e | JobEvent e <- js]
  let toClaim = fromEvents <> [(k, x, ts) | JobClaim x ts ks <- js, k <- ks]
  claimed <- fmap catMaybes . forM toClaim $ \(k, x, ts) ->
    afterClaim <$> claim c k >>= \case
      ReloadNow -> return (Just (x, (ts, [k])))
      Skip -> return Nothing
      ClaimLater -> later c (JobClaim x ts [k]) >> return Nothing
  let held =
        HashMap.fromListWith
          (\(a, b) (a', b') -> (a <> a', b <> b'))
          (claimed <> [(x, (ts, ks)) | JobHeld x ts ks <- js])
  pooledForConcurrentlyN_ parallelReloads (HashMap.toList held) $ \(x, (ts, ks)) ->
    afterReload <$> reload x ts >>= \case
      Finish -> finishClaims c ks
      Release -> releaseClaims c ks
      Hold -> extendClaims c ks >> later c (JobHeld x ts ks)
  pooledForConcurrentlyN_ parallelReloads (nub [x | JobCache x <- js, not (HashMap.member x held)]) $ \x ->
    cacheIsCached c x >>= \case
      True -> return ()
      False ->
        reload x HashSet.empty >>= \case
          Locked -> later c (JobCache x)
          _ -> return ()
  where
    reload x ts =
      tryAny (cacheRefresh c x ts) >>= \case
        Right r -> return r
        Left e -> do
          log' c LevelError ("Could not cache xpub: " <> cs (show e))
          return Locked

-- | How many xpubs a store reloads at once. Each holds the lock of its xpub,
-- so two stores never reload the same one; reloading several at once keeps
-- one large xpub from holding up the others.
parallelReloads :: Int
parallelReloads = 4

-- | Changes that an event makes to cached xpubs: for each xpub, the key of
-- the claim for the change, and the transactions that make it. Also keeps the
-- spenders of cached transactions up to date, which is harmless to repeat.
eventChanges :: Cache -> IORef (Maybe BlockHash) -> StoreEvent -> IO [(ByteString, XPubSpec, HashSet TxHash)]
eventChanges c lastBest e = do
  (name, tds) <- case e of
    StoreBestBlock h -> do
      prev <- readIORef lastBest
      writeIORef lastBest (Just h)
      ("b:" <> encode h,) <$> blockTxs c prev h
    StoreMempoolNew t -> ("t+:" <> encode t,) . maybeToList <$> viewed c (\v -> L.getTxData v t)
    -- the store keeps nothing of a transaction that it deleted. Redis has
    -- the body of every transaction of a cached xpub, so if it does not
    -- have this one, no entry does either
    StoreMempoolDelete t -> do
      noteLost c [t]
      ("t-:" <> encode t,) <$> lostTxs c [t]
    _ -> return ("", [])
  if null tds
    then return []
    else do
      updateSpenders c tds
      affected <- affectedXPubs c tds
      return [(c.prefix <> "ev:" <> name <> ":" <> xpubId x, x, ts) | (x, ts) <- affected]

-- | Claim a change. A claim is held for two minutes, so that a store that
-- dies while holding it does not keep others from the change for long.
claim :: Cache -> ByteString -> IO Claim
claim c k = do
  let opts =
        R.SetOpts
          { R.setSeconds = Nothing,
            R.setMilliseconds = Just 120000,
            R.setUnixSeconds = Nothing,
            R.setUnixMilliseconds = Nothing,
            R.setCondition = Just R.Nx,
            R.setKeepTTL = False
          }
  R.runRedis c.redis (R.setOpts k "busy" opts) >>= \case
    Right R.Ok -> return Claimed
    _ ->
      run c (R.get k) <&> \case
        Just "done" -> Done
        _ -> Busy

-- | Mark changes as applied, for an hour: long enough for every store in
-- sync to see the same events.
finishClaims :: Cache -> [ByteString] -> IO ()
finishClaims _ [] = return ()
finishClaims c ks =
  void . run c $ sequence <$> mapM (\k -> R.setex k 3600 "done") ks

-- | Give up changes, for another store to apply.
releaseClaims :: Cache -> [ByteString] -> IO ()
releaseClaims _ [] = return ()
releaseClaims c ks = void (run c (R.del (NonEmpty.fromList ks)))

-- | Hold changes for another two minutes.
extendClaims :: Cache -> [ByteString] -> IO ()
extendClaims _ [] = return ()
extendClaims c ks = void . run c $ sequence <$> mapM (`R.pexpire` 120000) ks

-- | Pass the events of the store that may change cached xpubs to the worker.
listener :: Cache -> Inbox StoreEvent -> IO ()
listener c events =
  loopForever $
    receive events >>= \case
      e@StoreBestBlock {} -> push c (JobEvent e)
      e@StoreMempoolNew {} -> push c (JobEvent e)
      e@StoreMempoolDelete {} -> push c (JobEvent e)
      _ -> return ()

-- | Transactions of a new best block, and of the blocks that a
-- reorganisation took out of the chain since the previous best block.
blockTxs :: Cache -> Maybe BlockHash -> BlockHash -> IO [TxData]
blockTxs c prev h =
  Q.getBlock c.db h >>= \case
    Nothing -> return []
    Just bd -> do
      reverted <- case prev of
        Just p | p /= bd.header.prev && p /= h -> walk p
        _ -> return []
      let hashes = bd.txs <> reverted
      found <- viewed c (\v -> L.getManyTxData v hashes)
      -- a transaction of a reverted block that did not go back to the
      -- mempool, as one that spends its coinbase does not, is gone from
      -- the database. Redis has its body if a cached xpub has it.
      let missing = [h' | (h', Nothing) <- zip hashes found]
      noteLost c missing
      (catMaybes found <>) <$> lostTxs c missing
  where
    -- blocks that are no longer in the main chain, from the old best block
    walk p =
      Q.getBlock c.db p >>= \case
        Just b | not b.main -> (b.txs <>) <$> walk b.header.prev
        _ -> return []

-- | Note that the store deleted transactions.
noteLost :: Cache -> [TxHash] -> IO ()
noteLost _ [] = return ()
noteLost c hs = atomicModifyIORef' c.lost $ \seen ->
  let more = foldr HashSet.insert seen hs
   in (if HashSet.size more > 100000 then HashSet.fromList hs else more, ())

-- | The bodies that Redis has of transactions that the store deleted,
-- marked as deleted. Redis has the body of every transaction of a cached
-- xpub, so one that it does not have is in no entry.
lostTxs :: Cache -> [TxHash] -> IO [TxData]
lostTxs c hs = map (\td -> (td :: TxData) {deleted = True}) . catMaybes <$> cacheTxData c hs

-- | Cached xpubs with an address that these transactions spend from or pay
-- to, with the transactions that touch each.
affectedXPubs :: Cache -> [TxData] -> IO [(XPubSpec, HashSet TxHash)]
affectedXPubs c tds = do
  let net = c.db.net
      ctx = c.db.ctx
      addrsOf :: TxData -> [Address]
      addrsOf td =
        nub . rights $
          map (scriptToAddressBS net ctx . (.script)) (IntMap.elems td.prevs)
            <> map (scriptToAddressBS net ctx . (.script)) td.tx.outputs
      byAddr =
        HashMap.fromListWith (<>) [(a, HashSet.singleton (txHash td.tx)) | td <- tds, a <- addrsOf td]
      addrs = HashMap.keys byAddr
  ids <- run c (sequence <$> mapM (R.smembers . addrKey c) addrs)
  let byId =
        HashMap.fromListWith
          (<>)
          [(i, ts) | (a, is) <- zip addrs ids, i <- is, Just ts <- [HashMap.lookup a byAddr]]
      spec i = R.hget (c.prefix <> "x:" <> i <> ":meta") "spec"
  specs <- run c (sequence <$> mapM spec (HashMap.keys byId))
  return
    [ (x, ts)
    | (Just bs, ts) <- zip specs (HashMap.elems byId),
      Right x <- [decode bs]
    ]

-- | Record who spends the outputs of cached transactions: these
-- transactions spend them, or no longer do if they were deleted.
updateSpenders :: Cache -> [TxData] -> IO ()
updateSpenders c tds =
  unless (null quads) $
    void (run c (R.eval spendsScript [] args :: Redis (Either Reply Integer)))
  where
    args = c.prefix : num (length quads) : concat quads
    quads =
      [ [ encode i.outpoint.hash,
          encode i.outpoint.index,
          if td.deleted then "" else encode (Spender (txHash td.tx) n),
          encode (txHash td.tx)
        ]
      | td <- tds,
        (n, i) <- zip [0 ..] td.tx.inputs
      ]

-------------------------------------------------------------------------------
-- Lua scripts
-------------------------------------------------------------------------------

-- The scripts that Redis runs are in files of their own under
-- @src\/Haskoin\/Store\/Cache\/lua@, where they can be read, linted and run
-- with @redis-cli --eval@ on their own, and are embedded here when the module
-- is compiled. Embedding them, rather than reading them when the server
-- starts, keeps the library free of data files that a program using it would
-- have to install and find. Their comments are sent with every call, so keep
-- them short; the full description of each is the Haddock of its binding.

-- | Delete an xpub. Arguments: prefix, id. Returns 1 if it was cached.
deleteScript :: ByteString
deleteScript = $(makeRelativeToProject "src/Haskoin/Store/Cache/lua/delete.lua" >>= embedFile)

-- | Evict the least recently used xpubs past a number. Arguments: prefix,
-- maximum. Returns how many were evicted.
evictScript :: ByteString
evictScript = $(makeRelativeToProject "src/Haskoin/Store/Cache/lua/evict.lua" >>= embedFile)

-- | A page of a sorted set of a cached xpub; see 'page'. Arguments: the
-- keys of the meta hash, of the sorted set and of the sorted set of
-- transactions, the key of the set of recent use, the time to mark the xpub
-- used with (empty for not now), the id of the xpub, the offset and the limit
-- (0 for no limit), and how the page starts: @top@; @score@ and a score;
-- @at@, a score, a number of bytes and the bytes of the start; or @tx@, a
-- transaction hash, a number of bytes and the bytes of the start. Returns
-- @{\"none\"}@ if the xpub is not cached, @{\"nostart\"}@ if the
-- transaction of @tx@ is not one of the xpub, or @{\"ok\"}@ followed by
-- each member and its score.
--
-- Bytes are compared one by one: Lua compares strings by the collation of
-- the locale of the server, which need not be the order of their bytes that
-- Redis and the database sort members by.
pageScript :: ByteString
pageScript = $(makeRelativeToProject "src/Haskoin/Store/Cache/lua/page.lua" >>= embedFile)

-- | Apply the differences of an entry of an xpub, unless the stored one is
-- stamped with a higher block, or is not the generation they were computed
-- from. Arguments: prefix, id, height, time of use, serialized xpub,
-- balances, generation (empty for none); then, each preceded by its number,
-- the addresses to add, the addresses to remove, the transactions to add or
-- move with their scores, the transactions to remove, the unspent outputs to
-- add with their scores, the unspent outputs to remove, the bodies to write
-- with their hashes, and the spenders with their transaction and output
-- index. Returns @{\"stale\"}@ if the stored entry has a higher block,
-- @{\"changed\"}@ if its generation is not the one given,
-- @{\"missing\", ...}@ with the hashes of added transactions that have no
-- body in Redis nor in the arguments, or @{\"ok\"}@. Nothing is written
-- unless it returns @{\"ok\"}@.
--
-- A transaction removed from the entry loses this xpub from its set of
-- xpubs, and its body goes when no cached xpub has it any more.
commitScript :: ByteString
commitScript = $(makeRelativeToProject "src/Haskoin/Store/Cache/lua/commit.lua" >>= embedFile)

-- | Rewrite bodies that Redis has, with their spenders; see 'rewriteBodies'.
-- Arguments: prefix, then as at the end of 'commitScript': the number of
-- bodies, the hash and body of each, the number of spenders, and the
-- transaction, output index and spender of each. Returns the hashes of the
-- bodies that Redis does not have, which it does not write.
bodiesScript :: ByteString
bodiesScript = $(makeRelativeToProject "src/Haskoin/Store/Cache/lua/bodies.lua" >>= embedFile)

-- | Record or forget spenders of outputs of cached transactions. Arguments:
-- prefix, number of entries, then for each: hash of the transaction that
-- created the output, output index, spender (empty to forget) and hash of
-- the spending transaction. Only transactions whose body is cached are
-- touched, and a spender is only forgotten if it is still the one given.
spendsScript :: ByteString
spendsScript = $(makeRelativeToProject "src/Haskoin/Store/Cache/lua/spends.lua" >>= embedFile)

-- | Release a lock if it still holds the given token. Keys: lock. Arguments:
-- token.
unlockScript :: ByteString
unlockScript = $(makeRelativeToProject "src/Haskoin/Store/Cache/lua/unlock.lua" >>= embedFile)
