{-# LANGUAGE RecordWildCards #-}
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
-- The data of a cached xpub is never patched. Whenever a block or a mempool
-- transaction touches one of its addresses, or a reorganisation changes one
-- of its transactions, the store that saw it reads everything about the xpub
-- again from its database, with the same functions that answer uncached
-- queries ("Haskoin.Store.Database.Query"), and replaces the whole entry in
-- Redis in one atomic step. A cached answer is therefore always an answer the
-- database gave, and the cache cannot drift from it, as the cache it replaces
-- did when it applied each change by itself. The price is more work per
-- change: a payment to a large xpub reloads all of it. Reloads of one xpub are
-- coalesced, so a burst of payments costs at most one reload in progress and
-- one waiting.
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
--   cached xpub in full, transactions it has not imported included.
--
-- The first two rules are the pure decisions of
-- "Haskoin.Store.Cache.Protocol", where a model test checks them with several
-- stores catching up in random interleavings.
--
-- A lock per xpub keeps two stores from reloading it at the same time, and
-- every change of several keys is a Lua script, so readers never see half of
-- one. Stores that answer from Redis serve the same data; stores whose
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
--   the best block it was read at (@height@) and the format version (@ver@). @\<id\>@ is the
--   hexadecimal of the first 16 bytes of the SHA-256 of the serialized xpub.
-- * @x:\<id\>:bal@: balances of the addresses up to the gap, serialized.
-- * @x:\<id\>:txs@: sorted set of transaction hashes, scored by 'blockRefScore'.
-- * @x:\<id\>:utxo@: sorted set of serialized unspent outputs, scored the same.
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
  )
where

import Control.Monad (forM, forM_, unless, void, when)
import Data.Bits (shift, (.&.), (.|.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as C8
import Data.Either (rights)
import Data.Functor ((<&>))
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
    readIORef,
    readTVar,
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
  reloadTime <- newStatTiming s "cache.reload" 10
  return CacheMetrics {..}
  where
    c x = newStatCounter s ("cache." <> x) 10

-- | A running cache.
data Cache = Cache
  { redis :: !Connection,
    prefix :: !ByteString,
    db :: !DatabaseReader,
    chain :: !Chain,
    minAddrs :: !Int,
    maxXPubs :: !Int,
    logger :: !Logger,
    metrics :: !(Maybe CacheMetrics),
    -- | what this store did to the cache since it started
    counters :: !(IORef CacheCounters),
    -- | work for the worker, newest first
    jobs :: !(TVar [Job])
  }

-- | What a store did to the cache since it started.
data CacheCounters = CacheCounters
  { -- | xpubs read again from the database and written
    reloads :: !Int,
    -- | reloads given up because Redis knew more
    behind :: !Int
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
  counters <- newIORef (CacheCounters 0 0)
  metrics <- mapM newCacheMetrics cfg.stats
  let cache =
        Cache
          { redis = conn,
            prefix = cachePrefix db.net,
            db = db,
            chain = chain,
            minAddrs = cfg.minAddrs,
            maxXPubs = cfg.maxXPubs,
            logger = logger,
            metrics = metrics,
            counters = counters,
            jobs = jobs
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
  now <- nowMs
  r <- run c $ do
    b <- R.get (xkey c x "bal")
    _ <- R.zadd (lruKey c) [(fromIntegral now, xpubId x)]
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
cacheXPubTxCount c x =
  withCached c x $ fromIntegral <$> run c (R.zcard (xkey c x "txs"))

-- | Page of the transactions of a cached xpub, newest first, as the database
-- would give it.
cacheXPubTxs :: Cache -> XPubSpec -> Limits -> IO (Maybe [TxRef])
cacheXPubTxs c x limits =
  withCached c x $ do
    let from = case limits.start of
          Just (AtTx th) -> Just (const (encode th))
          _ -> Nothing
    xs <- page c (xkey c x "txs") limits from =<< startPos c limits
    return [TxRef {txid = t, block = scoreBlockRef s} | (bs, s) <- xs, Right t <- [decode bs]]

-- | Page of the unspent outputs of a cached xpub, newest first, given its
-- balances, which give the paths of the addresses.
cacheXPubUnspents :: Cache -> XPubSpec -> [XPubBal] -> Limits -> IO (Maybe [XPubUnspent])
cacheXPubUnspents c x bals limits =
  withCached c x $ do
    -- members are serialized unspent outputs, which start with their
    -- block position, then the output point
    let from = case limits.start of
          Just (AtTx th) -> Just (\ref -> encode ref <> encode (OutPoint th 0))
          _ -> Nothing
    xs <- page c (xkey c x "utxo") limits from =<< startPos c limits
    let paths = HashMap.fromList [(b.balance.address, b.path) | b <- bals]
    return
      [ XPubUnspent {unspent = u, path = p}
      | (bs, _) <- xs,
        Right u <- [decode bs],
        Just a <- [u.address],
        Just p <- [HashMap.lookup a paths]
      ]

-- | Run a read only if the xpub is cached, marking it as used.
withCached :: Cache -> XPubSpec -> IO a -> IO (Maybe a)
withCached c x f = do
  now <- nowMs
  cached <- run c $ do
    e <- R.exists (xkey c x "meta")
    _ <- R.zadd (lruKey c) [(fromIntegral now, xpubId x)]
    return e
  if cached then Just <$> f else return Nothing

-- | Position to start a page at, if the page has a start: the position of
-- the start transaction, or of the last block at a height. 'Nothing' if the
-- start transaction is not known.
startPos :: Cache -> Limits -> IO (Maybe (Maybe BlockRef))
startPos c limits = case limits.start of
  Nothing -> return (Just Nothing)
  Just (AtBlock h) -> return (Just (Just (BlockRef h maxBound)))
  Just (AtTx th) ->
    cacheTxData c [th] >>= \case
      [Just td] -> return (Just (Just td.block))
      _ ->
        Q.getTxData c.db th >>= \case
          Just td -> return (Just (Just td.block))
          Nothing -> return Nothing

-- | Members of a sorted set with their scores, from a position, at an offset,
-- up to a limit (0 for all). When the page starts at a transaction, the
-- members with the score of its position whose bytes sort before the bytes
-- that the function gives for that position are skipped: those of mempool
-- transactions of the same second with smaller hashes, as the database does.
-- A start that is not found gives nothing.
page :: Cache -> ByteString -> Limits -> Maybe (BlockRef -> ByteString) -> Maybe (Maybe BlockRef) -> IO [(ByteString, Double)]
page _ _ _ _ Nothing = return []
page c key limits _ (Just Nothing) =
  run c $
    R.zrangeWithscores
      key
      (fromIntegral limits.offset)
      (if limits.limit == 0 then -1 else fromIntegral limits.offset + fromIntegral limits.limit - 1)
page c key limits from (Just (Just ref)) = do
  let score = blockRefScore ref
  skip <- case from of
    Nothing -> return 0
    Just bytes -> do
      ties <- run c (R.zrangebyscore key score score)
      return (length (filter (< bytes ref) ties))
  run c $
    R.zrangebyscoreWithscoresLimit
      key
      score
      (1 / 0)
      (fromIntegral limits.offset + fromIntegral skip)
      (if limits.limit == 0 then -1 else fromIntegral limits.limit)

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
-- used addresses. It is loaded in the background; the caller keeps the
-- answer it has.
cacheRequest :: Cache -> XPubSpec -> [XPubBal] -> IO ()
cacheRequest c x bals =
  when (length (filter (not . nullBalance . (.balance)) bals) >= c.minAddrs) $
    push c (JobCache x)

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

-- | Reload an xpub from the database and replace its entry in Redis, unless
-- another store holds its lock or Redis knows more than this store (see
-- 'Reload'). The transactions given are the ones that changed it: their
-- bodies are written again even if Redis has them.
--
-- A store never writes data that knows less than what is in Redis, so stores
-- that see the chain and the mempool at slightly different times do not undo
-- each other's work. Every body that the new entry refers to is in Redis
-- before the entry is: the entry and the missing bodies are written in one
-- Lua script, which writes nothing if a body is missing, so a store that is
-- behind never finds a cached transaction that it cannot serve.
cacheRefresh :: Cache -> XPubSpec -> HashSet TxHash -> IO Reload
cacheRefresh c x changed =
  fromMaybe Locked <$> withLock c x (timed (go (3 :: Int) []))
  where
    def' = Limits {limit = 0, offset = 0, start = Nothing}
    go tries extra = do
      (stamp, old) <- run c $ do
        h <- R.hget (xkey c x "meta") "height"
        o <- R.zrangeWithscores (xkey c x "txs") 0 (-1)
        return ((,) <$> h <*> o)
      height <- bestHeight c
      let stamped = readMaybe . C8.unpack =<< stamp
      if not (mayReplace stamped height False)
        then behind
        else do
          bals <- Q.xPubBals c.db x
          txs <- Q.xPubTxs c.db bals def'
          let new = HashSet.fromList (map (encode . (.txid)) txs)
              gone = [h | (bs, _) <- old, not (HashSet.member bs new), Right h <- [decode bs]]
          -- a transaction that Redis has and this store has never seen
          known <- Q.getManyTxData c.db gone
          if not (mayReplace stamped height (any isNothing known))
            then behind
            else do
              utxos <- Q.xPubUnspents c.db bals def'
              present <- run c (sequence <$> mapM (R.exists . txKey c . (.txid)) txs)
              now <- nowMs
              let scores = HashMap.fromList old
                  wanted =
                    nub $
                      extra
                        <> [ t.txid
                           | (t, p) <- zip txs present,
                             not p
                               || HashMap.lookup (encode t.txid) scores /= Just (blockRefScore t.block)
                               || t.txid `HashSet.member` changed
                           ]
              bodies <- catMaybes <$> Q.getManyTxData c.db wanted
              let args =
                    [c.prefix, xpubId x, num height, num now, encode x, encode bals]
                      <> counted [encode b.balance.address | b <- bals]
                      <> counted2 [(encode t.txid, num' (blockRefScore t.block)) | t <- txs]
                      <> counted2 [(encode u.unspent, num' (blockRefScore u.unspent.block)) | u <- utxos]
                      <> bodyArgs bodies
              run c (R.eval commitScript [] args) >>= \case
                ["ok"] -> do
                  void (cacheEvict c c.maxXPubs)
                  count c (.reloads) 1
                  atomicModifyIORef' c.counters (\k -> (k {reloads = k.reloads + 1} :: CacheCounters, ()))
                  return Reloaded
                ("missing" : missing)
                  | tries > 0 -> go (tries - 1) (rights (map decode missing))
                _ -> behind
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
  forM_ (HashMap.toList held) $ \(x, (ts, ks)) ->
    afterReload <$> reload x ts >>= \case
      Finish -> finishClaims c ks
      Release -> releaseClaims c ks
      Hold -> extendClaims c ks >> later c (JobHeld x ts ks)
  forM_ (nub [x | JobCache x <- js, not (HashMap.member x held)]) $ \x ->
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
    StoreMempoolNew t -> ("t+:" <> encode t,) . maybeToList <$> Q.getTxData c.db t
    StoreMempoolDelete t -> ("t-:" <> encode t,) . maybeToList <$> Q.getTxData c.db t
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
      catMaybes <$> Q.getManyTxData c.db (bd.txs <> reverted)
  where
    -- blocks that are no longer in the main chain, from the old best block
    walk p =
      Q.getBlock c.db p >>= \case
        Just b | not b.main -> (b.txs <>) <$> walk b.header.prev
        _ -> return []

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
  let byId = HashMap.fromListWith (<>) [(i, ts) | (a, is) <- zip addrs ids, i <- is, Just ts <- [HashMap.lookup a byAddr]]
  specs <- run c (sequence <$> mapM (\i -> R.hget (c.prefix <> "x:" <> i <> ":meta") "spec") (HashMap.keys byId))
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
    void (run c (R.eval spendsScript [] (c.prefix : num (length quads) : concat quads) :: Redis (Either Reply Integer)))
  where
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

-- | Lua that removes everything of the xpub with the id in @id@ from the
-- cache, releasing the transactions it refers to. Expects @p@ (the prefix)
-- and @id@ to be defined.
cleanupLua :: ByteString
cleanupLua =
  "local xk = p .. 'x:' .. id .. ':'\n\
  \for _, a in ipairs(redis.call('SMEMBERS', xk .. 'addrs')) do\n\
  \  redis.call('SREM', p .. 'a:' .. a, id)\n\
  \end\n\
  \for _, t in ipairs(redis.call('ZRANGE', xk .. 'txs', 0, -1)) do\n\
  \  local rk = p .. 'txref:' .. t\n\
  \  redis.call('SREM', rk, id)\n\
  \  if redis.call('SCARD', rk) == 0 then\n\
  \    redis.call('DEL', rk, p .. 'tx:' .. t, p .. 'sp:' .. t)\n\
  \  end\n\
  \end\n\
  \local existed = redis.call('DEL', xk .. 'meta', xk .. 'bal', xk .. 'txs', xk .. 'utxo', xk .. 'addrs')\n\
  \redis.call('ZREM', p .. 'lru', id)\n"

-- | Delete an xpub. Arguments: prefix, id. Returns 1 if it was cached.
deleteScript :: ByteString
deleteScript =
  "local p = ARGV[1]\nlocal id = ARGV[2]\n"
    <> cleanupLua
    <> "if existed > 0 then return 1 else return 0 end\n"

-- | Evict the least recently used xpubs past a number. Arguments: prefix,
-- maximum. Returns how many were evicted.
evictScript :: ByteString
evictScript =
  "local p = ARGV[1]\nlocal max = tonumber(ARGV[2])\nlocal n = 0\n\
  \while redis.call('ZCARD', p .. 'lru') > max do\n\
  \  local id = redis.call('ZRANGE', p .. 'lru', 0, 0)[1]\n"
    <> cleanupLua
    <> "  n = n + 1\nend\nreturn n\n"

-- | Replace the entry of an xpub, unless the stored one is stamped with a
-- higher block. Arguments: prefix, id, height, time of use, serialized xpub,
-- balances; the number of addresses and the addresses; the number of
-- transactions and the hash and score of each; the number of unspent outputs
-- and the output and score of each; the number of bodies and the hash and
-- body of each; the number of spenders and the hash, output index and spender
-- of each. Returns @{\"stale\"}@ if the stored entry has a higher block,
-- @{\"missing\", ...}@ with the hashes of transactions that have no body in
-- Redis nor in the arguments, without writing anything, or @{\"ok\"}@.
commitScript :: ByteString
commitScript =
  "local p = ARGV[1]\nlocal id = ARGV[2]\n\
  \local h = tonumber(ARGV[3])\n\
  \local xk = p .. 'x:' .. id .. ':'\n\
  \local meta = xk .. 'meta'\n\
  \local oh = redis.call('HGET', meta, 'height')\n\
  \if oh and h < tonumber(oh) then return {'stale'} end\n\
  \local i = 7\n\
  \local na = tonumber(ARGV[i]); local ai = i + 1; i = ai + na\n\
  \local nt = tonumber(ARGV[i]); local ti = i + 1; i = ti + 2 * nt\n\
  \local nu = tonumber(ARGV[i]); local ui = i + 1; i = ui + 2 * nu\n\
  \local nb = tonumber(ARGV[i]); local bi = i + 1; i = bi + 2 * nb\n\
  \local ns = tonumber(ARGV[i]); local si = i + 1\n\
  \local given = {}\n\
  \for j = 0, nb - 1 do given[ARGV[bi + 2 * j]] = true end\n\
  \local missing = {'missing'}\n\
  \for j = 0, nt - 1 do\n\
  \  local t = ARGV[ti + 2 * j]\n\
  \  if not given[t] and redis.call('EXISTS', p .. 'tx:' .. t) == 0 then\n\
  \    table.insert(missing, t)\n\
  \  end\n\
  \end\n\
  \if #missing > 1 then return missing end\n\
  \local addrs = {}\n\
  \for j = 0, na - 1 do addrs[ARGV[ai + j]] = true end\n\
  \for _, a in ipairs(redis.call('SMEMBERS', xk .. 'addrs')) do\n\
  \  if not addrs[a] then redis.call('SREM', p .. 'a:' .. a, id) end\n\
  \end\n\
  \redis.call('DEL', xk .. 'addrs')\n\
  \for a, _ in pairs(addrs) do\n\
  \  redis.call('SADD', p .. 'a:' .. a, id)\n\
  \  redis.call('SADD', xk .. 'addrs', a)\n\
  \end\n\
  \local txs = {}\n\
  \for j = 0, nt - 1 do txs[ARGV[ti + 2 * j]] = true end\n\
  \for _, t in ipairs(redis.call('ZRANGE', xk .. 'txs', 0, -1)) do\n\
  \  if not txs[t] then\n\
  \    local rk = p .. 'txref:' .. t\n\
  \    redis.call('SREM', rk, id)\n\
  \    if redis.call('SCARD', rk) == 0 then\n\
  \      redis.call('DEL', rk, p .. 'tx:' .. t, p .. 'sp:' .. t)\n\
  \    end\n\
  \  end\n\
  \end\n\
  \redis.call('DEL', xk .. 'txs')\n\
  \for j = 0, nt - 1 do\n\
  \  local t = ARGV[ti + 2 * j]\n\
  \  redis.call('ZADD', xk .. 'txs', ARGV[ti + 2 * j + 1], t)\n\
  \  redis.call('SADD', p .. 'txref:' .. t, id)\n\
  \end\n\
  \redis.call('DEL', xk .. 'utxo')\n\
  \for j = 0, nu - 1 do\n\
  \  redis.call('ZADD', xk .. 'utxo', ARGV[ui + 2 * j + 1], ARGV[ui + 2 * j])\n\
  \end\n\
  \for j = 0, nb - 1 do\n\
  \  local t = ARGV[bi + 2 * j]\n\
  \  redis.call('SET', p .. 'tx:' .. t, ARGV[bi + 2 * j + 1])\n\
  \  redis.call('DEL', p .. 'sp:' .. t)\n\
  \end\n\
  \for j = 0, ns - 1 do\n\
  \  local k = si + 3 * j\n\
  \  if given[ARGV[k]] then\n\
  \    redis.call('HSET', p .. 'sp:' .. ARGV[k], ARGV[k + 1], ARGV[k + 2])\n\
  \  end\n\
  \end\n\
  \redis.call('SET', xk .. 'bal', ARGV[6])\n\
  \redis.call('HSET', meta, 'spec', ARGV[5], 'height', ARGV[3], 'ver', '2')\n\
  \redis.call('ZADD', p .. 'lru', ARGV[4], id)\n\
  \return {'ok'}\n"

-- | Record or forget spenders of outputs of cached transactions. Arguments:
-- prefix, number of entries, then for each: hash of the transaction that
-- created the output, output index, spender (empty to forget) and hash of
-- the spending transaction. Only transactions whose body is cached are
-- touched, and a spender is only forgotten if it is still the one given.
spendsScript :: ByteString
spendsScript =
  "local p = ARGV[1]\nlocal n = tonumber(ARGV[2])\nlocal i = 3\n\
  \for j = 1, n do\n\
  \  local t = ARGV[i]\n\
  \  if redis.call('EXISTS', p .. 'tx:' .. t) == 1 then\n\
  \    local sk = p .. 'sp:' .. t\n\
  \    if ARGV[i + 2] ~= '' then\n\
  \      redis.call('HSET', sk, ARGV[i + 1], ARGV[i + 2])\n\
  \    else\n\
  \      local cur = redis.call('HGET', sk, ARGV[i + 1])\n\
  \      if cur and string.sub(cur, 1, 32) == ARGV[i + 3] then\n\
  \        redis.call('HDEL', sk, ARGV[i + 1])\n\
  \      end\n\
  \    end\n\
  \  end\n\
  \  i = i + 4\n\
  \end\n\
  \return 1\n"

-- | Release a lock if it still holds the given token. Keys: lock. Arguments:
-- token.
unlockScript :: ByteString
unlockScript =
  "if redis.call('GET', KEYS[1]) == ARGV[1] then\n\
  \  return redis.call('DEL', KEYS[1])\n\
  \end\n\
  \return 0\n"
