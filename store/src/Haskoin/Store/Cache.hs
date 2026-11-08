{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NoFieldSelectors #-}
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
-- Stores index the same chain at slightly different times. To keep a store
-- that is behind from replacing newer data, every entry carries a stamp: the
-- height of the best block of the database it was read from, and the time it
-- was read. An entry is only replaced by one with a higher height, or the same
-- height and a later time. Every change that involves more than one key runs
-- as a Lua script, so readers never see half of an update, and a lock per xpub
-- keeps two stores from reloading the same xpub at once. Stores that answer
-- from Redis serve the same data; stores whose databases disagree can still
-- serve different answers for xpubs that are not cached.
--
-- = Keys
--
-- Every key starts with @hs2:\<network\>:@, so this cache can share a Redis with
-- the cache of older versions of the store, whose keys have no such prefix,
-- and with stores of other networks.
--
-- * @x:\<id\>:meta@: hash with the serialized xpub (@spec@), the stamp
--   (@height@, @time@) and the format version (@ver@). @\<id\>@ is the
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

    -- * Changing
    cacheRequest,
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
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.IntMap.Strict qualified as IntMap
import Data.List (nub)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Maybe (catMaybes, fromMaybe, isJust)
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
import Haskoin.Store.Common (Limits (..), Start (..), StoreEvent (..), loopForever)
import Haskoin.Store.Data
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader (DatabaseReader (..))
import NQE (Inbox, Publisher, receive, withSubscription)
import UnliftIO
  ( TVar,
    atomically,
    checkSTM,
    handleAny,
    link,
    modifyTVar',
    newTVarIO,
    readTVar,
    tryAny,
    withAsync,
    writeTVar,
  )
import System.Random (randomIO)
import System.Metrics.StatsD
  ( StatCounter,
    StatTiming,
    Stats,
    addTiming,
    incrementCounter,
    newStatCounter,
    newStatTiming,
  )
import UnliftIO.Concurrent (threadDelay)

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
    -- | xpubs to reload, with the transactions that made them change
    pending :: !(TVar (HashMap XPubSpec (HashSet TxHash)))
  }

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
  pending <- newTVarIO HashMap.empty
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
            pending = pending
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
    enqueue c [(x, HashSet.empty)]

-- | Reload xpubs because these transactions changed them.
enqueue :: Cache -> [(XPubSpec, HashSet TxHash)] -> IO ()
enqueue c xs =
  atomically . modifyTVar' c.pending $ \m ->
    foldr (\(x, ts) -> HashMap.insertWith (<>) x ts) m xs

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
-- another store holds its lock or has newer data. The transactions given are
-- the ones that changed it: their bodies are written again even if the xpub
-- had them already. Returns whether the entry was replaced.
cacheRefresh :: Cache -> XPubSpec -> HashSet TxHash -> IO Bool
cacheRefresh c x changed =
  withLock c x $ timed $ do
    old <- HashMap.fromList <$> run c (R.zrangeWithscores (xkey c x "txs") 0 (-1))
    height <- bestHeight c
    bals <- Q.xPubBals c.db x
    txs <- Q.xPubTxs c.db bals def'
    utxos <- Q.xPubUnspents c.db bals def'
    time <- nowMs
    let wanted =
          [ t.txid
          | t <- txs,
            HashMap.lookup (encode t.txid) old /= Just (blockRefScore t.block)
              || t.txid `HashSet.member` changed
          ]
    bodies <- catMaybes <$> Q.getManyTxData c.db wanted
    let args =
          [c.prefix, xpubId x, num height, num time, num time, encode x, encode bals]
            <> counted [encode b.balance.address | b <- bals]
            <> counted2 [(encode t.txid, num' (blockRefScore t.block)) | t <- txs]
            <> counted2 [(encode u.unspent, num' (blockRefScore u.unspent.block)) | u <- utxos]
            <> bodyArgs bodies
    r <- run c (R.eval commitScript [] args)
    case r of
      ("ok" : missing) -> do
        unless (null missing) $ do
          more <- catMaybes <$> Q.getManyTxData c.db (rights (map decode missing))
          void (run c (R.eval fillScript [] (c.prefix : bodyArgs more) :: Redis (Either Reply Integer)))
        void (cacheEvict c c.maxXPubs)
        count c (.reloads) 1
        return True
      _ -> count c (.stale) 1 >> return False
  where
    timed f = do
      start <- nowMs
      r <- f
      end <- nowMs
      forM_ c.metrics $ \m -> addTiming m.reloadTime (fromInteger (end - start))
      return r
    def' = Limits {limit = 0, offset = 0, start = Nothing}

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

-- | Run an action holding the lock of an xpub. 'False' if another store holds
-- it. The lock expires after two minutes in case its holder dies.
withLock :: Cache -> XPubSpec -> IO Bool -> IO Bool
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
      return r
    _ -> return False

-------------------------------------------------------------------------------
-- Keeping up with the store
-------------------------------------------------------------------------------

-- | Reload the xpubs waiting to be reloaded, forever. An xpub whose lock is
-- taken, or that cannot be reloaded because the store is not in sync, waits
-- a second and is tried again.
worker :: Cache -> IO ()
worker c = loopForever $ do
  batch <- atomically $ do
    m <- readTVar c.pending
    checkSTM (not (HashMap.null m))
    writeTVar c.pending HashMap.empty
    return m
  synced <- inSync c
  failed <-
    if not synced
      then return (HashMap.toList batch)
      else fmap catMaybes . forM (HashMap.toList batch) $ \(x, ts) ->
        tryAny (cacheRefresh c x ts) >>= \case
          Right True -> return Nothing
          Right False -> return (Just (x, ts))
          Left e -> do
            log' c LevelError ("Could not cache xpub: " <> cs (show e))
            return (Just (x, ts))
  unless (null failed) $ do
    threadDelay 1000000
    enqueue c failed

-- | Turn the events of the store into xpubs to reload, and keep the spenders
-- of cached transactions up to date.
listener :: Cache -> Inbox StoreEvent -> IO ()
listener c events = do
  lastBest <- newTVarIO Nothing
  loopForever . handleAny (\e -> log' c LevelError ("Cache listener: " <> cs (show e))) $ do
    e <- receive events
    tds <- case e of
      StoreBestBlock h -> do
        prev <- atomically (readTVar lastBest)
        atomically (writeTVar lastBest (Just h))
        blockTxs c prev h
      StoreMempoolNew t -> maybe [] pure <$> Q.getTxData c.db t
      StoreMempoolDelete t -> maybe [] pure <$> Q.getTxData c.db t
      _ -> return []
    unless (null tds) $ do
      updateSpenders c tds
      affected <- affectedXPubs c tds
      enqueue c affected

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

-- | Lua that writes bodies and spenders from the arguments at index @i@: the
-- number of bodies, then hash and body of each, then the number of spenders,
-- then hash, output index and spender of each. Bodies are only written for
-- transactions that some cached xpub refers to, unless @force@ is true.
bodiesLua :: ByteString
bodiesLua =
  "local nb = tonumber(ARGV[i]); i = i + 1\n\
  \local written = {}\n\
  \for j = 1, nb do\n\
  \  local t = ARGV[i]\n\
  \  if force or redis.call('SCARD', p .. 'txref:' .. t) > 0 then\n\
  \    redis.call('SET', p .. 'tx:' .. t, ARGV[i + 1])\n\
  \    redis.call('DEL', p .. 'sp:' .. t)\n\
  \    written[t] = true\n\
  \  end\n\
  \  i = i + 2\n\
  \end\n\
  \local ns = tonumber(ARGV[i]); i = i + 1\n\
  \for j = 1, ns do\n\
  \  if written[ARGV[i]] then\n\
  \    redis.call('HSET', p .. 'sp:' .. ARGV[i], ARGV[i + 1], ARGV[i + 2])\n\
  \  end\n\
  \  i = i + 3\n\
  \end\n"

-- | Write bodies that a commit found missing. Arguments: prefix, then the
-- bodies and spenders as in 'bodiesLua'.
fillScript :: ByteString
fillScript =
  "local p = ARGV[1]\nlocal i = 2\nlocal force = false\n"
    <> bodiesLua
    <> "return 1\n"

-- | Replace the entry of an xpub, unless the stored one has a newer stamp.
-- Arguments: prefix, id, height, time, time of use, serialized xpub,
-- balances; the number of addresses and the addresses; the number of
-- transactions and the hash and score of each; the number of unspent outputs
-- and the output and score of each; the bodies and spenders as in
-- 'bodiesLua'. Returns @{\"stale\"}@ if the stored entry is newer, or
-- @{\"ok\", ...}@ with the hashes of transactions whose body Redis still lacks.
commitScript :: ByteString
commitScript =
  "local p = ARGV[1]\nlocal id = ARGV[2]\n\
  \local h = tonumber(ARGV[3])\nlocal t = tonumber(ARGV[4])\n\
  \local xk = p .. 'x:' .. id .. ':'\n\
  \local meta = xk .. 'meta'\n\
  \local oh = redis.call('HGET', meta, 'height')\n\
  \if oh then\n\
  \  local ot = tonumber(redis.call('HGET', meta, 'time'))\n\
  \  oh = tonumber(oh)\n\
  \  if h < oh or (h == oh and t < ot) then return {'stale'} end\n\
  \end\n\
  \local i = 8\n\
  \local na = tonumber(ARGV[i]); i = i + 1\n\
  \local addrs = {}\n\
  \for j = 1, na do addrs[ARGV[i]] = true; i = i + 1 end\n\
  \for _, a in ipairs(redis.call('SMEMBERS', xk .. 'addrs')) do\n\
  \  if not addrs[a] then redis.call('SREM', p .. 'a:' .. a, id) end\n\
  \end\n\
  \redis.call('DEL', xk .. 'addrs')\n\
  \for a, _ in pairs(addrs) do\n\
  \  redis.call('SADD', p .. 'a:' .. a, id)\n\
  \  redis.call('SADD', xk .. 'addrs', a)\n\
  \end\n\
  \local nt = tonumber(ARGV[i]); i = i + 1\n\
  \local txs = {}\n\
  \local first = i\n\
  \for j = 1, nt do txs[ARGV[i]] = true; i = i + 2 end\n\
  \for _, tx in ipairs(redis.call('ZRANGE', xk .. 'txs', 0, -1)) do\n\
  \  if not txs[tx] then\n\
  \    local rk = p .. 'txref:' .. tx\n\
  \    redis.call('SREM', rk, id)\n\
  \    if redis.call('SCARD', rk) == 0 then\n\
  \      redis.call('DEL', rk, p .. 'tx:' .. tx, p .. 'sp:' .. tx)\n\
  \    end\n\
  \  end\n\
  \end\n\
  \redis.call('DEL', xk .. 'txs')\n\
  \local k = first\n\
  \for j = 1, nt do\n\
  \  redis.call('ZADD', xk .. 'txs', ARGV[k + 1], ARGV[k])\n\
  \  redis.call('SADD', p .. 'txref:' .. ARGV[k], id)\n\
  \  k = k + 2\n\
  \end\n\
  \local nu = tonumber(ARGV[i]); i = i + 1\n\
  \redis.call('DEL', xk .. 'utxo')\n\
  \for j = 1, nu do\n\
  \  redis.call('ZADD', xk .. 'utxo', ARGV[i + 1], ARGV[i])\n\
  \  i = i + 2\n\
  \end\n\
  \local force = false\n"
    <> bodiesLua
    <> "redis.call('SET', xk .. 'bal', ARGV[7])\n\
       \redis.call('HSET', meta, 'spec', ARGV[6], 'height', ARGV[3], 'time', ARGV[4], 'ver', '1')\n\
       \redis.call('ZADD', p .. 'lru', ARGV[5], id)\n\
       \local missing = {'ok'}\n\
       \k = first\n\
       \for j = 1, nt do\n\
       \  if redis.call('EXISTS', p .. 'tx:' .. ARGV[k]) == 0 then\n\
       \    table.insert(missing, ARGV[k])\n\
       \  end\n\
       \  k = k + 2\n\
       \end\n\
       \return missing\n"

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
