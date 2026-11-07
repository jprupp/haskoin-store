{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE NoFieldSelectors #-}

-- |
-- Module      : Haskoin.Store.Cache
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Redis cache for extended public keys (xpubs).
--
-- Answering a query about an xpub from the database means deriving its
-- addresses and reading the index of every one of them, which is slow for
-- wallets with many used addresses. When such an xpub is queried, its balances,
-- transactions and unspent outputs are copied to Redis, and later queries are
-- answered from there. Only xpubs with at least 'minAddrs' used addresses are
-- cached, and only while the store is in sync.
--
-- A cache writer process ('cacheWriter') keeps cached xpubs up to date as new
-- blocks and mempool transactions arrive. When Redis holds more than 'maxKeys'
-- keys, the xpubs used least recently are evicted.
--
-- = Redis keys
--
-- * @b\<xpub\>@: hash of derivation path to balance of each address.
-- * @t\<xpub\>@: sorted set of transactions, scored by 'blockRefScore'.
-- * @u\<xpub\>@: sorted set of unspent outputs, scored by 'blockRefScore'.
-- * @a\<address\>@: xpub and derivation path of a cached address.
-- * @max@: sorted set of cached xpubs, scored by time of last use.
-- * @mempool@: sorted set of mempool transactions.
-- * @head@: last block that the cache has processed.
-- * @xlock-\<xpub\>@, @lock@, @i\<xpub\>@: locks.
--
-- Here @\<xpub\>@ and @\<address\>@ stand for their binary serialization.
-- = Why a cache, and why Redis
--
-- Most xpubs are small and fast to answer from RocksDB. A few, such as the
-- wallets of exchanges and payment processors, have thousands of used addresses
-- and are queried constantly. Answering them from the database means deriving and
-- reading every address each time. Keeping their balances, transactions and
-- unspent outputs ready in sorted sets turns those queries into a single range
-- read.
--
-- Redis fits because sorted sets are exactly the shape of these queries, and
-- because several store instances can share one cache. Only large xpubs are
-- cached ('minAddrs'), because small ones gain nothing, and xpubs not used for a
-- while are evicted, so the cache holds the working set rather than everything
-- ever queried.
--
-- Locks keep two store instances, or two requests, from building the cache of
-- the same xpub at the same time. A request that cannot get a lock in time is
-- answered from the database rather than made to wait.
module Haskoin.Store.Cache
  ( CacheConfig (..),
    CacheMetrics,
    CacheT,
    CacheError (..),
    newCacheMetrics,
    withCache,
    connectRedis,
    blockRefScore,
    scoreBlockRef,
    CacheWriter,
    CacheWriterInbox,
    cacheNewBlock,
    cacheNewTx,
    cacheSyncMempool,
    cacheWriter,
    cacheDelXPubs,
    isInCache,
  )
where

import Control.DeepSeq (NFData)
import Control.Monad (forM, forM_, unless, void, when)
import Control.Monad.Reader (ReaderT (..), asks)
import Control.Monad.Trans (lift)
import Data.Bits (shift, (.&.), (.|.))
import Data.ByteString (ByteString)
import Data.Default (def)
import Data.Either (fromRight, isRight, rights)
import Data.Functor ((<&>))
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet qualified as HashSet
import Data.IntMap.Strict qualified as I
import Data.List (sort)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Map.Strict qualified as Map
import Data.Maybe
  ( catMaybes,
    mapMaybe,
  )
import Data.Serialize (Serialize, decode, encode)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word32, Word64)
import Database.Redis
  ( Connection,
    Redis,
    RedisCtx,
    Reply,
    checkedConnect,
    defaultConnectInfo,
    hgetall,
    parseConnectInfo,
    zadd,
    zrangeWithscores,
    zrangebyscoreWithscoresLimit,
    zrem,
  )
import Database.Redis qualified as Redis
import GHC.Generics (Generic)
import Haskoin
  ( Address,
    BlockHash,
    BlockNode (..),
    Ctx,
    DerivPathI (..),
    KeyIndex,
    Network,
    OutPoint (..),
    Tx (..),
    TxHash,
    TxIn (..),
    TxOut (..),
    XPubKey,
    blockHashToHex,
    derivePubPath,
    eitherToMaybe,
    headerHash,
    pathToList,
    scriptToAddressBS,
    txHash,
    txHashToHex,
    xPubAddr,
    xPubCompatWitnessAddr,
    xPubExport,
    xPubWitnessAddr,
  )
import Haskoin.Node
  ( Chain,
    LogLevel (..),
    Logger,
    chainGetBest,
    chainGetBlock,
    chainGetParents,
    chainGetSplitBlock,
    logAt,
  )
import Haskoin.Store.Common
import Haskoin.Store.Data
import Haskoin.Store.Database.Reader (DatabaseReader, withDB)
import NQE
  ( Inbox,
    Listen,
    Mailbox,
    mailboxEmptySTM,
    query,
    receive,
    receiveSTM,
    requeueSTM,
    send,
  )
import System.Metrics.StatsD
import UnliftIO
  ( Exception,
    MonadIO,
    MonadUnliftIO,
    atomically,
    bracket,
    liftIO,
    throwIO,
    withAsync,
  )
import UnliftIO.Concurrent (threadDelay)

-- | Run a Redis command. Throws 'RedisError' if Redis returns an error.
runRedis :: (MonadIO m) => Redis (Either Reply a) -> CacheX m a
runRedis action = do
  conn <- asks (.redis)
  logger <- asks (.logger)
  withRedis logger conn action

-- | Run a Redis command on a connection. Logs and throws 'RedisError' if Redis returns an error.
withRedis ::
  (MonadIO m) => Logger -> Connection -> Redis (Either Reply a) -> m a
withRedis logger conn action =
  liftIO (Redis.runRedis conn action) >>= \case
    Right x -> return x
    Left e -> do
      logAt LevelError logger $ "Got error from Redis: " <> cs (show e)
      throwIO (RedisError e)

-- | Cache configuration.
data CacheConfig = CacheConfig
  { -- | connection to Redis
    redis :: !Connection,
    -- | cache xpubs with at least this many used addresses
    minAddrs :: !Int,
    -- | evict xpubs when Redis holds more keys than this
    maxKeys :: !Integer,
    -- | header chain, used to know whether the store is in sync
    chain :: !Chain,
    -- | StatsD metrics, if enabled
    metrics :: !(Maybe CacheMetrics),
    -- | log messages here
    logger :: !Logger
  }

-- | StatsD metrics of the cache.
data CacheMetrics = CacheMetrics
  { -- | xpub balance queries answered from the cache (@cache.hits@)
    hits :: !StatCounter,
    -- | xpub balance queries not in the cache (@cache.misses@)
    misses :: !StatCounter,
    -- | locks acquired (@cache.lock_acquired@)
    lockAcquired :: !StatCounter,
    -- | locks released (@cache.lock_released@)
    lockReleased :: !StatCounter,
    -- | locks that could not be acquired (@cache.lock_failed@)
    lockFailed :: !StatCounter,
    -- | xpub balances served (@cache.xpub_balances@)
    xBalances :: !StatCounter,
    -- | xpub unspent outputs served (@cache.xpub_unspents@)
    xUnspents :: !StatCounter,
    -- | xpub transactions served (@cache.xpub_txs@)
    xTxs :: !StatCounter,
    -- | xpub transaction counts served (@cache.xpub_tx_count@)
    xTxCount :: !StatCounter,
    -- | milliseconds to cache an xpub (@cache.index@)
    indexTime :: !StatTiming,
    -- | height of the last block processed by the cache (@cache.height@)
    height :: !StatGauge
  }

-- | Register the cache metrics with a StatsD client.
newCacheMetrics ::
  (MonadIO m) =>
  Logger ->
  Stats ->
  Connection ->
  DatabaseReader ->
  m CacheMetrics
newCacheMetrics logger s r db = do
  h <- redisBest
  hits <- c "cache.hits"
  misses <- c "cache.misses"
  lockAcquired <- c "cache.lock_acquired"
  lockReleased <- c "cache.lock_released"
  lockFailed <- c "cache.lock_failed"
  indexTime <- d "cache.index"
  xBalances <- c "cache.xpub_balances"
  xUnspents <- c "cache.xpub_unspents"
  xTxs <- c "cache.xpub_txs"
  xTxCount <- c "cache.xpub_tx_count"
  height <- newStatGauge s "cache.height" h
  return CacheMetrics {..}
  where
    redisBest =
      maybe dbBest dbBlock =<< withRedis logger r redisGetHead
    dbBlock h =
      maybe 0 (fromIntegral . (.height)) <$> withDB db (getBlock h)
    dbBest =
      maybe (return 0) dbBlock =<< withDB db getBestBlock
    c x = newStatCounter s x 10
    d x = newStatTiming s x 10

-- | Record how long an action takes in a timing metric.
withTiming ::
  (MonadUnliftIO m) =>
  (CacheMetrics -> StatTiming) ->
  CacheX m a ->
  CacheX m a
withTiming df go =
  asks (.metrics) >>= \case
    Nothing -> go
    Just m ->
      bracket
        time
        (end m)
        (const go)
  where
    time = round . (* 1000) <$> liftIO getPOSIXTime
    end m t1 = time >>= addTiming (df m) . subtract t1

-- | Run an action with the metrics, if they are enabled.
withMetrics ::
  (MonadIO m) => (CacheMetrics -> CacheX m a) -> CacheX m ()
withMetrics go = asks (.metrics) >>= mapM_ go

-- | Monad transformer that may use the cache. With 'Nothing', every query goes
-- to the underlying store. Run it with 'withCache'.
type CacheT = ReaderT (Maybe CacheConfig)

-- | Monad transformer that uses the cache.
type CacheX = ReaderT CacheConfig

-- | Log a message with the logger of the cache.
logDebug, logInfo, logWarn, logError :: (MonadIO m) => Text -> CacheX m ()
logDebug = cacheLog LevelDebug
logInfo = cacheLog LevelInfo
logWarn = cacheLog LevelWarn
logError = cacheLog LevelError

-- | Log a message at a level with the logger of the cache.
cacheLog :: (MonadIO m) => LogLevel -> Text -> CacheX m ()
cacheLog l msg = asks (.logger) >>= \lg -> logAt l lg msg

-- | Errors of the cache.
data CacheError
  = -- | Redis returned an error
    RedisError Reply
  | -- | a Redis transaction failed
    RedisTxError !String
  | -- | the cache is inconsistent with the database
    LogicError !String
  deriving (Show, Eq, Generic, NFData, Exception)

-- | Connect to Redis at a URL such as @redis:\/\/localhost:6379@, or with the
-- default settings if the URL is empty. Calls 'error' if the URL is invalid.
connectRedis :: (MonadIO m) => String -> m Connection
connectRedis redisurl = do
  conninfo <-
    if null redisurl
      then return defaultConnectInfo
      else case parseConnectInfo redisurl of
        Left e -> error e
        Right r -> return r
  liftIO (checkedConnect conninfo)

instance (StoreReadBase m) => StoreReadBase (CacheX m) where
  getCtx = lift getCtx
  getNetwork = lift getNetwork
  getBestBlock = lift getBestBlock
  getBlocksAtHeight = lift . getBlocksAtHeight
  getBlock = lift . getBlock
  getTxData = lift . getTxData
  getManyTxData = lift . getManyTxData
  getSpender = lift . getSpender
  getBalance = lift . getBalance
  getUnspent = lift . getUnspent
  getMempool = lift getMempool
  getMempoolPage = lift . getMempoolPage

instance (StoreReadBase m) => StoreReadBase (CacheT m) where
  getCtx = lift getCtx
  getNetwork = lift getNetwork
  getBestBlock = lift getBestBlock
  getBlocksAtHeight = lift . getBlocksAtHeight
  getBlock = lift . getBlock
  getTxData = lift . getTxData
  getManyTxData = lift . getManyTxData
  getSpender = lift . getSpender
  getBalance = lift . getBalance
  getUnspent = lift . getUnspent
  getMempool = lift getMempool
  getMempoolPage = lift . getMempoolPage

instance
  (MonadUnliftIO m, StoreReadExtra m) =>
  StoreReadExtra (CacheX m)
  where
  getBalances = lift . getBalances
  getAddressesTxs addrs = lift . getAddressesTxs addrs
  getAddressTxs addr = lift . getAddressTxs addr
  getAddressUnspents addr = lift . getAddressUnspents addr
  getAddressesUnspents addrs = lift . getAddressesUnspents addrs
  getMaxGap = lift getMaxGap
  getInitialGap = lift getInitialGap
  getNumTxData = lift . getNumTxData
  xPubTxs = getXPubTxs
  xPubTxCount = getXPubTxCount
  xPubBals = getXPubBalances
  xPubUnspents = getXPubUnspents

instance
  (MonadUnliftIO m, StoreReadExtra m) =>
  StoreReadExtra (CacheT m)
  where
  getBalances = lift . getBalances
  getAddressesTxs addrs = lift . getAddressesTxs addrs
  getAddressTxs addr = lift . getAddressTxs addr
  getAddressUnspents addr = lift . getAddressUnspents addr
  getAddressesUnspents addrs = lift . getAddressesUnspents addrs
  getMaxGap = lift getMaxGap
  getInitialGap = lift getInitialGap
  getNumTxData = lift . getNumTxData
  xPubBals xpub =
    ReaderT $ \case
      Nothing -> xPubBals xpub
      Just cfg -> runReaderT (xPubBals xpub) cfg
  xPubUnspents xpub xbals limits =
    ReaderT $ \case
      Nothing -> xPubUnspents xpub xbals limits
      Just cfg -> runReaderT (xPubUnspents xpub xbals limits) cfg
  xPubTxs xpub xbals limits =
    ReaderT $ \case
      Nothing -> xPubTxs xpub xbals limits
      Just cfg -> runReaderT (xPubTxs xpub xbals limits) cfg
  xPubTxCount xpub xbals =
    ReaderT $ \case
      Nothing -> xPubTxCount xpub xbals
      Just cfg -> runReaderT (xPubTxCount xpub xbals) cfg

-- | Run queries with the cache, or without it if 'Nothing'.
withCache :: Maybe CacheConfig -> CacheT m a -> m a
withCache s f = runReaderT f s

-- | Prefix of the keys of xpub balances.
balancesPfx :: ByteString
balancesPfx = "b"

-- | Prefix of the keys of xpub transactions.
txSetPfx :: ByteString
txSetPfx = "t"

-- | Prefix of the keys of xpub unspent outputs.
utxoPfx :: ByteString
utxoPfx = "u"

-- | Prefix of the keys that mark an xpub as being cached.
idxPfx :: ByteString
idxPfx = "i"

-- | Key of the lock held while caching an xpub.
xpubLock :: XPubSpec -> ByteString
xpubLock xpub = "xlock-" <> encode xpub

-- | Run a read of the cache entries of an xpub in the same round trip to
-- Redis as the check that the xpub is cached. Redis pipelines the two
-- commands, since the reply of the first is not read before the second is
-- sent. The read is 'Nothing' if the xpub is not cached; it then found no
-- entries, as the keys of an xpub are all deleted together.
whenXPubCached ::
  (MonadIO m) =>
  XPubSpec ->
  Redis (Either Reply a) ->
  CacheX m (Maybe a)
whenXPubCached xpub action = do
  (cached, x) <- runRedis $ do
    e <- redisIsXPubCached xpub
    r <- action
    return $ (,) <$> e <*> r
  return $ if cached then Just x else Nothing

-- | The score to start reading a sorted set of an xpub from, when it does
-- not need a database lookup.
startScore :: Maybe Start -> Maybe (Maybe Double)
startScore Nothing = Just Nothing
startScore (Just (AtBlock h)) = Just (Just (blockRefScore (BlockRef h maxBound)))
startScore (Just AtTx {}) = Nothing

-- | Transactions of an xpub, from the cache if it is cached, or from the store otherwise.
getXPubTxs ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  XPubSpec ->
  [XPubBal] ->
  Limits ->
  CacheX m [TxRef]
getXPubTxs xpub xbals limits =
  case startScore limits.start of
    Just score ->
      whenXPubCached xpub (get_txs score) >>= \case
        Just xs -> do
          touchKeys [xpub]
          hit (map (\(t, s) -> TxRef {txid = t, block = scoreBlockRef s}) xs)
        Nothing -> lift $ xPubTxs xpub xbals limits
    Nothing -> do
      cached <- isXPubCached xpub
      if cached
        then hit =<< cacheGetXPubTxs xpub limits
        else lift $ xPubTxs xpub xbals limits
  where
    get_txs score =
      getFromSortedSet
        (txSetPfx <> encode xpub)
        score
        limits.offset
        limits.limit
    hit txs = do
      withMetrics $ \s ->
        incrementCounter s.xTxs (length txs)
      return txs

-- | Number of transactions of an xpub, from the cache if it is cached, or from the store otherwise.
getXPubTxCount ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  XPubSpec ->
  [XPubBal] ->
  CacheX m Word32
getXPubTxCount xpub xbals =
  whenXPubCached xpub (redisGetXPubTxCount xpub) >>= \case
    Just count -> do
      withMetrics $ \s ->
        incrementCounter s.xTxCount 1
      touchKeys [xpub]
      return (fromInteger count)
    Nothing -> lift $ xPubTxCount xpub xbals

-- | Unspent outputs of an xpub, from the cache if it is cached, or from the store otherwise.
getXPubUnspents ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  XPubSpec ->
  [XPubBal] ->
  Limits ->
  CacheX m [XPubUnspent]
getXPubUnspents xpub xbals limits =
  case startScore limits.start of
    Just score ->
      whenXPubCached xpub (get_ops score) >>= \case
        Just xs -> do
          touchKeys [xpub]
          hit (map fst xs)
        Nothing -> lift $ xPubUnspents xpub xbals limits
    Nothing -> do
      cached <- isXPubCached xpub
      if cached
        then hit . map snd =<< cacheGetXPubUnspents xpub limits
        else lift $ xPubUnspents xpub xbals limits
  where
    get_ops score =
      getFromSortedSet
        (utxoPfx <> encode xpub)
        score
        limits.offset
        limits.limit
    hit ops = do
      uns <- catMaybes <$> lift (mapM getUnspent ops)
      net <- lift getNetwork
      ctx <- lift getCtx
      let f u =
            either
              (const Nothing)
              (\a -> Just (a, u))
              (scriptToAddressBS net ctx u.script)
          xm =
            let j x = (x.balance.address, x)
                k = (> 0) . (.balance.utxo)
             in HashMap.fromList $ map j $ filter k xbals
          g a = HashMap.lookup a xm
          h u x =
            XPubUnspent
              { unspent = u,
                path = x.path
              }
          us = mapMaybe f uns
          i a u = h u <$> g a
      withMetrics $ \s -> incrementCounter s.xUnspents (length us)
      return $ mapMaybe (uncurry i) us

-- | Balances of an xpub, from the cache if it is cached. Otherwise read them from
-- the store and cache the xpub if it has enough used addresses.
getXPubBalances ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  XPubSpec ->
  CacheX m [XPubBal]
getXPubBalances xpub =
  whenXPubCached xpub (redisGetXPubBalances xpub) >>= \case
    Just xbals -> do
      touchKeys [xpub]
      withMetrics $ \s -> do
        incrementCounter s.hits 1
        incrementCounter s.xBalances (length xbals)
      return xbals
    Nothing -> do
      withMetrics $ \s -> incrementCounter s.misses 1
      withRetryLock (xpubLock xpub) (load_and_cache xpub) >>= \case
        Just bals -> return bals
        Nothing -> do
          -- Another thread held the lock for too long: serve from the
          -- database without caching rather than wait any longer.
          logWarn "Could not acquire xpub lock, serving from database"
          lift $ xPubBals xpub
  where
    load_and_cache x = do
      bals <- lift $ xPubBals x
      newXPubC x bals
      return bals

-- | Whether an xpub is cached. Always 'False' without a cache.
isInCache :: (MonadIO m) => XPubSpec -> CacheT m Bool
isInCache xpub =
  ReaderT $ \case
    Nothing -> return False
    Just cfg -> runReaderT (isXPubCached xpub) cfg

-- | Whether an xpub is cached.
isXPubCached :: (MonadIO m) => XPubSpec -> CacheX m Bool
isXPubCached xpub = runRedis (redisIsXPubCached xpub)

-- | Redis command to check whether an xpub is cached.
redisIsXPubCached :: (RedisCtx m f) => XPubSpec -> m (f Bool)
redisIsXPubCached xpub = Redis.exists (balancesPfx <> encode xpub)

-- | Redis command to count the transactions of an xpub.
redisGetXPubTxCount :: (RedisCtx m f) => XPubSpec -> m (f Integer)
redisGetXPubTxCount xpub = Redis.zcard (txSetPfx <> encode xpub)

-- | Page of the cached transactions of an xpub.
cacheGetXPubTxs ::
  (StoreReadBase m, MonadIO m) =>
  XPubSpec ->
  Limits ->
  CacheX m [TxRef]
cacheGetXPubTxs xpub limits =
  case limits.start of
    Nothing ->
      go1 Nothing
    Just (AtTx th) ->
      lift (getTxData th) >>= \case
        Just TxData {block = b@BlockRef {}} ->
          go1 $ Just (blockRefScore b)
        _ ->
          go2 th
    Just (AtBlock h) ->
      go1 (Just (blockRefScore (BlockRef h maxBound)))
  where
    go1 score = do
      xs <-
        runRedis $
          getFromSortedSet
            (txSetPfx <> encode xpub)
            score
            limits.offset
            limits.limit
      touchKeys [xpub]
      return $ map (uncurry f) xs
    go2 hash = do
      xs <-
        runRedis $
          getFromSortedSet
            (txSetPfx <> encode xpub)
            Nothing
            0
            0
      touchKeys [xpub]
      let xs' =
            if any ((== hash) . fst) xs
              then dropWhile ((/= hash) . fst) xs
              else []
      return $
        map (uncurry f) $
          l $
            drop (fromIntegral limits.offset) xs'
    l =
      if limits.limit > 0
        then take (fromIntegral limits.limit)
        else id
    f t s = TxRef {txid = t, block = scoreBlockRef s}

-- | Page of the cached unspent outputs of an xpub, with their positions.
cacheGetXPubUnspents ::
  (StoreReadBase m, MonadIO m) =>
  XPubSpec ->
  Limits ->
  CacheX m [(BlockRef, OutPoint)]
cacheGetXPubUnspents xpub limits =
  case limits.start of
    Nothing ->
      go1 Nothing
    Just (AtTx th) ->
      lift (getTxData th) >>= \case
        Just TxData {block = b@BlockRef {}} ->
          go1 (Just (blockRefScore b))
        _ ->
          go2 th
    Just (AtBlock h) ->
      go1 (Just (blockRefScore (BlockRef h maxBound)))
  where
    go1 score = do
      xs <-
        runRedis $
          getFromSortedSet
            (utxoPfx <> encode xpub)
            score
            limits.offset
            limits.limit
      touchKeys [xpub]
      return $ map (uncurry f) xs
    go2 hash = do
      xs <-
        runRedis $
          getFromSortedSet
            (utxoPfx <> encode xpub)
            Nothing
            0
            0
      touchKeys [xpub]
      let xs' =
            if any ((== hash) . (.hash) . fst) xs
              then dropWhile ((/= hash) . (.hash) . fst) xs
              else []
      return $
        map (uncurry f) $
          l $
            drop (fromIntegral limits.offset) xs'
    l =
      if limits.limit > 0
        then take (fromIntegral limits.limit)
        else id
    f o s = (scoreBlockRef s, o)

-- | Redis command to read the cached balances of an xpub, sorted by path.
redisGetXPubBalances :: (Functor f, RedisCtx m f) => XPubSpec -> m (f [XPubBal])
redisGetXPubBalances xpub =
  fmap (sort . map (uncurry f)) <$> getAllFromMap (balancesPfx <> encode xpub)
  where
    f p b = XPubBal {path = p, balance = b}

-- | Score of a position in a Redis sorted set, such that sorting by score
-- ascending sorts from newest to oldest. Mempool transactions get negative
-- scores from their time, so they come first; confirmed ones get positive
-- scores that decrease with height and position.
--
-- One number has to order mempool and confirmed transactions in a single sorted
-- set, newest first, exactly as the database does. Floating point is what Redis
-- offers, so the height and position are packed into the 53 bits a double holds
-- exactly.
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

-- | Redis command to read members of a sorted set, decoded, with their scores,
-- from a minimum score if given, skipping an offset, up to a count (@0@ for
-- all).
getFromSortedSet ::
  (Applicative f, RedisCtx m f, Serialize a) =>
  ByteString ->
  Maybe Double ->
  Word32 ->
  Word32 ->
  m (f [(a, Double)])
getFromSortedSet key Nothing off 0 = do
  xs <- zrangeWithscores key (fromIntegral off) (-1)
  return $ do
    ys <- map (\(x, s) -> (,s) <$> decode x) <$> xs
    return (rights ys)
getFromSortedSet key Nothing off count = do
  xs <-
    zrangeWithscores
      key
      (fromIntegral off)
      (fromIntegral off + fromIntegral count - 1)
  return $ do
    ys <- map (\(x, s) -> (,s) <$> decode x) <$> xs
    return (rights ys)
getFromSortedSet key (Just score) off 0 = do
  xs <-
    zrangebyscoreWithscoresLimit
      key
      score
      (1 / 0)
      (fromIntegral off)
      (-1)
  return $ do
    ys <- map (\(x, s) -> (,s) <$> decode x) <$> xs
    return (rights ys)
getFromSortedSet key (Just score) off count = do
  xs <-
    zrangebyscoreWithscoresLimit
      key
      score
      (1 / 0)
      (fromIntegral off)
      (fromIntegral count)
  return $ do
    ys <- map (\(x, s) -> (,s) <$> decode x) <$> xs
    return (rights ys)

-- | Redis command to read all fields of a hash, decoded.
getAllFromMap ::
  (Functor f, RedisCtx m f, Serialize k, Serialize v) =>
  ByteString ->
  m (f [(k, v)])
getAllFromMap n = do
  fxs <- hgetall n
  return $ do
    xs <- fxs
    return
      [ (k, v)
      | (k', v') <- xs,
        let k = fromRight undefined $ decode k',
        let v = fromRight undefined $ decode v'
      ]

-- | Messages handled by the cache writer.
data CacheWriterMessage
  = -- | a new block was imported
    CacheNewBlock
  | -- | a mempool transaction was added or removed
    CacheNewTx !TxHash
  | -- | internal: a batch of coalesced 'CacheNewTx' events
    CacheNewTxs ![TxHash]
  | -- | sync the mempool with the database, then reply
    CacheSyncMempool !(Listen ())

-- | Inbox of the cache writer process.
type CacheWriterInbox = Inbox CacheWriterMessage

-- | Mailbox to send messages to the cache writer.
type CacheWriter = Mailbox CacheWriterMessage

-- | Xpub that a cached address belongs to.
data AddressXPub = AddressXPub
  { -- | the xpub
    spec :: !XPubSpec,
    -- | derivation path of the address below the xpub
    path :: ![KeyIndex]
  }
  deriving (Show, Eq, Generic, NFData, Serialize)

-- | Key of the sorted set of mempool transactions.
mempoolSetKey :: ByteString
mempoolSetKey = "mempool"

-- | Prefix of the keys of cached addresses.
addrPfx :: ByteString
addrPfx = "a"

-- | Key of the last block processed by the cache.
bestBlockKey :: ByteString
bestBlockKey = "head"

-- | Key of the sorted set of cached xpubs by time of last use.
maxKey :: ByteString
maxKey = "max"

-- | Address of an extended public key for a type of address.
xPubAddrFunction :: Ctx -> DeriveType -> XPubKey -> Address
xPubAddrFunction ctx DeriveNormal = xPubAddr ctx
xPubAddrFunction ctx DeriveP2SH = xPubCompatWitnessAddr ctx
xPubAddrFunction ctx DeriveP2WPKH = xPubWitnessAddr ctx

-- | Run the cache writer process: bring the cache up to date with the database,
-- then handle messages forever. Notifications of new mempool transactions that
-- pile up are handled together, up to 1000 at a time.
cacheWriter ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  CacheConfig ->
  CacheWriterInbox ->
  m ()
cacheWriter cfg inbox =
  runReaderT go cfg
  where
    go = do
      newBlockC
      syncMempoolC
      loopForever $ do
        logDebug "Awaiting event..."
        x <- receive inbox
        case x of
          CacheNewTx txid -> do
            -- Coalesce: every queued CacheNewTx is synced in one pass instead
            -- of one Redis round-trip sequence per transaction, so the inbox
            -- cannot grow without bound when Redis is slower than the mempool.
            more <- liftIO (atomically (drainNewTxs []))
            cacheWriterReact (CacheNewTxs (txid : more))
          _ -> cacheWriterReact x
    drainNewTxs acc
      | length acc >= 1000 = return (reverse acc)
      | otherwise = do
          empty <- mailboxEmptySTM inbox
          if empty
            then return (reverse acc)
            else
              receiveSTM inbox >>= \case
                CacheNewTx t -> drainNewTxs (t : acc)
                other -> do
                  requeueSTM other inbox
                  return (reverse acc)

-- | Try to take a lock that expires after five minutes. Returns 'True' if taken.
lockIt :: (MonadIO m) => ByteString -> CacheX m Bool
lockIt l = do
  go >>= \case
    Right Redis.Ok -> do
      logDebug "Acquired lock"
      withMetrics $ \s -> incrementCounter s.lockAcquired 1
      return True
    Right Redis.Pong -> do
      logError "Unexpected pong when acquiring lock"
      withMetrics $ \s -> incrementCounter s.lockFailed 1
      return False
    Right (Redis.Status s) -> do
      logError $
        "Unexpected status acquiring lock: " <> cs s
      withMetrics $ \m -> incrementCounter m.lockFailed 1
      return False
    Left (Redis.Bulk Nothing) -> do
      logDebug "Lock already taken"
      withMetrics $ \s -> incrementCounter s.lockFailed 1
      return False
    Left e -> do
      logError "Error when trying to acquire lock"
      withMetrics $ \s -> incrementCounter s.lockFailed 1
      throwIO (RedisError e)
  where
    go = do
      conn <- asks (.redis)
      liftIO . Redis.runRedis conn $ do
        let opts =
              Redis.SetOpts
                { Redis.setSeconds = Just 300,
                  Redis.setMilliseconds = Nothing,
                  Redis.setUnixSeconds = Nothing,
                  Redis.setUnixMilliseconds = Nothing,
                  Redis.setCondition = Just Redis.Nx,
                  Redis.setKeepTTL = False
                }
        Redis.setOpts l "locked" opts

-- | Extend a lock for another five minutes.
refreshLock :: (MonadIO m) => ByteString -> CacheX m ()
refreshLock l = void . runRedis $ do
  let opts =
        Redis.SetOpts
          { Redis.setSeconds = Just 300,
            Redis.setMilliseconds = Nothing,
            Redis.setUnixSeconds = Nothing,
            Redis.setUnixMilliseconds = Nothing,
            Redis.setCondition = Just Redis.Xx,
            Redis.setKeepTTL = False
          }
  Redis.setOpts l "locked" opts

-- | Release a lock.
unlockIt :: (MonadIO m) => ByteString -> CacheX m ()
unlockIt l = void . runRedis . Redis.del $ NonEmpty.singleton l

-- | Run an action holding a lock, refreshing it every two and a half minutes. Returns 'Nothing' if the lock is taken.
withLock ::
  (MonadUnliftIO m) =>
  ByteString ->
  CacheX m a ->
  CacheX m (Maybe a)
withLock l f =
  bracket (lockIt l) (`when` unlockIt l) $ \case
    True -> Just <$> go
    False -> return Nothing
  where
    go = withAsync refresh $ const f
    refresh = loopForever $ do
      threadDelay (150 * 1000 * 1000)
      refreshLock l

-- | Run the action holding the lock, retrying with exponential backoff
-- while another thread holds it. The delay between attempts grows from
-- 0.2 s to at most 5 s, and after about 30 s of waiting it gives up and
-- returns 'Nothing' instead of keeping the calling thread blocked forever.
withRetryLock ::
  (MonadUnliftIO m) =>
  ByteString ->
  CacheX m a ->
  CacheX m (Maybe a)
withRetryLock l f =
  go 0 (200 * 1000)
  where
    max_delay = 5 * 1000 * 1000
    max_wait = 30 * 1000 * 1000
    go waited n =
      withLock l f >>= \case
        Just x -> return (Just x)
        Nothing
          | waited >= max_wait -> return Nothing
          | otherwise -> do
              threadDelay n
              go (waited + n) (min max_delay (n * 2))

-- | Evict the least recently used xpubs while Redis holds more than the maximum number of keys.
pruneDB ::
  (MonadUnliftIO m, StoreReadBase m) => CacheX m ()
pruneDB = void . withLock "lock" $ do
  x <- asks (.maxKeys)
  s <- runRedis Redis.dbsize
  logDebug "Pruning old xpubs"
  when (s > x) $
    void . delXPubKeys . map fst
      =<< runRedis (getFromSortedSet maxKey Nothing 0 (fromIntegral (s - x)))

-- | Record that xpubs were just used.
touchKeys :: (MonadIO m) => [XPubSpec] -> CacheX m ()
touchKeys xpubs = do
  now <- round <$> liftIO getPOSIXTime :: (MonadIO m) => m Int
  runRedis $ redisTouchKeys now xpubs

-- | Redis command to record that xpubs were used at a time.
redisTouchKeys :: (Monad f, RedisCtx m f, Real a) => a -> [XPubSpec] -> m (f ())
redisTouchKeys _ [] = return $ return ()
redisTouchKeys now xpubs =
  void <$> Redis.zadd maxKey (map ((realToFrac now,) . encode) xpubs)

-- | Handle a message sent to the cache writer.
cacheWriterReact ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  CacheWriterMessage ->
  CacheX m ()
cacheWriterReact CacheNewBlock = do
  logDebug "Received new block event"
  pruneDB
  newBlockC
  syncMempoolC
cacheWriterReact (CacheNewTx txid) =
  cacheWriterReact (CacheNewTxs [txid])
cacheWriterReact (CacheNewTxs txids) = do
  logDebug $
    "Received " <> cs (show (length txids)) <> " new transaction event(s)"
  pruneDB
  syncNewTxC txids
cacheWriterReact (CacheSyncMempool l) = do
  logDebug "Received sync mempool event"
  pruneDB
  newBlockC
  syncMempoolC
  atomically $ l ()

-- | Number of used addresses in a list of xpub balances.
lenNotNull :: [XPubBal] -> Int
lenNotNull = length . filter (not . nullBalance . (.balance))

-- | Cache an xpub, if it has enough used addresses, the store is in sync, and no
-- other thread is caching it already.
newXPubC ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  XPubSpec ->
  [XPubBal] ->
  CacheX m ()
newXPubC xpub xbals =
  should_index >>= \i -> when i $
    bracket set_index unset_index $ \j -> when j $
      withTiming (.indexTime) $ do
        xpubtxt <- xpubText xpub
        logDebug $
          "Caching "
            <> xpubtxt
            <> ": "
            <> cs (show (length xbals))
            <> " addresses / "
            <> cs (show (lenNotNull xbals))
            <> " used"
        utxo <- lift $ xPubUnspents xpub xbals def
        logDebug $
          "Caching "
            <> xpubtxt
            <> ": "
            <> cs (show (length utxo))
            <> " utxos"
        xtxs <- lift $ xPubTxs xpub xbals def
        logDebug $
          "Caching "
            <> xpubtxt
            <> ": "
            <> cs (show (length xtxs))
            <> " txs"
        now <- round <$> liftIO getPOSIXTime :: (MonadIO m) => m Int
        runRedis $ do
          b <- redisTouchKeys now [xpub]
          c <- redisAddXPubBalances xpub xbals
          d <- redisAddXPubUnspents xpub (map op utxo)
          e <- redisAddXPubTxs xpub xtxs
          return $ b >> c >> d >> e >> return ()
        logDebug $ "Cached " <> xpubtxt
  where
    op XPubUnspent {unspent = u} = (u.outpoint, u.block)
    should_index =
      asks (.minAddrs) >>= \x ->
        if x <= lenNotNull xbals
          then inSync
          else return False
    key = idxPfx <> encode xpub
    opts =
      Redis.SetOpts
        { Redis.setSeconds = Just 600,
          Redis.setMilliseconds = Nothing,
          Redis.setUnixSeconds = Nothing,
          Redis.setUnixMilliseconds = Nothing,
          Redis.setCondition = Just Redis.Nx,
          Redis.setKeepTTL = False
        }
    red = Redis.setOpts key "1" opts
    unset_index y =
      when y . void . runRedis . Redis.del $ NonEmpty.singleton key
    set_index = do
      conn <- asks (.redis)
      liftIO (Redis.runRedis conn red) <&> isRight

-- | Whether the best block of the store is the best header of the node.
inSync ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  CacheX m Bool
inSync =
  lift getBestBlock >>= \case
    Nothing -> return False
    Just bb -> do
      ch <- asks (.chain)
      cb <- liftIO $ chainGetBest ch
      return $ cb.height > 0 && headerHash cb.header == bb

-- | Bring the cache up to date with the best block of the store, processing every
-- block since the last one it processed, across reorganisations.
newBlockC ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  CacheX m ()
newBlockC =
  inSync >>= \s -> when s . void . withLock "lock" $ do
    get_best_block_node >>= \case
      Nothing -> logError "No best block available"
      Just best_block_node ->
        cacheGetHead >>= \case
          Nothing -> do
            logInfo "Initializing best cache block"
            importBlockC $ headerHash best_block_node.header
          Just cache_head_hash ->
            get_block_node cache_head_hash >>= \case
              Nothing -> do
                logError $
                  "Could not get best cache block: "
                    <> blockHashToHex cache_head_hash
              Just cache_head_node -> do
                blocks <- get_blocks cache_head_node best_block_node
                mapM_ importBlockC blocks
  where
    get_best_block_node =
      lift getBestBlock >>= \case
        Nothing -> return Nothing
        Just best_block_hash -> get_block_node best_block_hash
    get_block_node block_hash = do
      ch <- asks (.chain)
      liftIO $ chainGetBlock ch block_hash
    get_blocks left_node right_node = do
      ch <- asks (.chain)
      split_node <- liftIO $ chainGetSplitBlock ch left_node right_node
      let split_node_hash = headerHash split_node.header
          right_node_hash = headerHash right_node.header
      if split_node_hash == right_node_hash
        then return []
        else do
          let fork_height = split_node.height + 1
          left_parents <- liftIO $ chainGetParents ch fork_height left_node
          right_parents <- liftIO $ chainGetParents ch fork_height right_node
          let blocks = reverse left_parents <> right_parents <> pure right_node
          return $ map (headerHash . (.header)) blocks

-- | Update cached xpubs with the transactions of a block.
importBlockC ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  BlockHash ->
  CacheX m ()
importBlockC bh =
  lift (getBlock bh) >>= \case
    Just bd -> do
      let ths = bd.txs
      tds <- sortTxData . catMaybes <$> mapM (lift . getTxData) ths
      logDebug $
        "Importing "
          <> cs (show (length tds))
          <> " transactions from block "
          <> blockHashToHex bh
      importMultiTxC tds
      logDebug $
        "Done importing "
          <> cs (show (length tds))
          <> " transactions from block "
          <> blockHashToHex bh
      cacheSetHead bd
    Nothing -> do
      logError $
        "Could not get block: "
          <> blockHashToHex bh
      throwIO . LogicError . cs $
        "Could not get block: "
          <> blockHashToHex bh

-- | Update cached xpubs with transactions: their transaction sets, unspent
-- outputs, balances and the mempool. Addresses past the gap are derived and
-- added when new addresses get used.
importMultiTxC ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  [TxData] ->
  CacheX m ()
importMultiTxC txs = do
  net <- lift getNetwork
  ctx <- lift getCtx
  logDebug $ "Processing " <> cs (show (length txs)) <> " txs"
  logDebug $
    "Getting address information for "
      <> cs (show (length (alladdrs net ctx)))
      <> " addresses"
  addrmap <- getaddrmap net ctx
  let addrs = HashMap.keys addrmap
  logDebug $
    "Getting balances for "
      <> cs (show (HashMap.size addrmap))
      <> " addresses"
  balmap <- getbalances addrs
  logDebug $
    "Getting unspent data for "
      <> cs (show (length (allops net ctx)))
      <> " outputs"
  unspentmap <- getunspents net ctx
  gap <- lift getMaxGap
  now <- round <$> liftIO getPOSIXTime :: (MonadIO m) => m Int
  let xpubs = allxpubsls addrmap
  forM_ (zip [(1 :: Int) ..] xpubs) $ \(i, xpub) -> do
    xpubtxt <- xpubText xpub
    logDebug $
      "Affected xpub "
        <> cs (show i)
        <> "/"
        <> cs (show (length xpubs))
        <> ": "
        <> xpubtxt
  addrs' <- do
    logDebug $
      "Getting xpub balances for "
        <> cs (show (length xpubs))
        <> " xpubs"
    xmap <- getxbals xpubs
    let addrmap' = faddrmap (HashMap.keysSet xmap) addrmap
    logDebug "Starting Redis import pipeline"
    runRedis $ do
      x <- redisImportMultiTx net ctx addrmap' unspentmap txs
      y <- redisUpdateBalances addrmap' balmap
      z <- redisTouchKeys now (HashMap.keys xmap)
      return $ x >> y >> z >> return ()
    logDebug "Completed Redis pipeline"
    return $ getNewAddrs ctx gap xmap (HashMap.elems addrmap')
  cacheAddAddresses addrs'
  where
    alladdrsls net ctx = HashSet.toList (alladdrs net ctx)
    faddrmap xmap = HashMap.filter (\a -> a.spec `elem` xmap)
    getaddrmap net ctx =
      HashMap.fromList
        . catMaybes
        . zipWith (\a -> fmap (a,)) (alladdrsls net ctx)
        <$> cacheGetAddrsInfo (alladdrsls net ctx)
    getunspents net ctx =
      HashMap.fromList
        . catMaybes
        . zipWith (\p -> fmap (p,)) (allops net ctx)
        <$> lift (mapM getUnspent (allops net ctx))
    getbalances addrs =
      HashMap.fromList . zip addrs <$> mapM (lift . getDefaultBalance) addrs
    getxbals xpubs = do
      bals <- runRedis . fmap sequence . forM xpubs $ \xpub -> do
        bs <- redisGetXPubBalances xpub
        return $ (,) xpub <$> bs
      return $ HashMap.filter (not . null) (HashMap.fromList bals)
    allops net ctx =
      map snd $
        concatMap (txInputs net ctx) txs
          <> concatMap (txOutputs net ctx) txs
    alladdrs net ctx =
      HashSet.fromList $
        map fst $
          concatMap (txInputs net ctx) txs
            <> concatMap (txOutputs net ctx) txs
    allxpubsls = HashSet.toList . allxpubs
    allxpubs =
      HashSet.fromList . map (.spec) . HashMap.elems

-- | Redis commands to add or remove transactions and unspent outputs of cached xpubs.
redisImportMultiTx ::
  (Monad f, RedisCtx m f) =>
  Network ->
  Ctx ->
  HashMap Address AddressXPub ->
  HashMap OutPoint Unspent ->
  [TxData] ->
  m (f ())
redisImportMultiTx net ctx addrmap unspentmap tds = do
  xs <- mapM importtxentries tds
  return $ sequence_ xs
  where
    uns p i =
      case HashMap.lookup p unspentmap of
        Just u ->
          redisAddXPubUnspents i.spec [(p, u.block)]
        Nothing -> redisRemXPubUnspents i.spec [p]
    addtx tx a p =
      case HashMap.lookup a addrmap of
        Just i -> do
          let tr =
                TxRef
                  { txid = txHash tx.tx,
                    block = tx.block
                  }
          x <- redisAddXPubTxs i.spec [tr]
          y <- uns p i
          return $ x >> y >> return ()
        Nothing -> return (pure ())
    remtx tx a p =
      case HashMap.lookup a addrmap of
        Just i -> do
          x <- redisRemXPubTxs i.spec [txHash tx.tx]
          y <- uns p i
          return $ x >> y >> return ()
        Nothing -> return (pure ())
    importtxentries td =
      if td.deleted
        then do
          x <-
            mapM
              (uncurry (remtx td))
              (txaddrops td)
          y <- redisRemFromMempool [txHash td.tx]
          return $ sequence_ x >> void y
        else do
          a <-
            sequence
              <$> mapM
                (uncurry (addtx td))
                (txaddrops td)
          b <-
            case td.block of
              b@MemRef {} ->
                let tr =
                      TxRef
                        { txid = txHash td.tx,
                          block = b
                        }
                 in redisAddToMempool [tr]
              _ -> redisRemFromMempool [txHash td.tx]
          return $ a >> b >> return ()
    txaddrops td = txInputs net ctx td <> txOutputs net ctx td

-- | Redis commands to update the balances of cached addresses.
redisUpdateBalances ::
  (Monad f, RedisCtx m f) =>
  HashMap Address AddressXPub ->
  HashMap Address Balance ->
  m (f ())
redisUpdateBalances addrmap balmap =
  fmap (fmap mconcat . sequence) . forM (HashMap.keys addrmap) $ \a ->
    case (HashMap.lookup a addrmap, HashMap.lookup a balmap) of
      (Just ainfo, Just bal) ->
        redisAddXPubBalances ainfo.spec [xpubbal ainfo bal]
      _ -> return (pure ())
  where
    xpubbal ainfo bal =
      XPubBal {path = ainfo.path, balance = bal}

-- | Add newly derived addresses to their cached xpubs, with their balances,
-- transactions and unspent outputs, and keep deriving while they are used.
cacheAddAddresses ::
  (StoreReadExtra m, MonadUnliftIO m) =>
  [(Address, AddressXPub)] ->
  CacheX m ()
cacheAddAddresses [] = logDebug "No further addresses to add"
cacheAddAddresses addrs = do
  ctx <- lift getCtx
  logDebug $
    "Adding " <> cs (show (length addrs)) <> " new generated addresses"
  logDebug "Getting balances"
  balmap <- HashMap.fromListWith (<>) <$> mapM (uncurry getbal) addrs
  logDebug "Getting unspent outputs"
  utxomap <- HashMap.fromListWith (<>) <$> mapM (uncurry getutxo) addrs
  logDebug "Getting transactions"
  txmap <- HashMap.fromListWith (<>) <$> mapM (uncurry gettxmap) addrs
  logDebug "Running Redis pipeline"
  runRedis $ do
    a <- forM (HashMap.toList balmap) (uncurry redisAddXPubBalances)
    b <- forM (HashMap.toList utxomap) (uncurry redisAddXPubUnspents)
    c <- forM (HashMap.toList txmap) (uncurry redisAddXPubTxs)
    return $ sequence_ a >> sequence_ b >> sequence_ c
  logDebug "Completed Redis pipeline"
  let xpubs =
        HashSet.toList
          . HashSet.fromList
          . map (.spec)
          $ Map.elems amap
  logDebug "Getting xpub balances"
  xmap <- getbals xpubs
  gap <- lift getMaxGap
  let notnulls = getnotnull balmap
      addrs' = getNewAddrs ctx gap xmap notnulls
  cacheAddAddresses addrs'
  where
    getbals xpubs = runRedis $ do
      bs <- sequence <$> forM xpubs redisGetXPubBalances
      return $
        HashMap.filter (not . null)
          . HashMap.fromList
          . zip xpubs
          <$> bs
    amap = Map.fromList addrs
    getnotnull =
      let f xpub =
            map $ \bal ->
              AddressXPub
                { spec = xpub,
                  path = bal.path
                }
          g = filter (not . nullBalance . (.balance))
       in concatMap (uncurry f) . HashMap.toList . HashMap.map g
    getbal a i =
      let f b =
            ( i.spec,
              [XPubBal {balance = b, path = i.path}]
            )
       in f <$> lift (getDefaultBalance a)
    getutxo a i =
      let f us =
            ( i.spec,
              map (\u -> (u.outpoint, u.block)) us
            )
       in f <$> lift (getAddressUnspents a def)
    gettxmap a i =
      let f ts = (i.spec, ts)
       in f <$> lift (getAddressTxs a def)

-- | Addresses to derive and add to cached xpubs to keep the gap after the used ones.
getNewAddrs ::
  Ctx ->
  KeyIndex ->
  HashMap XPubSpec [XPubBal] ->
  [AddressXPub] ->
  [(Address, AddressXPub)]
getNewAddrs ctx gap xpubs =
  concatMap $ \a ->
    case HashMap.lookup a.spec xpubs of
      Nothing -> []
      Just bals -> addrsToAdd ctx gap bals a

-- | Update cached xpubs with new or deleted transactions, if in sync.
syncNewTxC ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  [TxHash] ->
  CacheX m ()
syncNewTxC ths =
  inSync >>= \s -> when s . void . withLock "lock" $ do
    txs <- catMaybes <$> mapM (lift . getTxData) ths
    unless (null txs) $ do
      forM_ txs $ \tx ->
        logDebug $
          "Synchronizing transaction: " <> txHashToHex (txHash tx.tx)
      importMultiTxC txs

-- | Update cached xpubs with every mempool difference between the cache and the store, if in sync.
syncMempoolC ::
  (MonadUnliftIO m, StoreReadExtra m) =>
  CacheX m ()
syncMempoolC =
  inSync >>= \s -> when s . void . withLock "lock" $ do
    nodepool <- HashSet.fromList . map snd <$> lift getMempool
    cachepool <- HashSet.fromList . map snd <$> cacheGetMempool
    let diff1 = HashSet.difference nodepool cachepool
    let diff2 = HashSet.difference cachepool nodepool
    let diffset = diff1 <> diff2
    let tids = HashSet.toList diffset
    txs <- catMaybes <$> mapM (lift . getTxData) tids
    unless (null txs) $ do
      logDebug $
        "Synchronizing " <> cs (show (length txs)) <> " mempool transactions"
      importMultiTxC txs

-- | Mempool as seen by the cache, newest first.
cacheGetMempool :: (MonadIO m) => CacheX m [(UnixTime, TxHash)]
cacheGetMempool = runRedis redisGetMempool

-- | Last block processed by the cache.
cacheGetHead :: (MonadIO m) => CacheX m (Maybe BlockHash)
cacheGetHead = runRedis redisGetHead

-- | Set the last block processed by the cache.
cacheSetHead :: (MonadIO m) => BlockData -> CacheX m ()
cacheSetHead bd = do
  logDebug $ "Cache head set to: " <> blockHashToHex bh
  void $ runRedis (redisSetHead bh)
  withMetrics $ \s -> setGauge s.height (fromIntegral bd.height)
  where
    bh = headerHash bd.header

-- | Xpub and path of each address, or 'Nothing' if the address is not cached.
cacheGetAddrsInfo ::
  (MonadIO m) => [Address] -> CacheX m [Maybe AddressXPub]
cacheGetAddrsInfo as = runRedis (redisGetAddrsInfo as)

-- | Redis command to add transactions to the mempool set.
redisAddToMempool :: (Applicative f, RedisCtx m f) => [TxRef] -> m (f Integer)
redisAddToMempool [] = return (pure 0)
redisAddToMempool btxs =
  zadd mempoolSetKey $
    map
      (\btx -> (blockRefScore btx.block, encode btx.txid))
      btxs

-- | Redis command to remove transactions from the mempool set.
redisRemFromMempool ::
  (Applicative f, RedisCtx m f) => [TxHash] -> m (f Integer)
redisRemFromMempool xs =
  case NonEmpty.nonEmpty xs of
    Nothing -> return (pure 0)
    Just xs' -> zrem mempoolSetKey $ NonEmpty.map encode xs'

-- | Redis command to record the xpub and path of an address.
redisSetAddrInfo ::
  (Functor f, RedisCtx m f) => Address -> AddressXPub -> m (f ())
redisSetAddrInfo a i = void <$> Redis.set (addrPfx <> encode a) (encode i)

-- | Remove xpubs from the cache. Returns the number of keys deleted.
cacheDelXPubs ::
  (MonadIO m, StoreReadBase m) =>
  [XPubSpec] ->
  CacheT m Integer
cacheDelXPubs xpubs = ReaderT $ \case
  Just cache -> runReaderT (delXPubKeys xpubs) cache
  Nothing -> return 0

-- | Remove xpubs from the cache. Returns the number of keys deleted.
delXPubKeys ::
  (MonadIO m, StoreReadBase m) =>
  [XPubSpec] ->
  CacheX m Integer
delXPubKeys [] = return 0
delXPubKeys xpubs = do
  forM_ xpubs $ \x -> do
    xtxt <- xpubText x
    logDebug $ "Deleting xpub: " <> xtxt
  xbals <-
    runRedis . fmap sequence . forM xpubs $ \xpub -> do
      bs <- redisGetXPubBalances xpub
      return $ (xpub,) <$> bs
  runRedis $ fmap sum . sequence <$> forM xbals (uncurry redisDelXPubKeys)

-- | Redis commands to remove an xpub and its addresses from the cache.
redisDelXPubKeys ::
  (Monad f, RedisCtx m f) => XPubSpec -> [XPubBal] -> m (f Integer)
redisDelXPubKeys xpub bals = go $ map (.balance.address) bals
  where
    go addrs = do
      addrcount <-
        case NonEmpty.nonEmpty addrs of
          Nothing -> return (pure 0)
          Just addrs' -> Redis.del $ NonEmpty.map ((addrPfx <>) . encode) addrs'
      txsetcount <- Redis.del $ NonEmpty.singleton (txSetPfx <> encode xpub)
      utxocount <- Redis.del $ NonEmpty.singleton (utxoPfx <> encode xpub)
      balcount <- Redis.del $ NonEmpty.singleton (balancesPfx <> encode xpub)
      x <- Redis.zrem maxKey $ NonEmpty.singleton (encode xpub)
      return $ do
        _ <- x
        addrs' <- addrcount
        txset' <- txsetcount
        utxo' <- utxocount
        bal' <- balcount
        return $ addrs' + txset' + utxo' + bal'

-- | Redis command to add transactions to an xpub.
redisAddXPubTxs ::
  (Applicative f, RedisCtx m f) => XPubSpec -> [TxRef] -> m (f Integer)
redisAddXPubTxs _ [] = return (pure 0)
redisAddXPubTxs xpub btxs =
  zadd (txSetPfx <> encode xpub) $
    map (\t -> (blockRefScore t.block, encode t.txid)) btxs

-- | Redis command to remove transactions from an xpub.
redisRemXPubTxs ::
  (Applicative f, RedisCtx m f) => XPubSpec -> [TxHash] -> m (f Integer)
redisRemXPubTxs xpub txhs =
  case NonEmpty.nonEmpty txhs of
    Nothing -> return (pure 0)
    Just txhs' -> zrem (txSetPfx <> encode xpub) (NonEmpty.map encode txhs')

-- | Redis command to add unspent outputs to an xpub.
redisAddXPubUnspents ::
  (Applicative f, RedisCtx m f) =>
  XPubSpec ->
  [(OutPoint, BlockRef)] ->
  m (f Integer)
redisAddXPubUnspents _ [] =
  return (pure 0)
redisAddXPubUnspents xpub utxo =
  zadd (utxoPfx <> encode xpub) $
    map (\(p, r) -> (blockRefScore r, encode p)) utxo

-- | Redis command to remove unspent outputs from an xpub.
redisRemXPubUnspents ::
  (Applicative f, RedisCtx m f) => XPubSpec -> [OutPoint] -> m (f Integer)
redisRemXPubUnspents _ [] =
  return (pure 0)
redisRemXPubUnspents xpub ops =
  case NonEmpty.nonEmpty ops of
    Nothing -> return (pure 0)
    Just ops' -> zrem (utxoPfx <> encode xpub) (NonEmpty.map encode ops')

-- | Redis commands to set balances of an xpub and record its addresses.
redisAddXPubBalances ::
  (Monad f, RedisCtx m f) => XPubSpec -> [XPubBal] -> m (f ())
redisAddXPubBalances xpub bals =
  case NonEmpty.nonEmpty bals of
    Nothing -> return (pure ())
    Just bals' -> do
      let bs = NonEmpty.map (\b -> (encode b.path, encode b.balance)) bals'
      Redis.hset (balancesPfx <> encode xpub) bs
      ys <- forM bals' $ \b ->
        redisSetAddrInfo
          b.balance.address
          AddressXPub
            { spec = xpub,
              path = b.path
            }
      return $ sequence_ ys

-- | Redis command to set the last block processed by the cache.
redisSetHead :: (RedisCtx m f) => BlockHash -> m (f Redis.Status)
redisSetHead bh = Redis.set bestBlockKey (encode bh)

-- | Redis commands to read the xpub and path of addresses.
redisGetAddrsInfo ::
  (Monad f, RedisCtx m f) => [Address] -> m (f [Maybe AddressXPub])
redisGetAddrsInfo [] = return (pure [])
redisGetAddrsInfo as = do
  is <- mapM (\a -> Redis.get (addrPfx <> encode a)) as
  return $ do
    is' <- sequence is
    return $ map (eitherToMaybe . decode =<<) is'

-- | Addresses to derive after a newly used address, so that the gap of unused
-- addresses after the last used one on its chain is kept.
addrsToAdd ::
  Ctx ->
  KeyIndex ->
  [XPubBal] ->
  AddressXPub ->
  [(Address, AddressXPub)]
addrsToAdd ctx gap xbals addrinfo
  | null fbals =
      []
  | not haschange =
      zipWith f addrs list
        <> zipWith f changeaddrs changelist
  | otherwise =
      zipWith f addrs list
  where
    haschange = any ((== 1) . (!! 0) . (.path)) xbals
    f a p = (a, AddressXPub {spec = xpub, path = p})
    dchain = addrinfo.path !! 0
    fbals = filter ((== dchain) . (!! 0) . (.path)) xbals
    maxidx = maximum (map ((!! 1) . (.path)) fbals)
    xpub = addrinfo.spec
    aidx = addrinfo.path !! 1
    ixs =
      if gap > maxidx - aidx
        then [maxidx + 1 .. aidx + gap]
        else []
    paths = map (Deriv :/ dchain :/) ixs
    keys = map (\p -> derivePubPath ctx p xpub.key)
    list = map pathToList paths
    xpubf = xPubAddrFunction ctx xpub.deriv
    addrs = map xpubf (keys paths)
    changepaths = map (Deriv :/ 1 :/) [0 .. gap - 1]
    changeaddrs = map xpubf (keys changepaths)
    changelist = map pathToList changepaths

-- | Sort transactions so that each comes after those it spends.
sortTxData :: [TxData] -> [TxData]
sortTxData tds =
  let txm = Map.fromList (map (\d -> (txHash d.tx, d)) tds)
      ths = map (txHash . snd) (sortTxs (map (.tx) tds))
   in mapMaybe (`Map.lookup` txm) ths

-- | Addresses and outpoints of the outputs spent by a transaction.
txInputs :: Network -> Ctx -> TxData -> [(Address, OutPoint)]
txInputs net ctx td =
  let is = td.tx.inputs
      ps = I.toAscList td.prevs
      as = map (scriptToAddressBS net ctx . (.script) . snd) ps
      f (Right a) i = Just (a, i.outpoint)
      f (Left _) _ = Nothing
   in catMaybes (zipWith f as is)

-- | Addresses and outpoints of the outputs of a transaction.
txOutputs :: Network -> Ctx -> TxData -> [(Address, OutPoint)]
txOutputs net ctx td =
  let ps =
        zipWith
          ( \i _ ->
              OutPoint
                { hash = txHash td.tx,
                  index = i
                }
          )
          [0 ..]
          td.tx.outputs
      as = map (scriptToAddressBS net ctx . (.script)) td.tx.outputs
      f (Right a) p = Just (a, p)
      f (Left _) _ = Nothing
   in catMaybes (zipWith f as ps)

-- | Redis command to read the last block processed by the cache.
redisGetHead :: (Functor f, RedisCtx m f) => m (f (Maybe BlockHash))
redisGetHead = do
  x <- Redis.get bestBlockKey
  return $ (eitherToMaybe . decode =<<) <$> x

-- | Redis command to read the mempool set, newest first.
redisGetMempool :: (Applicative f, RedisCtx m f) => m (f [(UnixTime, TxHash)])
redisGetMempool = do
  xs <- getFromSortedSet mempoolSetKey Nothing 0 0
  return $ map (uncurry f) <$> xs
  where
    f t s = ((scoreBlockRef s).timestamp, t)

-- | Text form of an xpub for logs, with a suffix for its address type.
xpubText ::
  (StoreReadBase m) =>
  XPubSpec ->
  CacheX m Text
xpubText xpub = do
  net <- lift getNetwork
  let suffix = case xpub.deriv of
        DeriveNormal -> ""
        DeriveP2SH -> "/p2sh"
        DeriveP2WPKH -> "/p2wpkh"
  ctx <- lift getCtx
  return . cs $ suffix <> xPubExport net ctx xpub.key

-- | Tell the cache writer that a new block was imported.
cacheNewBlock :: (MonadIO m) => CacheWriter -> m ()
cacheNewBlock = send CacheNewBlock

-- | Tell the cache writer that a mempool transaction was added or removed.
cacheNewTx :: (MonadIO m) => TxHash -> CacheWriter -> m ()
cacheNewTx = send . CacheNewTx

-- | Ask the cache writer to sync the mempool, and wait until it is done.
cacheSyncMempool :: (MonadIO m) => CacheWriter -> m ()
cacheSyncMempool = query CacheSyncMempool
