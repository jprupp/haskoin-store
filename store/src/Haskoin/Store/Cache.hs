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
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE NoFieldSelectors #-}

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
import Control.Monad (forM, forM_, forever, unless, void, when)
import Control.Monad.Logger
  ( MonadLoggerIO,
    logDebugS,
    logErrorS,
    logInfoS,
  )
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
    chainGetBest,
    chainGetBlock,
    chainGetParents,
    chainGetSplitBlock,
  )
import Haskoin.Store.Common
import Haskoin.Store.Data
import Haskoin.Store.Database.Reader (DatabaseReader, withDB)
import NQE
  ( Inbox,
    Listen,
    Mailbox,
    query,
    receive,
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

runRedis :: (MonadLoggerIO m) => Redis (Either Reply a) -> CacheX m a
runRedis action = do
  conn <- asks (.redis)
  withRedis conn action

withRedis :: (MonadLoggerIO m) => Connection -> Redis (Either Reply a) -> m a
withRedis conn action =
  liftIO (Redis.runRedis conn action) >>= \case
    Right x -> return x
    Left e -> do
      $(logErrorS) "Cache" $ "Got error from Redis: " <> cs (show e)
      throwIO (RedisError e)

data CacheConfig = CacheConfig
  { redis :: !Connection,
    minAddrs :: !Int,
    maxKeys :: !Integer,
    chain :: !Chain,
    metrics :: !(Maybe CacheMetrics)
  }

data CacheMetrics = CacheMetrics
  { hits :: !StatCounter,
    misses :: !StatCounter,
    lockAcquired :: !StatCounter,
    lockReleased :: !StatCounter,
    lockFailed :: !StatCounter,
    xBalances :: !StatCounter,
    xUnspents :: !StatCounter,
    xTxs :: !StatCounter,
    xTxCount :: !StatCounter,
    indexTime :: !StatTiming,
    height :: !StatGauge
  }

newCacheMetrics ::
  (MonadLoggerIO m) =>
  Stats ->
  Connection ->
  DatabaseReader ->
  m CacheMetrics
newCacheMetrics s r db = do
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
      maybe dbBest dbBlock =<< withRedis r redisGetHead
    dbBlock h =
      maybe 0 (fromIntegral . (.height)) <$> withDB db (getBlock h)
    dbBest =
      maybe (return 0) dbBlock =<< withDB db getBestBlock
    c x = newStatCounter s x 10
    d x = newStatTiming s x 10

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

withMetrics ::
  (MonadIO m) => (CacheMetrics -> CacheX m a) -> CacheX m ()
withMetrics go = asks (.metrics) >>= mapM_ go

type CacheT = ReaderT (Maybe CacheConfig)

type CacheX = ReaderT CacheConfig

data CacheError
  = RedisError Reply
  | RedisTxError !String
  | LogicError !String
  deriving (Show, Eq, Generic, NFData, Exception)

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
  getSpender = lift . getSpender
  getBalance = lift . getBalance
  getUnspent = lift . getUnspent
  getMempool = lift getMempool

instance (StoreReadBase m) => StoreReadBase (CacheT m) where
  getCtx = lift getCtx
  getNetwork = lift getNetwork
  getBestBlock = lift getBestBlock
  getBlocksAtHeight = lift . getBlocksAtHeight
  getBlock = lift . getBlock
  getTxData = lift . getTxData
  getSpender = lift . getSpender
  getBalance = lift . getBalance
  getUnspent = lift . getUnspent
  getMempool = lift getMempool

instance
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadExtra m) =>
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
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadExtra m) =>
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

withCache :: Maybe CacheConfig -> CacheT m a -> m a
withCache s f = runReaderT f s

balancesPfx :: ByteString
balancesPfx = "b"

txSetPfx :: ByteString
txSetPfx = "t"

utxoPfx :: ByteString
utxoPfx = "u"

idxPfx :: ByteString
idxPfx = "i"

xpubLock :: XPubSpec -> ByteString
xpubLock xpub = "xlock-" <> encode xpub

getXPubTxs ::
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadExtra m) =>
  XPubSpec ->
  [XPubBal] ->
  Limits ->
  CacheX m [TxRef]
getXPubTxs xpub xbals limits = do
  cached <- isXPubCached xpub
  if cached
    then do
      txs <- cacheGetXPubTxs xpub limits
      withMetrics $ \s ->
        incrementCounter s.xTxs (length txs)
      return txs
    else lift $ xPubTxs xpub xbals limits

getXPubTxCount ::
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadExtra m) =>
  XPubSpec ->
  [XPubBal] ->
  CacheX m Word32
getXPubTxCount xpub xbals = do
  cached <- isXPubCached xpub
  if cached
    then do
      withMetrics $ \s ->
        incrementCounter s.xTxCount 1
      cacheGetXPubTxCount xpub
    else lift $ xPubTxCount xpub xbals

getXPubUnspents ::
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadExtra m) =>
  XPubSpec ->
  [XPubBal] ->
  Limits ->
  CacheX m [XPubUnspent]
getXPubUnspents xpub xbals limits = do
  cached <- isXPubCached xpub
  if cached
    then do
      ops <- map snd <$> cacheGetXPubUnspents xpub limits
      uns <- catMaybes <$> lift (mapM getUnspent ops)
      ctx <- lift getCtx
      let f u =
            either
              (const Nothing)
              (\a -> Just (a, u))
              (scriptToAddressBS ctx u.script)
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
    else lift $ xPubUnspents xpub xbals limits

getXPubBalances ::
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadExtra m) =>
  XPubSpec ->
  CacheX m [XPubBal]
getXPubBalances xpub = do
  cached <- isXPubCached xpub
  if cached
    then do
      xbals <- cacheGetXPubBalances xpub
      withMetrics $ \s -> do
        incrementCounter s.hits 1
        incrementCounter s.xBalances (length xbals)
      return xbals
    else do
      withMetrics $ \s -> incrementCounter s.misses 1
      withRetryLock (xpubLock xpub) $ do
        bals <- lift $ xPubBals xpub
        newXPubC xpub bals
        return bals

isInCache :: (MonadLoggerIO m) => XPubSpec -> CacheT m Bool
isInCache xpub =
  ReaderT $ \case
    Nothing -> return False
    Just cfg -> runReaderT (isXPubCached xpub) cfg

isXPubCached :: (MonadLoggerIO m) => XPubSpec -> CacheX m Bool
isXPubCached xpub = runRedis (redisIsXPubCached xpub)

redisIsXPubCached :: (RedisCtx m f) => XPubSpec -> m (f Bool)
redisIsXPubCached xpub = Redis.exists (balancesPfx <> encode xpub)

cacheGetXPubBalances :: (MonadLoggerIO m) => XPubSpec -> CacheX m [XPubBal]
cacheGetXPubBalances xpub = do
  bals <- runRedis $ redisGetXPubBalances xpub
  touchKeys [xpub]
  return bals

cacheGetXPubTxCount ::
  (MonadUnliftIO m, MonadLoggerIO m) =>
  XPubSpec ->
  CacheX m Word32
cacheGetXPubTxCount xpub = do
  count <- fromInteger <$> runRedis (redisGetXPubTxCount xpub)
  touchKeys [xpub]
  return count

redisGetXPubTxCount :: (RedisCtx m f) => XPubSpec -> m (f Integer)
redisGetXPubTxCount xpub = Redis.zcard (txSetPfx <> encode xpub)

cacheGetXPubTxs ::
  (StoreReadBase m, MonadLoggerIO m) =>
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

cacheGetXPubUnspents ::
  (StoreReadBase m, MonadLoggerIO m) =>
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

redisGetXPubBalances :: (Functor f, RedisCtx m f) => XPubSpec -> m (f [XPubBal])
redisGetXPubBalances xpub =
  fmap (sort . map (uncurry f)) <$> getAllFromMap (balancesPfx <> encode xpub)
  where
    f p b = XPubBal {path = p, balance = b}

blockRefScore :: BlockRef -> Double
blockRefScore BlockRef {height = h, position = p} =
  fromIntegral (0x001fffffffffffff - (h' .|. p'))
  where
    h' = (fromIntegral h .&. 0x07ffffff) `shift` 26 :: Word64
    p' = (fromIntegral p .&. 0x03ffffff) :: Word64
blockRefScore MemRef {timestamp = t} = negate t'
  where
    t' = fromIntegral (t .&. 0x001fffffffffffff)

scoreBlockRef :: Double -> BlockRef
scoreBlockRef s
  | s < 0 = MemRef {timestamp = n}
  | otherwise = BlockRef {height = h, position = p}
  where
    n = truncate (abs s) :: Word64
    m = 0x001fffffffffffff - n
    h = fromIntegral (m `shift` (-26))
    p = fromIntegral (m .&. 0x03ffffff)

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

data CacheWriterMessage
  = CacheNewBlock
  | CacheNewTx !TxHash
  | CacheSyncMempool !(Listen ())

type CacheWriterInbox = Inbox CacheWriterMessage

type CacheWriter = Mailbox CacheWriterMessage

data AddressXPub = AddressXPub
  { spec :: !XPubSpec,
    path :: ![KeyIndex]
  }
  deriving (Show, Eq, Generic, NFData, Serialize)

mempoolSetKey :: ByteString
mempoolSetKey = "mempool"

addrPfx :: ByteString
addrPfx = "a"

bestBlockKey :: ByteString
bestBlockKey = "head"

maxKey :: ByteString
maxKey = "max"

xPubAddrFunction :: Ctx -> DeriveType -> XPubKey -> Address
xPubAddrFunction ctx DeriveNormal = xPubAddr ctx
xPubAddrFunction ctx DeriveP2SH = xPubCompatWitnessAddr ctx
xPubAddrFunction ctx DeriveP2WPKH = xPubWitnessAddr ctx

cacheWriter ::
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadExtra m) =>
  CacheConfig ->
  CacheWriterInbox ->
  m ()
cacheWriter cfg inbox =
  runReaderT go cfg
  where
    go = do
      newBlockC
      syncMempoolC
      forever $ do
        $(logDebugS) "Cache" "Awaiting event..."
        x <- receive inbox
        cacheWriterReact x

lockIt :: (MonadLoggerIO m) => ByteString -> CacheX m Bool
lockIt l = do
  go >>= \case
    Right Redis.Ok -> do
      $(logDebugS) "Cache" "Acquired lock"
      withMetrics $ \s -> incrementCounter s.lockAcquired 1
      return True
    Right Redis.Pong -> do
      $(logErrorS)
        "Cache"
        "Unexpected pong when acquiring lock"
      withMetrics $ \s -> incrementCounter s.lockFailed 1
      return False
    Right (Redis.Status s) -> do
      $(logErrorS) "Cache" $
        "Unexpected status acquiring lock: " <> cs s
      withMetrics $ \m -> incrementCounter m.lockFailed 1
      return False
    Left (Redis.Bulk Nothing) -> do
      $(logDebugS) "Cache" "Lock already taken"
      withMetrics $ \s -> incrementCounter s.lockFailed 1
      return False
    Left e -> do
      $(logErrorS)
        "Cache"
        "Error when trying to acquire lock"
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

refreshLock :: (MonadLoggerIO m) => ByteString -> CacheX m ()
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

unlockIt :: (MonadLoggerIO m) => ByteString -> CacheX m ()
unlockIt l = void . runRedis . Redis.del $ NonEmpty.singleton l

withLock ::
  (MonadLoggerIO m, MonadUnliftIO m) =>
  ByteString ->
  CacheX m a ->
  CacheX m (Maybe a)
withLock l f =
  bracket (lockIt l) (`when` unlockIt l) $ \case
    True -> Just <$> go
    False -> return Nothing
  where
    go = withAsync refresh $ const f
    refresh = forever $ do
      threadDelay (150 * 1000 * 1000)
      refreshLock l

withRetryLock ::
  (MonadLoggerIO m, MonadUnliftIO m) =>
  ByteString ->
  CacheX m a ->
  CacheX m a
withRetryLock l f =
  go (200 * 1000)
  where
    go n =
      withLock l f >>= \case
        Just x -> return x
        Nothing -> do
          threadDelay n
          go (n * 2)

pruneDB ::
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadBase m) => CacheX m ()
pruneDB = void . withLock "lock" $ do
  x <- asks (.maxKeys)
  s <- runRedis Redis.dbsize
  $(logDebugS) "Cache" "Pruning old xpubs"
  when (s > x) $
    void . delXPubKeys . map fst
      =<< runRedis (getFromSortedSet maxKey Nothing 0 (fromIntegral (s - x)))

touchKeys :: (MonadLoggerIO m) => [XPubSpec] -> CacheX m ()
touchKeys xpubs = do
  now <- round <$> liftIO getPOSIXTime :: (MonadIO m) => m Int
  runRedis $ redisTouchKeys now xpubs

redisTouchKeys :: (Monad f, RedisCtx m f, Real a) => a -> [XPubSpec] -> m (f ())
redisTouchKeys _ [] = return $ return ()
redisTouchKeys now xpubs =
  void <$> Redis.zadd maxKey (map ((realToFrac now,) . encode) xpubs)

cacheWriterReact ::
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadExtra m) =>
  CacheWriterMessage ->
  CacheX m ()
cacheWriterReact CacheNewBlock = do
  $(logDebugS) "Cache" "Received new block event"
  pruneDB
  newBlockC
  syncMempoolC
cacheWriterReact (CacheNewTx txid) = do
  $(logDebugS) "Cache" $
    "Received new transaction event: " <> txHashToHex txid
  pruneDB
  syncNewTxC [txid]
cacheWriterReact (CacheSyncMempool l) = do
  $(logDebugS) "Cache" "Received sync mempool event"
  pruneDB
  newBlockC
  syncMempoolC
  atomically $ l ()

lenNotNull :: [XPubBal] -> Int
lenNotNull = length . filter (not . nullBalance . (.balance))

newXPubC ::
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadExtra m) =>
  XPubSpec ->
  [XPubBal] ->
  CacheX m ()
newXPubC xpub xbals =
  should_index >>= \i -> when i $
    bracket set_index unset_index $ \j -> when j $
      withTiming (.indexTime) $ do
        xpubtxt <- xpubText xpub
        $(logDebugS) "Cache" $
          "Caching "
            <> xpubtxt
            <> ": "
            <> cs (show (length xbals))
            <> " addresses / "
            <> cs (show (lenNotNull xbals))
            <> " used"
        utxo <- lift $ xPubUnspents xpub xbals def
        $(logDebugS) "Cache" $
          "Caching "
            <> xpubtxt
            <> ": "
            <> cs (show (length utxo))
            <> " utxos"
        xtxs <- lift $ xPubTxs xpub xbals def
        $(logDebugS) "Cache" $
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
        $(logDebugS) "Cache" $ "Cached " <> xpubtxt
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

newBlockC ::
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadExtra m) =>
  CacheX m ()
newBlockC =
  inSync >>= \s -> when s . void . withLock "lock" $ do
    get_best_block_node >>= \case
      Nothing -> $(logErrorS) "Cache" "No best block available"
      Just best_block_node ->
        cacheGetHead >>= \case
          Nothing -> do
            $(logInfoS) "Cache" "Initializing best cache block"
            importBlockC $ headerHash best_block_node.header
          Just cache_head_hash ->
            get_block_node cache_head_hash >>= \case
              Nothing -> do
                $(logErrorS) "Cache" $
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

importBlockC ::
  (MonadUnliftIO m, StoreReadExtra m, MonadLoggerIO m) =>
  BlockHash ->
  CacheX m ()
importBlockC bh =
  lift (getBlock bh) >>= \case
    Just bd -> do
      let ths = bd.txs
      tds <- sortTxData . catMaybes <$> mapM (lift . getTxData) ths
      $(logDebugS) "Cache" $
        "Importing "
          <> cs (show (length tds))
          <> " transactions from block "
          <> blockHashToHex bh
      importMultiTxC tds
      $(logDebugS) "Cache" $
        "Done importing "
          <> cs (show (length tds))
          <> " transactions from block "
          <> blockHashToHex bh
      cacheSetHead bd
    Nothing -> do
      $(logErrorS) "Cache" $
        "Could not get block: "
          <> blockHashToHex bh
      throwIO . LogicError . cs $
        "Could not get block: "
          <> blockHashToHex bh

importMultiTxC ::
  (MonadUnliftIO m, StoreReadExtra m, MonadLoggerIO m) =>
  [TxData] ->
  CacheX m ()
importMultiTxC txs = do
  ctx <- lift getCtx
  $(logDebugS) "Cache" $ "Processing " <> cs (show (length txs)) <> " txs"
  $(logDebugS) "Cache" $
    "Getting address information for "
      <> cs (show (length (alladdrs ctx)))
      <> " addresses"
  addrmap <- getaddrmap ctx
  let addrs = HashMap.keys addrmap
  $(logDebugS) "Cache" $
    "Getting balances for "
      <> cs (show (HashMap.size addrmap))
      <> " addresses"
  balmap <- getbalances addrs
  $(logDebugS) "Cache" $
    "Getting unspent data for "
      <> cs (show (length (allops ctx)))
      <> " outputs"
  unspentmap <- getunspents ctx
  gap <- lift getMaxGap
  now <- round <$> liftIO getPOSIXTime :: (MonadIO m) => m Int
  let xpubs = allxpubsls addrmap
  forM_ (zip [(1 :: Int) ..] xpubs) $ \(i, xpub) -> do
    xpubtxt <- xpubText xpub
    $(logDebugS) "Cache" $
      "Affected xpub "
        <> cs (show i)
        <> "/"
        <> cs (show (length xpubs))
        <> ": "
        <> xpubtxt
  addrs' <- do
    $(logDebugS) "Cache" $
      "Getting xpub balances for "
        <> cs (show (length xpubs))
        <> " xpubs"
    xmap <- getxbals xpubs
    let addrmap' = faddrmap (HashMap.keysSet xmap) addrmap
    $(logDebugS) "Cache" "Starting Redis import pipeline"
    runRedis $ do
      x <- redisImportMultiTx ctx addrmap' unspentmap txs
      y <- redisUpdateBalances addrmap' balmap
      z <- redisTouchKeys now (HashMap.keys xmap)
      return $ x >> y >> z >> return ()
    $(logDebugS) "Cache" "Completed Redis pipeline"
    return $ getNewAddrs ctx gap xmap (HashMap.elems addrmap')
  cacheAddAddresses addrs'
  where
    alladdrsls ctx = HashSet.toList (alladdrs ctx)
    faddrmap xmap = HashMap.filter (\a -> a.spec `elem` xmap)
    getaddrmap ctx =
      HashMap.fromList
        . catMaybes
        . zipWith (\a -> fmap (a,)) (alladdrsls ctx)
        <$> cacheGetAddrsInfo (alladdrsls ctx)
    getunspents ctx =
      HashMap.fromList
        . catMaybes
        . zipWith (\p -> fmap (p,)) (allops ctx)
        <$> lift (mapM getUnspent (allops ctx))
    getbalances addrs =
      HashMap.fromList . zip addrs <$> mapM (lift . getDefaultBalance) addrs
    getxbals xpubs = do
      bals <- runRedis . fmap sequence . forM xpubs $ \xpub -> do
        bs <- redisGetXPubBalances xpub
        return $ (,) xpub <$> bs
      return $ HashMap.filter (not . null) (HashMap.fromList bals)
    allops ctx =
      map snd $
        concatMap (txInputs ctx) txs
          <> concatMap (txOutputs ctx) txs
    alladdrs ctx =
      HashSet.fromList $
        map fst $
          concatMap (txInputs ctx) txs
            <> concatMap (txOutputs ctx) txs
    allxpubsls = HashSet.toList . allxpubs
    allxpubs =
      HashSet.fromList . map (.spec) . HashMap.elems

redisImportMultiTx ::
  (Monad f, RedisCtx m f) =>
  Ctx ->
  HashMap Address AddressXPub ->
  HashMap OutPoint Unspent ->
  [TxData] ->
  m (f ())
redisImportMultiTx ctx addrmap unspentmap tds = do
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
    txaddrops td = txInputs ctx td <> txOutputs ctx td

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

cacheAddAddresses ::
  (StoreReadExtra m, MonadUnliftIO m, MonadLoggerIO m) =>
  [(Address, AddressXPub)] ->
  CacheX m ()
cacheAddAddresses [] = $(logDebugS) "Cache" "No further addresses to add"
cacheAddAddresses addrs = do
  ctx <- lift getCtx
  $(logDebugS) "Cache" $
    "Adding " <> cs (show (length addrs)) <> " new generated addresses"
  $(logDebugS) "Cache" "Getting balances"
  balmap <- HashMap.fromListWith (<>) <$> mapM (uncurry getbal) addrs
  $(logDebugS) "Cache" "Getting unspent outputs"
  utxomap <- HashMap.fromListWith (<>) <$> mapM (uncurry getutxo) addrs
  $(logDebugS) "Cache" "Getting transactions"
  txmap <- HashMap.fromListWith (<>) <$> mapM (uncurry gettxmap) addrs
  $(logDebugS) "Cache" "Running Redis pipeline"
  runRedis $ do
    a <- forM (HashMap.toList balmap) (uncurry redisAddXPubBalances)
    b <- forM (HashMap.toList utxomap) (uncurry redisAddXPubUnspents)
    c <- forM (HashMap.toList txmap) (uncurry redisAddXPubTxs)
    return $ sequence_ a >> sequence_ b >> sequence_ c
  $(logDebugS) "Cache" "Completed Redis pipeline"
  let xpubs =
        HashSet.toList
          . HashSet.fromList
          . map (.spec)
          $ Map.elems amap
  $(logDebugS) "Cache" "Getting xpub balances"
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

syncNewTxC ::
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadExtra m) =>
  [TxHash] ->
  CacheX m ()
syncNewTxC ths =
  inSync >>= \s -> when s . void . withLock "lock" $ do
    txs <- catMaybes <$> mapM (lift . getTxData) ths
    unless (null txs) $ do
      forM_ txs $ \tx ->
        $(logDebugS) "Cache" $
          "Synchronizing transaction: " <> txHashToHex (txHash tx.tx)
      importMultiTxC txs

syncMempoolC ::
  (MonadUnliftIO m, MonadLoggerIO m, StoreReadExtra m) =>
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
      $(logDebugS) "Cache" $
        "Synchronizing " <> cs (show (length txs)) <> " mempool transactions"
      importMultiTxC txs

cacheGetMempool :: (MonadLoggerIO m) => CacheX m [(UnixTime, TxHash)]
cacheGetMempool = runRedis redisGetMempool

cacheGetHead :: (MonadLoggerIO m) => CacheX m (Maybe BlockHash)
cacheGetHead = runRedis redisGetHead

cacheSetHead :: (MonadLoggerIO m, StoreReadBase m) => BlockData -> CacheX m ()
cacheSetHead bd = do
  $(logDebugS) "Cache" $ "Cache head set to: " <> blockHashToHex bh
  void $ runRedis (redisSetHead bh)
  withMetrics $ \s -> setGauge s.height (fromIntegral bd.height)
  where
    bh = headerHash bd.header

cacheGetAddrsInfo ::
  (MonadLoggerIO m) => [Address] -> CacheX m [Maybe AddressXPub]
cacheGetAddrsInfo as = runRedis (redisGetAddrsInfo as)

redisAddToMempool :: (Applicative f, RedisCtx m f) => [TxRef] -> m (f Integer)
redisAddToMempool [] = return (pure 0)
redisAddToMempool btxs =
  zadd mempoolSetKey $
    map
      (\btx -> (blockRefScore btx.block, encode btx.txid))
      btxs

redisRemFromMempool ::
  (Applicative f, RedisCtx m f) => [TxHash] -> m (f Integer)
redisRemFromMempool xs =
  case NonEmpty.nonEmpty xs of
    Nothing -> return (pure 0)
    Just xs' -> zrem mempoolSetKey $ NonEmpty.map encode xs'

redisSetAddrInfo ::
  (Functor f, RedisCtx m f) => Address -> AddressXPub -> m (f ())
redisSetAddrInfo a i = void <$> Redis.set (addrPfx <> encode a) (encode i)

cacheDelXPubs ::
  (MonadLoggerIO m, StoreReadBase m) =>
  [XPubSpec] ->
  CacheT m Integer
cacheDelXPubs xpubs = ReaderT $ \case
  Just cache -> runReaderT (delXPubKeys xpubs) cache
  Nothing -> return 0

delXPubKeys ::
  (MonadLoggerIO m, StoreReadBase m) =>
  [XPubSpec] ->
  CacheX m Integer
delXPubKeys [] = return 0
delXPubKeys xpubs = do
  forM_ xpubs $ \x -> do
    xtxt <- xpubText x
    $(logDebugS) "Cache" $ "Deleting xpub: " <> xtxt
  xbals <-
    runRedis . fmap sequence . forM xpubs $ \xpub -> do
      bs <- redisGetXPubBalances xpub
      return $ (xpub,) <$> bs
  runRedis $ fmap sum . sequence <$> forM xbals (uncurry redisDelXPubKeys)

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

redisAddXPubTxs ::
  (Applicative f, RedisCtx m f) => XPubSpec -> [TxRef] -> m (f Integer)
redisAddXPubTxs _ [] = return (pure 0)
redisAddXPubTxs xpub btxs =
  zadd (txSetPfx <> encode xpub) $
    map (\t -> (blockRefScore t.block, encode t.txid)) btxs

redisRemXPubTxs ::
  (Applicative f, RedisCtx m f) => XPubSpec -> [TxHash] -> m (f Integer)
redisRemXPubTxs xpub txhs =
  case NonEmpty.nonEmpty txhs of
    Nothing -> return (pure 0)
    Just txhs' -> zrem (txSetPfx <> encode xpub) (NonEmpty.map encode txhs')

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

redisRemXPubUnspents ::
  (Applicative f, RedisCtx m f) => XPubSpec -> [OutPoint] -> m (f Integer)
redisRemXPubUnspents _ [] =
  return (pure 0)
redisRemXPubUnspents xpub ops =
  case NonEmpty.nonEmpty ops of
    Nothing -> return (pure 0)
    Just ops' -> zrem (utxoPfx <> encode xpub) (NonEmpty.map encode ops')

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

redisSetHead :: (RedisCtx m f) => BlockHash -> m (f Redis.Status)
redisSetHead bh = Redis.set bestBlockKey (encode bh)

redisGetAddrsInfo ::
  (Monad f, RedisCtx m f) => [Address] -> m (f [Maybe AddressXPub])
redisGetAddrsInfo [] = return (pure [])
redisGetAddrsInfo as = do
  is <- mapM (\a -> Redis.get (addrPfx <> encode a)) as
  return $ do
    is' <- sequence is
    return $ map (eitherToMaybe . decode =<<) is'

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

sortTxData :: [TxData] -> [TxData]
sortTxData tds =
  let txm = Map.fromList (map (\d -> (txHash d.tx, d)) tds)
      ths = map (txHash . snd) (sortTxs (map (.tx) tds))
   in mapMaybe (`Map.lookup` txm) ths

txInputs :: Ctx -> TxData -> [(Address, OutPoint)]
txInputs ctx td =
  let is = td.tx.inputs
      ps = I.toAscList td.prevs
      as = map (scriptToAddressBS ctx . (.script) . snd) ps
      f (Right a) i = Just (a, i.outpoint)
      f (Left _) _ = Nothing
   in catMaybes (zipWith f as is)

txOutputs :: Ctx -> TxData -> [(Address, OutPoint)]
txOutputs ctx td =
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
      as = map (scriptToAddressBS ctx . (.script)) td.tx.outputs
      f (Right a) p = Just (a, p)
      f (Left _) _ = Nothing
   in catMaybes (zipWith f as ps)

redisGetHead :: (Functor f, RedisCtx m f) => m (f (Maybe BlockHash))
redisGetHead = do
  x <- Redis.get bestBlockKey
  return $ (eitherToMaybe . decode =<<) <$> x

redisGetMempool :: (Applicative f, RedisCtx m f) => m (f [(UnixTime, TxHash)])
redisGetMempool = do
  xs <- getFromSortedSet mempoolSetKey Nothing 0 0
  return $ map (uncurry f) <$> xs
  where
    f t s = ((scoreBlockRef s).timestamp, t)

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

cacheNewBlock :: (MonadIO m) => CacheWriter -> m ()
cacheNewBlock = send CacheNewBlock

cacheNewTx :: (MonadIO m) => TxHash -> CacheWriter -> m ()
cacheNewTx = send . CacheNewTx

cacheSyncMempool :: (MonadIO m) => CacheWriter -> m ()
cacheSyncMempool = query CacheSyncMempool
