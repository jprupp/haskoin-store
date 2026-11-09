-- |
-- Module      : Haskoin.Store.Database.Query
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Queries against the RocksDB database, as plain functions in 'IO' that take
-- the 'DatabaseReader' as their first argument.
--
-- = Why plain functions
--
-- The queries used to be the methods of two classes, run in
-- @ReaderT DatabaseReader@, and reached the web server through more layers of
-- @ReaderT@ for the cache. Every caller then
-- had to live in a stack of monad transformers to ask the database anything,
-- and every layer had to lift or re-implement every method. A function that
-- takes the reader and runs in 'IO' can be called from anywhere, needs no
-- instance, and says in its type exactly what it reads from.
--
-- Streams are still conduits, run in 'IO': they are a good fit for merging the
-- sorted indices of many addresses and reading only as far as a page needs.
module Haskoin.Store.Database.Query
  ( -- * All of them
    databaseRead,

    -- * Blocks
    getBestBlock,
    getBlocksAtHeight,
    getBlock,

    -- * Transactions
    getTxData,
    getManyTxData,
    getNumTxData,
    getSpender,

    -- * Addresses
    getBalance,
    getBalances,
    getUnspent,
    getAddressTxs,
    getAddressesTxs,
    getAddressUnspents,
    getAddressesUnspents,

    -- * Mempool
    getMempool,
    getMempoolPage,
    getMempoolBefore,

    -- * Extended public keys
    xPubBals,
    xPubUnspents,
    xPubTxs,
    xPubTxCount,
    getAddressesTxCount,
  )
where

import Conduit
  ( ConduitT,
    concatC,
    concatMapC,
    dropWhileC,
    lift,
    mapC,
    runConduit,
    sinkList,
    takeC,
    takeWhileC,
    yield,
    (.|),
  )
import Control.Monad (replicateM, (>=>))
import Data.Bits ((.&.))
import Data.ByteString qualified as BS
import Data.Default (def)
import Data.Functor ((<&>))
import Data.IntMap.Strict qualified as IntMap
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes)
import Data.Ord (Down (..))
import Data.Serialize (Serialize, decode, encode)
import Data.Word (Word32, Word64)
import Database.RocksDB
  ( ColumnFamily,
    DB,
    Iterator,
    createIterator,
    destroyIterator,
    iterKey,
    iterLast,
    iterNext,
    iterPrev,
    iterSeek,
    iterValid,
    multiGetCF,
    withIterCF,
  )
import Database.RocksDB.Query
  ( matching,
    matchingAsListCF,
    matchingSkip,
    retrieve,
    retrieveCF,
  )
import Haskoin
  ( Address,
    BlockHash,
    BlockHeight,
    OutPoint (..),
    TxHash,
    txHash,
  )
import Haskoin.Store.Common
  ( Limits (..),
    Start (..),
    StoreRead (..),
    applyLimitsC,
    deOffset,
    joinDescStreams,
    mergeDescGroups,
    scanXPubBals,
    txRefKey,
    unspentKey,
  )
import Haskoin.Store.Data
import Haskoin.Store.Database.Reader
import Haskoin.Store.Database.Types
import System.Metrics.StatsD (decrementGauge, incrementCounter, incrementGauge)
import UnliftIO
  ( bracket,
    bracket_,
    mask_,
    modifyIORef',
    newIORef,
    readIORef,
  )

-- | The queries of a database as a 'StoreRead', for code that takes its
-- queries as a value, such as the web server, which puts the Redis cache in
-- front of some of them.
databaseRead :: DatabaseReader -> StoreRead
databaseRead r =
  StoreRead
    { net = r.net,
      ctx = r.ctx,
      getBestBlock = getBestBlock r,
      getBlocksAtHeight = getBlocksAtHeight r,
      getBlock = getBlock r,
      getTxData = getTxData r,
      getManyTxData = getManyTxData r,
      getSpender = getSpender r,
      getBalance = getBalance r,
      getUnspent = getUnspent r,
      getMempool = getMempool r,
      getMempoolPage = getMempoolPage r,
      getAddressesTxs = getAddressesTxs r,
      getAddressesUnspents = getAddressesUnspents r,
      initialGap = r.initGap,
      maxGap = r.maxGap,
      getNumTxData = getNumTxData r,
      getBalances = getBalances r,
      getAddressTxs = getAddressTxs r,
      getAddressUnspents = getAddressUnspents r,
      xPubBals = xPubBals r,
      xPubUnspents = \_ -> xPubUnspents r,
      xPubTxs = \_ -> xPubTxs r,
      xPubTxCount = \_ -> xPubTxCount r
    }

-- | Count something in the metrics, if they are enabled.
count :: DatabaseReader -> (DataMetrics -> IO ()) -> IO ()
count r f = mapM_ f r.metrics

-------------------------------------------------------------------------------
-- Blocks
-------------------------------------------------------------------------------

-- | Hash of the best block, if the database has any.
getBestBlock :: DatabaseReader -> IO (Maybe BlockHash)
getBestBlock r = do
  count r $ \s -> incrementCounter s.dataBestCount 1
  retrieve r.db BestKey

-- | Hashes of the blocks at a height, of the main chain and of side chains.
getBlocksAtHeight :: DatabaseReader -> BlockHeight -> IO [BlockHash]
getBlocksAtHeight r h =
  retrieveCF r.db (heightCF r.db) (HeightKey h) >>= \case
    Nothing -> return []
    Just ls -> do
      count r $ \s -> incrementCounter s.dataBlockCount (length ls)
      return ls

-- | Data of a block.
getBlock :: DatabaseReader -> BlockHash -> IO (Maybe BlockData)
getBlock r h =
  retrieveCF r.db (blockCF r.db) (BlockKey h) >>= \case
    Nothing -> return Nothing
    Just b -> do
      count r $ \s -> incrementCounter s.dataBlockCount 1
      return (Just b)

-------------------------------------------------------------------------------
-- Transactions
-------------------------------------------------------------------------------

-- | Data of a transaction, including deleted ones.
getTxData :: DatabaseReader -> TxHash -> IO (Maybe TxData)
getTxData r th =
  retrieveCF r.db (txCF r.db) (TxKey th) >>= \case
    Nothing -> return Nothing
    Just t -> do
      count r $ \s -> incrementCounter s.dataTxCount 1
      return (Just t)

-- | Data of many transactions, in the order of the hashes, read with one
-- multi-get.
getManyTxData :: DatabaseReader -> [TxHash] -> IO [Maybe TxData]
getManyTxData r ths = do
  tds <- retrieveManyCF r.db (txCF r.db) (map TxKey ths)
  count r $ \s -> incrementCounter s.dataTxCount (length (catMaybes tds))
  return tds

-- | Transactions whose hash, read as a number, matches a short identifier of
-- the Blockchain.info API. There may be more than one.
getNumTxData :: DatabaseReader -> Word64 -> IO [TxData]
getNumTxData r i = do
  let (sk, w) = decodeTxKey i
  ls <- matchingAsListCF r.db (txCF r.db) (TxKeyS sk)
  let f t =
        let bs = encode $ txHash t.tx
            b = BS.head (BS.drop 6 bs)
            w' = b .&. 0xf8
         in w == w'
      txs = filter f $ map snd ls
  count r $ \s -> incrementCounter s.dataTxCount (length txs)
  return txs

-- | Transaction that spends an output, if any.
getSpender :: DatabaseReader -> OutPoint -> IO (Maybe Spender)
getSpender r op =
  getTxData r op.hash <&> \case
    Nothing -> Nothing
    Just td -> IntMap.lookup (fromIntegral op.index) td.spenders

-------------------------------------------------------------------------------
-- Addresses
-------------------------------------------------------------------------------

-- | Balance of an address, if it was ever used.
getBalance :: DatabaseReader -> Address -> IO (Maybe Balance)
getBalance r a = do
  count r $ \s -> incrementCounter s.dataBalanceCount 1
  fmap (valToBalance a) <$> retrieveCF r.db (balanceCF r.db) (BalKey a)

-- | Balances of addresses, in the same order, read with one multi-get.
-- Addresses never used get a zero balance.
getBalances :: DatabaseReader -> [Address] -> IO [Balance]
getBalances _ [] = return []
getBalances r as = do
  count r $ \s -> incrementCounter s.dataBalanceCount (length as)
  vals <- retrieveManyCF r.db (balanceCF r.db) (map BalKey as)
  return (zipWith f as vals)
  where
    f a Nothing = zeroBalance a
    f a (Just v) = valToBalance a v

-- | Unspent output, if it exists and is unspent.
getUnspent :: DatabaseReader -> OutPoint -> IO (Maybe Unspent)
getUnspent r p =
  retrieveCF r.db (unspentCF r.db) (UnspentKey p) >>= \case
    Nothing -> return Nothing
    Just val -> do
      count r $ \s -> incrementCounter s.dataUnspentCount 1
      return (Just (valToUnspent r.net r.ctx p val))

-- | Transactions of an address, newest first.
getAddressTxs :: DatabaseReader -> Address -> Limits -> IO [TxRef]
getAddressTxs r a limits = do
  txs <- withIterCFStat r (addrTxCF r.db) $ \it ->
    runConduit $
      addressConduit r a limits.start it
        .| applyLimitsC limits
        .| sinkList
  count r $ \s -> incrementCounter s.dataAddrTxCount (length txs)
  return txs

-- | Transactions of any of the addresses, newest first, each once.
getAddressesTxs :: DatabaseReader -> [Address] -> Limits -> IO [TxRef]
getAddressesTxs r addrs limits =
  withManyIters r (addrTxCF r.db) (length addrs) $ \its -> do
    txs <- joinConduits txRefKey (zipWith (\a -> addressConduit r a limits.start) addrs its) limits
    count r $ \s -> incrementCounter s.dataAddrTxCount (length txs)
    return txs

-- | Unspent outputs of an address, newest first.
getAddressUnspents :: DatabaseReader -> Address -> Limits -> IO [Unspent]
getAddressUnspents r a limits = do
  us <- withIterCFStat r (addrOutCF r.db) $ \it ->
    runConduit $
      unspentConduit r a limits.start it
        .| applyLimitsC limits
        .| sinkList
  count r $ \s -> incrementCounter s.dataUnspentCount (length us)
  return us

-- | Unspent outputs of any of the addresses, newest first.
getAddressesUnspents :: DatabaseReader -> [Address] -> Limits -> IO [Unspent]
getAddressesUnspents r addrs limits =
  withManyIters r (addrOutCF r.db) (length addrs) $ \its -> do
    uns <- joinConduits unspentKey (zipWith (\a -> unspentConduit r a limits.start) addrs its) limits
    count r $ \s -> incrementCounter s.dataUnspentCount (length uns)
    return uns

-- | Stream the transactions of an address, newest first, from a starting point.
-- If the start is a transaction that is not found, nothing is streamed.
addressConduit ::
  DatabaseReader ->
  Address ->
  Maybe Start ->
  Iterator ->
  ConduitT i TxRef IO ()
addressConduit r a s it =
  pipe .| mapC (uncurry f)
  where
    f (AddrTxKey _ t) () = t
    f _ _ = undefined
    pipe = case s of
      Nothing ->
        matching it (AddrTxKeyA a)
      Just (AtBlock bh) ->
        matchingSkip it (AddrTxKeyA a) (AddrTxKeyB a (BlockRef bh maxBound))
      Just (AtTx txh) ->
        lift (getTxData r txh) >>= \case
          Just TxData {block = b@BlockRef {}} ->
            matchingSkip it (AddrTxKeyA a) (AddrTxKeyB a b)
          Just TxData {block = b@MemRef {}} ->
            -- Mempool transactions that arrived in the same second are
            -- keyed by hash, so seek to the time and drop those before it.
            let start = txRefKey (TxRef b txh)
                before (AddrTxKey _ t, ()) = txRefKey t > start
                before _ = undefined
             in matchingSkip it (AddrTxKeyA a) (AddrTxKeyB a b)
                  .| (dropWhileC before >> mapC id)
          Nothing -> return ()

-- | Stream the unspent outputs of an address, newest first, from a starting
-- point. If the start is a transaction that is not found, nothing is streamed.
unspentConduit ::
  DatabaseReader ->
  Address ->
  Maybe Start ->
  Iterator ->
  ConduitT i Unspent IO ()
unspentConduit r a s it =
  x .| mapC (uncurry (toUnspent r.net r.ctx)) .| concatC
  where
    x = case s of
      Nothing ->
        matching it (AddrOutKeyA a)
      Just (AtBlock h) ->
        matchingSkip it (AddrOutKeyA a) (AddrOutKeyB a (BlockRef h maxBound))
      Just (AtTx txh) ->
        lift (getTxData r txh) >>= \case
          Just TxData {block = b@BlockRef {}} ->
            matchingSkip it (AddrOutKeyA a) (AddrOutKeyB a b)
          Just TxData {block = b@MemRef {}} ->
            -- The outputs of the start transaction come first among those
            -- of the same second whose hashes are not smaller.
            let start = (b, Down (encode (OutPoint txh 0)))
                before (AddrOutKey _ ref p, _) = (ref, Down (encode p)) > start
                before _ = undefined
             in matchingSkip it (AddrOutKeyA a) (AddrOutKeyB a b)
                  .| (dropWhileC before >> mapC id)
          Nothing -> return ()

-------------------------------------------------------------------------------
-- Mempool
-------------------------------------------------------------------------------

-- | The whole mempool, newest first.
getMempool :: DatabaseReader -> IO [(UnixTime, TxHash)]
getMempool r = getMempoolPage r def

-- | Page of the mempool, newest first, from the offset and limit of the
-- 'Limits'; its start is ignored. Reads only the entries of the page from
-- the index by time.
getMempoolPage :: DatabaseReader -> Limits -> IO [(UnixTime, TxHash)]
getMempoolPage r limits = do
  count r $ \s -> incrementCounter s.dataMempoolCount 1
  withIterCF r.db (mempoolCF r.db) $ \it ->
    runConduit $
      mempoolNewestC it
        .| applyLimitsC limits
        .| sinkList

-- | Mempool transactions that entered it before the given time, oldest
-- first, at most as many as given. Reads only those entries of the index.
getMempoolBefore :: DatabaseReader -> UnixTime -> Int -> IO [(UnixTime, TxHash)]
getMempoolBefore r time n =
  withIterCF r.db (mempoolCF r.db) $ \it ->
    runConduit $
      matching it MemTimeKeyS
        .| concatMapC entry
        .| takeWhileC ((< time) . fst)
        .| takeC n
        .| sinkList
  where
    entry (MemTimeKey t h, ()) = Just (t, h)
    entry (MemTimeKeyS, ()) = Nothing

-- | Mempool from the index by time, newest first. The index sorts last in
-- the mempool column family, so seek past its prefix and walk backwards
-- until a key is no longer in it.
mempoolNewestC :: Iterator -> ConduitT i (UnixTime, TxHash) IO ()
mempoolNewestC it = do
  lift $ do
    -- 0x0c is the first prefix after the 0x0b of 'MemTimeKey'
    iterSeek it (BS.singleton 0x0c)
    iterValid it >>= \case
      True -> iterPrev it
      False -> iterLast it
  go
  where
    go =
      lift (iterKey it) >>= \case
        Just k | Right (MemTimeKey t h) <- decode k -> do
          yield (t, h)
          lift (iterPrev it)
          go
        _ -> return ()

-------------------------------------------------------------------------------
-- Extended public keys
-------------------------------------------------------------------------------

-- | Balances of the addresses of an extended public key, derived up to the
-- exact gap (see 'scanXPubBals').
xPubBals :: DatabaseReader -> XPubSpec -> IO [XPubBal]
xPubBals r xpub = do
  Derived ext chg <- derivedAddrs r xpub
  bals <- scanXPubBals (getBalances r) r.initGap r.maxGap ext chg
  count r $ \s -> incrementCounter s.dataXPubBals (length bals)
  return bals

-- | Unspent outputs of an extended public key, newest first, given its
-- balances from 'xPubBals'.
--
-- The first unspent outputs of each address, at most offset plus limit of
-- them, sorted together. The addresses are read in step, so that each is
-- read only as far as the page needs, rather than decoding the whole quota
-- of every address to keep one page of it.
xPubUnspents :: DatabaseReader -> [XPubBal] -> Limits -> IO [XPubUnspent]
xPubUnspents r xbals limits = do
  let quota = case (deOffset limits).limit of
        0 -> Nothing
        l -> Just l
      stream b it =
        unspentConduit r b.balance.address limits.start it
          .| mapC (\u -> XPubUnspent {path = b.path, unspent = u})
  us <- withManyIters r (addrOutCF r.db) (length cs) $ \its ->
    runConduit $
      mergeDescGroups (.unspent.block) (unspentKey . (.unspent)) quota (zipWith stream cs its)
        .| applyLimitsC limits
        .| sinkList
  count r $ \s -> incrementCounter s.dataXPubUnspents (length us)
  return us
  where
    cs = filter ((> 0) . (.balance.utxo)) xbals

-- | Transactions of an extended public key, newest first, given its balances
-- from 'xPubBals'.
xPubTxs :: DatabaseReader -> [XPubBal] -> Limits -> IO [TxRef]
xPubTxs r xbals limits = do
  txs <- getAddressesTxs r (usedAddresses xbals) limits
  count r $ \s -> incrementCounter s.dataXPubTxs (length txs)
  return txs

-- | Number of transactions of an extended public key, given its balances
-- from 'xPubBals'.
--
-- Each transaction is counted once even when it touches several addresses.
-- The entries of an address are sorted, and the same transaction has the same
-- key after the address prefix in every address, so the addresses are merged
-- by those raw key bytes and the distinct ones counted, without decoding any
-- key or reading any value.
xPubTxCount :: DatabaseReader -> [XPubBal] -> IO Word32
xPubTxCount r xbals = do
  count r $ \s -> incrementCounter s.dataXPubTxCount 1
  getAddressesTxCount r (usedAddresses xbals)

-- | Number of distinct transactions of a set of addresses: each transaction
-- is counted once even when it touches several of them. It reads only the
-- keys of the index, once each, as 'xPubTxCount' does, so it costs about as
-- much as reading the references of the transactions once, and much less
-- than paging through them.
getAddressesTxCount :: DatabaseReader -> [Address] -> IO Word32
getAddressesTxCount _ [] = return 0
getAddressesTxCount r as =
  withManyIters r (addrTxCF r.db) (length as) $ \its ->
    fromIntegral <$> countDistinctSuffixes (zip (map (encode . AddrTxKeyA) as) its)

-- | Addresses of the balances that were ever used.
usedAddresses :: [XPubBal] -> [Address]
usedAddresses = map (.address) . filter (not . nullBalance) . map (.balance)

-- | Number of distinct keys, after their prefix, in a set of iterators that
-- each walk the keys with one prefix, in order.
countDistinctSuffixes :: [(BS.ByteString, Iterator)] -> IO Int
countDistinctSuffixes streams = do
  heads <- mapM start streams
  go 0 (Map.fromListWith (<>) [(k, [x]) | (k, x) <- catMaybes heads])
  where
    start x@(pfx, it) = iterSeek it pfx >> current x
    current x@(pfx, it) =
      iterKey it <&> \case
        Just k | pfx `BS.isPrefixOf` k -> Just (BS.drop (BS.length pfx) k, x)
        _ -> Nothing
    advance x@(_, it) = iterNext it >> current x
    go !n m = case Map.minViewWithKey m of
      Nothing -> return n
      Just ((_, xs), m') -> do
        nexts <- catMaybes <$> mapM advance xs
        go (n + 1) (foldr (\(k, x) -> Map.insertWith (<>) k [x]) m' nexts)

-------------------------------------------------------------------------------
-- Iterators and multi-gets
-------------------------------------------------------------------------------

-- | Run an action with an iterator on a column family, counting it in the
-- metrics.
withIterCFStat :: DatabaseReader -> ColumnFamily -> (Iterator -> IO a) -> IO a
withIterCFStat r cf f =
  bracket_ (gauge r 1) (gauge r (-1)) $ withIterCF r.db cf f

-- | Run an action with this many iterators on a column family. They are
-- opened and closed in a single bracket rather than one bracket each: each
-- bracket leaves a frame on the stack, and every call into RocksDB through
-- an iterator walks the stack, so with one bracket per address an xpub of a
-- hundred addresses made each step of each iterator a hundred times slower.
withManyIters :: DatabaseReader -> ColumnFamily -> Int -> ([Iterator] -> IO a) -> IO a
withManyIters r cf i f =
  bracket_ (gauge r i) (gauge r (negate i)) $
    bracket (newIORef []) (readIORef >=> mapM_ destroyIterator) $ \ref -> do
      its <- replicateM i . mask_ $ do
        it <- createIterator r.db (Just cf)
        modifyIORef' ref (it :)
        return it
      f its

-- | Change the gauge of open iterators.
gauge :: DatabaseReader -> Int -> IO ()
gauge r n = count r $ \s ->
  if n >= 0 then incrementGauge s.dataIters n else decrementGauge s.dataIters (negate n)

-- | Values of many keys of a column family, read with one multi-get, in
-- the order of the keys.
retrieveManyCF :: (Serialize key, Serialize value) => DB -> ColumnFamily -> [key] -> IO [Maybe value]
retrieveManyCF db cf keys =
  map (fmap (either error id . decode)) <$> multiGetCF db cf (map encode keys)

-- | Merge streams sorted newest first, drop duplicates, and apply the offset
-- and limit.
joinConduits :: (Ord k) => (o -> k) -> [ConduitT () o IO ()] -> Limits -> IO [o]
joinConduits key cs l =
  runConduit $ joinDescStreams key cs .| applyLimitsC l .| sinkList
