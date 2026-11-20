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
-- They read the chain, which is all that the database has: the mempool is
-- the pool that the fleet shares in Redis. "Haskoin.Store.Database.Query.Pooled"
-- puts the two together, and is what the store answers with.
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
  ( -- * Blocks
    getBestBlock,
    getBlocksAtHeight,
    getBlock,
    getReplaced,
    getReplacedAt,

    -- * Transactions
    getTxData,
    getTxRecord,
    getManyTxData,
    getManyTxBytes,
    liveTxBytes,
    getChainSpender,
    getChainSpenders,
    withChainSpenders,
    getNumTxData,
    getSpender,

    -- * Addresses
    getBalance,
    getTokBal,
    getAddressTokens,
    getCategoryHolders,
    getCategoryTxs,
    getCategoryUnspents,
    getBalances,
    getUnspent,
    getAddressTxs,
    getAddressesTxs,
    getAddressUnspents,
    getAddressesUnspents,

    -- * Extended public keys
    xPubBals,
    xPubUnspents,
    xPubTxs,
    xPubTxCount,
    getAddressesTxCount,

    -- * Building blocks

    -- | What the queries above are made of, for the queries of
    -- "Haskoin.Store.Database.Query.Pooled", which read the same column
    -- families for the part of their answers that is in the chain.
    count,
    withManyIters,
    retrieveManyCF,
    retrieveManyBytesCF,
    scan,
    scanFrom,
    joinConduits,
    usedAddresses,
  )
where

import Conduit
  ( ConduitT,
    concatC,
    concatMapC,
    dropWhileC,
    lift,
    liftIO,
    mapC,
    runConduit,
    sinkList,
    yield,
    (.|),
  )
import Control.Exception (mask_)
import Control.Monad (forM, forM_, when, (>=>))
import Data.Bits ((.&.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Default (def)
import Data.Functor ((<&>))
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.IntMap.Strict qualified as IntMap
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, listToMaybe)
import Data.Ord (Down (..))
import Data.Serialize (Serialize, decode, encode)
import Data.Word (Word32, Word64)
import Database.RocksDB
  ( ChunkConfig (..),
    ColumnFamily,
    IterConfig (..),
    Iterator,
    createIteratorConfig,
    getCF,
    iterChunk,
    iterSeek,
    multiGetCF,
    prefixUpperBound,
    withIterConfig,
  )
import Database.RocksDB.Query
  ( matchingAsListCF,
    retrieve,
    retrieveCF,
  )
import Haskoin
  ( Address,
    BlockHash,
    BlockHeight,
    OutPoint (..),
    Tx (..),
    TxHash,
    eitherToMaybe,
    scriptToAddressBS,
  )
import Haskoin.Store.Common
  ( Limits (..),
    Replaced,
    Start (..),
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
import Haskoin.Store.Database.Slices (blockData, txBytes)
import Haskoin.Store.Database.Types
import Haskoin.Store.Exception (bracket, bracket_)
import System.Metrics.StatsD (decrementGauge, incrementCounter, incrementGauge)

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

-- | What is kept of a transaction that a block replaced ('Replaced').
getReplaced :: DatabaseReader -> TxHash -> IO (Maybe Replaced)
getReplaced r h = retrieveCF r.db (blockCF r.db) (ReplacedKey h)

-- | The transactions that the block at a height replaced.
getReplacedAt :: DatabaseReader -> BlockHeight -> IO [TxHash]
getReplacedAt r h = do
  rows <- matchingAsListCF r.db (heightCF r.db) (ReplacedAtKeyH h)
  return [t | (ReplacedAtKey _ t, ()) <- rows]

-- | Data of a block. Its list of transactions is decoded when it is used,
-- as "Haskoin.Store.Database.Slices" explains: most reads of a block want
-- its height or its header.
getBlock :: DatabaseReader -> BlockHash -> IO (Maybe BlockData)
getBlock r h =
  getCF r.db (blockCF r.db) (encode (BlockKey h)) >>= \case
    Nothing -> return Nothing
    Just bytes -> do
      count r $ \s -> incrementCounter s.dataBlockCount 1
      return (Just (either error id (blockData bytes)))

-------------------------------------------------------------------------------
-- Transactions
-------------------------------------------------------------------------------

-- | Data of a transaction, including deleted ones, with the inputs of
-- blocks that spend its outputs.
getTxData :: DatabaseReader -> TxHash -> IO (Maybe TxData)
getTxData r th =
  getTxRecord r th >>= \case
    Nothing -> return Nothing
    Just t -> do
      count r $ \s -> incrementCounter s.dataTxCount 1
      listToMaybe <$> withChainSpenders r [(th, t)]

-- | The record of a transaction as the database has it, including deleted
-- ones. Since version 22 the spenders of its outputs are not in it
-- ('getChainSpenders'); a database of version 20 or 21 has them there.
-- This is what the rules that write the database read: they must not
-- write back what was read from somewhere else.
getTxRecord :: DatabaseReader -> TxHash -> IO (Maybe TxData)
getTxRecord r th = retrieveCF r.db (txCF r.db) (TxKey th)

-- | Data of many transactions, in the order of the hashes, read with one
-- multi-get, with the inputs of blocks that spend their outputs.
getManyTxData :: DatabaseReader -> [TxHash] -> IO [Maybe TxData]
getManyTxData r ths = do
  found <- retrieveManyCF r (txCF r.db) (map TxKey ths)
  count r $ \s -> incrementCounter s.dataTxCount (length (catMaybes found))
  spent <- withChainSpenders r [(h, td) | (h, Just td) <- zip ths found]
  return (fill found spent)
  where
    fill (Just _ : more) (td : tds) = Just td : fill more tds
    fill (Nothing : more) tds = Nothing : fill more tds
    fill _ _ = []

-- | Transactions as stored, in the format of the network, without the
-- deleted ones: slices of their values ("Haskoin.Store.Database.Slices"),
-- without the spenders of their outputs.
getManyTxBytes :: DatabaseReader -> [TxHash] -> IO [Maybe ByteString]
getManyTxBytes r ths = do
  found <- map (>>= liveTxBytes) <$> retrieveManyBytesCF r (txCF r.db) (map TxKey ths)
  count r $ \s -> incrementCounter s.dataTxCount (length (catMaybes found))
  return found

-- | The transaction of a stored 'TxData', unless it is deleted.
liveTxBytes :: ByteString -> Maybe ByteString
liveTxBytes value = case txBytes value of
  Right (bytes, False) -> Just bytes
  Right (_, True) -> Nothing
  Left e -> error ("liveTxBytes: " <> e)

-- | The input of a block that spends an output, if one does: one read by
-- key, of the group of outputs that it is in, in a database of version 22.
getChainSpender :: DatabaseReader -> OutPoint -> IO (Maybe Spender)
getChainSpender r p =
  retrieveCF r.db (spenderCF r.db) (spenderKey p) <&> \case
    Just (SpenderVal m) -> IntMap.lookup (fromIntegral p.index) m
    Nothing -> Nothing

-- | The inputs of blocks that spend the outputs of a transaction, by
-- output, read with one scan of the keys of the transaction.
getChainSpenders :: DatabaseReader -> TxHash -> IO (IntMap.IntMap Spender)
getChainSpenders r h = do
  rows <- matchingAsListCF r.db (spenderCF r.db) (SpenderKeyS h)
  return (IntMap.unions [m | (_ :: SpenderKey, SpenderVal m) <- rows])

-- | Records of transactions, with the inputs of blocks that spend their
-- outputs added to whatever spenders the records have themselves, which
-- those of a database older than version 22 do.
--
-- The spenders are read by the keys of the groups of outputs, all the
-- groups of all the transactions in one multi-get. A group is sixteen
-- outputs, so nearly every transaction is one key, and a page of a
-- hundred transactions is one more read of a hundred keys. A scan would
-- need an iterator for each transaction, at ten microseconds apiece. Only
-- a transaction with more groups than 'wideTx' is scanned, so that a
-- payout to thousands is not hundreds of keys to ask for.
--
-- Each record comes with the hash of its transaction, which whoever read
-- it by its key has: working it out again from the record is two rounds
-- of SHA-256 over the whole transaction, and cost more than everything
-- else here together when this was first written without it.
withChainSpenders :: DatabaseReader -> [(TxHash, TxData)] -> IO [TxData]
withChainSpenders _ [] = return []
withChainSpenders r tds = do
  let groups td = (fromIntegral (length td.tx.outputs) + spenderGroup - 1) `div` spenderGroup
      keys (h, td) = [SpenderKey h g | g <- take (fromIntegral (groups td)) [0 ..]]
      narrow (_, td) = groups td <= wideTx
      asked = concatMap keys (filter narrow tds)
  found <- case asked of
    [] -> return []
    _ -> retrieveManyCF r (spenderCF r.db) asked
  let byKey = Map.fromList [(k, m) | (k, Just (SpenderVal m)) <- zip asked found]
      keyed x = IntMap.unions [m | k <- keys x, Just m <- [Map.lookup k byKey]]
  forM tds $ \x@(h, td) -> do
    chain <- if narrow x then return (keyed x) else getChainSpenders r h
    return (td :: TxData) {spenders = IntMap.union td.spenders chain}

-- | The number of groups of outputs above which the spenders of a
-- transaction are read with a scan instead of by key.
wideTx :: Word32
wideTx = 16

-- | Transactions whose hash, read as a number, matches a short identifier of
-- the Blockchain.info API. There may be more than one.
getNumTxData :: DatabaseReader -> Word64 -> IO [TxData]
getNumTxData r i = do
  let (sk, w) = decodeTxKey i
  ls <- matchingAsListCF r.db (txCF r.db) (TxKeyS sk)
  let f h =
        let bs = encode h
            b = BS.head (BS.drop 6 bs)
            w' = b .&. 0xf8
         in w == w'
      txs = [(h, td) | (TxKey h, td) <- ls, f h]
  count r $ \s -> incrementCounter s.dataTxCount (length txs)
  withChainSpenders r txs

-- | Transaction that spends an output, if any: by its key, or, in a
-- database older than version 22, from the record of the transaction that
-- made the output.
getSpender :: DatabaseReader -> OutPoint -> IO (Maybe Spender)
getSpender r op =
  getChainSpender r op >>= \case
    Just s -> return (Just s)
    Nothing ->
      getTxRecord r op.hash <&> \case
        Nothing -> Nothing
        Just td -> IntMap.lookup (fromIntegral op.index) td.spenders

-------------------------------------------------------------------------------
-- Addresses
-------------------------------------------------------------------------------

-- | The tokens of a category that an address holds in the chain, if it
-- ever held any (CashTokens).
getTokBal :: DatabaseReader -> Address -> TxHash -> IO (Maybe TokBalVal)
getTokBal r a c = retrieveCF r.db (tokBalCF r.db) (TokBalKey a c)

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
  vals <- retrieveManyCF r (balanceCF r.db) (map BalKey as)
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
      return (Just (valToUnspent r.net p val))

-- | Transactions of an address, newest first.
getAddressTxs :: DatabaseReader -> Address -> Limits -> IO [TxRef]
getAddressTxs r a limits = do
  txs <- withIterCFStat r (addrTxCF r.db) (AddrTxKeyA a) $ \it ->
    runConduit $
      addressConduit r a limits.start it
        .| applyLimitsC limits
        .| sinkList
  count r $ \s -> incrementCounter s.dataAddrTxCount (length txs)
  return txs

-- | Transactions of any of the addresses, newest first, each once.
getAddressesTxs :: DatabaseReader -> [Address] -> Limits -> IO [TxRef]
getAddressesTxs r addrs limits =
  withManyIters r (addrTxCF r.db) (map AddrTxKeyA addrs) $ \its -> do
    txs <- joinConduits txRefKey (zipWith (\a -> addressConduit r a limits.start) addrs its) limits
    count r $ \s -> incrementCounter s.dataAddrTxCount (length txs)
    return txs

-- | Unspent outputs of an address, newest first.
getAddressUnspents :: DatabaseReader -> Address -> Limits -> IO [Unspent]
getAddressUnspents r a limits = do
  us <- withIterCFStat r (addrOutCF r.db) (AddrOutKeyA a) $ \it ->
    runConduit $
      unspentConduit r a limits.start it
        .| applyLimitsC limits
        .| sinkList
  count r $ \s -> incrementCounter s.dataUnspentCount (length us)
  return us

-- | Unspent outputs of any of the addresses, newest first.
getAddressesUnspents :: DatabaseReader -> [Address] -> Limits -> IO [Unspent]
getAddressesUnspents r addrs limits =
  withManyIters r (addrOutCF r.db) (map AddrOutKeyA addrs) $ \its -> do
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
        scan it (AddrTxKeyA a)
      Just (AtBlock bh) ->
        scanFrom it (AddrTxKeyA a) (AddrTxKeyB a (BlockRef bh maxBound))
      Just (AtTx txh) ->
        lift (getTxRecord r txh) >>= \case
          Just TxData {block = b@BlockRef {}} ->
            scanFrom it (AddrTxKeyA a) (AddrTxKeyB a b)
          Just TxData {block = b@MemRef {}} ->
            -- Mempool transactions that arrived in the same second are
            -- keyed by hash, so seek to the time and drop those before it.
            let start = txRefKey (TxRef b txh)
                before (AddrTxKey _ t, ()) = txRefKey t > start
                before _ = undefined
             in scanFrom it (AddrTxKeyA a) (AddrTxKeyB a b)
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
  x .| mapC (uncurry (toUnspent r.net)) .| concatC
  where
    x = case s of
      Nothing ->
        scan it (AddrOutKeyA a)
      Just (AtBlock h) ->
        scanFrom it (AddrOutKeyA a) (AddrOutKeyB a (BlockRef h maxBound))
      Just (AtTx txh) ->
        lift (getTxRecord r txh) >>= \case
          Just TxData {block = b@BlockRef {}} ->
            scanFrom it (AddrOutKeyA a) (AddrOutKeyB a b)
          Just TxData {block = b@MemRef {}} ->
            -- The outputs of the start transaction come first among those
            -- of the same second whose hashes are not smaller.
            let start = (b, Down (encode (OutPoint txh 0)))
                before (AddrOutKey _ ref p, _) = (ref, Down (encode p)) > start
                before _ = undefined
             in scanFrom it (AddrOutKeyA a) (AddrOutKeyB a b)
                  .| (dropWhileC before >> mapC id)
          Nothing -> return ()

-------------------------------------------------------------------------------
-- Tokens of Bitcoin Cash
-------------------------------------------------------------------------------

-- | The token balances of an address in the chain, one per category, in
-- the order of the categories' bytes.
getAddressTokens :: DatabaseReader -> Address -> IO [(TxHash, TokBalVal)]
getAddressTokens r a =
  withIterCFStat r (tokBalCF r.db) (TokBalKeyA a) $ \it ->
    runConduit $ scan it (TokBalKeyA a) .| concatMapC (\case (TokBalKey _ c, v) -> [(c, v)]; _ -> []) .| sinkList

-- | The holders of a category in the chain, one token balance per address,
-- in the order of the addresses' bytes, with an offset and a limit.
getCategoryHolders :: DatabaseReader -> TxHash -> Limits -> IO [(Address, TokBalVal)]
getCategoryHolders r c limits =
  withIterCFStat r (holderCF r.db) (HolderKeyC c) $ \it ->
    runConduit $
      scan it (HolderKeyC c)
        .| concatMapC (\case (HolderKey _ a, v) -> [(a, v)]; _ -> [])
        .| applyLimitsC limits
        .| sinkList

-- | Transactions that moved tokens of a category, in the chain, newest
-- first, from a block or a transaction as for an address.
getCategoryTxs :: DatabaseReader -> TxHash -> Limits -> IO [TxRef]
getCategoryTxs r c limits =
  withIterCFStat r (catTxCF r.db) (CatTxKeyC c) $ \it ->
    runConduit $
      from it .| concatMapC (\case (CatTxKey _ t, ()) -> [t]; _ -> []) .| applyLimitsC limits .| sinkList
  where
    from it = case limits.start of
      Nothing -> scan it (CatTxKeyC c)
      Just (AtBlock bh) -> scanFrom it (CatTxKeyC c) (CatTxKeyB c (BlockRef bh maxBound))
      Just (AtTx txh) ->
        lift (getTxRecord r txh) >>= \case
          Just TxData {block = b@BlockRef {}} -> scanFrom it (CatTxKeyC c) (CatTxKeyB c b)
          _ -> return ()

-- | Unspent outputs that hold tokens of a category, in the chain, newest
-- first, from a block or a transaction as for an address.
getCategoryUnspents :: DatabaseReader -> TxHash -> Limits -> IO [Unspent]
getCategoryUnspents r c limits =
  withIterCFStat r (catOutCF r.db) (CatOutKeyC c) $ \it ->
    runConduit $
      from it .| concatMapC (\case (CatOutKey _ b p, v) -> [unspent b p v]; _ -> []) .| applyLimitsC limits .| sinkList
  where
    unspent block outpoint (OutVal value script) =
      Unspent {block, outpoint, value, script, address = eitherToMaybe (scriptToAddressBS r.net script)}
    from it = case limits.start of
      Nothing -> scan it (CatOutKeyC c)
      Just (AtBlock bh) -> scanFrom it (CatOutKeyC c) (CatOutKeyB c (BlockRef bh maxBound))
      Just (AtTx txh) ->
        lift (getTxRecord r txh) >>= \case
          Just TxData {block = b@BlockRef {}} -> scanFrom it (CatOutKeyC c) (CatOutKeyB c b)
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
  us <- withManyIters r (addrOutCF r.db) (map (AddrOutKeyA . (.balance.address)) cs) $ \its ->
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
  withManyIters r (addrTxCF r.db) (map AddrTxKeyA as) $ \its ->
    fromIntegral <$> countDistinctSuffixes (zip (map (encode . AddrTxKeyA) as) its)

-- | Addresses of the balances that were ever used.
usedAddresses :: [XPubBal] -> [Address]
usedAddresses = map (.address) . filter (not . nullBalance) . map (.balance)

-- | The keys of one prefix that an iterator walks, with those already read
-- from it: 'countDistinctSuffixes' takes them one at a time, and the
-- iterator gives them a few hundred at a time.
data Suffixes = Suffixes
  { prefix :: !BS.ByteString,
    iterator :: !Iterator,
    -- | keys read and not yet taken, without their prefix
    ahead :: ![BS.ByteString],
    -- | whether the iterator has more of them
    more :: !Bool
  }

-- | Number of distinct keys, after their prefix, in a set of iterators that
-- each walk the keys with one prefix, in order.
--
-- The keys are read in chunks, one foreign call for a few hundred of them.
-- Read one at a time, each key cost the thread its core for the call and a
-- wait to get one back, which on a busy server was most of the time of
-- counting the transactions of a large wallet.
countDistinctSuffixes :: [(BS.ByteString, Iterator)] -> IO Int
countDistinctSuffixes streams = do
  heads <- mapM start streams
  go 0 (Map.fromListWith (<>) [(k, [x]) | (k, x) <- catMaybes heads])
  where
    start (pfx, it) = iterSeek it pfx >> next (Suffixes pfx it [] True)
    -- the next key of a stream, and the stream after it
    next s = case s.ahead of
      k : ks -> return (Just (k, s {ahead = ks} :: Suffixes))
      []
        | s.more -> do
            (rows, more) <- iterChunk s.iterator def {chunkPrefix = s.prefix}
            case map (BS.drop (BS.length s.prefix) . fst) rows of
              [] -> return Nothing
              k : ks -> return (Just (k, s {ahead = ks, more = more} :: Suffixes))
        | otherwise -> return Nothing
    go !n m = case Map.minViewWithKey m of
      Nothing -> return n
      Just ((_, xs), m') -> do
        nexts <- catMaybes <$> mapM next xs
        go (n + 1) (foldr (\(k, x) -> Map.insertWith (<>) k [x]) m' nexts)

-------------------------------------------------------------------------------
-- Iterators and multi-gets
-------------------------------------------------------------------------------

-- | Run an action with an iterator on a column family, counting it in the
-- metrics.
--
-- The iterator is for the keys that start with a prefix, and RocksDB
-- stops it where they end ('scanOf').
withIterCFStat :: (Serialize k) => DatabaseReader -> ColumnFamily -> k -> (Iterator -> IO a) -> IO a
withIterCFStat r cf prefix f =
  bracket_ (gauge r 1) (gauge r (-1)) $ withIterConfig r.db (Just cf) (scanOf prefix) f

-- | An iterator for the keys that start with a prefix, such as those of one
-- address: one that RocksDB itself stops at the end of them.
--
-- A scan that stops by looking at the key that it has reached makes
-- RocksDB find that key first, and if the keys after the prefix were
-- deleted, it steps over the marker of every one of them to get there.
-- With an upper bound it stops at the bound. The layout of the chain keeps
-- such runs of deleted keys short as things are, so this is for the day that they do not: after a
-- reorganisation that reverts many blocks, or a change of layout.
scanOf :: (Serialize k) => k -> IterConfig
scanOf prefix = def {upperBound = prefixUpperBound (encode prefix)}

-- | Run an action with this many iterators on a column family. They are
-- opened and closed in a single bracket rather than one bracket each: each
-- bracket leaves a frame on the stack, and every call into RocksDB through
-- an iterator walks the stack, so with one bracket per address an xpub of a
-- hundred addresses made each step of each iterator a hundred times slower.
--
-- There is one iterator for each of the prefixes given, in their order,
-- and each is bounded to the keys of its prefix ('scanOf').
withManyIters :: (Serialize k) => DatabaseReader -> ColumnFamily -> [k] -> ([Iterator] -> IO a) -> IO a
withManyIters r cf prefixes f =
  bracket_ (gauge r i) (gauge r (negate i)) $
    bracket (newIORef []) (readIORef >=> sequence_) $ \ref -> do
      its <- forM prefixes $ \prefix -> mask_ $ do
        (it, destroy) <- createIteratorConfig r.db (Just cf) (scanOf prefix)
        modifyIORef' ref (destroy :)
        return it
      f its
  where
    i = length prefixes

-- | Change the gauge of open iterators.
gauge :: DatabaseReader -> Int -> IO ()
gauge r n = count r $ \s ->
  if n >= 0 then incrementGauge s.dataIters n else decrementGauge s.dataIters (negate n)

-- | Values of many keys of a column family, read with one multi-get, in
-- the order of the keys.
--
-- One key is read by itself. A multi-get costs about as much as a read to
-- set up and a third of one for each key, as measured, so it is the slower
-- way to read one key, and most requests about one transaction or one
-- address ask for one.
retrieveManyCF :: (Serialize key, Serialize value) => DatabaseReader -> ColumnFamily -> [key] -> IO [Maybe value]
retrieveManyCF r cf keys = map (fmap (either error id . decode)) <$> retrieveManyBytesCF r cf keys

-- | The bytes of the values of many keys, as 'retrieveManyCF' reads them,
-- for values that are read in part ("Haskoin.Store.Database.Slices").
retrieveManyBytesCF :: (Serialize key) => DatabaseReader -> ColumnFamily -> [key] -> IO [Maybe ByteString]
retrieveManyBytesCF _ _ [] = return []
retrieveManyBytesCF r cf [key] = pure <$> getCF r.db cf (encode key)
retrieveManyBytesCF r cf keys = multiGetCF r.db cf (map encode keys)

-- | The entries of the keys that start with a prefix, read in chunks as
-- the consumer asks for them: what 'Database.RocksDB.Query.matching' does,
-- with each chunk read in a safe call into RocksDB.
-- The chunks start at 16 entries, so that a consumer that takes a few
-- makes the database read few, and grow four times at each call up to
-- 256, and to a mebibyte at most.
scan :: (Serialize key, Serialize value) => Iterator -> key -> ConduitT i (key, value) IO ()
scan it base = scanFrom it base base

-- | Like 'scan', but from the second key, or after it if it has no entry:
-- for the pages after the first, which seek straight to where they start.
scanFrom ::
  (Serialize key, Serialize value) =>
  Iterator ->
  key ->
  key ->
  ConduitT i (key, value) IO ()
scanFrom it base start = do
  liftIO (iterSeek it (encode start))
  go 16
  where
    prefix = encode base
    go n = do
      let chunk = def {chunkPrefix = prefix, chunkEntries = n, chunkBytes = 1_048_576}
      (entries, more) <- liftIO (iterChunk it chunk)
      forM_ entries $ \(k, v) -> yield (either error id (decode k), either error id (decode v))
      when more (go (min 256 (n * 4)))

-- | Merge streams sorted newest first, drop duplicates, and apply the offset
-- and limit.
joinConduits :: (Ord k) => (o -> k) -> [ConduitT () o IO ()] -> Limits -> IO [o]
joinConduits key cs l =
  runConduit $ joinDescStreams key cs .| applyLimitsC l .| sinkList
