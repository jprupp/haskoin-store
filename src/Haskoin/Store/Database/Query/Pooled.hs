-- |
-- Module      : Haskoin.Store.Database.Query.Pooled
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The queries of the store with a mempool that is not the store's own laid
-- over the chain: the shared pool of a fleet of stores, which
-- "Haskoin.Store.Fleet.Redis" keeps in Redis. See "A fleet that answers as
-- one" in @docs\/layered-database.md@.
--
-- = Why another set of queries
--
-- "Haskoin.Store.Database.Query.Layered" answers from the two areas of one
-- database, read through one snapshot. Stores that share a Redis are to
-- give the same answer to the same request, and their own mempools differ.
-- So each answers from the chain as it was at the block that the fleet
-- agreed on, which every store has the same, and from the one pool that
-- they share. The chain's part of an answer is read as the layered queries
-- read it, with the same code. The mempool's part comes through a
-- t'PoolReads', a handful of functions that say what a pool has: its
-- summaries, the entries of an address by the page, bodies, spenders and
-- its list.
--
-- The alternative was to give the layered queries the same handful of
-- functions for the store's own mempool area. They read that area through
-- iterators that are made once for a request and moved from address to
-- address, which is where their speed comes from, and which a function
-- that returns a page would hide. The two sets share what is the same, and
-- the tests hold this one to the answers of the other: 'localPool' is the
-- store's own mempool area as a t'PoolReads', and with it every query here
-- must answer as the layered one does.
--
-- = What a pool cannot say
--
-- A transaction that left a mempool unconfirmed is kept by the store that
-- deleted it, and by no other. A fleet cannot answer the same about it from
-- every store, so these queries do not know deleted transactions at all: a
-- page that starts at one is empty.
module Haskoin.Store.Database.Query.Pooled
  ( PoolReads (..),
    pooledRead,
    localPool,
  )
where

import Conduit
  ( ConduitT,
    await,
    awaitForever,
    lift,
    mapC,
    runConduit,
    sinkList,
    yield,
    yieldMany,
    (.|),
  )
import Control.Monad (forM)
import Data.Bits ((.&.))
import Data.ByteString qualified as BS
import Data.Functor ((<&>))
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet qualified as HashSet
import Data.IntMap.Strict qualified as IntMap
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, isJust)
import Data.Serialize (encode)
import Data.Word (Word16, Word32, Word64)
import Database.RocksDB (Iterator)
import Database.RocksDB.Query (matchingAsListCF, retrieveCF)
import Haskoin
  ( Address,
    OutPoint (..),
    Tx (..),
    TxHash,
    TxOut (..),
    eitherToMaybe,
    scriptToAddressBS,
    txHash,
  )
import Haskoin.Store.Common
  ( Limits (..),
    Start (..),
    StoreRead (..),
    applyLimitsC,
    deOffset,
    mergeDescGroups,
    notDeleted,
    scanXPubBals,
    txRefKey,
    unspentKey,
  )
import Haskoin.Store.Data
  ( Balance (..),
    BlockRef (..),
    Spender,
    TxData (..),
    TxRef (..),
    UnixTime,
    Unspent (..),
    XPubBal (..),
    XPubUnspent (..),
  )
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered (From (..), chainTxsC, chainUnspentsC, withSpenders)
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    Derived (..),
    addrOutCF,
    addrTxCF,
    derivedAddrs,
    mempoolAddrCF,
    mempoolCF,
    txCF,
  )
import Haskoin.Store.Database.Types
  ( MemAddrKey (..),
    MemAddrVal (..),
    MemSpenderKey (..),
    MemSummary (..),
    TxKey (..),
    decodeTxKey,
  )

-- | What a mempool has, as much as the queries need of it. Each function
-- answers about the pool as it is when asked: one that goes with a block
-- must fail, by throwing, when the pool has moved to another.
data PoolReads = PoolReads
  { -- | the summary of each address, in their order, or 'Nothing' for an
    -- address that the pool does not touch
    summaries :: [Address] -> IO [Maybe MemSummary],
    -- | transactions of an address, newest first and in the order of their
    -- hashes within a second, from the one that entered at a time with a
    -- hash, or where it would be, or from the newest; at most so many
    addressTxs :: Address -> Maybe (UnixTime, TxHash) -> Int -> IO [(UnixTime, TxHash)],
    -- | outputs that the pool pays to an address, in the same order, from
    -- one of a time or from the newest, at most so many, each with whether
    -- the pool also spends it
    addressOuts :: Address -> Maybe (UnixTime, OutPoint) -> Int -> IO [(Unspent, Bool)],
    -- | whether the pool spends each of some outputs of an address
    spentOutputs :: Address -> [OutPoint] -> IO [Bool],
    -- | the bodies of transactions of the pool, in their order, without
    -- spenders
    bodies :: [TxHash] -> IO [Maybe TxData],
    -- | the input of the pool that spends each output, if one does
    spenders :: [OutPoint] -> IO [Maybe Spender],
    -- | the list of the pool, newest first and by hash downwards within a
    -- second, from an offset, at most so many, or all of it for 0
    page :: Word32 -> Word32 -> IO [(UnixTime, TxHash)],
    -- | the transactions of the pool whose hash starts with six bytes, as
    -- 'decodeTxKey' gives them
    startingWith :: (Word32, Word16) -> IO [TxHash]
  }

-- | The queries of a store that answers from the chain as a reader has it,
-- which is to be the view of the database at a block, with a pool on top.
pooledRead :: DatabaseReader -> PoolReads -> StoreRead
pooledRead r pool =
  StoreRead
    { net = r.net,
      ctx = r.ctx,
      getBestBlock = Q.getBestBlock r,
      getBlocksAtHeight = Q.getBlocksAtHeight r,
      getBlock = Q.getBlock r,
      getTxData = \h -> only <$> getManyTxData r pool [h],
      getManyTxData = getManyTxData r pool,
      getSpender = getSpender r pool,
      getBalance = getBalance r pool,
      getUnspent = getUnspent r pool,
      getMempool = pool.page 0 0,
      getMempoolPage = \l -> pool.page l.offset l.limit,
      getAddressesTxs = getAddressesTxs r pool,
      getAddressesUnspents = getAddressesUnspents r pool,
      initialGap = r.initGap,
      maxGap = r.maxGap,
      getNumTxData = getNumTxData r pool,
      getBalances = getBalances r pool,
      getAddressTxs = \a -> getAddressesTxs r pool [a],
      getAddressUnspents = \a -> getAddressesUnspents r pool [a],
      xPubBals = \xpub -> do
        Derived ext chg <- derivedAddrs r xpub
        scanXPubBals (getBalances r pool) r.initGap r.maxGap ext chg,
      xPubUnspents = \_ -> xPubUnspents r pool,
      xPubTxs = \_ xbals -> getAddressesTxs r pool (Q.usedAddresses xbals),
      xPubTxCount = \_ xbals -> getAddressesTxCount r pool (Q.usedAddresses xbals),
      getAddressesTxCount = getAddressesTxCount r pool
    }
  where
    only = \case
      [found] -> found
      _ -> Nothing

-------------------------------------------------------------------------------
-- Transactions
-------------------------------------------------------------------------------

-- | Data of many transactions, in the order of the hashes: those of the
-- pool from it, the others from the chain, each with the spenders that
-- blocks and the pool give to its outputs. Deleted ones are not known.
getManyTxData :: DatabaseReader -> PoolReads -> [TxHash] -> IO [Maybe TxData]
getManyTxData _ _ [] = return []
getManyTxData r pool hs = do
  found <- known r pool hs
  spent <- poolSpenders pool (Map.toList found)
  return [withSpenders spent h <$> Map.lookup h found | h <- hs]

-- | The bodies of transactions by hash: those that the pool has, and those
-- that the chain has for the others, with the spenders that blocks give to
-- their outputs.
known :: DatabaseReader -> PoolReads -> [TxHash] -> IO (Map TxHash TxData)
known r pool hs = do
  unconfirmed <- Map.fromList . catMaybes . zipWith (fmap . (,)) hs <$> pool.bodies hs
  let rest = filter (`Map.notMember` unconfirmed) hs
  chain <- case rest of
    [] -> return Map.empty
    _ -> Map.fromList . catMaybes . zipWith (fmap . (,)) rest <$> Q.retrieveManyCF r.db (txCF r.db) (map TxKey rest)
  return (unconfirmed <> Map.mapMaybe notDeleted' chain)
  where
    notDeleted' = notDeleted . Just

-- | The spenders that the pool has for the outputs of transactions that no
-- block spends, in one read.
poolSpenders :: PoolReads -> [(TxHash, TxData)] -> IO (HashMap.HashMap TxHash (IntMap.IntMap Spender))
poolSpenders pool txs = do
  found <- case points of
    [] -> return []
    _ -> pool.spenders points
  return . HashMap.fromListWith IntMap.union $
    [(p.hash, IntMap.singleton (fromIntegral p.index) s) | (p, Just s) <- zip points found]
  where
    points =
      [ OutPoint h i
      | (h, td) <- txs,
        (i, _) <- zip [0 ..] td.tx.outputs,
        IntMap.notMember (fromIntegral i) td.spenders
      ]

-- | Transactions whose hash, read as a number, matches a short identifier
-- of the Blockchain.info API, in the order of their hashes.
getNumTxData :: DatabaseReader -> PoolReads -> Word64 -> IO [TxData]
getNumTxData r pool i = do
  inPool <- pool.startingWith prefix
  unconfirmed <- catMaybes <$> pool.bodies inPool
  chain <- matchingAsListCF r.db (txCF r.db) (TxKeyS prefix)
  let found =
        Map.fromList [(txHash td.tx, td) | td <- unconfirmed]
          <> Map.fromList [(h, td) | (TxKey h, td) <- chain, not td.deleted]
      matches = filter (seventh . fst) (Map.toAscList found)
  spent <- poolSpenders pool matches
  return [withSpenders spent h td | (h, td) <- matches]
  where
    (prefix, bits) = decodeTxKey i
    seventh h = BS.index (encode h) 6 .&. 0xf8 == bits

-- | Input that spends an output, in the pool or in a block.
getSpender :: DatabaseReader -> PoolReads -> OutPoint -> IO (Maybe Spender)
getSpender r pool p =
  pool.spenders [p] >>= \case
    [Just s] -> return (Just s)
    _ -> Q.getSpender r p

-------------------------------------------------------------------------------
-- Addresses
-------------------------------------------------------------------------------

-- | Balance of an address, with the pool applied.
getBalance :: DatabaseReader -> PoolReads -> Address -> IO (Maybe Balance)
getBalance r pool a = M.summaryBalance a <$> Q.getBalance r a <*> (only <$> pool.summaries [a])
  where
    only = \case
      [s] -> s
      _ -> Nothing

-- | Balances of addresses, in the same order, with the pool applied.
getBalances :: DatabaseReader -> PoolReads -> [Address] -> IO [Balance]
getBalances _ _ [] = return []
getBalances r pool as = zipWith apply <$> Q.getBalances r as <*> pool.summaries as
  where
    apply c = maybe c (M.applySummary c)

-- | Unspent output, if it exists and neither a block nor the pool spends
-- it.
getUnspent :: DatabaseReader -> PoolReads -> OutPoint -> IO (Maybe Unspent)
getUnspent r pool p =
  pool.spenders [p] >>= \case
    [Just _] -> return Nothing
    _ ->
      Q.getUnspent r p >>= \case
        Just u -> return (Just u)
        Nothing -> (>>= output) . only <$> pool.bodies [p.hash]
  where
    only = \case
      [found] -> found
      _ -> Nothing
    output td = case drop (fromIntegral p.index) td.tx.outputs of
      o : _ ->
        Just
          Unspent
            { block = td.block,
              outpoint = p,
              value = o.value,
              script = o.script,
              address = eitherToMaybe (scriptToAddressBS r.net r.ctx o.script)
            }
      [] -> Nothing

-- | Where the page of a query starts, or 'Nothing' if it starts at a
-- transaction that neither the pool nor the chain has.
pageStart :: DatabaseReader -> PoolReads -> Maybe Start -> IO (Maybe From)
pageStart _ _ Nothing = return (Just Top)
pageStart _ _ (Just (AtBlock height)) = return (Just (InChain (BlockRef height maxBound)))
pageStart r pool (Just (AtTx h)) =
  known r pool [h] <&> \found -> case (.block) <$> Map.lookup h found of
    Nothing -> Nothing
    Just (MemRef time) -> Just (InMempool time h)
    Just position@BlockRef {} -> Just (InChain position)

-- | Answer a query from where its page starts.
paged :: DatabaseReader -> PoolReads -> Limits -> (From -> IO [a]) -> IO [a]
paged r pool limits answer = pageStart r pool limits.start >>= maybe (return []) answer

-- | A stream of what a function gives by the page: it is asked for a first
-- page from a start, and then for more from the last item that it gave,
-- which it gives again if it still has it, for as long as it gives full
-- pages. The pages grow, so that a short list costs one small read and a
-- long one few large ones.
pages :: (Eq k) => (x -> k) -> (Maybe k -> Int -> IO [x]) -> Maybe k -> ConduitT i x IO ()
pages key fetch = go 64 Nothing
  where
    go size after start = do
      rows <- lift (fetch start size)
      let fresh = maybe rows (\k -> dropWhile ((== k) . key) rows) after
      yieldMany fresh
      case reverse rows of
        final : _ | length rows == size -> go (min 4096 (size * 4)) (Just (key final)) (Just (key final))
        _ -> return ()

-- | The transactions of an address in the pool that belong in a list that
-- starts somewhere, newest first: none when it starts in the chain, or when
-- the pool has nothing for the address.
poolTxsC :: PoolReads -> From -> Address -> Maybe MemSummary -> ConduitT i TxRef IO ()
poolTxsC pool from a summary = case (from, summary) of
  (InChain _, _) -> return ()
  (_, Nothing) -> return ()
  (Top, Just _) -> stream Nothing
  (InMempool time h, Just _) -> stream (Just (time, h))
  where
    stream start = pages id (pool.addressTxs a) start .| mapC (\(t, h) -> TxRef {block = MemRef t, txid = h})

-- | The summaries of addresses for a list that starts somewhere: none are
-- read for one that starts in the chain.
summariesFor :: PoolReads -> From -> [Address] -> IO [Maybe MemSummary]
summariesFor pool from as = case from of
  InChain _ -> return (map (const Nothing) as)
  _ -> pool.summaries as

-- | Transactions of any of the addresses, newest first, each once.
getAddressesTxs :: DatabaseReader -> PoolReads -> [Address] -> Limits -> IO [TxRef]
getAddressesTxs _ _ [] _ = return []
getAddressesTxs r pool as limits = paged r pool limits $ \from -> do
  sums <- summariesFor pool from as
  Q.withManyIters r (addrTxCF r.db) (length as) $ \its ->
    Q.joinConduits txRefKey (zipWith3 (stream from) as sums its) limits
  where
    stream from a s it = poolTxsC pool from a s >> chainTxsC from a it

-- | The stream of unspent outputs of each address, from where a page
-- starts, given the iterators on the chain's index: the outputs that the
-- pool pays and does not spend, then those of the chain that the pool does
-- not spend. The pool is asked which outputs of the chain it spends
-- several at a time, four at first and then four times as many each time,
-- so that the first output of each of many addresses does not cost a batch
-- of rows read ahead, and no more once as many were found as its summary
-- counts.
unspentStreams ::
  DatabaseReader ->
  PoolReads ->
  From ->
  [Address] ->
  IO ([Iterator] -> [ConduitT () Unspent IO ()])
unspentStreams r pool from as = do
  sums <- pool.summaries as
  return $ \its -> zipWith3 stream as sums its
  where
    stream a summary it = fresh a summary >> (chainUnspentsC r from a it .| hide a (maybe 0 hidden summary) 4)
    hidden s = s.spentCount - s.bothCount
    fresh a = \case
      Just s | s.paidCount > s.bothCount -> case from of
        Top -> outs a Nothing
        InMempool time h -> outs a (Just (time, OutPoint h 0))
        InChain _ -> return ()
      _ -> return ()
    outs a start =
      pages (\(u, _) -> position u) (pool.addressOuts a) start
        .| awaitForever (\(u, spent) -> if spent then return () else yield u)
    position u = case u.block of
      MemRef t -> (t, u.outpoint)
      BlockRef {} -> (0, u.outpoint)
    hide :: Address -> Word64 -> Int -> ConduitT Unspent Unspent IO ()
    hide _ 0 _ = awaitForever yield
    hide a n size = do
      batch <- gather size
      case batch of
        [] -> return ()
        _ -> do
          flags <- lift (pool.spentOutputs a (map (.outpoint) batch))
          yieldMany [u | (u, False) <- zip batch flags]
          hide a (n - min n (fromIntegral (length (filter id flags)))) (min 256 (size * 4))
    gather :: Int -> ConduitT Unspent o IO [Unspent]
    gather 0 = return []
    gather k =
      await >>= \case
        Nothing -> return []
        Just u -> (u :) <$> gather (k - 1)

-- | Unspent outputs of any of the addresses, newest first.
getAddressesUnspents :: DatabaseReader -> PoolReads -> [Address] -> Limits -> IO [Unspent]
getAddressesUnspents _ _ [] _ = return []
getAddressesUnspents r pool as limits = paged r pool limits $ \from -> do
  streams <- unspentStreams r pool from as
  Q.withManyIters r (addrOutCF r.db) (length as) $ \its ->
    Q.joinConduits unspentKey (streams its) limits

-- | Unspent outputs of an extended public key, newest first, given its
-- balances.
xPubUnspents :: DatabaseReader -> PoolReads -> [XPubBal] -> Limits -> IO [XPubUnspent]
xPubUnspents r pool xbals limits = paged r pool limits $ \from -> do
  let quota = case (deOffset limits).limit of
        0 -> Nothing
        l -> Just l
      tag b = mapC (\u -> XPubUnspent {path = b.path, unspent = u})
  streams <- unspentStreams r pool from (map (.balance.address) funded)
  Q.withManyIters r (addrOutCF r.db) (length funded) $ \its ->
    runConduit $
      mergeDescGroups
        (.unspent.block)
        (unspentKey . (.unspent))
        quota
        (zipWith (\b s -> s .| tag b) funded (streams its))
        .| applyLimitsC limits
        .| sinkList
  where
    funded = filter ((> 0) . (.balance.utxo)) xbals

-- | Number of distinct transactions of a set of addresses: those of the
-- chain, and those of the pool, read only when more than one of the
-- addresses has some there.
getAddressesTxCount :: DatabaseReader -> PoolReads -> [Address] -> IO Word32
getAddressesTxCount _ _ [] = return 0
getAddressesTxCount r pool as = do
  inBlocks <- Q.getAddressesTxCount r as
  sums <- pool.summaries as
  inPool <- case [(a, s) | (a, Just s) <- zip as sums, s.txs > 0] of
    [] -> return 0
    [(_, s)] -> return (fromIntegral s.txs)
    several -> do
      txs <- forM several $ \(a, s) -> runConduit (poolTxsC pool Top a (Just s) .| mapC (.txid) .| sinkList)
      return (HashSet.size (HashSet.fromList (concat txs)))
  return (inBlocks + fromIntegral inPool)

-------------------------------------------------------------------------------
-- The store's own mempool area as a pool
-------------------------------------------------------------------------------

-- | The mempool area of a database, as a reader has it, as a pool. The
-- queries of this module with it must answer as those of
-- "Haskoin.Store.Database.Query.Layered" do, but for deleted transactions:
-- that is what the tests check, and what this is for.
localPool :: DatabaseReader -> PoolReads
localPool r =
  PoolReads
    { summaries = M.getSummaries r,
      addressTxs = \a start n -> do
        rows <- entries a
        let listed = [(t, h) | (MemAddrTxKey _ t h, _) <- rows]
            from (t, h) = dropWhile (\(t', h') -> t' > t || (t' == t && h' < h)) listed
        return (take n (maybe listed from start)),
      addressOuts = \a start n -> do
        rows <- entries a
        let spent = HashSet.fromList [p | (MemAddrSpentKey _ p, _) <- rows]
            made t p v s = Unspent {block = MemRef t, outpoint = p, value = v, script = s, address = Just a}
            listed = [(made t p v s, HashSet.member p spent) | (MemAddrOutKey _ t p, MemAddrOutVal v s) <- rows]
            place (u, _) = case u.block of
              MemRef t -> (t, encode u.outpoint)
              BlockRef {} -> (0, encode u.outpoint)
            from (t, p) = dropWhile (\x -> let (t', p') = place x in t' > t || (t' == t && p' < encode p)) listed
        return (take n (maybe listed from start)),
      spentOutputs = \a ps ->
        forM ps $ \p ->
          isJust <$> (retrieveCF r.db (mempoolAddrCF r.db) (MemAddrSpentKey a p) :: IO (Maybe MemAddrVal)),
      bodies = \hs -> case hs of
        [] -> return []
        _ -> Q.retrieveManyCF r.db (mempoolCF r.db) (map TxKey hs),
      spenders = \ps -> case ps of
        [] -> return []
        _ -> Q.retrieveManyCF r.db (mempoolCF r.db) (map MemSpenderKey ps),
      page = \offset limit -> Q.getMempoolPage r Limits {limit = limit, offset = offset, start = Nothing},
      startingWith = \prefix -> do
        rows <- matchingAsListCF r.db (mempoolCF r.db) (TxKeyS prefix)
        return [h | (TxKey h, _ :: TxData) <- rows]
    }
  where
    entries :: Address -> IO [(MemAddrKey, MemAddrVal)]
    entries a = matchingAsListCF r.db (mempoolAddrCF r.db) (MemAddrKeyA a)
