-- |
-- Module      : Haskoin.Store.Database.Query.Pooled
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The queries of the store: the chain, as the database has it, with the
-- mempool laid over it. The mempool is the pool that the stores of a fleet
-- share, which "Haskoin.Store.Fleet.Redis" keeps in Redis; the database
-- has no mempool of its own.
--
-- = Why the pool comes as functions
--
-- Stores that share a Redis are to give the same answer to the same
-- request. So each answers from the chain as it was at the block that the
-- fleet agreed on, which every store has the same, read through a view of
-- its database at that block ("Haskoin.Store.Database.Views"), and from the
-- one pool that they share. The mempool's part comes through a
-- t'PoolReads', a handful of functions that say what a pool has: its
-- summaries, the entries of an address by the page, bodies, spenders,
-- its list, and what it does to tokens (CashTokens). A pool far away answers each in one round trip, and
-- 'emptyPool' answers for no pool at all, which is how the chain alone is
-- read ('chainRead').
--
-- = What a pool cannot say
--
-- A transaction that left a mempool unconfirmed is kept nowhere, but for
-- the bodies that a database of version 20 still has of those that its own
-- mempool deleted, which no other store has. A fleet cannot answer the same
-- about them from every store, so these queries do not know deleted
-- transactions at all: a page that starts at one is empty.
module Haskoin.Store.Database.Query.Pooled
  ( PoolReads (..),
    pooledRead,
    emptyPool,
    chainRead,
    pageStart,
    From (..),
    withSpenders,
    summaryBalance,
    applySummary,
  )
where

import Conduit
  ( ConduitT,
    await,
    awaitForever,
    concatMapC,
    lift,
    mapC,
    runConduit,
    sinkList,
    yield,
    yieldMany,
    (.|),
  )
import Control.Applicative ((<|>))
import Control.Monad (forM)
import Data.Bits ((.&.))
import Data.ByteString qualified as BS
import Data.Bytes.Put (runPutS)
import Data.Bytes.Serial (serialize)
import Data.Default (def)
import Data.Functor ((<&>))
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet qualified as HashSet
import Data.IntMap.Strict qualified as IntMap
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe, isJust)
import Data.Serialize (Serialize, encode)
import Data.Word (Word16, Word32, Word64)
import Database.RocksDB (Iterator)
import Database.RocksDB.Query (matchingAsListCF)
import Haskoin
  ( Address,
    OutPoint (..),
    Tx (..),
    TxHash,
    TxOut (..),
    eitherToMaybe,
    scriptToAddressBS,
  )
import Haskoin.Store.Common
  ( Limits (..),
    Replaced,
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
    TokenBalance (..),
    TxData (..),
    TxRef (..),
    UnixTime,
    Unspent (..),
    XPubBal (..),
    XPubUnspent (..),
    zeroBalance,
  )
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    Derived (..),
    addrOutCF,
    addrTxCF,
    catOutCF,
    catTxCF,
    derivedAddrs,
    txCF,
    withSnapshotReader,
  )
import Haskoin.Store.Database.Types
  ( AddrOutKey (..),
    AddrTxKey (..),
    CatOutKey (..),
    CatTxKey (..),
    CategorySummary (..),
    MemSummary (..),
    OutVal (..),
    PoolTokens (..),
    TokBalVal (..),
    TxKey (..),
    decodeTxKey,
    toUnspent,
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
    -- | for each of some transactions, in their order: its body if it is
    -- in the pool, without spenders, and the inputs of the pool that spend
    -- its outputs, by output, whether it is in the pool or in the chain.
    -- The two come together because nearly every question about a
    -- transaction needs both, and a pool that is far away answers them in
    -- one go.
    bodies :: [TxHash] -> IO [(Maybe TxData, IntMap.IntMap Spender)],
    -- | the input of the pool that spends each output, if one does
    spenders :: [OutPoint] -> IO [Maybe Spender],
    -- | the list of the pool, newest first and by hash downwards within a
    -- second, from an offset, at most so many, or all of it for 0
    page :: Word32 -> Word32 -> IO [(UnixTime, TxHash)],
    -- | the transactions of the pool whose hash starts with six bytes, as
    -- 'decodeTxKey' gives them
    startingWith :: (Word32, Word16) -> IO [TxHash],
    -- | what the pool keeps of a transaction that a block replaced as the
    -- tip moved past it ('Haskoin.Store.Fleet.Pool.advanceReplacing')
    replaced :: TxHash -> IO (Maybe Replaced),
    -- | what the pool does to the tokens of each category at an address,
    -- in no order (CashTokens)
    addressTokens :: Address -> IO [(TxHash, PoolTokens)],
    -- | what the pool does to the tokens of a category at each address, in
    -- no order
    categoryHolders :: TxHash -> IO [(Address, PoolTokens)],
    -- | what the pool does to a category, or 'Nothing' if it does not move
    -- it
    categorySummary :: TxHash -> IO (Maybe CategorySummary),
    -- | transactions that move tokens of a category, as 'addressTxs'
    categoryTxs :: TxHash -> Maybe (UnixTime, TxHash) -> Int -> IO [(UnixTime, TxHash)],
    -- | outputs that the pool makes and that hold tokens of a category, as
    -- 'addressOuts', each with whether the pool also uses it up
    categoryOuts :: TxHash -> Maybe (UnixTime, OutPoint) -> Int -> IO [(Unspent, Bool)]
  }

-- | The queries of a store that answers from the chain as a reader has it,
-- which is to be the view of the database at a block, with a pool on top.
pooledRead :: DatabaseReader -> PoolReads -> StoreRead
pooledRead r pool =
  StoreRead
    { net = r.net,
      getBestBlock = Q.getBestBlock r,
      getBlocksAtHeight = Q.getBlocksAtHeight r,
      getBlock = Q.getBlock r,
      getTxData = \h -> only <$> getManyTxData r pool [h],
      getManyTxData = getManyTxData r pool,
      getManyTxBytes = getManyTxBytes r pool,
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
      getAddressesTxCount = getAddressesTxCount r pool,
      -- from the pool, which keeps what the blocks replaced as the tip
      -- moves past them, the same for every store: not from the store's
      -- own database, which has what its own mempool lost
      getReplaced = pool.replaced,
      getAddressTokens = getAddressTokens r pool,
      getTokenHolders = getTokenHolders r pool,
      getTokenTxs = getTokenTxs r pool,
      getTokenUnspents = getTokenUnspents r pool
    }
  where
    only = \case
      [found] -> found
      _ -> Nothing

-- | A pool that has nothing. The queries of this module with it answer
-- from the chain alone.
emptyPool :: PoolReads
emptyPool =
  PoolReads
    { summaries = return . map (const Nothing),
      addressTxs = \_ _ _ -> return [],
      addressOuts = \_ _ _ -> return [],
      spentOutputs = \_ ps -> return (map (const False) ps),
      bodies = return . map (const (Nothing, IntMap.empty)),
      spenders = return . map (const Nothing),
      page = \_ _ -> return [],
      startingWith = \_ -> return [],
      replaced = \_ -> return Nothing,
      addressTokens = \_ -> return [],
      categoryHolders = \_ -> return [],
      categorySummary = \_ -> return Nothing,
      categoryTxs = \_ _ _ -> return [],
      categoryOuts = \_ _ _ -> return []
    }

-- | The queries of the chain alone, each on a snapshot of the database of
-- its own, taken when it is asked: for code that asks one thing at a time,
-- outside of a request, such as the block store and the web server before
-- it has a view of the fleet tip. Queries asked one after the other may see
-- different blocks.
chainRead :: DatabaseReader -> StoreRead
chainRead r =
  StoreRead
    { net = r.net,
      getBestBlock = one (.getBestBlock),
      getBlocksAtHeight = \h -> one (\q -> q.getBlocksAtHeight h),
      getBlock = \h -> one (\q -> q.getBlock h),
      getTxData = \h -> one (\q -> q.getTxData h),
      getManyTxData = \hs -> one (\q -> q.getManyTxData hs),
      getManyTxBytes = \hs -> one (\q -> q.getManyTxBytes hs),
      getSpender = \p -> one (\q -> q.getSpender p),
      getBalance = \a -> one (\q -> q.getBalance a),
      getUnspent = \p -> one (\q -> q.getUnspent p),
      getMempool = return [],
      getMempoolPage = \_ -> return [],
      getAddressesTxs = \as l -> one (\q -> q.getAddressesTxs as l),
      getAddressesUnspents = \as l -> one (\q -> q.getAddressesUnspents as l),
      initialGap = r.initGap,
      maxGap = r.maxGap,
      getNumTxData = \n -> one (\q -> q.getNumTxData n),
      getBalances = \as -> one (\q -> q.getBalances as),
      getAddressTxs = \a l -> one (\q -> q.getAddressTxs a l),
      getAddressUnspents = \a l -> one (\q -> q.getAddressUnspents a l),
      xPubBals = \x -> one (\q -> q.xPubBals x),
      xPubUnspents = \x bs l -> one (\q -> q.xPubUnspents x bs l),
      xPubTxs = \x bs l -> one (\q -> q.xPubTxs x bs l),
      xPubTxCount = \x bs -> one (\q -> q.xPubTxCount x bs),
      getAddressesTxCount = \as -> one (\q -> q.getAddressesTxCount as),
      getReplaced = \_ -> return Nothing,
      getAddressTokens = \a -> one (\q -> q.getAddressTokens a),
      getTokenHolders = \c l -> one (\q -> q.getTokenHolders c l),
      getTokenTxs = \c l -> one (\q -> q.getTokenTxs c l),
      getTokenUnspents = \c l -> one (\q -> q.getTokenUnspents c l)
    }
  where
    one :: (StoreRead -> IO a) -> IO a
    one f = withSnapshotReader r (\s -> f (pooledRead s emptyPool))

-------------------------------------------------------------------------------
-- Transactions
-------------------------------------------------------------------------------

-- | Add the spenders that the pool has for the outputs of a transaction
-- to those in its body.
withSpenders :: HashMap TxHash (IntMap.IntMap Spender) -> TxHash -> TxData -> TxData
withSpenders spent h td = case HashMap.lookup h spent of
  Nothing -> td
  Just unconfirmed -> (td :: TxData) {spenders = IntMap.union td.spenders unconfirmed}

-- | Data of many transactions, in the order of the hashes: those of the
-- pool from it, the others from the chain, each with the spenders that
-- blocks and the pool give to its outputs. Deleted ones are not known.
getManyTxData :: DatabaseReader -> PoolReads -> [TxHash] -> IO [Maybe TxData]
getManyTxData _ _ [] = return []
getManyTxData r pool hs = do
  found <- known r pool hs
  return [Map.lookup h found | h <- hs]

-- | Transactions in the format of the network: those that the pool has,
-- serialized from what it gives, and those that the chain has, as stored,
-- for the others, without the deleted ones and without spenders.
getManyTxBytes :: DatabaseReader -> PoolReads -> [TxHash] -> IO [Maybe BS.ByteString]
getManyTxBytes _ _ [] = return []
getManyTxBytes r pool hs = do
  inPool <- zip hs <$> pool.bodies hs
  let unconfirmed = Map.fromList [(h, runPutS (serialize td.tx)) | (h, (Just td, _)) <- inPool]
      rest = filter (`Map.notMember` unconfirmed) hs
  chain <-
    Map.fromList . catMaybes . zipWith (\h v -> (,) h <$> (v >>= Q.liveTxBytes)) rest
      <$> Q.retrieveManyBytesCF r (txCF r.db) (map TxKey rest)
  return [Map.lookup h unconfirmed <|> Map.lookup h chain | h <- hs]

-- | The data of transactions by hash: those that the pool has, and those
-- that the chain has for the others, each with the spenders that blocks
-- and the pool give to its outputs. One read of the pool, and one of the
-- chain for what the pool does not have.
known :: DatabaseReader -> PoolReads -> [TxHash] -> IO (Map TxHash TxData)
known _ _ [] = return Map.empty
known r pool hs = do
  inPool <- zip hs <$> pool.bodies hs
  let unconfirmed = Map.fromList [(h, td) | (h, (Just td, _)) <- inPool]
      spent = HashMap.fromList [(h, spenders) | (h, (_, spenders)) <- inPool, not (IntMap.null spenders)]
      rest = filter (`Map.notMember` unconfirmed) hs
  chain <- case rest of
    [] -> return Map.empty
    _ -> do
      found <- catMaybes . zipWith (fmap . (,)) rest <$> Q.retrieveManyCF r (txCF r.db) (map TxKey rest)
      -- with the spenders that blocks give to their outputs
      Map.fromList . zip (map fst found) <$> Q.withChainSpenders r found
  return (Map.mapWithKey (withSpenders spent) (unconfirmed <> Map.mapMaybe notDeleted' chain))
  where
    notDeleted' = notDeleted . Just

-- | Transactions whose hash, read as a number, matches a short identifier
-- of the Blockchain.info API, in the order of their hashes.
getNumTxData :: DatabaseReader -> PoolReads -> Word64 -> IO [TxData]
getNumTxData r pool i = do
  inPool <- pool.startingWith prefix
  chain <- matchingAsListCF r.db (txCF r.db) (TxKeyS prefix)
  let confirmed = [h | (TxKey h, td :: TxData) <- chain, not td.deleted]
  found <- known r pool (filter seventh (inPool <> confirmed))
  return (Map.elems found)
  where
    (prefix, bits) = decodeTxKey i
    seventh h = BS.index (encode h) 6 .&. 0xf8 == bits

-- | Input that spends an output, in the pool or in a block.
getSpender :: DatabaseReader -> PoolReads -> OutPoint -> IO (Maybe Spender)
getSpender r pool p =
  pool.spenders [p] >>= \case
    [Just s] -> return (Just s)
    _ -> Q.getChainSpender r p

-------------------------------------------------------------------------------
-- Addresses
-------------------------------------------------------------------------------

-- | Balance of an address, with the pool applied.
getBalance :: DatabaseReader -> PoolReads -> Address -> IO (Maybe Balance)
getBalance r pool a = summaryBalance a <$> Q.getBalance r a <*> (only <$> pool.summaries [a])
  where
    only = \case
      [s] -> s
      _ -> Nothing

-- | Balances of addresses, in the same order, with the pool applied.
getBalances :: DatabaseReader -> PoolReads -> [Address] -> IO [Balance]
getBalances _ _ [] = return []
getBalances r pool as = zipWith apply <$> Q.getBalances r as <*> pool.summaries as
  where
    apply c = maybe c (applySummary c)

-- | The balance that the chain has for an address with the summary of what
-- the pool does at it applied.
applySummary :: Balance -> MemSummary -> Balance
applySummary c s =
  Balance
    { address = c.address,
      confirmed = c.confirmed `minus` (s.spent `minus` s.both),
      unconfirmed = c.unconfirmed + (s.paid `minus` s.both),
      utxo = (c.utxo `minus` (s.spentCount `minus` s.bothCount)) + (s.paidCount `minus` s.bothCount),
      txs = c.txs + s.txs,
      received = c.received + s.paid
    }
  where
    minus x y = if y > x then 0 else x - y

-- | Balance of an address as the API gives it, from its balance in the
-- chain and its summary in the pool, either of which it may not have.
summaryBalance :: Address -> Maybe Balance -> Maybe MemSummary -> Maybe Balance
summaryBalance _ chain Nothing = chain
summaryBalance a chain (Just s) = Just (applySummary (fromMaybe (zeroBalance a) chain) s)

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
      [(found, _)] -> found
      _ -> Nothing
    output td = case drop (fromIntegral p.index) td.tx.outputs of
      o : _ ->
        Just
          Unspent
            { block = td.block,
              outpoint = p,
              value = o.value,
              script = o.script,
              address = eitherToMaybe (scriptToAddressBS r.net o.script)
            }
      [] -> Nothing

-- | Where a page starts in a list that has the pool's part first and the
-- chain's part after it.
data From
  = -- | at the newest item
    Top
  | -- | in the pool's part, at the transaction that entered the pool at
    -- this time and has this hash, or where it would be
    InMempool !UnixTime !TxHash
  | -- | in the chain's part, at this position: nothing of the pool is in
    -- the page
    InChain !BlockRef

-- | Transactions of an address in the chain, newest first, from where a
-- page starts, streamed from the index of the address.
chainTxsC :: From -> Address -> Iterator -> ConduitT i TxRef IO ()
chainTxsC from a it = chain .| concatMapC ref
  where
    chain = case from of
      InChain position -> Q.scanFrom it (AddrTxKeyA a) (AddrTxKeyB a position)
      _ -> Q.scan it (AddrTxKeyA a)
    ref :: (AddrTxKey, ()) -> Maybe TxRef
    ref (AddrTxKey _ t, ()) = Just t
    ref _ = Nothing

-- | Unspent outputs of an address in the chain, newest first, from where a
-- page starts, streamed from the index of the address. Those that the
-- pool spends are among them.
chainUnspentsC :: DatabaseReader -> From -> Address -> Iterator -> ConduitT i Unspent IO ()
chainUnspentsC r from a it = chain .| concatMapC output
  where
    chain = case from of
      InChain position -> Q.scanFrom it (AddrOutKeyA a) (AddrOutKeyB a position)
      _ -> Q.scan it (AddrOutKeyA a)
    output :: (AddrOutKey, OutVal) -> Maybe Unspent
    output = uncurry (toUnspent r.net)

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
-- page of a size from a start, and then for more from the last item that
-- it gave, which it gives again if it still has it, for as long as it gives
-- full pages. The pages grow, so that a long list costs few reads. The
-- first is given the size of what the query is asked for, so that one read
-- is enough for a page of the API: each read of a pool that is far away
-- costs more than the rows it brings.
pages :: (Eq k) => (x -> k) -> (Maybe k -> Int -> IO [x]) -> Int -> Maybe k -> ConduitT i x IO ()
pages key fetch first = go first Nothing
  where
    go size after start = do
      rows <- lift (fetch start size)
      let fresh = maybe rows (\k -> dropWhile ((== k) . key) rows) after
      yieldMany fresh
      case reverse rows of
        final : _ | length rows == size -> go (min 4096 (size * 4)) (Just (key final)) (Just (key final))
        _ -> return ()

-- | The transactions of an address in the pool that belong in a list that
-- starts somewhere, newest first, read so many at first: none when the
-- list starts in the chain, or when the pool is known to have nothing for
-- the address.
poolTxsC :: PoolReads -> From -> Int -> Address -> Bool -> ConduitT i TxRef IO ()
poolTxsC pool from first a touched = case (from, touched) of
  (InChain _, _) -> return ()
  (_, False) -> return ()
  (Top, True) -> stream Nothing
  (InMempool time h, True) -> stream (Just (time, h))
  where
    stream start = pages id (pool.addressTxs a) first start .| mapC (\(t, h) -> TxRef {block = MemRef t, txid = h})

-- | How many items to ask a pool for at first, for a query with limits: as
-- many as the page and what is skipped before it, and one more, so that a
-- page that is not full is known to be the end; within bounds, for a query
-- without a limit.
firstPage :: Limits -> Int
firstPage limits = case limits.limit of
  0 -> 256
  n -> max 16 (min 1024 (fromIntegral (n + limits.offset) + 1))

-- | Whether a pool may have transactions of each of some addresses, for a
-- list that starts somewhere. For one address it is not asked: its
-- transactions are, which says as much in the same read. For several, the
-- summaries are read, in one read for all of them, so that only those that
-- have something are asked for it.
touchedBy :: PoolReads -> From -> [Address] -> IO [Bool]
touchedBy pool from as = case (from, as) of
  (InChain _, _) -> return (map (const False) as)
  (_, [_]) -> return [True]
  _ -> map isJust <$> pool.summaries as

-- | Transactions of any of the addresses, newest first, each once.
getAddressesTxs :: DatabaseReader -> PoolReads -> [Address] -> Limits -> IO [TxRef]
getAddressesTxs _ _ [] _ = return []
getAddressesTxs r pool as limits = paged r pool limits $ \from -> do
  touched <- touchedBy pool from as
  Q.withManyIters r (addrTxCF r.db) (map AddrTxKeyA as) $ \its ->
    Q.joinConduits txRefKey (zipWith3 (stream from) as touched its) limits
  where
    stream from a t it = poolTxsC pool from (firstPage limits) a t >> chainTxsC from a it

-- | The stream of unspent outputs of each address, from where a page
-- starts, given the iterators on the chain's index: the outputs that the
-- pool pays and does not spend, then those of the chain that the pool does
-- not spend ('unspentStream'). One read of the pool gives the summaries of
-- all the addresses, which say which of them have anything in the pool.
unspentStreams ::
  DatabaseReader ->
  PoolReads ->
  From ->
  Limits ->
  [Address] ->
  IO ([Iterator] -> [ConduitT () Unspent IO ()])
unspentStreams r pool from limits as = do
  sums <- pool.summaries as
  return $ \its -> zipWith3 stream as sums its
  where
    stream a summary it =
      unspentStream
        from
        limits
        (maybe False (\s -> s.paidCount > s.bothCount) summary)
        (pool.addressOuts a)
        (pool.spentOutputs a)
        (maybe 0 (\s -> s.spentCount - s.bothCount) summary)
        (chainUnspentsC r from a it)

-- | A stream of unspent outputs, newest first, from where a page starts:
-- the outputs that the pool makes and does not spend, if it has any, then
-- those of the chain without the ones that the pool spends, of which it
-- has so many. The pool is asked which outputs of the chain it spends
-- several at a time, four at first and then four times as many each time,
-- so that the first output of each of many lists does not cost a batch of
-- rows read ahead, and no more once as many were found as it spends.
unspentStream ::
  From ->
  Limits ->
  -- | whether the pool makes outputs of the list that it does not spend
  Bool ->
  -- | the outputs of the list that the pool makes, by the page
  (Maybe (UnixTime, OutPoint) -> Int -> IO [(Unspent, Bool)]) ->
  -- | whether the pool spends each of some outputs of the list
  ([OutPoint] -> IO [Bool]) ->
  -- | how many outputs of the chain in the list the pool spends
  Word64 ->
  -- | the list in the chain
  ConduitT () Unspent IO () ->
  ConduitT () Unspent IO ()
unspentStream from limits made outs spentBy hidden chain = fresh >> (chain .| hide hidden 4)
  where
    fresh
      | made = case from of
          Top -> stream Nothing
          InMempool time h -> stream (Just (time, OutPoint h 0))
          InChain _ -> return ()
      | otherwise = return ()
    -- some of what the pool makes it also spends, so more is asked for
    stream start =
      pages (\(u, _) -> position u) outs (2 * firstPage limits) start
        .| awaitForever (\(u, spent) -> if spent then return () else yield u)
    position u = case u.block of
      MemRef t -> (t, u.outpoint)
      BlockRef {} -> (0, u.outpoint)
    hide :: Word64 -> Int -> ConduitT Unspent Unspent IO ()
    hide 0 _ = awaitForever yield
    hide n size = do
      batch <- gather size
      case batch of
        [] -> return ()
        _ -> do
          flags <- lift (spentBy (map (.outpoint) batch))
          yieldMany [u | (u, False) <- zip batch flags]
          hide (n - min n (fromIntegral (length (filter id flags)))) (min 256 (size * 4))
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
  streams <- unspentStreams r pool from limits as
  Q.withManyIters r (addrOutCF r.db) (map AddrOutKeyA as) $ \its ->
    Q.joinConduits unspentKey (streams its) limits

-- | Unspent outputs of an extended public key, newest first, given its
-- balances.
xPubUnspents :: DatabaseReader -> PoolReads -> [XPubBal] -> Limits -> IO [XPubUnspent]
xPubUnspents r pool xbals limits = paged r pool limits $ \from -> do
  let quota = case (deOffset limits).limit of
        0 -> Nothing
        l -> Just l
      tag b = mapC (\u -> XPubUnspent {path = b.path, unspent = u})
  streams <- unspentStreams r pool from limits (map (.balance.address) funded)
  Q.withManyIters r (addrOutCF r.db) (map (AddrOutKeyA . (.balance.address)) funded) $ \its ->
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
      txs <- forM several $ \(a, _) -> runConduit (poolTxsC pool Top 256 a True .| mapC (.txid) .| sinkList)
      return (HashSet.size (HashSet.fromList (concat txs)))
  return (inBlocks + fromIntegral inPool)

-------------------------------------------------------------------------------
-- Tokens
-------------------------------------------------------------------------------

-- | The token balances of an address, one per category, in the order of
-- the categories' bytes as the chain keeps them, with the pool applied:
-- the categories of the chain, and those that only the pool moves at the
-- address.
getAddressTokens :: DatabaseReader -> PoolReads -> Address -> IO [TokenBalance]
getAddressTokens r pool a = do
  chain <- Q.getAddressTokens r a
  pooled <- pool.addressTokens a
  return [tokenBalance a c v t | (c, v, t) <- overlay chain pooled]

-- | The holders of a category, one token balance per address, in the order
-- of the addresses' bytes as the chain keeps them, with an offset and a
-- limit, and the pool applied. The addresses that only the pool has can
-- fall anywhere in the list, so with any of them the chain's list is read
-- from its start to the end of the page, which the offset and the limit
-- are then applied to.
getTokenHolders :: DatabaseReader -> PoolReads -> TxHash -> Limits -> IO [TokenBalance]
getTokenHolders r pool c limits =
  pool.categoryHolders c >>= \case
    [] -> map (\(a, v) -> tokenBalance a c v Nothing) <$> Q.getCategoryHolders r c limits
    pooled -> do
      let upTo = case limits.limit of
            0 -> limits {offset = 0}
            n -> limits {offset = 0, limit = limits.offset + n}
      chain <- Q.getCategoryHolders r c upTo
      let listed = [tokenBalance a c v t | (a, v, t) <- overlay chain pooled]
          page = drop (fromIntegral limits.offset) listed
      return $ case limits.limit of
        0 -> page
        n -> take (fromIntegral n) page

-- | Token balances of the chain and what the pool does to tokens, by the
-- same key, a category or an address: one for each key that either has,
-- in the order of the keys' bytes, which is that of the chain's index.
overlay :: (Serialize k) => [(k, TokBalVal)] -> [(k, PoolTokens)] -> [(k, TokBalVal, Maybe PoolTokens)]
overlay chain pooled = Map.elems (Map.unionWith joined inChain inPool)
  where
    inChain = Map.fromList [(encode k, (k, v, Nothing)) | (k, v) <- chain]
    inPool = Map.fromList [(encode k, (k, def, Just t)) | (k, t) <- pooled]
    joined (k, v, _) (_, _, t) = (k, v, t)

-- | A token balance as the API answers it, from what the chain has, with
-- what the pool does applied, as 'applySummary' does for a balance.
tokenBalance :: Address -> TxHash -> TokBalVal -> Maybe PoolTokens -> TokenBalance
tokenBalance address category v pooled =
  TokenBalance
    { address,
      category,
      confirmedTokens = v.amount `minus` p.spent,
      unconfirmedTokens = p.free,
      nfts = (v.nfts `minus` p.spentNfts) + p.freeNfts,
      tokenUtxo = (v.outputs `minus` p.spentOutputs) + p.freeOutputs,
      tokenTxs = v.count + p.txs,
      receivedTokens = v.received + p.received
    }
  where
    p = fromMaybe (PoolTokens 0 0 0 0 0 0 0 0) pooled
    minus x y = if y > x then 0 else x - y

-- | Transactions that moved tokens of a category, newest first: those of
-- the pool, then those of the chain, paged as those of an address.
getTokenTxs :: DatabaseReader -> PoolReads -> TxHash -> Limits -> IO [TxRef]
getTokenTxs r pool c limits = paged r pool limits $ \from ->
  Q.withManyIters r (catTxCF r.db) [CatTxKeyC c] $ \its ->
    Q.joinConduits txRefKey [inPool from >> chainCatTxsC from c it | it <- its] limits
  where
    inPool from = case from of
      InChain _ -> return ()
      Top -> stream Nothing
      InMempool time h -> stream (Just (time, h))
    stream start =
      pages id (pool.categoryTxs c) (firstPage limits) start .| mapC (\(t, h) -> TxRef {block = MemRef t, txid = h})

-- | Transactions of a category in the chain, newest first, from where a
-- page starts.
chainCatTxsC :: From -> TxHash -> Iterator -> ConduitT i TxRef IO ()
chainCatTxsC from c it = chain .| concatMapC ref
  where
    chain = case from of
      InChain position -> Q.scanFrom it (CatTxKeyC c) (CatTxKeyB c position)
      _ -> Q.scan it (CatTxKeyC c)
    ref :: (CatTxKey, ()) -> Maybe TxRef
    ref (CatTxKey _ t, ()) = Just t
    ref _ = Nothing

-- | Unspent outputs that hold tokens of a category, newest first: those
-- that the pool makes and does not use up, then those of the chain that
-- the pool does not use up, as for an address ('unspentStream').
getTokenUnspents :: DatabaseReader -> PoolReads -> TxHash -> Limits -> IO [Unspent]
getTokenUnspents r pool c limits = paged r pool limits $ \from -> do
  summary <- pool.categorySummary c
  let stream it =
        unspentStream
          from
          limits
          (maybe False ((> 0) . (.free)) summary)
          (pool.categoryOuts c)
          (fmap (map isJust) . pool.spenders)
          (maybe 0 (.spent) summary)
          (chainCatUnspentsC r from c it)
  Q.withManyIters r (catOutCF r.db) [CatOutKeyC c] $ \its -> Q.joinConduits unspentKey (map stream its) limits

-- | Unspent outputs of a category in the chain, newest first, from where a
-- page starts. Those that the pool uses up are among them.
chainCatUnspentsC :: DatabaseReader -> From -> TxHash -> Iterator -> ConduitT i Unspent IO ()
chainCatUnspentsC r from c it = chain .| concatMapC output
  where
    chain = case from of
      InChain position -> Q.scanFrom it (CatOutKeyC c) (CatOutKeyB c position)
      _ -> Q.scan it (CatOutKeyC c)
    output :: (CatOutKey, OutVal) -> Maybe Unspent
    output (CatOutKey _ block outpoint, OutVal value script) =
      Just Unspent {block, outpoint, value, script, address = eitherToMaybe (scriptToAddressBS r.net script)}
    output _ = Nothing
