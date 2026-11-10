-- |
-- Module      : Haskoin.Store.Database.Query.Layered
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The queries of "Haskoin.Store.Database.Query" for the layered layout of
-- the database, in which the chain and the mempool are kept apart; see
-- @docs\/layered-database.md@. These are the queries that the store
-- answers with. Each answers as the query of the same name answers on a
-- database of version 20, pages included. The tests check that, on
-- databases migrated from that version and on databases built by the rules
-- of "Haskoin.Store.Logic.Layered".
--
-- = Two sources
--
-- A query reads the column families of the chain, with the functions of
-- "Haskoin.Store.Database.Query", reads what the mempool area has for the
-- same transaction, output or address, with those of
-- "Haskoin.Store.Database.Mempool", and puts the two together.
--
-- = Why a view
--
-- The two reads must see the database at the same moment. A block could
-- confirm a transaction between them. Read from the mempool before the block
-- and from the chain after it, the transaction would be listed twice, and
-- its payment counted twice in a balance; read the other way around, it
-- would be missing. The queries of version 20 need no such care, because each of their
-- answers is one key or one scan.
--
-- So every query takes a t'View', a snapshot of the database, and 'withView'
-- is the only way to get one: a query cannot be given the database itself by
-- mistake. The web server is meant to take one view for each request. That
-- also makes the several queries of a request agree with each other, which
-- they did not have to in version 20.
--
-- = Pages
--
-- Lists are ordered newest first, and unconfirmed transactions are newer
-- than any block. The list of an address is therefore what the mempool has
-- for it followed by what the chain has, and that is the sequence that the
-- single index of version 20 holds. A page is cut from it by the same
-- rules: where it starts, then the offset, then the limit. Both parts are
-- streamed, each from its own index, only as far as the page needs.
--
-- = Reading no more than the answer
--
-- Nearly every address has nothing in the mempool, and a few have thousands
-- of entries there. Each address that the mempool touches has a summary of
-- its entries, a t'MemSummary', which every query about an address reads
-- first, by key. A balance is the chain's balance with the summary applied.
-- A list reads the entries only of the addresses that have a summary, and
-- the summary says how many outputs the mempool spends, so that no more are
-- looked for. The first version of these queries scanned the entries of
-- every address for every question, which measured ten thousand times
-- slower for a busy address, and a hundred times slower for any address
-- after a block, when the scan steps over all that the block deleted.
module Haskoin.Store.Database.Query.Layered
  ( -- * A view of the database
    View,
    withView,
    layeredRead,
    standaloneRead,
    viewReader,

    -- * Parts shared with the queries of a fleet
    From (..),
    chainTxsC,
    chainUnspentsC,
    withSpenders,

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
import Control.Monad (forM)
import Data.Bits ((.&.))
import Data.ByteString qualified as BS
import Data.Functor ((<&>))
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet qualified as HashSet
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.List (partition)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, isJust)
import Data.Serialize (Serialize, encode)
import Data.Word (Word32, Word64)
import Database.RocksDB (ColumnFamily, Iterator, multiGetCF, withSnapshot)
import Database.RocksDB.Query
  ( matching,
    matchingAsListCF,
    matchingSkip,
    retrieveCF,
  )
import Haskoin
  ( Address,
    BlockHash,
    BlockHeight,
    OutPoint (..),
    Tx (..),
    TxHash,
    TxOut (..),
    eitherToMaybe,
    scriptToAddressBS,
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
    BlockData,
    BlockRef (..),
    Spender,
    TxData (..),
    TxRef (..),
    UnixTime,
    Unspent (..),
    XPubBal (..),
    XPubSpec,
    XPubUnspent (..),
  )
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
  ( DataMetrics (..),
    DatabaseReader (..),
    Derived (..),
    addrOutCF,
    addrTxCF,
    derivedAddrs,
    mempoolAddrCF,
    mempoolCF,
    txCF,
  )
import Haskoin.Store.Database.Types
  ( AddrOutKey (..),
    AddrTxKey (..),
    MemAddrKey (..),
    MemAddrVal (..),
    MemDeletedKey (..),
    MemSpenderKey (..),
    MemSummary (..),
    OutVal,
    TxKey (..),
    decodeTxKey,
    toUnspent,
  )
import System.Metrics.StatsD (incrementCounter)

-------------------------------------------------------------------------------
-- A view of the database
-------------------------------------------------------------------------------

-- | A database of the layered layout as it was at one moment: both of its
-- areas, read through one RocksDB snapshot. Whatever is written after the
-- view was taken, the queries on it go on answering from that moment.
newtype View = View DatabaseReader

-- | The reader of a view: the database as it was at the moment of the
-- view. The queries of "Haskoin.Store.Database.Query" on it give what the
-- chain had then.
viewReader :: View -> DatabaseReader
viewReader (View r) = r

-- | Run an action with a view of a database as it is now.
--
-- Taking a view costs about a microsecond, so there is no reason to share
-- one between requests, and a reason not to: RocksDB keeps the data that a
-- view can see for as long as the view is open.
--
-- The view, and anything made from it, such as the queries of 'layeredRead',
-- must not be used after the action returns: the snapshot is released then,
-- and RocksDB would be handed a pointer to freed memory. An action that
-- gives back something lazy or something that reads later, such as the body
-- of a streamed response, has to run inside.
withView :: DatabaseReader -> (View -> IO a) -> IO a
withView r action = withSnapshot r.db $ \snapshot -> action (View r {db = snapshot})

-- | The queries of a view as a t'StoreRead', for the web server, as
-- 'Q.databaseRead' gives those of a database of version 20.
layeredRead :: View -> StoreRead
layeredRead v@(View r) =
  StoreRead
    { net = r.net,
      ctx = r.ctx,
      getBestBlock = getBestBlock v,
      getBlocksAtHeight = getBlocksAtHeight v,
      getBlock = getBlock v,
      getTxData = fmap notDeleted . getTxData v,
      getManyTxData = fmap (map notDeleted) . getManyTxData v,
      getSpender = getSpender v,
      getBalance = getBalance v,
      getUnspent = getUnspent v,
      getMempool = getMempool v,
      getMempoolPage = getMempoolPage v,
      getAddressesTxs = getAddressesTxs v,
      getAddressesUnspents = getAddressesUnspents v,
      initialGap = r.initGap,
      maxGap = r.maxGap,
      getNumTxData = fmap (filter (not . (.deleted))) . getNumTxData v,
      getBalances = getBalances v,
      getAddressTxs = getAddressTxs v,
      getAddressUnspents = getAddressUnspents v,
      xPubBals = xPubBals v,
      xPubUnspents = \_ -> xPubUnspents v,
      xPubTxs = \_ -> xPubTxs v,
      xPubTxCount = \_ -> xPubTxCount v,
      getAddressesTxCount = getAddressesTxCount v
    }

-- | The queries of a database as a t'StoreRead', each on a view of its
-- own, taken when it is asked. For code that asks one thing at a time and
-- has no request to hold a view for. Queries asked one after the other may
-- see different moments, so what needs several of them to agree takes a
-- view with 'withView'.
standaloneRead :: DatabaseReader -> StoreRead
standaloneRead r =
  StoreRead
    { net = r.net,
      ctx = r.ctx,
      getBestBlock = one (.getBestBlock),
      getBlocksAtHeight = \h -> one (\q -> q.getBlocksAtHeight h),
      getBlock = \h -> one (\q -> q.getBlock h),
      getTxData = \h -> one (\q -> q.getTxData h),
      getManyTxData = \hs -> one (\q -> q.getManyTxData hs),
      getSpender = \p -> one (\q -> q.getSpender p),
      getBalance = \a -> one (\q -> q.getBalance a),
      getUnspent = \p -> one (\q -> q.getUnspent p),
      getMempool = one (.getMempool),
      getMempoolPage = \l -> one (\q -> q.getMempoolPage l),
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
      getAddressesTxCount = \as -> one (\q -> q.getAddressesTxCount as)
    }
  where
    one :: (StoreRead -> IO a) -> IO a
    one f = withView r (f . layeredRead)

-------------------------------------------------------------------------------
-- Blocks
-------------------------------------------------------------------------------

-- | Hash of the best block, if the database has any.
getBestBlock :: View -> IO (Maybe BlockHash)
getBestBlock (View r) = Q.getBestBlock r

-- | Hashes of the blocks at a height, of the main chain and of side chains.
getBlocksAtHeight :: View -> BlockHeight -> IO [BlockHash]
getBlocksAtHeight (View r) = Q.getBlocksAtHeight r

-- | Data of a block.
getBlock :: View -> BlockHash -> IO (Maybe BlockData)
getBlock (View r) = Q.getBlock r

-------------------------------------------------------------------------------
-- Transactions
-------------------------------------------------------------------------------

-- | Data of a transaction, with the spenders of its outputs, those in blocks
-- and those in the mempool. A transaction in the mempool comes from the
-- mempool area, a confirmed one from the chain, and one that was deleted
-- from where it was kept: the mempool area, or the chain for those that
-- version 20 deleted.
getTxData :: View -> TxHash -> IO (Maybe TxData)
getTxData v h =
  getManyTxData v [h] <&> \case
    [found] -> found
    _ -> Nothing

-- | Data of many transactions, in the order of the hashes. Each column
-- family that has to be read is read with one multi-get.
getManyTxData :: View -> [TxHash] -> IO [Maybe TxData]
getManyTxData (View r) hs = do
  found <- bodies r hs
  spent <- mempoolSpenders r (Map.toList found)
  let tds = [withSpenders spent h <$> Map.lookup h found | h <- hs]
  Q.count r $ \s -> incrementCounter s.dataTxCount (length (catMaybes tds))
  return tds

-- | Transactions whose hash, read as a number, matches a short identifier of
-- the Blockchain.info API, deleted ones included, in the order of their
-- hashes. There may be more than one.
getNumTxData :: View -> Word64 -> IO [TxData]
getNumTxData (View r) i = do
  unconfirmed <- matchingAsListCF r.db (mempoolCF r.db) (TxKeyS prefix)
  chain <- matchingAsListCF r.db (txCF r.db) (TxKeyS prefix)
  gone <- matchingAsListCF r.db (mempoolCF r.db) (MemDeletedKeyP prefix)
  let found =
        preferred
          (Map.fromList [(h, td) | (TxKey h, td) <- unconfirmed])
          (Map.fromList [(h, td) | (TxKey h, td) <- chain])
          (Map.fromList [(h, td) | (MemDeletedKey h, td) <- gone])
      matches = filter (seventh . fst) (Map.toAscList found)
  spent <- mempoolSpenders r matches
  Q.count r $ \s -> incrementCounter s.dataTxCount (length matches)
  return [withSpenders spent h td | (h, td) <- matches]
  where
    (prefix, bits) = decodeTxKey i
    -- the keys match on the first six bytes of the hash; the identifier
    -- also has the five high bits of the seventh
    seventh h = BS.index (encode h) 6 .&. 0xf8 == bits

-- | Input that spends an output, in the mempool or in a block.
getSpender :: View -> OutPoint -> IO (Maybe Spender)
getSpender (View r) p =
  retrieveCF r.db (mempoolCF r.db) (MemSpenderKey p) >>= \case
    Just s -> return (Just s)
    Nothing -> Q.getSpender r p

-- | The bodies of transactions by hash, each from where it is kept, with
-- the spenders that blocks give to its outputs and without those of the
-- mempool.
--
-- The mempool area is asked first, although most transactions asked for are
-- confirmed: with its filter, finding that a transaction is not there costs
-- a fraction of reading a body, and the chain is then asked only for the
-- others.
bodies :: DatabaseReader -> [TxHash] -> IO (Map TxHash TxData)
bodies r hs = do
  unconfirmed <- look (mempoolCF r.db) TxKey hs
  chain <- look (txCF r.db) TxKey (filter (`Map.notMember` unconfirmed) hs)
  let live h = Map.member h unconfirmed || maybe False (not . (.deleted)) (Map.lookup h chain)
  gone <- look (mempoolCF r.db) MemDeletedKey (filter (not . live) hs)
  return (preferred unconfirmed chain gone)
  where
    look :: (Serialize key) => ColumnFamily -> (TxHash -> key) -> [TxHash] -> IO (Map TxHash TxData)
    look _ _ [] = return Map.empty
    look cf key ks =
      Map.fromList . catMaybes . zipWith (fmap . (,)) ks
        <$> Q.retrieveManyCF r.db cf (map key ks)

-- | The body to answer with for each transaction, given the bodies that the
-- mempool area has for transactions in the mempool, those that the chain
-- has, and those that the mempool area has for deleted transactions.
--
-- A transaction that is in the mempool or confirmed is in one place only.
-- One that was deleted can also have a body that version 20 left in the
-- chain, marked as deleted, which counts only when nothing else is known of
-- the transaction.
preferred :: Map TxHash TxData -> Map TxHash TxData -> Map TxHash TxData -> Map TxHash TxData
preferred unconfirmed chain gone =
  unconfirmed <> Map.filter (not . (.deleted)) chain <> gone <> chain

-- | The spenders that the mempool has for the outputs of transactions,
-- given their bodies as 'bodies' gives them.
--
-- Only the outputs that no block spends are looked up, since the mempool
-- cannot spend the others: all of them with one multi-get, which the filter
-- of the column family answers for the many outputs that the mempool does
-- not spend. A transaction with more than 'scanFrom' such outputs has its
-- spenders read with a scan of their common prefix instead.
mempoolSpenders :: DatabaseReader -> [(TxHash, TxData)] -> IO (HashMap TxHash (IntMap Spender))
mempoolSpenders r txs = do
  found <- case points of
    [] -> return []
    _ -> Q.retrieveManyCF r.db (mempoolCF r.db) (map MemSpenderKey points)
  scanned <- mapM (\(h, _) -> (h,) <$> M.getMempoolSpenders r h) wide
  return . HashMap.fromListWith IntMap.union $
    [(p.hash, IntMap.singleton (fromIntegral p.index) s) | (p, Just s) <- zip points found]
      <> scanned
  where
    open :: [(TxHash, [Word32])]
    open =
      [ (h, [i | (i, _) <- zip [0 ..] td.tx.outputs, IntMap.notMember (fromIntegral i) td.spenders])
      | (h, td) <- txs,
        not td.deleted
      ]
    (narrow, wide) = partition (null . drop scanFrom . snd) open
    points = [OutPoint h i | (h, is) <- narrow, i <- is]

-- | Number of outputs of a transaction above which the mempool spenders of
-- the transaction are read with a scan. A scan costs about as much as eight
-- lookups in a multi-get whatever it finds, by the measurements in
-- @docs\/layered-database.md@.
scanFrom :: Int
scanFrom = 8

-- | Add the spenders that the mempool has for the outputs of a transaction
-- to those in its body.
withSpenders :: HashMap TxHash (IntMap Spender) -> TxHash -> TxData -> TxData
withSpenders spent h td = case HashMap.lookup h spent of
  Nothing -> td
  Just unconfirmed -> (td :: TxData) {spenders = IntMap.union td.spenders unconfirmed}

-------------------------------------------------------------------------------
-- Addresses
-------------------------------------------------------------------------------

-- | Balance of an address, with the mempool applied. 'Nothing' if neither
-- the chain nor the mempool has anything for the address; see
-- 'M.mergeBalance' for where version 20 says zero instead.
--
-- It is two reads by key, the balance of the chain and the summary of the
-- mempool, however much the mempool does at the address.
getBalance :: View -> Address -> IO (Maybe Balance)
getBalance (View r) a = M.summaryBalance a <$> Q.getBalance r a <*> (only <$> M.getSummaries r [a])
  where
    only = \case
      [s] -> s
      _ -> Nothing

-- | Balances of addresses, in the same order, with the mempool applied.
-- Addresses never used get a zero balance. Two multi-gets, one in each area.
getBalances :: View -> [Address] -> IO [Balance]
getBalances (View r) as = zipWith apply <$> Q.getBalances r as <*> M.getSummaries r as
  where
    apply c = maybe c (M.applySummary c)

-- | Unspent output, if it exists and neither a block nor the mempool spends
-- it: one that the chain has, or one of a mempool transaction.
getUnspent :: View -> OutPoint -> IO (Maybe Unspent)
getUnspent (View r) p =
  retrieveCF r.db (mempoolCF r.db) (MemSpenderKey p) >>= \case
    Just (_ :: Spender) -> return Nothing
    Nothing ->
      Q.getUnspent r p >>= \case
        Just u -> return (Just u)
        Nothing -> (>>= output) <$> M.getMempoolTx r p.hash
  where
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

-- | Transactions of an address, newest first.
getAddressTxs :: View -> Address -> Limits -> IO [TxRef]
getAddressTxs v a = getAddressesTxs v [a]

-- | Transactions of any of the addresses, newest first, each once.
getAddressesTxs :: View -> [Address] -> Limits -> IO [TxRef]
getAddressesTxs (View r) as limits = paged r limits $ \from -> do
  txs <- withMempoolIters r from as (const True) $ \mempools ->
    Q.withManyIters r (addrTxCF r.db) (length as) $ \its ->
      Q.joinConduits txRefKey (zipWith3 (addressTxsC from) as (map (fmap snd) mempools) its) limits
  Q.count r $ \s -> incrementCounter s.dataAddrTxCount (length txs)
  return txs

-- | Unspent outputs of an address, newest first.
getAddressUnspents :: View -> Address -> Limits -> IO [Unspent]
getAddressUnspents v a = getAddressesUnspents v [a]

-- | Unspent outputs of any of the addresses, newest first.
getAddressesUnspents :: View -> [Address] -> Limits -> IO [Unspent]
getAddressesUnspents (View r) as limits = paged r limits $ \from -> do
  us <- withUnspentStreams r from as $ \streams ->
    Q.joinConduits unspentKey streams limits
  Q.count r $ \s -> incrementCounter s.dataUnspentCount (length us)
  return us

-------------------------------------------------------------------------------
-- Pages
-------------------------------------------------------------------------------

-- | Where a page starts in a list that has the mempool's part first and the
-- chain's part after it.
data From
  = -- | at the newest item
    Top
  | -- | in the mempool's part, at the transaction that entered the mempool
    -- at this time and has this hash, or where it would be
    InMempool !UnixTime !TxHash
  | -- | in the chain's part, at this position: nothing of the mempool is in
    -- the page
    InChain !BlockRef

-- | Where the page of a query starts, or 'Nothing' if it starts at a
-- transaction that is not known, which makes the page empty.
--
-- A page can start at a transaction that was deleted. It then starts where
-- that transaction was, as it did in version 20.
pageStart :: DatabaseReader -> Maybe Start -> IO (Maybe From)
pageStart _ Nothing = return (Just Top)
pageStart _ (Just (AtBlock height)) = return (Just (InChain (BlockRef height maxBound)))
pageStart r (Just (AtTx h)) =
  bodies r [h] <&> \found -> case (.block) <$> Map.lookup h found of
    Nothing -> Nothing
    Just (MemRef time) -> Just (InMempool time h)
    Just position@BlockRef {} -> Just (InChain position)

-- | Answer a query from where its page starts.
paged :: DatabaseReader -> Limits -> (From -> IO [a]) -> IO [a]
paged r limits answer = pageStart r limits.start >>= maybe (return []) answer

-- | Run an action with, for each address, its summary and an iterator on
-- its entries in the mempool area, or 'Nothing' for an address whose
-- entries the list does not need: one that the mempool does not touch, which
-- is nearly all of them, one whose summary says that there is nothing of
-- the kind wanted, and any address when the page starts in the chain, as
-- every page after the first of a long history does.
--
-- The summaries are read by key in one multi-get, which the filter of the
-- column family answers for the addresses that have none. No iterator is
-- moved to an address with nothing there: such a seek costs ten times a
-- read by key, and far more when it has to step over what a block just
-- deleted.
withMempoolIters ::
  DatabaseReader ->
  From ->
  [Address] ->
  (MemSummary -> Bool) ->
  ([Maybe (MemSummary, Iterator)] -> IO a) ->
  IO a
withMempoolIters r from as wanted action = do
  summaries <- case from of
    InChain _ -> return (map (const Nothing) as)
    _ -> map (>>= \s -> if wanted s then Just s else Nothing) <$> M.getSummaries r as
  Q.withManyIters r (mempoolAddrCF r.db) (length (catMaybes summaries)) (action . pair summaries)
  where
    pair (Just s : rest) (it : its) = Just (s, it) : pair rest its
    pair (_ : rest) its = Nothing : pair rest its
    pair [] _ = []

-- | Transactions of an address, newest first, from where a page starts:
-- those of the mempool that belong in it, streamed from the entries of the
-- address if it has any, then those of the chain, streamed from its index.
-- Neither is read further than the page goes.
addressTxsC :: From -> Address -> Maybe Iterator -> Iterator -> ConduitT i TxRef IO ()
addressTxsC from a mempool it = mapM_ (mempoolTxsC from a) mempool >> chainTxsC from a it

-- | Transactions of an address in the chain, newest first, from where a
-- page starts, streamed from the index of the address.
chainTxsC :: From -> Address -> Iterator -> ConduitT i TxRef IO ()
chainTxsC from a it = chain .| concatMapC ref
  where
    chain = case from of
      InChain position -> matchingSkip it (AddrTxKeyA a) (AddrTxKeyB a position)
      _ -> matching it (AddrTxKeyA a)
    ref :: (AddrTxKey, ()) -> Maybe TxRef
    ref (AddrTxKey _ t, ()) = Just t
    ref _ = Nothing

-- | Mempool transactions of an address, newest first, from where a page
-- starts. Those that entered the mempool in the same second are in the
-- order of their hashes, which is the order of their keys.
mempoolTxsC :: From -> Address -> Iterator -> ConduitT i TxRef IO ()
mempoolTxsC from a it = rows .| concatMapC ref
  where
    rows = case from of
      Top -> matching it (MemAddrTxKeyA a)
      InMempool time h -> matchingSkip it (MemAddrTxKeyA a) (MemAddrTxKey a time h)
      InChain _ -> return ()
    ref :: (MemAddrKey, MemAddrVal) -> Maybe TxRef
    ref (MemAddrTxKey _ t h, _) = Just TxRef {block = MemRef t, txid = h}
    ref _ = Nothing

-- | Run an action with the stream of unspent outputs of each address, from
-- where a page starts.
--
-- The summaries are read whatever the start of the page: the chain still
-- has, as unspent, the outputs that the mempool spends, on every page.
withUnspentStreams ::
  DatabaseReader ->
  From ->
  [Address] ->
  ([ConduitT i Unspent IO ()] -> IO a) ->
  IO a
withUnspentStreams r from as action = do
  summaries <- M.getSummaries r as
  let fresh = case from of
        InChain _ -> map (const Nothing) as
        _ -> map (>>= \s -> if s.paidCount > s.bothCount then Just s else Nothing) summaries
  Q.withManyIters r (mempoolAddrCF r.db) (length (catMaybes fresh)) $ \mits ->
    Q.withManyIters r (addrOutCF r.db) (length as) $ \its ->
      action (zipWith3 stream (zip as summaries) (pair fresh mits) its)
  where
    stream (a, s) = addressUnspentsC r from a s
    pair (Just _ : rest) (it : its) = Just it : pair rest its
    pair (_ : rest) its = Nothing : pair rest its
    pair [] _ = []

-- | Unspent outputs of an address, newest first, from where a page starts,
-- given the summary of what the mempool does at the address, if anything,
-- and an iterator on its entries if the page has some: the outputs that the
-- mempool created and has not spent, then those of the chain that the
-- mempool does not spend, streamed from the index of the address.
--
-- Whether the mempool spends an output is a read by key, and the summary
-- says how many of them there are to find: none for nearly every address,
-- so that nothing is read, and when the last one is found the reads stop.
addressUnspentsC ::
  DatabaseReader ->
  From ->
  Address ->
  Maybe MemSummary ->
  Maybe Iterator ->
  Iterator ->
  ConduitT i Unspent IO ()
addressUnspentsC r from a summary mempool it = do
  mapM_ (\mit -> fresh mit .| concatMapC created .| hide (maybe 0 (.bothCount) summary) 1) mempool
  chainUnspentsC r from a it .| hide (maybe 0 (\s -> s.spentCount - s.bothCount) summary) 1
  where
    -- the outputs of the transaction that the page starts at come first
    -- among those of the same second whose hashes are not smaller
    fresh mit = case from of
      Top -> matching mit (MemAddrOutKeyA a)
      InMempool time h -> matchingSkip mit (MemAddrOutKeyA a) (MemAddrOutKey a time (OutPoint h 0))
      InChain _ -> return ()
    created :: (MemAddrKey, MemAddrVal) -> Maybe Unspent
    created (MemAddrOutKey _ t p, MemAddrOutVal v s) =
      Just Unspent {block = MemRef t, outpoint = p, value = v, script = s, address = Just a}
    created _ = Nothing
    -- let outputs through, but for those that the mempool spends, of which
    -- there are no more than a number. They are asked about several at a
    -- time, in one multi-get, which costs a fifth of asking about each: one
    -- at first, then twice as many each time. A page of one address is then
    -- a few reads, and the first output of each of the hundreds of
    -- addresses of a wallet, which is all that a merge of them may take,
    -- does not cost a batch of rows read ahead for each.
    hide :: Word64 -> Int -> ConduitT Unspent Unspent IO ()
    hide 0 _ = awaitForever yield
    hide n size =
      gather size >>= \case
        [] -> return ()
        batch -> do
          found <- lift (spentAmong batch)
          yieldMany [u | (u, False) <- zip batch found]
          hide (n - min n (fromIntegral (length (filter id found)))) (min hideBatch (size * 2))
    -- whether the mempool spends each of some outputs of the address: a
    -- read by key for each of a few, and one multi-get for more, which
    -- only then costs less
    spentAmong :: [Unspent] -> IO [Bool]
    spentAmong batch
      | length batch < 8 = forM batch $ \u -> do
          found <- retrieveCF r.db (mempoolAddrCF r.db) (MemAddrSpentKey a u.outpoint)
          return (isJust (found :: Maybe MemAddrVal))
      | otherwise =
          map isJust <$> multiGetCF r.db (mempoolAddrCF r.db) [encode (MemAddrSpentKey a u.outpoint) | u <- batch]
    gather :: Int -> ConduitT Unspent o IO [Unspent]
    gather 0 = return []
    gather k =
      await >>= \case
        Nothing -> return []
        Just u -> (u :) <$> gather (k - 1)

-- | The most outputs that are asked about at once when looking for those
-- that the mempool spends. More would save little, and each one is a row
-- read ahead of what a page may need.
hideBatch :: Int
hideBatch = 32

-- | Unspent outputs of an address in the chain, newest first, from where a
-- page starts, streamed from the index of the address. Those that the
-- mempool spends are among them.
chainUnspentsC :: DatabaseReader -> From -> Address -> Iterator -> ConduitT i Unspent IO ()
chainUnspentsC r from a it = chain .| concatMapC output
  where
    chain = case from of
      InChain position -> matchingSkip it (AddrOutKeyA a) (AddrOutKeyB a position)
      _ -> matching it (AddrOutKeyA a)
    output :: (AddrOutKey, OutVal) -> Maybe Unspent
    output = uncurry (toUnspent r.net r.ctx)

-------------------------------------------------------------------------------
-- Mempool
-------------------------------------------------------------------------------

-- | The whole mempool, newest first.
getMempool :: View -> IO [(UnixTime, TxHash)]
getMempool (View r) = Q.getMempool r

-- | Page of the mempool, newest first, from the offset and limit of the
-- t'Limits'; its start is ignored. The list of the mempool and its index by
-- time are the same in both layouts.
getMempoolPage :: View -> Limits -> IO [(UnixTime, TxHash)]
getMempoolPage (View r) = Q.getMempoolPage r

-------------------------------------------------------------------------------
-- Extended public keys
-------------------------------------------------------------------------------

-- | Balances of the addresses of an extended public key, derived up to the
-- exact gap (see 'scanXPubBals'), with the mempool applied. An address that
-- only the mempool has used counts as used, as it did in version 20.
xPubBals :: View -> XPubSpec -> IO [XPubBal]
xPubBals v@(View r) xpub = do
  Derived ext chg <- derivedAddrs r xpub
  bals <- scanXPubBals (getBalances v) r.initGap r.maxGap ext chg
  Q.count r $ \s -> incrementCounter s.dataXPubBals (length bals)
  return bals

-- | Unspent outputs of an extended public key, newest first, given its
-- balances from @xPubBals@. The addresses are read in step, each only as far
-- as the page needs, as 'Q.xPubUnspents' reads them.
xPubUnspents :: View -> [XPubBal] -> Limits -> IO [XPubUnspent]
xPubUnspents (View r) xbals limits = paged r limits $ \from -> do
  let quota = case (deOffset limits).limit of
        0 -> Nothing
        l -> Just l
      tag b = mapC (\u -> XPubUnspent {path = b.path, unspent = u})
  us <- withUnspentStreams r from (map (.balance.address) funded) $ \streams ->
    runConduit $
      mergeDescGroups (.unspent.block) (unspentKey . (.unspent)) quota (zipWith (\b s -> s .| tag b) funded streams)
        .| applyLimitsC limits
        .| sinkList
  Q.count r $ \s -> incrementCounter s.dataXPubUnspents (length us)
  return us
  where
    funded = filter ((> 0) . (.balance.utxo)) xbals

-- | Transactions of an extended public key, newest first, given its balances
-- from @xPubBals@.
xPubTxs :: View -> [XPubBal] -> Limits -> IO [TxRef]
xPubTxs v@(View r) xbals limits = do
  txs <- getAddressesTxs v (Q.usedAddresses xbals) limits
  Q.count r $ \s -> incrementCounter s.dataXPubTxs (length txs)
  return txs

-- | Number of transactions of an extended public key, given its balances
-- from @xPubBals@.
xPubTxCount :: View -> [XPubBal] -> IO Word32
xPubTxCount v@(View r) xbals = do
  Q.count r $ \s -> incrementCounter s.dataXPubTxCount 1
  getAddressesTxCount v (Q.usedAddresses xbals)

-- | Number of distinct transactions of a set of addresses: each transaction
-- is counted once even when it touches several of them.
--
-- Those of the chain are counted from the keys of its index, as
-- 'Q.getAddressesTxCount' counts them, and those of the mempool from the
-- summaries of the addresses: nothing more is read when none of them, or
-- only one, has transactions in the mempool, and otherwise the transactions
-- of those that have are read, to count each once. No transaction is in
-- both areas.
getAddressesTxCount :: View -> [Address] -> IO Word32
getAddressesTxCount _ [] = return 0
getAddressesTxCount (View r) as = do
  inBlocks <- Q.getAddressesTxCount r as
  inMempool <- withMempoolIters r Top as ((> 0) . (.txs)) $ \mempools ->
    case [(a, m) | (a, Just m) <- zip as mempools] of
      [] -> return 0
      [(_, (s, _))] -> return (fromIntegral s.txs)
      several -> do
        txs <- forM several $ \(a, (_, it)) -> runConduit (mempoolTxsC Top a it .| mapC (.txid) .| sinkList)
        return (HashSet.size (HashSet.fromList (concat txs)))
  return (inBlocks + fromIntegral inMempool)
