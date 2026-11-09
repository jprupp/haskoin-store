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
-- @docs\/layered-database.md@. The store does not use this layout yet. Each
-- query here answers as the query of the same name answers on a database of
-- today's layout, pages included. The tests check that, on databases
-- migrated from today's layout and on databases built by the rules of
-- "Haskoin.Store.Logic.Layered".
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
-- would be missing. Today's queries need no such care, because each of their
-- answers is one key or one scan.
--
-- So every query takes a t'View', a snapshot of the database, and 'withView'
-- is the only way to get one: a query cannot be given the database itself by
-- mistake. The web server is meant to take one view for each request. That
-- also makes the several queries of a request agree with each other, which
-- they do not have to today.
--
-- = Pages
--
-- Lists are ordered newest first, and unconfirmed transactions are newer
-- than any block. The list of an address is therefore what the mempool has
-- for it followed by what the chain has, and that is the sequence that the
-- single index of today's layout holds. A page is cut from it by the same
-- rules: where it starts, then the offset, then the limit. The mempool's part
-- is small and read whole. The chain's part is streamed from its index, only
-- as far as the page needs.
module Haskoin.Store.Database.Query.Layered
  ( -- * A view of the database
    View,
    withView,
    layeredRead,

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
    concatMapC,
    filterC,
    mapC,
    runConduit,
    sinkList,
    yieldMany,
    (.|),
  )
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
import Data.Maybe (catMaybes)
import Data.Ord (Down (..))
import Data.Serialize (Serialize, encode)
import Data.Word (Word32, Word64)
import Database.RocksDB (ColumnFamily, Iterator, withSnapshot)
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
import Haskoin.Store.Database.Mempool (AddressMempool (..))
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
  ( DataMetrics (..),
    DatabaseReader (..),
    Derived (..),
    addrOutCF,
    addrTxCF,
    derivedAddrs,
    mempoolCF,
    txCF,
  )
import Haskoin.Store.Database.Types
  ( AddrOutKey (..),
    AddrTxKey (..),
    MemDeletedKey (..),
    MemSpenderKey (..),
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
-- 'Q.databaseRead' gives those of a database of today's layout.
layeredRead :: View -> StoreRead
layeredRead v@(View r) =
  StoreRead
    { net = r.net,
      ctx = r.ctx,
      getBestBlock = getBestBlock v,
      getBlocksAtHeight = getBlocksAtHeight v,
      getBlock = getBlock v,
      getTxData = getTxData v,
      getManyTxData = getManyTxData v,
      getSpender = getSpender v,
      getBalance = getBalance v,
      getUnspent = getUnspent v,
      getMempool = getMempool v,
      getMempoolPage = getMempoolPage v,
      getAddressesTxs = getAddressesTxs v,
      getAddressesUnspents = getAddressesUnspents v,
      initialGap = r.initGap,
      maxGap = r.maxGap,
      getNumTxData = getNumTxData v,
      getBalances = getBalances v,
      getAddressTxs = getAddressTxs v,
      getAddressUnspents = getAddressUnspents v,
      xPubBals = xPubBals v,
      xPubUnspents = \_ -> xPubUnspents v,
      xPubTxs = \_ -> xPubTxs v,
      xPubTxCount = \_ -> xPubTxCount v
    }

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
getBalance :: View -> Address -> IO (Maybe Balance)
getBalance (View r) a = M.mergeBalance a <$> Q.getBalance r a <*> M.getAddressMempool r a

-- | Balances of addresses, in the same order, with the mempool applied.
-- Addresses never used get a zero balance.
getBalances :: View -> [Address] -> IO [Balance]
getBalances (View r) as = zipWith M.applyMempool <$> Q.getBalances r as <*> M.getAddressMempools r as

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
  unconfirmed <- mempoolTxs r from as
  txs <- Q.withManyIters r (addrTxCF r.db) (length as) $ \its ->
    Q.joinConduits txRefKey (zipWith3 (addressTxsC from) as unconfirmed its) limits
  Q.count r $ \s -> incrementCounter s.dataAddrTxCount (length txs)
  return txs

-- | Unspent outputs of an address, newest first.
getAddressUnspents :: View -> Address -> Limits -> IO [Unspent]
getAddressUnspents v a = getAddressesUnspents v [a]

-- | Unspent outputs of any of the addresses, newest first.
getAddressesUnspents :: View -> [Address] -> Limits -> IO [Unspent]
getAddressesUnspents (View r) as limits = paged r limits $ \from -> do
  mempools <- M.getAddressMempools r as
  us <- Q.withManyIters r (addrOutCF r.db) (length as) $ \its ->
    Q.joinConduits unspentKey (zipWith3 (addressUnspentsC r from) as mempools its) limits
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
-- that transaction was, as it does today.
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

-- | The mempool transactions of each address that belong in a list that
-- starts somewhere, newest first. The mempool area is not read for a list
-- that starts in the chain, which is what every page after the first of a
-- long history does.
mempoolTxs :: DatabaseReader -> From -> [Address] -> IO [[TxRef]]
mempoolTxs r from as = case from of
  Top -> map (.txs) <$> M.getAddressMempools r as
  InMempool time h -> map (dropWhile (newer time h) . (.txs)) <$> M.getAddressMempools r as
  InChain _ -> return (map (const []) as)
  where
    -- those that entered the mempool in the same second are in the order of
    -- their hashes
    newer time h t = txRefKey t > (MemRef time, Down h)

-- | Transactions of an address, newest first, from where a page starts:
-- those of the mempool that belong in it, as 'mempoolTxs' gives them, then
-- those of the chain, streamed from the index of the address.
addressTxsC :: From -> Address -> [TxRef] -> Iterator -> ConduitT i TxRef IO ()
addressTxsC from a unconfirmed it = yieldMany unconfirmed >> (chain .| concatMapC ref)
  where
    chain = case from of
      InChain position -> matchingSkip it (AddrTxKeyA a) (AddrTxKeyB a position)
      _ -> matching it (AddrTxKeyA a)
    ref :: (AddrTxKey, ()) -> Maybe TxRef
    ref (AddrTxKey _ t, ()) = Just t
    ref _ = Nothing

-- | Unspent outputs of an address, newest first, from where a page starts,
-- given what the mempool does at the address: the outputs that the mempool
-- created and has not spent, then those of the chain that the mempool does
-- not spend, streamed from the index of the address.
addressUnspentsC :: DatabaseReader -> From -> Address -> AddressMempool -> Iterator -> ConduitT i Unspent IO ()
addressUnspentsC r from a m it = yieldMany fresh >> (chain .| concatMapC output .| filterC visible)
  where
    fresh = case from of
      Top -> M.mempoolUnspents m
      InMempool time h -> dropWhile (newer time h) (M.mempoolUnspents m)
      InChain _ -> []
    -- the outputs of the transaction that the page starts at come first
    -- among those of the same second whose hashes are not smaller
    newer time h u = unspentKey u > (MemRef time, Down (encode (OutPoint h 0)))
    chain = case from of
      InChain position -> matchingSkip it (AddrOutKeyA a) (AddrOutKeyB a position)
      _ -> matching it (AddrOutKeyA a)
    output :: (AddrOutKey, OutVal) -> Maybe Unspent
    output = uncurry (toUnspent r.net r.ctx)
    -- the chain still has, as unspent, the outputs that the mempool spends
    visible u = not (HashMap.member u.outpoint m.spent)

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
-- only the mempool has used counts as used, as it does today.
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
  mempools <- M.getAddressMempools r (map (.balance.address) funded)
  let quota = case (deOffset limits).limit of
        0 -> Nothing
        l -> Just l
      stream b m it =
        addressUnspentsC r from b.balance.address m it
          .| mapC (\u -> XPubUnspent {path = b.path, unspent = u})
  us <- Q.withManyIters r (addrOutCF r.db) (length funded) $ \its ->
    runConduit $
      mergeDescGroups (.unspent.block) (unspentKey . (.unspent)) quota (zipWith3 stream funded mempools its)
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
-- entries of the addresses. No transaction is in both.
getAddressesTxCount :: View -> [Address] -> IO Word32
getAddressesTxCount _ [] = return 0
getAddressesTxCount (View r) as = do
  inBlocks <- Q.getAddressesTxCount r as
  mempools <- M.getAddressMempools r as
  let inMempool = HashSet.fromList [t.txid | m <- mempools, t <- m.txs]
  return (inBlocks + fromIntegral (HashSet.size inMempool))
