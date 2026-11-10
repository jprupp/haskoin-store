{-# LANGUAGE RecordWildCards #-}

-- |
-- Module      : Haskoin.Store.Common
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Interfaces shared by every part of the store.
--
-- 'StoreRead' is the queries that the web server asks about blocks,
-- transactions, addresses and extended public keys, as a record of functions
-- in 'IO': "Haskoin.Store.Database.Query" gives them for the RocksDB
-- database, and the web server puts the Redis cache in front of some.
--
-- This module also has helpers for pagination ('Limits'), for deriving
-- addresses from extended public keys ('scanXPubBals'), for the order of
-- transactions and outputs ('txRefKey', 'unspentKey'), and for merging sorted
-- streams.
--
-- = Why pages start at a transaction
--
-- Paging by offset alone gets slower the deeper you go, because the store must
-- read and drop every result before the page, and it shifts when new
-- transactions arrive. 'Start' lets a client continue from the last transaction
-- it saw: the store seeks straight to it, and new arrivals do not move the page.
module Haskoin.Store.Common
  ( Limits (..),
    Start (..),
    StoreRead (..),
    StoreEvent (..),
    getActiveBlock,
    getActiveTxData,
    notDeleted,
    getDefaultBalance,
    getTransaction,
    getTransactions,
    getNumTransaction,
    blockAtOrAfter,
    blockAtOrBefore,
    blockAtOrAfterMTP,
    xPubSummary,
    deriveAddresses,
    deriveFunction,
    scanXPubBals,
    txRefKey,
    unspentKey,
    deOffset,
    applyLimits,
    applyLimitsC,
    applyLimit,
    applyLimitC,
    sortTxs,
    nub',
    microseconds,
    streamThings,
    joinDescStreams,
    mergeDescGroups,
    loopForever,
  )
where

import Conduit
  ( ConduitT,
    await,
    dropC,
    mapC,
    sealConduitT,
    takeC,
    yield,
    ($$++),
  )
import Control.Monad.Trans (lift)
import Data.ByteString (ByteString)
import Data.Default (Default (..))
import Data.Functor ((<&>))
import Data.Graph qualified as G
import Data.HashMap.Strict qualified as M
import Data.HashSet qualified as H
import Data.Hashable (Hashable)
import Data.List (sortOn)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe, mapMaybe)
import Data.Ord (Down (..))
import Data.Serialize (encode)
import Data.Time.Clock.System
  ( getSystemTime,
    systemNanoseconds,
    systemSeconds,
  )
import Data.Word (Word32, Word64)
import Haskoin
import Haskoin.Node (Chain, Peer, chainRunHeaders)
import Haskoin.Store.Data
  ( Balance (..),
    BlockData (..),
    BlockRef (..),
    DeriveType (..),
    Spender,
    Transaction (..),
    TxData (..),
    TxRef (..),
    UnixTime,
    Unspent (..),
    XPubBal (..),
    XPubSpec (..),
    XPubSummary (..),
    XPubUnspent (..),
    nullBalance,
    toTransaction,
    zeroBalance,
  )

-- | Function to derive the address at an index of an extended public key.
type DeriveAddr = XPubKey -> KeyIndex -> Address

-- | Number of items to skip.
type Offset = Word32

-- | Maximum number of items to return; @0@ means no limit.
type Limit = Word32

-- | Where a page of results ordered from newest to oldest starts.
data Start
  = -- | at this transaction; for an unconfirmed transaction, at the time it
    -- entered the mempool, and for a confirmed one, at its position in the
    -- chain
    AtTx !TxHash
  | -- | at the last transaction confirmed at or below this block height.
    -- Nothing of the mempool is in the page, not even when the height is
    -- above the best block
    AtBlock !BlockHeight
  deriving (Eq, Show)

-- | Pagination of a query. Results are ordered from newest to oldest. The
-- 'start' is applied first, then the 'offset', then the 'limit'.
data Limits = Limits
  { -- | maximum number of results; @0@ for no limit
    limit :: !Word32,
    -- | number of results to skip after the start
    offset :: !Word32,
    -- | where to start, or from the newest result if 'Nothing'
    start :: !(Maybe Start)
  }
  deriving (Eq, Show)

-- | No limit, no offset, start from the newest result.
defaultLimits :: Limits
defaultLimits = Limits {limit = 0, offset = 0, start = Nothing}

instance Default Limits where
  def = defaultLimits

-- | The queries that the web server answers with, as plain functions in
-- 'IO'. "Haskoin.Store.Database.Query" gives them for the database
-- ('Haskoin.Store.Database.Query.databaseRead'); the web server puts the
-- Redis cache in front of some of them.
--
-- A record of functions rather than a class: a class needs a monad to pick
-- its instance, and the store has no monads of its own to pick it with, only
-- 'IO'. The record makes the source of every answer an ordinary value that
-- can be passed around and replaced, as @nocache=true@ does.
data StoreRead = StoreRead
  { -- | network the store indexes
    net :: !Network,
    -- | context for secp256k1 operations, needed to parse and derive keys
    ctx :: !Ctx,
    -- | hash of the best block imported, or 'Nothing' if none yet
    getBestBlock :: IO (Maybe BlockHash),
    -- | hashes of all blocks imported at a height, side chains included
    getBlocksAtHeight :: BlockHeight -> IO [BlockHash],
    -- | data of a block imported in the past, whether still in the main
    -- chain or not; check 'Haskoin.Store.Data.main' to know
    getBlock :: BlockHash -> IO (Maybe BlockData),
    -- | data of a transaction that is confirmed or in the mempool. One
    -- that was deleted is not returned, here or by the two queries below;
    -- see 'notDeleted'
    getTxData :: TxHash -> IO (Maybe TxData),
    -- | data of many transactions, in the order of their hashes, read
    -- together
    getManyTxData :: [TxHash] -> IO [Maybe TxData],
    -- | transaction input that spends an output, if it is spent
    getSpender :: OutPoint -> IO (Maybe Spender),
    -- | balance of an address, or 'Nothing' if it was never used
    getBalance :: Address -> IO (Maybe Balance),
    -- | unspent output, or 'Nothing' if it does not exist or is spent
    getUnspent :: OutPoint -> IO (Maybe Unspent),
    -- | whole mempool, newest first
    getMempool :: IO [(UnixTime, TxHash)],
    -- | page of the mempool, newest first, from the offset and limit of the
    -- 'Limits'; its start is ignored
    getMempoolPage :: Limits -> IO [(UnixTime, TxHash)],
    -- | transactions involving any of the addresses, newest first, without
    -- duplicates
    getAddressesTxs :: [Address] -> Limits -> IO [TxRef],
    -- | unspent outputs of any of the addresses, newest first
    getAddressesUnspents :: [Address] -> Limits -> IO [Unspent],
    -- | unused addresses to look at for an extended public key with no
    -- transactions yet
    initialGap :: !Word32,
    -- | consecutive unused addresses after which derivation of an extended
    -- public key stops
    maxGap :: !Word32,
    -- | transactions whose hash, read as a number, matches the short
    -- identifier of the Blockchain.info compatible API; there may be more
    -- than one
    getNumTxData :: Word64 -> IO [TxData],
    -- | balances of the addresses, in the same order; addresses never used
    -- get a zero balance
    getBalances :: [Address] -> IO [Balance],
    -- | transactions involving an address, newest first
    getAddressTxs :: Address -> Limits -> IO [TxRef],
    -- | unspent outputs of an address, newest first
    getAddressUnspents :: Address -> Limits -> IO [Unspent],
    -- | balances of the used addresses of an extended public key, with
    -- their derivation paths
    xPubBals :: XPubSpec -> IO [XPubBal],
    -- | unspent outputs of an extended public key, newest first, given its
    -- balances from 'xPubBals'
    xPubUnspents :: XPubSpec -> [XPubBal] -> Limits -> IO [XPubUnspent],
    -- | transactions of an extended public key, newest first, given its
    -- balances from 'xPubBals'
    xPubTxs :: XPubSpec -> [XPubBal] -> Limits -> IO [TxRef],
    -- | number of transactions of an extended public key, given its
    -- balances from 'xPubBals'
    xPubTxCount :: XPubSpec -> [XPubBal] -> IO Word32,
    -- | number of distinct transactions of a set of addresses, counted
    -- without reading them
    getAddressesTxCount :: [Address] -> IO Word32
  }

-- | Data of a block, or 'Nothing' if it is unknown or not in the main chain.
getActiveBlock :: StoreRead -> BlockHash -> IO (Maybe BlockData)
getActiveBlock r bh =
  r.getBlock bh <&> \case
    Just b | b.main -> Just b
    _ -> Nothing

-- | Leave out a transaction that was deleted: replaced, dropped from the
-- mempool, or the coinbase of a block that left the main chain.
--
-- The store used to answer with such transactions, marked as deleted, so
-- that a wallet could say what became of a payment. Some clients do not
-- look at the mark, and took a payment that will never happen for one that
-- is pending. So every 'StoreRead' leaves them out, and to the API a
-- deleted transaction is one that the store does not know. Since database
-- version 22 nothing is kept of them at all; this is for the bodies that
-- version 20 left in a database that was migrated.
notDeleted :: Maybe TxData -> Maybe TxData
notDeleted (Just td) | td.deleted = Nothing
notDeleted found = found

-- | Data of a transaction, or 'Nothing' if it is unknown or was deleted.
-- The same as the query of a 'StoreRead' now that it leaves deleted
-- transactions out.
getActiveTxData :: StoreRead -> TxHash -> IO (Maybe TxData)
getActiveTxData r th =
  r.getTxData th <&> \case
    Just td | not td.deleted -> Just td
    _ -> Nothing

-- | Balance of an address, or a zero balance if the address was never used.
getDefaultBalance :: StoreRead -> Address -> IO Balance
getDefaultBalance r a = fromMaybe (zeroBalance a) <$> r.getBalance a

-- | Infinite list of the addresses of an extended public key, with their
-- indices, from the given index on.
deriveAddresses :: DeriveAddr -> XPubKey -> Word32 -> [(Word32, Address)]
deriveAddresses derive xpub start = map (\i -> (i, derive xpub i)) [start ..]

-- | Address derivation function for a type of address: legacy pay-to-public-key
-- hash, pay-to-witness-public-key-hash nested in pay-to-script-hash, or native
-- pay-to-witness-public-key-hash.
deriveFunction :: Ctx -> DeriveType -> DeriveAddr
deriveFunction ctx DeriveNormal i = fst . deriveAddr ctx i
deriveFunction ctx DeriveP2SH i = fst . deriveCompatWitnessAddr ctx i
deriveFunction ctx DeriveP2WPKH i = fst . deriveWitnessAddr ctx i

-- | Balances of the addresses of an extended public key, derived up to the
-- gap: every address of each chain up to the last used one, followed by
-- exactly @gap@ unused ones. Arguments, in order: a function to read the
-- balances of addresses, the initial gap, the gap, the external chain and
-- the change chain (both infinite lists of indices and addresses).
--
-- The first @initial gap@ external addresses are read first. If none is used,
-- the key is taken as unused, those balances are returned and nothing more is
-- read. Otherwise each chain is read until the last @gap@ addresses are
-- unused, counting the unused addresses at the end of the initial block too.
-- The initial block is always returned whole, even when it reaches past the
-- gap after its last used address (an initial gap larger than the gap): it
-- was read, so a payment to any of its addresses is seen, and the cache must
-- keep those addresses to see it too.
--
-- = Why the gap is exact
--
-- The gap is the number of unused addresses a wallet may hand out in a row.
-- Wallets follow BIP 44 and stop looking for payments after that many, so a
-- store that looked further would show payments the wallet cannot see, and a
-- store that sometimes looked further and sometimes not would make the answer
-- depend on how the reads happened to be batched. Earlier versions read 32
-- addresses at a time and checked the gap only between batches, so they
-- looked between 33 and 64 addresses past the last used one, and started
-- counting again at the end of the initial block. The Redis cache keeps
-- exactly @gap@ unused addresses, so cached and uncached answers differed
-- whenever a payment fell in the extra range.
--
-- Reads are still batched: each read asks for as many addresses as are still
-- needed to complete the gap if all of them turn out unused, so no address
-- past the gap is ever read, and a run of unused addresses costs one read.
scanXPubBals ::
  (Monad m) =>
  ([Address] -> m [Balance]) ->
  Word32 ->
  Word32 ->
  [(Word32, Address)] ->
  [(Word32, Address)] ->
  m [XPubBal]
scanXPubBals getBals igap gap ext chg = do
  let (first, rest) = splitAt (fromIntegral igap) ext
  ext1 <- readChain 0 first
  if all nullxb ext1
    then return ext1
    else do
      ext2 <- go (length ext1) 0 (reverse ext1) rest
      chg' <- go 0 1 [] chg
      return (ext2 <> chg')
  where
    g = fromIntegral gap :: Int
    nullxb b = nullBalance b.balance
    readChain m as = do
      bs <- getBals (map snd as)
      return $ zipWith (\(n, _) b -> XPubBal {path = [m, n], balance = b}) as bs
    -- acc holds the balances read so far, newest first; keep is the number
    -- of addresses at the start of the chain to return in any case
    trailing = length . takeWhile nullxb
    go keep m acc as
      | trailing acc >= g = return (trim keep acc)
      | otherwise =
          case splitAt (g - trailing acc) as of
            ([], _) -> return (trim keep acc)
            (now, later) -> do
              bs <- readChain m now
              go keep m (reverse bs <> acc) later
    trim keep acc =
      let (ns, ys) = span nullxb acc
          gapped = reverse ys <> take g (reverse ns)
       in if length gapped >= keep then gapped else take keep (reverse acc)

-- | Summary of the balances of an extended public key, from the balances of its
-- addresses. The received total only counts external addresses, and the
-- indices are those of the last used external and change addresses.
xPubSummary :: XPubSpec -> [XPubBal] -> XPubSummary
xPubSummary _xspec xbals =
  XPubSummary
    { confirmed = sum (map (.balance.confirmed) bs),
      unconfirmed = sum (map (.balance.unconfirmed) bs),
      received = rx,
      utxo = uc,
      change = ch,
      external = ex
    }
  where
    bs = filter (not . nullBalance . (.balance)) xbals
    ex = foldl max 0 [i | XPubBal {path = [0, i]} <- bs]
    ch = foldl max 0 [i | XPubBal {path = [1, i]} <- bs]
    uc = sum [b.balance.utxo | b <- bs]
    xt = [b | b@XPubBal {path = [0, _]} <- bs]
    rx = sum [b.balance.received | b <- xt]

-- | Transaction with its metadata, or 'Nothing' if it is unknown.
getTransaction :: StoreRead -> TxHash -> IO (Maybe Transaction)
getTransaction r h = fmap (toTransaction r.net r.ctx) <$> r.getTxData h

-- | Many transactions, in the order of their hashes, read together.
getTransactions :: StoreRead -> [TxHash] -> IO [Maybe Transaction]
getTransactions r hs = map (fmap (toTransaction r.net r.ctx)) <$> r.getManyTxData hs

-- | Transactions that match a short numeric identifier; see 'getNumTxData'.
getNumTransaction :: StoreRead -> Word64 -> IO [Transaction]
getNumTransaction r i = map (toTransaction r.net r.ctx) <$> r.getNumTxData i

-- | First block in the main chain whose timestamp is at or after the given time,
-- or 'Nothing' if there is none.
blockAtOrAfter :: StoreRead -> Chain -> UnixTime -> IO (Maybe BlockData)
blockAtOrAfter r ch q =
  chainRunHeaders ch (firstGreaterOrEqual r.net f) >>= \case
    Nothing -> return Nothing
    Just x -> r.getBlock (headerHash x.header)
  where
    f :: (BlockHeaders n) => BlockNode -> n Ordering
    f x =
      let t = fromIntegral x.header.timestamp
       in return $ t `compare` q

-- | Last block in the main chain whose timestamp is at or before the given time,
-- or 'Nothing' if there is none.
blockAtOrBefore :: StoreRead -> Chain -> UnixTime -> IO (Maybe BlockData)
blockAtOrBefore r ch q =
  chainRunHeaders ch (lastSmallerOrEqual r.net f) >>= \case
    Nothing -> return Nothing
    Just x -> r.getBlock (headerHash x.header)
  where
    f :: (BlockHeaders n) => BlockNode -> n Ordering
    f x =
      let t = fromIntegral x.header.timestamp
       in return $ t `compare` q

-- | First block in the main chain whose median time past is at or after the given
-- time, or 'Nothing' if there is none.
blockAtOrAfterMTP :: StoreRead -> Chain -> UnixTime -> IO (Maybe BlockData)
blockAtOrAfterMTP r ch q =
  chainRunHeaders ch (firstGreaterOrEqual r.net f) >>= \case
    Nothing -> return Nothing
    Just x -> r.getBlock (headerHash x.header)
  where
    f :: (BlockHeaders n) => BlockNode -> n Ordering
    f x = do
      t <- fromIntegral <$> mtp x
      return $ t `compare` q

-- | Events that the store can generate.
data StoreEvent
  = -- | a block was imported and is now the best block
    StoreBestBlock !BlockHash
  | -- | a transaction entered the mempool
    StoreMempoolNew !TxHash
  | -- | a transaction left the mempool without being confirmed, for example
    -- because it was replaced or a conflicting transaction was confirmed
    StoreMempoolDelete !TxHash
  | -- | a peer connected
    StorePeerConnected !Peer
  | -- | a peer disconnected
    StorePeerDisconnected !Peer
  | -- | a peer answered a ping with this nonce
    StorePeerPong !Peer !Word64
  | -- | a peer announced transactions
    StoreTxAnnounce !Peer ![TxHash]
  | -- | a peer rejected a transaction, with a code and a reason
    StoreTxReject !Peer !TxHash !RejectCode !ByteString

-- | Apply the offset and limit of 'Limits' to a list. The start is ignored.
applyLimits :: Limits -> [a] -> [a]
applyLimits Limits {..} = applyLimit limit . applyOffset offset

-- | Drop the first items of a list.
applyOffset :: Offset -> [a] -> [a]
applyOffset = drop . fromIntegral

-- | Keep the first items of a list; @0@ keeps them all.
applyLimit :: Limit -> [a] -> [a]
applyLimit 0 = id
applyLimit l = take (fromIntegral l)

-- | Fold the offset into the limit, so that a query returns enough results to
-- apply the offset afterwards, for example when merging results from several
-- sources.
--
-- When results from several sources are merged, the offset can only be applied
-- to the merged stream. Each source must therefore return the first
-- @offset + limit@ results.
deOffset :: Limits -> Limits
deOffset l = case l.limit of
  0 -> l {offset = 0}
  _ -> l {limit = l.limit + l.offset, offset = 0}

-- | Conduit version of 'applyLimits'.
applyLimitsC :: (Monad m) => Limits -> ConduitT i i m ()
applyLimitsC Limits {..} = applyOffsetC offset >> applyLimitC limit

-- | Conduit version of 'applyOffset'.
applyOffsetC :: (Monad m) => Offset -> ConduitT i i m ()
applyOffsetC = dropC . fromIntegral

-- | Conduit version of 'applyLimit'.
applyLimitC :: (Monad m) => Limit -> ConduitT i i m ()
applyLimitC 0 = mapC id
applyLimitC l = takeC (fromIntegral l)

-- | Sort transactions so that every transaction comes after those it spends.
-- Each transaction is returned with its position in the original list.
sortTxs :: [Tx] -> [(Word32, Tx)]
sortTxs txs = ts
  where
    hm :: M.HashMap TxHash Int
    hm = M.fromList $ zip (map txHash txs) [0 ..]
    ns :: [(Tx, Int, [Int])]
    ns =
      let is tx = nub' $ map (.outpoint.hash) tx.inputs
          hs tx = mapMaybe (`M.lookup` hm) (is tx)
          tp i tx = (tx, i, hs tx)
       in zipWith tp [0 ..] txs
    (g, n, _) = G.graphFromEdges ns
    vs = G.reverseTopSort g
    ts =
      let f (tx, i, _) = (fromIntegral i, tx)
       in map (f . n) vs

-- | Remove duplicates from a list. The order of the result is unspecified.
nub' :: (Hashable a) => [a] -> [a]
nub' = H.toList . H.fromList

-- | A 'Control.Monad.forever' that stays a flat loop in any monad.
-- 'Control.Monad.forever' sequences with @*>@; monad-logger's @LoggingT@ leaves @*>@ at its default
-- @(id <$ a) <*> b@ and @ReaderT@ defines @>>@ as @*>@, so a loop that is
-- polymorphic in its monad and not specialised by GHC keeps an @<*>@
-- continuation frame and two thunks per iteration for its whole lifetime.
-- Sequencing with @>>=@ only is a tail call in all these stacks.
loopForever :: (Monad m) => m a -> m b
loopForever action = go
  where
    go = action >>= \_ -> go
{-# INLINE loopForever #-}

-- | Current time in microseconds since the Unix epoch.
microseconds :: IO Integer
microseconds =
  let f t =
        toInteger (systemSeconds t) * 1000000
          + toInteger (systemNanoseconds t) `div` 1000
   in f <$> getSystemTime

-- | Stream every result of a paged query, fetching one page at a time.
--
-- If a function to get the transaction of a result is given, each page starts
-- at the last transaction of the previous one. Otherwise pages are fetched by
-- offset, doubling their size each time up to 1024 results.
--
-- This is how endpoints that combine many sources, such as the transactions of
-- every address of an xpub, avoid loading everything: each source is read page by
-- page, only as far as the merged output needs.
streamThings ::
  (Monad m) =>
  (Limits -> m [a]) ->
  Maybe (a -> TxHash) ->
  Limits ->
  ConduitT () a m ()
streamThings getit gettx limits =
  lift (getit limits) >>= \case
    [] -> return ()
    ls -> mapM_ yield ls >> go limits (last ls)
  where
    -- Without a transaction to start the next page from, the next page
    -- starts at an offset, which the store reads and drops. Double the
    -- page each time, up to 'maxPage', so that the rows read and dropped
    -- stay in proportion to the rows streamed, instead of growing with
    -- their square.
    h l x = case gettx of
      Just g -> Just l {offset = 1, start = Just (AtTx (g x))}
      Nothing -> case l.limit of
        0 -> Nothing
        _ -> Just l {offset = l.offset + l.limit, limit = max l.limit (min maxPage (2 * l.limit))}
    maxPage = 1024
    go l x = case h l x of
      Nothing -> return ()
      Just l' ->
        lift (getit l') >>= \case
          [] -> return ()
          ls -> mapM_ yield ls >> go l' (last ls)

-- | Merge streams sorted in decreasing order of a key into one stream sorted
-- in decreasing order of that key, dropping items whose key is the same as
-- the one before. The key must be the order the streams are really sorted
-- in: an item that comes out of order in its stream can be emitted twice.
--
-- Use 'txRefKey' for transactions and 'unspentKey' for unspent outputs: they
-- are the order the database and the cache both keep, including the order of
-- ties between mempool transactions that arrived in the same second.
joinDescStreams ::
  (Monad m, Ord k) =>
  (a -> k) ->
  [ConduitT () a m ()] ->
  ConduitT () a m ()
joinDescStreams key xs = do
  let ss = map sealConduitT xs
  go Nothing =<< g ss
  where
    j (x, y) = (\v -> (key v, [(v, x)])) <$> y
    g ss =
      let l = mapMaybe j <$> lift (traverse ($$++ await) ss)
       in Map.fromListWith (++) <$> l
    go m mp = case Map.lookupMax mp of
      Nothing -> return ()
      Just (k, vss) -> do
        case m of
          Just k' | k == k' -> return ()
          _ -> yield (fst (last vss))
        mp1 <- g (map snd vss)
        let mp2 = Map.deleteMax mp
            mp' = Map.unionWith (++) mp1 mp2
        go (Just k) mp'

-- | Order of transaction references, newest first when sorted in decreasing
-- order: by block position, and between mempool transactions that arrived in
-- the same second, by the bytes of their hashes in increasing order.
--
-- The tie order is not the derived 'Ord' of 'TxRef', which would sort the
-- hashes in decreasing order. It is the order of the database keys, which
-- serialize the hash after the block position, and of the Redis sorted sets
-- of the cache, which break ties between equal scores by the bytes of the
-- member. Using it everywhere makes every source agree, so that merged pages
-- have no duplicates and the cache answers exactly like the database.
txRefKey :: TxRef -> (BlockRef, Down TxHash)
txRefKey t = (t.block, Down t.txid)

-- | Order of unspent outputs, newest first when sorted in decreasing order:
-- by block position, then by the serialized output point in increasing
-- order. See 'txRefKey' for why the ties are broken by serialized bytes.
unspentKey :: Unspent -> (BlockRef, Down ByteString)
unspentKey u = (u.block, Down (encode u.outpoint))

-- | Merge streams whose keys never increase, taking at most the given number
-- of items from each stream ('Nothing': all of them). The items come out in
-- groups of equal key, in decreasing order of key, and each group is sorted
-- in decreasing order of the second function, which must agree with the
-- first. So the output is the same as
-- sorting the first items of every stream in decreasing order, but a stream
-- is read only as far as the consumer needs: a consumer that stops after
-- the first n items makes each stream be read only a little past its first
-- n items, instead of to the end of its quota.
mergeDescGroups ::
  (Monad m, Ord o, Ord k) =>
  (a -> k) ->
  (a -> o) ->
  Maybe Word32 ->
  [ConduitT () a m ()] ->
  ConduitT () a m ()
mergeDescGroups key order quota streams = do
  heads <- lift $ mapM (next quota . sealConduitT) streams
  go (insertAll (catMaybes heads) Map.empty)
  where
    insertAll hs mp =
      foldr (\h@(x, _, _) -> Map.insertWith (<>) (Down (key x)) [h]) mp hs
    -- the next item of a stream, if any is left within its quota
    next (Just 0) _ = return Nothing
    next q s = do
      (s', mx) <- s $$++ await
      return $ (\x -> (x, subtract 1 <$> q, s')) <$> mx
    -- all the items of a stream with this key, and the first one after them
    group k (x, q, s) = do
      n <- next q s
      case n of
        Just h@(y, _, _)
          | key y == k -> do
              (ys, rest) <- group k h
              return (x : ys, rest)
        _ -> return ([x], n)
    go mp = case Map.minViewWithKey mp of
      Nothing -> return ()
      Just ((Down k, hs), mp') -> do
        gs <- lift $ mapM (group k) hs
        mapM_ yield (sortOn (Down . order) (concatMap fst gs))
        go (insertAll (mapMaybe snd gs) mp')
