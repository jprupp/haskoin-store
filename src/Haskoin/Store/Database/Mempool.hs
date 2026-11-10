-- |
-- Module      : Haskoin.Store.Database.Mempool
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The mempool area of the layered layout of the database, and how it is
-- applied on top of the chain. The design, and the reasons for it, are in
-- @docs\/layered-database.md@. This is the layout that the store runs on,
-- version 21. Its rules are checked by the tests of
-- "Haskoin.Store.Database.Migrate" against the answers of a database of
-- version 20, in which the mempool was mixed into the chain. The queries
-- that apply them are in "Haskoin.Store.Database.Query.Layered".
--
-- = Two layers
--
-- In the layered layout the column families of the chain hold only what
-- follows from the blocks of the main chain, and change only when a block is
-- imported or reverted. Everything about unconfirmed transactions is in two
-- column families of its own, 'mempoolCF' and 'mempoolAddrCF'. A query reads
-- the chain, reads what the mempool has for the same transaction, output or
-- address, and puts the two together. Emptying the mempool area leaves the
-- chain as it is, with an empty mempool on top.
--
-- = Every key belongs to one transaction
--
-- 'mempoolRecords' gives the keys that a mempool transaction contributes.
-- They depend on that transaction alone: on its inputs, with the outputs
-- they spend, which it carries with it, and on its outputs. The mempool area
-- is the union of the keys of its transactions. So a transaction enters the
-- mempool by writing its keys and leaves it by deleting them, in any order,
-- and when a block confirms some transactions and not their descendants,
-- nothing has to be worked out again for those that stay.
--
-- The alternative is to keep, for the mempool, the same values that the
-- chain has: a balance, a list of unspent outputs. Such values depend on the
-- chain under them. A balance with the mempool applied is wrong as soon as a
-- block pays the address, and every block would have to find and correct
-- the values it made stale. The keys here say what the mempool does, not
-- what the result is, so they stay right whatever happens to the chain.
--
-- What a query needs is worked out when it is answered, from the few entries
-- that the mempool has for an address, an t'AddressMempool':
--
-- * an output that the mempool creates and does not spend is an unspent
--   output of the mempool ('mempoolUnspents');
--
-- * an output that the mempool spends and did not create belongs to the
--   chain, which still has it as unspent, and is left out of what the chain
--   says ('hiddenOutputs');
--
-- * an output that the mempool both creates and spends shows up in neither,
--   and only counts towards what the address received.
--
-- = The summary of an address
--
-- The entries of an address are few for nearly every address, and many
-- thousands for some. So each address that the mempool touches also has a
-- t'MemSummary', one key with what its entries add up to ('summarise'). A
-- balance is the balance of the chain and the summary ('applySummary'), and
-- the queries read the entries themselves only for lists, as far as a page
-- goes.
--
-- The summary is not the key of one transaction, but it is a function of
-- the entries of its address and of nothing else, so it shares what makes
-- them safe: nothing that happens to the chain makes it wrong.
-- 'wrongSummaries' checks that, and 'rebuildSummaries' writes them all
-- again from the entries.
module Haskoin.Store.Database.Mempool
  ( -- * What a transaction contributes
    MemRecords (..),
    mempoolRecords,
    recordEntries,
    insertRecordOps,
    deleteRecordOps,

    -- * Reading the mempool area
    AddressMempool (..),
    getAddressMempool,
    getAddressMempools,
    getMempoolTx,
    getMempoolSpenders,
    getDeletedTx,
    wipeMempoolArea,

    -- * Applying the mempool on top of the chain
    mempoolUnspents,
    hiddenOutputs,
    applyMempool,
    mergeBalance,

    -- * The summary of an address
    summarise,
    summaryEntries,
    applySummary,
    summaryBalance,
    getSummaries,
    rebuildSummaries,
    wrongSummaries,
    addressArea,
  )
where

import Conduit (runConduit, sinkList, (.|))
import Data.ByteString (ByteString)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet qualified as HashSet
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.Maybe (fromMaybe, isNothing, mapMaybe)
import Data.Serialize (Serialize, decode, encode)
import Data.Word (Word32, Word64)
import Database.RocksDB
  ( BatchOp (..),
    ColumnFamily,
    DB,
    Iterator,
    iterFirst,
    iterKey,
    iterNext,
    multiGetCF,
    withIterCF,
  )
import Database.RocksDB.Query
  ( matching,
    matchingAsListCF,
    retrieveCF,
    writeBatch,
  )
import Haskoin
  ( Address,
    Ctx,
    Network,
    OutPoint (..),
    Tx (..),
    TxHash,
    TxIn (..),
    TxOut (..),
    eitherToMaybe,
    nullOutPoint,
    scriptToAddressBS,
    txHash,
  )
import Haskoin.Store.Common (nub')
import Haskoin.Store.Data
  ( Balance (..),
    BlockRef (..),
    Prev (..),
    Spender (..),
    TxData (..),
    TxRef (..),
    UnixTime,
    Unspent (..),
    zeroBalance,
  )
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    mempoolAddrCF,
    mempoolCF,
  )
import Haskoin.Store.Database.Types
  ( MemAddrKey (..),
    MemAddrVal (..),
    MemDeletedKey (..),
    MemSpenderKey (..),
    MemSummary (..),
    MemTimeKey (..),
    MemTxKey (..),
    TxKey (..),
  )

-------------------------------------------------------------------------------
-- What a transaction contributes
-------------------------------------------------------------------------------

-- | What one mempool transaction contributes to the mempool area.
data MemRecords = MemRecords
  { -- | hash of the transaction
    txid :: !TxHash,
    -- | time it entered the mempool: its place in the list of the mempool
    -- and in the index by time
    time :: !UnixTime,
    -- | its body, without spenders and at a mempool position
    body :: !TxData,
    -- | the outputs it spends, each with the input that spends it
    spends :: ![(OutPoint, Spender)],
    -- | for each address it touches: the transaction itself, the outputs of
    -- the address that it spends and the outputs that it pays to the
    -- address
    addresses :: ![(MemAddrKey, MemAddrVal)]
  }
  deriving (Eq, Show)

-- | What a mempool transaction that entered the mempool at a time
-- contributes to the mempool area.
--
-- It is a function of the transaction and that time and of nothing else:
-- not of the chain, and not of the other transactions in the mempool. The
-- body is stored without spenders, which have keys of their own, and as not
-- deleted. The flag for replace-by-fee is kept as given; it is the one
-- thing here that can depend on other transactions, because a transaction
-- inherits it from unconfirmed ancestors, and it is corrected when one of
-- them is confirmed.
mempoolRecords :: Network -> Ctx -> UnixTime -> TxData -> MemRecords
mempoolRecords net ctx time td =
  MemRecords
    { txid = h,
      time,
      body = (td :: TxData) {block = MemRef time, deleted = False, spenders = IntMap.empty},
      spends = [(op, Spender h k) | (k, op) <- spent],
      addresses =
        [(MemAddrTxKey a time h, MemAddrTxVal) | a <- touched]
          <> [ (MemAddrSpentKey a op, MemAddrSpentVal p.value)
             | (k, op) <- spent,
               Just p <- [IntMap.lookup (fromIntegral k) td.prevs],
               Just a <- [address p.script]
             ]
          <> [ (MemAddrOutKey a time (OutPoint h i), MemAddrOutVal o.value o.script)
             | (i, o) <- zip [0 ..] td.tx.outputs,
               Just a <- [address o.script]
             ]
    }
  where
    h = txHash td.tx
    spent = [(k, i.outpoint) | (k, i) <- zip [0 :: Word32 ..] td.tx.inputs, i.outpoint /= nullOutPoint]
    address = eitherToMaybe . scriptToAddressBS net ctx
    touched =
      nub' $
        mapMaybe (address . (.script)) (IntMap.elems td.prevs)
          <> mapMaybe (address . (.script)) td.tx.outputs

-- | The keys and values of a transaction, serialized, for each of the two
-- column families of the mempool area: the mempool column family first,
-- then the one of address entries.
recordEntries :: MemRecords -> ([(ByteString, ByteString)], [(ByteString, ByteString)])
recordEntries r =
  ( [ kv (MemTxKey r.txid) r.time,
      kv (MemTimeKey r.time r.txid) (),
      kv (TxKey r.txid) r.body
    ]
      <> [kv (MemSpenderKey op) s | (op, s) <- r.spends],
    [kv k v | (k, v) <- r.addresses]
  )
  where
    kv :: (Serialize k, Serialize v) => k -> v -> (ByteString, ByteString)
    kv k v = (encode k, encode v)

-- | Operations that write the keys of a transaction to the mempool area.
insertRecordOps :: DB -> MemRecords -> [BatchOp]
insertRecordOps db r =
  [PutCF (mempoolCF db) k v | (k, v) <- general]
    <> [PutCF (mempoolAddrCF db) k v | (k, v) <- byAddress]
  where
    (general, byAddress) = recordEntries r

-- | Operations that delete the keys of a transaction from the mempool area.
deleteRecordOps :: DB -> MemRecords -> [BatchOp]
deleteRecordOps db r =
  [DelCF (mempoolCF db) k | (k, _) <- general]
    <> [DelCF (mempoolAddrCF db) k | (k, _) <- byAddress]
  where
    (general, byAddress) = recordEntries r

-------------------------------------------------------------------------------
-- Reading the mempool area
-------------------------------------------------------------------------------

-- | What the mempool does at an address. It is small, usually empty, and
-- read with one scan of the entries of the address.
data AddressMempool = AddressMempool
  { -- | mempool transactions that spend from or pay to the address, newest
    -- first, in the order of 'Haskoin.Store.Common.txRefKey'
    txs :: ![TxRef],
    -- | outputs of the address that mempool transactions spend, with their
    -- amounts, whether a block or the mempool created them
    spent :: !(HashMap OutPoint Word64),
    -- | outputs that mempool transactions pay to the address, whether the
    -- mempool spends them or not, newest first, in the order of
    -- 'Haskoin.Store.Common.unspentKey'
    created :: ![Unspent]
  }
  deriving (Eq, Show)

-- | What the mempool does at an address.
getAddressMempool :: DatabaseReader -> Address -> IO AddressMempool
getAddressMempool r a = withIterCF r.db (mempoolAddrCF r.db) (`addressMempool` a)

-- | What the mempool does at each of some addresses, in their order.
--
-- All of them are read through one iterator, which is moved to each address
-- in turn. A query about an extended public key asks about hundreds of
-- addresses, nearly all with nothing in the mempool; opening an iterator for
-- each would cost more than the reads, each of which stops at the filter.
getAddressMempools :: DatabaseReader -> [Address] -> IO [AddressMempool]
getAddressMempools _ [] = return []
getAddressMempools r as = withIterCF r.db (mempoolAddrCF r.db) $ \it -> mapM (addressMempool it) as

-- | What the mempool does at an address, read with an iterator on the
-- column family of the entries by address, which is left wherever the scan
-- ended.
addressMempool :: Iterator -> Address -> IO AddressMempool
addressMempool it a = do
  rows <- runConduit $ matching it (MemAddrKeyA a) .| sinkList
  return
    AddressMempool
      { txs = [TxRef {block = MemRef t, txid = h} | (MemAddrTxKey _ t h, _) <- rows],
        spent = HashMap.fromList [(p, v) | (MemAddrSpentKey _ p, MemAddrSpentVal v) <- rows],
        created =
          [ Unspent {block = MemRef t, outpoint = p, value = v, script = s, address = Just a}
          | (MemAddrOutKey _ t p, MemAddrOutVal v s) <- rows
          ]
      }

-- | Body of a transaction that is in the mempool, without its spenders.
getMempoolTx :: DatabaseReader -> TxHash -> IO (Maybe TxData)
getMempoolTx r h = retrieveCF r.db (mempoolCF r.db) (TxKey h)

-- | Mempool inputs that spend the outputs of a transaction, by output index.
-- The transaction may be confirmed or in the mempool.
getMempoolSpenders :: DatabaseReader -> TxHash -> IO (IntMap Spender)
getMempoolSpenders r h = do
  rows <- matchingAsListCF r.db (mempoolCF r.db) (MemSpenderKeyS h)
  return $ IntMap.fromList [(fromIntegral p.index, s) | (MemSpenderKey p, s) <- rows]

-- | Body of a transaction that left the mempool without being confirmed.
getDeletedTx :: DatabaseReader -> TxHash -> IO (Maybe TxData)
getDeletedTx r h = retrieveCF r.db (mempoolCF r.db) (MemDeletedKey h)

-- | Delete every key of the mempool area, and say how many there were. The
-- chain is not touched: afterwards the database is that of a store whose
-- mempool is empty, as if every unconfirmed transaction had been deleted one
-- by one, except that it does not remember having seen them.
--
-- This is all that wiping the mempool takes in the layered layout, and what
-- can be done if the mempool area is ever found to be wrong.
wipeMempoolArea :: DatabaseReader -> IO Int
wipeMempoolArea r = sum <$> mapM wipe [mempoolCF r.db, mempoolAddrCF r.db]
  where
    wipe cf = go 0
      where
        go n =
          firstKeys cf >>= \case
            [] -> return n
            ks -> writeBatch r.db (map (DelCF cf) ks) >> go (n + length ks)
    firstKeys :: ColumnFamily -> IO [ByteString]
    firstKeys cf = withIterCF r.db cf $ \it -> iterFirst it >> collect it (10000 :: Int)
    collect _ 0 = return []
    collect it n =
      iterKey it >>= \case
        Nothing -> return []
        Just k -> iterNext it >> (k :) <$> collect it (n - 1)

-------------------------------------------------------------------------------
-- Applying the mempool on top of the chain
-------------------------------------------------------------------------------

-- | Outputs of an address that the mempool created and has not spent,
-- newest first.
mempoolUnspents :: AddressMempool -> [Unspent]
mempoolUnspents m = [u | u <- m.created, not (HashMap.member u.outpoint m.spent)]

-- | Outputs of an address that the chain has as unspent and that the mempool
-- spends, with their amounts: those that the mempool spends and did not
-- create itself.
hiddenOutputs :: AddressMempool -> HashMap OutPoint Word64
hiddenOutputs m = HashMap.filterWithKey (\p _ -> not (HashSet.member p made)) m.spent
  where
    made = HashSet.fromList (map (.outpoint) m.created)

-- | The balance that the chain has for an address, with what the mempool
-- does at the address applied: the balance as the API gives it.
--
-- As in version 20, an output of a block that a mempool transaction spends no
-- longer counts as confirmed balance, and the unconfirmed balance is what
-- the mempool pays to the address and has not spent.
--
-- The amounts taken from the chain can never be more than it has, as long
-- as every output that the mempool spends exists. If they ever are, the
-- result stops at zero rather than wrapping around.
applyMempool :: Balance -> AddressMempool -> Balance
applyMempool c m = applySummary c (summarise m)

-- | Balance of an address as the API gives it, from its balance in the
-- chain, if it has one, and what the mempool does at it ('applyMempool').
-- 'Nothing' if the address was never used in either.
--
-- Version 20 answers with a balance of zero, rather than with 'Nothing',
-- for an address whose only transactions were deleted from the mempool: it
-- leaves the balance that it wrote for them behind. The API shows an unknown
-- address as a balance of zero, so the two are the same to it.
mergeBalance :: Address -> Maybe Balance -> AddressMempool -> Maybe Balance
mergeBalance a chain m
  | isNothing chain && null m.txs && HashMap.null m.spent && null m.created = Nothing
  | otherwise = Just (applyMempool (fromMaybe (zeroBalance a) chain) m)

-------------------------------------------------------------------------------
-- The summary of an address
-------------------------------------------------------------------------------

-- | What the entries of an address add up to.
summarise :: AddressMempool -> MemSummary
summarise m =
  MemSummary
    { txs = fromIntegral (length m.txs),
      paid = sum (map (.value) m.created),
      paidCount = fromIntegral (length m.created),
      spent = sum (HashMap.elems m.spent),
      spentCount = fromIntegral (HashMap.size m.spent),
      both = sum (map (.value) inside),
      bothCount = fromIntegral (length inside)
    }
  where
    inside = [u | u <- m.created, HashMap.member u.outpoint m.spent]

-- | The summaries that go with some entries by address, which may be of
-- many addresses: what the column family must have next to them, for the
-- migration, for repairs and for the tests.
summaryEntries :: [(MemAddrKey, MemAddrVal)] -> [(MemAddrKey, MemAddrVal)]
summaryEntries rows =
  [ (MemAddrSumKey a, MemAddrSumVal (summarise m))
  | (a, m) <- HashMap.toList (HashMap.fromListWith (flip merge) (mapMaybe single rows))
  ]
  where
    entries ts sp cr = AddressMempool {txs = ts, spent = sp, created = cr}
    merge x y = entries (x.txs <> y.txs) (x.spent <> y.spent) (x.created <> y.created)
    single = \case
      (MemAddrTxKey a t h, _) -> Just (a, entries [TxRef {block = MemRef t, txid = h}] HashMap.empty [])
      (MemAddrSpentKey a p, MemAddrSpentVal v) -> Just (a, entries [] (HashMap.singleton p v) [])
      (MemAddrOutKey a t p, MemAddrOutVal v s) ->
        let u = Unspent {block = MemRef t, outpoint = p, value = v, script = s, address = Just a}
         in Just (a, entries [] HashMap.empty [u])
      _ -> Nothing

-- | The balance that the chain has for an address with the summary of what
-- the mempool does at it applied: what 'applyMempool' gives, from the
-- summary instead of the entries.
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
-- chain and its summary in the mempool, either of which it may not have:
-- what 'mergeBalance' gives, without reading the entries.
summaryBalance :: Address -> Maybe Balance -> Maybe MemSummary -> Maybe Balance
summaryBalance _ chain Nothing = chain
summaryBalance a chain (Just s) = Just (applySummary (fromMaybe (zeroBalance a) chain) s)

-- | The summaries of addresses, in their order, read by key with one
-- multi-get: 'Nothing' for an address that the mempool does not touch,
-- which is nearly all of them, and which the filter of the column family
-- answers.
getSummaries :: DatabaseReader -> [Address] -> IO [Maybe MemSummary]
getSummaries _ [] = return []
getSummaries r as = map (>>= decoded) <$> multiGetCF r.db (mempoolAddrCF r.db) (map (encode . MemAddrSumKey) as)
  where
    decoded bytes = case decode bytes of
      Right (MemAddrSumVal s) -> Just s
      _ -> Nothing

-- | The addresses whose summary is not what their entries add up to, each
-- with the summary that it should have and the one that it has. There are
-- none unless something is broken: this is what the tests hold the rules
-- to after every operation.
wrongSummaries :: DatabaseReader -> IO [(MemAddrKey, Maybe MemAddrVal, Maybe MemAddrVal)]
wrongSummaries r = do
  rows <- matchingAsListCF r.db (mempoolAddrCF r.db) MemAddrKeyS
  let found = HashMap.fromList [(k, v) | (k@MemAddrSumKey {}, v) <- rows]
      expected = HashMap.fromList (summaryEntries rows)
  return
    [ (k, HashMap.lookup k expected, HashMap.lookup k found)
    | k <- HashMap.keys (HashMap.union expected found),
      HashMap.lookup k expected /= HashMap.lookup k found
    ]

-- | Everything that the column family of entries by address holds for a
-- mempool of some transactions, serialized, in no order: the entries of
-- each, and the summaries of the addresses that they touch.
addressArea :: [MemRecords] -> [(ByteString, ByteString)]
addressArea records = [(encode k, encode v) | (k, v) <- entries <> summaryEntries entries]
  where
    entries = concatMap (.addresses) records

-- | Work every summary out again from the entries, and say how many
-- addresses have one. The migration does this once everything is moved, and
-- it is what puts the summaries right if they are ever in doubt.
rebuildSummaries :: DatabaseReader -> IO Int
rebuildSummaries r = do
  rows <- matchingAsListCF r.db cf MemAddrKeyS
  let old = [DelCF cf (encode k) | (k@MemAddrSumKey {}, _ :: MemAddrVal) <- rows]
      new = summaryEntries rows
  writeBatch r.db (old <> [PutCF cf (encode k) (encode v) | (k, v) <- new])
  return (length new)
  where
    cf = mempoolAddrCF r.db
