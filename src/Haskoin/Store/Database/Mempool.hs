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
-- @docs\/layered-database.md@. The store does not use this layout yet: these
-- are its rules, checked by the tests of "Haskoin.Store.Database.Migrate"
-- against the answers of the database as it is today.
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
module Haskoin.Store.Database.Mempool
  ( -- * What a transaction contributes
    MemRecords (..),
    mempoolRecords,
    insertRecordOps,
    deleteRecordOps,

    -- * Reading the mempool area
    AddressMempool (..),
    getAddressMempool,
    getMempoolTx,
    getMempoolSpenders,
    getDeletedTx,
    wipeMempoolArea,

    -- * Applying the mempool on top of the chain
    mempoolUnspents,
    hiddenOutputs,
    mergeBalance,
    mergeUnspents,
    mergeTxRefs,

    -- * Queries over both layers
    getTxData,
    getSpender,
    getUnspent,
    getBalance,
    getAddressTxs,
    getAddressUnspents,
  )
where

import Control.Applicative ((<|>))
import Data.ByteString (ByteString)
import Data.Default (def)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet qualified as HashSet
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.Maybe (fromMaybe, isNothing, mapMaybe)
import Data.Serialize (Serialize, encode)
import Data.Word (Word32, Word64)
import Database.RocksDB
  ( BatchOp (..),
    ColumnFamily,
    DB,
    iterFirst,
    iterKey,
    iterNext,
    withIterCF,
  )
import Database.RocksDB.Query
  ( matchingAsListCF,
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
import Haskoin.Store.Database.Query qualified as Q
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
    MemTimeKey (..),
    MemTxKey (..),
    TxKey (..),
  )

-------------------------------------------------------------------------------
-- What a transaction contributes
-------------------------------------------------------------------------------

-- | The keys and values that one mempool transaction contributes to the
-- mempool area, serialized, for each of its two column families.
data MemRecords = MemRecords
  { -- | in the mempool column family: its place in the list of the mempool
    -- and in the index by time, its body, and the outputs it spends
    general :: ![(ByteString, ByteString)],
    -- | in the column family of mempool address records: for each address
    -- it touches, the transaction itself, the outputs of the address that
    -- it spends and the outputs that it pays to the address
    byAddress :: ![(ByteString, ByteString)]
  }
  deriving (Eq, Show)

-- | The keys of a mempool transaction that entered the mempool at a time.
--
-- They are a function of the transaction and that time and of nothing
-- else: not of the chain, and not of the other transactions in the mempool.
-- The body is stored without spenders, which have keys of their own, and as
-- not deleted. The flag for replace-by-fee is kept as given; it is the one
-- thing here that can depend on other transactions, because a transaction
-- inherits it from unconfirmed ancestors, and it is corrected when one of
-- them is confirmed.
mempoolRecords :: Network -> Ctx -> UnixTime -> TxData -> MemRecords
mempoolRecords net ctx time td =
  MemRecords
    { general =
        [ kv (MemTxKey h) time,
          kv (MemTimeKey time h) (),
          kv (TxKey h) body
        ]
          <> [kv (MemSpenderKey op) (Spender h k) | (k, op) <- spends],
      byAddress =
        [kv (MemAddrTxKey a time h) MemAddrTxVal | a <- touched]
          <> [ kv (MemAddrSpentKey a op) (MemAddrSpentVal p.value)
             | (k, op) <- spends,
               Just p <- [IntMap.lookup (fromIntegral k) td.prevs],
               Just a <- [address p.script]
             ]
          <> [ kv (MemAddrOutKey a time (OutPoint h i)) (MemAddrOutVal o.value o.script)
             | (i, o) <- zip [0 ..] td.tx.outputs,
               Just a <- [address o.script]
             ]
    }
  where
    h = txHash td.tx
    body = (td :: TxData) {block = MemRef time, deleted = False, spenders = IntMap.empty}
    spends = [(k, i.outpoint) | (k, i) <- zip [0 :: Word32 ..] td.tx.inputs, i.outpoint /= nullOutPoint]
    address = eitherToMaybe . scriptToAddressBS net ctx
    touched =
      nub' $
        mapMaybe (address . (.script)) (IntMap.elems td.prevs)
          <> mapMaybe (address . (.script)) td.tx.outputs
    kv :: (Serialize k, Serialize v) => k -> v -> (ByteString, ByteString)
    kv k v = (encode k, encode v)

-- | Operations that write the keys of a transaction to the mempool area.
insertRecordOps :: DB -> MemRecords -> [BatchOp]
insertRecordOps db r =
  [PutCF (mempoolCF db) k v | (k, v) <- r.general]
    <> [PutCF (mempoolAddrCF db) k v | (k, v) <- r.byAddress]

-- | Operations that delete the keys of a transaction from the mempool area.
deleteRecordOps :: DB -> MemRecords -> [BatchOp]
deleteRecordOps db r =
  [DelCF (mempoolCF db) k | (k, _) <- r.general]
    <> [DelCF (mempoolAddrCF db) k | (k, _) <- r.byAddress]

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
getAddressMempool r a = do
  rows <- matchingAsListCF r.db (mempoolAddrCF r.db) (MemAddrKeyA a)
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

-- | Balance of an address as the API gives it, from its balance in the
-- chain, if it has one, and what the mempool does at it. 'Nothing' if the
-- address was never used in either.
--
-- As today, an output of a block that a mempool transaction spends no
-- longer counts as confirmed balance, and the unconfirmed balance is what
-- the mempool pays to the address and has not spent.
--
-- The amounts taken from the chain can never be more than it has, as long
-- as every output that the mempool spends exists. If they ever are, the
-- result stops at zero rather than wrapping around.
mergeBalance :: Address -> Maybe Balance -> AddressMempool -> Maybe Balance
mergeBalance a chain m
  | isNothing chain && null m.txs && HashMap.null m.spent && null m.created = Nothing
  | otherwise =
      Just
        Balance
          { address = a,
            confirmed = c.confirmed `minus` sum (HashMap.elems hidden),
            unconfirmed = c.unconfirmed + sum (map (.value) fresh),
            utxo = (c.utxo `minus` fromIntegral (HashMap.size hidden)) + fromIntegral (length fresh),
            txs = c.txs + fromIntegral (length m.txs),
            received = c.received + sum (map (.value) m.created)
          }
  where
    c = fromMaybe (zeroBalance a) chain
    hidden = hiddenOutputs m
    fresh = mempoolUnspents m
    minus x y = if y > x then 0 else x - y

-- | Unspent outputs of an address as the API gives them, newest first, from
-- those that the chain has, newest first, and what the mempool does at the
-- address: the unspent outputs of the mempool, then those of the chain that
-- the mempool does not spend.
mergeUnspents :: AddressMempool -> [Unspent] -> [Unspent]
mergeUnspents m chain =
  mempoolUnspents m <> [u | u <- chain, not (HashMap.member u.outpoint m.spent)]

-- | Transactions of an address as the API gives them, newest first: those of
-- the mempool, then those of the chain.
mergeTxRefs :: AddressMempool -> [TxRef] -> [TxRef]
mergeTxRefs m chain = m.txs <> chain

-------------------------------------------------------------------------------
-- Queries over both layers
-------------------------------------------------------------------------------

-- | Data of a transaction, with the spenders of its outputs, those in blocks
-- and those in the mempool. A transaction in the mempool comes from the
-- mempool area, a confirmed one from the chain, and one that was deleted
-- from where it was kept: the mempool area, or the chain for those that
-- version 20 deleted.
getTxData :: DatabaseReader -> TxHash -> IO (Maybe TxData)
getTxData r h =
  getMempoolTx r h >>= \case
    Just td -> Just <$> spent td
    Nothing ->
      Q.getTxData r h >>= \case
        Just td | not td.deleted -> Just <$> spent td
        old -> (<|> old) <$> getDeletedTx r h
  where
    spent td = do
      ms <- getMempoolSpenders r h
      return (td :: TxData) {spenders = IntMap.union td.spenders ms}

-- | Input that spends an output, in the mempool or in a block.
getSpender :: DatabaseReader -> OutPoint -> IO (Maybe Spender)
getSpender r p =
  retrieveCF r.db (mempoolCF r.db) (MemSpenderKey p) >>= \case
    Just s -> return (Just s)
    Nothing -> Q.getSpender r p

-- | Unspent output, if it exists and neither a block nor the mempool spends
-- it: one that the chain has, or one of a mempool transaction.
getUnspent :: DatabaseReader -> OutPoint -> IO (Maybe Unspent)
getUnspent r p =
  retrieveCF r.db (mempoolCF r.db) (MemSpenderKey p) >>= \case
    Just (_ :: Spender) -> return Nothing
    Nothing ->
      Q.getUnspent r p >>= \case
        Just u -> return (Just u)
        Nothing -> (>>= output) <$> getMempoolTx r p.hash
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

-- | Balance of an address, with the mempool applied.
getBalance :: DatabaseReader -> Address -> IO (Maybe Balance)
getBalance r a = mergeBalance a <$> Q.getBalance r a <*> getAddressMempool r a

-- | All transactions of an address, newest first, with those of the mempool.
getAddressTxs :: DatabaseReader -> Address -> IO [TxRef]
getAddressTxs r a = mergeTxRefs <$> getAddressMempool r a <*> Q.getAddressTxs r a def

-- | All unspent outputs of an address, newest first, with the mempool
-- applied.
getAddressUnspents :: DatabaseReader -> Address -> IO [Unspent]
getAddressUnspents r a = mergeUnspents <$> getAddressMempool r a <*> Q.getAddressUnspents r a def
