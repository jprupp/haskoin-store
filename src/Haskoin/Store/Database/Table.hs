-- |
-- Module      : Haskoin.Store.Database.Table
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The hash tables of a batch of writes ("Haskoin.Store.Database.Writer"):
-- mutable tables whose keys are hashed quickly.
--
-- = Why not the 'Hashable' instances of the keys
--
-- A batch keeps its changes in hash tables keyed by outputs, addresses and
-- pairs of them, and importing a block makes some forty insertions and
-- lookups for each transaction. The 'Hashable' instances of those types are
-- derived generically, which walks the structure of the type for each
-- hash: 0.4 microseconds for an output or an address, 0.8 for an address
-- with a transaction, where hashing the 32 bytes of a transaction hash
-- takes 0.03. An insertion under an address and a transaction took 3.4
-- microseconds, and the tables were most of the time of a block once its
-- reads were out of the way.
--
-- Every one of those keys holds a hash already: of a transaction, a public
-- key or a script. Those bytes are as good as random, so 'tableHash' mixes
-- them and nothing else: the same output of two transactions, or two
-- outputs of the same address, differ in them. The insertion above takes
-- 0.6 microseconds with it.
--
-- The alternatives were faster instances in haskoin-core, which is where
-- they belong and would serve everyone, but is another package with its
-- own releases; and tables keyed by the serialised keys, which hash fast
-- too, but serialising a key costs more than this hash, and has to be done
-- for lookups as well.
module Haskoin.Store.Database.Table
  ( Table,
    TableKey (..),
    new,
    lookup,
    insert,
    delete,
    toList,
  )
where

import Data.Coerce (coerce)
import Data.HashTable.IO qualified as H
import Data.Hashable (Hashable (..))
import Data.Word (Word32)
import Haskoin (Address (..), BlockHash, OutPoint (..), TxHash)
import Haskoin.Store.Data (BlockRef, TxRef (..))
import Haskoin.Store.Database.Types (MemAddrKey (..))
import Prelude hiding (lookup)

-- | A mutable hash table, for one thread at a time.
newtype Table k v = Table (H.BasicHashTable (Key k) v)

-- | A key as the table holds it, hashed with 'tableHash'.
newtype Key k = Key k
  deriving (Eq)

instance (TableKey k) => Hashable (Key k) where
  hashWithSalt salt (Key k) = tableHash salt k

-- | What can be the key of a t'Table'.
class (Eq k) => TableKey k where
  -- | Mix a key into a salt. Two keys that are equal must give the same
  -- result; two that are not should differ in what is mixed, though they
  -- need not: the table compares the keys themselves in the end.
  tableHash :: Int -> k -> Int

instance TableKey TxHash where
  tableHash = hashWithSalt

instance TableKey BlockHash where
  tableHash = hashWithSalt

instance TableKey Word32 where
  tableHash = hashWithSalt

-- | The hash of the transaction and the index of the output.
instance TableKey OutPoint where
  tableHash salt p = salt `hashWithSalt` p.hash `hashWithSalt` p.index

-- | The hash that the address holds, for the usual kinds of address, each
-- with a salt of its own, so that a script and a key with the same hash
-- stay apart. The others are hashed as their type says.
instance TableKey Address where
  tableHash salt = \case
    PubKeyAddress h -> hashWithSalt salt h
    ScriptAddress h -> hashWithSalt (salt + 1) h
    WitnessPubKeyAddress h -> hashWithSalt (salt + 2) h
    WitnessScriptAddress h -> hashWithSalt (salt + 3) h
    a -> hashWithSalt (salt + 4) a

-- | A group of outputs of a transaction: its hash and the number of the
-- group.
instance TableKey (TxHash, Word32) where
  tableHash salt (h, g) = salt `hashWithSalt` h `hashWithSalt` g

-- | The address and the hash of the transaction. The position of the
-- transaction is left out: a transaction has one at a time.
instance TableKey (Address, TxRef) where
  tableHash salt (a, t) = tableHash salt a `hashWithSalt` t.txid

-- | The address and the output, without the position of its transaction.
instance TableKey (Address, BlockRef, OutPoint) where
  tableHash salt (a, _, p) = tableHash (tableHash salt a) p

-- | The address, and the transaction or the output of the entry, each
-- kind of entry with a salt of its own. The short keys, which only scans
-- use, are hashed as their type says.
instance TableKey MemAddrKey where
  tableHash salt = \case
    MemAddrTxKey a _ h -> tableHash salt a `hashWithSalt` h
    MemAddrSpentKey a p -> tableHash (tableHash (salt + 1) a) p
    MemAddrOutKey a _ p -> tableHash (tableHash (salt + 2) a) p
    MemAddrSumKey a -> tableHash (salt + 3) a
    k -> hashWithSalt (salt + 4) k

-- | An empty table.
new :: IO (Table k v)
new = Table <$> H.new

-- | The value of a key, if the table has it.
lookup :: (TableKey k) => Table k v -> k -> IO (Maybe v)
lookup (Table t) k = H.lookup t (Key k)

-- | Set the value of a key.
insert :: (TableKey k) => Table k v -> k -> v -> IO ()
insert (Table t) k = H.insert t (Key k)

-- | Take a key out of the table, if it is there.
delete :: (TableKey k) => Table k v -> k -> IO ()
delete (Table t) k = H.delete t (Key k)

-- | Every key with its value, in no particular order.
toList :: forall k v. (TableKey k) => Table k v -> IO [(k, v)]
toList (Table t) = coerce <$> (H.toList t :: IO [(Key k, v)])
