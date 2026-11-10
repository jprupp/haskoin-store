{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE RecordWildCards #-}

-- |
-- Module      : Haskoin.Store.Database.Types
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Keys and values stored in RocksDB.
--
-- Every key type serializes to a distinct first byte, so that keys of different
-- types never collide even when they share a column family. Many key types
-- have short variants (suffixed @A@, @B@ or @S@) that serialize to a prefix of
-- the full key, for use with the prefix queries of "Database.RocksDB.Query".
--
-- +-----------+--------------+----------------------------------------+
-- | Prefix    | Key          | Value                                  |
-- +===========+==============+========================================+
-- | 32 x 0x00 | 'BestKey'    | hash of the best block                 |
-- +-----------+--------------+----------------------------------------+
-- | 0x01      | 'BlockKey'   | 'BlockData'                            |
-- +-----------+--------------+----------------------------------------+
-- | 0x02      | 'TxKey'      | 'TxData'                               |
-- +-----------+--------------+----------------------------------------+
-- | 0x03      | 'HeightKey'  | hashes of the blocks at a height       |
-- +-----------+--------------+----------------------------------------+
-- | 0x04      | 'BalKey'     | 'BalVal'                               |
-- +-----------+--------------+----------------------------------------+
-- | 0x05      | 'AddrTxKey'  | nothing                                |
-- +-----------+--------------+----------------------------------------+
-- | 0x06      | 'AddrOutKey' | 'OutVal'                               |
-- +-----------+--------------+----------------------------------------+
-- | 0x07      | 'MemKey'     | whole mempool (legacy)                 |
-- +-----------+--------------+----------------------------------------+
-- | 0x08      | 'MemTxKey'   | time a transaction entered the mempool |
-- +-----------+--------------+----------------------------------------+
-- | 0x09      | 'UnspentKey' | 'UnspentVal'                           |
-- +-----------+--------------+----------------------------------------+
-- | 0x0a      | 'VersionKey' | database version                       |
-- +-----------+--------------+----------------------------------------+
-- | 0x0b      | 'MemTimeKey' | nothing                                |
-- +-----------+--------------+----------------------------------------+
--
-- = Keys of the layered layout
--
-- The store does not write these yet. They belong to the layout of
-- @docs\/layered-database.md@, version 21, in which the column families of
-- the chain hold nothing from the mempool and everything about the mempool
-- is in two column families of its own. t'TxKey' is used there too, for the
-- bodies of mempool transactions. "Haskoin.Store.Database.Mempool" reads
-- and writes them, and "Haskoin.Store.Database.Migrate" moves a database of
-- version 20 to that layout.
--
-- +-----------+------------------+-------------------------------------------+
-- | Prefix    | Key              | Value                                     |
-- +===========+==================+===========================================+
-- | 0x0c      | t'MemSpenderKey' | 'Spender': mempool input that spends an   |
-- |           |                  | output                                    |
-- +-----------+------------------+-------------------------------------------+
-- | 0x0d      | 'MemAddrKey'     | 'MemAddrVal': what the mempool does at an |
-- |           |                  | address                                   |
-- +-----------+------------------+-------------------------------------------+
-- | 0x0e      | t'MemDeletedKey' | 'TxData' of a transaction that left the   |
-- |           |                  | mempool unconfirmed                       |
-- +-----------+------------------+-------------------------------------------+
-- | 0x0f      | t'MigrationKey'  | version a migration in progress started   |
-- |           |                  | from                                      |
-- +-----------+------------------+-------------------------------------------+
module Haskoin.Store.Database.Types
  ( AddrTxKey (..),
    AddrOutKey (..),
    BestKey (..),
    BlockKey (..),
    BalKey (..),
    HeightKey (..),
    MemKey (..),
    MemTxKey (..),
    MemTimeKey (..),
    MemSpenderKey (..),
    MemAddrKey (..),
    MemAddrVal (..),
    MemSummary (..),
    MemDeletedKey (..),
    MigrationKey (..),
    TxKey (..),
    decodeTxKey,
    UnspentKey (..),
    VersionKey (..),
    BalVal (..),
    valToBalance,
    balanceToVal,
    UnspentVal (..),
    toUnspent,
    unspentToVal,
    valToUnspent,
    OutVal (..),
  )
where

import Control.DeepSeq (NFData)
import Control.Monad (guard)
import Data.Bits
  ( shift,
    (.&.),
  )
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Default (Default (..))
import Data.Hashable (Hashable)
import Data.Serialize
  ( Serialize (..),
    getBytes,
    getWord16be,
    getWord32be,
    getWord64be,
    getWord8,
    putWord64be,
    putWord8,
    runGet,
    runPut,
  )
import Data.Word (Word16, Word32, Word64, Word8)
import Database.RocksDB.Query (Key, KeyValue)
import GHC.Generics (Generic)
import Haskoin
  ( Address,
    BlockHash,
    BlockHeight,
    Network,
    OutPoint (..),
    TxHash,
    eitherToMaybe,
    scriptToAddressBS,
  )
import Haskoin.Crypto
import Haskoin.Store.Data
  ( Balance (..),
    BlockData,
    BlockRef,
    Spender,
    TxData,
    TxRef (..),
    UnixTime,
    Unspent (..),
  )

-- | Database key for an address transaction.
data AddrTxKey
  = -- | key for a transaction affecting an address
    AddrTxKey !Address !TxRef
  | -- | short key that matches all entries
    AddrTxKeyA !Address
  | -- | short key that matches the entries of an address at a position in
    -- the chain
    AddrTxKeyB !Address !BlockRef
  | -- | short key that matches the entries of every address
    AddrTxKeyS
  deriving (Show, Eq, Ord, Generic, Hashable)

instance Serialize AddrTxKey where
  -- 0x05 · Address · BlockRef · TxHash

  put (AddrTxKey a TxRef {block = b, txid = t}) = do
    put (AddrTxKeyB a b)
    put t
  -- 0x05 · Address
  put (AddrTxKeyA a) = do
    put AddrTxKeyS
    put a
  -- 0x05 · Address · BlockRef
  put (AddrTxKeyB a b) = do
    put (AddrTxKeyA a)
    put b
  -- 0x05
  put AddrTxKeyS = putWord8 0x05
  get = do
    guard . (== 0x05) =<< getWord8
    a <- get
    b <- get
    t <- get
    return $ AddrTxKey a TxRef {block = b, txid = t}

instance Key AddrTxKey

instance KeyValue AddrTxKey ()

-- | Database key for an address output.
data AddrOutKey
  = -- | full key
    AddrOutKey !Address !BlockRef !OutPoint
  | -- | short key for all spent or unspent outputs
    AddrOutKeyA !Address
  | -- | short key for the outputs of an address at a position in the chain
    AddrOutKeyB !Address !BlockRef
  | -- | short key for the outputs of every address
    AddrOutKeyS
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize AddrOutKey where
  -- 0x06 · StoreAddr · BlockRef · OutPoint

  put (AddrOutKey a b p) = do
    put (AddrOutKeyB a b)
    put p
  -- 0x06 · StoreAddr · BlockRef
  put (AddrOutKeyB a b) = do
    put (AddrOutKeyA a)
    put b
  -- 0x06 · StoreAddr
  put (AddrOutKeyA a) = do
    put AddrOutKeyS
    put a
  -- 0x06
  put AddrOutKeyS = putWord8 0x06
  get = do
    guard . (== 0x06) =<< getWord8
    AddrOutKey <$> get <*> get <*> get

instance Key AddrOutKey

-- | Value of an unspent output of an address: amount in satoshi and output
-- script.
data OutVal = OutVal !Word64 !ByteString
  deriving (Show, Read, Eq, Ord, Generic, Hashable, Serialize)

instance KeyValue AddrOutKey OutVal

-- | Transaction database key.
data TxKey
  = -- | key of a transaction
    TxKey !TxHash
  | -- | short key that matches transactions whose hash starts with these six
    -- bytes; see 'decodeTxKey'
    TxKeyS !(Word32, Word16)
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize TxKey where
  -- 0x02 · TxHash
  put (TxKey h) = do
    putWord8 0x02
    put h
  put (TxKeyS h) = do
    putWord8 0x02
    put h
  get = do
    guard . (== 0x02) =<< getWord8
    TxKey <$> get

-- | Split a short numeric transaction identifier, as used by the Blockchain.info
-- compatibility API, into the first six bytes of the transaction hash and the
-- five high bits of the seventh byte. Only the low 53 bits of the identifier
-- are used.
decodeTxKey :: Word64 -> ((Word32, Word16), Word8)
decodeTxKey i =
  let masked = i .&. 0x001fffffffffffff
      wb = masked `shift` 11
      bs = runPut (putWord64be wb)
      g = do
        w1' <- getWord32be
        w2' <- getWord16be
        w3' <- getWord8
        return (w1', w2', w3')
      (w1, w2, w3) =
        case runGet g bs of
          Right x -> x
          Left e -> error e
   in ((w1, w2), w3)

instance Key TxKey

instance KeyValue TxKey TxData

-- | Unspent output database key.
data UnspentKey
  = -- | key of an unspent output
    UnspentKey !OutPoint
  | -- | short key that matches the unspent outputs of a transaction
    UnspentKeyS !TxHash
  | -- | short key that matches every unspent output
    UnspentKeyB
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize UnspentKey where
  -- 0x09 · TxHash · Index
  put (UnspentKey OutPoint {hash = h, index = i}) = do
    putWord8 0x09
    put h
    put i
  -- 0x09 · TxHash
  put (UnspentKeyS t) = do
    putWord8 0x09
    put t
  -- 0x09
  put UnspentKeyB = putWord8 0x09
  get = do
    guard . (== 0x09) =<< getWord8
    h <- get
    i <- get
    return $ UnspentKey OutPoint {hash = h, index = i}

instance Key UnspentKey

instance KeyValue UnspentKey UnspentVal

-- | Build an 'Unspent' from an address output entry. Returns 'Nothing' for a short key.
toUnspent :: Network -> Ctx -> AddrOutKey -> OutVal -> Maybe Unspent
toUnspent net ctx (AddrOutKey _address block outpoint) (OutVal value script) =
  Just
    Unspent
      { block = block,
        value = value,
        script = script,
        outpoint = outpoint,
        address = eitherToMaybe (scriptToAddressBS net ctx script)
      }
toUnspent _ _ _ _ = Nothing

-- | Mempool transaction database key. Legacy: databases of version 18 kept the
-- whole mempool under this key. It is only read to migrate them.
data MemKey
  = MemKey
  deriving (Show, Read)

instance Serialize MemKey where
  -- 0x07
  put MemKey = putWord8 0x07
  get = do
    guard . (== 0x07) =<< getWord8
    return MemKey

instance Key MemKey

instance KeyValue MemKey [(UnixTime, TxHash)]

-- | Key of one mempool transaction in the mempool column family, with the
-- time it entered the mempool as its value. Replaces 'MemKey', which kept
-- the whole mempool in a single value.
data MemTxKey
  = -- | key of a mempool transaction
    MemTxKey !TxHash
  | -- | short key that matches every mempool transaction
    MemTxKeyS
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize MemTxKey where
  -- 0x08 · TxHash
  put (MemTxKey h) = do
    put MemTxKeyS
    put h
  -- 0x08
  put MemTxKeyS = putWord8 0x08
  get = do
    guard . (== 0x08) =<< getWord8
    MemTxKey <$> get

instance Key MemTxKey

instance KeyValue MemTxKey UnixTime

-- | Index of the mempool by the time each transaction entered it, in the
-- mempool column family next to 'MemTxKey'. Keys sort by time, then by
-- hash, as the pairs in @getMempool@ do, so a page of the mempool is read
-- with a short scan instead of loading and sorting every entry.
data MemTimeKey
  = -- | entry of a mempool transaction, by time and hash
    MemTimeKey !UnixTime !TxHash
  | -- | short key that matches every entry
    MemTimeKeyS
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize MemTimeKey where
  -- 0x0b · UnixTime (big endian) · TxHash
  put (MemTimeKey t h) = do
    put MemTimeKeyS
    putWord64be t
    put h
  -- 0x0b
  put MemTimeKeyS = putWord8 0x0b
  get = do
    guard . (== 0x0b) =<< getWord8
    MemTimeKey <$> getWord64be <*> get

instance Key MemTimeKey

instance KeyValue MemTimeKey ()

-- | Key of the input of a mempool transaction that spends an output, in the
-- mempool column family of the layered layout. The output may be one of a
-- confirmed transaction or of another mempool transaction: the key is the
-- same, so nothing has to change here when the transaction that created the
-- output is confirmed.
--
-- Spenders in the chain's column families are part of the body of the
-- transaction whose output they spend. A mempool spender cannot be: writing
-- it there would let the mempool change the data of the chain.
data MemSpenderKey
  = -- | key of the mempool spender of an output
    MemSpenderKey !OutPoint
  | -- | short key that matches the mempool spenders of the outputs of a
    -- transaction
    MemSpenderKeyS !TxHash
  | -- | short key that matches every mempool spender
    MemSpenderKeyB
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize MemSpenderKey where
  -- 0x0c · TxHash · Index
  put (MemSpenderKey OutPoint {hash = h, index = i}) = do
    put (MemSpenderKeyS h)
    put i
  -- 0x0c · TxHash
  put (MemSpenderKeyS h) = do
    put MemSpenderKeyB
    put h
  -- 0x0c
  put MemSpenderKeyB = putWord8 0x0c
  get = do
    guard . (== 0x0c) =<< getWord8
    h <- get
    i <- get
    return $ MemSpenderKey OutPoint {hash = h, index = i}

instance Key MemSpenderKey

instance KeyValue MemSpenderKey Spender

-- | Key of something that the mempool does at an address, in the column
-- family of mempool address records of the layered layout. There are three
-- kinds, and all those of an address are read with one scan of its prefix.
--
-- Every entry depends on one mempool transaction alone: on its inputs, with
-- the outputs they spend, and on its outputs. None says whether an output
-- that is spent was created by a confirmed transaction or by another
-- mempool transaction, nor whether an output that is created is spent. So
-- a transaction is added by writing its entries and removed by deleting
-- them, whatever else is in the mempool, and nothing here changes when a
-- block confirms some transactions and not others. What a query needs is
-- worked out from the entries of the address when it is answered; see
-- "Haskoin.Store.Database.Mempool".
--
-- Times are stored complemented, as in a mempool 'BlockRef', so that entries
-- come out newest first, which is the order of the API.
data MemAddrKey
  = -- | a mempool transaction that spends from or pays to the address, by
    -- the time it entered the mempool
    MemAddrTxKey !Address !UnixTime !TxHash
  | -- | an output of the address that a mempool transaction spends
    MemAddrSpentKey !Address !OutPoint
  | -- | an output that a mempool transaction pays to the address, spent or
    -- not, by the time the transaction entered the mempool
    MemAddrOutKey !Address !UnixTime !OutPoint
  | -- | what the entries of an address add up to; see t'MemSummary'
    MemAddrSumKey !Address
  | -- | short key that matches the transactions of an address
    MemAddrTxKeyA !Address
  | -- | short key that matches the outputs paid to an address
    MemAddrOutKeyA !Address
  | -- | short key that matches every entry of an address
    MemAddrKeyA !Address
  | -- | short key that matches the entries of every address
    MemAddrKeyS
  deriving (Show, Eq, Ord, Generic, Hashable)

instance Serialize MemAddrKey where
  -- 0x0d · Address · 0x00 · time · TxHash
  put (MemAddrTxKey a t h) = do
    put (MemAddrKeyA a)
    putWord8 0x00
    putWord64be (maxBound - t)
    put h
  -- 0x0d · Address · 0x01 · OutPoint
  put (MemAddrSpentKey a p) = do
    put (MemAddrKeyA a)
    putWord8 0x01
    put p
  -- 0x0d · Address · 0x02 · time · OutPoint
  put (MemAddrOutKey a t p) = do
    put (MemAddrKeyA a)
    putWord8 0x02
    putWord64be (maxBound - t)
    put p
  -- 0x0d · Address · 0x03
  put (MemAddrSumKey a) = do
    put (MemAddrKeyA a)
    putWord8 0x03
  -- 0x0d · Address · 0x00
  put (MemAddrTxKeyA a) = do
    put (MemAddrKeyA a)
    putWord8 0x00
  -- 0x0d · Address · 0x02
  put (MemAddrOutKeyA a) = do
    put (MemAddrKeyA a)
    putWord8 0x02
  -- 0x0d · Address
  put (MemAddrKeyA a) = do
    put MemAddrKeyS
    put a
  -- 0x0d
  put MemAddrKeyS = putWord8 0x0d
  get = do
    guard . (== 0x0d) =<< getWord8
    a <- get
    getWord8 >>= \case
      0x00 -> MemAddrTxKey a . (maxBound -) <$> getWord64be <*> get
      0x01 -> MemAddrSpentKey a <$> get
      0x02 -> MemAddrOutKey a . (maxBound -) <$> getWord64be <*> get
      0x03 -> return (MemAddrSumKey a)
      _ -> fail "Cannot decode MemAddrKey"

instance Key MemAddrKey

-- | Value of a 'MemAddrKey': nothing for a transaction, the amount in
-- satoshi of an output that is spent, and the amount and script of an output
-- that is created. The first byte says which, so that a value can be decoded
-- without its key.
data MemAddrVal
  = -- | value of a 'MemAddrTxKey'
    MemAddrTxVal
  | -- | value of a 'MemAddrSpentKey': amount of the output
    MemAddrSpentVal !Word64
  | -- | value of a 'MemAddrOutKey': amount and script of the output
    MemAddrOutVal !Word64 !ByteString
  | -- | value of a 'MemAddrSumKey'
    MemAddrSumVal !MemSummary
  deriving (Show, Read, Eq, Ord, Generic, Hashable, NFData)

-- | What the entries of an address in the mempool area add up to, kept so
-- that its balance is one read by key however many entries there are, and
-- so that an address with nothing in the mempool is found by a key that is
-- not there instead of a scan.
--
-- It is a function of the entries of the address, and of nothing else: not
-- of the chain. Whether an output that the mempool spends is one of the
-- chain or one that the mempool made is the difference between 'spent' and
-- 'both', and 'both' only changes when one of the two transactions enters
-- or leaves the mempool. So the summary stays right when a block confirms
-- some transactions and not others, like the entries themselves.
data MemSummary = MemSummary
  { -- | mempool transactions that touch the address
    txs :: !Word64,
    -- | amount of the outputs that the mempool pays to the address
    paid :: !Word64,
    -- | number of those outputs
    paidCount :: !Word64,
    -- | amount of the outputs of the address that the mempool spends
    spent :: !Word64,
    -- | number of those outputs
    spentCount :: !Word64,
    -- | amount of the outputs that the mempool both pays and spends
    both :: !Word64,
    -- | number of those outputs
    bothCount :: !Word64
  }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, Serialize, NFData)

instance Serialize MemAddrVal where
  put MemAddrTxVal = putWord8 0x00
  put (MemAddrSpentVal v) = putWord8 0x01 >> putWord64be v
  put (MemAddrOutVal v s) = putWord8 0x02 >> putWord64be v >> put s
  put (MemAddrSumVal s) = putWord8 0x03 >> put s
  get =
    getWord8 >>= \case
      0x00 -> return MemAddrTxVal
      0x01 -> MemAddrSpentVal <$> getWord64be
      0x02 -> MemAddrOutVal <$> getWord64be <*> get
      0x03 -> MemAddrSumVal <$> get
      _ -> fail "Cannot decode MemAddrVal"

instance KeyValue MemAddrKey MemAddrVal

-- | Key of the body of a transaction that left the mempool without being
-- confirmed, in the mempool column family of the layered layout: it was
-- replaced, it conflicted with a block, or it was too old. The store keeps
-- such bodies, marked as deleted, so that the API can go on saying what
-- became of a transaction that it once showed.
--
-- Version 20 keeps them in the column family of transactions, under
-- t'TxKey'. There, deleting a mempool transaction writes to the data of the
-- chain, which the layered layout does not allow.
data MemDeletedKey
  = -- | key of a deleted transaction
    MemDeletedKey !TxHash
  | -- | short key that matches the deleted transactions whose hash starts
    -- with these six bytes, as 'TxKeyS' does for the others; see
    -- 'decodeTxKey'
    MemDeletedKeyP !(Word32, Word16)
  | -- | short key that matches every deleted transaction
    MemDeletedKeyS
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize MemDeletedKey where
  -- 0x0e · TxHash
  put (MemDeletedKey h) = do
    put MemDeletedKeyS
    put h
  -- 0x0e · first six bytes of a TxHash
  put (MemDeletedKeyP h) = do
    put MemDeletedKeyS
    put h
  -- 0x0e
  put MemDeletedKeyS = putWord8 0x0e
  get = do
    guard . (== 0x0e) =<< getWord8
    MemDeletedKey <$> get

instance Key MemDeletedKey

instance KeyValue MemDeletedKey TxData

-- | Key that is present while a migration of the database is under way,
-- with the version that it started from. A migration that works in several
-- batches sets it with the first and removes it with the last, so that a
-- store that finds it knows to carry on, whatever the version says.
data MigrationKey = MigrationKey
  deriving (Eq, Show, Read, Ord, Generic, Hashable)

instance Serialize MigrationKey where
  -- 0x0f
  put MigrationKey = putWord8 0x0f
  get = do
    guard . (== 0x0f) =<< getWord8
    return MigrationKey

instance Key MigrationKey

instance KeyValue MigrationKey Word32

-- | Block entry database key.
newtype BlockKey = BlockKey BlockHash
  deriving stock (Show, Read, Eq, Ord, Generic)
  deriving anyclass (Hashable)

instance Serialize BlockKey where
  -- 0x01 · BlockHash
  put (BlockKey h) = do
    putWord8 0x01
    put h
  get = do
    guard . (== 0x01) =<< getWord8
    BlockKey <$> get

instance Key BlockKey

instance KeyValue BlockKey BlockData

-- | Block height database key.
newtype HeightKey = HeightKey BlockHeight
  deriving stock (Show, Read, Eq, Ord, Generic)
  deriving anyclass (Hashable)

instance Serialize HeightKey where
  -- 0x03 · BlockHeight
  put (HeightKey h) = do
    putWord8 0x03
    put h
  get = do
    guard . (== 0x03) =<< getWord8
    HeightKey <$> get

instance Key HeightKey

instance KeyValue HeightKey [BlockHash]

-- | Address balance database key.
data BalKey
  = -- | key of the balance of an address
    BalKey !Address
  | -- | short key that matches every balance
    BalKeyS
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize BalKey where
  -- 0x04 · Address
  put (BalKey a) = do
    putWord8 0x04
    put a
  -- 0x04
  put BalKeyS = putWord8 0x04
  get = do
    guard . (== 0x04) =<< getWord8
    BalKey <$> get

instance Key BalKey

instance KeyValue BalKey BalVal

-- | Key for best block in database.
data BestKey = BestKey
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize BestKey where
  -- 0x00 × 32
  put BestKey = put (BS.replicate 32 0x00)
  get = do
    guard . (== BS.replicate 32 0x00) =<< getBytes 32
    return BestKey

instance Key BestKey

instance KeyValue BestKey BlockHash

-- | Key for database version.
data VersionKey = VersionKey
  deriving (Eq, Show, Read, Ord, Generic, Hashable)

instance Serialize VersionKey where
  -- 0x0a
  put VersionKey = putWord8 0x0a
  get = do
    guard . (== 0x0a) =<< getWord8
    return VersionKey

instance Key VersionKey

instance KeyValue VersionKey Word32

-- | Balance of an address as stored in the database. The address itself is in
-- the key.
data BalVal = BalVal
  { -- | confirmed balance in satoshi
    confirmed :: !Word64,
    -- | unconfirmed balance in satoshi
    unconfirmed :: !Word64,
    -- | number of unspent outputs
    utxo :: !Word64,
    -- | number of transactions
    txs :: !Word64,
    -- | total amount received in satoshi, including unconfirmed
    received :: !Word64
  }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, Serialize, NFData)

-- | Build a 'Balance' from an address and its stored value.
valToBalance :: Address -> BalVal -> Balance
valToBalance address BalVal {..} =
  Balance {..}

-- | Stored value of a 'Balance'.
balanceToVal :: Balance -> BalVal
balanceToVal Balance {..} =
  BalVal {..}

-- | Default balance for an address.
instance Default BalVal where
  def =
    BalVal
      { confirmed = 0,
        unconfirmed = 0,
        utxo = 0,
        txs = 0,
        received = 0
      }

-- | Unspent output as stored in the database. The outpoint is in the key.
data UnspentVal = UnspentVal
  { -- | block or mempool position of the transaction
    block :: !BlockRef,
    -- | amount in satoshi
    value :: !Word64,
    -- | output script
    script :: !ByteString
  }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, Serialize, NFData)

-- | Split an 'Unspent' into its key and its stored value.
unspentToVal :: Unspent -> (OutPoint, UnspentVal)
unspentToVal Unspent {..} = (outpoint, UnspentVal {..})

-- | Build an 'Unspent' from its outpoint and stored value, decoding the address from the script.
valToUnspent :: Network -> Ctx -> OutPoint -> UnspentVal -> Unspent
valToUnspent net ctx outpoint UnspentVal {..} =
  Unspent {..}
  where
    address = eitherToMaybe (scriptToAddressBS net ctx script)
