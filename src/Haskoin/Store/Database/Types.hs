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
-- | 0x07      | @MemKey@     | whole mempool (legacy)                 |
-- +-----------+--------------+----------------------------------------+
-- | 0x08      | 'MemTxKey'   | time a transaction entered the mempool |
-- +-----------+--------------+----------------------------------------+
-- | 0x09      | 'UnspentKey' | 'UnspentVal'                           |
-- +-----------+--------------+----------------------------------------+
-- | 0x0a      | 'VersionKey' | database version                       |
-- +-----------+--------------+----------------------------------------+
-- | 0x0b      | @MemTimeKey@ | nothing                                |
-- +-----------+--------------+----------------------------------------+
--
-- = Keys of the old mempool, and later ones
--
-- They are the keys of the two column families that held the mempool of
-- the store until version 1.16.0, when the mempool moved to the pool of the
-- fleet, in Redis. Nothing reads or writes them any more; they are kept to
-- say what such a database holds. t'MemSummary' lives on as what the pool
-- says of an address ("Haskoin.Store.Database.Query.Pooled").
--
-- +-----------+------------------+-------------------------------------------+
-- | Prefix    | Key              | Value                                     |
-- +===========+==================+===========================================+
-- | 0x0c      | @MemSpenderKey@  | 'Spender': mempool input that spends an   |
-- |           |                  | output                                    |
-- +-----------+------------------+-------------------------------------------+
-- | 0x0d      | @MemAddrKey@     | @MemAddrVal@: what the mempool did at an  |
-- |           |                  | address                                   |
-- +-----------+------------------+-------------------------------------------+
-- | 0x0e      | @MemDeletedKey@  | 'TxData' of a transaction that left the   |
-- |           |                  | mempool unconfirmed: not written any more |
-- +-----------+------------------+-------------------------------------------+
-- | 0x0f      | t'MigrationKey'  | version a migration in progress started   |
-- |           |                  | from                                      |
-- +-----------+------------------+-------------------------------------------+
-- | 0x10      | t'SpenderKey'    | t'SpenderVal': inputs of blocks that      |
-- |           |                  | spend outputs of a transaction            |
-- +-----------+------------------+-------------------------------------------+
-- | 0x13      | t'TokBalKey'     | t'TokBalVal': tokens of a category at an  |
-- |           |                  | address                                   |
-- +-----------+------------------+-------------------------------------------+
-- | 0x14      | t'HolderKey'     | t'TokBalVal': the same, category first    |
-- +-----------+------------------+-------------------------------------------+
-- | 0x15      | t'CatTxKey'      | nothing: a transaction of a category      |
-- +-----------+------------------+-------------------------------------------+
-- | 0x16      | t'CatOutKey'     | t'OutVal': an unspent output of a         |
-- |           |                  | category                                  |
-- +-----------+------------------+-------------------------------------------+
--
-- Since version 23 the values of 'BalKey' and of t'SpenderKey' can also
-- be written as merge records, which RocksDB combines into the value:
-- "Haskoin.Store.Database.Merge". Since version 24 those of t'TokBalKey'
-- and t'HolderKey' too, with the operator of the balances.
module Haskoin.Store.Database.Types
  ( AddrTxKey (..),
    AddrOutKey (..),
    BestKey (..),
    BlockKey (..),
    BalKey (..),
    TokBalKey (..),
    HolderKey (..),
    TokBalVal (..),
    addTokBalVal,
    CatTxKey (..),
    CatOutKey (..),
    HeightKey (..),
    MemTxKey (..),
    MemSummary (..),
    PoolTokens (..),
    CategorySummary (..),
    MigrationKey (..),
    ReplacedKey (..),
    ReplacedAtKey (..),
    SpenderKey (..),
    SpenderVal (..),
    spenderGroup,
    spenderKey,
    TxKey (..),
    decodeTxKey,
    UnspentKey (..),
    VersionKey (..),
    BalVal (..),
    valToBalance,
    balanceToVal,
    addBalVal,
    UnspentVal (..),
    toUnspent,
    unspentToVal,
    valToUnspent,
    OutVal (..),
  )
where

import Control.DeepSeq (NFData)
import Control.Monad (forM_, guard, replicateM)
import Data.Bits
  ( shift,
    (.&.),
  )
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Default (Default (..))
import Data.Hashable (Hashable)
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.Serialize
  ( Get,
    Serialize (..),
    getBytes,
    getWord16be,
    getWord32be,
    getWord8,
    putWord32be,
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
import Haskoin.Store.Common (Replaced)
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
toUnspent :: Network -> AddrOutKey -> OutVal -> Maybe Unspent
toUnspent net (AddrOutKey _address block outpoint) (OutVal value script) =
  Just
    Unspent
      { block = block,
        value = value,
        script = script,
        outpoint = outpoint,
        address = eitherToMaybe (scriptToAddressBS net script)
      }
toUnspent _ _ _ = Nothing

-- | Key of one mempool transaction in the mempool column family, with the
-- time it entered the mempool as its value. Replaces @MemKey@, which kept
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

-- | What the mempool does at an address, added up: what the pool of the
-- fleet gives for an address ("Haskoin.Store.Fleet.Pool"), so that its
-- balance is one read however many transactions touch it.
--
-- It is a function of the transactions of the pool that touch the address,
-- and of nothing else: not of the chain. Whether an output that the pool
-- spends is one of the chain or one that the pool made is the difference
-- between 'spent' and 'both', and 'both' only changes when one of the two
-- transactions enters or leaves the pool. So the summary stays right when
-- a block confirms some transactions and not others.
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

-- | What the mempool does to the tokens of one category at one address,
-- added up, as the pool of the fleet keeps it
-- ("Haskoin.Store.Fleet.Pool.Decide"): what a token
-- balance of the chain needs to become one with the mempool.
--
-- Unlike t'MemSummary', it does not add up what is paid and what is spent
-- and take off what is both. Every output that holds tokens is counted at
-- most once: as free, while the pool makes it and does not use it up, or
-- as spent, while the pool uses it up and the chain made it. So no sum is
-- ever more than the tokens that exist, which a supply of the largest
-- amount that moves twice in the pool would be otherwise. The amount paid
-- is the exception, as the chain's amount received is.
data PoolTokens = PoolTokens
  { -- | mempool transactions that move tokens of the category at the
    -- address
    txs :: !Word64,
    -- | fungible tokens of the outputs that the mempool makes and does not
    -- use up
    free :: !Word64,
    -- | non-fungible tokens of those outputs
    freeNfts :: !Word64,
    -- | number of those outputs
    freeOutputs :: !Word64,
    -- | fungible tokens of the outputs of the chain that the mempool uses
    -- up
    spent :: !Word64,
    -- | non-fungible tokens of those outputs
    spentNfts :: !Word64,
    -- | number of those outputs
    spentOutputs :: !Word64,
    -- | fungible tokens of all the outputs that the mempool makes, as the
    -- amount received of the chain counts them
    received :: !Word64
  }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, Serialize, NFData)

-- | What the mempool does to the tokens of one category, as the pool of the
-- fleet keeps it: enough to know whether a list of the category needs to
-- read the pool.
data CategorySummary = CategorySummary
  { -- | mempool transactions that move tokens of the category
    txs :: !Word64,
    -- | outputs that the mempool makes and does not use up
    free :: !Word64,
    -- | outputs of the chain that the mempool uses up
    spent :: !Word64
  }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, Serialize, NFData)

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

-- | Key of what is kept of a mempool transaction that a block replaced
-- ('Replaced'), in the @block@ column family, since haskoin-store 1.13.0.
-- It is part of the chain: written by the import of blocks until version
-- 1.16.0, when the shared pool took this over, and deleted by the import
-- and the revert of blocks.
data ReplacedKey = ReplacedKey !TxHash
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize ReplacedKey where
  -- 0x11 · TxHash
  put (ReplacedKey h) = putWord8 0x11 >> put h
  get = do
    guard . (== 0x11) =<< getWord8
    ReplacedKey <$> get

instance Key ReplacedKey

instance KeyValue ReplacedKey Replaced

-- | The transactions replaced by the block at a height, in the @height@
-- column family, which the revert of the block forgets, and which are
-- forgotten 'Haskoin.Store.Common.replacedKept' blocks later. The height comes first, written
-- big-endian, so that a scan of one height finds them all. The column
-- family has no prefix extractor, which a scan by five bytes needs.
data ReplacedAtKey
  = -- | a transaction replaced at a height
    ReplacedAtKey !BlockHeight !TxHash
  | -- | short key that matches the transactions replaced at a height
    ReplacedAtKeyH !BlockHeight
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize ReplacedAtKey where
  -- 0x12 · height · TxHash
  put (ReplacedAtKey h t) = put (ReplacedAtKeyH h) >> put t
  -- 0x12 · height
  put (ReplacedAtKeyH h) = putWord8 0x12 >> putWord32be h
  get = do
    guard . (== 0x12) =<< getWord8
    ReplacedAtKey <$> getWord32be <*> get

instance Key ReplacedAtKey

instance KeyValue ReplacedAtKey ()

-- | Key of the inputs of blocks that spend some outputs of a transaction,
-- in the @spender@ column family, since database version 22: the
-- transaction, and the number of a group of 'spenderGroup' of its outputs
-- that follow each other. The value has the spender of each output of the
-- group that is spent in a t'SpenderVal'.
--
-- Until then the spenders of the outputs of a confirmed transaction were in
-- its own record, 'TxData', which was read and written whole again every
-- time a block spent one of them or was reverted: once for each of its
-- outputs, over years, for a record of any size. Now the record of a
-- transaction is written when a block confirms it and never again, and
-- spending an output reads and writes a value of a few dozen bytes.
--
-- = Why groups
--
-- A key for each spent output would make a spend one write that reads
-- nothing. But a transaction is read far more often than it is spent
-- from, and has to be read with its spenders: that would take a key for
-- each of its outputs, with each read of it. One value
-- for all the outputs of a transaction would cost one key to read, and
-- bring back the trouble with large transactions: a payout to ten
-- thousand would rewrite half a megabyte ten thousand times. With groups
-- of sixteen nearly every transaction has one key, and no value is larger
-- than sixteen spenders.
--
-- The groups of a transaction are next to each other, so all its spenders
-- can also be read with one scan of their common prefix, which is what the
-- prefix of the column family is.
data SpenderKey
  = -- | spenders of a group of outputs of a transaction
    SpenderKey !TxHash !Word32
  | -- | short key that matches the groups of a transaction
    SpenderKeyS !TxHash
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize SpenderKey where
  -- 0x10 · TxHash · group
  put (SpenderKey h g) = do
    put (SpenderKeyS h)
    put g
  -- 0x10 · TxHash
  put (SpenderKeyS h) = do
    putWord8 0x10
    put h
  get = do
    guard . (== 0x10) =<< getWord8
    h <- get
    SpenderKey h <$> get

instance Key SpenderKey

instance KeyValue SpenderKey SpenderVal

-- | The number of outputs whose spenders are under one t'SpenderKey'.
spenderGroup :: Word32
spenderGroup = 16

-- | The key that has the spender of an output.
spenderKey :: OutPoint -> SpenderKey
spenderKey p = SpenderKey p.hash (p.index `div` spenderGroup)

-- | The inputs of blocks that spend outputs of a group, by the number of
-- the output in its transaction. A group none of whose outputs is spent
-- has no key.
--
-- The value is one byte with the number of entries, then the entries in
-- ascending order of output, each of forty bytes: the number of the output
-- (four bytes, most significant first), the hash of the transaction that
-- spends it (32) and the number of the input that does (four, most
-- significant first).
--
-- Since database version 23 the column family has merge records too, which
-- the import of a block writes instead of reading a group to add to it
-- ("Haskoin.Store.Database.Merge"). A merge record is of this type and
-- these bytes, with the spenders to add. RocksDB joins them with the group
-- when it is read, the newest entry winning for an output that several
-- have, and a reader never sees one.
newtype SpenderVal = SpenderVal (IntMap Spender)
  deriving (Show, Eq)

instance Serialize SpenderVal where
  put (SpenderVal m) = do
    putWord8 (fromIntegral (IntMap.size m))
    forM_ (IntMap.toAscList m) $ \(i, spender) -> put (fromIntegral i :: Word32) >> put spender
  get = do
    n <- getWord8
    SpenderVal . IntMap.fromList <$> replicateM (fromIntegral n) ((,) . fromIntegral <$> (get :: Get Word32) <*> get)

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

-- | Key of the tokens of one category that an address holds, in the column
-- family @tok-bal@ (CashTokens, @docs\/cashtokens.md@). The same values are
-- kept under t'HolderKey' too, category first, to list the holders of a
-- category.
data TokBalKey
  = -- | the tokens of a category at an address
    TokBalKey !Address !TxHash
  | -- | short key that matches the token balances of an address
    TokBalKeyA !Address
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize TokBalKey where
  -- 0x13 · Address · category
  put (TokBalKey a c) = put (TokBalKeyA a) >> put c
  -- 0x13 · Address
  put (TokBalKeyA a) = putWord8 0x13 >> put a
  get = do
    guard . (== 0x13) =<< getWord8
    TokBalKey <$> get <*> get

instance Key TokBalKey

instance KeyValue TokBalKey TokBalVal

-- | Key of the tokens of a category at an address, category first, in the
-- column family @holder@: the holders of a category are a scan of its
-- prefix. Written with the same values as t'TokBalKey'.
data HolderKey
  = -- | the tokens of a category at an address
    HolderKey !TxHash !Address
  | -- | short key that matches the holders of a category
    HolderKeyC !TxHash
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize HolderKey where
  -- 0x14 · category · Address
  put (HolderKey c a) = put (HolderKeyC c) >> put a
  -- 0x14 · category
  put (HolderKeyC c) = putWord8 0x14 >> put c
  get = do
    guard . (== 0x14) =<< getWord8
    HolderKey <$> get <*> get

instance Key HolderKey

instance KeyValue HolderKey TokBalVal

-- | The tokens of a category that an address holds in the chain: five
-- numbers, as a t'BalVal' has, so that the merge operator of the balances
-- adds them ("Haskoin.Store.Database.Merge"). The tokens in the mempool are
-- in the pool that the fleet shares, not here.
data TokBalVal = TokBalVal
  { -- | fungible tokens in unspent outputs
    amount :: !Word64,
    -- | non-fungible tokens in unspent outputs
    nfts :: !Word64,
    -- | unspent outputs that hold tokens of the category
    outputs :: !Word64,
    -- | transactions that moved tokens of the category to or from the
    -- address
    count :: !Word64,
    -- | fungible tokens that the outputs of the address ever received
    received :: !Word64
  }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, Serialize, NFData)

instance Default TokBalVal where
  def = TokBalVal {amount = 0, nfts = 0, outputs = 0, count = 0, received = 0}

-- | The sum of two token balances, each number modulo 2^64, as the merge
-- operator adds them.
addTokBalVal :: TokBalVal -> TokBalVal -> TokBalVal
addTokBalVal a b =
  TokBalVal
    { amount = a.amount + b.amount,
      nfts = a.nfts + b.nfts,
      outputs = a.outputs + b.outputs,
      count = a.count + b.count,
      received = a.received + b.received
    }

-- | Database key of a transaction that moved tokens of a category, in the
-- column family @cat-tx@: as t'AddrTxKey', with the category in place of
-- the address.
data CatTxKey
  = -- | a transaction of a category
    CatTxKey !TxHash !TxRef
  | -- | short key that matches the transactions of a category
    CatTxKeyC !TxHash
  | -- | short key that matches the transactions of a category at a position
    -- in the chain
    CatTxKeyB !TxHash !BlockRef
  deriving (Show, Eq, Ord, Generic, Hashable)

instance Serialize CatTxKey where
  -- 0x15 · category · BlockRef · TxHash
  put (CatTxKey c TxRef {block = b, txid = t}) = put (CatTxKeyB c b) >> put t
  -- 0x15 · category · BlockRef
  put (CatTxKeyB c b) = put (CatTxKeyC c) >> put b
  -- 0x15 · category
  put (CatTxKeyC c) = putWord8 0x15 >> put c
  get = do
    guard . (== 0x15) =<< getWord8
    c <- get
    b <- get
    t <- get
    return $ CatTxKey c TxRef {block = b, txid = t}

instance Key CatTxKey

instance KeyValue CatTxKey ()

-- | Database key of an unspent output that holds tokens of a category, in
-- the column family @cat-out@: as t'AddrOutKey', with the category in place
-- of the address.
data CatOutKey
  = -- | an unspent output of a category
    CatOutKey !TxHash !BlockRef !OutPoint
  | -- | short key that matches the unspent outputs of a category
    CatOutKeyC !TxHash
  | -- | short key that matches the unspent outputs of a category at a
    -- position in the chain
    CatOutKeyB !TxHash !BlockRef
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

instance Serialize CatOutKey where
  -- 0x16 · category · BlockRef · OutPoint
  put (CatOutKey c b p) = put (CatOutKeyB c b) >> put p
  -- 0x16 · category · BlockRef
  put (CatOutKeyB c b) = put (CatOutKeyC c) >> put b
  -- 0x16 · category
  put (CatOutKeyC c) = putWord8 0x16 >> put c
  get = do
    guard . (== 0x16) =<< getWord8
    CatOutKey <$> get <*> get <*> get

instance Key CatOutKey

instance KeyValue CatOutKey OutVal

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
--
-- The value is the five numbers, each of eight bytes with the most
-- significant first: forty bytes.
--
-- Since database version 23 the column family has merge records too, which
-- the import of a block writes instead of reading a balance to change it
-- ("Haskoin.Store.Database.Merge"). A merge record is of this type and
-- these bytes, with five differences in place of the five numbers: each is
-- added to its number modulo 2^64, so that a number goes down by the two's
-- complement of what is taken off. RocksDB adds them up when the balance
-- is read ('addBalVal' is the same sum), and a reader never sees one.
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

-- | The sum of two stored balances, or of a balance and its differences,
-- or of two sets of differences: each number modulo 2^64. It is what the
-- merge operator of the column family does
-- ('Haskoin.Store.Database.Merge.balanceOperator').
addBalVal :: BalVal -> BalVal -> BalVal
addBalVal a b =
  BalVal
    { confirmed = a.confirmed + b.confirmed,
      unconfirmed = a.unconfirmed + b.unconfirmed,
      utxo = a.utxo + b.utxo,
      txs = a.txs + b.txs,
      received = a.received + b.received
    }

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
valToUnspent :: Network -> OutPoint -> UnspentVal -> Unspent
valToUnspent net outpoint UnspentVal {..} =
  Unspent {..}
  where
    address = eitherToMaybe (scriptToAddressBS net script)
