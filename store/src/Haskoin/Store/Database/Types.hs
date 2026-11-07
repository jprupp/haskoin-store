{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NoFieldSelectors #-}

{- |
Module      : Haskoin.Store.Database.Types
Copyright   : (c) Haskoin Developers
License     : MIT
Maintainer  : jprupp@protonmail.ch
Stability   : experimental
Portability : POSIX

Keys and values stored in RocksDB.

Every key type serializes to a distinct first byte, so that keys of different
types never collide even when they share a column family. Many key types
have short variants (suffixed @A@, @B@ or @S@) that serialize to a prefix of
the full key, for use with the prefix queries of "Database.RocksDB.Query".

+-----------+--------------+----------------------------------------+
| Prefix    | Key          | Value                                  |
+===========+==============+========================================+
| 32 x 0x00 | 'BestKey'    | hash of the best block                 |
+-----------+--------------+----------------------------------------+
| 0x01      | 'BlockKey'   | 'BlockData'                            |
+-----------+--------------+----------------------------------------+
| 0x02      | 'TxKey'      | 'TxData'                               |
+-----------+--------------+----------------------------------------+
| 0x03      | 'HeightKey'  | hashes of the blocks at a height       |
+-----------+--------------+----------------------------------------+
| 0x04      | 'BalKey'     | 'BalVal'                               |
+-----------+--------------+----------------------------------------+
| 0x05      | 'AddrTxKey'  | nothing                                |
+-----------+--------------+----------------------------------------+
| 0x06      | 'AddrOutKey' | 'OutVal'                               |
+-----------+--------------+----------------------------------------+
| 0x07      | 'MemKey'     | whole mempool (legacy)                 |
+-----------+--------------+----------------------------------------+
| 0x08      | 'MemTxKey'   | time a transaction entered the mempool |
+-----------+--------------+----------------------------------------+
| 0x09      | 'UnspentKey' | 'UnspentVal'                           |
+-----------+--------------+----------------------------------------+
| 0x0a      | 'VersionKey' | database version                       |
+-----------+--------------+----------------------------------------+
| 0x0b      | 'MemTimeKey' | nothing                                |
+-----------+--------------+----------------------------------------+
-}
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
-- hash, as the pairs in 'getMempool' do, so a page of the mempool is read
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

-- | Block entry database key.
newtype BlockKey = BlockKey BlockHash
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

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
  deriving (Show, Read, Eq, Ord, Generic, Hashable)

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
