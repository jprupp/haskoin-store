{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NoFieldSelectors #-}

module Haskoin.Store.Database.Types
  ( AddrTxKey (..),
    AddrOutKey (..),
    BestKey (..),
    BlockKey (..),
    BalKey (..),
    HeightKey (..),
    MemKey (..),
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
  | AddrTxKeyB !Address !BlockRef
  | AddrTxKeyS
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
  | AddrOutKeyB !Address !BlockRef
  | AddrOutKeyS
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

data OutVal = OutVal !Word64 !ByteString
  deriving (Show, Read, Eq, Ord, Generic, Hashable, Serialize)

instance KeyValue AddrOutKey OutVal

-- | Transaction database key.
data TxKey
  = TxKey !TxHash
  | TxKeyS !(Word32, Word16)
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
  = UnspentKey !OutPoint
  | UnspentKeyS !TxHash
  | UnspentKeyB
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

toUnspent :: Ctx -> AddrOutKey -> OutVal -> Maybe Unspent
toUnspent ctx (AddrOutKey _address block outpoint) (OutVal value script) =
  Just
    Unspent
      { block = block,
        value = value,
        script = script,
        outpoint = outpoint,
        address = eitherToMaybe (scriptToAddressBS ctx script)
      }
toUnspent _ _ _ = Nothing

-- | Mempool transaction database key.
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
  = BalKey !Address
  | BalKeyS
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

data BalVal = BalVal
  { confirmed :: !Word64,
    unconfirmed :: !Word64,
    utxo :: !Word64,
    txs :: !Word64,
    received :: !Word64
  }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, Serialize, NFData)

valToBalance :: Address -> BalVal -> Balance
valToBalance address BalVal {..} =
  Balance {..}

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

data UnspentVal = UnspentVal
  { block :: !BlockRef,
    value :: !Word64,
    script :: !ByteString
  }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, Serialize, NFData)

unspentToVal :: Unspent -> (OutPoint, UnspentVal)
unspentToVal Unspent {..} = (outpoint, UnspentVal {..})

valToUnspent :: Ctx -> OutPoint -> UnspentVal -> Unspent
valToUnspent ctx outpoint UnspentVal {..} =
  Unspent {..}
  where
    address = eitherToMaybe (scriptToAddressBS ctx script)
