{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NoFieldSelectors #-}

module Haskoin.Store.Data
  ( -- * Address Balances
    Balance (..),
    zeroBalance,
    nullBalance,

    -- * Block Data
    BlockData (..),
    confirmed,

    -- * Transactions
    TxRef (..),
    TxData (..),
    txDataFee,
    isCoinbaseTx,
    Transaction (..),
    transactionData,
    fromTransaction,
    toTransaction,
    StoreInput (..),
    isCoinbase,
    StoreOutput (..),
    Prev (..),
    Spender (..),
    BlockRef (..),
    UnixTime,
    getUnixTime,
    putUnixTime,
    BlockPos,

    -- * Unspent Outputs
    Unspent (..),

    -- * Extended Public Keys
    XPubSpec (..),
    XPubBal (..),
    XPubUnspent (..),
    XPubSummary (..),
    DeriveType (..),
    textToDeriveType,
    deriveTypeToText,

    -- * Other Data
    TxId (..),
    GenericResult (..),
    SerialList (..),
    RawResult (..),
    RawResultList (..),
    PeerInfo (..),
    Healthy (..),
    BlockHealth (..),
    TimeHealth (..),
    CountHealth (..),
    MaxHealth (..),
    HealthCheck (..),
    Event (..),
    Except (..),

    -- * Blockchain.info API
    BinfoInfo (..),
    BinfoBlockId (..),
    BinfoTxId (..),
    encodeBinfoTxId,
    BinfoFilter (..),
    BinfoMultiAddr (..),
    BinfoShortBal (..),
    BinfoBalance (..),
    toBinfoAddrs,
    BinfoRawAddr (..),
    BinfoAddr (..),
    parseBinfoAddr,
    BinfoWallet (..),
    BinfoUnspent (..),
    binfoHexValue,
    BinfoUnspents (..),
    BinfoBlock (..),
    toBinfoBlock,
    BinfoTx (..),
    relevantTxs,
    toBinfoTx,
    toBinfoTxSimple,
    BinfoTxInput (..),
    BinfoTxOutput (..),
    BinfoSpender (..),
    BinfoXPubPath (..),
    BinfoBlockInfo (..),
    toBinfoBlockInfo,
    BinfoSymbol (..),
    BinfoTicker (..),
    BinfoRate (..),
    BinfoHistory (..),
    toBinfoHistory,
    BinfoDate (..),
    BinfoHeader (..),
    BinfoMempool (..),
    BinfoBlockInfos (..),
  )
where

import Control.Applicative (optional, (<|>))
import Control.DeepSeq (NFData)
import Control.Exception (Exception)
import Control.Monad (guard, join, mzero, unless, (<=<))
import Data.Aeson
  ( Encoding,
    FromJSON (..),
    ToJSON (..),
    Value (..),
    (.!=),
    (.:),
    (.:?),
    (.=),
  )
import Data.Aeson qualified as A
import Data.Aeson.Encoding qualified as A
import Data.Aeson.Types (Parser)
import Data.Binary (Binary (get, put))
import Data.Bits (Bits (..))
import Data.Bool (bool)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Builder qualified as Builder
import Data.Bytes.Get
import Data.Bytes.Get qualified as Bytes.Get
import Data.Bytes.Put
import Data.Bytes.Serial
import Data.Default (Default (..))
import Data.Foldable (toList)
import Data.Function (on)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.Hashable (Hashable (..))
import Data.Int (Int32, Int64)
import Data.IntMap qualified as IntMap
import Data.IntMap.Strict (IntMap)
import Data.Maybe
  ( catMaybes,
    fromMaybe,
    isJust,
    isNothing,
    mapMaybe,
    maybeToList,
  )
import Data.Serialize (Serialize (..))
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Text.Lazy qualified as LazyText
import Data.Text.Lazy.Encoding qualified as LazyText
import Data.Time (UTCTime (UTCTime), rfc822DateFormat)
import Data.Time.Clock.POSIX
  ( posixSecondsToUTCTime,
    utcTimeToPOSIXSeconds,
  )
import Data.Time.Format
  ( defaultTimeLocale,
    formatTime,
    parseTimeM,
  )
import Data.Time.Format.ISO8601
  ( iso8601ParseM,
    iso8601Show,
  )
import Data.Vector qualified as V
import Data.Word (Word32, Word64)
import GHC.Generics (Generic)
import Haskoin
import Web.Scotty.Trans (Parsable (..))

data DeriveType
  = DeriveNormal
  | DeriveP2SH
  | DeriveP2WPKH
  deriving (Show, Eq, Generic, NFData)

textToDeriveType :: Text -> Maybe DeriveType
textToDeriveType "normal" = Just DeriveNormal
textToDeriveType "compat" = Just DeriveP2SH
textToDeriveType "segwit" = Just DeriveP2WPKH
textToDeriveType _ = Nothing

deriveTypeToText :: DeriveType -> Text
deriveTypeToText DeriveNormal = "normal"
deriveTypeToText DeriveP2SH = "compat"
deriveTypeToText DeriveP2WPKH = "segwit"

instance Serial DeriveType where
  serialize DeriveNormal = putWord8 0x00
  serialize DeriveP2SH = putWord8 0x01
  serialize DeriveP2WPKH = putWord8 0x02

  deserialize =
    getWord8 >>= \case
      0x00 -> return DeriveNormal
      0x01 -> return DeriveP2SH
      0x02 -> return DeriveP2WPKH
      _ -> return DeriveNormal

instance Binary DeriveType where
  put = serialize
  get = deserialize

instance Serialize DeriveType where
  put = serialize
  get = deserialize

instance Default DeriveType where
  def = DeriveNormal

instance Parsable DeriveType where
  parseParam txt =
    case textToDeriveType (LazyText.toStrict txt) of
      Nothing -> Left "invalid derivation type"
      Just x -> Right x

data XPubSpec = XPubSpec
  { key :: !XPubKey,
    deriv :: !DeriveType
  }
  deriving (Show, Eq, Generic, NFData)

instance Hashable XPubSpec where
  hashWithSalt i = hashWithSalt i . (.key.key)

instance Serial XPubSpec where
  serialize XPubSpec {key = k, deriv = t} = do
    putWord8 k.depth
    serialize k.parent
    putWord32be k.index
    serialize k.chain
    putByteString k.key.get
    serialize t
  deserialize = do
    depth <- getWord8
    parent <- deserialize
    index <- getWord32be
    chain <- deserialize
    key <- PubKey <$> getByteString 64
    deriv <- deserialize
    return XPubSpec {key = XPubKey {..}, deriv}

instance Serialize XPubSpec where
  put = serialize
  get = deserialize

instance Binary XPubSpec where
  put = serialize
  get = deserialize

type UnixTime = Word64

type BlockPos = Word32

-- | Binary such that ordering is inverted.
putUnixTime :: (MonadPut m) => Word64 -> m ()
putUnixTime w = putWord64be $ maxBound - w

getUnixTime :: (MonadGet m) => m Word64
getUnixTime = (maxBound -) <$> getWord64be

-- | Reference to a block where a transaction is stored.
data BlockRef
  = BlockRef
      { -- | block height in the chain
        height :: !BlockHeight,
        -- | position of transaction within the block
        position :: !Word32
      }
  | MemRef
      { timestamp :: !UnixTime
      }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, NFData)

-- | Serial entities will sort in reverse order.
instance Serial BlockRef where
  serialize MemRef {timestamp = t} = do
    putWord8 0x00
    putUnixTime t
  serialize BlockRef {height = h, position = p} = do
    putWord8 0x01
    putWord32be (maxBound - h)
    putWord32be (maxBound - p)
  deserialize =
    getWord8 >>= \case
      0x00 -> getmemref
      0x01 -> getblockref
      _ -> fail "Cannot decode BlockRef"
    where
      getmemref = do
        MemRef <$> getUnixTime
      getblockref = do
        h <- (maxBound -) <$> getWord32be
        p <- (maxBound -) <$> getWord32be
        return BlockRef {height = h, position = p}

instance Serialize BlockRef where
  put = serialize
  get = deserialize

instance Binary BlockRef where
  put = serialize
  get = deserialize

confirmed :: BlockRef -> Bool
confirmed BlockRef {} = True
confirmed MemRef {} = False

instance ToJSON BlockRef where
  toJSON BlockRef {height = h, position = p} =
    A.object
      [ "height" .= h,
        "position" .= p
      ]
  toJSON MemRef {timestamp = t} =
    A.object ["mempool" .= t]
  toEncoding BlockRef {height = h, position = p} =
    A.pairs $
      mconcat
        [ "height" `A.pair` A.word32 h,
          "position" `A.pair` A.word32 p
        ]
  toEncoding MemRef {timestamp = t} =
    A.pairs $ "mempool" `A.pair` A.word64 t

instance FromJSON BlockRef where
  parseJSON =
    A.withObject "BlockRef" $ \o -> b o <|> m o
    where
      b o = do
        height <- o .: "height"
        position <- o .: "position"
        return BlockRef {..}
      m o =
        MemRef <$> o .: "mempool"

-- | Transaction in relation to an address.
data TxRef = TxRef
  { -- | block information
    block :: !BlockRef,
    -- | transaction hash
    txid :: !TxHash
  }
  deriving (Show, Eq, Ord, Generic, Hashable, NFData)

instance Serial TxRef where
  serialize (TxRef b h) = do
    serialize b
    serialize h

  deserialize =
    TxRef <$> deserialize <*> deserialize

instance Binary TxRef where
  put = serialize
  get = deserialize

instance Serialize TxRef where
  put = serialize
  get = deserialize

instance ToJSON TxRef where
  toJSON x =
    A.object
      [ "txid" .= x.txid,
        "block" .= x.block
      ]
  toEncoding btx =
    A.pairs $
      mconcat
        [ "txid" `A.pair` toEncoding btx.txid,
          "block" `A.pair` toEncoding btx.block
        ]

instance FromJSON TxRef where
  parseJSON =
    A.withObject "TxRef" $ \o -> do
      txid <- o .: "txid"
      block <- o .: "block"
      return TxRef {..}

-- | Address balance information.
data Balance = Balance
  { -- | address balance
    address :: !Address,
    -- | confirmed balance
    confirmed :: !Word64,
    -- | unconfirmed balance
    unconfirmed :: !Word64,
    -- | number of unspent outputs
    utxo :: !Word64,
    -- | number of transactions
    txs :: !Word64,
    -- | total amount from all outputs in this address
    received :: !Word64
  }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, NFData)

instance Serial Balance where
  serialize b = do
    serialize b.address
    putWord64be b.confirmed
    putWord64be b.unconfirmed
    putWord64be b.utxo
    putWord64be b.txs
    putWord64be b.received

  deserialize = do
    address <- deserialize
    confirmed <- getWord64be
    unconfirmed <- getWord64be
    utxo <- getWord64be
    txs <- getWord64be
    received <- getWord64be
    return Balance {..}

instance Binary Balance where
  put = serialize
  get = deserialize

instance Serialize Balance where
  put = serialize
  get = deserialize

zeroBalance :: Address -> Balance
zeroBalance a =
  Balance
    { address = a,
      confirmed = 0,
      unconfirmed = 0,
      utxo = 0,
      txs = 0,
      received = 0
    }

nullBalance :: Balance -> Bool
nullBalance
  Balance
    { confirmed = 0,
      unconfirmed = 0,
      utxo = 0,
      txs = 0,
      received = 0
    } = True
nullBalance _ = False

instance MarshalJSON Network Balance where
  marshalValue net b =
    A.object
      [ "address" .= marshalValue net b.address,
        "confirmed" .= b.confirmed,
        "unconfirmed" .= b.unconfirmed,
        "utxo" .= b.utxo,
        "txs" .= b.txs,
        "received" .= b.received
      ]

  marshalEncoding net b =
    A.pairs $
      mconcat
        [ "address" `A.pair` marshalEncoding net b.address,
          "confirmed" `A.pair` A.word64 b.confirmed,
          "unconfirmed" `A.pair` A.word64 b.unconfirmed,
          "utxo" `A.pair` A.word64 b.utxo,
          "txs" `A.pair` A.word64 b.txs,
          "received" `A.pair` A.word64 b.received
        ]

  unmarshalValue net =
    A.withObject "Balance" $ \o -> do
      confirmed <- o .: "confirmed"
      unconfirmed <- o .: "unconfirmed"
      utxo <- o .: "utxo"
      txs <- o .: "txs"
      received <- o .: "received"
      address <- unmarshalValue net =<< o .: "address"
      return Balance {..}

-- | Unspent output.
data Unspent = Unspent
  { block :: !BlockRef,
    outpoint :: !OutPoint,
    value :: !Word64,
    script :: !ByteString,
    address :: !(Maybe Address)
  }
  deriving (Show, Eq, Ord, Generic, Hashable, NFData)

instance Serial Unspent where
  serialize u = do
    serialize u.block
    serialize u.outpoint
    putWord64be u.value
    putLengthBytes u.script
    putMaybe serialize u.address

  deserialize = do
    block <- deserialize
    outpoint <- deserialize
    value <- getWord64be
    script <- getLengthBytes
    address <- getMaybe deserialize
    return Unspent {..}

instance Binary Unspent where
  put = serialize
  get = deserialize

instance Serialize Unspent where
  put = serialize
  get = deserialize

instance Coin Unspent where
  coinValue = (.value)

instance MarshalJSON Network Unspent where
  marshalValue net u =
    A.object
      [ "address" .= fmap (marshalValue net) u.address,
        "block" .= u.block,
        "txid" .= u.outpoint.hash,
        "index" .= u.outpoint.index,
        "pkscript" .= encodeHex u.script,
        "value" .= u.value
      ]

  marshalEncoding net u =
    A.pairs $
      mconcat
        [ "address" `A.pair` maybe A.null_ (marshalEncoding net) u.address,
          "block" `A.pair` toEncoding u.block,
          "txid" `A.pair` toEncoding u.outpoint.hash,
          "index" `A.pair` A.word32 u.outpoint.index,
          "pkscript" `A.pair` hexEncoding (B.fromStrict u.script),
          "value" `A.pair` A.word64 u.value
        ]

  unmarshalValue net =
    A.withObject "Unspent" $ \o -> do
      block <- o .: "block"
      hash <- o .: "txid"
      index <- o .: "index"
      value <- o .: "value"
      script <- o .: "pkscript" >>= jsonHex
      address <-
        o .: "address"
          >>= maybe (return Nothing) (optional . unmarshalValue net)
      return Unspent {outpoint = OutPoint {..}, ..}

-- | Database value for a block entry.
data BlockData = BlockData
  { -- | height of the block in the chain
    height :: !BlockHeight,
    -- | is this block in the main chain?
    main :: !Bool,
    -- | accumulated work in that block
    work :: !Integer,
    -- | block header
    header :: !BlockHeader,
    -- | size of the block including witnesses
    size :: !Word32,
    -- | weight of this block (for segwit networks)
    weight :: !Word32,
    -- | block transactions
    txs :: ![TxHash],
    -- | sum of all transaction outputs
    outputs :: !Word64,
    -- | sum of all transaction fee
    fee :: !Word64,
    -- | block subsidy
    subsidy :: !Word64
  }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, NFData)

instance Serial BlockData where
  serialize b = do
    putWord32be b.height
    serialize b.main
    putInteger b.work
    serialize b.header
    putWord32be b.size
    putWord32be b.weight
    putList serialize b.txs
    putWord64be b.outputs
    putWord64be b.fee
    putWord64be b.subsidy

  deserialize = do
    height <- getWord32be
    main <- deserialize
    work <- getInteger
    header <- deserialize
    size <- getWord32be
    weight <- getWord32be
    txs <- getList deserialize
    outputs <- getWord64be
    fee <- getWord64be
    subsidy <- getWord64be
    return BlockData {..}

instance Serialize BlockData where
  put = serialize
  get = deserialize

instance Binary BlockData where
  put = serialize
  get = deserialize

instance FromJSON BlockData where
  parseJSON =
    A.withObject "BlockData" $ \o -> do
      height <- o .: "height"
      main <- o .: "mainchain"
      prev <- o .: "previous"
      timestamp <- o .: "time"
      version <- o .: "version"
      bits <- o .: "bits"
      nonce <- o .: "nonce"
      size <- o .: "size"
      txs <- o .: "tx"
      TxHash merkle <- o .: "merkle"
      subsidy <- o .: "subsidy"
      fee <- o .: "fees"
      outputs <- o .: "outputs"
      work <- o .: "work"
      weight <- o .:? "weight" .!= 0
      return BlockData {header = BlockHeader {..}, ..}

instance MarshalJSON Network BlockData where
  marshalValue net b =
    A.object $
      [ "hash" .= headerHash b.header,
        "height" .= b.height,
        "mainchain" .= b.main,
        "previous" .= b.header.prev,
        "time" .= b.header.timestamp,
        "version" .= b.header.version,
        "bits" .= b.header.bits,
        "nonce" .= b.header.nonce,
        "size" .= b.size,
        "tx" .= b.txs,
        "merkle" .= TxHash b.header.merkle,
        "subsidy" .= b.subsidy,
        "fees" .= b.fee,
        "outputs" .= b.outputs,
        "work" .= b.work
      ]
        <> ["weight" .= b.weight | net.segWit]

  marshalEncoding net b =
    A.pairs $
      mconcat
        [ "hash" `A.pair` toEncoding (headerHash b.header),
          "height" `A.pair` A.word32 b.height,
          "mainchain" `A.pair` A.bool b.main,
          "previous" `A.pair` toEncoding b.header.prev,
          "time" `A.pair` A.word32 b.header.timestamp,
          "version" `A.pair` A.word32 b.header.version,
          "bits" `A.pair` A.word32 b.header.bits,
          "nonce" `A.pair` A.word32 b.header.nonce,
          "size" `A.pair` A.word32 b.size,
          "tx" `A.pair` A.list toEncoding b.txs,
          "merkle" `A.pair` toEncoding (TxHash b.header.merkle),
          "subsidy" `A.pair` A.word64 b.subsidy,
          "fees" `A.pair` A.word64 b.fee,
          "outputs" `A.pair` A.word64 b.outputs,
          "work" `A.pair` A.integer b.work,
          bool mempty ("weight" `A.pair` A.word32 b.weight) net.segWit
        ]

  unmarshalValue net = parseJSON

data StoreInput
  = StoreCoinbase
      { outpoint :: !OutPoint,
        sequence :: !Word32,
        script :: !ByteString,
        witness :: !WitnessStack
      }
  | StoreInput
      { outpoint :: !OutPoint,
        sequence :: !Word32,
        script :: !ByteString,
        pkscript :: !ByteString,
        value :: !Word64,
        witness :: !WitnessStack,
        address :: !(Maybe Address)
      }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, NFData)

instance Serial StoreInput where
  serialize StoreCoinbase {..} = do
    putWord8 0x00
    serialize outpoint
    putWord32be sequence
    putLengthBytes script
    putList putLengthBytes witness
  serialize StoreInput {..} = do
    putWord8 0x01
    serialize outpoint
    putWord32be sequence
    putLengthBytes script
    putLengthBytes pkscript
    putWord64be value
    putList putLengthBytes witness
    putMaybe serialize address

  deserialize =
    getWord8 >>= \case
      0x00 -> do
        outpoint <- deserialize
        sequence <- getWord32be
        script <- getLengthBytes
        witness <- getList getLengthBytes
        return StoreCoinbase {..}
      0x01 -> do
        outpoint <- deserialize
        sequence <- getWord32be
        script <- getLengthBytes
        pkscript <- getLengthBytes
        value <- getWord64be
        witness <- getList getLengthBytes
        address <- getMaybe deserialize
        return StoreInput {..}
      x -> fail $ "Unknown input id: " <> cs (show x)

instance Serialize StoreInput where
  put = serialize
  get = deserialize

instance Binary StoreInput where
  put = serialize
  get = deserialize

isCoinbaseTx :: Tx -> Bool
isCoinbaseTx = all ((== nullOutPoint) . (.outpoint)) . (.inputs)

isCoinbase :: StoreInput -> Bool
isCoinbase StoreCoinbase {} = True
isCoinbase StoreInput {} = False

instance MarshalJSON Network StoreInput where
  marshalValue net StoreInput {..} =
    A.object
      [ "coinbase" .= False,
        "txid" .= outpoint.hash,
        "output" .= outpoint.index,
        "sigscript" .= String (encodeHex script),
        "sequence" .= sequence,
        "pkscript" .= String (encodeHex pkscript),
        "value" .= value,
        "address" .= (marshalValue net <$> address),
        "witness" .= map encodeHex witness
      ]
  marshalValue net StoreCoinbase {..} =
    A.object
      [ "coinbase" .= True,
        "txid" .= outpoint.hash,
        "output" .= outpoint.index,
        "sigscript" .= String (encodeHex script),
        "sequence" .= sequence,
        "pkscript" .= Null,
        "value" .= Null,
        "address" .= Null,
        "witness" .= map encodeHex witness
      ]

  marshalEncoding net StoreInput {..} =
    A.pairs $
      mconcat
        [ "coinbase" `A.pair` A.bool False,
          "txid" `A.pair` toEncoding outpoint.hash,
          "output" `A.pair` A.word32 outpoint.index,
          "sigscript" `A.pair` hexEncoding (B.fromStrict script),
          "sequence" `A.pair` A.word32 sequence,
          "pkscript" `A.pair` hexEncoding (B.fromStrict pkscript),
          "value" `A.pair` A.word64 value,
          "address" `A.pair` maybe A.null_ (marshalEncoding net) address,
          "witness" `A.pair` A.list (hexEncoding . B.fromStrict) witness
        ]
  marshalEncoding net StoreCoinbase {..} =
    A.pairs $
      mconcat
        [ "coinbase" .= True,
          "txid" `A.pair` toEncoding outpoint.hash,
          "output" `A.pair` A.word32 outpoint.index,
          "sigscript" `A.pair` hexEncoding (B.fromStrict script),
          "sequence" `A.pair` A.word32 sequence,
          "pkscript" `A.pair` A.null_,
          "value" `A.pair` A.null_,
          "address" `A.pair` A.null_,
          "witness" `A.pair` A.list (hexEncoding . B.fromStrict) witness
        ]

  unmarshalValue net =
    A.withObject "StoreInput" $ \o -> do
      coinbase <- o .: "coinbase"
      outpoint <- OutPoint <$> o .: "txid" <*> o .: "output"
      sequence <- o .: "sequence"
      witness <- mapM jsonHex =<< o .:? "witness" .!= []
      script <- o .: "sigscript" >>= jsonHex
      if coinbase
        then return StoreCoinbase {..}
        else do
          pkscript <- o .: "pkscript" >>= jsonHex
          value <- o .: "value"
          address <-
            o .: "address"
              >>= maybe (return Nothing) (optional . unmarshalValue net)
          return StoreInput {..}

jsonHex :: Text -> Parser ByteString
jsonHex s =
  case decodeHex s of
    Nothing -> fail "Could not decode hex"
    Just b -> return b

-- | Information about input spending output.
data Spender = Spender
  { -- | input transaction hash
    txid :: !TxHash,
    -- | input position in transaction
    index :: !Word32
  }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, NFData)

instance Serial Spender where
  serialize s = do
    serialize s.txid
    putWord32be s.index
  deserialize = Spender <$> deserialize <*> getWord32be

instance Serialize Spender where
  put = serialize
  get = deserialize

instance Binary Spender where
  put = serialize
  get = deserialize

instance ToJSON Spender where
  toJSON s =
    A.object
      [ "txid" .= s.txid,
        "input" .= s.index
      ]
  toEncoding s =
    A.pairs $
      mconcat
        [ "txid" `A.pair` toEncoding s.txid,
          "input" `A.pair` A.word32 s.index
        ]

instance FromJSON Spender where
  parseJSON =
    A.withObject "Spender" $ \o ->
      Spender <$> o .: "txid" <*> o .: "input"

-- | Output information.
data StoreOutput = StoreOutput
  { value :: !Word64,
    script :: !ByteString,
    spender :: !(Maybe Spender),
    address :: !(Maybe Address)
  }
  deriving (Show, Read, Eq, Ord, Generic, Hashable, NFData)

instance Serial StoreOutput where
  serialize o = do
    putWord64be o.value
    putLengthBytes o.script
    putMaybe serialize o.spender
    putMaybe serialize o.address
  deserialize = do
    value <- getWord64be
    script <- getLengthBytes
    spender <- getMaybe deserialize
    address <- getMaybe deserialize
    return StoreOutput {..}

instance Serialize StoreOutput where
  put = serialize
  get = deserialize

instance Binary StoreOutput where
  put = serialize
  get = deserialize

instance MarshalJSON Network StoreOutput where
  marshalValue net o =
    A.object
      [ "address" .= (marshalValue net <$> o.address),
        "pkscript" .= encodeHex o.script,
        "value" .= o.value,
        "spent" .= isJust o.spender,
        "spender" .= o.spender
      ]

  marshalEncoding net o =
    A.pairs $
      mconcat
        [ "address" `A.pair` maybe A.null_ (marshalEncoding net) o.address,
          "pkscript" `A.pair` hexEncoding (B.fromStrict o.script),
          "value" `A.pair` A.word64 o.value,
          "spent" `A.pair` A.bool (isJust o.spender),
          "spender" `A.pair` toEncoding o.spender
        ]

  unmarshalValue net =
    A.withObject "StoreOutput" $ \o -> do
      value <- o .: "value"
      script <- o .: "pkscript" >>= jsonHex
      spender <- o .: "spender"
      address <-
        o .: "address"
          >>= maybe (return Nothing) (optional . unmarshalValue net)
      return StoreOutput {..}

data Prev = Prev
  { script :: !ByteString,
    value :: !Word64
  }
  deriving (Show, Eq, Ord, Generic, Hashable, NFData)

instance Serial Prev where
  serialize p = do
    putLengthBytes p.script
    putWord64be p.value
  deserialize = do
    script <- getLengthBytes
    value <- getWord64be
    return Prev {..}

instance Binary Prev where
  put = serialize
  get = deserialize

instance Serialize Prev where
  put = serialize
  get = deserialize

toInput :: Ctx -> TxIn -> Maybe Prev -> WitnessStack -> StoreInput
toInput ctx i Nothing w =
  StoreCoinbase
    { outpoint = i.outpoint,
      sequence = i.sequence,
      script = i.script,
      witness = w
    }
toInput ctx i (Just p) w =
  StoreInput
    { outpoint = i.outpoint,
      sequence = i.sequence,
      script = i.script,
      pkscript = p.script,
      value = p.value,
      witness = w,
      address = eitherToMaybe (scriptToAddressBS ctx p.script)
    }

toOutput :: Ctx -> TxOut -> Maybe Spender -> StoreOutput
toOutput ctx o s =
  StoreOutput
    { value = o.value,
      script = o.script,
      spender = s,
      address = eitherToMaybe (scriptToAddressBS ctx o.script)
    }

data TxData = TxData
  { block :: !BlockRef,
    tx :: !Tx,
    prevs :: !(IntMap Prev),
    deleted :: !Bool,
    rbf :: !Bool,
    timestamp :: !Word64,
    spenders :: !(IntMap Spender)
  }
  deriving (Show, Eq, Ord, Generic, NFData)

instance Serial TxData where
  serialize t = do
    serialize t.block
    serialize t.tx
    putIntMap (putWord64be . fromIntegral) serialize t.prevs
    serialize t.deleted
    serialize t.rbf
    putWord64be t.timestamp
    putIntMap (putWord64be . fromIntegral) serialize t.spenders
  deserialize = do
    block <- deserialize
    tx <- deserialize
    prevs <- getIntMap (fromIntegral <$> getWord64be) deserialize
    deleted <- deserialize
    rbf <- deserialize
    timestamp <- getWord64be
    spenders <- getIntMap (fromIntegral <$> getWord64be) deserialize
    return TxData {..}

instance Serialize TxData where
  put = serialize
  get = deserialize

instance Binary TxData where
  put = serialize
  get = deserialize

txDataFee :: TxData -> Word64
txDataFee t =
  if isCoinbaseTx t.tx
    then 0
    else inputs - outputs
  where
    inputs = sum $ map (.value) $ IntMap.elems t.prevs
    outputs = sum $ map (.value) t.tx.outputs

toTransaction :: Ctx -> TxData -> Transaction
toTransaction ctx t =
  Transaction
    { version = t.tx.version,
      locktime = t.tx.locktime,
      block = t.block,
      deleted = t.deleted,
      rbf = t.rbf,
      timestamp = t.timestamp,
      txid = txHash t.tx,
      inputs = ins,
      outputs = outs,
      size =
        fromIntegral $
          B.length $
            runPutS $
              serialize t.tx,
      weight =
        let b = B.length $ runPutS $ serialize witless
            x = B.length $ runPutS $ serialize t.tx
         in fromIntegral $ b * 3 + x,
      fee =
        if any isCoinbase ins
          then 0
          else inv - outv
    }
  where
    ins = zipWith f [0 ..] t.tx.inputs
    witless =
      let Tx {..} = t.tx
       in Tx {witness = [], ..}
    inv = sum (map (.value) ins)
    outs = zipWith g [0 ..] t.tx.outputs
    outv = sum $ map (.value) outs
    ws = take (length t.tx.inputs) $ t.tx.witness <> repeat []
    f n i = toInput ctx i (IntMap.lookup n t.prevs) (ws !! n)
    g n o = toOutput ctx o $ IntMap.lookup n t.spenders

fromTransaction :: Transaction -> TxData
fromTransaction t =
  TxData
    { block = t.block,
      tx = tx,
      deleted = t.deleted,
      rbf = t.rbf,
      timestamp = t.timestamp,
      prevs =
        IntMap.fromList $
          catMaybes $
            zipWith f [0 ..] t.inputs,
      spenders =
        IntMap.fromList $
          catMaybes $
            zipWith g [0 ..] t.outputs
    }
  where
    tx = transactionData t
    f _ StoreCoinbase {} = Nothing
    f n StoreInput {script = s, value = v} =
      Just (n, Prev {script = s, value = v})
    g _ StoreOutput {spender = Nothing} = Nothing
    g n StoreOutput {spender = Just s} = Just (n, s)

-- | Detailed transaction information.
data Transaction = Transaction
  { -- | block information for this transaction
    block :: !BlockRef,
    -- | transaction version
    version :: !Word32,
    -- | lock time
    locktime :: !Word32,
    -- | transaction inputs
    inputs :: ![StoreInput],
    -- | transaction outputs
    outputs :: ![StoreOutput],
    -- | this transaction has been deleted and is no longer valid
    deleted :: !Bool,
    -- | this transaction can be replaced in the mempool
    rbf :: !Bool,
    -- | time the transaction was first seen or time of block
    timestamp :: !Word64,
    -- | transaction id
    txid :: !TxHash,
    -- | serialized transaction size (includes witness data)
    size :: !Word32,
    -- | transaction weight
    weight :: !Word32,
    -- | fees that this transaction pays (0 for coinbase)
    fee :: !Word64
  }
  deriving (Show, Eq, Ord, Generic, Hashable, NFData)

instance Serial Transaction where
  serialize t = do
    serialize t.block
    putWord32be t.version
    putWord32be t.locktime
    putList serialize t.inputs
    putList serialize t.outputs
    serialize t.deleted
    serialize t.rbf
    putWord64be t.timestamp
    serialize t.txid
    putWord32be t.size
    putWord32be t.weight
    putWord64be t.fee
  deserialize = do
    block <- deserialize
    version <- getWord32be
    locktime <- getWord32be
    inputs <- getList deserialize
    outputs <- getList deserialize
    deleted <- deserialize
    rbf <- deserialize
    timestamp <- getWord64be
    txid <- deserialize
    size <- getWord32be
    weight <- getWord32be
    fee <- getWord64be
    return Transaction {..}

instance Serialize Transaction where
  put = serialize
  get = deserialize

instance Binary Transaction where
  put = serialize
  get = deserialize

transactionData :: Transaction -> Tx
transactionData t =
  Tx
    { inputs = map i t.inputs,
      outputs = map o t.outputs,
      version = t.version,
      locktime = t.locktime,
      witness = w $ map (.witness) t.inputs
    }
  where
    i StoreCoinbase {..} = TxIn {..}
    i StoreInput {..} = TxIn {..}
    o StoreOutput {..} = TxOut {..}
    w xs
      | all null xs = []
      | otherwise = xs

instance MarshalJSON Network Transaction where
  marshalValue net t =
    A.object
      [ "txid" .= t.txid,
        "size" .= t.size,
        "version" .= t.version,
        "locktime" .= t.locktime,
        "fee" .= t.fee,
        "inputs" .= map (marshalValue net) t.inputs,
        "outputs" .= map (marshalValue net) t.outputs,
        "block" .= t.block,
        "deleted" .= t.deleted,
        "time" .= t.timestamp,
        "rbf" .= t.rbf,
        "weight" .= t.weight
      ]

  marshalEncoding net t =
    A.pairs $
      mconcat
        [ "txid" `A.pair` toEncoding t.txid,
          "size" `A.pair` A.word32 t.size,
          "version" `A.pair` A.word32 t.version,
          "locktime" `A.pair` A.word32 t.locktime,
          "fee" `A.pair` A.word64 t.fee,
          "inputs" `A.pair` A.list (marshalEncoding net) t.inputs,
          "outputs" `A.pair` A.list (marshalEncoding net) t.outputs,
          "block" `A.pair` toEncoding t.block,
          "deleted" `A.pair` A.bool t.deleted,
          "time" `A.pair` A.word64 t.timestamp,
          "rbf" `A.pair` A.bool t.rbf,
          "weight" `A.pair` A.word32 t.weight
        ]

  unmarshalValue net = A.withObject "Transaction" $ \o -> do
    version <- o .: "version"
    locktime <- o .: "locktime"
    inputs <- o .: "inputs" >>= mapM (unmarshalValue net)
    outputs <- o .: "outputs" >>= mapM (unmarshalValue net)
    block <- o .: "block"
    deleted <- o .: "deleted"
    timestamp <- o .: "time"
    rbf <- o .:? "rbf" .!= False
    weight <- o .:? "weight" .!= 0
    size <- o .: "size"
    txid <- o .: "txid"
    fee <- o .: "fee"
    return Transaction {..}

-- | Information about a connected peer.
data PeerInfo = PeerInfo
  { -- | user agent string
    userAgent :: !ByteString,
    -- | network address
    address :: !String,
    -- | version number
    version :: !Word32,
    -- | services field
    services :: !Word64,
    -- | will relay transactions
    relay :: !Bool
  }
  deriving (Show, Eq, Ord, Generic, NFData)

instance Serial PeerInfo where
  serialize p = do
    putLengthBytes p.userAgent
    putLengthBytes $ T.encodeUtf8 $ T.pack p.address
    putWord32be p.version
    putWord64be p.services
    serialize p.relay
  deserialize = do
    userAgent <- getLengthBytes
    address <- T.unpack . T.decodeUtf8 <$> getLengthBytes
    version <- getWord32be
    services <- getWord64be
    relay <- deserialize
    return PeerInfo {..}

instance Serialize PeerInfo where
  put = serialize
  get = deserialize

instance Binary PeerInfo where
  put = serialize
  get = deserialize

instance ToJSON PeerInfo where
  toJSON p =
    A.object
      [ "useragent" .= String (T.decodeUtf8 p.userAgent),
        "address" .= p.address,
        "version" .= p.version,
        "services"
          .= String (encodeHex $ runPutS $ serialize p.services),
        "relay" .= p.relay
      ]
  toEncoding p =
    A.pairs $
      mconcat
        [ "useragent" `A.pair` A.text (T.decodeUtf8 p.userAgent),
          "address" `A.pair` toEncoding p.address,
          "version" `A.pair` A.word32 p.version,
          "services" `A.pair` hexEncoding (runPutL $ serialize p.services),
          "relay" `A.pair` A.bool p.relay
        ]

instance FromJSON PeerInfo where
  parseJSON =
    A.withObject "PeerInfo" $ \o -> do
      String userAgentText <- o .: "useragent"
      let userAgent = T.encodeUtf8 userAgentText
      address <- o .: "address"
      version <- o .: "version"
      services <-
        o .: "services" >>= jsonHex >>= \b ->
          case runGetS deserialize b of
            Left e -> fail $ "Could not decode services: " <> e
            Right s -> return s
      relay <- o .: "relay"
      return PeerInfo {..}

-- | Address balances for an extended public key.
data XPubBal = XPubBal
  { path :: ![KeyIndex],
    balance :: !Balance
  }
  deriving (Show, Ord, Eq, Generic, NFData)

instance Serial XPubBal where
  serialize b = do
    putList putWord32be b.path
    serialize b.balance
  deserialize = do
    path <- getList getWord32be
    balance <- deserialize
    return XPubBal {..}

instance Serialize XPubBal where
  put = serialize
  get = deserialize

instance Binary XPubBal where
  put = serialize
  get = deserialize

instance MarshalJSON Network XPubBal where
  marshalValue net b =
    A.object
      [ "path" .= b.path,
        "balance" .= marshalValue net b.balance
      ]

  marshalEncoding net b =
    A.pairs $
      mconcat
        [ "path" `A.pair` A.list A.word32 b.path,
          "balance" `A.pair` marshalEncoding net b.balance
        ]

  unmarshalValue net =
    A.withObject "XPubBal" $ \o -> do
      path <- o .: "path"
      balance <- unmarshalValue net =<< o .: "balance"
      return XPubBal {..}

-- | Unspent transaction for extended public key.
data XPubUnspent = XPubUnspent
  { unspent :: !Unspent,
    path :: ![KeyIndex]
  }
  deriving (Show, Eq, Ord, Generic, NFData)

instance Serial XPubUnspent where
  serialize u = do
    putList putWord32be u.path
    serialize u.unspent
  deserialize = do
    path <- getList getWord32be
    unspent <- deserialize
    return XPubUnspent {..}

instance Serialize XPubUnspent where
  put = serialize
  get = deserialize

instance Binary XPubUnspent where
  put = serialize
  get = deserialize

instance MarshalJSON Network XPubUnspent where
  marshalValue net u =
    A.object
      [ "unspent" .= marshalValue net u.unspent,
        "path" .= u.path
      ]

  marshalEncoding net u =
    A.pairs $
      mconcat
        [ "unspent" `A.pair` marshalEncoding net u.unspent,
          "path" `A.pair` A.list A.word32 u.path
        ]

  unmarshalValue net =
    A.withObject "XPubUnspent" $ \o -> do
      unspent <- o .: "unspent" >>= unmarshalValue net
      path <- o .: "path"
      return XPubUnspent {..}

data XPubSummary = XPubSummary
  { confirmed :: !Word64,
    unconfirmed :: !Word64,
    received :: !Word64,
    utxo :: !Word64,
    external :: !Word32,
    change :: !Word32
  }
  deriving (Eq, Show, Generic, NFData)

instance Serial XPubSummary where
  serialize s = do
    putWord64be s.confirmed
    putWord64be s.unconfirmed
    putWord64be s.received
    putWord64be s.utxo
    putWord32be s.external
    putWord32be s.change
  deserialize = do
    confirmed <- getWord64be
    unconfirmed <- getWord64be
    received <- getWord64be
    utxo <- getWord64be
    external <- getWord32be
    change <- getWord32be
    return XPubSummary {..}

instance Binary XPubSummary where
  put = serialize
  get = deserialize

instance Serialize XPubSummary where
  put = serialize
  get = deserialize

instance ToJSON XPubSummary where
  toJSON s =
    A.object
      [ "balance"
          .= A.object
            [ "confirmed" .= s.confirmed,
              "unconfirmed" .= s.unconfirmed,
              "received" .= s.received,
              "utxo" .= s.utxo
            ],
        "indices"
          .= A.object
            [ "change" .= s.change,
              "external" .= s.external
            ]
      ]
  toEncoding s =
    A.pairs $
      mconcat
        [ A.pair "balance" $
            A.pairs $
              mconcat
                [ "confirmed" `A.pair` A.word64 s.confirmed,
                  "unconfirmed" `A.pair` A.word64 s.unconfirmed,
                  "received" `A.pair` A.word64 s.received,
                  "utxo" `A.pair` A.word64 s.utxo
                ],
          A.pair "indices" $
            A.pairs $
              mconcat
                [ "change" `A.pair` A.word32 s.change,
                  "external" `A.pair` A.word32 s.external
                ]
        ]

instance FromJSON XPubSummary where
  parseJSON =
    A.withObject "XPubSummary" $ \o -> do
      b <- o .: "balance"
      i <- o .: "indices"
      confirmed <- b .: "confirmed"
      unconfirmed <- b .: "unconfirmed"
      received <- b .: "received"
      utxo <- b .: "utxo"
      change <- i .: "change"
      external <- i .: "external"
      return XPubSummary {..}

class Healthy a where
  isOK :: a -> Bool

data BlockHealth = BlockHealth
  { headers :: !BlockHeight,
    blocks :: !BlockHeight,
    max :: !Int32
  }
  deriving (Show, Eq, Generic, NFData)

instance Serial BlockHealth where
  serialize h = do
    serialize (isOK h)
    putWord32be h.headers
    putWord32be h.blocks
    putInt32be h.max
  deserialize = do
    k <- deserialize
    headers <- getWord32be
    blocks <- getWord32be
    max <- getInt32be
    let h = BlockHealth {..}
    unless (k == isOK h) $ fail "Inconsistent health check"
    return h

instance Serialize BlockHealth where
  put = serialize
  get = deserialize

instance Binary BlockHealth where
  put = serialize
  get = deserialize

instance Healthy BlockHealth where
  isOK x =
    h - b <= x.max
    where
      h = fromIntegral x.headers
      b = fromIntegral x.blocks

instance ToJSON BlockHealth where
  toJSON h =
    A.object
      [ "headers" .= h.headers,
        "blocks" .= h.blocks,
        "diff" .= diff,
        "max" .= h.max,
        "ok" .= isOK h
      ]
    where
      diff = toInteger h.headers - toInteger h.blocks

instance FromJSON BlockHealth where
  parseJSON =
    A.withObject "BlockHealth" $ \o -> do
      headers <- o .: "headers"
      blocks <- o .: "blocks"
      max <- o .: "max"
      return BlockHealth {..}

data TimeHealth = TimeHealth
  { age :: !Int64,
    max :: !Int64
  }
  deriving (Show, Eq, Generic, NFData)

instance Serial TimeHealth where
  serialize h = do
    serialize (isOK h)
    putInt64be h.age
    putInt64be h.max
  deserialize = do
    k <- deserialize
    age <- getInt64be
    max <- getInt64be
    let t = TimeHealth {..}
    unless (k == isOK t) $ fail "Inconsistent health check"
    return t

instance Binary TimeHealth where
  put = serialize
  get = deserialize

instance Serialize TimeHealth where
  put = serialize
  get = deserialize

instance Healthy TimeHealth where
  isOK TimeHealth {..} =
    age <= max

instance ToJSON TimeHealth where
  toJSON h =
    A.object
      [ "age" .= h.age,
        "max" .= h.max,
        "ok" .= isOK h
      ]

instance FromJSON TimeHealth where
  parseJSON =
    A.withObject "TimeHealth" $ \o -> do
      age <- o .: "age"
      max <- o .: "max"
      return TimeHealth {..}

data CountHealth = CountHealth
  { count :: !Int64,
    min :: !Int64
  }
  deriving (Show, Eq, Generic, NFData)

instance Serial CountHealth where
  serialize h = do
    serialize (isOK h)
    putInt64be h.count
    putInt64be h.min
  deserialize = do
    k <- deserialize
    count <- getInt64be
    min <- getInt64be
    let c = CountHealth {..}
    unless (k == isOK c) $ fail "Inconsistent health check"
    return c

instance Serialize CountHealth where
  put = serialize
  get = deserialize

instance Binary CountHealth where
  put = serialize
  get = deserialize

instance Healthy CountHealth where
  isOK CountHealth {..} = min <= count

instance ToJSON CountHealth where
  toJSON h =
    A.object
      [ "count" .= h.count,
        "min" .= h.min,
        "ok" .= isOK h
      ]

instance FromJSON CountHealth where
  parseJSON =
    A.withObject "CountHealth" $ \o -> do
      count <- o .: "count"
      min <- o .: "min"
      return CountHealth {..}

data MaxHealth = MaxHealth
  { count :: !Int64,
    max :: !Int64
  }
  deriving (Show, Eq, Generic, NFData)

instance Serial MaxHealth where
  serialize h = do
    serialize $ isOK h
    putInt64be h.count
    putInt64be h.max
  deserialize = do
    k <- deserialize
    count <- getInt64be
    max <- getInt64be
    let h = MaxHealth {..}
    unless (k == isOK h) $ fail "Inconsistent health check"
    return h

instance Binary MaxHealth where
  put = serialize
  get = deserialize

instance Serialize MaxHealth where
  put = serialize
  get = deserialize

instance Healthy MaxHealth where
  isOK MaxHealth {..} = count <= max

instance ToJSON MaxHealth where
  toJSON h =
    A.object
      [ "count" .= h.count,
        "max" .= h.max,
        "ok" .= isOK h
      ]

instance FromJSON MaxHealth where
  parseJSON =
    A.withObject "MaxHealth" $ \o -> do
      count <- o .: "count"
      max <- o .: "max"
      return MaxHealth {..}

data HealthCheck = HealthCheck
  { blocks :: !BlockHealth,
    lastBlock :: !TimeHealth,
    lastTx :: !TimeHealth,
    pendingTxs :: !MaxHealth,
    peers :: !CountHealth,
    network :: !String,
    version :: !String,
    time :: !Word64
  }
  deriving (Show, Eq, Generic, NFData)

instance Serial HealthCheck where
  serialize h = do
    serialize $ isOK h
    serialize h.blocks
    serialize h.lastBlock
    serialize h.lastTx
    serialize h.pendingTxs
    serialize h.peers
    putLengthBytes $ T.encodeUtf8 $ T.pack h.network
    putLengthBytes $ T.encodeUtf8 $ T.pack h.version
    putWord64be h.time
  deserialize = do
    k <- deserialize
    blocks <- deserialize
    lastBlock <- deserialize
    lastTx <- deserialize
    pendingTxs <- deserialize
    peers <- deserialize
    network <- T.unpack . T.decodeUtf8 <$> getLengthBytes
    version <- T.unpack . T.decodeUtf8 <$> getLengthBytes
    time <- getWord64be
    let h = HealthCheck {..}
    unless (k == isOK h) $ fail "Inconsistent health check"
    return h

instance Binary HealthCheck where
  put = serialize
  get = deserialize

instance Serialize HealthCheck where
  put = serialize
  get = deserialize

instance Healthy HealthCheck where
  isOK h =
    and
      [ isOK h.blocks,
        isOK h.lastBlock,
        isOK h.lastTx,
        isOK h.pendingTxs,
        isOK h.peers
      ]

instance ToJSON HealthCheck where
  toJSON h =
    A.object
      [ "blocks" .= h.blocks,
        "last-block" .= h.lastBlock,
        "last-tx" .= h.lastTx,
        "pending-txs" .= h.pendingTxs,
        "peers" .= h.peers,
        "net" .= h.network,
        "version" .= h.version,
        "time" .= show (posixSecondsToUTCTime $ fromIntegral h.time),
        "ok" .= isOK h
      ]

instance FromJSON HealthCheck where
  parseJSON =
    A.withObject "HealthCheck" $ \o -> do
      blocks <- o .: "blocks"
      lastBlock <- o .: "last-block"
      lastTx <- o .: "last-tx"
      pendingTxs <- o .: "pending-txs"
      peers <- o .: "peers"
      network <- o .: "net"
      version <- o .: "version"
      utcTime <- read <$> o .: "time"
      let time = round $ utcTimeToPOSIXSeconds utcTime
      return HealthCheck {..}

data Event
  = EventBlock !BlockHash
  | EventTx !TxHash
  deriving (Show, Eq, Generic, NFData)

instance Serial Event where
  serialize (EventBlock bh) = putWord8 0x00 >> serialize bh
  serialize (EventTx th) = putWord8 0x01 >> serialize th
  deserialize =
    getWord8 >>= \case
      0x00 -> EventBlock <$> deserialize
      0x01 -> EventTx <$> deserialize
      _ -> fail "Not an Event"

instance Serialize Event where
  put = serialize
  get = deserialize

instance Binary Event where
  put = serialize
  get = deserialize

instance ToJSON Event where
  toJSON (EventTx h) =
    A.object ["type" .= String "tx", "id" .= h]
  toJSON (EventBlock h) =
    A.object ["type" .= String "block", "id" .= h]
  toEncoding (EventTx h) =
    A.pairs ("type" `A.pair` A.text "tx" <> "id" `A.pair` toEncoding h)
  toEncoding (EventBlock h) =
    A.pairs ("type" `A.pair` A.text "block" <> "id" `A.pair` toEncoding h)

instance FromJSON Event where
  parseJSON =
    A.withObject "event" $ \o -> do
      t <- o .: "type"
      case t of
        "tx" -> do
          i <- o .: "id"
          return $ EventTx i
        "block" -> do
          i <- o .: "id"
          return $ EventBlock i
        _ -> fail $ "Could not recognize event type: " <> t

newtype GenericResult a = GenericResult {get :: a}
  deriving (Show, Eq, Generic, NFData)

instance (Serial a) => Serial (GenericResult a) where
  serialize (GenericResult x) = serialize x
  deserialize = GenericResult <$> deserialize

instance (Serial a) => Serialize (GenericResult a) where
  put = serialize
  get = deserialize

instance (Serial a) => Binary (GenericResult a) where
  put = serialize
  get = deserialize

instance (ToJSON a) => ToJSON (GenericResult a) where
  toJSON (GenericResult b) = A.object ["result" .= b]
  toEncoding (GenericResult b) = A.pairs ("result" `A.pair` toEncoding b)

instance (FromJSON a) => FromJSON (GenericResult a) where
  parseJSON =
    A.withObject "GenericResult" $ \o -> GenericResult <$> o .: "result"

newtype RawResult a = RawResult {get :: a}
  deriving (Show, Eq, Generic, NFData)

instance (Serial a) => Serial (RawResult a) where
  serialize (RawResult x) = serialize x
  deserialize = RawResult <$> deserialize

instance (Serial a) => Serialize (RawResult a) where
  put = serialize
  get = deserialize

instance (Serial a) => Binary (RawResult a) where
  put = serialize
  get = deserialize

instance (Serial a) => ToJSON (RawResult a) where
  toJSON (RawResult b) =
    A.object ["result" .= String (encodeHex $ runPutS $ serialize b)]
  toEncoding (RawResult b) =
    A.pairs $ "result" `A.pair` hexEncoding (runPutL $ serialize b)

instance (Serial a) => FromJSON (RawResult a) where
  parseJSON =
    A.withObject "RawResult" $ \o -> do
      res <- o .: "result"
      let m =
            eitherToMaybe . Bytes.Get.runGetS deserialize
              =<< decodeHex res
      maybe mzero (return . RawResult) m

newtype SerialList a = SerialList {get :: [a]}
  deriving (Show, Eq, Generic, NFData)

instance Semigroup (SerialList a) where
  SerialList a <> SerialList b = SerialList (a <> b)

instance Monoid (SerialList a) where
  mempty = SerialList mempty

instance (Serial a) => Serial (SerialList a) where
  serialize (SerialList ls) = putList serialize ls
  deserialize = SerialList <$> getList deserialize

instance (ToJSON a) => ToJSON (SerialList a) where
  toJSON (SerialList ls) = toJSON ls
  toEncoding (SerialList ls) = A.list toEncoding ls

instance (FromJSON a) => FromJSON (SerialList a) where
  parseJSON = fmap SerialList . parseJSON

newtype RawResultList a = RawResultList {get :: [a]}
  deriving (Show, Eq, Generic, NFData)

instance (Serial a) => Serial (RawResultList a) where
  serialize (RawResultList xs) =
    mapM_ serialize xs
  deserialize = RawResultList <$> go
    where
      go =
        isEmpty >>= \case
          True -> return []
          False -> (:) <$> deserialize <*> go

instance (Serial a) => Serialize (RawResultList a) where
  put = serialize
  get = deserialize

instance (Serial a) => Binary (RawResultList a) where
  put = serialize
  get = deserialize

instance Semigroup (RawResultList a) where
  (RawResultList a) <> (RawResultList b) = RawResultList $ a <> b

instance Monoid (RawResultList a) where
  mempty = RawResultList mempty

instance (Serial a) => ToJSON (RawResultList a) where
  toJSON (RawResultList xs) =
    toJSON $ String . encodeHex . runPutS . serialize <$> xs
  toEncoding (RawResultList xs) =
    A.list (hexEncoding . runPutL . serialize) xs

instance (Serial a) => FromJSON (RawResultList a) where
  parseJSON =
    A.withArray "RawResultList" $ \vec ->
      RawResultList <$> mapM parseElem (toList vec)
    where
      deser = eitherToMaybe . runGetS deserialize
      parseElem =
        A.withText "RawResultListItem" $ \t ->
          maybe mzero return (decodeHex t >>= deser)

newtype TxId
  = TxId TxHash
  deriving (Show, Eq, Generic, NFData)

instance Serial TxId where
  serialize (TxId h) = serialize h
  deserialize = TxId <$> deserialize

instance Serialize TxId where
  put = serialize
  get = deserialize

instance Binary TxId where
  put = serialize
  get = deserialize

instance ToJSON TxId where
  toJSON (TxId h) = A.object ["txid" .= h]
  toEncoding (TxId h) = A.pairs ("txid" `A.pair` toEncoding h)

instance FromJSON TxId where
  parseJSON = A.withObject "txid" $ \o -> TxId <$> o .: "txid"

data Except
  = ThingNotFound
  | ServerError
  | BadRequest
  | UserError !String
  | StringError !String
  | TxIndexConflict ![TxHash]
  | ServerTimeout
  | RequestTooLarge
  deriving (Show, Eq, Ord, Generic, NFData)

instance Exception Except

instance ToJSON Except where
  toJSON e =
    A.object $
      case e of
        ThingNotFound ->
          [ "error" .= String "not-found-or-invalid-arg",
            "message" .= String "Item not found or argument invalid"
          ]
        ServerError ->
          [ "error" .= String "server-error",
            "message" .= String "Server error"
          ]
        BadRequest ->
          [ "error" .= String "bad-request",
            "message" .= String "Invalid request"
          ]
        UserError msg' ->
          [ "error" .= String "user-error",
            "message" .= String (cs msg')
          ]
        StringError msg' ->
          [ "error" .= String "string-error",
            "message" .= String (cs msg')
          ]
        TxIndexConflict txids ->
          [ "error" .= String "multiple-tx-index",
            "message" .= String "Multiple txs match that index",
            "txids" .= txids
          ]
        ServerTimeout ->
          [ "error" .= String "server-timeout",
            "message" .= String "Request is taking too long"
          ]
        RequestTooLarge ->
          [ "error" .= String "request-too-large",
            "message" .= String "Request body too large"
          ]

instance FromJSON Except where
  parseJSON =
    A.withObject "Except" $ \o -> do
      ctr <- o .: "error"
      msg' <- o .:? "message" .!= ""
      case ctr of
        String "not-found-or-invalid-arg" ->
          return ThingNotFound
        String "server-error" ->
          return ServerError
        String "bad-request" ->
          return BadRequest
        String "user-error" ->
          return $ UserError msg'
        String "string-error" ->
          return $ StringError msg'
        String "multiple-tx-index" -> do
          txids <- o .: "txids"
          return $ TxIndexConflict txids
        String "server-timeout" ->
          return ServerTimeout
        String "request-too-large" ->
          return RequestTooLarge
        _ -> mzero

---------------------------------------
-- Blockchain.info API Compatibility --
---------------------------------------

toIntTxId :: TxHash -> Word64
toIntTxId h =
  let bs = runPutS (serialize h)
      Right w64 = runGetS getWord64be bs
   in w64 `shift` (-11)

data BinfoBlockId
  = BinfoBlockHash !BlockHash
  | BinfoBlockIndex !Word32
  deriving (Eq, Show, Read, Generic, NFData)

instance Parsable BinfoBlockId where
  parseParam t =
    hex <> igr
    where
      hex = case hexToBlockHash (LazyText.toStrict t) of
        Nothing -> Left "could not decode txid"
        Just h -> Right $ BinfoBlockHash h
      igr = BinfoBlockIndex <$> parseParam t

data BinfoTxId
  = BinfoTxIdHash !TxHash
  | BinfoTxIdIndex !Word64
  deriving (Eq, Show, Read, Generic, NFData)

encodeBinfoTxId :: Bool -> TxHash -> BinfoTxId
encodeBinfoTxId False = BinfoTxIdHash
encodeBinfoTxId True = BinfoTxIdIndex . toIntTxId

instance Parsable BinfoTxId where
  parseParam t =
    hex <> igr
    where
      hex =
        case hexToTxHash (LazyText.toStrict t) of
          Nothing -> Left "could not decode txid"
          Just h -> Right $ BinfoTxIdHash h
      igr = BinfoTxIdIndex <$> parseParam t

instance ToJSON BinfoTxId where
  toJSON (BinfoTxIdHash h) = toJSON h
  toJSON (BinfoTxIdIndex i) = toJSON i
  toEncoding (BinfoTxIdHash h) = toEncoding h
  toEncoding (BinfoTxIdIndex i) = A.word64 i

instance FromJSON BinfoTxId where
  parseJSON v =
    BinfoTxIdHash <$> parseJSON v
      <|> BinfoTxIdIndex <$> parseJSON v

data BinfoFilter
  = BinfoFilterAll
  | BinfoFilterSent
  | BinfoFilterReceived
  | BinfoFilterMoved
  | BinfoFilterConfirmed
  | BinfoFilterMempool
  deriving (Eq, Show, Generic, NFData)

instance Parsable BinfoFilter where
  parseParam t =
    parseParam t >>= \case
      (0 :: Int) -> return BinfoFilterAll
      1 -> return BinfoFilterSent
      2 -> return BinfoFilterReceived
      3 -> return BinfoFilterMoved
      5 -> return BinfoFilterConfirmed
      6 -> return BinfoFilterAll
      7 -> return BinfoFilterMempool
      _ -> Left "could not parse filter parameter"

data BinfoMultiAddr = BinfoMultiAddr
  { addresses :: ![BinfoBalance],
    wallet :: !BinfoWallet,
    txs :: ![BinfoTx],
    info :: !BinfoInfo,
    recommendFee :: !Bool,
    cashAddr :: !Bool
  }
  deriving (Eq, Show, Generic, NFData)

instance MarshalJSON (Network, Ctx) BinfoMultiAddr where
  marshalValue (net', ctx) m =
    A.object $
      [ "addresses" .= map (marshalValue (net, ctx)) m.addresses,
        "wallet" .= m.wallet,
        "txs" .= map (marshalValue (net, ctx)) m.txs,
        "info" .= m.info,
        "recommend_include_fee" .= m.recommendFee
      ]
        ++ ["cash_addr" .= True | m.cashAddr]
    where
      net = if not m.cashAddr && net' == bch then btc else net'

  unmarshalValue (net, ctx) =
    A.withObject "BinfoMultiAddr" $ \o -> do
      addresses <- mapM (unmarshalValue (net, ctx)) =<< o .: "addresses"
      wallet <- o .: "wallet"
      txs <- mapM (unmarshalValue (net, ctx)) =<< o .: "txs"
      info <- o .: "info"
      recommendFee <- o .: "recommend_include_fee"
      cashAddr <- o .:? "cash_addr" .!= False
      return BinfoMultiAddr {..}

  marshalEncoding (net', ctx) m =
    A.pairs $
      mconcat
        [ "addresses" `A.pair` A.list (marshalEncoding (net, ctx)) m.addresses,
          "wallet" `A.pair` toEncoding m.wallet,
          "txs" `A.pair` A.list (marshalEncoding (net, ctx)) m.txs,
          "info" `A.pair` toEncoding m.info,
          "recommend_include_fee" `A.pair` A.bool m.recommendFee,
          if m.cashAddr then "cash_addr" `A.pair` A.bool True else mempty
        ]
    where
      net = if not m.cashAddr && net' == bch then btc else net'

data BinfoRawAddr = BinfoRawAddr
  { address :: !BinfoAddr,
    balance :: !Word64,
    ntx :: !Word64,
    utxo :: !Word64,
    received :: !Word64,
    sent :: !Int64,
    txs :: ![BinfoTx]
  }
  deriving (Eq, Show, Generic, NFData)

instance MarshalJSON (Network, Ctx) BinfoRawAddr where
  marshalValue (net, ctx) r =
    A.object
      [ "hash160" .= h160,
        "address" .= address,
        "n_tx" .= r.ntx,
        "n_unredeemed" .= r.utxo,
        "total_received" .= r.received,
        "total_sent" .= r.sent,
        "final_balance" .= r.balance,
        "txs" .= map (marshalValue (net, ctx)) r.txs
      ]
    where
      address = case r.address of
        BinfoAddr a -> marshalValue net a
        BinfoXpub x -> marshalValue (net, ctx) x
      h160 =
        encodeHex . runPutS . serialize
          <$> case r.address of
            BinfoAddr a -> addressHash160 a
            _ -> Nothing

  marshalEncoding (net, ctx) r =
    A.pairs $
      mconcat
        [ "hash160" `A.pair` fromMaybe A.null_ h160,
          "address" `A.pair` address,
          "n_tx" `A.pair` A.word64 r.ntx,
          "n_unredeemed" `A.pair` A.word64 r.utxo,
          "total_received" `A.pair` A.word64 r.received,
          "total_sent" `A.pair` A.int64 r.sent,
          "final_balance" `A.pair` A.word64 r.balance,
          "txs" `A.pair` A.list (marshalEncoding (net, ctx)) r.txs
        ]
    where
      address = case r.address of
        BinfoAddr a -> marshalEncoding net a
        BinfoXpub x -> marshalEncoding (net, ctx) x
      h160 =
        hexEncoding . runPutL . serialize
          <$> case r.address of
            BinfoAddr a -> addressHash160 a
            _ -> Nothing

  unmarshalValue (net, ctx) =
    A.withObject "BinfoRawAddr" $ \o -> do
      addr <- o .: "address"
      address <-
        BinfoAddr <$> unmarshalValue net addr
          <|> BinfoXpub <$> unmarshalValue (net, ctx) addr
      balance <- o .: "final_balance"
      utxo <- o .: "n_unredeemed"
      ntx <- o .: "n_tx"
      received <- o .: "total_received"
      sent <- o .: "total_sent"
      txs <- mapM (unmarshalValue (net, ctx)) =<< o .: "txs"
      return BinfoRawAddr {..}

data BinfoShortBal = BinfoShortBal
  { final :: !Word64,
    ntx :: !Word64,
    received :: !Word64
  }
  deriving (Eq, Show, Read, Generic, NFData)

instance ToJSON BinfoShortBal where
  toJSON b =
    A.object
      [ "final_balance" .= b.final,
        "n_tx" .= b.ntx,
        "total_received" .= b.received
      ]
  toEncoding b =
    A.pairs $
      mconcat
        [ "final_balance" `A.pair` A.word64 b.final,
          "n_tx" `A.pair` A.word64 b.ntx,
          "total_received" `A.pair` A.word64 b.received
        ]

instance FromJSON BinfoShortBal where
  parseJSON =
    A.withObject "BinfoShortBal" $ \o -> do
      final <- o .: "final_balance"
      ntx <- o .: "n_tx"
      received <- o .: "total_received"
      return BinfoShortBal {..}

data BinfoBalance
  = BinfoAddrBalance
      { address :: !Address,
        txs :: !Word64,
        received :: !Word64,
        sent :: !Word64,
        balance :: !Word64
      }
  | BinfoXPubBalance
      { xpub :: !XPubKey,
        txs :: !Word64,
        received :: !Word64,
        sent :: !Word64,
        balance :: !Word64,
        external :: !Word32,
        change :: !Word32
      }
  deriving (Eq, Show, Generic, NFData)

instance MarshalJSON (Network, Ctx) BinfoBalance where
  marshalValue (net, ctx) b@BinfoAddrBalance {} =
    A.object
      [ "address" .= marshalValue net b.address,
        "final_balance" .= b.balance,
        "n_tx" .= b.txs,
        "total_received" .= b.received,
        "total_sent" .= b.sent
      ]
  marshalValue (net, ctx) b@BinfoXPubBalance {} =
    A.object
      [ "address" .= marshalValue (net, ctx) b.xpub,
        "change_index" .= b.change,
        "account_index" .= b.external,
        "final_balance" .= b.balance,
        "n_tx" .= b.txs,
        "total_received" .= b.received,
        "total_sent" .= b.sent
      ]

  unmarshalValue (net, ctx) =
    A.withObject "BinfoBalance" $ \o -> x o <|> a o
    where
      x o = do
        xpub <- unmarshalValue (net, ctx) =<< o .: "address"
        change <- o .: "change_index"
        external <- o .: "account_index"
        balance <- o .: "final_balance"
        txs <- o .: "n_tx"
        received <- o .: "total_received"
        sent <- o .: "total_sent"
        return BinfoXPubBalance {..}
      a o = do
        address <- unmarshalValue net =<< o .: "address"
        balance <- o .: "final_balance"
        txs <- o .: "n_tx"
        received <- o .: "total_received"
        sent <- o .: "total_sent"
        return BinfoAddrBalance {..}

  marshalEncoding (net, ctx) b@BinfoAddrBalance {} =
    A.pairs $
      mconcat
        [ "address" `A.pair` marshalEncoding net b.address,
          "final_balance" `A.pair` A.word64 b.balance,
          "n_tx" `A.pair` A.word64 b.txs,
          "total_received" `A.pair` A.word64 b.received,
          "total_sent" `A.pair` A.word64 b.sent
        ]
  marshalEncoding (net, ctx) b@BinfoXPubBalance {} =
    A.pairs $
      mconcat
        [ "address" `A.pair` marshalEncoding (net, ctx) b.xpub,
          "change_index" `A.pair` A.word32 b.change,
          "account_index" `A.pair` A.word32 b.external,
          "final_balance" `A.pair` A.word64 b.balance,
          "n_tx" `A.pair` A.word64 b.txs,
          "total_received" `A.pair` A.word64 b.received,
          "total_sent" `A.pair` A.word64 b.sent
        ]

instance MarshalJSON (Network, Ctx) [BinfoBalance] where
  marshalValue (net, ctx) addrs =
    toJSON $ map (marshalValue (net, ctx)) addrs
  marshalEncoding (net, ctx) =
    A.list (marshalEncoding (net, ctx))
  unmarshalValue (net, ctx) =
    A.withArray "[BinfoBalance]" $
      fmap V.toList . mapM (unmarshalValue (net, ctx))

data BinfoWallet = BinfoWallet
  { balance :: !Word64,
    txs :: !Word64,
    filtered :: !Word64,
    received :: !Word64,
    sent :: !Word64
  }
  deriving (Eq, Show, Generic, NFData)

instance ToJSON BinfoWallet where
  toJSON w =
    A.object
      [ "final_balance" .= w.balance,
        "n_tx" .= w.txs,
        "n_tx_filtered" .= w.filtered,
        "total_received" .= w.received,
        "total_sent" .= w.sent
      ]
  toEncoding w =
    A.pairs $
      mconcat
        [ "final_balance" `A.pair` A.word64 w.balance,
          "n_tx" `A.pair` A.word64 w.txs,
          "n_tx_filtered" `A.pair` A.word64 w.filtered,
          "total_received" `A.pair` A.word64 w.received,
          "total_sent" `A.pair` A.word64 w.sent
        ]

instance FromJSON BinfoWallet where
  parseJSON =
    A.withObject "BinfoWallet" $ \o -> do
      balance <- o .: "final_balance"
      txs <- o .: "n_tx"
      filtered <- o .: "n_tx_filtered"
      received <- o .: "total_received"
      sent <- o .: "total_sent"
      return BinfoWallet {..}

binfoHexValue :: Word64 -> Text
binfoHexValue w64 =
  encodeHex $
    if B.null bs || B.head bs `testBit` 7
      then B.cons 0x00 bs
      else bs
  where
    bs =
      B.dropWhile (== 0x00) $
        runPutS $
          serialize w64

data BinfoUnspent = BinfoUnspent
  { txid :: !TxHash,
    index :: !Word32,
    script :: !ByteString,
    value :: !Word64,
    confirmations :: !Int32,
    txidx :: !BinfoTxId,
    xpub :: !(Maybe BinfoXPubPath)
  }
  deriving (Eq, Show, Generic, NFData)

instance MarshalJSON (Network, Ctx) BinfoUnspent where
  marshalValue (net, ctx) u =
    A.object $
      [ "tx_hash_big_endian" .= u.txid,
        "tx_hash" .= encodeHex (runPutS (serialize u.txid.get)),
        "tx_output_n" .= u.index,
        "script" .= encodeHex u.script,
        "value" .= u.value,
        "value_hex" .= binfoHexValue u.value,
        "confirmations" .= u.confirmations,
        "tx_index" .= u.txidx
      ]
        <> [ "xpub" .= marshalValue (net, ctx) x
           | x <- maybeToList u.xpub
           ]

  marshalEncoding (net, ctx) u =
    A.pairs $
      mconcat
        [ "tx_hash_big_endian" `A.pair` toEncoding u.txid,
          "tx_hash" `A.pair` hexEncoding (runPutL (serialize u.txid.get)),
          "tx_output_n" `A.pair` A.word32 u.index,
          "script" `A.pair` hexEncoding (B.fromStrict u.script),
          "value" `A.pair` A.word64 u.value,
          "value_hex" `A.pair` A.text (binfoHexValue u.value),
          "confirmations" `A.pair` A.int32 u.confirmations,
          "tx_index" `A.pair` toEncoding u.txidx,
          maybe mempty (("xpub" `A.pair`) . marshalEncoding (net, ctx)) u.xpub
        ]

  unmarshalValue (net, ctx) =
    A.withObject "BinfoUnspent" $ \o -> do
      txid <- o .: "tx_hash_big_endian"
      index <- o .: "tx_output_n"
      script <- maybe mzero return . decodeHex =<< o .: "script"
      value <- o .: "value"
      confirmations <- o .: "confirmations"
      txidx <- o .: "tx_index"
      xpub <- mapM (unmarshalValue (net, ctx)) =<< o .:? "xpub"
      return BinfoUnspent {..}

newtype BinfoUnspents = BinfoUnspents [BinfoUnspent]
  deriving (Eq, Show, Generic, NFData)

instance MarshalJSON (Network, Ctx) BinfoUnspents where
  marshalValue (net, ctx) (BinfoUnspents us) =
    A.object
      [ "notice" .= T.empty,
        "unspent_outputs" .= map (marshalValue (net, ctx)) us
      ]

  marshalEncoding (net, ctx) (BinfoUnspents us) =
    A.pairs $
      mconcat
        [ "notice" `A.pair` A.text T.empty,
          "unspent_outputs" `A.pair` A.list (marshalEncoding (net, ctx)) us
        ]

  unmarshalValue (net, ctx) =
    A.withObject "BinfoUnspents" $ \o -> do
      us <- mapM (unmarshalValue (net, ctx)) =<< o .: "unspent_outputs"
      return (BinfoUnspents us)

toBinfoBlock :: BlockData -> [BinfoTx] -> [BlockHash] -> BinfoBlock
toBinfoBlock b transactions next_blocks =
  BinfoBlock
    { hash = headerHash b.header,
      version = b.header.version,
      prev = b.header.prev,
      merkle = b.header.merkle,
      timestamp = b.header.timestamp,
      bits = b.header.bits,
      next = next_blocks,
      fee = b.fee,
      nonce = b.header.nonce,
      ntx = fromIntegral (length transactions),
      size = b.size,
      index = b.height,
      main = b.main,
      height = b.height,
      weight = b.weight,
      txs = transactions
    }

data BinfoBlock = BinfoBlock
  { hash :: !BlockHash,
    version :: !Word32,
    prev :: !BlockHash,
    merkle :: !Hash256,
    timestamp :: !Word32,
    bits :: !Word32,
    next :: ![BlockHash],
    fee :: !Word64,
    nonce :: !Word32,
    ntx :: !Word32,
    size :: !Word32,
    index :: !Word32,
    main :: !Bool,
    height :: !Word32,
    weight :: !Word32,
    txs :: ![BinfoTx]
  }
  deriving (Eq, Show, Generic, NFData)

instance MarshalJSON (Network, Ctx) BinfoBlock where
  marshalValue (net, ctx) b =
    A.object
      [ "hash" .= b.hash,
        "ver" .= b.version,
        "prev_block" .= b.prev,
        "mrkl_root" .= TxHash b.merkle,
        "time" .= b.timestamp,
        "bits" .= b.bits,
        "next_block" .= b.next,
        "fee" .= b.fee,
        "nonce" .= b.nonce,
        "n_tx" .= b.ntx,
        "size" .= b.size,
        "block_index" .= b.index,
        "main_chain" .= b.main,
        "height" .= b.height,
        "weight" .= b.weight,
        "tx" .= map (marshalValue (net, ctx)) b.txs
      ]

  marshalEncoding (net, ctx) b =
    A.pairs $
      mconcat
        [ "hash" `A.pair` toEncoding b.hash,
          "ver" `A.pair` A.word32 b.version,
          "prev_block" `A.pair` toEncoding b.prev,
          "mrkl_root" `A.pair` toEncoding (TxHash b.merkle),
          "time" `A.pair` A.word32 b.timestamp,
          "bits" `A.pair` A.word32 b.bits,
          "next_block" `A.pair` toEncoding b.next,
          "fee" `A.pair` A.word64 b.fee,
          "nonce" `A.pair` A.word32 b.nonce,
          "n_tx" `A.pair` A.word32 b.ntx,
          "size" `A.pair` A.word32 b.size,
          "block_index" `A.pair` A.word32 b.index,
          "main_chain" `A.pair` A.bool b.main,
          "height" `A.pair` A.word32 b.height,
          "weight" `A.pair` A.word32 b.weight,
          "tx" `A.pair` A.list (marshalEncoding (net, ctx)) b.txs
        ]

  unmarshalValue (net, ctx) =
    A.withObject "BinfoBlock" $ \o -> do
      hash <- o .: "hash"
      version <- o .: "ver"
      prev <- o .: "prev_block"
      merkle <- (\(TxHash h) -> h) <$> o .: "mrkl_root"
      timestamp <- o .: "time"
      bits <- o .: "bits"
      next <- o .: "next_block"
      fee <- o .: "fee"
      nonce <- o .: "nonce"
      ntx <- o .: "n_tx"
      size <- o .: "size"
      index <- o .: "block_index"
      main <- o .: "main_chain"
      height <- o .: "height"
      weight <- o .: "weight"
      txs <- o .: "tx" >>= mapM (unmarshalValue (net, ctx))
      return BinfoBlock {..}

instance MarshalJSON (Network, Ctx) [BinfoBlock] where
  marshalValue (net, ctx) blocks =
    A.object ["blocks" .= map (marshalValue (net, ctx)) blocks]

  marshalEncoding (net, ctx) blocks =
    A.pairs $ "blocks" `A.pair` A.list (marshalEncoding (net, ctx)) blocks

  unmarshalValue (net, ctx) =
    A.withObject "blocks" $ \o ->
      mapM (unmarshalValue (net, ctx)) =<< o .: "blocks"

data BinfoTx = BinfoTx
  { txid :: !TxHash,
    version :: !Word32,
    inputCount :: !Word32,
    outputCount :: !Word32,
    size :: !Word32,
    weight :: !Word32,
    fee :: !Word64,
    relayed :: !ByteString,
    locktime :: !Word32,
    index :: !BinfoTxId,
    doubleSpend :: !Bool,
    rbf :: !Bool,
    balance :: !(Maybe (Int64, Int64)),
    timestamp :: !Word64,
    blockIndex :: !(Maybe Word32),
    blockHeight :: !(Maybe Word32),
    inputs :: ![BinfoTxInput],
    outputs :: ![BinfoTxOutput]
  }
  deriving (Eq, Show, Generic, NFData)

instance MarshalJSON (Network, Ctx) BinfoTx where
  marshalValue (net, ctx) t =
    A.object $
      [ "hash" .= t.txid,
        "ver" .= t.version,
        "vin_sz" .= t.inputCount,
        "vout_sz" .= t.outputCount,
        "size" .= t.size,
        "weight" .= t.weight,
        "fee" .= t.fee,
        "relayed_by" .= T.decodeUtf8 t.relayed,
        "lock_time" .= t.locktime,
        "tx_index" .= t.index,
        "double_spend" .= t.doubleSpend,
        "time" .= t.timestamp,
        "block_index" .= t.blockIndex,
        "block_height" .= t.blockHeight,
        "inputs" .= map (marshalValue (net, ctx)) t.inputs,
        "out" .= map (marshalValue (net, ctx)) t.outputs
      ]
        ++ bal
        ++ rbf
    where
      bal =
        case t.balance of
          Nothing -> []
          Just (r, b) -> ["result" .= r, "balance" .= b]
      rbf = ["rbf" .= True | t.rbf]

  marshalEncoding (net, ctx) t =
    A.pairs $
      mconcat
        [ "hash" `A.pair` toEncoding t.txid,
          "ver" `A.pair` A.word32 t.version,
          "vin_sz" `A.pair` A.word32 t.inputCount,
          "vout_sz" `A.pair` A.word32 t.outputCount,
          "size" `A.pair` A.word32 t.size,
          "weight" `A.pair` A.word32 t.weight,
          "fee" `A.pair` A.word64 t.fee,
          "relayed_by" `A.pair` A.text (T.decodeUtf8 t.relayed),
          "lock_time" `A.pair` A.word32 t.locktime,
          "tx_index" `A.pair` toEncoding t.index,
          "double_spend" `A.pair` A.bool t.doubleSpend,
          "time" `A.pair` A.word64 t.timestamp,
          "block_index" `A.pair` toEncoding t.blockIndex,
          "block_height" `A.pair` maybe A.null_ A.word32 t.blockHeight,
          "inputs" `A.pair` A.list (marshalEncoding (net, ctx)) t.inputs,
          "out" `A.pair` A.list (marshalEncoding (net, ctx)) t.outputs,
          bal,
          rbf
        ]
    where
      bal =
        case t.balance of
          Nothing -> mempty
          Just (r, b) ->
            "result" `A.pair` A.int64 r <> "balance" `A.pair` A.int64 b
      rbf = if t.rbf then "rbf" .= True else mempty

  unmarshalValue (net, ctx) = A.withObject "BinfoTx" $ \o -> do
    txid <- o .: "hash"
    version <- o .: "ver"
    inputCount <- o .: "vin_sz"
    outputCount <- o .: "vout_sz"
    size <- o .: "size"
    weight <- o .: "weight"
    fee <- o .: "fee"
    relayed <- T.encodeUtf8 <$> o .: "relayed_by"
    locktime <- o .: "lock_time"
    index <- o .: "tx_index"
    doubleSpend <- o .: "double_spend"
    timestamp <- o .: "time"
    blockIndex <- o .: "block_index"
    blockHeight <- o .: "block_height"
    inputs <- o .: "inputs" >>= mapM (unmarshalValue (net, ctx))
    outputs <- o .: "out" >>= mapM (unmarshalValue (net, ctx))
    rbf <- o .:? "rbf" .!= False
    res <- o .:? "result"
    bal <- o .:? "balance"
    let balance = (,) <$> res <*> bal
    return BinfoTx {..}

instance MarshalJSON (Network, Ctx) [BinfoTx] where
  marshalValue (net, ctx) txs =
    toJSON $ map (marshalValue (net, ctx)) txs
  marshalEncoding (net, ctx) =
    A.list (marshalEncoding (net, ctx))
  unmarshalValue (net, ctx) =
    A.withArray "[BinfoTx]" $
      fmap V.toList . mapM (unmarshalValue (net, ctx))

data BinfoTxInput = BinfoTxInput
  { sequence :: !Word32,
    witness :: !ByteString,
    script :: !ByteString,
    index :: !Word32,
    output :: !BinfoTxOutput
  }
  deriving (Eq, Show, Generic, NFData)

instance MarshalJSON (Network, Ctx) BinfoTxInput where
  marshalValue (net, ctx) i =
    A.object
      [ "sequence" .= i.sequence,
        "witness" .= encodeHex i.witness,
        "script" .= encodeHex i.script,
        "index" .= i.index,
        "prev_out" .= marshalValue (net, ctx) i.output
      ]

  marshalEncoding (net, ctx) i =
    A.pairs $
      mconcat
        [ "sequence" `A.pair` A.word32 i.sequence,
          "witness" `A.pair` hexEncoding (B.fromStrict i.witness),
          "script" `A.pair` hexEncoding (B.fromStrict i.script),
          "index" `A.pair` A.word32 i.index,
          "prev_out" `A.pair` marshalEncoding (net, ctx) i.output
        ]

  unmarshalValue (net, ctx) =
    A.withObject "BinfoTxInput" $ \o -> do
      sequence <- o .: "sequence"
      witness <-
        maybe mzero return . decodeHex
          =<< o .: "witness"
      script <-
        maybe mzero return . decodeHex
          =<< o .: "script"
      index <- o .: "index"
      output <-
        o .: "prev_out"
          >>= unmarshalValue (net, ctx)
      return BinfoTxInput {..}

data BinfoTxOutput = BinfoTxOutput
  { typ :: !Int,
    spent :: !Bool,
    value :: !Word64,
    index :: !Word32,
    txidx :: !BinfoTxId,
    script :: !ByteString,
    spenders :: ![BinfoSpender],
    address :: !(Maybe Address),
    xpub :: !(Maybe BinfoXPubPath)
  }
  deriving (Eq, Show, Generic, NFData)

instance MarshalJSON (Network, Ctx) BinfoTxOutput where
  marshalValue (net, ctx) o =
    A.object $
      [ "type" .= o.typ,
        "spent" .= o.spent,
        "value" .= o.value,
        "spending_outpoints" .= o.spenders,
        "n" .= o.index,
        "tx_index" .= o.txidx,
        "script" .= encodeHex o.script
      ]
        <> [ "addr" .= marshalValue net a
           | a <- maybeToList o.address
           ]
        <> [ "xpub" .= marshalValue (net, ctx) x
           | x <- maybeToList o.xpub
           ]

  marshalEncoding (net, ctx) o =
    A.pairs $
      mconcat $
        [ "type" `A.pair` A.int o.typ,
          "spent" `A.pair` A.bool o.spent,
          "value" `A.pair` A.word64 o.value,
          "spending_outpoints" `A.pair` A.list toEncoding o.spenders,
          "n" `A.pair` A.word32 o.index,
          "tx_index" `A.pair` toEncoding o.txidx,
          "script" `A.pair` hexEncoding (B.fromStrict o.script)
        ]
          <> [ "addr" `A.pair` marshalEncoding net a
             | a <- maybeToList o.address
             ]
          <> [ "xpub" `A.pair` marshalEncoding (net, ctx) x
             | x <- maybeToList o.xpub
             ]

  unmarshalValue (net, ctx) =
    A.withObject "BinfoTxOutput" $ \o -> do
      typ <- o .: "type"
      spent <- o .: "spent"
      value <- o .: "value"
      spenders <- o .: "spending_outpoints"
      index <- o .: "n"
      txidx <- o .: "tx_index"
      script <- maybe mzero return . decodeHex =<< o .: "script"
      address <- o .:? "addr" >>= mapM (unmarshalValue net)
      xpub <- o .:? "xpub" >>= mapM (unmarshalValue (net, ctx))
      return BinfoTxOutput {..}

data BinfoSpender = BinfoSpender
  { txidx :: !BinfoTxId,
    input :: !Word32
  }
  deriving (Eq, Show, Generic, NFData)

instance ToJSON BinfoSpender where
  toJSON s =
    A.object
      [ "tx_index" .= s.txidx,
        "n" .= s.input
      ]
  toEncoding s =
    A.pairs $
      mconcat
        [ "tx_index" `A.pair` toEncoding s.txidx,
          "n" `A.pair` A.word32 s.input
        ]

instance FromJSON BinfoSpender where
  parseJSON =
    A.withObject "BinfoSpender" $ \o -> do
      txidx <- o .: "tx_index"
      input <- o .: "n"
      return BinfoSpender {..}

data BinfoXPubPath = BinfoXPubPath
  { key :: !XPubKey,
    deriv :: !SoftPath
  }
  deriving (Eq, Show, Generic, NFData)

instance Ord BinfoXPubPath where
  compare = compare `on` f
    where
      f b =
        ( b.key.parent,
          b.deriv
        )

instance MarshalJSON (Network, Ctx) BinfoXPubPath where
  marshalValue (net, ctx) p =
    A.object
      [ "m" .= marshalValue (net, ctx) p.key,
        "path" .= ("M" ++ pathToStr p.deriv)
      ]

  marshalEncoding (net, ctx) p =
    A.pairs $
      mconcat
        [ "m" `A.pair` marshalEncoding (net, ctx) p.key,
          "path" `A.pair` A.string ("M" ++ pathToStr p.deriv)
        ]

  unmarshalValue (net, ctx) =
    A.withObject "BinfoXPubPath" $ \o -> do
      key <- o .: "m" >>= unmarshalValue (net, ctx)
      deriv <- fromMaybe "bad xpub path" . parseSoft <$> o .: "path"
      return BinfoXPubPath {..}

data BinfoInfo = BinfoInfo
  { connected :: !Word32,
    conversion :: !Double,
    fiat :: !BinfoSymbol,
    crypto :: !BinfoSymbol,
    head :: !BinfoBlockInfo
  }
  deriving (Eq, Show, Generic, NFData)

instance ToJSON BinfoInfo where
  toJSON i =
    A.object
      [ "nconnected" .= i.connected,
        "conversion" .= i.conversion,
        "symbol_local" .= i.fiat,
        "symbol_btc" .= i.crypto,
        "latest_block" .= i.head
      ]
  toEncoding i =
    A.pairs $
      mconcat
        [ "nconnected" `A.pair` A.word32 i.connected,
          "conversion" `A.pair` A.double i.conversion,
          "symbol_local" `A.pair` toEncoding i.fiat,
          "symbol_btc" `A.pair` toEncoding i.crypto,
          "latest_block" `A.pair` toEncoding i.head
        ]

instance FromJSON BinfoInfo where
  parseJSON =
    A.withObject "BinfoInfo" $ \o -> do
      connected <- o .: "nconnected"
      conversion <- o .: "conversion"
      fiat <- o .: "symbol_local"
      crypto <- o .: "symbol_btc"
      head <- o .: "latest_block"
      return BinfoInfo {..}

data BinfoBlockInfo = BinfoBlockInfo
  { hash :: !BlockHash,
    height :: !BlockHeight,
    timestamp :: !Word32,
    index :: !BlockHeight
  }
  deriving (Eq, Show, Generic, NFData)

instance ToJSON BinfoBlockInfo where
  toJSON i =
    A.object
      [ "hash" .= i.hash,
        "height" .= i.height,
        "time" .= i.timestamp,
        "block_index" .= i.index
      ]
  toEncoding i =
    A.pairs $
      mconcat
        [ "hash" `A.pair` toEncoding i.hash,
          "height" `A.pair` A.word32 i.height,
          "time" `A.pair` A.word32 i.timestamp,
          "block_index" `A.pair` A.word32 i.index
        ]

instance FromJSON BinfoBlockInfo where
  parseJSON =
    A.withObject "BinfoBlockInfo" $ \o -> do
      hash <- o .: "hash"
      height <- o .: "height"
      timestamp <- o .: "time"
      index <- o .: "block_index"
      return BinfoBlockInfo {..}

toBinfoBlockInfo :: BlockData -> BinfoBlockInfo
toBinfoBlockInfo d =
  BinfoBlockInfo
    { hash = headerHash d.header,
      height = d.height,
      timestamp = d.header.timestamp,
      index = d.height
    }

data BinfoRate = BinfoRate
  { timestamp :: !Word64,
    price :: !Double,
    vol24 :: !Double
  }
  deriving (Eq, Show, Generic, NFData)

instance ToJSON BinfoRate where
  toJSON r =
    A.object
      [ "timestamp" .= r.timestamp,
        "price" .= r.price,
        "volume24h" .= r.vol24
      ]
  toEncoding r =
    A.pairs $
      mconcat
        [ "timestamp" `A.pair` A.word64 r.timestamp,
          "price" `A.pair` A.double r.price,
          "volume24h" `A.pair` A.double r.vol24
        ]

instance FromJSON BinfoRate where
  parseJSON =
    A.withObject "BinfoRate" $ \o -> do
      timestamp <- o .: "timestamp"
      price <- o .: "price"
      vol24 <- o .: "volume24h"
      return BinfoRate {..}

data BinfoHistory = BinfoHistory
  { date :: !Text,
    time :: !Text,
    typ :: !Text,
    amount :: !Double,
    valueThen :: !Double,
    valueNow :: !Double,
    rateThen :: !Double,
    txid :: !TxHash,
    fee :: !Double
  }
  deriving (Eq, Show, Generic, NFData)

instance ToJSON BinfoHistory where
  toJSON h =
    A.object
      [ "date" .= h.date,
        "time" .= h.time,
        "type" .= h.typ,
        "amount" .= h.amount,
        "value_then" .= h.valueThen,
        "value_now" .= h.valueNow,
        "exchange_rate_then" .= h.rateThen,
        "tx" .= h.txid,
        "fee" .= h.fee
      ]
  toEncoding h =
    A.pairs $
      mconcat
        [ "date" `A.pair` A.text h.date,
          "time" `A.pair` A.text h.time,
          "type" `A.pair` A.text h.typ,
          "amount" `A.pair` A.double h.amount,
          "value_then" `A.pair` A.double h.valueThen,
          "value_now" `A.pair` A.double h.valueNow,
          "exchange_rate_then" `A.pair` A.double h.rateThen,
          "tx" `A.pair` toEncoding h.txid,
          "fee" `A.pair` A.double h.fee
        ]

instance FromJSON BinfoHistory where
  parseJSON =
    A.withObject "BinfoHistory" $ \o -> do
      date <- o .: "date"
      time <- o .: "time"
      typ <- o .: "type"
      amount <- o .: "amount"
      valueThen <- o .: "value_then"
      valueNow <- o .: "value_now"
      rateThen <- o .: "exchange_rate_then"
      txid <- o .: "tx"
      fee <- o .: "fee"
      return BinfoHistory {..}

toBinfoHistory ::
  Int64 ->
  Word64 ->
  Double ->
  Double ->
  Word64 ->
  TxHash ->
  BinfoHistory
toBinfoHistory satoshi timestamp rateThen rateNow fee txid =
  BinfoHistory
    { date = T.pack $ formatTime defaultTimeLocale "%Y-%m-%d" t,
      time = T.pack $ formatTime defaultTimeLocale "%H:%M:%S GMT %Ez" t,
      typ = if satoshi <= 0 then "sent" else "received",
      amount = fromRational v,
      fee = fromRational f,
      valueThen = fromRational v1,
      valueNow = fromRational v2,
      rateThen,
      txid
    }
  where
    t = posixSecondsToUTCTime (realToFrac timestamp)
    v = toRational satoshi / (100 * 1000 * 1000)
    r1 = toRational rateThen
    r2 = toRational rateNow
    f = toRational fee / (100 * 1000 * 1000)
    v1 = v * r1
    v2 = v * r2

newtype BinfoDate = BinfoDate Word64
  deriving (Eq, Show, Read, Generic, NFData)

instance Parsable BinfoDate where
  parseParam t =
    maybeToEither "Cannot parse date"
      . fmap (BinfoDate . round . utcTimeToPOSIXSeconds)
      $ p "%d-%m-%Y" <|> p "%d/%m/%Y"
    where
      s = LazyText.unpack t
      p fmt = parseTimeM False defaultTimeLocale fmt s

data BinfoTicker = BinfoTicker
  { fifteen :: !Double,
    last :: !Double,
    buy :: !Double,
    sell :: !Double,
    symbol :: !Text
  }
  deriving (Eq, Show, Generic, NFData)

instance Default BinfoTicker where
  def =
    BinfoTicker
      { symbol = "XXX",
        fifteen = 0.0,
        last = 0.0,
        buy = 0.0,
        sell = 0.0
      }

instance ToJSON BinfoTicker where
  toJSON t =
    A.object
      [ "symbol" .= t.symbol,
        "sell" .= t.sell,
        "buy" .= t.buy,
        "last" .= t.last,
        "15m" .= t.fifteen
      ]
  toEncoding t =
    A.pairs $
      mconcat
        [ "symbol" `A.pair` A.text t.symbol,
          "sell" `A.pair` A.double t.sell,
          "buy" `A.pair` A.double t.buy,
          "last" `A.pair` A.double t.last,
          "15m" `A.pair` A.double t.fifteen
        ]

instance FromJSON BinfoTicker where
  parseJSON =
    A.withObject "BinfoTicker" $ \o -> do
      symbol <- o .: "symbol"
      fifteen <- o .: "15m"
      sell <- o .: "sell"
      buy <- o .: "buy"
      last <- o .: "last"
      return BinfoTicker {..}

data BinfoSymbol = BinfoSymbol
  { code :: !Text,
    symbol :: !Text,
    name :: !Text,
    conversion :: !Double,
    after :: !Bool,
    local :: !Bool
  }
  deriving (Eq, Show, Generic, NFData)

instance Default BinfoSymbol where
  def =
    BinfoSymbol
      { code = "XXX",
        symbol = "¤",
        name = "No currency",
        conversion = 0.0,
        after = False,
        local = True
      }

instance ToJSON BinfoSymbol where
  toJSON s =
    A.object
      [ "code" .= s.code,
        "symbol" .= s.symbol,
        "name" .= s.name,
        "conversion" .= s.conversion,
        "symbolAppearsAfter" .= s.after,
        "local" .= s.local
      ]
  toEncoding s =
    A.pairs $
      mconcat
        [ "code" `A.pair` A.text s.code,
          "symbol" `A.pair` A.text s.symbol,
          "name" `A.pair` A.text s.name,
          "conversion" `A.pair` A.double s.conversion,
          "symbolAppearsAfter" `A.pair` A.bool s.after,
          "local" `A.pair` A.bool s.local
        ]

instance FromJSON BinfoSymbol where
  parseJSON =
    A.withObject "BinfoSymbol" $ \o -> do
      code <- o .: "code"
      symbol <- o .: "symbol"
      name <- o .: "name"
      conversion <- o .: "conversion"
      after <- o .: "symbolAppearsAfter"
      local <- o .: "local"
      return BinfoSymbol {..}

relevantTxs ::
  HashSet Address ->
  Bool ->
  Transaction ->
  HashSet TxHash
relevantTxs addrs prune t =
  HashSet.fromList $ ins <> outs
  where
    p a =
      prune
        && getTxResult addrs t > 0
        && not (HashSet.member a addrs)
    f o = do
      Spender {txid} <- o.spender
      a <- o.address
      guard $ p a
      return txid
    outs = mapMaybe f t.outputs
    g StoreCoinbase {} = Nothing
    g StoreInput {outpoint = OutPoint h i} = Just h
    ins = mapMaybe g t.inputs

toBinfoAddrs ::
  HashMap Address Balance ->
  HashMap XPubSpec [XPubBal] ->
  HashMap XPubSpec Word64 ->
  [BinfoBalance]
toBinfoAddrs onlyAddrs onlyXpubs xpubTxs =
  xpubBals <> addrBals
  where
    xpubBal k xs =
      let f x =
            case x.path of
              [0, _] -> x.balance.received
              _ -> 0
          g x = x.balance.confirmed + x.balance.unconfirmed
          i m x =
            case x.path of
              [m', n] | m == m' -> n + 1
              _ -> 0
          received = sum $ map f xs
          bal = fromIntegral $ sum $ map g xs
          sent = if bal <= received then received - bal else 0
          count = HashMap.lookupDefault 0 k xpubTxs
          ax = foldl max 0 $ map (i 0) xs
          cx = foldl max 0 $ map (i 1) xs
       in BinfoXPubBalance
            { xpub = k.key,
              txs = count,
              received = received,
              sent = sent,
              balance = bal,
              external = ax,
              change = cx
            }
    xpubBals = map (uncurry xpubBal) $ HashMap.toList onlyXpubs
    addrBals =
      let f Balance {..} =
            let sent = received - balance
                balance = confirmed + unconfirmed
             in BinfoAddrBalance {..}
       in map f $ HashMap.elems onlyAddrs

toBinfoTxSimple ::
  Bool ->
  Transaction ->
  BinfoTx
toBinfoTxSimple numtxid =
  toBinfoTx numtxid HashMap.empty False 0

toBinfoTxInputs ::
  Bool ->
  HashMap Address (Maybe BinfoXPubPath) ->
  Transaction ->
  [BinfoTxInput]
toBinfoTxInputs numtxid abook t =
  zipWith f [0 ..] t.inputs
  where
    f n i =
      BinfoTxInput
        { index = n,
          sequence = i.sequence,
          script = i.script,
          witness = wit i,
          output = prev n i
        }
    wit i =
      case i.witness of
        [] -> B.empty
        ws -> runPutS (put_witness ws)
    prev = inputToBinfoTxOutput numtxid abook t
    put_witness ws = do
      putVarInt (length ws)
      mapM_ put_item ws
    put_item bs = do
      putVarInt (B.length bs)
      putByteString bs

transactionHeight :: Transaction -> Maybe BlockHeight
transactionHeight Transaction {deleted = True} = Nothing
transactionHeight Transaction {block = MemRef _} = Nothing
transactionHeight Transaction {block = BlockRef h _} = Just h

toBinfoTx ::
  Bool ->
  HashMap Address (Maybe BinfoXPubPath) ->
  Bool ->
  Int64 ->
  Transaction ->
  BinfoTx
toBinfoTx numtxid abook prune bal t =
  BinfoTx
    { version = t.version,
      weight = t.weight,
      relayed = "0.0.0.0",
      txid = txHash tx,
      index = encodeBinfoTxId numtxid (txHash tx),
      inputCount = fromIntegral $ length t.inputs,
      outputCount = fromIntegral $ length t.outputs,
      blockIndex = transactionHeight t,
      blockHeight = transactionHeight t,
      doubleSpend = t.deleted,
      balance =
        if simple
          then Nothing
          else Just (getTxResult aset t, bal),
      outputs =
        let p = prune && getTxResult aset t > 0
            f = toBinfoTxOutput numtxid abook p t
         in catMaybes $ zipWith f [0 ..] t.outputs,
      inputs = toBinfoTxInputs numtxid abook t,
      size = t.size,
      rbf = t.rbf,
      fee = t.fee,
      locktime = t.locktime,
      timestamp = t.timestamp
    }
  where
    tx = transactionData t
    aset = HashMap.keysSet abook
    simple = HashMap.null abook && bal == 0

getTxResult :: HashSet Address -> Transaction -> Int64
getTxResult aset t =
  inputSum + outputSum
  where
    inputSum = sum $ map inputValue t.inputs
    inputValue StoreCoinbase {} = 0
    inputValue StoreInput {address, value} =
      case address of
        Nothing -> 0
        Just a ->
          if testAddr a
            then negate $ fromIntegral value
            else 0
    testAddr a = HashSet.member a aset
    outputSum = sum $ map outValue t.outputs
    outValue StoreOutput {address, value} =
      case address of
        Nothing -> 0
        Just a ->
          if testAddr a
            then fromIntegral value
            else 0

toBinfoTxOutput ::
  Bool ->
  HashMap Address (Maybe BinfoXPubPath) ->
  Bool ->
  Transaction ->
  Word32 ->
  StoreOutput ->
  Maybe BinfoTxOutput
toBinfoTxOutput numtxid abook prune t index o =
  if prune && notInBook
    then Nothing
    else
      Just
        BinfoTxOutput
          { typ = 0,
            spent = isJust o.spender,
            value = o.value,
            index = index,
            txidx = encodeBinfoTxId numtxid $ txHash $ transactionData t,
            script = o.script,
            spenders = maybeToList $ toBinfoSpender numtxid <$> o.spender,
            address = o.address,
            xpub = o.address >>= join . flip HashMap.lookup abook
          }
  where
    notInBook = isNothing $ o.address >>= flip HashMap.lookup abook

toBinfoSpender :: Bool -> Spender -> BinfoSpender
toBinfoSpender numtxid s =
  BinfoSpender
    { txidx = encodeBinfoTxId numtxid s.txid,
      input = s.index
    }

inputToBinfoTxOutput ::
  Bool ->
  HashMap Address (Maybe BinfoXPubPath) ->
  Transaction ->
  Word32 ->
  StoreInput ->
  BinfoTxOutput
inputToBinfoTxOutput numtxid abook t n i =
  BinfoTxOutput
    { typ = 0,
      spent = True,
      txidx = encodeBinfoTxId numtxid i.outpoint.hash,
      value =
        case i of
          StoreCoinbase {} -> 0
          StoreInput {value} -> value,
      script =
        case i of
          StoreCoinbase {} -> B.empty
          StoreInput {pkscript} -> pkscript,
      address =
        case i of
          StoreCoinbase {} -> Nothing
          StoreInput {address} -> address,
      index = i.outpoint.index,
      spenders =
        [ BinfoSpender
            (encodeBinfoTxId numtxid (txHash (transactionData t)))
            n
        ],
      xpub =
        case i of
          StoreCoinbase {} -> Nothing
          StoreInput {address} -> address >>= join . flip HashMap.lookup abook
    }

data BinfoAddr
  = BinfoAddr !Address
  | BinfoXpub !XPubKey
  deriving (Eq, Show, Generic, Hashable, NFData)

parseBinfoAddr :: Network -> Ctx -> Text -> Maybe [BinfoAddr]
parseBinfoAddr net ctx "" = Just []
parseBinfoAddr net ctx s =
  mapM f $
    concatMap (filter (not . T.null) . T.splitOn ",") (T.splitOn "|" s)
  where
    f x =
      BinfoAddr <$> textToAddr net x
        <|> BinfoXpub <$> xPubImport net ctx x

data BinfoHeader = BinfoHeader
  { hash :: !BlockHash,
    timestamp :: !Timestamp,
    index :: !Word32,
    height :: !BlockHeight,
    txids :: ![BinfoTxId]
  }
  deriving (Eq, Show, Generic, NFData)

instance ToJSON BinfoHeader where
  toJSON h =
    A.object
      [ "hash" .= h.hash,
        "time" .= h.timestamp,
        "block_index" .= h.index,
        "height" .= h.height,
        "txIndexes" .= h.txids
      ]
  toEncoding h =
    A.pairs $
      mconcat
        [ "hash" `A.pair` toEncoding h.hash,
          "time" `A.pair` A.word32 h.timestamp,
          "block_index" `A.pair` A.word32 h.index,
          "height" `A.pair` A.word32 h.height,
          "txIndexes" `A.pair` A.list toEncoding h.txids
        ]

instance FromJSON BinfoHeader where
  parseJSON =
    A.withObject "BinfoHeader" $ \o -> do
      hash <- o .: "hash"
      timestamp <- o .: "time"
      index <- o .: "block_index"
      height <- o .: "height"
      txids <- o .: "txIndexes"
      return BinfoHeader {..}

newtype BinfoMempool = BinfoMempool {get :: [BinfoTx]}
  deriving (Eq, Show, Generic, NFData)

instance MarshalJSON (Network, Ctx) BinfoMempool where
  marshalValue (net, ctx) (BinfoMempool txs) =
    A.object ["txs" .= map (marshalValue (net, ctx)) txs]

  marshalEncoding (net, ctx) (BinfoMempool txs) =
    A.pairs $ A.pair "txs" $ A.list (marshalEncoding (net, ctx)) txs

  unmarshalValue (net, ctx) =
    A.withObject "BinfoMempool" $ \o ->
      BinfoMempool <$> (mapM (unmarshalValue (net, ctx)) =<< o .: "txs")

newtype BinfoBlockInfos = BinfoBlockInfos {get :: [BinfoBlockInfo]}
  deriving (Eq, Show, Generic, NFData)

instance ToJSON BinfoBlockInfos where
  toJSON b = A.object ["blocks" .= b.get]
  toEncoding b =
    A.pairs $ A.pair "blocks" $ A.list toEncoding b.get

instance FromJSON BinfoBlockInfos where
  parseJSON =
    A.withObject "BinfoBlockInfos" $ \o ->
      BinfoBlockInfos <$> o .: "blocks"
