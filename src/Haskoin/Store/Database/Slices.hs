{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE RecordWildCards #-}

-- |
-- Module      : Haskoin.Store.Database.Slices
-- Copyright   : No rights reserved
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Stored values read in part: what an answer needs, and nothing more.
--
-- A value of the database is decoded whole, as a rule, into the type that
-- it stores. Two of them are often read for a small part of what they hold:
--
-- * A block ('BlockData') holds the hash of every one of its transactions.
--   Most reads of a block want its height, its header or its time: the
--   health check, the best block of a request, a block without its
--   transactions. Decoding the hashes took 180 µs for a block of 3,000
--   transactions and 2.4 ms for one of 20,000, where the rest took less
--   than a microsecond.
--
-- * A transaction ('TxData') holds the transaction in the format of the
--   network, which is what a raw answer sends. Building the transaction to
--   serialize it again took ten times as long as finding its bytes, and
--   read the spenders of its outputs, which a raw answer does not need.
--
-- 'blockData' gives the block whole, with its list of transactions decoded
-- the first time that it is used: the type stays the same, and every caller
-- gains. 'txBytes' gives the bytes of the transaction, a slice of the value,
-- found by skipping over the parts of the transaction rather than building
-- them. Both are held to the full decoders by the tests.
module Haskoin.Store.Database.Slices
  ( blockData,
    txBytes,
  )
where

import Control.Monad (replicateM, replicateM_, unless, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Bytes.Get (runGetS)
import Data.Bytes.Serial (deserialize)
import Data.Serialize.Get
  ( Get,
    getBytes,
    getWord32be,
    getWord64be,
    getWord8,
    lookAhead,
    remaining,
    runGet,
    skip,
  )
import Haskoin (BlockHeader, TxHash, VarInt (..), getInteger)
import Haskoin.Store.Data (BlockData (..), BlockRef)

-- | A block as stored, decoded with its list of transactions left as bytes
-- until it is used. The list is then decoded at once, as the full decoder
-- would have: what the block holds is the same either way. Its bytes were
-- checked to be there, so decoding them later cannot fail.
blockData :: ByteString -> Either String BlockData
blockData = runGet $ do
  height <- getWord32be
  main <- deserialize
  work <- getInteger
  header <- deserialize :: Get BlockHeader
  size <- getWord32be
  weight <- getWord32be
  n <- count
  left <- remaining
  when (n > left `div` 32) $ fail "blockData: fewer bytes than transactions"
  hashes <- getBytes (32 * n)
  outputs <- getWord64be
  fee <- getWord64be
  subsidy <- getWord64be
  return BlockData {txs = later n hashes, ..}
  where
    -- the first cell of the list is made at once, as the field is strict;
    -- its head, the coinbase, which a block without its transactions still
    -- shows, is decoded by itself, and the tail all at once when it is used
    later :: Int -> ByteString -> [TxHash]
    later 0 _ = []
    later n hashes =
      decoded deserialize (B.take 32 hashes)
        : decoded (replicateM (n - 1) deserialize) (B.drop 32 hashes)
    decoded :: Get a -> ByteString -> a
    decoded parser bytes = either error id (runGetS parser bytes)

-- | The transaction of a stored 'TxData', in the format of the network, as
-- 'Data.Bytes.Serial.serialize' wrote it there, and whether the transaction
-- is deleted. The parts of the transaction are skipped over by their
-- lengths, as the decoder of haskoin-core reads them, without building
-- them; the checks of that decoder were made when the value was written.
txBytes :: ByteString -> Either String (ByteString, Bool)
txBytes value = flip runGet value $ do
  _ <- deserialize :: Get BlockRef
  before <- remaining
  skipTx
  after <- remaining
  -- the outputs that the inputs spend: a key and a script and a value each
  entries <- count
  replicateM_ entries (skip 8 >> count >>= skip >> skip 8)
  deleted <- deserialize
  let start = B.length value - before
  return (B.take (before - after) (B.drop start value), deleted)

-- | Skip a transaction in the format of the network: the old one, or the
-- one with a witness of BIP 144, told apart as the decoder of haskoin-core
-- tells them, by the flags after a marker of zero.
skipTx :: Get ()
skipTx = do
  skip 4
  flags <- lookAhead $ do
    marker <- getWord8
    if marker == 0 then Just <$> getWord8 else return Nothing
  case flags of
    Just 0x01 -> do
      skip 2
      inputs <- varCount
      replicateM_ inputs input
      varCount >>= flip replicateM_ output
      replicateM_ inputs (varCount >>= flip replicateM_ (varCount >>= skip))
      skip 4
    Just 0x00 -> legacy
    Nothing -> legacy
    Just other -> fail ("txBytes: unknown flags " <> show other)
  where
    legacy = do
      varCount >>= flip replicateM_ input
      varCount >>= flip replicateM_ output
      skip 4
    -- an outpoint, a script and a sequence
    input = skip 36 >> varCount >>= skip >> skip 4
    -- a value and a script
    output = skip 8 >> varCount >>= skip

-- | A count of the network's format, as a variable integer.
varCount :: Get Int
varCount = do
  VarInt n <- deserialize
  unless (n <= fromIntegral (maxBound :: Int)) $ fail "txBytes: count too large"
  return (fromIntegral n)

-- | A 64-bit count of the store's format, that must fit an 'Int'.
count :: Get Int
count = do
  n <- getWord64be
  unless (n <= fromIntegral (maxBound :: Int)) $ fail "count too large"
  return (fromIntegral n)
