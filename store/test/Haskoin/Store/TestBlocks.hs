{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Regtest blocks for tests. Their headers carry valid proof of work, so
-- they pass haskoin-node's header checks as well as the store's import.
module Haskoin.Store.TestBlocks
  ( nextBlock,
    coinbaseTx,
    script,
  )
where

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.List (find)
import Data.Maybe (fromMaybe)
import Data.Serialize (encode)
import Haskoin
  ( Block (..),
    BlockHeader (..),
    BlockHeight,
    BlockNode (..),
    Network,
    Tx (..),
    TxIn (..),
    TxOut (..),
    buildMerkleRoot,
    headerHash,
    isValidPOW,
    nullOutPoint,
    txHash,
  )

-- | The next block, 600 seconds after its parent, with a coinbase in front
-- of the given transactions.
nextBlock :: Network -> BlockHeight -> BlockHeader -> [Tx] -> (Block, BlockNode)
nextBlock net height parent txs =
  (Block header all_txs, BlockNode header (height + 1) 0 prev)
  where
    prev = headerHash parent
    all_txs = coinbaseTx (height + 1) : txs
    merkle = buildMerkleRoot (map txHash all_txs)
    candidate = BlockHeader 0x20000000 prev merkle (parent.timestamp + 600) parent.bits
    -- Regtest accepts about half of all hashes, so the nonce search is short.
    header =
      fromMaybe (error "No nonce satisfies regtest proof of work") $
        find (isValidPOW net) (map candidate [0 ..])

-- | Coinbase of the block at this height.
coinbaseTx :: BlockHeight -> Tx
coinbaseTx h = Tx 1 [TxIn nullOutPoint (encode h) maxBound] [TxOut 5000000000 script] [] 0

-- | Every output pays to this pay-to-public-key-hash script, so that address
-- balances are kept for all of them.
script :: ByteString
script = "\x76\xa9\x14" <> B.replicate 20 0x11 <> "\x88\xac"
