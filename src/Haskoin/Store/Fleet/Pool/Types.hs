-- |
-- Module      : Haskoin.Store.Fleet.Pool.Types
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- What goes to the shared mempool in Redis and comes back from it, as the
-- stores of a fleet agree on it: transactions with what they do at
-- addresses and to tokens, what blocks did to the pool, and what became of
-- an offer or of a move of the tip. In a module of its own so that the
-- decisions ("Haskoin.Store.Fleet.Pool.Decide") and what carries them out
-- ("Haskoin.Store.Fleet.Redis") share it.
module Haskoin.Store.Fleet.Pool.Types
  ( Moved (..),
    Offered (..),
    PoolEntry (..),
    PoolTokenOut (..),
    PoolTx (..),
    PoolChange (..),
  )
where

import Data.ByteString (ByteString)
import Data.Word (Word32, Word64)

-- | What became of an attempt to move the tip; see
-- t'Haskoin.Store.Fleet.Move'.
data Moved
  = -- | the tip is now this block
    MovedTo !ByteString
  | -- | the tip is where the live stores agree, or they agree on nothing
    Stays
  | -- | another store moved the tip first
    Raced
  deriving (Eq, Show)

-- | What became of an offer; see t'Haskoin.Store.Fleet.Pool.Offer'.
data Offered
  = -- | it is in the pool now
    Taken
  | -- | it was there already
    Known
  | -- | these transactions of the pool spend what it spends
    Conflict ![ByteString]
  | -- | these outputs are neither made by the pool nor unspent at the tip
    Orphan ![ByteString]
  | -- | the tip is not the one that the store checked the transaction
    -- against: check again and offer again
    Late
  deriving (Eq, Show)

-- | What a transaction does at an address; see
-- t'Haskoin.Store.Fleet.Pool.Entry'. Addresses, outputs and scripts are
-- whatever bytes the stores of a fleet agree on.
data PoolEntry
  = -- | it spends from the address or pays to it
    PoolTouches !ByteString
  | -- | it spends this output of the address, of this amount
    PoolSpends !ByteString !ByteString !Word64
  | -- | it pays this output to the address, with this amount and script
    PoolPays !ByteString !ByteString !Word64 !ByteString
  | -- | it makes an output that holds tokens (CashTokens)
    PoolMakes !PoolTokenOut
  | -- | it uses up an output that holds tokens
    PoolUses !PoolTokenOut
  deriving (Eq, Show)

-- | An output that holds tokens, as an entry of the pool gives it, so that
-- the scripts in Redis never read the token prefix of a script.
data PoolTokenOut = PoolTokenOut
  { -- | the address of the output, if it has one
    address :: !(Maybe ByteString),
    output :: !ByteString,
    category :: !ByteString,
    -- | fungible tokens
    amount :: !Word64,
    -- | whether it also holds a non-fungible token
    nft :: !Bool,
    -- | the value of the output and its script: what the pool keeps of an
    -- output that it makes, to list it with the category. Not given for
    -- an output that it uses up.
    coins :: !(Maybe (Word64, ByteString))
  }
  deriving (Eq, Show)

-- | A transaction as the pool keeps it.
data PoolTx = PoolTx
  { txid :: !ByteString,
    -- | when it entered the mempool of the store that offers it. The pool
    -- keeps the time of the first offer, so that every store lists the
    -- transaction in the same place.
    time :: !Word64,
    -- | the outputs that it spends, in the order of its inputs
    inputs :: ![ByteString],
    -- | what a store needs to answer about it
    body :: !ByteString,
    -- | what it does at each address
    entries :: ![PoolEntry]
  }
  deriving (Eq, Show)

-- | What the blocks between two tips did; see
-- 'Haskoin.Store.Fleet.Pool.Change'.
data PoolChange = PoolChange
  { -- | transactions that come back
    returned :: ![PoolTx],
    -- | transactions of abandoned blocks that cannot come back
    gone :: ![ByteString],
    -- | transactions that the new blocks confirm
    confirmed :: ![ByteString],
    -- | outputs that the new blocks spend, each followed by the transaction
    -- of the block that spends it and the height of that block in four
    -- bytes, big-endian: what replaces a transaction of the pool that
    -- spends it ('Haskoin.Store.Fleet.Pool.advanceReplacing')
    spent :: ![ByteString],
    -- | the height of the block that the tip moves to, by which what was
    -- replaced 2016 blocks before is forgotten, if it is known
    height :: !(Maybe Word32),
    -- | transactions of the abandoned blocks that the store has as
    -- replaced, each followed like an output of 'spent' by what replaced
    -- it: kept as replaced once the tip reaches it
    lost :: ![ByteString]
  }
  deriving (Eq, Show)
