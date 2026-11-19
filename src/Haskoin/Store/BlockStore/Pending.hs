-- |
-- Module      : Haskoin.Store.BlockStore.Pending
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The transactions that the block store asked its peers for and that have
-- not arrived yet, as a pure value with transitions.
--
-- A transaction that a peer announces is requested once, and not again
-- while the request is fresh: another peer that announces it meanwhile is
-- not asked. When it arrives it is no longer a request ('answered'), and
-- goes on to the pool of the fleet. A request that no peer answers is
-- forgotten after 'requestTimeout', so that the next announcement asks
-- again.
--
-- The block store holds no transaction here: what arrives is offered to
-- the pool at once, and what waits for its parents waits in the fleet
-- member ("Haskoin.Store.Fleet.Member").
--
-- = Why a pure value
--
-- The block store kept the recent requests in a variable changed by
-- several functions, and forgot each request in a thread of its own that
-- slept for twenty seconds: one thread per transaction requested. Here
-- every change is a function, the clock is an argument, and requests are
-- forgotten by 'prune' on the clock tick of the block store.
module Haskoin.Store.BlockStore.Pending
  ( -- * State
    Pending,
    empty,
    isPending,
    size,

    -- * Transitions
    requested,
    answered,
    prune,

    -- * Timeouts
    requestTimeout,
  )
where

import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime)
import Haskoin (TxHash)

-- | Transactions requested from peers and not received yet, with when.
newtype Pending = Pending (HashMap TxHash UTCTime)
  deriving stock (Show, Eq)

-- | How long a request keeps the transaction from being requested again.
requestTimeout :: NominalDiffTime
requestTimeout = 20

-- | Nothing requested.
empty :: Pending
empty = Pending HashMap.empty

-- | Whether a transaction was requested recently, so that it is not
-- requested again.
isPending :: TxHash -> Pending -> Bool
isPending h (Pending asked) = HashMap.member h asked

-- | How many transactions are requested and not received yet.
size :: Pending -> Int
size (Pending asked) = HashMap.size asked

-- | Transactions were requested.
requested :: UTCTime -> [TxHash] -> Pending -> Pending
requested now hs (Pending asked) = Pending (HashMap.union (HashMap.fromList [(h, now) | h <- hs]) asked)

-- | A transaction that was requested arrived, to be offered to the pool
-- of the fleet: it is no longer a request.
answered :: TxHash -> Pending -> Pending
answered h (Pending asked) = Pending (HashMap.delete h asked)

-- | Forget requests older than 'requestTimeout'.
prune :: UTCTime -> Pending -> Pending
prune now (Pending asked) = Pending (HashMap.filter (\t -> now `diffUTCTime` t <= requestTimeout) asked)
