-- |
-- Module      : Haskoin.Store.Fleet.Pool
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The mempool that stores sharing one Redis answer from, as pure decisions:
-- the second part of "A fleet that answers as one" in
-- @docs\/layered-database.md@, after the fleet tip of "Haskoin.Store.Fleet".
-- Nothing carries it out against Redis yet.
--
-- = The problem
--
-- Every store has a mempool of its own, filled in the order in which its
-- peers sent it transactions, and no two are the same. Stores that answer
-- from the same block still answer differently if each lays its own mempool
-- on top.
--
-- = One pool, the mempool as of the fleet tip
--
-- The stores keep one pool in Redis and all answer from it. It goes with
-- the fleet tip: it holds transactions that are valid on top of the chain
-- up to that block, and it changes with the tip in the same step, so that
-- no request sees a block without what it did to the pool.
--
-- A store's own mempool does not go away. It is what the store offers to
-- the pool and relays to its peers, and what it answers from when Redis is
-- down.
--
-- = The rules
--
-- * __A store offers what it has__ ('offer'). Each input of the transaction
--   must spend an output of a transaction of the pool, or one that the store
--   finds unspent in its view at the fleet tip. A store that is ahead of the
--   tip holds back what only makes sense after its newer blocks.
--
-- * __The first of two that spend the same output stays__, unless the store
--   that offers the second names the first as replaced, having replaced it
--   by its own rules. This is a rule, not a truth, as it is for a node that
--   keeps the first of two conflicting transactions that it hears of.
--
-- * __A transaction leaves only for a reason__ ('withdraw', 'advance'): it
--   was replaced, it is too old, or the tip moved to a block that confirms
--   it or spends what it spends. A store that simply does not have a
--   transaction leaves it alone, because another store has it.
--
-- * __Whatever goes takes along what spends it.__ A transaction whose
--   parent left unconfirmed spends an output that does not exist.
--
-- * __The pool follows the tip__ ('advance'). The store that moves the tip
--   says what the blocks in between did: which transactions they confirmed
--   and which outputs they spent, and, across a reorganisation, which
--   transactions of the abandoned blocks come back and which are gone for
--   good, as their coinbases are.
--
-- Transactions and outputs can be of any type that can be compared; the
-- pool only needs to know which transaction made an output.
module Haskoin.Store.Fleet.Pool
  ( -- * The pool
    Pool (..),
    empty,
    member,
    descendants,

    -- * Offering and withdrawing
    Offer (..),
    Refusal (..),
    offer,
    withdraw,

    -- * Following the tip
    Change (..),
    advance,
  )
where

import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (mapMaybe)
import Data.Set (Set)
import Data.Set qualified as Set

-- | The shared mempool, for transactions of type @t@ and outputs of type
-- @o@. The second and third fields follow from the first, and are kept so
-- that nothing has to be searched for.
data Pool t o = Pool
  { -- | the outputs that each transaction spends
    spends :: !(Map t [o]),
    -- | the transaction that spends each output that is spent
    spender :: !(Map o t),
    -- | the transactions of the pool that spend outputs of each transaction,
    -- whether that one is in the pool or in the chain: a confirmed
    -- transaction can come back in a reorganisation, and what spends it must
    -- then be found again
    children :: !(Map t (Set t))
  }
  deriving (Eq, Show)

-- | A pool with nothing in it.
empty :: Pool t o
empty = Pool {spends = Map.empty, spender = Map.empty, children = Map.empty}

-- | Whether a transaction is in the pool.
member :: (Ord t) => t -> Pool t o -> Bool
member t pool = Map.member t pool.spends

-- | The transactions of the pool that spend outputs of the given ones, or
-- of those in turn, with the given ones that are in the pool.
descendants :: (Ord t) => [t] -> Pool t o -> Set t
descendants from pool = go Set.empty (filter (`member` pool) from)
  where
    go seen [] = seen
    go seen (t : rest)
      | Set.member t seen = go seen rest
      | otherwise = go (Set.insert t seen) (maybe [] Set.toList (Map.lookup t pool.children) <> rest)

-- | Why a transaction was not taken.
data Refusal t o
  = -- | it is in the pool already, which is no harm
    Known
  | -- | these transactions of the pool spend what it spends, and were there
    -- first
    Conflict ![t]
  | -- | these inputs spend outputs that are neither made by the pool nor
    -- unspent at the tip
    Orphan ![o]
  deriving (Eq, Show)

-- | What became of an offer.
data Offer t o
  = Taken !(Pool t o)
  | Refused !(Refusal t o)
  deriving (Eq, Show)

-- | Offer a transaction to the pool, given the transaction that made each
-- output, a test for the outputs that the offering store finds unspent in
-- its view at the fleet tip, the outputs that the transaction spends, and
-- the transactions of the pool that the store replaced with it.
--
-- Those leave first, with what spends them. The transaction is then taken
-- if nothing left in the pool spends what it spends, and if each of its
-- inputs is made by the pool or unspent at the tip.
offer :: (Ord t, Ord o) => (o -> t) -> (o -> Bool) -> t -> [o] -> [t] -> Pool t o -> Offer t o
offer maker unspent t inputs replaced pool0
  | member t pool0 = Refused Known
  | not (null rivals) = Refused (Conflict rivals)
  | not (null orphans) = Refused (Orphan orphans)
  | otherwise = Taken (insert maker t inputs pool)
  where
    pool = withdraw replaced pool0
    rivals = Set.toList (Set.fromList (mapMaybe (`Map.lookup` pool.spender) inputs))
    orphans = [o | o <- inputs, not (member (maker o) pool), not (unspent o)]

-- | Put a transaction in, without asking.
insert :: (Ord t, Ord o) => (o -> t) -> t -> [o] -> Pool t o -> Pool t o
insert maker t inputs pool =
  Pool
    { spends = Map.insert t inputs pool.spends,
      spender = foldl' (\m o -> Map.insert o t m) pool.spender inputs,
      children = foldl' (\m o -> Map.insertWith Set.union (maker o) (Set.singleton t) m) pool.children inputs
    }

-- | Take transactions out of the pool, with everything that spends them.
withdraw :: (Ord t, Ord o) => [t] -> Pool t o -> Pool t o
withdraw ts pool = foldl' (flip remove) pool (Set.toList (descendants ts pool))

-- | Take one transaction out, leaving what spends it.
remove :: (Ord t, Ord o) => t -> Pool t o -> Pool t o
remove t pool =
  Pool
    { spends = Map.delete t pool.spends,
      spender = foldl' (flip Map.delete) pool.spender inputs,
      children = Map.filter (not . Set.null) (Map.map (Set.delete t) pool.children)
    }
  where
    inputs = Map.findWithDefault [] t pool.spends

-- | What the blocks between two tips did, as far as the pool is concerned.
data Change t o = Change
  { -- | transactions of abandoned blocks that can be unconfirmed again, with
    -- what they spend, in an order in which each comes after those it spends
    returned :: ![(t, [o])],
    -- | transactions of abandoned blocks that cannot, such as coinbases:
    -- their outputs no longer exist
    gone :: ![t],
    -- | transactions that the new blocks confirm
    confirmed :: ![t],
    -- | outputs that the transactions of the new blocks spend
    spent :: ![o]
  }
  deriving (Eq, Show)

-- | Bring the pool from one tip to another.
--
-- The abandoned blocks come first: their transactions that can come back
-- do, and what spends the others leaves. Then the new blocks: what they
-- confirm leaves the pool, alone, since what spends it now spends an output
-- of the chain; and what spends an output that they spend leaves with
-- everything that spends it.
advance :: (Ord t, Ord o) => (o -> t) -> Change t o -> Pool t o -> Pool t o
advance maker change pool0 = clashed
  where
    back = foldl' (\p (t, inputs) -> if member t p then p else insert maker t inputs p) pool0 change.returned
    orphaned = concatMap (\t -> maybe [] Set.toList (Map.lookup t back.children)) change.gone
    alive = withdraw orphaned back
    settled = foldl' (flip remove) alive change.confirmed
    clashed = withdraw (mapMaybe (`Map.lookup` settled.spender) change.spent) settled
