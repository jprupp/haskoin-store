{-@ LIQUID "--reflection" @-}
{-@ LIQUID "--ple" @-}

-- | Taking transactions out of the pool, proved to be the same in any
-- order: the heart of the pool as a pure value ("Haskoin.Store.Fleet.Pool"),
-- in a module that Liquid Haskell checks (@proofs\/@).
--
-- = Why
--
-- A move of the tip marks the transactions that leave the pool, and they
-- are taken out afterwards, a few at a time, by whoever gets to them: the
-- store that moved the tip, another store, a request that has to read one
-- (@docs\/tip-first.md@). That is right only if the order does not
-- matter. For the pool in Redis, small worlds are gone through and random
-- histories are tried (@scripts\/check-pool.sh@,
-- @Haskoin.Store.Fleet.DecideSpec@). Here it is proved, for every pool
-- and every two transactions, of the pool as a pure value, which is what
-- the pool in Redis is held to.
--
-- = What is proved
--
-- The pool is three maps: what each transaction spends, which
-- transaction spends each output, and which transactions spend from each
-- transaction. 'removeCore' takes one transaction out of the three.
-- 'anyOrder' says that taking out one and then another leaves the same
-- three maps as taking them out the other way round. Any order of any
-- number of transactions is a series of such swaps, so all orders leave
-- the same.
--
-- = What is assumed
--
-- What "Data.Map" does, as far as this goes, in five laws (@lookGone@,
-- @lookLess@, @lookWout@, @lookFull@, @same@): what a lookup gives after
-- a key is deleted, after a set of keys is taken out, after a member is
-- taken out of every set, and after the empty sets are dropped; and that
-- two maps with the same lookups are the same map. Each is one line, in
-- one place. Everything else follows from them, and Liquid Haskell checks
-- that it does.
--
-- = What is not proved here
--
-- That the pool in Redis does what this value does: that is sampled
-- (@Haskoin.Store.Fleet.RedisSpec@) and checked in small worlds. And
-- nothing of what the pool keeps for addresses and tokens: in the pure
-- pool those are worked out from the transactions that it has
-- (@addresses@, @totals@), so they follow the three maps.
module Haskoin.Store.Fleet.Pool.Order
  ( Core (..),
    removeCore,
    anyOrder,
  )
where

import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set (Set)
import Data.Set qualified as Set

-- | The three maps of the pool: what each transaction spends, the
-- transaction that spends each output, and the transactions that spend
-- from each transaction.
data Core t o = Core (Map t [o]) (Map o t) (Map t (Set t))

{-@ data Core t o = Core
      { cSpends :: Map t [o],
        cSpender :: Map o t,
        cKids :: Map t (Set t)
      } @-}

-------------------------------------------------------------------------------
-- "Data.Map", as far as this goes
-------------------------------------------------------------------------------

-- A lookup, a map without a key, without a set of keys, with a member
-- taken out of every set, and without its empty sets. Each has a name in
-- the types that are checked, and the functions below are what the rest
-- of the module calls.
{-@ measure look :: Map k v -> k -> Maybe v @-}
{-@ measure gone :: Map k v -> k -> Map k v @-}
{-@ measure less :: Map k v -> Set k -> Map k v @-}
{-@ measure wout :: Map k (Set t) -> t -> Map k (Set t) @-}
{-@ measure full :: Map k (Set t) -> Map k (Set t) @-}
{-@ measure ins :: Maybe [o] -> Set o @-}

{-@ assume mLookup :: (Ord k) => x:k -> m:Map k v -> {r:Maybe v | r == look m x} @-}
mLookup :: (Ord k) => k -> Map k v -> Maybe v
mLookup = Map.lookup

{-@ assume mDelete :: (Ord k) => x:k -> m:Map k v -> {n:Map k v | n == gone m x} @-}
mDelete :: (Ord k) => k -> Map k v -> Map k v
mDelete = Map.delete

{-@ assume mLess :: (Ord k) => s:Set k -> m:Map k v -> {n:Map k v | n == less m s} @-}
mLess :: (Ord k) => Set k -> Map k v -> Map k v
mLess s m = Map.withoutKeys m s

{-@ assume mWithout :: (Ord t) => x:t -> m:Map k (Set t) -> {n:Map k (Set t) | n == wout m x} @-}
mWithout :: (Ord t) => t -> Map k (Set t) -> Map k (Set t)
mWithout x = Map.map (\s -> Set.difference s (Set.singleton x))

{-@ assume mFull :: (Ord t) => m:Map k (Set t) -> {n:Map k (Set t) | n == full m} @-}
mFull :: (Ord t) => Map k (Set t) -> Map k (Set t)
mFull = Map.filter (/= Set.empty)

-- | What a key has once a member is taken out of every set: the set that
-- it had without that member, and no set if it had none.
{-@ reflect minus @-}
minus :: (Ord t) => t -> Maybe (Set t) -> Maybe (Set t)
minus _ Nothing = Nothing
minus x (Just s) = Just (Set.difference s (Set.singleton x))

-- | What a key has once the empty sets are dropped: the set that it had
-- if that was not empty, and nothing otherwise.
{-@ reflect kept @-}
kept :: (Ord t) => Maybe (Set t) -> Maybe (Set t)
kept Nothing = Nothing
kept (Just s) = if s == Set.empty then Nothing else Just s

-- | The outputs that a transaction spends, as a set: none for a
-- transaction that the pool does not have.
{-@ assume spentBy :: (Ord o) => r:Maybe [o] -> {s:Set o | s == ins r} @-}
spentBy :: (Ord o) => Maybe [o] -> Set o
spentBy = maybe Set.empty Set.fromList

-- | Assumed: a map without a key has nothing at that key, and at any
-- other what it had.
{-@ assume lookGone :: m:Map k v -> x:k -> y:k
      -> {look (gone m x) y == (if y == x then Nothing else look m y)} @-}
lookGone :: Map k v -> k -> k -> ()
lookGone _ _ _ = ()

-- | Assumed: a map without a set of keys has nothing at a key of the set,
-- and at any other what it had.
{-@ assume lookLess :: m:Map k v -> s:Set k -> y:k
      -> {look (less m s) y == (if Set_mem y s then Nothing else look m y)} @-}
lookLess :: Map k v -> Set k -> k -> ()
lookLess _ _ _ = ()

-- | Assumed: with a member taken out of every set, a key has the set that
-- it had without that member, and no set if it had none.
{-@ assume lookWout :: m:Map k (Set t) -> x:t -> y:k -> {look (wout m x) y == minus x (look m y)} @-}
lookWout :: Map k (Set t) -> t -> k -> ()
lookWout _ _ _ = ()

-- | Assumed: without its empty sets, a map has at a key the set that it
-- had if that was not empty, and nothing otherwise.
{-@ assume lookFull :: m:Map k (Set t) -> y:k -> {look (full m) y == kept (look m y)} @-}
lookFull :: Map k (Set t) -> k -> ()
lookFull _ _ = ()

-- | Assumed: two maps that give the same at every key are the same map.
{-@ assume same :: m:Map k v -> n:Map k v -> (y:k -> {look m y == look n y}) -> {m == n} @-}
same :: Map k v -> Map k v -> (k -> ()) -> ()
same _ _ _ = ()

-------------------------------------------------------------------------------
-- Taking one transaction out
-------------------------------------------------------------------------------

-- | Take one transaction out of the three maps, leaving what spends it:
-- it spends nothing any more, nothing is spent by it, and it spends from
-- no transaction. A transaction from which nothing spends any more has no
-- set left.
{-@ removeCore :: (Ord t, Ord o) => x:t -> c:Core t o
      -> {d:Core t o | cSpends d == gone (cSpends c) x
                    && cSpender d == less (cSpender c) (ins (look (cSpends c) x))
                    && cKids d == full (wout (cKids c) x)} @-}
removeCore :: (Ord t, Ord o) => t -> Core t o -> Core t o
removeCore x (Core spends spender kids) =
  Core (mDelete x spends) (mLess (spentBy (mLookup x spends)) spender) (mFull (mWithout x kids))

-------------------------------------------------------------------------------
-- Any order
-------------------------------------------------------------------------------

-- | Two facts, both known where this is used.
both :: () -> () -> ()
both _ _ = ()

-- | Proved: at any key, deleting two keys in one order gives what
-- deleting them in the other gives.
{-@ goneAt :: (Ord k) => m:Map k v -> a:k -> b:k -> y:k
      -> {look (gone (gone m b) a) y == look (gone (gone m a) b) y} @-}
goneAt :: (Ord k) => Map k v -> k -> k -> k -> ()
goneAt m a b y =
  both
    (both (lookGone (mDelete b m) a y) (lookGone m b y))
    (both (lookGone (mDelete a m) b y) (lookGone m a y))

-- | Proved: at any key, taking two sets of keys out in one order gives
-- what taking them out in the other gives.
{-@ lessAt :: (Ord k) => m:Map k v -> s:Set k -> u:Set k -> y:k
      -> {look (less (less m u) s) y == look (less (less m s) u) y} @-}
lessAt :: (Ord k) => Map k v -> Set k -> Set k -> k -> ()
lessAt m s u y =
  both
    (both (lookLess (mLess u m) s y) (lookLess m u y))
    (both (lookLess (mLess s m) u y) (lookLess m s y))

-- | Proved: for what one key has, taking two members out, dropping the
-- set each time that it is empty, gives the same in either order. A set
-- that the first leaves empty is dropped at once, and the second then
-- finds none; taken the other way round, the second leaves a set that the
-- first empties, and it is dropped then.
{-@ swap :: (Ord t) => a:t -> b:t -> v:Maybe (Set t)
      -> {kept (minus a (kept (minus b v))) == kept (minus b (kept (minus a v)))} @-}
swap :: (Ord t) => t -> t -> Maybe (Set t) -> ()
swap _ _ Nothing = ()
swap a b (Just s)
  | withoutB == Set.empty && withoutA == Set.empty = ()
  | withoutB == Set.empty = ()
  | withoutA == Set.empty = ()
  | otherwise = ()
  where
    withoutA = Set.difference s (Set.singleton a)
    withoutB = Set.difference s (Set.singleton b)

-- | Proved: at any key, taking two transactions out of the sets of those
-- that spend from each transaction, and dropping the empty sets each
-- time, gives the same in either order.
{-@ kidsAt :: (Ord k, Ord t) => m:Map k (Set t) -> a:t -> b:t -> y:k
      -> {look (full (wout (full (wout m b)) a)) y == look (full (wout (full (wout m a)) b)) y} @-}
kidsAt :: (Ord k, Ord t) => Map k (Set t) -> t -> t -> k -> ()
kidsAt m a b y =
  both (swap a b (mLookup y m)) $
    both
    ( both
        (both (lookFull (mWithout a (mFull (mWithout b m))) y) (lookWout (mFull (mWithout b m)) a y))
        (both (lookFull (mWithout b m) y) (lookWout m b y))
    )
    ( both
        (both (lookFull (mWithout b (mFull (mWithout a m))) y) (lookWout (mFull (mWithout a m)) b y))
        (both (lookFull (mWithout a m) y) (lookWout m a y))
    )

-- | Proved: taking one transaction out and then another leaves the same
-- three maps as taking them out the other way round.
--
-- The one thing that could differ is what the second is found to spend,
-- since the first is gone by then: it does not, because a transaction
-- that is not the first one spends what it spent before
-- (@lookGone@).
{-@ anyOrder :: (Ord t, Ord o) => a:t -> b:t -> c:Core t o
      -> {r:(Core t o, Core t o) | cSpends (fst r) == cSpends (snd r)
                                && cSpender (fst r) == cSpender (snd r)
                                && cKids (fst r) == cKids (snd r)} @-}
anyOrder :: (Ord t, Ord o) => t -> t -> Core t o -> (Core t o, Core t o)
anyOrder a b c@(Core spends spender kids)
  | a == b = (removeCore a (removeCore b c), removeCore b (removeCore a c))
  | otherwise =
      const
        (removeCore a (removeCore b c), removeCore b (removeCore a c))
        ( both
            ( both
                (same (mDelete a (mDelete b spends)) (mDelete b (mDelete a spends)) (goneAt spends a b))
                (both (lookGone spends b a) (lookGone spends a b))
            )
            ( both
                ( same
                    (mLess sa (mLess sb spender))
                    (mLess sb (mLess sa spender))
                    (lessAt spender sa sb)
                )
                ( same
                    (mFull (mWithout a (mFull (mWithout b kids))))
                    (mFull (mWithout b (mFull (mWithout a kids))))
                    (kidsAt kids a b)
                )
            )
        )
  where
    sa = spentBy (mLookup a spends)
    sb = spentBy (mLookup b spends)
