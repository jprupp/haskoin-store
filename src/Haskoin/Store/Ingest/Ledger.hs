-- |
-- Module      : Haskoin.Store.Ingest.Ledger
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Where each transaction id is on its way to the pool, and the only
-- functions that can put one there or take it away. It is the part of
-- "Haskoin.Store.Ingest" that two of its guarantees rest on
-- (@docs\/mempool-ingestion.md@), kept apart and small so that they can be
-- __proved for every state__, by Liquid Haskell, and not only checked in
-- small worlds:
--
-- * __T1, nothing leaves in silence.__ A t'Ledger' is abstract: the machine
--   cannot build one or reach into one. An id loses its place through
--   'leave' alone, and 'leave' says so, with an output, in the same call.
--   Every other function here keeps every id. So whatever the machine
--   does, an id that it had and no longer has was said to be 'Gone'.
--
-- * __T2, the bounds.__ A ledger never has more ids than its room, nor
--   more transactions held than its room for those. 'put' is the one
--   function that adds, and it refuses when there is no room.
--
-- The refinement types in the comments that start with @{-\@@ state this,
-- and @cabal build@ in @proofs\/@ checks them. Liquid Haskell does not see
-- through the dot of a record field, so this module reads its records by
-- matching on them, unlike the rest of the store.
--
-- What is assumed, not proved, is what "Data.Map" does: that an insert
-- adds its key and at most one to the size, that a delete takes its key
-- away, that a lookup finds a key exactly when it is there. Those
-- assumptions are the @assume@ lines below.
module Haskoin.Store.Ingest.Ledger
  ( -- * The types of the machine
    Tick,
    Place (..),
    Hold (..),
    Output (..),
    End (..),
    Reason (..),
    goneOf,

    -- * The ledger
    Ledger,
    newLedger,
    placesOf,
    heldOf,
    hasPlace,

    -- * Changing it
    put,
    retarget,
    rehold,
    retime,
    leave,
  )
where

import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust)
import Data.Set (Set)
import Data.Set qualified as Set

-- | A moment, in milliseconds of a clock that does not go back.
type Tick = Int

-- | Where an id is.
data Place p h b
  = -- | not settled, and to request from one of these peers, each with
    -- the time since which it is wanted of that peer
    Wanted !(Map p Tick)
  | -- | requested of a peer at a time; the others named it too
    Asked !p !Tick !(Set p)
  | -- | its body is here, since a time
    Held !b !Tick !(Hold h)
  deriving (Eq, Ord, Show)

-- | What a transaction that is held waits for.
data Hold h
  = -- | to be offered at the next occasion
    Ready
  | -- | the pool's verdict on an offer
    Offering
  | -- | these transactions that it spends from, and a time to offer it
    -- again whether they came or not
    Waiting !(Set h) !Tick
  | -- | the next tick, after an offer that could not be judged
    Later
  deriving (Eq, Ord, Show)

-- | What the runner is to do.
data Output p h b
  = -- | request these transactions of a peer (@getdata@)
    Get !p ![h]
  | -- | ask a peer for its mempool (@mempool@)
    Sweep !p
  | -- | find out which of these, that a peer named, the pool has decided
    -- on
    Check !p ![h]
  | -- | offer a transaction to the pool
    Offer !h !b
  | -- | an id left the machine, and how
    Gone !h !End
  deriving (Eq, Ord, Show)

-- | How an id left.
data End
  = -- | the pool decided on it
    Done
  | -- | given up
    GaveUp !Reason
  deriving (Eq, Ord, Show)

-- | Why an id was given up.
data Reason
  = -- | every peer that named it says it does not have it
    NodeHasNot
  | -- | a request for it got no answer, and no other peer named it
    Unanswered
  | -- | there was no room for it
    NoRoom
  | -- | the peers that named it are gone
    PeerGone
  | -- | what it spends from did not come
    WaitedTooLong
  | -- | the pool could not say whether it had decided on it
    Unjudged
  deriving (Eq, Ord, Show, Enum, Bounded)

-- What Liquid Haskell is told of maps: their keys and their size, and
-- what the functions used here do to them. The assumptions are on
-- functions of this module that only name those of "Data.Map": on the
-- functions of "Data.Map" themselves they would hold for every map of
-- every module that imports this one, and Liquid Haskell would carry
-- them through all of the machine.

{-@ measure mkeys :: Map k v -> Set k @-}

{-@ measure msize :: Map k v -> Int @-}

{-@ assume mEmpty :: {m:Map k v | Set_emp (mkeys m) && msize m = 0} @-}
mEmpty :: Map k v
mEmpty = Map.empty

{-@ assume mSize :: m:Map k v -> {n:Nat | n = msize m} @-}
mSize :: Map k v -> Int
mSize = Map.size

{-@ assume mLookup :: (Ord k) => x:k -> m:Map k v -> {r:Maybe v | isJust r <=> Set_mem x (mkeys m)} @-}
mLookup :: (Ord k) => k -> Map k v -> Maybe v
mLookup = Map.lookup

{-@ assume mInsert :: (Ord k) => x:k -> v -> m:Map k v
      -> {n:Map k v | mkeys n = Set_cup (mkeys m) (Set_sng x)
                      && msize n = (if (Set_mem x (mkeys m)) then (msize m) else (msize m + 1))} @-}
mInsert :: (Ord k) => k -> v -> Map k v -> Map k v
mInsert = Map.insert

{-@ assume mDelete :: (Ord k) => x:k -> m:Map k v
      -> {n:Map k v | mkeys n = Set_dif (mkeys m) (Set_sng x) && msize n <= msize m && 0 <= msize n} @-}
mDelete :: (Ord k) => k -> Map k v -> Map k v
mDelete = Map.delete

{-@ assume mAdjust :: (Ord k) => (v -> v) -> k -> m:Map k v
      -> {n:Map k v | mkeys n = mkeys m && msize n = msize m} @-}
mAdjust :: (Ord k) => (v -> v) -> k -> Map k v -> Map k v
mAdjust = Map.adjust

{-@ assume mMap :: (v -> w) -> m:Map k v -> {n:Map k w | mkeys n = mkeys m && msize n = msize m} @-}
mMap :: (v -> w) -> Map k v -> Map k w
mMap = Map.map

-- | The places of the ids, with how many of them are transactions held,
-- within the room for each. The fields are not strict: Liquid Haskell
-- cannot refine a constructor whose numbers GHC unpacks. Every function
-- here builds a ledger from values that it has already looked at.
data Ledger p h b = Ledger Int Int (Map h (Place p h b)) Int
  deriving (Eq, Ord, Show)

{-@ data Ledger p h b = Ledger
      { lRoom :: Nat,
        lHeldRoom :: Nat,
        lPlaces :: {m:Map h (Place p h b) | msize m <= lRoom},
        lHeld :: {n:Nat | n <= lHeldRoom}
      } @-}

-- | The id that an output says is gone, if it says so of one: what the
-- proof of 'leave' is stated with.
{-@ measure goneOf @-}
goneOf :: Output p h b -> Set h
goneOf (Gone h _) = Set.singleton h
goneOf (Get _ _) = Set.empty
goneOf (Sweep _) = Set.empty
goneOf (Check _ _) = Set.empty
goneOf (Offer _ _) = Set.empty

-- | A ledger with nothing in it, with room for so many ids, and for so
-- many transactions held.
{-@ newLedger :: Nat -> Nat -> Ledger p h b @-}
newLedger :: Int -> Int -> Ledger p h b
newLedger room heldRoom = Ledger room heldRoom mEmpty 0

-- | The places of the ids.
{-@ placesOf :: Ledger p h b -> Map h (Place p h b) @-}
placesOf :: Ledger p h b -> Map h (Place p h b)
placesOf (Ledger _ _ places _) = places

-- | How many transactions are held.
{-@ heldOf :: l:Ledger p h b -> {n:Nat | n <= lHeldRoom l} @-}
heldOf :: Ledger p h b -> Int
heldOf (Ledger _ _ _ held) = held

-- | Whether an id has a place.
{-@ hasPlace :: (Ord h) => h -> Ledger p h b -> Bool @-}
hasPlace :: (Ord h) => h -> Ledger p h b -> Bool
hasPlace h (Ledger _ _ places _) = isJust (mLookup h places)

isHeld :: Place p h b -> Bool
isHeld (Held {}) = True
isHeld _ = False

-- | Give an id a place, or another place: 'Nothing' when there is no room
-- for one more id, or for one more transaction held. No id loses its
-- place.
{-@ put :: (Ord h) => x:h -> Place p h b -> l:Ledger p h b
      -> Maybe ({n:Ledger p h b | mkeys (lPlaces n) = Set_cup (mkeys (lPlaces l)) (Set_sng x)}) @-}
put :: (Ord h) => h -> Place p h b -> Ledger p h b -> Maybe (Ledger p h b)
put h new (Ledger room heldRoom places held) =
  case mLookup h places of
    Nothing
      | mSize places >= room -> Nothing
      | isHeld new ->
          if held >= heldRoom
            then Nothing
            else Just (Ledger room heldRoom (mInsert h new places) (held + 1))
      | otherwise -> Just (Ledger room heldRoom (mInsert h new places) held)
    Just old
      | isHeld new && not (isHeld old) ->
          if held >= heldRoom
            then Nothing
            else Just (Ledger room heldRoom (mInsert h new places) (held + 1))
      | isHeld old && not (isHeld new) ->
          Just (Ledger room heldRoom (mInsert h new places) (if held > 0 then held - 1 else 0))
      | otherwise -> Just (Ledger room heldRoom (mInsert h new places) held)

-- | Another place for an id that has one and is not a transaction held,
-- when the new place is not one of those either: wanted, or asked of a
-- peer. Anything else is left as it is. Every id keeps a place.
{-@ retarget :: (Ord h) => h -> Place p h b -> l:Ledger p h b
      -> {n:Ledger p h b | mkeys (lPlaces n) = mkeys (lPlaces l)} @-}
retarget :: (Ord h) => h -> Place p h b -> Ledger p h b -> Ledger p h b
retarget h new (Ledger room heldRoom places held)
  | isHeld new = Ledger room heldRoom places held
  | otherwise = Ledger room heldRoom (mAdjust (\old -> if isHeld old then old else new) h places) held

-- | What a transaction that is held waits for. An id that is not held is
-- left as it is. Every id keeps a place.
{-@ rehold :: (Ord h) => h -> Hold h -> l:Ledger p h b
      -> {n:Ledger p h b | mkeys (lPlaces n) = mkeys (lPlaces l)} @-}
rehold :: (Ord h) => h -> Hold h -> Ledger p h b -> Ledger p h b
rehold h hold (Ledger room heldRoom places held) =
  Ledger room heldRoom (mAdjust held' h places) held
  where
    held' (Held b since _) = Held b since hold
    held' other = other

-- | Every time in the ledger, changed by a function. Every id keeps its
-- place.
{-@ retime :: (Tick -> Tick) -> l:Ledger p h b
      -> {n:Ledger p h b | mkeys (lPlaces n) = mkeys (lPlaces l)} @-}
retime :: (Tick -> Tick) -> Ledger p h b -> Ledger p h b
retime f (Ledger room heldRoom places held) = Ledger room heldRoom (mMap place places) held
  where
    place (Wanted ms) = Wanted (Map.map f ms)
    place (Asked p t ps) = Asked p (f t) ps
    place (Held b since hold) = Held b (f since) (waits hold)
    waits (Waiting ps t) = Waiting ps (f t)
    waits other = other

-- | The one way out: an id loses its place, and an output says so and how,
-- at the head of those said so far. An id that had no place is said to be
-- gone too: it was learned and let go in one step.
{-@ leave :: (Ord h) => x:h -> End -> l:Ledger p h b -> os:[Output p h b]
      -> ( {n:Ledger p h b | mkeys (lPlaces n) = Set_dif (mkeys (lPlaces l)) (Set_sng x)},
           {ns:[Output p h b] | goneOf (head ns) == Set_sng x && tail ns == os} ) @-}
leave :: (Ord h) => h -> End -> Ledger p h b -> [Output p h b] -> (Ledger p h b, [Output p h b])
leave h end (Ledger room heldRoom places held) said =
  (Ledger room heldRoom (mDelete h places) held', Gone h end : said)
  where
    held' = case mLookup h places of
      Just old | isHeld old, held > 0 -> held - 1
      _ -> held
