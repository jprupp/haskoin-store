{-@ LIQUID "--reflection" @-}
{-@ LIQUID "--ple" @-}
-- 'onKey', 'onCell' and 'underWhat' say which key, which write and which
-- mark a write has, for the kind of write that has one: the types that
-- Liquid Haskell checks let them be asked of that kind only.
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

-- |
-- Module      : Haskoin.Store.Fleet.Atomic.Cells
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- What a write does, said once: the part of "Haskoin.Store.Fleet.Atomic"
-- that Liquid Haskell checks (@proofs\/@ of the repository).
--
-- = The problem
--
-- A decision reads back what it has written. So what each write does was
-- written twice: in the model of Redis, which says what Redis holds after
-- a write, and in the function that changes an answer of Redis by the
-- writes that came after it. If the two differed, a decision would see
-- something else than Redis would have said, and decide wrongly without
-- anything failing. That they agreed was tested on drawn cases.
--
-- = What is done instead
--
-- Redis is a map from keys to what they hold, and a hash, a set and a
-- sorted set are maps too: from fields to values, from members to nothing,
-- from members to scores. Each is a __map of cells__, and a write is to
-- the cell of one key ('On'), to every cell ('All'), or to the cells of
-- the keys that something marks ('Under'), as a wipe is to the keys that
-- start with some bytes.
--
-- What a write does to one cell is a function, such as 'cellAfter'. Both
-- sides use the same one: the model changes the cell of the key that the
-- write names with it ('mapAfter'), and a decision takes the answer that
-- it has for a cell through it ('at'). So there is one statement of what
-- @HSET@ or @ZADD NX@ does.
--
-- = What is proved
--
-- What is left to get wrong is the map around the cells: that the write
-- reaches the cell of its key and no other. That is the theorem here,
-- 'cellAfterAll': for any function of a cell, any key, and any writes one
-- after the other, the cell of the key in the map after them is what the
-- cell was, taken through 'at' for each write. Liquid Haskell checks it
-- for every case, by 'step' for one write and by induction for a list.
--
-- 'thereAtAllSame' is the same for a question that only asks whether a
-- cell holds something, as @HEXISTS@ and @SISMEMBER@ do: that is known
-- after any writes from whether it did before, without what it held.
--
-- = What it assumes
--
-- Four facts of "Data.Map", written as @assume@ lines: that a lookup after
-- a cell was put gives that cell at its key ('selSame') and what was there
-- at any other ('selOther'), that an empty map has no cell ('selNone'),
-- and that a map filtered by its keys has the cells of the keys that stay
-- ('selLess'). And the arithmetic of @INCRBY@ is not looked into: it is a
-- function given from outside, the same for both sides.
--
-- The bytes are a type of any kind here, with nothing asked of it but
-- that two can be compared: the theorems hold for whatever they are.
module Haskoin.Store.Fleet.Atomic.Cells
  ( -- * Cells
    CellOp (..),
    Add,
    cellAfter,
    thereAfter,
    RankOp (..),
    rankAfter,

    -- * A map of cells
    MapOp (..),
    at,
    atAll,
    mapAfter,
    thereAt,
    thereAtAll,

    -- * What is assumed of "Data.Map"
    selSame,
    selOther,
    selNone,
    selLess,

    -- * What a write is, for the types that are checked
    isOn,
    isAll,
    isUnder,
    onKey,
    onCell,
    underWhat,

    -- * What is proved
    step,
    cellAfterAll,
    thereStep,
    thereAtStep,
    thereAtAllSame,
  )
where

import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust)

-------------------------------------------------------------------------------
-- cells
-------------------------------------------------------------------------------

-- | A write to one cell that holds bytes: put bytes in it, take out what
-- it holds, or add to the number that it holds.
data CellOp b = Put b | Clear | Plus Integer

-- | What adding a number to what a cell holds, or to nothing, gives: the
-- arithmetic of @INCRBY@, given from outside and not looked into here.
type Add b = Integer -> Maybe b -> b

-- | What a cell holds after a write, from what it held.
{-@ reflect cellAfter @-}
cellAfter :: Add b -> CellOp b -> Maybe b -> Maybe b
cellAfter _ (Put x) _ = Just x
cellAfter _ Clear _ = Nothing
cellAfter add (Plus n) v = Just (add n v)

-- | Whether there is something in a cell after a write, from whether there was.
{-@ reflect thereAfter @-}
thereAfter :: CellOp b -> Bool -> Bool
thereAfter (Put _) _ = True
thereAfter Clear _ = False
thereAfter (Plus _) _ = True

-- | Whether a cell holds something after a write is what 'thereAfter'
-- says, from whether it did.
{-@ thereStep :: add:Add b -> c:CellOp b -> v:Maybe b -> {isJust (cellAfter add c v) == thereAfter c (isJust v)} @-}
thereStep :: Add b -> CellOp b -> Maybe b -> ()
thereStep _ (Put _) _ = ()
thereStep _ Clear _ = ()
thereStep _ (Plus _) _ = ()

-- | A write to the score of one member of a sorted set: give it a score,
-- give it one only if it has none, as @ZADD NX@ does, or take the member
-- out.
data RankOp s = Rank s | RankNew s | Unrank

-- | The score of a member after a write, from the score that it had.
{-@ reflect rankAfter @-}
rankAfter :: RankOp s -> Maybe s -> Maybe s
rankAfter (Rank s) _ = Just s
rankAfter (RankNew s) v = if isJust v then v else Just s
rankAfter Unrank _ = Nothing

-------------------------------------------------------------------------------
-- a map of cells
-------------------------------------------------------------------------------

-- | A write to a map of cells: to the cell of one key, to every cell, or to
-- the cells of the keys that something marks.
data MapOp p k c = On k c | All | Under p

-- | The cell of a key after a write to the map, from what the cell was:
-- what a decision takes its answer through. Given are what marks a key
-- and what a write does to a cell.
{-@ reflect at @-}
at :: (Eq k) => (p -> k -> Bool) -> (c -> Maybe v -> Maybe v) -> k -> MapOp p k c -> Maybe v -> Maybe v
at _ cell x (On y c) v = if y == x then cell c v else v
at _ _ _ All _ = Nothing
at marks _ x (Under p) v = if marks p x then Nothing else v

-- | 'at' for writes one after the other.
{-@ reflect atAll @-}
atAll :: (Eq k) => (p -> k -> Bool) -> (c -> Maybe v -> Maybe v) -> k -> [MapOp p k c] -> Maybe v -> Maybe v
atAll _ _ _ [] v = v
atAll marks cell x (op : ops) v = atAll marks cell x ops (at marks cell x op v)

-- "Data.Map", as far as this goes: a lookup, a cell put at a key or taken
-- from it, the empty map, and a map without the keys that something
-- marks. Each has a name in the types that are checked ('sel', 'put',
-- 'none', 'less'), and the functions below are what the rest of the
-- module calls, so that what is assumed of "Data.Map" is in these lines
-- and nowhere else.
{-@ measure sel :: Map k v -> k -> Maybe v @-}
{-@ measure put :: Map k v -> k -> Maybe v -> Map k v @-}
{-@ measure none :: Map k v -> Bool @-}
{-@ measure less :: Map k v -> p -> Map k v @-}

{-@ assume mLookup :: (Ord k) => x:k -> m:Map k v -> {r:Maybe v | r == sel m x} @-}
mLookup :: (Ord k) => k -> Map k v -> Maybe v
mLookup = Map.lookup

{-@ assume mPut :: (Ord k) => x:k -> c:Maybe v -> m:Map k v -> {n:Map k v | n == put m x c} @-}
mPut :: (Ord k) => k -> Maybe v -> Map k v -> Map k v
mPut x c = Map.alter (const c) x

{-@ assume mNone :: {m:Map k v | none m} @-}
mNone :: Map k v
mNone = Map.empty

{-@ assume mLess :: marks:(p -> k -> Bool) -> q:p -> m:Map k v -> {n:Map k v | n == less m q} @-}
mLess :: (p -> k -> Bool) -> p -> Map k v -> Map k v
mLess marks q = Map.filterWithKey (\k _ -> not (marks q k))

-- | Assumed: a lookup after a cell was put at a key gives that cell.
{-@ assume selSame :: m:Map k v -> x:k -> c:Maybe v -> {sel (put m x c) x == c} @-}
selSame :: Map k v -> k -> Maybe v -> ()
selSame _ _ _ = ()

-- | Assumed: a lookup at another key gives what was there before.
{-@ assume selOther :: m:Map k v -> x:k -> c:Maybe v -> y:{k | y /= x} -> {sel (put m x c) y == sel m y} @-}
selOther :: Map k v -> k -> Maybe v -> k -> ()
selOther _ _ _ _ = ()

-- | Assumed: the empty map has no cell.
{-@ assume selNone :: m:{Map k v | none m} -> x:k -> {sel m x == Nothing} @-}
selNone :: Map k v -> k -> ()
selNone _ _ = ()

-- | Assumed: a map without the keys that something marks has no cell at
-- such a key, and at any other the cell that it had.
{-@ assume selLess :: marks:(p -> k -> Bool) -> m:Map k v -> q:p -> x:k
      -> {sel (less m q) x == (if marks q x then Nothing else sel m x)} @-}
selLess :: (p -> k -> Bool) -> Map k v -> p -> k -> ()
selLess _ _ _ _ = ()

-- | Whether a write is to the cell of one key.
{-@ measure isOn @-}
isOn :: MapOp p k c -> Bool
isOn (On _ _) = True
isOn All = False
isOn (Under _) = False

-- | Whether a write is to every cell.
{-@ measure isAll @-}
isAll :: MapOp p k c -> Bool
isAll (On _ _) = False
isAll All = True
isAll (Under _) = False

-- | Whether a write is to the cells of the keys that something marks.
{-@ measure isUnder @-}
isUnder :: MapOp p k c -> Bool
isUnder (On _ _) = False
isUnder All = False
isUnder (Under _) = True

-- | The key of a write to one cell.
{-@ measure onKey @-}
{-@ onKey :: {op:MapOp p k c | isOn op} -> k @-}
onKey :: MapOp p k c -> k
onKey (On y _) = y

-- | What a write to one cell does to it.
{-@ measure onCell @-}
{-@ onCell :: {op:MapOp p k c | isOn op} -> c @-}
onCell :: MapOp p k c -> c
onCell (On _ c) = c

-- | What marks the keys of a write to marked cells.
{-@ measure underWhat @-}
{-@ underWhat :: {op:MapOp p k c | isUnder op} -> p @-}
underWhat :: MapOp p k c -> p
underWhat (Under q) = q

-- | The map after a write: what the model of Redis is changed by, for the
-- keys of Redis and for the fields, members and scores inside a key. Its
-- type says which map that is for each kind of write, in the names of
-- "Data.Map" above.
{-@ mapAfter :: (Ord k) => marks:(p -> k -> Bool) -> cell:(c -> Maybe v -> Maybe v) -> op:MapOp p k c -> m:Map k v
      -> {n:Map k v | (isAll op => none n)
                      && (isOn op => n == put m (onKey op) (cell (onCell op) (sel m (onKey op))))
                      && (isUnder op => n == less m (underWhat op))} @-}
mapAfter :: (Ord k) => (p -> k -> Bool) -> (c -> Maybe v -> Maybe v) -> MapOp p k c -> Map k v -> Map k v
mapAfter _ cell (On y c) m = mPut y (cell c (mLookup y m)) m
mapAfter _ _ All _ = mNone
mapAfter marks _ (Under q) m = mLess marks q m

-- | One write: the cell of a key, in any map that is what 'mapAfter' gives,
-- is what 'at' says, from what the cell was.
{-@ step :: (Ord k) => marks:(p -> k -> Bool) -> cell:(c -> Maybe v -> Maybe v) -> x:k -> op:MapOp p k c -> m:Map k v
      -> n:{Map k v | (isAll op => none n)
                      && (isOn op => n == put m (onKey op) (cell (onCell op) (sel m (onKey op))))
                      && (isUnder op => n == less m (underWhat op))}
      -> {sel n x == at marks cell x op (sel m x)} @-}
step :: (Ord k) => (p -> k -> Bool) -> (c -> Maybe v -> Maybe v) -> k -> MapOp p k c -> Map k v -> Map k v -> ()
step _ cell x (On y c) m _
  | y == x = selSame m y (cell c (mLookup y m))
  | otherwise = selOther m y (cell c (mLookup y m)) x
step _ _ x All _ n = selNone n x
step marks _ x (Under q) m _ = selLess marks m q x

-- | Any writes, one after the other: the cell of a key in the map after
-- them is what the cell was, taken through 'at' for each write.
{-@ cellAfterAll :: (Ord k) => marks:(p -> k -> Bool) -> cell:(c -> Maybe v -> Maybe v) -> x:k -> ops:[MapOp p k c]
      -> m:Map k v -> {r:Maybe v | r == atAll marks cell x ops (sel m x)} @-}
cellAfterAll :: (Ord k) => (p -> k -> Bool) -> (c -> Maybe v -> Maybe v) -> k -> [MapOp p k c] -> Map k v -> Maybe v
cellAfterAll _ _ x [] m = mLookup x m
cellAfterAll marks cell x (op : ops) m =
  let n = mapAfter marks cell op m
   in step marks cell x op m n `seq` cellAfterAll marks cell x ops n

-------------------------------------------------------------------------------
-- whether a cell holds something
-------------------------------------------------------------------------------

-- | Whether the cell of a key holds something after a write to the map,
-- from whether it did: 'at' for a question that asks no more.
{-@ reflect thereAt @-}
thereAt :: (Eq k) => (p -> k -> Bool) -> k -> MapOp p k (CellOp b) -> Bool -> Bool
thereAt _ x (On y c) b = if y == x then thereAfter c b else b
thereAt _ _ All _ = False
thereAt marks x (Under p) b = if marks p x then False else b

-- | 'thereAt' for writes one after the other.
{-@ reflect thereAtAll @-}
thereAtAll :: (Eq k) => (p -> k -> Bool) -> k -> [MapOp p k (CellOp b)] -> Bool -> Bool
thereAtAll _ _ [] b = b
thereAtAll marks x (op : ops) b = thereAtAll marks x ops (thereAt marks x op b)

-- | One write: whether the cell holds something after it, by 'at', is what
-- 'thereAt' says.
{-@ thereAtStep :: (Eq k) => marks:(p -> k -> Bool) -> add:Add b -> x:k -> op:MapOp p k (CellOp b) -> v:Maybe b
      -> {isJust (at marks (cellAfter add) x op v) == thereAt marks x op (isJust v)} @-}
thereAtStep :: (Eq k) => (p -> k -> Bool) -> Add b -> k -> MapOp p k (CellOp b) -> Maybe b -> ()
thereAtStep _ add x (On y c) v
  | y == x = thereStep add c v
  | otherwise = ()
thereAtStep _ _ _ All _ = ()
thereAtStep marks _ x (Under p) _
  | marks p x = ()
  | otherwise = ()

-- | Whether a cell holds something after any writes is known from whether
-- it did before them, without what it held.
{-@ thereAtAllSame :: (Eq k) => marks:(p -> k -> Bool) -> add:Add b -> x:k -> ops:[MapOp p k (CellOp b)] -> v:Maybe b
      -> {isJust (atAll marks (cellAfter add) x ops v) == thereAtAll marks x ops (isJust v)} @-}
thereAtAllSame :: (Eq k) => (p -> k -> Bool) -> Add b -> k -> [MapOp p k (CellOp b)] -> Maybe b -> ()
thereAtAllSame _ _ _ [] _ = ()
thereAtAllSame marks add x (op : ops) v =
  thereAtStep marks add x op v `seq` thereAtAllSame marks add x ops (at marks (cellAfter add) x op v)
