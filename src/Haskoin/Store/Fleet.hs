-- |
-- Module      : Haskoin.Store.Fleet
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- How stores that share one Redis agree on the block that they all answer
-- from, as pure decisions. "Haskoin.Store.Fleet.Redis" carries them out.
-- This is the first part of the design in @docs\/layered-database.md@, "A
-- fleet that answers as one", written down so that it can be tested before
-- a store depends on it.
--
-- = The problem
--
-- Stores import the same blocks at slightly different times. Between the
-- moment the first of them has a block and the moment the last has it, the
-- same request gets different answers depending on which store a load
-- balancer picks. Making the stores commit a block together would need
-- every one of them to be there and on time, and one that stalls would hold
-- up all the others.
--
-- = The idea: agree on the state, not on the moment
--
-- Redis holds the /fleet tip/: the newest block that every live store has.
-- Each store imports blocks whenever it gets them, and keeps a view of its
-- database at each recent block. A request reads the fleet tip and is
-- answered from the view at that block. So every store gives the same
-- answer, and the fleet moves to a new block in one step, when the tip
-- moves, although no two stores imported it at the same time.
--
-- = The rules
--
-- * __A store reports its recent blocks__, newest first, under a lease that
--   it has to renew ('report'). A store that dies or hangs stops counting
--   when its lease runs out, and holds nobody up. It reports only the blocks
--   that it still has a view of: a view that was let go cannot be had again,
--   and the model test found at once what happens when the tip moves to such
--   a block. A store that is alive but stuck on old blocks would hold the
--   tip back for as long as it renews its lease, so a store stops reporting
--   when its own health checks fail.
--
-- * __A store reports only if it has a view of the fleet tip__ ('mayReport').
--   One that is catching up, or that just started, would otherwise pull the
--   tip back to an older block, or to one that it alone has.
--
-- * __The tip moves to the newest block that every live store reported__
--   ('agreed', 'move'). Any store may move it. The move is a compare and
--   set, so two stores that try at once do the same thing once.
--
-- * __A store keeps the views of the blocks of its last report, and of the
--   tip that it read after making it__ ('keep'). The tip can only move to a
--   block that every live store reported, so a live store always has the
--   view that it is asked for.
--
-- * __A store that does not have a view of the tip does not answer from
--   another block__ ('answer'). It was away for longer than its lease, and
--   says that it is behind until it has caught up.
--
-- A reorganisation needs no rule of its own. The tip is a block, not a
-- height: when every live store has moved to the other branch, the newest
-- block that they all report is on it, and the tip goes there.
--
-- The stores and the blocks can be of any type that can be compared, so
-- that the tests can run fleets of small numbers.
module Haskoin.Store.Fleet
  ( -- * The state in Redis
    Fleet (..),
    Report (..),
    Time,
    empty,

    -- * Reporting
    mayReport,
    report,
    live,

    -- * Moving the tip
    agreed,
    Move (..),
    move,

    -- * In a store
    keep,
    Answer (..),
    answer,
  )
where

import Data.List (find)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Word (Word64)

-- | A moment, in any unit that the stores share. In Redis it is the clock
-- of Redis, so that the stores do not have to agree on theirs.
type Time = Word64

-- | What a store last said about itself.
data Report b = Report
  { -- | the blocks of its main chain that it has views of, newest first
    recent :: ![b],
    -- | the moment its lease runs out
    expires :: !Time
  }
  deriving (Eq, Show)

-- | What Redis holds for a fleet of stores of type @s@ with blocks of type
-- @b@.
data Fleet s b = Fleet
  { -- | the block that every store answers from, once there is one
    tip :: !(Maybe b),
    -- | the last report of each store, live or not
    reports :: !(Map s (Report b))
  }
  deriving (Eq, Show)

-- | A fleet that no store has joined.
empty :: Fleet s b
empty = Fleet {tip = Nothing, reports = Map.empty}

-- | The reports whose lease has not run out.
live :: Time -> Fleet s b -> Map s (Report b)
live now fleet = Map.filter ((> now) . (.expires)) fleet.reports

-- | Whether a store may report, given a test for the blocks that it has a
-- view of. It may when it has a view of the tip. It also may when there is
-- no tip yet, or when no other store is live: then there is nobody to
-- disagree with, and a fleet whose stores were all away has to be able to
-- start again.
mayReport :: (Ord s) => Time -> s -> (b -> Bool) -> Fleet s b -> Bool
mayReport now store has fleet =
  maybe True has fleet.tip || Map.null (Map.delete store (live now fleet))

-- | Note the recent blocks of a store, newest first, under a lease of some
-- length. The store must have checked 'mayReport' against the same state:
-- in Redis the two are one script.
report :: (Ord s) => Time -> Time -> s -> [b] -> Fleet s b -> Fleet s b
report now lease store blocks fleet =
  fleet {reports = Map.insert store Report {recent = blocks, expires = now + lease} fleet.reports}

-- | The newest block that every live store reported, if there is one, and
-- if any store is live. Stores report their blocks newest first, so it is
-- the first block of any one report that all the others have.
agreed :: (Eq b) => Time -> Fleet s b -> Maybe b
agreed now fleet = case Map.elems (live now fleet) of
  [] -> Nothing
  first : others -> find (\b -> all (elem b . (.recent)) others) first.recent

-- | What became of an attempt to move the tip.
data Move s b
  = -- | the tip is now this block
    Moved !(Fleet s b)
  | -- | the tip is already where the live stores agree, or they agree on
    -- nothing
    Stays
  | -- | the tip was not the one that the store expected: another store moved
    -- it first
    Raced
  deriving (Eq, Show)

-- | Move the tip to the block that the live stores agree on, given the tip
-- that the caller saw. The caller says what it saw because it also changes
-- the shared mempool for the blocks between the two tips, and that change
-- is right for one starting point only.
move :: (Eq b) => Time -> Maybe b -> Fleet s b -> Move s b
move now seen fleet
  | seen /= fleet.tip = Raced
  | otherwise = case agreed now fleet of
      Just b | Just b /= fleet.tip -> Moved fleet {tip = Just b}
      _ -> Stays

-- | The blocks whose views a store must keep: those of its last report,
-- and the tip that it read after that report was made. It may let go of
-- every other view.
--
-- The order matters. A tip read before the report could have moved, in
-- between, to a block that only the earlier report had.
keep :: [b] -> Maybe b -> [b]
keep reported seen = maybe reported (: reported) seen

-- | What a store answers a request from.
data Answer b
  = -- | the view at this block, as every other store does
    At !b
  | -- | its own best block: there is no fleet tip yet
    Alone
  | -- | nothing: it has no view of the fleet tip, so any answer would differ
    -- from those of the other stores
    Behind
  deriving (Eq, Show)

-- | What a store answers a request from, given a test for the blocks that
-- it has a view of and the fleet as the request found it.
answer :: (b -> Bool) -> Fleet s b -> Answer b
answer has fleet = case fleet.tip of
  Nothing -> Alone
  Just b
    | has b -> At b
    | otherwise -> Behind
