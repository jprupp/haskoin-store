-- |
-- Module      : Haskoin.Store.Cache.Protocol
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The rules that stores sharing one Redis follow to apply each change of a
-- cached xpub once, as pure decisions. "Haskoin.Store.Cache" carries them
-- out against Redis.
--
-- Each change (a block, or a transaction entering or leaving the mempool,
-- for one xpub) has a claim in Redis that goes through these states:
--
-- @
--            claim            reload succeeds
--   absent ─────────► busy ─────────────────► done (for an hour)
--     ▲                 │
--     └─────────────────┘ Redis knew more: release
-- @
--
-- A store that finds the claim busy tries again later; one that finds it
-- done skips the change. While it holds the claim, it reloads the xpub from
-- its database, unless the entry in Redis knows more than the database
-- ('mayReplace'), in which case it releases the claim and writes nothing.
--
-- = Why these rules
--
-- * __A change is applied once.__ The first store to see a change claims it;
--   stores that see it later, because they are behind, find it done. A set of
--   stores catching up does no work for changes already applied.
-- * __Stores never undo each other.__ An entry is stamped with the best block
--   of the store that wrote it, and no store replaces an entry stamped with a
--   higher block, or one with a transaction it has never seen. Without this
--   rule, a store that is behind would replace a newer entry with an older
--   one, and the next store would put the newer one back.
-- * __An entry is complete.__ The entry is written with every transaction
--   body that it needs, in one script, so a store that is behind can serve
--   it without its database.
module Haskoin.Store.Cache.Protocol
  ( -- * Claims
    Claim (..),
    OnClaim (..),
    afterClaim,

    -- * Reloads
    Reload (..),
    OnReload (..),
    afterReload,
    mayReplace,
  )
where

import Haskoin (BlockHeight)

-- | The state of the claim of a change, as a store that tries to claim it
-- finds it.
data Claim
  = -- | it was absent: this store holds it now
    Claimed
  | -- | another store applied the change
    Done
  | -- | another store holds the claim
    Busy
  deriving stock (Eq, Show, Enum, Bounded)

-- | What to do after trying to claim a change.
data OnClaim
  = -- | reload the xpub, holding the claim
    ReloadNow
  | -- | nothing: the change was applied
    Skip
  | -- | try to claim it again later, in case its holder gives it up
    ClaimLater
  deriving stock (Eq, Show)

-- | What to do after trying to claim a change.
afterClaim :: Claim -> OnClaim
afterClaim = \case
  Claimed -> ReloadNow
  Done -> Skip
  Busy -> ClaimLater

-- | What became of a reload.
data Reload
  = -- | the entry was replaced
    Reloaded
  | -- | Redis knows more about the xpub than this store: its entry is stamped
    -- with a higher block, or has a transaction this store has never seen. It
    -- is left for a store that knows more.
    Behind
  | -- | another store holds the lock of the xpub
    Locked
  deriving stock (Eq, Show, Enum, Bounded)

-- | What to do with the claims of the changes that a reload was for.
data OnReload
  = -- | mark them done
    Finish
  | -- | give them up, for a store that knows more
    Release
  | -- | keep holding them, and reload again later
    Hold
  deriving stock (Eq, Show)

-- | What to do with the claims of the changes that a reload was for.
afterReload :: Reload -> OnReload
afterReload = \case
  Reloaded -> Finish
  Behind -> Release
  Locked -> Hold

-- | Whether a store may replace the entry of an xpub: given the block height
-- that the entry is stamped with, if there is an entry, the height of the
-- best block of the store, and whether the entry has a transaction that the
-- store has never seen. An entry of the same height may be replaced, as it
-- can differ only in the mempool. The commit script checks the height again,
-- atomically, as another store may write between the check and the commit.
mayReplace :: Maybe Integer -> BlockHeight -> Bool -> Bool
mayReplace stamp height unknownTxs =
  maybe True (<= toInteger height) stamp && not unknownTxs
