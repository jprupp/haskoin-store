-- |
-- Module      : Haskoin.Store.BlockStore.Sync
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The state of the block download, as a pure state machine.
--
-- The block store downloads blocks from one peer at a time. It is either
-- 'Idle', with no syncing peer, or 'Syncing' from one peer, with the blocks
-- requested from it and not yet imported, in chain order. Every change of
-- that state is one of the transitions of this module, and the block store
-- only carries out what they decide: lock or release a peer, send a request,
-- disconnect a peer.
--
-- = Why a state machine
--
-- The block store used to keep this state in a @TVar (Maybe Syncing)@ that a
-- dozen functions changed in place, each with its own check of who the peer
-- was and what had been asked. Bugs in such code, like a peer left locked or
-- a block accepted that nobody asked for, show up as a sync that stops for no
-- visible reason. Pure transitions can be tested on their own, with any
-- sequence of events, and the rules are in one place:
--
-- * There is at most one syncing peer, and only 'start' sets one, from 'Idle'.
-- * Only blocks requested from the syncing peer are accepted ('accept'), so a
--   block from another peer, or one nobody asked for, is a reason to
--   disconnect the peer that sent it.
-- * A block is requested at most once while it is outstanding ('request').
-- * The peer times out when it has delivered nothing for a while
--   ('timedOut'); delivering a block or starting resets the clock.
--
-- The peer is a type parameter so that the tests can use plain numbers.
module Haskoin.Store.BlockStore.Sync
  ( -- * State
    Sync (..),
    Download (..),
    syncingPeer,
    outstanding,
    lastRequested,

    -- * Transitions
    start,
    release,
    Accept (..),
    accept,
    imported,
    request,
    forgetRequests,

    -- * Decisions
    wantsMore,
    needsMoreBlocks,
    timedOut,
  )
where

import Data.List (delete)
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime)
import Haskoin (BlockHash)

-- | State of the block download from peers of type @p@.
data Sync p
  = -- | no syncing peer
    Idle
  | -- | downloading from a peer
    Syncing !(Download p)
  deriving stock (Eq, Show)

-- | A download from the syncing peer.
data Download p = Download
  { -- | the peer, locked for the block store
    peer :: !p,
    -- | when it last delivered a block, or when syncing started
    time :: !UTCTime,
    -- | blocks requested and not yet imported, in chain order
    blocks :: ![BlockHash]
  }
  deriving stock (Eq, Show)

-- | The syncing peer, if any.
syncingPeer :: Sync p -> Maybe p
syncingPeer Idle = Nothing
syncingPeer (Syncing d) = Just d.peer

-- | Blocks requested from the syncing peer and not yet imported.
outstanding :: Sync p -> [BlockHash]
outstanding Idle = []
outstanding (Syncing d) = d.blocks

-- | The last block requested, if any is outstanding: the next request
-- starts after it.
lastRequested :: Sync p -> Maybe BlockHash
lastRequested s = case outstanding s of
  [] -> Nothing
  bs -> Just (last bs)

-- | Start syncing from a peer, if there is no syncing peer yet. The caller
-- locks the peer first, and frees it again if this gives 'Nothing'.
start :: UTCTime -> p -> Sync p -> Maybe (Sync p)
start now p Idle = Just (Syncing (Download p now []))
start _ _ (Syncing _) = Nothing

-- | Stop syncing from a peer, when it disconnects or the store is in sync.
-- Gives the new state, and whether the peer was the syncing peer, in which
-- case the caller frees it. Another peer leaves the state as it is.
release :: (Eq p) => p -> Sync p -> (Bool, Sync p)
release p (Syncing d)
  | d.peer == p = (True, Idle)
release _ s = (False, s)

-- | What to do with a block that a peer sent.
data Accept
  = -- | it was requested from this peer: import it
    Requested
  | -- | the peer is not the syncing peer
    NotSyncingPeer
  | -- | the syncing peer sent a block that is not outstanding
    NotRequested
  deriving stock (Eq, Show)

-- | Whether to import a block that a peer sent. Anything but 'Requested'
-- means the peer misbehaves, or that the requests were forgotten after a
-- reorganisation and the block is from the old chain.
accept :: (Eq p) => p -> BlockHash -> Sync p -> Accept
accept p h = \case
  Syncing d
    | d.peer /= p -> NotSyncingPeer
    | h `elem` d.blocks -> Requested
    | otherwise -> NotRequested
  Idle -> NotSyncingPeer

-- | A requested block was imported: it is no longer outstanding, and the
-- peer is not idle.
imported :: UTCTime -> BlockHash -> Sync p -> Sync p
imported _ _ Idle = Idle
imported now h (Syncing d) = Syncing d {time = now, blocks = delete h d.blocks}

-- | Blocks were requested from the syncing peer. Blocks already outstanding
-- are not added twice. Nothing changes when idle, as there is nobody to ask.
request :: [BlockHash] -> Sync p -> Sync p
request _ Idle = Idle
request hs (Syncing d) = Syncing d {blocks = foldl' add d.blocks hs}
  where
    add bs h = if h `elem` bs then bs else bs <> [h]

-- | Forget the outstanding requests, after blocks were reverted: they
-- continued a chain that is no longer the best. Blocks of them that still
-- arrive are 'NotRequested'.
forgetRequests :: Sync p -> Sync p
forgetRequests Idle = Idle
forgetRequests (Syncing d) = Syncing d {blocks = []}

-- | The syncing peer, if it is time to ask it for more blocks, given the
-- size of the window.
wantsMore :: Int -> Sync p -> Maybe p
wantsMore _ Idle = Nothing
wantsMore window (Syncing d)
  | needsMoreBlocks window (length d.blocks) = Just d.peer
  | otherwise = Nothing

-- | Whether to ask for the next batch, given the window size and how many
-- requested blocks are still waiting to be imported. Asking once fewer than
-- a quarter of the window remain keeps the peer busy, while the queue of
-- downloaded blocks never grows much beyond one window. The threshold is at
-- least one, so that a window smaller than four still refills once empty.
needsMoreBlocks :: Int -> Int -> Bool
needsMoreBlocks window n = n < max 1 (window `div` 4)

-- | The syncing peer, if it has delivered nothing for longer than the
-- timeout.
timedOut :: NominalDiffTime -> UTCTime -> Sync p -> Maybe p
timedOut _ _ Idle = Nothing
timedOut timeout now (Syncing d)
  | now `diffUTCTime` d.time > timeout = Just d.peer
  | otherwise = Nothing
