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
-- The block store asks one peer at a time for blocks, the syncing peer. The
-- state holds the blocks that are wanted, in chain order, the peer that is
-- being asked, and the peers that were asked before it and have yet to
-- deliver. Every change of that state is one of the transitions of this
-- module, and the block store only carries out what they decide: lock or
-- release a peer, send a request, disconnect a peer.
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
-- * There is at most one syncing peer, and only 'start' and 'pass' set one.
-- * A block is accepted only from a peer that was asked for it ('accept'),
--   so a block that nobody asked that peer for is a reason to disconnect it.
-- * A block is wanted at most once ('request').
-- * A peer that has delivered nothing for a while is passed over
--   ('stalled', 'pass'), and one that has delivered nothing for long is
--   disconnected ('timedOut', 'expired').
--
-- = A peer that does not deliver
--
-- A peer may announce a block and never send it, or send it slowly. There
-- used to be one timeout for that, the one that says a peer is dead, of two
-- minutes: the store waited it out, disconnected the peer, and asked the
-- next. A new block could take two minutes and six seconds to arrive on a
-- store with twenty good peers.
--
-- Now a peer that delivers nothing for a short time, the stall, is passed
-- over: the next peer is asked for the same blocks, and becomes the syncing
-- peer. The one before is not disconnected. It is still owed its blocks,
-- and if it sends one that is still wanted, the block is imported: whoever
-- delivers first wins. One that is not wanted any more is dropped, and is
-- no reason to disconnect anyone. Only a peer that has still not delivered
-- when the long timeout runs out is disconnected, as before.
--
-- The stall doubles each time a peer is passed over, up to the long
-- timeout, and goes back to its start when a block is imported. A store on
-- a link that cannot bring a block within the first stall therefore still
-- gets it, from the first peer that it asked, at the price of having asked
-- others too. Making the single timeout short was the alternative: it
-- disconnects every peer in turn on such a link, each before its block is
-- whole, and the store never syncs.
--
-- The peer is a type parameter so that the tests can use plain numbers.
module Haskoin.Store.BlockStore.Sync
  ( -- * State
    Sync (..),
    Asked (..),
    idle,
    syncingPeer,
    outstanding,
    lastRequested,
    passedPeers,

    -- * Transitions
    start,
    pass,
    release,
    Accept (..),
    accept,
    imported,
    request,
    forgetRequests,

    -- * Decisions
    wantsMore,
    needsMoreBlocks,
    patience,
    stalled,
    timedOut,
    expired,
  )
where

import Data.List (delete)
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime)
import Haskoin (BlockHash)

-- | State of the block download from peers of type @p@.
data Sync p = Sync
  { -- | blocks requested and not yet imported, in chain order
    wanted :: ![BlockHash],
    -- | the syncing peer, locked for the block store, if there is one
    current :: !(Maybe (Asked p)),
    -- | when a block was last imported, or the syncing peer last changed
    time :: !UTCTime,
    -- | syncing peers passed over since a block was last imported
    passes :: !Int,
    -- | peers that were passed over and still owe blocks
    passed :: ![Asked p]
  }
  deriving stock (Eq, Show)

-- | A peer and the blocks that it was asked for and has not sent.
data Asked p = Asked
  { peer :: !p,
    -- | when it became the syncing peer or, once passed over, when it
    -- last delivered: the start of its silence
    since :: !UTCTime,
    owes :: ![BlockHash]
  }
  deriving stock (Eq, Show)

-- | No syncing peer, and nothing wanted.
idle :: UTCTime -> Sync p
idle now = Sync {wanted = [], current = Nothing, time = now, passes = 0, passed = []}

-- | The syncing peer, if any.
syncingPeer :: Sync p -> Maybe p
syncingPeer s = (.peer) <$> s.current

-- | Blocks requested and not yet imported.
outstanding :: Sync p -> [BlockHash]
outstanding = (.wanted)

-- | The last block requested, if any is outstanding: the next request
-- starts after it.
lastRequested :: Sync p -> Maybe BlockHash
lastRequested s = case outstanding s of
  [] -> Nothing
  bs -> Just (last bs)

-- | The peers that were passed over and still owe blocks.
passedPeers :: Sync p -> [p]
passedPeers s = map (.peer) s.passed

-- | Start syncing from a peer, if there is no syncing peer. The caller must
-- have locked the peer, and releases it if this gives 'Nothing'.
start :: (Eq p) => UTCTime -> p -> Sync p -> Maybe (Sync p)
start now p s = case s.current of
  Nothing -> Just (enlist now p [] s) {time = now, wanted = [], passes = 0}
  Just _ -> Nothing

-- | Make a peer the syncing peer, owed some blocks. If it was passed over
-- before, it still owes what it owed then: a peer is in the state once.
enlist :: (Eq p) => UTCTime -> p -> [BlockHash] -> Sync p -> Sync p
enlist now p hs s =
  s
    { current = Just (Asked p now (before <> filter (`notElem` before) hs)),
      passed = filter ((/= p) . (.peer)) s.passed
    }
  where
    before = concat [a.owes | a <- s.passed, a.peer == p]

-- | Pass over the syncing peer for another, which the caller has locked,
-- and gives what the new peer is to be asked for: the blocks that are
-- wanted. The peer passed over is given too, for the caller to release; it
-- stays owed its blocks, and has been silent since the clock last started.
-- Nothing changes without a syncing peer, or if the peer is the syncing peer.
pass :: (Eq p) => UTCTime -> p -> Sync p -> (Maybe (p, [BlockHash]), Sync p)
pass now p s = case s.current of
  Nothing -> (Nothing, s)
  Just old
    | old.peer == p -> (Nothing, s)
    | otherwise ->
        ( Just (old.peer, s.wanted),
          (enlist now p s.wanted s {passed = [old {since = s.time} | not (null old.owes)] <> s.passed})
            { time = now,
              passes = s.passes + 1
            }
        )

-- | Forget a peer, as when it disconnects. Says whether it was the syncing
-- peer, for the caller to unlock it; the blocks wanted are then forgotten
-- with it, for the next syncing peer to be asked from the best block on.
release :: (Eq p) => p -> Sync p -> (Bool, Sync p)
release p s = case s.current of
  Just a | a.peer == p -> (True, others {current = Nothing, wanted = [], passes = 0})
  _ -> (False, others)
  where
    others = s {passed = filter ((/= p) . (.peer)) s.passed}

-- | What to do with a block that a peer sent.
data Accept
  = -- | this peer was asked for it and it is wanted: import it
    Requested
  | -- | this peer was asked for it, and another delivered it first: drop it
    Late
  | -- | the peer was not asked for any block
    NotSyncingPeer
  | -- | the peer was asked for blocks, and not for this one
    NotRequested
  deriving stock (Eq, Show)

-- | Decide what to do with a block that a peer sent, and note that the peer
-- no longer owes it.
accept :: (Eq p) => p -> BlockHash -> Sync p -> (Accept, Sync p)
accept p h s
  | Just a <- s.current, a.peer == p = case sent a of
      Nothing -> (NotRequested, s)
      Just a' -> (verdict, s {current = Just a'})
  | Just a <- lookupPassed = case sent a of
      Nothing -> (NotRequested, s)
      Just a' -> (verdict, s {passed = [a' | not (null a'.owes)] <> filter ((/= p) . (.peer)) s.passed})
  | otherwise = (NotSyncingPeer, s)
  where
    lookupPassed = case filter ((== p) . (.peer)) s.passed of
      a : _ -> Just a
      [] -> Nothing
    sent a
      | h `elem` a.owes = Just a {owes = delete h a.owes}
      | otherwise = Nothing
    verdict = if h `elem` s.wanted then Requested else Late

-- | A block was imported: it is no longer wanted, and the clock and the
-- stall start again.
imported :: UTCTime -> BlockHash -> Sync p -> Sync p
imported now h s = s {wanted = delete h s.wanted, time = now, passes = 0}

-- | Blocks were requested from the syncing peer. Those already wanted are
-- not added again. Nothing changes without a syncing peer.
request :: [BlockHash] -> Sync p -> Sync p
request hs s = case s.current of
  Nothing -> s
  Just a -> s {wanted = s.wanted <> new, current = Just a {owes = a.owes <> new}}
  where
    new = foldl' (\bs h -> if h `elem` bs || h `elem` s.wanted then bs else bs <> [h]) [] hs

-- | Forget the blocks that are wanted, as when the chain is reorganised:
-- they will be asked for again from the new best block. The peers that
-- were asked still owe them, so that sending one is no fault of theirs.
forgetRequests :: Sync p -> Sync p
forgetRequests s = s {wanted = []}

-- | The syncing peer, if it should be asked for the next batch of blocks.
wantsMore :: Int -> Sync p -> Maybe p
wantsMore window s
  | needsMoreBlocks window (length s.wanted) = syncingPeer s
  | otherwise = Nothing

-- | Whether to ask for the next batch, given the window size and how many
-- requested blocks are still waiting to be imported. Asking once fewer than
-- a quarter of the window remain keeps the peer busy, while the queue of
-- downloaded blocks never grows much beyond one window. The threshold is at
-- least one, so that a window smaller than four still refills once empty.
needsMoreBlocks :: Int -> Int -> Bool
needsMoreBlocks window n = n < max 1 (window `div` 4)

-- | How long the syncing peer has to deliver a block before another is
-- asked: the stall, doubled for each peer passed over since a block was
-- last imported, and never longer than the timeout.
patience :: NominalDiffTime -> NominalDiffTime -> Sync p -> NominalDiffTime
patience stall timeout s = min timeout (stall * 2 ^ min 30 s.passes)

-- | The syncing peer, if blocks are wanted and none was imported for
-- longer than its 'patience': time to ask another peer, with 'pass'.
stalled :: NominalDiffTime -> NominalDiffTime -> UTCTime -> Sync p -> Maybe p
stalled stall timeout now s
  | null s.wanted = Nothing
  | now `diffUTCTime` s.time > patience stall timeout s = syncingPeer s
  | otherwise = Nothing

-- | The syncing peer, if no block was imported for longer than the
-- timeout: nobody could be asked in its place, and it is to be
-- disconnected.
timedOut :: NominalDiffTime -> UTCTime -> Sync p -> Maybe p
timedOut timeout now s
  | now `diffUTCTime` s.time > timeout = syncingPeer s
  | otherwise = Nothing

-- | The peers passed over that still owe blocks longer than the timeout
-- after they were asked: they are to be disconnected.
expired :: NominalDiffTime -> UTCTime -> Sync p -> [p]
expired timeout now s = [a.peer | a <- s.passed, now `diffUTCTime` a.since > timeout]
