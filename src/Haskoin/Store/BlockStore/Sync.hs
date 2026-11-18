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
-- = How long a stall is
--
-- The stall follows the syncing peer's own pace ('patience'): four times
-- the recent gap between the blocks that it delivered ('gap'), but at least
-- 'stallFloor' and at most the stall that the store was given. A store that
-- imports hundreds of small blocks a second sees a peer that has gone quiet
-- in two seconds, not ten; one that takes seconds over each large block is
-- given longer. A fixed stall was the alternative: short enough to catch a
-- quiet peer quickly, it passes over every peer on a slow link, and long
-- enough for a slow link, it leaves a fast store idle for most of the time.
-- Until a block is imported, there is no pace to go by, and the stall is
-- the one given.
--
-- The stall doubles each time a peer is passed over, up to the long
-- timeout, and goes back to its start when a block is imported. A store on
-- a link that cannot bring a block within the first stall therefore still
-- gets it, from the first peer that it asked, at the price of having asked
-- others too. Making the single timeout short was the alternative: it
-- disconnects every peer in turn on such a link, each before its block is
-- whole, and the store never syncs.
--
-- = Who is asked
--
-- A peer is asked for a block only if it answers a ping first ('ping',
-- 'pong'). The peers that could be asked are all pinged at once, and the
-- first to answer becomes the syncing peer: the one that is quickest now,
-- which the median of its old pings, by which the node sorts its peers,
-- does not tell. A peer that takes longer than the wait to answer is not
-- asked while another may still answer in time. Only when every peer that
-- was pinged is late is one of them asked all the same ('overdue'): a
-- store on a slow link has only late peers, and must still sync.
--
-- Which peers could be asked is not decided here: the block store pings
-- only those that have announced the block that it needs.
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
    Probe (..),
    Pinged (..),
    probing,

    -- * Transitions
    start,
    pass,
    release,
    Accept (..),
    accept,
    imported,
    request,
    forgetRequests,
    ping,
    pong,
    skip,
    settle,

    -- * Decisions
    wantsMore,
    needsMoreBlocks,
    patience,
    stallFloor,
    stallFactor,
    stalled,
    timedOut,
    expired,
    overdue,
  )
where

import Data.List (delete, find, nub, sortOn)
import Data.Maybe (isJust, isNothing)
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime)
import Data.Word (Word64)
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
    -- | the recent gap between imported blocks, a moving average, if a
    -- block was imported: the pace that 'patience' follows
    gap :: !(Maybe NominalDiffTime),
    -- | peers that were passed over and still owe blocks
    passed :: ![Asked p],
    -- | the peers that were pinged to find the next syncing peer, if one
    -- is being looked for
    probe :: !(Maybe (Probe p))
  }
  deriving stock (Eq, Show)

-- | Peers that were pinged, one of which is to be asked for blocks. All
-- are pinged with the same nonce, so that their answers are told from
-- those to the pings of the node.
data Probe p = Probe
  { nonce :: !Word64,
    -- | never empty, and in the order that they were pinged
    pinged :: ![Pinged p]
  }
  deriving stock (Eq, Show)

-- | A peer that was pinged, when, and when it answered if it was too late
-- to be chosen for it.
data Pinged p = Pinged
  { peer :: !p,
    at :: !UTCTime,
    answered :: !(Maybe UTCTime)
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
idle now =
  Sync {wanted = [], current = Nothing, time = now, passes = 0, gap = Nothing, passed = [], probe = Nothing}

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

-- | Whether peers were pinged and none was chosen yet.
probing :: Sync p -> Bool
probing s = isJust s.probe

-- | Start syncing from a peer, if there is no syncing peer. The caller must
-- have locked the peer, and releases it if this gives 'Nothing'. The search
-- for a peer is over.
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
      passed = filter ((/= p) . (.peer)) s.passed,
      probe = Nothing
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
    others = skip p s {passed = filter ((/= p) . (.peer)) s.passed}

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
  | Just a <- s.current,
    a.peer == p = case sent a of
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
-- stall start again. The time since the clock last started goes into the
-- recent gap between blocks, with a weight of a quarter, so that a few
-- slow blocks lengthen the stall and a burst of quick ones does not make it
-- jumpy.
imported :: UTCTime -> BlockHash -> Sync p -> Sync p
imported now h s = s {wanted = delete h s.wanted, time = now, passes = 0, gap = Just gap'}
  where
    sample = max 0 (now `diffUTCTime` s.time)
    gap' = maybe sample (\g -> (3 * g + sample) / 4) s.gap

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

-- | Peers are to be pinged, to ask the first that answers for blocks. Gives
-- the nonce to ping them with and those to ping: the ones that were not
-- pinged already in this search, which goes on with the nonce that it
-- started with. The nonce given is used if this starts the search.
ping :: (Eq p) => UTCTime -> Word64 -> [p] -> Sync p -> (Maybe (Word64, [p]), Sync p)
ping now fresh ps s = case new of
  [] -> (Nothing, s)
  _ -> (Just (n, new), s {probe = Just (Probe n (old <> [Pinged p now Nothing | p <- new]))})
  where
    (n, old) = maybe (fresh, []) (\b -> (b.nonce, b.pinged)) s.probe
    new = nub (filter (`notElem` map (.peer) old) ps)

-- | A peer answered a ping. Says whether it is to be asked for blocks: it
-- was pinged in this search and answered within the wait. It is then out
-- of the search, which the caller ends by making it the syncing peer
-- ('start', 'pass'), or which goes on without it if the peer cannot be
-- locked. A late answer is kept, for 'overdue' to prefer the peer to
-- those that never answered.
pong :: (Eq p) => NominalDiffTime -> UTCTime -> p -> Word64 -> Sync p -> (Bool, Sync p)
pong wait now p n s = case s.probe of
  Just b
    | b.nonce == n,
      Just g <- find ((== p) . (.peer)) b.pinged,
      isNothing g.answered ->
        if now `diffUTCTime` g.at <= wait
          then (True, skip p s)
          else (False, s {probe = Just (Probe n (map late b.pinged))})
  _ -> (False, s)
  where
    late g = if g.peer == p then Pinged p g.at (Just now) else g

-- | Leave a peer out of the search for a syncing peer, which ends if it
-- was the last one pinged.
skip :: (Eq p) => p -> Sync p -> Sync p
skip p s = case s.probe of
  Nothing -> s
  Just b -> case filter ((/= p) . (.peer)) b.pinged of
    [] -> s {probe = Nothing}
    rest -> s {probe = Just (Probe b.nonce rest)}

-- | End the search for a syncing peer.
settle :: Sync p -> Sync p
settle s = s {probe = Nothing}

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
-- asked: 'stallFactor' times the recent gap between blocks, at least
-- 'stallFloor' and at most the stall given, or the stall given if no block
-- was imported yet; doubled for each peer passed over since a block was
-- last imported, and never longer than the timeout.
patience :: NominalDiffTime -> NominalDiffTime -> Sync p -> NominalDiffTime
patience stall timeout s = min timeout (base * 2 ^ min 30 s.passes)
  where
    base = maybe stall (\g -> min stall (max stallFloor (stallFactor * g))) s.gap

-- | The shortest stall, however quick the peer was: long enough for a round
-- trip across the world and the next block of a quick peer, short enough
-- that a store importing hundreds of blocks a second idles only briefly.
stallFloor :: NominalDiffTime
stallFloor = 2

-- | How many of its recent gaps between blocks a peer may be silent before
-- it is passed over. Blocks come in bursts, so the gap varies; a factor of
-- four lets a peer take a pause well beyond its usual one.
stallFactor :: NominalDiffTime
stallFactor = 4

-- | The peers to ask all the same, best first, if none of those pinged
-- answered within the wait and none can any more: there is no other
-- option. Those that answered late come first, in the order that they
-- answered, then those that have not answered, in the order that they were
-- pinged. None while a peer that was pinged may still answer in time.
overdue :: NominalDiffTime -> UTCTime -> Sync p -> [p]
overdue wait now s = case s.probe of
  Just b
    | all late b.pinged ->
        map (.peer) (sortOn (.answered) (filter (isJust . (.answered)) b.pinged))
          <> map (.peer) (filter (isNothing . (.answered)) b.pinged)
  _ -> []
  where
    late g = isJust g.answered || now `diffUTCTime` g.at > wait

-- | The syncing peer, if blocks are wanted and none was imported for
-- longer than its 'patience': time to ask another peer, with 'pass'. The
-- block store asks this both when it starts a search and when a pinged peer
-- answers, and gets the same answer both times unless a block came in
-- between.
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
