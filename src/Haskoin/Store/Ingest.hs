-- |
-- Module      : Haskoin.Store.Ingest
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- How transactions get from the nodes that a store is a peer of to the
-- mempool that its fleet shares: one pure machine, 'step', that decides
-- everything between an announcement and the pool's verdict.
-- @docs\/mempool-ingestion.md@ has what it must guarantee (T1 to T5) and
-- what it may assume of a node (A1 to A6); this module is written to be
-- checked against that, and @Haskoin.Store.IngestSpec@ does the checking.
--
-- = Why one machine
--
-- The path was five stages joined by IO code, each free to lose a
-- transaction on terms of its own: a full queue, a request forgotten after
-- twenty seconds, a store out of sync for a moment. Each loss was excused
-- by "a peer announces again", and a node announces a transaction to a
-- peer once. Every stage passed its tests, and stores fell thousands of
-- transactions behind their nodes. What was wrong was in how the stages
-- compose, where nothing was stated and so nothing could be checked.
--
-- = The shape of it
--
-- * __Every id has one place__ ('Place'): wanted, asked of a peer, or
--   held with its body. The places are the values of one map, so an id is
--   never in two, and it leaves the map through one function, which says
--   why in an output ('Gone'). Nothing leaves in silence. The map is in
--   "Haskoin.Store.Ingest.Ledger", which this module cannot reach into:
--   that is what lets Liquid Haskell prove it of every state.
--
-- * __An id gets a place once the pool says that it has not decided on
--   it__ ('Check', 'Checked'). What a peer names is first asked of the
--   pool, and what is settled never takes room. A machine that gave the
--   named ids a place while it asked went round for ever on a mempool
--   larger than its room: each reading named the settled ones first, and
--   they took the room of those that it lacked.
--
-- * __Giving up is allowed, and remembered.__ When there is no room, when
--   a peer goes, when a request gets no answer, the id is given up with a
--   reason and the machine is /dirty/. A dirty machine reads the whole
--   mempool of its sources again ('Sweep'), which names every transaction
--   that was given up and that a node still has. That is what makes every
--   loss temporary.
--
-- * __A window for each peer__, the most requests out to it: it grows
--   with every answer and halves when a request gets none, between the
--   bounds of the configuration.
--
-- * __A transaction that spends from one the machine is still fetching
--   waits for it__, and is offered when that one settles. A chain of
--   transactions that arrives in any order is offered root first, one
--   offer each.
--
-- = What the runner does
--
-- 'step' takes an 'Input' and gives 'Output's. The runner turns messages
-- of peers, answers of Redis and the clock into inputs, and carries out
-- the outputs: it sends, asks Redis, offers to the pool, and counts. It
-- decides nothing. A 'Check' and an 'Offer' are answered later by
-- 'Checked' and 'Offered', so that what may happen in between is a matter
-- of the order of inputs, which the specs go through exhaustively.
--
-- The peers, the ids and the bodies can be of any type that can be
-- compared, so that the specs run the machine on small numbers.
module Haskoin.Store.Ingest
  ( -- * The machine
    Ingest,
    Config (..),
    defaultConfig,
    start,
    step,
    Input (..),
    Output (..),
    Verdict (..),
    End (..),
    Reason (..),
    Tick,

    -- * Reading it
    Place (..),
    Hold (..),
    placeOf,
    ids,
    pending,
    holding,
    isDirty,
    hasSource,
    sourceCount,
    windowOf,

    -- * For the specs
    wellFormed,
    rebase,
  )
where

import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set (Set)
import Data.Set qualified as Set
import Haskoin.Store.Ingest.Ledger
  ( End (..),
    Hold (..),
    Ledger,
    Output (..),
    Place (..),
    Reason (..),
    Tick,
    heldOf,
    newLedger,
    placesOf,
    rehold,
    retarget,
    retime,
  )
import Haskoin.Store.Ingest.Ledger qualified as L

-- | The bounds and the waits of a machine.
data Config = Config
  { -- | the most ids that the machine keeps a place for
    room :: !Int,
    -- | the most transactions that it holds the body of
    heldRoom :: !Int,
    -- | the fewest and the most requests out to one peer
    minWindow :: !Int,
    maxWindow :: !Int,
    -- | how long a request waits for its answer
    patience :: !Tick,
    -- | how long a transaction is held for transactions that it spends
    -- from and that do not come
    orphanLife :: !Tick,
    -- | the least time before a transaction that waits for others is
    -- offered again whether they came or not; it doubles from there
    retry :: !Tick,
    -- | the least time between two readings of the sources
    sweepGap :: !Tick,
    -- | a reading of the sources this often even when nothing was given
    -- up, or 0 for none
    sweepEvery :: !Tick
  }
  deriving (Eq, Ord, Show)

-- | The bounds of a store: room for the ids of two full mempools of
-- Bitcoin Core, a hundred thousand bodies, windows from 16 to 4,096, a
-- request given twenty seconds and an orphan twenty minutes, offered again
-- after two seconds and then twice as long each time, and a reading of the
-- sources at most every ten seconds and at least every ten minutes.
defaultConfig :: Config
defaultConfig =
  Config
    { room = 1000000,
      heldRoom = 100000,
      minWindow = 16,
      maxWindow = 4096,
      patience = 20000,
      orphanLife = 1200000,
      retry = 2000,
      sweepGap = 10000,
      sweepEvery = 600000
    }

-- | What the machine knows of a peer.
data Link h = Link
  { -- | whether it gives its whole mempool when asked
    source :: !Bool,
    -- | the most requests out to it
    window :: !Int,
    -- | the requests out to it
    out :: !Int,
    -- | the ids that it named and that are wanted, with the time since
    -- which each is wanted of it: the oldest is asked for first
    queue :: !(Set (Tick, h)),
    -- | the requests out to it, with the time of each: the oldest times
    -- out first
    asks :: !(Set (Tick, h)),
    -- | when its mempool was last asked for
    swept :: !(Maybe Tick),
    -- | whether something was given up since, that it may still have
    owed :: !Bool
  }
  deriving (Eq, Ord, Show)

-- | The machine.
data Ingest p h b = Ingest
  { config :: !Config,
    now :: !Tick,
    -- | whether the store has the blocks of its nodes
    synced :: !Bool,
    -- | where each id is, and how many transactions are held: what
    -- only "Haskoin.Store.Ingest.Ledger" can change
    ledger :: !(Ledger p h b),
    links :: !(Map p (Link h)),
    -- | for a transaction, those held that wait for it
    waiters :: !(Map h (Set h)),
    -- | those held that wait, by the time to offer them again
    tries :: !(Set (Tick, h)),
    -- | those held that are ready to offer
    ready :: !(Set h),
    -- | those held whose offer could not be judged
    later :: !(Set h),
    -- | whether something was given up that a node may still have
    dirty :: !Bool
  }
  deriving (Eq, Ord, Show)

-- | What happens to the machine.
data Input p h b
  = -- | the time
    Tick !Tick
  | -- | a peer connected; whether it is a source
    PeerUp !p !Bool
  | -- | a peer is gone
    PeerDown !p
  | -- | whether the store has the blocks of its nodes
    Synced !Bool
  | -- | a peer named transactions (@inv@)
    Named !p ![h]
  | -- | a transaction came: from a peer, or from a client of the store
    Body !(Maybe p) !h !b
  | -- | a peer does not have what it was asked for (@notfound@)
    Missing !p ![h]
  | -- | the answer to a 'Check' of what a peer named: those that are
    -- settled, and those that are not
    Checked !p ![h] ![h]
  | -- | a 'Check' that got no answer
    Unchecked !p ![h]
  | -- | the pool's verdict on an 'Offer'
    Offered !h !(Verdict h)
  | -- | the pool lost what it had
    Emptied
  deriving (Eq, Ord, Show)

-- | What the pool says of an offer.
data Verdict h
  = -- | it has the transaction now, or a block does
    Settled
  | -- | it lacks these transactions that this one spends from
    Orphan ![h]
  | -- | it could not say: ask again
    Again
  deriving (Eq, Ord, Show)

-- | A machine with nothing in it and no peer, at a time.
start :: Config -> Tick -> Ingest p h b
start config now =
  Ingest
    { config,
      now,
      synced = False,
      ledger = newLedger (max 0 config.room) (max 0 config.heldRoom),
      links = Map.empty,
      waiters = Map.empty,
      tries = Set.empty,
      ready = Set.empty,
      later = Set.empty,
      dirty = False
    }

-- | The places of the ids.
places :: Ingest p h b -> Map h (Place p h b)
places s = placesOf s.ledger

-- | Change the ledger of a machine with one of the functions that keep
-- every id in its place.
edit :: (Ledger p h b -> Ledger p h b) -> Ingest p h b -> Ingest p h b
edit f s = s {ledger = f s.ledger}

-- | Where an id is, if the machine has it.
placeOf :: (Ord h) => h -> Ingest p h b -> Maybe (Place p h b)
placeOf h s = Map.lookup h (places s)

-- | The ids that the machine has a place for.
ids :: Ingest p h b -> Set h
ids s = Map.keysSet (places s)

-- | How many requests are out: what the health check of a store counts.
pending :: Ingest p h b -> Int
pending s = sum (map (.out) (Map.elems s.links))

-- | How many bodies are held.
holding :: Ingest p h b -> Int
holding s = heldOf s.ledger

-- | Whether something was given up that a node may still have, and a
-- source has not been read since, or there is none to read.
isDirty :: Ingest p h b -> Bool
isDirty s = s.dirty || any (\l -> l.source && l.owed) (Map.elems s.links)

-- | Whether a peer gives its whole mempool when asked. Without one, what
-- the store misses stays missed.
hasSource :: Ingest p h b -> Bool
hasSource s = any (.source) (Map.elems s.links)

-- | How many peers give their whole mempool when asked.
sourceCount :: Ingest p h b -> Int
sourceCount s = length (filter (.source) (Map.elems s.links))

-- | The window of a peer.
windowOf :: (Ord p) => p -> Ingest p h b -> Maybe Int
windowOf p s = (.window) <$> Map.lookup p s.links

-- | A step of the machine, given the transactions that a body spends
-- from: what it becomes, and what the runner is to do, in order.
step ::
  (Ord p, Ord h) =>
  (b -> [h]) ->
  Input p h b ->
  Ingest p h b ->
  (Ingest p h b, [Output p h b])
step parents input s0 =
  let (s1, o1) = react parents input s0
      (s2, o2) = pump s1
   in (s2, o1 <> o2)

-- | A machine and what it has said so far, newest first.
type Acc p h b = (Ingest p h b, [Output p h b])

-- | What an input does, before the machine looks for what it can do next.
react :: (Ord p, Ord h) => (b -> [h]) -> Input p h b -> Ingest p h b -> (Ingest p h b, [Output p h b])
react parents input s = case input of
  Tick t -> done (onTick (max s.now t) (s {now = max s.now t}, []))
  PeerUp p src
    | Map.member p s.links -> (s, [])
    | otherwise ->
        let link =
              Link
                { source = src,
                  window = s.config.minWindow,
                  out = 0,
                  queue = Set.empty,
                  asks = Set.empty,
                  swept = Nothing,
                  owed = False
                }
         in (s {links = Map.insert p link s.links}, [])
  PeerDown p -> done (peerDown p (s, []))
  Synced b -> (s {synced = b}, [])
  Named p hs
    | Map.member p s.links ->
        -- what has a place is named by one more peer; the rest is asked
        -- of the pool before it gets one
        let one (st, new) h
              | Map.member h (places st) = (also p h st, new)
              | otherwise = (st, h : new)
            (s', fresh) = foldl' one (s, []) hs
         in (s', [Check p (reverse fresh) | not (null fresh)])
    -- a peer that the machine does not know: it cannot be asked
    | otherwise -> (s, [])
  Body from h b -> done (body parents from h b (s, []))
  Missing p hs -> done (foldl' (unanswered NodeHasNot False p) (s, []) hs)
  Checked p settled unsettled ->
    done (foldl' (unsettled' p) (foldl' settle (s, []) settled) unsettled)
  Unchecked _ hs ->
    done (foldl' (\acc h -> if known h acc then acc else forget Unjudged h acc) (s, []) hs)
  Offered h v -> done (offered h v (s, []))
  Emptied -> (s {dirty = True}, [])
  where
    done (s', outs) = (s', reverse outs)
    known h (s', _) = Map.member h (places s')

-- | One more peer named an id that has a place.
also :: (Ord p, Ord h) => p -> h -> Ingest p h b -> Ingest p h b
also p h s = case Map.lookup h (places s) of
  Just (Wanted ms) | Map.notMember p ms -> queued p h (put (Wanted (Map.insert p s.now ms)))
  Just (Asked q t ps) | p /= q -> put (Asked q t (Set.insert p ps))
  _ -> s
  where
    put place = edit (retarget h place) s

-- | Put an id, wanted of a peer since now, among those to ask it for.
queued :: (Ord p, Ord h) => p -> h -> Ingest p h b -> Ingest p h b
queued p h s = s {links = Map.adjust (\l -> l {queue = Set.insert (s.now, h) l.queue}) p s.links}

-- | An id that has a place and is not held is wanted of these peers, since
-- now.
want :: (Ord p, Ord h) => h -> Set p -> Ingest p h b -> Ingest p h b
want h ps s = foldl' (\st p -> queued p h st) placed ps
  where
    placed = edit (retarget h (Wanted (Map.fromSet (const s.now) ps))) s

-- | An id is no longer wanted of the peers that it was wanted of: it is
-- out of their queues. Its place is for the caller to change.
unwant :: (Ord p, Ord h) => h -> Map p Tick -> Ingest p h b -> Ingest p h b
unwant h ms s = s {links = Map.foldlWithKey' out s.links ms}
  where
    out ls p t = Map.adjust (\l -> l {queue = Set.delete (t, h) l.queue}) p ls

-- | An id is given up, for a reason, wherever it is. It is something that
-- a node may still have, unless the node said that it has not.
forget :: (Ord p, Ord h) => Reason -> h -> Acc p h b -> Acc p h b
forget why h acc =
  let (s, said) = leave h (GaveUp why) acc
   in (s {dirty = s.dirty || why /= NodeHasNot}, said)

-- | The one way out of the machine: the id loses its place, the indexes
-- that mention it are put right, and an output says how it left. An id
-- that has no place leaves too, with the same word: it was learned and
-- let go in the same step.
leave :: (Ord p, Ord h) => h -> End -> Acc p h b -> Acc p h b
leave h end (s, said) =
  let s' = unplace h s
      (ledger, said') = L.leave h end s'.ledger said
   in (s' {ledger}, said')

-- | Take an id out of every index that its place puts it in. Its place
-- is for the caller to change or to delete.
unplace :: (Ord p, Ord h) => h -> Ingest p h b -> Ingest p h b
unplace h s = case Map.lookup h (places s) of
  Just (Wanted ms) -> unwant h ms s
  Just (Asked p t _) -> answered p t h s
  Just (Held _ _ hold) -> unhold h hold s
  _ -> s

-- | A request to a peer is no longer out.
answered :: (Ord p, Ord h) => p -> Tick -> h -> Ingest p h b -> Ingest p h b
answered p t h s = s {links = Map.adjust (\l -> l {out = l.out - 1, asks = Set.delete (t, h) l.asks}) p s.links}

-- | Take a held transaction out of the indexes of what it waits for.
unhold :: (Ord h) => h -> Hold h -> Ingest p h b -> Ingest p h b
unhold h hold s = case hold of
  Ready -> s {ready = Set.delete h s.ready}
  Offering -> s
  Waiting ps t ->
    s
      { tries = Set.delete (t, h) s.tries,
        waiters = foldl' (\w p -> Map.update (nonEmpty . Set.delete h) p w) s.waiters ps
      }
  Later -> s {later = Set.delete h s.later}
  where
    nonEmpty x = if Set.null x then Nothing else Just x

-- | The pool has decided on an id: it leaves, whether it had a place or
-- was only being checked, and what waited for it is ready.
settle :: (Ord p, Ord h) => Acc p h b -> h -> Acc p h b
settle acc h = wake h (leave h Done acc)

-- | The transactions that waited for one are let go of it, and those that
-- wait for nothing else are ready.
wake :: (Ord h) => h -> Acc p h b -> Acc p h b
wake h (s, said) = (foldl' free s {waiters = Map.delete h s.waiters} children, said)
  where
    children = maybe [] Set.toList (Map.lookup h s.waiters)
    free st c = case Map.lookup c (places st) of
      Just (Held _ _ (Waiting ps t)) ->
        let rest = Set.delete h ps
         in if Set.null rest
              then (edit (rehold c Ready) st) {tries = Set.delete (t, c) st.tries, ready = Set.insert c st.ready}
              else edit (rehold c (Waiting rest t)) st
      _ -> st

-- | The pool has not decided on an id that a peer named: it is wanted of
-- that peer, if the peer is still there and there is room.
unsettled' :: (Ord p, Ord h) => p -> Acc p h b -> h -> Acc p h b
unsettled' p acc@(s, said) h
  -- the peer left while the pool was asked: it cannot be asked for it
  | Map.notMember p s.links = if Map.member h (places s) then acc else forget PeerGone h acc
  | Map.member h (places s) = (also p h s, said)
  | otherwise = case L.put h (Wanted (Map.singleton p s.now)) s.ledger of
      Just ledger -> (queued p h s {ledger}, said)
      Nothing -> forget NoRoom h acc

-- | A request of a peer got no answer, or the answer that the peer does
-- not have it: the id is wanted of the others that named it, or given up.
-- Says whether to give the peer a smaller window.
unanswered :: (Ord p, Ord h) => Reason -> Bool -> p -> Acc p h b -> h -> Acc p h b
unanswered why shrink p acc@(s, said) h = case Map.lookup h (places s) of
  Just (Asked q t ps)
    | q == p ->
        let s1 = narrowed (answered p t h s)
         in if Set.null ps
              then forget why h (edit (retarget h (Wanted Map.empty)) s1, said)
              else (want h ps s1, said)
  _ -> acc
  where
    narrowed st
      | shrink = st {links = Map.adjust (\l -> l {window = max st.config.minWindow (l.window `div` 2)}) p st.links}
      | otherwise = grown p st

-- | A peer answered: its window grows by one, up to the most.
grown :: (Ord p) => p -> Ingest p h b -> Ingest p h b
grown p s = s {links = Map.adjust (\l -> l {window = min s.config.maxWindow (l.window + 1)}) p s.links}

-- | A transaction came. It is held: to offer, or to wait for those that it
-- spends from and that the machine is still getting.
body :: (Ord p, Ord h) => (b -> [h]) -> Maybe p -> h -> b -> Acc p h b -> Acc p h b
body parents from h b acc@(s, said) = case Map.lookup h (places s) of
  Just (Held {}) -> acc
  Just (Asked q _ _) -> hold (credit q (unplace h s))
  Just _ -> hold (unplace h s)
  Nothing -> hold s
  where
    -- the peer that was asked answered, if it is the one that sent this
    credit q st = if from == Just q then grown q st else st
    -- the body is held if the ledger has room: for one more id if this
    -- one is new, and for one more body
    hold st = case holdAs st of
      Just (how, indexed)
        | Just ledger <- L.put h (Held b st.now how) st.ledger -> (indexed {ledger}, said)
      _ -> gone st
    -- what the body waits for, with the indexes that say so
    holdAs st
      | Set.null waits = Just (Ready, st {ready = Set.insert h st.ready})
      | mayWait st = let (t, st') = waiting h st.now waits st in Just (Waiting waits t, st')
      | otherwise = Nothing
      where
        waits = Set.fromList [p | p <- parents b, p /= h, Map.member p (places st)]
    -- no room for the body: the id goes, from wherever it was
    gone st = forget NoRoom h (edit (retarget h (Wanted Map.empty)) st, said)

-- | Whether one more transaction may be held to wait for others: never
-- the last of the room for bodies, which is kept for those that can be
-- offered as they come. With every body waiting for another that had no
-- room, nothing would ever be offered.
mayWait :: Ingest p h b -> Bool
mayWait s = Set.size s.tries < s.config.heldRoom - 1

-- | A held transaction is ready to offer.
makeReady :: (Ord h) => h -> Ingest p h b -> Ingest p h b
makeReady h s = (edit (rehold h Ready) s) {ready = Set.insert h s.ready}

-- | Note that a held transaction waits for others, and when to offer it
-- again in any case: after a time that grows with how long it has been
-- held. Gives that time; its place is for the caller to set.
waiting :: (Ord h) => h -> Tick -> Set h -> Ingest p h b -> (Tick, Ingest p h b)
waiting h since ps s =
  let t = s.now + max s.config.retry (s.now - since)
   in ( t,
        s
          { tries = Set.insert (t, h) s.tries,
            waiters = foldl' (\w p -> Map.insertWith Set.union p (Set.singleton h) w) s.waiters ps
          }
      )

-- | The pool's verdict on an offer.
offered :: (Ord p, Ord h) => h -> Verdict h -> Acc p h b -> Acc p h b
offered h v acc@(s, said) = case Map.lookup h (places s) of
  Just (Held _ since Offering) -> case v of
    Settled -> settle acc h
    Orphan ps
      | s.now - since > s.config.orphanLife -> forget WaitedTooLong h acc
      | otherwise -> case Set.delete h (Set.fromList ps) of
          waits
            | Set.null waits -> again
            | mayWait s ->
                let (t, s') = waiting h since waits s
                 in (edit (rehold h (Waiting waits t)) s', said)
            | otherwise -> forget NoRoom h acc
    Again -> again
  _ -> acc
  where
    again = ((edit (rehold h Later) s) {later = Set.insert h s.later}, said)

-- | A peer is gone: what was asked of it is wanted of the others, and
-- what only it named is given up.
peerDown :: (Ord p, Ord h) => p -> Acc p h b -> Acc p h b
peerDown p (s, said)
  | Map.notMember p s.links = (s, said)
  | otherwise = foldl' one (s {links = Map.delete p s.links}, said) (Map.toList (places s))
  where
    one acc@(st, o) (h, place) = case place of
      Wanted ms
        | Map.member p ms ->
            let rest = Map.delete p ms
             in if Map.null rest then gone acc h else (edit (retarget h (Wanted rest)) st, o)
      Asked q t ps
        | q == p ->
            if Set.null ps
              then gone (edit (retarget h (Wanted Map.empty)) st, o) h
              else (want h ps st, o)
        | Set.member p ps -> (edit (retarget h (Asked q t (Set.delete p ps))) st, o)
      _ -> acc
    gone acc h = forget PeerGone h acc

-- | What the time brings: requests that waited too long, and held
-- transactions to offer again or to give up.
onTick :: (Ord p, Ord h) => Tick -> Acc p h b -> Acc p h b
onTick t acc0 = retried (laterOnes (foldl' overdue acc0 (Map.keys (fst acc0).links)))
  where
    -- the requests of a peer that are out for longer than the patience:
    -- those that are so now, each once, so that this ends whatever the
    -- indexes say
    overdue acc@(s, _) p = case Map.lookup p s.links of
      Just l ->
        let late = [h | (at, h) <- Set.toAscList l.asks, t - at > s.config.patience]
         in foldl' (unanswered Unanswered True p) acc late
      Nothing -> acc
    -- offers that could not be judged are ready again
    laterOnes (s, said) =
      let again st h = case Map.lookup h (places st) of
            Just (Held _ _ Later) -> makeReady h st
            _ -> st
       in (foldl' again s {later = Set.empty} s.later, said)
    -- held transactions whose time to be offered again has come: those
    -- whose time it is now, each once
    retried acc@(s0, _) = foldl' retry1 acc (takeWhile ((<= t) . fst) (Set.toAscList s0.tries))
    retry1 acc@(s, said) (at, h) = case Map.lookup h (places s) of
      Just (Held _ since hold@(Waiting _ _))
        | t - since > s.config.orphanLife -> forget WaitedTooLong h acc
        | otherwise -> (makeReady h (unhold h hold s), said)
      _ -> (s {tries = Set.delete (at, h) s.tries}, said)

-- | What the machine can do as it is: read its sources if something is
-- owed or it is time, request what is wanted within the windows, and
-- offer what is ready. Only a store that has the blocks of its nodes does
-- any of it.
pump :: (Ord p, Ord h) => Ingest p h b -> (Ingest p h b, [Output p h b])
pump s0
  | not s0.synced = (s0, [])
  | otherwise =
      let (s1, sweeps) = sweep s0
          (s2, gets) = foldl' request (s1, []) (Map.keys s1.links)
          (s3, offers) = offer s2
       in (s3, sweeps <> reverse gets <> offers)
  where
    request (s, said) p =
      let (s', got) = requests p s
       in (s', [Get p (reverse got) | not (null got)] <> said)

-- | Request of a peer the ids that are wanted of it, oldest first, as
-- many as its window has room for: those at the front of its queue now,
-- each once. Gives them newest first.
requests :: (Ord p, Ord h) => p -> Ingest p h b -> (Ingest p h b, [h])
requests p s = case Map.lookup p s.links of
  Just l -> foldl' ask (s, []) (take (l.window - l.out) (map snd (Set.toAscList l.queue)))
  Nothing -> (s, [])
  where
    ask (st, got) h = case Map.lookup h (places st) of
      Just (Wanted ms) ->
        let s1 = unwant h ms st
            out l = l {out = l.out + 1, asks = Set.insert (st.now, h) l.asks}
            asked = Asked p st.now (Set.delete p (Map.keysSet ms))
         in ((edit (retarget h asked) s1) {links = Map.adjust out p s1.links}, h : got)
      _ -> (st, got)

-- | Ask the sources for their mempools: each one when it connects, when
-- something was given up since it was last asked, and when it is time,
-- and none sooner than the gap after its last time. What was given up is
-- owed by every source, since any of their nodes may have it, and a
-- source owes it until it is asked: reading one does not clear another.
sweep :: (Ord p) => Ingest p h b -> (Ingest p h b, [Output p h b])
sweep s0
  | null due = (s, [])
  | otherwise =
      ( s {links = foldl' (\ls p -> Map.adjust (\l -> l {swept = Just s.now, owed = False}) p ls) s.links due},
        map Sweep due
      )
  where
    -- with a source to read, what was given up becomes what each owes
    s
      | s0.dirty && any (.source) (Map.elems s0.links) =
          s0 {dirty = False, links = Map.map (\l -> if l.source then l {owed = True} else l) s0.links}
      | otherwise = s0
    due = [p | (p, l) <- Map.toList s.links, l.source, wants l]
    wants l = case l.swept of
      -- a node does not announce what it took before the peer connected
      Nothing -> True
      Just at ->
        (l.owed && s.now - at >= s.config.sweepGap)
          || (s.config.sweepEvery > 0 && s.now - at >= s.config.sweepEvery)

-- | Offer what is ready.
offer :: (Ord h) => Ingest p h b -> (Ingest p h b, [Output p h b])
offer s = (foldl' mark s {ready = Set.empty} offers, [Offer h b | (h, b) <- offers])
  where
    offers = [(h, b) | h <- Set.toList s.ready, Just (Held b _ Ready) <- [Map.lookup h (places s)]]
    mark st (h, _) = edit (rehold h Offering) st

-- | What is wrong with a machine, if anything: the invariant that every
-- step keeps (@Haskoin.Store.IngestSpec@). Each line is something that
-- another part of the machine relies on.
wellFormed :: (Ord p, Ord h) => Ingest p h b -> [String]
wellFormed s =
  concat
    [ ["more ids than there is room for" | length placed > s.config.room],
      ["more bodies than there is room for" | heldOf s.ledger > s.config.heldRoom],
      ["the count of bodies is wrong" | heldOf s.ledger /= length [() | Held {} <- map snd placed]],
      ["the count of requests out to a peer is wrong" | l <- Map.elems s.links, l.out /= Set.size l.asks],
      [ "a window out of its bounds"
      | l <- Map.elems s.links,
        l.window < s.config.minWindow || l.window > s.config.maxWindow
      ],
      ["more requests out than the most a window allows" | l <- Map.elems s.links, l.out > s.config.maxWindow],
      ["a peer that is gone is still named" | ps <- namers, not (ps `Set.isSubsetOf` Map.keysSet s.links)],
      ["an id that nobody named is waiting to be asked for" | Wanted ms <- map snd placed, Map.null ms],
      ["every body held waits for another" | s.config.heldRoom > 0, Set.size s.tries >= s.config.heldRoom],
      [ "the ids to ask a peer for are not those wanted of it"
      | (p, l) <- Map.toList s.links,
        l.queue /= Set.fromList [(t, h) | (h, Wanted ms) <- placed, Just t <- [Map.lookup p ms]]
      ],
      [ "the requests out to a peer are not those asked of it"
      | (p, l) <- Map.toList s.links,
        l.asks /= Set.fromList [(t, h) | (h, Asked q t _) <- placed, q == p]
      ],
      [ "the transactions to offer are not those ready"
      | s.ready /= Set.fromList [h | (h, Held _ _ Ready) <- placed]
      ],
      [ "the transactions to offer later are not those"
      | s.later /= Set.fromList [h | (h, Held _ _ Later) <- placed]
      ],
      [ "the times to offer again are not those of what waits"
      | s.tries /= Set.fromList [(t, h) | (h, Held _ _ (Waiting _ t)) <- placed]
      ],
      ["a transaction waits for nothing" | Held _ _ (Waiting ps _) <- map snd placed, Set.null ps],
      [ "the index of what waits for a transaction is wrong"
      | s.waiters
          /= Map.fromListWith
            Set.union
            [(p, Set.singleton h) | (h, Held _ _ (Waiting ps _)) <- placed, p <- Set.toList ps]
      ]
    ]
  where
    -- the places, read once
    placed = Map.toList (places s)
    namers =
      concat
        [ case place of
            Wanted ms -> [Map.keysSet ms]
            Asked p _ ps -> [Set.insert p ps]
            Held {} -> []
        | place <- map snd placed
        ]

-- | The same machine with its clock at 0 and every time counted from
-- there, none further back than a bound. Two machines that differ only in
-- when they are, and in how long ago things happened that happened longer
-- ago than matters, become equal: what lets a spec go through every state
-- of a machine whose clock never stops.
rebase :: (Ord h) => Tick -> Ingest p h b -> Ingest p h b
rebase bound s =
  s
    { now = 0,
      ledger = retime rel s.ledger,
      links = Map.map link s.links,
      tries = Set.map (\(t, h) -> (rel t, h)) s.tries
    }
  where
    rel t = max (negate bound) (t - s.now)
    link l = l {queue = times l.queue, asks = times l.asks, swept = rel <$> l.swept}
    times = Set.map (\(t, h) -> (rel t, h))
