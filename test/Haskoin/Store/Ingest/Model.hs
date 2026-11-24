{-# OPTIONS_GHC -O1 #-}

-- |
-- Module      : Haskoin.Store.Ingest.Model
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- A model checker for "Haskoin.Store.Ingest": the machine itself, the
-- 'step' that a store runs, put in a small world of nodes, channels and a
-- pool, and taken through __every__ order in which things can happen
-- there. @docs\/mempool-ingestion.md@ has what is checked, T1 to T7.
--
-- The module is compiled with optimisation whatever the build: going
-- through about a million states without it takes minutes.
--
-- = Why not random tests
--
-- A property test draws runs. The failures that this replaces were each
-- one particular order of two events: a header that came between a
-- request and its answer, an announcement while a queue was full. A
-- draw finds such an order seldom, and going through all of them finds
-- it always. The price is a small world: a few transactions and peers.
-- The bet, which model checkers make, is that a wrong rule shows in a
-- small world already.
--
-- = The world
--
-- Nodes hold mempools and follow the rules that the document assumes of
-- a node (A1 to A8): a node names a transaction to a peer once, names its
-- whole mempool when a source is asked, sends what it named and still
-- has, and says so when it has it no longer. Between a node and the
-- machine there is a channel each way, in order. The pool settles a
-- transaction whose parents are settled. What the machine asks of the
-- pool waits in a line, and other things happen meanwhile.
--
-- = Steps of two kinds
--
-- A node asked for a transaction sends it if it has it, whether it named
-- it to the peer or not: the machine asks for what a transaction spends
-- from when the pool lacks it (A7).
--
-- * __Things that go well__: a message is delivered, the pool answers,
--   the store is in sync again, time passes while nothing else can.
-- * __Disturbances__: a node takes or drops a transaction, a block
--   confirms one, a peer goes and comes, the pool fails to answer or
--   loses everything, a request gets no answer in time, the store is out
--   of sync, another store offers a transaction first.
--
-- Safety is checked in every state that any mix of both reaches.
-- Convergence is checked from every such state: let only things go well
-- from there, and every run must end, in a state where the pool has
-- every transaction of the mempool of every source. That is T3, "from
-- any state that the store can reach".
module Haskoin.Store.Ingest.Model
  ( Scenario (..),
    Disturbance (..),
    Result (..),
    explore,
    wander,
    scenarios,
    withoutOrder,
    large,
    describeResult,
  )
where

import Control.Monad (when)
import Data.Foldable (toList)
import Data.List (intercalate, sortOn)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Sequence (Seq, (|>))
import Data.Sequence qualified as Seq
import Data.Set (Set)
import Data.Set qualified as Set
import Haskoin.Store.Ingest

type P = Int

type H = Int

-- | A transaction body: the transactions that it spends from.
newtype B = B [H]
  deriving (Eq, Ord, Show)

-- | A world to go through.
data Scenario = Scenario
  { name :: String,
    config :: Config,
    -- | the transactions that there can be, each with those it spends from
    txs :: Map H [H],
    -- | the peers, and whether each is a source
    peers :: [(P, Bool)],
    -- | whether the peers are all connections to one node, with one
    -- mempool, and which of them are no peers at all but the RPC of that
    -- node: sources that answer and never announce. A store that is given
    -- the RPC of a node that whitelists nobody has both, a peer that
    -- announces and is no source, and a source that does not announce.
    oneNode :: Bool,
    feeds :: [P],
    -- | the peers that are nodes of the operator: the others are
    -- strangers
    owned :: [P],
    -- | the transactions that are forged, which no honest node takes, and
    -- the peers that have them all the same, from the start
    forged :: [H],
    liars :: [P],
    -- | the transactions that are good and that the store cannot verify
    scripts :: [H],
    -- | whether a source names a transaction __before__ those that it
    -- spends from, against A5: for the world that shows what A5 is needed
    -- for ('withoutOrder'), and for no world that has to pass
    backwards :: Bool,
    disturbances :: [Disturbance],
    -- | disturbances stop while more than this many messages and
    -- questions are on their way: the world is not infinitely faster
    -- than the store
    busy :: Int,
    -- | the most states to go through, after which the search says that
    -- the world is too large and stops
    limit :: Int
  }

data Disturbance
  = -- | a node takes a transaction, or drops one
    Mempools
  | -- | a block confirms a transaction
    Blocks
  | -- | the store is out of sync for a while
    Desyncs
  | -- | a peer goes, and comes back
    PeerDrops
  | -- | the pool does not answer
    PoolFails
  | -- | the pool loses what it had
    PoolEmptied
  | -- | a request gets no answer in time
    Timeouts
  | -- | a client gives the store a transaction
    Clients
  | -- | another store of the fleet offers a transaction first
    OtherStores
  deriving (Eq, Ord, Show, Enum, Bounded)

data Node = Node
  { has :: !(Set H),
    -- | what it named to the store on this connection
    told :: !(Set H),
    -- | what it named when it was last asked for its mempool, and has had
    -- ever since
    answered :: !(Set H),
    online :: !Bool,
    source :: !Bool
  }
  deriving (Eq, Ord, Show)

data ToNode = MGet ![H] | MSweep
  deriving (Eq, Ord, Show)

data ToStore = MInv ![H] | MTx !H | MNotFound ![H]
  deriving (Eq, Ord, Show)

-- | A question to the pool, or an offer with whether it is vouched for.
data Op = OCheck !P ![H] | OOffer !H !B !Bool
  deriving (Eq, Ord, Show)

data World = World
  { machine :: !(Ingest P H B),
    nodes :: !(Map P Node),
    pool :: !(Set H),
    confirmed :: !(Set H),
    toNode :: !(Map P (Seq ToNode)),
    toStore :: !(Map P (Seq ToStore)),
    asked :: !(Seq Op),
    inSync :: !Bool,
    -- | whether the store ever had the blocks of its nodes
    hadBlocks :: !Bool
  }
  deriving (Eq, Ord, Show)

-- | What a search found.
data Result = Result
  { states :: !Int,
    transitions :: !Int,
    -- | what went wrong, with the steps that lead there, if anything did
    failure :: !(Maybe (String, [String]))
  }

describeResult :: Result -> String
describeResult r = case r.failure of
  Nothing -> show r.states <> " states, " <> show r.transitions <> " transitions"
  Just (what, path) -> what <> "\nafter:\n" <> intercalate "\n" (map ("  " <>) path)

parentsOf :: B -> [H]
parentsOf (B ps) = ps

-- | How far back a time matters in the worlds here: beyond the life of an
-- orphan, every age is the same.
horizon :: Scenario -> Tick
horizon sc = sc.config.orphanLife + sc.config.patience + 2

settled :: World -> Set H
settled w = w.pool `Set.union` w.confirmed

initial :: Scenario -> World
initial sc =
  World
    { machine = start sc.config 0,
      nodes =
        Map.fromList
          [ (p, Node (if p `elem` sc.liars then Set.fromList sc.forged else Set.empty) Set.empty Set.empty False src)
          | (p, src) <- sc.peers
          ],
      pool = Set.empty,
      confirmed = Set.empty,
      toNode = Map.fromList [(p, Seq.empty) | (p, _) <- sc.peers],
      toStore = Map.fromList [(p, Seq.empty) | (p, _) <- sc.peers],
      asked = Seq.empty,
      inSync = False,
      hadBlocks = False
    }

-- | Give the machine an input: check what the step did against the rules
-- that hold of every step, and send its outputs on their way.
feed :: Scenario -> Input P H B -> World -> Either String World
feed sc input w = do
  let before = w.machine
      (after, outs) = step parentsOf input before
      -- what comes into the machine with an input: a body, and the ids
      -- that the pool was asked about; what a peer names goes to the pool
      -- first
      learned = Set.fromList $ case input of
        Body _ h _ -> [h]
        Checked _ yes no -> yes <> no
        Unchecked _ hs -> hs
        -- what the pool lacks of what a transaction spends from is
        -- learned when it gets a place, to be asked for; without one it
        -- is what the transaction waits for, and nothing of its own
        Offered _ (Orphan ps) -> filter (`Set.member` ids after) ps
        _ -> []
      gone = Set.fromList [h | Gone h _ <- outs]
  -- T1: nothing leaves in silence, and nothing comes from nowhere
  check "an id left without a word, or came from nowhere" $
    Set.union (ids before) learned == Set.union (ids after) gone
  check "an id is said to have left and is still there" $
    Set.null (Set.intersection (ids after) gone)
  -- T1 for what a peer names: it has a place or the pool is asked, once
  -- the store has had its blocks. Before that nothing of a peer is taken
  -- in, which is checked below.
  case input of
    Named p hs
      | w.hadBlocks,
        Just (Just _) <- Just (windowOf p before) ->
          check "a named id has no place and the pool is not asked" $
            all (\h -> Set.member h (ids after) || h `elem` concat [cs | Check _ cs <- outs]) hs
    _ -> return ()
  -- T6: what the pool lacks of what a held transaction spends from has a
  -- place, to be asked for, if there is a peer to ask and room. It is
  -- so for a verdict that the machine was waiting for: one that comes
  -- after the transaction was given up, and came again and waits, is not
  -- of this offer, and the machine does nothing with it. Left out, that
  -- made this check fail once in two thousand runs of the larger world.
  case input of
    Offered h (Orphan ps)
      | Just (Held _ _ Offering) <- placeOf h before,
        Just (Held _ _ (Waiting _ _)) <- placeOf h after,
        any (\(p, _) -> windowOf p after /= Nothing) sc.peers,
        Set.size (ids after) < sc.config.room ->
          check "the pool lacks what a held transaction spends from, and it is not asked for" $
            all (`Set.member` ids after) (filter (/= h) ps)
    _ -> return ()
  -- What waits for transactions that all have a place is not offered when
  -- its time comes: the pool would only say that it lacks them, and the
  -- held one is made ready when the last of them settles. A machine that
  -- offered it all the same spent most of its offers on that when a whole
  -- mempool came at once. One of them that was given up, in this step
  -- too, has no place after it.
  case input of
    Tick _ ->
      check "a held transaction is offered at its time while all that it waits for have a place" $
        and
          [ any (`Set.notMember` ids after) (Set.toList ps)
          | Offer h _ <- outs,
            Just (Held _ _ (Waiting ps _)) <- [placeOf h before]
          ]
    _ -> return ()
  -- And the other way: one of those that it waits for has no place, so
  -- nothing will wake it, and its time has come. It waits no longer: it
  -- is ready, offered, or given up for having waited too long.
  case input of
    Tick t ->
      check "a held transaction still waits after its time for a transaction that has no place" $
        and
          [ case placeOf h after of
              Just (Held _ _ (Waiting _ _)) -> False
              _ -> True
          | h <- Set.toList (ids before),
            Just (Held _ _ (Waiting ps at)) <- [placeOf h before],
            at <= t,
            any (`Set.notMember` ids before) (Set.toList ps)
          ]
    _ -> return ()
  -- T8: a stranger is not taken at its word. What a node of the operator
  -- names or sends is no longer held for one to name it.
  case input of
    Named p hs
      | p `elem` sc.owned ->
          check "a node of the operator named a transaction that is still held for one to name it" $
            and [not (doubted h) | h <- hs]
    Body (Just p) h _
      | p `elem` sc.owned,
        Just (Just _) <- Just (windowOf p before) ->
          check "a node of the operator sent a transaction that is still held for one to name it" (not (doubted h))
    -- what is forged is given up, and what cannot be verified is held for
    -- a node of the operator, offered again if one named it meanwhile, or
    -- given up for want of room: never offered as it is, and never ready
    Offered h Forged
      | Just (Held _ _ Offering) <- placeOf h before ->
          check "a forged transaction is kept" (Set.notMember h (ids after))
    Offered h Unverified
      | Just (Held _ _ Offering) <- placeOf h before ->
          check "a transaction that cannot be verified is not held for a node of the operator to name it" $
            case placeOf h after of
              Just (Held _ _ (Doubted _)) -> True
              Just (Held _ _ Later) -> vouchedFor h after
              Just (Held _ _ Offering) -> vouchedFor h after
              Nothing -> True
              _ -> False
    _ -> return ()
  -- T9: the store does not say that its pool has the mempool of its nodes
  -- before it has. When the machine starts to say so, no request for a
  -- mempool is on its way to a node of the operator, and the pool has
  -- decided on all that each such node named when it was last asked and
  -- has had ever since.
  --
  -- The machine takes a source to have answered when as long passed as
  -- a request is given (A10). A world in which requests get no answer in
  -- time breaks that on purpose, and is not held to this.
  when (Timeouts `notElem` sc.disturbances && standing before /= PoolSynced && standing after == PoolSynced) $ do
    check "the pool is said to be in sync while a node of the operator has not answered for its mempool" $
      and [MSweep `notElem` toList q | (p, q) <- Map.toList w.toNode, trustedHere p]
    check "the pool is said to be in sync and lacks a transaction that a node of the operator named and has" $
      and
        [ Set.filter (`Set.member` node.answered) node.has `Set.isSubsetOf` Set.union (settled w) (unread p)
        | (p, node) <- Map.toList w.nodes,
          trustedHere p
        ]
  -- it says so only of a node of the operator that gives its mempool:
  -- what strangers name says nothing of what the pool should have
  --
  -- A peer connects one step before the machine is told whose it is:
  -- between the two it is a stranger to the machine, and this is not asked.
  case input of
    PeerUp p _ | p `elem` sc.owned -> return ()
    _ ->
      check "the pool is said to be in sync, or syncing, with no node of the operator to measure it against" $
        (standing after == PoolUnknown) == not (or [trustedHere p | p <- Map.keys w.nodes])
  -- and a pool that lost what it had is not in sync
  case input of
    Emptied -> check "the pool lost what it had and is still said to be in sync" (standing after /= PoolSynced)
    _ -> return ()
  -- Until the store first has its blocks, nothing that a peer names or
  -- sends is taken in, and nothing is asked for it.
  let untaken =
        check "something of a peer was taken in before the store had its blocks" $
          ids after == ids before && null outs
  case input of
    Named _ _ | not w.hadBlocks -> untaken
    Body (Just _) _ _ | not w.hadBlocks -> untaken
    _ -> return ()
  -- the invariant, which has the bounds of T2
  case wellFormed after of
    [] -> return ()
    wrong -> Left ("after " <> show input <> ": " <> intercalate "; " wrong)
  -- T5
  check "the count of requests out is wrong" $
    pending after == length [() | h <- Set.toList (ids after), Just (Asked {}) <- [placeOf h after]]
  w' <- foldl' (\acc o -> acc >>= send o) (Right w) outs
  return w' {machine = rebase (horizon sc) after}
  where
    check what ok = if ok then Right () else Left (what <> ", after " <> show input)
    -- a node of the operator that is connected and gives its mempool
    trustedHere p = p `elem` sc.owned && maybe False (\n -> n.online && n.source) (Map.lookup p w.nodes)
    -- what a peer named in messages that the store has not read
    unread p = Set.fromList (concat [hs | MInv hs <- maybe [] toList (Map.lookup p w.toStore)])
    doubted h = case placeOf h (fst (step parentsOf input w.machine)) of
      Just (Held _ _ (Doubted _)) -> True
      _ -> False
    -- the transactions that another spends from
    spentFrom = Set.fromList (concat (Map.elems sc.txs))
    send o x = case o of
      Get p hs -> do
        node <- maybe (Left "asked a peer that is not there") Right (Map.lookup p x.nodes)
        -- T4: only a peer that is there, only for what it named or for
        -- what a transaction spends from, within its window
        check "asked a peer that is gone" node.online
        check "asked a peer for what it did not name and nothing spends from" $
          all (\h -> Set.member h node.told || Set.member h spentFrom) hs
        check "asked for nothing" (not (null hs))
        return x {toNode = Map.adjust (|> MGet hs) p x.toNode}
      Sweep p -> do
        node <- maybe (Left "swept a peer that is not there") Right (Map.lookup p x.nodes)
        check "asked the mempool of a peer that would drop the store for it" (node.online && node.source)
        return x {toNode = Map.adjust (|> MSweep) p x.toNode}
      Check p hs -> return x {asked = x.asked |> OCheck p hs}
      Offer h b -> do
        -- T8: what is forged is never offered as a node took it
        let vouched = vouchedFor h (fst (step parentsOf input w.machine))
        check "a forged transaction is offered as vouched for" (not (vouched && h `elem` sc.forged))
        return x {asked = x.asked |> OOffer h b vouched}
      Gone _ _ -> return x

-- | The steps in which things go well, from a world.
calm :: Scenario -> World -> [(String, Either String World)]
calm sc w = if null moves then idle else moves
  where
    moves = concat [toNodes, toStores, answers, resync]
    toNodes =
      [ ( "the node of peer " <> show p <> " reads " <> show msg,
          Right (nodeReads sc p msg w {toNode = Map.insert p rest w.toNode})
        )
      | (p, q) <- Map.toList w.toNode,
        msg Seq.:<| rest <- [q]
      ]
    toStores =
      [ ( "the store reads " <> show msg <> " of peer " <> show p,
          feed sc (storeReads p msg) w {toStore = Map.insert p rest w.toStore}
        )
      | (p, q) <- Map.toList w.toStore,
        msg Seq.:<| rest <- [q]
      ]
    answers = case w.asked of
      OCheck p hs Seq.:<| rest ->
        let (yes, no) = (filter (`Set.member` settled w) hs, filter (`Set.notMember` settled w) hs)
         in [("the pool answers the check of " <> show hs, feed sc (Checked p yes no) w {asked = rest})]
      -- the runner looks at what is not vouched for before it offers it
      OOffer h _ False Seq.:<| rest
        | h `elem` sc.forged ->
            [("the store finds " <> show h <> " forged", feed sc (Offered h Forged) w {asked = rest})]
        | h `elem` sc.scripts,
          Set.notMember h (settled w) ->
            [("the store cannot verify " <> show h, feed sc (Offered h Unverified) w {asked = rest})]
      OOffer h b _ Seq.:<| rest ->
        let missing = filter (`Set.notMember` settled w) (parentsOf b)
         in if null missing
              then
                [ ( "the pool takes " <> show h,
                    feed sc (Offered h Settled) w {asked = rest, pool = Set.insert h w.pool}
                  )
                ]
              else [("the pool lacks parents of " <> show h, feed sc (Offered h (Orphan missing)) w {asked = rest})]
      _ -> []
    resync = [("the store is in sync", feed sc (Synced True) w {inSync = True, hadBlocks = True}) | not w.inSync]
    -- time passes only when nothing else can happen: a second at a time
    idle = [("a second passes", feed sc (Tick 1) w)]
    storeReads p = \case
      MInv hs -> Named p hs
      MTx h -> Body (Just p) h (B (Map.findWithDefault [] h sc.txs))
      MNotFound hs -> Missing p hs

-- | What a node does with a message of the store (A2, A3).
nodeReads :: Scenario -> P -> ToNode -> World -> World
nodeReads sc p msg w = case Map.lookup p w.nodes of
  Nothing -> w
  Just node -> case msg of
    MSweep
      | Set.null node.has -> w
      | otherwise ->
          w
            { nodes = Map.insert p node {told = Set.union node.told node.has, answered = node.has} w.nodes,
              -- A5: parents before children, unless the world is one
              -- that breaks it
              toStore = Map.adjust (|> MInv (sortOn inOrder (Set.toAscList node.has))) p w.toStore
            }
    MGet hs ->
      let (found, lost) = (filter (`Set.member` node.has) hs, filter (`Set.notMember` node.has) hs)
          answers = map MTx found <> [MNotFound lost | not (null lost)]
          reply = foldl' (|>) (fromMaybe' (Map.lookup p w.toStore)) answers
       in w {toStore = Map.insert p reply w.toStore}
  where
    fromMaybe' = maybe Seq.empty id
    inOrder h = if sc.backwards then negate (depth h) else depth h
    depth :: H -> Int
    depth h = case Map.findWithDefault [] h sc.txs of
      [] -> 0
      ps -> 1 + maximum (map depth ps)

-- | The disturbances of a scenario, from a world.
disturbed :: Scenario -> World -> [(String, Either String World)]
disturbed sc w
  | inFlight > sc.busy = []
  | otherwise = concatMap of' sc.disturbances
  where
    inFlight =
      sum (map Seq.length (Map.elems w.toNode))
        + sum (map Seq.length (Map.elems w.toStore))
        + Seq.length w.asked
    everywhere = Set.unions (map (.has) (Map.elems w.nodes))
    of' = \case
      Mempools
        | sc.oneNode ->
            [ ("the node takes " <> show h, Right (foldl' (\x p -> arriveIn x p h) w (Map.keys w.nodes)))
            | (h, ps) <- Map.toList sc.txs,
              h `notElem` sc.forged,
              Set.notMember h everywhere,
              Set.notMember h w.confirmed,
              all (\x -> Set.member x everywhere || Set.member x w.confirmed) ps
            ]
              <> [ ( "the node drops " <> show h,
                     Right w {nodes = Map.map (\n -> without (family h) n) w.nodes}
                   )
                 | h <- Set.toList everywhere
                 ]
        | otherwise ->
            [ ("the node of peer " <> show p <> " takes " <> show h, Right (arriveIn w p h))
            | (p, node) <- Map.toList w.nodes,
              (h, ps) <- Map.toList sc.txs,
              h `notElem` sc.forged,
              Set.notMember h node.has,
              Set.notMember h w.confirmed,
              all (\x -> Set.member x node.has || Set.member x w.confirmed) ps
            ]
              <> [ ( "the node of peer " <> show p <> " drops " <> show h,
                     Right w {nodes = Map.insert p (without (family h) node) w.nodes}
                   )
                 | (p, node) <- Map.toList w.nodes,
                   h <- Set.toList node.has
                 ]
      Blocks ->
        [ ( "a block confirms " <> show h,
            Right
              w
                { confirmed = Set.insert h w.confirmed,
                  pool = Set.delete h w.pool,
                  nodes = Map.map (without (Set.singleton h)) w.nodes
                }
          )
        | h <- Set.toList everywhere,
          h `notElem` sc.forged,
          all (`Set.member` w.confirmed) (Map.findWithDefault [] h sc.txs)
        ]
      Desyncs -> [("the store is out of sync", feed sc (Synced False) w {inSync = False}) | w.inSync]
      PeerDrops ->
        [ ( "peer " <> show p <> " is gone",
            feed
              sc
              (PeerDown p)
              w
                { nodes = Map.insert p node {online = False, told = Set.empty, answered = Set.empty} w.nodes,
                  toNode = Map.insert p Seq.empty w.toNode,
                  toStore = Map.insert p Seq.empty w.toStore
                }
          )
        | (p, node) <- Map.toList w.nodes,
          node.online
        ]
      PoolFails -> case w.asked of
        OCheck p hs Seq.:<| rest ->
          [("the pool does not answer the check of " <> show hs, feed sc (Unchecked p hs) w {asked = rest})]
        OOffer h _ _ Seq.:<| rest ->
          [("the pool does not judge " <> show h, feed sc (Offered h Again) w {asked = rest})]
        _ -> []
      PoolEmptied -> [("the pool loses what it had", feed sc Emptied w {pool = Set.empty}) | not (Set.null w.pool)]
      Timeouts ->
        [ ("a request gets no answer in time", feed sc (Tick (sc.config.patience + 1)) w)
        | pending w.machine > 0
        ]
      Clients ->
        [ ("a client gives the store " <> show h, feed sc (Body Nothing h (B ps)) w)
        | (h, ps) <- Map.toList sc.txs,
          Set.notMember h w.confirmed
        ]
      OtherStores ->
        [ ("another store offers " <> show h <> " first", Right w {pool = Set.insert h w.pool})
        | (h, ps) <- Map.toList sc.txs,
          h `notElem` sc.forged,
          Set.notMember h (settled w),
          all (`Set.member` settled w) ps
        ]
    -- a node no longer has some transactions
    without gone n = n {has = Set.difference n.has gone, answered = Set.difference n.answered gone}
    -- a transaction and all that spends from it
    family h = close (Set.singleton h)
      where
        close s =
          let s' = Set.union s (Set.fromList [c | (c, ps) <- Map.toList sc.txs, any (`Set.member` s) ps])
           in if s' == s then s else close s'
    -- A1: a node names a transaction once, to a peer that is there. Its
    -- RPC names nothing by itself.
    arriveIn x p h = case Map.lookup p x.nodes of
      Just node
        | node.online && p `notElem` sc.feeds ->
            x
              { nodes = Map.insert p node {has = Set.insert h node.has, told = Set.insert h node.told} x.nodes,
                toStore = Map.adjust (|> MInv [h]) p x.toStore
              }
        | otherwise -> x {nodes = Map.insert p node {has = Set.insert h node.has} x.nodes}
      Nothing -> x

-- | A peer comes: not a disturbance, and not something that has to
-- happen. A peer that stays away is no source.
--
-- The name of a peer stands for one connection, as in haskoin-node, where
-- every connection is a peer of its own: an answer of the pool about what
-- a peer named is for that connection, never for a later one to the same
-- node. A world has few names, so a name is used again only once nothing
-- on its way mentions it, which no one can tell from a new name.
comes :: Scenario -> World -> [(String, Either String World)]
comes sc w =
  [ ( "peer " <> show p <> " connects",
      feed sc (PeerUp p node.source) w {nodes = Map.insert p node {online = True} w.nodes}
        >>= \x -> if p `elem` sc.owned then feed sc (Own p) x else Right x
    )
  | (p, node) <- Map.toList w.nodes,
    not node.online,
    null [() | OCheck q _ <- toList w.asked, q == p]
  ]

-- | T3 at rest: the pool has every transaction of the mempool of every
-- source that is connected.
--
-- Of a node that the store has the RPC of and a peer to, that is so while
-- the peer is connected: the RPC announces nothing, so what the node
-- takes while no peer is there to be told is in the pool at the next
-- reading of the RPC, which is by the clock and not by anything that
-- happens.
converged :: Scenario -> World -> Bool
converged sc w
  | sc.oneNode && not (or [node.online | (p, node) <- Map.toList w.nodes, p `notElem` sc.feeds]) = True
  | otherwise =
      and
        [ dueOf sc w p node `Set.isSubsetOf` settled w
        | (p, node) <- Map.toList w.nodes,
          node.online,
          node.source
        ]

-- | What the pool must have of what a source has. A node of the operator
-- is taken at its word: all that it has. Of a stranger, what the store
-- can verify: not what is forged, not what it cannot verify unless a node
-- of the operator that is there has it too, and nothing that spends from
-- either.
dueOf :: Scenario -> World -> P -> Node -> Set H
dueOf sc w p node
  | p `elem` sc.owned = node.has
  | otherwise = Set.filter (not . barred) node.has
  where
    barred h = h `elem` held || any barred (Map.findWithDefault [] h sc.txs)
    held = sc.forged <> [h | h <- sc.scripts, Set.notMember h vouchable]
    vouchable =
      Set.unions [n.has | (q, n) <- Map.toList w.nodes, q `elem` sc.owned, n.online, n.source]
        `Set.union` settled w

-- | Go through every state of a scenario. In each: the rules of every
-- step. From each, with only things going well: no way to go on for ever,
-- and no way to stop short of 'converged'.
explore :: Scenario -> Result
explore sc = search (Map.singleton w0 Nothing) [w0] [] 0
  where
    w0 = initial sc
    -- breadth first, so that the steps to a failure are the fewest
    search seen [] [] n = lively seen n
    search seen [] later n = search seen (reverse later) [] n
    search seen (w : rest) later n
      | Map.size seen > sc.limit =
          Result (Map.size seen) n (Just ("more states than the limit: a smaller world is needed", []))
      | otherwise = searchOne seen w rest later n
    searchOne seen w rest later n =
      let next = calm sc w <> disturbed sc w <> comes sc w
       in case [(label, why) | (label, Left why) <- next] of
            (label, why) : _ -> Result (Map.size seen) n (Just (why, trail seen w <> [label]))
            [] ->
              let new = [(x, label) | (label, Right x) <- next, Map.notMember x seen]
                  seen' = foldl' (\m (x, label) -> Map.insertWith (\_ old -> old) x (Just (label, w)) m) seen new
                  fresh = Set.toList (Set.fromList (map fst new))
               in search seen' rest (reverse fresh <> later) (n + length next)
    trail seen w = reverse (go w)
      where
        go x = case Map.lookup x seen of
          Just (Just (label, from)) -> label : go from
          _ -> []
    -- with only things going well: every run ends, and ends converged
    lively seen n = case walk Map.empty (Map.keys seen) of
      Left (what, w, path) ->
        let steps = trail seen w <> ("--- from here, only things that go well:" : path)
         in Result (Map.size seen) n (Just (what, steps))
      Right _ -> Result (Map.size seen) n Nothing
    round' = "with only things going well, the store goes round for ever without the mempool of a source"
    short = "the store is at rest and the pool lacks transactions of the mempool of a source"
    -- depth first, with the states on the way marked: meeting one again is a
    -- way to go on for ever
    walk done [] = Right done
    walk done (w : ws) = visit done w [] >>= \done' -> walk done' ws
    visit done w path = case Map.lookup w done of
      Just True -> Right done
      Just False -> Left (round', w, reverse path)
      Nothing ->
        let next = [(label, x) | (label, Right x) <- calm sc w, x /= w]
         in if null next
              then
                if converged sc w
                  then Right (Map.insert w True done)
                  else Left (short, w, reverse path)
              else do
                let on acc (label, x) = acc >>= \d -> visit d x (label : path)
                done' <- foldl' on (Right (Map.insert w False done)) next
                return (Map.insert w True done')

-- | One run through a world, with a number for each choice: the rules of
-- every step are checked along it, and at its end, with only things
-- going well from there, the store must come to rest with the mempool of
-- every source. For worlds too large to go through whole, a sample of
-- their runs: evidence of another kind, and weaker.
wander :: Scenario -> [Int] -> Either (String, [String]) ()
wander sc = go (initial sc) []
  where
    go w path [] = settleDown w path (10000 :: Int)
    go w path (n : ns) =
      let next = calm sc w <> disturbed sc w <> comes sc w
       in case next !! (abs n `mod` length next) of
            (label, Left why) -> Left (why, reverse (label : path))
            (label, Right w') -> go w' (label : path) ns
    settleDown w path fuel = case [(label, x) | (label, x) <- calm sc w, x /= Right w] of
      [] | converged sc w -> Right ()
      [] -> Left ("the store is at rest and the pool lacks transactions of the mempool of a source", reverse path)
      (label, Left why) : _ -> Left (why, reverse (label : path))
      (label, Right w') : _
        | fuel <= 0 -> Left ("with only things going well, the store does not come to rest", reverse path)
        | otherwise -> settleDown w' (label : path) (fuel - 1)

-- | A world too large to go through whole, for 'wander': two chains of
-- three, two sources and a peer that is none, and every disturbance.
large :: Scenario
large =
  Scenario
    { name = "two chains of three, three peers, every disturbance",
      config =
        Config
          { room = 5,
            heldRoom = 3,
            minWindow = 1,
            maxWindow = 3,
            patience = 2,
            orphanLife = 6,
            retry = 1,
            sweepGap = 2,
            sweepEvery = 0,
            sweepFast = 0
          },
      txs = Map.fromList [(1, []), (2, [1]), (3, [2]), (4, []), (5, [4]), (6, [5, 3])],
      peers = [(1, True), (2, True), (3, False)],
      oneNode = False,
      feeds = [],
      owned = [],
      forged = [],
      liars = [],
      scripts = [],
      backwards = False,
      disturbances = [minBound .. maxBound],
      busy = 6,
      limit = 0
    }

-- | A world that must __fail__: a chain of two and room for one id and one
-- body, as one of 'scenarios', with a source that names the child before
-- what it spends from, against A5. The child gets the only room, cannot
-- be offered without its parent and cannot wait in the last room, so it
-- is given up, and the next reading of the source goes the same way: the
-- store goes round for ever.
--
-- It is here to say what A5 is needed for. With room to spare the order
-- of names does not matter, as the worlds that ask for a child first
-- show; at the bound it decides which ids get the room. A source that
-- named in another order would make a store with less room than its
-- node's mempool has transactions never take the rest.
withoutOrder :: Scenario
withoutOrder =
  Scenario
    { name = "a chain of two, room for one id and one body, a source that names the child first",
      config =
        Config
          { room = 1,
            heldRoom = 1,
            minWindow = 1,
            maxWindow = 2,
            patience = 1,
            orphanLife = 2,
            retry = 1,
            sweepGap = 1,
            sweepEvery = 0,
            sweepFast = 0
          },
      txs = Map.fromList [(1, []), (2, [1])],
      peers = [(1, True)],
      oneNode = False,
      feeds = [],
      owned = [],
      forged = [],
      liars = [],
      scripts = [],
      backwards = True,
      disturbances = [Mempools],
      busy = 2,
      limit = 1500000
    }

-- | The worlds that the specs go through: each small enough to go
-- through whole in seconds. A world has the mempools of its nodes
-- changing and one or two other disturbances, so that every disturbance
-- meets every other in some world, two at a time; all of them at once
-- only fit with one transaction.
scenarios :: [Scenario]
scenarios =
  [ world "one transaction, every disturbance" one [(1, True)] [minBound .. maxBound] small,
    world "a chain of two, blocks" chain [(1, True)] [Mempools, Blocks] small,
    world "a chain of two, a peer that goes" chain [(1, True)] [Mempools, PeerDrops] small,
    world "a chain of two, requests that get no answer" chain [(1, True)] [Mempools, Timeouts] small,
    world "a chain of two, a pool that fails" chain [(1, True)] [Mempools, PoolFails] small,
    world "a chain of two, a pool that loses what it had" chain [(1, True)] [Mempools, PoolEmptied] small,
    world "a chain of two, a store out of sync" chain [(1, True)] [Mempools, Desyncs] small,
    world "a chain of two, clients" chain [(1, True)] [Mempools, Clients] small,
    world "a chain of two, another store" chain [(1, True)] [Mempools, OtherStores] small,
    world "a chain of two, room for one id and one body" chain [(1, True)] [Mempools, Clients] tight,
    world "a chain asked for child first, room for one body" upside [(1, True)] [Mempools] small {heldRoom = 1},
    world "a chain of three asked for child first, room for two bodies" upside3 [(1, True)] [Mempools] two,
    world "a chain of two, a peer that goes and no answers" chain [(1, True)] [Mempools, PeerDrops, Timeouts] small,
    world "a chain of two, the pool failing and emptied" chain [(1, True)] [Mempools, PoolFails, PoolEmptied] small,
    world "a chain of two, blocks and a store out of sync" chain [(1, True)] [Mempools, Blocks, Desyncs] small,
    world "one transaction, a source and a peer that is none" one mixed [Mempools, PeerDrops, Timeouts] small,
    world "one transaction, two sources" one [(1, True), (2, True)] [Mempools, PeerDrops] small,
    -- a node that whitelists nobody, and a store that has its RPC: a
    -- source that does not announce, and a peer that announces and is no
    -- source, with one mempool between them
    (world "one transaction, one node: its RPC and a peer that is no source" one mixed [Mempools, PeerDrops] small)
      { oneNode = True,
        feeds = [1]
      },
    (world "a chain of two, one node: its RPC and a peer that is no source" chain mixed [Mempools, PeerDrops] small)
      { oneNode = True,
        feeds = [1]
      },
    -- T8: peer 1 is a stranger and peer 2 a node of the operator. The
    -- store cannot verify transaction 1, and 2 spends from it.
    (world "a stranger and a node of the operator, a transaction that cannot be verified" one both wavering small)
      { owned = [2],
        scripts = [1]
      },
    (world "the same, with blocks" one both [Mempools, Blocks] small)
      { owned = [2],
        scripts = [1]
      },
    -- T9: the pool is measured against the node of the operator, and is
    -- not in sync after it lost what it had
    (world "a stranger and a node of the operator, a pool that loses what it had" one both losing small)
      { owned = [2]
      },
    -- one node with two connections, so that the two have one mempool:
    -- a peer that is a stranger, and the RPC, which is the operator's
    (world "the same, and a transaction that spends from it" chain both [Mempools] small)
      { owned = [2],
        scripts = [1],
        oneNode = True,
        feeds = [2]
      },
    -- nobody to vouch for it: the store comes to rest without it, and
    -- without what spends from it
    (world "only a stranger, a transaction that the store cannot verify" chain [(1, True)] wavering small)
      { scripts = [1]
      },
    -- the stranger has a forged transaction, and one that spends from it
    (world "a stranger that lies, and a node of the operator" one both wavering small)
      { owned = [2],
        forged = [1],
        liars = [1]
      },
    (world "the same, and a transaction that is good" pair both [Mempools] small)
      { owned = [2],
        forged = [1],
        liars = [1]
      },
    (world "only a stranger, which lies, a pool that fails" three [(1, True)] [Mempools, PoolFails] small)
      { forged = [1, 2],
        liars = [1]
      }
  ]
  where
    losing = [Mempools, PoolEmptied]
    world name txs peers disturbances config =
      Scenario
        { name,
          config,
          txs,
          peers,
          oneNode = False,
          feeds = [],
          owned = [],
          forged = [],
          liars = [],
          scripts = [],
          backwards = False,
          disturbances,
          busy = 2,
          limit = 1500000
        }
    mixed = [(1, True), (2, False)]
    both = [(1, True), (2, True)]
    wavering = [Mempools, PeerDrops]
    -- a chain of two and a transaction by itself
    three = Map.fromList [(1, []), (2, [1]), (3, [])]
    pair = Map.fromList [(1, []), (2, [])]
    tight = small {room = 1, heldRoom = 1}
    two = small {heldRoom = 2, maxWindow = 3}
    one = Map.fromList [(1, [])]
    chain = Map.fromList [(1, []), (2, [1])]
    -- the child has the smaller id, so it is asked for first
    upside = Map.fromList [(2, []), (1, [2])]
    upside3 = Map.fromList [(3, []), (2, [3]), (1, [2])]
    small =
      Config
        { room = 8,
          heldRoom = 8,
          minWindow = 1,
          maxWindow = 2,
          patience = 1,
          orphanLife = 2,
          retry = 1,
          sweepGap = 1,
          sweepEvery = 0,
          sweepFast = 0
        }
