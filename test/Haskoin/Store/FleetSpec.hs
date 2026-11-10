module Haskoin.Store.FleetSpec (spec) where

import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set (Set)
import Data.Set qualified as Set
import Haskoin.Store.Fleet
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck

-- | A store of the model: its main chain, newest block first, and the blocks
-- that it has views of.
data Node = Node
  { chain :: ![Int],
    views :: !(Set Int)
  }
  deriving (Show)

-- | A fleet of three stores, the chain that the network has, newest block
-- first, the state in Redis, and the time.
data World = World
  { network :: ![Int],
    fresh :: !Int,
    nodes :: !(Map Int Node),
    redis :: !(Fleet Int Int),
    now :: !Time
  }
  deriving (Show)

-- | Something that happens.
data Op
  = -- | the network finds a block
    Grow
  | -- | the network replaces its last blocks, as many as given, with as many
    -- new ones and one more
    Reorg !Int
  | -- | a store catches up with the network by so many steps, each the
    -- reversal or the import of one block
    Sync !Int !Int
  | -- | a store reports, moves the tip if it can, and lets go of the views
    -- that it need not keep
    Beat !Int
  | -- | time passes
    Tick !Time
  deriving (Show)

instance Arbitrary Op where
  arbitrary =
    frequency
      [ (3, pure Grow),
        (1, Reorg <$> choose (1, 3)),
        (8, Sync <$> choose (0, 2) <*> choose (1, 4)),
        (8, Beat <$> choose (0, 2)),
        (3, Tick <$> choose (1, 6))
      ]

-- | How a store of the model behaves: whether it asks before it reports,
-- and whether it keeps the views that the rules tell it to.
data Rules = Rules
  { asks :: !Bool,
    keeps :: !Bool
  }

byTheRules :: Rules
byTheRules = Rules {asks = True, keeps = True}

-- | Leases last ten ticks, and a store reports its six newest blocks.
lease :: Time
lease = 10

window :: Int
window = 6

start :: World
start =
  World
    { network = [0],
      fresh = 1,
      nodes = Map.fromList [(s, Node {chain = [0], views = Set.singleton 0}) | s <- [0 .. 2]],
      redis = empty,
      now = 0
    }

step :: Rules -> World -> Op -> World
step rules w = \case
  Grow -> w {network = w.fresh : w.network, fresh = w.fresh + 1}
  Reorg n ->
    let kept = drop (min n (length w.network - 1)) w.network
        new = [w.fresh + n, w.fresh + n - 1 .. w.fresh]
     in w {network = new <> kept, fresh = w.fresh + n + 1}
  Sync s n -> w {nodes = Map.adjust (\node -> iterate catchUp node !! n) s w.nodes}
  Tick dt -> w {now = w.now + dt}
  Beat s ->
    let node = w.nodes Map.! s
        has b = Set.member b node.views
        -- only what it still has a view of: a view that was let go cannot be
        -- had again
        blocks = filter has (take window node.chain)
        allowed = not rules.asks || mayReport w.now s has w.redis
        reported = report w.now lease s blocks w.redis
        moved = case move w.now reported.tip reported of
          Moved fleet -> fleet
          _ -> reported
        kept
          | rules.keeps = Set.fromList (keep blocks moved.tip) `Set.intersection` node.views
          | otherwise = Set.fromList (take 1 node.chain)
     in if allowed
          then w {redis = moved, nodes = Map.insert s node {views = kept} w.nodes}
          else w
  where
    -- one step towards the chain of the network: revert a block that it
    -- does not have, or import the next one that it has
    catchUp node
      | b : rest <- node.chain, b `notElem` w.network = node {chain = rest}
      | next : _ <- reverse (takeWhile (`notElem` node.chain) w.network) =
          node {chain = next : node.chain, views = Set.insert next node.views}
      | otherwise = node

-- | The stores whose lease has not run out and that have no view of the
-- tip: those that would have to refuse a request although they are counted
-- in.
stranded :: World -> [Int]
stranded w =
  [ s
  | Just t <- [w.redis.tip],
    s <- Map.keys (live w.now w.redis),
    not (Set.member t (w.nodes Map.! s).views)
  ]

-- | Every state that a list of operations goes through.
states :: Rules -> [Op] -> [World]
states rules = scanl (step rules) start

spec :: Spec
spec = describe "fleet tip" $ do
  it "is the newest block that every live store reported" $ do
    let fleet = foldl' (\f (s, bs) -> report 0 10 s bs f) (empty :: Fleet Int Int) reports'
        reports' = [(0, [5, 4, 3, 2]), (1, [4, 3, 2, 1]), (2, [7, 6, 3, 2])]
    agreed 0 fleet `shouldBe` Just 3
    -- a store whose lease has run out no longer counts
    agreed 12 (report 12 10 0 [5, 4, 3] (report 12 10 1 [4, 3] fleet)) `shouldBe` Just 4
    agreed 20 fleet `shouldBe` Nothing

  it "moves once when two stores try at the same time" $ do
    let fleet = report 0 10 0 [2, 1] (report 0 10 1 [2, 1] (empty :: Fleet Int Int))
    case move 0 Nothing fleet of
      Moved next -> do
        next.tip `shouldBe` Just 2
        -- the second store saw the same tip as the first, and is too late
        move 0 Nothing next `shouldBe` Raced
        move 0 (Just 2) next `shouldBe` Stays
      other -> expectationFailure (show other)

  it "keeps a store that is behind from pulling the tip back" $ do
    let ahead = report 0 10 0 [9, 8, 7, 6] (empty :: Fleet Int Int)
        fleet = case move 0 Nothing ahead of
          Moved f -> f
          _ -> ahead
    fleet.tip `shouldBe` Just 9
    mayReport 1 1 (`elem` [7, 6, 5 :: Int]) fleet `shouldBe` False
    mayReport 1 1 (`elem` [9, 8, 7 :: Int]) fleet `shouldBe` True
    -- when the store that was ahead is gone, the other may start again
    mayReport 11 1 (`elem` [7, 6, 5 :: Int]) fleet `shouldBe` True

  it "answers from the tip, from its own block before there is one, or not at all" $ do
    let fleet = (empty :: Fleet Int Int) {tip = Just 4}
    answer (const True) (empty :: Fleet Int Int) `shouldBe` Alone
    answer (== 4) fleet `shouldBe` At 4
    answer (== 5) fleet `shouldBe` Behind

  modifyMaxSuccess (const 2000) $ do
    it "never leaves a live store without a view of the tip" $
      property $ \ops ->
        let trouble = [(w.redis.tip, stranded w) | w <- states byTheRules ops, not (null (stranded w))]
         in counterexample (show (take 1 trouble)) (null trouble)

    it "moves along and across branches, never to a block that no store has" $
      property $ \ops ->
        let tips = [t | w <- states byTheRules ops, Just t <- [w.redis.tip]]
            made w = w.fresh
         in conjoin
              [ cover 5 (length (Set.fromList tips) > 3) "the tip was at more than three blocks" True,
                property (all (< made (last (states byTheRules ops))) tips)
              ]

    it "ends at the best block once every store has caught up and reported" $
      property $ \ops ->
        let settled = foldl' (step byTheRules) (last (states byTheRules ops)) settle
            settle = [Tick 20] <> [Sync s 100 | s <- [0 .. 2]] <> [Beat s | s <- [0 .. 2]] <> [Beat 0]
         in (maybe [] pure settled.redis.tip, stranded settled) === (take 1 settled.network, [])

    it "needs the store to ask before it reports" $
      expectFailure . property $ \ops ->
        all (null . stranded) (states byTheRules {asks = False} ops)

    it "needs the store to keep the views of what it reported" $
      expectFailure . property $ \ops ->
        all (null . stranded) (states byTheRules {keeps = False} ops)
