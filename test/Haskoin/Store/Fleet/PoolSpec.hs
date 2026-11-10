module Haskoin.Store.Fleet.PoolSpec (spec) where

import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set (Set)
import Data.Set qualified as Set
import Haskoin.Store.Fleet.Pool
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck

-- | An output: the transaction that made it, and which of its two outputs.
type Out = (Int, Int)

-- | A chain, newest block first, each block its coinbase and then its
-- other transactions; what every transaction ever made spends; the shared
-- pool; and the number of the next transaction.
data World = World
  { chain :: ![[Int]],
    inputs :: !(Map Int [Out]),
    pool :: !(Pool Int Out),
    fresh :: !Int
  }
  deriving (Show)

-- | Something that happens, with a number to choose the details from.
data Op
  = -- | a store offers a transaction, which may spend what is not there or
    -- what the pool spends, naming what it replaces or not
    Offer !Int
  | -- | a store withdraws a transaction
    Withdraw !Int
  | -- | the tip moves to a new block, with some of the pool and perhaps a
    -- transaction that the pool has not seen
    Mine !Int
  | -- | the tip moves back a block
    Unwind
  deriving (Show)

instance Arbitrary Op where
  arbitrary =
    frequency
      [ (8, Offer <$> choose (0, 100000)),
        (1, Withdraw <$> choose (0, 100000)),
        (3, Mine <$> choose (0, 100000)),
        (1, pure Unwind)
      ]

start :: World
start = World {chain = [[0]], inputs = Map.singleton 0 [], pool = empty, fresh = 1}

-- | The outputs of the chain that no transaction of the chain spends.
unspent :: World -> Set Out
unspent w = made `Set.difference` used
  where
    txs = concat w.chain
    made = Set.fromList [(t, i) | t <- txs, i <- [0, 1]]
    used = Set.fromList (concatMap (\t -> Map.findWithDefault [] t w.inputs) txs)

pick :: Int -> [a] -> [a]
pick _ [] = []
pick n xs = [xs !! (n `mod` length xs)]

step :: World -> Op -> World
step w = \case
  Offer n ->
    let t = w.fresh
        chainOuts = Set.toList (unspent w)
        poolOuts = [(p, i) | p <- Map.keys w.pool.spends, i <- [0, 1]]
        -- now and then an output that never was, or one long spent
        odd' = [(t + 7, 0), (0, 0)]
        from = case n `mod` 10 of
          0 -> odd'
          k | k < 6 -> chainOuts
          _ -> poolOuts <> chainOuts
        spent = Set.toList (Set.fromList (pick n from <> pick (n `div` 7) (chainOuts <> poolOuts)))
        rivals = [x | o <- spent, Just x <- [Map.lookup o w.pool.spender]]
        replaced = if even (n `div` 3) then rivals else []
        w' = w {fresh = t + 1, inputs = Map.insert t spent w.inputs}
     in case offer fst (`Set.member` unspent w) t spent replaced w.pool of
          Taken p -> w' {pool = p}
          Refused _ -> w'
  Withdraw n -> w {pool = withdraw (pick n (Map.keys w.pool.spends)) w.pool}
  Mine n ->
    let coinbase = w.fresh
        free = unspent w
        -- transactions of the pool whose parents are in the chain or picked
        -- before them, as a block needs
        ready picked t = all (\o -> Set.member o free || fst o `elem` picked) (Map.findWithDefault [] t w.pool.spends)
        close picked =
          case [t | t <- Map.keys w.pool.spends, t `notElem` picked, ready picked t] of
            t : _ | length picked < n `mod` 5 -> close (picked <> [t])
            _ -> picked
        fromPool = close []
        -- and one that the pool never saw, which may spend what it spends
        taken = concatMap (\t -> Map.findWithDefault [] t w.pool.spends) fromPool
        unseen = [(coinbase + 1, pick (n `div` 5) [o | o <- Set.toList free, o `notElem` taken]) | even n]
        unseen' = [(t, os) | (t, os) <- unseen, not (null os)]
        block = coinbase : fromPool <> map fst unseen'
        ins = foldr (uncurry Map.insert) (Map.insert coinbase [] w.inputs) unseen'
        change =
          Change
            { returned = [],
              gone = [],
              confirmed = block,
              spent = concatMap (\t -> Map.findWithDefault [] t ins) block
            }
     in w {chain = block : w.chain, inputs = ins, fresh = coinbase + 2, pool = advance fst change w.pool}
  Unwind -> case w.chain of
    (coinbase : txs) : rest@(_ : _) ->
      let change =
            Change
              { returned = [(t, Map.findWithDefault [] t w.inputs) | t <- txs],
                gone = [coinbase],
                confirmed = [],
                spent = []
              }
       in w {chain = rest, pool = advance fst change w.pool}
    _ -> w

-- | What is wrong with the pool of a world, if anything.
wrong :: World -> [String]
wrong w =
  [ "transaction " <> show t <> " spends " <> show o <> ", which nothing made or something spent"
  | (t, os) <- Map.toList w.pool.spends,
    o <- os,
    not (member (fst o) w.pool),
    not (Set.member o free)
  ]
    <> ["transaction " <> show t <> " is in a block and in the pool" | t <- concat w.chain, member t w.pool]
    <> ["two transactions spend one output" | Map.size spenders /= length (concat (Map.elems w.pool.spends))]
    <> ["the index of spenders is off" | spenders /= w.pool.spender]
    <> ["the index of children is off" | kids /= w.pool.children]
  where
    free = unspent w
    spenders = Map.fromList [(o, t) | (t, os) <- Map.toList w.pool.spends, o <- os]
    kids = Map.fromListWith Set.union [(fst o, Set.singleton t) | (t, os) <- Map.toList w.pool.spends, o <- os]

spec :: Spec
spec = describe "shared mempool" $ do
  let a = 10 :: Int
      b = 11 :: Int
      c = 12 :: Int
      onChain o = fst o == (0 :: Int)
      taken = \case
        Taken p -> p
        Refused why -> error (show why)
      one = taken (offer fst onChain a [(0, 0)] [] (empty :: Pool Int Out))
      two = taken (offer fst onChain b [(a, 0)] [] one)

  it "keeps the first of two that spend the same output, unless it is replaced" $ do
    offer fst onChain c [(0, 0)] [] two `shouldBe` Refused (Conflict [a])
    -- replacing the first takes what spends it along
    Map.keys (taken (offer fst onChain c [(0, 0)] [a] two)).spends `shouldBe` [c]

  it "refuses what spends an output that is neither in the pool nor unspent at the tip" $ do
    offer fst onChain c [(5, 0)] [] two `shouldBe` Refused (Orphan [(5, 0)])
    offer fst onChain a [(0, 0)] [] two `shouldBe` Refused Known

  it "lets a confirmed transaction go alone, and a conflict with all that spends it" $ do
    let confirmedA = Change {returned = [], gone = [], confirmed = [a], spent = [(0, 0)]}
        rival = Change {returned = [], gone = [], confirmed = [c], spent = [(0, 0)]}
    Map.keys (advance fst confirmedA two).spends `shouldBe` [b]
    advance fst rival two `shouldBe` empty

  it "takes back what an abandoned block had confirmed, with what spends it" $ do
    let forward = Change {returned = [], gone = [], confirmed = [a], spent = [(0, 0)]}
        back = Change {returned = [(a, [(0, 0)])], gone = [], confirmed = [], spent = []}
        undone = advance fst back (advance fst forward two)
    undone `shouldBe` two
    -- the transaction is the parent of the other undone
    withdraw [a] undone `shouldBe` empty

  modifyMaxSuccess (const 3000) $ do
    it "stays valid on top of the tip, whatever is offered, mined and unwound" $
      property $ \ops ->
        let worlds = scanl step start ops
            trouble = [(i, problems) | (i, w) <- zip [0 :: Int ..] worlds, let problems = wrong w, not (null problems)]
            final = last worlds
         in counterexample (show (take 1 trouble)) $
              cover 20 (Map.size final.pool.spends >= 3) "three or more in the pool at the end" $
                cover 20 (length final.chain >= 3) "three or more blocks at the end" (null trouble)
