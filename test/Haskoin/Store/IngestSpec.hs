-- |
-- Module      : Haskoin.Store.IngestSpec
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The checks of "Haskoin.Store.Ingest" against
-- @docs\/mempool-ingestion.md@. Three kinds, and what each is worth:
--
-- * __every order of events of a small world__
--   ("Haskoin.Store.Ingest.Model"): nothing is sampled, so a pass means
--   that no order of events in that world breaks a rule, and that from
--   every state the store comes to rest with the mempool of its sources;
--
-- * __sampled runs of a larger world__: weaker, and there to say whether
--   what holds in the small worlds still does with more of everything;
--
-- * __examples__, which prove nothing and show what the rules mean.
module Haskoin.Store.IngestSpec (spec) where

import Control.Monad (forM_)
import Data.IntMap.Strict qualified as IntMap
import Data.IntSet qualified as IntSet
import Data.List (foldl', partition)
import Data.Sequence qualified as Seq
import Haskoin.Store.Ingest
import Haskoin.Store.Ingest.Model
import Test.Hspec hiding (pending)
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck (arbitrary, choose, counterexample, forAll, property, vectorOf)

spec :: Spec
spec = do
  -- a group for each world, so that haskell-dev runs them in different
  -- processes (haskell-dev.toml)
  forM_ scenarios $ \sc ->
    describe ("In every order of events of " <> sc.name) $
      it "keeps the rules and converges" $ do
        let result = explore sc
        case result.failure of
          Nothing -> result.states `shouldSatisfy` (> 100)
          Just _ -> expectationFailure (describeResult result)
  describe "With a source that names a transaction before what it spends from, against A5" $
    it "a store with room for one id goes round for ever: the order is needed at the bound" $
      case (explore withoutOrder).failure of
        Just (what, _) -> what `shouldContain` "goes round for ever"
        Nothing -> expectationFailure "The world that breaks A5 converges"
  describe "in runs drawn from a larger world" $
    modifyMaxSuccess (const 2000) $
      it ("keeps the rules and converges: " <> large.name) $
        forAll (choose (0, 400) >>= \n -> vectorOf n arbitrary) $ \choices ->
          case wander large choices of
            Right () -> property True
            Left (what, path) -> counterexample (unlines (what : map ("  " <>) path)) False
  describe "A whole mempool that comes at once, most of it spending from the rest" $
    it "is offered to the pool once each, and no transaction twice" $ do
      -- as many as a full mempool of Bitcoin, in chains of 24
      let count = 65000
          (taken, refused, time) = refill count
      -- every one is in the pool, in the time of that many offers and of
      -- the seconds in which the machine waits for nothing
      taken `shouldBe` count
      time `shouldSatisfy` (< count + 5000)
      -- and the pool was asked for nothing that it could only refuse
      refused `shouldBe` 0
  describe "for example" $ do
    it "offers a chain root first, whatever the order its transactions come in" $ do
      -- 3 spends from 2, which spends from 1, and they come child first
      let came = [Body (Just 1) 3 [2], Body (Just 1) 2 [1], Body (Just 1) 1 []]
          (s, outs) = run ([Named 1 [1, 2, 3], Checked 1 [] [1, 2, 3]] <> came)
      [h | Offer h _ <- outs] `shouldBe` [1 :: Int]
      let (_, more) = run' [Offered 1 Settled, Offered 2 Settled, Offered 3 Settled] s
      [h | Offer h _ <- more] `shouldBe` [2, 3]
      [h | Gone h Done <- more] `shouldBe` [1, 2, 3]
    it "keeps what a peer names while the store is out of sync, and asks for it after" $ do
      let (s, outs) = run' [Synced False, Named 1 [7], Checked 1 [] [7]] ready
      [() | Get {} <- outs] `shouldBe` []
      placeOf 7 s `shouldSatisfy` \case
        Just (Wanted _) -> True
        _ -> False
      snd (run' [Synced True] s) `shouldBe` [Get 1 [7 :: Int]]
    it "reads its source again after it gave something up" $ do
      let tight = (start defaultConfig {room = 1} 0 :: Ingest Int Int [Int])
          (s, outs) = run' [PeerUp 1 True, Synced True, Named 1 [1, 2], Checked 1 [] [1, 2]] tight
      [h | Gone h (GaveUp NoRoom) <- outs] `shouldBe` [2]
      isDirty s `shouldBe` True
      -- not sooner than the gap, and then it does
      snd (run' [Tick 1000] s) `shouldBe` []
      snd (run' [Tick 10000] s) `shouldBe` [Sweep 1]
    it "reads its source after thirty seconds while transactions come, and ever less often when none do" $ do
      -- a transaction that the pool lacks is named, comes and is taken
      let came h = [Named 1 [h], Checked 1 [] [h], Body (Just 1) h [], Offered h Settled]
          readings t s = [() | Sweep _ <- snd (run' [Tick t] s)]
          at t s = fst (run' [Tick t] s)
          (s1, _) = run (came 1)
      -- not before thirty seconds, and then
      readings 29999 s1 `shouldBe` []
      readings 30000 s1 `shouldBe` [()]
      -- one more comes: thirty seconds again
      let (s2, _) = run' (came 2) (at 30000 s1)
      readings 59999 s2 `shouldBe` []
      readings 60000 s2 `shouldBe` [()]
      -- none comes: thirty, then a minute, two, four, eight, and ten for good
      let times = [90000, 150000, 270000, 510000, 990000, 1590000, 2190000]
          quiet = scanl (flip at) (at 60000 s2) times
      forM_ (zip quiet times) $ \(s, t) -> do
        readings (t - 1) s `shouldBe` []
        readings t s `shouldBe` [()]
      -- and thirty seconds after one comes again
      let (s4, _) = run' (came 3) (last quiet)
      readings (2190000 + 29999) s4 `shouldBe` []
      readings (2190000 + 30000) s4 `shouldBe` [()]
    it "asks a peer for no more than its window, which grows as the peer answers" $ do
      let (s, outs) = run [Named 1 [1 .. 100], Checked 1 [] [1 .. 100]]
      [length hs | Get _ hs <- outs] `shouldBe` [16]
      pending s `shouldBe` 16
      let (s', more) = run' [Body (Just 1) 1 []] s
      [length hs | Get _ hs <- more] `shouldBe` [2]
      windowOf 1 s' `shouldBe` Just 17
    it "does not offer at its time what waits for a transaction that has a place, and does once it has none" $ do
      -- 2 spends from 1, and comes first: it waits for 1, which is asked for
      let (s, outs) = run [Named 1 [1, 2], Checked 1 [] [1, 2], Body (Just 1) 2 [1]]
      [h | Offer h _ <- outs] `shouldBe` []
      -- its time comes, after two seconds, four and eight, and 1 is still
      -- on its way: nothing to offer
      let (s', more) = run' [Tick 2000, Tick 4000, Tick 8000] s
      [h | Offer h _ <- more] `shouldBe` []
      -- the node says that it has not 1, which is given up: at its next
      -- time 2 is offered, since nothing else would wake it
      let (_, last') = run' [Missing 1 [1], Tick 16000] s'
      [h | Gone h (GaveUp NodeHasNot) <- last'] `shouldBe` [1]
      [h | Offer h _ <- last'] `shouldBe` [2 :: Int]
    it "holds what a stranger sent and the store cannot verify until a node of the operator names it" $ do
      -- peer 1 is a stranger, and what it sent is not vouched for
      let (s, outs) = run [Named 1 [7], Checked 1 [] [7], Body (Just 1) 7 []]
      [h | Offer h _ <- outs] `shouldBe` [7 :: Int]
      vouchedFor 7 s `shouldBe` False
      -- the store cannot verify it: held, and not offered when time passes
      let (doubting, more) = run' [Offered 7 Unverified, Tick 1000, Tick 2000] s
      [h | Offer h _ <- more] `shouldBe` []
      placeOf 7 doubting `shouldSatisfy` \case
        Just (Held _ _ (Doubted _)) -> True
        _ -> False
      -- peer 2, a node of the operator, names it: offered again, as
      -- vouched for
      let (named, last') = run' [PeerUp 2 True, Own 2, Named 2 [7]] doubting
      [h | Offer h _ <- last'] `shouldBe` [7]
      vouchedFor 7 named `shouldBe` True
    it "gives up what nobody vouched for after twenty minutes, and reads no source for it" $ do
      let (s, _) = run [Named 1 [7], Checked 1 [] [7], Body (Just 1) 7 [], Offered 7 Unverified]
          (later', outs) = run' [Tick 1199000, Tick 1201000] s
      [h | Gone h (GaveUp NotVouched) <- outs] `shouldBe` [7 :: Int]
      isDirty later' `shouldBe` False
    it "gives up a forged transaction with all that waits for it, and reads no source for them" $ do
      -- 8 spends from 7, and waits for it
      let came = [Named 1 [7, 8], Checked 1 [] [7, 8], Body (Just 1) 8 [7], Body (Just 1) 7 []]
          (s, outs) = run' [Offered 7 Forged] (fst (run came))
      [h | Gone h (GaveUp IsForged) <- outs] `shouldBe` [7, 8 :: Int]
      isDirty s `shouldBe` False
    it "takes a node of the operator at its word, for what it sends and for what that spends from" $ do
      -- peer 1 is a node of the operator; 8 spends from 7, which the pool
      -- lacks and nobody named
      let own = fst (run' [Own 1] ready)
          (s, _) = run' [Named 1 [8], Checked 1 [] [8], Body (Just 1) 8 [7], Offered 8 (Orphan [7])] own
      vouchedFor 8 s `shouldBe` True
      vouchedFor 7 s `shouldBe` True
  where
    ready :: Ingest Int Int [Int]
    ready = fst (run' [PeerUp 1 True, Synced True] (start defaultConfig 0))
    run = flip run' ready
    run' inputs s0 = foldl' (\(s, outs) i -> let (s', o) = step id i s in (s', outs <> o)) (s0, []) inputs

-- | A store with an empty pool and one source whose mempool has so many
-- transactions, in chains of 24: what a store goes through when its Redis
-- lost its data, or when it starts with a new pool. Gives how many
-- transactions the pool took, how many offers it refused for lack of what
-- the transaction spends from, and the time that it all took.
--
-- It is the runner in small: it carries out what the machine says, one
-- output at a time, and tells the machine the time once a second. The
-- node names its whole mempool when asked and gives every transaction
-- that it is asked for. The pool takes a transaction when it has all
-- that it spends from. Only an offer takes time, a millisecond: about
-- what one took on a server, and what made the futile ones matter.
--
-- The ids are scattered, as hashes are, so that the order in which the
-- machine asks for transactions says nothing of which spends from which:
-- most come before what they spend from, and wait for it.
refill :: Int -> (Int, Int, Tick)
refill count = go (start defaultConfig 0) IntSet.empty 0 0 0 0 (Seq.fromList [PeerUp 1 True, Synced True])
  where
    -- a prime above the count: multiplying by a number below it scatters
    -- the ids and gives no two the same
    prime = 65537
    idOf k = ((k + 1) * 48271) `mod` prime
    bodies = IntMap.fromList [(idOf k, [idOf (k - 1) | k `mod` 24 /= 0]) | k <- [0 .. count - 1]]
    names = IntMap.keys bodies
    cost = 1
    go ::
      Ingest Int Int [Int] ->
      IntSet.IntSet ->
      Tick ->
      Tick ->
      Int ->
      Int ->
      Seq.Seq (Input Int Int [Int]) ->
      (Int, Int, Tick)
    go s pool now told taken refused queue
      -- all in, or an hour gone: the test says which
      | IntSet.size pool == count || now > 3600000 = (taken, refused, now)
      | otherwise = case Seq.viewl queue of
          -- nothing to do until the next second
          Seq.EmptyL -> let t = told + 1000 in feed (Tick t) s pool t t taken refused Seq.empty
          i Seq.:< rest
            | now - told >= 1000 -> feed (Tick now) s pool now now taken refused queue
            | otherwise -> feed i s pool now told taken refused rest
    feed i s pool now told taken refused queue =
      let (s', outs) = step id i s
          (pool', now', taken', refused', answers) = foldl' perform (pool, now, taken, refused, []) outs
       in go s' pool' now' told taken' refused' (queue <> Seq.fromList (reverse answers))
    perform (pool, now, taken, refused, answers) = \case
      Sweep p -> (pool, now, taken, refused, Named p names : answers)
      Check p hs ->
        let (yes, no) = partition (`IntSet.member` pool) hs
         in (pool, now, taken, refused, Checked p yes no : answers)
      Get p hs -> (pool, now, taken, refused, reverse [Body (Just p) h (bodies IntMap.! h) | h <- hs] <> answers)
      Offer h b -> case filter (`IntSet.notMember` pool) b of
        [] -> (IntSet.insert h pool, now + cost, taken + 1, refused, Offered h Settled : answers)
        lacks -> (pool, now + cost, taken, refused + 1, Offered h (Orphan lacks) : answers)
      Gone _ _ -> (pool, now, taken, refused, answers)
