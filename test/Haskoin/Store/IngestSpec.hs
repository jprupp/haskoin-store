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
import Data.List (foldl')
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
  describe "in runs drawn from a larger world" $
    modifyMaxSuccess (const 2000) $
      it ("keeps the rules and converges: " <> large.name) $
        forAll (choose (0, 400) >>= \n -> vectorOf n arbitrary) $ \choices ->
          case wander large choices of
            Right () -> property True
            Left (what, path) -> counterexample (unlines (what : map ("  " <>) path)) False
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
    it "asks a peer for no more than its window, which grows as the peer answers" $ do
      let (s, outs) = run [Named 1 [1 .. 100], Checked 1 [] [1 .. 100]]
      [length hs | Get _ hs <- outs] `shouldBe` [16]
      pending s `shouldBe` 16
      let (s', more) = run' [Body (Just 1) 1 []] s
      [length hs | Get _ hs <- more] `shouldBe` [2]
      windowOf 1 s' `shouldBe` Just 17
  where
    ready :: Ingest Int Int [Int]
    ready = fst (run' [PeerUp 1 True, Synced True] (start defaultConfig 0))
    run = flip run' ready
    run' inputs s0 = foldl' (\(s, outs) i -> let (s', o) = step id i s in (s', outs <> o)) (s0, []) inputs
