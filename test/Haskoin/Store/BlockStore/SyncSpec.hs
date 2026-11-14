{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}

-- | The state machine of the block download, driven by random sequences of
-- events, with its rules checked after every step.
module Haskoin.Store.BlockStore.SyncSpec (spec) where

import Data.ByteString.Char8 qualified as C8
import Data.List (nub)
import Data.Maybe (fromMaybe, isNothing)
import Data.Time.Calendar (fromGregorian)
import Data.Time.Clock (NominalDiffTime, UTCTime (..), addUTCTime, diffUTCTime)
import Haskoin (BlockHash (..), sha256)
import Haskoin.Store.BlockStore.Sync
import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

-- | Something that happens to the block store, from one of four peers.
data Event
  = Start Int
  | Pass Int
  | Release Int
  | Request [Int]
  | -- | a peer sends a block, which is imported if it is accepted
    Deliver Int Int
  | Forget
  | Wait Int
  deriving stock (Show)

instance Arbitrary Event where
  arbitrary =
    frequency
      [ (2, Start <$> peer),
        (2, Pass <$> peer),
        (1, Release <$> peer),
        (3, Request <$> listOf block),
        (4, Deliver <$> peer <*> block),
        (1, pure Forget),
        (2, Wait <$> choose (0, 30))
      ]
    where
      peer = choose (0, 3)
      block = choose (0, 9)

-- | A state and the time it is at.
type World = (UTCTime, Sync Int)

hash :: Int -> BlockHash
hash = BlockHash . sha256 . C8.pack . show

t0 :: UTCTime
t0 = UTCTime (fromGregorian 2026 11 9) 0

at :: Int -> UTCTime
at secs = addUTCTime (fromIntegral secs) t0

-- | Apply an event as the block store does: imports only what 'accept'
-- takes.
step :: World -> Event -> World
step (now, s) = \case
  Start p -> (now, fromMaybe s (start now p s))
  Pass p -> (now, snd (pass now p s))
  Release p -> (now, snd (release p s))
  Request bs -> (now, request (map hash bs) s)
  Deliver p b -> case accept p (hash b) s of
    (Requested, s') -> (now, imported now (hash b) s')
    (_, s') -> (now, s')
  Forget -> (now, forgetRequests s)
  Wait secs -> (addUTCTime (fromIntegral secs) now, s)

-- | Every state that a sequence of events goes through.
run :: [Event] -> [World]
run = scanl step (t0, idle t0)

-- | Each state with the event that follows it and the state after.
steps :: [Event] -> [(World, Event, World)]
steps es = zip3 (run es) es (drop 1 (run es))

timeout, stall :: NominalDiffTime
timeout = 60
stall = 10

-- | What a peer owes, as the syncing peer or as one passed over.
owed :: Int -> Sync Int -> Maybe [BlockHash]
owed p s = case [a.owes | a <- maybe [] pure s.current <> s.passed, a.peer == p] of
  [] -> Nothing
  os -> Just (concat os)

spec :: Spec
spec = describe "Block download" $ do
  prop "never wants a block twice" $ \es ->
    all (\(_, s) -> nub (outstanding s) == outstanding s) (run es)

  prop "has each peer once, and no peer passed over that owes nothing" $ \es ->
    and
      [ nub peers == peers && all (not . null . (.owes)) s.passed
      | (_, s) <- run es,
        let peers = maybe [] pure (syncingPeer s) <> passedPeers s
      ]

  prop "changes its syncing peer only by starting, passing it over or releasing it" $ \es ->
    and
      [ case e of
          Start p -> syncingPeer s' == if isNothing (syncingPeer s) then Just p else syncingPeer s
          Pass p -> syncingPeer s' == (p <$ syncingPeer s)
          Release p | syncingPeer s == Just p -> isNothing (syncingPeer s')
          _ -> syncingPeer s' == syncingPeer s
      | ((_, s), e, (_, s')) <- steps es
      ]

  prop "keeps a peer that is passed over owed its blocks" $ \es ->
    and
      [ owed old s' == owed old s
      | ((_, s), Pass p, (_, s')) <- steps es,
        Just old <- [syncingPeer s],
        old /= p,
        owed old s /= Just []
      ]

  prop "accepts a block only from a peer that was asked for it, as often as it was asked" $ \es ->
    forAll (choose (0, 3)) $ \peer -> forAll (hash <$> choose (0, 9)) $ \h ->
      let (_, s) = last (run es)
          (verdict, s') = accept peer h s
          copies = length . filter (== h) . fromMaybe []
       in conjoin
            [ verdict === case owed peer s of
                Nothing -> NotSyncingPeer
                Just os
                  | h `notElem` os -> NotRequested
                  | h `elem` outstanding s -> Requested
                  | otherwise -> Late,
              -- the peer owes one copy fewer: it may send a block as many
              -- times as it was asked for it, as after a reorganisation
              counterexample "still owed" $
                verdict `notElem` [Requested, Late] || copies (owed peer s') == copies (owed peer s) - 1
            ]

  prop "imports a block from whichever peer delivers it first" $ \es ->
    and
      [ hash b `notElem` outstanding s'
      | ((_, s), Deliver p b, (_, s')) <- steps es,
        fst (accept p (hash b) s) == Requested
      ]

  prop "keeps the requested blocks in the order they were asked for" $ \es bs ->
    let (_, s) = last (run es)
        s' = request (map hash bs) s
     in case syncingPeer s of
          Nothing -> outstanding s' === outstanding s
          Just _ -> take (length (outstanding s)) (outstanding s') === outstanding s

  prop "asks the next syncing peer for all that is wanted" $ \es ->
    and
      [ fmap snd asked == Just (outstanding s) && all (`elem` fromMaybe [] (owed p s')) (outstanding s)
      | ((now, s), Pass p, _) <- steps es,
        Just old <- [syncingPeer s],
        old /= p,
        let (asked, s') = pass now p s
      ]

  prop "passes over a peer only when blocks are wanted and none came within its patience" $ \es ->
    and
      [ stalled stall timeout now s
          == if not (null (outstanding s)) && now `diffUTCTime` s.time > patience stall timeout s
            then syncingPeer s
            else Nothing
      | (now, s) <- run es
      ]

  prop "is patient for the stall at least, and for the timeout at most" $ \es ->
    and [patience stall timeout s >= stall && patience stall timeout s <= timeout | (_, s) <- run es]

  prop "times out only a syncing peer when nothing was imported for longer than the timeout" $ \es ->
    and
      [ timedOut timeout now s == if now `diffUTCTime` s.time > timeout then syncingPeer s else Nothing
      | (now, s) <- run es
      ]

  prop "expires the peers passed over that owe blocks for longer than the timeout" $ \es ->
    and
      [ expired timeout now s == [a.peer | a <- s.passed, now `diffUTCTime` a.since > timeout]
      | (now, s) <- run es
      ]

  prop "asks for more blocks exactly when the window has room" $ \es (Positive window) ->
    let (_, s) = last (run es)
     in wantsMore window s === case syncingPeer s of
          Just p | needsMoreBlocks window (length (outstanding s)) -> Just p
          _ -> Nothing

  describe "needsMoreBlocks" $ do
    it "asks once fewer than a quarter of the window remain" $ do
      needsMoreBlocks 64 16 `shouldBe` False
      needsMoreBlocks 64 15 `shouldBe` True
    it "asks once none remain, even with a window below four" $ do
      needsMoreBlocks 1 1 `shouldBe` False
      needsMoreBlocks 1 0 `shouldBe` True
      needsMoreBlocks 3 0 `shouldBe` True

  it "resets the clock when a block is imported" $ do
    let s = fromMaybe (idle t0) (start t0 (0 :: Int) (idle t0))
        s' = imported (at 50) (hash 1) (request [hash 1] s)
    timedOut timeout (at 100) s `shouldBe` Just 0
    timedOut timeout (at 100) s' `shouldBe` Nothing

  describe "a peer that does not deliver" $ do
    -- peer 0 is asked for block 1 and stays silent
    let asked = request [hash 1] (fromMaybe (idle t0) (start t0 (0 :: Int) (idle t0)))
    it "is passed over after the stall, and not before" $ do
      stalled stall timeout (at 10) asked `shouldBe` Nothing
      stalled stall timeout (at 11) asked `shouldBe` Just 0
    it "is not passed over when nothing is wanted" $
      stalled stall timeout (at 1000) (fromMaybe (idle t0) (start t0 (0 :: Int) (idle t0))) `shouldBe` Nothing
    it "has its block taken if it delivers before the next peer" $ do
      let (told, s) = pass (at 11) 1 asked
      told `shouldBe` Just (0, [hash 1])
      syncingPeer s `shouldBe` Just 1
      passedPeers s `shouldBe` [0]
      fst (accept 0 (hash 1) s) `shouldBe` Requested
    it "has its block dropped, and is not at fault, if the next peer delivered first" $ do
      let (_, s) = pass (at 11) 1 asked
          (first, s') = accept 1 (hash 1) s
          s'' = imported (at 12) (hash 1) s'
      first `shouldBe` Requested
      fst (accept 0 (hash 1) s'') `shouldBe` Late
    it "gives the next peers twice as long each time, up to the timeout" $ do
      let (_, s1) = pass (at 11) 1 asked
          (_, s2) = pass (at 32) 2 s1
          (_, s3) = pass (at 73) 3 s2
      map (patience stall timeout) [asked, s1, s2, s3] `shouldBe` [10, 20, 40, 60]
      -- peer 1 was asked at 11 seconds, and has 20 from then
      stalled stall timeout (at 31) s1 `shouldBe` Nothing
      stalled stall timeout (at 32) s1 `shouldBe` Just 1
    it "goes back to the first stall when a block is imported" $ do
      let (_, s1) = pass (at 11) 1 asked
          s2 = imported (at 12) (hash 1) (snd (accept 1 (hash 1) s1))
      patience stall timeout s2 `shouldBe` 10
    it "is disconnected if it still owes its block after the timeout" $ do
      let (_, s) = pass (at 11) 1 asked
      -- silent since the start, not since it was passed over
      expired timeout (at 60) s `shouldBe` []
      expired timeout (at 61) s `shouldBe` [0]
    it "is forgotten when it disconnects" $ do
      let (_, s) = pass (at 11) 1 asked
          (wasSyncing, s') = release 0 s
      wasSyncing `shouldBe` False
      passedPeers s' `shouldBe` []
      fst (accept 0 (hash 1) s') `shouldBe` NotSyncingPeer
    it "still owes its blocks if it becomes the syncing peer again" $ do
      let (_, s1) = pass (at 11) 1 asked
          (_, s2) = release 1 s1
          s3 = fromMaybe s2 (start (at 20) 0 s2)
      passedPeers s3 `shouldBe` []
      -- the block is not wanted any more: the request went with peer 1
      fst (accept 0 (hash 1) s3) `shouldBe` Late
