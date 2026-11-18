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

  prop "is patient for the floor at least, and for the timeout at most" $ \es ->
    and [patience stall timeout s >= stallFloor && patience stall timeout s <= timeout | (_, s) <- run es]

  prop "is patient for the stall given until a block is imported, then for at most that" $ \es ->
    and
      [ if isNothing s.gap then base == stall else base <= stall
      | (_, s) <- run es,
        s.passes == 0,
        let base = patience stall timeout s
      ]

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
    it "goes back to a stall of its own pace when a block is imported" $ do
      let (_, s1) = pass (at 11) 1 asked
          s2 = imported (at 12) (hash 1) (snd (accept 1 (hash 1) s1))
      -- one second to deliver: four seconds of patience, no doubling
      patience stall timeout s2 `shouldBe` 4
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

  describe "a stall that follows the peer's pace" $ do
    let ms = (/ 1000) . fromIntegral :: Int -> NominalDiffTime
        later = flip addUTCTime t0
        started = fromMaybe (idle t0) (start t0 (0 :: Int) (idle t0))
        -- peer 0 delivers blocks 1 to 99 of 100, one every 10 ms
        quick =
          foldl
            (\s' b -> imported (later (ms (10 * b))) (hash b) (snd (accept 0 (hash b) s')))
            (request (map hash [1 .. 100]) started)
            [1 .. 99]
    it "passes over a quick peer that went quiet after the floor, not the stall" $ do
      patience stall timeout quick `shouldBe` stallFloor
      stalled stall timeout (later (ms 990 + stallFloor)) quick `shouldBe` Nothing
      stalled stall timeout (later (ms 1000 + stallFloor)) quick `shouldBe` Just 0
    it "gives a slow peer several of its gaps, up to the stall" $ do
      let slow g =
            foldl
              (\s' b -> imported (later (g * fromIntegral b)) (hash b) (snd (accept 0 (hash b) s')))
              (request (map hash [1 .. 10]) started)
              [1 .. 9 :: Int]
      patience stall timeout (slow 1) `shouldBe` 4
      patience stall timeout (slow 5) `shouldBe` stall
    it "is not made jumpy by one quick block after slow ones" $ do
      let s = imported (later 3.1) (hash 3) (snd (accept 0 (hash 3) slow2))
          slow2 =
            imported (later 3) (hash 2) . snd . accept 0 (hash 2) $
              imported (later 1.5) (hash 1) (snd (accept 0 (hash 1) (request (map hash [1 .. 4]) started)))
      patience stall timeout s `shouldSatisfy` (> stallFloor)
    it "starts the clock of the next peer when it passes over a quiet one" $ do
      let (_, s) = pass (later (ms 1000 + stallFloor + 1)) 1 quick
      -- the next peer gets twice the floor, from when it was asked
      patience stall timeout s `shouldBe` 2 * stallFloor
      stalled stall timeout (later (ms 1000 + 3 * stallFloor)) s `shouldBe` Nothing

  describe "a peer that answers a ping while the syncing peer is stalled" $ do
    let wait = 5 :: NominalDiffTime
        asked = request [hash 1] (fromMaybe (idle t0) (start t0 (0 :: Int) (idle t0)))
        -- the tick at 11 seconds finds peer 0 stalled and pings 1 and 2
        (Just (n, pinged), searching) = ping (at 11) 3 [1, 2] asked
    it "is chosen at once, and the stalled peer is passed over for it" $ do
      pinged `shouldBe` [1, 2]
      stalled stall timeout (at 11) asked `shouldBe` Just 0
      let (chosen, s) = pong wait (at 11) 2 n searching
      chosen `shouldBe` True
      -- the block store passes over when the peer is still stalled
      stalled stall timeout (at 11) s `shouldBe` Just 0
      let (told, s') = pass (at 11) 2 s
      told `shouldBe` Just (0, [hash 1])
      syncingPeer s' `shouldBe` Just 2
      probing s' `shouldBe` False
      overdue wait (at 17) s' `shouldBe` []
    it "is not chosen if the syncing peer delivered in the meantime" $ do
      let (_, s) = pong wait (at 11) 2 n searching
          s' = imported (at 11) (hash 1) (snd (accept 0 (hash 1) s))
      stalled stall timeout (at 11) s' `shouldBe` Nothing

  describe "Looking for a peer to ask" $ do
    let wait = 5 :: NominalDiffTime
        (Just (n, _), searching) = ping (at 0) 7 [0, 1] (idle t0)
    it "pings the peers once, all with the same nonce" $ do
      n `shouldBe` 7
      probing searching `shouldBe` True
      -- peer 2 announces the block later: it is pinged, the others not again
      let (more, s) = ping (at 1) 9 [0, 1, 2] searching
      more `shouldBe` Just (7, [2])
      fst (ping (at 2) 9 [0, 1, 2] s) `shouldBe` Nothing
    it "chooses the first peer that answers in time" $ do
      let (chosen, s) = pong wait (at 1) 1 7 searching
      chosen `shouldBe` True
      -- the search goes on with the rest until that peer is made the syncing peer
      probing s `shouldBe` True
      probing <$> start (at 1) 1 s `shouldBe` Just False
    it "takes no answer to another ping, nor from a peer that was not pinged" $ do
      pong wait (at 1) 1 8 searching `shouldBe` (False, searching)
      pong wait (at 1) 3 7 searching `shouldBe` (False, searching)
    it "does not choose a peer that answers late while another may be in time" $ do
      let (_, s1) = ping (at 3) 9 [2] searching
          (chosen, s2) = pong wait (at 6) 0 7 s1
      chosen `shouldBe` False
      -- peer 1 is late too, and peer 2 has until 8 seconds
      overdue wait (at 6) s2 `shouldBe` []
      fst (pong wait (at 7) 2 7 s2) `shouldBe` True
    it "asks a late peer when there is no other option, one that answered first" $ do
      overdue wait (at 5) searching `shouldBe` []
      overdue wait (at 6) searching `shouldBe` [0, 1]
      let (_, s) = pong wait (at 7) 1 7 searching
      overdue wait (at 7) s `shouldBe` [1, 0]
    it "ends when the last peer pinged is gone" $ do
      let (_, s1) = release 0 searching
      probing s1 `shouldBe` True
      probing (skip 1 s1) `shouldBe` False
      probing (settle searching) `shouldBe` False
