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

-- | Something that happens to the block store, from one of three peers.
data Event
  = Start Int
  | Release Int
  | Request [Int]
  | Import Int
  | Forget
  | Wait Int
  deriving stock (Show)

instance Arbitrary Event where
  arbitrary =
    frequency
      [ (2, Start <$> peer),
        (1, Release <$> peer),
        (3, Request <$> listOf block),
        (3, Import <$> block),
        (1, pure Forget),
        (2, Wait <$> choose (0, 30))
      ]
    where
      peer = choose (0, 2)
      block = choose (0, 9)

-- | A state and the time it is at.
type World = (UTCTime, Sync Int)

hash :: Int -> BlockHash
hash = BlockHash . sha256 . C8.pack . show

t0 :: UTCTime
t0 = UTCTime (fromGregorian 2026 11 9) 0

-- | Apply an event as the block store does: imports only what 'accept'
-- takes.
step :: World -> Event -> World
step (now, s) = \case
  Start p -> (now, fromMaybe s (start now p s))
  Release p -> (now, snd (release p s))
  Request bs -> (now, request (map hash bs) s)
  Import b -> case syncingPeer s of
    Just p | accept p (hash b) s == Requested -> (now, imported now (hash b) s)
    _ -> (now, s)
  Forget -> (now, forgetRequests s)
  Wait secs -> (addUTCTime (fromIntegral secs) now, s)

-- | Every state that a sequence of events goes through.
run :: [Event] -> [World]
run = scanl step (t0, Idle)

timeout :: NominalDiffTime
timeout = 60

spec :: Spec
spec = describe "Block download" $ do
  prop "never has a block outstanding twice" $ \es ->
    all (\(_, s) -> nub (outstanding s) == outstanding s) (run es)

  prop "has a syncing peer only after starting, and keeps it until released" $ \es ->
    and
      [ case e of
          Start _ -> syncingPeer s' == syncingPeer s || isNothing (syncingPeer s)
          Release p
            | syncingPeer s == Just p -> syncingPeer s' == Nothing
          _ -> syncingPeer s' == syncingPeer s
      | ((_, s), e, (_, s')) <- zip3 (run es) es (drop 1 (run es))
      ]

  prop "accepts a block only from the syncing peer, and only if outstanding" $ \es ->
    forAll (choose (0, 2)) $ \peer -> forAll (hash <$> choose (0, 9)) $ \h ->
      let (_, s) = last (run es)
       in accept peer h s === case syncingPeer s of
            Just q
              | q /= peer -> NotSyncingPeer
              | h `elem` outstanding s -> Requested
              | otherwise -> NotRequested
            Nothing -> NotSyncingPeer

  prop "keeps the requested blocks in the order they were asked for" $ \es bs ->
    let (_, s) = last (run es)
        s' = request (map hash bs) s
     in case syncingPeer s of
          Nothing -> outstanding s' === []
          Just _ -> take (length (outstanding s)) (outstanding s') === outstanding s

  prop "times out only a peer that delivered nothing for longer than the timeout" $ \es ->
    and
      [ case s of
          Idle -> isNothing (timedOut timeout now s)
          Syncing d -> (timedOut timeout now s == Just d.peer) == (now `diffUTCTime` d.time > timeout)
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

  it "resets the clock of the peer when it delivers a block" $ do
    let s = Syncing (Download (0 :: Int) t0 [])
        later = addUTCTime 50 t0
        s' = imported later (hash 1) (request [hash 1] s)
    timedOut timeout (addUTCTime 100 t0) s `shouldBe` Just 0
    timedOut timeout (addUTCTime 100 t0) s' `shouldBe` Nothing
