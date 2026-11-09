-- | The protocol of stores sharing one Redis, against a model of Redis:
-- several stores catch up with a chain in which every block changes one
-- cached xpub, in a random interleaving, and decide with the functions of
-- "Haskoin.Store.Cache.Protocol" what to do at each step.
module Haskoin.Store.Cache.ProtocolSpec (spec) where

import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Haskoin.Store.Cache.Protocol
import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

-- | The claim of the change of a block.
data Key = Absent | BusyBy Int | DoneKey
  deriving stock (Eq, Show)

-- | Redis: the height the entry is stamped with, and the claims.
data Redis = Redis
  { stamp :: !(Maybe Integer),
    claims :: !(IntMap Key)
  }
  deriving stock (Show)

-- | A store: its height, the change it holds, and changes to claim again.
data Store = Store
  { height :: !Int,
    holding :: !(Maybe Int),
    retry :: ![Int]
  }

-- | What happened.
data Outcome = Outcome
  { -- | the stamp written by each write, by the store and change it was for
    writes :: ![(Int, Int, Integer)],
    -- | claims given up
    releases :: !Int,
    redis :: !Redis,
    -- | whether every store reached the tip with nothing left to do
    finished :: !Bool
  }
  deriving stock (Show)

-- | Try to claim the change of a block, and do what 'afterClaim' says.
tryClaim :: Int -> Int -> Redis -> Store -> (Redis, Store)
tryClaim s i r st =
  let found = case IntMap.findWithDefault Absent i r.claims of
        Absent -> Claimed
        DoneKey -> Done
        BusyBy _ -> Busy
   in case afterClaim found of
        ReloadNow -> (r {claims = IntMap.insert i (BusyBy s) r.claims}, st {holding = Just i})
        Skip -> (r, st)
        ClaimLater -> (r, st {retry = st.retry <> [i]})

-- | A step of a store, if it can take one: reload for the change it holds,
-- claim a change again, or import the next block and claim its change.
steps :: Int -> Int -> Outcome -> Store -> [(Outcome, Store)]
steps tip s o st = case st.holding of
  Just i ->
    let h = toInteger st.height
        -- the entry has a transaction the store has not seen if it is
        -- stamped with a later block, as each block adds one
        unknown = maybe False (> h) o.redis.stamp
        reload = if mayReplace o.redis.stamp (fromIntegral st.height) unknown then Reloaded else Behind
        r = o.redis
     in case afterReload reload of
          Finish ->
            [ ( o {writes = o.writes <> [(s, i, h)], redis = r {stamp = Just h, claims = IntMap.insert i DoneKey r.claims}},
                st {holding = Nothing}
              )
            ]
          Release ->
            [ ( o {releases = o.releases + 1, redis = r {claims = IntMap.insert i Absent r.claims}},
                st {holding = Nothing}
              )
            ]
          Hold -> []
  Nothing ->
    [ let (r, st') = tryClaim s i o.redis st {retry = rest} in (o {redis = r}, st')
    | i : rest <- [st.retry]
    ]
      <> [ let (r, st') = tryClaim s (st.height + 1) o.redis st {height = st.height + 1}
            in (o {redis = r}, st')
         | st.height < tip
         ]

-- | Run stores that start at the given heights until none can take a step,
-- picking the next step at random. The changes of the blocks up to the
-- lowest store were applied before.
simulate :: Int -> [Int] -> Gen Outcome
simulate tip starts =
  simulateFrom
    (Redis (Just (toInteger (minimum starts))) (IntMap.fromList [(i, DoneKey) | i <- [1 .. minimum starts]]))
    tip
    starts

-- | Like 'simulate', from a state of Redis.
simulateFrom :: Redis -> Int -> [Int] -> Gen Outcome
simulateFrom r0 tip starts = go (10000 :: Int) start0 (IntMap.fromList (zip [0 ..] stores0))
  where
    stores0 = [Store h Nothing [] | h <- starts]
    start0 = Outcome {writes = [], releases = 0, redis = r0, finished = False}
    go 0 o _ = return o
    go n o stores =
      case [(s, x) | (s, st) <- IntMap.toList stores, x <- steps tip s o st] of
        [] -> return o {finished = True}
        xs -> do
          (s, (o', st')) <- elements xs
          go (n - 1) o' (IntMap.insert s st' stores)

-- | A small chain and up to four stores at random heights on it.
chain :: Gen (Int, [Int])
chain = do
  tip <- choose (1, 12)
  stores <- choose (1, 4)
  starts <- vectorOf stores (choose (0, tip))
  return (tip, starts)

spec :: Spec
spec = describe "Stores sharing Redis" $ do
  prop "all reach the tip with nothing left to do" $
    forAll chain $
      \(tip, starts) -> forAll (simulate tip starts) $ \o -> o.finished

  prop "end with the entry of the tip" $
    forAll chain $ \(tip, starts) -> forAll (simulate tip starts) $ \o ->
      o.redis.stamp === Just (toInteger tip)

  prop "never replace an entry with one that knows less" $
    forAll chain $ \(tip, starts) -> forAll (simulate tip starts) $ \o ->
      let stamps = [h | (_, _, h) <- o.writes]
       in and (zipWith (<=) stamps (drop 1 stamps))

  prop "write once per change, and again only for a change given up" $
    forAll chain $ \(tip, starts) -> forAll (simulate tip starts) $ \o ->
      length o.writes <= tip - minimum starts + o.releases

  prop "leave nothing to do for stores that catch up after the first" $
    forAll chain $ \(tip, starts) ->
      forAll (simulate tip [minimum starts]) $ \first ->
        forAll (simulateFrom first.redis tip starts) $ \rest ->
          rest.writes === []

  it "claims, skips or tries again as the claim says" $
    map afterClaim [minBound .. maxBound] `shouldBe` [ReloadNow, Skip, ClaimLater]

  it "finishes, releases or holds as the reload says" $
    map afterReload [minBound .. maxBound] `shouldBe` [Finish, Release, Hold]

  it "replaces an entry of the same height or lower, never a higher one" $ do
    mayReplace Nothing 5 False `shouldBe` True
    mayReplace (Just 5) 5 False `shouldBe` True
    mayReplace (Just 6) 5 False `shouldBe` False
    mayReplace (Just 4) 5 True `shouldBe` False
