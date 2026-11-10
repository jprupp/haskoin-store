module Haskoin.Store.Fleet.RedisSpec (spec) where

import Control.Monad (foldM_, forM_)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.Containers.ListUtils (nubOrd, nubOrdOn)
import Data.IORef (atomicModifyIORef', newIORef)
import Data.List (sort)
import Data.Map.Strict qualified as Map
import Data.Serialize (encode)
import Data.Set qualified as Set
import Data.Word (Word32)
import Database.Redis qualified as Redis
import Haskoin.Store.Fleet (Time, empty, live, mayReport, move, report)
import Haskoin.Store.Fleet qualified as F
import Haskoin.Store.Fleet.Pool qualified as P
import Haskoin.Store.Fleet.Redis
import Haskoin.Store.Integration.Image (testImage)
import Haskoin.Store.Integration.Podman (hostPort, podmanUsable, withContainer)
import Haskoin.Store.Integration.Valkey (redisContainer, waitRedis)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck

-- | A step of a fleet of three stores with blocks numbered from 0 to 9.
data Step
  = -- | a store reports some blocks, having views of some
    Report !Int ![Word32] ![Word32]
  | -- | a store tries to move the tip: from the tip as it is, or from
    -- another block, as one that was beaten to it would
    Shift !(Maybe Word32)
  | -- | milliseconds pass
    Wait !Time
  deriving (Show)

instance Arbitrary Step where
  arbitrary =
    frequency
      [ (5, Report <$> choose (0, 2) <*> blocks <*> blocks),
        (4, pure (Shift Nothing)),
        (1, Shift . Just <$> choose (0, 9)),
        (2, Wait <$> choose (1, 6))
      ]
    where
      blocks = choose (0, 5) >>= \n -> vectorOf n (choose (0, 9))

spec :: Spec
spec = do
  usable <- runIO podmanUsable
  describe "fleet tip in Redis" $ case usable of
    Left why -> it "does what the rules say" (pendingWith why)
    Right () -> aroundAll withRedis $ do
      modifyMaxSuccess (const 300) $
        it "does what the rules say, step by step" $ \(conn, fresh) ->
          property $ \steps -> ioProperty $ do
            keys <- fresh
            foldM_ (check conn keys) (empty :: F.Fleet ByteString ByteString, 1000) (steps :: [Step])
      it "counts leases on the clock of Redis when given no other" $ \(conn, fresh) -> do
        keys <- fresh
        fleetReport conn keys Nothing 60000 "a" [block 2, block 1] [] `shouldReturn` True
        fleetMove conn keys Nothing Nothing `shouldReturn` MovedTo (block 2)
        fleetTip conn keys `shouldReturn` Just (block 2)
        -- a store without a view of the tip is kept out while the first is live
        fleetReport conn keys Nothing 60000 "b" [block 1] [block 1] `shouldReturn` False
        fleetReport conn keys Nothing 60000 "b" [block 2] [block 2] `shouldReturn` True
        map fst <$> fleetLive conn keys `shouldReturn` ["a", "b"]
      modifyMaxSuccess (const 300) $
        it "keeps the shared mempool as the rules say, step by step" $ \(conn, fresh) ->
          property $ \steps -> ioProperty $ do
            keys <- fresh
            foldM_ (pooled conn keys) (P.empty, Nothing, 0) (steps :: [PoolStep])
      it "turns away an offer that was checked against another tip" $ \(conn, fresh) -> do
        keys <- fresh
        fleetReport conn keys Nothing 60000 "a" [block 1] [] `shouldReturn` True
        fleetMove conn keys Nothing Nothing `shouldReturn` MovedTo (block 1)
        poolOffer conn keys 4 Nothing (block 7) [(out 0 0, True)] "seven" [] `shouldReturn` Late
        poolOffer conn keys 4 (Just (block 1)) (block 7) [(out 0 0, True)] "seven" [] `shouldReturn` Taken
        poolBody conn keys (block 7) `shouldReturn` Just "seven"
        -- the tip does not move to a block that the stores do not agree on
        fleetAdvance conn keys 4 Nothing (Just (block 1)) (block 2) nothing `shouldReturn` Raced
        fleetReport conn keys Nothing 60000 "a" [block 2, block 1] [block 1] `shouldReturn` True
        let confirms = nothing {confirmed = [block 7], spent = [out 0 0]}
        fleetAdvance conn keys 4 Nothing (Just (block 1)) (block 2) confirms `shouldReturn` MovedTo (block 2)
        poolSpends conn keys `shouldReturn` []
        poolBody conn keys (block 7) `shouldReturn` Nothing
  where
    -- carry a step out in Redis and in the model, and compare
    check conn keys (model, now) = \case
      Wait ms -> return (model, now + ms)
      Report s recent views -> do
        let name = C.pack (show s)
            allowed = mayReport now name (`elem` map block views) model
        fleetReport conn keys (Just now) lease name (map block recent) (map block views)
          `shouldReturn` allowed
        let model' = if allowed then report now lease name (map block recent) model else model
        same conn keys now model'
        return (model', now)
      Shift wrong -> do
        let seen = maybe model.tip (Just . block) wrong
            (expected, model') = case move now seen model of
              F.Moved next -> (maybe Stays MovedTo next.tip, next)
              F.Stays -> (Stays, model)
              F.Raced -> (Raced, model)
        fleetMove conn keys (Just now) seen `shouldReturn` expected
        same conn keys now model'
        return (model', now)
    nothing = PoolChange {returned = [], gone = [], confirmed = [], spent = []}
    -- carry a step of the pool out in Redis and in the model, and compare
    pooled conn keys (model, tip, n) = \case
      Give t inputs replaced -> do
        let outs = [(out a i, ok) | (a, i, ok) <- nubOrdOn (\(a, i, _) -> (a, i)) inputs]
            unspent o = lookup o outs == Just True
            (expected, model') = case P.offer maker unspent (block t) (map fst outs) (map block replaced) model of
              P.Taken next -> (Taken, next)
              P.Refused P.Known -> (Known, model)
              P.Refused (P.Conflict rivals) -> (Conflict rivals, model)
              P.Refused (P.Orphan os) -> (Orphan os, model)
        got <- poolOffer conn keys 4 tip (block t) outs "body" (map block replaced)
        sorted got `shouldBe` sorted expected
        samePool conn keys model'
        return (model', tip, n)
      Take ts -> do
        let model' = P.withdraw (map block ts) model
        left <- poolWithdraw conn keys 4 (map block ts)
        Set.fromList left `shouldBe` Map.keysSet model.spends `Set.difference` Map.keysSet model'.spends
        samePool conn keys model'
        return (model', tip, n)
      Go back dead done used -> do
        -- one store, which reports a block that no step has had
        let target = block (1000 + n)
            returned' = [(block t, [out a i | (a, i) <- nubOrd ins]) | (t, ins) <- back]
            change = P.Change returned' (map block dead) (map block done) [out a i | (a, i) <- used]
            sent = PoolChange [(t, os, "back") | (t, os) <- returned'] change.gone change.confirmed change.spent
        fleetReport conn keys (Just 0) 10 "a" [target] (maybe [] pure tip) `shouldReturn` True
        fleetAdvance conn keys 4 (Just 0) tip target sent `shouldReturn` MovedTo target
        let model' = P.advance maker change model
        samePool conn keys model'
        return (model', Just target, n + 1)
    maker = B.take 4
    sorted = \case
      Conflict rivals -> Conflict (sort rivals)
      other -> other
    samePool conn keys model = do
      let key k = keys.prefix <> k
          list :: Redis.Redis (Either Redis.Reply a) -> IO a
          list = fmap (either (error . show) id) . Redis.runRedis conn
      spends <- poolSpends conn keys
      Map.fromList spends `shouldBe` Map.map B.concat model.spends
      spender <- list (Redis.hgetall (key "pool:spender"))
      Map.fromList spender `shouldBe` model.spender
      kids <- list (Redis.keys (key "pool:kids:*"))
      Set.fromList kids `shouldBe` Set.map (key . ("pool:kids:" <>)) (Map.keysSet model.children)
      forM_ (Map.toList model.children) $ \(t, expected) -> do
        members <- list (Redis.smembers (key ("pool:kids:" <> t)))
        Set.fromList members `shouldBe` expected
    same conn keys now model = do
      fleetTip conn keys `shouldReturn` (model :: F.Fleet ByteString ByteString).tip
      leases <- fleetLive conn keys
      Map.keys (Map.filter (> now) (Map.fromList leases)) `shouldBe` Map.keys (live now model)
    lease = 10

-- | A block of the tests, or a transaction: four bytes.
block :: Word32 -> ByteString
block = encode

-- | An output of a transaction of the tests.
out :: Word32 -> Word32 -> ByteString
out t i = encode t <> encode i

-- | A step of the shared mempool, with transactions numbered from 0 to 11.
data PoolStep
  = -- | a transaction is offered, spending some outputs, each found unspent
    -- at the tip or not, and replacing some transactions
    Give !Word32 ![(Word32, Word32, Bool)] ![Word32]
  | -- | transactions are withdrawn
    Take ![Word32]
  | -- | the tip moves: some transactions come back with what they spend,
    -- some are gone, some are confirmed, and some outputs are spent
    Go ![(Word32, [(Word32, Word32)])] ![Word32] ![Word32] ![(Word32, Word32)]
  deriving (Show)

instance Arbitrary PoolStep where
  arbitrary =
    frequency
      [ (8, Give <$> tx <*> few ((,,) <$> tx <*> choose (0, 1) <*> arbitrary) <*> few tx),
        (1, Take <$> few tx),
        (3, Go <$> few ((,) <$> tx <*> few output) <*> few tx <*> few tx <*> few output)
      ]
    where
      tx = choose (0, 11)
      output = (,) <$> tx <*> choose (0, 1)
      few :: Gen a -> Gen [a]
      few one = choose (0, 3) >>= (`vectorOf` one)

-- | A Redis in a container, and a way to get the keys of a fleet that no
-- test has used.
withRedis :: ((Redis.Connection, IO FleetKeys) -> IO ()) -> IO ()
withRedis go = do
  image <- testImage
  withContainer (redisContainer image) $ \c -> do
    port <- hostPort c 6379
    let url = "redis://127.0.0.1:" <> show port
    waitRedis url
    conn <- Redis.checkedConnect (either error id (Redis.parseConnectInfo url))
    counter <- newIORef (0 :: Int)
    go (conn, (\n -> fleetKeys (C.pack ("t" <> show n <> ":")) 4) <$> atomicModifyIORef' counter (\n -> (n + 1, n)))
