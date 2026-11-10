module Haskoin.Store.Fleet.RedisSpec (spec) where

import Control.Monad (foldM_)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as C
import Data.IORef (atomicModifyIORef', newIORef)
import Data.Map.Strict qualified as Map
import Data.Serialize (encode)
import Data.Word (Word32)
import Database.Redis qualified as Redis
import Haskoin.Store.Fleet (Time, empty, live, mayReport, move, report)
import Haskoin.Store.Fleet qualified as F
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
    same conn keys now model = do
      fleetTip conn keys `shouldReturn` (model :: F.Fleet ByteString ByteString).tip
      leases <- fleetLive conn keys
      Map.keys (Map.filter (> now) (Map.fromList leases)) `shouldBe` Map.keys (live now model)
    lease = 10

-- | A block of the tests: four bytes.
block :: Word32 -> ByteString
block = encode

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
