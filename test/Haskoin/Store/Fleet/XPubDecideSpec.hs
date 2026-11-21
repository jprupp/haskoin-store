-- | The decisions of the cache of extended public keys
-- ("Haskoin.Store.Fleet.XPub.Decide") beside the script that wrote the
-- cache before them ("Haskoin.Store.Fleet.Oracle"), in a Redis: the same
-- steps by each, under two prefixes, and all that Redis has under each
-- compared after every step. Both have the same pool under them, kept by
-- the same decisions, so that what differs can only be the cache.
module Haskoin.Store.Fleet.XPubDecideSpec (spec) where

import Control.Monad (foldM_, forM_, void)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.Containers.ListUtils (nubOrd, nubOrdOn)
import Data.Serialize (encode)
import Data.Word (Word32)
import Database.Redis qualified as Redis
import Haskoin.Store.Fleet.Atomic.Redis (transact)
import Haskoin.Store.Fleet.DecideSpec (Dump, everything)
import Haskoin.Store.Fleet.Oracle
import Haskoin.Store.Fleet.Pool.Decide qualified as P
import Haskoin.Store.Fleet.Redis
import Haskoin.Store.Fleet.RedisSpec (address, block, out, txOf, withRedis)
import Haskoin.Store.Fleet.XPub.Decide qualified as X
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.Podman (podmanUsable)
import Test.QuickCheck

spec :: Spec
spec = do
  usable <- runIO podmanUsable
  describe "the cache of extended public keys in Redis" $ case usable of
    Left why -> it "is written in Haskell as the script wrote it" (pendingWith why)
    Right () -> aroundAll withRedis $ do
      modifyMaxSuccess (const 300) $
        it "is written in Haskell as the script wrote it, state by state" $ \(conn, fresh) ->
          property $ \steps -> ioProperty $ do
            lua <- fresh
            ours <- fresh
            foldM_ (both conn lua ours) (Nothing, Nothing, 0) (steps :: [CacheStep])

-- | Where a sequence of steps is: the fleet tip, the token of the move of
-- the cache that began last, and how many times the tip was moved.
type Walk = (Maybe ByteString, Maybe ByteString, Word32)

-- | A step of the cache, with keys numbered from 0 to 2, addresses from 0
-- to 3 and transactions from 0 to 11.
data CacheStep
  = -- | addresses are added to the entry of a key: with the numbers that
    -- the entry has or with others, at a cost, each address in one of the
    -- two chains of the key, with transactions and outputs of the chain
    Build !Word32 !Bool !Integer ![(Word32, Bool, [(Word32, Word32)], [(Word32, Word32, Word32)])]
  | -- | a move of the tip begins, under one of two tokens
    Begin !Bool
  | -- | a piece of a move, under one of two tokens: for each address the
    -- transactions of the abandoned blocks, those of the new ones, the
    -- outputs that are gone and those that are unspent
    Move !Bool ![(Word32, [Word32], [(Word32, Word32)], [(Word32, Word32)], [(Word32, Word32, Word32)])]
  | -- | a move is given up
    Abort !Bool
  | -- | the move under way went quiet long ago
    Quiet
  | Drop !Word32
  | Evict !Int
  | -- | a transaction is offered to the pool, spending some outputs
    Offer !Word32 ![(Word32, Word32)]
  | -- | the tip moves, with the move of the cache that began last or with
    -- none
    Commit !Bool
  | -- | the tip moves without the pool and the cache, which is left behind
    Shift
  | -- | the next step is by a store that saw another tip
    Astray !CacheStep
  deriving (Show)

instance Arbitrary CacheStep where
  arbitrary =
    frequency
      [ (6, Build <$> key <*> frequency [(5, pure True), (1, pure False)] <*> choose (0, 50) <*> few added),
        (2, Begin <$> arbitrary),
        (4, Move <$> arbitrary <*> few moved),
        (1, Abort <$> arbitrary),
        (1, pure Quiet),
        (1, Drop <$> key),
        (1, Evict <$> choose (0, 3)),
        (5, Offer <$> tx <*> few output),
        (2, Commit <$> arbitrary),
        (1, pure Shift),
        (1, Astray <$> oneof [Build <$> key <*> pure True <*> choose (0, 50) <*> few added, Begin <$> arbitrary]),
        (1, Astray <$> (Move <$> arbitrary <*> few moved))
      ]
    where
      key = choose (0, 2)
      tx = choose (0, 11)
      place = choose (1, 5)
      output = (,) <$> tx <*> choose (0, 1)
      added = (,,,) <$> choose (0, 3) <*> arbitrary <*> few ((,) <$> tx <*> place) <*> few row
      moved = (,,,,) <$> choose (0, 3) <*> few tx <*> few ((,) <$> tx <*> place) <*> few output <*> few row
      row = (,,) <$> tx <*> choose (0, 1) <*> place
      few :: Gen a -> Gen [a]
      few one = choose (0, 3) >>= (`vectorOf` one)

-- | Carry a step out by the script under one prefix and by the decisions
-- under another, and compare what each said and all that Redis has.
both :: Redis.Connection -> FleetKeys -> FleetKeys -> Walk -> CacheStep -> IO Walk
both conn lua ours (tip, under, n) (Astray step) = do
  -- a tip that no step moves to
  (_, under', n') <- both conn lua ours (Just "away", under, n) step
  return (tip, under', n')
both conn (lua :: FleetKeys) (ours :: FleetKeys) (tip, under, n) step = do
  let k = X.cacheKeys ours.prefix
      seen = maybe "" id tip
      token b = if b then "one" else "two"
      row (t, i, s) = (out t i, fromIntegral s, C.pack (show (t * 10 + i + 1)), "script")
      onBoth :: (FleetKeys -> IO a) -> IO ()
      onBoth f = forM_ [lua, ours] (void . f)
      go :: FleetKeys -> Redis.Redis (Either Redis.Reply a) -> IO ()
      go _ f = Redis.runRedis conn f >>= either (error . show) (const (return ()))
  walk <- case step of
    Build i right cost as -> do
      had <- luaHave conn lua (name i)
      let expected = if right then had else (fst had + 1, snd had)
          additions =
            [ X.Addition
                { address = address a,
                  path = B.singleton (if second then 1 else 0) <> encode a,
                  balance = if even a then "balance" else "",
                  txs = [(block t, fromIntegral s) | (t, s) <- nubOrdOn fst txs],
                  outs = map row (nubOrdOn (\(t, o, _) -> (t, o)) outs)
                }
            | (a, second, txs, outs) <- nubOrdOn (\(a, _, _, _) -> a) as
            ]
      expectedStep <- luaBuild conn lua seen (name i) expected cost additions
      transact conn 1 (X.build k seen (name i) expected cost additions) `shouldReturn` Just expectedStep
      return (tip, under, n)
    Begin b -> do
      began <- luaBegin conn lua (token b) seen
      transact conn 1 (X.begin k (token b) seen) `shouldReturn` Just began
      return (tip, if began == X.BeganMove then Just (token b) else under, n)
    Move b as -> do
      let pieces =
            [ X.Piece
                { address = address a,
                  balance = if odd a then "moved" else "",
                  abandoned = map block (nubOrd old),
                  added = [(block t, fromIntegral s) | (t, s) <- nubOrdOn fst new],
                  gone = [out t i | (t, i) <- nubOrd gone],
                  unspent = map row (nubOrdOn (\(t, o, _) -> (t, o)) unspent)
                }
            | (a, old, new, gone, unspent) <- nubOrdOn (\(a, _, _, _, _) -> a) as
            ]
      written <- luaPiece conn lua (token b) seen pieces
      transact conn 1 (X.piece k (token b) seen pieces) `shouldReturn` Just written
      return (tip, under, n)
    Abort b -> do
      luaAbort conn lua (token b)
      transact conn 1 (X.abort k (token b)) `shouldReturn` Just ()
      return (tip, under, n)
    -- the move under way last wrote at the start of time
    Quiet -> do
      onBoth $ \keys -> do
        Right v <- Redis.runRedis conn (Redis.get (keys.prefix <> "fx:moving"))
        forM_ v $ \moving ->
          go keys (Redis.set (keys.prefix <> "fx:moving") (C.takeWhile (/= ' ') moving <> " 0"))
      return (tip, under, n)
    Drop i -> do
      luaDrop conn lua (name i)
      transact conn 1 (X.drop k (name i)) `shouldReturn` Just ()
      return (tip, under, n)
    Evict most -> do
      evicted <- luaEvict conn lua most
      transact conn 1 (X.evict k most) `shouldReturn` Just evicted
      return (tip, under, n)
    -- the pool under both is kept by the same decisions
    Offer t inputs -> do
      let outs = [out a i | (a, i) <- nubOrd inputs, a < t]
      results <- mapM (\keys -> poolOffer conn keys 4 tip (txOf t outs) (map (const True) outs) []) [lua, ours]
      results `shouldSatisfy` \case
        [a, b] -> a == b
        _ -> False
      return (tip, under, n)
    Commit mine -> do
      let target = block (1000 + n)
          with = if mine then maybe "" id under else ""
          change = PoolChange [] [] [] [] (Just (700 * (n + 1))) []
      onBoth $ \keys -> fleetReport conn keys (Just 0) 10 "a" [target] (maybe [] pure tip)
      moved <- mapM (\keys -> fleetCommit conn keys 4 (Just 0) "a" tip target (Just change) [] with) [lua, ours]
      case moved of
        [a, b] -> do
          b `shouldBe` a
          return (if a == MovedTo target then (Just target, Nothing, n + 1) else (tip, under, n + 1))
        _ -> fail "Two fleets"
    Shift -> do
      let target = block (2000 + n)
      onBoth $ \keys -> fleetReport conn keys (Just 0) 10 "a" [target] (maybe [] pure tip)
      moved <- mapM (\keys -> fleetMove conn keys (Just 0) tip) [lua, ours]
      case moved of
        [a, b] -> do
          b `shouldBe` a
          return (if a == MovedTo target then (Just target, under, n + 1) else (tip, under, n + 1))
        _ -> fail "Two fleets"
    Astray _ -> fail "Astray twice"
  there <- everything conn lua.prefix
  here <- everything conn ours.prefix
  quietly here `shouldBe` quietly there
  return walk
  where
    name i = C.pack ("key" <> show i)

-- | All that Redis has, with the move under way by its token and not by
-- the time at which it last wrote.
quietly :: Dump -> Dump
quietly dump = [(key, if key == "fx:moving" then map (C.takeWhile (/= ' ')) v else v) | (key, v) <- dump]

-- | Not used by a spec: the pool's keys of a prefix, for whoever reads
-- this to find them.
_poolKeys :: FleetKeys -> P.Keys
_poolKeys keys = P.poolKeys keys.prefix 4
