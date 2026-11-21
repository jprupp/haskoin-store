-- | The decisions of the shared mempool ("Haskoin.Store.Fleet.Pool.Decide")
-- beside the script that they replace ("Haskoin.Store.Fleet.Oracle"), in
-- a Redis: the same steps by each, under two prefixes, and all that Redis
-- has under each compared after every step.
module Haskoin.Store.Fleet.DecideSpec (spec, Dump, everything) where

import Control.Monad (foldM_, forM, forM_, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.Containers.ListUtils (nubOrd, nubOrdOn)
import Data.List (sort)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Serialize (encode)
import Data.Set qualified as Set
import Data.Word (Word32)
import Database.Redis qualified as Redis
import Haskoin.Store.Fleet.Atomic.Redis (transact)
import Haskoin.Store.Fleet.Oracle
import Haskoin.Store.Fleet.Pool.Decide qualified as D
import Haskoin.Store.Fleet.Redis
import Haskoin.Store.Fleet.RedisSpec (PoolStep (..), address, block, number, out, timeOf, txOf, withRedis)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.Podman (podmanUsable)
import Test.QuickCheck

spec :: Spec
spec = do
  usable <- runIO podmanUsable
  describe "the shared mempool in Redis" $ case usable of
    Left why -> it "is decided in Haskell as the script decides it" (pendingWith why)
    Right () -> aroundAll withRedis $ do
      modifyMaxSuccess (const 300) $
        it "is decided in Haskell as the script decides it, state by state" $ \(conn, fresh) ->
          property $ \steps -> ioProperty $ do
            lua <- fresh
            ours <- fresh
            forM_ [lua, ours] (watched conn)
            foldM_ (both conn lua ours) ([], [], Nothing, 0) (steps :: [PoolStep])

-- | All that Redis has under a prefix: each key with what it holds.
type Dump = [(ByteString, [ByteString])]

-- | Where a sequence of steps is: what Redis had under each prefix after
-- the step before, the tip, and how many times it moved or tried to.
type Walk = (Dump, Dump, Maybe ByteString, Word32)

sorted :: Offered -> Offered
sorted = \case
  Conflict rivals -> Conflict (sort rivals)
  other -> other

-- cached keys that have three of the four addresses, one with a
-- transaction and an output that the chain's layer has already
watched :: Redis.Connection -> FleetKeys -> IO ()
watched conn (keys :: FleetKeys) = do
  let key k = keys.prefix <> k
      go :: Redis.Redis (Either Redis.Reply a) -> IO ()
      go f = Redis.runRedis conn f >>= either (error . show) (const (return ()))
  forM_ [(0, ["x1"]), (1, ["x1", "x2"]), (2, ["x2"])] $ \(a, ids) ->
    go (Redis.sadd (key ("fx:of:" <> address a)) (NonEmpty.fromList ids))
  go (Redis.zadd (key "fx:e:x1:txs") [(5, block 3)])
  go (Redis.zadd (key "fx:e:x1:outs") [(5, out 3 0)])

-- carry a step out by the script under one prefix and by the decision
-- in Haskell under another, and compare what each said and all that
-- Redis has under each
both :: Redis.Connection -> FleetKeys -> FleetKeys -> Walk -> PoolStep -> IO Walk
both conn (lua :: FleetKeys) (ours :: FleetKeys) (there0, here0, tip, n) step = do
  let k = D.poolKeys ours.prefix 4
      go :: Redis.Redis (Either Redis.Reply a) -> IO ()
      go f = Redis.runRedis conn f >>= either (error . show) (const (return ()))
      onBoth f = forM_ [lua, ours] (\(keys :: FleetKeys) -> f (keys.prefix <>))
  (tip', n') <- case step of
    Give t inputs replaced -> do
      let outs = [(out a i, ok) | (a, i, ok) <- nubOrdOn (\(a, i, _) -> (a, i)) inputs, a < t]
          tx = txOf t (map fst outs)
      expected <- luaOffer conn lua 4 tip tx (map snd outs) (map block replaced)
      got <- transact conn 1 (D.offer k tip (map snd outs) (map block replaced) tx)
      fmap sorted got `shouldBe` Just (sorted expected)
      return (tip, n)
    Take ts -> do
      expected <- luaWithdraw conn lua 4 (map block ts)
      got <- transact conn 1 (D.withdraw k (map block ts))
      fmap Set.fromList got `shouldBe` Just (Set.fromList expected)
      return (tip, n)
    -- the tip moves, as in the steps of the model above, in each of
    -- the three ways and with and without a move of the cache under
    -- way; then what has been in the pool since before a time leaves,
    -- and the fleet forgets what it saw first
    Go back dead done used -> do
      let target = block (1000 + n)
          height = 700 * (n + 1)
          returned' = [(block t, [out a i | (a, i) <- nubOrd ins, a < t]) | (t, ins) <- back]
          labelled = [(out a i, block ((a * 7 + i * 3 + n) `mod` 12) <> encode height) | (a, i) <- used]
          sent =
            PoolChange
              [txOf (number t) os | (t, os) <- returned']
              (map block dead)
              (map block done)
              [o <> w | (o, w) <- labelled]
              (Just height)
              [block t <> block w <> encode height | t <- take 1 dead, let w = (t * 5 + n) `mod` 12]
          token = if n `mod` 6 `elem` [2, 4] then "tok" else ""
      -- entries of the cache that a move marked, one of them gone
      when (n `mod` 4 == 1) . onBoth $ \key -> go (Redis.sadd (key "fx:marked") (NonEmpty.fromList ["x1", "x3"]))
      -- a move of the cache under way: that of the store, or none
      when (n `mod` 6 == 2) . onBoth $ \key -> go (Redis.set (key "fx:moving") "tok 1")
      forM_ [lua, ours] $ \keys ->
        fleetReport conn keys (Just 0) 10 "a" [target] (maybe [] pure tip) `shouldReturn` True
      (moved, decided) <-
        if n `mod` 5 == 3
          then
            (,)
              <$> luaCommit conn lua 4 (Just 0) "a" tip target Nothing [] ""
              <*> transact conn 1 (D.advance k tip target (Just 0) 4 (D.AloneNotKnowing "a") [] "")
          else
            if n `mod` 7 == 5
              then
                (,)
                  <$> luaRestart conn lua 4 (Just 0) tip target
                  <*> transact conn 1 (D.advance k tip target (Just 0) 4 D.TooLong [] "")
              else
                (,)
                  <$> luaCommit conn lua 4 (Just 0) "a" tip target (Just sent) [block n, target] token
                  <*> transact conn 1 (D.advance k tip target (Just 0) 4 (D.Changing sent) [block n, target] token)
      decided `shouldBe` Just moved
      let cutoff = toInteger (timeOf (sum dead))
          most = length done + 1
      expected <- luaExpire conn lua 4 cutoff most
      got <- transact conn 1 (D.expire k cutoff most)
      fmap Set.fromList got `shouldBe` Just (Set.fromList expected)
      kept <- luaTrim conn lua (length used)
      transact conn 1 (D.trim k (length used)) `shouldReturn` Just kept
      return (if moved == MovedTo target then Just target else tip, n + 1)
  there <- everything conn lua.prefix
  here <- everything conn ours.prefix
  versions here0 here `shouldBe` versions there0 there
  return (there, here, tip', n')

-- the version of an entry of the cache as whether it is new since the
-- step before. Every change to an address gives the entries that have
-- it a new version, the next number: when transactions that spend from
-- the same one leave together, in no order, which entry ends with the
-- highest number is no rule, and that each has a new one is
versions :: Dump -> Dump -> Dump
versions earlier now =
  [ if "fx:e:" `B.isPrefixOf` key && ":meta" `B.isSuffixOf` key
      then (key, [if lookup key earlier == Just v then "the version it had" else "a new version"])
      else (key, v)
  | (key, v) <- now
  ]

-- all that Redis has under a prefix. Of what the pool keeps of a
-- transaction to take it out again, which each writes in its own way,
-- only for which transactions; of what the fleet saw, not when; of the
-- events, not their numbers nor their order
everything :: Redis.Connection -> ByteString -> IO Dump
everything conn p = do
  let list :: Redis.Redis (Either Redis.Reply a) -> IO a
      list = fmap (either (error . show) id) . Redis.runRedis conn
      name = B.drop (B.length p)
  ks <- sort <$> list (Redis.keys (p <> "*"))
  forM ks $ \key ->
    (name key,) <$> do
      kind <- list (Redis.getType key)
      case kind of
        Redis.String -> maybe [] pure <$> list (Redis.get key)
        Redis.Hash
          | name key == "pool:entries" -> sort . map fst <$> list (Redis.hgetall key)
          | otherwise -> sort . map (\(f, v) -> f <> " = " <> v) <$> list (Redis.hgetall key)
        Redis.Set -> sort <$> list (Redis.smembers key)
        Redis.ZSet
          | name key == "fleet:seen" -> sort . map fst <$> list (Redis.zrangeWithscores key 0 (-1))
          | otherwise -> map (\(x, sc) -> x <> " @ " <> C.pack (show sc)) <$> list (Redis.zrangeWithscores key 0 (-1))
        -- transactions that spend from the same one leave in the
        -- order in which Redis lists a set, which is no order
        Redis.Stream ->
          sort . map (B.intercalate " " . concatMap (\(f, v) -> [f, v]) . (.keyValues))
            <$> list (Redis.xrange key "-" "+" Nothing)
        other -> return [C.pack (show other)]
