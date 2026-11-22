-- | The decisions of the shared mempool ("Haskoin.Store.Fleet.Pool.Decide")
-- beside the script that they replace ("Haskoin.Store.Fleet.Oracle"), in
-- a Redis: the same steps by each, under two prefixes, and all that Redis
-- has under each compared after every step.
module Haskoin.Store.Fleet.DecideSpec (spec, Dump, everything) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (withAsync)
import Control.Monad (foldM_, forM, forM_, forever, void, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.Containers.ListUtils (nubOrd, nubOrdOn)
import Data.Functor.Identity (runIdentity)
import Data.List (sort)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Serialize (encode)
import Data.Set qualified as Set
import Data.Word (Word32)
import Database.Redis qualified as Redis
import Haskoin.Store.Fleet.Atomic
  ( Atomic,
    Backend (..),
    Db,
    Op (..),
    Outcome (..),
    answer,
    emptyDb,
    perform,
    runAtomic,
    runPure,
  )
import Haskoin.Store.Fleet.Atomic.Redis (transact)
import Haskoin.Store.Fleet.Oracle
import Haskoin.Store.Fleet.Pool.Decide qualified as D
import Haskoin.Store.Fleet.Redis
import Haskoin.Store.Fleet.RedisSpec (PoolStep (..), address, block, number, out, timeOf, txOf, withRedis)
import Haskoin.Store.Fleet.XPub (Begun (..), cacheBegin)
import Haskoin.Store.Fleet.XPub.Decide qualified as X
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess, prop)
import Test.Podman (podmanUsable)
import Test.QuickCheck

spec :: Spec
spec = do
  describe "a move of the tip, in the model of Redis" $ do
    modifyMaxSuccess (const 2000) $
      prop "has no answer changed by what others decide while its store has the floor" $
        \earlier during confirmed -> undisturbed True earlier during confirmed
    modifyMaxSuccess (const 2000) $
      prop "has answers changed by them when no store has the floor" $
        \earlier during confirmed -> expectFailure (undisturbed False earlier during confirmed)
  usable <- runIO podmanUsable
  describe "the shared mempool in Redis" $ case usable of
    Left why -> it "is decided in Haskell as the script decides it" (pendingWith why)
    Right () -> aroundAll withRedis $ do
      it "moves the tip over a full block while transactions are offered, stores report and keys are built" $
        \(conn, fresh) -> crowded conn =<< fresh
      modifyMaxSuccess (const 300) $
        it "is decided in Haskell as the script decides it, state by state" $ \(conn, fresh) ->
          property $ \steps -> ioProperty $ do
            lua <- fresh
            ours <- fresh
            forM_ [lua, ours] (watched conn)
            foldM_ (both conn lua ours) ([], [], Nothing, 0) (steps :: [PoolStep])

-- | A move of the tip is decided, and before it is carried out other
-- decisions are: transactions offered, withdrawn and expired, what the
-- fleet saw trimmed, entries of the cache built, dropped and evicted,
-- another store that tries to move the tip, and one that renews its
-- lease. Every answer that the move depends on must be the same after
-- them: then the script carries the move out, whatever the others did.
--
-- With the floor ('Haskoin.Store.Fleet.XPub.Decide.begin'), that holds:
-- all of them give way. Without it, it does not, which the second
-- property says, so that the first is known to see what it is for.
undisturbed :: Bool -> [PoolStep] -> [PoolStep] -> [Word32] -> Property
undisturbed held earlier during confirmed =
  case runIdentity (runAtomic (inModel begun) move) of
    Stuck why -> counterexample (show why) False
    Decided _ depends _ _ ->
      conjoin [counterexample (show q) (answer later q === a) | (q, a) <- depends]
  where
    now = 5000
    k = D.poolKeys "" 4
    kx = X.cacheKeys ""
    target = block 1000
    inModel db = Backend {fetch = return . map (answer db), time = return now}
    -- two live stores that agree on the block, cached keys at three of
    -- the four addresses, and a pool
    start =
      foldl
        (flip perform)
        emptyDb
        ( [HSet "fleet:lease" store "900000" | store <- ["a", "b"]]
            <> [HSet "fleet:recent" store target | store <- ["a", "b"]]
            <> [SAdd ("fx:of:" <> address a) i | (a, ids) <- [(0, ["x1"]), (1, ["x1", "x2"]), (2, ["x2"])], i <- ids]
        )
    pool = foldl (flip others) start earlier
    begun = if held then decided (X.begin kx "mover" "") pool else pool
    move :: (Monad m) => Atomic m Moved
    move =
      D.advance
        k
        Nothing
        target
        (Just now)
        4
        (D.Changing (PoolChange [] [] (map block confirmed) [] (Just 100) []))
        []
        (if held then "mover" else "")
    -- the other store renews its lease, and the rest decide
    later = foldl (flip others) (perform (HSet "fleet:lease" "b" "950000") begun) during
    decided :: (forall m. (Monad m) => Atomic m a) -> Db -> Db
    decided decision db = either (error . show) snd (runPure now decision db)
    others :: PoolStep -> Db -> Db
    others step = case step of
      Give t inputs replaced ->
        let outs = [(out a i, ok) | (a, i, ok) <- nubOrdOn (\(a, i, _) -> (a, i)) inputs, a < t]
         in decided (D.offer k Nothing (map snd outs) (map block replaced) (txOf t (map fst outs)))
      Take ts -> decided (D.withdraw k (map block ts))
      Go back dead done used ->
        decided (D.expire k (toInteger (timeOf (sum dead))) (length done + 1))
          . decided (D.trim k (length used))
          . decided (X.evict kx (length done))
          . decided (X.drop kx "x1")
          . decided (X.build kx "" "x3" (0, 0) 1 [added (sum dead)])
          . decided (X.begin kx "other" "")
          . decided (X.piece kx "other" "" [X.Piece (address (fromIntegral (length back))) "" [] [] [] []])
          . decided (X.abort kx "other")
    added a = X.Addition {address = address a, path = "\0\0\0\0\0", balance = "", txs = [], outs = []}

-- | A move of the tip that takes eight hundred transactions out of the
-- pool, while three others write without a pause: transactions that spend
-- from those are offered, another store renews its lease, and entries of
-- the cache are built at the addresses of the block. The move is one
-- decision that is carried out only if nothing that it read has changed,
-- so each of the three alone had it refused, every time: on a server the
-- tip stopped at the first block that confirmed a few thousand
-- transactions of the pool. The store that moves the tip has the floor
-- now, and the others give way.
crowded :: Redis.Connection -> FleetKeys -> IO ()
crowded conn keys = do
  forM_ txs $ \tx -> poolOffer conn keys 4 Nothing tx [True] [] `shouldReturn` Taken
  forM_ ["a", "b"] $ \store -> fleetReport conn keys Nothing 60000 store [target] [] `shouldReturn` True
  -- some of the addresses are those of cached keys already
  forM_ [1 .. 20] built
  withAsync (forM_ (cycle [1 .. n]) (\c -> offered c >> threadDelay 5000)) $ \_ ->
    withAsync (forever (fleetReport conn keys Nothing 60000 "b" [target] [] >> threadDelay 5000)) $ \_ ->
      withAsync (forM_ (cycle [21 .. n]) (\c -> built c >> threadDelay 5000)) $ \_ -> do
        threadDelay 300000
        cacheBegin conn keys "token" "" `shouldReturn` Begun
        fleetCommit conn keys 4 Nothing "a" Nothing target (Just change) [] "token" `shouldReturn` MovedTo target
  fleetTip conn keys `shouldReturn` Just target
  left <- map fst <$> poolSpends conn keys
  filter (`elem` map (.txid) txs) left `shouldBe` []
  where
    n = 800 :: Word32
    target = block 77
    txs = map one [1 .. n]
    change = PoolChange [] [] (map (.txid) txs) [] (Just 100) []
    addr c = C.pack ("addr" <> show c)
    one c =
      PoolTx
        { txid = encode c,
          time = 1700000000 + fromIntegral c,
          inputs = [encode (1000000 + c) <> encode (0 :: Word32)],
          body = B.replicate 250 7,
          signals = False,
          entries =
            [ PoolTouches (addr c),
              PoolPays (addr c) (encode c <> encode (0 :: Word32)) 1000 "script",
              PoolTouches (addr (c + 1)),
              PoolSpends (addr (c + 1)) (encode (1000000 + c) <> encode (0 :: Word32)) 1000
            ]
        }
    -- a transaction that spends the output of one of the block's, at its
    -- address: taken, late or an orphan, as the move goes
    offered c = void (poolOffer conn keys 4 Nothing (child c) [False] [])
    child c =
      PoolTx
        { txid = encode (5000000 + c),
          time = 1700009000,
          inputs = [encode c <> encode (0 :: Word32)],
          body = B.replicate 250 7,
          signals = False,
          entries = [PoolTouches (addr c), PoolSpends (addr c) (encode c <> encode (0 :: Word32)) 1000]
        }
    -- a cached key with one address of the block
    built c = void (transact conn 1 (building c))
    building :: (Monad m) => Word32 -> Atomic m X.Step
    building c =
      X.build
        (X.cacheKeys keys.prefix)
        ""
        (C.pack ("key" <> show c))
        (0, 0)
        1
        [X.Addition {address = addr c, path = "\0\0\0\0\0", balance = "", txs = [], outs = []}]

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
