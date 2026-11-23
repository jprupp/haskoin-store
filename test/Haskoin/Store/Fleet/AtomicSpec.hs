module Haskoin.Store.Fleet.AtomicSpec (spec) where

import Control.Concurrent.Async (forConcurrently_)
import Control.Exception (throwIO, try)
import Control.Monad (forM_, replicateM_, void)
import Control.Monad.State.Strict (runState, state)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as C
import Data.Functor.Identity (runIdentity)
import Data.IORef (atomicModifyIORef', newIORef)
import Data.Maybe (isJust)
import Database.Redis qualified as Redis
import Haskoin.Store.Fleet.Atomic
import Haskoin.Store.Fleet.Atomic.Redis
import Haskoin.Store.Integration.Valkey (redisContainer, redisURL)
import Haskoin.Store.Startup (redisConnectInfo)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess, prop)
import Test.Podman (podmanUsable, withContainer)
import Test.QuickCheck

spec :: Spec
spec = do
  describe "a decision, in the model of Redis" $ do
    modifyMaxSuccess (const 5000) $
      prop "sees what it wrote as Redis would say it, or says that it cannot tell" $
        forAll writesAndQuestion $ \(earlier, during, q) ->
          let db = foldl (flip perform) emptyDb (map (under "") earlier)
              ops = map (under "") during
              later = foldl (flip perform) db ops
              told' = runPure 0 (mapM_ write ops >> query (questionUnder "" q)) db
           in classify (either (const True) (const False) told') "cannot tell" $ case told' of
                Left _ -> property True
                Right (a, db') -> (a, db') === (answer later (questionUnder "" q), later)
    modifyMaxSuccess (const 5000) $
      prop "leaves Redis the same without the writes that a later one undoes" $
        forAll ((,) <$> listOf anOp <*> listOf anOp) $ \(earlier, during) ->
          let db = foldl (flip perform) emptyDb earlier
           in foldl (flip perform) db (tidy during) === foldl (flip perform) db during
    it "tells what it cannot tell, and no more than that" $ do
      let z = "z1"
          full = perform (HSet "h1" "a" "1") emptyDb
          cannot ops q = either (const True) (const False) (runPure 0 (mapM_ write ops >> query q) emptyDb)
      -- a member that may have been there
      cannot [ZAdd z 1 "a"] (ZCard z) `shouldBe` True
      cannot [ZRemRangeByRank z 0 0] (ZScore z "a") `shouldBe` True
      cannot [ZAdd z 1 "a"] (ZRangeByScore z MinusInf PlusInf (Just 1)) `shouldBe` True
      -- a key that was there, after a field was taken out of it
      either (const True) (const False) (runPure 0 (write (HDel "h1" "a") >> query (Exists "h1")) full)
        `shouldBe` True
      cannot [HDel "h1" "a"] (Exists "h1") `shouldBe` False
      -- and what it can
      cannot [ZAdd z 1 "a"] (ZRangeByScore z MinusInf PlusInf Nothing) `shouldBe` False
      cannot [ZAdd z 1 "a", ZRem z "a"] (ZScore z "a") `shouldBe` False
      cannot [HDel "h1" "a", HSet "h1" "b" "1"] (Exists "h1") `shouldBe` False
      cannot [Wipe "h", HIncrBy "h1" "c1" 2] (HGet "h1" "c1") `shouldBe` False
    it "writes to a key that a mark guards only while Redis has no mark, and depends on that" $ do
      let mark = "m1"
          marked = perform (HSet mark "t" "") emptyDb
          writing :: (Monad m) => Atomic m ()
          writing = guarding mark >> write (HSet "h1" "f" "v") >> write (HSet "h1" "g" "w")
          after = perform (HSet "h1" "g" "w") . perform (HSet "h1" "f" "v")
          inModel db = Backend {fetch = return . map (answer db), time = return 0}
      -- no mark: written, and the decision depends on there being none
      runPure 0 writing emptyDb `shouldBe` Right ((), after emptyDb)
      case runIdentity (runAtomic (inModel emptyDb) writing) of
        Decided () depends _ _ -> depends `shouldBe` [(Exists mark, Flag False)]
        Stuck why -> expectationFailure (show why)
      -- a mark: the decision is stopped, with nothing written
      either heldByMark (const False) (runPure 0 writing marked) `shouldBe` True
      -- a decision that says that it writes among marks does
      runPure 0 (unguarded >> writing) marked `shouldBe` Right ((), after marked)
      -- and the decision that wrote the mark itself goes on writing
      runPure 0 (write (HSet mark "t" "") >> writing) emptyDb `shouldBe` Right ((), after marked)
    it "depends on what it asked, and not on what it only had fetched" $ do
      let decision :: (Monad m) => Atomic m ()
          decision = prefetch [Get "s1", Get "n1"] >> void (get "s1")
      case runPure 0 decision emptyDb of
        Right ((), db) -> db `shouldBe` emptyDb
        Left why -> expectationFailure (show why)
    -- Redis is asked three times, and its clock is ten seconds later
    -- each time that it is read
    it "has one clock, however many times Redis is asked" $ do
      let decision :: (Monad m) => Atomic m (Integer, Integer)
          decision = do
            first <- clock
            a <- get "s1"
            b <- get (maybe "s2" (const "s3") a)
            _ <- get (maybe "n1" (const "n2") b)
            second <- clock
            return (first, second)
          ticking =
            Backend
              { fetch = return . map (answer (perform (Set "s1" "x") (perform (Set "s3" "y") emptyDb))),
                time = state (\t -> (t, t + 10000))
              }
      case runState (runAtomic ticking decision) 1000 of
        (Decided times _ _ rounds, _) -> (times, rounds) `shouldBe` ((1000, 1000), 3)
        (Stuck why, _) -> expectationFailure (show why)
    -- three keys, each holding the name of the next
    it "names questions ahead in steps, is gone through once, and depends on none of them" $ do
      let db = foldr perform emptyDb [Set "s1" "s2", Set "s2" "s3", Set "s3" "x"]
          naming next = \case
            Text (Just k) -> next k
            _ -> []
          steps = [Ahead (Get "s1") (naming (\a -> [Ahead (Get a) (naming (\b -> [Ahead (Get b) (const [])]))]))]
          decision :: (Monad m) => Atomic m (Maybe ByteString, Int)
          decision = do
            ready <- ahead steps
            -- a pass that is left, and one that is gone through
            if ready then (,1) <$> get "s3" else return (Nothing, 0)
          model = Backend {fetch = return . map (answer db), time = return 0}
      case runIdentity (runAtomic model decision) of
        Decided result depends _ rounds ->
          (result, map fst depends, rounds) `shouldBe` ((Just "x", 1), [Get "s3"], 3)
        Stuck why -> expectationFailure (show why)
  usable <- runIO podmanUsable
  describe "a decision, in Redis" $ case usable of
    Left why -> it "is carried out in one step" (pendingWith why)
    Right () -> aroundAll withRedis $ do
      -- Redis closes the connection of a command of more than 1,048,576
      -- arguments, which a decision reached as an argument for each word:
      -- the move of the tip over one block had 1,008,352.
      it "is carried out with more words than Redis takes arguments" $ \(conn, fresh) -> do
        p <- fresh
        let n = 280000 :: Int
            field i = C.pack (show (i :: Int))
            key i = p <> "h" <> field (i `mod` 7)
            large :: (Monad m) => Atomic m ()
            large = do
              was <- mapM (\i -> hget (key i) (field i)) [1 .. 2000]
              forM_ [1 .. n] $ \i -> write (HSet (key i) (field i) "v")
              write (Set (p <> "s1") (C.pack (show (length (filter isJust was)))))
        transact conn 1 large `shouldReturn` Just ()
        Right sizes <- Redis.runRedis conn (mapM (\i -> Redis.hlen (key i)) [0 .. 6] >>= return . sequence)
        sum sizes `shouldBe` toInteger n
        -- and again, with what it wrote to ask about
        transact conn 1 large `shouldReturn` Just ()
        Right said <- Redis.runRedis conn (Redis.get (p <> "s1"))
        said `shouldBe` Just "2000"
      modifyMaxSuccess (const 300) $
        it "finds Redis to say what the model says, after any writes" $ \(conn, fresh) ->
          property . forAll (listOf anOp) $ \during -> ioProperty $ do
            p <- fresh
            let ops = map (under p) during
                db = foldl (flip perform) emptyDb ops
                qs = map (questionUnder p) everyQuestion
            transact conn 1 (mapM_ write ops) `shouldReturn` Just ()
            Just answers <- transact conn 1 (queries qs)
            return (conjoin [counterexample (show q) (a === answer db q) | (q, a) <- zip qs answers])
      modifyMaxSuccess (const 300) $
        it "sees what it wrote as Redis says it afterwards" $ \(conn, fresh) ->
          property . forAll writesAndQuestion $ \(earlier, during, q) -> ioProperty $ do
            p <- fresh
            let (old, new, asked') = (map (under p) earlier, map (under p) during, questionUnder p q)
            transact conn 1 (mapM_ write old) `shouldReturn` Just ()
            planned <- (Just <$> plan conn (mapM_ write new >> query asked')) `orStuck` Nothing
            case planned of
              Nothing -> return (property True)
              Just told' -> do
                commit conn told' `shouldReturn` True
                Just [now] <- transact conn 1 (queries [asked'])
                return (told'.result === now)
      it "does not write to a key that a mark guards when the mark came after it was decided" $ \(conn, fresh) -> do
        p <- fresh
        let (mark, key) = (p <> "m1", p <> "h1")
            writing :: (Monad m) => Atomic m ()
            writing = guarding mark >> write (HSet key "f" "v")
        planned <- plan conn writing
        transact conn 1 (write (HSet mark "t" "")) `shouldReturn` Just ()
        carry conn planned `shouldReturn` Left (Exists mark)
        transact conn 1 (query (HGet key "f")) `shouldReturn` Just (Text Nothing)
        -- decided with the mark there, it is stopped before it writes
        stopped <- try (plan conn writing) :: IO (Either Failure (Planned ()))
        either (const True) (const False) stopped `shouldBe` True
        transact conn 1 (query (HGet key "f")) `shouldReturn` Just (Text Nothing)
      it "changes nothing when an answer that it depends on is no longer true" $ \(conn, fresh) -> do
        p <- fresh
        let (k, other, out) = (p <> "s1", p <> "s2", p <> "h1")
            decision :: (Monad m) => Atomic m (Maybe ByteString)
            decision = do
              v <- get k
              _ <- get other
              write (HSet out "a" "1")
              write (HSet out "seen" (maybe "nothing" id v))
              return v
        transact conn 1 (write (Set k "one")) `shouldReturn` Just ()
        stale <- plan conn decision
        stale.result `shouldBe` Just "one"
        -- another store changes what it read, before it is carried out
        transact conn 1 (write (Set k "two")) `shouldReturn` Just ()
        commit conn stale `shouldReturn` False
        transact conn 1 (hgetall out) `shouldReturn` Just mempty
        -- made again, it is carried out
        transact conn 3 decision `shouldReturn` Just (Just "two")
        transact conn 1 (hget out "seen") `shouldReturn` Just (Just "two")
        -- and what it did not ask about may change
        fine <- plan conn decision
        transact conn 1 (write (Set (p <> "s3") "x")) `shouldReturn` Just ()
        commit conn fine `shouldReturn` True
      it "notices a set or a hash that changed, in whatever order Redis lists it" $ \(conn, fresh) -> do
        p <- fresh
        let (g, h) = (p <> "g1", p <> "h1")
            members = [C.pack (show n) | n <- [1 .. 200 :: Int]]
            decision :: (Monad m) => Atomic m Int
            decision = do
              s <- smembers g
              m <- hgetall h
              write (Set (p <> "s1") "done")
              return (length s + length m)
        transact conn 1 (forM_ members (\x -> write (SAdd g x) >> write (HSet h x x))) `shouldReturn` Just ()
        same <- plan conn decision
        same.result `shouldBe` 400
        commit conn same `shouldReturn` True
        changed <- plan conn decision
        transact conn 1 (write (HSet h "7" "other")) `shouldReturn` Just ()
        commit conn changed `shouldReturn` False
        gone <- plan conn decision
        transact conn 1 (write (SRem g "200")) `shouldReturn` Just ()
        commit conn gone `shouldReturn` False
      it "loses no change when many decide at once" $ \(conn, fresh) -> do
        p <- fresh
        let k = p <> "n1"
            add :: (Monad m) => Atomic m ()
            add = do
              v <- get k
              write (Set k (C.pack (show (maybe (0 :: Integer) (maybe 0 fst . C.readInteger) v + 1))))
        forConcurrently_ [1 .. 8 :: Int] $ \_ ->
          replicateM_ 50 (transact conn 10000 add >>= (`shouldSatisfy` isJust))
        transact conn 1 (get k) `shouldReturn` Just (Just "400")
      it "reads the clock of Redis, in milliseconds" $ \(conn, _) -> do
        Just t <- transact conn 1 clock
        t `shouldSatisfy` (> 1700000000000)
  where
    -- a decision that asks what cannot be told is no failure of the test
    orStuck f x =
      try f >>= \case
        Right a -> return a
        Left (Faulty _) -> return x
        Left e -> throwIO e

-- | Writes before a decision, the writes of the decision, and a question:
-- all about a few keys, each of one kind.
writesAndQuestion :: Gen ([Op], [Op], Query)
writesAndQuestion = (,,) <$> listOf anOp <*> listOf anOp <*> elements everyQuestion

-- | A write to one of a few keys, each of the kind that the write is for:
-- a key is of the kind that the first letter of its name says.
anOp :: Gen Op
anOp =
  oneof
    [ Set "s1" <$> word,
      return (Del "s1"),
      IncrBy "n1" <$> choose (-3, 3),
      return (Del "n1"),
      HSet <$> hash <*> field <*> word,
      HDel <$> hash <*> field,
      HIncrBy <$> hash <*> return "c1" <*> choose (-3, 3),
      HDel <$> hash <*> return "c1",
      Del <$> hash,
      SAdd "g1" <$> word,
      SRem "g1" <$> word,
      return (Del "g1"),
      ZAdd <$> sorted <*> aScore <*> word,
      ZAddNX <$> sorted <*> aScore <*> word,
      ZRem <$> sorted <*> word,
      ZRemRangeByRank <$> sorted <*> choose (-3, 3) <*> choose (-3, 3),
      Del <$> sorted,
      XAdd "x1" 5 . pure . ("k",) <$> word,
      Wipe <$> elements ["h", "z", "s", "g1"]
    ]
  where
    hash = elements ["h1", "h2"]
    sorted = elements ["z1", "z2"]
    field = elements ["a", "b"]

word :: Gen ByteString
word = elements ["a", "b", "c", "", "\0\255", "a b"]

aScore :: Gen Score
aScore = fromIntegral <$> elements [-2, -1, 0, 1, 2, 1700000000000 :: Integer]

-- | Every question about the keys of 'anOp'.
everyQuestion :: [Query]
everyQuestion =
  [Get "s1", Get "n1", Exists "s1", Exists "x1", Exists "nothing"]
    <> concat
      [ [HGetAll h, HKeys h, HAbove h 0, HAbove h (-2), Exists h, HGet h "c1", HExists h "c1"]
          <> concat [[HGet h f, HExists h f] | f <- ["a", "b"]]
      | h <- ["h1", "h2"]
      ]
    <> [SMembers "g1", Exists "g1"]
    <> [SIsMember "g1" w | w <- words']
    <> concat
      [ [ZCard z, Exists z, ZRangeByScore z MinusInf PlusInf Nothing, ZRangeByScore z (At (-1)) (At 1) Nothing]
          <> [ZRangeByScore z MinusInf (At 1) (Just 2), ZRangeByScore z (At 2) PlusInf Nothing]
          <> [ZScore z w | w <- words']
      | z <- ["z1", "z2"]
      ]
  where
    words' = ["a", "b", "c", "", "\0\255", "a b"]

-- | A write to the same key under a prefix.
under :: ByteString -> Op -> Op
under p = \case
  Set k v -> Set (p <> k) v
  Del k -> Del (p <> k)
  IncrBy k n -> IncrBy (p <> k) n
  HSet k f v -> HSet (p <> k) f v
  HDel k f -> HDel (p <> k) f
  HIncrBy k f n -> HIncrBy (p <> k) f n
  SAdd k x -> SAdd (p <> k) x
  SRem k x -> SRem (p <> k) x
  ZAdd k s x -> ZAdd (p <> k) s x
  ZAddNX k s x -> ZAddNX (p <> k) s x
  ZRem k x -> ZRem (p <> k) x
  ZRemRangeByRank k a b -> ZRemRangeByRank (p <> k) a b
  XAdd k n fs -> XAdd (p <> k) n fs
  Wipe q -> Wipe (p <> q)

-- | A question about the same key under a prefix.
questionUnder :: ByteString -> Query -> Query
questionUnder p = \case
  Get k -> Get (p <> k)
  HGet k f -> HGet (p <> k) f
  HExists k f -> HExists (p <> k) f
  HGetAll k -> HGetAll (p <> k)
  HKeys k -> HKeys (p <> k)
  HAbove k n -> HAbove (p <> k) n
  SIsMember k x -> SIsMember (p <> k) x
  SMembers k -> SMembers (p <> k)
  ZScore k x -> ZScore (p <> k) x
  ZCard k -> ZCard (p <> k)
  ZRangeByScore k lo hi n -> ZRangeByScore (p <> k) lo hi n
  Exists k -> Exists (p <> k)

-- | A Valkey, and prefixes that no test has used.
withRedis :: ((Redis.Connection, IO ByteString) -> IO ()) -> IO ()
withRedis go =
  withContainer redisContainer $ \p -> do
    conn <- Redis.checkedConnect (either error id (redisConnectInfo (redisURL p)))
    counter <- newIORef (0 :: Int)
    go (conn, (\n -> C.pack ("t" <> show n <> ":")) <$> atomicModifyIORef' counter (\n -> (n + 1, n)))
