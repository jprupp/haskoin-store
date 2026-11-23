-- | The decisions of the shared mempool ("Haskoin.Store.Fleet.Pool.Decide")
-- beside the script that they replace ("Haskoin.Store.Fleet.Oracle"), in
-- a Redis: the same steps by each, under two prefixes, and all that Redis
-- has under each compared after every step.
module Haskoin.Store.Fleet.DecideSpec (spec, Dump, everything) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (withAsync)
import Control.Monad (foldM_, forM, forM_, forever, unless, void, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.Containers.ListUtils (nubOrd, nubOrdOn)
import Data.Either (isLeft)
import Data.Foldable (toList)
import Data.Functor.Identity (runIdentity)
import Data.List (sort)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Map.Strict qualified as Map
import Data.Sequence qualified as Seq
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
    Value (..),
    answer,
    emptyDb,
    perform,
    runAtomic,
    runPure,
  )
import Haskoin.Store.Fleet.Atomic.Redis (Planned (..), carry, plan, transact)
import Haskoin.Store.Fleet.Oracle
import Haskoin.Store.Fleet.Pool.Decide qualified as D
import Haskoin.Store.Fleet.Redis
import Haskoin.Store.Fleet.RedisSpec (PoolStep (..), address, block, number, out, timeOf, txOf, withRedis)
import Haskoin.Store.Fleet.XPub (Begun (..), cacheBeat, cacheBegin)
import Haskoin.Store.Fleet.XPub.Decide qualified as X
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess, prop)
import Test.Podman (podmanUsable)
import Test.QuickCheck

spec :: Spec
spec = do
  describe "a move of the tip, in the model of Redis" $ do
    modifyMaxSuccess (const 2000) $
      prop "has no answer changed by what others decide, for as long as its store says that it is at it" $
        \earlier during confirmed -> undisturbed Kept earlier during confirmed
    modifyMaxSuccess (const 2000) $
      prop "has answers changed by them ten seconds after its store last said so" $
        \earlier during confirmed -> expectFailure (undisturbed Silent earlier during confirmed)
    modifyMaxSuccess (const 2000) $
      prop "has answers changed by them when no store has the floor" $
        \earlier during confirmed -> expectFailure (undisturbed Free earlier during confirmed)
  -- The move that only marks what leaves the pool, against the one that
  -- takes it out in its own step: in the model, where thousands of
  -- histories take a minute, and with the others trying to write between
  -- two takings out. A group of its own, and so a process of its own. It
  -- passed ten thousand histories when it was written, and found in the
  -- first thousand what three hundred against Redis had not: that a
  -- count can end as a zero or as nothing ('marking').
  describe "the pool after a move of the tip, in the model of Redis" $
    modifyMaxSuccess (const 3000) $
      prop "lets go of what was marked, and ends as after the move that took it out, whatever others try" $
        \earlier during confirmed used order -> marking earlier during confirmed used order
  usable <- runIO podmanUsable
  -- a group of its own, and so a process of its own: it takes its time
  describe "the floor, in Redis" $ case usable of
    Left why -> it "is the store's for as long as it says that it is at its move" (pendingWith why)
    Right () -> aroundAll withRedis $ do
      it "is the store's for as long as it says that it is at its move: a move is carried out after twelve seconds" $
        \(conn, fresh) -> longMove True conn =<< fresh
      it "is the store's for ten seconds if it says nothing: a move is refused after twelve" $
        \(conn, fresh) -> longMove False conn =<< fresh
  describe "the shared mempool in Redis" $ case usable of
    Left why -> it "is decided in Haskell as the script decides it" (pendingWith why)
    Right () -> aroundAll withRedis $ do
      it "moves the tip over a full block while transactions are offered, stores report and keys are built" $
        \(conn, fresh) -> crowded conn =<< fresh
      -- The move names what it will ask before it is gone through
      -- ('Haskoin.Store.Fleet.Pool.Decide.foreseen'), in steps: the tip;
      -- what is asked of each transaction; what is asked of its addresses
      -- and which its children are; what is asked of the children and of
      -- the cached keys at its addresses; and what the children spend
      -- from. Then it is gone through once. A question of the move that
      -- is not named there makes these numbers one more, and the move of
      -- a full block a pass longer, which is seconds on a server.
      it "asks Redis no more times about a large block than about a small one" $ \(conn, fresh) -> do
        let asked related n = do
              keys <- fresh
              let txs = map blockTx [1 .. n]
                  -- transactions of the pool, each with a child, that
                  -- spend what a transaction of the block spends, which
                  -- the pool never had
                  rivals = [blockTx c | related, c <- [9001 .. 9010]]
                  winners = [(encode (8000000 + i :: Word32), o) | (i, r) <- zip [1 ..] rivals, o <- r.inputs]
                  change =
                    PoolChange
                      { returned = [],
                        gone = [],
                        confirmed = map (.txid) txs <> map fst winners,
                        spent =
                          [o <> tx.txid <> encode (100 :: Word32) | tx <- txs, o <- tx.inputs]
                            <> [o <> t <> encode (100 :: Word32) | (t, o) <- winners],
                        height = Just 100,
                        lost = []
                      }
                  move :: (Monad m) => Atomic m Moved
                  move =
                    D.advance
                      (D.poolKeys keys.prefix 4)
                      Nothing
                      (block 77)
                      Nothing
                      keys.size
                      (D.Changing change)
                      []
                      "token"
                  cached :: (Monad m) => Atomic m X.Step
                  cached =
                    X.build
                      (X.cacheKeys keys.prefix)
                      ""
                      "key1"
                      (0, 0)
                      1
                      [X.Addition {address = "addr3", path = "\0\0\0\0\0", balance = "", txs = [], outs = []}]
              forM_ txs $ \tx -> poolOffer conn keys 4 Nothing tx [True] [] `shouldReturn` Taken
              when related $ do
                -- one in four has a child that stays, a cached key has
                -- one of the addresses, and ten transactions with their
                -- children spend what the block spends
                forM_ [c | c <- [1 .. n], c `mod` 4 == 0] $ \c ->
                  poolOffer conn keys 4 Nothing (childTx c) [False] [] `shouldReturn` Taken
                forM_ rivals $ \r -> do
                  poolOffer conn keys 4 Nothing r [True] [] `shouldReturn` Taken
                  poolOffer conn keys 4 Nothing (childTx (number r.txid)) [False] [] `shouldReturn` Taken
                void (transact conn 1 cached)
              fleetReport conn keys Nothing 60000 "a" [block 77] [] `shouldReturn` True
              cacheBegin conn keys "token" "" `shouldReturn` Begun
              planned <- plan conn move
              planned.result `shouldBe` MovedTo (block 77)
              return planned.rounds
        mapM (asked False) [40, 400] `shouldReturn` [3, 3]
        -- one less than when the move took them out itself: it asks
        -- nothing of the addresses of what leaves
        mapM (asked True) [40, 400] `shouldReturn` [4, 4]
      -- A move marks what leaves the pool, and a read that would go by
      -- a marked transaction takes it out first. So whatever is read
      -- between the move and the end of the taking out is what is read
      -- once all of it is out: here every kind of read, of every address,
      -- transaction and output of these histories, from a pool that has
      -- only been marked, against the same pool with everything out.
      modifyMaxSuccess (const 300) $
        it "is read while a block's transactions leave as it is read once they are out" $ \(conn, fresh) ->
          property $ \steps -> ioProperty $ do
            marked <- fresh
            out' <- fresh
            foldM_ (reading conn marked out') (Nothing, 0) (steps :: [PoolStep])
      modifyMaxSuccess (const 300) $
        it "is decided in Haskell as the script decides it, state by state" $ \(conn, fresh) ->
          property $ \steps -> ioProperty $ do
            lua <- fresh
            ours <- fresh
            forM_ [lua, ours] (watched conn)
            foldM_ (both conn lua ours) ([], [], Nothing, 0) (steps :: [PoolStep])

-- | A move of the tip that is carried out twelve seconds after it was
-- decided, as that of a full block is on a slow machine, while
-- transactions that spend from those of the block are offered without a
-- pause, and with leases of ten seconds, as stores have them.
--
-- The store says every two seconds that it is at its move, as a member
-- does (@moveTip@), or says nothing. Saying it, it keeps the floor and
-- the move is carried out. Saying nothing, it has the floor for ten
-- seconds, the transactions are taken after that, and the move is
-- refused: what happened on a server at every block, the floor lasting
-- ten seconds from when it was taken.
longMove :: Bool -> Redis.Connection -> FleetKeys -> IO ()
longMove saying conn keys = do
  forM_ txs $ \tx -> poolOffer conn keys 4 Nothing tx [True] [] `shouldReturn` Taken
  fleetReport conn keys Nothing 10000 "a" [target] [] `shouldReturn` True
  cacheBegin conn keys "token" "" `shouldReturn` Begun
  poolOffer conn keys 4 Nothing (childTx 1) [False] [] `shouldReturn` Late
  planned <- plan conn move
  planned.result `shouldBe` MovedTo target
  carried <-
    withAsync (forM_ (cycle [1 .. n]) (\c -> offered c >> threadDelay 5000)) $ \_ ->
      withAsync (when saying (forever (threadDelay 2000000 >> cacheBeat conn keys "token"))) $ \_ -> do
        threadDelay 12000000
        carry conn planned
  if saying
    then do
      carried `shouldBe` Right ()
      fleetTip conn keys `shouldReturn` Just target
      cacheBeat conn keys "token" `shouldReturn` False
    else do
      carried `shouldSatisfy` isLeft
      fleetTip conn keys `shouldReturn` Nothing
  where
    n = 200 :: Word32
    target = block 77
    txs = map blockTx [1 .. n]
    offered c = void (poolOffer conn keys 4 Nothing (childTx c) [False] [])
    move :: (Monad m) => Atomic m Moved
    move =
      D.advance
        (D.poolKeys keys.prefix 4)
        Nothing
        target
        Nothing
        keys.size
        -- the move that takes everything out in one step, as a move
        -- that undoes a block still does: the long decision is what
        -- the floor is for. One that only marks what leaves depends on
        -- nothing that these offers change, and is not refused.
        (D.AtOnce (PoolChange [] [] (map (.txid) txs) [] (Just 100) []))
        []
        "token"

-- | A transaction that spends the output of one of a block, at its
-- address.
childTx :: Word32 -> PoolTx
childTx c =
  PoolTx
    { txid = encode (5000000 + c),
      time = 1700009000,
      inputs = [encode c <> encode (0 :: Word32)],
      body = B.replicate 250 7,
      entries =
        [ PoolTouches (C.pack ("addr" <> show c)),
          PoolSpends (C.pack ("addr" <> show c)) (encode c <> encode (0 :: Word32)) 1000
        ]
    }

-- | A transaction of a block, with an address of its own and one that it
-- shares with the next.
blockTx :: Word32 -> PoolTx
blockTx c =
  PoolTx
    { txid = encode c,
      time = 1700000000 + fromIntegral c,
      inputs = [encode (1000000 + c) <> encode (0 :: Word32)],
      body = B.replicate 250 7,
      entries =
        [ PoolTouches (addr c),
          PoolPays (addr c) (encode c <> encode (0 :: Word32)) 1000 "script",
          PoolTouches (addr (c + 1)),
          PoolSpends (addr (c + 1)) (encode (1000000 + c) <> encode (0 :: Word32)) 1000
        ]
    }
  where
    addr a = C.pack ("addr" <> show a)

-- | What the store that moves the tip does for the floor.
data Floor
  = -- | nothing: it moves the tip without a move of the cache
    Free
  | -- | it takes it, and says nothing after
    Silent
  | -- | it takes it, and says every seven seconds that it is at its move
    Kept

-- | A move of the tip is decided, and before it is carried out other
-- decisions are, seven seconds after each other: transactions offered,
-- withdrawn and expired, what the fleet saw trimmed, entries of the cache
-- built, dropped and evicted, another store that tries to move the tip,
-- and one that renews its lease. Every answer that the move depends on
-- must be the same after them: then the script carries the move out,
-- whatever the others did.
--
-- With the floor kept ('Haskoin.Store.Fleet.XPub.Decide.beat'), that
-- holds, however long the others go on: all of them give way, and what
-- the store says to keep the floor changes no answer of its own move.
-- With the floor taken and nothing said after, it does not: the others
-- stop giving way after ten seconds, which is how the tip of a server
-- stayed where it was, its move taking longer than that to decide. Nor
-- does it without the floor. The last two properties say so, so that the
-- first is known to see what it is for.
undisturbed :: Floor -> [PoolStep] -> [PoolStep] -> [Word32] -> Property
undisturbed how earlier during confirmed =
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
    held = case how of
      Free -> False
      _ -> True
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
    pool = foldl (flip (others now)) start earlier
    begun = if held then decided now (X.begin kx "mover" "") pool else pool
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
    -- the other store renews its lease, and the rest decide, each seven
    -- seconds after the one before, the store of the move saying a second
    -- before each that it is at it, if it does
    later = foldl next (perform (HSet "fleet:lease" "b" "950000") begun) (zip [1 ..] during)
    next db (i, step) =
      let at = now + 7000 * i
       in others at step $ case how of
            Kept -> decided (at - 1000) (X.beat kx "mover") db
            _ -> db
    decided :: Integer -> (forall m. (Monad m) => Atomic m a) -> Db -> Db
    decided at decision db = either (error . show) snd (runPure at decision db)
    others :: Integer -> PoolStep -> Db -> Db
    others at step = case step of
      Give t inputs replaced ->
        let outs = [(out a i, ok) | (a, i, ok) <- nubOrdOn (\(a, i, _) -> (a, i)) inputs, a < t]
         in decided at (D.offer k Nothing (map snd outs) (map block replaced) (txOf t (map fst outs)))
      Take ts -> decided at (D.withdraw k (map block ts))
      Go back dead done _ ->
        decided at (D.expire k (toInteger (timeOf (sum dead))) (length done + 1))
          . decided at (X.evict kx (length done))
          . decided at (X.drop kx "x1")
          . decided at (X.build kx "" "x3" (0, 0) 1 [added (sum dead)])
          . decided at (X.begin kx "other" "")
          . decided at (X.piece kx "other" "" [X.Piece (address (fromIntegral (length back))) "" [] [] [] []])
          . decided at (X.abort kx "other")
    added a = X.Addition {address = address a, path = "\0\0\0\0\0", balance = "", txs = [], outs = []}

-- | A pool, and a move of the tip over a block that confirms some
-- transactions and spends some outputs, made twice: taking everything out
-- in its own step, and marking what leaves, which is then taken out in an
-- order and in pieces that the test chooses.
--
-- While a transaction is marked, the others change nothing: an offer, a
-- withdrawal, an expiry, and the building, dropping and evicting of an
-- entry of the cache each leave Redis as it was. And once the last is
-- out, Redis has what the move in one step left, the events in another
-- order: a block is told before what it replaced is told as gone.
marking :: [PoolStep] -> [PoolStep] -> [Word32] -> [(Word32, Word32)] -> [Int] -> Property
marking earlier during confirmed used order =
  conjoin
    [ counterexample "the others changed Redis while a transaction was marked" quiet,
      counterexample "marks are left" (Map.notMember "pool:leaving" after),
      counterexample (show differing) (null differing)
    ]
  where
    -- the keys that the two do not end with the same in: marked and
    -- taken out first, in one step second
    differing =
      [ (key, Map.lookup key (told after), Map.lookup key (told once))
      | key <- Map.keys (Map.union after once),
        Map.lookup key (told after) /= Map.lookup key (told once)
      ]
    now = 5000
    k = D.poolKeys "" 4
    kx = X.cacheKeys ""
    target = block 1000
    start =
      foldl
        (flip perform)
        emptyDb
        ( [HSet "fleet:lease" "a" "900000", HSet "fleet:recent" "a" target]
            <> [SAdd ("fx:of:" <> address a) i | (a, ids) <- [(0, ["x1"]), (1, ["x1", "x2"]), (2, ["x2"])], i <- ids]
        )
    pool = foldl (flip others) start earlier
    change =
      PoolChange
        []
        []
        (map block confirmed)
        [out a i <> block ((a * 7 + i * 3) `mod` 12) <> encode (100 :: Word32) | (a, i) <- used]
        (Just 100)
        []
    move :: D.How -> (forall m. (Monad m) => Atomic m Moved)
    move how = D.advance k Nothing target (Just now) 4 how [] ""
    once = decided (move (D.AtOnce change)) pool
    begun = decided (move (D.Changing change)) pool
    leaving db = case Map.lookup "pool:leaving" db of
      Just (Hash fields) -> Map.keys fields
      _ -> []
    -- the marked, in the order that the test gives, and in pieces of one
    -- to three
    shuffled = map snd (sort (zip (order <> [0 ..]) (leaving begun)))
    pieces n xs = case splitAt (1 + n `mod` 3) xs of
      (a, []) -> [a]
      (a, rest) -> a : pieces (n + 1) rest
    (after, quiet) = foldl step (begun, True) (zip (pieces (length order) shuffled) trying)
    trying = cycle (Nothing : map Just during)
    -- the others try first, and change nothing; then some are taken out
    step (db, ok) (piece, other) =
      let tried = maybe db (`others` db) other
       in (decided (D.takeOut k piece) tried, ok && (null (leaving db) || tried == db))
    decided :: (forall m. (Monad m) => Atomic m a) -> Db -> Db
    decided decision db = either (error . show) snd (runPure now decision db)
    others :: PoolStep -> Db -> Db
    others = \case
      Give t inputs replaced ->
        let outs = [(out a i, ok) | (a, i, ok) <- nubOrdOn (\(a, i, _) -> (a, i)) inputs, a < t]
         in decided (D.offer k Nothing (map snd outs) (map block replaced) (txOf t (map fst outs)))
      Take ts -> decided (D.withdraw k (map block ts))
      Go _ dead done _ ->
        decided (D.expire k (toInteger (timeOf (sum dead))) (length done + 1))
          . decided (X.evict kx (length done))
          . decided (X.drop kx "x1")
          . decided (X.build kx "" "x3" (0, 0) 1 [added (sum dead)])
    added a = X.Addition {address = address a, path = "\0\0\0\0\0", balance = "", txs = [], outs = []}
    -- Redis with the events in no order, and with the version of an
    -- entry of the cache as whether it has one: which entry ends with the
    -- highest number depends on the order in which transactions left.
    --
    -- And with a count of zero as no count. A count is added to and
    -- taken from, one transaction at a time, and where two transactions
    -- that leave together count the same output, one as made and the
    -- other as used up, the order in which they leave decides whether
    -- the count went through one on its way to nothing: Redis keeps a
    -- zero for a count that did, and nothing for one that did not. Every
    -- read takes a count that is not there for zero (@read.lua@,
    -- @xpub.lua@), and no decision asks for one of these but the count of
    -- transactions, which each transaction adds one to.
    told = Map.mapWithKey $ \key -> \case
      Stream events -> Stream (Seq.fromList (sort (toList events)))
      Hash fields
        | "fx:e:" `B.isPrefixOf` key && ":meta" `B.isSuffixOf` key -> Hash (Map.adjust (const "some") "ver" fields)
        | any (`B.isPrefixOf` key) ["pool:asum:", "pool:csum:", "pool:tsum:"] -> Hash (Map.filter (/= "0") fields)
      other -> other

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
        -- the tip is at the block, and what the block confirmed is
        -- marked and still in the pool: nothing is offered into it, and
        -- it is taken out two hundred at a time, under the same traffic
        fleetTip conn keys `shouldReturn` Just target
        marked <- poolLeaving conn keys
        length marked `shouldBe` fromIntegral n
        poolOffer conn keys 4 (Just target) (child (1 :: Word32)) [False] [] `shouldReturn` Late
        let pieces xs = case splitAt 200 xs of
              (a, []) -> [a]
              (a, rest) -> a : pieces rest
        taken <- mapM (poolTakeOut conn keys 4) (pieces marked)
        sum taken `shouldBe` fromIntegral n
        poolLeaving conn keys `shouldReturn` []
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
      when (n `mod` 6 == 2) . onBoth $ \key -> go (Redis.set (key "fx:moving") "tok")
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
      -- The move in Haskell only marks what leaves the pool when no
      -- block is undone, where the script took it out: it is taken out
      -- here, in an order and in pieces that change from one step to
      -- the next, and the two are the same again after it. Until then
      -- nothing else is put in or taken out.
      marked <- either (error . show) id <$> Redis.runRedis conn (Redis.hkeys (ours.prefix <> "pool:leaving"))
      unless (null marked) $ do
        transact conn 1 (D.offer k (Just target) [] [] (txOf 4000 [])) `shouldReturn` Just Late
        transact conn 1 (D.withdraw k marked) `shouldReturn` Just []
        let order = if even n then marked else reverse marked
            size = 1 + fromIntegral (n `mod` 3)
            pieces xs = case splitAt size xs of
              (a, []) -> [a]
              (a, rest) -> a : pieces rest
        counts <- forM (pieces order) (\piece -> transact conn 1 (D.takeOut k piece))
        fmap sum (sequence counts) `shouldBe` Just (length marked)
        -- and taking them out again takes nothing
        transact conn 1 (D.takeOut k marked) `shouldReturn` Just 0
      let cutoff = toInteger (timeOf (sum dead))
          most = length done + 1
      expected <- luaExpire conn lua 4 cutoff most
      got <- transact conn 1 (D.expire k cutoff most)
      fmap Set.fromList got `shouldBe` Just (Set.fromList expected)
      return (if moved == MovedTo target then Just target else tip, n + 1)
  there <- everything conn lua.prefix
  here <- everything conn ours.prefix
  versions here0 here `shouldBe` versions there0 there
  return (there, here, tip', n')

-- carry a step out under two prefixes, by the decisions. After a move,
-- under the first what leaves the pool is only marked, and under the
-- second it is all taken out: every read of the first is then the read
-- of the second, and takes out what it has to. Then the rest is taken
-- out, and Redis has the same under both.
reading ::
  Redis.Connection ->
  FleetKeys ->
  FleetKeys ->
  (Maybe ByteString, Word32) ->
  PoolStep ->
  IO (Maybe ByteString, Word32)
reading conn (marked :: FleetKeys) (settled :: FleetKeys) (tip, n) step = do
  let each f = forM_ [marked, settled] (\(keys :: FleetKeys) -> f keys (D.poolKeys keys.prefix 4))
      everyone = [0 .. 12] :: [Word32]
      outs = [out t i | t <- everyone, i <- [0, 1]]
      reads at =
        [("sums", map address everyone), ("bodies", [B.concat (map block everyone)])]
          <> [("spenders", [B.concat outs]), ("replaced", map block everyone)]
          <> [("page", ["0", "0"]), ("page", ["1", "3"]), ("ids", [""]), ("ids", ["\0\0\0"])]
          <> concat
            [ [ ("txs", [address a, "", "", "100"]),
                ("outs", [address a, "", "", "100"]),
                ("spent", [address a, B.concat outs]),
                ("tokens", [address a])
              ]
            | a <- at
            ]
  case step of
    Give t inputs replaced -> do
      let spent = [(out a i, ok) | (a, i, ok) <- nubOrdOn (\(a, i, _) -> (a, i)) inputs, a < t]
          tx = txOf t (map fst spent)
      each $ \_ k -> void (transact conn 1 (D.offer k tip (map snd spent) (map block replaced) tx))
      return (tip, n)
    Take ts -> do
      each $ \_ k -> void (transact conn 1 (D.withdraw k (map block ts)))
      return (tip, n)
    Go back dead done used -> do
      let target = block (1000 + n)
          height = 700 * (n + 1)
          -- two moves in three undo no block, and so only mark
          undo = n `mod` 3 == 2
          labelled = [(out a i, block ((a * 7 + i * 3 + n) `mod` 12) <> encode height) | (a, i) <- used]
          sent =
            PoolChange
              [txOf (number (block t)) [out a i | (a, i) <- nubOrd ins, a < t] | undo, (t, ins) <- back]
              [block t | undo, t <- dead]
              (map block done)
              [o <> w | (o, w) <- labelled]
              (Just height)
              []
      results <- forM [marked, settled] $ \(keys :: FleetKeys) -> do
        fleetReport conn keys (Just 0) 10 "a" [target] (maybe [] pure tip) `shouldReturn` True
        transact conn 1 (D.advance (D.poolKeys keys.prefix 4) tip target (Just 0) 4 (D.Changing sent) [] "")
      let tip' = if all (== Just (MovedTo target)) results then Just target else tip
      take 1 results `shouldBe` drop 1 results
      -- everything out under the second prefix
      left <- poolLeaving conn settled
      unless (null left) (void (poolTakeOut conn settled 4 left))
      -- every read under the first is the read under the second; the
      -- addresses in an order of their own each time
      let at = drop (fromIntegral n `mod` 5) everyone <> take (fromIntegral n `mod` 5) everyone
      forM_ (reads at) $ \(what, args) -> do
        got <- poolRead conn marked 4 tip' what args
        expected <- poolRead conn settled 4 tip' what args
        (what, args, got) `shouldBe` (what, args, expected)
      rest <- poolLeaving conn marked
      unless (null rest) (void (poolTakeOut conn marked 4 rest))
      there <- everything conn marked.prefix
      here <- everything conn settled.prefix
      versions [] there `shouldBe` versions [] here
      return (tip', n + 1)

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
-- only for which transactions; of the events, not their numbers nor
-- their order. What the fleet saw is left out: the script kept a set of
-- it, by which the stores decided what to fetch, and the decisions keep
-- none, since a store fetches whatever the pool has not
everything :: Redis.Connection -> ByteString -> IO Dump
everything conn p = do
  let list :: Redis.Redis (Either Redis.Reply a) -> IO a
      list = fmap (either (error . show) id) . Redis.runRedis conn
      name = B.drop (B.length p)
  ks <- sort . filter ((/= "fleet:seen") . name) <$> list (Redis.keys (p <> "*"))
  forM ks $ \key ->
    (name key,) <$> do
      kind <- list (Redis.getType key)
      case kind of
        Redis.String -> maybe [] pure <$> list (Redis.get key)
        Redis.Hash
          | name key == "pool:entries" -> sort . map fst <$> list (Redis.hgetall key)
          -- a count of zero is no count, as in the model ('marking')
          | any (`B.isPrefixOf` name key) ["pool:asum:", "pool:csum:", "pool:tsum:"] ->
              sort . map (\(f, v) -> f <> " = " <> v) . filter ((/= "0") . snd) <$> list (Redis.hgetall key)
          | otherwise -> sort . map (\(f, v) -> f <> " = " <> v) <$> list (Redis.hgetall key)
        Redis.Set -> sort <$> list (Redis.smembers key)
        Redis.ZSet ->
          map (\(x, sc) -> x <> " @ " <> C.pack (show sc)) <$> list (Redis.zrangeWithscores key 0 (-1))
        -- transactions that spend from the same one leave in the
        -- order in which Redis lists a set, which is no order
        Redis.Stream ->
          sort . map (B.intercalate " " . concatMap (\(f, v) -> [f, v]) . (.keyValues))
            <$> list (Redis.xrange key "-" "+" Nothing)
        other -> return [C.pack (show other)]
