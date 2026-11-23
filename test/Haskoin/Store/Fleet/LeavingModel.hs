{-# OPTIONS_GHC -O1 #-}

-- | The pool after a move of the tip, checked in small worlds: every pool,
-- every block and every order of taking out, with the decisions of the
-- store itself. @docs\/tip-first.md@ says what the protocol is, and
-- @scripts\/check-pool.sh@ runs this without building the store.
--
-- = What a world is
--
-- A few transactions that could be in a pool, the candidates: some spend
-- from others, two spend the same coin, some move tokens, and cached keys
-- may watch their addresses. From them:
--
-- * every pool: each set of candidates is offered, and the pool takes
--   those that it can;
-- * every block: each set of candidates is confirmed. A candidate that the
--   pool does not have and that spends what one of the pool spends makes
--   that one invalid;
-- * every order of taking out: from the pool as the move marked it, each
--   marked transaction is taken out next, in turn, down to none.
--
-- = What is checked
--
-- For every pool and every block:
--
-- * __What is marked.__ The move that marks, marks the transactions that
--   the move in one step takes out, and no other.
--
-- And in every state on the way, with some transactions out and some
-- still marked:
--
-- * __Writers wait.__ An offer, a withdrawal, an expiry, a build, a drop
--   and an eviction of an entry of the cache, the beginning of a move of
--   the cache and another move of the tip each leave Redis as it is. Each
--   asks for itself whether anything is marked, and a write to a key of
--   the pool or of the cache asks whatever the decision does
--   ('Haskoin.Store.Fleet.Atomic.guarding'): this holds with either of
--   the two taken away, and fails with both.
-- * __A read is exact.__ For each address, category of tokens,
--   transaction, output and cached key: if none of the transactions that
--   its read would go by is marked, then all that the read reads is
--   already what it is once everything is out. And taking out those that
--   are marked makes it so.
-- * __Any order ends the same.__ With no mark left, Redis has what the
--   move in one step left, and a new transaction is taken again.
--
-- Two states are the same here with the events in no order, the version
-- of an entry of the cache as whether it has one, and a count of zero as
-- no count ('same'): @docs\/fleet-mempool.md@ says why.
--
-- = What it does not show
--
-- Nothing about a pool of thousands. The reads are those of
-- @lua\/read.lua@ and @lua\/xpub.lua@ as this module says them again in
-- Haskell ('aims'): that the scripts read those keys and name those
-- transactions is sampled against a Redis
-- (@Haskoin.Store.Fleet.DecideSpec@), not checked here.
module Haskoin.Store.Fleet.LeavingModel
  ( World (..),
    Report (..),
    worlds,
    spoilt,
    explore,
    describeReport,
  )
where

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Foldable (toList)
import Data.List (nub, sort, subsequences)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Sequence qualified as Seq
import Data.Serialize (encode)
import Data.Set qualified as Set
import Data.Word (Word32, Word64)
import Haskoin.Store.Fleet.Atomic (Atomic, Db, Op (..), Value (..), emptyDb, heldByMark, perform, runPure)
import Haskoin.Store.Fleet.Pool.Decide qualified as D
import Haskoin.Store.Fleet.Pool.Types
import Haskoin.Store.Fleet.XPub.Decide qualified as X

-------------------------------------------------------------------------------
-- Worlds
-------------------------------------------------------------------------------

-- | An output: where it is, the address that it pays, its amount, and the
-- tokens that it holds, a category and how many.
data Coin = Coin
  { ref :: !ByteString,
    at :: !ByteString,
    worth :: !Word64,
    kind :: !(Maybe (ByteString, Word64))
  }

-- | A transaction that could be in the pool: its number, what it spends
-- and what it pays.
data Cand = Cand
  { number :: !Word32,
    uses :: ![Coin],
    makes :: ![Coin]
  }

-- | A world: its name, its candidates, and for some addresses the cached
-- keys that have them.
data World = World
  { name :: !String,
    cands :: ![Cand],
    watching :: ![(ByteString, [ByteString])]
  }

-- | What going through a world found: how many pairs of a pool and a
-- block, how many states between a move and the last transaction out,
-- and the first thing that did not hold.
data Report = Report
  { pairs :: !Int,
    states :: !Int,
    failure :: !(Maybe String)
  }

describeReport :: Report -> String
describeReport r = case r.failure of
  Nothing -> show r.pairs <> " pools and blocks, " <> show r.states <> " states: all hold"
  Just why -> "FAILS after " <> show r.pairs <> " pools and blocks: " <> why

idOf :: Word32 -> ByteString
idOf = encode

cand :: Word32 -> [Coin] -> [(ByteString, Word64, Maybe (ByteString, Word64))] -> Cand
cand n ins outs =
  Cand n ins [Coin (idOf n <> encode i) a v t | (i, (a, v, t)) <- zip [0 :: Word32 ..] outs]

-- | The transaction of a candidate as a store offers it
-- (@Haskoin.Store.Fleet.Member.poolTx@).
poolTxOf :: Cand -> PoolTx
poolTxOf c =
  PoolTx
    { txid = idOf c.number,
      time = 1000 + fromIntegral c.number,
      inputs = map (.ref) c.uses,
      body = "body",
      entries =
        [PoolTouches a | a <- nub (map (.at) (c.uses <> c.makes))]
          <> [PoolSpends i.at i.ref i.worth | i <- c.uses]
          <> [PoolPays o.at o.ref o.worth "script" | o <- c.makes]
          <> [PoolMakes ((tokens o t) {coins = Just (o.worth, "script")}) | o <- c.makes, Just t <- [o.kind]]
          <> [PoolUses (tokens i t) | i <- c.uses, Just t <- [i.kind]]
    }
  where
    tokens x (category, amount) =
      PoolTokenOut {address = Just x.at, output = x.ref, category, amount, nft = False, coins = Nothing}

a1, a2, cat1, cat2 :: ByteString
a1 = "addrA"
a2 = "addrB"
cat1 = "cat1"
cat2 = "cat2"

-- | Coins of the chain, unspent at the tip before the block.
c0, c1, c2, c3 :: Coin
c0 = Coin "chain-c0" a1 10 Nothing
c1 = Coin "chain-c1" a2 20 Nothing
c2 = Coin "chain-c2" a1 30 (Just (cat1, 100))
c3 = Coin "chain-c3" a2 5 Nothing

-- | The worlds. Each has a chain of transactions that spend from one
-- another and two that spend the same coin, so that a block can confirm
-- any of them, make any of them invalid, and leave the children of what
-- it confirms.
worlds :: [World]
worlds =
  [ World "a chain of three, two that spend one coin, and one that joins two" plain [],
    World "the same, with cached keys at both addresses" plain [(a1, ["x1"]), (a2, ["x1", "x2"])],
    World "tokens made, moved and used up, and two that spend one coin of tokens" coloured [(a1, ["x1"])],
    World "coins and tokens together, with cached keys" mixed [(a1, ["x1"]), (a2, ["x2"])]
  ]
  where
    plain =
      let t0 = cand 0 [c0] [(a2, 6, Nothing), (a1, 3, Nothing)]
          t1 = cand 1 (take 1 t0.makes) [(a1, 5, Nothing)]
          t2 = cand 2 t1.makes [(a2, 4, Nothing)]
          t3 = cand 3 [c0] [(a2, 9, Nothing)]
          t4 = cand 4 (c1 : drop 1 t0.makes) [(a1, 20, Nothing)]
       in [t0, t1, t2, t3, t4]
    coloured =
      let t0 = cand 0 [c2] [(a2, 29, Just (cat1, 60)), (a1, 1, Just (cat1, 40))]
          t1 = cand 1 (take 1 t0.makes) [(a1, 28, Just (cat1, 60))]
          t2 = cand 2 [c2] [(a1, 29, Just (cat1, 100))]
          t3 = cand 3 (c3 : drop 1 t0.makes) [(a2, 5, Just (cat1, 40))]
          t4 = cand 4 [c1] [(a2, 19, Just (cat2, 7))]
       in [t0, t1, t2, t3, t4]
    mixed =
      let t0 = cand 0 [c0] [(a2, 9, Nothing)]
          t1 = cand 1 t0.makes [(a1, 8, Nothing)]
          t2 = cand 2 [c0] [(a1, 9, Nothing)]
          t3 = cand 3 [c2] [(a2, 29, Just (cat1, 100))]
          t4 = cand 4 (t3.makes <> t1.makes) [(a1, 36, Just (cat1, 100))]
          t5 = cand 5 [c2, c1] [(a1, 49, Just (cat1, 100))]
       in [t0, t1, t2, t3, t4, t5]

-------------------------------------------------------------------------------
-- Redis, and the decisions
-------------------------------------------------------------------------------

now :: Integer
now = 5000

k :: D.Keys
k = D.poolKeys "" 4

kx :: X.Keys
kx = X.cacheKeys ""

target :: ByteString
target = idOf 1000

run :: (forall m. (Monad m) => Atomic m a) -> Db -> (a, Db)
run decision db = either (error . show) id (runPure now decision db)

-- | What a decision leaves of Redis. One that is stopped at a mark
-- leaves Redis as it is: a write to a key of the pool or of the cache
-- asks first whether anything is marked
-- ('Haskoin.Store.Fleet.Atomic.guarding'), whatever the decision does
-- for itself.
attempt :: (forall m. (Monad m) => Atomic m a) -> Db -> Db
attempt decision db = case runPure now decision db of
  Right (_, after) -> after
  Left why
    | heldByMark why -> db
    | otherwise -> error (show why)

hashOf :: ByteString -> Db -> Map ByteString ByteString
hashOf key db = case Map.lookup key db of
  Just (Hash fields) -> fields
  _ -> Map.empty

membersOf :: ByteString -> Db -> [ByteString]
membersOf key db = case Map.lookup key db of
  Just (Sorted members) -> Map.keys members
  Just (Group members) -> Map.keys members
  Just (Hash fields) -> Map.keys fields
  _ -> []

-- | The transactions that are marked as leaving.
marked :: Db -> [ByteString]
marked = membersOf "pool:leaving"

-- | The transactions of the pool.
pooled :: Db -> [ByteString]
pooled = membersOf "pool:spends"

-- | Redis with one live store that reported the block, and the cached
-- keys at their addresses.
start :: World -> Db
start w =
  foldl
    (flip perform)
    emptyDb
    ( [HSet "fleet:lease" "a" "900000", HSet "fleet:recent" "a" target]
        <> [SAdd ("fx:of:" <> a) i | (a, ids) <- w.watching, i <- ids]
        -- each cached key has a priority, so that an eviction has
        -- something to evict
        <> [ZAdd "fx:prio" 1 i | i <- nub (concatMap snd w.watching)]
    )

-- | Offer a candidate, with the tip that the store saw and whether each
-- coin that it spends is unspent in the chain at that tip.
offering :: Maybe ByteString -> (Coin -> Bool) -> Cand -> Db -> (Offered, Db)
offering tip unspent c = run (offer tip unspent c)

offer :: Maybe ByteString -> (Coin -> Bool) -> Cand -> (forall m. (Monad m) => Atomic m Offered)
offer tip unspent c = D.offer k tip (map unspent c.uses) [] (poolTxOf c)

-- | Whether a coin is one of the chain.
ofChain :: Coin -> Bool
ofChain c = "chain-" `B.isPrefixOf` c.ref

-- | Two states of Redis that are the same: with the events in no order,
-- the version of an entry of the cache as whether it has one, and a count
-- of zero as no count.
same :: Db -> Db
same = Map.mapWithKey $ \key -> \case
  Stream events -> Stream (Seq.fromList (sort (toList events)))
  Hash fields
    | "fx:e:" `B.isPrefixOf` key && ":meta" `B.isSuffixOf` key -> Hash (Map.adjust (const "some") "ver" fields)
    | any (`B.isPrefixOf` key) ["pool:asum:", "pool:csum:", "pool:tsum:"] -> Hash (Map.filter (/= "0") fields)
  other -> other

-------------------------------------------------------------------------------
-- What a read reads, and which transactions it goes by
-------------------------------------------------------------------------------

-- | What is read of the pool: by address, by category of tokens, a
-- transaction with what spends it, an output with what spends it, what
-- replaced a transaction, a cached key, and the list of the pool.
data Aim
  = OfAddress !ByteString
  | OfCategory !ByteString
  | OfTx !ByteString
  | OfOutput !ByteString
  | OfReplaced !ByteString
  | OfKey !ByteString
  | WholePool
  deriving (Show)

-- | Everything that a world can ask for.
aims :: World -> [Aim]
aims w =
  map OfAddress [a1, a2]
    <> map OfCategory [cat1, cat2]
    <> map (OfTx . idOf . (.number)) w.cands
    <> map (OfOutput . (.ref)) (concatMap (\c -> c.uses <> c.makes) w.cands)
    <> map (OfReplaced . idOf . (.number)) w.cands
    <> map OfKey (nub (concatMap snd w.watching))
    <> [WholePool]

-- | The transactions that a read goes by, and that are marked: those that
-- the scripts name, for the store to take out before it reads again.
inTheWay :: Aim -> Db -> [ByteString]
inTheWay aim db = filter (`elem` marked db) $ case aim of
  OfAddress a -> membersOf ("pool:atx:" <> a) db
  OfCategory c -> membersOf ("pool:ctx:" <> c) db
  OfTx t -> t : membersOf ("pool:kids:" <> t) db
  OfOutput o -> toList (Map.lookup o (hashOf "pool:spender" db))
  OfReplaced t -> [t]
  OfKey i -> membersOf ("fx:e:" <> i <> ":txs") db
  -- the list of the pool is read without what is marked: nothing is in
  -- its way
  WholePool -> []

-- | All that a read reads, as a value to compare.
readOf :: World -> Aim -> Db -> [(ByteString, String)]
readOf w aim db0 = case aim of
  OfAddress a ->
    keys (\key -> key `elem` map (<> a) atAddress || ("pool:tsum:" `B.isPrefixOf` key && a `B.isSuffixOf` key))
  OfCategory c ->
    keys (\key -> key `elem` map (<> c) ofCategory || ("pool:tsum:" <> c) `B.isPrefixOf` key)
      <> [("spender of " <> o, show (field "pool:spender" o)) | o <- membersOf ("pool:cout:" <> c) db]
  OfTx t ->
    [ ("body", show (field "pool:body" t)),
      ("kids", show (membersOf ("pool:kids:" <> t) db)),
      ("what the kids spend", show [field "pool:spends" kid | kid <- membersOf ("pool:kids:" <> t) db])
    ]
  OfOutput o ->
    let s = field "pool:spender" o
     in [("spender", show s), ("what it spends", show (s >>= field "pool:spends"))]
  OfReplaced t -> [("replaced by", show (field "pool:rep" t))]
  OfKey i ->
    keys (\key -> key `elem` map (("fx:e:" <> i) <>) [":txs", ":outs", ":oval"])
      <> concat [keys (== ("pool:asum:" <> a)) | (a, ids) <- w.watching, i `elem` ids]
      <> [("spender of " <> o, show (field "pool:spender" o)) | o <- membersOf ("fx:e:" <> i <> ":outs") db]
  WholePool -> case Map.lookup "pool:time" db of
    Just (Sorted members) -> [("the pool", show (Map.toList (Map.withoutKeys members (Set.fromList (marked db)))))]
    _ -> [("the pool", "[]")]
  where
    db = same db0
    field key f = Map.lookup f (hashOf key db)
    keys p = [(key, show v) | (key, v) <- Map.toList db, p key]
    atAddress = ["pool:atx:", "pool:aout:", "pool:aval:", "pool:aspent:", "pool:asum:", "pool:acat:"]
    ofCategory = ["pool:ctx:", "pool:cout:", "pool:cval:", "pool:csum:", "pool:chold:"]

-------------------------------------------------------------------------------
-- Going through a world
-------------------------------------------------------------------------------

-- | The first world gone through with a fault put in: what a block is in
-- conflict with is not marked. It has to fail, or the checker sees
-- nothing.
spoilt :: Report
spoilt = case worlds of
  w : _ -> exploring (Map.adjust dropRivals "pool:leaving") w
  [] -> Report 0 0 Nothing
  where
    dropRivals = \case
      Hash fields -> Hash (Map.filter B.null fields)
      other -> other

-- | Go through a world: every pool, every block, and every order of
-- taking out.
explore :: World -> Report
explore = exploring id

exploring :: (Db -> Db) -> World -> Report
exploring fault w = go (Report 0 0 Nothing) [(s, c) | s <- subsequences w.cands, c <- subsequences w.cands]
  where
    go r [] = r
    go r ((s, c) : rest) = case pair fault w s c of
      Left why -> r {pairs = r.pairs + 1, failure = Just (describe s c <> ": " <> why)}
      Right n -> go r {pairs = r.pairs + 1, states = r.states + n} rest
    describe s c =
      "a pool offered " <> show (map (.number) s) <> ", a block that confirms " <> show (map (.number) c)

-- | One pool and one block: how many states were gone through, or what
-- did not hold.
pair :: (Db -> Db) -> World -> [Cand] -> [Cand] -> Either String Int
pair fault w offered confirmed = do
  let (movedOnce, once) = run (move (D.AtOnce change)) pool
      (movedMarking, begun0) = run (move (D.Changing change)) pool
      begun = fault begun0
  check (movedOnce == MovedTo target && movedMarking == MovedTo target) "the tip did not move"
  check
    (sort (marked begun) == sort (filter (`notElem` pooled once) (pooled pool)))
    ("what is marked, " <> show (marked begun) <> ", is not what the move in one step takes out")
  -- all of them in one decision
  let (taken, atOnce) = run (D.takeOut k (marked begun)) begun
  check (taken == length (marked begun)) "taking all of them out at once did not count them all"
  check (same atOnce == same once) "taken out in one decision, Redis is not as after the move in one step"
  walk once [begun] [] 0
  where
    check ok why = if ok then Right () else Left why
    pool = foldl (\db c -> snd (offering Nothing ofChain c db)) (start w) offered
    change =
      PoolChange
        { returned = [],
          gone = [],
          confirmed = map (idOf . (.number)) confirmed,
          spent = [i.ref <> idOf c.number <> encode (100 :: Word32) | c <- confirmed, i <- c.uses],
          height = Just 100,
          lost = []
        }
    move :: D.How -> (forall m. (Monad m) => Atomic m Moved)
    move how = D.advance k Nothing target (Just now) 4 how [] ""
    -- the chain at the new tip: the coins of the chain and what the block
    -- made, without what the block spent
    spentByBlock = [i.ref | c <- confirmed, i <- c.uses]
    unspentAfter coin =
      (ofChain coin || any (\c -> coin.ref `elem` map (.ref) c.makes) confirmed) && coin.ref `notElem` spentByBlock
    -- every state between the move and the last transaction out, each
    -- once
    walk _ [] _ n = Right n
    walk once (db : todo) seen n
      | db `elem` seen = walk once todo seen n
      | otherwise = do
          let left = marked db
              final = foldl (\d t -> snd (run (D.takeOut k [t]) d)) db left
          if null left
            then do
              check (Map.notMember "pool:leaving" db) "no transaction is marked and the marks are still there"
              check (same db == same once) "with everything out, Redis is not as after the move in one step"
              -- and the pool takes a new transaction again
              let fresh = cand 99 [Coin "chain-c9" a1 7 Nothing] [(a2, 6, Nothing)]
              check (fst (offering (Just target) (const True) fresh db) == Taken) "the pool takes nothing after it"
            else do
              mapM_ (waits db) (others db)
          mapM_ (aimed db final) (aims w)
          walk once ([snd (run (D.takeOut k [t]) db) | t <- left] <> todo) (db : seen) (n + 1)
    waits db (what, f) = check (f db == db) (what <> " changed Redis while a transaction was marked")
    -- a read: exact if nothing marked is in its way, and made exact by
    -- taking out what is
    aimed db final aim = do
      let inWay = inTheWay aim db
          helped = snd (run (D.takeOut k inWay) db)
      check
        (not (null inWay) || readOf w aim db == readOf w aim final)
        (show aim <> " goes by no marked transaction, and reads other than once everything is out")
      check
        (null (inTheWay aim helped))
        (show aim <> " still goes by a marked transaction after those in its way were taken out")
      check
        (readOf w aim helped == readOf w aim final)
        (show aim <> ", with what was in its way taken out, reads other than once everything is out")
    -- what else writes to the pool or to the cache, each as a change of
    -- Redis
    others :: Db -> [(String, Db -> Db)]
    others db =
      [ ("an offer of " <> show c.number, attempt (offer (Just target) unspentAfter c))
      | c <- w.cands
      ]
        <> [ ("an offer of a child of " <> show c.number, attempt (offer (Just target) unspentAfter (child c)))
           | c <- w.cands,
             idOf c.number `elem` pooled db
           ]
        <> [ ("a withdrawal", attempt (D.withdraw k (pooled db))),
             ("an expiry", attempt (D.expire k 1000000000 100)),
             ("a build of an entry", attempt (X.build kx target "x9" (0, 0) 1 [added])),
             ("a drop of an entry", attempt (X.drop kx "x1")),
             ("an eviction", attempt (X.evict kx 0)),
             ("the beginning of a move of the cache", attempt (X.begin kx "other" target)),
             ("another move of the tip", moving)
           ]
    -- the store reports a newer block, which the tip would move to: only
    -- what the move itself changes counts
    moving db =
      let reported = perform (HSet "fleet:recent" "a" (idOf 1001 <> target)) db
          next :: (forall m. (Monad m) => Atomic m Moved)
          next = D.advance k (Just target) (idOf 1001) (Just now) 4 (D.Changing none) [] ""
          moved = attempt next reported
       in if moved == reported then db else moved
    child c = cand (90 + c.number) (take 1 c.makes) [(a1, 1, Nothing)]
    added = X.Addition {address = a1, path = "\0\0\0\0\0", balance = "", txs = [], outs = []}
    none = PoolChange [] [] [] [] (Just 101) []
