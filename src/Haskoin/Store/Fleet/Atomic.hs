-- |
-- Module      : Haskoin.Store.Fleet.Atomic
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Decisions about the state that a fleet keeps in Redis, made in Haskell
-- and carried out in one step: transactional memory over Redis.
-- "Haskoin.Store.Fleet.Atomic.Redis" runs them against a Redis; this
-- module is the pure part, which needs none.
--
-- = The problem
--
-- The stores of a fleet change what they share in Redis with Lua scripts,
-- because Redis runs a script alone, start to end, and so a script is one
-- decision whatever the other stores do. But a script that fails half way
-- keeps what it wrote: Redis undoes nothing. The script of the shared
-- mempool is eight hundred lines that read and write in turn, so an error
-- in it after its first write, a field that is not there or a number that
-- Redis does not take, leaves the pool half changed, with its indexes out
-- of step. And the rules of the pool were written twice, in Lua to run and
-- in Haskell as a model ("Haskoin.Store.Fleet.Pool"), held together by
-- tests that draw sequences at random.
--
-- = What is done instead
--
-- A decision is a computation in t'Atomic'. It reads Redis only through
-- 'query', which keeps every question with its answer, and it changes
-- nothing: 'write' adds to a list. When it ends, one small script is given
-- the questions with their answers and the list. The script asks every
-- question again; if one answer differs it changes nothing and says so,
-- and the decision is made again from new answers. If all are the same it
-- carries out the list.
--
-- That is as good as running the decision inside Redis. Redis runs the
-- script alone, so the answers are all true at one moment, that of the
-- script. A decision is a function of its answers and of nothing else,
-- which its type says: it runs in any monad, so it can do nothing but ask
-- and write. So what the script carries out is what the decision would
-- have written had it run at that moment.
--
-- And it cannot fail half way: the script compares before it writes
-- anything, and then runs plain commands with nothing to decide between
-- them. An error in a decision is an exception in Haskell, before Redis is
-- told anything.
--
-- = A decision sees what it wrote
--
-- A script reads back what it has written, and a decision here does too:
-- 'query' answers from what Redis said and from what the decision wrote
-- since ('replay'). What Redis is asked is always the state before the
-- decision, and that is what the script compares.
--
-- Where what was written leaves an answer unknown, such as how many
-- members a sorted set has after one was added that may have been there,
-- 'query' fails with t'Unanswerable' rather than guess. Nothing in the rules
-- of the fleet asks such a question; one that did would be found by its
-- first test.
--
-- = The model of Redis
--
-- 'Db' is the part of Redis that the fleet uses, as a pure value, with the
-- questions ('answer') and the writes ('perform') of this module. A
-- decision runs against it as it runs against Redis ('runPure'), so the
-- rules of the fleet are tested, and can be gone through exhaustively,
-- without a Redis. That it says what Redis says is tested against a real
-- one, question by question and write by write.
module Haskoin.Store.Fleet.Atomic
  ( -- * Questions and writes
    Key,
    Score,
    Bound (..),
    Query (..),
    Answer (..),
    Op (..),
    queryKey,
    opKey,

    -- * Decisions
    Atomic,
    Backend (..),
    Unanswerable (..),
    query,
    queries,
    prefetch,
    write,
    clock,
    Outcome (..),
    runAtomic,

    -- * Reading
    get,
    hget,
    hexists,
    hgetall,
    sismember,
    smembers,
    zscore,
    zcard,
    zrangeByScore,
    exists,

    -- * What a decision wrote, seen by its questions
    replay,

    -- * A model of Redis
    Db,
    Value (..),
    emptyDb,
    answer,
    perform,
    runPure,
  )
where

import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT, runExceptT, throwE)
import Control.Monad.Trans.Reader (ReaderT, ask, runReaderT)
import Control.Monad.Trans.State.Strict (StateT, gets, modify', runStateT)
import Data.Functor.Identity (Identity (..))
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.Foldable (toList)
import Data.List (sortOn)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Sequence (Seq, (|>))
import Data.Sequence qualified as Seq
import Data.Set (Set)
import Data.Set qualified as Set

-- | The name of a key of Redis.
type Key = ByteString

-- | The score of a member of a sorted set. The fleet only uses whole
-- numbers that a double holds exactly: times, heights and their negatives.
type Score = Double

-- | An end of a range of scores.
data Bound = MinusInf | PlusInf | At !Score
  deriving (Eq, Ord, Show)

-- | A question to Redis. Each is one command that changes nothing.
data Query
  = -- | @GET@
    Get !Key
  | -- | @HGET@
    HGet !Key !ByteString
  | -- | @HEXISTS@
    HExists !Key !ByteString
  | -- | @HGETALL@
    HGetAll !Key
  | -- | @SISMEMBER@
    SIsMember !Key !ByteString
  | -- | @SMEMBERS@
    SMembers !Key
  | -- | @ZSCORE@
    ZScore !Key !ByteString
  | -- | @ZCARD@
    ZCard !Key
  | -- | @ZRANGEBYSCORE@ with the scores, and with at most so many members
    -- if a number is given
    ZRangeByScore !Key !Bound !Bound !(Maybe Int)
  | -- | @EXISTS@
    Exists !Key
  deriving (Eq, Ord, Show)

-- | What Redis says to a question.
data Answer
  = -- | to 'Get' and 'HGet'
    Text !(Maybe ByteString)
  | -- | to 'HExists', 'SIsMember' and 'Exists'
    Flag !Bool
  | -- | to 'HGetAll'
    Fields !(Map ByteString ByteString)
  | -- | to 'SMembers'
    Members !(Set ByteString)
  | -- | to 'ZScore'
    Scored !(Maybe Score)
  | -- | to 'ZCard'
    Count !Integer
  | -- | to 'ZRangeByScore': the members in the order of Redis, by score and
    -- then by their bytes
    Ranked ![(ByteString, Score)]
  deriving (Eq, Ord, Show)

-- | A change to Redis. Each is one command that cannot fail on a key of
-- the kind that it is for, and the keys of the fleet each have one kind.
data Op
  = -- | @SET@
    Set !Key !ByteString
  | -- | @DEL@
    Del !Key
  | -- | @INCR@
    Incr !Key
  | -- | @HSET@
    HSet !Key !ByteString !ByteString
  | -- | @HDEL@
    HDel !Key !ByteString
  | -- | @HINCRBY@
    HIncrBy !Key !ByteString !Integer
  | -- | @SADD@
    SAdd !Key !ByteString
  | -- | @SREM@
    SRem !Key !ByteString
  | -- | @ZADD@
    ZAdd !Key !Score !ByteString
  | -- | @ZADD NX@: only if the member is not there
    ZAddNX !Key !Score !ByteString
  | -- | @ZREM@
    ZRem !Key !ByteString
  | -- | @ZREMRANGEBYRANK@
    ZRemRangeByRank !Key !Integer !Integer
  | -- | @XADD@ to a stream that keeps about so many entries, with these
    -- fields
    XAdd !Key !Int ![(ByteString, ByteString)]
  | -- | delete every key that starts with these bytes
    Wipe !ByteString
  deriving (Eq, Ord, Show)

-- | The key that a question is about.
queryKey :: Query -> Key
queryKey = \case
  Get k -> k
  HGet k _ -> k
  HExists k _ -> k
  HGetAll k -> k
  SIsMember k _ -> k
  SMembers k -> k
  ZScore k _ -> k
  ZCard k -> k
  ZRangeByScore k _ _ _ -> k
  Exists k -> k

-- | The key that a write changes, or the start of those that it deletes.
opKey :: Op -> Either ByteString Key
opKey = \case
  Set k _ -> Right k
  Del k -> Right k
  Incr k -> Right k
  HSet k _ _ -> Right k
  HDel k _ -> Right k
  HIncrBy k _ _ -> Right k
  SAdd k _ -> Right k
  SRem k _ -> Right k
  ZAdd k _ _ -> Right k
  ZAddNX k _ _ -> Right k
  ZRem k _ -> Right k
  ZRemRangeByRank k _ _ -> Right k
  XAdd k _ _ -> Right k
  Wipe p -> Left p

-------------------------------------------------------------------------------
-- What a decision wrote, seen by its questions
-------------------------------------------------------------------------------

-- | A question cannot be answered from what Redis said and what the
-- decision wrote since: the question, and why.
data Unanswerable = Unanswerable !Query !String
  deriving (Eq, Show)

-- | The answer to a question after writes, given the answer before them.
-- The writes are those to the key of the question, in their order, with a
-- 'Del' for each 'Wipe' that takes the key.
--
-- For every question, answer and list of writes that it has an answer
-- for, the answer is what Redis says after the writes if it said the
-- first before them: 'answer' and 'perform' are the statement of that,
-- and the specs check one against the other, and both against a Redis.
replay :: Query -> Answer -> [Op] -> Either Unanswerable Answer
replay q before ops = case (q, before) of
  (Get _, Text v) -> Text <$> foldM' text v
  (HGet _ f, Text v) -> Text <$> foldM' (field f) v
  (HExists _ f, Flag b) -> Flag <$> foldM' (has f) b
  (HGetAll _, Fields m) -> Fields <$> foldM' fields m
  (SIsMember _ x, Flag b) -> Flag <$> foldM' (isIn x) b
  (SMembers _, Members s) -> Members <$> foldM' members s
  (ZScore _ x, Scored s) -> Scored <$> foldM' (score x) s
  (ZCard _, Count n) -> Count <$> foldM' count n
  (ZRangeByScore _ lo hi limit, Ranked ms)
    | null ops -> Right before
    | Just _ <- limit -> no "a range with a limit, of a sorted set that was written to"
    | otherwise -> Ranked . sortOn (\(x, s) -> (s, x)) . Map.toList <$> foldM' (ranked lo hi) (Map.fromList ms)
  (Exists _, Flag b) ->
    foldM' there (Just b) >>= \case
      Just b' -> Right (Flag b')
      Nothing -> no "whether a key is there after something was taken out of it"
  _ -> no "an answer of another kind than the question"
  where
    no :: String -> Either Unanswerable a
    no = Left . Unanswerable q
    wrong :: Op -> Either Unanswerable a
    wrong op = no ("a write of another kind than the key: " <> show op)
    foldM' :: (a -> Op -> Either Unanswerable a) -> a -> Either Unanswerable a
    foldM' f z = foldl' (\acc op -> acc >>= (`f` op)) (Right z) ops
    number :: Maybe ByteString -> Either Unanswerable Integer
    number Nothing = Right 0
    number (Just v) = case C.readInteger v of
      Just (n, rest) | B.null rest -> Right n
      _ -> no "a number, of a value that is none"
    text v = \case
      Set _ x -> Right (Just x)
      Del _ -> Right Nothing
      Incr _ -> Just . C.pack . show . (+ 1) <$> number v
      op -> wrong op
    field f v = \case
      HSet _ g x | g == f -> Right (Just x)
      HDel _ g | g == f -> Right Nothing
      HIncrBy _ g n | g == f -> Just . C.pack . show . (+ n) <$> number v
      Del _ -> Right Nothing
      op | hashOp op -> Right v
      op -> wrong op
    has f b = \case
      HSet _ g _ | g == f -> Right True
      HDel _ g | g == f -> Right False
      HIncrBy _ g _ | g == f -> Right True
      Del _ -> Right False
      op | hashOp op -> Right b
      op -> wrong op
    fields m = \case
      HSet _ g x -> Right (Map.insert g x m)
      HDel _ g -> Right (Map.delete g m)
      HIncrBy _ g n -> (\v -> Map.insert g (C.pack (show (v + n))) m) <$> number (Map.lookup g m)
      Del _ -> Right Map.empty
      op -> wrong op
    isIn x b = \case
      SAdd _ y | y == x -> Right True
      SRem _ y | y == x -> Right False
      Del _ -> Right False
      op | setOp op -> Right b
      op -> wrong op
    members s = \case
      SAdd _ y -> Right (Set.insert y s)
      SRem _ y -> Right (Set.delete y s)
      Del _ -> Right Set.empty
      op -> wrong op
    score x s = \case
      ZAdd _ v y | y == x -> Right (Just v)
      ZAddNX _ v y | y == x -> Right (Just (fromMaybe v s))
      ZRem _ y | y == x -> Right Nothing
      Del _ -> Right Nothing
      ZRemRangeByRank {} -> no "a score, of a sorted set that lost members by their rank"
      op | sortedOp op -> Right s
      op -> wrong op
    count _ = \case
      Del _ -> Right 0
      op | sortedOp op -> no "how many members a sorted set has after it was written to"
      op -> wrong op
    ranked lo hi m = \case
      ZAdd _ v y
        | within lo hi v -> Right (Map.insert y v m)
        | otherwise -> Right (Map.delete y m)
      ZRem _ y -> Right (Map.delete y m)
      Del _ -> Right Map.empty
      op | sortedOp op -> no ("a range, of a sorted set after " <> show op)
      op -> wrong op
    -- whether the key is there: known, or not known any more
    there known = \case
      Del _ -> Right (Just False)
      op | makes op -> Right (Just True)
      _ -> Right (if known == Just False then Just False else Nothing)

-- | Whether a score is between two bounds, both included.
within :: Bound -> Bound -> Score -> Bool
within lo hi v = above && below
  where
    above = case lo of
      MinusInf -> True
      PlusInf -> False
      At x -> v >= x
    below = case hi of
      PlusInf -> True
      MinusInf -> False
      At x -> v <= x

hashOp, setOp, sortedOp, makes :: Op -> Bool
hashOp = \case
  HSet {} -> True
  HDel {} -> True
  HIncrBy {} -> True
  _ -> False
setOp = \case
  SAdd {} -> True
  SRem {} -> True
  _ -> False
sortedOp = \case
  ZAdd {} -> True
  ZAddNX {} -> True
  ZRem {} -> True
  ZRemRangeByRank {} -> True
  _ -> False
makes = \case
  Set {} -> True
  Incr {} -> True
  HSet {} -> True
  HIncrBy {} -> True
  SAdd {} -> True
  ZAdd {} -> True
  ZAddNX {} -> True
  XAdd {} -> True
  _ -> False

-------------------------------------------------------------------------------
-- Decisions
-------------------------------------------------------------------------------

-- | What a decision asks of what runs it: the answers of Redis to
-- questions, all at once, and the clock of Redis in milliseconds.
data Backend m = Backend
  { fetch :: !([Query] -> m [Answer]),
    time :: !(m Integer)
  }

-- | What a decision has done so far, in one pass over it.
data Session = Session
  { -- | what Redis said, to every question that it was asked
    said :: !(Map Query Answer),
    -- | the questions that the decision asked, and so depends on
    used :: !(Set Query),
    -- | the questions that Redis was not asked yet: the pass goes on as if
    -- the answer to each were nothing, and is made again with the answers
    missed :: !(Set Query),
    -- | how many writes there are
    next :: !Int,
    -- | the writes to each key, each with its number among all
    byKey :: !(Map Key (Seq (Int, Op))),
    -- | the wipes, each with its number
    wipes :: !(Seq (Int, ByteString)),
    -- | every write, in order
    ops :: !(Seq Op),
    -- | the clock, once it was read
    clockAt :: !(Maybe Integer)
  }

-- | A decision about the state in Redis: a computation that asks ('query'),
-- writes ('write') and can do nothing else, whatever monad @m@ runs it.
-- 'runAtomic' gives what it decided: the questions that it depends on with
-- their answers, and its writes.
--
-- A decision is run more than once ('runAtomic'), so it must be the same
-- computation each time: it is, having nothing to differ by but its
-- answers.
newtype Atomic m a = Atomic (ReaderT (Backend m) (ExceptT Stop (StateT Session m)) a)
  deriving newtype (Functor, Applicative, Monad)

-- | The state of a decision.
session :: (Monad m) => StateT Session m a -> Atomic m a
session = Atomic . lift . lift

-- | Stop a decision.
stop :: (Monad m) => Stop -> Atomic m a
stop = Atomic . lift . throwE

-- | Something of what runs the decision.
backend :: (Monad m) => (Backend m -> m a) -> Atomic m a
backend f = Atomic (ask >>= lift . lift . lift . f)

-- | Why a decision stopped before its end.
newtype Stop = Cannot Unanswerable

-- | The clock of Redis, in milliseconds: read once in a decision, and the
-- same every time after. The script does not compare it, as it compares
-- answers: a decision may use it for what it writes, and must not let it
-- change what it decides.
clock :: (Monad m) => Atomic m Integer
clock =
  session (gets (.clockAt)) >>= \case
    Just t -> return t
    Nothing -> do
      t <- backend (.time)
      session (modify' (\s -> s {clockAt = Just t}))
      return t

-- | Name questions that the decision will probably ask, so that Redis is
-- asked them with the next that it has to be asked, in the same round
-- trip. The decision does not depend on them: an answer is compared by
-- the script only if the decision asks for it. It changes how many round
-- trips a decision takes, and nothing else.
prefetch :: (Monad m) => [Query] -> Atomic m ()
prefetch qs = session . modify' $ \s ->
  s {missed = foldl' (\m q -> if Map.member q s.said then m else Set.insert q m) s.missed qs}

-- | The answer of a Redis that has nothing: what a pass goes on with for a
-- question that Redis was not asked yet.
nothing :: Query -> Answer
nothing = \case
  Get _ -> Text Nothing
  HGet _ _ -> Text Nothing
  HExists _ _ -> Flag False
  HGetAll _ -> Fields Map.empty
  SIsMember _ _ -> Flag False
  SMembers _ -> Members Set.empty
  ZScore _ _ -> Scored Nothing
  ZCard _ -> Count 0
  ZRangeByScore {} -> Ranked []
  Exists _ -> Flag False

-- | Ask a question: what Redis says, as changed by what the decision wrote
-- since. The decision depends on what Redis said from here on.
query :: (Monad m) => Query -> Atomic m Answer
query q = do
  s <- session (gets id)
  let written = writesTo (queryKey q) s
  case Map.lookup q s.said of
    Just before -> do
      session (modify' (\x -> x {used = Set.insert q x.used}))
      either (stop . Cannot) return (replay q before written)
    -- Redis was not asked: this pass will not count, and goes on only to
    -- find what else there is to ask
    Nothing -> do
      session (modify' (\x -> x {missed = Set.insert q x.missed}))
      return (either (const (nothing q)) id (replay q (nothing q) written))

-- | 'query' several.
queries :: (Monad m) => [Query] -> Atomic m [Answer]
queries = mapM query

-- | The writes of a decision to a key, in order, a wipe that takes the key
-- being a 'Del' of it.
writesTo :: Key -> Session -> [Op]
writesTo k s = map snd (merge (toList own) wiped)
  where
    own = fromMaybe Seq.empty (Map.lookup k s.byKey)
    wiped = [(n, Del k) | (n, p) <- toList s.wipes, p `B.isPrefixOf` k]
    merge xs [] = xs
    merge [] ys = ys
    merge (x : xs) (y : ys)
      | fst x <= fst y = x : merge xs (y : ys)
      | otherwise = y : merge (x : xs) ys

-- | Add a write to those of the decision. Nothing is changed in Redis
-- until the decision ends and the script finds its answers still true.
write :: (Monad m) => Op -> Atomic m ()
write op = session . modify' $ \s ->
  let n = s.next
      s' = s {next = n + 1, ops = s.ops |> op}
   in case opKey op of
        Right k -> s' {byKey = Map.insertWith (flip (<>)) k (Seq.singleton (n, op)) s.byKey}
        Left p -> s' {wipes = s.wipes |> (n, p)}

-- | How a decision ended.
data Outcome a
  = -- | with a result, the questions that it depends on with what Redis
    -- said to each, its writes in order, and how many times Redis was
    -- asked
    Decided a ![(Query, Answer)] ![Op] !Int
  | -- | it asked what cannot be answered: a fault of the decision
    Stuck !Unanswerable

-- | Run a decision. Nothing is changed: what it decided is for the caller
-- to carry out, if the answers that it depends on are still true.
--
-- The decision is gone through as many times as it takes to know what it
-- asks. A pass that comes to a question that Redis was not asked goes on
-- as if the answer were nothing, to find what else it would ask; then
-- Redis is asked all of those at once, and the decision is gone through
-- again from its start. The last pass is the one that counts: every
-- answer in it is one that Redis gave, and what a pass before it made of
-- an answer that it did not have is dropped whole. So a decision that
-- asks a hundred questions asks Redis a few times and not a hundred, and
-- which questions go together is found, not written down.
--
-- It ends because each time Redis is asked at least one question that it
-- was not asked before, and a decision that ends asks finitely many.
runAtomic :: (Monad m) => Backend m -> Atomic m a -> m (Outcome a)
runAtomic with (Atomic f) = go Map.empty (0 :: Int)
  where
    go known rounds = do
      (r, s) <- runStateT (runExceptT (runReaderT f with)) (start known)
      if Set.null s.missed
        then return $ case r of
          Left (Cannot why) -> Stuck why
          Right a -> Decided a [(q, x) | q <- Set.toList s.used, Just x <- [Map.lookup q known]] (toList s.ops) rounds
        else do
          let new = Set.toList s.missed
          answers <- with.fetch new
          go (foldl' (\m (q, a) -> Map.insert q a m) known (zip new answers)) (rounds + 1)
    start known =
      Session
        { said = known,
          used = Set.empty,
          missed = Set.empty,
          next = 0,
          byKey = Map.empty,
          wipes = Seq.empty,
          ops = Seq.empty,
          clockAt = Nothing
        }

-------------------------------------------------------------------------------
-- Reading
-------------------------------------------------------------------------------

-- | An answer of another kind than its question, which 'replay' and the
-- backends never give.
mismatch :: (Monad m) => Query -> Atomic m a
mismatch q = stop (Cannot (Unanswerable q "an answer of another kind than the question"))

-- | @GET@.
get :: (Monad m) => Key -> Atomic m (Maybe ByteString)
get k =
  query (Get k) >>= \case
    Text v -> return v
    _ -> mismatch (Get k)

-- | @HGET@.
hget :: (Monad m) => Key -> ByteString -> Atomic m (Maybe ByteString)
hget k f =
  query (HGet k f) >>= \case
    Text v -> return v
    _ -> mismatch (HGet k f)

-- | @HEXISTS@.
hexists :: (Monad m) => Key -> ByteString -> Atomic m Bool
hexists k f =
  query (HExists k f) >>= \case
    Flag v -> return v
    _ -> mismatch (HExists k f)

-- | @HGETALL@.
hgetall :: (Monad m) => Key -> Atomic m (Map ByteString ByteString)
hgetall k =
  query (HGetAll k) >>= \case
    Fields v -> return v
    _ -> mismatch (HGetAll k)

-- | @SISMEMBER@.
sismember :: (Monad m) => Key -> ByteString -> Atomic m Bool
sismember k x =
  query (SIsMember k x) >>= \case
    Flag v -> return v
    _ -> mismatch (SIsMember k x)

-- | @SMEMBERS@.
smembers :: (Monad m) => Key -> Atomic m (Set ByteString)
smembers k =
  query (SMembers k) >>= \case
    Members v -> return v
    _ -> mismatch (SMembers k)

-- | @ZSCORE@.
zscore :: (Monad m) => Key -> ByteString -> Atomic m (Maybe Score)
zscore k x =
  query (ZScore k x) >>= \case
    Scored v -> return v
    _ -> mismatch (ZScore k x)

-- | @ZCARD@.
zcard :: (Monad m) => Key -> Atomic m Integer
zcard k =
  query (ZCard k) >>= \case
    Count v -> return v
    _ -> mismatch (ZCard k)

-- | @ZRANGEBYSCORE@, with the scores.
zrangeByScore :: (Monad m) => Key -> Bound -> Bound -> Maybe Int -> Atomic m [(ByteString, Score)]
zrangeByScore k lo hi limit =
  query (ZRangeByScore k lo hi limit) >>= \case
    Ranked v -> return v
    _ -> mismatch (ZRangeByScore k lo hi limit)

-- | @EXISTS@.
exists :: (Monad m) => Key -> Atomic m Bool
exists k =
  query (Exists k) >>= \case
    Flag v -> return v
    _ -> mismatch (Exists k)

-------------------------------------------------------------------------------
-- A model of Redis
-------------------------------------------------------------------------------

-- | What a key of Redis holds.
data Value
  = Bytes !ByteString
  | Hash !(Map ByteString ByteString)
  | Group !(Set ByteString)
  | Sorted !(Map ByteString Score)
  | -- | a stream: the fields of its entries, oldest first
    Stream !(Seq [(ByteString, ByteString)])
  deriving (Eq, Show)

-- | The keys of a Redis and what each holds. A key that holds nothing is
-- not there, as in Redis.
type Db = Map Key Value

-- | A Redis with nothing in it.
emptyDb :: Db
emptyDb = Map.empty

-- | What a Redis in this state says to a question. A question about a key
-- of another kind is answered as if the key were not there; the fleet asks
-- none.
answer :: Db -> Query -> Answer
answer db = \case
  Get k -> Text (case Map.lookup k db of Just (Bytes v) -> Just v; _ -> Nothing)
  HGet k f -> Text (Map.lookup f (hash k))
  HExists k f -> Flag (Map.member f (hash k))
  HGetAll k -> Fields (hash k)
  SIsMember k x -> Flag (Set.member x (group k))
  SMembers k -> Members (group k)
  ZScore k x -> Scored (Map.lookup x (sorted k))
  ZCard k -> Count (fromIntegral (Map.size (sorted k)))
  ZRangeByScore k lo hi limit ->
    Ranked . maybe id take limit . sortOn (\(x, s) -> (s, x)) $
      [(x, s) | (x, s) <- Map.toList (sorted k), within lo hi s]
  Exists k -> Flag (Map.member k db)
  where
    hash k = case Map.lookup k db of Just (Hash m) -> m; _ -> Map.empty
    group k = case Map.lookup k db of Just (Group s) -> s; _ -> Set.empty
    sorted k = case Map.lookup k db of Just (Sorted m) -> m; _ -> Map.empty

-- | A Redis after a write. A write to a key of another kind changes
-- nothing here, where Redis would refuse it; the fleet makes none.
perform :: Op -> Db -> Db
perform = \case
  Set k v -> Map.insert k (Bytes v)
  Del k -> Map.delete k
  Incr k -> Map.alter (Just . Bytes . C.pack . show . (+ 1) . maybe 0 numberOf) k
  HSet k f v -> onHash k (Map.insert f v)
  HDel k f -> onHash k (Map.delete f)
  HIncrBy k f n -> onHash k (\m -> Map.insert f (C.pack (show (maybe 0 integer (Map.lookup f m) + n))) m)
  SAdd k x -> onGroup k (Set.insert x)
  SRem k x -> onGroup k (Set.delete x)
  ZAdd k s x -> onSorted k (Map.insert x s)
  ZAddNX k s x -> onSorted k (Map.insertWith (\_ old -> old) x s)
  ZRem k x -> onSorted k (Map.delete x)
  ZRemRangeByRank k from to -> onSorted k (dropRanks from to)
  XAdd k keep fields -> Map.alter (Just . Stream . trim keep . (|> fields) . entries) k
  Wipe p -> Map.filterWithKey (\k _ -> not (p `B.isPrefixOf` k))
  where
    numberOf = \case
      Bytes v -> integer v
      _ -> 0
    integer v = maybe 0 fst (C.readInteger v)
    entries = \case
      Just (Stream es) -> es
      _ -> Seq.empty
    -- a stream keeps about so many: here, no more than that
    trim keep es = Seq.drop (Seq.length es - keep) es
    onHash k f = Map.alter (\v -> keepHash (f (case v of Just (Hash m) -> m; _ -> Map.empty))) k
    keepHash m = if Map.null m then Nothing else Just (Hash m)
    onGroup k f = Map.alter (\v -> keepGroup (f (case v of Just (Group s) -> s; _ -> Set.empty))) k
    keepGroup s = if Set.null s then Nothing else Just (Group s)
    onSorted k f = Map.alter (\v -> keepSorted (f (case v of Just (Sorted m) -> m; _ -> Map.empty))) k
    keepSorted m = if Map.null m then Nothing else Just (Sorted m)
    -- the members from one rank to another, both included, counted from 0
    -- and, below zero, from the end
    dropRanks from to m =
      let ranked = sortOn (\(x, s) -> (s, x)) (Map.toList m)
          n = fromIntegral (length ranked) :: Integer
          -- as Redis counts: a start before the first member is the
          -- first, and an end before it is no member at all
          a = if from < 0 then max 0 (n + from) else from
          b = min (n - 1) (if to < 0 then n + to else to)
          gone = [x | (i, (x, _)) <- zip [0 ..] ranked, i >= a, i <= b]
       in foldl' (flip Map.delete) m gone

-- | Run a decision against the model, and carry out what it decided: the
-- result and the model after it. Nothing else changes the model
-- meanwhile, so the answers are always still true. The clock is given.
runPure :: Integer -> (forall m. (Monad m) => Atomic m a) -> Db -> Either Unanswerable (a, Db)
runPure now decision db =
  case runIdentity (runAtomic model decision) of
    Decided a _ written _ -> Right (a, foldl' (flip perform) db written)
    Stuck why -> Left why
  where
    model :: Backend Identity
    model = Backend {fetch = return . map (answer db), time = return now}
