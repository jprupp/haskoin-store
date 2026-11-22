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
-- = What this does not give: a turn
--
-- A decision that loses the race is made again, and may lose again. One
-- that asks a few questions wins soon. One that asks tens of thousands,
-- over a second or more, while others write several times a second,
-- loses every time: it is right whenever it is carried out, and it never
-- is. Nothing here prevents that. A decision that reads much has to be
-- given the floor by the others, as the move of the tip is
-- ('Haskoin.Store.Fleet.Pool.Decide.floorHeld'), for as long as it takes
-- and not for a time that it is expected to take, and has to be tested
-- while others write and at the time that it takes on a slow machine.
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
    Ahead (..),
    ahead,
    write,
    clock,
    Outcome (..),
    runAtomic,
    tidy,

    -- * Reading
    get,
    hget,
    hexists,
    hgetall,
    hkeys,
    habove,
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
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.Foldable (toList)
import Data.Functor.Identity (Identity (..))
import Data.List (sortOn)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Sequence (Seq, (|>))
import Data.Sequence qualified as Seq
import Data.Set (Set)
import Data.Set qualified as Set
import Haskoin.Store.Fleet.Atomic.Cells
  ( CellOp (..),
    MapOp (..),
    RankOp (..),
    at,
    cellAfter,
    mapAfter,
    rankAfter,
    thereAt,
  )

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
  | -- | @HKEYS@
    HKeys !Key
  | -- | the fields of a hash whose value is a whole number above a bound.
    -- A decision that only needs to know which leases still run asks this,
    -- and so does not depend on when each runs out: a lease that is
    -- renewed is the same answer.
    HAbove !Key !Integer
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
  | -- | to 'SMembers' and 'HKeys'
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
  | -- | @INCRBY@
    IncrBy !Key !Integer
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
  HKeys k -> k
  HAbove k _ -> k
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
  IncrBy k _ -> Right k
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
-- What a write does to one cell, a field of a hash, a member of a set or
-- the score of one, is not said here: it is the function of
-- "Haskoin.Store.Fleet.Atomic.Cells" that the model of Redis below uses
-- too ('perform'). So for a question about one cell, the answer here is
-- what the model says after the writes, which Liquid Haskell checks
-- ('Haskoin.Store.Fleet.Atomic.Cells.cellAfterAll',
-- 'Haskoin.Store.Fleet.Atomic.Cells.thereAtAllSame'). For all the fields
-- of a hash and all the members of a set, the writes are done to the
-- answer by the function that does them to the model. For the rest, the
-- fields of a hash by name, a range or a count of a sorted set, whether a
-- key is there, the specs check it against the model on drawn cases, and
-- against a Redis.
replay :: Query -> Answer -> [Op] -> Either Unanswerable Answer
replay q before ops = case (q, before) of
  (Get _, Text v) -> Text <$> foldM' text v
  (HGet _ f, Text v) -> Text <$> foldM' (field f) v
  (HExists _ f, Flag b) -> Flag <$> foldM' (\x op -> (\o -> thereAt unmarked f o x) <$> kind hashOp op) b
  (HGetAll _, Fields m) -> Fields <$> foldM' fields m
  (HKeys _, Members s) -> Members . Map.keysSet <$> foldM' (those nameOp) (Map.fromSet (const ()) s)
  (HAbove _ _, Members _)
    | null ops -> Right before
    | otherwise -> no "which numbers of a hash are above a bound, after the hash was written to"
  (SIsMember _ x, Flag b) -> Flag <$> foldM' (\y op -> (\o -> thereAt unmarked x o y) <$> kind groupOp op) b
  (SMembers _, Members s) -> Members . Map.keysSet <$> foldM' (those groupOp) (Map.fromSet (const ()) s)
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
    -- a write as one to a key of some kind, or it is to a key of another
    kind :: (Op -> Maybe o) -> Op -> Either Unanswerable o
    kind as op = maybe (wrong op) Right (as op)
    -- Redis adds to a number only: a decision that adds to anything else
    -- would have its writes refused half way, so it is stopped here
    adds :: Maybe ByteString -> Either Unanswerable ()
    adds v = if maybe True numeric v then Right () else no "a number, of a value that is none"
    text v op =
      kind textOp op >>= \c -> case c of
        Plus _ -> cellAfter addTo c v <$ adds v
        _ -> Right (cellAfter addTo c v)
    field f v op =
      kind hashOp op >>= \o -> case o of
        On g (Plus _) | g == f -> at unmarked (cellAfter addTo) f o v <$ adds v
        _ -> Right (at unmarked (cellAfter addTo) f o v)
    fields m op =
      kind hashOp op >>= \o -> case o of
        On g (Plus _) -> mapAfter unmarked (cellAfter addTo) o m <$ adds (Map.lookup g m)
        _ -> Right (mapAfter unmarked (cellAfter addTo) o m)
    -- the names of what a hash or a set holds, as a map of cells that
    -- hold nothing
    those :: (Op -> Maybe (Within (CellOp ()))) -> Map ByteString () -> Op -> Either Unanswerable (Map ByteString ())
    those as m op = (\o -> mapAfter unmarked (cellAfter nothingTo) o m) <$> kind as op
    score x s = \case
      ZRemRangeByRank {} -> no "a score, of a sorted set that lost members by their rank"
      op -> (\o -> at unmarked rankAfter x o s) <$> kind rankOp op
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

-- | A write to a map inside what a key holds: to a field of a hash, a
-- member of a set or of a sorted set, or to all of them. Nothing marks
-- those, as the start of a key marks the keys that a wipe takes.
type Within c = MapOp () ByteString c

-- | No field or member is marked.
unmarked :: () -> ByteString -> Bool
unmarked _ _ = False

-- | Add to the number that bytes say, as @INCRBY@ does, or to 0 for none.
addTo :: Integer -> Maybe ByteString -> ByteString
addTo n v = C.pack (show (maybe 0 (maybe 0 fst . C.readInteger) v + n))

-- | What adding gives a cell that holds nothing: nothing.
nothingTo :: Integer -> Maybe () -> ()
nothingTo _ _ = ()

-- | Whether bytes are a whole number, all of them.
numeric :: ByteString -> Bool
numeric v = case C.readInteger v of
  Just (_, rest) -> B.null rest
  Nothing -> False

-- | A write to a key that holds bytes, as a write to one cell.
textOp :: Op -> Maybe (CellOp ByteString)
textOp = \case
  Set _ x -> Just (Put x)
  Del _ -> Just Clear
  IncrBy _ n -> Just (Plus n)
  _ -> Nothing

-- | A write to a hash, as a write to the map of its fields.
hashOp :: Op -> Maybe (Within (CellOp ByteString))
hashOp = \case
  HSet _ f x -> Just (On f (Put x))
  HDel _ f -> Just (On f Clear)
  HIncrBy _ f n -> Just (On f (Plus n))
  Del _ -> Just All
  _ -> Nothing

-- | A write to a hash, as a write to the names of its fields.
nameOp :: Op -> Maybe (Within (CellOp ()))
nameOp = \case
  HSet _ f _ -> Just (On f (Put ()))
  HDel _ f -> Just (On f Clear)
  HIncrBy _ f _ -> Just (On f (Put ()))
  Del _ -> Just All
  _ -> Nothing

-- | A write to a set, as a write to the map of its members.
groupOp :: Op -> Maybe (Within (CellOp ()))
groupOp = \case
  SAdd _ x -> Just (On x (Put ()))
  SRem _ x -> Just (On x Clear)
  Del _ -> Just All
  _ -> Nothing

-- | A write to a sorted set that changes the score of one member or takes
-- all of them, as a write to the map of its scores. One that takes
-- members by their rank is not one of those.
rankOp :: Op -> Maybe (Within (RankOp Score))
rankOp = \case
  ZAdd _ s x -> Just (On x (Rank s))
  ZAddNX _ s x -> Just (On x (RankNew s))
  ZRem _ x -> Just (On x Unrank)
  Del _ -> Just All
  _ -> Nothing

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

sortedOp, makes :: Op -> Bool
sortedOp = \case
  ZAdd {} -> True
  ZAddNX {} -> True
  ZRem {} -> True
  ZRemRangeByRank {} -> True
  _ -> False
makes = \case
  Set {} -> True
  IncrBy {} -> True
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
    -- | of those, the writes to one cell of a key: a field of a hash, a
    -- member of a set or of a sorted set
    byCell :: !(Map (Key, ByteString) (Seq (Int, Op))),
    -- | and those to all the cells of a key at once
    byWhole :: !(Map Key (Seq (Int, Op))),
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
-- same every time after, however many times Redis is asked before the
-- decision is made. The script does not compare it, as it compares
-- answers, and it is earlier than the script: by a moment for most
-- decisions, and by ten seconds and more for a move of the tip over a
-- full block.
--
-- So a decision may use it for what it writes, and may judge by it only
-- what stays true as time goes on, or what the time since does not matter
-- to: that a lease has run out, or that a move went quiet for ten
-- seconds, is as true later; that a lease has not run out yet is what a
-- script that ran when the decision began would have found.
--
-- It was read again each time Redis was asked, once. A move of the tip
-- that Redis was asked about for ten seconds then judged the leases by a
-- clock ten seconds after its store renewed its own, found no store
-- live, and was refused, turn after turn.
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

-- | A question to name ahead, with those that its answer names: what a
-- decision will ask of the children of a transaction is known only once
-- Redis has said which they are.
data Ahead = Ahead !Query (Answer -> [Ahead])

-- | Name questions ahead in steps: these, and, of each that Redis has
-- answered, those that its answer names, and so on. It says whether Redis
-- has answered all of them.
--
-- = Why
--
-- A decision is gone through from its start each time Redis is asked
-- ('runAtomic'). 'prefetch' is for what a decision can name before it
-- asks anything. What it asks next, of what the first answers name, cost
-- a whole pass for each step of such a chain: the move of the tip over a
-- block of four thousand transactions was gone through five times, three
-- of them to ask a few hundred questions each about the children of its
-- transactions.
--
-- = What a decision may do with the answer
--
-- It may leave a pass in which this says no, with any result: Redis has
-- not answered a question that was named here, so the pass does not
-- count, and is gone through again once Redis has. So the steps of a
-- chain cost a question each to Redis and not a pass each over the whole
-- decision. In the pass that counts Redis has answered everything, and
-- this says yes.
--
-- The answers that the steps are worked out from are what Redis said and
-- go nowhere else: the decision depends on a question only by asking it
-- ('query'), and the script compares only those. Naming too little costs
-- a pass, and too much costs its answer.
ahead :: (Monad m) => [Ahead] -> Atomic m Bool
ahead hints = do
  s <- session (gets id)
  let go ready missing = \case
        [] -> (ready, missing)
        Ahead q next : rest -> case Map.lookup q s.said of
          Just a -> go ready missing (next a <> rest)
          Nothing -> go False (Set.insert q missing) rest
      (answered, missed) = go True s.missed hints
  session (modify' (\x -> x {missed}))
  return answered

-- | The answer of a Redis that has nothing: what a pass goes on with for a
-- question that Redis was not asked yet.
nothing :: Query -> Answer
nothing = \case
  Get _ -> Text Nothing
  HGet _ _ -> Text Nothing
  HExists _ _ -> Flag False
  HGetAll _ -> Fields Map.empty
  HKeys _ -> Members Set.empty
  HAbove _ _ -> Members Set.empty
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
  let written = writesTo q s
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

-- | The writes of a decision that a question has to go through, in
-- order, a wipe that takes the key being a 'Del' of it.
--
-- For a question about one cell, a field of a hash, a member of a set or
-- the score of a member, those are the writes to that cell and the writes
-- to all the cells of the key: the writes to its other cells change
-- nothing for it ('Haskoin.Store.Fleet.Atomic.Cells.skipOthers'). A
-- decision that takes thousands of transactions out of the pool writes to
-- thousands of fields of one hash, and asks about each: going through all
-- the writes to the key for each question took time that grew with the
-- square of the block. For any other question they are all the writes to
-- its key.
writesTo :: Query -> Session -> [Op]
writesTo q s = map snd (merge (toList own) wiped)
  where
    k = queryKey q
    own = case cellOf q of
      Just c -> mergeSeq (found (k, c) s.byCell) (found k s.byWhole)
      Nothing -> found k s.byKey
    found :: (Ord a) => a -> Map a (Seq (Int, Op)) -> Seq (Int, Op)
    found x = fromMaybe Seq.empty . Map.lookup x
    mergeSeq a b = Seq.fromList (merge (toList a) (toList b))
    wiped = [(n, Del k) | (n, p) <- toList s.wipes, p `B.isPrefixOf` k]
    merge xs [] = xs
    merge [] ys = ys
    merge (x : xs) (y : ys)
      | fst x <= fst y = x : merge xs (y : ys)
      | otherwise = y : merge (x : xs) ys

-- | The cell that a question is about, if it is about one.
cellOf :: Query -> Maybe ByteString
cellOf = \case
  HGet _ f -> Just f
  HExists _ f -> Just f
  SIsMember _ x -> Just x
  ZScore _ x -> Just x
  _ -> Nothing

-- | The cell that a write is to, if it is to one; nothing for a write to
-- all the cells of its key, or to a key that has none.
cellOfOp :: Op -> Maybe ByteString
cellOfOp = \case
  HSet _ f _ -> Just f
  HDel _ f -> Just f
  HIncrBy _ f _ -> Just f
  SAdd _ x -> Just x
  SRem _ x -> Just x
  ZAdd _ _ x -> Just x
  ZAddNX _ _ x -> Just x
  ZRem _ x -> Just x
  _ -> Nothing

-- | Add a write to those of the decision. Nothing is changed in Redis
-- until the decision ends and the script finds its answers still true.
write :: (Monad m) => Op -> Atomic m ()
write op = session . modify' $ \s ->
  let n = s.next
      s' = s {next = n + 1, ops = s.ops |> op}
   in case opKey op of
        Right k ->
          let one = Seq.singleton (n, op)
              s'' = s' {byKey = Map.insertWith (flip (<>)) k one s.byKey}
           in case cellOfOp op of
                Just c -> s'' {byCell = Map.insertWith (flip (<>)) (k, c) one s.byCell}
                Nothing -> s'' {byWhole = Map.insertWith (flip (<>)) k one s.byWhole}
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
runAtomic with (Atomic f) = go Map.empty (0 :: Int) Nothing
  where
    -- with the clock that an earlier pass read, if one did: the clock of
    -- a decision is read once, and not once in each pass
    go known rounds before = do
      (r, s) <- runStateT (runExceptT (runReaderT f with)) (start known before)
      if Set.null s.missed
        then return $ case r of
          Left (Cannot why) -> Stuck why
          Right a -> Decided a [(q, x) | q <- Set.toList s.used, Just x <- [Map.lookup q known]] (toList s.ops) rounds
        else do
          let new = Set.toList s.missed
          answers <- with.fetch new
          go (foldl' (\m (q, a) -> Map.insert q a m) known (zip new answers)) (rounds + 1) s.clockAt
    start known before =
      Session
        { said = known,
          used = Set.empty,
          missed = Set.empty,
          next = 0,
          byKey = Map.empty,
          byCell = Map.empty,
          byWhole = Map.empty,
          wipes = Seq.empty,
          ops = Seq.empty,
          clockAt = before
        }

-- | The writes of a decision without those that a later one undoes: a
-- write to a key that is deleted afterwards, or wiped, changes nothing in
-- the end. Redis is the same after the writes that are left, in their
-- order, as after all of them.
--
-- A decision writes as the script before it did, one thing at a time: a
-- transaction that leaves the pool takes five counts off the summary of
-- an address, and then the summary is deleted, since nothing is left of
-- it. For a block that confirms thousands of transactions, most of what
-- was written was undone a few writes later, and all of it was sent to
-- Redis.
tidy :: [Op] -> [Op]
tidy written = snd (foldl' keep ((Set.empty, []), []) (reverse written))
  where
    -- from the last write to the first, with the keys that a later write
    -- deletes and the starts of those that one wipes; what is kept is put
    -- in front of what was kept after it, and so comes out in its order
    keep ((gone, wiped), kept) op = case opKey op of
      Left p -> ((gone, p : wiped), op : kept)
      Right k
        | Set.member k gone || any (`B.isPrefixOf` k) wiped -> ((gone, wiped), kept)
        | Del _ <- op -> ((Set.insert k gone, wiped), op : kept)
        | otherwise -> ((gone, wiped), op : kept)

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

-- | @HKEYS@.
hkeys :: (Monad m) => Key -> Atomic m (Set ByteString)
hkeys k =
  query (HKeys k) >>= \case
    Members v -> return v
    _ -> mismatch (HKeys k)

-- | The fields of a hash whose value is a whole number above a bound.
habove :: (Monad m) => Key -> Integer -> Atomic m (Set ByteString)
habove k n =
  query (HAbove k n) >>= \case
    Members v -> return v
    _ -> mismatch (HAbove k n)

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
  | -- | a set: its members
    Group !(Map ByteString ())
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
  HGet k f -> Text (Map.lookup f (hash (Map.lookup k db)))
  HExists k f -> Flag (Map.member f (hash (Map.lookup k db)))
  HGetAll k -> Fields (hash (Map.lookup k db))
  HKeys k -> Members (Map.keysSet (hash (Map.lookup k db)))
  HAbove k n -> Members (Map.keysSet (Map.filter (maybe False ((> n) . fst) . C.readInteger) (hash (Map.lookup k db))))
  SIsMember k x -> Flag (Map.member x (group (Map.lookup k db)))
  SMembers k -> Members (Map.keysSet (group (Map.lookup k db)))
  ZScore k x -> Scored (Map.lookup x (sorted (Map.lookup k db)))
  ZCard k -> Count (fromIntegral (Map.size (sorted (Map.lookup k db))))
  ZRangeByScore k lo hi limit ->
    Ranked . maybe id take limit . sortOn (\(x, s) -> (s, x)) $
      [(x, s) | (x, s) <- Map.toList (sorted (Map.lookup k db)), within lo hi s]
  Exists k -> Flag (Map.member k db)

-- | The fields of what a key holds, if it is a hash, the members if it is
-- a set, and the scores if it is a sorted set: none for a key that is not
-- there or holds something else.
hash :: Maybe Value -> Map ByteString ByteString
hash = \case
  Just (Hash m) -> m
  _ -> Map.empty

group :: Maybe Value -> Map ByteString ()
group = \case
  Just (Group m) -> m
  _ -> Map.empty

sorted :: Maybe Value -> Map ByteString Score
sorted = \case
  Just (Sorted m) -> m
  _ -> Map.empty

-- | A Redis after a write: the map of its keys after a write to the cell
-- of one key, or to the cells of the keys that start with some bytes
-- ('Haskoin.Store.Fleet.Atomic.Cells.mapAfter').
perform :: Op -> Db -> Db
perform op = mapAfter B.isPrefixOf holds route
  where
    route = case opKey op of
      Right k -> On k op
      Left p -> Under p

-- | What a key holds after a write to it, from what it held: the cell of
-- the key. A hash, a set and a sorted set are maps of cells in turn, and
-- change by the same function, each with the function of its cells; one
-- that is left with nothing is no key any more, as in Redis.
--
-- A write to a key that holds something of another kind changes nothing
-- here, where Redis would refuse it; the fleet makes none.
holds :: Op -> Maybe Value -> Maybe Value
holds op v = case (op, v) of
  (Del _, _) -> Nothing
  (Wipe _, _) -> Nothing
  (_, Just (Bytes x)) | Just c <- textOp op -> Bytes <$> cellAfter addTo c (Just x)
  (_, Nothing) | Just c <- textOp op -> Bytes <$> cellAfter addTo c Nothing
  (_, Just (Hash m)) | Just o <- hashOp op -> kept Hash (mapAfter unmarked (cellAfter addTo) o m)
  (_, Nothing) | Just o <- hashOp op -> kept Hash (mapAfter unmarked (cellAfter addTo) o Map.empty)
  (_, Just (Group m)) | Just o <- groupOp op -> kept Group (mapAfter unmarked (cellAfter nothingTo) o m)
  (_, Nothing) | Just o <- groupOp op -> kept Group (mapAfter unmarked (cellAfter nothingTo) o Map.empty)
  (_, Just (Sorted m)) | Just o <- rankOp op -> kept Sorted (mapAfter unmarked rankAfter o m)
  (_, Nothing) | Just o <- rankOp op -> kept Sorted (mapAfter unmarked rankAfter o Map.empty)
  (ZRemRangeByRank _ from to, Just (Sorted m)) -> kept Sorted (dropRanks from to m)
  (XAdd _ keep fields, Just (Stream es)) -> Just (Stream (trim keep (es |> fields)))
  (XAdd _ keep fields, Nothing) -> Just (Stream (trim keep (Seq.singleton fields)))
  _ -> v
  where
    kept con m = if Map.null m then Nothing else Just (con m)
    -- a stream keeps about so many: here, no more than that
    trim keep es = Seq.drop (Seq.length es - keep) es
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
