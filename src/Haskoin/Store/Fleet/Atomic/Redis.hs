{-# LANGUAGE TemplateHaskell #-}

-- |
-- Module      : Haskoin.Store.Fleet.Atomic.Redis
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Decisions of "Haskoin.Store.Fleet.Atomic", run against a Redis:
-- 'transact' asks Redis what a decision asks, and gives the script
-- (@lua\/atomic.lua@) what it decided, to carry out if the answers are
-- still true.
--
-- What a decision is compared against is what Redis said, byte for byte,
-- in the script's own words: the script writes each answer as one string,
-- when it is asked and again when it compares, and this module keeps the
-- string and gives it back. So nothing here has to write an answer as the
-- script would, and the two cannot differ on how.
--
-- A decision that asks nothing more than once, and writes nothing, is not
-- given to the script: all that it knows, Redis said in one call, and so
-- at one moment.
--
-- = A decision of any size
--
-- A move of the tip over a full block asks tens of thousands of questions
-- and writes hundreds of thousands of words. Redis takes 1,048,576
-- arguments in a command and closes the connection of one with more, so
-- questions and writes go a thousand to an argument, each argument a
-- list in MessagePack that the script unpacks when it comes to it
-- (@packed@). The writes are sent without those that a later delete
-- undoes ('tidy'), and those to one key that follow each other as one
-- command with all their members (@commands@): what Redis has after them
-- is the same.
module Haskoin.Store.Fleet.Atomic.Redis
  ( transact,
    transactTelling,
    insist,
    Failure (..),
    atomicScript,

    -- * In two steps
    Planned (..),
    plan,
    commit,
    carry,

    -- * How questions, answers and writes are put
    asked,
    command,
    readAnswer,
    Told (..),
    told,
  )
where

import Control.Applicative ((<|>))
import Control.Exception (Exception, throwIO)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Builder qualified as BB
import Data.ByteString.Char8 qualified as C
import Data.ByteString.Lazy qualified as BL
import Data.FileEmbed (embedFile, makeRelativeToProject)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Database.Redis (Connection, Redis, Reply, runRedis)
import Haskoin.Store.Fleet.Atomic
import Haskoin.Store.Fleet.Script (Script, evalScript, script)
import Numeric (showFFloat)
import Text.Read (readMaybe)

-- | Why a decision was not carried out.
data Failure
  = -- | Redis answered with an error, or with what cannot be read
    Unheard !String
  | -- | the decision asked what cannot be answered: a fault of the decision
    Faulty !Unanswerable
  deriving (Show)

instance Exception Failure

-- | The script that asks and that compares and writes.
atomicScript :: Script
atomicScript = script $(makeRelativeToProject "src/Haskoin/Store/Fleet/lua/atomic.lua" >>= embedFile)
{-# NOINLINE atomicScript #-}

-- | Make a decision and carry it out, as one step: the result of the
-- decision, or 'Nothing' if other stores changed what it asked about
-- every time, as many times as it may be made. Throws a t'Failure' if
-- Redis does not answer or the decision is at fault; nothing was written
-- then.
--
-- The decision is made again from new answers each time: what it decided
-- from answers that are no longer true is dropped whole.
transact :: Connection -> Int -> (forall m. (Monad m) => Atomic m a) -> IO (Maybe a)
transact conn tries = transactTelling conn tries (\_ -> return ())

-- | 'transact', telling of each time that the decision was not carried
-- out which question has another answer now: the first of them, in the
-- order in which the decision asked.
--
-- A decision that is refused says nothing of why, and one that is refused
-- time after time has a cause: a move of the tip was refused for hours on
-- a server, and which of its sixty thousand answers had changed had to be
-- guessed.
transactTelling ::
  Connection ->
  Int ->
  (Query -> IO ()) ->
  (forall m. (Monad m) => Atomic m a) ->
  IO (Maybe a)
transactTelling conn tries tell decision
  | tries <= 0 = return Nothing
  | otherwise = do
      planned <- plan conn decision
      carry conn planned >>= \case
        Right () -> return (Just planned.result)
        Left changed -> do
          tell changed
          transactTelling conn (tries - 1) tell decision

-- | Carry out a decision that has to be: one that other stores keep
-- changing the ground of is decided again, a hundred times before this
-- gives up with an exception. Nothing was written then.
insist :: Connection -> (forall m. (Monad m) => Atomic m a) -> IO a
insist conn decision =
  transact conn 100 decision
    >>= maybe (throwIO (Unheard "The fleet changed under a decision a hundred times")) return

-- | What a decision decided, from what Redis said: nothing is changed yet.
data Planned a = Planned
  { result :: a,
    -- | the questions that the decision depends on, each with what the
    -- script wrote for its answer
    guards :: ![(Query, ByteString)],
    writes :: ![Op],
    -- | how many times Redis was asked: with one, all that the decision
    -- knows Redis said at one moment
    rounds :: !Int
  }

-- | Make a decision, and change nothing. Throws a t'Failure' if Redis does
-- not answer or the decision is at fault.
plan :: Connection -> (forall m. (Monad m) => Atomic m a) -> IO (Planned a)
plan conn decision = do
  heard <- newIORef Map.empty
  now <- newIORef Nothing
  -- The clock of a decision is that of Redis when the decision first
  -- asked it anything, whenever the decision first reads the clock
  -- ('clock').
  let fetch qs = do
        (t, raws) <- reading conn qs
        modifyIORef' now (<|> Just t)
        modifyIORef' heard (\m -> foldr (uncurry Map.insert) m (zip qs raws))
        either (throwIO . Unheard) return (mapM (uncurry readAnswer) (zip qs raws))
      time =
        readIORef now >>= \case
          Just t -> return t
          Nothing -> do
            t <- fst <$> reading conn []
            modifyIORef' now (<|> Just t)
            return t
  runAtomic Backend {fetch, time} decision >>= \case
    Stuck why -> throwIO (Faulty why)
    Decided result depends writes rounds -> do
      raws <- readIORef heard
      let guards = [(q, raw) | (q, _) <- depends, Just raw <- [Map.lookup q raws]]
      return Planned {result, guards, writes, rounds}

-- | Carry out what a decision decided, if the answers that it depends on
-- are still true: whether it was carried out. If not, nothing changed.
commit :: Connection -> Planned a -> IO Bool
commit conn planned = either (const False) (const True) <$> carry conn planned

-- | 'commit', saying which question has another answer now if it was not
-- carried out: the first of them.
carry :: Connection -> Planned a -> IO (Either Query ())
carry conn planned
  | null planned.writes && planned.rounds <= 1 = return (Right ())
  | otherwise =
      applying conn planned.guards planned.writes >>= \case
        Nothing -> return (Right ())
        Just n -> case drop (n - 1) planned.guards of
          (q, _) : _ | n >= 1 -> return (Left q)
          _ -> throwIO (Unheard ("The script said that answer " <> show n <> " changed, of " <> counted))
  where
    counted = show (length planned.guards)

-- | Ask the script questions: the clock of Redis and the answers, each as
-- the script writes it.
--
-- The questions go as a few arguments, a thousand to each ('packed'). As
-- an argument each, with their words, the move of the tip over one block
-- of Bitcoin came to 1,008,352 arguments, and Redis closes the connection
-- of a command with more than 1,048,576.
reading :: Connection -> [Query] -> IO (Integer, [ByteString])
reading conn qs =
  run conn (evalScript atomicScript [] ("read" : map packed (pieces 1000 asking))) >>= \case
    t : raws
      | length raws == length qs, Just (ms, rest) <- C.readInteger t, B.null rest -> return (ms, raws)
    other -> throwIO (Unheard ("The script answered " <> show (length other) <> " words to " <> show (length qs)))
  where
    asking = map (several . map word . asked) qs

-- | Give the script the answers that a decision depends on and its writes:
-- nothing if it carried them out, and otherwise the number, from 1, of
-- the first answer that is another now.
--
-- They go a thousand to an argument, the answers first, and the script
-- unpacks one argument at a time: unpacked all at once, the two million
-- words of a large block were more than a Redis of the tests had memory
-- for. The writes are those that are not undone by a later one ('tidy'),
-- as few commands as say them ('commands').
applying :: Connection -> [(Query, ByteString)] -> [Op] -> IO (Maybe Int)
applying conn guards writes =
  run conn (evalScript atomicScript [] args) >>= \case
    ["done"] -> return Nothing
    ["raced", n] | Just (i, rest) <- C.readInt n, B.null rest -> return (Just i)
    other -> throwIO (Unheard ("The script answered " <> show (other :: [ByteString])))
  where
    guarded = pieces 1000 [several (word raw : map word (asked q)) | (q, raw) <- guards]
    written = pieces 1000 (map (several . map word) (commands (tidy writes)))
    args = "apply" : num (length guarded) : map packed (guarded <> written)

-- | So many at a time.
pieces :: Int -> [a] -> [[a]]
pieces _ [] = []
pieces n xs = let (a, b) = splitAt n xs in a : pieces n b

-- | Writes as commands of Redis, as few as say them: the writes to one key
-- that are of one kind and follow each other are one command with all
-- their members, five hundred at most, where Redis has such a command.
--
-- The writes to a key stay in their order. Those to different keys change
-- places, which changes nothing: each is to its own key. A wipe is to many
-- keys, so nothing changes places across one.
commands :: [Op] -> [[ByteString]]
commands = concatMap stretch . splitOn
  where
    -- the writes between two wipes, and each wipe by itself
    splitOn ops = case break wipe ops of
      (before, w : after) -> before : [w] : splitOn after
      (before, []) -> [before]
    wipe = \case
      Wipe _ -> True
      _ -> False
    stretch ops = concatMap merged (Map.elems (byKey ops))
    -- the writes to each key, in their order, the keys in the order in
    -- which they are first written to
    byKey ops =
      let add (order, m) op = case opKey op of
            Right k -> case Map.lookup k order of
              Just n -> (order, Map.adjust (op :) n m)
              Nothing -> let n = Map.size order in (Map.insert k n order, Map.insert n [op] m)
            Left _ -> let n = Map.size order in (Map.insert (C.pack ('!' : show n)) n order, Map.insert n [op] m)
       in Map.map reverse (snd (foldl add (Map.empty, Map.empty :: Map.Map Int [Op]) ops))
    merged [] = []
    merged (op : ops) = case op of
      HSet k f v -> run' (\case HSet _ g x -> Just [g, x]; _ -> Nothing) ["HSET", k] [f, v] ops
      HDel k f -> run' (\case HDel _ g -> Just [g]; _ -> Nothing) ["HDEL", k] [f] ops
      SAdd k x -> run' (\case SAdd _ y -> Just [y]; _ -> Nothing) ["SADD", k] [x] ops
      SRem k x -> run' (\case SRem _ y -> Just [y]; _ -> Nothing) ["SREM", k] [x] ops
      ZRem k x -> run' (\case ZRem _ y -> Just [y]; _ -> Nothing) ["ZREM", k] [x] ops
      ZAdd k sc x -> run' (\case ZAdd _ t y -> Just [scoreText t, y]; _ -> Nothing) ["ZADD", k] [scoreText sc, x] ops
      _ -> command op : merged ops
    -- a command with the members of the writes of its kind that follow
    run' same start first ops =
      let more = takeWhile' same ops
          rest = drop (length more) ops
       in [start <> concat some | some <- pieces 500 (first : more)] <> merged rest
    takeWhile' same = \case
      op : ops | Just x <- same op -> x : takeWhile' same ops
      _ -> []

-- | A list or bytes, to send as one argument.
data Packed = Listed' [Packed] | Bytes'' !ByteString

several :: [Packed] -> Packed
several = Listed'

word :: ByteString -> Packed
word = Bytes''

-- | Lists of lists of bytes as one string, in MessagePack, which the
-- script unpacks with the library that Redis gives its scripts: a list
-- as an array, and bytes as a raw string of the first version of the
-- format, which every version of that library reads.
packed :: [Packed] -> ByteString
packed = BL.toStrict . BB.toLazyByteString . one . Listed'
  where
    one = \case
      Listed' xs -> array (length xs) <> foldMap one xs
      Bytes'' b -> raw (B.length b) <> BB.byteString b
    array n
      | n < 16 = BB.word8 (0x90 + fromIntegral n)
      | n < 65536 = BB.word8 0xdc <> BB.word16BE (fromIntegral n)
      | otherwise = BB.word8 0xdd <> BB.word32BE (fromIntegral n)
    raw n
      | n < 32 = BB.word8 (0xa0 + fromIntegral n)
      | n < 65536 = BB.word8 0xda <> BB.word16BE (fromIntegral n)
      | otherwise = BB.word8 0xdb <> BB.word32BE (fromIntegral n)

run :: Connection -> Redis (Either Reply a) -> IO a
run conn f = runRedis conn f >>= either (throwIO . Unheard . show) return

num :: Int -> ByteString
num = C.pack . show

-- | A question as the script takes it: the shape of its answer, and the
-- words of its command.
asked :: Query -> [ByteString]
asked q = shape : ws
  where
    (shape, ws) = case q of
      Get k -> ("p", ["GET", k])
      HGet k f -> ("p", ["HGET", k, f])
      HExists k f -> ("p", ["HEXISTS", k, f])
      HGetAll k -> ("h", ["HGETALL", k])
      HKeys k -> ("u", ["HKEYS", k])
      HAbove k n -> ("k", [C.pack (show n), "HGETALL", k])
      SIsMember k x -> ("p", ["SISMEMBER", k, x])
      SMembers k -> ("u", ["SMEMBERS", k])
      ZScore k x -> ("p", ["ZSCORE", k, x])
      ZCard k -> ("p", ["ZCARD", k])
      ZRangeByScore k lo hi limit ->
        ( "p",
          ["ZRANGEBYSCORE", k, bound lo, bound hi, "WITHSCORES"]
            <> maybe [] (\n -> ["LIMIT", "0", num n]) limit
        )
      Exists k -> ("p", ["EXISTS", k])
    bound = \case
      MinusInf -> "-inf"
      PlusInf -> "+inf"
      At s -> scoreText s

-- | A write as the script takes it: the words of one command of Redis, or
-- @!WIPE@ and the start of the keys to delete.
command :: Op -> [ByteString]
command = \case
  Set k v -> ["SET", k, v]
  Del k -> ["DEL", k]
  IncrBy k n -> ["INCRBY", k, C.pack (show n)]
  HSet k f v -> ["HSET", k, f, v]
  HDel k f -> ["HDEL", k, f]
  HIncrBy k f n -> ["HINCRBY", k, f, C.pack (show n)]
  SAdd k x -> ["SADD", k, x]
  SRem k x -> ["SREM", k, x]
  ZAdd k s x -> ["ZADD", k, scoreText s, x]
  ZAddNX k s x -> ["ZADD", k, "NX", scoreText s, x]
  ZRem k x -> ["ZREM", k, x]
  ZRemRangeByRank k from to -> ["ZREMRANGEBYRANK", k, C.pack (show from), C.pack (show to)]
  XAdd k keep fields -> ["XADD", k, "MAXLEN", "~", num keep, "*"] <> concat [[f, v] | (f, v) <- fields]
  Wipe p -> ["!WIPE", p]

-- | A score as Redis takes it. The scores of the fleet are whole numbers,
-- written without a fraction or an exponent.
scoreText :: Score -> ByteString
scoreText s
  | s == fromIntegral whole && abs s < 1e17 = C.pack (show whole)
  | otherwise = C.pack (showFFloat Nothing s "")
  where
    whole = round s :: Integer

-- | What the script says: nothing, a number, bytes, or a list.
data Told = None | Number !Integer | Bytes' !ByteString | Listed ![Told]
  deriving (Eq, Show)

-- | Read what the script wrote for an answer, all of it.
told :: ByteString -> Either String Told
told raw = case one raw of
  Right (t, rest) | B.null rest -> Right t
  Right _ -> Left "more than an answer"
  Left e -> Left e
  where
    one s = case C.uncons s of
      Just ('n', rest) -> Right (None, rest)
      Just ('i', rest)
        | Just (n, more) <- C.readInteger rest,
          Just (';', after) <- C.uncons more ->
            Right (Number n, after)
      Just ('s', rest)
        | Just (n, more) <- C.readInt rest,
          Just (':', after) <- C.uncons more,
          B.length after >= n ->
            Right (Bytes' (B.take n after), B.drop n after)
      Just ('a', rest) | Just (n, more) <- C.readInt rest, Just (';', after) <- C.uncons more -> many n after []
      _ -> Left ("an answer that cannot be read: " <> show (B.take 40 s))
    many :: Int -> ByteString -> [Told] -> Either String (Told, ByteString)
    many 0 s acc = Right (Listed (reverse acc), s)
    many n s acc = one s >>= \(t, rest) -> many (n - 1) rest (t : acc)

-- | The answer to a question, from what the script wrote for it.
readAnswer :: Query -> ByteString -> Either String Answer
readAnswer q raw = do
  t <- told raw
  let wrong = Left ("Not an answer to " <> show q <> ": " <> show t)
  case (q, t) of
    (Get _, None) -> Right (Text Nothing)
    (Get _, Bytes' v) -> Right (Text (Just v))
    (HGet _ _, None) -> Right (Text Nothing)
    (HGet _ _, Bytes' v) -> Right (Text (Just v))
    (HExists _ _, Number n) -> Right (Flag (n /= 0))
    (SIsMember _ _, Number n) -> Right (Flag (n /= 0))
    (Exists _, Number n) -> Right (Flag (n /= 0))
    (HGetAll _, Listed ps) -> maybe wrong (Right . Fields . Map.fromList) (mapM pair ps)
    (SMembers _, Listed xs) -> maybe wrong (Right . Members . Set.fromList) (mapM bytes xs)
    (HKeys _, Listed xs) -> maybe wrong (Right . Members . Set.fromList) (mapM bytes xs)
    (HAbove _ _, Listed xs) -> maybe wrong (Right . Members . Set.fromList) (mapM bytes xs)
    (ZScore _ _, None) -> Right (Scored Nothing)
    (ZScore _ _, Bytes' v) -> maybe wrong (Right . Scored . Just) (score v)
    (ZCard _, Number n) -> Right (Count n)
    (ZRangeByScore {}, Listed xs) -> maybe wrong (Right . Ranked) (ranked xs)
    _ -> wrong
  where
    bytes = \case
      Bytes' v -> Just v
      _ -> Nothing
    pair = \case
      Listed [Bytes' f, Bytes' v] -> Just (f, v)
      _ -> Nothing
    ranked = \case
      [] -> Just []
      Bytes' x : Bytes' s : rest -> (:) . (x,) <$> score s <*> ranked rest
      _ -> Nothing
    -- a score as Redis writes it: a whole number, or a number with a
    -- fraction or an exponent, which Haskell reads without the plus sign
    score :: ByteString -> Maybe Score
    score v = case C.unpack v of
      "inf" -> Just (1 / 0)
      "-inf" -> Just (-1 / 0)
      s -> readMaybe (filter (/= '+') s)
