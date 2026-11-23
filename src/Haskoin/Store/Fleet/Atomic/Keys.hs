-- |
-- Module      : Haskoin.Store.Fleet.Atomic.Keys
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The questions and writes of "Haskoin.Store.Fleet.Atomic" with the kind
-- of each key in its type: what the decisions of the fleet are written
-- with.
--
-- = The problem
--
-- The script that carries out a decision compares every answer before it
-- writes, and then runs plain commands, so it cannot fail half way: unless
-- Redis refuses one of the commands. Redis refuses a command to a key of
-- another kind, an @HSET@ to a set, and an addition to what is no number.
-- That no decision makes such a write was a convention, kept by reading
-- eight hundred lines.
--
-- = What is done instead
--
-- A key has its kind in its type, t'K', given once, where the key is
-- named: in the record of the keys of the pool or of the cache. Each
-- question and each write here takes keys of one kind, so that a write of
-- another kind to a key does not compile. A key that holds a number, or a
-- hash that holds numbers, is a kind of its own ('Number', 'Counts'): it
-- can be added to, and it is only ever given a number.
--
-- A decision that imports this module, and not the writes of
-- "Haskoin.Store.Fleet.Atomic" itself, cannot write a command that Redis
-- refuses for the kind of its key. The two modules of decisions do.
--
-- = What it does not do
--
-- It does not know what a key holds in Redis: two records that name the
-- same key with two kinds would compile. So a key is named once. The pool
-- and the cache write some of the same keys, the version of the entries
-- and the lists of an entry among them, and the cache takes those from
-- the record of the pool.
module Haskoin.Store.Fleet.Atomic.Keys
  ( -- * Keys
    Kind (..),
    K,
    Family,
    named,
    guarded,
    family,
    guardedFamily,
    under,
    keyName,

    -- * Decisions
    Atomic,
    clock,
    prefetch,
    unguarded,

    -- * Questions named ahead, in steps
    Ahead,
    ahead,
    only,
    thenText,
    thenField,
    thenMember,
    thenMembers,
    thenAbove,
    askingCount,
    askingScore,
    askingRange,
    askingWhether,
    Query,
    Score,
    Bound (..),

    -- * Any key
    del,
    exists,
    wipe,

    -- * Bytes
    get,
    set,
    asking,

    -- * A number
    getNumber,
    setNumber,
    incrBy,
    askingNumber,

    -- * A hash
    hget,
    hexists,
    hgetall,
    hkeys,
    hset,
    hdel,
    askingField,
    askingWhetherField,

    -- * A hash of numbers
    hgetNumber,
    hsetNumber,
    hincrBy,
    hfieldsAbove,

    -- * A set
    sismember,
    smembers,
    sadd,
    srem,
    askingMember,
    askingMembers,

    -- * A sorted set
    zscore,
    zcard,
    zrangeByScore,
    zadd,
    zaddNew,
    zrem,
    zremRangeByRank,

    -- * A stream
    xadd,
  )
where

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.Map.Strict (Map)
import Data.Set (Set)
import Haskoin.Store.Fleet.Atomic
  ( Ahead (..),
    Atomic,
    Bound (..),
    Op (..),
    Query (..),
    Score,
    ahead,
    clock,
    prefetch,
    unguarded,
    write,
  )
import Haskoin.Store.Fleet.Atomic qualified as A

-- | What a key of Redis holds, as far as the fleet tells them apart.
data Kind
  = -- | bytes
    Text
  | -- | a whole number, which can be added to
    Number
  | -- | fields with bytes
    Hash
  | -- | fields with whole numbers, which can be added to
    Counts
  | -- | members
    Group
  | -- | members with scores
    Sorted
  | -- | entries, oldest first
    Stream

-- | A key that holds something of one kind, and the mark that guards it,
-- if one does ('guarded').
data K (kind :: Kind) = K !ByteString !(Maybe ByteString)
  deriving (Eq, Show)

-- | The start of the keys of one kind that there are for each of
-- something: for each address, each transaction, each entry. With the
-- mark that guards them all, if one does.
data Family (kind :: Kind) = Family !ByteString !(Maybe ByteString)

-- | A key by its name. Its kind is that of the place that it is put in:
-- the field of a record of keys.
named :: ByteString -> K kind
named k = K k Nothing

-- | A key that a mark guards: given the name of the mark, a key of its
-- own, and the name of the key. No decision can write to the key while
-- Redis has the mark, and a decision that writes to it is carried out
-- only if Redis still has no mark then
-- ('Haskoin.Store.Fleet.Atomic.guarding'). Every write of this module to
-- such a key asks first, so a decision has no way to write without it,
-- short of saying so ('unguarded').
--
-- The keys of the pool and of the cache of extended public keys are
-- guarded by the mark of the transactions that a move of the tip left to
-- be taken out (@docs\/tip-first.md@).
guarded :: ByteString -> ByteString -> K kind
guarded mark k = K k (Just mark)

-- | A family of keys by what they all start with.
family :: ByteString -> Family kind
family p = Family p Nothing

-- | A family of keys that a mark guards ('guarded').
guardedFamily :: ByteString -> ByteString -> Family kind
guardedFamily mark p = Family p (Just mark)

-- | The key of a family for one thing.
under :: Family kind -> ByteString -> K kind
under (Family p mark) x = K (p <> x) mark

-- | The name of a key.
keyName :: K kind -> ByteString
keyName (K k _) = k

num :: Integer -> ByteString
num = C.pack . show

-- | The whole number that bytes say, all of them.
whole :: ByteString -> Maybe Integer
whole v = case C.readInteger v of
  Just (n, rest) | B.null rest -> Just n
  _ -> Nothing

-- | A question to name ahead, and nothing after it ('ahead').
only :: Query -> Ahead
only q = Ahead q (const [])

-- | The question that 'get' asks, and what to name from its answer.
thenText :: K 'Text -> (Maybe ByteString -> [Ahead]) -> Ahead
thenText (K k _) next = Ahead (Get k) $ \case
  A.Text v -> next v
  _ -> []

-- | The question that 'hget' asks, and what to name from its answer.
thenField :: K 'Hash -> ByteString -> (Maybe ByteString -> [Ahead]) -> Ahead
thenField (K k _) f next = Ahead (HGet k f) $ \case
  A.Text v -> next v
  _ -> []

-- | The question that 'sismember' asks, and what to name from its answer.
thenMember :: K 'Group -> ByteString -> (Bool -> [Ahead]) -> Ahead
thenMember (K k _) x next = Ahead (SIsMember k x) $ \case
  A.Flag v -> next v
  _ -> []

-- | The question that 'smembers' asks, and what to name from its answer.
thenMembers :: K 'Group -> (Set ByteString -> [Ahead]) -> Ahead
thenMembers (K k _) next = Ahead (SMembers k) $ \case
  A.Members v -> next v
  _ -> []

-- | The question that 'hfieldsAbove' asks, and what to name from its
-- answer.
thenAbove :: K 'Counts -> Integer -> (Set ByteString -> [Ahead]) -> Ahead
thenAbove (K k _) n next = Ahead (HAbove k n) $ \case
  A.Members v -> next v
  _ -> []

-- | The question that 'hgetNumber' asks.
askingCount :: K 'Counts -> ByteString -> Query
askingCount (K k _) = HGet k

-- | The question that 'zscore' asks.
askingScore :: K 'Sorted -> ByteString -> Query
askingScore (K k _) = ZScore k

-- | The question that 'zrangeByScore' asks.
askingRange :: K 'Sorted -> Bound -> Bound -> Maybe Int -> Query
askingRange (K k _) = ZRangeByScore k

-- | The question that 'exists' asks.
askingWhether :: K kind -> Query
askingWhether (K k _) = Exists k

-- | @DEL@.
del :: (Monad m) => K kind -> Atomic m ()
del (K k mark) = mapM_ A.guarding mark >> write (Del k)

-- | @EXISTS@.
exists :: (Monad m) => K kind -> Atomic m Bool
exists (K k _) = A.exists k

-- | Delete every key that starts with some bytes, of whatever kind.
wipe :: (Monad m) => ByteString -> Atomic m ()
wipe = write . Wipe

-- | @GET@.
get :: (Monad m) => K 'Text -> Atomic m (Maybe ByteString)
get (K k _) = A.get k

-- | @SET@.
set :: (Monad m) => K 'Text -> ByteString -> Atomic m ()
set (K k mark) v = mapM_ A.guarding mark >> write (Set k v)

-- | The question that 'get' asks, to name it before it is asked
-- ('prefetch').
asking :: K 'Text -> Query
asking (K k _) = Get k

-- | The number that a key holds, or 0 for a key that is not there.
getNumber :: (Monad m) => K 'Number -> Atomic m Integer
getNumber (K k _) = maybe 0 (maybe 0 id . whole) <$> A.get k

-- | Give a key a number.
setNumber :: (Monad m) => K 'Number -> Integer -> Atomic m ()
setNumber (K k mark) n = mapM_ A.guarding mark >> write (Set k (num n))

-- | @INCRBY@.
incrBy :: (Monad m) => K 'Number -> Integer -> Atomic m ()
incrBy (K k mark) n = mapM_ A.guarding mark >> write (IncrBy k n)

-- | The question that 'getNumber' asks.
askingNumber :: K 'Number -> Query
askingNumber (K k _) = Get k

-- | @HGET@.
hget :: (Monad m) => K 'Hash -> ByteString -> Atomic m (Maybe ByteString)
hget (K k _) = A.hget k

-- | @HEXISTS@.
hexists :: (Monad m) => K 'Hash -> ByteString -> Atomic m Bool
hexists (K k _) = A.hexists k

-- | @HGETALL@.
hgetall :: (Monad m) => K 'Hash -> Atomic m (Map ByteString ByteString)
hgetall (K k _) = A.hgetall k

-- | @HKEYS@.
hkeys :: (Monad m) => K 'Hash -> Atomic m (Set ByteString)
hkeys (K k _) = A.hkeys k

-- | @HSET@.
hset :: (Monad m) => K 'Hash -> ByteString -> ByteString -> Atomic m ()
hset (K k mark) f v = mapM_ A.guarding mark >> write (HSet k f v)

-- | @HDEL@.
hdel :: (Monad m) => K 'Hash -> ByteString -> Atomic m ()
hdel (K k mark) f = mapM_ A.guarding mark >> write (HDel k f)

-- | The question that 'hget' asks.
askingField :: K 'Hash -> ByteString -> Query
askingField (K k _) = HGet k

-- | The question that 'hexists' asks.
askingWhetherField :: K 'Hash -> ByteString -> Query
askingWhetherField (K k _) = HExists k

-- | The number of a field, or nothing for a field that is not there.
hgetNumber :: (Monad m) => K 'Counts -> ByteString -> Atomic m (Maybe Integer)
hgetNumber (K k _) f = (>>= whole) <$> A.hget k f

-- | Give a field a number.
hsetNumber :: (Monad m) => K 'Counts -> ByteString -> Integer -> Atomic m ()
hsetNumber (K k mark) f n = mapM_ A.guarding mark >> write (HSet k f (num n))

-- | @HINCRBY@.
hincrBy :: (Monad m) => K 'Counts -> ByteString -> Integer -> Atomic m ()
hincrBy (K k mark) f n = mapM_ A.guarding mark >> write (HIncrBy k f n)

-- | The fields whose number is above a bound: for a decision that needs
-- to know which leases still run and not when each runs out, so that a
-- lease that is renewed meanwhile changes nothing for it.
hfieldsAbove :: (Monad m) => K 'Counts -> Integer -> Atomic m (Set ByteString)
hfieldsAbove (K k _) = A.habove k

-- | @SISMEMBER@.
sismember :: (Monad m) => K 'Group -> ByteString -> Atomic m Bool
sismember (K k _) = A.sismember k

-- | @SMEMBERS@.
smembers :: (Monad m) => K 'Group -> Atomic m (Set ByteString)
smembers (K k _) = A.smembers k

-- | @SADD@.
sadd :: (Monad m) => K 'Group -> ByteString -> Atomic m ()
sadd (K k mark) x = mapM_ A.guarding mark >> write (SAdd k x)

-- | @SREM@.
srem :: (Monad m) => K 'Group -> ByteString -> Atomic m ()
srem (K k mark) x = mapM_ A.guarding mark >> write (SRem k x)

-- | The question that 'sismember' asks.
askingMember :: K 'Group -> ByteString -> Query
askingMember (K k _) = SIsMember k

-- | The question that 'smembers' asks.
askingMembers :: K 'Group -> Query
askingMembers (K k _) = SMembers k

-- | @ZSCORE@.
zscore :: (Monad m) => K 'Sorted -> ByteString -> Atomic m (Maybe Score)
zscore (K k _) = A.zscore k

-- | @ZCARD@.
zcard :: (Monad m) => K 'Sorted -> Atomic m Integer
zcard (K k _) = A.zcard k

-- | @ZRANGEBYSCORE@, with the scores.
zrangeByScore :: (Monad m) => K 'Sorted -> Bound -> Bound -> Maybe Int -> Atomic m [(ByteString, Score)]
zrangeByScore (K k _) = A.zrangeByScore k

-- | @ZADD@.
zadd :: (Monad m) => K 'Sorted -> Score -> ByteString -> Atomic m ()
zadd (K k mark) s x = mapM_ A.guarding mark >> write (ZAdd k s x)

-- | @ZADD NX@: only if the member is not there.
zaddNew :: (Monad m) => K 'Sorted -> Score -> ByteString -> Atomic m ()
zaddNew (K k mark) s x = mapM_ A.guarding mark >> write (ZAddNX k s x)

-- | @ZREM@.
zrem :: (Monad m) => K 'Sorted -> ByteString -> Atomic m ()
zrem (K k mark) x = mapM_ A.guarding mark >> write (ZRem k x)

-- | @ZREMRANGEBYRANK@.
zremRangeByRank :: (Monad m) => K 'Sorted -> Integer -> Integer -> Atomic m ()
zremRangeByRank (K k mark) from to = mapM_ A.guarding mark >> write (ZRemRangeByRank k from to)

-- | @XADD@ to a stream that keeps about so many entries.
xadd :: (Monad m) => K 'Stream -> Int -> [(ByteString, ByteString)] -> Atomic m ()
xadd (K k mark) keep fields = mapM_ A.guarding mark >> write (XAdd k keep fields)
