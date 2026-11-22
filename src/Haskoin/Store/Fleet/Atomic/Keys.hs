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
    family,
    under,
    keyName,

    -- * Decisions
    Atomic,
    clock,
    prefetch,
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
import Haskoin.Store.Fleet.Atomic (Atomic, Bound (..), Op (..), Query (..), Score, clock, prefetch, write)
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

-- | A key that holds something of one kind.
newtype K (kind :: Kind) = K ByteString
  deriving (Eq, Show)

-- | The start of the keys of one kind that there are for each of
-- something: for each address, each transaction, each entry.
newtype Family (kind :: Kind) = Family ByteString

-- | A key by its name. Its kind is that of the place that it is put in:
-- the field of a record of keys.
named :: ByteString -> K kind
named = K

-- | A family of keys by what they all start with.
family :: ByteString -> Family kind
family = Family

-- | The key of a family for one thing.
under :: Family kind -> ByteString -> K kind
under (Family p) x = K (p <> x)

-- | The name of a key.
keyName :: K kind -> ByteString
keyName (K k) = k

num :: Integer -> ByteString
num = C.pack . show

-- | The whole number that bytes say, all of them.
whole :: ByteString -> Maybe Integer
whole v = case C.readInteger v of
  Just (n, rest) | B.null rest -> Just n
  _ -> Nothing

-- | @DEL@.
del :: (Monad m) => K kind -> Atomic m ()
del (K k) = write (Del k)

-- | @EXISTS@.
exists :: (Monad m) => K kind -> Atomic m Bool
exists (K k) = A.exists k

-- | Delete every key that starts with some bytes, of whatever kind.
wipe :: (Monad m) => ByteString -> Atomic m ()
wipe = write . Wipe

-- | @GET@.
get :: (Monad m) => K 'Text -> Atomic m (Maybe ByteString)
get (K k) = A.get k

-- | @SET@.
set :: (Monad m) => K 'Text -> ByteString -> Atomic m ()
set (K k) v = write (Set k v)

-- | The question that 'get' asks, to name it before it is asked
-- ('prefetch').
asking :: K 'Text -> Query
asking (K k) = Get k

-- | The number that a key holds, or 0 for a key that is not there.
getNumber :: (Monad m) => K 'Number -> Atomic m Integer
getNumber (K k) = maybe 0 (maybe 0 id . whole) <$> A.get k

-- | Give a key a number.
setNumber :: (Monad m) => K 'Number -> Integer -> Atomic m ()
setNumber (K k) n = write (Set k (num n))

-- | @INCRBY@.
incrBy :: (Monad m) => K 'Number -> Integer -> Atomic m ()
incrBy (K k) n = write (IncrBy k n)

-- | The question that 'getNumber' asks.
askingNumber :: K 'Number -> Query
askingNumber (K k) = Get k

-- | @HGET@.
hget :: (Monad m) => K 'Hash -> ByteString -> Atomic m (Maybe ByteString)
hget (K k) = A.hget k

-- | @HEXISTS@.
hexists :: (Monad m) => K 'Hash -> ByteString -> Atomic m Bool
hexists (K k) = A.hexists k

-- | @HGETALL@.
hgetall :: (Monad m) => K 'Hash -> Atomic m (Map ByteString ByteString)
hgetall (K k) = A.hgetall k

-- | @HKEYS@.
hkeys :: (Monad m) => K 'Hash -> Atomic m (Set ByteString)
hkeys (K k) = A.hkeys k

-- | @HSET@.
hset :: (Monad m) => K 'Hash -> ByteString -> ByteString -> Atomic m ()
hset (K k) f v = write (HSet k f v)

-- | @HDEL@.
hdel :: (Monad m) => K 'Hash -> ByteString -> Atomic m ()
hdel (K k) f = write (HDel k f)

-- | The question that 'hget' asks.
askingField :: K 'Hash -> ByteString -> Query
askingField (K k) = HGet k

-- | The question that 'hexists' asks.
askingWhetherField :: K 'Hash -> ByteString -> Query
askingWhetherField (K k) = HExists k

-- | The number of a field, or nothing for a field that is not there.
hgetNumber :: (Monad m) => K 'Counts -> ByteString -> Atomic m (Maybe Integer)
hgetNumber (K k) f = (>>= whole) <$> A.hget k f

-- | Give a field a number.
hsetNumber :: (Monad m) => K 'Counts -> ByteString -> Integer -> Atomic m ()
hsetNumber (K k) f n = write (HSet k f (num n))

-- | @HINCRBY@.
hincrBy :: (Monad m) => K 'Counts -> ByteString -> Integer -> Atomic m ()
hincrBy (K k) f n = write (HIncrBy k f n)

-- | The fields whose number is above a bound: for a decision that needs
-- to know which leases still run and not when each runs out, so that a
-- lease that is renewed meanwhile changes nothing for it.
hfieldsAbove :: (Monad m) => K 'Counts -> Integer -> Atomic m (Set ByteString)
hfieldsAbove (K k) = A.habove k

-- | @SISMEMBER@.
sismember :: (Monad m) => K 'Group -> ByteString -> Atomic m Bool
sismember (K k) = A.sismember k

-- | @SMEMBERS@.
smembers :: (Monad m) => K 'Group -> Atomic m (Set ByteString)
smembers (K k) = A.smembers k

-- | @SADD@.
sadd :: (Monad m) => K 'Group -> ByteString -> Atomic m ()
sadd (K k) x = write (SAdd k x)

-- | @SREM@.
srem :: (Monad m) => K 'Group -> ByteString -> Atomic m ()
srem (K k) x = write (SRem k x)

-- | The question that 'sismember' asks.
askingMember :: K 'Group -> ByteString -> Query
askingMember (K k) = SIsMember k

-- | The question that 'smembers' asks.
askingMembers :: K 'Group -> Query
askingMembers (K k) = SMembers k

-- | @ZSCORE@.
zscore :: (Monad m) => K 'Sorted -> ByteString -> Atomic m (Maybe Score)
zscore (K k) = A.zscore k

-- | @ZCARD@.
zcard :: (Monad m) => K 'Sorted -> Atomic m Integer
zcard (K k) = A.zcard k

-- | @ZRANGEBYSCORE@, with the scores.
zrangeByScore :: (Monad m) => K 'Sorted -> Bound -> Bound -> Maybe Int -> Atomic m [(ByteString, Score)]
zrangeByScore (K k) = A.zrangeByScore k

-- | @ZADD@.
zadd :: (Monad m) => K 'Sorted -> Score -> ByteString -> Atomic m ()
zadd (K k) s x = write (ZAdd k s x)

-- | @ZADD NX@: only if the member is not there.
zaddNew :: (Monad m) => K 'Sorted -> Score -> ByteString -> Atomic m ()
zaddNew (K k) s x = write (ZAddNX k s x)

-- | @ZREM@.
zrem :: (Monad m) => K 'Sorted -> ByteString -> Atomic m ()
zrem (K k) x = write (ZRem k x)

-- | @ZREMRANGEBYRANK@.
zremRangeByRank :: (Monad m) => K 'Sorted -> Integer -> Integer -> Atomic m ()
zremRangeByRank (K k) from to = write (ZRemRangeByRank k from to)

-- | @XADD@ to a stream that keeps about so many entries.
xadd :: (Monad m) => K 'Stream -> Int -> [(ByteString, ByteString)] -> Atomic m ()
xadd (K k) keep fields = write (XAdd k keep fields)
