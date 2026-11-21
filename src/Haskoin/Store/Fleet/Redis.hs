{-# LANGUAGE TemplateHaskell #-}

-- |
-- Module      : Haskoin.Store.Fleet.Redis
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The rules of "Haskoin.Store.Fleet" carried out against Redis: the state
-- of a fleet in three keys, and each change that has to be one step made
-- one.
--
-- = Why one step
--
-- Whether a store may report depends on the tip and on the leases of the
-- others, and the block that the tip moves to depends on every report. Read
-- and written in separate commands, each could change in between: two
-- stores would both find themselves alone, or the tip would move to a block
-- that a store had just stopped reporting. Redis runs a script without
-- letting anything else in, so a rule that is a script sees one state and
-- changes it at once.
--
-- The reports and the move of the tip alone are short Lua scripts. The
-- changes to the shared mempool, and the move of the tip with the pool,
-- are decisions made in Haskell ("Haskoin.Store.Fleet.Pool.Decide") and
-- carried out by one script that decides nothing
-- ("Haskoin.Store.Fleet.Atomic"): a script of eight hundred lines that
-- failed half way left the pool half changed, and its rules had to be kept
-- twice.
--
-- = Whose clock
--
-- Leases are measured on the clock of Redis, read inside the scripts. The
-- stores then need not agree on the time, only on which Redis they share. A
-- time can be given instead, which is how the tests make leases run out
-- without waiting.
--
-- = Blocks and stores
--
-- A block is its hash, or any bytes of one fixed size, and a store is a
-- name that no other store of the fleet has.
--
-- = The shared mempool
--
-- The rules of "Haskoin.Store.Fleet.Pool" are the decisions of
-- "Haskoin.Store.Fleet.Pool.Decide", with the pool in keys next to those
-- of the tip. A transaction is its id, of one
-- fixed size, and an output is the id of the transaction that made it and
-- four more bytes. An offer says which tip the store checked the
-- transaction against, and is turned away if the tip has moved since: what
-- was unspent at one block may be spent at the next. Moving the tip and
-- changing the pool for the blocks in between is one step ('fleetAdvance'),
-- so that no request finds a block without what it did to the pool.
--
-- A transaction comes with what it does at each address, a t'PoolEntry'
-- for each, and the pool keeps what it holds for each address in step: its
-- transactions and the outputs paid to it, each in a sorted set ordered as
-- queries list them, the outputs of it that are spent, and the sums that a
-- balance needs. 'Haskoin.Store.Fleet.Pool.addresses' says what those
-- should be. Kept as the transactions come and go, they cost a few commands
-- for each entry; worked out when asked, a balance would cost a read of
-- every entry of the address, which is what the store's own mempool area
-- did at first and was measured to be thousands of times too slow for a
-- busy address.
--
-- The tokens of Bitcoin Cash come the same way: each output that holds
-- some and that a transaction makes or uses up is an entry, a
-- t'PoolTokenOut' with its category and amounts read by the store, and
-- the script keeps for each category its transactions and outputs, and for
-- each address and category the sums that a token balance needs.
-- 'Haskoin.Store.Fleet.Pool.tokenTotals' says what those should be.
module Haskoin.Store.Fleet.Redis
  ( FleetKeys (..),
    fleetKeys,
    Moved (..),
    fleetReport,
    fleetMove,
    fleetTip,
    fleetLive,

    -- * The shared mempool
    Offered (..),
    PoolChange (..),
    PoolTx (..),
    PoolEntry (..),
    PoolTokenOut (..),
    poolOffer,
    poolWithdraw,
    poolExpire,
    fleetAdvance,
    fleetCommit,
    fleetRestart,
    fleetRestarts,
    FleetEvent (..),
    fleetEventsEnd,
    fleetEventsAfter,
    poolSpends,
    poolBody,
    poolRead,

    -- * What the fleet has seen
    fleetTrimSeen,
    fleetSeen,
    fleetSettled,

    -- * Scripts
    Script,
    script,
    evalScript,
  )
where

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.FileEmbed (embedFile, makeRelativeToProject)
import Data.Maybe (fromMaybe, isJust)
import Database.Redis (Connection, Redis, Reply (..))
import Database.Redis qualified as R
import Haskoin.Store.Fleet (Time)
import Haskoin.Store.Fleet.Atomic (Atomic)
import Haskoin.Store.Fleet.Atomic.Redis (transact)
import Haskoin.Store.Fleet.Pool.Decide qualified as D
import Haskoin.Store.Fleet.Pool.Types
import Haskoin.Store.Fleet.Script (Script, evalScript, script)

-- | The keys of a fleet in Redis, and the size of its blocks in bytes.
data FleetKeys = FleetKeys
  { -- | what every key of the fleet starts with
    prefix :: !ByteString,
    -- | the fleet tip: a block
    tip :: !ByteString,
    -- | a hash from each store to the time its lease runs out, in
    -- milliseconds
    lease :: !ByteString,
    -- | a hash from each store to its recent blocks, newest first, one
    -- after the other
    recent :: !ByteString,
    -- | bytes in a block
    size :: !Int
  }

-- | The keys of the fleet under a prefix, such as that of the cache of a
-- network, for blocks of a size.
fleetKeys :: ByteString -> Int -> FleetKeys
fleetKeys prefix size =
  FleetKeys
    { prefix,
      tip = prefix <> "fleet:tip",
      lease = prefix <> "fleet:lease",
      recent = prefix <> "fleet:recent",
      size
    }

-- | Report the recent blocks of a store, newest first, under a lease of so
-- many milliseconds, given the blocks that the store has views of. Says
-- whether the report was taken: it is not when the store has no view of the
-- tip and another store is live ('Haskoin.Store.Fleet.mayReport').
fleetReport :: Connection -> FleetKeys -> Maybe Time -> Time -> ByteString -> [ByteString] -> [ByteString] -> IO Bool
fleetReport conn keys clock term store blocks views =
  (== 1) <$> run conn (evalScript reportScript (scriptKeys keys) args :: Redis (Either Reply Integer))
  where
    args = [store, num term, maybe "" num clock, num keys.size, B.concat blocks, B.concat views]

-- | Move the tip to the newest block that every live store reported, given
-- the tip that the caller saw.
fleetMove :: Connection -> FleetKeys -> Maybe Time -> Maybe ByteString -> IO Moved
fleetMove conn keys clock seen =
  run conn (evalScript moveScript (scriptKeys keys) args) >>= \case
    ["moved", block] -> return (MovedTo block)
    ["stays"] -> return Stays
    ["raced"] -> return Raced
    other -> fail ("Unexpected answer of the fleet script: " <> show (other :: [ByteString]))
  where
    args = [maybe "" id seen, maybe "" num clock, num keys.size]

-- | The fleet tip, if there is one.
fleetTip :: Connection -> FleetKeys -> IO (Maybe ByteString)
fleetTip conn keys = run conn (R.get keys.tip)

-- | The stores that have a lease, with the time each runs out, in
-- milliseconds. Leases that ran out are in it until the next report.
fleetLive :: Connection -> FleetKeys -> IO [(ByteString, Time)]
fleetLive conn keys = map (fmap (maybe 0 (fromIntegral . fst) . C.readInteger)) <$> run conn (R.hgetall keys.lease)

-- | Offer a transaction to the pool: given the size of a transaction id,
-- the tip that the store checked against, the transaction, whether the
-- store finds each output that it spends unspent at that tip, in the order
-- of its inputs, and the transactions that it replaces.
poolOffer ::
  Connection ->
  FleetKeys ->
  Int ->
  Maybe ByteString ->
  PoolTx ->
  [Bool] ->
  [ByteString] ->
  IO Offered
poolOffer conn keys tsize seen tx unspents replaced =
  fromMaybe Late <$> transact conn 5 (D.offer (D.poolKeys keys.prefix tsize) seen unspents replaced tx)

-- | Take transactions out of the pool, with all that spends them. Gives
-- those that left.
poolWithdraw :: Connection -> FleetKeys -> Int -> [ByteString] -> IO [ByteString]
poolWithdraw conn keys tsize ts = decided conn (D.withdraw (D.poolKeys keys.prefix tsize) ts)

-- | Take out of the pool what has been in it since before a time, in
-- seconds of the clock of Redis, so many at most, with all that spends
-- them, choosing and withdrawing them in one call. Gives those that left.
poolExpire :: Connection -> FleetKeys -> Int -> Integer -> Int -> IO [ByteString]
poolExpire conn keys tsize before most = decided conn (D.expire (D.poolKeys keys.prefix tsize) before most)

-- | Keep the newest of the transactions that the fleet has seen, so
-- many. Gives how many are kept.
--
-- The fleet keeps, in one sorted set, each transaction that the pool has
-- decided on: each that entered it, that an offer found in it or in
-- conflict with it, and that left it unconfirmed, by the time at which it
-- was first seen. No store fetches one of them when a peer announces it
-- ('fleetSeen'). The pool's script adds them, with @ZADD NX@, in the step
-- that decides, so a transaction that a store received and has not offered
-- is not in the set: if the store stops first, another fetches it. In
-- Redis, the set is the same for every store and outlives a store that
-- restarts; in the memory of each store, as it was before, every store
-- fetched each transaction once, and again after a restart. The set is
-- trimmed here, when it has grown a hundredth past its bound, and not on
-- every addition.
fleetTrimSeen :: Connection -> FleetKeys -> Int -> IO Integer
fleetTrimSeen conn keys most = decided conn (D.trim (D.poolKeys keys.prefix 0) most)

-- | Of these transactions, which the fleet has seen ('fleetTrimSeen'), in one
-- round trip.
fleetSeen :: Connection -> FleetKeys -> [ByteString] -> IO [Bool]
fleetSeen _ _ [] = return []
fleetSeen conn keys ts =
  map isJust <$> run conn scores
  where
    scores :: Redis (Either Reply [Maybe ByteString])
    scores = R.sendRequest ("ZMSCORE" : keys.prefix <> "fleet:seen" : ts)

-- | Of these transactions, which are settled: the pool has them, or the
-- fleet remembers that the pool decided on them ('fleetSeen'). Both are
-- asked in one round trip.
--
-- What the fleet has seen is trimmed to the newest ('fleetTrimSeen'), and
-- a pool larger than that has transactions that are no longer among them:
-- those are found in the pool itself.
fleetSettled :: Connection -> FleetKeys -> [ByteString] -> IO [Bool]
fleetSettled _ _ [] = return []
fleetSettled conn keys ts = do
  (seen, held) <- run conn $ do
    a <- scores "fleet:seen"
    b <- scores "pool:time"
    return ((,) <$> a <*> b)
  return (zipWith (\x y -> isJust x || isJust y) seen held)
  where
    scores :: ByteString -> Redis (Either Reply [Maybe ByteString])
    scores k = R.sendRequest ("ZMSCORE" : keys.prefix <> k : ts)

-- | Move the tip to a block and bring the pool there, in one step: given
-- the tip that the caller saw, the block, which must be the one that the
-- live stores agree on, and what the blocks in between did.
--
-- A store that cannot say what they did, because it does not have the old
-- tip, gives 'Nothing', and the pool is emptied: but only if no other
-- store is live, as when a fleet comes back after all its stores were
-- away. With another store live the answer is 'Raced', and the tip waits
-- for a store that knows the way. When the way is known and too long,
-- see 'fleetRestart'.
--
-- The last argument is the blocks that the tip moves through, oldest
-- first and the new tip last, for the events of the fleet ('FleetEvent'):
-- with none given, the new tip is the one block told.
fleetAdvance ::
  Connection ->
  FleetKeys ->
  Int ->
  Maybe Time ->
  -- | the name of the store that moves the tip
  ByteString ->
  Maybe ByteString ->
  ByteString ->
  Maybe PoolChange ->
  [ByteString] ->
  IO Moved
fleetAdvance conn keys tsize clock store seen target known path =
  fleetCommit conn keys tsize clock store seen target known path ""

-- | 'fleetAdvance' as the commit of a move of the tip with the cache of
-- extended public keys: given also the token of the move of the cache that
-- the store began ('Haskoin.Store.Fleet.XPub.cacheBegin'), whose pieces
-- marked the entries that it wrote. The cache is never emptied here: a
-- store that cannot say what the abandoned blocks held does not move the
-- tip. The pool, the tip and the cache change together: every marked entry gets
-- a new version and the cache is at the new tip, in the same script. The
-- answer is 'Raced' if another move of the cache is under way, or if the
-- store's was aborted.
fleetCommit ::
  Connection ->
  FleetKeys ->
  Int ->
  Maybe Time ->
  -- | the name of the store that moves the tip: without a change of the
  -- pool, it empties the pool only if it is the one store that is live
  ByteString ->
  Maybe ByteString ->
  ByteString ->
  Maybe PoolChange ->
  [ByteString] ->
  ByteString ->
  IO Moved
fleetCommit conn keys tsize clock store seen target known path token =
  advance conn keys tsize clock seen target (maybe (D.AloneNotKnowing store) D.Changing known) (path, token)

-- | Move the tip to a block and empty the pool, for a store that has both
-- the tip and the block and finds the way between them longer than it
-- goes through.
--
-- Unlike a store that does not have the old tip, it may do this with
-- other stores live. The length of the way is the same for all of them, so
-- none would ever say what the blocks did, and the tip would stay where it
-- is for good: a fleet whose stores came back together after half a day
-- away answered nothing from then on. Emptying the pool costs the
-- transactions that were in it until the stores fill it again: what the
-- fleet has seen is forgotten with it, and the count of 'fleetRestarts'
-- goes up, so that each store reads the mempool of its node and asks its
-- peers for theirs.
fleetRestart :: Connection -> FleetKeys -> Int -> Maybe Time -> Maybe ByteString -> ByteString -> IO Moved
fleetRestart conn keys tsize clock seen target =
  advance conn keys tsize clock seen target D.TooLong ([], "")

-- | How many times the pool was emptied, by 'fleetRestart' or by a store
-- alone that did not have the old tip. A store that sees another number
-- than at its last turn fills the pool again, as after Redis lost its
-- data: the emptied pool is shared, so every store sees the same count
-- and any of them that can read a mempool helps.
fleetRestarts :: Connection -> FleetKeys -> IO Integer
fleetRestarts conn keys =
  maybe 0 (maybe 0 fst . C.readInteger) <$> run conn (R.get (keys.prefix <> "fleet:restarts"))

-- | A move of the tip with the pool and the cache, as one decision
-- ('Haskoin.Store.Fleet.Pool.Decide.advance'). Another store that changes
-- what the decision asked about makes it be decided again, three times at
-- most: then the move is 'Raced', and the store tries at its next turn.
advance ::
  Connection ->
  FleetKeys ->
  Int ->
  Maybe Time ->
  Maybe ByteString ->
  ByteString ->
  D.How ->
  ([ByteString], ByteString) ->
  IO Moved
advance conn keys tsize clock seen target how (path, token) =
  fromMaybe Raced <$> transact conn 3 decision
  where
    decision :: (Monad m) => Atomic m Moved
    decision = D.advance (D.poolKeys keys.prefix tsize) seen target (fromIntegral <$> clock) keys.size how path token

-- | Carry out a decision that has to be: one that other stores keep
-- changing the ground of is decided again, a hundred times before this
-- gives up with an exception. Nothing was written then.
decided :: Connection -> (forall m. (Monad m) => Atomic m a) -> IO a
decided conn decision =
  transact conn 100 decision >>= maybe (fail "The fleet changed under a decision a hundred times") return

-- | Read from the pool as it is at a tip: given the size of a transaction
-- id, the tip that the caller answers from, what to read and its arguments,
-- as @read.lua@ takes them. 'Nothing' if the tip is another one by now.
poolRead :: Connection -> FleetKeys -> Int -> Maybe ByteString -> ByteString -> [ByteString] -> IO (Maybe [ByteString])
poolRead conn keys tsize tip what args =
  run conn (evalScript readScript [] (what : keys.prefix : num tsize : maybe "" id tip : args)) >>= \case
    "ok" : rows -> return (Just rows)
    ["raced"] -> return Nothing
    other -> fail ("Unexpected answer of the read script: " <> show (take 1 other))

-- | Something that happened to the fleet, as every store of it tells its
-- clients.
--
-- Every store is a member of a fleet, even a fleet of one. The stores of a
-- fleet answer from one tip and one pool, and their events have to be those
-- of that tip and that pool, not what each store's own node saw: otherwise
-- a client would hear of a transaction from one server and find that the
-- next does not have it. So the script that
-- changes the pool or moves the tip appends what it did to a stream in
-- Redis, in the same step, and every store reads the stream.
data FleetEvent
  = -- | the fleet tip moved to this block, or through it
    FleetBlock !ByteString
  | -- | a transaction entered the pool
    FleetEntered !ByteString
  | -- | a transaction left the pool unconfirmed
    FleetLeft !ByteString
  deriving (Eq, Ord, Show)

-- | The place in the stream of events after the last event that there is:
-- where a store that starts listening now begins.
fleetEventsEnd :: Connection -> FleetKeys -> IO ByteString
fleetEventsEnd conn keys =
  run conn (R.sendRequest ["XREVRANGE", keys.prefix <> "fleet:events", "+", "-", "COUNT", "1"]) >>= \case
    R.MultiBulk (Just [R.MultiBulk (Just (R.Bulk (Just at) : _))]) -> return at
    _ -> return "0-0"

-- | The events after a place in the stream, each with its place, waiting
-- up to so many milliseconds for one if there is none yet.
fleetEventsAfter :: Connection -> FleetKeys -> ByteString -> Integer -> IO [(ByteString, FleetEvent)]
fleetEventsAfter conn keys from wait =
  run conn (R.sendRequest ["XREAD", "BLOCK", num wait, "STREAMS", keys.prefix <> "fleet:events", from]) >>= \case
    R.MultiBulk (Just [R.MultiBulk (Just [_, R.MultiBulk (Just records)])]) -> return (concatMap record records)
    _ -> return []
  where
    record = \case
      R.MultiBulk (Just [R.Bulk (Just at), R.MultiBulk (Just fields)]) ->
        case (lookup "k" (pairs fields), lookup "h" (pairs fields)) of
          (Just "b", Just h) -> [(at, FleetBlock h)]
          (Just "t", Just h) -> [(at, FleetEntered h)]
          (Just "d", Just h) -> [(at, FleetLeft h)]
          _ -> []
      _ -> []
    pairs (R.Bulk (Just k) : R.Bulk (Just v) : more) = (k, v) : pairs more
    pairs _ = []

-- | The transactions of the pool, each with the outputs that it spends one
-- after the other.
poolSpends :: Connection -> FleetKeys -> IO [(ByteString, ByteString)]
poolSpends conn keys = run conn (R.hgetall (keys.prefix <> "pool:spends"))

-- | The body of a transaction of the pool.
poolBody :: Connection -> FleetKeys -> ByteString -> IO (Maybe ByteString)
poolBody conn keys t = run conn (R.hget (keys.prefix <> "pool:body") t)

scriptKeys :: FleetKeys -> [ByteString]
scriptKeys keys = [keys.tip, keys.lease, keys.recent]

num :: (Show a) => a -> ByteString
num = C.pack . show

-- | Run Redis commands, throwing if Redis answers with an error.
run :: Connection -> Redis (Either Reply a) -> IO a
run conn f =
  R.runRedis conn f >>= \case
    Right x -> return x
    Left e -> fail ("Redis error: " <> show e)

-- | 'Haskoin.Store.Fleet.mayReport' and 'Haskoin.Store.Fleet.report' in one
-- step.
reportScript :: Script
reportScript = script $(makeRelativeToProject "src/Haskoin/Store/Fleet/lua/report.lua" >>= embedFile)
{-# NOINLINE reportScript #-}

-- | 'Haskoin.Store.Fleet.move'.
moveScript :: Script
moveScript = script $(makeRelativeToProject "src/Haskoin/Store/Fleet/lua/move.lua" >>= embedFile)
{-# NOINLINE moveScript #-}

-- | What a store reads from the pool, each read with a check that the tip
-- is the one that the store answers from.
readScript :: Script
readScript = script $(makeRelativeToProject "src/Haskoin/Store/Fleet/lua/read.lua" >>= embedFile)
{-# NOINLINE readScript #-}
