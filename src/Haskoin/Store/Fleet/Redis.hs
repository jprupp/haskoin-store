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
-- of a fleet in three keys, and a Lua script for each step that has to be
-- one step.
--
-- = Why scripts
--
-- Whether a store may report depends on the tip and on the leases of the
-- others, and the block that the tip moves to depends on every report. Read
-- and written in separate commands, each could change in between: two
-- stores would both find themselves alone, or the tip would move to a block
-- that a store had just stopped reporting. Redis runs a script without
-- letting anything else in, so each rule sees one state and changes it at
-- once.
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
-- The rules of "Haskoin.Store.Fleet.Pool" are one more script, with the
-- pool in keys next to those of the tip. A transaction is its id, of one
-- fixed size, and an output is the id of the transaction that made it and
-- four more bytes. An offer says which tip the store checked the
-- transaction against, and is turned away if the tip has moved since: what
-- was unspent at one block may be spent at the next. Moving the tip and
-- changing the pool for the blocks in between is one step ('fleetAdvance'),
-- so that no request finds a block without what it did to the pool.
--
-- A transaction comes with what it does at each address (t'PoolEntry'),
-- and the script keeps what the pool holds for each address in step: its
-- transactions and the outputs paid to it, each in a sorted set ordered as
-- queries list them, the outputs of it that are spent, and the sums that a
-- balance needs. 'Haskoin.Store.Fleet.Pool.addresses' says what those
-- should be. Kept as the transactions come and go, they cost a few commands
-- for each entry; worked out when asked, a balance would cost a read of
-- every entry of the address, which is what the store's own mempool area
-- did at first and was measured to be thousands of times too slow for a
-- busy address.
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
    poolOffer,
    poolWithdraw,
    fleetAdvance,
    FleetEvent (..),
    fleetEventsEnd,
    fleetEventsAfter,
    poolSpends,
    poolBody,
    poolRead,
  )
where

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.FileEmbed (embedFile, makeRelativeToProject)
import Data.Word (Word64)
import Database.Redis (Connection, Redis, Reply)
import Database.Redis qualified as R
import Haskoin.Store.Fleet (Time)

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

-- | What became of an attempt to move the tip; see 'Haskoin.Store.Fleet.Move'.
data Moved
  = -- | the tip is now this block
    MovedTo !ByteString
  | -- | the tip is where the live stores agree, or they agree on nothing
    Stays
  | -- | another store moved the tip first
    Raced
  deriving (Eq, Show)

-- | Report the recent blocks of a store, newest first, under a lease of so
-- many milliseconds, given the blocks that the store has views of. Says
-- whether the report was taken: it is not when the store has no view of the
-- tip and another store is live ('Haskoin.Store.Fleet.mayReport').
fleetReport :: Connection -> FleetKeys -> Maybe Time -> Time -> ByteString -> [ByteString] -> [ByteString] -> IO Bool
fleetReport conn keys clock term store blocks views =
  (== 1) <$> run conn (R.eval reportScript (scriptKeys keys) args :: Redis (Either Reply Integer))
  where
    args = [store, num term, maybe "" num clock, num keys.size, B.concat blocks, B.concat views]

-- | Move the tip to the newest block that every live store reported, given
-- the tip that the caller saw.
fleetMove :: Connection -> FleetKeys -> Maybe Time -> Maybe ByteString -> IO Moved
fleetMove conn keys clock seen =
  run conn (R.eval moveScript (scriptKeys keys) args) >>= \case
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

-- | What became of an offer; see 'Haskoin.Store.Fleet.Pool.Offer'.
data Offered
  = -- | it is in the pool now
    Taken
  | -- | it was there already
    Known
  | -- | these transactions of the pool spend what it spends
    Conflict ![ByteString]
  | -- | these outputs are neither made by the pool nor unspent at the tip
    Orphan ![ByteString]
  | -- | the tip is not the one that the store checked the transaction
    -- against: check again and offer again
    Late
  deriving (Eq, Show)

-- | What a transaction does at an address; see
-- 'Haskoin.Store.Fleet.Pool.Entry'. Addresses, outputs and scripts are
-- whatever bytes the stores of a fleet agree on.
data PoolEntry
  = -- | it spends from the address or pays to it
    PoolTouches !ByteString
  | -- | it spends this output of the address, of this amount
    PoolSpends !ByteString !ByteString !Word64
  | -- | it pays this output to the address, with this amount and script
    PoolPays !ByteString !ByteString !Word64 !ByteString
  deriving (Eq, Show)

-- | A transaction as the pool keeps it.
data PoolTx = PoolTx
  { txid :: !ByteString,
    -- | when it entered the mempool of the store that offers it. The pool
    -- keeps the time of the first offer, so that every store lists the
    -- transaction in the same place.
    time :: !Word64,
    -- | the outputs that it spends, in the order of its inputs
    inputs :: ![ByteString],
    -- | what a store needs to answer about it
    body :: !ByteString,
    -- | whether it says itself that it can be replaced by fee. Whether it
    -- can also depends on the transactions of the pool that it spends
    -- from, which the pool works out and keeps right as those come and go.
    signals :: !Bool,
    -- | what it does at each address
    entries :: ![PoolEntry]
  }
  deriving (Eq, Show)

-- | What the blocks between two tips did; see
-- 'Haskoin.Store.Fleet.Pool.Change'.
data PoolChange = PoolChange
  { -- | transactions that come back
    returned :: ![PoolTx],
    -- | transactions of abandoned blocks that cannot come back
    gone :: ![ByteString],
    -- | transactions that the new blocks confirm
    confirmed :: ![ByteString],
    -- | outputs that the new blocks spend
    spent :: ![ByteString]
  }
  deriving (Eq, Show)

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
  run conn (R.eval poolScript [] args) >>= \case
    ["taken"] -> return Taken
    ["known"] -> return Known
    "conflict" : rivals -> return (Conflict rivals)
    "orphan" : outputs -> return (Orphan outputs)
    ["raced"] -> return Late
    other -> fail ("Unexpected answer of the pool script: " <> show (other :: [ByteString]))
  where
    flags = C.pack [if unspent then '1' else '0' | unspent <- unspents]
    args = ["offer", keys.prefix, num tsize, maybe "" id seen, flags, B.concat replaced] <> txArgs tx

-- | Take transactions out of the pool, with all that spends them. Gives
-- those that left.
poolWithdraw :: Connection -> FleetKeys -> Int -> [ByteString] -> IO [ByteString]
poolWithdraw conn keys tsize ts =
  run conn (R.eval poolScript [] ["withdraw", keys.prefix, num tsize, B.concat ts])

-- | Move the tip to a block and bring the pool there, in one step: given
-- the tip that the caller saw, the block, which must be the one that the
-- live stores agree on, and what the blocks in between did.
--
-- A store that cannot say what they did, because it does not have the old
-- tip or because the way from it is too long, gives 'Nothing', and the pool
-- is emptied: but only if no other store is live, as when a fleet comes
-- back after all its stores were away. The stores then offer their mempools
-- to the empty pool. With another store live the answer is 'Raced', and
-- the tip waits for a store that knows the way.
--
-- The last argument is the blocks that the tip moves through, oldest
-- first and the new tip last, for the events of the fleet ('FleetEvent'):
-- with none given, the new tip is the one block told.
fleetAdvance ::
  Connection ->
  FleetKeys ->
  Int ->
  Maybe Time ->
  Maybe ByteString ->
  ByteString ->
  Maybe PoolChange ->
  [ByteString] ->
  IO Moved
fleetAdvance conn keys tsize clock seen target known path =
  run conn (R.eval poolScript [] args) >>= \case
    ["moved"] -> return (MovedTo target)
    ["stays"] -> return Stays
    ["raced"] -> return Raced
    other -> fail ("Unexpected answer of the pool script: " <> show (other :: [ByteString]))
  where
    args =
      ["advance", keys.prefix, num tsize, maybe "" id seen, target, maybe "" num clock, num keys.size]
        <> [maybe "1" (const "0") known]
        <> [B.concat change.gone, B.concat change.confirmed, B.concat change.spent]
        <> [num (length change.returned)]
        <> concatMap txArgs change.returned
        <> [B.concat path]

    change = maybe (PoolChange [] [] [] []) id known

-- | Read from the pool as it is at a tip: given the size of a transaction
-- id, the tip that the caller answers from, what to read and its arguments,
-- as @read.lua@ takes them. 'Nothing' if the tip is another one by now.
poolRead :: Connection -> FleetKeys -> Int -> Maybe ByteString -> ByteString -> [ByteString] -> IO (Maybe [ByteString])
poolRead conn keys tsize tip what args =
  run conn (R.eval readScript [] (what : keys.prefix : num tsize : maybe "" id tip : args)) >>= \case
    "ok" : rows -> return (Just rows)
    ["raced"] -> return Nothing
    other -> fail ("Unexpected answer of the read script: " <> show (take 1 other))

-- | Something that happened to the fleet, as every store of it tells its
-- clients.
--
-- A store alone tells what its own node saw. The stores of a fleet answer
-- from one tip and one pool, and their events have to be those of that tip
-- and that pool: otherwise a client would hear of a transaction from one
-- server and find that the next does not have it. So the script that
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

-- | A transaction as the arguments of the pool script.
txArgs :: PoolTx -> [ByteString]
txArgs tx =
  [tx.txid, num tx.time, B.concat tx.inputs, tx.body, if tx.signals then "1" else "0", num (length tx.entries)]
    <> concatMap entry tx.entries
  where
    entry = \case
      PoolTouches a -> ["t", a, "", "", ""]
      PoolSpends a o v -> ["s", a, o, num v, ""]
      PoolPays a o v s -> ["o", a, o, num v, s]

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
reportScript :: ByteString
reportScript = $(makeRelativeToProject "src/Haskoin/Store/Fleet/lua/report.lua" >>= embedFile)

-- | 'Haskoin.Store.Fleet.move'.
moveScript :: ByteString
moveScript = $(makeRelativeToProject "src/Haskoin/Store/Fleet/lua/move.lua" >>= embedFile)

-- | What a store reads from the pool, each read with a check that the tip
-- is the one that the store answers from.
readScript :: ByteString
readScript = $(makeRelativeToProject "src/Haskoin/Store/Fleet/lua/read.lua" >>= embedFile)

-- | 'Haskoin.Store.Fleet.Pool.offer', 'Haskoin.Store.Fleet.Pool.withdraw',
-- and 'Haskoin.Store.Fleet.Pool.advance' with the move of the tip.
poolScript :: ByteString
poolScript = $(makeRelativeToProject "src/Haskoin/Store/Fleet/lua/pool.lua" >>= embedFile)
