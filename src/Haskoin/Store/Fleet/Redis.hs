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
-- one step. Nothing in the store calls this yet.
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
    poolOffer,
    poolWithdraw,
    fleetAdvance,
    poolSpends,
    poolBody,
  )
where

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.FileEmbed (embedFile, makeRelativeToProject)
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

-- | What the blocks between two tips did; see
-- 'Haskoin.Store.Fleet.Pool.Change'.
data PoolChange = PoolChange
  { -- | transactions that come back, each with the outputs that it spends
    -- and its body
    returned :: ![(ByteString, [ByteString], ByteString)],
    -- | transactions of abandoned blocks that cannot come back
    gone :: ![ByteString],
    -- | transactions that the new blocks confirm
    confirmed :: ![ByteString],
    -- | outputs that the new blocks spend
    spent :: ![ByteString]
  }
  deriving (Eq, Show)

-- | Offer a transaction to the pool: given the size of a transaction id,
-- the tip that the store checked against, the id, each output that the
-- transaction spends with whether the store finds it unspent at that tip,
-- the body to keep, and the transactions that it replaces.
poolOffer ::
  Connection ->
  FleetKeys ->
  Int ->
  Maybe ByteString ->
  ByteString ->
  [(ByteString, Bool)] ->
  ByteString ->
  [ByteString] ->
  IO Offered
poolOffer conn keys tsize seen t inputs body replaced =
  run conn (R.eval poolScript [] args) >>= \case
    ["taken"] -> return Taken
    ["known"] -> return Known
    "conflict" : rivals -> return (Conflict rivals)
    "orphan" : outputs -> return (Orphan outputs)
    ["raced"] -> return Late
    other -> fail ("Unexpected answer of the pool script: " <> show (other :: [ByteString]))
  where
    flags = C.pack [if unspent then '1' else '0' | (_, unspent) <- inputs]
    args =
      ["offer", keys.prefix, num tsize, maybe "" id seen, t, B.concat (map fst inputs), flags, body]
        <> [B.concat replaced]

-- | Take transactions out of the pool, with all that spends them. Gives
-- those that left.
poolWithdraw :: Connection -> FleetKeys -> Int -> [ByteString] -> IO [ByteString]
poolWithdraw conn keys tsize ts =
  run conn (R.eval poolScript [] ["withdraw", keys.prefix, num tsize, B.concat ts])

-- | Move the tip to a block and bring the pool there, in one step: given
-- the tip that the caller saw, the block, which must be the one that the
-- live stores agree on, and what the blocks in between did.
fleetAdvance :: Connection -> FleetKeys -> Int -> Maybe Time -> Maybe ByteString -> ByteString -> PoolChange -> IO Moved
fleetAdvance conn keys tsize clock seen target change =
  run conn (R.eval poolScript [] args) >>= \case
    ["moved"] -> return (MovedTo target)
    ["stays"] -> return Stays
    ["raced"] -> return Raced
    other -> fail ("Unexpected answer of the pool script: " <> show (other :: [ByteString]))
  where
    args =
      ["advance", keys.prefix, num tsize, maybe "" id seen, target, maybe "" num clock, num keys.size]
        <> [num (length change.returned)]
        <> concat [[t, B.concat inputs, body] | (t, inputs, body) <- change.returned]
        <> [B.concat change.gone, B.concat change.confirmed, B.concat change.spent]

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

-- | 'Haskoin.Store.Fleet.Pool.offer', 'Haskoin.Store.Fleet.Pool.withdraw',
-- and 'Haskoin.Store.Fleet.Pool.advance' with the move of the tip.
poolScript :: ByteString
poolScript = $(makeRelativeToProject "src/Haskoin/Store/Fleet/lua/pool.lua" >>= embedFile)
