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
module Haskoin.Store.Fleet.Redis
  ( FleetKeys (..),
    fleetKeys,
    Moved (..),
    fleetReport,
    fleetMove,
    fleetTip,
    fleetLive,
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
  { -- | the fleet tip: a block
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
    { tip = prefix <> "fleet:tip",
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
