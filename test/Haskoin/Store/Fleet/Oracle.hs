{-# LANGUAGE TemplateHaskell #-}

-- | The script that kept the shared mempool before its rules were written
-- in Haskell ("Haskoin.Store.Fleet.Pool.Decide"), with the functions that
-- called it, as they were. No store runs it any more. The specs run it
-- beside the decisions, under another prefix of the same Redis, and
-- compare all that Redis has under each after every step: the script is
-- what the stores ran for months, so that the two do the same is what says
-- that nothing changed for them.
module Haskoin.Store.Fleet.Oracle
  ( luaOffer,
    luaWithdraw,
    luaExpire,
    luaCommit,
    luaRestart,

    -- * The cache of extended public keys
    luaBuild,
    luaBegin,
    luaPiece,
    luaAbort,
    luaDrop,
    luaEvict,
    luaHave,
  )
where

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.FileEmbed (embedFile, makeRelativeToProject)
import Data.Maybe (fromMaybe)
import Database.Redis (Connection, Redis, Reply)
import Database.Redis qualified as R
import Haskoin.Store.Fleet (Time)
import Haskoin.Store.Fleet.Redis
import Haskoin.Store.Fleet.XPub.Decide (Addition (..), Began (..), Piece (..), Step (..))
import Haskoin.Store.XPub (greedyLua)

luaOffer :: Connection -> FleetKeys -> Int -> Maybe ByteString -> PoolTx -> [Bool] -> [ByteString] -> IO Offered
luaOffer conn keys tsize seen tx unspents replaced =
  run conn (evalScript poolScript [] args) >>= \case
    ["taken"] -> return Taken
    ["known"] -> return Known
    "conflict" : rivals -> return (Conflict rivals)
    "orphan" : outputs -> return (Orphan outputs)
    ["raced"] -> return Late
    other -> fail ("Unexpected answer of the pool script: " <> show (other :: [ByteString]))
  where
    flags = C.pack [if unspent then '1' else '0' | unspent <- unspents]
    args = ["offer", keys.prefix, num tsize, fromMaybe "" seen, flags, B.concat replaced] <> txArgs tx

luaWithdraw :: Connection -> FleetKeys -> Int -> [ByteString] -> IO [ByteString]
luaWithdraw conn keys tsize ts =
  run conn (evalScript poolScript [] ["withdraw", keys.prefix, num tsize, B.concat ts])

luaExpire :: Connection -> FleetKeys -> Int -> Integer -> Int -> IO [ByteString]
luaExpire conn keys tsize before most =
  run conn (evalScript poolScript [] ["expire", keys.prefix, num tsize, C.pack (show before), num most])

-- | The commit of a move of the tip: by a store that says what the blocks
-- did, or by one that does not know and may empty the pool if it is alone.
luaCommit ::
  Connection ->
  FleetKeys ->
  Int ->
  Maybe Time ->
  ByteString ->
  Maybe ByteString ->
  ByteString ->
  Maybe PoolChange ->
  [ByteString] ->
  ByteString ->
  IO Moved
luaCommit conn keys tsize clock store seen target known path token =
  advance conn keys tsize clock seen target how (fromMaybe unchanged known) (path, token)
  where
    how = maybe ("1|" <> store) (const "0") known
    unchanged = PoolChange [] [] [] [] Nothing []

-- | A move of the tip that empties the pool, the way being too long.
luaRestart :: Connection -> FleetKeys -> Int -> Maybe Time -> Maybe ByteString -> ByteString -> IO Moved
luaRestart conn keys tsize clock seen target =
  advance conn keys tsize clock seen target "2" (PoolChange [] [] [] [] Nothing []) ([], "")

advance ::
  Connection ->
  FleetKeys ->
  Int ->
  Maybe Time ->
  Maybe ByteString ->
  ByteString ->
  ByteString ->
  PoolChange ->
  ([ByteString], ByteString) ->
  IO Moved
advance conn keys tsize clock seen target how change (path, token) =
  run conn (evalScript poolScript [] args) >>= \case
    ["moved"] -> return (MovedTo target)
    ["stays"] -> return Stays
    ["raced"] -> return Raced
    other -> fail ("Unexpected answer of the pool script: " <> show (other :: [ByteString]))
  where
    args =
      ["advance", keys.prefix, num tsize, fromMaybe "" seen, target, maybe "" num clock, num keys.size, how]
        <> [maybe "" num change.height]
        <> [B.concat change.gone, B.concat change.confirmed, B.concat change.spent, B.concat change.lost]
        <> [num (length change.returned)]
        <> concatMap txArgs change.returned
        <> [B.concat path, token]

-- | A transaction as the arguments of the pool script.
txArgs :: PoolTx -> [ByteString]
txArgs tx =
  -- the script took whether a transaction says that it can be replaced by
  -- fee, and kept which could be: no transaction says so any more
  [tx.txid, num tx.time, B.concat tx.inputs, tx.body, "0", num (length tx.entries)]
    <> concatMap entry tx.entries
  where
    entry = \case
      PoolTouches a -> ["t", a, "", "", "", ""]
      PoolSpends a o v -> ["s", a, o, num v, "", ""]
      PoolPays a o v s -> ["o", a, o, num v, s, ""]
      PoolMakes k -> token (if k.nft then "M" else "m") k
      PoolUses k -> token (if k.nft then "U" else "u") k
    token kind k =
      [kind, fromMaybe "" k.address, k.output, num k.amount, maybe "" coins k.coins, k.category]
    coins (v, s) = num v <> " " <> s

num :: (Show a) => a -> ByteString
num = C.pack . show

run :: Connection -> Redis (Either Reply a) -> IO a
run conn f = R.runRedis conn f >>= either (fail . ("Redis error: " <>) . show) return

poolScript :: Script
poolScript = script $(makeRelativeToProject "test/Haskoin/Store/Fleet/pool.lua" >>= embedFile)
{-# NOINLINE poolScript #-}

-- | The script of the cache of extended public keys as it was when it
-- wrote as well as read ("Haskoin.Store.Fleet.XPub.Decide" writes now).
xpubScript :: Script
xpubScript = script (greedyLua <> $(makeRelativeToProject "test/Haskoin/Store/Fleet/xpub.lua" >>= embedFile))
{-# NOINLINE xpubScript #-}

xpub :: Connection -> FleetKeys -> ByteString -> [ByteString] -> IO [ByteString]
xpub conn keys what args = run conn (evalScript xpubScript [] (what : keys.prefix : "4" : args))

-- | A whole number of a score, as the script takes it.
whole :: Double -> ByteString
whole s = num (round s :: Integer)

luaHave :: Connection -> FleetKeys -> ByteString -> IO (Integer, Integer)
luaHave conn keys i =
  run conn (evalScript xpubScript [] ["have", keys.prefix, "4", i]) >>= \case
    [n0, n1] -> return (n0, n1)
    other -> fail ("Unexpected answer of the xpub script: " <> show (other :: [Integer]))

luaBuild ::
  Connection -> FleetKeys -> ByteString -> ByteString -> (Integer, Integer) -> Integer -> [Addition] -> IO Step
luaBuild conn keys tip i (n0, n1) cost additions =
  xpub conn keys "build" ([tip, i, num n0, num n1, num cost, num (length additions)] <> concatMap one additions)
    >>= \case
      ["ok"] -> return StepAdded
      ["raced"] -> return StepRaced
      ["behind"] -> return StepBehind
      ["changed"] -> return StepChanged
      ["busy"] -> return StepBusy
      other -> fail ("Unexpected answer of the xpub script: " <> show other)
  where
    one (a :: Addition) =
      [a.address, a.path, a.balance, num (length a.txs)]
        <> concat [[t, whole s] | (t, s) <- a.txs]
        <> [num (length a.outs)]
        <> concat [[o, whole s, amount, sc] | (o, s, amount, sc) <- a.outs]

luaBegin :: Connection -> FleetKeys -> ByteString -> ByteString -> IO Began
luaBegin conn keys token seen =
  xpub conn keys "begin" [token, seen] >>= \case
    ["ok"] -> return BeganMove
    ["busy"] -> return OtherMoving
    ["raced"] -> return TipElsewhere
    other -> fail ("Unexpected answer of the xpub script: " <> show other)

luaPiece :: Connection -> FleetKeys -> ByteString -> ByteString -> [Piece] -> IO Bool
luaPiece conn keys token seen pieces =
  xpub conn keys "piece" ([token, seen, num (length pieces)] <> concatMap one pieces) >>= \case
    ["ok"] -> return True
    ["raced"] -> return False
    other -> fail ("Unexpected answer of the xpub script: " <> show other)
  where
    one (p :: Piece) =
      [p.address, p.balance, num (length p.abandoned)]
        <> p.abandoned
        <> [num (length p.added)]
        <> concat [[t, whole s] | (t, s) <- p.added]
        <> [num (length p.gone)]
        <> p.gone
        <> [num (length p.unspent)]
        <> concat [[o, whole s, amount, sc] | (o, s, amount, sc) <- p.unspent]

luaAbort :: Connection -> FleetKeys -> ByteString -> IO ()
luaAbort conn keys token = () <$ xpub conn keys "abort" [token]

luaDrop :: Connection -> FleetKeys -> ByteString -> IO ()
luaDrop conn keys i = () <$ xpub conn keys "drop" [i]

luaEvict :: Connection -> FleetKeys -> Int -> IO Integer
luaEvict conn keys most = run conn (evalScript xpubScript [] ["evict", keys.prefix, "4", num most])
