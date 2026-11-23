{-# LANGUAGE TemplateHaskell #-}

-- |
-- Module      : Haskoin.Store.XPub.Lock
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- One store of a fleet indexes an xpub at a time: a lock per xpub in the
-- Redis that the stores share, taken by the indexer
-- ("Haskoin.Store.XPub.Indexer") when it queues the xpub and let go when
-- the job has written the entry (@docs\/xpub-indexing.md@, The indexer).
--
-- = Why
--
-- A load balancer spreads a burst of requests for one xpub over every
-- store of a fleet. Without a lock, each store reads the xpub from its own
-- database, for each request that comes before the first of them has
-- cached it. With it, the first store to queue the xpub reads it, once, and
-- the others tell their clients when to come back: the value of the lock
-- says when its holder expects to be done.
--
-- = The key
--
-- @\<prefix\>idx:\<id\>@, with the prefix of the fleet and the id of
-- 'Haskoin.Store.Fleet.XPub.xpubId', holding @\<token\> \<done\> \<store\>@: a random
-- token of the holder, when it expects the job to be done in milliseconds
-- of the clock of Redis, and the name of the store. The scripts read the
-- clock of Redis, so that stores whose clocks differ agree on the time.
--
-- * __Taken when the xpub is queued__ ('acquire'), with @SET NX PX@, not
--   when the job starts, so that the other stores stop at once. A store
--   that loses the race is told who holds it and until when.
-- * __It lasts 30 seconds, renewed every ten__ ('renew') for every job the
--   store holds, with a new estimate of when it will be done. A store that
--   dies lets its locks go within half a minute.
-- * __Let go by its holder only__ ('release'): a script deletes the key if
--   it still holds the holder's token, so a store whose lock expired and
--   was taken by another does not delete the other's.
--
-- The lock is for efficiency, not for the safety of the cache: the scripts
-- of "Haskoin.Store.Fleet.XPub" keep an entry right whoever writes it, and a store
-- that has lost its lock, because it could not renew it in time, writes
-- what it read as safely as any other.
--
-- It replaces the lock of the old cache of a store alone that was held for the time
-- of a reload only (@lock:\<id\>@, two minutes, not renewed): taken that
-- late, it let several stores read the same xpub for requests that came at
-- the same time.
--
-- Every store is a member of a fleet and takes its locks in Redis. The
-- tests of the indexer use 'noLock', which always gives the lock.
module Haskoin.Store.XPub.Lock
  ( Lock (..),
    Taken (..),
    noLock,
    LockConfig (..),
    defaultLockConfig,
    redisLock,
    lockKey,
  )
where

import Control.Monad (forM, void)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as C8
import Data.FileEmbed (embedFile, makeRelativeToProject)
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Maybe (catMaybes)
import Data.Word (Word64)
import Database.Redis (Connection, Redis, Reply)
import Database.Redis qualified as R
import System.Random (randomIO)
import Text.Read (readMaybe)

-- | The lock of a key, in a fleet or not. Times are in microseconds from
-- now.
data Lock k = Lock
  { -- | take the lock of a key, saying in how long the job should be done
    acquire :: k -> Int -> IO Taken,
    -- | renew the locks of these keys that this store holds, each with a
    -- new estimate; returns how many it no longer held
    renew :: [(k, Int)] -> IO Int,
    -- | let go of the lock of a key, if this store holds it
    release :: k -> IO (),
    -- | how long until the holder of the lock of a key expects to be done,
    -- and its name, if the key is locked
    holder :: k -> IO (Maybe (Int, ByteString)),
    -- | how often to renew the locks held
    every :: !Int
  }

-- | What came of trying to take a lock.
data Taken
  = -- | this store holds it now
    Took
  | -- | another store holds it, and expects to be done in so many
    -- microseconds, zero or less if it is late
    Held !Int
  deriving (Eq, Show)

-- | A lock that is always given, never held by another: for the tests.
noLock :: Lock k
noLock =
  Lock
    { acquire = \_ _ -> return Took,
      renew = \_ -> return 0,
      release = \_ -> return (),
      holder = \_ -> return Nothing,
      every = 10000000
    }

-- | Settings of the lock in Redis.
data LockConfig = LockConfig
  { -- | prefix of the keys: the prefix of the fleet's keys
    prefix :: !ByteString,
    -- | name of this store, in the value of the lock, for those who look
    name :: !ByteString,
    -- | milliseconds that a lock lasts if it is not renewed; it is renewed
    -- three times as often
    ttl :: !Int
  }

-- | A lock of 30 seconds with these prefix and name.
defaultLockConfig :: ByteString -> ByteString -> LockConfig
defaultLockConfig prefix name = LockConfig {prefix, name, ttl = 30000}

-- | The key of the lock of an xpub, by the id of the xpub
-- ('Haskoin.Store.Fleet.XPub.xpubId').
lockKey :: ByteString -> ByteString -> ByteString
lockKey prefix i = prefix <> "idx:" <> i

-- | Locks in Redis, for keys with these ids. The store keeps the token of
-- each lock it holds, to renew and let go of it.
redisLock :: LockConfig -> Connection -> (k -> ByteString) -> IO (Lock k)
redisLock cfg conn ident = do
  tokens <- newIORef HashMap.empty
  let key = lockKey cfg.prefix . ident
      ms us = num (max 0 us `div` 1000)
      acquire k est = do
        token <- num <$> (randomIO :: IO Word64)
        r <- run (R.eval takeScript [key k] [token, ms est, cfg.name, num cfg.ttl])
        case r of
          ["ok"] -> do
            atomicModifyIORef' tokens (\m -> (HashMap.insert (ident k) token m, ()))
            return Took
          "held" : left : _ -> return (Held (maybe 0 (* 1000) (readMaybe (C8.unpack left))))
          _ -> fail ("Unexpected answer of Redis to the lock of an xpub: " <> show r)
      renew ks = do
        m <- readIORef tokens
        let held = catMaybes [(\t -> (key k, t, ms est)) <$> HashMap.lookup (ident k) m | (k, est) <- ks]
        fmap sum . forM (chunks held) $ \ch ->
          fromInteger
            <$> run
              ( R.eval
                  renewScript
                  [k | (k, _, _) <- ch]
                  (num cfg.ttl : concat [[t, e] | (_, t, e) <- ch])
              )
      release k = do
        t <- atomicModifyIORef' tokens (\m -> (HashMap.delete (ident k) m, HashMap.lookup (ident k) m))
        case t of
          Nothing -> return ()
          Just token -> void (run (R.eval releaseScript [key k] [token] :: Redis (Either Reply Integer)))
      holder k =
        run (R.eval peekScript [key k] []) >>= \case
          [left, who] | Just n <- readMaybe (C8.unpack left) -> return (Just (n * 1000, who))
          _ -> return Nothing
  return Lock {acquire, renew, release, holder, every = cfg.ttl * 1000 `div` 3}
  where
    run :: Redis (Either Reply a) -> IO a
    run f =
      R.runRedis conn f >>= \case
        Right x -> return x
        Left e -> fail ("Redis error: " <> show e)
    chunks [] = []
    chunks xs = let (a, b) = splitAt 500 xs in a : chunks b

num :: (Integral a) => a -> ByteString
num = C8.pack . show . toInteger

-- | Take a lock. Keys: the lock. Arguments: the token, milliseconds until
-- the job should be done, the name of the store, milliseconds that the lock
-- lasts. Returns @{\"ok\"}@ if it was taken, or @{\"held\", left, name}@:
-- milliseconds until the holder expects to be done, by the clock of Redis,
-- and its name.
takeScript :: ByteString
takeScript = $(makeRelativeToProject "src/Haskoin/Store/XPub/lua/take.lua" >>= embedFile)

-- | Renew locks. Keys: the locks. Arguments: milliseconds that a lock
-- lasts, then for each key its token and milliseconds until its job should
-- be done. A lock whose key no longer holds the token is not touched.
-- Returns how many were not held.
renewScript :: ByteString
renewScript = $(makeRelativeToProject "src/Haskoin/Store/XPub/lua/renew.lua" >>= embedFile)

-- | Delete a lock if it holds the token. Keys: the lock. Arguments: the
-- token. Returns 1 if it was deleted.
releaseScript :: ByteString
releaseScript = $(makeRelativeToProject "src/Haskoin/Store/XPub/lua/release.lua" >>= embedFile)

-- | Read a lock. Keys: the lock. Returns nothing if it is not held, or
-- milliseconds until its holder expects to be done, and its name.
peekScript :: ByteString
peekScript = $(makeRelativeToProject "src/Haskoin/Store/XPub/lua/peek.lua" >>= embedFile)
