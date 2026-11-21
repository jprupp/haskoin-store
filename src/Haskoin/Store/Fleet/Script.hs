-- |
-- Module      : Haskoin.Store.Fleet.Script
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Lua scripts that Redis keeps compiled, called by their digest: what
-- every module that runs a script of the fleet shares.
module Haskoin.Store.Fleet.Script
  ( Script,
    script,
    evalScript,
  )
where

import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Database.Redis (Redis, RedisResult, Reply (..))
import Database.Redis qualified as R
import System.IO.Unsafe (unsafePerformIO)

-- | A Lua script that Redis keeps compiled under its digest.
--
-- @EVAL@ sends the whole script with every call, and Redis hashes it every
-- time to find it compiled: for the scripts of the pool, several kilobytes
-- for a read of a few bytes. That was most of what Redis did for a simple
-- request, and Redis has one thread for the whole fleet. So a script is
-- loaded once, its digest kept here, and called by it with @EVALSHA@. A
-- Redis that lost its scripts, as one that restarted, says @NOSCRIPT@, and
-- the script is loaded again. The digest is the same on every Redis, so
-- one is kept for the program, not for each connection.
data Script = Script
  { source :: !ByteString,
    digest :: !(IORef (Maybe ByteString))
  }

-- | A script of this source. Only for a top-level binding marked NOINLINE,
-- so that there is one of each script.
script :: ByteString -> Script
script s = unsafePerformIO (Script s <$> newIORef Nothing)
{-# NOINLINE script #-}

-- | Run a script by its digest, as @EVAL@ runs one by its source.
evalScript :: (RedisResult a) => Script -> [ByteString] -> [ByteString] -> Redis (Either Reply a)
evalScript sc keys args =
  liftIO (readIORef sc.digest) >>= \case
    Nothing -> load
    Just sha ->
      R.evalsha sha keys args >>= \case
        Left (Error e) | "NOSCRIPT" `B.isPrefixOf` e -> load
        r -> return r
  where
    load =
      R.scriptLoad sc.source >>= \case
        Left e -> return (Left e)
        Right sha -> do
          liftIO (writeIORef sc.digest (Just sha))
          R.evalsha sha keys args

