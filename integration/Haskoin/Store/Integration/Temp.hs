-- |
-- Module      : Haskoin.Store.Integration.Temp
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Temporary directories for the tests and the benchmark:
-- made for an action, and removed with all that is in them when it ends.
--
-- These are the functions of the same name of "System.IO.Temp", with one
-- difference: the directory is removed by the 'bracket' of
-- "Haskoin.Store.Exception", whose cleanup cannot be cut short. A test that
-- is stopped from outside while it removes its database, by a timeout or by
-- a second Ctrl-C, still removes all of it, where the others could leave
-- half of it behind. A database of a test is tens of megabytes, often in
-- memory ('withRamDir'), so what is left
-- behind matters.
module Haskoin.Store.Integration.Temp
  ( withSystemTempDirectory,
    withTempDirectory,
    withRamDir,
  )
where

import Control.Exception (IOException, handle)
import Haskoin.Store.Exception (bracket)
import System.Directory (doesDirectoryExist, getTemporaryDirectory, removeDirectoryRecursive)
import System.Environment (lookupEnv)
import System.IO.Temp (createTempDirectory, getCanonicalTemporaryDirectory)

-- | Run an action with a new directory under the temporary directory of
-- the system, and remove it afterwards, however the action ends. The name
-- of the directory starts with the text given.
withSystemTempDirectory :: String -> (FilePath -> IO a) -> IO a
withSystemTempDirectory template action = do
  tmp <- getCanonicalTemporaryDirectory
  withTempDirectory tmp template action

-- | 'withSystemTempDirectory' for a directory under the one given. A
-- failure to remove the directory is ignored: the result or the error of
-- the action is what matters to the caller.
withTempDirectory :: FilePath -> String -> (FilePath -> IO a) -> IO a
withTempDirectory parent template = bracket (createTempDirectory parent template) remove
  where
    remove = handle (\(_ :: IOException) -> return ()) . removeDirectoryRecursive

-- | A temporary directory in memory: under @/dev/shm@ if there is one, else
-- under @$XDG_RUNTIME_DIR@, else the usual temporary directory. The
-- databases of the integration tests are there, so that their writes cost
-- no disk.
withRamDir :: (FilePath -> IO a) -> IO a
withRamDir f = do
  shm <- doesDirectoryExist "/dev/shm"
  xdg <- lookupEnv "XDG_RUNTIME_DIR"
  tmp <- getTemporaryDirectory
  let base
        | shm = "/dev/shm"
        | Just x <- xdg = x
        | otherwise = tmp
  withTempDirectory base "haskoin-store-test" f
