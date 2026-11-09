-- |
-- Module      : Haskoin.Store.Integration.Podman
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Containers for the integration tests, run with Podman.
--
-- The integration tests need real nodes and a real Redis, which are easiest
-- to get, at pinned versions and without installing anything, as container
-- images. Podman runs them without a daemon and without root, so a developer
-- or a CI runner can use it as it is.
--
-- Everything is kept off the disk and cleaned up:
--
-- * containers run with @--rm@ and are removed with @podman rm --force@ when
--   the test that started them ends, even if it fails;
-- * the volumes that images declare are mounted as tmpfs
--   (@--image-volume tmpfs@), and data directories are tmpfs mounts of
--   limited size;
-- * every container has a memory limit;
-- * containers carry a label with the process ID of the test run, and
--   containers left by a test run that was killed (whose process is gone) are
--   removed before new ones start.
--
-- Ports are published on random ports of the loopback interface, so several
-- test runs can share a machine.
module Haskoin.Store.Integration.Podman
  ( podmanUsable,
    ContainerSpec (..),
    Container (..),
    withContainer,
    hostPort,
  )
where

import Control.Monad (forM_, void)
import Data.Char (isDigit, isSpace)
import Data.List (dropWhileEnd)
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))
import System.IO (hPutStrLn, stderr)
import System.Process (getCurrentPid, readProcessWithExitCode)
import UnliftIO (bracket, onException, timeout, tryAny)
import UnliftIO.Directory (doesPathExist)

-- | Whether Podman can run containers here, or why not. Set
-- @HASKOIN_STORE_NO_PODMAN@ to a value other than @0@ to skip the tests that
-- need it.
podmanUsable :: IO (Either String ())
podmanUsable = do
  skip <- lookupEnv "HASKOIN_STORE_NO_PODMAN"
  case skip of
    Just v | v `notElem` ["", "0"] -> return (Left "HASKOIN_STORE_NO_PODMAN is set")
    _ -> do
      r <-
        tryAny . timeout (60 * 1000 * 1000) $
          readProcessWithExitCode "podman" ["info", "--format", "{{.Host.Arch}}"] ""
      -- containers left by a test run that was killed are removed before
      -- anything else, even if this run fails before starting any
      case r of
        Right (Just (ExitSuccess, _, _)) -> removeStale
        _ -> return ()
      return $ case r of
        Left e -> Left ("Podman is not available: " <> show e)
        Right Nothing -> Left "podman info did not answer within a minute"
        Right (Just (ExitSuccess, _, _)) -> Right ()
        Right (Just (_, _, err)) -> Left ("Podman cannot run containers: " <> trim err)

-- | What to run.
data ContainerSpec = ContainerSpec
  { -- | short name, part of the container name
    name :: !String,
    -- | image, with a pinned tag
    image :: !String,
    -- | options of @podman run@, such as memory limits and tmpfs mounts
    options :: ![String],
    -- | command and arguments, after the image
    command :: ![String],
    -- | TCP ports to publish on random ports of 127.0.0.1
    ports :: ![Int]
  }

-- | A running container.
data Container = Container
  { -- | container ID
    cid :: !String,
    -- | container name
    name :: !String
  }

-- | Label on every container started by the tests. Its value is the process
-- ID of the test run.
testLabel :: String
testLabel = "haskoin-store-test"

-- | Run a container while an action runs, and remove it afterwards. If the
-- action throws, the last lines of the log of the container are printed to
-- help find out why.
withContainer :: ContainerSpec -> (Container -> IO a) -> IO a
withContainer spec act = do
  removeStale
  pid <- show <$> getCurrentPid
  let cname = testLabel <> "-" <> pid <> "-" <> spec.name
      args =
        ["run", "--detach", "--rm", "--name", cname]
          <> ["--label", testLabel <> "=" <> pid, "--image-volume", "tmpfs"]
          <> spec.options
          <> concatMap (\p -> ["--publish", "127.0.0.1::" <> show p]) spec.ports
          <> [spec.image]
          <> spec.command
  bracket (start cname args) stop $ \c -> act c `onException` logs c
  where
    start cname args = do
      (code, out, err) <- readProcessWithExitCode "podman" args ""
      case code of
        ExitSuccess -> return Container {cid = trim out, name = cname}
        _ -> do
          -- a container may exist even though the command failed
          void $ readProcessWithExitCode "podman" ["rm", "--force", cname] ""
          fail ("podman run " <> spec.image <> " failed: " <> trim err)
    stop :: Container -> IO ()
    stop c =
      void $ readProcessWithExitCode "podman" ["rm", "--force", "--time", "2", c.cid] ""
    logs :: Container -> IO ()
    logs c = do
      (_, out, err) <- readProcessWithExitCode "podman" ["logs", "--tail", "40", c.cid] ""
      hPutStrLn stderr $ "--- last lines of the log of " <> c.name <> " ---\n" <> out <> err

-- | Remove the containers of test runs whose process is gone.
removeStale :: IO ()
removeStale = do
  (code, out, _) <-
    readProcessWithExitCode
      "podman"
      [ "ps",
        "--all",
        "--filter",
        "label=" <> testLabel,
        "--format",
        "{{.ID}} {{index .Labels \"" <> testLabel <> "\"}}"
      ]
      ""
  case code of
    ExitSuccess ->
      forM_ (map words (lines out)) $ \case
        [cid, pid] | all isDigit pid -> do
          alive <- doesPathExist ("/proc/" <> pid)
          if alive
            then return ()
            else void $ readProcessWithExitCode "podman" ["rm", "--force", cid] ""
        _ -> return ()
    _ -> return ()

-- | Port of the host where a port of a container is published.
hostPort :: Container -> Int -> IO Int
hostPort c p = do
  (code, out, err) <- readProcessWithExitCode "podman" ["port", c.cid, show p <> "/tcp"] ""
  case (code, lines out) of
    (ExitSuccess, l : _)
      | [(n, "")] <- reads (reverse (takeWhile (/= ':') (reverse (trim l)))) ->
          return n
    _ -> fail ("Cannot find the published port " <> show p <> " of " <> c.name <> ": " <> trim err)

trim :: String -> String
trim = dropWhileEnd isSpace . dropWhile isSpace
