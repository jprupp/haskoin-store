-- |
-- Module      : Haskoin.Store.Integration.Valkey
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- A Redis server for the integration tests and the stress test: Valkey, the
-- Redis-compatible server that Ubuntu ships, from the test image.
module Haskoin.Store.Integration.Valkey
  ( redisContainer,
    waitRedis,
  )
where

import Database.Redis qualified as Redis
import Haskoin.Store.Integration.Image (valkeyBin)
import Haskoin.Store.Integration.Podman (ContainerSpec (..))
import UnliftIO (bracket, tryAny)
import UnliftIO.Concurrent (threadDelay)

-- | Valkey, the Redis-compatible server of Ubuntu, from the test image, listening on every interface of the container
-- (only its published port is reachable, on 127.0.0.1), with nothing saved
-- to disk and at most 128 MiB of data.
redisContainer :: String -> ContainerSpec
redisContainer image =
  ContainerSpec
    { name = "redis",
      image = image,
      options = ["--memory", "256m", "--entrypoint", valkeyBin],
      command =
        [ "--bind",
          "0.0.0.0",
          "--protected-mode",
          "no",
          "--save",
          "",
          "--appendonly",
          "no",
          "--maxmemory",
          "128mb"
        ],
      ports = [6379]
    }

-- | Wait until Redis answers, for up to 30 seconds.
waitRedis :: String -> IO ()
waitRedis url = go (120 :: Int)
  where
    info = either error id (Redis.parseConnectInfo url)
    go n =
      tryAny (bracket (Redis.checkedConnect info) Redis.disconnect (const (return ()))) >>= \case
        Right () -> return ()
        Left e
          | n <= 0 -> fail ("Redis did not start: " <> show e)
          | otherwise -> threadDelay 250000 >> go (n - 1)
