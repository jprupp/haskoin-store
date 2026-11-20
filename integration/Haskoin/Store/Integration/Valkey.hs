-- |
-- Module      : Haskoin.Store.Integration.Valkey
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- A Redis server for the integration tests: Valkey, the Redis-compatible
-- server that Ubuntu ships, from the test image, in a container of
-- podman-test ("Test.Podman").
--
-- It listens on a Unix socket of its pod and on no port, so tests that run
-- at once each reach their own, and the store reaches it as @unix:PATH@
-- ('Haskoin.Store.Startup.redisConnectInfo').
module Haskoin.Store.Integration.Valkey
  ( redisContainer,
    redisURL,
  )
where

import Haskoin.Store.Integration.Image (testImage, valkeyBin)
import Test.Podman (Address (..), ContainerSpec (..), Endpoint (..), Pod, address, container, socketInside)

-- | Valkey, from the test image, on the socket @redis@ of its pod, open to
-- every user so that the tests can reach it from outside the container,
-- with nothing saved to disk, at most 128 MiB of data and 256 MiB of
-- memory.
redisContainer :: ContainerSpec
redisContainer =
  (container "redis" testImage)
    { entrypoint = Just valkeyBin,
      command =
        [ "--port",
          "0",
          "--unixsocket",
          socketInside "redis",
          "--unixsocketperm",
          "777",
          "--save",
          "",
          "--appendonly",
          "no",
          "--maxmemory",
          "128mb"
        ],
      memory = Just "256m",
      endpoints = [Native "redis"]
    }

-- | The URL of the Valkey of a pod, as the store reads it: @unix:PATH@.
redisURL :: Pod -> String
redisURL p = case address p "redis" of
  UnixSocket path -> "unix:" <> path
  TcpPort host port -> "redis://" <> host <> ":" <> show port
