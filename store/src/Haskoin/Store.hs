{-# LANGUAGE DuplicateRecordFields #-}

-- |
-- Module      : Haskoin.Store
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- A block chain indexer for Bitcoin (BTC) and Bitcoin Cash (BCH).
--
-- Haskoin Store connects to the peer-to-peer network with
-- <https://hackage.haskell.org/package/haskoin-node haskoin-node>, downloads
-- blocks and mempool transactions, and indexes them in RocksDB. The indices
-- answer questions about blocks, transactions, address balances, unspent
-- outputs and extended public keys. An optional Redis cache speeds up queries
-- about extended public keys with many addresses. A REST API built on these
-- indices is in "Haskoin.Store.Web".
--
-- = Running a store
--
-- 'withStore' starts everything from a 'StoreConfig': the database, the node,
-- the block store that imports data, and the cache if configured. The 'Store'
-- it returns has a 'DatabaseReader' and, if the cache is on, a 'CacheConfig'.
-- Run queries with 'withDB', or inside 'withCache' to use the cache.
--
-- @
-- 'withStore' cfg $ \\store ->
--   'withSubscription' store.pub $ \\events -> do
--     best <- 'withDB' store.db 'getBestBlock'
--     ...
-- @
--
-- = Modules
--
-- * "Haskoin.Store.Data": types of the data returned by the store and the API.
-- * "Haskoin.Store.Common": the 'StoreReadBase' and 'StoreReadExtra' query
--   interfaces.
-- * "Haskoin.Store.Database.Reader": queries against RocksDB.
-- * "Haskoin.Store.BlockStore": the process that imports blocks and
--   transactions.
-- * "Haskoin.Store.Cache": the Redis cache for extended public keys.
-- * "Haskoin.Store.Web": the REST API server.
module Haskoin.Store
  ( -- * Store
    Store (..),
    StoreConfig (..),
    StoreEvent (..),
    withStore,
    module Haskoin.Store.BlockStore,
    module Haskoin.Store.Web,
    module Haskoin.Store.Database.Reader,
    module Haskoin.Store.Database.Types,
    module Haskoin.Store.Data,

    -- * Cache
    CacheConfig (..),
    CacheT,
    CacheError (..),
    withCache,
    connectRedis,
    isInCache,

    -- * Store Reader
    StoreReadBase (..),
    StoreReadExtra (..),
    Limits (..),
    Start (..),

    -- * Useful Functions
    getTransaction,
    getTransactions,
    getDefaultBalance,
    getActiveTxData,
    blockAtOrBefore,
  )
where

import Haskoin.Store.BlockStore
import Haskoin.Store.Cache
import Haskoin.Store.Common
import Haskoin.Store.Data
import Haskoin.Store.Database.Reader
import Haskoin.Store.Database.Types
import Haskoin.Store.Manager
import Haskoin.Store.Web
