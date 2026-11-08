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
-- = Purpose
--
-- A block chain is easy to append to and hard to query. Bitcoin nodes store
-- blocks, and can answer "give me block N", but not "what is the balance of this
-- address" or "which transactions belong to this wallet". Wallets, block
-- explorers and payment processors all need those answers, quickly, from many
-- clients at once. Haskoin Store maintains the indices that make them cheap, and
-- serves them over HTTP.
--
-- = Design
--
-- * __No full node required.__ The store talks to the peer-to-peer network itself
--   through haskoin-node, so a deployment is one program and a database.
--
-- * __Every import is atomic.__ A block or a batch of mempool transactions is
--   applied as one RocksDB write. A crash leaves the database at the previous
--   block, never half-way through one, so there is no recovery step.
--
-- * __Indices are designed around reads.__ Keys are laid out so that every API
--   query is a seek and a short scan, newest first; see
--   "Haskoin.Store.Database.Types".
--
-- * __One query interface, several back ends.__ The database, the import buffer
--   and the Redis cache all implement 'StoreReadBase' and 'StoreReadExtra', so
--   the same query code runs against any of them.
--
-- * __The mempool is a first-class citizen.__ Unconfirmed transactions are indexed
--   like confirmed ones, ordered before them, and kept across restarts, because
--   wallets care about them most.
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
import Haskoin.Store.Common
import Haskoin.Store.Data
import Haskoin.Store.Database.Reader
import Haskoin.Store.Database.Types
import Haskoin.Store.Manager
import Haskoin.Store.Web
