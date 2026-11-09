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
-- outputs and extended public keys. A REST API built on these indices is in
-- "Haskoin.Store.Web". (The Redis cache of extended public keys is being
-- rewritten; until it is back, every query reads the database.)
--
-- = Running a store
--
-- 'withStore' starts everything from a 'StoreConfig': the database, the node
-- and the block store that imports data. The 'Store' it returns has a
-- 'DatabaseReader'; query it with the functions of
-- "Haskoin.Store.Database.Query", which take the reader and run in 'IO'.
--
-- @
-- 'withStore' cfg $ \\store ->
--   withSubscription store.pub $ \\events -> do
--     best <- Haskoin.Store.Database.Query.getBestBlock store.db
--     ...
-- @
--
-- = Modules
--
-- * "Haskoin.Store.Data": types of the data returned by the store and the API.
-- * "Haskoin.Store.Common": helpers for paging, ordering and deriving
--   addresses, and 'StoreRead', the queries of a store as a record of
--   functions in 'IO'.
-- * "Haskoin.Store.Database.Reader": opening the RocksDB database.
-- * "Haskoin.Store.Database.Query": queries against RocksDB, as plain
--   functions.
-- * "Haskoin.Store.Database.Mempool", "Haskoin.Store.Database.Migrate" and
--   "Haskoin.Store.Logic.Layered": a layout of the database in which the
--   mempool is kept apart from the chain, the migration to it, and the rules
--   that import blocks and transactions in it. The store does not use them
--   yet; see @docs\/layered-database.md@.
-- * "Haskoin.Store.BlockStore": the process that imports blocks and
--   transactions.
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
-- * __Plain functions in 'IO'.__ Queries take the database reader, or a
--   'StoreRead' record of them, which the web server builds with the Redis
--   cache in front of the queries about xpubs. Nothing runs in a stack of
--   monad transformers, except the conduits that merge sorted streams. The
--   web server runs on WAI with a router of its own.
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

    -- * Cache
    Cache,
    CacheConfig (..),
    cacheIsCached,
    cacheDelete,
    cacheEvict,

    -- * Store Reader
    StoreRead (..),
    databaseRead,
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
-- the instances of the store classes for the database reader
import Haskoin.Store.Database.Query (databaseRead)
import Haskoin.Store.Database.Reader
import Haskoin.Store.Database.Types
import Haskoin.Store.Manager
import Haskoin.Store.Web
