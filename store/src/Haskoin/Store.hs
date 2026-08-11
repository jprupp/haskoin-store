{-# LANGUAGE DuplicateRecordFields #-}

module Haskoin.Store
  ( Store (..),
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

    -- * Useful Fuctions
    getTransaction,
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
