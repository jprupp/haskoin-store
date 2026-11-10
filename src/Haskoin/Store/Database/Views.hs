-- |
-- Module      : Haskoin.Store.Database.Views
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The database as it was at each of its recent blocks: a RocksDB snapshot
-- for each, kept until it is no longer needed. Nothing in the store uses
-- this yet. It is the first part of the store's side of "A fleet that
-- answers as one" in @docs\/layered-database.md@.
--
-- = Why
--
-- Stores that share a Redis all answer from one block, the fleet tip
-- ("Haskoin.Store.Fleet"), which is usually not the newest block that a
-- store has: the tip waits for the slowest store. So a store has to be able
-- to answer from a block that it has already built on, and a snapshot is how
-- RocksDB gives the database as it was. A snapshot cannot be taken of the
-- past, only of the present, so the store takes one as each block is
-- imported and keeps it.
--
-- = Letting go
--
-- RocksDB holds on to every version of a key that a snapshot can see, so
-- snapshots are let go as soon as the rules of the fleet allow ('keepViews').
-- A request may be reading through one at that moment. Releasing it then
-- would hand RocksDB freed memory, so each view counts its readers, and one
-- that is let go while it is read is released when the last reader is done.
--
-- The blocks can be of any type that can be compared, as in
-- "Haskoin.Store.Fleet".
module Haskoin.Store.Database.Views
  ( Views,
    newViews,
    closeViews,
    takeView,
    heldViews,
    keepViews,
    withViewAt,
  )
where

import Control.Monad (forM_, when)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Database.RocksDB (DB, Snapshot, createSnapshot, releaseSnapshot)
import Haskoin.Store.Database.Reader (DatabaseReader (..))
import UnliftIO
  ( TVar,
    atomically,
    bracket,
    mask_,
    newTVarIO,
    readTVar,
    readTVarIO,
    stateTVar,
    writeTVar,
  )

-- | The views of a database, by block.
data Views b = Views
  { reader :: !DatabaseReader,
    held :: !(TVar (Map b Held))
  }

-- | A snapshot, the number of readers that are using it, and whether it is
-- to be released when the last of them is done.
data Held = Held
  { snapshot :: !(DB, Snapshot),
    readers :: !Int,
    doomed :: !Bool
  }

-- | No views yet, of a database.
newViews :: DatabaseReader -> IO (Views b)
newViews reader = Views reader <$> newTVarIO Map.empty

-- | Take a view of the database as it is now, for a block. The caller must
-- be the one that writes to the database, between the import of that block
-- and its next write, so that the view is of that block and nothing else.
-- A view that the block already has is kept, and is no longer to be
-- released: a block that comes back after a reorganisation is the same
-- block.
takeView :: (Ord b) => Views b -> b -> IO ()
takeView views block = mask_ $ do
  fresh <- createSnapshot views.reader.db
  spare <- atomically . stateTVar views.held $ \m -> case Map.lookup block m of
    Just old -> (True, Map.insert block old {doomed = False} m)
    Nothing -> (False, Map.insert block Held {snapshot = fresh, readers = 0, doomed = False} m)
  when spare (releaseSnapshot fresh)

-- | The blocks that there is a view of and that is not on its way out.
heldViews :: Views b -> IO [b]
heldViews views = Map.keys . Map.filter (not . (.doomed)) <$> readTVarIO views.held

-- | Let go of every view but those of the given blocks. A view that is
-- being read is released when its last reader is done, and counts as gone
-- from now on.
keepViews :: (Ord b) => Views b -> [b] -> IO ()
keepViews views blocks = mask_ $ do
  free <- atomically . stateTVar views.held $ \m ->
    let (stay, go) = Map.partitionWithKey (\b _ -> b `elem` blocks) m
        (idle, busy) = Map.partition ((== 0) . (.readers)) go
     in (Map.elems idle, stay <> Map.map (\h -> h {doomed = True}) busy)
  forM_ free (releaseSnapshot . (.snapshot))

-- | Run an action with the database as it was at a block, if there is a
-- view of it: a reader whose every read goes through the snapshot. The
-- reader must not be used after the action returns.
withViewAt :: (Ord b) => Views b -> b -> (DatabaseReader -> IO a) -> IO (Maybe a)
withViewAt views block action = bracket enter leave (mapM run)
  where
    enter = atomically $ do
      m <- readTVar views.held
      case Map.lookup block m of
        Just h | not h.doomed -> do
          writeTVar views.held (Map.insert block h {readers = h.readers + 1} m)
          return (Just h)
        _ -> return Nothing
    run h = action views.reader {db = fst h.snapshot}
    leave Nothing = return ()
    leave (Just _) = do
      free <- atomically . stateTVar views.held $ \m -> case Map.lookup block m of
        Just h
          | h.readers <= 1 && h.doomed -> (Just h, Map.delete block m)
          | otherwise -> (Nothing, Map.insert block h {readers = h.readers - 1} m)
        Nothing -> (Nothing, m)
      forM_ free (releaseSnapshot . (.snapshot))

-- | Release every view. None may be in use.
closeViews :: Views b -> IO ()
closeViews views = mask_ $ do
  m <- atomically (stateTVar views.held (,Map.empty))
  forM_ m (releaseSnapshot . (.snapshot))
