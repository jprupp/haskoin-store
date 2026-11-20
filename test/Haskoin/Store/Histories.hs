-- | Databases for tests, and blocks to put in them: what the tests of the
-- database, of the block import and of the views share. The database holds
-- the chain alone; the mempool is the pool of the fleet, in Redis.
module Haskoin.Store.Histories
  ( -- * Databases
    openLayered,
    openOlder,
    dump,

    -- * Blocks
    run,
    startChain,
    startChainAs,
    mine,
    mineAs,
    revert,
  )
where

import Control.Exception (try)
import Control.Monad (void)
import Data.ByteString (ByteString)
import Database.RocksDB (ColumnFamily, Config (..), iterEntry, iterFirst, iterNext, withIterCF)
import Haskoin (Block (..), BlockHash, Network, Tx, headerHash)
import Haskoin.Node (LogLevel (..), Logger (..))
import Haskoin.Store.Data (BlockData (..))
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    layeredFamilyConfig,
    mergeGuardFamily,
    tokenFamilyConfig,
    withLayeredReader,
    withReader,
  )
import Haskoin.Store.Database.Writer (Writer, runWriter)
import Haskoin.Store.Logic (ImportException, importBlock, initBest, revertBlock)
import Haskoin.Store.TestBlocks (nextBlock)

-- | Open a database as the store does.
openLayered :: Network -> FilePath -> (DatabaseReader -> IO a) -> IO a
openLayered net dir = withLayeredReader net 20 32 dir False Nothing Nothing Nothing Nothing Nothing Nothing

-- | Open a database as a store of database version 22 did: without merge
-- operators, without the column family that is there since version 23 to
-- keep such a store out, and without those of the tokens, since version
-- 24. It is how the tests make a database that such a store left, and how
-- they try what such a store would do to a newer one.
openOlder :: Network -> FilePath -> (DatabaseReader -> IO a) -> IO a
openOlder net dir =
  withReader older (\_ -> return ()) net 20 32 dir False Nothing Nothing Nothing Nothing Nothing Nothing
  where
    older bloom cached =
      [ (name, config {mergeOperator = Nothing})
      | (name, config) <- layeredFamilyConfig bloom cached,
        name /= mergeGuardFamily,
        name `notElem` map fst (tokenFamilyConfig bloom cached)
      ]

-- | Run a batch of writes, and give what it returned, or the reason why it
-- wrote nothing.
run :: DatabaseReader -> (Writer -> IO a) -> IO (Either ImportException a)
run db = try . runWriter db.net db quiet
  where
    quiet = Logger {level = LevelError, source = "", push = \_ -> return ()}

-- | Give a new database its genesis block.
startChain :: DatabaseReader -> IO ()
startChain db = startChainAs db.net db

-- | 'startChain' with the rules of the chain run as on another network.
startChainAs :: Network -> DatabaseReader -> IO ()
startChainAs net db = runAs net db initBest >>= either (fail . show) return

-- | 'run' with the rules of the chain run as on another network: the
-- blocks are those of the network of the database, and the scripts are
-- read as on the other. With Bitcoin Cash without its CashAddr prefix, an
-- output that holds tokens has no address and no tokens, as in a store of
-- database version 23, which read scripts with haskoin-core 2.0.
runAs :: Network -> DatabaseReader -> (Writer -> IO a) -> IO (Either ImportException a)
runAs net db = try . runWriter net (db :: DatabaseReader) {net = net} quiet
  where
    quiet = Logger {level = LevelError, source = "", push = \_ -> return ()}

-- | Import a block with these transactions on the best block, and give its
-- hash.
mine :: DatabaseReader -> [Tx] -> IO BlockHash
mine db txs = do
  best <- Q.getBestBlock db >>= maybe (fail "no best block") return
  parent <- Q.getBlock db best >>= maybe (fail "no data for the best block") return
  let (block, node) = nextBlock db.net parent.height parent.header txs
  run db (\w -> importBlock w block node) >>= either (fail . show) (void . return)
  return (headerHash block.header)

-- | 'mine' with the rules of the chain run as on another network
-- ('runAs').
mineAs :: Network -> DatabaseReader -> [Tx] -> IO BlockHash
mineAs net db txs = do
  best <- Q.getBestBlock db >>= maybe (fail "no best block") return
  parent <- Q.getBlock db best >>= maybe (fail "no data for the best block") return
  let (block, node) = nextBlock db.net parent.height parent.header txs
  runAs net db (\w -> importBlock w block node) >>= either (fail . show) (void . return)
  return (headerHash block.header)

-- | Revert the best block.
revert :: DatabaseReader -> IO ()
revert db = do
  best <- Q.getBestBlock db >>= maybe (fail "no best block") return
  run db (`revertBlock` best) >>= either (fail . show) return

-- | Every key and value of a column family.
dump :: DatabaseReader -> ColumnFamily -> IO [(ByteString, ByteString)]
dump db cf = withIterCF db.db cf $ \iter -> iterFirst iter >> go iter
  where
    go iter =
      iterEntry iter >>= \case
        Nothing -> return []
        Just e -> iterNext iter >> (e :) <$> go iter
