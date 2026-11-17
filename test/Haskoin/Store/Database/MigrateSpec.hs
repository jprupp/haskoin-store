module Haskoin.Store.Database.MigrateSpec (spec) where

import Control.Exception (Exception (..))
import Control.Monad (forM, forM_, replicateM_, void, when)
import Data.ByteString qualified as B
import Data.List (isInfixOf)
import Data.Serialize (encode)
import Data.Word (Word32)
import Database.RocksDB (columnFamilies, put)
import Database.RocksDB.Query (insert, insertOp, retrieve, writeBatch)
import Haskoin (TxHash, btcRegTest)
import Haskoin.Store.Data (UnixTime)
import Haskoin.Store.Database.Migrate
import Haskoin.Store.Database.Reader (DatabaseReader (..))
import Haskoin.Store.Database.Types (MigrationKey (..), VersionKey (..))
import Haskoin.Store.Histories
import Haskoin.Store.Integration.Temp (withSystemTempDirectory)
import System.IO.Error (ioeGetErrorString)
import Test.Hspec

spec :: Spec
spec = describe "the version of the database" $ do
  it "is the current one for a new database, and nothing is migrated" $
    withSystemTempDirectory "haskoin-store-migrate-" $ \dir ->
      openLayered net dir $ \db -> do
        upgradeDatabase (\_ -> return ()) db `shouldReturn` nothing
        retrieve db.db VersionKey `shouldReturn` Just currentVersion
        -- a second run finds nothing to do, and says nothing
        upgradeDatabase (\_ -> fail "nothing to say") db `shouldReturn` nothing

  it "refuses a version that the store does not know, untouched" $
    forM_ [16, 17 :: Word32] $ \v ->
      withSystemTempDirectory "haskoin-store-migrate-" $ \dir ->
        openLayered net dir $ \db -> do
          insert db.db VersionKey v
          upgradeDatabase (\_ -> return ()) db `shouldThrow` (== UnknownVersion (Just v))
          retrieve db.db VersionKey `shouldReturn` Just v

  it "refuses the database of a newer store, and one whose version does not fit its marker, untouched" $
    withSystemTempDirectory "haskoin-store-merges-" $ \dir ->
      openLayered net dir $ \db -> do
        void (upgradeDatabase (\_ -> return ()) db)
        retrieve db.db VersionKey `shouldReturn` Just (23 :: Word32)
        insert db.db VersionKey (24 :: Word32)
        upgradeDatabase (\_ -> return ()) db `shouldThrow` (== UnknownVersion (Just 24))
        retrieve db.db VersionKey `shouldReturn` Just (24 :: Word32)
        displayException (UnknownVersion (Just 24)) `shouldSatisfy` ("newer haskoin-store" `isInfixOf`)
        -- no migration leaves a marker with the current version
        writeBatch db.db [insertOp VersionKey currentVersion, insertOp MigrationKey layeredVersion]
        upgradeDatabase (\_ -> return ()) db `shouldThrow` (== UnknownVersion (Just currentVersion))
        migrateRecords (\_ -> return ()) db `shouldThrow` (== UnknownVersion (Just currentVersion))
        retrieve db.db VersionKey `shouldReturn` Just currentVersion

  it "takes a database of version 18 or 19 to the current one, reading its mempool where it is" $
    forM_ [18, 19 :: Word32] $ \v ->
      withSystemTempDirectory "haskoin-store-migrate-" $ \dir -> do
        openOlder net dir $ \db -> do
          insert db.db VersionKey v
          -- version 18 listed its mempool under one key, here empty
          when (v == 18) $ put db.db (B.singleton 0x07) (encode ([] :: [(UnixTime, TxHash)]))
        openLayered net dir $ \db -> do
          done <- upgradeDatabase (\_ -> return ()) db
          done.moved `shouldBe` 0
          retrieve db.db VersionKey `shouldReturn` Just currentVersion
          (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Nothing
          -- and a second run finds nothing to do
          upgradeDatabase (\_ -> return ()) db `shouldReturn` nothing

  it "takes a database of version 22 with one write" $
    withSystemTempDirectory "haskoin-store-merges-" $ \dir -> do
      openOlder net dir $ \db -> do
        insert db.db VersionKey recordsVersion
        length (columnFamilies db.db) `shouldBe` 10
      openLayered net dir $ \db -> do
        upgradeDatabase (\_ -> return ()) db `shouldReturn` nothing
        retrieve db.db VersionKey `shouldReturn` Just currentVersion
        (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Nothing
        length (columnFamilies db.db) `shouldBe` 11

  it "cannot be opened by a store that does not know the merge operators, which leaves its merge records be" $
    withSystemTempDirectory "haskoin-store-merges-" $ \dir -> do
      -- blocks with merge records, which are in the write-ahead log and
      -- in no table file when the database is closed
      written <- openLayered net dir $ \db -> do
        void (upgradeDatabase (\_ -> return ()) db)
        startChain db
        replicateM_ 5 (mine db [])
        contents db
      length [() | cf <- written, not (null cf)] `shouldSatisfy` (> 5)
      -- RocksDB would open the database without the merge operators,
      -- and drop those merge records: it does not get that far, for
      -- want of the column family that such a store does not know
      openOlder net dir (\_ -> return ()) `shouldThrow` \e ->
        "merge-operators" `isInfixOf` ioeGetErrorString e
      openLayered net dir contents `shouldReturn` written
  where
    net = btcRegTest
    nothing = Migration {moved = 0, batches = 0, checked = 0, rewritten = 0}
    contents db = forM (columnFamilies db.db) (dump db)
