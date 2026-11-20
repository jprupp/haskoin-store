module Haskoin.Store.Database.MigrateSpec (spec) where

import Control.Exception (Exception (..))
import Control.Monad (forM, forM_, replicateM_, void, when)
import Data.ByteString qualified as B
import Data.List (isInfixOf)
import Data.Maybe (fromJust)
import Data.Serialize (encode)
import Data.Word (Word32)
import Database.RocksDB (columnFamilies, put)
import Database.RocksDB.Query (insert, insertOp, retrieve, writeBatch)
import Haskoin
  ( NFToken (..),
    Network (..),
    OutPoint (..),
    TokenCapability (..),
    TokenData (..),
    Tx (..),
    TxHash,
    TxIn (..),
    TxOut (..),
    bchRegTest,
    btcRegTest,
    encodeTokenPrefix,
    txHash,
  )
import Haskoin.Store.Data (BlockData (..), UnixTime)
import Haskoin.Store.Database.Migrate
import Haskoin.Store.Database.Migrate.Tokens (migrateTokens)
import Haskoin.Store.Database.Query qualified as Q
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
        retrieve db.db VersionKey `shouldReturn` Just currentVersion
        insert db.db VersionKey (currentVersion + 1)
        upgradeDatabase (\_ -> return ()) db `shouldThrow` (== UnknownVersion (Just (currentVersion + 1)))
        retrieve db.db VersionKey `shouldReturn` Just (currentVersion + 1)
        displayException (UnknownVersion (Just (currentVersion + 1)))
          `shouldSatisfy` ("newer haskoin-store" `isInfixOf`)
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
        -- the merge guard and the four of the tokens
        length (columnFamilies db.db) `shouldBe` 15

  it "takes a database of version 23 to the current one on a network without tokens" $
    withSystemTempDirectory "haskoin-store-tokens-" $ \dir ->
      openLayered net dir $ \db -> do
        insert db.db VersionKey mergesVersion
        upgradeDatabase (\_ -> return ()) db `shouldReturn` nothing
        retrieve db.db VersionKey `shouldReturn` Just currentVersion

  it "takes a database of version 23 of Bitcoin Cash to the current one, indexing its tokens" $
    withSystemTempDirectory "haskoin-store-tokens-" $ \dir ->
      openLayered bchRegTest dir $ \db -> do
        insert db.db VersionKey mergesVersion
        upgradeDatabase (\_ -> return ()) db `shouldReturn` nothing
        retrieve db.db VersionKey `shouldReturn` Just currentVersion
        (retrieve db.db MigrationKey :: IO (Maybe Word32)) `shouldReturn` Nothing

  it "indexes the tokens of a database of version 23 as the import of its blocks does" $
    withSystemTempDirectory "haskoin-store-tokens-" $ \dir -> do
      -- the same blocks, imported as version 24 does and as version 23 did
      fresh <- tokenChain bchRegTest (dir <> "/fresh")
      old <- tokenChain bchRegTest {cashAddrPrefix = Nothing} (dir <> "/old")
      fresh `shouldNotBe` old
      migrated <- openLayered bchRegTest (dir <> "/old") $ \db -> do
        insert db.db VersionKey mergesVersion
        n <- migrateTokens (\_ -> return ()) 0 db
        -- the two transactions that hold tokens, and only those
        n `shouldBe` 2
        families db
      migrated `shouldBe` fresh

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
    -- every column family that a block writes to, but the spenders'
    -- groups, which the tokens do not touch: those of the chain and those
    -- of the tokens
    families db = forM [0, 1, 2, 4, 5, 6, 7, 11, 12, 13, 14] $ \i -> dump db (columnFamilies db.db !! i)
    -- a chain on Bitcoin Cash with tokens, imported with the rules of a
    -- network, and what its column families hold
    tokenChain rules path = openLayered bchRegTest path $ \db -> do
      startChainAs rules db
      first <- mineAs rules db []
      coinbase <-
        Q.getBlock db first >>= \case
          Just bd | h : _ <- bd.txs -> return h
          _ -> fail "no coinbase"
      let minted = TokenData coinbase 1000 (Just (NFToken MintingCapability "\x01"))
          alice = "\x76\xa9\x14" <> B.replicate 20 0x11 <> "\x88\xac"
          bob = "\x76\xa9\x14" <> B.replicate 20 0x22 <> "\x88\xac"
          holding t lock = fromJust (encodeTokenPrefix t) <> lock
          -- alice gets coins without tokens too, so her history has the
          -- transaction already in version 23
          genesis =
            Tx
              1
              [TxIn (OutPoint coinbase 0) "" maxBound]
              [TxOut 1000 (holding minted alice), TxOut 2000 bob, TxOut 3000 alice]
              []
              0
          move =
            Tx
              1
              [TxIn (OutPoint (txHash genesis) 0) "" maxBound]
              [ TxOut 600 (holding (TokenData coinbase 400 Nothing) bob),
                TxOut 400 (holding minted {amount = 600} alice)
              ]
              []
              0
      _ <- mineAs rules db [genesis]
      _ <- mineAs rules db [move]
      families db
    nothing = Migration {moved = 0, batches = 0, checked = 0, rewritten = 0}
    contents db = forM (columnFamilies db.db) (dump db)
