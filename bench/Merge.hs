-- | What merge records cost the readers of the database.
--
-- > haskoin-store-bench merge [KEYS [READS [CACHE_MIB [DIR]]]]
--
-- Since database version 23 the import of a block does not read the
-- balances of its addresses or the spenders of the outputs that it spends:
-- it writes merge records, which say how they change, and RocksDB works
-- out the values when they are read ("Haskoin.Store.Database.Merge"). The
-- work has moved to whoever reads a key that has merge records which no
-- compaction has reached yet: RocksDB looks into every table file that has
-- one of them, down to the value, and merges. This says how long that
-- takes.
--
-- It builds a database with KEYS balances and KEYS groups of spenders
-- (100,000) for each of four kinds of key: with no merge record, with one,
-- with three and with six. Every key gets a whole value first, and those
-- are compacted to the last level, where the data of a store is. Then come
-- six rounds: each writes a merge record for the keys that are to have as
-- many, and flushes, so that each round is a table file of its own.
-- RocksDB is kept from compacting them, which a store is not: there, what
-- is measured here lasts until a compaction has merged the records.
--
-- Three reads are timed for each kind of key, READS of them (20,000) at
-- random keys, from one thread, five times over: the balance of an
-- address, the spender of an output, and all the spenders of a
-- transaction, which is a scan with an iterator. Then everything is
-- compacted, which leaves whole values, and the same reads are timed
-- again: the same keys and the same answers, as a store had them before.
--
-- The block cache is CACHE_MIB (256), which holds all of it, so that what
-- is timed is the work of RocksDB and of the merge and not the disk. With
-- a cache too small for the data, such as 1, every block that is looked
-- into is fetched and decompressed again, as on a store whose cache is a
-- fraction of its database. DIR is where the database goes: a temporary
-- directory unless given.
module Merge (mergeBench) where

import Control.Monad (forM, forM_, unless, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.IntMap.Strict qualified as IntMap
import Data.List (sort)
import Data.Maybe (fromMaybe)
import Data.Serialize (encode)
import Data.Word (Word32, Word64)
import Database.RocksDB
  ( BatchOp (..),
    Config (..),
    Property (..),
    compactRange,
    flush,
    getPropertyCF,
    write,
  )
import GHC.Clock (getMonotonicTime)
import Haskoin
import Haskoin.Store.Data (Balance (..), Spender (..))
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    balanceCF,
    layeredFamilyConfig,
    spenderCF,
    withReader,
  )
import Haskoin.Store.Database.Types (BalKey (..), BalVal (..), SpenderKey (..), SpenderVal (..))
import Haskoin.Store.Integration.Temp (withSystemTempDirectory, withTempDirectory)
import System.IO (hFlush, stdout)
import Text.Printf (printf)

net :: Network
net = btcRegTest

-- | The numbers of merge records that the four kinds of key have.
pending :: [Int]
pending = [0, 1, 3, 6]

mergeBench :: [String] -> IO ()
mergeBench args = do
  let number i d = maybe d read (lookup i (zip [0 :: Int ..] args))
      keys = number 0 100000
      reads' = number 1 20000
      cache = number 2 256 * 1024 * 1024
      inDir = case drop 3 args of
        dir : _ -> withTempDirectory dir "haskoin-store-merge-"
        [] -> withSystemTempDirectory "haskoin-store-merge-"
  inDir $ \dir -> open cache dir $ \db -> do
    let balances = balanceCF db.db
        spenders = spenderCF db.db
        both action = action balances >> action spenders
        files cf = maybe "?" C.unpack <$> getPropertyCF db.db cf (NumFilesAtLevel 0)
    printf "%d balances and %d groups of spenders for each number of merge records\n" keys keys
    hFlush stdout
    -- a whole value for every key, at the last level
    forM_ pending $ \kind ->
      forM_ (chunks 5000 [0 .. keys - 1]) $ \some ->
        write db.db $
          concat
            [ [ PutCF balances (encode (BalKey (address kind i))) (encode whole),
                PutCF spenders (encode (SpenderKey (made kind i) 0)) (encode (SpenderVal (entries [0, 1])))
              ]
            | i <- some
            ]
    both (flush db.db . Just)
    both (\cf -> compactRange db.db (Just cf) Nothing Nothing)
    -- a merge record a round for the keys that are to have as many, each
    -- round in a table file of its own
    forM_ [1 .. maximum pending] $ \turn -> do
      forM_ (filter (>= turn) pending) $ \kind ->
        forM_ (chunks 5000 [0 .. keys - 1]) $ \some ->
          write db.db $
            concat
              [ [ MergeCF balances (encode (BalKey (address kind i))) (encode difference),
                  MergeCF spenders (encode (SpenderKey (made kind i) 0)) (encode (SpenderVal (entries [1 + turn])))
                ]
              | i <- some
              ]
      both (flush db.db . Just)
    level0 <- (,) <$> files balances <*> files spenders
    printf "table files above the last level: %s of balances, %s of spenders\n\n" (fst level0) (snd level0)
    hFlush stdout
    let measure = forM pending $ \kind -> do
          let picked = take reads' [n `mod` keys | n <- iterate (\n -> (n * 7919 + 104729) `mod` 1000003) (kind + 1)]
              time action = do
                -- once unmeasured, so that the cache has what there is
                mapM_ action picked
                passes <- forM [1 .. 5 :: Int] $ \_ -> do
                  t0 <- getMonotonicTime
                  mapM_ action picked
                  t1 <- getMonotonicTime
                  return (1e6 * (t1 - t0) / fromIntegral (length picked))
                return (sort passes !! 2)
          balance <- time $ \i -> do
            b <- Q.getBalance db (address kind i)
            unless (fmap (.txs) b == Just (fromIntegral (1 + kind))) (fail "a balance is not what was merged")
          spender <- time $ \i -> do
            found <- Q.getChainSpender db (OutPoint (made kind i) (if kind == 0 then 1 else 2))
            when (found == Nothing) (fail "a spender is not there")
          scan <- time $ \i -> do
            m <- Q.getChainSpenders db (made kind i)
            unless (IntMap.size m == 2 + kind) (fail "the spenders are not those that were merged")
          return (kind, balance :: Double, spender :: Double, scan :: Double)
    merged <- measure
    both (\cf -> compactRange db.db (Just cf) Nothing Nothing)
    compacted <- measure
    printf "microseconds a read, median of 5 passes of %d reads at random keys, one thread\n" reads'
    printf "block cache of %d MiB\n\n" (cache `div` (1024 * 1024))
    let table :: String -> ((Int, Double, Double, Double) -> Double) -> IO ()
        table title of' = do
          printf "%s\n" title
          printf "  %-22s %16s %18s\n" (s "merge records") (s "in table files") (s "after compaction")
          forM_ (zip merged compacted) $ \(m@(kind, _, _, _), c) ->
            printf "  %-22d %16.1f %18.1f\n" kind (of' m) (of' c)
          printf "\n"
    table "the balance of an address (a point read)" (\(_, x, _, _) -> x)
    table "the spender of an output (a point read of its group)" (\(_, _, x, _) -> x)
    table "the spenders of a transaction (a scan of its groups)" (\(_, _, _, x) -> x)
    hFlush stdout
  where
    s :: String -> String
    s = id
    whole = BalVal {confirmed = 5000, unconfirmed = 0, utxo = 2, txs = 1, received = 5000}
    difference = BalVal {confirmed = 1000, unconfirmed = 0, utxo = 1, txs = 1, received = 1000}
    entries outputs = IntMap.fromList [(o, Spender (made 9 o) (fromIntegral o)) | o <- outputs]

-- | Open a database as the store does, with a block cache and Bloom
-- filters, but for the compactions of RocksDB, which are off: a merge
-- record stays in the table file that it was flushed to.
open :: Int -> FilePath -> (DatabaseReader -> IO a) -> IO a
open cache dir =
  withReader families (\_ -> return ()) net 20 32 dir True Nothing Nothing Nothing Nothing (Just cache) Nothing
  where
    families bloom cached =
      [(name, config {disableAutoCompactions = True}) | (name, config) <- layeredFamilyConfig bloom cached]

-- | The address of a kind of key and a number.
address :: Int -> Int -> Address
address kind i =
  fromMaybe (error "no address") . eitherToMaybe . scriptToAddressBS net $
    "\x76\xa9\x14" <> B.take 20 (hashOf kind i) <> "\x88\xac"

-- | The transaction of a kind of key and a number.
made :: Int -> Int -> TxHash
made kind i = TxHash (sha256 (hashOf kind i))

hashOf :: Int -> Int -> ByteString
hashOf kind i = encode (sha256 (encode (fromIntegral kind :: Word32, fromIntegral i :: Word64)))

chunks :: Int -> [a] -> [[a]]
chunks _ [] = []
chunks n xs = let (a, b) = splitAt n xs in a : chunks n b
