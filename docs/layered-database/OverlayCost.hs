{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | What the lookups of a mempool layer cost, for docs/layered-database.md.
--
-- The design reads the mempool from a column family of its own, next to
-- every read of the chain. This program fills a column family laid out as
-- the design proposes with synthetic mempool transactions (two inputs from
-- confirmed transactions and two outputs, at four addresses, 13 keys each),
-- and times each kind of lookup a query would add, first against RocksDB
-- and then against the same data held in hash maps in memory. It also says
-- how much memory those maps take.
--
-- It is not part of the package: it is the evidence for the numbers in the
-- design, kept so that they can be checked and repeated. From the root of
-- the repository:
--
-- > stack ghc -- -O1 -threaded -rtsopts -with-rtsopts=-T \
-- >   -outputdir /tmp/overlay-cost -o /tmp/overlay-cost/run \
-- >   docs/layered-database/OverlayCost.hs
-- > /tmp/overlay-cost/run /tmp/overlay-cost/db 50000 load
-- > /tmp/overlay-cost/run /tmp/overlay-cost/db 50000 keep
--
-- The first run loads the transactions and reads them while most are still
-- in the memtable; the second finds them in table files, where RocksDB put
-- them when the database was opened again.
--
-- The first line of output says whether the table files have Bloom filters.
-- With rocksdb-haskell-jprupp 2.6.1 and RocksDB 6.24 or later they had none,
-- whatever the configuration asked for; the store is built with 2.6.3 now,
-- which builds them. The output kept next to this file has both: its
-- measurements with filters were made before the fix was released, against
-- a checkout of the binding, by adding
--
-- > -i/path/to/rocksdb-haskell/src -hide-package rocksdb-haskell-jprupp -lrocksdb
--
-- to the first command.
module Main (main) where

import Control.DeepSeq (NFData, force)
import Control.Exception (evaluate)
import Control.Monad (forM, forM_, replicateM_, void, when)
import Data.Bits (shiftR, xor)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Builder qualified as BB
import Data.ByteString.Char8 qualified as C
import Data.ByteString.Lazy qualified as BL
import Data.ByteString.Short (ShortByteString, toShort)
import Data.Default (def)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Maybe (fromMaybe, isJust)
import Data.Time.Clock (diffUTCTime, getCurrentTime)
import Data.Word (Word32, Word64)
import Database.RocksDB
import GHC.Generics (Generic)
import GHC.Stats (GCDetails (..), RTSStats (..), getRTSStats)
import System.Environment (getArgs)
import System.Mem (performMajorGC)
import Text.Printf (printf)

-------------------------------------------------------------------------------
-- Synthetic data
-------------------------------------------------------------------------------

-- | A step of the SplitMix64 generator.
mix :: Word64 -> Word64
mix z0 =
  let z1 = (z0 `xor` (z0 `shiftR` 30)) * 0xbf58476d1ce4e5b9
      z2 = (z1 `xor` (z1 `shiftR` 27)) * 0x94d049bb133111eb
   in z2 `xor` (z2 `shiftR` 31)

-- | Pseudo-random bytes from a seed.
bytes :: Word64 -> Int -> ByteString
bytes seed n =
  B.take n . BL.toStrict . BB.toLazyByteString $
    mconcat [BB.word64BE (mix (seed * 0x9e3779b97f4a7c15 + i)) | i <- [1 .. fromIntegral ((n + 7) `div` 8)]]

w64 :: Word64 -> ByteString
w64 = BL.toStrict . BB.toLazyByteString . BB.word64BE

-- | Hash of a mempool transaction, serialized address, and hash of a
-- confirmed transaction that a mempool transaction spends.
txid, addr, parent :: Word64 -> ByteString
txid i = bytes i 32
addr j = B.cons 0x00 (bytes (1000000000 + j) 20)
parent i = bytes (2000000000 + i) 32

outpoint :: ByteString -> Word64 -> ByteString
outpoint h i = h <> B.drop 4 (w64 i)

-- | A mempool transaction: the time it arrived, the two outputs it spends
-- and the two it creates, each with its address.
data Tx = Tx
  { hash :: !ByteString,
    seen :: !Word64,
    ins :: ![(ByteString, ByteString)],
    outs :: ![(ByteString, ByteString)]
  }

tx :: Word64 -> Tx
tx i =
  Tx
    { hash = h,
      seen = 1790000000 + i `div` 7,
      ins = [(addr (4 * i + k), outpoint (parent (2 * i + k)) k) | k <- [0, 1]],
      outs = [(addr (4 * i + 2 + k), outpoint h k) | k <- [0, 1]]
    }
  where
    h = txid i

amount :: Word64
amount = 50000

-------------------------------------------------------------------------------
-- The mempool area in RocksDB
-------------------------------------------------------------------------------

-- | The keys of a transaction, as in the table of the design.
txOps :: ColumnFamily -> Word64 -> [BatchOp]
txOps cf i =
  [ PutCF cf (B.cons 0x08 t.hash) (w64 t.seen),
    PutCF cf (B.cons 0x0b (w64 t.seen <> t.hash)) "",
    PutCF cf (B.cons 0x02 t.hash) (bytes (3000000000 + i) 500)
  ]
    <> concat
      [ [ PutCF cf (B.cons 0x0d op) (t.hash <> B.drop 4 (w64 k)),
          PutCF cf (B.cons 0x0c (a <> B.cons 0x00 (w64 t.seen <> t.hash))) "",
          PutCF cf (B.cons 0x0c (a <> B.cons 0x01 op)) (w64 amount)
        ]
      | (k, (a, op)) <- zip [0 ..] t.ins
      ]
    <> concat
      [ [ PutCF cf (B.cons 0x0c (a <> B.cons 0x00 (w64 t.seen <> t.hash))) "",
          PutCF cf (B.cons 0x0c (a <> B.cons 0x02 (w64 t.seen <> op))) (w64 amount <> script)
        ]
      | (a, op) <- t.outs
      ]
  where
    t = tx i
    script = bytes (4000000000 + i) 25

-------------------------------------------------------------------------------
-- The same data in memory
-------------------------------------------------------------------------------

-- | Input that spends an output.
data Spender = Spender !ShortByteString !Word32
  deriving (Generic, NFData)

-- | What the mempool does at an address: its transactions, the outputs of
-- the address that they spend, and the outputs that they pay to it.
data Page = Page
  { txs :: ![(Word64, ShortByteString)],
    spent :: ![(ShortByteString, Word64)],
    paid :: ![(Word64, ShortByteString, Word64, ShortByteString)]
  }
  deriving (Generic, NFData)

instance Semigroup Page where
  a <> b = Page (a.txs <> b.txs) (a.spent <> b.spent) (a.paid <> b.paid)

-- | The indices of the mempool area, decoded: no transaction bodies.
data Mirror = Mirror
  { times :: !(HashMap ShortByteString Word64),
    spenders :: !(HashMap ShortByteString Spender),
    pages :: !(HashMap ShortByteString Page)
  }
  deriving (Generic, NFData)

mirror :: Word64 -> Mirror
mirror count =
  Mirror
    { times = HashMap.fromList [(toShort t.hash, t.seen) | t <- ts],
      spenders =
        HashMap.fromList
          [(toShort op, Spender (toShort t.hash) k) | t <- ts, (k, (_, op)) <- zip [0 ..] t.ins],
      pages =
        HashMap.fromListWith (<>) $
          [ (toShort a, Page [(t.seen, h)] [(toShort op, amount)] [])
          | t <- ts,
            let h = toShort t.hash,
            (a, op) <- t.ins
          ]
            <> [ (toShort a, Page [(t.seen, h)] [] [(t.seen, toShort op, amount, script)])
               | t <- ts,
                 let h = toShort t.hash,
                 (a, op) <- t.outs
               ]
    }
  where
    ts = map tx [0 .. count - 1]
    script = toShort (bytes 4000000000 25)

-- | Bytes of live data on the heap after a major collection.
liveBytes :: IO Word64
liveBytes = performMajorGC >> gcdetails_live_bytes . gc <$> getRTSStats

-------------------------------------------------------------------------------
-- Timing
-------------------------------------------------------------------------------

-- | Time an action that does a number of lookups, and print the time of
-- one, with how many of them found something.
time :: String -> Int -> IO Int -> IO ()
time name n act = do
  t0 <- getCurrentTime
  found <- act
  t1 <- getCurrentTime
  let ns = realToFrac (diffUTCTime t1 t0) * 1e9 / fromIntegral n :: Double
  printf "%-58s %7.0f ns  (%d found)\n" name ns found

main :: IO ()
main = do
  [dir, countArg, load] <- getArgs
  let count = read countArg :: Word64
      -- as the store opens its database: a block cache of 1 GiB shared by
      -- the column families, with their index and filter blocks in it
      cfg =
        def
          { createIfMissing = True,
            createMissingColumnFamilies = True,
            blockCacheSize = Just (1024 * 1024 * 1024),
            cacheIndexAndFilterBlocks = True
          }
      -- as the mempool column family of the store with --bloom: a Bloom
      -- filter on whole keys, no prefix
      family = def {prefixLength = Nothing, bloomFilter = True, cacheIndexAndFilterBlocks = True}
      -- as its column families of addresses: the filter is on the first 22
      -- bytes of the key, the kind of key and the address
      byAddress = family {prefixLength = Just 22}
      reps = 200000 :: Int
      -- seeds of transactions, addresses and outputs that are not there
      absent = 5000000000 :: Word64
      -- keys are made before the clock starts
      keysOf key = do
        let ks = map key [1 .. fromIntegral reps]
        mapM_ evaluate ks
        return ks
      noTx i = B.cons 0x02 (txid (absent + i))
      someTx i = B.cons 0x02 (txid (i `mod` count))
      noSpender i = B.cons 0x0d (outpoint (parent (absent + i)) 0)
      noSpenders i = B.cons 0x0d (txid (absent + i))
      noAddress i = B.cons 0x0c (addr (absent + i))
      someAddress i = B.cons 0x0c (addr (i `mod` (4 * count)))
  withDBCF dir cfg [("mempool", family), ("mempool-addr", byAddress)] $ \db -> do
    (cf, cfa) <- case columnFamilies db of
      [c, a] -> return (c, a)
      _ -> fail "expected two column families"
    when (load == "load") $ do
      forM_ [0, 1000 .. count - 1] $ \b -> do
        let ops = concatMap (txOps cf) [b .. min (count - 1) (b + 999)]
        -- the keys of addresses again, in the family filtered by address
        write db (ops <> [PutCF cfa k v | PutCF _ k v <- ops, B.take 1 k == "\x0c"])
      printf "loaded %d transactions, %d keys\n" count (13 * count)
    let pointGets name d key = do
          ks <- keysOf key
          time name reps $ do
            n <- newIORef (0 :: Int)
            forM_ ks $ \k -> do
              r <- getCF d cf k
              when (isJust r) (modifyIORef' n (+ 1))
            readIORef n
        -- one iterator for all the scans, as a query would use
        scans name d family' prefix = do
          ps <- keysOf prefix
          time name reps $
            withIterCF d family' $ \it -> do
              n <- newIORef (0 :: Int)
              forM_ ps $ \p -> do
                iterSeek it p
                let go =
                      iterKey it >>= \case
                        Just key | p `B.isPrefixOf` key -> do
                          void (iterValue it >>= evaluate)
                          modifyIORef' n (+ 1)
                          iterNext it
                          go
                        _ -> return ()
                go
              readIORef n
        multiGets name d size key = do
          ks <- keysOf key
          let chunks [] = []
              chunks xs = let (c, r) = splitAt size xs in c : chunks r
          time name reps $ do
            rs <- forM (chunks ks) $ \g -> length . filter isJust <$> multiGetCF d cf g
            return (sum rs)
        run label d = do
          putStrLn label
          pointGets "  get: transaction not in the mempool" d noTx
          pointGets "  get: transaction in the mempool (500 bytes)" d someTx
          pointGets "  get: output with no mempool spender" d noSpender
          multiGets "  multi-get of 100, each: transaction not in the mempool" d 100 noTx
          multiGets "  multi-get of 100, each: output with no mempool spender" d 100 noSpender
          scans "  scan: address with nothing in the mempool" d cf noAddress
          scans "  scan: address with one mempool transaction" d cf someAddress
          scans "  scan: spenders of a transaction with none" d cf noSpenders
          scans "  scan, family filtered by address: nothing in the mempool" d cfa noAddress
          scans "  scan, family filtered by address: one transaction" d cfa someAddress
    -- whether the table files have Bloom filters at all
    props <- getPropertyCF db cf (PropertyName "rocksdb.aggregated-table-properties")
    let field name =
          case [f | f <- B.split 0x3b (fromMaybe "" props), name `B.isInfixOf` f] of
            f : _ -> C.unpack (B.drop 1 (B.dropWhile (/= 0x3d) f))
            [] -> "?"
    printf
      "table files of the mempool family: %s keys, %s of them in %s bytes of Bloom filters\n"
      (field "# entries=")
      (field "# entries for filter")
      (field "filter block size")
    putStrLn "RocksDB"
    time "  snapshot: create and release" reps $ do
      replicateM_ reps (withSnapshot db (\_ -> return ()))
      return 0
    time "  iterator: create and destroy" reps $ do
      replicateM_ reps (withIterCF db cf (\_ -> return ()))
      return 0
    run "RocksDB, reading the live database" db
    withSnapshot db (run "RocksDB, reading through a snapshot")
  -- the same lookups in memory
  before <- liveBytes
  m <- evaluate (force (mirror count))
  after <- liveBytes
  let lookups :: String -> HashMap ShortByteString v -> (Word64 -> ByteString) -> IO ()
      lookups name table key = do
        ks <- map (toShort . B.drop 1) <$> keysOf key
        mapM_ evaluate ks
        time name reps . evaluate $ length (filter (`HashMap.member` table) ks)
  putStrLn "Hash maps in memory"
  lookups "  lookup: transaction not in the mempool" m.times noTx
  lookups "  lookup: output with no mempool spender" m.spenders noSpender
  lookups "  lookup: address with nothing in the mempool" m.pages noAddress
  lookups "  lookup: address with one mempool transaction" m.pages someAddress
  printf
    "  size: %d bytes for each of %d transactions, %.1f MiB in all\n"
    ((after - before) `div` count)
    (HashMap.size m.times)
    (fromIntegral (after - before) / 1048576 :: Double)
