-- | How long a full block takes to import, in stages and as it did until
-- version 1.15.1.
--
-- > haskoin-store-bench import [OUTPUTS [TXS [BLOCKS [CACHE_MIB [DIR]]]]] +RTS -N4
--
-- It builds a regtest database with OUTPUTS unspent outputs (a million by
-- default) over half as many addresses, with the rules of the store,
-- closes it so that RocksDB writes it all to table files, and makes two
-- copies. It then imports the same blocks of TXS transactions (3,500) into
-- each: into one with 'importBlockSequential' and no read cache, which is
-- the import as it was until version 1.15.1 but for the hash tables and
-- the serialising of the batch, which it shares with the new one; and into
-- the other with 'importBlock'.
--
-- = Four kinds of block
--
-- BLOCKS blocks (5) of each kind are imported, in this order:
--
-- 1. with an empty mempool, as a store that syncs or catches up does;
--
-- 2. with an empty mempool again, and chains of transactions in the block
--    ('Chained'): three inputs in four spend an output that a transaction
--    of the same block makes, as on Bitcoin mainnet today, where the
--    blocks of the first kind have one in twenty;
--
-- 3. with every transaction of the block in the mempool first, as at the
--    tip of the chain;
--
-- 4. with a mempool of TXS transactions that have nothing to do with the
--    block, and a block of chains again, none of whose transactions the
--    mempool has seen: what a miner was given directly.
--
-- The fourth kind is there because nothing measured it, and it was the
-- slow one. A block whose transactions the mempool has is taken out of
-- the mempool, and one that meets an empty mempool asks it nothing. A
-- block that the mempool has not seen, on a mempool that is not empty,
-- has to find out which mempool transactions it conflicts with. That
-- looked in the chain for the spender of every output that the block both
-- makes and spends, one read each, and a block of mainnet spent ten
-- seconds on it.
--
-- = What a block is made of
--
-- A transaction of the first and third kinds spends two outputs spread
-- over the set, or one of them and an output of the transaction before it
-- in the block for one in ten, and pays an address that has outputs and a
-- new one. So a block of 3,500 spends about 6,650 unspent outputs, adds a
-- spender to the groups of as many transactions and changes 14,000
-- balances, about what a block of mainnet does. The sequential import
-- reads all of those. The import in stages reads the outputs, and since
-- database version 23 writes the spenders and the balances as merge
-- records, without reading them ("Haskoin.Store.Database.Merge"). The
-- outputs of the set come from transactions of 50 outputs each, and a
-- block spends outputs of thousands of them.
--
-- A block of the second and fourth kinds has as many transactions, inputs
-- and outputs, in chains of a hundred ('chainLength'), and spends about
-- 1,850 outputs of the set.
--
-- The database is built by the import in stages, so the balances and the
-- spenders that it starts with are merge records in table files, as on a
-- store: the sequential import, which reads them, has RocksDB merge them.
--
-- Before the fourth kind it says what the copy in memory of the mempool
-- area costs ("Haskoin.Store.Database.Copy"), and what is remembered with
-- it of the outputs that mempool transactions spend: the transactions of
-- one more block are given to the mempool, where they stay for the blocks
-- of the fourth kind, and the copy is loaded from the database again.
-- With @+RTS -T@ it also says how much heap they take and how long a full
-- collection takes with the copy. For a mempool of the size of mainnet's,
-- ask for blocks of 25,000 transactions:
--
-- > haskoin-store-bench import 1000000 25000 1 +RTS -N4 -A64m -T
--
-- The block cache is CACHE_MIB (32), a fraction of the database, so that
-- most reads fetch and decompress a block of a table file, as on a store
-- of mainnet. DIR is where the databases go: a temporary directory unless
-- given.
module Import (importBench) where

import Control.Concurrent (getNumCapabilities)
import Control.DeepSeq (force)
import Control.Exception (evaluate)
import Control.Monad (forM_, unless, void, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet qualified as HashSet
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.List (find)
import Data.Maybe (fromMaybe)
import Data.Serialize (encode)
import Data.Word (Word64)
import GHC.Stats (GCDetails (..), RTSStats (..), getRTSStats, getRTSStatsEnabled)
import Haskoin
import Haskoin.Node (LogLevel (..), Logger (..))
import Haskoin.Store.Data (BlockData (..))
import Haskoin.Store.Database.Copy (copySize, spentOutputs)
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    forgetMempoolCopy,
    knownMempoolCopy,
    mempoolCopy,
    withLayeredReader,
  )
import Haskoin.Store.Database.Writer
  ( BatchReport (..),
    BlockTimes (..),
    ReadTimes (..),
    Writer,
    noReads,
    runWriter,
    runWriterReport,
    stopwatch,
    withoutReadCache,
  )
import Haskoin.Store.Integration.Temp (withSystemTempDirectory, withTempDirectory)
import Haskoin.Store.Logic.Layered (importBlock, importBlockSequential, initBest, newMempoolTx)
import System.Directory (copyFile, createDirectory, listDirectory)
import System.FilePath ((</>))
import System.IO (hFlush, stdout)
import System.Mem (performMajorGC)
import Text.Printf (printf)

net :: Network
net = btcRegTest

quiet :: Logger
quiet = Logger {level = LevelError, source = "", push = \_ -> return ()}

-- | What to measure.
data Plan = Plan
  { -- | unspent outputs to start with
    outputs :: !Int,
    -- | transactions in a measured block
    txs :: !Int,
    -- | measured blocks of each kind
    blocks :: !Int,
    -- | block cache of RocksDB, in bytes
    cache :: !Int
  }

importBench :: [String] -> IO ()
importBench args = do
  let number i d = maybe d read (lookup i (zip [0 :: Int ..] args))
      plan =
        Plan
          { outputs = number 0 1000000,
            txs = number 1 3500,
            blocks = number 2 5,
            cache = number 3 32 * 1024 * 1024
          }
      inDir = case drop 4 args of
        dir : _ -> withTempDirectory dir "haskoin-store-import-"
        [] -> withSystemTempDirectory "haskoin-store-import-"
  inDir (run plan)

run :: Plan -> FilePath -> IO ()
run plan dir = do
  cores <- getNumCapabilities
  printf "cores used: %d\n" cores
  printf "building a database with %d unspent outputs at %d addresses\n" plan.outputs (addresses plan)
  hFlush stdout
  let before = dir </> "before"
      after = dir </> "after"
  createDirectory before
  (tip, took) <- stopwatch (build plan before)
  printf "built in %.1f s\n" took
  copyDirectory before after
  -- the blocks to measure, the same for both databases, each kind with
  -- outputs of the set of its own
  let n = plan.blocks
      (tip1, empty) = measured plan Spread tip 0 n
      (tip2, emptyChained) = measured plan Chained tip1 n n
      (tip3, pooled) = measured plan Spread tip2 (2 * n) n
      (tip4, unseen) = measured plan Chained tip3 (3 * n) n
      -- one more block, which is not imported: its transactions go to the
      -- mempool and stay there
      others = map fst (snd (measured plan Spread tip4 (4 * n) 1))
  _ <- evaluate (force (map fst (empty <> emptyChained <> pooled <> unseen) <> others))
  forM_ [("spread" :: String, empty), ("chained", emptyChained)] $ \(shape, blocks) ->
    printf
      "%s blocks of %d transactions, %d inputs and %d outputs: %d inputs spend an output of the block\n"
      shape
      (plan.txs + 1)
      (inputsOf blocks)
      (outputsOf blocks)
      (insideOf blocks)
  putStrLn ""
  hFlush stdout
  (old, oldBest) <- open plan before $ \db -> do
    a <- mapM (measure db False sequential) empty
    b <- mapM (measure db False sequential) emptyChained
    c <- mapM (measure db True sequential) pooled
    mapM_ (fill db) others
    d <- mapM (measure db False sequential) unseen
    ((a, b, c, d),) <$> Q.getBestBlock db
  (new, newBest, copied) <- open plan after $ \db -> do
    a <- mapM (measure db False importBlock) empty
    b <- mapM (measure db False importBlock) emptyChained
    c <- mapM (measure db True importBlock) pooled
    -- the transactions that stay in the mempool, and what their copy
    -- costs
    copied <- mapM (copyCost db) others
    d <- mapM (measure db False importBlock) unseen
    best <- Q.getBestBlock db
    return ((a, b, c, d), best, copied)
  when (oldBest /= newBest || oldBest /= Just (headerHash (fst tip4))) $
    fail "the two databases did not end at the last block"
  let (oldEmpty, oldEmptyChained, oldPooled, oldUnseen) = old
      (newEmpty, newEmptyChained, newPooled, newUnseen) = new
  report "mempool empty, as when syncing or catching up" oldEmpty newEmpty
  report
    "mempool empty, a block of chains: three inputs in four spend an output of the block"
    oldEmptyChained
    newEmptyChained
  report "every transaction in the mempool first, as at the tip" oldPooled newPooled
  report "a mempool of other transactions, and a block of chains that it has not seen" oldUnseen newUnseen
  mapM_ putStr copied
  where
    sequential w = importBlockSequential (withoutReadCache w)
    inputsOf bs = sum [length tx.inputs | (b, _) <- take 1 bs, tx <- b.txs]
    outputsOf bs = sum [length tx.outputs | (b, _) <- take 1 bs, tx <- b.txs]
    -- inputs of the first block that spend an output of the block itself
    insideOf bs =
      length
        [ ()
        | (b, _) <- take 1 bs,
          let made = HashSet.fromList (map txHash b.txs),
          tx <- b.txs,
          i <- tx.inputs,
          i.outpoint.hash `HashSet.member` made
        ]

-- | Print the mean time of a block, before and after, and the stages.
report :: String -> [(BlockTimes, BatchReport)] -> [(BlockTimes, BatchReport)] -> IO ()
report title before after = do
  let batches = map snd after
      average f = sum (map f batches) `div` max 1 (length batches)
  when (map ((.operations) . snd) before /= map (.operations) batches) $
    fail "the two imports did not write the same number of keys"
  printf "%s (milliseconds a block, mean of %d)\n" title (length new)
  printf "  one batch a block: %d keys set or deleted, %d bytes\n" (average (.operations)) (average (.bytes))
  printf "  %-40s %10s %10s\n" ("" :: String) ("sequential" :: String) ("in stages" :: String)
  line "prepare" (.prepare)
  line "read ahead" (.prefetch)
  line "  unspent outputs" (.reads.unspentReads)
  line "  groups of spenders" (.reads.spenderReads)
  line "  balances" (.reads.balanceReads)
  line "  mempool" (.reads.mempoolReads)
  line "apply the rules (sequential: reads too)" (.apply)
  line "write the batch" (.write)
  line "whole block" total
  printf "  %-40s %20.1fx\n\n" ("faster by" :: String) (mean total old / mean total new)
  hFlush stdout
  where
    old = map fst before
    new = map fst after
    total t = t.prepare + t.prefetch + t.apply + t.write
    mean f ts = 1000 * sum (map f ts) / fromIntegral (max 1 (length ts))
    line :: String -> (BlockTimes -> Double) -> IO ()
    line name f = printf "  %-40s %10.1f %10.1f\n" name (mean f old) (mean f new)

-- | Open a database as the store does, with a block cache of the size of
-- the plan and Bloom filters.
open :: Plan -> FilePath -> (DatabaseReader -> IO a) -> IO a
open plan dir = withLayeredReader net 20 32 dir True Nothing Nothing Nothing Nothing (Just plan.cache) Nothing

-- | Import a block and say how long each stage took. With the flag, its
-- transactions are given to the mempool first, which is not measured. The
-- sequential import does not note its stages: all its time before the
-- write is given as that of applying the rules.
measure ::
  DatabaseReader ->
  Bool ->
  (Writer -> Block -> BlockNode -> IO (BlockData, [a])) ->
  (Block, BlockNode) ->
  IO (BlockTimes, BatchReport)
measure db pool rules (block, node) = do
  when pool (fill db block)
  inside <- newIORef 0
  ((_, batch), whole) <- stopwatch . runWriterReport net db quiet $ \w -> do
    (_, took) <- stopwatch (rules w block node)
    modifyIORef' inside (+ took)
  applied <- readIORef inside
  return . (,batch) $ case batch.block of
    Just t -> t
    Nothing -> BlockTimes {prepare = 0, prefetch = 0, reads = noReads, apply = applied, write = whole - applied}

-- | Give the transactions of a block to the mempool, a hundred in a
-- batch.
fill :: DatabaseReader -> Block -> IO ()
fill db block =
  forM_ (chunks 100 (drop 1 block.txs)) $ \txs ->
    runWriter net db quiet $ \w -> forM_ txs $ \tx -> do
      accepted <- newMempoolTx w tx 1700000000
      unless accepted (fail "a transaction was not accepted into the mempool")

-- | What the copy in memory of the mempool area costs, with the
-- transactions of a block in the mempool: how many keys and bytes it has,
-- how long it takes to load from the database, and, if the program runs
-- with @+RTS -T@, how much of the heap it is and how long a full
-- collection takes with it and without it.
--
-- It says the same of what is remembered of the outputs that those
-- transactions spend, which the batches that put them in the mempool left
-- with the copy: how many, and how much heap they take. Loading the copy
-- again, as this does to time it, starts with none remembered, so the
-- heap of the copy that the batches made, less that of the copy that was
-- loaded, is theirs.
copyCost :: DatabaseReader -> Block -> IO String
copyCost db block = do
  fill db block
  let txs = length block.txs - 1
  -- counted now, so that nothing holds on to the copy that the batches
  -- made once it is forgotten
  kept <- knownMempoolCopy db >>= evaluate . maybe 0 (HashMap.size . spentOutputs)
  remembering <- live
  forgetMempoolCopy db
  (without, collecting) <- stopwatch live
  (copy, loading) <- stopwatch (mempoolCopy db)
  (with, collectingWith) <- stopwatch live
  let (keys, bytes) = copySize copy
      heap = case (,,) <$> with <*> without <*> remembering of
        Nothing -> ["  heap: run with +RTS -T to have it measured\n"]
        Just (a, b, c) ->
          [ printf "  heap: %.1f MiB, %d bytes a transaction\n" (mib (a - b)) ((a - b) `div` max 1 txs),
            printf
              "  a full collection: %.0f ms with the copy, %.0f ms without\n"
              (1000 * collectingWith)
              (1000 * collecting),
            printf
              "  heap of the outputs remembered: %.1f MiB, %d bytes an output\n"
              (mib (c - a))
              ((c - a) `div` max 1 kept)
          ]
  return . concat $
    [ printf "the copy in memory of the mempool area, with %d transactions in the mempool\n" txs,
      printf "  %d keys, %.1f MiB of keys and values\n" keys (mib bytes),
      printf "  loaded from the database in %.0f ms\n" (1000 * loading),
      printf "  %d outputs that those transactions spend are remembered\n" kept
    ]
      <> heap
  where
    mib :: Int -> Double
    mib n = fromIntegral n / (1024 * 1024)
    -- bytes of live data after a full collection, if the runtime counts
    live = do
      counted <- getRTSStatsEnabled
      performMajorGC
      if counted then Just . fromIntegral . (.gc.gcdetails_live_bytes) <$> getRTSStats else return Nothing

chunks :: Int -> [a] -> [[a]]
chunks _ [] = []
chunks n xs = let (a, b) = splitAt n xs in a : chunks n b

copyDirectory :: FilePath -> FilePath -> IO ()
copyDirectory from to = do
  createDirectory to
  names <- listDirectory from
  forM_ names $ \name -> copyFile (from </> name) (to </> name)

-------------------------------------------------------------------------------
-- The database to start from
-------------------------------------------------------------------------------

-- | Outputs of a funding transaction, after its change.
fanout :: Int
fanout = 50

-- | Funding transactions in a block.
perBlock :: Int
perBlock = 400

-- | Satoshi of each output of the set.
coin :: Word64
coin = 2000

-- | Addresses that the outputs of the set are spread over: each has two.
addresses :: Plan -> Int
addresses plan = max 1 (plan.outputs `div` 2)

-- | A script that pays one of many addresses: to a public key hash for
-- even numbers, to a witness public key hash for odd ones.
payTo :: Int -> ByteString
payTo n
  | even n = "\x76\xa9\x14" <> h <> "\x88\xac"
  | otherwise = "\x00\x14" <> h
  where
    h = B.take 20 (encode (sha256 (encode (fromIntegral n :: Word64))))

coinbase :: BlockHeight -> Tx
coinbase h = Tx 1 [TxIn nullOutPoint (encode h) maxBound] [TxOut 5000000000 (payTo 0)] [] 0

-- | The funding transactions: a chain, each spending the change of the one
-- before and paying 'fanout' outputs of the set. The first spends the
-- coinbase of the first block.
funding :: Plan -> [Tx]
funding plan = go (0 :: Int) (OutPoint (txHash (coinbase 1)) 0) 5000000000
  where
    count = (plan.outputs + fanout - 1) `div` fanout
    go k from value
      | k >= count = []
      | otherwise =
          let left = value - fromIntegral fanout * coin
              paid = [TxOut coin (payTo (1 + (k * fanout + i) `mod` addresses plan)) | i <- [0 .. fanout - 1]]
              tx = Tx 2 [TxIn from "" maxBound] (TxOut left (payTo 0) : paid) [] 0
           in tx : go (k + 1) (OutPoint (txHash tx) 0) left

-- | The outputs of the set, in the order in which the measured blocks
-- spend them: one of each funding transaction in turn, so that the inputs
-- of a block are spread over all of them.
spendable :: Plan -> [OutPoint]
spendable plan =
  [ OutPoint h (fromIntegral i)
  | i <- [1 .. fanout],
    h <- hashes
  ]
  where
    hashes = map txHash (funding plan)

-- | Build the database: the genesis block, a block with the coin, and the
-- blocks of the funding transactions. Gives the header and height of the
-- last block.
build :: Plan -> FilePath -> IO (BlockHeader, BlockHeight)
build plan dir = open plan dir $ \db -> do
  runWriter net db quiet initBest
  let first = mine net.genesisHeader 1 []
      go parent [] = return parent
      go (header, height) (txs : more) = do
        let next = mine header (height + 1) txs
        add db next
        go (tipOf next) more
  add db first
  go (tipOf first) (chunks perBlock (funding plan))
  where
    add db (block, node) = void (runWriter net db quiet (\w -> importBlock w block node))

tipOf :: (Block, BlockNode) -> (BlockHeader, BlockHeight)
tipOf (block, node) = (block.header, node.height)

-- | A block on a parent, with a coinbase in front of the transactions.
mine :: BlockHeader -> BlockHeight -> [Tx] -> (Block, BlockNode)
mine parent height txs = (Block header all', BlockNode header height 0 prev)
  where
    prev = headerHash parent
    all' = coinbase height : txs
    merkle = either error id (buildMerkleRoot (map txHash all'))
    candidate = BlockHeader 0x20000000 prev merkle (parent.timestamp + 600) parent.bits
    header = fromMaybe (error "no nonce") (find (isValidPOW net) (map candidate [0 ..]))

-------------------------------------------------------------------------------
-- The blocks to measure
-------------------------------------------------------------------------------

-- | How the transactions of a measured block are tied to each other.
data Shape
  = -- | nearly every input spends an output of the set: one in twenty
    -- spends an output of the transaction before it in the block
    Spread
  | -- | chains of 'chainLength' transactions, each spending outputs of the
    -- ones before it in its chain: three inputs in four spend an output of
    -- the block
    Chained

-- | Transactions in a chain of a block of chains.
chainLength :: Int
chainLength = 100

-- | Blocks to measure on a tip, given their shape and how many were made
-- before them, which says which outputs of the set are left to spend.
-- Gives the new tip too.
measured ::
  Plan ->
  Shape ->
  (BlockHeader, BlockHeight) ->
  Int ->
  Int ->
  ((BlockHeader, BlockHeight), [(Block, BlockNode)])
measured plan shape tip done count = foldl' step (tip, []) [done .. done + count - 1]
  where
    coins = spendable plan
    step ((header, height), acc) k =
      let block = mine header (height + 1) (payments plan shape k (drop (k * 2 * plan.txs) coins))
       in (tipOf block, acc <> [block])

-- | The transactions of the measured block of a number, given the outputs
-- of the set that are left. Each spends two outputs and makes two, to an
-- address of the set and to a new one. Each input has a witness of the
-- size of a signature and a public key.
--
-- Which outputs a transaction spends depends on the shape of the block:
--
-- * 'Spread': two outputs of the set, but for one transaction in ten,
--   which spends an output of the transaction before it instead of the
--   second.
--
-- * 'Chained': the first transaction of a chain spends two outputs of the
--   set. Each of the others spends the first output of the transaction
--   before it, and with it the second output of the transaction two
--   before, for every other one, or an output of the set. No output is
--   spent twice, and of a chain of a hundred, 148 inputs of 200 spend an
--   output of the block. Bitcoin mainnet had 3,000 to 6,500 such inputs
--   in a block in November 2026, of some 7,000.
payments :: Plan -> Shape -> Int -> [OutPoint] -> [Tx]
payments plan shape k = go (0 :: Int) []
  where
    witness = [B.replicate 72 0x30, B.replicate 33 0x02]
    -- the outputs of the block that a transaction spends, given its number
    -- and the hashes of those before it, the last one first
    inside Spread i (h : _) | i `mod` 10 == 9 = [OutPoint h 0]
    inside Spread _ _ = []
    inside Chained i before = case (i `mod` chainLength, before) of
      (0, _) -> []
      (j, h : h' : _) | j >= 2 && even j -> [OutPoint h 0, OutPoint h' 1]
      (_, h : _) -> [OutPoint h 0]
      (_, []) -> []
    go i before coins
      | i >= plan.txs = []
      | otherwise =
          let made = inside shape i before
              (fresh, rest) = splitAt (2 - length made) coins
              mine' = made <> fresh
              n = k * plan.txs + i
              old = payTo (1 + (n * 7919) `mod` addresses plan)
              new = payTo (addresses plan + 1 + n)
              tx =
                Tx
                  2
                  [TxIn p "" maxBound | p <- mine']
                  [TxOut 1900 old, TxOut 1900 new]
                  (map (const witness) mine')
                  0
           in if length mine' < 2
                then error "not enough outputs: fewer blocks or transactions, or more outputs"
                else tx : go (i + 1) (txHash tx : before) rest
