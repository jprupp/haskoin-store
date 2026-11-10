{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | What the layered layout costs when writing and when reading, for
-- docs/layered-database.md: its rules and its queries against today's, on
-- the same history.
--
-- It is not part of the package: it is the evidence for the numbers in the
-- design. Build the package first, then, from the root of the repository:
--
-- > stack ghc -- -O1 -threaded -rtsopts -outputdir /tmp/query-cost \
-- >   -o /tmp/query-cost/run docs/layered-database/QueryCost.hs
-- > /tmp/query-cost/run make mixed /tmp/query-cost/a 200000 50000 10000 off
-- > /tmp/query-cost/run make layered /tmp/query-cost/b 200000 50000 10000 off
-- > /tmp/query-cost/run time /tmp/query-cost/a /tmp/query-cost/b off
--
-- @make@ writes a database with the rules of one layout, @mixed@ for
-- today's: a chain of that many transactions in blocks of 1,000, then that
-- many transactions in the mempool, then blocks that confirm the oldest of
-- them, as many as the third number says. It prints how long the store took
-- over each part. The history depends on the numbers alone, so both layouts
-- get the same one. The last word turns Bloom filters on or off for the
-- column families of the chain, as @--bloom@ does; those of the mempool area
-- of the layered layout always have them.
--
-- @time@ opens both databases, asks both the same questions, checks that
-- the answers are the same, and prints how long each took. Run it after
-- @make@ has ended, so that the data is in table files, as it is in a store
-- that has been running for a while.
--
-- > cp -r /tmp/query-cost/b /tmp/query-cost/c
-- > /tmp/query-cost/run seeks layered /tmp/query-cost/c off 5000 block
--
-- @seeks@ times the first step of a scan by address, for addresses that
-- have nothing: in the index of transactions by address for today's layout,
-- and in the entries of the mempool by address for the layered one, and
-- then the balance of each of those addresses as the queries of the layout
-- read it, which for the layered layout is by key and without a scan. It
-- does so twice: with the database as it was left, and after the oldest mempool
-- transactions, as many as given, are confirmed by a @block@, or the newest
-- are deleted with @delete@. It changes the database, so give it a copy.
--
-- = The history
--
-- Every transaction spends the oldest output in circulation and, once there
-- are 100,000 of them, also one of the 2,000 newest, and pays two outputs.
-- So most outputs are spent in the end, a transaction usually builds on
-- another of the same block, and a mempool transaction usually spends one
-- confirmed output and one of another mempool transaction.
--
-- Of every 1,000 outputs:
--
-- * 20 go to one address, the busiest, all along;
--
-- * 50 go to the 400 addresses of the busy wallet, an extended public key
--   with 200 addresses in use on each of its two chains, all along;
--
-- * 50 go to the cold wallet, of the same size, while the chain is made.
--   They are never spent, so the mempool has nothing for it;
--
-- * 50 go to the quiet wallet, of the same size, in the same way, and five
--   late transactions of the mempool pay it too;
--
-- * one goes to the small wallet, of 20 addresses, in the same way, and two
--   late transactions of the mempool pay it too;
--
-- * the rest go to 100,000 other addresses.
module Main (main) where

import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate)
import Control.Monad (filterM, foldM, forM, forM_, unless, void, when)
import Data.Bits (shiftR, xor)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.List (find, sort)
import Data.Maybe (catMaybes, fromMaybe, mapMaybe)
import Data.Sequence (Seq, ViewL (..), (|>))
import Data.Sequence qualified as Seq
import Data.Serialize (encode)
import Data.Word (Word64)
import Database.RocksDB
  ( ColumnFamily,
    DB,
    Property (..),
    getPropertyCF,
    iterEntry,
    iterNext,
    iterSeek,
    iterValid,
    multiGetCF,
    withIterCF,
    withSnapshot,
  )
import GHC.Clock (getMonotonicTimeNSec)
import Haskoin
import Haskoin.Node (LogLevel (..), Logger (..))
import Haskoin.Store.Common
  ( Limits (..),
    Start (..),
    StoreRead (..),
    deriveAddresses,
    deriveFunction,
    sortTxs,
  )
import Haskoin.Store.Data
  ( Balance (..),
    BlockData (..),
    DeriveType (..),
    TxData (..),
    TxRef (..),
    UnixTime,
    XPubBal (..),
    XPubSpec (..),
    zeroBalance,
  )
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered qualified as L
import Haskoin.Store.Database.Reader
import Haskoin.Store.Database.Types (AddrTxKey (..), MemAddrKey (..))
import Haskoin.Store.Database.Writer (Writer, runWriter)
import Haskoin.Store.Logic qualified as Mixed
import Haskoin.Store.Logic.Layered qualified as Layered
import System.Environment (getArgs)
import System.IO (BufferMode (..), hSetBuffering, stdout)
import System.Mem (performMajorGC)
import Text.Printf (printf)

-------------------------------------------------------------------------------
-- Databases
-------------------------------------------------------------------------------

net :: Network
net = btcRegTest

quiet :: Logger
quiet = Logger {level = LevelError, source = "", push = \_ -> return ()}

-- | The layout of today, where the chain and the mempool share their
-- indices, and the layered one.
data Layout = Mixed | Layered

-- | Open a database of a layout with the settings that the store has by
-- default: gaps of 20 and 32, 2,000 open files, write buffers of 256 MiB, a
-- log of 1 GiB and a block cache of 1 GiB.
open :: Ctx -> Layout -> Bool -> FilePath -> (DatabaseReader -> IO a) -> IO a
open ctx layout bloom dir =
  reader net ctx 20 32 dir bloom (Just 2000) (Just (256 * mib)) (Just (1024 * mib)) (Just (1024 * mib)) Nothing
  where
    mib = 1024 * 1024
    reader = case layout of
      Mixed -> withDatabaseReader
      Layered -> withLayeredReader

-------------------------------------------------------------------------------
-- The history
-------------------------------------------------------------------------------

-- | A number that looks random, from one that does not.
mix :: Word64 -> Word64
mix z0 = z3 `xor` (z3 `shiftR` 31)
  where
    z1 = z0 + 0x9e3779b97f4a7c15
    z2 = (z1 `xor` (z1 `shiftR` 30)) * 0xbf58476d1ce4e5b9
    z3 = (z2 `xor` (z2 `shiftR` 27)) * 0x94d049bb133111eb

-- | A script that pays to one of many addresses.
plain :: Word64 -> ByteString
plain n = "\x76\xa9\x14" <> B.take 20 (encode (sha256 (encode n))) <> "\x88\xac"

-- | The scripts of the busiest address and of every miner.
hot, miner :: ByteString
hot = plain 4000000000
miner = plain 3999999999

-- | The wallets of the history.
busyWallet, coldWallet, quietWallet, smallWallet :: Ctx -> XPubSpec
busyWallet ctx = wallet ctx "the busy wallet of the measurements of haskoin-store"
coldWallet ctx = wallet ctx "the cold wallet of the measurements of haskoin-store"
quietWallet ctx = wallet ctx "the quiet wallet of the measurements of haskoin-store"
smallWallet ctx = wallet ctx "the small wallet of the measurements of haskoin-store"

wallet :: Ctx -> ByteString -> XPubSpec
wallet ctx seed = XPubSpec {key = deriveXPubKey ctx (makeXPrvKey seed), deriv = DeriveNormal}

-- | The scripts of the first addresses of each of the two chains of a
-- wallet, as many of each as given.
scriptsOf :: Ctx -> XPubSpec -> Int -> Seq ByteString
scriptsOf ctx spec n = Seq.fromList (mapMaybe (addressToScriptBS ctx) (branch 0 <> branch 1))
  where
    branch m = map snd (take n (deriveAddresses (deriveFunction ctx spec.deriv) (pubSubKey ctx spec.key m) 0))

-- | The scripts of the wallets, and how long the chain and the mempool of
-- the history are, in transactions.
data Places = Places
  { busy :: !(Seq ByteString),
    cold :: !(Seq ByteString),
    calm :: !(Seq ByteString),
    few :: !(Seq ByteString),
    chainTxs :: !Word64,
    poolTxs :: !Word64
  }

places :: Ctx -> Int -> Int -> Places
places ctx chainTxs poolTxs =
  Places
    { busy = scriptsOf ctx (busyWallet ctx) 200,
      cold = scriptsOf ctx (coldWallet ctx) 200,
      calm = scriptsOf ctx (quietWallet ctx) 200,
      few = scriptsOf ctx (smallWallet ctx) 10,
      chainTxs = fromIntegral chainTxs,
      poolTxs = fromIntegral poolTxs
    }

-- | The script that an output of the history pays to, by its number, and
-- whether the output stays in circulation, to be spent later.
place :: Places -> Word64 -> (ByteString, Bool)
place p k
  | k `elem` [end - 1000 * j - 1 | j <- [1 .. 5]] = (among p.calm, False)
  | k `elem` [end - 1000 * j - 501 | j <- [1 .. 2]] = (among p.few, False)
  | r < 20 = (hot, True)
  | r < 70 = (among p.busy, True)
  | chain && r < 120 = (among p.cold, False)
  | chain && r < 170 = (among p.calm, False)
  | chain && r < 171 = (among p.few, False)
  | otherwise = (plain (s `mod` 100000), True)
  where
    r = mix k `mod` 1000
    s = mix (k + 0x5555555555555555)
    chain = k < 2 * p.chainTxs
    end = 2 * (p.chainTxs + p.poolTxs)
    among xs = Seq.index xs (fromIntegral (s `mod` fromIntegral (Seq.length xs)))

-- | An unspent output and its amount.
data Spendable = Spendable !OutPoint !Word64

-- | The outputs in circulation, oldest first, and how many transactions
-- were made.
data History = History
  { coins :: !(Seq Spendable),
    made :: !Word64
  }

-- | The next transaction of a history.
payment :: Places -> History -> (Tx, History)
payment p hist = (tx, History {coins = rest <> Seq.fromList new, made = n + 1})
  where
    n = hist.made
    (oldest, others) = case Seq.viewl hist.coins of
      c :< cs -> (c, cs)
      EmptyL -> error "no coins"
    (spent, rest)
      | Seq.length others < 100000 = ([oldest], others)
      | otherwise =
          let i = Seq.length others - 1 - fromIntegral (mix (n + 77) `mod` 2000)
           in ([oldest, Seq.index others i], Seq.deleteAt i others)
    total = sum [v | Spendable _ v <- spent]
    half = (total - 200) `div` 2
    paid = [(half, place p (2 * n)), (total - 200 - half, place p (2 * n + 1))]
    tx = Tx 2 [TxIn o "" maxBound | Spendable o _ <- spent] [TxOut v s | (v, (s, _)) <- paid] [] 0
    new = [Spendable (OutPoint (txHash tx) i) v | (i, (v, (_, True))) <- zip [0 ..] paid]

-- | The next transactions of a history, as many as given.
payments :: Places -> Int -> History -> ([Tx], History)
payments p count hist0 = go count hist0 []
  where
    go 0 hist acc = (reverse acc, hist)
    go k hist acc = let (tx, hist') = payment p hist in tx `seq` go (k - 1 :: Int) hist' (tx : acc)

-- | A block at a height, on a parent, with a coinbase in front of some
-- transactions, and the output of that coinbase.
block :: BlockHeight -> BlockHeader -> [Tx] -> (Block, BlockNode, Spendable)
block height parent txs =
  (Block header (coinbase : txs), BlockNode header height 0 prev, Spendable (OutPoint (txHash coinbase) 0) reward)
  where
    reward = 5000000000
    prev = headerHash parent
    coinbase = Tx 1 [TxIn nullOutPoint (encode height) maxBound] [TxOut reward miner] [] 0
    merkle = either error id (buildMerkleRoot (map txHash (coinbase : txs)))
    candidate = BlockHeader 0x20000000 prev merkle (parent.timestamp + 600) parent.bits
    header = fromMaybe (error "no nonce") (find (isValidPOW net) (map candidate [0 ..]))

chunks :: Int -> [a] -> [[a]]
chunks _ [] = []
chunks n xs = let (a, b) = splitAt n xs in a : chunks n b

-------------------------------------------------------------------------------
-- Writing
-------------------------------------------------------------------------------

-- | Write a database with the rules of a layout: a chain, a mempool on top
-- of it, and blocks that confirm part of that mempool. Only the time that
-- the store takes is counted, not the time to make the history.
make :: Ctx -> Layout -> Bool -> FilePath -> Int -> Int -> Int -> IO ()
make ctx layout bloom dir chainTxs poolTxs confirmed =
  open ctx layout bloom dir $ \db -> do
    clock <- newIORef 0
    let write :: (Writer -> IO a) -> IO a
        write action = do
          t0 <- getMonotonicTimeNSec
          x <- runWriter net ctx db quiet action
          t1 <- getMonotonicTimeNSec
          modifyIORef' clock (+ (t1 - t0))
          return x
        mine (hist, height, parent) txs = do
          let (b, node, coin) = block (height + 1) parent txs
          void (write (\w -> importBlock w b node))
          return (hist {coins = hist.coins |> coin}, height + 1, b.header)
        p = places ctx chainTxs poolTxs
    write initBest
    Just best <- Q.getBestBlock db
    Just genesis <- Q.getBlock db best
    -- a block with a coin to begin with, then the chain
    start <- mine (History {coins = Seq.empty, made = 0}, genesis.height, genesis.header) []
    writeIORef clock 0
    let next (hist, height, parent) _ =
          let (txs, hist') = payments p 1000 hist
           in mine (hist', height, parent) txs
    chained <- foldM next start [1 .. chainTxs `div` 1000]
    report clock "importing a chain" chainTxs
    -- the mempool, a hundred transactions to a second
    let (hist1, height1, parent1) = chained
        (pool, hist2) = payments p poolTxs hist1
    taken <- forM (zip [0 ..] (chunks 100 pool)) $ \(second, txs) ->
      write (\w -> forM txs (\tx -> newMempoolTx w tx (1700000000 + second)))
    unless (and (concat taken)) (fail "a transaction was not taken into the mempool")
    report clock "taking transactions into the mempool" poolTxs
    -- blocks that confirm the oldest of the mempool
    (_, height2, _) <- foldM mine (hist2, height1, parent1) (chunks 1000 (take confirmed pool))
    report clock "importing blocks that confirm mempool transactions" confirmed
    left <- length <$> Q.getMempool db
    printf "%d blocks, %d transactions left in the mempool\n" height2 left
  where
    initBest = case layout of
      Mixed -> Mixed.initBest
      Layered -> Layered.initBest
    importBlock = case layout of
      Mixed -> Mixed.importBlock
      Layered -> Layered.importBlock
    newMempoolTx :: Writer -> Tx -> UnixTime -> IO Bool
    newMempoolTx = case layout of
      Mixed -> Mixed.newMempoolTx
      Layered -> Layered.newMempoolTx

-- | Say how long the store took over some transactions, and start counting
-- again.
report :: IORef Word64 -> String -> Int -> IO ()
report clock what count = do
  ns <- readIORef clock
  writeIORef clock 0
  when (count > 0) $
    printf
      "%s: %.1f seconds, %.0f microseconds for each transaction\n"
      what
      (fromIntegral ns / 1e9 :: Double)
      (fromIntegral ns / 1e3 / fromIntegral count :: Double)

-------------------------------------------------------------------------------
-- Reading
-------------------------------------------------------------------------------

-- | Ask a database of today's layout and one of the layered layout the same
-- questions, check that they answer the same, and print how long each
-- takes.
time :: Ctx -> Bool -> FilePath -> FilePath -> IO ()
time ctx bloom dirOld dirNew =
  open ctx Mixed bloom dirOld $ \old ->
    open ctx Layered bloom dirNew $ \new -> do
      let chain = [("tx", txCF), ("addr-tx", addrTxCF), ("balance", balanceCF), ("mempool", mempoolCF)]
      filters "today's" old chain
      filters "the layered" new (chain <> [("mempool-addr", mempoolAddrCF)])
      -- what to ask about
      pool <- map snd <$> Q.getMempool old
      Just best <- Q.getBestBlock old
      Just tip <- Q.getBlock old best
      middle <- Q.getBlocksAtHeight old (tip.height `div` 2) >>= mapM (Q.getBlock old)
      let confirmedTxs = concat [drop 1 b.txs | Just b <- middle]
          address = either error id . scriptToAddressBS net ctx
          busiest = address hot
          entries :: M.AddressMempool -> Int
          entries m = length m.txs + HashMap.size m.spent + length m.created
          used a = maybe False ((> 0) . (.txs)) <$> Q.getBalance old a
          touched a = entries <$> M.getAddressMempool new a
          others = [address (plain k) | k <- [0 .. 99999]]
      known <- filterM used others
      counted <- mapM (\a -> (a,) <$> touched a) known
      let calmOnes = take 1000 [a | (a, 0) <- counted]
          stirred = take 1000 [a | (a, n) <- counted, n >= 1, n <= 4]
          never = [address (plain k) | k <- [200000 .. 200999]]
          wallets =
            [ ("cold wallet", coldWallet ctx),
              ("quiet wallet", quietWallet ctx),
              ("busy wallet", busyWallet ctx),
              ("small wallet", smallWallet ctx)
            ]
      Just hotBalance <- Q.getBalance old busiest
      hotEntries <- touched busiest
      printf "%d blocks, %d transactions in the mempool\n" tip.height (length pool)
      printf
        "the busiest address: %d transactions, %d entries in the mempool area\n"
        hotBalance.txs
        hotEntries
      balances <- forM wallets $ \(name, spec) -> do
        bals <- Q.xPubBals old spec
        theirs <- L.withView new (`L.xPubBals` spec)
        unless (theirs == bals) (fail "the balances of a wallet differ")
        count <- Q.xPubTxCount old bals
        inPool <- sum . map entries <$> M.getAddressMempools new (map (.balance.address) bals)
        printf
          "the %s: %d addresses read, %d transactions, %d entries in the mempool area\n"
          name
          (length bals)
          count
          inPool
        return (name, spec, bals)
      printf "\n%-58s %10s %10s %7s\n" (text "microseconds for each") (text "today") (text "layered") (text "ratio")
      let rowOver :: (NFData a, Eq a) => Int -> String -> (Int -> StoreRead -> IO a) -> IO ()
          rowOver checks name question = do
            let today i = question i (Q.databaseRead old)
                layered i = L.withView new (question i . L.layeredRead)
            forM_ [0 .. checks - 1] $ \i -> do
              a <- today i
              b <- layered i
              unless (a == b) (fail (name <> ": the answers differ"))
            t0 <- best3 today
            t1 <- best3 layered
            printf "%-58s %10.1f %10.1f %7.2f\n" name t0 t1 (t1 / t0)
          -- a question about many things in turn, and one about one thing
          row, single :: (NFData a, Eq a) => String -> (Int -> StoreRead -> IO a) -> IO ()
          row = rowOver 1000
          single = rowOver 3
          page = Limits {limit = 100, offset = 0, start = Nothing}
          from h = (page :: Limits) {start = Just (AtTx h)}
          -- the first transaction at or below the middle of the chain
          far = Limits {limit = 1, offset = 0, start = Just (AtBlock (tip.height `div` 2))}
          pick :: Seq a -> Int -> a
          pick xs i = Seq.index xs (i `mod` Seq.length xs)
          windows :: [a] -> Seq [a]
          windows xs = Seq.fromList (take 50 (chunks 100 (cycle xs)))
          balanceOf q a = fromMaybe (zeroBalance a) <$> q.getBalance a
          calmSeq = Seq.fromList calmOnes
          stirredSeq = Seq.fromList stirred
          neverSeq = Seq.fromList never
          confirmedSeq = Seq.fromList confirmedTxs
          poolSeq = Seq.fromList (take 5000 pool)
          calmWindows = windows calmOnes
          confirmedWindows = windows confirmedTxs
          poolWindows = windows (take 5000 pool)
      row "balance: address with nothing in the mempool" $ \i q -> balanceOf q (pick calmSeq i)
      row "balance: address with a transaction or two in the mempool" $ \i q -> balanceOf q (pick stirredSeq i)
      single "balance: the busiest address" $ \_ q -> balanceOf q busiest
      row "balance: address that was never used" $ \i q -> balanceOf q (pick neverSeq i)
      row "balances of 100 addresses with nothing in the mempool" $ \i q -> q.getBalances (pick calmWindows i)
      row "transaction: confirmed" $ \i q -> q.getTxData (pick confirmedSeq i)
      row "transaction: in the mempool" $ \i q -> q.getTxData (pick poolSeq i)
      row "100 transactions: confirmed" $ \i q -> q.getManyTxData (pick confirmedWindows i)
      row "100 transactions: in the mempool" $ \i q -> q.getManyTxData (pick poolWindows i)
      row "spender of an output of a confirmed transaction" $ \i q -> q.getSpender (OutPoint (pick confirmedSeq i) 0)
      row "unspent output, if it is" $ \i q -> q.getUnspent (OutPoint (pick confirmedSeq i) 1)
      row "address with nothing in the mempool: its transactions" $ \i q -> q.getAddressTxs (pick calmSeq i) page
      row "address with a little in the mempool: its transactions" $ \i q -> q.getAddressTxs (pick stirredSeq i) page
      single "busiest address: first 100 transactions" $ \_ q -> q.getAddressTxs busiest page
      farHot <- map (.txid) <$> Q.getAddressTxs old busiest far
      forM_ farHot $ \h ->
        single "busiest address: 100 transactions, far back" $ \_ q -> q.getAddressTxs busiest (from h)
      single "busiest address: first 100 unspent outputs" $ \_ q -> q.getAddressUnspents busiest page
      forM_ balances $ \(name, spec, bals) -> do
        single (name <> ": balances") $ \_ q -> q.xPubBals spec
        single (name <> ": first 100 transactions") $ \_ q -> q.xPubTxs spec bals page
        farBack <- map (.txid) <$> Q.xPubTxs old bals far
        forM_ farBack $ \h ->
          single (name <> ": 100 transactions, far back") $ \_ q -> q.xPubTxs spec bals (from h)
        single (name <> ": first 100 unspent outputs") $ \_ q -> q.xPubUnspents spec bals page
        single (name <> ": number of transactions") $ \_ q -> q.xPubTxCount spec bals
      -- what the times on the right are made of
      let part :: (NFData a) => String -> (DatabaseReader -> Int -> IO a) -> IO ()
          part name action = do
            t <- best3 (\i -> withSnapshot new.db (\snapshot -> action (new :: DatabaseReader) {db = snapshot} i))
            printf "%-58s %10s %10.1f\n" name (text "") t
          family r = mempoolAddrCF r.db
          keysOf = map (encode . MemAddrKeyA)
          calmKeys = windows (keysOf calmOnes)
          busiestKey = encode (MemAddrKeyA busiest)
          walk it n =
            iterEntry it >>= \case
              Just (k, _) | busiestKey `B.isPrefixOf` k -> iterNext it >> walk it (n + 1 :: Int)
              _ -> return n
      putStrLn "\nparts of the times on the right, each in a snapshot of the layered database"
      part "a snapshot, taken and released" $ \_ _ -> return ()
      part "balances of 100 addresses: the chain's part" $ \r i -> Q.getBalances r (pick calmWindows i)
      part "balances of 100 addresses: the mempool's part" $ \r i ->
        sum . map entries <$> M.getAddressMempools r (pick calmWindows i)
      part "  100 seeks of one iterator that find nothing" $ \r i ->
        withIterCF r.db (family r) $ \it ->
          length . filter id <$> mapM (\k -> iterSeek it k >> iterValid it) (pick calmKeys i)
      part "  100 keys that are not there, in one multi-get" $ \r i ->
        length . filter (/= Nothing) <$> multiGetCF r.db (family r) (pick calmKeys i)
      part "busiest address: its mempool entries, read and decoded" $ \r _ ->
        entries <$> M.getAddressMempool r busiest
      part "  the same entries, read and not decoded" $ \r _ ->
        withIterCF r.db (family r) $ \it -> iterSeek it busiestKey >> walk it 0

text :: String -> String
text = id

-------------------------------------------------------------------------------
-- Seeking
-------------------------------------------------------------------------------

-- | Time the first step of a scan by address, for 2,000 addresses that have
-- nothing, in the index that the queries of a layout scan: before and after
-- mempool transactions are confirmed by a block, or deleted.
seeks :: Ctx -> Layout -> Bool -> FilePath -> Int -> Bool -> IO ()
seeks ctx layout bloom dir count deleting =
  open ctx layout bloom dir $ \db -> do
    let address = either error id . scriptToAddressBS net ctx
        never = [address (plain k) | k <- [200000 .. 201999]]
        (cf, key) = case layout of
          Mixed -> (addrTxCF db.db, encode . AddrTxKeyA)
          Layered -> (mempoolAddrCF db.db, encode . MemAddrKeyA)
        once = withIterCF db.db cf $ \it -> forM never $ \a -> do
          t0 <- getMonotonicTimeNSec
          iterSeek it (key a)
          _ <- iterValid it
          t1 <- getMonotonicTimeNSec
          return (t1 - t0)
            -- the balance of each of the same addresses, as the queries of the
        -- layout read it
        balance a = case layout of
          Mixed -> void (Q.getBalance db a)
          Layered -> void (L.withView db (`L.getBalance` a))
        asked = forM never $ \a -> do
          t0 <- getMonotonicTimeNSec
          balance a
          t1 <- getMonotonicTimeNSec
          return (t1 - t0)
        say what = do
          measure (what <> ", seek") once
          measure (what <> ", balance") asked
        measure what run = do
          _ <- run
          ts <- sort <$> run
          let n = length ts
              micro t = fromIntegral t / 1000 :: Double
              at q = micro (ts !! min (n - 1) (n * q `div` 1000))
          printf
            "%-30s mean %8.1f, median %8.1f, 99th of 100 %8.1f, longest %8.1f microseconds\n"
            (text what)
            (micro (sum ts) / fromIntegral n)
            (at 500)
            (at 990)
            (micro (last ts))
    say "as it was left"
    pool <- Q.getMempool db
    if deleting
      then do
        let newest = map snd (take count pool)
        runWriter net ctx db quiet (\w -> mapM_ (deleteUnconfirmedTx w False) newest)
        printf "%d of the %d mempool transactions deleted\n" (length newest) (length pool)
        say "after the deletions"
      else do
        -- the oldest, up to the end of a second, so that none is left
        -- without one that it spends
        let oldest = reverse pool
            cut = fst (oldest !! min count (length oldest - 1))
        bodies <- catMaybes <$> mapM (body db . snd) (takeWhile ((< cut) . fst) oldest)
        Just best <- Q.getBestBlock db
        Just tip <- Q.getBlock db best
        let txs = map snd (sortTxs (map (.tx) bodies))
            (b, node, _) = block (tip.height + 1) tip.header txs
        void (runWriter net ctx db quiet (\w -> importBlock w b node))
        printf "%d of the %d mempool transactions confirmed by a block\n" (length txs) (length pool)
        say "after the block"
  where
    body db h = case layout of
      Mixed -> Q.getTxData db h
      Layered -> M.getMempoolTx db h
    importBlock = case layout of
      Mixed -> Mixed.importBlock
      Layered -> Layered.importBlock
    deleteUnconfirmedTx = case layout of
      Mixed -> Mixed.deleteUnconfirmedTx
      Layered -> Layered.deleteUnconfirmedTx

-- | Say how many keys the table files of some column families have, and
-- how many of them are in Bloom filters.
filters :: String -> DatabaseReader -> [(String, DB -> ColumnFamily)] -> IO ()
filters name db families =
  forM_ families $ \(family, cf) -> do
    props <- getPropertyCF db.db (cf db.db) (PropertyName "rocksdb.aggregated-table-properties")
    let field label =
          case [f | f <- B.split 0x3b (fromMaybe "" props), label `B.isInfixOf` f] of
            f : _ -> C.unpack (B.drop 1 (B.dropWhile (/= 0x3d) f))
            [] -> "?"
    printf
      "%s database, table files of %s: %s keys, %s of them in %s bytes of Bloom filters\n"
      name
      family
      (field "# entries=")
      (field "# entries for filter")
      (field "filter block size")

-- | The time that an action takes, in microseconds: the best of three
-- means, each over enough runs to take a third of a second. The action is
-- given the number of the run, to ask about something else each time.
best3 :: (NFData a) => (Int -> IO a) -> IO Double
best3 action = minimum <$> mapM (const mean) [1 :: Int .. 3]
  where
    mean = performMajorGC >> go 16
    go :: Int -> IO Double
    go n = do
      t0 <- getMonotonicTimeNSec
      forM_ [0 .. n - 1] (\i -> action i >>= evaluate . rnf)
      t1 <- getMonotonicTimeNSec
      let ns = fromIntegral (t1 - t0) :: Double
      if ns < 3.3e8 && n < 4000000 then go (n * 4) else return (ns / fromIntegral n / 1000)

main :: IO ()
main = withContext $ \ctx -> do
  hSetBuffering stdout LineBuffering
  getArgs >>= \case
    ["make", layout, dir, chainTxs, poolTxs, confirmed, bloom] ->
      make ctx (layoutOf layout) (flag bloom) dir (read chainTxs) (read poolTxs) (read confirmed)
    ["time", dirOld, dirNew, bloom] -> time ctx (flag bloom) dirOld dirNew
    ["seeks", layout, dir, bloom, count, what] ->
      seeks ctx (layoutOf layout) (flag bloom) dir (read count) (what == "delete")
    _ ->
      fail . unlines $
        [ "usage: make mixed|layered DIR CHAIN MEMPOOL CONFIRMED on|off",
          "       time DIR DIR on|off",
          "       seeks mixed|layered DIR on|off COUNT block|delete"
        ]
  where
    layoutOf = \case
      "mixed" -> Mixed
      "layered" -> Layered
      other -> error ("not a layout: " <> other)
    flag = \case
      "on" -> True
      "off" -> False
      other -> error ("neither on nor off: " <> other)
