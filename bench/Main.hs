-- | Load-test server for the web API.
--
-- Syncs a synthetic regtest chain from an in-process mock peer into a fresh
-- database, then serves the web API on it until killed, so that a load
-- generator such as wrk can be pointed at it. See @scripts/bench-web.sh@.
--
-- > haskoin-store-bench PORT BLOCKS [REDIS_URL]
--
-- Four other things are measured without a server: @haskoin-store-bench
-- micro@ times the pure functions that encode a response,
-- @haskoin-store-bench import@ the import of full blocks ("Import"),
-- @haskoin-store-bench merge@ what the merge records of those blocks cost
-- the readers of the database ("Merge"), and @haskoin-store-bench
-- admission@ how the limit on long requests behaves under load
-- ("Admission").
--
-- Set @BENCH_LOG=1@ to print the log of the store, such as to see why it
-- does not sync.
--
-- Every block holds a coinbase and ten transactions. The coinbase and one
-- output of each transaction pay the "hot" address, so it gains eleven
-- transactions per block. Nine outputs per block go to the first 100
-- external and change addresses of the benchmark xpub, in turn. Both are
-- printed once the chain is synced. With 400 blocks or more, the last one
-- also holds 3,000 transactions that spend outputs of the hot address.
--
-- While it serves, the server reads commands from its standard input, each
-- of which mines blocks on its chain and announces them, to measure how
-- the store keeps up with what comes after the xpub is cached:
--
-- * @idle N@: N blocks with nothing but their coinbase, which touch no
--   address of the xpub;
-- * @pay@: a block as those of the chain, which pays nine addresses of the
--   xpub;
-- * @addr M I@: a block that pays the address of index I of chain M of the
--   xpub, once: one past the gap is not seen until a payment within the gap
--   brings it in.
--
-- It prints @mined H@ with the height of the last block once it has
-- announced them.
module Main (main) where

import Admission (admissionBench)
import Conduit
import Control.Applicative ((<|>))
import Control.Concurrent.Async (withAsync)
import Control.Concurrent.STM (TVar, atomically, modifyTVar', newTVarIO, readTVar, readTVarIO)
import Control.DeepSeq (NFData, force)
import Control.Exception (evaluate)
import Control.Monad (forM_, forever, unless)
import Data.Aeson (toEncoding)
import Data.Aeson.Encoding (encodingToLazyByteString)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Default (def)
import Data.HashMap.Strict qualified as HashMap
import Data.IntMap.Strict qualified as IntMap
import Data.List (find)
import Data.Maybe (fromMaybe, listToMaybe, mapMaybe)
import Data.Serialize (decode, encode, runGet, runPut)
import Data.Text qualified as T
import Data.Text.IO qualified as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Vector qualified as V
import Data.Word (Word32)
import GHC.Clock (getMonotonicTimeNSec)
import Haskoin
import Haskoin.Node
import Haskoin.Node.Address (peerHostAddress)
import Haskoin.Store
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Integration.Temp (withSystemTempDirectory)
import Import (importBench)
import Merge (mergeBench)
import NQE
import Network.Socket (SockAddr (..))
import System.Environment (getArgs, lookupEnv)
import System.IO (hFlush, isEOF, stdout)
import System.Random (randomIO)

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["micro"] -> micro
    "import" : more -> importBench more
    "merge" : more -> mergeBench more
    "admission" : more -> admissionBench more
    _ -> server args

server :: [String] -> IO ()
server args = do
  let (port, nblocks, redis) = case args of
        [p, n] -> (read p, read n, Nothing)
        [p, n, r] -> (read p, read n, Just r)
        _ -> error "usage: haskoin-store-bench PORT BLOCKS [REDIS_URL]"
  withSystemTempDirectory "haskoin-store-bench-" $ \dir -> do
    let net = bchRegTest
        blocks = chain net nblocks
    mock <- Mock <$> newTVarIO [] <*> newTVarIO []
    logged <- maybe False (`notElem` ["", "0"]) <$> lookupEnv "BENCH_LOG"
    -- MAX_REQUESTS and MIN_REQUESTS: the ceiling and floor of the limit on
    -- the long requests answered at once; no limit by default.
    -- TARGET_DELAY: the scheduler's delay, in milliseconds, over which the
    -- limit comes down; the store's default otherwise.
    capped <- maybe 0 read <$> lookupEnv "MAX_REQUESTS"
    floored <- maybe 1 read <$> lookupEnv "MIN_REQUESTS"
    delay <- maybe (def :: WebLimits).targetDelay read <$> lookupEnv "TARGET_DELAY"
    -- BLOCK_CACHE_MB and WRITE_BUFFER_MB: RocksDB's block cache and write
    -- buffer, for a store whose reads go to the disk; 1024 MB and RocksDB's
    -- default otherwise
    cache <- maybe 1024 read <$> lookupEnv "BLOCK_CACHE_MB"
    buffer <- fmap read <$> lookupEnv "WRITE_BUFFER_MB"
    let withLog f = if logged then withLogger LevelDebug f else f quiet
    withLog $ \logger -> withStore (storeConfig net dir redis blocks mock logger cache buffer) $ \store -> do
      withSubscription store.pub $ \sub -> waitHeight store sub nblocks
      T.putStrLn $ "hot address: " <> addrText net hotAddr
      T.putStrLn $ "xpub: " <> xPubExport net benchXPub
      T.putStrLn $ "xpub address: " <> addrText net (xpubAddr 0 0)
      bh <- Q.getBestBlock store.db
      T.putStrLn $ "best block: " <> maybe "" blockHashToHex bh
      putStrLn $ "serving on port " <> show port
      hFlush stdout
      let limits = def {maxRequests = capped, minRequests = floored, targetDelay = delay}
      -- with BENCH_LOG=1 the web server logs each request too
      withAsync (commands net blocks mock) $ \_ ->
        runWeb ((webConfig store port) {limits = limits, logger = if logged then logger else quiet} :: WebConfig)

-- | What the mock peers share: the blocks mined after the chain, oldest
-- first, and where to announce new ones.
data Mock = Mock
  { extra :: !(TVar [Block]),
    peers :: !(TVar [Mailbox ByteString])
  }

-- | Mine the blocks that the commands on the standard input ask for, on top
-- of the chain and of those mined before, and announce them to every peer.
commands :: Network -> [Block] -> Mock -> IO ()
commands net blocks mock = loop
  where
    loop =
      isEOF >>= \case
        True -> return ()
        False -> do
          ws <- words <$> getLine
          more <- readTVarIO mock.extra
          let tip = last (blocks <> more)
              h0 = length blocks + length more
              new = case ws of
                ["idle", n] -> grow tip (h0 + 1) [const [] | _ <- [1 .. read n :: Int]]
                ["pay"] -> grow tip (h0 + 1) [\h -> spends h (txHash (coinbase (h - 1)))]
                ["addr", m, i] -> grow tip (h0 + 1) [\h -> [toAddr (read m) (read i) (txHash (coinbase (h - 1)))]]
                _ -> []
          unless (null new) $ do
            atomically $ modifyTVar' mock.extra (<> new)
            ps <- readTVarIO mock.peers
            let announce = MHeaders (Headers [(b.header, VarInt (fromIntegral (length b.txs))) | b <- new])
            forM_ ps $ \s -> runPut (putMessage net announce) `send` s
            putStrLn ("mined " <> show (h0 + length new))
            hFlush stdout
          loop
    grow _ _ [] = []
    grow parent h (f : fs) =
      let b = mine net parent.header (coinbase h : f h)
       in b : grow b (h + 1) fs
    toAddr m i cb =
      Tx
        1
        [TxIn (OutPoint cb 0) "" maxBound]
        [TxOut 400000000 (scriptOf (xpubAddr m i)), TxOut 4500000000 hotScript]
        []
        0

addrText :: Network -> Address -> T.Text
addrText net = fromMaybe (error "address") . addrToText net

waitHeight :: Store -> Inbox StoreEvent -> Int -> IO ()
waitHeight store sub n = do
  h <-
    Q.getBestBlock store.db >>= \case
      Nothing -> return 0
      Just bh -> maybe 0 (.height) <$> Q.getBlock store.db bh
  unless (fromIntegral h >= n) $ do
    _ <- receiveMatch sub $ \case
      StoreBestBlock b -> Just b
      _ -> Nothing
    waitHeight store sub n

-- | The store of the benchmark, with the megabytes of its block cache and
-- of its write buffer, if not RocksDB's default.
storeConfig ::
  Network -> FilePath -> Maybe String -> [Block] -> Mock -> Logger -> Int -> Maybe Int -> StoreConfig
storeConfig net dir redis blocks mock logger cache buffer =
  StoreConfig
    { maxPeers = 20,
      -- the mock peer, which lives in memory ('mockPeer'), by a name: a Unix
      -- socket, so that no address or port of the machine is named
      initPeers = ["unix:mock-peer"],
      peerPermissions = mempty,
      discover = False,
      db = dir,
      net = net,
      redis = redis,
      gap = 32,
      initGap = 20,
      redisMaxXPubs = 10000,
      xpubs = defaultXPubConfig,
      blockWindow = 64,
      mempoolTimeout = 14,
      peerTimeout = 60,
      blockStall = 10,
      blockPing = 5,
      maxPeerLife = 48 * 3600,
      connect = mockPeer blocks mock net ad,
      stats = Nothing,
      bloom = True,
      maxOpenFiles = Nothing,
      writeBufferSize = (* (1024 * 1024)) <$> buffer,
      maxWalSize = Nothing,
      backgroundJobs = Nothing,
      blockCache = Just (cache * 1024 * 1024),
      logger = logger
    }
  where
    ad = NetworkAddress nodeNetwork (sockToHostAddress (SockAddrInet 0 0))

quiet :: Logger
quiet = Logger {level = LevelError, source = "", push = \_ -> return ()}

webConfig :: Store -> Int -> WebConfig
webConfig store port =
  WebConfig
    { host = "127.0.0.1",
      port = port,
      store = store,
      maxLaggingBlocks = 1000000,
      maxPendingTxs = 1000000,
      minPeers = 0,
      limits = def,
      version = "bench",
      stats = Nothing,
      tickerRefresh = 90 * 1000 * 1000,
      tickerURL = "",
      priceHistoryURL = "",
      noBlockchainInfo = False,
      healthCheckInterval = 30,
      accessLog = False,
      tls = Nothing,
      nodeRpc = Nothing,
      nodeRpcSocket = Nothing,
      logger = quiet
    }

---------------------
-- Synthetic chain --
---------------------

benchXPub :: XPubKey
benchXPub = deriveXPubKey (makeXPrvKey "haskoin-store benchmark seed")

xpubAddr :: KeyIndex -> KeyIndex -> Address
xpubAddr m i = fst (deriveAddr (pubSubKey benchXPub m) i)

hotAddr :: Address
hotAddr = pubKeyAddr (derivePublicKey (wrapSecKey True key))
  where
    key = fromMaybe (error "key") (secKey (B.replicate 32 0x42))

scriptOf :: Address -> ByteString
scriptOf = fromMaybe (error "script") . addressToScriptBS

-- | Blocks at heights 1 to n on top of the regtest genesis. The last block
-- of a chain of 400 blocks or more also spends 3,000 outputs that earlier
-- blocks paid to the hot address, one transaction each, so that there is a
-- large block to answer about.
chain :: Network -> Int -> [Block]
chain net n = go 1 net.genesisHeader
  where
    hot = hotScript
    -- the transactions of the block at a height, after its coinbase
    regular h
      | h <= 1 = []
      | otherwise = spends h (txHash (coinbase (h - 1)))
    go h parent
      | h > n = []
      | otherwise =
          let extra = if h == n && n >= 400 then wide else []
              block = mine net parent (coinbase h : regular h <> extra)
           in block : go (h + 1) block.header
    -- the outputs to the hot address of the transactions of blocks 2 to
    -- 335, nine a block, each spent by a transaction of its own
    wide =
      [ Tx 1 [TxIn (OutPoint (txHash t) 1) "" maxBound] [TxOut 98990000 hot] [] 0
      | h <- [2 .. 335],
        t <- drop 1 (regular h)
      ]

-- | The script of the hot address.
hotScript :: ByteString
hotScript = scriptOf hotAddr

-- | The coinbase of the block at a height: it pays the hot address.
coinbase :: Int -> Tx
coinbase h = Tx 1 [TxIn nullOutPoint (encode (fromIntegral h :: Word32)) maxBound] [TxOut 5000000000 hotScript] [] 0

-- | The transactions of a block at a height but its coinbase, spending a
-- coinbase: one that splits it in nine, and nine that each pay an address of
-- the xpub, in turn, and the hot address.
spends :: Int -> TxHash -> [Tx]
spends h cb =
  let t1 = Tx 1 [TxIn (OutPoint cb 0) "" maxBound] (replicate 9 (TxOut 500000000 hotScript)) [] 0
      tk k =
        let j = fromIntegral (h * 9 + k) :: Word32
            a = xpubAddr (j `mod` 2) ((j `div` 2) `mod` 50)
         in Tx
              1
              [TxIn (OutPoint (txHash t1) (fromIntegral k)) "" maxBound]
              [TxOut 400000000 (scriptOf a), TxOut 99000000 hotScript]
              []
              0
   in t1 : map tk [0 .. 8]

mine :: Network -> BlockHeader -> [Tx] -> Block
mine net parent txs = Block header txs
  where
    merkle = either error id (buildMerkleRoot (map txHash txs))
    candidate = BlockHeader 0x20000000 (headerHash parent) merkle (parent.timestamp + 600) parent.bits
    header =
      fromMaybe (error "no nonce") $
        find (isValidPOW net) (map candidate [0 ..])

-----------------------
-- Microbenchmarks --
-----------------------

-- | Time per call of the pure functions on the encoding path of a response.
-- Each call gets one of 64 different inputs, so that GHC cannot share the
-- result between calls.
micro :: IO ()
micro = do
  let net = bchRegTest
      blocks = chain net 66
      txs = V.fromList [b.txs !! 2 | b <- drop 2 blocks]
      addrs = V.generate 64 (xpubAddr 0 . fromIntegral)
      hot = scriptOf hotAddr
      txdata tx =
        TxData
          { block = BlockRef 3 2,
            tx = tx,
            prevs = IntMap.fromList [(0, Prev hot 500000000)],
            deleted = False,
            rbf = False,
            timestamp = 1700000000,
            spenders = IntMap.fromList [(0, Spender (txHash tx) 0)]
          }
      tds = V.map txdata txs
      tdbytes = V.map encode tds
      ts = V.map (toTransaction net) tds
      pick :: V.Vector a -> Int -> a
      pick v i = v V.! (i `mod` 64)
  _ <- evaluate (force (tds, tdbytes, ts, addrs))
  bench "addrToText cashaddr" (addrToText net . pick addrs)
  bench "addrToText base58" (addrToText btc . pick addrs)
  bench "Address JSON (cashaddr)" (encodingToLazyByteString . marshalEncoding net . pick addrs)
  bench "txHash" (txHash . pick txs)
  bench "TxData decode" (\i -> either error id (decode (pick tdbytes i)) :: TxData)
  bench "toTransaction" (toTransaction net . pick tds)
  bench "Transaction JSON" (encodingToLazyByteString . marshalEncoding net . pick ts)
  bench "TxHash JSON" (encodingToLazyByteString . toEncoding . txHash . pick txs)
  bench "BlockRef JSON" (\i -> encodingToLazyByteString (toEncoding (BlockRef (fromIntegral i) 1)))
  bench "StoreOutput JSON" (encodingToLazyByteString . marshalEncoding net . firstOf . (.outputs) . pick ts)
  bench "StoreInput JSON" (encodingToLazyByteString . marshalEncoding net . firstOf . (.inputs) . pick ts)
  where
    n = 200000
    firstOf = fromMaybe (error "empty") . listToMaybe
    bench :: (NFData a) => String -> (Int -> a) -> IO ()
    bench name f = do
      _ <- evaluate (force (f 0))
      t0 <- getMonotonicTimeNSec
      let go i
            | i >= n = return ()
            | otherwise = evaluate (force (f i)) >> go (i + 1)
      go 0
      t1 <- getMonotonicTimeNSec
      putStrLn $ name <> ": " <> show (fromIntegral (t1 - t0) / fromIntegral n / 1000 :: Double) <> " us"

---------------
-- Mock peer --
---------------

mockPeer :: [Block] -> Mock -> Network -> NetworkAddress -> SockAddr -> WithConnection
mockPeer blocks mock net ad sa f = do
  r <- newInbox
  s <- newInbox
  let s' = inboxToMailbox s
  atomically $ modifyTVar' mock.peers (s' :)
  withAsync (go r s') $ \_ -> do
    let o = conduitMailbox r
        i = repeatMC (receive s)
    f (Conduits i o) :: IO ()
  where
    heights = HashMap.fromList (zip (map (headerHash . (.header)) blocks) [1 :: Int ..])
    byHash = HashMap.fromList [(headerHash b.header, b) | b <- blocks]
    go :: Inbox ByteString -> Mailbox ByteString -> IO ()
    go r s = do
      nonce <- randomIO
      now <- round <$> getPOSIXTime
      let rmt = NetworkAddress 0 (peerHostAddress sa)
          ver = buildVersion net nonce 0 ad rmt now
      runPut (putMessage net (MVersion ver)) `send` s
      runConduit $
        repeatMC (receive r)
          .| inc
          .| concatMapMC (liftIO . react)
          .| mapC (runPut . putMessage net)
          .| conduitMailbox s
    inc = forever $ do
      x <- takeCE 24 .| foldC
      case decode x of
        Left _ -> error "mock peer: bad message header"
        Right (MessageHeader _ _ len _) -> do
          y <- takeCE (fromIntegral len) .| foldC
          case runGet (getMessage net) (x <> y) of
            Right m -> yield m
            Left e -> error ("mock peer: " <> e)
    -- the blocks mined after the chain are few: they are looked up in a list
    react :: Message -> IO [Message]
    react (MPing (Ping n)) = return [MPong (Pong n)]
    react (MVersion _) = return [MVerAck]
    react (MGetHeaders (GetHeaders _ locator _)) = do
      more <- atomically (readTVar mock.extra)
      let later = HashMap.fromList (zip (map (headerHash . (.header)) more) [length blocks + 1 ..])
          at l = HashMap.lookup l heights <|> HashMap.lookup l later
          start = fromMaybe 0 (listToMaybe (mapMaybe at locator))
          hdr :: Block -> (BlockHeader, VarInt)
          hdr b = (b.header, VarInt (fromIntegral (length b.txs)))
      return [MHeaders (Headers (map hdr (take 2000 (drop start (blocks <> more)))))]
    react (MGetData (GetData ivs)) = do
      more <- atomically (readTVar mock.extra)
      let found h = HashMap.lookup h byHash <|> find ((== h) . headerHash . (.header)) more
      return [MBlock b | InvVector InvBlock h <- ivs, Just b <- [found (BlockHash h)]]
    react _ = return []
