-- | Load-test server for the web API.
--
-- Syncs a synthetic regtest chain from an in-process mock peer into a fresh
-- database, then serves the web API on it until killed, so that a load
-- generator such as wrk can be pointed at it. See @scripts/bench-web.sh@.
--
-- > haskoin-store-bench PORT BLOCKS [REDIS_URL]
--
-- Every block holds a coinbase and ten transactions. The coinbase and one
-- output of each transaction pay the "hot" address, so it gains eleven
-- transactions per block. Nine outputs per block go to the first 100
-- external and change addresses of the benchmark xpub, in turn. Both are
-- printed once the chain is synced.
module Main (main) where

import Conduit
import Control.DeepSeq (NFData, force)
import Control.Monad (forever, unless)
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
import Haskoin.Store
import Haskoin.Store.Database.Query qualified as Q
import NQE
import Network.Socket (SockAddr (..))
import System.Environment (getArgs)
import System.Random (randomIO)
import UnliftIO

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["micro"] -> micro
    _ -> server args

server :: [String] -> IO ()
server args = do
  let (port, nblocks, redis) = case args of
        [p, n] -> (read p, read n, Nothing)
        [p, n, r] -> (read p, read n, Just r)
        _ -> error "usage: haskoin-store-bench PORT BLOCKS [REDIS_URL]"
  createContext >>= \ctx ->
    withSystemTempDirectory "haskoin-store-bench-" $ \dir -> do
      let net = bchRegTest
          blocks = chain ctx net nblocks
      requests <- newTVarIO []
      withStore (storeConfig ctx net dir redis blocks requests) $ \store -> do
        withSubscription store.pub $ \sub -> waitHeight store sub nblocks
        T.putStrLn $ "hot address: " <> addrText net (hotAddr ctx)
        T.putStrLn $ "xpub: " <> xPubExport net ctx (benchXPub ctx)
        T.putStrLn $ "xpub address: " <> addrText net (xpubAddr ctx 0 0)
        bh <- Q.getBestBlock store.db
        T.putStrLn $ "best block: " <> maybe "" blockHashToHex bh
        putStrLn $ "serving on port " <> show port
        hFlush stdout
        runWeb (webConfig store port)

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

storeConfig ::
  Ctx -> Network -> FilePath -> Maybe String -> [Block] -> TVar [Int] -> StoreConfig
storeConfig ctx net dir redis blocks requests =
  StoreConfig
    { maxPeers = 20,
      initPeers = [],
      discover = True,
      db = dir,
      net = net,
      ctx = ctx,
      redis = redis,
      gap = 32,
      initGap = 20,
      redisMinAddrs = 20,
      redisMinTxs = 1000,
      redisMaxXPubs = 10000,
      noMempool = False,
      wipeMempool = False,
      syncMempool = False,
      blockWindow = 64,
      mempoolTimeout = 14,
      peerTimeout = 60,
      maxPeerLife = 48 * 3600,
      connect = mockPeer blocks requests net ad,
      stats = Nothing,
      bloom = True,
      maxOpenFiles = Nothing,
      writeBufferSize = Nothing,
      maxWalSize = Nothing,
      blockCache = Just (1024 * 1024 * 1024),
      logger = quiet
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
      noMempool = False,
      stats = Nothing,
      tickerRefresh = 90 * 1000 * 1000,
      tickerURL = "",
      priceHistoryURL = "",
      noXPub = False,
      noBlockchainInfo = False,
      healthCheckInterval = 30,
      logger = quiet
    }

---------------------
-- Synthetic chain --
---------------------

benchXPub :: Ctx -> XPubKey
benchXPub ctx = deriveXPubKey ctx (makeXPrvKey "haskoin-store benchmark seed")

xpubAddr :: Ctx -> KeyIndex -> KeyIndex -> Address
xpubAddr ctx m i = fst (deriveAddr ctx (pubSubKey ctx (benchXPub ctx) m) i)

hotAddr :: Ctx -> Address
hotAddr ctx = pubKeyAddr ctx (derivePublicKey ctx (wrapSecKey True key))
  where
    key = fromMaybe (error "key") (secKey (B.replicate 32 0x42))

scriptOf :: Ctx -> Address -> ByteString
scriptOf ctx = fromMaybe (error "script") . addressToScriptBS ctx

-- | Blocks at heights 1 to n on top of the regtest genesis.
chain :: Ctx -> Network -> Int -> [Block]
chain ctx net n = go 1 net.genesisHeader Nothing
  where
    hot = scriptOf ctx (hotAddr ctx)
    go h parent prevcb
      | h > n = []
      | otherwise =
          let cb = Tx 1 [TxIn nullOutPoint (encode (fromIntegral h :: Word32)) maxBound] [TxOut 5000000000 hot] [] 0
              txs = maybe [] (spends h) prevcb
              block = mine net parent (cb : txs)
           in block : go (h + 1) block.header (Just (txHash cb))
    spends h cb =
      let t1 = Tx 1 [TxIn (OutPoint cb 0) "" maxBound] (replicate 9 (TxOut 500000000 hot)) [] 0
          tk k =
            let j = fromIntegral (h * 9 + k) :: Word32
                a = xpubAddr ctx (j `mod` 2) ((j `div` 2) `mod` 50)
             in Tx
                  1
                  [TxIn (OutPoint (txHash t1) (fromIntegral k)) "" maxBound]
                  [TxOut 400000000 (scriptOf ctx a), TxOut 99000000 hot]
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
  ctx <- createContext
  let net = bchRegTest
      blocks = chain ctx net 66
      txs = V.fromList [b.txs !! 2 | b <- drop 2 blocks]
      addrs = V.generate 64 (xpubAddr ctx 0 . fromIntegral)
      hot = scriptOf ctx (hotAddr ctx)
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
      ts = V.map (toTransaction net ctx) tds
      pick :: V.Vector a -> Int -> a
      pick v i = v V.! (i `mod` 64)
  _ <- evaluate (force (tds, tdbytes, ts, addrs))
  bench "addrToText cashaddr" (addrToText net . pick addrs)
  bench "addrToText base58" (addrToText btc . pick addrs)
  bench "Address JSON (cashaddr)" (encodingToLazyByteString . marshalEncoding net . pick addrs)
  bench "txHash" (txHash . pick txs)
  bench "TxData decode" (\i -> either error id (decode (pick tdbytes i)) :: TxData)
  bench "toTransaction" (toTransaction net ctx . pick tds)
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

mockPeer :: [Block] -> TVar [Int] -> Network -> NetworkAddress -> SockAddr -> WithConnection
mockPeer blocks _requests net ad sa f = do
  r <- newInbox
  s <- newInbox
  let s' = inboxToMailbox s
  withAsync (go r s') $ \_ -> do
    let o = awaitForever (`send` r)
        i = forever (receive s >>= yield)
    f (Conduits i o) :: IO ()
  where
    heights = HashMap.fromList (zip (map (headerHash . (.header)) blocks) [1 :: Int ..])
    byHash = HashMap.fromList [(headerHash b.header, b) | b <- blocks]
    go :: Inbox ByteString -> Mailbox ByteString -> IO ()
    go r s = do
      nonce <- randomIO
      now <- round <$> liftIO getPOSIXTime
      let rmt = NetworkAddress 0 (sockToHostAddress sa)
          ver = buildVersion net nonce 0 ad rmt now
      runPut (putMessage net (MVersion ver)) `send` s
      runConduit $
        forever (receive r >>= yield)
          .| inc
          .| concatMapC react
          .| mapC (runPut . putMessage net)
          .| awaitForever (`send` s)
    inc = forever $ do
      x <- takeCE 24 .| foldC
      case decode x of
        Left _ -> error "mock peer: bad message header"
        Right (MessageHeader _ _ len _) -> do
          y <- takeCE (fromIntegral len) .| foldC
          case runGet (getMessage net) (x <> y) of
            Right m -> yield m
            Left e -> error ("mock peer: " <> e)
    react (MPing (Ping n)) = [MPong (Pong n)]
    react (MVersion _) = [MVerAck]
    react (MGetHeaders (GetHeaders _ locator _)) =
      let start = fromMaybe 0 (listToMaybe (mapMaybe (`HashMap.lookup` heights) locator))
          hdr :: Block -> (BlockHeader, VarInt)
          hdr b = (b.header, VarInt (fromIntegral (length b.txs)))
       in [MHeaders (Headers (map hdr (take 2000 (drop start blocks))))]
    react (MGetData (GetData ivs)) =
      [MBlock b | InvVector InvBlock h <- ivs, Just b <- [HashMap.lookup (BlockHash h) byHash]]
    react _ = []
