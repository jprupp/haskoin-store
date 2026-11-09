{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE RecordWildCards #-}

-- |
-- Module      : Main
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Stress test of the Redis cache of xpubs. Run it with
-- @scripts/stress-cache.sh@, which builds it, runs it and writes a report.
--
-- It starts a regtest chain of Bitcoin Core, Valkey and wrk in Podman
-- containers from the test image ("Haskoin.Store.Integration.Image"), and a
-- haskoin-store server with the cache on. A few wallets, whose keys this
-- program holds, get many coins; then the program makes transactions between
-- them, signed here and sent to the node in batches, and mines a block after
-- each batch. Every transaction changes two cached xpubs, so the cache
-- reloads them again and again while wrk asks the server about them on five
-- endpoints at once. At the end, the cached answers are compared with those
-- of the database.
--
-- Everything it measures goes to files in the output directory, as JSON
-- lines: @rounds.jsonl@ (one line per block), @samples.jsonl@ (one line per
-- second: heights, CPU, memory and the StatsD metrics of the server),
-- @wrk.jsonl@ (one line per run of wrk) and @summary.json@.
-- @scripts/stress-report.py@ turns them into a report.
--
-- = Why it works this way
--
-- * __Transactions are signed here.__ A node wallet makes a few hundred
--   transactions per second at best, and slower as it grows; signing with
--   the keys in memory and sending raw transactions in batches keeps the node
--   busy only with validating them.
-- * __Only confirmed coins are spent.__ A block is mined after each batch, so
--   chains of unconfirmed transactions, which the node limits, never form.
-- * __The server is a separate process__, the release binary, so that its
--   CPU and memory are its own, and it is configured from the command line
--   as in production.
-- * __Containers are removed__ when the program ends, even when it fails:
--   they run with @--rm@ inside brackets, and carry a label with the process
--   ID, so that @scripts/stress-cache.sh@ and the next run remove any that a
--   killed run left.
module Main (main) where

import Control.Concurrent.STM (TVar, atomically, newTVarIO, readTVarIO, writeTVar)
import Control.Lens ((&), (.~), (?~), (^.))
import Control.DeepSeq (force)
import Control.Exception (evaluate)
import Control.Monad (forM, forM_, unless, void, when)
import Data.Aeson (Value (..), eitherDecode, encode, object, (.=))
import Data.Aeson qualified as A
import Data.ByteString.Char8 qualified as C8
import Data.ByteString.Lazy qualified as BL
import Data.Bytes.Put (runPutS)
import Data.Bytes.Serial (serialize)
import Data.Foldable (toList)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.List (sort)
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Sequence (Seq)
import Data.Sequence qualified as Seq
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (decodeUtf8)
import Data.Time.Clock (UTCTime, diffUTCTime, getCurrentTime)
import Data.Word (Word64)
import Database.Redis qualified as Redis
import Haskoin hiding (Coin, Key)
import Haskoin.Store.Integration.Image (testImage, valkeyBin, wrkBin)
import Haskoin.Store.Integration.Node
import Haskoin.Store.Integration.Podman
import Haskoin.Store.Integration.Valkey (redisContainer, waitRedis)
import Network.Socket qualified as Socket
import Network.Socket.ByteString qualified as SocketB
import Network.Wreq qualified as Wreq
import Options.Applicative
import UnliftIO.Directory (createDirectoryIfMissing, makeAbsolute)
import System.Exit (ExitCode (..))
import System.IO (Handle, IOMode (..), hFlush, hPutStrLn, stderr, withFile)
import System.Process
import System.Random.Stateful (IOGenM, StdGen, mkStdGen, newIOGenM, uniformRM)
import Text.Read (readMaybe)
import UnliftIO (MVar, bracket, newMVar, tryAny, withAsync, withMVar)
import UnliftIO.Concurrent (threadDelay)

-------------------------------------------------------------------------------
-- Options
-------------------------------------------------------------------------------

data Opts = Opts
  { txs :: !Int,
    wallets :: !Int,
    addresses :: !Int,
    blockTxs :: !Int,
    batch :: !Int,
    connections :: !Int,
    wrkSeconds :: !Int,
    server :: !FilePath,
    serverRts :: !String,
    out :: !FilePath,
    chainMb :: !Int,
    seed :: !Int
  }

opts :: Parser Opts
opts = do
  txs <- num "txs" 200000 "Transactions to make between the wallets"
  wallets <- num "wallets" 4 "Wallets, each an xpub"
  addresses <- num "addresses" 200 "Receiving and change addresses of each wallet"
  blockTxs <- num "block-txs" 2000 "Transactions per block"
  batch <- num "batch" 250 "Transactions per call to the node"
  connections <- num "connections" 2 "Connections of wrk to each endpoint"
  wrkSeconds <- num "wrk-seconds" 10 "Seconds of each run of wrk"
  server <- strOption (long "server" <> metavar "PATH" <> help "haskoin-store binary")
  serverRts <-
    strOption (long "server-rts" <> metavar "FLAGS" <> value "-N4 -M2g" <> showDefault <> help "RTS flags of the server")
  out <- strOption (long "out" <> metavar "DIR" <> help "Directory for the results")
  chainMb <- num "chain-mb" 1024 "Size of the tmpfs of the chain of the node, in MiB"
  seed <- num "seed" 1 "Seed of the random choices"
  return Opts {..}
  where
    num name def h = option auto (long name <> metavar "N" <> value def <> showDefault <> help h)

-------------------------------------------------------------------------------
-- Main
-------------------------------------------------------------------------------

main :: IO ()
main = do
  o <- execParser (info (opts <**> helper) (fullDesc <> progDesc "Stress test of the Redis cache of haskoin-store"))
  outDir <- makeAbsolute o.out
  createDirectoryIfMissing True outDir
  podmanUsable >>= either fail return
  say "Building or finding the test image"
  image <- testImage
  withContext $ \ctx ->
    withContainer (valkey image) $ \rc -> do
      redisURL <- ("redis://127.0.0.1:" <>) . show <$> hostPort rc 6379
      waitRedis redisURL
      withContainer (nodeSpec o image) $ \nc -> do
        rpcPort <- hostPort nc 18443
        p2p <- hostPort nc 18444
        let node = Node {kind = BitcoinCore, url = "http://127.0.0.1:" <> show rpcPort <> "/", p2p = p2p}
        waitRpc node
        withContainer (wrkSpec image outDir) $ \wc ->
          withStatsd $ \statsPort stats ->
            withRamDir $ \dir ->
              withServer o outDir dir redisURL p2p statsPort $ \srv ->
                stress o {out = outDir} ctx node redisURL srv stats wc

-- | Valkey with room for the bodies of every transaction.
valkey :: String -> ContainerSpec
valkey image =
  let spec = redisContainer image
   in spec
        { options = ["--memory", "1g", "--entrypoint", valkeyBin],
          command = map (\a -> if a == "128mb" then "768mb" else a) spec.command
        }

-- | The regtest node, with a larger chain and mempool than in the tests.
nodeSpec :: Opts -> String -> ContainerSpec
nodeSpec o image =
  let spec = nodeContainer BitcoinCore image
   in spec
        { options = map grow spec.options,
          command = map bigger spec.command <> ["-rpcworkqueue=64", "-rpcthreads=8"]
        }
  where
    grow "1g" = "3g"
    grow a
      | "/chain:" `T.isPrefixOf` T.pack a = "/chain:rw,size=" <> show o.chainMb <> "m,mode=1777"
      | otherwise = a
    bigger "-dbcache=16" = "-dbcache=128"
    bigger "-maxmempool=16" = "-maxmempool=128"
    bigger a = a

-- | A container that runs wrk against the server, on the network of the
-- host, with the output directory, where the scripts of wrk are, mounted.
wrkSpec :: String -> FilePath -> ContainerSpec
wrkSpec image dir =
  ContainerSpec
    { name = "wrk",
      image = image,
      options =
        [ "--network",
          "host",
          "--memory",
          "512m",
          "--security-opt",
          "label=disable",
          "--volume",
          dir <> ":/stress:ro",
          "--entrypoint",
          "sleep"
        ],
      command = ["infinity"],
      ports = []
    }

-------------------------------------------------------------------------------
-- The server
-------------------------------------------------------------------------------

data Server = Server
  { url :: !String,
    pid :: !Int
  }

-- | Run the haskoin-store server while an action runs.
withServer :: Opts -> FilePath -> FilePath -> String -> Int -> Int -> (Server -> IO a) -> IO a
withServer o outDir dir redisURL p2p statsPort act =
  withFile (outDir <> "/server.log") WriteMode $ \logH -> do
    -- another program could take the port before the server binds it, but
    -- the server then fails to start, which is reported
    port <- freePort
    let gap = o.addresses + 20
        args =
          [ "--dir",
            dir,
            "--host",
            "127.0.0.1",
            "--port",
            show port,
            "--net",
            "btcreg",
            "--peer",
            "127.0.0.1:" <> show p2p,
            "--max-peers",
            "1",
            "--redis",
            "--redis-url",
            redisURL,
            "--redis-min-addrs",
            "10",
            "--xpub-gap",
            show gap,
            "--xpub-gap-init",
            show gap,
            "--statsd",
            "--statsd-host",
            "127.0.0.1",
            "--statsd-port",
            show statsPort,
            "--statsd-prefix",
            "stress",
            "--statsd-aggregates",
            "--ticker-url",
            "http://127.0.0.1:9/",
            "--price-history-url",
            "http://127.0.0.1:9/",
            "+RTS"
          ]
            <> words o.serverRts
            <> ["-RTS"]
        cp = (proc o.server args) {std_out = UseHandle logH, std_err = UseHandle logH}
    bracket (createProcess cp) stop $ \(_, _, _, ph) -> do
      pid <- maybe (fail "The server did not start") (return . fromIntegral) =<< getPid ph
      writeFile (outDir <> "/server.pid") (show pid)
      let srv = Server {url = "http://127.0.0.1:" <> show port, pid = pid}
      waitFor 60 "the server to answer" $ do
        getProcessExitCode ph >>= \case
          Just c -> fail ("The server exited with " <> show c <> "; see server.log")
          Nothing -> either (const False) (const True) <$> tryAny (getJSON srv "/health")
      act srv
  where
    stop (_, _, _, ph) = terminateProcess ph >> void (waitForProcess ph)

-- | A free TCP port of the loopback interface.
freePort :: IO Int
freePort =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \s -> do
    Socket.bind s (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    fromIntegral <$> Socket.socketPort s

-- | JSON answer of the server.
getJSON :: Server -> String -> IO Value
getJSON srv path = do
  let o = Wreq.defaults & Wreq.checkResponse ?~ (\_ _ -> return ())
  r <- Wreq.getWith o (srv.url <> path)
  either fail return (eitherDecode (r ^. Wreq.responseBody))

-------------------------------------------------------------------------------
-- Wallets
-------------------------------------------------------------------------------

-- | An address and its key.
data Key = Key
  { script :: !ScriptOutput,
    secret :: !SecKey
  }

-- | A wallet: its xpub, and its receiving and change addresses.
data Wallet = Wallet
  { xpub :: !Text,
    receive :: !(Seq Key),
    change :: !(Seq Key)
  }

-- | A coin that a wallet can spend.
data Coin = Coin
  { outpoint :: !OutPoint,
    value :: !Word64,
    wallet :: !Int,
    key :: !Key
  }

-- | The wallets, derived from a fixed seed, and the key of the miner.
makeWallets :: Ctx -> Opts -> ([Wallet], Key)
makeWallets ctx o = ([wallet (fromIntegral i) | i <- [0 .. o.wallets - 1]], keyOf (hardSubKey ctx root 9999) 0 0)
  where
    root = makeXPrvKey (C8.pack ("haskoin-store-stress-" <> show o.seed))
    wallet i =
      let w = hardSubKey ctx root i
       in Wallet
            { xpub = xPubExport btcRegTest ctx (deriveXPubKey ctx w),
              receive = Seq.fromList [keyOf w 0 j | j <- [0 .. fromIntegral o.addresses - 1]],
              change = Seq.fromList [keyOf w 1 j | j <- [0 .. fromIntegral o.addresses - 1]]
            }
    keyOf w c j =
      let k = prvSubKey ctx (prvSubKey ctx w c) j
          a = xPubAddr ctx (deriveXPubKey ctx k)
       in Key {script = fromMaybe (error "no script") (addressToOutput a), secret = k.key}

-- | Address of a key, as text.
keyAddress :: Key -> Text
keyAddress k = case k.script of
  PayPKHash h -> fromMaybe (error "no address") (addrToText btcRegTest (PubKeyAddress h))
  _ -> error "not a key hash"

-- | Sign a transaction that spends coins.
sign :: Ctx -> Tx -> [Coin] -> Tx
sign ctx tx coins =
  either error id $
    signTx
      btcRegTest
      ctx
      tx
      [SigInput c.key.script c.value c.outpoint sigHashAll Nothing | c <- coins]
      [c.key.secret | c <- coins]

-- | A payment from a coin to another wallet, with change back to the wallet
-- of the coin. Gives the transaction and its new coins.
pay :: Ctx -> IOGenM StdGen -> [Wallet] -> Coin -> IO (Tx, [Coin])
pay ctx g ws c = do
  w' <- (\i -> (c.wallet + i) `mod` length ws) <$> uniformRM (1, max 1 (length ws - 1)) g
  let to = ws !! w'
      from = ws !! c.wallet
  r <- (`Seq.index` 0) . flip Seq.drop to.receive <$> uniformRM (0, Seq.length to.receive - 1) g
  ch <- (`Seq.index` 0) . flip Seq.drop from.change <$> uniformRM (0, Seq.length from.change - 1) g
  amount <- uniformRM (10000, max 10000 (c.value `div` 2)) g
  -- every output is worth at least 10000, so a coin always pays the fee
  let fee = 1000
      outs
        | c.value >= amount + fee + 10000 = [(r.script, amount), (ch.script, c.value - amount - fee)]
        | otherwise = [(r.script, c.value - fee)]
      tx = sign ctx (buildTx ctx [c.outpoint] outs) [c]
      h = txHash tx
      owners = [(w', r), (c.wallet, ch)]
      coins = [Coin (OutPoint h i) v w k | (i, (_, v), (w, k)) <- zip3 [0 ..] outs owners]
  return (tx, coins)

-------------------------------------------------------------------------------
-- The node
-------------------------------------------------------------------------------

-- | Send transactions to the node in one call. Gives the error of each
-- transaction that the node refused.
sendRaw :: Node -> [Tx] -> IO [Maybe Value]
sendRaw node txs = do
  let o =
        Wreq.defaults
          & Wreq.auth ?~ Wreq.basicAuth "test" "test"
          & Wreq.checkResponse ?~ (\_ _ -> return ())
          & Wreq.header "Content-Type" .~ ["application/json"]
      body =
        A.toJSON
          [ object
              [ "jsonrpc" .= ("1.0" :: Text),
                "id" .= i,
                "method" .= ("sendrawtransaction" :: Text),
                "params" .= [encodeHex (runPutS (serialize tx))]
              ]
          | (i, tx) <- zip [0 :: Int ..] txs
          ]
  r <- Wreq.postWith o node.url body
  case eitherDecode (r ^. Wreq.responseBody) of
    Right (Array xs) ->
      return
        [ case field "error" x of
            Just Null -> Nothing
            Nothing -> Nothing
            e -> e
        | x <- toList xs
        ]
    Right v -> fail ("Unexpected answer to a batch: " <> take 400 (show v))
    Left e -> fail ("Bad answer to a batch: " <> e)

-- | The coinbase output of a block.
coinbaseOf :: Node -> Text -> IO (OutPoint, Word64)
coinbaseOf node h = do
  b <- rpc node "getblock" [String h, Number 2]
  case field "tx" b of
    Just (Array ts) | Just t <- Seq.lookup 0 (Seq.fromList (toList ts)) -> do
      txid <- maybe (fail "no txid") return (textField "txid" t >>= hexToTxHash)
      v <- case field "vout" t of
        Just (Array os) | o : _ <- toList os, Just v <- field "value" o -> return (sats v)
        _ -> fail "no coinbase output"
      return (OutPoint txid 0, v)
    _ -> fail ("No transactions in block " <> T.unpack h)

-- | Height of the best block of the node.
nodeHeight :: Node -> IO Int
nodeHeight node =
  rpc node "getblockcount" [] >>= \case
    Number n -> return (round n)
    v -> fail ("Unexpected block count: " <> show v)

-------------------------------------------------------------------------------
-- The stress test
-------------------------------------------------------------------------------

stress :: Opts -> Ctx -> Node -> String -> Server -> IORef Stats -> Container -> IO ()
stress o ctx node redisURL srv stats wc = do
  g <- newIOGenM (mkStdGen o.seed)
  let (ws, miner) = makeWallets ctx o
      minerAddr = keyAddress miner
      perFanOut = 1000
      fanOuts = max 1 ((o.blockTxs * 3 `div` 2 + perFanOut - 1) `div` perFanOut)
  writeJSON (o.out <> "/config.json") $
    object
      [ "txs" .= o.txs,
        "wallets" .= o.wallets,
        "addresses" .= o.addresses,
        "block_txs" .= o.blockTxs,
        "batch" .= o.batch,
        "connections" .= o.connections,
        "wrk_seconds" .= o.wrkSeconds,
        "server_rts" .= o.serverRts,
        "xpubs" .= map (.xpub) ws
      ]
  say ("Funding " <> show (length ws) <> " wallets with " <> show (fanOuts * perFanOut) <> " coins")
  funded <- generateTo node fanOuts minerAddr
  void $ generateTo node 100 minerAddr
  coins0 <- fmap concat . forM funded $ \h -> do
    (op, v) <- coinbaseOf node h
    let n = perFanOut
        each = (v - 200000) `div` fromIntegral n
        -- every receiving address of every wallet, in turn
        targets = take n (cycle [(wi, Seq.index w.receive j) | j <- [0 .. o.addresses - 1], (wi, w) <- zip [0 ..] ws])
        tx = sign ctx (buildTx ctx [op] [(k.script, each) | (_, k) <- targets]) [Coin op v (-1) miner]
    errs <- sendRaw node [tx]
    forM_ errs $ mapM_ (\e -> fail ("The node refused a funding transaction: " <> show e))
    return [Coin (OutPoint (txHash tx) i) each wi k | (i, (wi, k)) <- zip [0 ..] targets]
  void $ generateTo node 1 minerAddr
  say "Waiting for the server to sync and cache the xpubs"
  waitSynced node srv
  forM_ ws $ \w -> getJSON srv ("/xpub/" <> T.unpack w.xpub <> "/balances")
  waitFor 120 "the xpubs to be cached" $ do
    conn <- Redis.checkedConnect (either error id (Redis.parseConnectInfo redisURL))
    n <- either (const 0) id <$> Redis.runRedis conn (Redis.dbsize)
    Redis.disconnect conn
    return (n > 0)
  writeScripts o ws
  stop <- newTVarIO False
  start <- getCurrentTime
  withFile (o.out <> "/samples.jsonl") WriteMode $ \sh ->
    withFile (o.out <> "/wrk.jsonl") WriteMode $ \wh -> do
      wlock <- newMVar wh
      withAsync (sampler start node srv redisURL stats stop sh) $ \_ ->
        withAsync (wrkAll o srv wc start stop wlock) $ \_ -> do
          (sent, refused) <- withFile (o.out <> "/rounds.jsonl") WriteMode $ \rh ->
            rounds o ctx g node ws minerAddr start rh (Seq.fromList coins0)
          say "Waiting for the server to catch up"
          t0 <- getCurrentTime
          waitSynced node srv
          catchUp <- (`diffUTCTime` t0) <$> getCurrentTime
          atomically (writeTVar stop True)
          -- let the last runs of wrk and the sampler finish
          threadDelay ((o.wrkSeconds + 3) * 1000000)
          say "Comparing cached answers with the database"
          checks <- forM ws (consistent srv)
          end <- getCurrentTime
          height <- nodeHeight node
          writeJSON (o.out <> "/summary.json") $
            object
              [ "transactions_sent" .= sent,
                "transactions_refused" .= refused,
                "height" .= height,
                "seconds" .= (realToFrac (end `diffUTCTime` start) :: Double),
                "catch_up_seconds" .= (realToFrac catchUp :: Double),
                "consistency" .= concat checks
              ]
          say ("Done: " <> show sent <> " transactions, " <> show refused <> " refused")

-- | Make the transactions, a block at a time. Gives how many were sent and
-- how many the node refused.
rounds :: Opts -> Ctx -> IOGenM StdGen -> Node -> [Wallet] -> Text -> UTCTime -> Handle -> Seq Coin -> IO (Int, Int)
rounds o ctx g node ws minerAddr start rh = go 0 (0, 0)
  where
    total = o.txs
    go :: Int -> (Int, Int) -> Seq Coin -> IO (Int, Int)
    go r (sent, refused) pool
      | sent >= total = return (sent, refused)
      | otherwise = do
          let n = minimum [o.blockTxs, total - sent, Seq.length pool]
              (spend, rest) = Seq.splitAt n pool
          when (n == 0) $ fail "No coins left to spend"
          t0 <- getCurrentTime
          made <- mapM (pay ctx g ws) (toList spend)
          -- sign now, not when the transactions are sent
          mapM_ (evaluate . force . fst) made
          t1 <- getCurrentTime
          results <- fmap concat . forM (chunks o.batch (zip (toList spend) made)) $ \chunk -> do
            errs <- sendRaw node [tx | (_, (tx, _)) <- chunk]
            return (zip chunk errs)
          t2 <- getCurrentTime
          let accepted = [new | ((_, (_, new)), Nothing) <- results]
              back = [c | ((c, _), Just _) <- results]
              errors = [e | (_, Just e) <- results]
          case errors of
            e : _ -> say ("The node refused " <> show (length errors) <> " transactions, such as " <> take 300 (show e))
            [] -> return ()
          void $ generateTo node 1 minerAddr
          t3 <- getCurrentTime
          height <- nodeHeight node
          let sent' = sent + length accepted
          appendLine rh $
            object
              [ "round" .= r,
                "t" .= secs start t3,
                "height" .= height,
                "sent" .= sent',
                "refused" .= length errors,
                "sign_ms" .= ms t0 t1,
                "submit_ms" .= ms t1 t2,
                "mine_ms" .= ms t2 t3
              ]
          when (r `mod` 10 == 0) $
            say ("Round " <> show r <> ": " <> show sent' <> " transactions, height " <> show height)
          go (r + 1) (sent', refused + length errors) (rest <> Seq.fromList (back <> concat accepted))
    chunks k xs = case splitAt k xs of
      ([], _) -> []
      (a, b) -> a : chunks k b

-- | Wait until the server has the best block of the node.
waitSynced :: Node -> Server -> IO ()
waitSynced node srv =
  waitFor 1800 "the server to sync" $ do
    h <- nodeHeight node
    serverHeight srv <&&> (== h)
  where
    a <&&> f = maybe False f <$> a

-- | Height of the best block of the server.
serverHeight :: Server -> IO (Maybe Int)
serverHeight srv =
  tryAny (getJSON srv "/block/best?notx=true") <&> \case
    Right v | Just (Number n) <- field "height" v -> Just (round n)
    _ -> Nothing
  where
    (<&>) = flip fmap

-- | Compare the cached answers about an xpub with those of the database,
-- for up to a minute, as the cache may still be catching up.
consistent :: Server -> Wallet -> IO [Value]
consistent srv w =
  forM paths $ \p -> do
    ok <- tryWait (60 :: Int) p
    return (object ["xpub" .= w.xpub, "path" .= p, "same" .= ok])
  where
    paths = ["", "/balances", "/transactions?limit=100", "/unspent?limit=100"]
    base = "/xpub/" <> T.unpack w.xpub
    sep p = if '?' `elem` p then "&" else "?"
    tryWait n p = do
      a <- getJSON srv (base <> p)
      b <- getJSON srv (base <> p <> sep p <> "nocache=true")
      if a == b || n <= 0
        then return (a == b)
        else threadDelay 1000000 >> tryWait (n - 1) p

-------------------------------------------------------------------------------
-- wrk
-------------------------------------------------------------------------------

-- | Endpoints that wrk asks about, each rotating over every xpub.
endpoints :: [(String, Text -> Text)]
endpoints =
  [ ("summary", \x -> "/xpub/" <> x),
    ("balances", \x -> "/xpub/" <> x <> "/balances"),
    ("transactions", \x -> "/xpub/" <> x <> "/transactions?limit=100"),
    ("unspent", \x -> "/xpub/" <> x <> "/unspent?limit=100"),
    ("multiaddr", \x -> "/blockchain/multiaddr?active=" <> x <> "&n=50")
  ]

-- | A script of wrk per endpoint: it rotates over the paths, and prints its
-- results as JSON when done (latencies in microseconds).
writeScripts :: Opts -> [Wallet] -> IO ()
writeScripts o ws =
  forM_ endpoints $ \(name, path) ->
    writeFile (o.out <> "/" <> name <> ".lua") . unlines $
      ["local paths = {"]
        <> ["  \"" <> T.unpack (path w.xpub) <> "\"," | w <- ws]
        <> [ "}",
             "local i = 0",
             "request = function()",
             "  i = i % #paths + 1",
             "  return wrk.format(\"GET\", paths[i])",
             "end",
             "done = function(summary, latency, requests)",
             "  local e = summary.errors",
             "  io.write(string.format('{\"requests\":%d,\"duration_us\":%d,\"errors\":%d,\"non2xx\":%d,\"p50_us\":%d,\"p90_us\":%d,\"p99_us\":%d,\"max_us\":%d,\"mean_us\":%.1f}\\n',",
             "    summary.requests, summary.duration, e.connect + e.read + e.write + e.timeout, e.status,",
             "    latency:percentile(50), latency:percentile(90), latency:percentile(99), latency.max, latency.mean))",
             "end"
           ]

-- | Run wrk on every endpoint at once, over and over, until told to stop.
wrkAll :: Opts -> Server -> Container -> UTCTime -> TVar Bool -> MVar Handle -> IO ()
wrkAll o srv wc start stop lock = go endpoints
  where
    go [] = return ()
    go ((name, _) : rest) = withAsync (loop name) $ \_ -> go rest >> waitStop
    waitStop = readTVarIO stop >>= \s -> unless s (threadDelay 500000 >> waitStop)
    loop name = do
      s <- readTVarIO stop
      unless s $ do
        t <- getCurrentTime
        (code, out, err) <-
          readProcessWithExitCode
            "podman"
            [ "exec",
              wc.cid,
              wrkBin,
              "-t1",
              "-c" <> show o.connections,
              "-d" <> show o.wrkSeconds <> "s",
              "--timeout",
              "10s",
              "-s",
              "/stress/" <> name <> ".lua",
              srv.url
            ]
            ""
        let result = case [l | l <- lines out, take 1 l == "{"] of
              l : _ | Just (Object r) <- A.decode (BL.fromStrict (C8.pack l)) -> Object r
              _ -> object ["failed" .= True, "exit" .= show code, "stderr" .= take 300 err]
        withMVar lock $ \h ->
          appendLine h (merge (object ["t" .= secs start t, "endpoint" .= name]) result)
        loop name
    merge (Object a) (Object b) = Object (a <> b)
    merge a _ = a

-------------------------------------------------------------------------------
-- Sampling
-------------------------------------------------------------------------------

-- | Write a sample every second until told to stop: the heights of the node
-- and the server, the CPU and memory of the server, the memory of Valkey, and
-- the StatsD metrics of the server since the last sample.
sampler :: UTCTime -> Node -> Server -> String -> IORef Stats -> TVar Bool -> Handle -> IO ()
sampler start node srv redisURL stats stop h = do
  hz <- clockTicks
  conn <- Redis.checkedConnect (either error id (Redis.parseConnectInfo redisURL))
  let go prevTicks prevTime = do
        s <- readTVarIO stop
        unless s $ do
          threadDelay 1000000
          now <- getCurrentTime
          ticks <- cpuTicks srv.pid
          rss <- rssKb srv.pid
          nh <- either (const Nothing) Just <$> tryAny (nodeHeight node)
          sh <- serverHeight srv
          vm <- valkeyMemory conn
          m <- drainStats stats
          let cpu = fromIntegral (max 0 (ticks - prevTicks)) / fromIntegral hz / realToFrac (now `diffUTCTime` prevTime) * 100 :: Double
          appendLine h $
            object
              [ "t" .= secs start now,
                "node_height" .= nh,
                "server_height" .= sh,
                "cpu_percent" .= cpu,
                "rss_mb" .= (fromIntegral rss / 1024 :: Double),
                "valkey_mb" .= ((/ (1024 * 1024)) . fromIntegral <$> vm :: Maybe Double),
                "counters" .= m.counters,
                "gauges" .= m.gauges,
                "timings" .= HashMap.map timingStats m.timings
              ]
          go ticks now
  t <- getCurrentTime
  ticks0 <- cpuTicks srv.pid
  go ticks0 t
  where
    timingStats vs =
      let s = sort vs
          n = length s
          at p = s !! min (n - 1) (floor (p * fromIntegral n :: Double))
       in object ["n" .= n, "p50" .= at 0.5, "p95" .= at 0.95, "max" .= last s, "sum" .= sum s]

-- | Ticks of the clock of the kernel per second.
clockTicks :: IO Int
clockTicks = do
  r <- tryAny (readProcessWithExitCode "getconf" ["CLK_TCK"] "")
  return $ case r of
    Right (ExitSuccess, o, _) | Just n <- readMaybe o -> n
    _ -> 100

-- | CPU time of a process, in ticks of the clock of the kernel.
cpuTicks :: Int -> IO Int
cpuTicks pid = do
  r <- tryAny (readFile ("/proc/" <> show pid <> "/stat"))
  return $ case r of
    -- the fields after the name, which is in parentheses and may have spaces
    Right s
      | fs <- words (drop 2 (dropWhile (/= ')') s)),
        length fs > 13 ->
          sum (mapMaybe readMaybe [fs !! 11, fs !! 12])
    _ -> 0

-- | Resident memory of a process, in KiB.
rssKb :: Int -> IO Int
rssKb pid = do
  r <- tryAny (readFile ("/proc/" <> show pid <> "/status"))
  return $ case r of
    Right s | (v : _) <- [w | l <- lines s, ("VmRSS:" : w : _) <- [words l]], Just n <- readMaybe v -> n
    _ -> 0

-- | Memory used by Valkey, in bytes.
valkeyMemory :: Redis.Connection -> IO (Maybe Integer)
valkeyMemory conn =
  Redis.runRedis conn Redis.info <&&> \case
    Right s ->
      case [v | l <- C8.lines s, Just v <- [C8.stripPrefix "used_memory:" l]] of
        v : _ -> fst <$> C8.readInteger v
        [] -> Nothing
    Left _ -> Nothing
  where
    a <&&> f = f <$> a

-------------------------------------------------------------------------------
-- StatsD
-------------------------------------------------------------------------------

-- | Metrics received since they were last drained: counters added up,
-- gauges at their last value, and every timing.
data Stats = Stats
  { counters :: !(HashMap Text Double),
    gauges :: !(HashMap Text Double),
    timings :: !(HashMap Text [Double])
  }

-- | Receive the StatsD metrics of the server on a UDP port of the loopback
-- interface while an action runs.
withStatsd :: (Int -> IORef Stats -> IO a) -> IO a
withStatsd act =
  bracket (Socket.socket Socket.AF_INET Socket.Datagram Socket.defaultProtocol) Socket.close $ \s -> do
    Socket.bind s (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    port <- fromIntegral <$> Socket.socketPort s
    ref <- newIORef (Stats HashMap.empty HashMap.empty HashMap.empty)
    let receive = do
          bs <- SocketB.recv s 65535
          forM_ (C8.lines bs) $ \l -> forM_ (parseLine l) $ \m ->
            atomicModifyIORef' ref (\st -> (add m st, ()))
          receive
    withAsync receive $ \_ -> act port ref
  where
    add (k, v, t) st = case t of
      'c' -> st {counters = HashMap.insertWith (+) k v st.counters}
      'g' -> st {gauges = HashMap.insert k v st.gauges}
      _ -> st {timings = HashMap.insertWith (<>) k [v] st.timings}

-- | A StatsD line, @key:value|type@ with an optional @|\@rate@: the key
-- without the prefix of the server, the value (scaled up by the rate for a
-- sampled counter), and the first letter of the type.
parseLine :: C8.ByteString -> Maybe (Text, Double, Char)
parseLine l = do
  let (k, rest) = C8.break (== ':') l
  (v, t, rate) <- case C8.split '|' (C8.drop 1 rest) of
    [v, t] -> Just (v, t, 1)
    [v, t, r] | Just ('@', x) <- C8.uncons r -> (v,t,) <$> readMaybe (C8.unpack x)
    _ -> Nothing
  x <- readMaybe (C8.unpack (C8.dropWhile (== '+') v))
  c <- fst <$> C8.uncons t
  let key = T.replace "stress.btcreg." "" (decodeUtf8 k)
  return (key, if c == 'c' then x / rate else x, c)

-- | Take the counters and timings received since the last call; gauges
-- keep their values.
drainStats :: IORef Stats -> IO Stats
drainStats ref = atomicModifyIORef' ref $ \st -> (st {counters = HashMap.empty, timings = HashMap.empty}, st)

-------------------------------------------------------------------------------
-- Helpers
-------------------------------------------------------------------------------

say :: String -> IO ()
say s = hPutStrLn stderr ("stress: " <> s) >> hFlush stderr

-- | Try a check every quarter of a second, for up to some seconds.
waitFor :: Int -> String -> IO Bool -> IO ()
waitFor secs' what check = go (secs' * 4)
  where
    go n =
      check >>= \case
        True -> return ()
        False
          | n <= 0 -> fail ("Timed out waiting for " <> what)
          | otherwise -> threadDelay 250000 >> go (n - 1)

secs :: UTCTime -> UTCTime -> Double
secs a b = realToFrac (b `diffUTCTime` a)

ms :: UTCTime -> UTCTime -> Double
ms a b = 1000 * secs a b

appendLine :: Handle -> Value -> IO ()
appendLine h v = BL.hPut h (encode v <> "\n") >> hFlush h

writeJSON :: FilePath -> Value -> IO ()
writeJSON path = BL.writeFile path . encode
