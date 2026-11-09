-- |
-- Module      : Haskoin.Store.IntegrationSpec
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Tests of the whole store against regtest chains of Bitcoin Core and Bitcoin
-- Cash Node, with a Redis cache, all in Podman containers.
--
-- The unit tests check pieces against models; these check that the pieces
-- fit: that the store syncs from a real node, that the xpub endpoints find
-- the payments a wallet following the gap would find and no others, and that
-- the Redis cache answers exactly like the database. Each chain gets one node,
-- one Redis and one store. The store is served twice, by a web server with
-- the cache and by one without ('cache' set to 'Nothing'), so both read the
-- same database, mempool times included, and every difference between their
-- answers is a difference made by the cache.
--
-- Every test makes its own xpubs from a fixed seed, sends transactions to
-- chosen derivation paths, mines them, and checks the answers against a
-- ledger that it keeps itself: balance, transactions, unspent outputs, used
-- paths and indices. Then it compares the two web servers on every xpub and
-- Blockchain.info endpoint, with pages, starting points and filters. The cache
-- writer updates Redis in the background, so the comparison is retried for up
-- to 30 seconds before it fails.
--
-- The tests are skipped when Podman cannot run containers, or when
-- @HASKOIN_STORE_NO_PODMAN@ is set. Containers are removed when the tests
-- end, data is kept in tmpfs mounts and in a temporary directory under
-- @\/dev\/shm@ (or @$XDG_RUNTIME_DIR@), and every container has a memory
-- limit. Set @HASKOIN_TEST_LOG@ to see the log of the store.
module Haskoin.Store.IntegrationSpec (spec) where

import Control.Lens ((&), (?~), (^.))
import Control.Monad (forM_, replicateM_, unless, void, when)
import Data.Aeson (Value (..), eitherDecode, object, toJSON, (.=))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.ByteString.Char8 qualified as C8
import Data.ByteString.Lazy qualified as BL
import Data.Default (def)
import Data.Either (isRight)
import Data.Foldable (toList)
import Data.HashSet qualified as HashSet
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.IntMap.Strict qualified as IntMap
import Data.List (nub, sort, sortOn)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, isJust, mapMaybe)
import Data.Ord (Down (..))
import Data.Serialize (decode, encode)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Word (Word32, Word64)
import Database.Redis qualified as Redis
import Haskoin
  ( Address,
    Ctx,
    Network,
    TxHash (..),
    XPrvKey,
    addrToText,
    addressToScriptBS,
    blockHashToHex,
    deriveXPubKey,
    encodeHex,
    hexToTxHash,
    makeXPrvKey,
    prepareContext,
    prvSubKey,
    pubSubKey,
    sha256,
    xPrvWif,
    xPubAddr,
    xPubCompatWitnessAddr,
    xPubExport,
    xPubWitnessAddr,
  )
import Haskoin.Node (LogLevel (..), Logger (..), withConnection, withLogger)
import Haskoin.Store
  ( DeriveType (..),
    Store (..),
    StoreConfig (..),
    WebConfig (..),
    XPubSpec (..),
    runWeb,
    withStore,
  )
import Haskoin.Store.Cache
  ( Cache,
    CacheCounters (..),
    Reload (..),
    cacheCounters,
    cacheEvict,
    cacheIsCached,
    cachePrefix,
    cacheRefresh,
    cacheTxData,
    cacheXPubBals,
    xpubId,
  )
import Haskoin.Store.Data (BlockRef (..), Spender (..), TxData (..), XPubBal)
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Integration.Image (testImage, valkeyBin)
import Haskoin.Store.Integration.Node
import Haskoin.Store.Integration.Podman
import Network.Socket qualified as Socket
import Network.Wreq qualified as Wreq
import Network.Wreq.Types (FormParam ((:=)))
import System.Environment (lookupEnv)
import Test.Hspec
import UnliftIO (bracket, link, tryAny, withAsync)
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.Directory (doesDirectoryExist, getTemporaryDirectory)
import UnliftIO.Temporary (withTempDirectory)

spec :: Spec
spec = do
  usable <- runIO podmanUsable
  describe "Regtest chains in Podman" $ case usable of
    Left why ->
      it "check the store against Bitcoin Core and Bitcoin Cash Node" $
        pendingWith why
    Right () -> prepareContext $ \ctx -> do
      aroundAll (withEnv BitcoinCore ctx) $
        describe "Bitcoin Core" (chainSpec BitcoinCore)
      aroundAll (withEnv BitcoinCashNode ctx) $
        describe "Bitcoin Cash Node" (chainSpec BitcoinCashNode)

chainSpec :: NodeKind -> SpecWith Env
chainSpec kind = do
  it "sees payments up to exactly the gap, and those the gap reaches later" gapTest
  it "does not see the payment 34 addresses past the last used one" noteTest
  it "follows spends and change, confirmed and in the mempool" spendTest
  it "pages mempool transactions of the same second without duplicates" tiesTest
  it "counts every transaction of a multiaddr wallet" multiaddrTest
  it "answers the address, block and transaction endpoints like the node" endpointsTest
  when (kind == BitcoinCore) $
    it "derives native and nested SegWit addresses" segwitTest
  it "does not use or fill the cache when asked not to" noCacheTest
  it "recovers from damaged entries and keeps newer data" damagedTest
  it "keeps the spenders of cached transactions up to date" spenderTest
  it "evicts the least recently used xpubs" evictTest
  it "drops a transaction double-spent in a reorganisation" reorgTest
  it "applies each change once with two stores sharing Redis" onceTest
  it "does not replace what it does not know" unknownTest
  it "leaves the keys of older caches alone" oldKeysTest

-------------------------------------------------------------------------------
-- Environment
-------------------------------------------------------------------------------

-- | A node, a store syncing from it with a Redis cache, and the two web
-- servers of the store.
data Env = Env
  { kind :: !NodeKind,
    node :: !Node,
    net :: !Network,
    ctx :: !Ctx,
    store :: !Store,
    -- | base URL of the web server with the cache
    cached :: !String,
    -- | base URL of the web server without the cache
    plain :: !String,
    -- | address of the wallet of the node, to mine to
    miner :: !Text,
    -- | the cache of the store
    redisCache :: !Cache,
    -- | connection to Redis, to look at the keys
    redis :: !Redis.Connection,
    -- | configuration of a store like this one with its database in a
    -- directory, to start another one
    configIn :: FilePath -> StoreConfig
  }

-- | Initial gap and gap of the store under test, the defaults.
initGap, gap :: Word32
initGap = 20
gap = 32

withEnv :: NodeKind -> Ctx -> (Env -> IO ()) -> IO ()
withEnv kind ctx go = do
  image <- testImage
  withContainer (redisContainer image) $ \redis -> do
    redisPort <- hostPort redis 6379
    let redisURL = "redis://127.0.0.1:" <> show redisPort
    waitRedis redisURL
    writeOldKeys redisURL
    withContainer (nodeContainer kind image) $ \c -> do
      rpcPort <- hostPort c 18443
      p2pPort <- hostPort c 18444
      let node =
            Node
              { kind = kind,
                url = "http://127.0.0.1:" <> show rpcPort <> "/",
                p2p = p2pPort
              }
      waitRpc node
      ensureWallet node
      miner <- getNewAddress node
      -- enough mature coinbases for the wallet to fund every test without
      -- long chains of unconfirmed change
      void $ generateTo node 150 miner
      withRamDir $ \dir -> withTestLogger $ \logger -> do
        let net = nodeNetwork kind
            cfg = storeConfig net ctx logger dir redisURL p2pPort
        withStore cfg $ \store -> do
            cachedPort <- freePort
            plainPort <- freePort
            let web s p = runWeb (webConfig logger s p)
            conn <- Redis.checkedConnect (either error id (Redis.parseConnectInfo redisURL))
            cache <- maybe (fail "The store has no cache") return store.cache
            withAsync (web store cachedPort) $ \a1 ->
              withAsync (web store {cache = Nothing} plainPort) $ \a2 -> do
                link a1
                link a2
                let env =
                      Env
                        { kind = kind,
                          node = node,
                          net = net,
                          ctx = ctx,
                          store = store,
                          cached = "http://127.0.0.1:" <> show cachedPort,
                          plain = "http://127.0.0.1:" <> show plainPort,
                          miner = miner,
                          redisCache = cache,
                          redis = conn,
                          configIn = \d -> storeConfig net ctx logger d redisURL p2pPort
                        }
                waitSynced env
                go env

-- | Valkey, the Redis-compatible server of Ubuntu, from the test image, listening on every interface of the container
-- (only its published port is reachable, on 127.0.0.1), with nothing saved
-- to disk and at most 128 MiB of data.
redisContainer :: String -> ContainerSpec
redisContainer image =
  ContainerSpec
    { name = "redis",
      image = image,
      options = ["--memory", "256m", "--entrypoint", valkeyBin],
      command =
        [ "--bind",
          "0.0.0.0",
          "--protected-mode",
          "no",
          "--save",
          "",
          "--appendonly",
          "no",
          "--maxmemory",
          "128mb"
        ],
      ports = [6379]
    }

-- | Wait until Redis answers, for up to 30 seconds.
waitRedis :: String -> IO ()
waitRedis url = go (120 :: Int)
  where
    info = either error id (Redis.parseConnectInfo url)
    go n =
      tryAny (bracket (Redis.checkedConnect info) Redis.disconnect (const (return ()))) >>= \case
        Right () -> return ()
        Left e
          | n <= 0 -> fail ("Redis did not start: " <> show e)
          | otherwise -> threadDelay 250000 >> go (n - 1)

-- | A temporary directory in memory: under @/dev/shm@ if there is one, else
-- under @$XDG_RUNTIME_DIR@, else the usual temporary directory.
withRamDir :: (FilePath -> IO a) -> IO a
withRamDir f = do
  shm <- doesDirectoryExist "/dev/shm"
  xdg <- lookupEnv "XDG_RUNTIME_DIR"
  tmp <- getTemporaryDirectory
  let base
        | shm = "/dev/shm"
        | Just x <- xdg = x
        | otherwise = tmp
  withTempDirectory base "haskoin-store-test" f

-- | A logger that prints the log of the store if @HASKOIN_TEST_LOG@ is set,
-- and discards it otherwise.
withTestLogger :: (Logger -> IO a) -> IO a
withTestLogger f =
  lookupEnv "HASKOIN_TEST_LOG" >>= \case
    Just v | v `notElem` ["", "0"] -> withLogger LevelDebug f
    _ -> f Logger {level = LevelError, source = "", push = \_ -> return ()}

-- | Store with the default gaps, caching every used xpub, with small RocksDB
-- buffers and caches.
storeConfig :: Network -> Ctx -> Logger -> FilePath -> String -> Int -> StoreConfig
storeConfig net ctx logger dir redisURL p2pPort =
  StoreConfig
    { maxPeers = 1,
      initPeers = ["127.0.0.1:" <> show p2pPort],
      discover = False,
      db = dir,
      net = net,
      redis = Just redisURL,
      ctx = ctx,
      initGap = initGap,
      gap = gap,
      redisMinAddrs = 1,
      redisMaxXPubs = 10000,
      noMempool = False,
      wipeMempool = False,
      syncMempool = True,
      blockWindow = 64,
      mempoolTimeout = 14,
      peerTimeout = 120,
      maxPeerLife = 48 * 3600,
      connect = withConnection,
      stats = Nothing,
      bloom = True,
      maxOpenFiles = Just 256,
      writeBufferSize = Just (16 * 1024 * 1024),
      maxWalSize = Just (64 * 1024 * 1024),
      blockCache = Just (16 * 1024 * 1024),
      logger = logger
    }

webConfig :: Logger -> Store -> Int -> WebConfig
webConfig logger store port =
  WebConfig
    { host = "127.0.0.1",
      port = port,
      store = store,
      maxLaggingBlocks = 100,
      maxPendingTxs = 10000,
      minPeers = 0,
      limits = def,
      version = "test",
      noMempool = False,
      stats = Nothing,
      tickerRefresh = 90 * 1000 * 1000,
      tickerURL = "http://127.0.0.1:9/",
      priceHistoryURL = "http://127.0.0.1:9/",
      noXPub = False,
      noBlockchainInfo = False,
      healthCheckInterval = 30,
      logger = logger
    }

-- | A free TCP port on the loopback interface.
freePort :: IO Int
freePort =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \s -> do
    Socket.bind s (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    fromIntegral <$> Socket.socketPort s

-------------------------------------------------------------------------------
-- Talking to the store
-------------------------------------------------------------------------------

-- | Status and JSON body of a request. A body that is not JSON, as the
-- plain-text Blockchain.info queries give, is returned as a string.
getJSON :: String -> String -> IO (Int, Value)
getJSON base path = do
  r <- Wreq.getWith opts (base <> path)
  return (r ^. Wreq.responseStatus . Wreq.statusCode, body (r ^. Wreq.responseBody))
  where
    opts = Wreq.defaults & Wreq.checkResponse ?~ (\_ _ -> return ())

-- | Like 'getJSON', posting a form.
postForm :: String -> String -> [(C8.ByteString, Text)] -> IO (Int, Value)
postForm base path params = do
  r <- Wreq.postWith opts (base <> path) [k := v | (k, v) <- params]
  return (r ^. Wreq.responseStatus . Wreq.statusCode, body (r ^. Wreq.responseBody))
  where
    opts = Wreq.defaults & Wreq.checkResponse ?~ (\_ _ -> return ())

body :: BL.ByteString -> Value
body b = either (const (String (cs b))) id (eitherDecode b)

-- | Body of a successful request to the server without the cache.
plainJSON :: Env -> String -> IO Value
plainJSON env path = do
  (code, v) <- getJSON env.plain path
  unless (code == 200) $
    expectationFailure ("GET " <> path <> " answered " <> show code <> ": " <> show v)
  return v

-- | Check that both web servers answer every request in the same way, and
-- successfully. The cache is updated in the background, so try for up to 30
-- seconds.
sameAnswers :: Env -> [String] -> IO ()
sameAnswers env paths = forM_ paths $ \p -> go p (120 :: Int)
  where
    go p n = do
      a <- getJSON env.cached p
      b <- getJSON env.plain p
      if a == b && fst a == 200
        then return ()
        else
          if n <= 0
            then
              expectationFailure $
                "GET "
                  <> p
                  <> "\nstatus with and without the cache: "
                  <> show (fst a, fst b)
                  <> "\nfirst difference: "
                  <> difference "" (snd a) (snd b)
            else threadDelay 250000 >> go p (n - 1)

-- | Where two JSON values first differ, with the two values there.
difference :: String -> Value -> Value -> String
difference at' (Object x) (Object y)
  | KeyMap.keys x /= KeyMap.keys y = at' <> ": keys " <> show (KeyMap.keys x) <> " and " <> show (KeyMap.keys y)
  | otherwise =
      case [ difference (at' <> "." <> show k) v w
           | (k, v) <- KeyMap.toList x,
             Just w <- [KeyMap.lookup k y],
             v /= w
           ] of
        d : _ -> d
        [] -> at' <> ": none"
difference at' (Array x) (Array y)
  | length x /= length y = at' <> ": " <> show (length x) <> " and " <> show (length y) <> " elements"
  | otherwise =
      case [difference (at' <> "[" <> show i <> "]") v w | (i, v, w) <- zip3 [0 :: Int ..] (toList x) (toList y), v /= w] of
        d : _ -> d
        [] -> at' <> ": none"
difference at' x y = at' <> ": " <> take 1000 (show x) <> " and " <> take 1000 (show y)

-- | Wait until the store has the best block of the node and every
-- transaction of its mempool, for up to 60 seconds.
waitSynced :: Env -> IO ()
waitSynced env = go (240 :: Int)
  where
    go n = do
      best <- bestBlockHash env.node
      pool <- rawMempool env.node
      r <- tryAny $ do
        (_, b) <- getJSON env.plain "/block/best?notx=true"
        (_, m) <- getJSON env.plain "/mempool"
        let ours = [t | String t <- arr m]
        return (textField "hash" b == Just best && all (`elem` ours) pool)
      case r of
        Right True -> return ()
        _
          | n <= 0 -> do
              (_, b) <- getJSON env.plain "/block/best?notx=true"
              (_, m) <- getJSON env.plain "/mempool"
              let ours = [t | String t <- arr m]
              expectationFailure $
                "The store did not sync to block "
                  <> cs best
                  <> "; its best block is "
                  <> show (textField "hash" b)
                  <> " and it lacks these transactions of the mempool of the node: "
                  <> show (filter (`notElem` ours) pool)
          | otherwise -> threadDelay 250000 >> go (n - 1)

-- | Mine blocks and wait for the store to have them.
mine :: Env -> Int -> IO [Text]
mine env n = do
  hs <- generateTo env.node n env.miner
  waitSynced env
  return hs

-------------------------------------------------------------------------------
-- Test wallets
-------------------------------------------------------------------------------

-- | An xpub of a test, with the key to spend from it.
data XPub = XPub
  { prv :: !XPrvKey,
    spec :: !XPubSpec,
    -- | serialized xpub
    text :: !String,
    -- | derivation path of the first addresses of each chain, by output script
    scripts :: !(Map.Map Text [Word32])
  }

-- | Xpub from a seed, different for each chain.
newXPub :: Env -> String -> DeriveType -> XPub
newXPub env seed deriv =
  XPub
    { prv = prv,
      spec = xspec,
      text = cs (xPubExport env.net env.ctx pub),
      scripts =
        Map.fromList
          [ (scriptHex env (addrOf env x [m, i]), [m, i])
          | m <- [0, 1],
            i <- [0 .. 120]
          ]
    }
  where
    prv = makeXPrvKey (C8.pack ("haskoin-store integration " <> show env.kind <> " " <> seed))
    pub = deriveXPubKey env.ctx prv
    xspec = XPubSpec {key = pub, deriv = deriv}
    x = XPub {prv = prv, spec = xspec, text = "", scripts = Map.empty}

-- | Address at a derivation path, computed with haskoin-core, not the store.
addrOf :: Env -> XPub -> [Word32] -> Address
addrOf env x [m, i] = f (pubSubKey env.ctx (pubSubKey env.ctx x.spec.key m) i)
  where
    f = case x.spec.deriv of
      DeriveNormal -> xPubAddr env.ctx
      DeriveP2SH -> xPubCompatWitnessAddr env.ctx
      DeriveP2WPKH -> xPubWitnessAddr env.ctx
addrOf _ _ p = error ("Bad path: " <> show p)

addrText :: Env -> Address -> Text
addrText env a = fromMaybe (error "Cannot encode address") (addrToText env.net a)

-- | Address at a path of an xpub, as text.
at :: Env -> XPub -> [Word32] -> Text
at env x = addrText env . addrOf env x

scriptHex :: Env -> Address -> Text
scriptHex env a =
  encodeHex (fromMaybe (error "No script for address") (addressToScriptBS env.ctx a))

-- | Private key of the address at a path, as WIF.
wifOf :: Env -> XPub -> [Word32] -> Text
wifOf env x [m, i] = xPrvWif env.net (prvSubKey env.ctx (prvSubKey env.ctx x.prv m) i)
wifOf _ _ p = error ("Bad path: " <> show p)

-- | Parameter that selects the derivation of an xpub.
deriveParam :: XPub -> String
deriveParam x = case x.spec.deriv of
  DeriveNormal -> "normal"
  DeriveP2SH -> "compat"
  DeriveP2WPKH -> "segwit"

-------------------------------------------------------------------------------
-- Ledger: what the test expects the store to know
-------------------------------------------------------------------------------

-- | Output paid to an address of an xpub.
data Coin = Coin
  { txid :: !Text,
    vout :: !Int,
    path :: ![Word32],
    value :: !Word64
  }
  deriving (Eq, Show)

-- | The outputs paid to an xpub, which of them are spent and by what, and
-- which transactions touch which paths.
data Ledger = Ledger
  { coins :: ![Coin],
    spent :: ![((Text, Int), Text)],
    touched :: ![(Text, [Word32])]
  }

newLedger :: IO (IORef Ledger)
newLedger = newIORef (Ledger [] [] [])

-- | Add a transaction to the ledger of an xpub, reading it from the node.
record :: Env -> XPub -> IORef Ledger -> Text -> IO ()
record env x ref txid = do
  tx <- getRawTx env.node txid
  let ins =
        [ (t, fromIntegral (sats' n))
        | i <- arr ((tx ! "vin")),
          Just t <- [textField "txid" i],
          Just n <- [field "vout" i]
        ]
      outs =
        [ (fromIntegral (sats' n), h, sats v)
        | o <- arr ((tx ! "vout")),
          Just n <- [field "n" o],
          Just v <- [field "value" o],
          Just h <- [textField "hex" =<< field "scriptPubKey" o]
        ]
  modifyIORef' ref $ \l ->
    let mine' = [c | c <- l.coins, (c.txid, c.vout) `elem` ins]
        new =
          [ Coin {txid = txid, vout = n, path = p, value = v}
          | (n, h, v) <- outs,
            Just p <- [Map.lookup h x.scripts]
          ]
     in Ledger
          { coins = l.coins <> new,
            spent = l.spent <> [((c.txid, c.vout), txid) | c <- mine'],
            touched =
              l.touched
                <> nub ([(txid, c.path) | c <- mine'] <> [(txid, c.path) | c <- new])
          }
  where
    -- integer JSON numbers
    sats' (Number n) = round n :: Integer
    sats' v = error ("Not a number: " <> show v)

-- | Remove a transaction from a ledger, as if it never happened.
forget :: IORef Ledger -> Text -> IO ()
forget ref txid = modifyIORef' ref $ \l ->
  Ledger
    { coins = [c | c <- l.coins, c.txid /= txid],
      spent = [s | s@(_, t) <- l.spent, t /= txid],
      touched = [t | t@(h, _) <- l.touched, h /= txid]
    }

-- | Unspent output at a path.
coinAt :: Ledger -> [Word32] -> Coin
coinAt l p =
  case [c | c <- l.coins, c.path == p, (c.txid, c.vout) `notElem` map fst l.spent] of
    c : _ -> c
    [] -> error ("No unspent output at " <> show p)

-- | Last used index that a wallet following the gap reaches from the used
-- indices of a chain, starting from those in the first @start@ addresses, or
-- -1 if it reaches none.
reach :: Word32 -> [Int] -> Int
reach start used = go first
  where
    first = maximum ((-1) : filter (< fromIntegral start) used)
    go l = case filter (\u -> u > l && u <= l + fromIntegral gap) used of
      [] -> l
      us -> go (maximum us)

-- | What the store should answer for an xpub.
data Expect = Expect
  { balance :: !Word64,
    txids :: ![Text],
    utxos :: ![(Text, Int)],
    used :: ![[Word32]],
    external :: !Int,
    change :: !Int
  }
  deriving (Eq, Show)

expect :: Ledger -> Expect
expect l =
  Expect
    { balance = sum [c.value | c <- live],
      txids = sort (nub [t | (t, p) <- l.touched, p `elem` visible]),
      utxos = sort [(c.txid, c.vout) | c <- live],
      used = sort visible,
      external = maximum (0 : [fromIntegral i | [0, i] <- visible]),
      change = maximum (0 : [fromIntegral i | [1, i] <- visible])
    }
  where
    paths = nub (map snd l.touched)
    ext = [fromIntegral i | [0, i] <- paths]
    chg = [fromIntegral i | [1, i] <- paths]
    lastExt = reach initGap ext
    lastChg = reach 0 chg
    visible
      | lastExt < 0 = []
      | otherwise =
          [ p
          | p@[m, i] <- paths,
            fromIntegral i <= if m == 0 then lastExt else lastChg
          ]
    live =
      [ c
      | c <- l.coins,
        c.path `elem` visible,
        (c.txid, c.vout) `notElem` map fst l.spent
      ]

-------------------------------------------------------------------------------
-- Checks
-------------------------------------------------------------------------------

-- | Check what the store says about an xpub against the ledger, then that
-- the cache says the same on every endpoint, and that the xpub is cached.
checkXPub :: Env -> XPub -> IORef Ledger -> IO ()
checkXPub env x ref = do
  e <- expect <$> readIORef ref
  let b = "/xpub/" <> x.text
      q = "?derive=" <> deriveParam x
  summary <- plainJSON env (b <> q)
  let bal = (summary ! "balance")
      idx = (summary ! "indices")
  (num "confirmed" bal + num "unconfirmed" bal) `shouldBe` e.balance
  num "utxo" bal `shouldBe` fromIntegral (length e.utxos)
  num "external" idx `shouldBe` fromIntegral e.external
  num "change" idx `shouldBe` fromIntegral e.change
  txs <- arr <$> plainJSON env (b <> "/transactions" <> q)
  let ts = mapMaybe (textField "txid") txs
  ts `shouldBe` nub ts
  sort ts `shouldBe` e.txids
  newestFirst (map blockOf txs)
  us <- arr <$> plainJSON env (b <> "/unspent" <> q)
  sort
    [ (t, fromIntegral (num "index" u'))
    | u <- us,
      Just u' <- [field "unspent" u],
      Just t <- [textField "txid" u']
    ]
    `shouldBe` e.utxos
  bs <- arr <$> plainJSON env (b <> "/balances" <> q)
  sort
    [ p
    | xb <- bs,
      Just bb <- [field "balance" xb],
      num "txs" bb > 0,
      Just (Array p') <- [field "path" xb],
      let p = map (fromIntegral . numV) (toList p')
    ]
    `shouldBe` e.used
  let heights = nub [show h | BlockRef h _ <- map blockOf txs]
  unless (null e.used) $ do
    _ <- getJSON env.cached (b <> "/balances" <> q)
    waitCached env x
  sameAnswers env (xpubPaths x ts heights)
  unless (null e.used) $ do
    -- every body of the xpub is in Redis, and is the body of the database,
    -- spenders included
    let hs = map (\t -> fromMaybe (error "bad txid") (hexToTxHash t)) e.txids
    waitFor "cached bodies" $ do
      fromCache <- cacheTxData env.redisCache hs
      fromDB <- Q.getManyTxData env.store.db hs
      return (fromCache == fromDB && all isJust fromCache)

-- | Requests about an xpub to compare between the servers. The balances come
-- first, as they are what puts an xpub in the cache.
xpubPaths :: XPub -> [Text] -> [String] -> [String]
xpubPaths x ts heights =
  [ b <> "/balances" <> q,
    b <> q,
    t,
    t <> "&limit=1",
    t <> "&limit=2&offset=1",
    t <> "&limit=3&offset=2",
    b <> "/transactions/full" <> q <> "&limit=4",
    u,
    u <> "&limit=1",
    u <> "&limit=2&offset=1",
    b <> q <> "&nocache=true",
    t <> "&nocache=true",
    u <> "&nocache=true"
  ]
    <> concat
      [ [ t <> "&height=" <> s <> "&limit=2",
          t <> "&height=" <> s <> "&limit=2&offset=1",
          u <> "&height=" <> s <> "&limit=2"
        ]
      | s <- map cs ts <> heights
      ]
    <> binfo
  where
    b = "/xpub/" <> x.text
    q = "?derive=" <> deriveParam x
    t = b <> "/transactions" <> q
    u = b <> "/unspent" <> q
    active = case x.spec.deriv of
      DeriveNormal -> "active="
      DeriveP2SH -> "activeP2SH="
      DeriveP2WPKH -> "activeBech32="
    m = "/blockchain/multiaddr?" <> active <> x.text
    binfo =
      [ m,
        m <> "&n=1",
        m <> "&n=2&offset=1",
        m <> "&filter=2",
        m <> "&filter=5",
        m <> "&filter=7",
        "/blockchain/unspent?" <> active <> x.text,
        "/blockchain/balance?" <> active <> x.text,
        "/blockchain/rawaddr/" <> x.text <> "?derive=" <> deriveParam x,
        "/blockchain/rawaddr/" <> x.text <> "?derive=" <> deriveParam x <> "&limit=2&offset=1",
        "/blockchain/xpub/" <> x.text <> "?derive=" <> deriveParam x
      ]

-- | Block of a transaction or unspent output in a JSON answer.
blockOf :: Value -> BlockRef
blockOf v = case field "block" v of
  Just b
    | Just m <- field "mempool" b -> MemRef (fromIntegral (numV m))
    | Just h <- field "height" b,
      Just p <- field "position" b ->
        BlockRef (fromIntegral (numV h)) (fromIntegral (numV p))
  _ -> error ("No block in " <> show v)

-- | Check that blocks come newest first: the mempool, then by height and
-- position.
newestFirst :: [BlockRef] -> IO ()
newestFirst bs = bs `shouldBe` sortOn Down bs

-- | Field of a JSON object, or null.
(!) :: Value -> Text -> Value
v ! k = fromMaybe Null (field k v)

arr :: Value -> [Value]
arr (Array a) = toList a
arr v = error ("Not an array: " <> take 500 (show v))

numV :: Value -> Integer
numV (Number n) = round n
numV v = error ("Not a number: " <> show v)

num :: Text -> Value -> Word64
num k v = maybe (error ("No " <> cs k <> " in " <> take 500 (show v))) (fromIntegral . numV) (field k v)

-- | Send from the wallet of the node to paths of an xpub, in one
-- transaction, and record it.
pay :: Env -> XPub -> IORef Ledger -> [([Word32], Word64)] -> IO Text
pay env x ref outs = do
  txid <- sendMany env.node [(at env x p, v) | (p, v) <- outs]
  record env x ref txid
  return txid

-- | Spend outputs of an xpub, signing with its keys, and record it.
spend :: Env -> XPub -> IORef Ledger -> [Coin] -> [(Text, Word64)] -> IO Text
spend env x ref cs' outs = do
  let ins = [object ["txid" .= c.txid, "vout" .= c.vout] | c <- cs']
      prevs =
        [ object
            [ "txid" .= c.txid,
              "vout" .= c.vout,
              "scriptPubKey" .= scriptHex env (addrOf env x c.path),
              "amount" .= btc c.value
            ]
        | c <- cs'
        ]
      wifs = nub [wifOf env x c.path | c <- cs']
  txid <- signAndSend env ins outs wifs prevs
  record env x ref txid
  return txid

-- | Create, sign with keys and send a transaction. Returns its hash.
signAndSend :: Env -> [Value] -> [(Text, Word64)] -> [Text] -> [Value] -> IO Text
signAndSend env ins outs wifs prevs = do
  raw <- rpc env.node "createrawtransaction" [toJSON ins, object [Key.fromText a .= btc v | (a, v) <- outs]]
  signed <- rpc env.node "signrawtransactionwithkey" [raw, toJSON wifs, toJSON prevs]
  unless (field "complete" signed == Just (Bool True)) $
    expectationFailure ("Could not sign: " <> show signed)
  hex <- maybe (fail "No hex") return (field "hex" signed)
  rpc env.node "sendrawtransaction" [hex] >>= \case
    String t -> return t
    v -> fail ("Unexpected answer to sendrawtransaction: " <> show v)

-------------------------------------------------------------------------------
-- Tests
-------------------------------------------------------------------------------

-- | Payments at the edge of the gap. Index 37 is 32 past index 5, the last
-- used one before it, so it is seen; index 70 is 33 past index 37, so it is
-- not. Then a payment to index 69, 32 past index 37, makes index 70 be seen
-- too: the cache has to derive and read further on its own to find it.
gapTest :: Env -> IO ()
gapTest env = do
  let x = newXPub env "gap" DeriveNormal
  ref <- newLedger
  void $
    pay env x ref $
      [([0, 0], 100000), ([0, 1], 200000), ([0, 2], 300000), ([0, 5], 400000)]
        <> [([0, 37], 500000), ([0, 70], 600000)]
  void $ mine env 1
  e1 <- expect <$> readIORef ref
  e1.used `shouldBe` [[0, 0], [0, 1], [0, 2], [0, 5], [0, 37]]
  e1.balance `shouldBe` 1500000
  checkXPub env x ref
  void $ pay env x ref [([0, 69], 700000)]
  waitSynced env
  e2 <- expect <$> readIORef ref
  e2.used `shouldBe` [[0, 0], [0, 1], [0, 2], [0, 5], [0, 37], [0, 69], [0, 70]]
  checkXPub env x ref
  void $ mine env 1
  checkXPub env x ref

-- | The wallet of the Blockchain.com comparison: receive indices 0 to 35
-- used, and a payment to index 69, 34 past index 35. With a gap of 32 it is
-- not seen, as Blockchain.com does not see it.
noteTest :: Env -> IO ()
noteTest env = do
  let x = newXPub env "blockchain.com" DeriveNormal
  ref <- newLedger
  void $ pay env x ref $ [([0, i], 10000 + fromIntegral i) | i <- [0 .. 35]] <> [([0, 69], 280348)]
  void $ mine env 1
  e <- expect <$> readIORef ref
  e.external `shouldBe` 35
  e.balance `shouldBe` sum [10000 + i | i <- [0 .. 35]]
  checkXPub env x ref
  ma <- plainJSON env ("/blockchain/multiaddr?active=" <> x.text)
  let w = (ma ! "wallet")
  num "final_balance" w `shouldBe` e.balance
  num "n_tx" w `shouldBe` 1
  case arr ((ma ! "addresses")) of
    [a] -> do
      num "account_index" a `shouldBe` 36
      num "change_index" a `shouldBe` 0
      num "n_tx" a `shouldBe` 1
    as -> expectationFailure ("Expected one xpub, got " <> show as)

-- | Spends from two addresses of an xpub at once, with change, then a spend
-- of the change while it is still in the mempool.
spendTest :: Env -> IO ()
spendTest env = do
  let x = newXPub env "spend" DeriveNormal
  ref <- newLedger
  void $ pay env x ref [([0, 0], 1000000), ([0, 1], 2000000)]
  void $ mine env 1
  checkXPub env x ref
  out1 <- getNewAddress env.node
  l1 <- readIORef ref
  void $
    spend env x ref [coinAt l1 [0, 0], coinAt l1 [0, 1]] $
      [(out1, 1500000), (at env x [1, 0], 1490000)]
  waitSynced env
  checkXPub env x ref
  out2 <- getNewAddress env.node
  l2 <- readIORef ref
  void $ spend env x ref [coinAt l2 [1, 0]] [(out2, 500000), (at env x [1, 1], 980000)]
  waitSynced env
  e <- expect <$> readIORef ref
  e.balance `shouldBe` 980000
  e.change `shouldBe` 1
  checkXPub env x ref
  void $ mine env 1
  checkXPub env x ref

-- | Mempool transactions sent one after the other, most of them in the same
-- second, touching the same addresses several times, one of them paying
-- three addresses of the xpub. The store orders those of the same second by
-- hash; it used to list some twice. Then page through the transactions one
-- at a time, as 'streamThings' does, and check that the pages add up.
tiesTest :: Env -> IO ()
tiesTest env = do
  let x = newXPub env "ties" DeriveNormal
  ref <- newLedger
  void $ pay env x ref [([0, 0], 100000)]
  void $ mine env 1
  void $ pay env x ref [([0, 1], 110000), ([0, 2], 120000), ([0, 3], 130000)]
  void $ pay env x ref [([0, 2], 140000), ([0, 4], 150000)]
  void $ pay env x ref [([0, 1], 160000), ([0, 4], 170000)]
  void $ pay env x ref [([0, 3], 180000)]
  void $ pay env x ref [([0, 0], 190000), ([0, 1], 200000)]
  waitSynced env
  checkXPub env x ref
  forM_ [env.plain, env.cached] (pagesAddUp x)
  void $ mine env 1
  checkXPub env x ref
  forM_ [env.plain, env.cached] (pagesAddUp x)
  where
    pagesAddUp x base = do
      let t = "/xpub/" <> x.text <> "/transactions?derive=normal"
      (_, whole) <- getJSON base t
      let go acc start = do
            let p = case start of
                  Nothing -> t <> "&limit=1"
                  Just s -> t <> "&limit=1&offset=1&height=" <> cs s
            (_, page) <- getJSON base p
            case mapMaybe (textField "txid") (arr page) of
              [] -> return (reverse acc)
              [h] -> go (h : acc) (Just h)
              hs -> fail ("Page of more than one: " <> show hs)
      paged <- go [] Nothing
      paged `shouldBe` mapMaybe (textField "txid") (arr whole)

-- | A multiaddr wallet of two xpubs and an address, with a transaction that
-- touches all three. The wallet counts every transaction once, whatever the
-- page size, and @onlyShow@ narrows the count.
multiaddrTest :: Env -> IO ()
multiaddrTest env = do
  let xa = newXPub env "multi a" DeriveNormal
      xb = newXPub env "multi b" DeriveNormal
      xq = newXPub env "multi address" DeriveNormal
      q = at env xq [0, 0]
  ra <- newLedger
  rb <- newLedger
  rq <- newLedger
  t1 <- sendMany env.node [(at env xa [0, 0], 100000), (at env xb [0, 0], 200000), (q, 300000)]
  forM_ [(xa, ra), (xb, rb), (xq, rq)] $ \(x, r) -> record env x r t1
  void $ pay env xa ra [([0, 1], 110000)]
  void $ pay env xb rb [([0, 1], 120000)]
  void $ mine env 1
  void $ pay env xq rq [([0, 0], 130000)]
  waitSynced env
  forM_ [(xa, ra), (xb, rb)] $ uncurry (checkXPub env)
  let active = xa.text <> "|" <> xb.text <> "|" <> cs q
      m = "/blockchain/multiaddr?active=" <> active
  w1 <- plainJSON env (m <> "&n=1")
  length (arr ((w1 ! "txs"))) `shouldBe` 1
  let wallet1 = (w1 ! "wallet")
  num "n_tx" wallet1 `shouldBe` 4
  num "n_tx_filtered" wallet1 `shouldBe` 4
  num "final_balance" wallet1 `shouldBe` (100000 + 200000 + 300000 + 110000 + 120000 + 130000)
  w2 <- plainJSON env (m <> "&onlyShow=" <> cs q)
  num "n_tx" ((w2 ! "wallet")) `shouldBe` 2
  length (arr ((w2 ! "txs"))) `shouldBe` 2
  w3 <- plainJSON env (m <> "&n=2&offset=1")
  length (arr ((w3 ! "txs"))) `shouldBe` 2
  forM_ (arr ((w1 ! "addresses"))) $ \a ->
    case textField "address" a of
      Just t
        | t == cs xa.text -> num "account_index" a `shouldBe` 2
        | t == cs xb.text -> num "account_index" a `shouldBe` 2
        | t == q -> num "final_balance" a `shouldBe` 430000
      _ -> return ()
  sameAnswers env $
    [ m,
      m <> "&n=1",
      m <> "&n=2&offset=1",
      m <> "&onlyShow=" <> cs q,
      m <> "&onlyShow=" <> xa.text,
      m <> "&filter=1",
      m <> "&filter=2",
      m <> "&filter=5",
      m <> "&filter=7",
      "/blockchain/unspent?active=" <> active,
      "/blockchain/balance?active=" <> active
    ]
  -- the request as the Blockchain.com wallet sends it: a form
  posted <-
    postForm
      env.cached
      "/blockchain/multiaddr"
      [("active", cs active), ("n", "10"), ("offset", "0"), ("format", "json")]
  got <- getJSON env.plain (m <> "&n=10&offset=0")
  posted `shouldBe` got

-- | Endpoints that do not involve xpubs, checked against the node.
endpointsTest :: Env -> IO ()
endpointsTest env = do
  let x = newXPub env "endpoints" DeriveNormal
      a = at env x [0, 0]
  ref <- newLedger
  t1 <- pay env x ref [([0, 0], 400000)]
  void $ mine env 1
  t2 <- pay env x ref [([0, 0], 50000)]
  waitSynced env
  best <- bestBlockHash env.node
  height <- rpc env.node "getblockcount" []
  b <- plainJSON env "/block/best?notx=true"
  textField "hash" b `shouldBe` Just best
  field "height" b `shouldBe` Just height
  hs <- arr <$> plainJSON env ("/block/height/" <> show (numV height) <> "?notx=true")
  map (textField "hash") hs `shouldBe` [Just best]
  tx <- plainJSON env ("/transaction/" <> cs t1)
  textField "txid" tx `shouldBe` Just t1
  rawNode <- rpc env.node "getrawtransaction" [String t1, Bool False]
  raw <- plainJSON env ("/transaction/" <> cs t1 <> "/raw")
  field "result" raw `shouldBe` Just rawNode
  bal <- plainJSON env ("/address/" <> cs a <> "/balance")
  num "confirmed" bal `shouldBe` 400000
  num "unconfirmed" bal `shouldBe` 50000
  num "txs" bal `shouldBe` 2
  ats <- arr <$> plainJSON env ("/address/" <> cs a <> "/transactions")
  mapMaybe (textField "txid") ats `shouldBe` [t2, t1]
  aus <- arr <$> plainJSON env ("/address/" <> cs a <> "/unspent")
  length aus `shouldBe` 2
  pool <- arr <$> plainJSON env "/mempool"
  (String t2 `elem` pool) `shouldBe` True
  lb <- plainJSON env "/blockchain/latestblock"
  textField "hash" lb `shouldBe` Just best
  (_, count) <- getJSON env.plain "/blockchain/q/getblockcount"
  count `shouldBe` height
  rt <- plainJSON env ("/blockchain/rawtx/" <> cs t1)
  textField "hash" rt `shouldBe` Just t1
  rb <- plainJSON env ("/blockchain/rawblock/" <> cs best)
  textField "hash" rb `shouldBe` Just best
  ab <- plainJSON env ("/blockchain/q/addressbalance/" <> cs a)
  ab `shouldBe` Number 450000
  ra <- plainJSON env ("/blockchain/rawaddr/" <> cs a)
  num "n_tx" ra `shouldBe` 2
  num "final_balance" ra `shouldBe` 450000
  peers <- arr <$> plainJSON env "/peers"
  length peers `shouldBe` 1
  sameAnswers env $
    [ "/block/best",
      "/block/height/" <> show (numV height),
      "/transaction/" <> cs t1,
      "/transaction/" <> cs t2,
      "/address/" <> cs a <> "/balance",
      "/address/" <> cs a <> "/transactions",
      "/address/" <> cs a <> "/transactions/full",
      "/address/" <> cs a <> "/unspent",
      "/mempool",
      "/blockchain/rawaddr/" <> cs a,
      "/blockchain/multiaddr?active=" <> cs a,
      "/blockchain/unspent?active=" <> cs a,
      "/blockchain/latestblock",
      "/blockchain/unconfirmed-transactions"
    ]

-- | Xpubs of native and nested SegWit addresses, with a spend from a native
-- SegWit address. Only Bitcoin has SegWit.
segwitTest :: Env -> IO ()
segwitTest env = do
  let xs = newXPub env "segwit" DeriveP2WPKH
      xc = newXPub env "compat" DeriveP2SH
  rs <- newLedger
  rc <- newLedger
  void $ pay env xs rs [([0, 0], 300000), ([0, 1], 400000)]
  void $ pay env xc rc [([0, 0], 500000), ([0, 3], 600000)]
  void $ mine env 1
  checkXPub env xs rs
  checkXPub env xc rc
  out <- getNewAddress env.node
  l <- readIORef rs
  void $ spend env xs rs [coinAt l [0, 0]] [(out, 100000), (at env xs [1, 0], 190000)]
  waitSynced env
  checkXPub env xs rs
  void $ mine env 1
  checkXPub env xs rs
  sameAnswers env ["/blockchain/multiaddr?activeBech32=" <> xs.text <> "&activeP2SH=" <> xc.text]

-- | A transaction confirmed in a block that a reorganisation removes, and
-- double-spent in the new chain, must disappear from the store and from the
-- cache.
--
-- Bitcoin Cash Node cannot mine a chosen transaction (@generateblock@), so
-- the double spend is set up with coinbase maturity: transaction A spends a
-- coinbase in the first block where it may, block X. Invalidating the parent
-- of X makes the coinbase immature again, so the node drops A from its
-- mempool. One block later the coinbase is mature, B spends it to another
-- address, and two more blocks make the new chain longer than the old one.
reorgTest :: Env -> IO ()
reorgTest env = do
  let x = newXPub env "reorg" DeriveNormal
      k = newXPub env "coinbase" DeriveNormal
  ref <- newLedger
  void $ mine env 1
  [cbBlock] <- generateTo env.node 1 (at env k [0, 0])
  block <- rpc env.node "getblock" [String cbBlock]
  cbTxid <- case field "tx" block of
    Just (Array ts) | String t : _ <- toList ts -> return t
    _ -> fail "No coinbase"
  cbTx <- getRawTx env.node cbTxid
  cbValue <- case field "vout" cbTx of
    Just (Array os) | o : _ <- toList os, Just v <- field "value" o -> return (sats v)
    _ -> fail "No coinbase output"
  let coinbaseSpend to amount =
        signAndSend
          env
          [object ["txid" .= cbTxid, "vout" .= (0 :: Int)]]
          [(to, amount)]
          [wifOf env k [0, 0]]
          [ object
              [ "txid" .= cbTxid,
                "vout" .= (0 :: Int),
                "scriptPubKey" .= scriptHex env (addrOf env k [0, 0]),
                "amount" .= btc cbValue
              ]
          ]
  -- 99 blocks on top: the coinbase may be spent in the next block. They are
  -- mined in batches, waiting for the store after each: haskoin-node can lose
  -- track of the headers announced when a node mines dozens of blocks in a
  -- second, and this test is about the reorganisation, not that.
  replicateM_ 9 (mine env 11)
  txA <- coinbaseSpend (at env x [0, 0]) (cbValue - 100000)
  record env x ref txA
  [blockX] <- mine env 1
  checkXPub env x ref
  header <- rpc env.node "getblockheader" [String blockX]
  parent <- maybe (fail "No parent") return (textField "previousblockhash" header)
  void $ rpc env.node "invalidateblock" [String parent]
  pool <- rawMempool env.node
  (txA `elem` pool) `shouldBe` False
  -- mined to the address of the invalidated parent, in the same second and
  -- without transactions, the block would be the parent again, which the
  -- node refuses as known to be invalid
  other <- getNewAddress env.node
  void $ generateTo env.node 1 other
  txB <- coinbaseSpend (at env x [0, 1]) (cbValue - 200000)
  void $ mine env 2
  forget ref txA
  record env x ref txB
  e <- expect <$> readIORef ref
  e.txids `shouldBe` [txB]
  checkXPub env x ref

-------------------------------------------------------------------------------
-- Cache
-------------------------------------------------------------------------------

-- | Try a check for up to 30 seconds.
waitFor :: String -> IO Bool -> IO ()
waitFor = waitForSeconds 30

-- | Like 'waitFor', with a limit in seconds.
waitForSeconds :: Int -> String -> IO Bool -> IO ()
waitForSeconds secs what check = go (secs * 4)
  where
    go n =
      check >>= \case
        True -> return ()
        False
          | n <= 0 -> expectationFailure ("Timed out waiting for " <> what)
          | otherwise -> threadDelay 250000 >> go (n - 1)

-- | Wait until the cache has an xpub.
waitCached :: Env -> XPub -> IO ()
waitCached env x = waitFor ("xpub " <> x.text <> " to be cached") (isCached env x)

-- | Whether an xpub is cached.
isCached :: Env -> XPub -> IO Bool
isCached env x = cacheIsCached env.redisCache x.spec

-- | Key of the entry of an xpub in Redis.
xpubKey :: Env -> XPub -> C8.ByteString -> C8.ByteString
xpubKey env x suffix = cachePrefix env.net <> "x:" <> xpubId x.spec <> ":" <> suffix

-- | Run Redis commands of a test.
redisRun :: Env -> Redis.Redis (Either Redis.Reply a) -> IO a
redisRun env f = Redis.runRedis env.redis f >>= either (fail . show) return

-- | Keys of the cache of older versions of the store, which the new cache
-- must not touch.
oldKeys :: [(C8.ByteString, C8.ByteString)]
oldKeys =
  [ ("head", "old head"),
    ("lock", "old lock"),
    ("mempool", "old mempool"),
    ("b\x01xpub", "old balances"),
    ("t\x01xpub", "old transactions"),
    ("a\x02address", "old address")
  ]

writeOldKeys :: String -> IO ()
writeOldKeys url = do
  conn <- Redis.checkedConnect (either error id (Redis.parseConnectInfo url))
  void . Redis.runRedis conn $ mapM_ (uncurry Redis.set) oldKeys
  Redis.disconnect conn

-- | Queries with @nocache=true@ must neither read the cache nor put the xpub
-- in it; a query without it then caches the xpub.
noCacheTest :: Env -> IO ()
noCacheTest env = do
  let x = newXPub env "nocache" DeriveNormal
      b = "/xpub/" <> x.text
  ref <- newLedger
  void $ pay env x ref [([0, 0], 100000), ([0, 1], 100000)]
  void $ mine env 1
  forM_ ["/transactions", "/transactions/full", "/unspent", "/balances", ""] $ \p -> do
    (code, _) <- getJSON env.cached (b <> p <> "?nocache=true")
    code `shouldBe` 200
  threadDelay 1000000
  isCached env x `shouldReturn` False
  checkXPub env x ref

-- | A cached entry that cannot be decoded is cached again instead of failing
-- the request, and a reload does not replace an entry stamped with a higher
-- block, as one written by a store that is ahead would be.
damagedTest :: Env -> IO ()
damagedTest env = do
  let x = newXPub env "damaged" DeriveNormal
  ref <- newLedger
  void $ pay env x ref [([0, 0], 100000), ([0, 1], 200000)]
  void $ mine env 1
  checkXPub env x ref
  void . redisRun env $ Redis.set (xpubKey env x "bal") "garbage"
  checkXPub env x ref
  bal <- redisRun env $ Redis.get (xpubKey env x "bal")
  (decode <$> bal :: Maybe (Either String [XPubBal])) `shouldSatisfy` maybe False isRight
  void . redisRun env $ Redis.hset (xpubKey env x "meta") (pure ("height", "999999999"))
  cacheRefresh env.redisCache x.spec HashSet.empty `shouldReturn` Behind
  void . redisRun env $ Redis.hset (xpubKey env x "meta") (pure ("height", "0"))
  cacheRefresh env.redisCache x.spec HashSet.empty `shouldReturn` Reloaded
  checkXPub env x ref

-- | An output of a cached transaction that pays an address outside the xpub
-- is spent by a transaction that does not touch the xpub. The xpub is not
-- reloaded, but the cached body must show the spender.
spenderTest :: Env -> IO ()
spenderTest env = do
  let x = newXPub env "spender" DeriveNormal
  ref <- newLedger
  w <- getNewAddress env.node
  raw <- rpc env.node "createrawtransaction" [toJSON ([] :: [Value]), object [Key.fromText (at env x [0, 0]) .= btc 200000, Key.fromText w .= btc 300000]]
  funded <- rpc env.node "fundrawtransaction" [raw]
  signed <- rpc env.node "signrawtransactionwithwallet" [(funded ! "hex")]
  txT <-
    rpc env.node "sendrawtransaction" [(signed ! "hex")] >>= \case
      String t -> return t
      v -> fail (show v)
  record env x ref txT
  void $ mine env 1
  checkXPub env x ref
  info <- rpc env.node "getaddressinfo" [String w]
  tx <- getRawTx env.node txT
  let wScript = textField "scriptPubKey" info
  vout <- case [ numV n
               | o <- arr ((tx ! "vout")),
                 Just n <- [field "n" o],
                 (textField "hex" =<< field "scriptPubKey" o) == wScript
               ] of
    v : _ -> return v
    [] -> fail "No output to the wallet address"
  w2 <- getNewAddress env.node
  raw2 <- rpc env.node "createrawtransaction" [toJSON [object ["txid" .= txT, "vout" .= vout]], object [Key.fromText w2 .= btc 290000]]
  signed2 <- rpc env.node "signrawtransactionwithwallet" [raw2]
  txU <-
    rpc env.node "sendrawtransaction" [(signed2 ! "hex")] >>= \case
      String t -> return t
      v -> fail (show v)
  waitSynced env
  let th = fromMaybe (error "bad txid") (hexToTxHash txT)
      spenderOf = \case
        [Just td] -> (.txid) <$> IntMap.lookup (fromIntegral vout) td.spenders
        _ -> Nothing
  waitFor "the spender of a cached body" $
    (== hexToTxHash txU) . spenderOf <$> cacheTxData env.redisCache [th]
  sameAnswers env ["/xpub/" <> x.text <> "/transactions/full", "/transaction/" <> cs txT]
  void $ mine env 1
  sameAnswers env ["/xpub/" <> x.text <> "/transactions/full", "/transaction/" <> cs txT]

-- | Evicting down to one xpub keeps the one used last, and removes everything
-- of the others: their entries, their addresses in the index, and the
-- bodies that no other cached xpub refers to.
evictTest :: Env -> IO ()
evictTest env = do
  let xa = newXPub env "evict a" DeriveNormal
      xb = newXPub env "evict b" DeriveNormal
  ra <- newLedger
  rb <- newLedger
  void $ pay env xa ra [([0, 0], 100000)]
  void $ pay env xb rb [([0, 0], 100000), ([0, 1], 100000)]
  void $ mine env 1
  checkXPub env xb rb
  checkXPub env xa ra
  _ <- getJSON env.cached ("/xpub/" <> xa.text <> "/balances")
  n <- cacheEvict env.redisCache 1
  n `shouldSatisfy` (>= 1)
  isCached env xa `shouldReturn` True
  isCached env xb `shouldReturn` False
  left <- redisRun env $ Redis.keys (xpubKey env xb "*")
  left `shouldBe` []
  addrs <- redisRun env $ Redis.smembers (cachePrefix env.net <> "a:" <> encode (addrOf env xb [0, 0]))
  addrs `shouldBe` []
  eb <- expect <$> readIORef rb
  forM_ eb.txids $ \t -> do
    let h = fromMaybe (error "bad txid") (hexToTxHash t)
    stored <- redisRun env $ Redis.exists (cachePrefix env.net <> "tx:" <> encode h)
    stored `shouldBe` False
  -- the evicted xpub is answered from the database and cached again
  checkXPub env xb rb

-- | The keys of older caches, written before the store started, are as they
-- were.
oldKeysTest :: Env -> IO ()
oldKeysTest env =
  forM_ oldKeys $ \(k, v) ->
    redisRun env (Redis.get k) `shouldReturn` Just v

-- | Run a second store, with its own database, syncing from the same node
-- and sharing the same Redis, while an action runs.
withOther :: Env -> (Store -> IO a) -> IO a
withOther env f = withRamDir $ \dir -> withStore (env.configIn dir) f

-- | Wait until a store has the best block of the node. A store that starts
-- late has the whole chain to sync, about 300 blocks by the end of the suite,
-- which takes the best part of a minute.
waitStore :: Env -> Store -> IO ()
waitStore env st =
  waitForSeconds 180 "the second store to sync" $ do
    best <- bestBlockHash env.node
    ours <- Q.getBestBlock st.db
    return (fmap blockHashToHex ours == Just best)

-- | Two stores share a Redis and see the same events. A payment to a cached
-- xpub, then the block that confirms it, are each applied once: one store
-- claims each change and reloads the xpub, the other finds it done.
--
-- The stores record their own times for mempool transactions, so which of
-- them wrote the entry shows in the times of its transactions; only the
-- balances are compared with the database here.
onceTest :: Env -> IO ()
onceTest env = withOther env $ \other -> do
  cache2 <- maybe (fail "The second store has no cache") return other.cache
  let x = newXPub env "once" DeriveNormal
      both = do
        a <- cacheCounters env.redisCache
        b <- cacheCounters cache2
        return (a.reloads + b.reloads, a.behind + b.behind)
      synced = waitSynced env >> waitStore env other
      balancesMatch =
        waitFor "the cached balances" $ do
          cached <- cacheXPubBals env.redisCache x.spec
          stored <- Q.xPubBals env.store.db x.spec
          return (cached == Just stored)
  ref <- newLedger
  void $ pay env x ref [([0, 0], 100000)]
  void $ mine env 1
  synced
  _ <- getJSON env.cached ("/xpub/" <> x.text <> "/balances")
  waitCached env x
  threadDelay 1000000
  (r0, b0) <- both
  void $ pay env x ref [([0, 1], 200000)]
  synced
  balancesMatch
  void $ mine env 1
  synced
  balancesMatch
  threadDelay 2000000
  (r1, b1) <- both
  (r1 - r0, b1 - b0) `shouldBe` (2, 0)

-- | Redis has a transaction of a cached xpub that this store has never seen:
-- a store that is behind must not replace the entry, which would drop it.
unknownTest :: Env -> IO ()
unknownTest env = do
  let x = newXPub env "unknown" DeriveNormal
      unknown = encode (TxHash (sha256 ("never seen" :: C8.ByteString)))
  ref <- newLedger
  void $ pay env x ref [([0, 0], 100000)]
  void $ mine env 1
  checkXPub env x ref
  void . redisRun env $ Redis.zadd (xpubKey env x "txs") [(1, unknown)]
  cacheRefresh env.redisCache x.spec HashSet.empty `shouldReturn` Behind
  void . redisRun env $ Redis.zrem (xpubKey env x "txs") (pure unknown)
  cacheRefresh env.redisCache x.spec HashSet.empty `shouldReturn` Reloaded
  checkXPub env x ref
