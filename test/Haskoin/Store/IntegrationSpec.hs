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

import Control.Lens ((&), (.~), (?~), (^.))
import Control.Monad (forM_, forever, unless, void, when)
import Data.Aeson (Value (..), eitherDecode, object, toJSON, (.=))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.ByteString.Char8 qualified as C8
import Data.ByteString.Lazy qualified as BL
import Data.Bytes.Put (runPutS)
import Data.Bytes.Serial (serialize)
import Data.Default (def)
import Data.Either (isRight)
import Data.Foldable (toList)
import Data.HashSet qualified as HashSet
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.IntMap.Strict qualified as IntMap
import Data.List (nub, sort, sortOn)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, isJust, mapMaybe)
import Data.Ord (Down (..))
import Data.Serialize (decode, encode)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word32, Word64)
import Database.Redis qualified as Redis
import Haskoin
  ( Address (..),
    BloomFlags (..),
    Ctx,
    Network (..),
    OutPoint (..),
    TxHash (..),
    XPrvKey,
    addrToText,
    addressToScriptBS,
    blockHashToHex,
    bloomCreate,
    bloomInsert,
    deriveXPubKey,
    encodeHex,
    headerHash,
    hexToBlockHash,
    hexToTxHash,
    makeXPrvKey,
    marshalJSON,
    prepareContext,
    prvSubKey,
    pubSubKey,
    sha256,
    txHash,
    txHashToHex,
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
    runWebSocket,
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
import Haskoin.Store.Common (StoreEvent (..))
import Haskoin.Store.Data
  ( Balance (..),
    BlockData (..),
    BlockRef (..),
    Event (..),
    EventCommand (..),
    Except (..),
    HealthCheck (..),
    BlockHealth (..),
    RawResult (..),
    SerialList (..),
    Spender (..),
    Transaction (..),
    TxData (..),
    XPubBal (..),
  )
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered qualified as L
import Haskoin.Store.Fleet.Member (memberTip)
import Haskoin.Store.Integration.Image (testImage)
import Haskoin.Store.Integration.Node
import Haskoin.Store.Integration.Podman
import Haskoin.Store.Integration.Valkey
import Haskoin.Store.WebClient qualified as Client
import Haskoin.Store.WebCommon (ApiResource, NoCache (..), NoTx (..))
import NQE (receive, withSubscription)
import Network.Socket qualified as Socket
import Network.WebSockets qualified as WS
import Network.Wreq qualified as Wreq
import Network.Wreq.Types (FormParam ((:=)))
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))
import System.Log.FastLogger (toLogStr)
import System.Process (readProcessWithExitCode)
import Test.Hspec
import UnliftIO (async, bracket, cancel, link, throwIO, try, tryAny, withAsync, withSystemTempDirectory)
import UnliftIO.Concurrent (threadDelay)

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
  when (kind == BitcoinCore) $ do
    it "derives native and nested SegWit addresses" segwitTest
    it "refuses the wrong kind of address for an xpub" deriveTest
  it "refuses unknown parameters and values it cannot read" strictTest
  it "does not use or fill the cache when asked not to" noCacheTest
  it "recovers from damaged entries and keeps newer data" damagedTest
  it "updates cached xpubs from what changed, and reads them whole when unsure" incrementalTest
  it "catches up with changes that the database has before their events" catchUpTest
  it "keeps the spenders of cached transactions up to date" spenderTest
  it "evicts the least recently used xpubs" evictTest
  it "answers an error in JSON, whatever was asked for" errorFormatTest
  it "answers the Haskell client of haskoin-store-data" clientTest
  it "speaks HTTP as browsers, proxies and long requests need it" httpTest
  it "serves HTTPS, and nothing else, when given a certificate" tlsTest
  it "tells each client of the transactions it chose, and takes up where it left" eventsTest
  it "drops a transaction double-spent in a reorganisation" reorgTest
  it "applies each change once with two stores sharing Redis" onceTest
  it "does not replace what it does not know" unknownTest
  it "leaves the keys of older caches alone" oldKeysTest
  it "answers from its database, and says so, while Redis does not answer" redisDownTest
  it "gives the same answers from every store of a fleet, from one block and one mempool" fleetTest

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
    configIn :: FilePath -> StoreConfig,
    -- | the same for a store that is a member of a fleet
    memberIn :: FilePath -> StoreConfig
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
            cfg = storeConfig net ctx logger dir redisURL p2pPort False
        withStore cfg $ \store -> do
          listening $ \(cachedSock, cachedPort) -> listening $ \(plainSock, plainPort) -> do
            let web s = runWebSocket (webConfig logger s)
            conn <- Redis.checkedConnect (either error id (Redis.parseConnectInfo redisURL))
            cache <- maybe (fail "The store has no cache") return store.cache
            withAsync (web store cachedSock) $ \a1 ->
              withAsync (web store {cache = Nothing} plainSock) $ \a2 -> do
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
                          configIn = \d -> storeConfig net ctx logger d redisURL p2pPort False,
                          memberIn = \d -> storeConfig net ctx logger d redisURL p2pPort True
                        }
                waitSynced env
                go env

-- | A logger that prints the log of the store if @HASKOIN_TEST_LOG@ is set,
-- and discards it otherwise. Each line starts with the time it was logged
-- at, as seconds of the hour to the millisecond: what several stores do at
-- once can only be put in order, and a wait told from a hang, by the time.
withTestLogger :: (Logger -> IO a) -> IO a
withTestLogger f =
  lookupEnv "HASKOIN_TEST_LOG" >>= \case
    Just v | v `notElem` ["", "0"] -> withLogger LevelDebug (f . timed)
    _ -> f Logger {level = LevelError, source = "", push = \_ -> return ()}
  where
    timed l = l {push = \line -> stamp >>= \at -> l.push (at <> line)} :: Logger
    stamp = do
      now <- getPOSIXTime
      let (seconds, millis) = (floor (now * 1000) `mod` 3600000 :: Int) `divMod` 1000
          padded = replicate (3 - length (show millis)) '0' <> show millis
      return (toLogStr (show seconds <> "." <> padded <> " "))

-- | Store with the default gaps, caching every used xpub, with small RocksDB
-- buffers and caches, as a member of a fleet or not.
storeConfig :: Network -> Ctx -> Logger -> FilePath -> String -> Int -> Bool -> StoreConfig
storeConfig net ctx logger dir redisURL p2pPort fleet =
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
      redisMinTxs = 1000,
      redisMaxXPubs = 10000,
      noMempool = False,
      wipeMempool = False,
      fleet = fleet,
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

webConfig :: Logger -> Store -> WebConfig
webConfig logger store =
  WebConfig
    { host = "127.0.0.1",
      port = 0,
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
      healthCheckInterval = 5,
      accessLog = False,
      tls = Nothing,
      logger = logger
    }

-- | A socket that listens on a port of the loopback interface that the
-- system chose, with that port. Nothing else can take the port between
-- choosing it and serving on it.
listening :: ((Socket.Socket, Int) -> IO a) -> IO a
listening f =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \s -> do
    Socket.bind s (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    Socket.listen s 128
    port <- Socket.socketPort s
    f (s, fromIntegral port)

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
      case [ difference (at' <> "[" <> show i <> "]") v w
           | (i, v, w) <- zip3 [0 :: Int ..] (toList x) (toList y),
             v /= w
           ] of
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
  DeriveNormal -> "legacy"
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
      fromDB <- L.withView env.store.db (`L.getManyTxData` hs)
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
      let t = "/xpub/" <> x.text <> "/transactions?derive=legacy"
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

-- | A parameter that an endpoint does not accept, or a value that it cannot
-- read, must give 400 with a message that names it, rather than an answer
-- that ignores it; and a value it can read must still be accepted.
strictTest :: Env -> IO ()
strictTest env = do
  let x = newXPub env "strict" DeriveNormal
      a = at env x [0, 0]
  (_, best) <- getJSON env.plain "/block/best"
  coinbase <- case best ! "tx" of
    Array ts | String t : _ <- toList ts -> return t
    _ -> fail "No coinbase"
  let refused =
        [ ("/block/best?limt=5", "Unknown parameter: limt"),
          ("/block/best?notx=yes", "Invalid value for parameter notx: yes"),
          ("/block/best?pretty=maybe", "Invalid value for parameter pretty"),
          ("/mempool?limit=-1", "Invalid value for parameter limit"),
          ("/address/" <> cs a <> "/transactions?offset=x", "Invalid value for parameter offset"),
          ("/xpub/" <> x.text <> "?derive=Legacy", "Invalid value for parameter derive"),
          ("/blockchain/latestblock?txidindex=1", "Invalid value for parameter txidindex"),
          ("/blockchain/latestblock?format=hex", "Invalid value for parameter format"),
          ("/blockchain/latestblock?pretty=true", "Unknown parameter: pretty"),
          ("/blockchain/rawtx/" <> cs coinbase <> "?format=xml", "Invalid value for parameter format"),
          ("/blockchain/multiaddr?active=" <> x.text <> "&filter=4", "Invalid value for parameter filter"),
          ("/blockchain/unspent?active=" <> cs a <> "&confirmations=-2", "Invalid value for parameter confirmations"),
          ("/blockchain/rawaddr/" <> cs a <> "?limit=ten", "Invalid value for parameter limit")
        ]
  forM_ refused $ \(p, msg) -> do
    (code, v) <- getJSON env.plain p
    (p, code, textField "error" v) `shouldBe` (p, 400, Just "user-error")
    fmap (T.isInfixOf msg) (textField "message" v) `shouldBe` Just True
  -- refused before anything is done: the body is not even read
  (code, v) <- postForm env.plain "/transactions" [("hex", "00")]
  (code, textField "message" v) `shouldBe` (400, Just "Unknown parameter: hex")
  forM_
    [ "/block/best?notx=true&pretty=true",
      "/blockchain/latestblock?format=json&txidindex=true",
      "/xpub/" <> x.text <> "?derive=normal&nocache=false"
    ]
    $ \p -> do
      (code', _) <- getJSON env.plain p
      (p, code') `shouldBe` (p, 200)

-- | An xpub asked about with the wrong @derive@ must give an error that names
-- the right one, from the database and from the cache, and from every xpub
-- endpoint; an xpub unused with every kind must still give an empty wallet.
deriveTest :: Env -> IO ()
deriveTest env = do
  let x = newXPub env "derive" DeriveP2WPKH
      fresh = newXPub env "derive-fresh" DeriveP2WPKH
  ref <- newLedger
  void $ pay env x ref [([0, 0], 300000)]
  void $ mine env 1
  checkXPub env x ref
  let b = "/xpub/" <> x.text
      ends = ["", "/transactions", "/transactions/full", "/balances", "/unspent"]
      wrong = [b <> e <> "?derive=" <> d | e <- ends, d <- ["legacy", "normal", "compat"]]
  forM_ (wrong <> ["/blockchain/rawaddr/" <> x.text <> "?derive=normal"]) $ \p ->
    forM_ [env.plain, env.cached] $ \base -> do
      (code, v) <- getJSON base p
      (p, code) `shouldBe` (p, 400)
      textField "error" v `shouldBe` Just "user-error"
      fmap (T.isInfixOf "derive=segwit") (textField "message" v) `shouldBe` Just True
  -- legacy is the default, and normal its old name
  forM_ ["", "?derive=legacy", "?derive=normal"] $ \q -> do
    (code, v) <- getJSON env.plain ("/xpub/" <> fresh.text <> q)
    code `shouldBe` 200
    num "confirmed" (v ! "balance") `shouldBe` 0

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
  -- 99 blocks on top: the coinbase may be spent in the next block
  void $ mine env 99
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
waitFor what check = go (120 :: Int)
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
  -- a reload writes only differences, computed from what Redis has, so
  -- members that should not be there go, and the generation goes up
  gen0 <- redisRun env $ Redis.hget (xpubKey env x "meta") "gen"
  void . redisRun env $ Redis.zadd (xpubKey env x "txs") [(1, "not a transaction")]
  void . redisRun env $ Redis.zadd (xpubKey env x "utxo") [(1, "not an output")]
  cacheRefresh env.redisCache x.spec HashSet.empty `shouldReturn` Reloaded
  redisRun env (Redis.zscore (xpubKey env x "txs") "not a transaction") `shouldReturn` Nothing
  redisRun env (Redis.zscore (xpubKey env x "utxo") "not an output") `shouldReturn` Nothing
  gen1 <- redisRun env $ Redis.hget (xpubKey env x "meta") "gen"
  (read . C8.unpack <$> gen1) `shouldBe` ((+ (1 :: Int)) . read . C8.unpack <$> gen0)
  checkXPub env x ref

-- | Changes of a cached xpub are applied from the transactions that make
-- them, without a full reload: the time of the last full reload in the entry
-- stays, and the answers stay those of the database. When Redis has a balance
-- that no changed transaction explains, a change was missed, and the next
-- reload reads the xpub whole, which repairs it.
incrementalTest :: Env -> IO ()
incrementalTest env = do
  let x = newXPub env "incremental" DeriveNormal
      meta k = redisRun env (Redis.hget (xpubKey env x "meta") k)
  ref <- newLedger
  void $ pay env x ref [([0, 0], 100000), ([0, 1], 200000), ([1, 0], 300000)]
  void $ mine env 1
  checkXPub env x ref
  full0 <- meta "full"
  full0 `shouldSatisfy` isJust
  meta "ver" `shouldReturn` Just "3"
  -- a payment in the mempool, its confirmation, and a spend
  void $ pay env x ref [([0, 2], 400000)]
  waitSynced env
  checkXPub env x ref
  void $ mine env 1
  checkXPub env x ref
  out <- getNewAddress env.node
  l <- readIORef ref
  void $ spend env x ref [coinAt l [0, 1]] [(out, 50000), (at env x [1, 1], 140000)]
  waitSynced env
  checkXPub env x ref
  void $ mine env 1
  checkXPub env x ref
  meta "full" `shouldReturn` full0
  -- the index of outputs by outpoint matches the outputs
  nu <- redisRun env (Redis.zcard (xpubKey env x "utxo"))
  no <- redisRun env (Redis.hlen (xpubKey env x "uop"))
  no `shouldBe` nu
  -- a balance that Redis has wrong, at an address the next change does not
  -- touch, as if a change had been missed
  Just blob <- redisRun env (Redis.get (xpubKey env x "bal"))
  bals <- either fail return (decode blob :: Either String [XPubBal])
  let target = addrOf env x [0, 0]
      wrong :: XPubBal -> XPubBal
      wrong b
        | b.balance.address == target =
            XPubBal {path = b.path, balance = (b.balance :: Balance) {received = b.balance.received + 1}}
        | otherwise = b
  void . redisRun env $ Redis.set (xpubKey env x "bal") (encode (map wrong bals))
  void $ pay env x ref [([0, 3], 500000)]
  waitSynced env
  checkXPub env x ref
  full1 <- meta "full"
  (full1 > full0) `shouldBe` True
  -- the wrong balance is gone: Redis has the balances of the database
  fromDB <- L.withView env.store.db (`L.xPubBals` x.spec)
  Just blob' <- redisRun env (Redis.get (xpubKey env x "bal"))
  decode blob' `shouldBe` Right fromDB

-- | Under load, the database can have transactions whose events the cache has
-- not applied yet, and the balances that a reload reads include them. The
-- reload finds them from the addresses whose balances moved, without reading
-- the xpub whole. Here the entry is wound back to before a payment, as if
-- its event had not come, and a reload is told only of an older change.
catchUpTest :: Env -> IO ()
catchUpTest env = do
  let x = newXPub env "catch-up" DeriveNormal
      key = xpubKey env x
      meta k = redisRun env (Redis.hget (key "meta") k)
  ref <- newLedger
  old <- pay env x ref [([0, 0], 100000)]
  void $ mine env 1
  checkXPub env x ref
  full0 <- meta "full"
  Just blob0 <- redisRun env (Redis.get (key "bal"))
  -- a payment in the mempool, applied by the cache
  newT <- pay env x ref [([0, 1], 200000)]
  waitSynced env
  checkXPub env x ref
  let new = fromMaybe (error "bad txid") (hexToTxHash newT)
      oldH = fromMaybe (error "bad txid") (hexToTxHash old)
  -- wind the entry back: balances, transaction and outputs of the payment
  uop <- redisRun env (Redis.hgetall (key "uop"))
  let paidBy o = either (const False) (\p -> p.hash == new) (decode o :: Either String OutPoint)
      ours = [(op, u) | (op, u) <- uop, paidBy op]
  outs <- maybe (fail "no outputs of the payment in the entry") return (NonEmpty.nonEmpty ours)
  void . redisRun env $ Redis.set (key "bal") blob0
  void . redisRun env $ Redis.zrem (key "txs") (NonEmpty.singleton (encode new))
  void . redisRun env $ Redis.zrem (key "utxo") (snd <$> outs)
  void . redisRun env $ Redis.hdel (key "uop") (fst <$> outs)
  cacheRefresh env.redisCache x.spec (HashSet.singleton oldH) `shouldReturn` Reloaded
  meta "full" `shouldReturn` full0
  redisRun env (Redis.zscore (key "txs") (encode new)) >>= (`shouldSatisfy` isJust)
  checkXPub env x ref
  fromDB <- L.withView env.store.db (`L.xPubBals` x.spec)
  Just blob1 <- redisRun env (Redis.get (key "bal"))
  decode blob1 `shouldBe` Right fromDB

-- | An output of a cached transaction that pays an address outside the xpub
-- is spent by a transaction that does not touch the xpub. The xpub is not
-- reloaded, but the cached body must show the spender.
spenderTest :: Env -> IO ()
spenderTest env = do
  let x = newXPub env "spender" DeriveNormal
  ref <- newLedger
  w <- getNewAddress env.node
  raw <-
    rpc
      env.node
      "createrawtransaction"
      [ toJSON ([] :: [Value]),
        object [Key.fromText (at env x [0, 0]) .= btc 200000, Key.fromText w .= btc 300000]
      ]
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
  raw2 <-
    rpc
      env.node
      "createrawtransaction"
      [ toJSON [object ["txid" .= txT, "vout" .= vout]],
        object [Key.fromText w2 .= btc 290000]
      ]
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

-- | While Redis does not answer, the store answers from its own database,
-- and its health check says that it does with status 203: not a failure,
-- since the store works, and not the 200 of a store that has everything.
-- When Redis is back, so is the 200. Redis is told to hold every client for
-- a while, which is what a server that hangs looks like from outside.
redisDownTest :: Env -> IO ()
redisDownTest env = do
  let x = newXPub env "redisdown" DeriveNormal
  ref <- newLedger
  void $ pay env x ref [([0, 0], 100000), ([1, 0], 50000)]
  void $ mine env 1
  checkXPub env x ref
  let redisOK (_, v) = (v ! "redis") ! "ok"
  (code0, healthy) <- getJSON env.cached "/health"
  code0 `shouldBe` 200
  redisOK (code0, healthy) `shouldBe` Bool True
  -- a store without a cache has no such check
  (_, bare) <- getJSON env.plain "/health"
  bare `shouldSatisfy` \case
    Object o -> not (KeyMap.member "redis" o)
    _ -> False
  held <- redisRun env (Redis.sendRequest ["CLIENT", "PAUSE", "12000"])
  held `shouldBe` Redis.Ok
  -- the first request waits two seconds for Redis and gives up on it
  (code, _) <- getJSON env.cached ("/xpub/" <> x.text <> "/balances")
  code `shouldBe` 200
  waitFor "the health check to notice that Redis is gone" $
    (== 203) . fst <$> getJSON env.cached "/health"
  redisOK <$> getJSON env.cached "/health" `shouldReturn` Bool False
  checkXPub env x ref
  waitFor "the health check to notice that Redis is back" $
    (== 200) . fst <$> getJSON env.cached "/health"
  checkXPub env x ref

-- | An error is answered in JSON, whatever the request asks for, and
-- every answer says that it depends on whether the client reads gzip, and
-- on nothing else.
errorFormatTest :: Env -> IO ()
errorFormatTest env = do
  let unknown = "/transaction/" <> replicate 64 'a'
      ask accept = do
        let opts =
              Wreq.defaults
                & Wreq.checkResponse ?~ (\_ _ -> return ())
                & maybe id (\a -> Wreq.header "Accept" .~ [a]) accept
        r <- Wreq.getWith opts (env.plain <> unknown)
        return
          ( r ^. Wreq.responseStatus . Wreq.statusCode,
            r ^. Wreq.responseHeader "Content-Type",
            r ^. Wreq.responseHeader "Vary",
            r ^. Wreq.responseBody
          )
  forM_ [Just "application/octet-stream", Nothing] $ \accept -> do
    (code, kind, vary, json) <- ask accept
    (code, kind, vary) `shouldBe` (404, "application/json", "Accept-Encoding")
    (body json ! "error") `shouldBe` String "not-found-or-invalid-arg"

-- | The client of haskoin-store-data reads what the server answers: values
-- in the JSON of the network, plain JSON, raw transactions, errors, and the
-- lines of the events.
clientTest :: Env -> IO ()
clientTest env = do
  let x = newXPub env "client" DeriveNormal
      -- the client puts the name of the network in the path, as
      -- api.haskoin.com has it, and the server of the tests has none
      conf = Client.ApiConfig {net = env.net {name = ""}, host = env.plain}
      call :: (ApiResource a b) => a -> IO b
      call r = Client.apiCall env.ctx conf r >>= either (fail . show) return
  heard <- newIORef []
  listener <- async . Client.apiEvents conf $ \e -> modifyIORef' heard (e :) >> return True
  -- the client is listening once it hears of a block
  void (mine env 1)
  waitFor "the client to listen" (not . null <$> readIORef heard)
  ref <- newLedger
  paid <- pay env x ref [([0, 0], 100000)]
  txid <- maybe (fail "Not a hash") return (hexToTxHash paid)
  -- the node relays the payment in its own time: a block before it would
  -- confirm it without it ever entering the mempool of the store
  waitFor "the client to hear of the payment" ((EventTx txid `elem`) <$> readIORef heard)
  [mined] <- mine env 1
  block <- call (Client.GetBlockBest (NoTx True))
  blockHashToHex (headerHash block.header) `shouldBe` mined
  tx <- call (Client.GetTx txid)
  tx.txid `shouldBe` txid
  raw <- call (Client.GetTxRaw txid)
  txHash raw.get `shouldBe` txid
  balances <- call (Client.GetAddrsBalance [addrOf env x [0, 0]])
  map (.confirmed) balances.get `shouldBe` [100000]
  keys <- call (Client.GetXPubBalances x.spec.key DeriveNormal (NoCache False))
  sum (map (.balance.confirmed) keys.get) `shouldBe` 100000
  health <- call Client.GetHealth
  health.blocks.blocks `shouldSatisfy` (> 0)
  -- an error comes back as the error that it is
  unknown <- Client.apiCall env.ctx conf (Client.GetTx (TxHash (sha256 ("nothing" :: C8.ByteString))))
  either Just (const Nothing) unknown `shouldBe` Just ThingNotFound
  hash <- maybe (fail "Not a hash") return (hexToBlockHash mined)
  waitFor "the client to hear of the block" ((EventBlock hash `elem`) <$> readIORef heard)
  cancel listener

-- | A web server on the store of the tests, given a certificate that is
-- made for the occasion: it answers over HTTPS, and does not answer a
-- request that is not encrypted. The requests are made with curl, told
-- not to mind who signed the certificate.
tlsTest :: Env -> IO ()
tlsTest env = withSystemTempDirectory "haskoin-store-tls-" $ \dir -> withTestLogger $ \logger -> do
  let certificate = dir <> "/cert.pem"
      key = dir <> "/key.pem"
      subject = ["-subj", "/CN=localhost", "-days", "1"]
      files = ["-keyout", key, "-out", certificate]
  (made, _, why) <-
    readProcessWithExitCode "openssl" (["req", "-x509", "-newkey", "rsa:2048", "-nodes"] <> files <> subject) ""
  (made, why) `shouldSatisfy` ((== ExitSuccess) . fst)
  listening $ \(sock, port) -> do
    let config = (webConfig logger env.store) {tls = Just (certificate, key)} :: WebConfig
        status scheme = do
          let url = scheme <> "://127.0.0.1:" <> show port <> "/health"
          let quietly = ["-sk", "-m", "5", "-o", "/dev/null", "-w", "%{http_code}"]
          (_, code, _) <- readProcessWithExitCode "curl" (quietly <> [url]) ""
          return code
    withAsync (runWebSocket config sock) $ \web -> do
      link web
      waitFor "the server to answer over HTTPS" ((== "200") <$> status "https")
      status "http" `shouldNotReturn` "200"

-- | The parts of HTTP that are not about any one endpoint: a request for
-- the headers alone, the question that a browser asks before a request
-- from another site, compression, a list of addresses in the body of a
-- request, and the reference of the API at the root of the server.
httpTest :: Env -> IO ()
httpTest env = do
  let x = newXPub env "http" DeriveNormal
      addresses = T.intercalate "," [at env x [0, i] | i <- [0 .. 2]]
      quiet = Wreq.defaults & Wreq.checkResponse ?~ (\_ _ -> return ())
      statusOf r = r ^. Wreq.responseStatus . Wreq.statusCode
  -- the headers of an answer, without the answer
  headed <- Wreq.headWith quiet (env.plain <> "/block/best")
  (statusOf headed, headed ^. Wreq.responseHeader "Content-Type") `shouldBe` (200, "application/json")
  -- what may be asked from another site
  asked <- Wreq.optionsWith quiet (env.plain <> "/transactions")
  statusOf asked `shouldBe` 204
  asked ^. Wreq.responseHeader "Access-Control-Allow-Methods" `shouldBe` "GET, HEAD, POST, DELETE, OPTIONS"
  asked ^. Wreq.responseHeader "Access-Control-Allow-Origin" `shouldBe` "*"
  -- compressed for a client that reads it, and the same answer
  let balances = "/address/balances?addresses=" <> T.unpack addresses
  packed <- Wreq.getWith (quiet & Wreq.header "Accept-Encoding" .~ ["gzip"]) (env.plain <> balances)
  packed ^. Wreq.responseHeader "Content-Encoding" `shouldBe` "gzip"
  (_, inQuery) <- getJSON env.plain balances
  -- the same list in the body of a request, where it may be as long as it likes
  (posted, inBody) <- postForm env.plain "/address/balances" [("addresses", addresses)]
  posted `shouldBe` 200
  inBody `shouldBe` inQuery
  length (arr inBody) `shouldBe` 3
  -- the reference of the API, as a page and as what the page shows
  page <- Wreq.getWith quiet (env.plain <> "/")
  (statusOf page, page ^. Wreq.responseHeader "Content-Type") `shouldBe` (200, "text/html; charset=utf-8")
  BL.toStrict (page ^. Wreq.responseBody) `shouldSatisfy` C8.isInfixOf "swagger-ui"
  reference <- Wreq.getWith quiet (env.plain <> "/swagger.yaml")
  statusOf reference `shouldBe` 200
  -- one server, the one that served it, wherever it is mounted
  let described = BL.toStrict (reference ^. Wreq.responseBody)
  described `shouldSatisfy` C8.isInfixOf "servers:\n  - url: './'\n"
  described `shouldNotSatisfy` C8.isInfixOf "api.haskoin.com"

-- | The events of the server over a WebSocket. A client that says nothing
-- hears of every transaction; one that names an extended public key, an
-- address or a Bloom filter hears of the transactions that touch them, and
-- all hear of the block. The key is
-- followed past the gap it had when it was named. A client that comes
-- back with the hash of an event hears what came after it, and one that
-- sends what the server cannot read, a binary frame among it, is
-- disconnected.
eventsTest :: Env -> IO ()
eventsTest env = do
  let x = newXPub env "events" DeriveNormal
      y = newXPub env "events-other" DeriveNormal
      port = read (reverse (takeWhile (/= ':') (reverse env.plain))) :: Int
      client path = WS.runClient "127.0.0.1" port path
      json = BL.toStrict . marshalJSON (env.net, env.ctx)
      -- every event that a client hears after saying something, in order
      hearing path say heard = client path $ \c -> do
        mapM_ (WS.sendTextData c) (say :: [C8.ByteString])
        forever $
          WS.receiveDataMessage c >>= \case
            WS.Text bytes _ -> modifyIORef' heard (either error id (eitherDecode bytes) :)
            WS.Binary _ -> error "A binary frame from the server"
      eventId = \case
        EventTx h -> txHashToHex h
        EventBlock h -> blockHashToHex h
      element = \case
        PubKeyAddress h -> runPutS (serialize h)
        _ -> error "Not the hash of a key"
      bloom = bloomInsert (bloomCreate 10 0.0000001 0 BloomUpdateNone) (element (addrOf env y [0, 1]))
  [all', key, address, filtered, quiet, keyFirst, addressFirst] <- mapM (const (newIORef [])) [1 .. 7 :: Int]
  listeners <-
    mapM
      async
      [ hearing "/events" [] all',
        hearing "/events" [json (EventWatchXPub x.spec)] key,
        hearing "/events" [json (EventWatch [addrOf env y [0, 0]])] address,
        hearing "/events" [json (EventFilterLoad bloom)] filtered,
        -- chosen in the URL, before the first event: blocks alone, a key
        -- under a type of address, an address
        hearing "/events?quiet=true" [] quiet,
        hearing ("/events?derive=legacy&xpub=" <> x.text) [] keyFirst,
        hearing ("/events?addresses=" <> T.unpack (at env y [0, 0])) [] addressFirst
      ]
  mapM_ link listeners
  threadDelay 1000000
  ref <- newLedger
  other <- newLedger
  let told heard n = waitFor "a client to hear of the events" ((>= n) . length <$> readIORef heard)
      paid who ledger path = do
        txid <- pay env who ledger [(path, 100000)]
        -- one at a time, so that the order of the events is known
        told all' . (+ 1) . length =<< readIORef all'
        maybe (fail "Not a hash") (return . EventTx) (hexToTxHash txid)
  one <- paid x ref [0, 0]
  two <- paid y other [0, 0]
  three <- paid y other [0, 1]
  -- past the gap of the key as it was named, within it since its payment
  four <- paid x ref [0, gap]
  [mined] <- mine env 1
  block <- maybe (fail "Not a hash") (return . EventBlock) (hexToBlockHash mined)
  told all' 5
  told key 3
  told address 2
  told filtered 2
  told quiet 1
  told keyFirst 3
  told addressFirst 2
  threadDelay 500000
  mapM_ cancel listeners
  (reverse <$> readIORef all') `shouldReturn` [one, two, three, four, block]
  (reverse <$> readIORef key) `shouldReturn` [one, four, block]
  (reverse <$> readIORef address) `shouldReturn` [two, block]
  (reverse <$> readIORef filtered) `shouldReturn` [three, block]
  (reverse <$> readIORef quiet) `shouldReturn` [block]
  (reverse <$> readIORef keyFirst) `shouldReturn` [one, four, block]
  (reverse <$> readIORef addressFirst) `shouldReturn` [two, block]
  -- from an event on: what came after it
  let since = "/events?after=" <> T.unpack (eventId one)
  again <- newIORef []
  resumed <- async (hearing since [] again)
  told again 4
  cancel resumed
  (reverse <$> readIORef again) `shouldReturn` [two, three, four, block]
  -- and the same without a WebSocket, one event to a line
  streamed <- eventLines (env.plain <> since) 4
  map (either error id . eitherDecode . BL.fromStrict) streamed `shouldBe` [two, three, four, block]
  -- the stream too can be of what touches an address
  chosen <- eventLines (env.plain <> since <> "&addresses=" <> T.unpack (at env y [0, 1])) 2
  map (either error id . eitherDecode . BL.fromStrict) chosen `shouldBe` [three, block]
  fst <$> getJSON env.plain "/events?addresses=nonsense" `shouldReturn` 400
  fst <$> getJSON env.plain "/events?derive=segwit" `shouldReturn` 400
  -- an event that the server does not have, and a hash that is not one
  fst <$> getJSON env.plain ("/events?after=" <> replicate 64 '0') `shouldReturn` 404
  fst <$> getJSON env.plain "/events?after=nonsense" `shouldReturn` 400
  refused <- tryAny (client ("/events?after=" <> replicate 64 '0') (\_ -> return ()))
  either (const True) (const False) refused `shouldBe` True
  -- a command that cannot be read ends the connection, with the reason,
  -- and so does a binary frame
  let sent message = do
        closed <- try . client "/events" $ \c -> do
          WS.sendDataMessage c message
          void (WS.receiveDataMessage c)
        case closed of
          Left (WS.CloseRequest code why) -> (code, why) `shouldBe` (1008, "Could not read the command")
          wrong -> expectationFailure ("Not closed: " <> show wrong)
  sent (WS.Text "{\"type\":\"nonsense\"}" Nothing)
  sent (WS.Binary "\x06")

-- | The first lines of a stream of events, which has no end: the request
-- is given up once they are there.
eventLines :: String -> Int -> IO [C8.ByteString]
eventLines url n = do
  got <- newIORef C8.empty
  let collect () chunk = do
        modifyIORef' got (<> chunk)
        enough <- (>= n) . C8.count '\n' <$> readIORef got
        when enough (throwIO (userError "enough"))
  _ <- tryAny (Wreq.foldGet collect () url)
  take n . C8.lines <$> readIORef got

-- | Two more stores, each with its own database, syncing from the same
-- node, as members of one fleet on the Redis of the tests, each with a web
-- server. The fleet agrees on the best block; a payment that both stores
-- hear of from the node reaches the pool once, and both answer every
-- question about it alike, with the one time that the pool has for it; and
-- when a block confirms it, the fleet moves there and the pool lets it go.
--
-- The payment is to an extended public key, which the fleet's cache of
-- them takes after the first request about it. Each store must then
-- answer about the key from the cache as it does when asked to answer from
-- its database, with the payment in the pool and after the block: the
-- cache is brought to the block by whichever store moved the tip.
fleetTest :: Env -> IO ()
fleetTest env = member "A" $ \a urlA -> member "B" $ \b urlB -> do
  one <- maybe (fail "The store is not a member of a fleet") return a.fleet
  let x = newXPub env "fleet" DeriveNormal
      q = "?derive=" <> deriveParam x
      address = "/address/" <> T.unpack (at env x [0, 0])
      xpub = "/xpub/" <> x.text
      paths txid =
        [ address <> "/balance",
          address <> "/transactions/full",
          address <> "/unspent",
          xpub <> q,
          xpub <> "/transactions/full" <> q,
          xpub <> "/unspent" <> q,
          "/mempool",
          "/transaction/" <> T.unpack txid
        ]
      atTip = waitFor "the fleet tip to be the best block of the node" $ do
        best <- bestBlockHash env.node
        tip <- memberTip one
        return (fmap blockHashToHex (tip >>= either (const Nothing) Just . decode) == Just best)
      alike what ps = waitFor what $ do
        fromA <- mapM (getJSON urlA) ps
        fromB <- mapM (getJSON urlB) ps
        return (fromA == fromB && all ((== 200) . fst) fromA)
      inPool txid = do
        let h = fromMaybe (error "bad txid") (hexToTxHash txid)
        redisRun env (Redis.hexists (cachePrefix env.net <> "pool:spends") (encode h))
      listed url = length . arr . snd <$> getJSON url (address <> "/transactions")
      about =
        [ xpub <> q,
          xpub <> "/balances" <> q,
          xpub <> "/transactions" <> q,
          xpub <> "/transactions/full" <> q,
          xpub <> "/unspent" <> q
        ]
      cachedKey = cachePrefix env.net <> "fx:e:" <> xpubId x.spec <> ":meta"
      -- the cache has the key, at the fleet tip, and each store answers
      -- about it as it does without the cache, and as the other store does
      throughCache what = do
        waitFor ("the cache to have the key, " <> what) $ do
          _ <- getJSON urlA (xpub <> "/balances" <> q)
          has <- redisRun env (Redis.exists cachedKey)
          tip <- memberTip one
          moves <- redisRun env (Redis.get (cachePrefix env.net <> "fleet:moves"))
          cached <- redisRun env (Redis.get (cachePrefix env.net <> "fx:tip"))
          let here = (\n t -> n <> "@" <> t) <$> moves <*> tip
          return (has && isJust tip && cached == here)
        alike ("the two stores to answer alike from the cache, " <> what) about
        waitFor ("each store to answer from the cache as from its database, " <> what) $ do
          let direct = map (<> "&nocache=true") about
          cachedA <- mapM (getJSON urlA) about
          directA <- mapM (getJSON urlA) direct
          cachedB <- mapM (getJSON urlB) about
          directB <- mapM (getJSON urlB) direct
          return (cachedA == directA && cachedB == directB && cachedA == cachedB)
  waitStore env a
  waitStore env b
  atTip
  -- what each store tells its clients from here on
  heardA <- newIORef []
  heardB <- newIORef []
  let said = \case
        StoreBestBlock h -> Just ("block " <> blockHashToHex h)
        StoreMempoolNew t -> Just ("new " <> txHashToHex t)
        StoreMempoolDelete t -> Just ("gone " <> txHashToHex t)
        _ -> Nothing
      listen st heard = withSubscription st.events $ \sub ->
        forever (receive sub >>= \e -> forM_ (said e) (\line -> modifyIORef' heard (line :)))
  listenA <- async (listen a heardA)
  listenB <- async (listen b heardB)
  threadDelay 500000
  ref <- newLedger
  txid <- pay env x ref [([0, 0], 100000)]
  waitFor "the pool of the fleet to have the payment" (inPool txid)
  waitFor "both stores to list the payment" $ (== (1, 1)) <$> ((,) <$> listed urlA <*> listed urlB)
  alike "the two stores to answer alike, with the payment in the pool" (paths txid)
  throughCache "with the payment in the pool"
  -- the payment is unconfirmed in the answers, at the time that the pool has
  (_, unconfirmed) <- getJSON urlA ("/transaction/" <> T.unpack txid)
  (unconfirmed ! "block") `shouldSatisfy` \case
    Object o -> KeyMap.member "mempool" o
    _ -> False
  void (mine env 1)
  waitStore env a
  waitStore env b
  atTip
  inPool txid `shouldReturn` False
  alike "the two stores to answer alike, with the payment confirmed" (paths txid)
  throughCache "with the payment confirmed"
  -- both told the same events in the same order: the payment as it
  -- entered the pool, and then the block, which is not told as the
  -- payment leaving
  best <- bestBlockHash env.node
  waitFor "both stores to tell of the block" $ do
    told <- (,) <$> readIORef heardA <*> readIORef heardB
    return (("block " <> best) `elem` fst told && ("block " <> best) `elem` snd told)
  toldA <- reverse <$> readIORef heardA
  toldB <- reverse <$> readIORef heardB
  toldA `shouldBe` toldB
  toldA `shouldBe` ["new " <> txid, "block " <> best]
  cancel listenA
  cancel listenB
  -- and a client that heard of the payment from one takes up on the other
  let since = "/events?after=" <> T.unpack txid
      then' = "{\"type\":\"block\",\"id\":\"" <> cs best <> "\"}"
  eventLines (urlA <> since) 1 `shouldReturn` [then']
  eventLines (urlB <> since) 1 `shouldReturn` [then']
  (_, confirmed) <- getJSON urlB ("/transaction/" <> T.unpack txid)
  (confirmed ! "block") `shouldSatisfy` \case
    Object o -> KeyMap.member "height" o
    _ -> False
  -- the health check runs every few seconds, and first ran before the
  -- store had synced
  waitFor "the health check of a member to pass" ((== 200) . fst <$> getJSON urlA "/health")
  (_, health) <- getJSON urlA "/health"
  ((health ! "redis") ! "ok") `shouldBe` Bool True
  where
    -- Both members and their web servers log through the logger of the
    -- tests, each with its name in front of its lines. A logger of their
    -- own would keep its lines in a buffer of its own, and they would be
    -- printed out of step with the rest: when the fleet test failed, that
    -- made a health check that was prompt look seconds late.
    member name f = withRamDir $ \dir -> do
      let config = env.memberIn dir
          shared = config.logger
          named = shared {push = \line -> shared.push (name <> " " <> line)} :: Logger
      withStore ((config :: StoreConfig) {logger = named}) $ \s ->
        listening $ \(sock, port) ->
          withAsync (runWebSocket (webConfig named s) sock) $ \web ->
            link web >> f s ("http://127.0.0.1:" <> show port)

-- | Run a second store, with its own database, syncing from the same node
-- and sharing the same Redis, while an action runs.
withOther :: Env -> (Store -> IO a) -> IO a
withOther env f = withRamDir $ \dir -> withStore (env.configIn dir) f

-- | Wait until a store has the best block of the node.
waitStore :: Env -> Store -> IO ()
waitStore env st =
  waitFor "the second store to sync" $ do
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
          stored <- L.withView env.store.db (`L.xPubBals` x.spec)
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
