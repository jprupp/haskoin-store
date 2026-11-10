{-# LANGUAGE RecordWildCards #-}

module Haskoin.StoreSpec (spec) where

import Conduit
import Control.Monad
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Base64
import Data.Either
import Data.HashMap.Strict qualified as HashMap
import Data.Maybe
import Data.Serialize
import Data.Time.Clock.POSIX
import Data.Word
import GHC.Stats (GCDetails (..), RTSStats (..), getRTSStats, getRTSStatsEnabled)
import Haskoin
import Haskoin.Node
import Haskoin.Store
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.TestBlocks (nextBlock)
import NQE
import Network.Socket
import System.Mem (performMajorGC)
import System.Random
import Test.Hspec
import UnliftIO

data TestStore = TestStore
  { testStoreDB :: !DatabaseReader,
    testStoreBlockStore :: !BlockStore,
    testStoreChain :: !Chain,
    testStoreEvents :: !(Inbox StoreEvent),
    -- | How many blocks each getdata sent to the mock peer asked for, oldest first.
    testStoreBlockRequests :: !(TVar [Int])
  }

spec :: Spec
spec = prepareContext $ \ctx -> do
  describe "Download" $ do
    it "gets 8 blocks" $
      withTestStore bchRegTest ctx "eight-blocks" $ \TestStore {..} -> do
        bs <- replicateM 8 . receiveMatch testStoreEvents $ \case
          StoreBestBlock b -> Just b
          _ -> Nothing
        let bestHash = last bs
        bestNodeM <- chainGetBlock testStoreChain bestHash
        bestNodeM `shouldSatisfy` isJust
        let bestNode = fromJust bestNodeM
            bestHeight = bestNode.height
        bestHeight `shouldBe` 8
    it "get a block and its transactions" $
      withTestStore bchRegTest ctx "get-block-txs" $ \TestStore {..} ->
        do
          let h1 = "5369ef2386c72acdf513ffd80aeba2a1774e2f004d120761e54a8bf614173f3e"
              get_the_block h =
                receive testStoreEvents >>= \case
                  StoreBestBlock b
                    | h <= 1 -> return b
                    | otherwise ->
                        get_the_block ((h :: Int) - 1)
                  _ -> get_the_block h
          bh <- get_the_block 15
          m <- Q.getBlock testStoreDB bh
          let bd = fromMaybe (error "Could not get block") m
          t1 <- Q.getTxData testStoreDB h1
          bd.height `shouldBe` 15
          length bd.txs `shouldBe` 1
          listToMaybe bd.txs `shouldBe` Just h1
          fmap (txHash . (.tx)) t1 `shouldBe` Just h1
    it "downloads 80 blocks with a request for 64 and one for the remaining 16" $
      withTestStore bchRegTest ctx "block-window" $ \TestStore {..} -> do
        bs <- replicateM 80 . receiveMatch testStoreEvents $ \case
          StoreBestBlock b -> Just b
          _ -> Nothing
        bestNodeM <- chainGetBlock testStoreChain (last bs)
        fmap (.height) bestNodeM `shouldBe` Just 80
        readTVarIO testStoreBlockRequests `shouldReturn` [64, 16]

  describe "Memory" $
    it "keeps the heap flat while syncing" $ do
      enabled <- getRTSStatsEnabled
      unless enabled $ pendingWith "run with +RTS -T to measure the heap"
      -- Each block passes messages through the peer, node and store loops.
      -- When forever went through a dictionary, each of six loops kept 33 to
      -- 107 bytes per message, a few hundred bytes per block in all: over
      -- the 5000 blocks measured, megabytes. The heap normally grows by about
      -- 330 kB here, and a leak of 500 bytes per block fails the test. The
      -- length is a multiple of 2000, so the last headers message the mock
      -- peer sends is empty, which haskoin-node before 2.1.2 did not handle.
      let blocks = chainOf 6000
      withTestStoreBlocks blocks bchRegTest ctx "heap" $ \TestStore {..} -> do
        let wait_for height =
              receive testStoreEvents >>= \case
                StoreBestBlock b ->
                  chainGetBlock testStoreChain b >>= \case
                    Just n | n.height >= height -> return ()
                    _ -> wait_for height
                _ -> wait_for height
            live = do
              performMajorGC
              (.gc.gcdetails_live_bytes) <$> getRTSStats
        wait_for 1000
        live_before <- live
        wait_for 6000
        live_after <- live
        let growth = fromIntegral live_after - fromIntegral live_before :: Integer
        putStrLn $ "heap growth over 5000 blocks: " <> show growth <> " bytes"
        growth `shouldSatisfy` (< 1024 * 1024)

withTestStore ::
  Network ->
  Ctx ->
  String ->
  (TestStore -> IO a) ->
  IO a
withTestStore = withTestStoreBlocks allBlocks

-- | Like 'withTestStore', with the mock peer serving these blocks.
withTestStoreBlocks ::
  [Block] ->
  Network ->
  Ctx ->
  String ->
  (TestStore -> IO a) ->
  IO a
withTestStoreBlocks blocks net ctx t f =
  withSystemTempDirectory ("haskoin-store-test-" <> t <> "-") $ \w -> do
    requests <- newTVarIO []
    let ad =
          NetworkAddress
            nodeNetwork
            (sockToHostAddress (SockAddrInet 0 0))
        cfg =
          StoreConfig
            { maxPeers = 20,
              initPeers = [],
              discover = True,
              db = w,
              net = net,
              ctx = ctx,
              redis = Nothing,
              gap = gap,
              initGap = 20,
              redisMinAddrs = 100,
              redisMinTxs = 1000,
              redisMaxXPubs = 10000,
              noMempool = False,
              wipeMempool = False,
              fleet = False,
              syncMempool = False,
              blockWindow = 64,
              mempoolTimeout = 14,
              peerTimeout = 60,
              maxPeerLife = 48 * 3600,
              connect = dummyPeerConnect blocks requests net ad,
              stats = Nothing,
              bloom = True,
              maxOpenFiles = Nothing,
              writeBufferSize = Nothing,
              maxWalSize = Nothing,
              blockCache = Nothing,
              logger = Logger {level = LevelError, source = "", push = \_ -> return ()}
            }
    withStore cfg $ \store ->
      withSubscription store.pub $ \sub ->
        f
          TestStore
            { testStoreDB = store.db,
              testStoreBlockStore = store.block,
              testStoreChain = store.chain,
              testStoreEvents = sub,
              testStoreBlockRequests = requests
            }

gap :: Word32
gap = 32

-- | The 15 canned blocks followed by 65 mined on top of them: more than one
-- 64-block window, so that a download needs a second request.
allBlocks :: [Block]
allBlocks = chainOf 80

-- | The canned blocks followed by blocks mined on top of them, this many in
-- all.
chainOf :: Int -> [Block]
chainOf n = take n (cannedBlocks <> mined (length cannedBlocks) (last cannedBlocks))
  where
    mined height parent = block : mined (height + 1) block
      where
        (block, _) = nextBlock bchRegTest (fromIntegral height) parent.header []

cannedBlocks :: [Block]
cannedBlocks =
  fromRight (error "Could not decode blocks") $
    runGet f (decodeBase64Lenient cannedBlocksBase64)
  where
    f = mapM (const get) [(1 :: Int) .. 15]

cannedBlocksBase64 :: ByteString
cannedBlocksBase64 =
  "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\
  \wT6Wn6LtY47B39Th44JKsZ8BGJCvc6wAAAAA"

dummyPeerConnect :: [Block] -> TVar [Int] -> Network -> NetworkAddress -> SockAddr -> WithConnection
dummyPeerConnect blocks requests net ad sa f = do
  r <- newInbox
  s <- newInbox
  let s' = inboxToMailbox s
  withAsync (go r s') $ \_ -> do
    let o = awaitForever (`send` r)
        i = forever (receive s >>= yield)
    f (Conduits i o) :: IO ()
  where
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
          .| iterMC (recordBlockRequest requests)
          .| concatMapC (mockPeerReact blocks)
          .| outc
          .| awaitForever (`send` s)
    outc = mapMC $ \msg' -> return $ runPut (putMessage net msg')
    inc = forever $ do
      x <- takeCE 24 .| foldC
      case decode x of
        Left _ ->
          error "Dummy peer not decode message header"
        Right (MessageHeader _ _ len _) -> do
          y <- takeCE (fromIntegral len) .| foldC
          case runGet (getMessage net) $ x `B.append` y of
            Right msg' -> yield msg'
            Left e ->
              error $
                "Dummy peer could not decode payload: " <> show e

-- | Note how many blocks a getdata asks for.
recordBlockRequest :: TVar [Int] -> Message -> IO ()
recordBlockRequest requests (MGetData (GetData ivs)) =
  atomically $ modifyTVar' requests (<> [length [() | InvVector InvBlock _ <- ivs]])
recordBlockRequest _ _ = return ()

mockPeerReact :: [Block] -> Message -> [Message]
mockPeerReact _ (MPing (Ping n)) = [MPong (Pong n)]
mockPeerReact _ (MVersion _) = [MVerAck]
mockPeerReact blocks (MGetHeaders (GetHeaders _ locator _)) = [MHeaders (Headers hs')]
  where
    f b = (b.header, VarInt $ fromIntegral $ length b.txs)
    -- Up to 2000 headers after the first block of the locator that the
    -- peer has, like a real peer; from the start if it has none of them.
    heights = HashMap.fromList (zip (map (headerHash . (.header)) blocks) [1 :: Int ..])
    start = fromMaybe 0 (listToMaybe (mapMaybe (`HashMap.lookup` heights) locator))
    hs' = map f (take 2000 (drop start blocks))
mockPeerReact blocks (MGetData (GetData ivs)) = mapMaybe f ivs
  where
    f (InvVector InvBlock h) = MBlock <$> HashMap.lookup (BlockHash h) byHash
    f _ = Nothing
    byHash = HashMap.fromList [(headerHash b.header, b) | b <- blocks]
mockPeerReact _ _ = []
