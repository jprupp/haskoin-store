-- | The fleet's cache of extended public keys, with two members on one
-- chain in RocksDB and a Redis in Podman: one build for a burst of
-- requests, the same refusal from every member, a dead holder of the dibs
-- taken over, exact reads across a change of the pool and a move of the
-- tip, a reorganisation that keeps the cache, the pool filled again after
-- Redis lost its data, and random histories after which every cached entry
-- is what the chain at the tip and the pool give.
module Haskoin.Store.Fleet.CacheSpec (spec) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently, withAsync)
import Control.Concurrent.STM (readTVarIO)
import Control.Exception (SomeException, try)
import Control.Monad (forM, forM_, join, replicateM_, unless, void, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Default (def)
import Data.Functor ((<&>))
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.IntMap.Strict qualified as IntMap
import Data.List (find)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (catMaybes, fromMaybe)
import Data.Serialize (encode)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word32, Word64, Word8)
import Database.Redis qualified as Redis
import Haskoin
  ( Block (..),
    BlockHash,
    BlockHeader (..),
    BlockNode (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    addressToScriptBS,
    btcRegTest,
    buildMerkleRoot,
    deriveXPubKey,
    doubleSHA256,
    isValidPOW,
    makeXPrvKey,
    nullOutPoint,
    scriptToAddressBS,
    txHash,
  )
import Haskoin.Store.BlockStore (toFetch)
import Haskoin.Store.Common (StoreRead (..), sortTxs)
import Haskoin.Store.Data
  ( BlockData (..),
    BlockRef (..),
    DeriveType (..),
    Prev (..),
    TxData (..),
    TxRef,
    Unspent (..),
    XPubBal,
    XPubSpec (..),
    XPubUnspent (..),
    confirmed,
  )
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Pooled (pooledRead)
import Haskoin.Store.Database.Reader (DatabaseReader (..), Derived (..), derivedAddrs)
import Haskoin.Store.Database.Views (withViewAt)
import Haskoin.Store.Fleet.Member
import Haskoin.Store.Fleet.Redis (FleetKeys (..))
import Haskoin.Store.Fleet.RedisSpec (withRedis)
import Haskoin.Store.Fleet.XPub
  ( Begun (..),
    Cached (..),
    cacheAbort,
    cacheBegin,
    cachePieces,
    cacheSize,
    cachedEntry,
    tipMoves,
    xpubId,
  )
import Haskoin.Store.Histories (openLayered, revert, run, startChain)
import Haskoin.Store.Integration.Podman (podmanUsable)
import Haskoin.Store.Integration.Temp (withSystemTempDirectory)
import Haskoin.Store.Logic (importBlock)
import Haskoin.Store.TestBlocks (script)
import Haskoin.Store.XPub (EntryMoved (..), XPubReads (..))
import Haskoin.Store.XPub.Indexer (Answer (..), Settings (..), defaultSettings, retryAfter)
import Haskoin.Store.XPub.Lock (Lock (..), LockConfig (..), Taken (..), defaultLockConfig, redisLock)
import Haskoin.Store.XPub.Service (Session (..), xpubAsk)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck

spec :: Spec
spec = do
  usable <- runIO podmanUsable
  describe "xpub cache of a fleet" $ case usable of
    Left why -> it "keeps entries equal to the chain and the pool" (pendingWith why)
    Right () -> aroundAll withRedis $ do
      it "builds a key once for a burst on two members, and both answer it" (`withBed` burstTest)
      it "refuses on two members with the same Retry-After" (`withBed` retryTest)
      it "takes over the dibs of a holder that died" (`withBed` deadHolderTest)
      it "has a request answered again when the pool changes the entry it read" (`withBed` poolChangeTest)
      it "has a request answered again when the tip moves" (`withBed` tipMoveTest)
      it "never answers an entry that a move of the tip is writing" (`withBed` markedTest)
      it "keeps the cache across a reorganisation, as the chain and the pool give it" (`withBed` reorgTest)
      it "fills the pool again after Redis lost its data" (`withBed` refillTest)
      it "fetches no transaction that another member received" (`withBed` seenTest)
      modifyMaxSuccess (const 10) $
        it "keeps every cached entry equal to the chain at the tip and the pool" $ \r ->
          property $ \steps -> ioProperty (withBed r (\bed -> modelTest bed (take 12 steps)) >> return True)

-------------------------------------------------------------------------------
-- The tests
-------------------------------------------------------------------------------

burstTest :: Bed -> IO ()
burstTest bed = do
  let x = xpub 1
  payKey bed x [0, 3] True
  answers <- mapConcurrently (\i -> xpubAsk (if even i then bed.a else bed.b).door x) [1 .. 20 :: Int]
  answers `shouldSatisfy` all (== Indexed)
  threadDelay 300000
  -- one build, which Redis announced to both
  forM_ [bed.a, bed.b] $ \m ->
    (HashMap.lookupDefault 0 (xpubId x) <$> readTVarIO m.built) `shouldReturn` 1
  forM_ [bed.a, bed.b] $ \m -> sameAsChain bed m x

retryTest :: Bed -> IO ()
retryTest bed = do
  let x = xpub 2
  ghost <- redisLock (defaultLockConfig (bed.prefix <> "fleet:") "ghost") bed.conn xpubId
  -- a job that another store expects to finish in a minute, past the wait
  taken ghost x 60000000
  answers <- forM [bed.a, bed.b] (\m -> xpubAsk m.door x)
  case answers of
    [Refused _ ta, Refused _ tb] -> do
      retryAfter ta `shouldBe` retryAfter tb
      retryAfter ta `shouldSatisfy` (> 10)
    other -> expectationFailure ("Expected two refusals: " <> show other)

deadHolderTest :: Bed -> IO ()
deadHolderTest bed = do
  let x = xpub 3
  payKey bed x [1] True
  ghost <- redisLock (defaultLockConfig (bed.prefix <> "fleet:") "ghost") {ttl = 1000} bed.conn xpubId
  -- its holder expects to be done in half a second, and never renews
  taken ghost x 500000
  xpubAsk bed.a.door x `shouldReturn` Indexed
  sameAsChain bed bed.a x

poolChangeTest :: Bed -> IO ()
poolChangeTest bed = do
  let x = xpub 4
  payKey bed x [0] True
  xpubAsk bed.a.door x `shouldReturn` Indexed
  tip <- fleetTipOf bed
  outcome <- answeringCached bed.a tip $ \_ sess _ -> do
    first <- sess.reads.balances x
    first `shouldSatisfy` (/= Nothing)
    payKey bed x [1] False
    try (sess.reads.transactions x def) :: IO (Either EntryMoved (Maybe [TxRef]))
  outcome `shouldBe` Just (Left EntryMoved)
  sameAsChain bed bed.a x

tipMoveTest :: Bed -> IO ()
tipMoveTest bed = do
  let x = xpub 5
  payKey bed x [0] True
  xpubAsk bed.a.door x `shouldReturn` Indexed
  tip <- fleetTipOf bed
  outcome <- answeringCached bed.a tip $ \_ sess _ -> do
    first <- sess.reads.balances x
    first `shouldSatisfy` (/= Nothing)
    payKey bed x [2] True
    try (sess.reads.transactions x def) :: IO (Either SomeException (Maybe [TxRef]))
  fmap (either (const True) (const False)) outcome `shouldBe` Just True
  sameAsChain bed bed.a x
  sameAsChain bed bed.b x

markedTest :: Bed -> IO ()
markedTest bed = do
  let x = xpub 6
  payKey bed x [0] True
  xpubAsk bed.a.door x `shouldReturn` Indexed
  tip <- fleetTipOf bed
  h <- best bed
  height <- maybe 0 (.height) <$> Q.getBlock bed.db h
  dest <- addressScript bed x 0
  coin : _ <- coins bed
  let tx = spend [coin] [(dest, 100000)]
      td =
        TxData
          { block = BlockRef (height + 1) 1,
            tx,
            prevs = IntMap.singleton 0 (Prev coin.script coin.value),
            deleted = False,
            rbf = False,
            timestamp = 0,
            spenders = IntMap.empty
          }
      token = "marked-test"
  cacheBegin bed.conn bed.a.keys token tip >>= \case
    Begun -> return ()
    _ -> expectationFailure "The move of the cache did not begin"
  Just moves <- withViewAt bed.a.views h (\r -> tipMoves bed.conn bed.a.keys r [] [td])
  cachePieces bed.conn bed.a.keys token tip moves `shouldReturn` True
  -- the entry is marked: it is not answered, half written
  cachedEntry bed.conn bed.a.keys tip bed.db.initGap bed.db.maxGap True x >>= \case
    Hit _ -> expectationFailure "A marked entry was answered"
    _ -> return ()
  cacheAbort bed.conn bed.a.keys token
  xpubAsk bed.a.door x `shouldReturn` Indexed
  sameAsChain bed bed.a x

reorgTest :: Bed -> IO ()
reorgTest bed = do
  let x = xpub 7
  payKey bed x [0] True
  xpubAsk bed.a.door x `shouldReturn` Indexed
  payKey bed x [1] True
  payKey bed x [2] True
  sameAsChain bed bed.a x
  -- the two blocks with the payments to 1 and 2 are abandoned for three
  -- empty ones: the payments come back to the pool
  reorganise bed 2
  (entries, _) <- cacheSize bed.conn bed.a.keys
  entries `shouldSatisfy` (> 0)
  forM_ [bed.a, bed.b] $ \m -> sameAsChain bed m x

-- | What one store received from a peer, another does not fetch when its
-- own peer announces it: both ask the same set in Redis.
seenTest :: Bed -> IO ()
seenTest bed = do
  coin : _ <- coins bed
  let tx = spend [coin] [(script, 100000)]
      other = TxHash (doubleSHA256 ("never sent" :: ByteString))
      fetch m = toFetch (\_ -> return False) (memberSeen m) (\_ -> return False)
  fetch bed.b [txHash tx, other] `shouldReturn` [txHash tx, other]
  offer bed tx
  fetch bed.b [txHash tx, other] `shouldReturn` [other]
  fetch bed.a [txHash tx, other] `shouldReturn` [other]

refillTest :: Bed -> IO ()
refillTest bed = do
  let x = xpub 8
  payKey bed x [0] True
  dest <- addressScript bed x 1
  coin : _ <- coins bed
  let tx = spend [coin] [(dest, 100000)]
  offer bed tx
  settle bed
  inPool bed tx `shouldReturn` True
  -- Redis loses the fleet's data
  lost <- Redis.runRedis bed.conn (Redis.keys (bed.prefix <> "*"))
  forM_ (either (const []) id lost) $ \k -> void (Redis.runRedis bed.conn (Redis.del (k :| [])))
  settle bed
  readTVarIO bed.a.refills `shouldReturn` 1
  readTVarIO bed.b.refills `shouldReturn` 1
  inPool bed tx `shouldReturn` False
  -- what the web server does then: the peers and the node announce their
  -- mempools again, and the member offers what they send
  offer bed tx
  settle bed
  inPool bed tx `shouldReturn` True
  xpubAsk bed.a.door x `shouldReturn` Indexed
  sameAsChain bed bed.a x

-------------------------------------------------------------------------------
-- The model test
-------------------------------------------------------------------------------

-- | A step of a random history.
data Step
  = -- | pay to an address of a key, in a block or in the pool
    Pay !Int !Word32 !Bool
  | -- | spend an output of a key, in a block or in the pool
    Spend !Int !Bool
  | -- | mine what the pool has
    Mine
  | -- | abandon so many blocks for one more
    Reorg !Int
  | -- | ask for the entry of a key
    Build !Int
  deriving (Eq, Show)

instance Arbitrary Step where
  arbitrary =
    frequency
      [ (4, Pay <$> choose (0, 1) <*> choose (0, 27) <*> arbitrary),
        (2, Spend <$> choose (0, 1) <*> arbitrary),
        (2, pure Mine),
        (1, Reorg <$> choose (1, 4)),
        (3, Build <$> choose (0, 1))
      ]

-- | After every step of a history, every entry that the cache has, read
-- through the scripts of Redis by each member, is what the chain at the
-- fleet tip and the pool give.
modelTest :: Bed -> [Step] -> IO ()
modelTest bed steps =
  forM_ steps $ \s -> do
    step s
    forM_ keys $ \x -> forM_ [bed.a, bed.b] $ \m ->
      cachedSeen m x >>= \case
        Nothing -> return ()
        Just seen -> expected bed x >>= \e -> (s, seen) `shouldBe` (s, e)
  where
    keys = [xpub 101, xpub 102]
    key i = keys !! i
    step = \case
      Pay i n inBlock -> payKey bed (key i) [n] inBlock
      Spend i inBlock -> do
        let x = key i
        outs <-
          atTip bed $ \q -> do
            bals <- q.xPubBals x
            q.xPubUnspents x bals def
        let usable = [u.unspent | u <- outs, not inBlock || confirmed u.unspent.block]
        case usable of
          [] -> return ()
          u : _ -> do
            let tx = spend [u] [(script, 1000)]
            if inBlock then mineBlock bed [tx] else offer bed tx
            settle bed
      Mine -> poolTxs bed >>= mineBlock bed >> settle bed
      Reorg n -> do
        height <- best bed >>= Q.getBlock bed.db <&> maybe 0 (.height)
        when (fromIntegral height > n + 1) (reorganise bed n)
      Build i -> void (xpubAsk bed.a.door (key i))
    cachedSeen m x = do
      tip <- fleetTipOf bed
      join <$> answeringCached m tip (\_ sess _ -> seenIn sess x)

-------------------------------------------------------------------------------
-- The test bed
-------------------------------------------------------------------------------

-- | Two members of a fleet on one database and one Redis.
data Bed = Bed
  { conn :: !Redis.Connection,
    prefix :: !ByteString,
    db :: !DatabaseReader,
    a :: !Member,
    b :: !Member,
    salt :: !(IORef Word32)
  }

-- | A new chain with a few blocks of coins, in a new database, and two
-- members on keys of Redis that no other test uses, with their indexers.
withBed :: (Redis.Connection, IO FleetKeys) -> (Bed -> IO x) -> IO x
withBed (conn, fresh) go = do
  keys <- fresh
  withSystemTempDirectory "haskoin-store-fleet-cache" $ \dir ->
    openLayered btcRegTest dir $ \db -> do
      startChain db
      a <- newMember (config keys.prefix "a") conn db
      b <- newMember (config keys.prefix "b") conn db
      salt <- newIORef 0
      let bed = Bed {conn, prefix = keys.prefix, db, a, b, salt}
      withAsync (memberCacheLoop a) $ \_ ->
        withAsync (memberCacheLoop b) $ \_ -> do
          noted bed
          replicateM_ 3 (mineBlock bed [])
          settle bed
          go bed
  where
    config p name =
      MemberConfig
        { prefix = p,
          name,
          lease = 60000,
          depth = 10,
          expiry = 0,
          cacheMax = 100,
          indexer = defaultSettings {workers = 2},
          stats = Nothing,
          note = \_ _ -> return ()
        }

-- | A key of the tests.
xpub :: Word8 -> XPubSpec
xpub n = XPubSpec {key = deriveXPubKey (makeXPrvKey (B.replicate 32 n)), deriv = DeriveNormal}

-- | The best block of the database.
best :: Bed -> IO BlockHash
best bed = Q.getBestBlock bed.db >>= maybe (fail "No best block") return

-- | The fleet tip, as Redis has it.
fleetTipOf :: Bed -> IO ByteString
fleetTipOf bed = memberTip bed.a >>= maybe (fail "No fleet tip") return

-- | Both members take a view of the best block, as the block store has
-- them do after each block.
noted :: Bed -> IO ()
noted bed = best bed >>= \h -> mapM_ (`noteBest` h) [bed.a, bed.b]

-- | Turns of both members until the fleet tip is the best block and both
-- know it.
settle :: Bed -> IO ()
settle bed = go (10 :: Int)
  where
    go n = do
      memberTick bed.a
      memberTick bed.b
      h <- best bed
      tip <- memberTip bed.a
      ta <- readTVarIO bed.a.tip
      tb <- readTVarIO bed.b.tip
      unless (tip == Just (encode h) && ta == Just h && tb == Just h) $
        if n > 0 then go (n - 1) else expectationFailure "The fleet tip did not reach the best block"

-- | Import a block with these transactions on the best block, with a
-- coinbase of its own, so that a block mined again at a height is another.
mineBlock :: Bed -> [Tx] -> IO ()
mineBlock bed txs = do
  n <- atomicModifyIORef' bed.salt (\s -> (s + 1, s))
  h <- best bed
  parent <- Q.getBlock bed.db h >>= maybe (fail "No data for the best block") return
  let height = parent.height + 1
      coinbase = Tx 1 [TxIn nullOutPoint (encode height <> encode n) maxBound] [TxOut 5000000000 script] [] 0
      included = coinbase : map snd (sortTxs txs)
      merkle = either error id (buildMerkleRoot (map txHash included))
      candidate = BlockHeader 0x20000000 h merkle (parent.header.timestamp + 600) parent.header.bits
      header = fromMaybe (error "No nonce") (find (isValidPOW btcRegTest) (map candidate [0 ..]))
  run bed.db (\w -> importBlock w (Block header included) (BlockNode header height 0 h))
    >>= either (fail . show) (void . return)
  noted bed

-- | Abandon so many blocks for one more, empty, and move the tip there.
reorganise :: Bed -> Int -> IO ()
reorganise bed n = do
  replicateM_ n (revert bed.db >> noted bed)
  replicateM_ (n + 1) (mineBlock bed [])
  settle bed

-- | Read with the queries of the view at the fleet tip and the pool.
atTip :: Bed -> (StoreRead -> IO x) -> IO x
atTip bed f =
  answering bed.a (\r pool -> f (pooledRead r pool)) >>= \case
    Answered x -> return x
    _ -> fail "No view of the fleet tip"

-- | Confirmed outputs to the script of the coinbases that the pool does
-- not spend.
coins :: Bed -> IO [Unspent]
coins bed = do
  let coinAddress = either error id (scriptToAddressBS btcRegTest script)
  filter (confirmed . (.block)) <$> atTip bed (\q -> q.getAddressUnspents coinAddress def)

-- | The output script of an address of the external chain of a key.
addressScript :: Bed -> XPubSpec -> Word32 -> IO ByteString
addressScript bed x i = do
  Derived ext _ <- derivedAddrs bed.db x
  maybe (fail "No address") return (lookup i ext >>= addressToScriptBS)

-- | A transaction that spends outputs and pays amounts to scripts, with
-- the change to the script of the coinbases.
spend :: [Unspent] -> [(ByteString, Word64)] -> Tx
spend us outs =
  Tx 1 [TxIn u.outpoint "" maxBound | u <- us] ([TxOut v s | (s, v) <- outs] <> [TxOut change script]) [] 0
  where
    change = sum (map (.value) us) - sum (map snd outs) - 1000

-- | Pay to addresses of a key from a coin, in a block or in the pool, and
-- move the tip.
payKey :: Bed -> XPubSpec -> [Word32] -> Bool -> IO ()
payKey bed x ns inBlock = do
  dests <- mapM (addressScript bed x) ns
  -- a coin that pays for all of it, not the change of a small spend
  filter ((> 100000 * fromIntegral (length ns) + 1000) . (.value)) <$> coins bed >>= \case
    [] -> return ()
    c : _ -> do
      let tx = spend [c] [(d, 100000) | d <- dests]
      if inBlock then mineBlock bed [tx] else offer bed tx
      settle bed

-- | Hand a transaction to member a, as a peer would.
offer :: Bed -> Tx -> IO ()
offer bed tx = do
  now <- round <$> getPOSIXTime
  memberReceived bed.a now tx

-- | Whether the pool has a transaction.
inPool :: Bed -> Tx -> IO Bool
inPool bed tx = atTip bed $ \q -> do
  found <- q.getTxData (txHash tx)
  return (maybe False (not . confirmed . (.block)) found)

-- | The transactions of the pool.
poolTxs :: Bed -> IO [Tx]
poolTxs bed = atTip bed $ \q -> do
  hs <- map snd <$> q.getMempool
  map (.tx) . catMaybes <$> q.getManyTxData hs

-- | Take the dibs of a key for a store that is not a member.
taken :: Lock XPubSpec -> XPubSpec -> Int -> IO ()
taken lock x eta =
  lock.acquire x eta >>= \case
    Took -> return ()
    _ -> expectationFailure "The ghost did not take the dibs"

-- | What a request reads of a key: balances, transactions, unspent outputs
-- and count.
data Seen = Seen ![XPubBal] ![TxRef] ![XPubUnspent] !Word32
  deriving (Eq, Show)

-- | What the cache has of a key, read in one session.
seenIn :: Session -> XPubSpec -> IO (Maybe Seen)
seenIn sess x =
  sess.reads.balances x >>= \case
    Nothing -> return Nothing
    Just bals -> do
      txs <- sess.reads.transactions x def
      outs <- sess.reads.unspents x bals def
      n <- sess.reads.count x
      return (Seen bals <$> txs <*> outs <*> n)

-- | What the chain at the fleet tip and the pool give for a key.
expected :: Bed -> XPubSpec -> IO Seen
expected bed x = atTip bed $ \q -> do
  bals <- q.xPubBals x
  Seen bals <$> q.xPubTxs x bals def <*> q.xPubUnspents x bals def <*> q.xPubTxCount x bals

-- | The cache answers for a key, through a member, what the chain at the
-- fleet tip and the pool give.
sameAsChain :: Bed -> Member -> XPubSpec -> IO ()
sameAsChain bed m x = do
  tip <- fleetTipOf bed
  seen <- join <$> answeringCached m tip (\_ sess _ -> seenIn sess x)
  e <- expected bed x
  seen `shouldBe` Just e
