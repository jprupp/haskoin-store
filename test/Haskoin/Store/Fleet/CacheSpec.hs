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
import Data.List (find, sortOn)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe, isJust)
import Data.Serialize (encode)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Word (Word32, Word64, Word8)
import Database.Redis qualified as Redis
import Haskoin
  ( Block (..),
    BlockHash,
    BlockHeader (..),
    BlockNode (..),
    NFToken (..),
    Network,
    OutPoint (..),
    TokenCapability (..),
    TokenData (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    addressToScriptBS,
    bchRegTest,
    btcRegTest,
    buildMerkleRoot,
    deriveXPubKey,
    doubleSHA256,
    encodeTokenPrefix,
    isValidPOW,
    makeXPrvKey,
    maxTokenAmount,
    nullOutPoint,
    scriptToAddressBS,
    txHash,
  )
import Haskoin qualified as H
import Haskoin.Store.Common (Limits (..), Start (..), StoreRead (..), sortTxs)
import Haskoin.Store.Data
  ( BlockData (..),
    BlockRef (..),
    DeriveType (..),
    Prev (..),
    TokenBalance (..),
    TxData (..),
    TxRef (..),
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
import Haskoin.Store.Fleet.Redis (FleetKeys (..), poolLeaving)
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
import Haskoin.Store.Ingest (Verdict (..))
import Haskoin.Store.Integration.Temp (withSystemTempDirectory)
import Haskoin.Store.Logic (importBlock)
import Haskoin.Store.TestBlocks (script)
import Haskoin.Store.XPub (EntryMoved (..), XPubReads (..))
import Haskoin.Store.XPub.Indexer (Answer (..), Settings (..), defaultSettings, retryAfter)
import Haskoin.Store.XPub.Lock (Lock (..), LockConfig (..), Taken (..), defaultLockConfig, redisLock)
import Haskoin.Store.XPub.Service (Session (..), xpubAsk)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.Podman (podmanUsable)
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
      it "answers a key without what a block took out of the pool, before the pool has let go of it" $
        (`withBed` leavingTest)
      it "answers a key without what a block made invalid, before the pool has let go of it" $
        (`withBed` conflictTest)
      it "keeps the cache across a reorganisation, as the chain and the pool give it" (`withBed` reorgTest)
      it "fills the pool again after Redis lost its data" (`withBed` refillTest)
      it "fills the pool again after it started again, once after Redis lost its data" (`withBed` restartTest)
      it "keeps what an abandoned block gave back when the chain moves under a move of the tip" $
        (`withBed` unsettledTest)
      it "counts as settled, for every member, what the pool has decided on" (`withBed` seenTest)
      it "offers again a transaction that left the pool and is sent again" (`withBed` resendTest)
      it "says which transaction an orphan waits for, and takes it after that one" (`withBed` orphanTest)
      it "looks at what no node of the operator vouches for, and takes anything that one does" $
        (`withBed` strangerTest)
      it "answers the tokens of the chain with what the pool does to them" (\r -> withBedOn bchRegTest r tokenTest)
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
  -- the requests that waited for the announcement are gone, and nothing of
  -- them is left: the map holds a key only while a request waits for it
  forM_ [bed.a, bed.b] $ \m -> HashMap.null <$> readTVarIO m.built `shouldReturn` True
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

-- | What the pool has decided on is settled for every store of the fleet,
-- which then does not fetch it when a peer names it: both ask the same
-- sets in Redis. What no store offered is not.
seenTest :: Bed -> IO ()
seenTest bed = do
  coin : _ <- coins bed
  let tx = spend [coin] [(script, 100000)]
      other = TxHash (doubleSHA256 ("never sent" :: ByteString))
      asked = [txHash tx, other]
  settle bed
  memberSettled bed.a asked `shouldReturn` Just [False, False]
  memberSettled bed.b asked `shouldReturn` Just [False, False]
  offer bed tx
  inPool bed tx `shouldReturn` True
  memberSettled bed.a asked `shouldReturn` Just [True, False]
  memberSettled bed.b asked `shouldReturn` Just [True, False]

-- | A transaction that a replacement pushed out of the pool, and that a
-- peer sends again, takes its place back: a member that kept a set of what
-- it had offered refused it as offered already.
resendTest :: Bed -> IO ()
resendTest bed = do
  coin : _ <- coins bed
  let first = spend [coin] [(script, 100000)]
      second = spend [coin] [(script, 200000)]
  offer bed first
  settle bed
  inPool bed first `shouldReturn` True
  offer bed second
  settle bed
  inPool bed second `shouldReturn` True
  inPool bed first `shouldReturn` False
  offer bed first
  settle bed
  inPool bed first `shouldReturn` True
  inPool bed second `shouldReturn` False

-- | A transaction that only strangers named is looked at before it is
-- offered ('memberOfferTimed' with no voucher): one whose signature does
-- not verify is forged, one that spends a coin locked by a script cannot
-- be verified, and neither is in the pool after. One that spends a coin
-- locked to a key, with the signature of that key, is taken.
--
-- The same transactions with a voucher are all taken, the forged one
-- too: a node of the operator took them, and the store takes what its
-- node takes. That is what became of every transaction, from any peer,
-- before the store told its own nodes from strangers.
strangerTest :: Bed -> IO ()
strangerTest bed = do
  coin : _ <- coins bed
  now <- round <$> getPOSIXTime
  let key = fromMaybe (error "No key") (H.secKey (B.replicate 31 0 <> "\x07"))
      toKey = H.PayPKHash (H.addressHash (H.exportPubKey True (H.derivePubKey key)))
      keyScript = H.encodeSerial toKey
      -- pay to the hash of a script: only a node runs the script
      toScript = "\xa9\x14" <> B.replicate 20 7 <> "\x87"
      fund = spend [coin] [(keyScript, 100000), (keyScript, 100000), (toScript, 100000)]
      out = OutPoint (txHash fund)
      pay n = Tx 1 [TxIn (out n) "" maxBound] [TxOut 90000 script] [] 0
      allOf = if isJust bed.net.sigHashForkId then H.setForkIdFlag H.sigHashAll else H.sigHashAll
      sign n = either error id (H.signTx bed.net (pay n) [H.SigInput toKey 100000 (out n) allOf Nothing] [key])
      good = sign 0
      -- signed, and then made to pay a satoshi less
      forged = (sign 1) {outputs = [TxOut 89999 script]} :: Tx
      scripted = pay 2
      offer vouched tx = fst <$> memberOfferTimed bed.a vouched now tx
  settle bed
  memberOffer bed.a now fund `shouldReturn` Settled
  offer False forged `shouldReturn` Forged
  offer False scripted `shouldReturn` Unverified
  mapM (inPool bed) [forged, scripted] `shouldReturn` [False, False]
  offer False good `shouldReturn` Settled
  inPool bed good `shouldReturn` True
  -- vouched for, they are taken as they are
  offer True scripted `shouldReturn` Settled
  offer True forged `shouldReturn` Settled
  mapM (inPool bed) [forged, scripted] `shouldReturn` [True, True]
  -- and what the pool has is not doubted, whoever names it again
  offer False scripted `shouldReturn` Settled

-- | A transaction offered before the one that it spends from is an
-- orphan, and the member says which transaction it lacks: what the path
-- into the pool waits for before it offers the orphan again
-- ("Haskoin.Store.Ingest").
orphanTest :: Bed -> IO ()
orphanTest bed = do
  coin : _ <- coins bed
  now <- round <$> getPOSIXTime
  let parent = spend [coin] [(script, 100000)]
      child = Tx 1 [TxIn (OutPoint (txHash parent) 0) "" maxBound] [TxOut 1000 script] [] 0
  settle bed
  memberOffer bed.a now child `shouldReturn` Orphan [txHash parent]
  memberOffer bed.a now parent `shouldReturn` Settled
  memberOffer bed.a now child `shouldReturn` Settled
  inPool bed child `shouldReturn` True

-- | Each member counts a refill when the pool starts again, which the count
-- of restarts says; and one only, not two, when Redis loses its data with
-- that count in it.
restartTest :: Bed -> IO ()
restartTest bed = do
  settle bed
  readTVarIO bed.a.refills `shouldReturn` 0
  -- what a restart of the pool does to the count
  void (Redis.runRedis bed.conn (Redis.incr (bed.prefix <> "fleet:restarts")))
  settle bed
  readTVarIO bed.a.refills `shouldReturn` 1
  readTVarIO bed.b.refills `shouldReturn` 1
  lost <- Redis.runRedis bed.conn (Redis.keys (bed.prefix <> "*"))
  forM_ (either (const []) id lost) $ \k -> void (Redis.runRedis bed.conn (Redis.del (k :| [])))
  settle bed
  readTVarIO bed.a.refills `shouldReturn` 2
  readTVarIO bed.b.refills `shouldReturn` 2

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

-- | A store alone in its fleet reports a block, and reverts it before it
-- moves the tip there, as a block store does in a reorganisation while the
-- member takes its turn. The tip waits for the next report instead of
-- moving with an emptied pool: a payment of an abandoned block that no
-- node has any more comes back to the pool, and is never in the pool while
-- the tip still has it confirmed.
unsettledTest :: Bed -> IO ()
unsettledTest bed = do
  -- the oldest coin, of a block that is not abandoned
  coin : _ <- sortOn (.block) <$> coins bed
  let tx = spend [coin] [(script, 100000)]
      confirmedAtTip = atTip bed $ \q -> maybe False (confirmed . (.block)) <$> q.getTxData (txHash tx)
  mineBlock bed [tx]
  settle bed
  confirmedAtTip `shouldReturn` True
  -- the other member stops: this one is alone
  void . Redis.runRedis bed.conn $ do
    void $ Redis.hdel bed.a.keys.lease ("b" :| [])
    Redis.hdel bed.a.keys.recent ("b" :| [])
  -- the block of the payment is reverted, and the store reports its parent
  revert bed.db >> noted bed
  void (report bed.a)
  -- and reverts the parent too before it moves the tip
  revert bed.db
  moveTip bed.a `shouldReturn` False
  confirmedAtTip `shouldReturn` True
  noted bed
  replicateM_ 3 (mineBlock bed [])
  settle bed
  inPool bed tx `shouldReturn` True

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

-- | The token balances, holders, transactions and unspent outputs of a
-- category, from the chain with the pool on top (CashTokens). A block makes
-- a supply of the largest amount and a non-fungible token; in the pool the
-- supply moves twice, which sums that counted it each time would not hold.
-- Once a block confirms the pool, the chain alone gives what the pool did.
tokenTest :: Bed -> IO ()
tokenTest bed = do
  c : _ <- coins bed
  let cat = c.outpoint.hash
      (alice, bob, carol) = (p2pkh 0x11, p2pkh 0x22, p2pkh 0x33)
      p2pkh n = "\x76\xa9\x14" <> B.replicate 20 n <> "\x88\xac"
      holding amount nft lock = fromMaybe (error "Bad tokens") (encodeTokenPrefix (TokenData cat amount nft)) <> lock
      addr lock = either error id (scriptToAddressBS bed.net lock)
      most = maxTokenAmount
      badge = Just (NFToken MutableCapability "x")
      minted = spend [c] [(holding most Nothing alice, 1000000), (holding 0 badge bob, 1000000)]
      -- one input and some outputs
      tx p outs = Tx 1 [TxIn p "" maxBound] [TxOut v lock | (lock, v) <- outs] [] 0
      moved =
        tx
          (OutPoint (txHash minted) 0)
          [(holding 1000 Nothing bob, 400000), (holding (most - 1000) Nothing alice, 400000)]
      onward = tx (OutPoint (txHash moved) 1) [(holding (most - 1000) Nothing carol, 300000)]
  mineBlock bed [minted]
  settle bed
  offer bed moved
  settle bed
  offer bed onward
  settle bed
  poolTxs bed >>= \txs -> map txHash txs `shouldMatchList` map txHash [moved, onward]
  let balance lock inBlocks unconfirmed nfts utxo txs received =
        TokenBalance
          { address = addr lock,
            category = cat,
            confirmedTokens = inBlocks,
            unconfirmedTokens = unconfirmed,
            nfts,
            tokenUtxo = utxo,
            tokenTxs = txs,
            receivedTokens = received
          }
      unsettled =
        [ balance alice 0 0 0 0 3 (most + most - 1000),
          balance bob 0 1000 1 2 2 1000,
          balance carol 0 (most - 1000) 0 1 1 (most - 1000)
        ]
      byAddress = sortOn (encode . (.address))
  -- an address: what the block gave it, with what the pool did
  forM_ unsettled $ \b -> atTip bed (\q -> q.getAddressTokens b.address) `shouldReturn` [b]
  -- the holders, in the order of their addresses, by the page
  atTip bed (\q -> q.getTokenHolders cat def) `shouldReturn` byAddress unsettled
  atTip bed (\q -> q.getTokenHolders cat def {offset = 1, limit = 1})
    `shouldReturn` take 1 (drop 1 (byAddress unsettled))
  -- the transactions: the two of the pool, then the block's
  txs <- atTip bed (\q -> q.getTokenTxs cat def)
  map (.txid) (take 2 txs) `shouldMatchList` map txHash [moved, onward]
  map (.txid) (drop 2 txs) `shouldBe` [txHash minted]
  -- from one of the pool: the page starts there
  later <- atTip bed (\q -> q.getTokenTxs cat def {start = Just (AtTx (txs !! 1).txid)})
  map (.txid) later `shouldBe` map (.txid) (drop 1 txs)
  -- the unspent outputs: those that the pool makes and keeps, then the
  -- block's that the pool does not spend
  outs <- atTip bed (\q -> q.getTokenUnspents cat def)
  map (.outpoint) (take 2 outs) `shouldMatchList` [OutPoint (txHash moved) 0, OutPoint (txHash onward) 0]
  map (.outpoint) (drop 2 outs) `shouldBe` [OutPoint (txHash minted) 1]
  map (.address) outs `shouldMatchList` map (Just . addr) [bob, carol, bob]
  -- confirmed, the chain alone gives the same, with what was unconfirmed
  -- now confirmed
  mineBlock bed [moved, onward]
  settle bed
  poolTxs bed `shouldReturn` []
  let settled = [b {confirmedTokens = b.confirmedTokens + b.unconfirmedTokens, unconfirmedTokens = 0} | b <- unsettled]
  forM_ settled $ \b -> atTip bed (\q -> q.getAddressTokens b.address) `shouldReturn` [b]
  outs' <- atTip bed (\q -> q.getTokenUnspents cat def)
  map (.outpoint) outs' `shouldMatchList` map (.outpoint) outs

-------------------------------------------------------------------------------
-- The test bed
-------------------------------------------------------------------------------

-- | Two members of a fleet on one database and one Redis.
data Bed = Bed
  { net :: !Network,
    conn :: !Redis.Connection,
    prefix :: !ByteString,
    db :: !DatabaseReader,
    a :: !Member,
    b :: !Member,
    salt :: !(IORef Word32)
  }

-- | A new chain with a few blocks of coins, in a new database, and two
-- members on keys of Redis that no other test uses, with their indexers.
withBed :: (Redis.Connection, IO FleetKeys) -> (Bed -> IO x) -> IO x
withBed = withBedOn btcRegTest

-- | 'withBed' on a network.
withBedOn :: Network -> (Redis.Connection, IO FleetKeys) -> (Bed -> IO x) -> IO x
withBedOn net (conn, fresh) go = do
  keys <- fresh
  withSystemTempDirectory "haskoin-store-fleet-cache" $ \dir ->
    openLayered net dir $ \db -> do
      startChain db
      a <- newMember (config keys.prefix "a") conn db
      b <- newMember (config keys.prefix "b") conn db
      salt <- newIORef 0
      let bed = Bed {net, conn, prefix = keys.prefix, db, a, b, salt}
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
-- | A block confirms a transaction of the pool at an address of a cached
-- key, and the store that moves the tip stops right after: the
-- transaction is marked as leaving, and still in the pool. The chain at
-- the tip has it, and so has the entry; what the pool keeps for the
-- address still counts it. A read of the key takes it out, and answers
-- what the chain and the pool give.
--
-- The address is one that the key has used already, so that the entry
-- has the addresses that a scan of the key finds, and is the one read:
-- with a new address the entry is a miss, the key is read address by
-- address, and this would pass without the cache taking anything out.
leavingTest :: Bed -> IO ()
leavingTest bed = do
  let x = xpub 9
  payKey bed x [0] True
  xpubAsk bed.a.door x `shouldReturn` Indexed
  dest <- addressScript bed x 0
  c : other : _ <- filter ((> 101000) . (.value)) <$> coins bed
  -- one that the block confirms, and one that stays in the pool: while
  -- the pool has a transaction at the key, what it keeps for each
  -- address is read
  let tx = spend [c] [(dest, 100000)]
      staying = spend [other] [(dest, 100000)]
  offer bed tx
  offer bed staying
  settle bed
  inPool bed tx `shouldReturn` True
  sameAsChain bed bed.a x
  -- the block, and a move of the tip with nothing after it
  mineBlock bed [tx]
  noted bed
  forM_ [bed.a, bed.b] (\m -> report m `shouldReturn` True)
  moveTip bed.a `shouldReturn` True
  poolLeaving bed.conn bed.a.keys `shouldReturn` [encode (txHash tx)]
  -- the entry is read, which takes the transaction out
  tip <- fleetTipOf bed
  cachedEntry bed.conn bed.a.keys tip bed.db.initGap bed.db.maxGap True x >>= \case
    Hit _ -> return ()
    _ -> expectationFailure "The entry of the key was not answered"
  poolLeaving bed.conn bed.a.keys `shouldReturn` []
  sameAsChain bed bed.a x
  settle bed
  sameAsChain bed bed.b x

-- | A block confirms a transaction that the pool never had, and that
-- spends what a transaction of the pool spends: the one of the pool pays
-- an address of a cached key, and is no longer valid. The store that
-- moves the tip stops right after, so it is marked as leaving and still
-- in the pool, with what it pays the key.
--
-- The chain does not have it and never will, so nothing of the chain's
-- side of the entry says that it is gone: only the mark does. A read of
-- the key takes it out, and answers what the chain and the pool give,
-- without the payment.
conflictTest :: Bed -> IO ()
conflictTest bed = do
  let x = xpub 11
  payKey bed x [0] True
  xpubAsk bed.a.door x `shouldReturn` Indexed
  dest <- addressScript bed x 0
  away <- addressScript bed (xpub 12) 0
  c : other : _ <- filter ((> 101000) . (.value)) <$> coins bed
  let loser = spend [c] [(dest, 100000)]
      winner = spend [c] [(away, 90000)]
      staying = spend [other] [(dest, 100000)]
  offer bed loser
  offer bed staying
  settle bed
  inPool bed loser `shouldReturn` True
  sameAsChain bed bed.a x
  before <- expected bed x
  -- the block with the other spend of the same coin, and a move of the
  -- tip with nothing after it
  mineBlock bed [winner]
  noted bed
  forM_ [bed.a, bed.b] (\m -> report m `shouldReturn` True)
  moveTip bed.a `shouldReturn` True
  poolLeaving bed.conn bed.a.keys `shouldReturn` [encode (txHash loser)]
  -- the entry is read, which takes the transaction out
  tip <- fleetTipOf bed
  cachedEntry bed.conn bed.a.keys tip bed.db.initGap bed.db.maxGap True x >>= \case
    Hit _ -> return ()
    _ -> expectationFailure "The entry of the key was not answered"
  poolLeaving bed.conn bed.a.keys `shouldReturn` []
  sameAsChain bed bed.a x
  inPool bed loser `shouldReturn` False
  inPool bed staying `shouldReturn` True
  -- and the key no longer has what the transaction paid it
  after <- expected bed x
  after `shouldNotBe` before
  settle bed
  sameAsChain bed bed.b x

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
      header = fromMaybe (error "No nonce") (find (isValidPOW bed.net) (map candidate [0 ..]))
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
  let coinAddress = either error id (scriptToAddressBS bed.net script)
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

-- | Offer a transaction to the pool through member a, as the path into
-- the pool does with what a peer sent: the pool has it, unless it is an
-- orphan. A member that has no view of the fleet tip yet takes a turn
-- first.
offer :: Bed -> Tx -> IO ()
offer bed tx = go (5 :: Int)
  where
    go n = do
      now <- round <$> getPOSIXTime
      memberOffer bed.a now tx >>= \case
        Again | n > 0 -> settle bed >> go (n - 1)
        Again -> expectationFailure "The member could not offer a transaction"
        _ -> return ()

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
