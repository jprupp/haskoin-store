{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedRecordDot #-}

module Haskoin.Store.CommonSpec (spec) where

import Conduit (runConduit, runConduitPure, sinkList, takeC, yieldMany, (.|))
import Control.Monad.State.Strict (State, evalState, execState, modify', runState)
import Data.ByteString.Char8 qualified as C8
import Data.IntSet qualified as IntSet
import Data.List (nub, sortOn)
import Data.Ord (Down (..))
import Data.Serialize (encode)
import Data.Word (Word32)
import Haskoin (Address (..), TxHash (..), ripemd160, sha256)
import Haskoin.Store.Common
  ( Limits (..),
    applyLimit,
    applyLimits,
    applyLimitsC,
    deOffset,
    joinDescStreams,
    mergeDescGroups,
    scanXPubBals,
    streamThings,
    txRefKey,
  )
import Haskoin.Store.Data
  ( Balance (..),
    BlockRef (..),
    TxRef (..),
    XPubBal (..),
    zeroBalance,
  )
import Test.Hspec
import Test.QuickCheck

spec :: Spec
spec = do
  describe "streamThings" $ do
    it "streams every item in order when paging by offset" $
      property $ \(NonNegative n) (Positive page) ->
        let xs = [1 .. n :: Int]
            (ys, _) = paged (fromIntegral (page `mod` 50 + 1 :: Int)) xs Nothing
         in ys === xs
    it "reads a number of rows in proportion to the rows taken" $
      property $ \(NonNegative n) ->
        let xs = [1 .. 100000 :: Int]
            (ys, reads) = paged 16 xs (Just n)
         in ys === take n xs .&&. counterexample (show reads) (reads <= 4 * n + 2048)
  describe "mergeDescGroups" $ do
    it "pages like sorting the first offset plus limit items of every stream" $
      property $ \(Streams ss) ->
        forAll (choose (0, 8)) $ \l ->
          forAll (choose (0, 8)) $ \o ->
            let limits = Limits l o Nothing
             in merged limits ss === reference limits ss
    it "stops reading once the page is full" $
      -- an undefined item after the first key of each stream would throw
      let ss = [[(9, 1), (8, 2)] <> undefined, [(9, 3), (7, 4)] <> undefined]
          limits = Limits 2 0 Nothing
       in merged limits ss `shouldBe` [(9, 3), (9, 1)]

  describe "scanXPubBals" $ do
    it "keeps exactly the gap after the last used address of each chain, and the initial block" $
      property $ \(Wallet igap gap ext chg) ->
        let paths = map (.path) (scan igap gap ext chg)
         in paths === modelPaths igap gap ext chg
    it "reads no address past the gap, beyond the initial block" $
      property $ \(Wallet igap gap ext chg) ->
        let seen = execState (scanXPubBals (countReads ext chg) igap gap (chain 0) (chain 1)) []
            limit m used
              | m == 0 = max (fromIntegral igap - 1) (lastReached igap gap used + fromIntegral gap)
              | otherwise = lastReached 0 gap used + fromIntegral gap
            ok (m, n) = n <= limit m (if m == 0 then ext else chg)
         in counterexample (show seen) (all ok seen)
    it "reads only the initial block of an unused xpub" $
      property $ \(NonNegative igap) (Positive gap) ->
        let seen = execState (scanXPubBals (countReads [] []) (igap `mod` 40) (gap `mod` 40) (chain 0) (chain 1)) []
         in sortOn id seen === [(0, n) | n <- [0 .. fromIntegral (igap `mod` 40) - 1]]
    it "counts the unused addresses at the end of the initial block" $
      -- index 0 used, initial gap 20, gap 32: the store looks at indices 1 to
      -- 32 and no further, while it used to read up to index 83
      let paths = map (.path) (scan 20 32 [0] [])
       in maximum [n | [0, n] <- paths] `shouldBe` 32
    it "does not see a payment 34 addresses past the last used one" $ do
      -- the wallet of the Blockchain.com comparison: receive indices 0 to 35
      -- used, then a payment to index 69
      let ext = [0 .. 35] <> [69]
          seen g = [n | XPubBal {path = [0, n], balance = b} <- scan 20 g ext [], b.txs > 0]
      maximum (seen 32) `shouldBe` 35
      maximum (seen 34) `shouldBe` 69
  describe "joinDescStreams with txRefKey" $ do
    it "merges without duplicates mempool transactions of the same second" $
      property $ \(TxStreams all' ss) ->
        let merged' = runConduitPure $ joinDescStreams txRefKey (map yieldMany ss) .| sinkList
            want = sortOn (Down . txRefKey) (nub (concat ss))
         in counterexample (show (length all')) (merged' === want)

-- | Addresses used on each chain, with the initial gap and the gap.
data Wallet = Wallet Word32 Word32 [Int] [Int]
  deriving (Show)

instance Arbitrary Wallet where
  arbitrary = do
    igap <- choose (0, 30)
    gap <- choose (1, 40)
    let used = fmap (IntSet.toList . IntSet.fromList) . listOf $ frequency [(4, choose (0, 40)), (1, choose (0, 150))]
    Wallet igap gap <$> used <*> used

-- | A fake address for a path.
fakeAddr :: Word32 -> Word32 -> Address
fakeAddr m n = PubKeyAddress (ripemd160 (C8.pack (show (m, n))))

chain :: Word32 -> [(Word32, Address)]
chain m = [(n, fakeAddr m n) | n <- [0 ..]]

-- | Balances of fake addresses: the used ones have one transaction.
balancesOf :: [Int] -> [Int] -> [Address] -> [Balance]
balancesOf ext chg = map f
  where
    used = [fakeAddr 0 (fromIntegral n) | n <- ext] <> [fakeAddr 1 (fromIntegral n) | n <- chg]
    f a
      | a `elem` used = (zeroBalance a) {txs = 1, received = 1}
      | otherwise = zeroBalance a

scan :: Word32 -> Word32 -> [Int] -> [Int] -> [XPubBal]
scan igap gap ext chg =
  evalState (scanXPubBals (return . balancesOf ext chg) igap gap (chain 0) (chain 1)) ()

-- | Read balances, recording the paths read.
countReads :: [Int] -> [Int] -> [Address] -> State [(Word32, Int)] [Balance]
countReads ext chg as = do
  let paths = [(m, fromIntegral n) | a <- as, m <- [0, 1], n <- [0 .. 400], fakeAddr m n == a]
  modify' (paths <>)
  return (balancesOf ext chg as)

-- | Last used index that a wallet following the gap reaches, or -1.
lastReached :: Word32 -> Word32 -> [Int] -> Int
lastReached igap gap used = go start
  where
    start = maximum ((-1) : filter (< fromIntegral igap) used)
    go l = case filter (\u -> u > l && u <= l + fromIntegral gap) used of
      [] -> l
      us -> go (maximum us)

-- | Paths that an exact gap gives: up to the gap after the last used address
-- of each chain, and the whole initial block of the external chain.
modelPaths :: Word32 -> Word32 -> [Int] -> [Int] -> [[Word32]]
modelPaths igap gap ext chg
  | not (any (< fromIntegral igap) ext) = [[0, fromIntegral n] | n <- [0 .. fromIntegral igap - 1 :: Int]]
  | otherwise =
      upto 0 (max (fromIntegral igap - 1) (lastReached igap gap ext + fromIntegral gap))
        <> upto 1 (lastReached 0 gap chg + fromIntegral gap)
  where
    upto m l = [[m, fromIntegral n] | n <- [0 .. l]]

-- | Transaction references and streams of subsets of them, each sorted as the
-- database sorts the transactions of an address, with many mempool
-- transactions that arrived in the same second.
data TxStreams = TxStreams [TxRef] [[TxRef]]
  deriving (Show)

instance Arbitrary TxStreams where
  arbitrary = do
    n <- choose (0, 40)
    refs <- mapM ref [1 .. n]
    k <- choose (1, 5)
    ss <- vectorOf k (sublistOf refs)
    return $ TxStreams refs (map (sortOn (Down . txRefKey)) ss)
    where
      ref :: Int -> Gen TxRef
      ref i = do
        b <- oneof [MemRef <$> choose (1, 3), BlockRef <$> choose (1, 3) <*> pure (fromIntegral i)]
        return TxRef {block = b, txid = TxHash (sha256 (encode i))}

-- | The paging of xpub unspent outputs before 'mergeDescGroups': the first
-- offset plus limit items of each stream, all sorted, then the page.
reference :: Limits -> [[(Int, Int)]] -> [(Int, Int)]
reference limits =
  applyLimits limits . sortOn Down . concatMap (applyLimit (deOffset limits).limit)

merged :: Limits -> [[(Int, Int)]] -> [(Int, Int)]
merged limits ss =
  runConduitPure $
    mergeDescGroups fst id quota (map yieldMany ss)
      .| applyLimitsC limits
      .| sinkList
  where
    quota = case (deOffset limits).limit of
      0 -> Nothing
      q -> Just (q :: Word32)

-- | Streams of (key, item) whose keys never increase, with many equal keys
-- and the items of each key in any order, like the unspent outputs of the
-- addresses of an xpub: keyed by block, in key order within a block.
-- Items are unique across all streams, as outputs are.
newtype Streams = Streams [[(Int, Int)]]
  deriving (Show)

instance Arbitrary Streams where
  arbitrary = do
    n <- choose (0, 6)
    ss <- vectorOf n $ do
      keys <- sortOn Down <$> listOf (choose (0, 5))
      items <- shuffle [0 .. length keys - 1]
      return (zip keys items)
    -- number the items so that they are unique across streams
    return . Streams $
      zipWith (\i -> map (\(k, x) -> (k, x * 10 + i))) [0 ..] ss

-- | Stream a list through 'streamThings' paging by offset, taking at most
-- the given number of items. Also gives the rows that the store read,
-- counting those it skipped to reach the offset of each page.
paged :: Word32 -> [Int] -> Maybe Int -> ([Int], Int)
paged page xs n =
  flip runState 0 . runConduit $
    streamThings getit Nothing (Limits page 0 Nothing)
      .| maybe sinkList (\k -> takeC k .| sinkList) n
  where
    getit :: Limits -> State Int [Int]
    getit l = do
      let ys = applyLimits l xs
      modify' (+ (fromIntegral l.offset + length ys))
      return ys
