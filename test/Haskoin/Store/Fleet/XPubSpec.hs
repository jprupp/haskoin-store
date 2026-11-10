module Haskoin.Store.Fleet.XPubSpec (spec) where

import Data.Functor.Identity (Identity (..))
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet qualified as HashSet
import Data.Serialize (encode)
import Data.Word (Word32)
import Haskoin (Address (..), addressHash)
import Haskoin.Store.Common (scanXPubBals)
import Haskoin.Store.Data (Balance (..), BlockRef (..), XPubBal (..), zeroBalance)
import Haskoin.Store.Fleet.XPub
import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

spec :: Spec
spec = describe "cache of extended public keys of a fleet" $ do
  describe "score" $ do
    prop "is undone by position, in the chain" $ \(h, p) ->
      let ref = BlockRef {height = h `mod` 0x08000000, position = p `mod` 0x04000000}
       in position (score ref) === ref
    prop "is undone by position, in the mempool" $ \t ->
      let ref = MemRef {timestamp = fromIntegral (t :: Word32) + 1}
       in position (score ref) === ref
    prop "puts the newest first" $ \(a, b) ->
      let ref (h, p) = BlockRef {height = h `mod` 0x08000000, position = p `mod` 0x04000000}
       in compare (score (ref a)) (score (ref b)) === compare (ref b) (ref a)
    it "puts the mempool before the chain, newest first" $ do
      score (MemRef 20) `shouldSatisfy` (< score (MemRef 10))
      score (MemRef 10) `shouldSatisfy` (< score (BlockRef 500 0))
  describe "window" $ do
    -- which addresses of the two chains of a key are used, and the gaps
    let key = do
          igap <- choose (1, 8)
          gap <- choose (1, 8)
          ext <- HashSet.fromList <$> listOf (choose (0, 30 :: Word32))
          chg <- HashSet.fromList <$> listOf (choose (0, 30 :: Word32))
          return (igap, gap, ext, chg)
        address m n = PubKeyAddress (addressHash (encode (m :: Word32, n :: Word32)))
        balance used m n =
          let zero = zeroBalance (address m n)
           in if HashSet.member n used then zero {txs = 1, received = 5} else zero
        scan (igap, gap, ext, chg) =
          let chain m = [(n, address m n) | n <- [0 ..]]
              known =
                HashMap.fromList $
                  [(address 0 n, balance ext 0 n) | n <- [0 .. 80]]
                    <> [(address 1 n, balance chg 1 n) | n <- [0 .. 80]]
              bal a = HashMap.lookupDefault (zeroBalance a) a known
           in runIdentity (scanXPubBals (return . map bal) igap gap (chain 0) (chain 1))
        rows found = [(m, n, b.balance) | b@XPubBal {path = [m, n]} <- found]
    it "gives the scan when the entry has the addresses that it reads" $
      property . forAll key $ \k@(igap, gap, _, _) ->
        let found = scan k in window igap gap (rows found) === Just found
    it "gives nothing when the entry lacks the last address of a chain" $
      property . forAll key $ \k@(igap, gap, _, _) ->
        let found = scan k
            final m = maximum [n | (m', n, _) <- rows found, m' == m]
            lacking m = [r | r@(m', n, _) <- rows found, m' /= m || n /= final m]
         in conjoin [window igap gap (lacking m) === Nothing | m <- [0, 1], any (\(m', _, _) -> m' == m) (rows found)]
    it "gives nothing when the entry has an address more" $
      property . forAll key $ \k@(igap, gap, ext, chg) ->
        let found = scan k
            next m = length [() | (m', _, _) <- rows found, m' == m]
            more m used = rows found <> [(m, fromIntegral (next m), balance used m (fromIntegral (next m)))]
            -- one more is another key, unless the scan read no change
            -- addresses at all and the one more is the first of them
            changeless = next 1 == 0
         in conjoin
              [ window igap gap (more 0 ext) === Nothing,
                changeless .||. window igap gap (more 1 chg) === Nothing
              ]
    it "gives nothing when a chain has a hole" $
      property . forAll key $ \k@(igap, gap, _, _) ->
        let found = scan k
            holed = [r | r@(m, n, _) <- rows found, (m, n) /= (0, 0)]
         in length found > 1 ==> window igap gap holed === Nothing
