-- | The transactions that the block store requested from peers.
module Haskoin.Store.BlockStore.PendingSpec (spec) where

import Data.ByteString.Char8 qualified as C8
import Data.Time.Calendar (fromGregorian)
import Data.Time.Clock (UTCTime (..), addUTCTime)
import Haskoin
import Haskoin.Store.BlockStore.Pending
import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

t0 :: UTCTime
t0 = UTCTime (fromGregorian 2026 11 9) 0

-- | Distinct transaction ids.
hashes :: Gen [TxHash]
hashes = do
  n <- choose (1, 12 :: Int)
  return [TxHash (sha256 (C8.pack (show i))) | i <- [1 .. n]]

spec :: Spec
spec = describe "Pending requests" $ do
  prop "does not request again what is requested" $
    forAll hashes $ \hs ->
      let p = requested t0 hs empty
       in all (`isPending` p) hs .&&. size p === length hs

  prop "forgets a request once its transaction arrives" $
    forAll hashes $ \hs ->
      let (got, still) = splitAt (length hs `div` 2) hs
          p = foldr answered (requested t0 hs empty) got
       in not (any (`isPending` p) got) .&&. all (`isPending` p) still .&&. size p === length still

  prop "forgets requests after twenty seconds" $
    forAll hashes $ \hs ->
      let asked = requested t0 hs empty
       in conjoin
            [ property (all (`isPending` prune (addUTCTime 20 t0) asked) hs),
              property (not (any (`isPending` prune (addUTCTime 21 t0) asked) hs))
            ]
