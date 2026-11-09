-- | Mempool transactions on their way into the database.
module Haskoin.Store.BlockStore.PendingSpec (spec) where

import Data.ByteString (ByteString)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet qualified as HashSet
import Data.List (elemIndex)
import Data.Time.Calendar (fromGregorian)
import Data.Time.Clock (UTCTime (..), addUTCTime)
import Haskoin
import Haskoin.Store.BlockStore.Pending
import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

t0 :: UTCTime
t0 = UTCTime (fromGregorian 2026 11 9) 0

-- | Transactions where each one spends an output from outside, or an output
-- of an earlier one: parents come first.
family :: Gen [Tx]
family = do
  n <- choose (1, 12)
  go n []
  where
    go 0 acc = return (reverse acc)
    go k acc = do
      parents <- sublistOf (map txHash acc)
      let outside = OutPoint (TxHash (sha256 ("outside" :: ByteString))) (fromIntegral k)
          ins = [TxIn op "" maxBound | op <- outside : [OutPoint h 0 | h <- parents]]
          tx = Tx 2 ins [TxOut 1000 "", TxOut 1000 ""] [] (fromIntegral k)
      go (k - 1 :: Int) (tx : acc)

-- | The parents of a transaction among others.
parentsIn :: [Tx] -> Tx -> [TxHash]
parentsIn txs tx = [op.hash | i <- tx.inputs, let op = i.outpoint, op.hash `elem` map txHash txs]

-- | Receive transactions, each missing those of its parents that are not
-- received before it.
receiveAll :: [Tx] -> Pending -> Pending
receiveAll txs p0 = foldl (\p tx -> received t0 tx (HashSet.fromList (parentsIn txs tx)) p) p0 txs

spec :: Spec
spec = describe "Pending transactions" $ do
  prop "takes only transactions with no missing parents, parents first" $
    forAll family $ \txs -> forAll (sublistOf txs) $ \done -> forAll (choose (0, 20)) $ \n ->
      let p = foldr (imported . txHash) (receiveAll txs empty) done
          (taken, rest) = takeReady n p
          order = map (txHash . (.tx)) taken
       in conjoin
            [ property (length taken <= n),
              property (all (null . (.deps)) taken),
              conjoin
                [ counterexample "a child before its parent" $
                    maybe True (> i) (elemIndex h order)
                | (i, t) <- zip [0 ..] taken,
                  h <- [txHash c | c <- txs, txHash t.tx `elem` parentsIn txs c]
                ],
              queued rest === queued p - length taken
            ]

  prop "makes a child ready once all its parents are in the database" $
    forAll family $ \txs ->
      let p = foldr (imported . txHash) (receiveAll txs empty) txs
       in fst (takeReady (length txs) p) `shouldMatchList'` txs

  prop "does not request again what is requested or received" $
    forAll family $ \txs ->
      let hs = map txHash txs
          (asked, got) = splitAt (length hs `div` 2) txs
          p = receiveAll got (requested t0 (map txHash asked) empty)
       in all (`isPending` p) hs .&&. size p === length hs

  prop "forgets requests after twenty seconds, and transactions after an hour" $
    forAll family $ \txs ->
      let hs = map txHash txs
          asked = requested t0 hs empty
          got = receiveAll txs empty
       in conjoin
            [ property (all (`isPending` prune (addUTCTime 20 t0) asked) hs),
              property (not (any (`isPending` prune (addUTCTime 21 t0) asked) hs)),
              queued (prune (addUTCTime 3600 t0) got) === length txs,
              queued (prune (addUTCTime 3601 t0) got) === 0
            ]

  prop "drops what reached the database and updates what still waits" $
    forAll family $ \txs ->
      let p = receiveAll txs empty
          (gone, still) = splitAt (length txs `div` 2) (map txHash txs)
          p' = refresh (HashMap.fromList ([(h, Nothing) | h <- gone] <> [(h, Just HashSet.empty) | h <- still])) p
       in queued p' === length still .&&. null (waiting p')
  where
    shouldMatchList' taken txs =
      HashSet.fromList (map (txHash . (.tx)) taken) === HashSet.fromList (map txHash txs)
