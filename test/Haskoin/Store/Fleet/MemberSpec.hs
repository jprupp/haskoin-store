module Haskoin.Store.Fleet.MemberSpec (spec) where

import Data.ByteString (ByteString)
import Data.List (sort, sortOn)
import Data.Map.Strict qualified as Map
import Data.Serialize (encode)
import Data.Word (Word64)
import Haskoin
  ( OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    sha256,
    txHash,
  )
import Haskoin.Store.Fleet.Member (offerOrder)
import Haskoin.Store.TestBlocks (script)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck (Gen, choose, elements, forAll, vectorOf, (===))

-- | Transactions that wait to be offered, each with the time at which the
-- store heard of it: some spend an output of one before them, the others a
-- coin of the chain. Times are drawn from a few seconds, so that many are
-- the same, as for a burst of transactions, and a child comes no sooner
-- than its parent.
waiting :: Gen [(Word64, Tx)]
waiting = do
  n <- choose (0, 60)
  go n []
  where
    go 0 made = return (reverse made)
    go k made = do
      parent <- if null made then return Nothing else elements (Nothing : map Just made)
      -- a child comes no sooner than its parent, as a node relays them
      time <- maybe (choose (0, 2)) (\(t, _) -> choose (t, t + 1)) parent
      let i = length made
          coin = OutPoint (TxHash (sha256 (encode i))) 0
          input = maybe coin (\(_, p) -> OutPoint (txHash p) 0) parent
          tx = Tx 1 [TxIn input "" maxBound] [TxOut (fromIntegral (1000 + i)) script] [] 0
      go (k - 1 :: Int) ((time, tx) : made)

-- | The map that the member keeps of what waits.
held :: [(Word64, Tx)] -> Map.Map TxHash (Word64, Tx)
held txs = Map.fromList [(txHash tx, (t, tx)) | (t, tx) <- txs]

spec :: Spec
spec = describe "the order of offers" $ modifyMaxSuccess (const 1000) $ do
  it "offers every transaction that waits, once" $
    forAll waiting $ \txs ->
      sort (map fst (offerOrder (held txs))) === sort (Map.keys (held txs))
  it "offers each after the transactions that it spends from, of those that wait" $
    forAll waiting $ \txs ->
      let order = map fst (offerOrder (held txs))
          place = Map.fromList (zip order [0 :: Int ..])
          late =
            [ (h, i.outpoint.hash)
            | (h, (_, tx)) <- Map.toList (held txs),
              i <- tx.inputs,
              Just p <- [Map.lookup i.outpoint.hash place],
              p > place Map.! h
            ]
       in late === []
  it "offers oldest first what does not spend from another that waits" $
    forAll waiting $ \txs ->
      let roots = [(t, tx) | (t, tx) <- txs, all (\i -> Map.notMember i.outpoint.hash (held txs)) tx.inputs]
          times = [t | (h, (t, _)) <- offerOrder (held txs), h `elem` map (txHash . snd) roots]
       in times === sort times
  it "offers a chain that came in one second in one round, root first" $ do
    let chain = take 24 (iterate child root)
        root = Tx 1 [TxIn (OutPoint (TxHash (sha256 ("coin" :: ByteString))) 0) "" maxBound] [TxOut 1000 script] [] 0
        child p = Tx 1 [TxIn (OutPoint (txHash p) 0) "" maxBound] [TxOut 1000 script] [] 0
        -- in the order of their ids, as the transactions of one second came
        shuffled = sortOn txHash chain
    map fst (offerOrder (held [(5, tx) | tx <- shuffled])) `shouldBe` map txHash chain
