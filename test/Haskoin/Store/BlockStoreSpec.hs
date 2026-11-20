-- | Which peers the block store asks for their mempools.
module Haskoin.Store.BlockStoreSpec (spec) where

import Haskoin (bch, btc)
import Haskoin.Store.BlockStore (asksMempool)
import Network.Socket (SockAddr (..), tupleToHostAddress)
import Test.Hspec

spec :: Spec
spec = describe "Asking for a mempool" $ do
  let local = SockAddrInet 8335 (tupleToHostAddress (127, 0, 0, 1))
      seed = SockAddrInet 8333 (tupleToHostAddress (192, 0, 2, 7))
  it "asks every peer on Bitcoin Cash, whose nodes answer anyone" $
    map (asksMempool bch []) [local, seed] `shouldBe` [True, True]
  it "asks only the peers given explicitly on Bitcoin, whose nodes disconnect the others" $ do
    map (asksMempool btc [local]) [local, seed] `shouldBe` [True, False]
    asksMempool btc [] local `shouldBe` False
