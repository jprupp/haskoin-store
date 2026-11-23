-- | The pool after a move of the tip, in the worlds of
-- "Haskoin.Store.Fleet.LeavingModel": every pool, every block and every
-- order of taking out. @scripts\/check-pool.sh@ goes through the same
-- worlds without building the store.
--
-- Each world is a group, and so a process of its own
-- (@haskell-dev.toml@).
module Haskoin.Store.Fleet.LeavingSpec (spec) where

import Control.Monad (forM_)
import Data.Maybe (isJust)
import Haskoin.Store.Fleet.LeavingModel
import Test.Hspec

spec :: Spec
spec = do
  forM_ (zip [0 :: Int ..] worlds) $ \(i, w) ->
    describe ("The pool after a move of the tip, world " <> show i <> ": " <> w.name) $
      it "has writers wait, every read exact and every order end the same" $
        (explore w).failure `shouldBe` Nothing
  describe "The pool after a move of the tip, with what a block is in conflict with left unmarked" $
    it "fails, so that the checker is known to see a fault" $
      spoilt.failure `shouldSatisfy` isJust
