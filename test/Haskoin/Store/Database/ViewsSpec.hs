module Haskoin.Store.Database.ViewsSpec (spec) where

import Data.List (sort)
import Haskoin (BlockHash, btcRegTest)
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Views
import Haskoin.Store.Histories
import Haskoin.Store.Integration.Temp (withSystemTempDirectory)
import Test.Hspec

spec :: Spec
spec = describe "views of the database by block" $ do
  let withChain go =
        withSystemTempDirectory "haskoin-store-views-" $ \dir ->
          openLayered btcRegTest dir $ \db -> do
            startChain db
            views <- newViews db
            let best = Q.getBestBlock db >>= maybe (fail "no best block") return
                next = do
                  b <- mine db []
                  takeView views b
                  return b
            go db views best next `shouldReturn` ()
            closeViews views

  it "answer from the block that each was taken at, whatever came after" $
    withChain $ \db views best next -> do
      first <- best
      takeView views first
      second <- next
      third <- next
      sort <$> heldViews views `shouldReturn` sort [first, second, third]
      Q.getBestBlock db `shouldReturn` Just third
      withViewAt views first Q.getBestBlock `shouldReturn` Just (Just first)
      withViewAt views second Q.getBestBlock `shouldReturn` Just (Just second)
      -- the second block was not there yet when the first view was taken
      (fmap (fmap (const ())) <$> withViewAt views first (`Q.getBlock` second)) `shouldReturn` Just Nothing

  it "let go of the views that are not to be kept" $
    withChain $ \_ views best next -> do
      first <- best
      takeView views first
      second <- next
      keepViews views [second]
      heldViews views `shouldReturn` [second]
      withViewAt views first Q.getBestBlock `shouldReturn` (Nothing :: Maybe (Maybe BlockHash))
      withViewAt views second Q.getBestBlock `shouldReturn` Just (Just second)

  it "keep a view that is being read until its reader is done" $
    withChain $ \_ views best next -> do
      first <- best
      takeView views first
      second <- next
      result <- withViewAt views first $ \old -> do
        keepViews views [second]
        -- gone for anyone who asks now
        heldViews views `shouldReturn` [second]
        withViewAt views first Q.getBestBlock `shouldReturn` (Nothing :: Maybe (Maybe BlockHash))
        -- and still there for the one that was reading, while blocks go on
        third <- next
        (,third) <$> Q.getBestBlock old
      fmap fst result `shouldBe` Just (Just first)
      -- the reader is done: the view is gone, and the others are as they were
      sort <$> heldViews views `shouldReturn` sort (second : maybe [] (pure . snd) result)
