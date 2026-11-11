module Haskoin.Store.OutageSpec (spec) where

import Control.Exception (ErrorCall (..), throwIO)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Text qualified as T
import Haskoin.Node (LogLevel (..))
import Haskoin.Store.Outage
import Test.Hspec

spec :: Spec
spec = describe "outage" $ do
  it "says once that it started and once that it is over" $ do
    said <- newIORef []
    outage <- newOutage
    let note level text = modifyIORef' said ((level, text) :)
        try' action = attempt outage note "reach the service" action
        works = return ()
        fails = throwIO (ErrorCall "no route")
    mapM try' [works, fails, fails, fails, works, works] `shouldReturn` [True, False, False, False, True, True]
    heard <- reverse <$> readIORef said
    map fst heard `shouldBe` [LevelWarn, LevelDebug, LevelDebug, LevelInfo]
    -- what failed and why, and what works again
    snd (head heard) `shouldSatisfy` T.isPrefixOf "Could not reach the service: no route"
    snd (last heard) `shouldBe` "Can reach the service again"
  it "warns again at the next outage" $ do
    said <- newIORef []
    outage <- newOutage
    let note level _ = modifyIORef' said (level :)
        fails = throwIO (ErrorCall "down") :: IO ()
    mapM_ (attempt outage note "reach the service") [fails, return (), fails]
    (reverse <$> readIORef said) `shouldReturn` [LevelWarn, LevelInfo, LevelWarn]
  it "gives the result of what worked, and nothing for what did not" $ do
    outage <- newOutage
    let quiet _ _ = return ()
    attempting outage quiet "count" (return (3 :: Int)) `shouldReturn` Just 3
    attempting outage quiet "count" (throwIO (ErrorCall "no") :: IO Int) `shouldReturn` Nothing
