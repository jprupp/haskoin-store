{-# LANGUAGE OverloadedRecordDot #-}

-- | The cap on the requests answered at once: how many run, who waits, in
-- what order, and who is refused.
module Haskoin.Store.Web.AdmissionSpec (spec) where

import Control.Concurrent (threadDelay)
import UnliftIO.Async (async, cancel, forConcurrently, wait)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (ErrorCall (..), throwIO, try)
import Control.Monad (forM, replicateM)
import Data.IORef (atomicModifyIORef', modifyIORef', newIORef, readIORef)
import Haskoin.Store.Web.Admission
import Test.Hspec

spec :: Spec
spec = describe "the cap on the requests answered at once" $ do
  it "runs no more than its slots at once, and everything in the end" $ do
    a <- newAdmission 3 100 5000000
    now <- newIORef (0 :: Int)
    most <- newIORef (0 :: Int)
    done <- forConcurrently [1 :: Int .. 40] $ \_ ->
      admitted a (return False) $ do
        n <- atomicModifyIORef' now (\n -> (n + 1, n + 1))
        modifyIORef' most (max n)
        threadDelay 2000
        atomicModifyIORef' now (\m -> (m - 1, ()))
        return True
    done `shouldSatisfy` and
    readIORef most `shouldReturn` 3
    load a `shouldReturn` Load {busy = 0, waiting = 0, refusals = 0}

  it "answers those that wait in the order that they came" $ do
    a <- newAdmission 1 10 5000000
    gate <- newEmptyMVar
    order <- newIORef []
    first <- async (admitted a (return ()) (takeMVar gate))
    settle a 1 0
    rest <- forM [1 :: Int .. 5] $ \i -> do
      t <- async (admitted a (return ()) (modifyIORef' order (<> [i])))
      settle a 1 i
      return t
    putMVar gate ()
    mapM_ wait (first : rest)
    readIORef order `shouldReturn` [1 .. 5]

  it "refuses a request that finds the line full, without making it wait" $ do
    a <- newAdmission 1 2 5000000
    gate <- newEmptyMVar
    held <- async (admitted a (return "refused") (("answered" :: String) <$ takeMVar gate))
    settle a 1 0
    queued <- replicateM 2 (async (admitted a (return "refused") (return "answered")))
    settle a 1 2
    admitted a (return "refused") (return "answered") `shouldReturn` "refused"
    (.refusals) <$> load a `shouldReturn` 1
    putMVar gate ()
    mapM wait (held : queued) `shouldReturn` replicate 3 "answered"

  it "refuses a request that has waited too long, and lets the next ones in" $ do
    a <- newAdmission 1 5 20000
    gate <- newEmptyMVar
    held <- async (admitted a (return False) (True <$ takeMVar gate))
    settle a 1 0
    admitted a (return False) (return True) `shouldReturn` False
    load a `shouldReturn` Load {busy = 1, waiting = 0, refusals = 1}
    putMVar gate ()
    wait held `shouldReturn` True
    admitted a (return False) (return True) `shouldReturn` True

  it "gives the slot back when the request fails" $ do
    a <- newAdmission 1 0 1000
    r <- try (admitted a (return ()) (throwIO (ErrorCall "broken")))
    r `shouldBe` Left (ErrorCall "broken")
    admitted a (return False) (return True) `shouldReturn` True

  it "takes a request that is stopped while it waits out of the line" $ do
    a <- newAdmission 1 5 5000000
    gate <- newEmptyMVar
    held <- async (admitted a (return ()) (takeMVar gate))
    settle a 1 0
    stopped <- async (admitted a (return ()) (return ()))
    settle a 1 1
    behind <- async (admitted a (return False) (return True))
    settle a 1 2
    cancel stopped
    settle a 1 1
    putMVar gate ()
    wait held
    -- the one behind it is not stuck after a ticket that nobody holds
    wait behind `shouldReturn` True

  it "is no cap at all without slots" $ do
    a <- newAdmission 0 0 0
    rs <- forConcurrently [1 :: Int .. 20] $ \_ -> admitted a (return False) (True <$ threadDelay 1000)
    rs `shouldSatisfy` and

-- | Wait until so many requests run and so many wait.
settle :: Admission -> Int -> Int -> IO ()
settle a busy waiting = go (2000 :: Int)
  where
    go 0 = expectationFailure "the requests did not settle" >> return ()
    go n = do
      l <- load a
      if l.busy == busy && l.waiting == waiting then return () else threadDelay 500 >> go (n - 1)

