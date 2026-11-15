-- | The handlers and cleanups of "Haskoin.Store.Exception": what stops a
-- thread from outside is not taken for an error, and a cleanup is not cut
-- short.
module Haskoin.Store.ExceptionSpec (spec) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (AsyncCancelled (..), async, cancel, waitCatch)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (ErrorCall (..), fromException, throwIO, try)
import Control.Monad (forM_, void)
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Haskoin.Store.Exception
import System.Timeout (timeout)
import Test.Hspec

spec :: Spec
spec = describe "exceptions" $ do
  describe "a handler for any error" $ do
    it "catches the errors of the code that it guards" $ do
      (broken `catchAny` \e -> return (show e)) `shouldReturn` "broken"
      handleAny (\e -> return (show e)) broken `shouldReturn` "broken"
      (either show id <$> tryAny broken) `shouldReturn` "broken"
      (either show id <$> tryAny (return "fine")) `shouldReturn` "fine"

    it "does not take a cancellation for an error" $
      forM_ guards $ \(name, guard) -> do
        started <- newEmptyMVar
        survived <- newIORef False
        a <- async $ do
          guard (putMVar started () >> threadDelay 10000000)
          writeIORef survived True
        takeMVar started
        cancel a
        ended <- waitCatch a
        (name, either fromException (const Nothing) ended) `shouldBe` (name, Just AsyncCancelled)
        (name,) <$> readIORef survived `shouldReturn` (name, False)

    it "does not take a timeout for an error" $
      forM_ guards $ \(name, guard) -> do
        caught <- newIORef (0 :: Int)
        -- what a loop that goes on after any error does, as those of the
        -- store do: were the timeout caught, this would return
        let step = guard (threadDelay 10000000) >> modifyIORef' caught (+ 1)
        (name,) <$> timeout 20000 step `shouldReturn` (name, Nothing)
        (name,) <$> readIORef caught `shouldReturn` (name, 0)

  describe "a cleanup" $ do
    it "runs to its end when the thread is stopped during it" $
      forM_ cleanups $ \(name, with) -> do
        cleaning <- newEmptyMVar
        finished <- newIORef False
        -- the cleanup waits, which is where an exception would get in
        let cleanup = putMVar cleaning () >> threadDelay 100000 >> writeIORef finished True
        a <- async (void (try @ErrorCall (with cleanup)))
        takeMVar cleaning
        -- returns when the thread has ended
        cancel a
        (name,) <$> readIORef finished `shouldReturn` (name, True)

    it "runs when the thread is stopped during the action" $ do
      let stopped with = do
            started <- newEmptyMVar
            cleaned <- newIORef False
            a <- async (with (putMVar started () >> threadDelay 10000000) (writeIORef cleaned True))
            takeMVar started
            cancel a
            ended <- waitCatch a
            either fromException (const Nothing) ended `shouldBe` Just AsyncCancelled
            readIORef cleaned `shouldReturn` True
      stopped (\action cleanup -> bracket (return ()) (const cleanup) (const action))
      stopped (\action cleanup -> bracket_ (return ()) cleanup action)
      stopped finally
      stopped onException

    it "runs after an action that worked, unless it is only for failures" $ do
      ran <- newIORef (0 :: Int)
      let cleanup = modifyIORef' ran (+ 1)
      bracket (return 'a') (const cleanup) return `shouldReturn` 'a'
      bracket_ (return ()) cleanup (return 'b') `shouldReturn` 'b'
      (return 'c' `finally` cleanup) `shouldReturn` 'c'
      readIORef ran `shouldReturn` 3
      (return 'd' `onException` cleanup) `shouldReturn` 'd'
      readIORef ran `shouldReturn` 3

    it "leaves the error of the action to be thrown when it fails too" $ do
      let failing = throwIO (ErrorCall "cleanup") :: IO ()
      try (bracket (return ()) (const failing) (const broken)) `shouldReturn` Left (ErrorCall "broken")
      try (bracket_ (return ()) failing broken) `shouldReturn` Left (ErrorCall "broken")
      try (broken `finally` failing) `shouldReturn` Left (ErrorCall "broken")
      try (broken `onException` failing) `shouldReturn` Left (ErrorCall "broken")
      -- with nothing else to say, its own error is thrown
      try (return () `finally` failing) `shouldReturn` Left (ErrorCall "cleanup")
      try (bracket_ (return ()) failing (return ())) `shouldReturn` Left (ErrorCall "cleanup")
  where
    broken = throwIO (ErrorCall "broken") :: IO String
    -- each way of going on after any error
    guards :: [(String, IO () -> IO ())]
    guards =
      [ ("catchAny", \action -> action `catchAny` \_ -> return ()),
        ("handleAny", handleAny (\_ -> return ())),
        ("tryAny", void . tryAny)
      ]
    -- each way of running a cleanup, reached without the thread being
    -- stopped: after an action that worked, or one that failed
    cleanups :: [(String, IO () -> IO ())]
    cleanups =
      [ ("bracket", \cleanup -> bracket (return ()) (const cleanup) return),
        ("bracket_", \cleanup -> bracket_ (return ()) cleanup (return ())),
        ("finally", \cleanup -> return () `finally` cleanup),
        ("onException", \cleanup -> void broken `onException` cleanup)
      ]
