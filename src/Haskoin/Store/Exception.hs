-- |
-- Module      : Haskoin.Store.Exception
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The few functions about exceptions that the store needs and that
-- "Control.Exception" does not have: handlers for any error that let a
-- thread be stopped, and cleanups that cannot be cut short. It is an
-- internal module, for the store, its tests and its tools.
--
-- = Why
--
-- A thread of the store is stopped from outside all the time: a request
-- that takes too long gets a 'System.Timeout.timeout', a loop that runs
-- under 'Control.Concurrent.Async.withAsync' is cancelled when its owner is
-- done, the web server kills the thread of a client that went away. Each of
-- those is an exception thrown to the thread, an /asynchronous/ one, and
-- two things in "Control.Exception" get in its way:
--
-- * A handler for 'SomeException' catches it too. A loop that logs every
--   failure and goes on (@forever (action \`catch\` log)@) then logs its own
--   cancellation and goes on, and never stops; a request that falls back to
--   an empty answer on any error answers its own timeout that way.
--   'catchAny', 'handleAny' and 'tryAny' catch every exception but those,
--   which they let through.
--
-- * The cleanup of 'Control.Exception.bracket' and
--   'Control.Exception.finally' runs with asynchronous exceptions masked,
--   but a second one still gets in where the cleanup waits for something,
--   and the rest of the cleanup is then skipped: a snapshot of the database
--   is not released, an iterator stays open, a gauge is left one too
--   high for good. 'bracket', 'bracket_', 'finally' and 'onException' here
--   run the cleanup under 'uninterruptibleMask_', so that it runs to its
--   end. And if the cleanup fails as well, the exception of the body is the
--   one thrown, since it is the one that says what went wrong.
--
-- = When to use which
--
-- Use these everywhere in the store in place of the functions of the same
-- name of "Control.Exception", and import the rest from there: 'throwIO',
-- 'Control.Exception.mask_', 'Control.Exception.evaluate' and so on.
-- Catching one type of synchronous error, such as an @ImportException@,
-- needs nothing from here: 'Control.Exception.catch',
-- 'Control.Exception.handle' and 'Control.Exception.try' do it right.
--
-- A cleanup under these functions must be short and sure to end, since
-- nothing can stop it: release, decrement, unsubscribe, or cancel a thread
-- that can be cancelled. A cleanup that may wait for long belongs in
-- "Control.Exception"'s functions, or better, should not wait.
--
-- = Where they come from
--
-- The names and the behaviour are those of the @unliftio@ library, which
-- the store used until 1.15.1 and whose functions run in any monad that
-- can be run in 'IO'. The store runs in 'IO' alone, so it keeps these few
-- functions for 'IO' and not the library. Writing 'uninterruptibleMask_'
-- around every cleanup, and a test of the type of exception in every
-- handler, was the alternative: something to forget at every use, where
-- this is one place to get it right.
--
-- What tells an asynchronous exception from another is its type, not how
-- it was thrown: it is one that is made a 'SomeException' through
-- t'SomeAsyncException', as 'Control.Exception.AsyncException',
-- 'Control.Concurrent.Async.AsyncCancelled' and the exception of
-- 'System.Timeout.timeout' are. So do not throw an ordinary exception to
-- another thread with 'Control.Exception.throwTo': handlers there take it
-- for an error of the code that they guard.
module Haskoin.Store.Exception
  ( -- * Handlers for any error
    catchAny,
    handleAny,
    tryAny,
    isSyncException,

    -- * Cleanups that run to their end
    bracket,
    bracket_,
    finally,
    onException,
  )
where

import Control.Exception
  ( SomeAsyncException (..),
    SomeException,
    fromException,
    mask,
    throwIO,
    uninterruptibleMask,
    uninterruptibleMask_,
  )
import Control.Exception qualified as E

-- | Whether an exception is an error of the code that threw it, and not one
-- thrown to a thread to stop it: whether it is not a t'SomeAsyncException'.
isSyncException :: SomeException -> Bool
isSyncException e = case fromException e of
  Just (SomeAsyncException _) -> False
  Nothing -> True

-- | Run an action, and a handler if it throws any exception but an
-- asynchronous one, which goes on to the caller as if there were no
-- handler. The handler runs with asynchronous exceptions masked, as that
-- of 'Control.Exception.catch' does.
catchAny :: IO a -> (SomeException -> IO a) -> IO a
catchAny action handler =
  action `E.catch` \e ->
    if isSyncException e
      then handler e
      else throwIO e

-- | 'catchAny' with the handler first.
handleAny :: (SomeException -> IO a) -> IO a -> IO a
handleAny = flip catchAny

-- | Run an action and give its result, or the exception that it threw
-- unless that is an asynchronous one, which is thrown on.
tryAny :: IO a -> IO (Either SomeException a)
tryAny action = (Right <$> action) `catchAny` (return . Left)

-- | Get a resource, use it, and release it however the use ends. Getting
-- it runs with asynchronous exceptions masked, and releasing it with them
-- masked uninterruptibly, so that a resource that was got is released and
-- the release is not cut short. If both the use and the release throw, the
-- exception of the use is the one thrown.
bracket :: IO a -> (a -> IO b) -> (a -> IO c) -> IO c
bracket acquire release use = mask $ \restore -> do
  x <- acquire
  E.try (restore (use x)) >>= \case
    Left (e :: SomeException) -> do
      -- the release may fail too, but the failure of the use is the one
      -- that says what went wrong
      _ :: Either SomeException b <- E.try (uninterruptibleMask_ (release x))
      throwIO e
    Right y -> do
      _ <- uninterruptibleMask_ (release x)
      return y

-- | 'bracket' for a resource that is not a value: something to do before
-- an action and something to undo after it, however it ends.
bracket_ :: IO a -> IO b -> IO c -> IO c
bracket_ before after action = bracket before (const after) (const action)

-- | Run an action, then a cleanup, however the action ends. The cleanup
-- runs with asynchronous exceptions masked uninterruptibly. If both throw,
-- the exception of the action is the one thrown.
finally :: IO a -> IO b -> IO a
finally action cleanup = uninterruptibleMask $ \restore ->
  E.try (restore action) >>= \case
    Left (e :: SomeException) -> do
      _ :: Either SomeException b <- E.try cleanup
      throwIO e
    Right x -> do
      _ <- cleanup
      return x

-- | Run an action, and a cleanup if it throws any exception, asynchronous
-- ones included, before the exception is thrown on. The cleanup runs with
-- asynchronous exceptions masked uninterruptibly, and an exception of its
-- own is dropped.
onException :: IO a -> IO b -> IO a
onException action cleanup = uninterruptibleMask $ \restore ->
  E.try (restore action) >>= \case
    Left (e :: SomeException) -> do
      _ :: Either SomeException b <- E.try cleanup
      throwIO e
    Right x -> return x
