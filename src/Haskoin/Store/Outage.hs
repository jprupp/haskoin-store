-- |
-- Module      : Haskoin.Store.Outage
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Saying once that something the store depends on stopped working, and
-- once that it works again.
--
-- The store goes on without Redis: it answers from its database and tries
-- again. Several loops try every second or two, and each used to log every
-- failure, so an outage of an hour was thousands of lines that said the
-- same, and nothing said when it was over. Here a failure is logged as a
-- warning when it is the first after things worked, for debugging while
-- they keep failing, and the first success after that is logged too.
--
-- The loops that depend on the same thing share one t'Outage', so that the
-- outage is told once and not once by each. If one of them fails for a
-- reason of its own while the others work, the log alternates between the
-- two lines, which is as loud as such a fault should be.
--
-- Logging at most one line a minute was the alternative. It needs no
-- signal of success, but it never says that the outage ended, and it
-- hides a second, different failure that comes within the minute.
module Haskoin.Store.Outage
  ( Outage,
    newOutage,
    attempt,
    attempting,
  )
where

import Control.Monad (when)
import Data.Maybe (isJust)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Haskoin.Node (LogLevel (..))
import UnliftIO (TVar, atomically, newTVarIO, swapTVar, tryAny)

-- | Whether the last attempt at something failed.
newtype Outage = Outage (TVar Bool)

-- | Nothing has failed yet.
newOutage :: IO Outage
newOutage = Outage <$> newTVarIO False

-- | Do something that may fail because a service is away, and say whether
-- it worked. A failure is logged with a function, as a warning if the
-- attempt before it worked and for debugging otherwise; a success after a
-- failure is logged as information. The text says what is attempted, to
-- follow "Could not" and "Can again": @"take a turn in the fleet"@.
--
-- Exceptions that are thrown to the thread from outside, as when it is
-- stopped, are not caught.
attempt :: Outage -> (LogLevel -> Text -> IO ()) -> Text -> IO () -> IO Bool
attempt outage note doing action = isJust <$> attempting outage note doing action

-- | 'attempt' for an action with a result, which is 'Nothing' when the
-- action failed.
attempting :: Outage -> (LogLevel -> Text -> IO ()) -> Text -> IO a -> IO (Maybe a)
attempting (Outage down) note doing action =
  tryAny action >>= \case
    Right x -> do
      was <- atomically (swapTVar down False)
      when was (note LevelInfo ("Can " <> doing <> " again"))
      return (Just x)
    Left e -> do
      was <- atomically (swapTVar down True)
      if was
        then note LevelDebug ("Could not " <> doing <> ": " <> cs (show e))
        else
          note LevelWarn . mconcat $
            [ "Could not ",
              doing,
              ": ",
              cs (show e),
              ". Going on without; this is said again only for debugging, until it works"
            ]
      return Nothing
