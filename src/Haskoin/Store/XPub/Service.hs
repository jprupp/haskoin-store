-- |
-- Module      : Haskoin.Store.XPub.Service
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The door to the cache of xpubs of the fleet ("Haskoin.Store.Fleet.XPub"),
-- for the web server (@docs\/xpub-indexing.md@). Every answer about an xpub
-- comes from the cache. A miss asks the indexer, which one store of the
-- fleet runs for the xpub, holding its dibs in Redis with the estimate of
-- when it will be done: a job expected past the wait is refused at once,
-- with the same @Retry-After@ on every store; one expected within it is
-- waited for, until the entry is announced or the wait runs out (429).
module Haskoin.Store.XPub.Service
  ( XPubConfig (..),
    defaultXPubConfig,
    Backend (..),
    XPubs (..),
    Session (..),
    unpinned,
    xpubAsk,
    xpubDelete,
    Answer (..),
    Refusal (..),
    retryAfter,
  )
where

import Data.Maybe (fromMaybe)
import GHC.Clock (getMonotonicTimeNSec)
import Haskoin.Store.Data (XPubSpec)
import Haskoin.Store.XPub (XPubReads (..))
import Haskoin.Store.XPub.Indexer (Answer (..), Refusal (..), Settings (..), defaultSettings, retryAfter)

-- | The settings of the indexer of xpubs.
newtype XPubConfig = XPubConfig {indexer :: Settings}
  deriving (Eq, Show)

defaultXPubConfig :: XPubConfig
defaultXPubConfig = XPubConfig {indexer = defaultSettings}

-- | The cache, as the door sees it.
data Backend = Backend
  { -- | ask its indexer for an xpub, waiting so many microseconds at most
    ask :: XPubSpec -> Int -> IO Answer,
    -- | wait at most so many microseconds for the job of another store
    await :: XPubSpec -> Int -> IO Bool,
    -- | in how many microseconds the job of an xpub is expected to be done,
    -- from the one estimate in its lock
    eta :: XPubSpec -> IO (Maybe Int),
    delete :: XPubSpec -> IO Bool
  }

-- | The door: the cache of the fleet, and how long a request waits. There
-- is always a cache behind it: a store has no way to answer about an xpub
-- but from the cache, and no setting turns it off; its size is all that
-- is set.
data XPubs = XPubs
  { backend :: !Backend,
    patience :: !Int
  }

-- | The reads of the cache for one request, each xpub at one version, and
-- what to run once the request has its answer.
data Session = Session
  { reads :: XPubReads,
    settle :: IO ()
  }

-- | No reads: every read is a miss. Outside of a request, and for a store
-- without Redis, which answers about no xpub.
unpinned :: Session
unpinned =
  Session
    { reads =
        XPubReads
          { balances = \_ -> return Nothing,
            transactions = \_ _ -> return Nothing,
            count = \_ -> return Nothing,
            unspents = \_ _ _ -> return Nothing,
            bodies = return . map (const Nothing),
            wallet = return
          },
      settle = return ()
    }

xpubDelete :: XPubs -> XPubSpec -> IO Bool
xpubDelete xs x = xs.backend.delete x

-- | Ask for an xpub that the cache does not have: refused at once if the
-- one estimate in its lock is past the wait, else queued or joined, and
-- waited for; a job of another store is waited for through the cache.
xpubAsk :: XPubs -> XPubSpec -> IO Answer
xpubAsk xs x = do
  start <- clockUs
  b.eta x >>= \case
    Just t | t > xs.patience -> return (Refused TooLong t)
    _ ->
      b.ask x xs.patience >>= \case
        Refused OtherStore t -> awaitOther xs b x start (start + t)
        Refused WaitedOut t -> Refused WaitedOut . fromMaybe t <$> b.eta x
        a -> return a
  where
    b = xs.backend

-- | Wait for the job of another store, which holds the dibs; every second
-- without the entry ask again, which takes the dibs if their holder died.
awaitOther :: XPubs -> Backend -> XPubSpec -> Int -> Int -> IO Answer
awaitOther xs b x start due0 = do
  now0 <- clockUs
  if due0 - now0 > xs.patience - (now0 - start)
    then return (Refused OtherStore (due0 - now0))
    else go due0
  where
    go due = do
      now <- clockUs
      let left = xs.patience - (now - start)
      if left <= 0
        then Refused OtherStore . max 1000000 . fromMaybe (due - now) <$> b.eta x
        else
          b.await x (min left 1000000) >>= \case
            True -> return Indexed
            False -> do
              now' <- clockUs
              let left' = xs.patience - (now' - start)
              if left' <= 0
                then go due
                else
                  b.ask x left' >>= \case
                    Refused OtherStore t -> go (now' + t)
                    a -> return a

clockUs :: IO Int
clockUs = (`div` 1000) . fromIntegral <$> getMonotonicTimeNSec
