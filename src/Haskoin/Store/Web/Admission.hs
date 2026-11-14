{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedRecordDot #-}

-- |
-- Module      : Haskoin.Store.Web.Admission
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- A cap on the long requests that are answered at once, so that a server
-- with more work than it can do stays quick at what is quick.
--
-- The web server answers each request in a thread of its own, and GHC gives
-- every thread a turn, with no priorities. A server that is asked for more
-- than its cores can do therefore slows down for everyone at once: each
-- request takes longer, so more of them are in flight, so each gets fewer
-- turns. Measured on a store of mainnet with 56 connections asking for long
-- lists, on a machine with no core to spare, @\/health@, which only reads
-- memory, took over a second, as did a path that does not exist. Nothing was
-- wrong with either: they waited for their turn behind everything else.
--
-- Here only so many long requests are answered at once ('slots'): those
-- for a list, for all of a block, for an extended public key. The next ones
-- wait in line, in the order that they came, and are answered as slots come
-- free. The line has a length ('room') and a wait has a limit ('patience'):
-- a request that finds the line full, or that waits that long, is refused,
-- and its client is told to try again, which costs the server nearly
-- nothing. The work in flight stays within what the cores can do, a request
-- that is let in is answered at the speed of a server that is not
-- overloaded, and a load balancer hears at once that this server is full.
--
-- = What is capped
--
-- Each route has a cost ('Haskoin.Store.Web.Router.Cost'), and only the
-- long ones take a slot. Those that answer from memory, like the health
-- check, and those that look up one thing by its key, like a transaction or
-- a balance, are answered at once: they are what tells an operator, or a
-- load balancer, how the server is, and what most clients ask for most.
--
-- The first version of this capped everything but the routes that answer
-- from memory, with two slots a core. Measured on stores of mainnet and
-- testnet under the load above, the health check went from 1.65 seconds
-- to 0.03 at the 95th percentile, and the server answered a third of the
-- requests that it had: a transaction asked for by its id waited in the
-- same line as a block of twenty seconds, and eight requests in flight
-- left the machine idle, since most of the time of a request goes in
-- RocksDB, on a thread of its own, often waiting for the disk.
--
-- = Alternatives
--
-- * /Priorities/ would let the short requests pass the long ones. GHC's
--   scheduler has none, and a queue of our own in front of it would be this
--   module with more than one line.
--
-- * /A limit in the proxy/, on connections or on the rate of requests,
--   protects the server from one client and belongs there too. It does not
--   know what a request costs, nor how many cores the server has.
--
-- * /Refusing at once/, with no line, turns every burst into errors that a
--   wait of a few milliseconds would have absorbed.
--
-- = A slot is held until the response is sent
--
-- A long response is read and encoded while it is sent
-- ("Haskoin.Store.Web.Pieces"), so the work of a request ends with its last
-- byte, and that is when its slot comes free. The price is that a client
-- that reads slowly holds a slot while it does. A proxy in front that
-- buffers responses, as nginx does by default, takes them at the speed of
-- the server; a server that faces slow clients by itself should have more
-- slots than cores.
module Haskoin.Store.Web.Admission
  ( Admission,
    newAdmission,
    admitted,
    Load (..),
    load,
  )
where

import Control.Concurrent.STM
  ( STM,
    TVar,
    atomically,
    check,
    modifyTVar',
    newTVarIO,
    orElse,
    readTVar,
    registerDelay,
    retry,
    stateTVar,
    writeTVar,
  )
import Control.Exception (finally, mask, onException)
import Data.Sequence (Seq, ViewL (..), (|>))
import Data.Sequence qualified as Seq

-- | The requests being answered and those that wait, and the limits of both.
data Admission = Admission
  { -- | requests answered at once; none or fewer is no cap
    slots :: !Int,
    -- | requests that may wait for a slot
    room :: !Int,
    -- | microseconds that a request waits before it is refused
    patience :: !Int,
    running :: !(TVar Int),
    -- | the tickets of the requests that wait, first come first
    line :: !(TVar (Seq Int)),
    ticket :: !(TVar Int),
    turned :: !(TVar Int)
  }

-- | A cap of so many requests at once, with so many more waiting for at most
-- so many microseconds. No slots, or fewer, is no cap at all.
newAdmission :: Int -> Int -> Int -> IO Admission
newAdmission slots room patience =
  Admission slots (max 0 room) (max 0 patience)
    <$> newTVarIO 0
    <*> newTVarIO Seq.empty
    <*> newTVarIO 0
    <*> newTVarIO 0

-- | What a request is told when it asks for a slot.
data Entry = Enter | Wait !Int | Full

-- | Answer a request in a slot, waiting in line for one if need be; or, if
-- the line is full or the wait too long, do the first action instead, which
-- should cost little: it runs without a slot. The slot is given back when
-- the second action ends, however it ends, and a request that is stopped
-- while it waits leaves the line.
admitted :: Admission -> IO a -> IO a -> IO a
admitted a refused action
  | a.slots <= 0 = action
  | otherwise = mask $ \restore -> do
      entered <-
        atomically entry >>= \case
          Enter -> return True
          Full -> return False
          Wait n -> restore (await n) `onException` atomically (leave n)
      if entered
        then restore action `finally` atomically (modifyTVar' a.running (subtract 1))
        else atomically (modifyTVar' a.turned (+ 1)) >> restore refused
  where
    entry :: STM Entry
    entry = do
      busy <- readTVar a.running
      waiting <- readTVar a.line
      if
        | busy < a.slots && Seq.null waiting -> Enter <$ writeTVar a.running (busy + 1)
        | Seq.length waiting >= a.room -> return Full
        | otherwise -> do
            n <- stateTVar a.ticket (\n -> (n, n + 1))
            Wait n <$ writeTVar a.line (waiting |> n)
    -- wait to be first in line with a slot free, or for the limit of the wait
    await n = do
      late <- registerDelay a.patience
      atomically $ turn n `orElse` (readTVar late >>= check >> leave n >> return False)
    turn n = do
      busy <- readTVar a.running
      waiting <- readTVar a.line
      case Seq.viewl waiting of
        first :< rest | first == n && busy < a.slots -> do
          writeTVar a.running (busy + 1)
          writeTVar a.line rest
          return True
        _ -> retry
    leave n = modifyTVar' a.line (Seq.filter (/= n))

-- | How full the server is.
data Load = Load
  { -- | requests being answered in a slot
    busy :: !Int,
    -- | requests waiting for a slot
    waiting :: !Int,
    -- | requests refused since the start
    refusals :: !Int
  }
  deriving stock (Show, Eq)

-- | How full the server is now.
load :: Admission -> IO Load
load a = atomically $ Load <$> readTVar a.running <*> (Seq.length <$> readTVar a.line) <*> readTVar a.turned
