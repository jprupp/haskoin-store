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
-- A limit on the long requests that are answered at once, so that a server
-- with more work than it can do stays quick at what is quick. The limit
-- moves with how busy the cores are ("Haskoin.Store.Web.Congestion"); the
-- requests over it wait in a line that refuses them when it stops moving
-- (CoDel). @docs/congestion-control.md@ explains the design for a reader
-- new to congestion control.
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
-- Here only so many long requests are answered at once: those for a list,
-- for all of a block, for an extended public key. Each holds a slot, and
-- the controller of "Haskoin.Store.Web.Congestion" sets how many slots
-- there are ('regulate'), between a floor and a ceiling. The next requests
-- wait in line, in the order that they came, and are answered as slots come
-- free. A request that the line cannot take is refused, and its client is
-- told to try again, which costs the server nearly nothing. The work in
-- flight stays within what the cores can do, a request that is let in is
-- answered at the speed of a server that is not overloaded, and a load
-- balancer hears at once that this server is full.
--
-- = What is limited
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
-- = The line: CoDel
--
-- A line is good when it absorbs a burst and empties again, and bad when it
-- stands: every request waits, and none is the better for it. CoDel, as
-- folly and Facebook's servers use it, tells the two apart by the shortest
-- wait of the requests let in during an interval of 100 ms. A line that
-- emptied even once in the interval let someone in at once; one whose
-- shortest wait was over 5 ms stood all along, and is /overloaded/ until
-- the end of the next interval. While it is overloaded, a request that
-- finds no free slot is refused at once, and one that has waited more than
-- 10 ms is refused: when its slot comes, which goes to the next, or before,
-- at the next request or sweep ('sweep'). The line drains in a moment, and
-- the clients that it refuses learn so at once, rather than after waiting
-- for nothing.
--
-- While the line is not overloaded, a request waits until it is given a
-- slot, or for at most a limit ('patience'), and a line already holding so
-- many requests ('room') takes no more. Both are bounds for safety, which
-- CoDel should leave untouched.
--
-- A request that waits sets no timer of its own: those that have waited
-- too long are at the front of the line, and are refused there at each
-- request, and ten times a second ('regulate'). Every timer of the program,
-- @threadDelay@ and @registerDelay@ included, goes through one thread of
-- GHC, its timer manager, and with a timer for each request in line, on
-- four cores kept busy, that thread was found blocked for seconds on a
-- thunk that a request had begun to evaluate when it lost its core: no
-- timer of the program fired meanwhile, the health check's included.
--
-- An interval ends at the first request, or sweep, after its end, and one
-- in which nobody was let in counts the wait of the first in line so far:
-- the next one in will have waited at least that long, and a line whose
-- slots are all held by requests of many seconds stands as surely as one
-- that moves slowly.
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
--   wait of a few milliseconds would have absorbed. /A fixed wait/, the
--   five seconds of the first version, keeps every request of a line that
--   stands waiting for its whole length before refusing it.
--
-- = A slot goes from hand to hand
--
-- A request that ends gives its slot to the first in line, who alone is
-- woken, and the slot counts as taken from then on. The first version let
-- the slot come free and had every request in line watch for it: each
-- change woke them all, to find that all but one had to wait again, and a
-- slot that had come free stayed empty until the first in line was given a
-- core. Measured with four slots and sixty requests in line on four cores
-- that forty other threads kept busy, the slots were 0.2 full on average
-- and answered 1,345 requests a second; handing them over, they are always
-- taken and answer 1,457. The gain is small because what a waiting request
-- waits for, once it has its slot, is still its turn on a core, which GHC
-- gives to every thread alike.
--
-- That turn is the price of a limit. With requests that compute for 10 ms,
-- on four cores (@bench/Admission.hs@), a fixed limit of sixteen answered
-- 2% fewer than no limit, and the limit that adapts, at about six slots,
-- 8% fewer: a slot handed to a thread that has yet to run is idle until it
-- does, and fewer slots leave less to fill the gap.
--
-- When the controller cuts the limit, the requests in flight run to their
-- end, and the slots that they leave are not handed on until fewer run than
-- the limit. When it raises the limit, the new slot goes to the first in
-- line at once.
--
-- = A slot is held until the response is sent
--
-- A long response is read and encoded while it is sent
-- ("Haskoin.Store.Web.Pieces"), so the work of a request ends with its last
-- byte, and that is when its slot comes free. The price is that a client
-- that reads slowly holds a slot while it does. A proxy in front that
-- buffers responses, as nginx does by default, takes them at the speed of
-- the server; a server that faces slow clients by itself should have a
-- higher floor.
module Haskoin.Store.Web.Admission
  ( -- * Admission
    Admission,
    Settings (..),
    newAdmission,
    admitted,
    Refusal (..),

    -- * The controller
    regulate,
    Report (..),
    setLimit,

    -- * How full the server is
    Load (..),
    Refusals (..),
    load,

    -- * Time, for tests and tools
    newAdmissionWith,
    sweep,
    interval,
    sojournTarget,
  )
where

import Control.Concurrent.STM
  ( STM,
    TVar,
    atomically,
    modifyTVar',
    newTVar,
    newTVarIO,
    readTVar,
    retry,
    writeTVar,
  )
import Control.Exception (finally, mask, onException)
import Control.Monad (forever, replicateM_, unless, when)
import Data.Sequence (Seq, ViewL (..), (|>))
import Data.Sequence qualified as Seq
import GHC.Clock (getMonotonicTimeNSec)
import Haskoin.Store.Web.Congestion
  ( Control (..),
    Delays (..),
    Observation (..),
    control,
    delays,
    snooze,
    withProbes,
  )

-- | How the long requests are let in.
data Settings = Settings
  { -- | most requests answered at once, the ceiling of the limit; none or
    -- fewer is no admission control at all
    ceiling :: !Int,
    -- | fewest: the floor of the limit, where it starts; at least one,
    -- and at most the ceiling
    floor :: !Int,
    -- | microseconds that a thread ready to run may wait for its turn
    -- before the limit comes down ("Haskoin.Store.Web.Congestion")
    target :: !Int,
    -- | requests that may wait in line
    room :: !Int,
    -- | microseconds that a request may wait while the line is not
    -- overloaded
    patience :: !Int
  }
  deriving stock (Eq, Show)

-- | The requests being answered and those that wait, and the limits of both.
data Admission = Admission
  { settings :: !Settings,
    -- | now, in nanoseconds of a monotonic clock; the tests have one that
    -- they move by hand, to test CoDel without waiting for it
    clock :: !(IO Int),
    -- | requests answered at once, which the controller sets
    limit :: !(TVar Int),
    -- | slots taken: by a request being answered, or given to one that
    -- waited and has yet to run; more than the limit after a cut
    running :: !(TVar Int),
    -- | the requests that wait, first come first
    line :: !(TVar (Seq Waiter)),
    -- | whether every slot was taken, or a request waited or was refused,
    -- since the last step of the controller
    saturated :: !(TVar Bool),
    codel :: !(TVar Codel),
    refusals :: !(TVar Refusals)
  }

-- | A request in line: when it came, and where it is told what became of
-- it.
data Waiter = Waiter
  { arrived :: !Int,
    verdict :: !(TVar Verdict)
  }

-- | What became of a request in line: still waiting, given a slot, refused
-- by CoDel, or refused for waiting too long.
data Verdict = Waiting | Given | Shed | Expired

-- | The state of CoDel.
data Codel = Codel
  { -- | when the interval ends, in nanoseconds
    ends :: !Int,
    -- | the shortest wait of the requests let in during the interval, in
    -- nanoseconds; 'maxBound' while there is none
    least :: !Int,
    -- | whether the line stood during the last interval
    overloaded :: !Bool
  }

-- | Why a request was refused.
data Refusal
  = -- | the line already held as many requests as it may
    LineFull
  | -- | it waited as long as a request may
    WaitedTooLong
  | -- | the line is overloaded (CoDel): it found no free slot, or it had
    -- waited more than twice 'sojournTarget'
    Overloaded
  deriving stock (Eq, Show)

-- | Requests refused since the start, by why.
data Refusals = Refusals
  { full :: !Int,
    waited :: !Int,
    shed :: !Int
  }
  deriving stock (Eq, Show)

-- | Nanoseconds of an interval of CoDel: 100 ms, as in CoDel and folly.
-- It should be about the time that a request takes, which for the long
-- requests of a store is tens to hundreds of milliseconds.
interval :: Int
interval = 100_000_000

-- | Nanoseconds that requests may wait in a line that works: 5 ms, as in
-- CoDel and folly. While the line is overloaded a request that waited
-- twice as long is refused.
sojournTarget :: Int
sojournTarget = 5_000_000

-- | Intervals of CoDel between two steps of the controller: half a second,
-- which holds fifty sleeps of each probe, and a few long requests.
stepIntervals :: Int
stepIntervals = 5

-- | Admission with these settings, on the monotonic clock of the system.
newAdmission :: Settings -> IO Admission
newAdmission = newAdmissionWith (fromIntegral <$> getMonotonicTimeNSec)

-- | Admission with these settings, on this clock, which gives nanoseconds.
-- The limit starts at the floor.
newAdmissionWith :: IO Int -> Settings -> IO Admission
newAdmissionWith clock given = do
  t <- clock
  Admission s clock
    <$> newTVarIO s.floor
    <*> newTVarIO 0
    <*> newTVarIO Seq.empty
    <*> newTVarIO False
    <*> newTVarIO Codel {ends = t + interval, least = maxBound, overloaded = False}
    <*> newTVarIO (Refusals 0 0 0)
  where
    s =
      given
        { floor = max 1 (min given.floor given.ceiling),
          room = max 0 given.room,
          patience = max 0 given.patience
        }

-- | What a request is told when it asks for a slot.
data Entry = Enter | Refuse !Refusal | Wait !Waiter

-- | Answer a request in a slot, waiting in line for one if need be; or, if
-- it is refused, do the first action instead, which should cost little: it
-- runs without a slot. The slot is given back when the second action ends,
-- however it ends, and a request that is stopped while it waits leaves the
-- line.
admitted :: Admission -> (Refusal -> IO a) -> IO a -> IO a
admitted a refused action
  | a.settings.ceiling <= 0 = action
  | otherwise = mask $ \restore -> do
      let answer = restore action `finally` (a.clock >>= atomically . release a)
      t <- a.clock
      atomically (entry a t) >>= \case
        Enter -> answer
        Refuse why -> restore (refused why)
        Wait w ->
          restore (await w) `onException` (a.clock >>= atomically . abandon a w) >>= \case
            Nothing -> answer
            Just why -> restore (refused why)

-- | Ask for a slot at the given time.
entry :: Admission -> Int -> STM Entry
entry a t = do
  tick a t
  n <- readTVar a.limit
  busy <- readTVar a.running
  -- nobody waits unless every slot is taken: 'release' and 'setLimit'
  -- see to it
  if busy < n
    then do
      writeTVar a.running (busy + 1)
      when (busy + 1 >= n) (saturate a)
      seen a 0
      return Enter
    else do
      saturate a
      c <- readTVar a.codel
      waiting <- readTVar a.line
      if
        | c.overloaded -> Refuse Overloaded <$ refuse a Overloaded
        | Seq.length waiting >= a.settings.room -> Refuse LineFull <$ refuse a LineFull
        | otherwise -> do
            w <- Waiter t <$> newTVar Waiting
            Wait w <$ writeTVar a.line (waiting |> w)

-- | Wait to be given a slot, or to be refused: 'Nothing' once given one.
await :: Waiter -> IO (Maybe Refusal)
await w =
  atomically $
    readTVar w.verdict >>= \case
      Waiting -> retry
      Given -> return Nothing
      Shed -> return (Just Overloaded)
      Expired -> return (Just WaitedTooLong)

-- | Stopped while waiting: out of the line, or, if a slot had just been
-- given, the slot goes to the next.
abandon :: Admission -> Waiter -> Int -> STM ()
abandon a w t =
  readTVar w.verdict >>= \case
    Given -> release a t
    _ -> leave a w

-- | Out of the line.
leave :: Admission -> Waiter -> STM ()
leave a w = modifyTVar' a.line (Seq.filter ((/= w.verdict) . (.verdict)))

-- | A request ends: its slot goes on, unless more run than the limit.
release :: Admission -> Int -> STM ()
release a t = do
  tick a t
  n <- readTVar a.limit
  busy <- readTVar a.running
  if busy > n then writeTVar a.running (busy - 1) else handOff a t

-- | The slot of a request that ends, or a new one, goes to the first in
-- line, who alone is woken; while the line is overloaded, to the first who
-- has not waited too long, the others being refused; or it comes free.
handOff :: Admission -> Int -> STM ()
handOff a t = do
  c <- readTVar a.codel
  (Seq.viewl <$> readTVar a.line) >>= \case
    EmptyL -> modifyTVar' a.running (subtract 1)
    w :< rest -> do
      writeTVar a.line rest
      -- the clock of another thread may have been read a moment after
      -- this one
      let sojourn = max 0 (t - w.arrived)
      seen a sojourn
      if c.overloaded && sojourn > 2 * sojournTarget
        then writeTVar w.verdict Shed >> refuse a Overloaded >> handOff a t
        else writeTVar w.verdict Given

-- | A request was let in after waiting so many nanoseconds.
seen :: Admission -> Int -> STM ()
seen a sojourn = do
  c <- readTVar a.codel
  when (sojourn < c.least) (writeTVar a.codel c {least = sojourn})

-- | End the interval of CoDel if its time has come, and refuse those that
-- have waited too long: for the line, or, while it is overloaded, for
-- CoDel.
tick :: Admission -> Int -> STM ()
tick a t = do
  c <- readTVar a.codel
  when (t >= c.ends) $ do
    shortest <-
      if c.least < maxBound
        then return c.least
        else
          -- nobody was let in: the next one will have waited at least as
          -- long as the first in line has
          readTVar a.line >>= \l -> return $ case Seq.viewl l of
            w :< _ -> max 0 (t - w.arrived)
            EmptyL -> 0
    writeTVar a.codel Codel {ends = t + interval, least = maxBound, overloaded = shortest > sojournTarget}
  readTVar a.codel >>= front . (.overloaded)
  where
    -- the line is in the order of arrival, so those who waited longest
    -- are at its front
    front overloaded =
      (Seq.viewl <$> readTVar a.line) >>= \case
        w :< rest
          | waited w >= a.settings.patience * 1000 -> out w rest Expired WaitedTooLong >> front overloaded
          | overloaded && waited w > 2 * sojournTarget -> do
              seen a (waited w)
              out w rest Shed Overloaded
              front overloaded
        _ -> return ()
    waited w = max 0 (t - w.arrived)
    out w rest verdict why = writeTVar a.line rest >> writeTVar w.verdict verdict >> refuse a why

-- | End the interval of CoDel if its time has come, and refuse those that
-- have waited too long, as every request does, and 'regulate' ten times a
-- second.
sweep :: Admission -> IO ()
sweep a = a.clock >>= atomically . tick a

-- | Every slot is taken, or a request could not have one at once.
saturate :: Admission -> STM ()
saturate a = readTVar a.saturated >>= \s -> unless s (writeTVar a.saturated True)

-- | Count a refusal.
refuse :: Admission -> Refusal -> STM ()
refuse a why = modifyTVar' a.refusals $ \r -> case why of
  LineFull -> r {full = r.full + 1}
  WaitedTooLong -> r {waited = r.waited + 1}
  Overloaded -> r {shed = r.shed + 1}

-- | Set the limit, as the controller does. Slots that it adds go to those
-- in line at once. The limit is kept between the floor and the ceiling.
setLimit :: Admission -> Int -> IO ()
setLimit a n = a.clock >>= atomically . resize a n

-- | Set the limit at the given time.
resize :: Admission -> Int -> Int -> STM ()
resize a n t = do
  tick a t
  old <- readTVar a.limit
  when (m /= old) (writeTVar a.limit m)
  fill
  where
    m = max a.settings.floor (min a.settings.ceiling n)
    fill = do
      busy <- readTVar a.running
      waiting <- readTVar a.line
      when (busy < m && not (Seq.null waiting)) $
        modifyTVar' a.running (+ 1) >> handOff a t >> fill

-- | What a step of the controller did.
data Report = Report
  { -- | the limit after the step
    limit :: !Int,
    -- | the 90th percentile of the probes' lateness during the step,
    -- without the pauses of GHC's collector, in microseconds: what the
    -- controller follows
    delay :: !Int,
    -- | the same with the collector's pauses, in microseconds
    lateness :: !Int,
    -- | whether the line is overloaded
    overloaded :: !Bool
  }
  deriving stock (Eq, Show)

-- | Run the controller and its probes, a step every half second for as long
-- as it runs, giving each t'Report' to the function, and 'sweep' every
-- interval of CoDel in between. It returns at once when admission control
-- is off, and starts no probe.
regulate :: Admission -> (Report -> IO ()) -> IO ()
regulate a report
  | a.settings.ceiling <= 0 = return ()
  | otherwise = withProbes $ \probes -> forever $ do
      replicateM_ (stepIntervals - 1) (snooze (interval `div` 1000) >> sweep a)
      snooze (interval `div` 1000)
      d <- delays probes
      t <- a.clock
      atomically (step d t) >>= report
  where
    settings = Control {floor = a.settings.floor, ceiling = a.settings.ceiling, target = a.settings.target}
    step d t = do
      n <- readTVar a.limit
      busy <- readTVar a.running
      full <- readTVar a.saturated
      writeTVar a.saturated False
      let seenNow = Observation {delay = d.scheduling, saturated = full || busy >= n, draining = busy > n}
          n' = control settings seenNow n
      resize a n' t
      c <- readTVar a.codel
      return Report {limit = n', delay = d.scheduling, lateness = d.raw, overloaded = c.overloaded}

-- | How full the server is.
data Load = Load
  { -- | requests that may be answered at once
    limit :: !Int,
    -- | requests being answered in a slot
    busy :: !Int,
    -- | requests waiting for a slot
    waiting :: !Int,
    -- | whether the line is overloaded (CoDel)
    overloaded :: !Bool,
    -- | requests refused since the start
    refused :: !Refusals
  }
  deriving stock (Eq, Show)

-- | How full the server is now.
load :: Admission -> IO Load
load a = atomically $ do
  c <- readTVar a.codel
  Load
    <$> readTVar a.limit
    <*> readTVar a.running
    <*> (Seq.length <$> readTVar a.line)
    <*> pure c.overloaded
    <*> readTVar a.refusals
