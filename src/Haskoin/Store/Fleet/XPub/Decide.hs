-- |
-- Module      : Haskoin.Store.Fleet.XPub.Decide
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Every change to the cache of extended public keys of a fleet, as
-- decisions of "Haskoin.Store.Fleet.Atomic": building an entry, moving
-- the chain's layer of the entries with the tip, in pieces, and dropping
-- and evicting entries. "Haskoin.Store.Fleet.XPub" says what an entry is
-- and why, and calls these.
--
-- = Why it is here and not in a script
--
-- These were commands of a Lua script, beside the reads of the cache. A
-- build writes thousands of rows for a large key, and a piece of a move
-- as many: a script that failed among them left an entry half written,
-- with addresses that its numbers did not count or rows of a block that
-- the tip had left, and Redis undoes nothing. As decisions, each is
-- carried out whole or not at all ("Haskoin.Store.Fleet.Atomic" says how),
-- and a fault in one is an exception in Haskell before Redis is told
-- anything.
--
-- The reads stay a script (@lua\/xpub.lua@). They change nothing, but for
-- noting that a key was used, and they are what every request for a key
-- costs: one call.
--
-- = A move that went quiet
--
-- A store that begins a move of the tip and then writes nothing for ten
-- seconds has its move ended by whoever comes across it here: the entries
-- that its pieces wrote are dropped, to be built again (@moving@). The
-- reads do not do that, as they once did: an entry that such a move marked
-- is a miss to them, and the build that follows a miss ends the move.
--
-- = How it is written
--
-- As the script was, a function for each of its functions, in its order
-- of reading and writing, so that the two can be compared line by line
-- and, in the tests, state by state against a Redis that runs the script.
module Haskoin.Store.Fleet.XPub.Decide
  ( Keys (..),
    cacheKeys,
    Addition (..),
    Piece (..),
    Row,
    Began (..),
    Step (..),
    build,
    begin,
    piece,
    abort,
    drop,
    evict,
  )
where

import Control.Monad (foldM, forM_, unless, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.Maybe (fromMaybe, isJust)
import Data.Set qualified as Set
import Haskoin.Store.Fleet.Atomic
import Prelude hiding (drop)

-- | The keys of the cache and of what it reads beside itself, under the
-- prefix of a fleet. A key that ends in a colon is the start of a key for
-- each address or entry.
data Keys = Keys
  { -- | the fleet tip
    fleet :: !ByteString,
    -- | how many times the tip moved
    moves :: !ByteString,
    -- | the version that the last change of an entry got
    xver :: !ByteString,
    -- | the place of the tip that the cache is at: a move and its block
    place :: !ByteString,
    -- | the entries by their priority, lowest first to go
    prio :: !ByteString,
    -- | the priority of the last entry that was evicted
    level :: !ByteString,
    -- | how many addresses the entries have between them
    addrs :: !ByteString,
    -- | the entries that have each address
    watchers :: !ByteString,
    -- | the start of the keys of each entry
    entry :: !ByteString,
    -- | the move of the tip under way: its token and when it last wrote
    moving :: !ByteString,
    -- | the entries that the move under way wrote
    marked :: !ByteString,
    -- | the transactions of the pool at each address, the outputs that it
    -- pays to each, and what it keeps for each output
    atx :: !ByteString,
    aout :: !ByteString,
    aval :: !ByteString
  }

-- | The keys under a prefix.
cacheKeys :: ByteString -> Keys
cacheKeys p =
  Keys
    { fleet = p <> "fleet:tip",
      moves = p <> "fleet:moves",
      xver = p <> "fleet:xver",
      place = p <> "fx:tip",
      prio = p <> "fx:prio",
      level = p <> "fx:level",
      addrs = p <> "fx:addrs",
      watchers = p <> "fx:of:",
      entry = p <> "fx:e:",
      moving = p <> "fx:moving",
      marked = p <> "fx:marked",
      atx = p <> "pool:atx:",
      aout = p <> "pool:aout:",
      aval = p <> "pool:aval:"
    }

-- | An output in a list of an entry: the output, its place in the list,
-- its amount in digits and its script.
type Row = (ByteString, Score, ByteString, ByteString)

-- | An address to add to an entry, with what the chain has for it.
data Addition = Addition
  { address :: !ByteString,
    -- | five bytes: the chain of the key that the address is in, and its
    -- number there
    path :: !ByteString,
    -- | its balance in the chain, or nothing
    balance :: !ByteString,
    -- | its transactions in the chain, each with its place in the list
    txs :: ![(ByteString, Score)],
    -- | its unspent outputs in the chain
    outs :: ![Row]
  }

-- | What a move of the tip does to the chain's layer of an address.
data Piece = Piece
  { address :: !ByteString,
    -- | its balance in the chain at the new block, or nothing
    balance :: !ByteString,
    -- | the transactions of the abandoned blocks
    abandoned :: ![ByteString],
    -- | those of the new blocks, each with its place in the list
    added :: ![(ByteString, Score)],
    -- | the outputs that are no longer unspent in the chain
    gone :: ![ByteString],
    -- | those that are unspent in the chain at the new block
    unspent :: ![Row]
  }

-- | What became of an attempt to begin a move.
data Began
  = -- | the move is under way
    BeganMove
  | -- | another store is moving the tip
    OtherMoving
  | -- | the fleet tip is not the one that the store saw
    TipElsewhere
  deriving (Eq, Show)

-- | What became of one write of a build.
data Step
  = -- | the addresses are in the entry
    StepAdded
  | -- | the fleet tip is another block
    StepRaced
  | -- | the cache is at another place of the tip
    StepBehind
  | -- | the entry does not have the numbers of addresses that the store
    -- found
    StepChanged
  | -- | a move of the tip is under way
    StepBusy
  deriving (Eq, Show)

num :: (Show a) => a -> ByteString
num = C.pack . show

-- | A number that Redis keeps as text, or 0.
number :: Maybe ByteString -> Integer
number = maybe 0 (maybe 0 fst . C.readInteger)

-- | What an entry keeps for an output: its amount, the length of its
-- address, the address and the script.
keptFor :: ByteString -> ByteString -> ByteString -> ByteString
keptFor a amount script = amount <> " " <> num (B.length a) <> " " <> a <> script

-- | The keys of an entry start with this.
entryOf :: Keys -> ByteString -> ByteString
entryOf k i = k.entry <> i <> ":"

-- | Delete the entry of a key.
drop :: (Monad m) => Keys -> ByteString -> Atomic m ()
drop k i = do
  let e = entryOf k i
  had <- Set.toList <$> hkeys (e <> "addrs")
  forM_ had $ \a -> write (SRem (k.watchers <> a) i)
  unless (null had) (write (IncrBy k.addrs (negate (toInteger (length had)))))
  forM_ ["addrs", "meta", "txs", "outs", "oval"] $ \part -> write (Del (e <> part))
  write (ZRem k.prio i)

-- | End a move of the tip that did not finish: what its pieces wrote is
-- dropped, entry by entry.
ended :: (Monad m) => Keys -> Atomic m ()
ended k = do
  ids <- smembers k.marked
  forM_ (Set.toList ids) (drop k)
  write (Del k.marked)
  write (Del k.moving)

-- | The token of the move of the tip under way, if there is one. A move
-- that wrote nothing for ten seconds is ended here, and there is none
-- then.
moving :: (Monad m) => Keys -> Atomic m (Maybe ByteString)
moving k =
  get k.moving >>= \case
    Nothing -> return Nothing
    Just v -> do
      let (token, wrote) = B.breakSubstring " " v
      now <- clock
      if now - number (Just (B.drop 1 wrote)) > 10000
        then ended k >> return Nothing
        else return (Just token)

-- | A use of an entry: its priority is the level plus what it cost
-- (GreedyDual).
used :: (Monad m) => Keys -> ByteString -> Integer -> Atomic m ()
used k i cost = do
  lowest <- number <$> get k.level
  write (ZAdd k.prio (fromIntegral (lowest + cost)) i)

-- | Delete the entries of the lowest priority past so many, raising the
-- level to that of each: how many were deleted.
evict :: (Monad m) => Keys -> Int -> Atomic m Integer
evict k most = do
  n <- zcard k.prio
  if n <= toInteger most
    then return 0
    else do
      lowest <- zrangeByScore k.prio MinusInf PlusInf (Just (fromInteger n - most))
      start <- fromIntegral . number <$> get k.level
      let go bar (i, p) = do
            when (p > bar) (write (Set k.level (num (round p :: Integer))))
            drop k i
            return (max bar p)
      _ <- foldM go (start :: Score) lowest
      return (toInteger (length lowest))

-- | End the move of a token, if it is the one under way: for a store
-- whose commit was refused.
abort :: (Monad m) => Keys -> ByteString -> Atomic m ()
abort k token = do
  under <- moving k
  when (under == Just token) (ended k)

-- | Begin a move of the tip under a token, from the tip that the store
-- saw. Builds wait until it is done, so that every entry is one that the
-- move brings along or one built after it.
begin :: (Monad m) => Keys -> ByteString -> ByteString -> Atomic m Began
begin k token saw = do
  at <- fromMaybe "" <$> get k.fleet
  if at /= saw
    then return TipElsewhere
    else do
      under <- moving k
      if isJust under && under /= Just token
        then return OtherMoving
        else do
          now <- clock
          write (Set k.moving (token <> " " <> num now))
          return BeganMove

-- | Write what a move of the tip does at addresses into every entry that
-- has one of them, and mark those entries: whether it was written, which
-- it is not if the tip is another block or the move another.
--
-- What is taken out of an entry is only what has a score of the chain, at
-- or above zero, so that the pool's layer stays as it is.
piece :: (Monad m) => Keys -> ByteString -> ByteString -> [Piece] -> Atomic m Bool
piece k token saw pieces = do
  at <- fromMaybe "" <$> get k.fleet
  under <- if at /= saw then return Nothing else moving k
  if at /= saw || under /= Just token
    then return False
    else do
      now <- clock
      write (Set k.moving (token <> " " <> num now))
      version <- (+ 1) . number <$> get k.xver
      write (IncrBy k.xver 1)
      forM_ pieces $ \p -> do
        ids <- smembers (k.watchers <> p.address)
        forM_ (Set.toList ids) $ \i -> do
          let e = entryOf k i
          write (SAdd k.marked i)
          write (HSet (e <> "meta") "ver" (num version))
          row <- hget (e <> "addrs") p.address
          forM_ row $ \r -> write (HSet (e <> "addrs") p.address (B.take 5 r <> p.balance))
          forM_ p.abandoned $ \t -> do
            ours <- chained (e <> "txs") t
            when ours (write (ZRem (e <> "txs") t))
          forM_ p.added $ \(t, s) -> write (ZAdd (e <> "txs") s t)
          forM_ p.gone $ \o -> do
            ours <- chained (e <> "outs") o
            when ours $ do
              write (ZRem (e <> "outs") o)
              write (HDel (e <> "oval") o)
          forM_ p.unspent $ \(o, s, amount, script) -> do
            write (ZAdd (e <> "outs") s o)
            write (HSet (e <> "oval") o (keptFor p.address amount script))
      return True
  where
    chained z x = maybe False (>= 0) <$> zscore z x

-- | Add addresses to the entry of a key, with what the chain has for each
-- and what the pool has for it now, which the pool keeps up from here on.
--
-- Given are the tip that the store answers from, the key, the numbers of
-- addresses that the entry is to have before, in each of the two chains
-- of the key, what the addresses cost to read, and the addresses. It is
-- for a request that missed, and so a use of the key, at the cost so far.
--
-- A cache with nothing in it starts at the place of the tip, here.
build :: (Monad m) => Keys -> ByteString -> ByteString -> (Integer, Integer) -> Integer -> [Addition] -> Atomic m Step
build k tip i expected cost additions = do
  at <- fromMaybe "" <$> get k.fleet
  if at /= tip
    then return StepRaced
    else do
      under <- moving k
      if isJust under
        then return StepBusy
        else do
          count <- number <$> get k.moves
          let here = num count <> "@" <> at
          place <- get k.place
          when (place == Nothing) (write (Set k.place here))
          if maybe False (/= here) place
            then return StepBehind
            else do
              n0 <- number <$> hget meta "n0"
              n1 <- number <$> hget meta "n1"
              if (n0, n1) /= expected
                then return StepChanged
                else do
                  spent <- (+ cost) . number <$> hget meta "cost"
                  write (HIncrBy meta "cost" cost)
                  used k i spent
                  (m0, m1) <- foldM add (n0, n1) additions
                  version <- (+ 1) . number <$> get k.xver
                  write (IncrBy k.xver 1)
                  write (HSet meta "n0" (num m0))
                  write (HSet meta "n1" (num m1))
                  write (HSet meta "ver" (num version))
                  write (IncrBy k.addrs (toInteger (length additions)))
                  return StepAdded
  where
    e = entryOf k i
    meta = e <> "meta"
    add (n0, n1) a = do
      write (HSet (e <> "addrs") a.address (a.path <> a.balance))
      write (SAdd (k.watchers <> a.address) i)
      forM_ a.txs $ \(t, s) -> write (ZAdd (e <> "txs") s t)
      forM_ a.outs $ \(o, s, amount, script) -> do
        write (ZAdd (e <> "outs") s o)
        write (HSet (e <> "oval") o (keptFor a.address amount script))
      -- what the pool has for the address
      theirs <- zrangeByScore (k.atx <> a.address) MinusInf PlusInf Nothing
      forM_ theirs $ \(t, s) -> write (ZAdd (e <> "txs") s t)
      paid <- zrangeByScore (k.aout <> a.address) MinusInf PlusInf Nothing
      forM_ paid $ \(o, s) -> do
        value <- hget (k.aval <> a.address) o
        forM_ value $ \v -> do
          let (amount, script) = B.breakSubstring " " v
          write (ZAdd (e <> "outs") s o)
          write (HSet (e <> "oval") o (keptFor a.address amount (B.drop 1 script)))
      return (if B.take 1 a.path == "\0" then (n0 + 1, n1) else (n0, n1 + 1))
