-- |
-- Module      : Haskoin.Store.Fleet.Pool.Decide
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The shared mempool of a fleet as it is kept in Redis, and every change
-- to it, as decisions of "Haskoin.Store.Fleet.Atomic": what a store reads
-- and writes when it offers a transaction, withdraws some, lets the old
-- ones go, or moves the tip.
--
-- "Haskoin.Store.Fleet.Pool" says what the pool is, as a pure value. This
-- module says how that value lies in Redis, with all that the stores read
-- beside it: what the pool does at each address and to each category of
-- tokens, which transactions can be replaced, the events, what the fleet
-- has seen, and the pool's layer of the cache of extended public keys.
--
-- = Why it is here and not in a script
--
-- These rules were a Lua script of eight hundred lines, which Redis ran
-- alone. That made each change one step, and it had two faults. A script
-- that fails after its first write leaves the pool half changed, since
-- Redis undoes nothing; and the rules were kept twice, in Lua to run and
-- in Haskell to test against. Here they are written once, in Haskell, and
-- "Haskoin.Store.Fleet.Atomic" makes each of them one step: the decision
-- is made from what Redis says, and carried out only if Redis still says
-- the same, by a script that decides nothing.
--
-- = How it is written
--
-- As the script was, on purpose: a function for each function of the
-- script, in its order of reading and writing, so that the two can be
-- compared line by line and, in the tests, state by state against a Redis
-- that runs the script. A decision sees what it has written, as a script
-- does, so the order means the same in both.
module Haskoin.Store.Fleet.Pool.Decide
  ( Keys (..),
    poolKeys,
    offer,
    withdraw,
    expire,
    trim,
    How (..),
    advance,
  )
where

import Control.Monad (foldM, forM, forM_, unless, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.List (nub)
import Data.List (sort)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, isJust, isNothing)
import Data.Serialize
  ( Get,
    Put,
    getByteString,
    getWord64be,
    getWord8,
    putByteString,
    putWord64be,
    putWord8,
    runGet,
    runPut,
  )
import Data.Set qualified as Set
import Data.Word (Word32, Word64)
import Haskoin.Store.Fleet.Atomic
import Haskoin.Store.Fleet.Pool.Types

-- | The keys of the pool and of what goes with it, under the prefix of a
-- fleet, and the size of a transaction id in bytes. A key that ends in a
-- colon is the start of a key for each address, category or transaction.
data Keys = Keys
  { prefix :: !ByteString,
    -- | bytes in a transaction id; an output is that and four more
    tsize :: !Int,
    tip :: !ByteString,
    -- | how many times the tip moved
    moves :: !ByteString,
    -- | how many times the pool was emptied
    restarts :: !ByteString,
    -- | until when each store is live
    lease :: !ByteString,
    -- | the recent blocks of each store, newest first
    recent :: !ByteString,
    events :: !ByteString,
    seen :: !ByteString,
    xver :: !ByteString,
    -- | the outputs that each transaction spends, one after the other
    spends :: !ByteString,
    -- | the transaction that spends each output
    spender :: !ByteString,
    body :: !ByteString,
    -- | the transactions of the pool that spend from each transaction
    kids :: !ByteString,
    -- | what each transaction does at addresses and to tokens, as it was
    -- put in, to take it out the same
    entries :: !ByteString,
    -- | every transaction by the time it entered a mempool
    time :: !ByteString,
    ids :: !ByteString,
    -- | those that say themselves that they can be replaced
    own :: !ByteString,
    -- | those that can be replaced
    rbf :: !ByteString,
    atx :: !ByteString,
    aout :: !ByteString,
    aval :: !ByteString,
    aspent :: !ByteString,
    asum :: !ByteString,
    tsum :: !ByteString,
    acat :: !ByteString,
    chold :: !ByteString,
    ctx :: !ByteString,
    cout :: !ByteString,
    cval :: !ByteString,
    csum :: !ByteString,
    -- | what a block replaced: the transaction that replaced it and the
    -- height of the block, with those by the transaction that replaced
    -- them and all by height
    rep :: !ByteString,
    repby :: !ByteString,
    repat :: !ByteString,
    -- | the same for what waits for the transaction that replaced it to
    -- be confirmed
    lost :: !ByteString,
    lostby :: !ByteString,
    lostat :: !ByteString,
    -- | the place of the tip that the cache of extended public keys is at
    xtip :: !ByteString,
    -- | the move of the cache that is under way
    moving :: !ByteString,
    -- | the entries of the cache that the move changed
    marked :: !ByteString,
    -- | the cached keys that have each address
    watchers :: !ByteString,
    -- | the start of the entries of the cache of extended public keys
    entry :: !ByteString
  }

-- | The keys under a prefix.
poolKeys :: ByteString -> Int -> Keys
poolKeys p tsize =
  Keys
    { prefix = p,
      tsize,
      tip = p <> "fleet:tip",
      moves = p <> "fleet:moves",
      restarts = p <> "fleet:restarts",
      lease = p <> "fleet:lease",
      recent = p <> "fleet:recent",
      events = p <> "fleet:events",
      seen = p <> "fleet:seen",
      xver = p <> "fleet:xver",
      spends = p <> "pool:spends",
      spender = p <> "pool:spender",
      body = p <> "pool:body",
      kids = p <> "pool:kids:",
      entries = p <> "pool:entries",
      time = p <> "pool:time",
      ids = p <> "pool:ids",
      own = p <> "pool:own",
      rbf = p <> "pool:rbf",
      atx = p <> "pool:atx:",
      aout = p <> "pool:aout:",
      aval = p <> "pool:aval:",
      aspent = p <> "pool:aspent:",
      asum = p <> "pool:asum:",
      tsum = p <> "pool:tsum:",
      acat = p <> "pool:acat:",
      chold = p <> "pool:chold:",
      ctx = p <> "pool:ctx:",
      cout = p <> "pool:cout:",
      cval = p <> "pool:cval:",
      csum = p <> "pool:csum:",
      rep = p <> "pool:rep",
      repby = p <> "pool:repby:",
      repat = p <> "pool:repat",
      lost = p <> "pool:lost",
      lostby = p <> "pool:lostby:",
      lostat = p <> "pool:lostat",
      xtip = p <> "fx:tip",
      moving = p <> "fx:moving",
      marked = p <> "fx:marked",
      watchers = p <> "fx:of:",
      entry = p <> "fx:e:"
    }

num :: (Show a) => a -> ByteString
num = C.pack . show

-- | The transaction that made an output.
maker :: Keys -> ByteString -> ByteString
maker k = B.take k.tsize

-- | Tell every store of the fleet that something happened: @t@ and a
-- transaction that entered the pool, @d@ and one that left it
-- unconfirmed. The events go in a stream, with the change, so that every
-- store serves the same events in the same order.
emit :: (Monad m) => Keys -> ByteString -> ByteString -> Atomic m ()
emit k kind what = write (XAdd k.events 10000 [("k", kind), ("h", what)])

-- | The fleet has seen a transaction: the pool decided on it. Kept by the
-- time at which it was first seen, on the clock of Redis.
sawIt :: (Monad m) => Keys -> ByteString -> Atomic m ()
sawIt k t = do
  now <- clock
  write (ZAddNX k.seen (fromIntegral now) t)

-- | Whether a transaction is in the pool.
member :: (Monad m) => Keys -> ByteString -> Atomic m Bool
member k = hexists k.spends

-- | The transactions of the pool among these, and all that spend them,
-- each once and none before one that it spends from among them.
descendants :: (Monad m) => Keys -> [ByteString] -> Atomic m [ByteString]
descendants k from = do
  roots <- filterM' (member k) from
  go Set.empty [] (reverse roots)
  where
    go _ order [] = return (reverse order)
    go done order (t : todo)
      | Set.member t done = go done order todo
      | otherwise = do
          children <- smembers (k.kids <> t)
          go (Set.insert t done) (t : order) (Set.toDescList children <> todo)
    filterM' p = fmap concat . mapM (\x -> (\ok -> [x | ok]) <$> p x)

-- | Whether a member of a list of an entry of the cache is in the pool's
-- layer, with a score below zero: what the pool puts in or takes out of an
-- entry is never what the chain's layer has, which a move of the tip
-- writes ahead of the pool.
pooled :: (Monad m) => ByteString -> ByteString -> Atomic m Bool
pooled z x = maybe False (< 0) <$> zscore z x

-- | Add to a field of a summary.
addTo :: (Monad m) => ByteString -> ByteString -> Integer -> Atomic m ()
addTo key field by = write (HIncrBy key field by)

-- | What the pool does to the tokens of an output, by category and by
-- address. An output is @free@ while the pool makes it and nothing in the
-- pool uses it up, and @spent@ while the pool uses it up and the chain
-- made it. An amount is kept in two parts, the digits above the last nine
-- and the last nine, as the stores read it.
tokens :: (Monad m) => Keys -> PoolTokenOut -> ByteString -> Integer -> Atomic m ()
tokens k o which sign = do
  addTo (k.csum <> o.category) which sign
  forM_ o.address $ \a -> do
    let key = k.tsum <> o.category <> a
        (hi, lo) = toInteger o.amount `divMod` 1000000000
    addTo key (which <> "Hi") (sign * hi)
    addTo key (which <> "Lo") (sign * lo)
    addTo key (which <> "Nfts") (sign * (if o.nft then 1 else 0))
    addTo key (which <> "Outputs") sign

-- | Put what a transaction does at addresses and to tokens in, with sign
-- 1, or take it out, with sign -1: the lists and the summary of each
-- address, the lists and counts of each category, and the pool's layer of
-- every cached key that has one of the addresses.
account :: (Monad m) => Keys -> ByteString -> Word64 -> [PoolEntry] -> Integer -> Atomic m ()
account k t at es sign = do
  forM_ es $ \case
    PoolMakes o -> do
      let c = o.category
      if entering
        then do
          write (ZAdd (k.cout <> c) newest o.output)
          write (HSet (k.cval <> c) o.output (maybe "" (\(v, s) -> num v <> " " <> s) o.coins))
        else do
          write (ZRem (k.cout <> c) o.output)
          write (HDel (k.cval <> c) o.output)
      -- an output that the pool uses up was spent from the chain until
      -- the pool made it, and is spent from the chain again once the pool
      -- no longer makes it
      used <- hexists k.spender o.output
      if used then tokens k o "spent" (negate sign) else tokens k o "free" sign
      forM_ o.address $ \a -> do
        let (hi, lo) = toInteger o.amount `divMod` 1000000000
        addTo (k.tsum <> c <> a) "receivedHi" (sign * hi)
        addTo (k.tsum <> c <> a) "receivedLo" (sign * lo)
    PoolUses o -> do
      ours <- member k (maker k o.output)
      if ours then tokens k o "free" (negate sign) else tokens k o "spent" sign
    PoolTouches a -> do
      watching <- Set.toList <$> smembers (k.watchers <> a)
      if entering then write (ZAdd (k.atx <> a) newest t) else write (ZRem (k.atx <> a) t)
      forM_ watching $ \i ->
        if entering
          then write (ZAddNX (k.entry <> i <> ":txs") newest t)
          else do
            mine <- pooled (k.entry <> i <> ":txs") t
            when mine (write (ZRem (k.entry <> i <> ":txs") t))
      addTo (k.asum <> a) "txs" sign
    PoolSpends a o v -> do
      if entering then write (HSet (k.aspent <> a) o (num v)) else write (HDel (k.aspent <> a) o)
      addTo (k.asum <> a) "spent" (sign * toInteger v)
      addTo (k.asum <> a) "spentCount" sign
      paid <- hexists (k.aval <> a) o
      when paid $ do
        addTo (k.asum <> a) "both" (sign * toInteger v)
        addTo (k.asum <> a) "bothCount" sign
    PoolPays a o v s -> do
      watching <- Set.toList <$> smembers (k.watchers <> a)
      if entering
        then do
          write (ZAdd (k.aout <> a) newest o)
          write (HSet (k.aval <> a) o (num v <> " " <> s))
        else do
          write (ZRem (k.aout <> a) o)
          write (HDel (k.aval <> a) o)
      forM_ watching $ \i -> do
        let outs = k.entry <> i <> ":outs"
        if entering
          then do
            there <- isJust <$> zscore outs o
            unless there $ do
              write (ZAdd outs newest o)
              write (HSet (k.entry <> i <> ":oval") o (num v <> " " <> num (B.length a) <> " " <> a <> s))
          else do
            mine <- pooled outs o
            when mine $ do
              write (ZRem outs o)
              write (HDel (k.entry <> i <> ":oval") o)
      addTo (k.asum <> a) "paid" (sign * toInteger v)
      addTo (k.asum <> a) "paidCount" sign
      gone <- hexists (k.aspent <> a) o
      when gone $ do
        addTo (k.asum <> a) "both" (sign * toInteger v)
        addTo (k.asum <> a) "bothCount" sign
  -- every entry with an address that the transaction touches has a new
  -- version, so that a request that read it before reads it again
  watched <- nub . concat <$> forM touched (\a -> Set.toList <$> smembers (k.watchers <> a))
  unless (null watched) $ do
    before <- maybe 0 (maybe 0 fst . C.readInteger) <$> get k.xver
    write (Incr k.xver)
    forM_ watched $ \i -> write (HSet (k.entry <> i <> ":meta") "ver" (num (before + 1)))
  -- the transaction in the lists of the categories that it moves, and in
  -- the count of each address that it moves tokens of
  forM_ cats $ \c -> do
    if entering then write (ZAdd (k.ctx <> c) newest t) else write (ZRem (k.ctx <> c) t)
    addTo (k.csum <> c) "txs" sign
  forM_ held $ \(c, a) -> do
    addTo (k.tsum <> c <> a) "txs" sign
    when entering $ do
      write (SAdd (k.acat <> a) c)
      write (SAdd (k.chold <> c) a)
  -- an address that no transaction touches has no summary, nor has a
  -- category or an address of one that no transaction moves
  unless entering $ do
    forM_ touched $ \a -> do
      left <- hget (k.asum <> a) "txs"
      when (left == Just "0") (write (Del (k.asum <> a)))
    forM_ cats $ \c -> do
      left <- hget (k.csum <> c) "txs"
      when (left == Just "0") (write (Del (k.csum <> c)))
    forM_ held $ \(c, a) -> do
      left <- hget (k.tsum <> c <> a) "txs"
      when (left == Just "0") $ do
        write (Del (k.tsum <> c <> a))
        write (SRem (k.acat <> a) c)
        write (SRem (k.chold <> c) a)
  where
    entering = sign > 0
    newest = negate (fromIntegral at) :: Score
    touched = nub (concatMap addressOf es)
    addressOf = \case
      PoolTouches a -> [a]
      PoolSpends a _ _ -> [a]
      PoolPays a _ _ _ -> [a]
      _ -> []
    moved = concatMap (\case PoolMakes o -> [o]; PoolUses o -> [o]; _ -> []) es
    cats = nub (map (.category) moved)
    held = nub [(o.category, a) | o <- moved, Just a <- [o.address]]

-- | Work out again whether a transaction of the pool can be replaced by
-- fee, which it can if it says so itself or spends from one of the pool
-- that can, and if that changed, the same for what spends it.
replaceable :: (Monad m) => Keys -> ByteString -> Atomic m ()
replaceable k t =
  hget k.spends t >>= \case
    Nothing -> return ()
    Just inputs -> do
      says <- sismember k.own t
      can <- if says then return True else anyM (sismember k.rbf . maker k) (outputs k inputs)
      was <- sismember k.rbf t
      when (can /= was) $ do
        write (if can then SAdd k.rbf t else SRem k.rbf t)
        children <- smembers (k.kids <> t)
        forM_ (Set.toList children) (replaceable k)
  where
    anyM p = foldM (\found x -> if found then return True else p x) False

-- | The outputs that are written one after the other.
outputs :: Keys -> ByteString -> [ByteString]
outputs k s
  | B.null s = []
  | otherwise = let (o, rest) = B.splitAt (k.tsize + 4) s in o : outputs k rest

-- | Take one transaction out, leaving what spends it: quietly if a block
-- confirmed it, which is no event of the mempool.
remove :: (Monad m) => Keys -> Bool -> ByteString -> Atomic m ()
remove k quiet t =
  hget k.spends t >>= \case
    Nothing -> return ()
    Just inputs -> do
      unless quiet $ do
        emit k "d" t
        sawIt k t
      (at, es) <- maybe (0, []) kept <$> hget k.entries t
      account k t at es (-1)
      write (HDel k.entries t)
      write (ZRem k.time t)
      write (ZRem k.ids t)
      forM_ (outputs k inputs) $ \o -> do
        write (HDel k.spender o)
        write (SRem (k.kids <> maker k o) t)
      write (HDel k.spends t)
      write (HDel k.body t)
      write (SRem k.own t)
      could <- sismember k.rbf t
      when could $ do
        write (SRem k.rbf t)
        children <- smembers (k.kids <> t)
        forM_ (Set.toList children) (replaceable k)

-- | Put a transaction in.
insert :: (Monad m) => Keys -> PoolTx -> Atomic m ()
insert k tx = do
  let t = tx.txid
  write (HSet k.spends t (B.concat tx.inputs))
  write (HSet k.body t tx.body)
  forM_ tx.inputs $ \o -> do
    write (HSet k.spender o t)
    write (SAdd (k.kids <> maker k o) t)
  write (HSet k.entries t (keep tx.time tx.entries))
  write (ZAdd k.time (fromIntegral tx.time) t)
  write (ZAdd k.ids 0 t)
  account k t tx.time tx.entries 1
  when tx.signals (write (SAdd k.own t))
  replaceable k t
  emit k "t" t
  sawIt k t

-- | Withdraw transactions and all that spends them: those that left.
withdraw :: (Monad m) => Keys -> [ByteString] -> Atomic m [ByteString]
withdraw k from = do
  order <- descendants k from
  forM_ order (remove k False)
  return order

-- | Withdraw what has been in the pool since before a time, so many at
-- most, chosen here and withdrawn in the same step. Chosen by the caller
-- in a step before, a transaction could leave and come back in between
-- with a new time, and be withdrawn for the time that it had.
expire :: (Monad m) => Keys -> Integer -> Int -> Atomic m [ByteString]
expire k before most = do
  old <- zrangeByScore k.time (At 0) (At (fromIntegral before)) (Just most)
  withdraw k (map fst old)

-- | Keep the newest so many of the transactions that the fleet has seen:
-- how many are kept. Trimmed only when the set has grown a hundredth past
-- its bound, so that most calls cost a count.
trim :: (Monad m) => Keys -> Int -> Atomic m Integer
trim k most = do
  n <- zcard k.seen
  let bound = toInteger most
  if n > bound + max 1 (bound `div` 100)
    then write (ZRemRangeByRank k.seen 0 (n - bound - 1)) >> return bound
    else return n

-- | Offer a transaction, which the store checked against a tip: the tip
-- that it saw or nothing, whether each output that the transaction spends
-- is unspent at that tip, the transactions that it replaces, and the
-- transaction.
--
-- It is taken if no other of the pool spends what it spends, and each
-- output that it spends is made by a transaction of the pool or unspent
-- at the tip. What it replaces leaves first, with all that spends it, but
-- only if it is taken.
offer :: (Monad m) => Keys -> Maybe ByteString -> [Bool] -> [ByteString] -> PoolTx -> Atomic m Offered
offer k saw unspent replaces tx = do
  prefetch (likely k tx <> map (HExists k.spends) replaces)
  at <- get k.tip
  if fromMaybe "" at /= fromMaybe "" saw
    then return Late
    else do
      known <- member k tx.txid
      if known
        then sawIt k tx.txid >> return Known
        else do
          leaving <- descendants k replaces
          let left = Set.fromList leaving
          spenders <- forM tx.inputs (hget k.spender)
          let rivals = nub [s | Just s <- spenders, Set.notMember s left]
          if not (null rivals)
            then sawIt k tx.txid >> return (Conflict rivals)
            else do
              made <- forM tx.inputs $ \o -> (&& Set.notMember (maker k o) left) <$> member k (maker k o)
              let orphans = [o | (o, ours, free) <- zip3 tx.inputs made (unspent <> repeat False), not ours, not free]
              if not (null orphans)
                then return (Orphan orphans)
                else do
                  forM_ leaving (remove k False)
                  insert k tx
                  return Taken

-- | What the offer of a transaction asks when the transaction is taken
-- and replaces nothing, which is what most offers are: named before the
-- first question, so that Redis is asked all of it at once. Nothing
-- depends on this list being right. A question that is missing costs a
-- round trip, and one too many costs its answer.
likely :: Keys -> PoolTx -> [Query]
likely k tx =
  [Get k.tip, HExists k.spends t, HGet k.spends t, SIsMember k.own t, SIsMember k.rbf t, SMembers (k.kids <> t)]
    <> concat
      [ [HGet k.spender o, HExists k.spends (maker k o), SIsMember k.rbf (maker k o)]
      | o <- tx.inputs
      ]
    <> concatMap entry tx.entries
    <> [Get k.xver]
  where
    t = tx.txid
    entry = \case
      PoolMakes o -> [HExists k.spender o.output]
      PoolUses o -> [HExists k.spends (maker k o.output)]
      PoolTouches a -> [SMembers (k.watchers <> a)]
      PoolSpends a o _ -> [SMembers (k.watchers <> a), HExists (k.aval <> a) o]
      PoolPays a o _ _ -> [SMembers (k.watchers <> a), HExists (k.aspent <> a) o]

-------------------------------------------------------------------------------
-- The tip moves
-------------------------------------------------------------------------------

-- | What a store that moves the tip knows of the way.
data How
  = -- | what the blocks between the two tips did to the pool
    Changing !PoolChange
  | -- | nothing, for it does not have the old tip: the pool is emptied,
    -- and only if this store, by its name, is the one that is live, since
    -- another may know the way
    AloneNotKnowing !ByteString
  | -- | that the way is longer than a store goes through, which is so for
    -- every store: the pool is emptied, or the tip would never move again
    TooLong

-- | A number that Redis keeps as text, or 0.
number :: Maybe ByteString -> Integer
number = maybe 0 (maybe 0 fst . C.readInteger)

-- | Four bytes, big-endian, as a number: the height at the end of what is
-- kept of a replaced transaction.
heightOf :: Keys -> ByteString -> Score
heightOf k value = fromIntegral (B.foldl' (\n b -> n * 256 + toInteger b) 0 (B.take 4 (B.drop k.tsize value)))

-- | Forget that these transactions were replaced.
unreplaced :: (Monad m) => Keys -> [ByteString] -> Atomic m ()
unreplaced k ts =
  forM_ ts $ \t ->
    hget k.rep t >>= \case
      Nothing -> return ()
      Just value -> do
        write (SRem (k.repby <> B.take k.tsize value) t)
        write (HDel k.rep t)
        write (ZRem k.repat t)

-- | Keep that a transaction was replaced: by a transaction, which comes
-- first in the value, at a height, which follows in four bytes. What was
-- kept of it before goes, with its place among what its winner replaced.
replaced :: (Monad m) => Keys -> ByteString -> ByteString -> Atomic m ()
replaced k t value = do
  unreplaced k [t]
  write (HSet k.rep t value)
  write (SAdd (k.repby <> B.take k.tsize value) t)
  write (ZAdd k.repat (heightOf k value) t)

-- | Pieces of so many bytes.
pieces :: Int -> ByteString -> [ByteString]
pieces n s
  | B.null s || n <= 0 = []
  | otherwise = let (a, rest) = B.splitAt n s in a : pieces n rest

-- | Move the tip, and the pool and the cache of extended public keys with
-- it, in one step: the commit of a move.
--
-- Given are the tip that the store saw or nothing, the block to move to,
-- the time in milliseconds or nothing for the clock of Redis, the size of
-- a block in bytes, what the store knows of the way, the blocks that the
-- tip moves through, oldest first, and the token of the move of the cache
-- that the store began, or none.
--
-- The tip moves only to the block that the live stores agree on: the
-- newest block of the first of them, by name, that every other has among
-- its recent ones. It says 'Raced' when the tip is not the one that the
-- store saw, when no store is live, when the block is not the one agreed
-- on, when a store that does not know the way is not alone, and when the
-- move of the cache is not the one that the store began; and it has
-- changed nothing then.
--
-- With the way known: the transactions of the abandoned blocks come back,
-- and what spends the same in the pool gives way; what spends from the
-- rest of them leaves; what the new blocks confirm leaves quietly; what
-- spends what they spend leaves with all that spends it, and is kept as
-- replaced, by which transaction and at which height. What is kept of
-- replaced transactions is forgotten 2016 blocks later.
advance ::
  (Monad m) =>
  Keys ->
  Maybe ByteString ->
  ByteString ->
  Maybe Integer ->
  Int ->
  How ->
  [ByteString] ->
  ByteString ->
  Atomic m Moved
advance k saw target given bsize how path token = do
  at <- fromMaybe "" <$> get k.tip
  if at /= fromMaybe "" saw
    then return Raced
    else
      if target == at
        then return Stays
        else do
          now <- maybe clock return given
          leases <- hgetall k.lease
          let live = sort [store | (store, expiry) <- Map.toList leases, number (Just expiry) > now]
          reports <- forM live (fmap (pieces bsize . fromMaybe "") . hget k.recent)
          let agreed = case reports of
                first : others -> take 1 [b | b <- first, all (elem b) others]
                [] -> []
          if null live || agreed /= [target]
            then return Raced
            else do
              count <- number <$> get k.moves
              let before = num count <> "@" <> at
                  after = num (count + 1) <> "@" <> target
              case how of
                AloneNotKnowing caller
                  | live /= [caller] -> return Raced
                  | otherwise -> emptied at after
                TooLong -> emptied at after
                Changing change -> do
                  current <- get k.moving
                  let mine = token <> " "
                      other = maybe False ((/= mine) . B.take (B.length mine)) current
                  if other || (isNothing current && not (B.null token))
                    then return Raced
                    else changed change before after
  where
    -- the cache of extended public keys at the new place of the tip: its
    -- entries gone if the pool is, or if it was not at the place before;
    -- and otherwise every entry that the move marked with a new version
    settle wipe before after = do
      place <- get k.xtip
      if wipe || maybe False (/= before) place
        then write (Wipe (k.prefix <> "fx:"))
        else do
          ids <- Set.toList <$> smembers k.marked
          unless (null ids) $ do
            version <- number <$> get k.xver
            write (Incr k.xver)
            forM_ ids $ \i -> do
              there <- exists (k.entry <> i <> ":meta")
              when there (write (HSet (k.entry <> i <> ":meta") "ver" (num (version + 1))))
          write (Del k.marked)
          write (Del k.moving)
      write (Set k.xtip after)
    -- the pool starts again. What it decided on is gone, so the fleet
    -- forgets what it saw, and a counter tells the stores to fill the
    -- pool again; the first tip of a fleet empties nothing.
    emptied at after = do
      write (Wipe (k.prefix <> "pool:"))
      write (Del k.seen)
      unless (B.null at) (write (Incr k.restarts))
      settle True "" after
      write (Set k.tip target)
      write (Incr k.moves)
      emit k "b" target
      return (MovedTo target)
    changed change before after = do
      -- the abandoned blocks: what can come back does, and what spends the
      -- same in the pool gives way. What a transaction that comes back
      -- replaced is not replaced any more.
      forM_ change.returned $ \tx -> do
        won <- smembers (k.repby <> tx.txid)
        unreplaced k (Set.toList won)
        ours <- member k tx.txid
        unless ours $ do
          rivals <- forM tx.inputs (hget k.spender)
          _ <- withdraw k [s | Just s <- rivals]
          insert k tx
      -- what the store has as replaced of the abandoned blocks waits for
      -- the transaction that replaced it
      forM_ (concatMap (pieces (2 * k.tsize + 4)) change.lost) $ \item -> do
        let (t, value) = B.splitAt k.tsize item
        old <- hget k.lost t
        forM_ old $ \v -> write (SRem (k.lostby <> B.take k.tsize v) t)
        write (HSet k.lost t value)
        write (SAdd (k.lostby <> B.take k.tsize value) t)
        write (ZAdd k.lostat (heightOf k value) t)
      -- what spends from those that are gone for good leaves
      orphaned <- concat <$> forM change.gone (fmap Set.toList . smembers . (k.kids <>))
      _ <- withdraw k orphaned
      -- the new blocks: what they confirm leaves alone
      forM_ change.confirmed (remove k True)
      -- what waited for a transaction that they confirm was replaced
      forM_ change.confirmed $ \t -> do
        waiting <- smembers (k.lostby <> t)
        forM_ (Set.toList waiting) $ \w -> do
          value <- hget k.lost w
          forM_ value (replaced k w)
          write (HDel k.lost w)
          write (ZRem k.lostat w)
        write (Del (k.lostby <> t))
      -- what spends what they spend leaves with all that spends it, and is
      -- kept as replaced
      forM_ (concatMap (pieces (2 * k.tsize + 8)) change.spent) $ \item -> do
        let (o, by) = B.splitAt (k.tsize + 4) item
        s <- hget k.spender o
        forM_ s $ \rival -> do
          left <- withdraw k [rival]
          forM_ left (\t -> replaced k t by)
      -- forget what was replaced, or waits, 2016 blocks before the block
      -- of the tip
      forM_ change.height $ \h -> when (h >= 2016) $ do
        let limit = At (fromIntegral (h - 2016 :: Word32))
        old <- zrangeByScore k.repat MinusInf limit Nothing
        unreplaced k (map fst old)
        stale <- zrangeByScore k.lostat MinusInf limit Nothing
        forM_ stale $ \(t, _) -> do
          value <- hget k.lost t
          forM_ value $ \v -> write (SRem (k.lostby <> B.take k.tsize v) t)
          write (HDel k.lost t)
          write (ZRem k.lostat t)
      settle False before after
      write (Set k.tip target)
      write (Incr k.moves)
      -- the blocks that the tip moved through, oldest first, if the store
      -- that moves it gave them, and the block that it is at now otherwise
      forM_ (if null path then [target] else path) (emit k "b")
      return (MovedTo target)

-------------------------------------------------------------------------------
-- What is kept of a transaction, to take it out as it was put in
-------------------------------------------------------------------------------

-- | The time and the entries of a transaction, as the pool keeps them.
keep :: Word64 -> [PoolEntry] -> ByteString
keep at es = runPut $ do
  putWord64be at
  putWord64be (fromIntegral (length es))
  mapM_ putEntry es

-- | What 'keep' wrote, or nothing for what it did not write.
kept :: ByteString -> (Word64, [PoolEntry])
kept = either (const (0, [])) id . runGet ((,) <$> getWord64be <*> listed)
  where
    listed = getWord64be >>= \n -> mapM (const getEntry) [1 .. n]

putBytes :: ByteString -> Put
putBytes s = putWord64be (fromIntegral (B.length s)) >> putByteString s

getBytes :: Get ByteString
getBytes = getWord64be >>= getByteString . fromIntegral

putEntry :: PoolEntry -> Put
putEntry = \case
  PoolTouches a -> putWord8 0 >> putBytes a
  PoolSpends a o v -> putWord8 1 >> putBytes a >> putBytes o >> putWord64be v
  PoolPays a o v s -> putWord8 2 >> putBytes a >> putBytes o >> putWord64be v >> putBytes s
  PoolMakes o -> putWord8 3 >> putToken o
  PoolUses o -> putWord8 4 >> putToken o
  where
    putToken o = do
      maybe (putWord8 0) (\a -> putWord8 1 >> putBytes a) o.address
      putBytes o.output
      putBytes o.category
      putWord64be o.amount
      putWord8 (if o.nft then 1 else 0)
      maybe (putWord8 0) (\(v, s) -> putWord8 1 >> putWord64be v >> putBytes s) o.coins

getEntry :: Get PoolEntry
getEntry =
  getWord8 >>= \case
    0 -> PoolTouches <$> getBytes
    1 -> PoolSpends <$> getBytes <*> getBytes <*> getWord64be
    2 -> PoolPays <$> getBytes <*> getBytes <*> getWord64be <*> getBytes
    3 -> PoolMakes <$> getToken
    4 -> PoolUses <$> getToken
    _ -> fail "Not an entry of the pool"
  where
    getToken = do
      address <- getWord8 >>= \case 0 -> return Nothing; _ -> Just <$> getBytes
      output <- getBytes
      category <- getBytes
      amount <- getWord64be
      nft <- (/= 0) <$> getWord8
      coins <- getWord8 >>= \case 0 -> return Nothing; _ -> Just <$> ((,) <$> getWord64be <*> getBytes)
      return PoolTokenOut {address, output, category, amount, nft, coins}
