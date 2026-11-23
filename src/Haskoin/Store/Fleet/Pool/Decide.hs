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
-- tokens, what the blocks replaced, what a move of the tip marked as
-- leaving, the events, and the pool's layer of the cache of extended
-- public keys.
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
    entryTxs,
    entryOuts,
    entryOval,
    entryMeta,
    floorHeld,
    leavingHeld,
    takeOut,
    offer,
    withdraw,
    expire,
    How (..),
    advance,
  )
where

import Control.Monad (foldM_, forM, forM_, unless, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.List (nub)
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
import Haskoin.Store.Fleet.Atomic.Keys
import Haskoin.Store.Fleet.Pool.Types

-- | The keys of the pool and of what goes with it, under the prefix of a
-- fleet, each with the kind of what it holds, and the size of a
-- transaction id in bytes. A family is the start of a key for each
-- address, category or transaction.
data Keys = Keys
  { prefix :: !ByteString,
    -- | bytes in a transaction id; an output is that and four more
    tsize :: !Int,
    tip :: !(K 'Text),
    -- | how many times the tip moved
    moves :: !(K 'Number),
    -- | how many times the pool was emptied
    restarts :: !(K 'Number),
    -- | until when each store is live
    lease :: !(K 'Counts),
    -- | the recent blocks of each store, newest first
    recent :: !(K 'Hash),
    events :: !(K 'Stream),
    xver :: !(K 'Number),
    -- | the outputs that each transaction spends, one after the other
    spends :: !(K 'Hash),
    -- | the transaction that spends each output
    spender :: !(K 'Hash),
    body :: !(K 'Hash),
    -- | the transactions of the pool that spend from each transaction
    kids :: !(Family 'Group),
    -- | what each transaction does at addresses and to tokens, as it was
    -- put in, to take it out the same
    entries :: !(K 'Hash),
    -- | every transaction by the time it entered a mempool
    time :: !(K 'Sorted),
    ids :: !(K 'Sorted),
    atx :: !(Family 'Sorted),
    aout :: !(Family 'Sorted),
    aval :: !(Family 'Hash),
    aspent :: !(Family 'Hash),
    asum :: !(Family 'Counts),
    tsum :: !(Family 'Counts),
    acat :: !(Family 'Group),
    chold :: !(Family 'Group),
    ctx :: !(Family 'Sorted),
    cout :: !(Family 'Sorted),
    cval :: !(Family 'Hash),
    csum :: !(Family 'Counts),
    -- | what a block replaced: the transaction that replaced it and the
    -- height of the block, with those by the transaction that replaced
    -- them and all by height
    rep :: !(K 'Hash),
    repby :: !(Family 'Group),
    repat :: !(K 'Sorted),
    -- | the same for what waits for the transaction that replaced it to
    -- be confirmed
    lost :: !(K 'Hash),
    lostby :: !(Family 'Group),
    lostat :: !(K 'Sorted),
    -- | the transactions that a move of the tip marked as leaving, and
    -- that are still in the pool ('takeOut'): nothing for one that a
    -- block confirmed, and for one that a block is in conflict with the
    -- transaction that replaced it and the height of the block
    leaving :: !(K 'Hash),
    -- | the place of the tip that the cache of extended public keys is at
    xtip :: !(K 'Text),
    -- | the move of the cache that is under way: its token
    moving :: !(K 'Text),
    -- | when the store that makes that move last said that it is at it
    beat :: !(K 'Number),
    -- | the entries of the cache that the move changed
    marked :: !(K 'Group),
    -- | the cached keys that have each address
    watchers :: !(Family 'Group),
    -- | the start of the entries of the cache of extended public keys
    entry :: !ByteString
  }

-- | The keys under a prefix.
poolKeys :: ByteString -> Int -> Keys
poolKeys p tsize =
  Keys
    { prefix = p,
      tsize,
      tip = named (p <> "fleet:tip"),
      moves = named (p <> "fleet:moves"),
      restarts = named (p <> "fleet:restarts"),
      lease = named (p <> "fleet:lease"),
      recent = named (p <> "fleet:recent"),
      events = named (p <> "fleet:events"),
      xver = named (p <> "fleet:xver"),
      spends = named (p <> "pool:spends"),
      spender = named (p <> "pool:spender"),
      body = named (p <> "pool:body"),
      kids = family (p <> "pool:kids:"),
      entries = named (p <> "pool:entries"),
      time = named (p <> "pool:time"),
      ids = named (p <> "pool:ids"),
      atx = family (p <> "pool:atx:"),
      aout = family (p <> "pool:aout:"),
      aval = family (p <> "pool:aval:"),
      aspent = family (p <> "pool:aspent:"),
      asum = family (p <> "pool:asum:"),
      tsum = family (p <> "pool:tsum:"),
      acat = family (p <> "pool:acat:"),
      chold = family (p <> "pool:chold:"),
      ctx = family (p <> "pool:ctx:"),
      cout = family (p <> "pool:cout:"),
      cval = family (p <> "pool:cval:"),
      csum = family (p <> "pool:csum:"),
      rep = named (p <> "pool:rep"),
      repby = family (p <> "pool:repby:"),
      repat = named (p <> "pool:repat"),
      lost = named (p <> "pool:lost"),
      lostby = family (p <> "pool:lostby:"),
      lostat = named (p <> "pool:lostat"),
      leaving = named (p <> "pool:leaving"),
      xtip = named (p <> "fx:tip"),
      moving = named (p <> "fx:moving"),
      beat = named (p <> "fx:beat"),
      marked = named (p <> "fx:marked"),
      watchers = family (p <> "fx:of:"),
      entry = p <> "fx:e:"
    }

-- | The lists of the entry of a cached key, what it keeps for each output,
-- and its numbers, of which the pool gives it a version.
entryTxs, entryOuts :: Keys -> ByteString -> K 'Sorted
entryTxs k i = named (k.entry <> i <> ":txs")
entryOuts k i = named (k.entry <> i <> ":outs")

entryOval :: Keys -> ByteString -> K 'Hash
entryOval k i = named (k.entry <> i <> ":oval")

entryMeta :: Keys -> ByteString -> K 'Counts
entryMeta k i = named (k.entry <> i <> ":meta")

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
emit k kind what = xadd k.events 10000 [("k", kind), ("h", what)]

-- | Whether a transaction is in the pool.
member :: (Monad m) => Keys -> ByteString -> Atomic m Bool
member k = hexists k.spends

-- | The transactions of the pool among these, and all that spend them,
-- each once and none before one that it spends from among them.
descendants :: (Monad m) => Keys -> [ByteString] -> Atomic m [ByteString]
descendants k = descendantsBut k Set.empty

-- | 'descendants' without some transactions, nor what spends from the
-- rest only through them: a transaction that a block confirms leaves
-- what spends from it in the pool.
descendantsBut :: (Monad m) => Keys -> Set.Set ByteString -> [ByteString] -> Atomic m [ByteString]
descendantsBut k but from = do
  roots <- filterM' (member k) from
  go Set.empty [] (reverse roots)
  where
    go _ order [] = return (reverse order)
    go done order (t : todo)
      | Set.member t done || Set.member t but = go done order todo
      | otherwise = do
          children <- smembers (under k.kids t)
          go (Set.insert t done) (t : order) (Set.toDescList children <> todo)
    filterM' p = fmap concat . mapM (\x -> (\ok -> [x | ok]) <$> p x)

-- | Whether a member of a list of an entry of the cache is in the pool's
-- layer, with a score below zero: what the pool puts in or takes out of an
-- entry is never what the chain's layer has, which a move of the tip
-- writes ahead of the pool.
pooled :: (Monad m) => K 'Sorted -> ByteString -> Atomic m Bool
pooled z x = maybe False (< 0) <$> zscore z x

-- | Add to a field of a summary.
addTo :: (Monad m) => K 'Counts -> ByteString -> Integer -> Atomic m ()
addTo = hincrBy

-- | What the pool does to the tokens of an output, by category and by
-- address. An output is @free@ while the pool makes it and nothing in the
-- pool uses it up, and @spent@ while the pool uses it up and the chain
-- made it. An amount is kept in two parts, the digits above the last nine
-- and the last nine, as the stores read it.
tokens :: (Monad m) => Keys -> PoolTokenOut -> ByteString -> Integer -> Atomic m ()
tokens k o which sign = do
  addTo (under k.csum o.category) which sign
  forM_ o.address $ \a -> do
    let key = under k.tsum (o.category <> a)
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
          zadd (under k.cout c) newest o.output
          hset (under k.cval c) o.output (maybe "" (\(v, s) -> num v <> " " <> s) o.coins)
        else do
          zrem (under k.cout c) o.output
          hdel (under k.cval c) o.output
      -- an output that the pool uses up was spent from the chain until
      -- the pool made it, and is spent from the chain again once the pool
      -- no longer makes it
      used <- hexists k.spender o.output
      if used then tokens k o "spent" (negate sign) else tokens k o "free" sign
      forM_ o.address $ \a -> do
        let (hi, lo) = toInteger o.amount `divMod` 1000000000
        addTo (under k.tsum (c <> a)) "receivedHi" (sign * hi)
        addTo (under k.tsum (c <> a)) "receivedLo" (sign * lo)
    PoolUses o -> do
      ours <- member k (maker k o.output)
      if ours then tokens k o "free" (negate sign) else tokens k o "spent" sign
    PoolTouches a -> do
      watching <- Set.toList <$> smembers (under k.watchers a)
      if entering then zadd (under k.atx a) newest t else zrem (under k.atx a) t
      forM_ watching $ \i ->
        if entering
          then zaddNew (entryTxs k i) newest t
          else do
            mine <- pooled (entryTxs k i) t
            when mine (zrem (entryTxs k i) t)
      addTo (under k.asum a) "txs" sign
    PoolSpends a o v -> do
      if entering then hset (under k.aspent a) o (num v) else hdel (under k.aspent a) o
      addTo (under k.asum a) "spent" (sign * toInteger v)
      addTo (under k.asum a) "spentCount" sign
      paid <- hexists (under k.aval a) o
      when paid $ do
        addTo (under k.asum a) "both" (sign * toInteger v)
        addTo (under k.asum a) "bothCount" sign
    PoolPays a o v s -> do
      watching <- Set.toList <$> smembers (under k.watchers a)
      if entering
        then do
          zadd (under k.aout a) newest o
          hset (under k.aval a) o (num v <> " " <> s)
        else do
          zrem (under k.aout a) o
          hdel (under k.aval a) o
      forM_ watching $ \i -> do
        let outs = entryOuts k i
        if entering
          then do
            there <- isJust <$> zscore outs o
            unless there $ do
              zadd outs newest o
              hset (entryOval k i) o (num v <> " " <> num (B.length a) <> " " <> a <> s)
          else do
            mine <- pooled outs o
            when mine $ do
              zrem outs o
              hdel (entryOval k i) o
      addTo (under k.asum a) "paid" (sign * toInteger v)
      addTo (under k.asum a) "paidCount" sign
      gone <- hexists (under k.aspent a) o
      when gone $ do
        addTo (under k.asum a) "both" (sign * toInteger v)
        addTo (under k.asum a) "bothCount" sign
  -- every entry with an address that the transaction touches has a new
  -- version, so that a request that read it before reads it again
  watched <- nub . concat <$> forM touched (\a -> Set.toList <$> smembers (under k.watchers a))
  unless (null watched) $ do
    before <- getNumber k.xver
    incrBy k.xver 1
    forM_ watched $ \i -> hsetNumber (entryMeta k i) "ver" (before + 1)
  -- the transaction in the lists of the categories that it moves, and in
  -- the count of each address that it moves tokens of
  forM_ cats $ \c -> do
    if entering then zadd (under k.ctx c) newest t else zrem (under k.ctx c) t
    addTo (under k.csum c) "txs" sign
  forM_ held $ \(c, a) -> do
    addTo (under k.tsum (c <> a)) "txs" sign
    when entering $ do
      sadd (under k.acat a) c
      sadd (under k.chold c) a
  -- an address that no transaction touches has no summary, nor has a
  -- category or an address of one that no transaction moves
  unless entering $ do
    forM_ touched $ \a -> do
      left <- hgetNumber (under k.asum a) "txs"
      when (left == Just 0) (del (under k.asum a))
    forM_ cats $ \c -> do
      left <- hgetNumber (under k.csum c) "txs"
      when (left == Just 0) (del (under k.csum c))
    forM_ held $ \(c, a) -> do
      left <- hgetNumber (under k.tsum (c <> a)) "txs"
      when (left == Just 0) $ do
        del (under k.tsum (c <> a))
        srem (under k.acat a) c
        srem (under k.chold c) a
  where
    entering = sign > 0
    newest = negate (fromIntegral at) :: Score
    touched = touchedBy es
    cats = categoriesOf es
    held = heldBy es

-- | The addresses that a transaction touches, each once.
touchedBy :: [PoolEntry] -> [ByteString]
touchedBy = nub . concatMap addressOf
  where
    addressOf = \case
      PoolTouches a -> [a]
      PoolSpends a _ _ -> [a]
      PoolPays a _ _ _ -> [a]
      _ -> []

-- | The outputs with tokens that a transaction makes or uses up.
tokenOuts :: [PoolEntry] -> [PoolTokenOut]
tokenOuts = concatMap (\case PoolMakes o -> [o]; PoolUses o -> [o]; _ -> [])

-- | The categories of the tokens that a transaction moves, each once.
categoriesOf :: [PoolEntry] -> [ByteString]
categoriesOf = nub . map (.category) . tokenOuts

-- | The categories that a transaction moves at each address, each once.
heldBy :: [PoolEntry] -> [(ByteString, ByteString)]
heldBy es = nub [(o.category, a) | o <- tokenOuts es, Just a <- [o.address]]

-- | What taking a transaction out asks about its addresses and tokens
-- ('account', with sign -1), given what it does there: for 'foreseen'.
-- What is asked of the cached keys at an address is named once Redis has
-- said which they are.
departing :: Keys -> ByteString -> [PoolEntry] -> [Ahead]
departing k t es =
  concatMap entry es
    <> [only (askingCount (under k.asum a) "txs") | a <- touchedBy es]
    <> [only (askingCount (under k.csum c) "txs") | c <- categoriesOf es]
    <> [only (askingCount (under k.tsum (c <> a)) "txs") | (c, a) <- heldBy es]
  where
    entry = \case
      PoolMakes o -> [only (askingWhetherField k.spender o.output)]
      PoolUses o -> [only (askingWhetherField k.spends (maker k o.output))]
      PoolTouches a -> [watching a (\i -> askingScore (entryTxs k i) t)]
      PoolSpends a o _ -> [only (askingWhetherField (under k.aval a) o)]
      PoolPays a o _ _ ->
        [ watching a (\i -> askingScore (entryOuts k i) o),
          only (askingWhetherField (under k.aspent a) o)
        ]
    watching a asks =
      thenMembers (under k.watchers a) $ \ids ->
        [only (askingNumber k.xver) | not (Set.null ids)] <> map (only . asks) (Set.toList ids)

-- | What a move of the tip over blocks asks, in the steps in which it can
-- be named: the tip; then what is asked of each transaction that the
-- blocks confirm and of each output that they spend, and which stores
-- are live; then, of each transaction, what taking it out asks about its
-- addresses ('departing'), and of each cached key at one of them.
--
-- And of each transaction of the pool that spends what a block spends:
-- what taking it out asks, and of all that spends from it.
--
-- These are the questions of a move with no block to undo, which is
-- nearly all of them. One that brings transactions back asks more, and
-- is gone through again for it. Nothing depends on this being right:
-- @asks Redis no more times@ in "Haskoin.Store.Fleet.DecideSpec" fails
-- when a question is missing here.
foreseen :: Keys -> Maybe ByteString -> ByteString -> Integer -> Bool -> PoolChange -> [Ahead]
foreseen k saw target now light change =
  [ thenText k.tip $ \tip ->
      let at = fromMaybe "" tip
       in if at /= fromMaybe "" saw || at == target then [] else rest
  ]
  where
    rest =
      [ thenAbove k.lease now (map (only . askingField k.recent) . Set.toList),
        only (askingNumber k.moves),
        only (asking k.moving),
        only (askingWhether k.leaving),
        only (asking k.xtip),
        thenMembers k.marked $ \ids ->
          [only (askingNumber k.xver) | not (Set.null ids)]
            <> [only (askingWhether (entryMeta k i)) | i <- Set.toList ids]
      ]
        <> [ only (askingRange z MinusInf (At (fromIntegral (h - 2016))) Nothing)
           | h <- maybe [] pure change.height,
             h >= 2016,
             z <- [k.repat, k.lostat]
           ]
        <> [ thenField k.spender (B.take (k.tsize + 4) item) (maybe [] rival)
           | item <- concatMap (pieces (2 * k.tsize + 8)) change.spent
           ]
        <> concatMap confirmed change.confirmed
    confirming = Set.fromList change.confirmed
    -- A transaction of the pool that spends what a block spends, and
    -- that the blocks do not confirm, leaves with all that spends it,
    -- and is kept as replaced. A block of Bitcoin has some of those, and
    -- finding them as it went had the move gone through five more times
    -- for sixty of them.
    rival t
      | Set.member t confirming = []
      | light = only (askingWhetherField k.spends t) : kin t
      | otherwise = leaver t
    -- all that spends from a transaction, to mark it: not what the
    -- blocks confirm, which leaves what spends from it
    kin t
      | Set.member t confirming = []
      | otherwise = [thenMembers (under k.kids t) (concatMap kin . Set.toList)]
    leaver t =
      [ only (askingWhetherField k.spends t),
        only (askingField k.spends t),
        only (askingField k.rep t),
        thenField k.entries t (maybe [] (departing k t . snd . kept)),
        thenMembers (under k.kids t) (concatMap leaver . Set.toList)
      ]
    confirmed t
      | light =
          [ only (askingWhetherField k.spends t),
            only (askingMembers (under k.lostby t))
          ]
      | otherwise =
          [ only (askingField k.spends t),
            only (askingMembers (under k.lostby t)),
            thenField k.entries t (maybe [] (departing k t . snd . kept))
          ]

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
      unless quiet (emit k "d" t)
      (at, es) <- maybe (0, []) kept <$> hget k.entries t
      account k t at es (-1)
      hdel k.entries t
      zrem k.time t
      zrem k.ids t
      forM_ (outputs k inputs) $ \o -> do
        hdel k.spender o
        srem (under k.kids (maker k o)) t
      hdel k.spends t
      hdel k.body t

-- | Put a transaction in.
insert :: (Monad m) => Keys -> PoolTx -> Atomic m ()
insert k tx = do
  let t = tx.txid
  hset k.spends t (B.concat tx.inputs)
  hset k.body t tx.body
  forM_ tx.inputs $ \o -> do
    hset k.spender o t
    sadd (under k.kids (maker k o)) t
  hset k.entries t (keep tx.time tx.entries)
  zadd k.time (fromIntegral tx.time) t
  zadd k.ids 0 t
  account k t tx.time tx.entries 1
  emit k "t" t

-- | Whether a store has the floor: it began a move of the tip
-- ('Haskoin.Store.Fleet.XPub.Decide.begin'), and said in the last ten
-- seconds that it is at it ('Haskoin.Store.Fleet.XPub.Decide.beat').
--
-- A move of the tip is one decision that reads what tens of thousands of
-- questions say, over seconds, and is carried out only if every answer
-- still holds. Transactions are offered several times a second, and each
-- offer changes some of those answers: left to themselves, offers would
-- have every move refused, and the tip would never move. So whatever else
-- writes to the pool or to the cache gives way while a store has the
-- floor, as everything gave way to the script that moved the tip, which
-- Redis ran alone. An offer is 'Late' then, and is made again.
--
-- The floor is the store's for as long as it says so, and not for ten
-- seconds from when it took it: the move over a block of Bitcoin was
-- decided in more than ten seconds on a server, lost the floor before it
-- was carried out, and was refused at every turn. What the store says is
-- written to a key of its own, which the move does not read, so that
-- saying it changes no answer of the move.
--
-- Giving way for no reason costs a moment, and the clock can only do
-- that: a move that looks under way and has just gone quiet. A move that
-- has gone quiet at the clock of this decision has still gone quiet when
-- the decision is carried out, if its store said nothing since, which the
-- script checks.
floorHeld :: (Monad m) => Keys -> Atomic m Bool
floorHeld k =
  get k.moving >>= \case
    Nothing -> return False
    Just _ -> do
      now <- clock
      said <- getNumber k.beat
      return (now - said <= 10000)

-- | Whether the pool still has transactions that a move of the tip marked
-- as leaving. Nothing else is put in or taken out of the pool until they
-- are out ('takeOut'): an offer is 'Late', a withdrawal takes nothing,
-- and the next move waits, as all of them do while a store has the floor.
leavingHeld :: (Monad m) => Keys -> Atomic m Bool
leavingHeld k = exists k.leaving

-- | Take out of the pool some of the transactions that a move of the tip
-- marked as leaving, and say how many of these were: one that a block
-- confirmed leaves quietly, and one that a block is in conflict with
-- leaves as withdrawn, and is kept as replaced. One that is not marked,
-- or no longer, is left alone, so that two stores may take the same ones
-- out.
--
-- A move of the tip over a full block took what the block confirmed out
-- of the pool in the same step: tens of thousands of questions and a
-- hundred thousand writes, for which the tip waited, seconds of them in
-- Redis, which answers nobody else meanwhile. The move now marks them,
-- which is one write for each, and the tip is at the new block. They
-- leave here, a few at a time, in the same way and to the same end, and
-- a request that would read one of them takes it out first.
takeOut :: (Monad m) => Keys -> [ByteString] -> Atomic m Int
takeOut k ts = do
  ready <- ahead [thenField k.leaving t (maybe [] (going t)) | t <- ts]
  if ready then length . filter id <$> mapM one ts else return 0
  where
    going t by =
      [ only (askingField k.spends t),
        thenField k.entries t (maybe [] (departing k t . snd . kept))
      ]
        <> [only (askingField k.rep t) | not (B.null by)]
    one t =
      hget k.leaving t >>= \case
        Nothing -> return False
        Just by -> do
          if B.null by
            then remove k True t
            else remove k False t >> replaced k t by
          hdel k.leaving t
          return True

-- | Withdraw transactions and all that spends them: those that left. None
-- while a store is moving the tip.
withdraw :: (Monad m) => Keys -> [ByteString] -> Atomic m [ByteString]
withdraw k from = do
  held <- floorHeld k
  busy <- leavingHeld k
  if held || busy then return [] else withdrawn k from

-- | 'withdraw' whoever has the floor: for the move of the tip itself.
withdrawn :: (Monad m) => Keys -> [ByteString] -> Atomic m [ByteString]
withdrawn k from = do
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

-- | Offer a transaction, which the store checked against a tip: the tip
-- that it saw or nothing, whether each output that the transaction spends
-- is unspent at that tip, the transactions that it replaces, and the
-- transaction.
--
-- It is taken if no other of the pool spends what it spends, and each
-- output that it spends is made by a transaction of the pool or unspent
-- at the tip. What it replaces leaves first, with all that spends it, but
-- only if it is taken.
--
-- It is 'Late' while a store is moving the tip ('floorHeld'), as it is
-- when the tip is no longer the one that the store saw: the store offers
-- it again.
offer :: (Monad m) => Keys -> Maybe ByteString -> [Bool] -> [ByteString] -> PoolTx -> Atomic m Offered
offer k saw unspent replaces tx = do
  prefetch (likely k tx <> map (askingWhetherField k.spends) replaces)
  at <- get k.tip
  held <- floorHeld k
  busy <- leavingHeld k
  if held || busy || fromMaybe "" at /= fromMaybe "" saw
    then return Late
    else do
      known <- member k tx.txid
      if known
        then return Known
        else do
          leaving <- descendants k replaces
          let left = Set.fromList leaving
          spenders <- forM tx.inputs (hget k.spender)
          let rivals = nub [s | Just s <- spenders, Set.notMember s left]
          if not (null rivals)
            then return (Conflict rivals)
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
  [ asking k.tip,
    asking k.moving,
    askingWhether k.leaving,
    askingWhetherField k.spends t,
    askingField k.spends t
  ]
    <> concat
      [ [askingField k.spender o, askingWhetherField k.spends (maker k o)]
      | o <- tx.inputs
      ]
    <> concatMap entry tx.entries
    <> [askingNumber k.xver]
  where
    t = tx.txid
    entry = \case
      PoolMakes o -> [askingWhetherField k.spender o.output]
      PoolUses o -> [askingWhetherField k.spends (maker k o.output)]
      PoolTouches a -> [askingMembers (under k.watchers a)]
      PoolSpends a o _ -> [askingMembers (under k.watchers a), askingWhetherField (under k.aval a) o]
      PoolPays a o _ _ -> [askingMembers (under k.watchers a), askingWhetherField (under k.aspent a) o]

-------------------------------------------------------------------------------
-- The tip moves
-------------------------------------------------------------------------------

-- | What a store that moves the tip knows of the way.
data How
  = -- | what the blocks between the two tips did to the pool. With no
    -- block to undo, what leaves the pool is marked and taken out
    -- afterwards ('takeOut'); otherwise as 'AtOnce'.
    Changing !PoolChange
  | -- | the same, with everything that leaves the pool taken out in the
    -- move itself
    AtOnce !PoolChange
  | -- | nothing, for it does not have the old tip: the pool is emptied,
    -- and only if this store, by its name, is the one that is live, since
    -- another may know the way
    AloneNotKnowing !ByteString
  | -- | that the way is longer than a store goes through, which is so for
    -- every store: the pool is emptied, or the tip would never move again
    TooLong

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
        srem (under k.repby (B.take k.tsize value)) t
        hdel k.rep t
        zrem k.repat t

-- | Keep that a transaction was replaced: by a transaction, which comes
-- first in the value, at a height, which follows in four bytes. What was
-- kept of it before goes, with its place among what its winner replaced.
replaced :: (Monad m) => Keys -> ByteString -> ByteString -> Atomic m ()
replaced k t value = do
  unreplaced k [t]
  hset k.rep t value
  sadd (under k.repby (B.take k.tsize value)) t
  zadd k.repat (heightOf k value) t

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
  -- What a move over blocks will ask, named before it is gone through
  -- ('foreseen'). A pass in which Redis has not answered all of that
  -- does not count, and is left at once, with a result that nobody sees.
  ready <- case how of
    Changing change -> do
      now <- maybe clock return given
      ahead (foreseen k saw target now (plain change) change)
    AtOnce change -> do
      now <- maybe clock return given
      ahead (foreseen k saw target now False change)
    _ -> return True
  if ready then moving else return Raced
  where
    moving = do
      at <- fromMaybe "" <$> get k.tip
      if at /= fromMaybe "" saw
        then return Raced
        else
          if target == at
            then return Stays
            else do
              now <- maybe clock return given
              -- which stores are live, and not until when: a store that
              -- renews its lease while this is decided changes nothing for it
              live <- Set.toAscList <$> hfieldsAbove k.lease now
              reports <- forM live (fmap (pieces bsize . fromMaybe "") . hget k.recent)
              let agreed = case reports of
                    first : others -> take 1 [b | b <- first, all (elem b) others]
                    [] -> []
              if null live || agreed /= [target]
                then return Raced
                else do
                  count <- getNumber k.moves
                  let before = num count <> "@" <> at
                      after = num (count + 1) <> "@" <> target
                  case how of
                    AloneNotKnowing caller
                      | live /= [caller] -> return Raced
                      | otherwise -> emptied at after
                    TooLong -> emptied at after
                    Changing change
                      | plain change -> floored (marked change before after)
                      | otherwise -> floored (changed change before after)
                    AtOnce change -> floored (changed change before after)
    -- A move over blocks is for the store that began the move of the
    -- cache, and waits for what the move before it left in the pool.
    floored go = do
      busy <- leavingHeld k
      current <- get k.moving
      let other = maybe False (/= token) current
      if busy
        then return Leaving
        else
          if other || (isNothing current && not (B.null token))
            then return Raced
            else go
    -- no block is undone: nothing comes back, and nothing that was
    -- replaced is to be kept
    plain change = null change.returned && null change.gone && null change.lost
    -- the cache of extended public keys at the new place of the tip: its
    -- entries gone if the pool is, or if it was not at the place before;
    -- and otherwise every entry that the move marked with a new version
    settle emptying before after = do
      place <- get k.xtip
      if emptying || maybe False (/= before) place
        then wipe (k.prefix <> "fx:")
        else do
          ids <- Set.toList <$> smembers k.marked
          unless (null ids) $ do
            version <- getNumber k.xver
            incrBy k.xver 1
            forM_ ids $ \i -> do
              there <- exists (entryMeta k i)
              when there (hsetNumber (entryMeta k i) "ver" (version + 1))
          del k.marked
          del k.moving
          del k.beat
      set k.xtip after
    -- the pool starts again, and a counter tells the stores to fill it
    -- again; the first tip of a fleet empties nothing.
    emptied at after = do
      wipe (k.prefix <> "pool:")
      unless (B.null at) (incrBy k.restarts 1)
      settle True "" after
      set k.tip target
      incrBy k.moves 1
      emit k "b" target
      return (MovedTo target)
    -- The move over new blocks alone, with what leaves the pool only
    -- marked: 'takeOut' takes it out, a few transactions at a time.
    marked change before after = do
      let confirming = Set.fromList change.confirmed
      forM_ change.confirmed $ \t -> do
        ours <- member k t
        when ours (hset k.leaving t "")
      -- what waited for a transaction that they confirm was replaced
      forM_ change.confirmed $ \t -> do
        waiting <- smembers (under k.lostby t)
        unless (Set.null waiting) $ do
          forM_ (Set.toList waiting) $ \w -> do
            value <- hget k.lost w
            forM_ value (replaced k w)
            hdel k.lost w
            zrem k.lostat w
          del (under k.lostby t)
      -- what spends what they spend leaves with all that spends it: each
      -- is marked with the first transaction that replaces it
      foldM_
        ( \seen item -> do
            let (o, by) = B.splitAt (k.tsize + 4) item
            s <- hget k.spender o
            case s of
              Just rival | Set.notMember rival confirming -> do
                order <- descendantsBut k confirming [rival]
                let new = filter (`Set.notMember` seen) order
                forM_ new (\t -> hset k.leaving t by)
                return (foldr Set.insert seen new)
              _ -> return seen
        )
        Set.empty
        (concatMap (pieces (2 * k.tsize + 8)) change.spent)
      forM_ change.height $ \h -> when (h >= 2016) $ do
        let limit = At (fromIntegral (h - 2016 :: Word32))
        old <- zrangeByScore k.repat MinusInf limit Nothing
        unreplaced k (map fst old)
        stale <- zrangeByScore k.lostat MinusInf limit Nothing
        forM_ stale $ \(t, _) -> do
          value <- hget k.lost t
          forM_ value $ \v -> srem (under k.lostby (B.take k.tsize v)) t
          hdel k.lost t
          zrem k.lostat t
      settle False before after
      set k.tip target
      incrBy k.moves 1
      forM_ (if null path then [target] else path) (emit k "b")
      return (MovedTo target)
    changed change before after = do
      -- the abandoned blocks: what can come back does, and what spends the
      -- same in the pool gives way. What a transaction that comes back
      -- replaced is not replaced any more.
      forM_ change.returned $ \tx -> do
        won <- smembers (under k.repby tx.txid)
        unreplaced k (Set.toList won)
        ours <- member k tx.txid
        unless ours $ do
          rivals <- forM tx.inputs (hget k.spender)
          _ <- withdrawn k [s | Just s <- rivals]
          insert k tx
      -- what the store has as replaced of the abandoned blocks waits for
      -- the transaction that replaced it
      forM_ (concatMap (pieces (2 * k.tsize + 4)) change.lost) $ \item -> do
        let (t, value) = B.splitAt k.tsize item
        old <- hget k.lost t
        forM_ old $ \v -> srem (under k.lostby (B.take k.tsize v)) t
        hset k.lost t value
        sadd (under k.lostby (B.take k.tsize value)) t
        zadd k.lostat (heightOf k value) t
      -- what spends from those that are gone for good leaves
      orphaned <- concat <$> forM change.gone (fmap Set.toList . smembers . under k.kids)
      _ <- withdrawn k orphaned
      -- the new blocks: what they confirm leaves alone
      forM_ change.confirmed (remove k True)
      -- what waited for a transaction that they confirm was replaced
      forM_ change.confirmed $ \t -> do
        waiting <- smembers (under k.lostby t)
        forM_ (Set.toList waiting) $ \w -> do
          value <- hget k.lost w
          forM_ value (replaced k w)
          hdel k.lost w
          zrem k.lostat w
        del (under k.lostby t)
      -- what spends what they spend leaves with all that spends it, and is
      -- kept as replaced
      forM_ (concatMap (pieces (2 * k.tsize + 8)) change.spent) $ \item -> do
        let (o, by) = B.splitAt (k.tsize + 4) item
        s <- hget k.spender o
        forM_ s $ \rival -> do
          left <- withdrawn k [rival]
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
          forM_ value $ \v -> srem (under k.lostby (B.take k.tsize v)) t
          hdel k.lost t
          zrem k.lostat t
      settle False before after
      set k.tip target
      incrBy k.moves 1
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
