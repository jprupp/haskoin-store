{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

-- |
-- Module      : Haskoin.Store.Fleet.XPub
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The cache of extended public keys of a fleet of stores: what a store that
-- answers as a member of a fleet ("Haskoin.Store.Fleet.Member") keeps in
-- the Redis that the fleet shares, so that a request about a wallet is one
-- or two reads of Redis instead of a read of the database for each of its
-- addresses. It is built on the shared pool, and described in
-- @docs\/one-fleet.md@ and @docs\/xpub-indexing.md@.
--
-- = What an entry is
--
-- An entry belongs to one extended public key and has, for the addresses
-- that a scan of the key reads ('Haskoin.Store.Common.scanXPubBals'):
--
-- * __the chain's layer__: the balance of each address in the chain, the
--   transactions of all of them, and their unspent outputs, as they are at
--   one block, which is the same for every entry and is written next to
--   them;
--
-- * __the pool's layer__: the transactions of the shared pool that touch
--   those addresses and the outputs that it pays to them. The pool puts
--   them in and takes them out in the step in which it changes, so this
--   layer is the pool, always.
--
-- Transactions and outputs of both layers are in one list each, ordered as
-- the API orders them, so a page is a range of a sorted set. A balance is
-- the chain's with the summary that the pool keeps for the address.
--
-- = Why it cannot give another answer than the database
--
-- A store of a fleet answers from the chain as it was at the fleet tip
-- with the pool on top ("Haskoin.Store.Database.Query.Pooled"). An entry is
-- that same answer worked out beforehand, and it is used only while that
-- holds:
--
-- * __The chain's layer follows from the chain__ at a block and the
--   addresses. Any store computes the same, so it does not matter which one
--   writes, and there is nothing for stores to disagree on.
--
-- * __A read answers only where the cache is__, which is a move of the
--   fleet tip and the block that it went to, and only while the tip is
--   there. A block alone would not do: the tip can leave a block and come
--   back to it, as when a block is reverted and mined again.
--
-- * __The tip and the cache move in one commit.__ The store that moves the
--   tip first writes what the blocks in between did at the addresses that
--   entries have ('tipMoves'), in pieces that mark the entries they touch
--   ('cachePieces'); readers wait for a marked entry. Then one step moves
--   the pool, the fleet tip and the cache together, gives each marked entry
--   a new version and clears the marks ('Haskoin.Store.Fleet.Redis.fleetCommit').
--   No request finds the tip without its cache, or an entry half moved.
--
-- * __A reorganisation is undone address by address__ ('tipMoves'), from
--   the views of the database at the old tip and the new one, and never
--   empties the cache. Only a move that empties the pool, or one whose
--   store cannot say what the abandoned blocks held, empties it too.
--
-- * __The addresses of a key depend on which are used__, and the pool
--   counts. So every read of balances runs the scan again over what the
--   entry has ('window'). An entry also has its lookahead, the next gap's
--   worth of addresses, indexed at build time ('lookahead'), so that a
--   payment that moves the gap is in the entry at once, through the pool,
--   and the entry still answers. A read misses only when the scan needs an
--   address that the entry lacks; when the lookahead runs low, the indexer
--   is asked to extend it, and reads go on meanwhile. The other reads say
--   how many addresses the entry has, and miss if it has another number.
--
-- = What it costs
--
-- An entry is built from the database by the store that missed, in the
-- background, a few addresses at a time ('cacheBuild'); when a key gains
-- addresses only those are read. Each move of the tip costs one question
-- to Redis about the addresses that its blocks touch, and a write for those
-- that entries have.
--
-- = Which entries stay
--
-- Every key that a request asks about is cached, and the cache evicts by
-- GreedyDual ('Haskoin.Store.XPub.greedyLua'): each
-- entry has what it cost to build ('Haskoin.Store.XPub.indexCost'), a use
-- gives it the level of the cache plus that cost, and the entry of the
-- lowest priority goes first, the level rising to it ('cacheEvict'). A
-- wallet of 40,000 addresses outlives an empty one used as recently, and
-- either goes once nobody asks for it.
module Haskoin.Store.Fleet.XPub
  ( -- * Reading
    Cached (..),
    Entry (..),
    cachedEntry,
    cachedTxs,
    cachedUnspents,
    cachedCount,
    window,

    -- * Writing
    Built (..),
    cacheBuild,
    lookahead,
    cacheDrop,
    cacheHave,
    cacheEvict,
    cacheSize,

    -- * Moving the tip
    AddressChange (..),
    blockChanges,
    cacheTracked,
    Begun (..),
    cacheBegin,
    AddressMove (..),
    tipMoves,
    cachePieces,
    cacheAbort,

    -- * Encoding
    xpubId,
    score,
    position,
  )
where

import Control.Concurrent (threadDelay)
import Control.Monad (forM)
import Data.Bits (shift, (.&.), (.|.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.Default (def)
import Data.FileEmbed (embedFile, makeRelativeToProject)
import Data.Functor ((<&>))
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.Int (Int64)
import Data.IntMap.Strict qualified as IntMap
import Data.List (sortOn)
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Serialize (decode, encode, runPut)
import Data.Serialize.Put (putWord32be, putWord8)
import Data.Text.Encoding (encodeUtf8)
import Data.Word (Word32, Word64)
import Database.Redis (Connection, Redis, Reply)
import Database.Redis qualified as R
import Haskoin
  ( Address (..),
    Network,
    OutPoint (..),
    Tx (..),
    TxHash,
    TxIn (..),
    TxOut (..),
    addressHash,
    encodeHex,
    nullOutPoint,
    scriptToAddressBS,
    sha256,
    txHash,
  )
import Haskoin.Store.Common (Limits (..), StoreRead (..), nub', scanXPubBals)
import Haskoin.Store.Data
  ( Balance (..),
    BlockRef (..),
    Prev (..),
    TxData (..),
    TxRef (..),
    Unspent (..),
    XPubBal (..),
    XPubSpec (..),
    XPubUnspent (..),
    nullBalance,
    zeroBalance,
  )
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Pooled (From (..), PoolReads (..), applySummary, emptyPool, pooledRead)
import Haskoin.Store.Database.Reader (DatabaseReader (..), Derived (..), derivedAddrs)
import Haskoin.Store.Database.Types (MemSummary (..))
import Haskoin.Store.Fleet.Atomic (Atomic)
import Haskoin.Store.Fleet.Atomic.Redis (insist, transact)
import Haskoin.Store.Fleet.Redis (FleetKeys (..), Script, evalScript, script)
import Haskoin.Store.Fleet.XPub.Decide qualified as X
import Haskoin.Store.XPub (greedyLua, indexCost)

-------------------------------------------------------------------------------
-- Encoding
-------------------------------------------------------------------------------

-- | What an extended public key is called in Redis: the start of a hash of
-- the key and of the kind of its addresses.
xpubId :: XPubSpec -> ByteString
xpubId = encodeUtf8 . encodeHex . B.take 16 . encode . sha256 . encode

-- | The score of a position in the sorted sets of an entry, such that
-- sorting by score puts the newest first, as the API lists transactions:
-- those of the pool have the negative of the time at which they entered
-- it, and those of the chain a positive number that goes down with height
-- and position in the block. Between equal scores Redis orders members by
-- their bytes, which for hashes and outputs is the order of the database
-- ('Haskoin.Store.Common.txRefKey', 'Haskoin.Store.Common.unspentKey').
--
-- It is a whole number: height and
-- position fit in the 53 bits that Redis keeps exactly.
score :: BlockRef -> Int64
score BlockRef {height = h, position = p} = 0x001fffffffffffff - (h' .|. p')
  where
    h' = (fromIntegral h .&. 0x07ffffff) `shift` 26
    p' = fromIntegral p .&. 0x03ffffff
score MemRef {timestamp = t} = negate (fromIntegral (t .&. 0x001fffffffffffff))

-- | The position that has a score: the inverse of 'score'.
position :: Int64 -> BlockRef
position s
  | s < 0 = MemRef {timestamp = fromIntegral (negate s)}
  | otherwise = BlockRef {height = fromIntegral (m `shift` (-26)), position = fromIntegral (m .&. 0x03ffffff)}
  where
    m = 0x001fffffffffffff - s

-------------------------------------------------------------------------------
-- Reading
-------------------------------------------------------------------------------

-- | What a read of the cache gave.
data Cached a
  = -- | the answer, which is the one that the database gives at the fleet
    -- tip with the pool on top
    Hit !a
  | -- | the cache cannot answer: ask the indexer
    Miss
  | -- | the fleet tip is another block than the one that the request
    -- answers from: answer again from the new tip
    Moved
  deriving (Eq, Show, Functor)

-- | The balances of a cached key, and what the other reads need of them.
data Entry = Entry
  { -- | the balances, as 'Haskoin.Store.Common.StoreRead' gives them
    balances :: ![XPubBal],
    -- | the number of addresses in each of the two chains of the key
    sizes :: !(Int, Int),
    -- | where each address is in the key. It is worked out when it is
    -- asked for, which only a list of unspent outputs does.
    paths :: HashMap Address [Word32],
    -- | the version of the entry: every change of it stamps a new one
    version :: !ByteString,
    -- | whether the lookahead of the entry is less than half the gap in a
    -- chain: payments used it up, and the indexer is to extend it
    short :: !Bool
  }
  deriving (Eq, Show)

-- | The balances of a key at the fleet tip, with the pool applied, if the
-- cache has the key with exactly the addresses that a scan of it reads
-- now. Arguments: the tip that the request answers from, the initial gap
-- and the gap of the store, and whether the read is a use of the key,
-- which renews its priority in the cache (GreedyDual).
cachedEntry :: Connection -> FleetKeys -> ByteString -> Word32 -> Word32 -> Bool -> XPubSpec -> IO (Cached Entry)
cachedEntry conn keys tip igap gap used spec =
  ask conn keys "bals" [tip, xpubId spec, if used then "1" else ""] <&> \case
    Hit (ver : rows) -> maybe Miss Hit (entry ver (mapMaybe row (groups 3 rows)))
    Hit [] -> Miss
    Miss -> Miss
    Moved -> Moved
  where
    row [a, kept, sums] = do
      address <- either (const Nothing) Just (decode a)
      let (path, bal) = B.splitAt 5 kept
      [m, _, _, _, _] <- Just (B.unpack path)
      n <- either (const Nothing) Just (decode (B.drop 1 path)) :: Maybe Word32
      let stored = if B.null bal then Nothing else either (const Nothing) Just (decode bal)
          chain = fromMaybe (zeroBalance address) stored
      return (fromIntegral m, n, maybe chain (applySummary chain) (summary sums))
    row _ = Nothing
    summary sums = case map number (C.words sums) of
      [txs, paid, paidCount, spent, spentCount, both, bothCount] -> Just MemSummary {..}
      _ -> Nothing
    entry version rows = do
      balances <- window igap gap rows
      -- the numbers of the entry, its lookahead included, which the other
      -- reads check
      let held m = length [() | (m', _, _) <- rows, m' == m]
          scanned m = length [() | XPubBal {path = m' : _} <- balances, m' == m]
          ahead m = held m - scanned m
      return
        Entry
          { balances,
            sizes = (held 0, held 1),
            paths = HashMap.fromList [(b.balance.address, b.path) | b <- balances],
            version,
            short = any (\m -> ahead m < fromIntegral gap `div` 2) [0, 1]
          }

-- | The balances of a key as a scan of it gives them, from the balances of
-- the addresses that an entry has, each with the chain that it is in and
-- its number there: if the scan, run over them, reads none that the entry
-- lacks, and every address of the entry that it does not read is unused.
-- 'Nothing' otherwise.
--
-- This is what makes an entry safe against the gap. The addresses of a key
-- are those up to the last one that is used and so many more, and a
-- payment in the pool counts as use. An entry is built with the addresses
-- of one moment and its lookahead, the next gap's worth after them in each
-- chain ('lookahead'), indexed as the others are, so that the pool puts a
-- payment to one of them in the entry at once. After such a payment the
-- key has more addresses, and the entry still has them; after a payment
-- is dropped it has fewer, and the rest are unused. Only a key that needs
-- more than the entry has, or an entry with a used address past what the
-- scan reads, gives 'Nothing', and the entry is built again.
window :: Word32 -> Word32 -> [(Word32, Word32, Balance)] -> Maybe [XPubBal]
window igap gap rows
  | not (contiguous ext && contiguous chg) = Nothing
  | otherwise = do
      found <- scanXPubBals (traverse (`HashMap.lookup` known)) igap gap (ext <> [beyond]) (chg <> [beyond])
      let read' = HashMap.fromList [(b.balance.address, ()) | b <- found]
          unread = [b | (_, _, b) <- rows, not (HashMap.member b.address read')]
      if all ((== 0) . (.txs)) unread then Just found else Nothing
  where
    chain m = sortOn fst [(n, b.address) | (m', n, b) <- rows, m' == m]
    ext = chain 0
    chg = chain 1
    contiguous as = map fst as == take (length as) [0 ..]
    known = HashMap.fromList [(b.address, b) | (_, _, b) <- rows]
    -- an address that the entry does not have, after the last of each
    -- chain: a scan that wants more than the entry has asks for it
    beyond = (maxBound, other 0)
    other :: Word32 -> Address
    other i =
      let a = PubKeyAddress (addressHash (encode i))
       in if HashMap.member a known then other (i + 1) else a

-- | A page of the transactions of a cached key, newest first, from where
-- the page starts, given the numbers of addresses that the caller has for
-- the key ('sizes').
cachedTxs ::
  Connection ->
  FleetKeys ->
  ByteString ->
  XPubSpec ->
  (Int, Int) ->
  From ->
  Limits ->
  IO (Cached (ByteString, [TxRef]))
cachedTxs conn keys tip spec (n0, n1) from limits =
  ask conn keys "txs" (who <> place from encode <> [num limits.offset, num limits.limit]) <&> \case
    Hit (v : rows) -> Hit (v, refs rows)
    Hit [] -> Miss
    Miss -> Miss
    Moved -> Moved
  where
    who = [tip, xpubId spec, num n0, num n1]
    refs rows =
      [ TxRef {block = position s, txid = h}
      | [m, sc] <- groups 2 rows,
        Right h <- [decode m],
        Just s <- [integer sc]
      ]

-- | A page of the unspent outputs of a cached key, newest first, each with
-- where its address is in the key ('paths').
cachedUnspents ::
  Connection ->
  FleetKeys ->
  ByteString ->
  XPubSpec ->
  Entry ->
  From ->
  Limits ->
  IO (Cached (ByteString, [XPubUnspent]))
cachedUnspents conn keys tip spec e from limits =
  ask conn keys "outs" (who <> place from first <> [num limits.offset, num limits.limit]) <&> \case
    Hit (v : rows) -> maybe Miss (Hit . (v,)) (mapM output (groups 3 rows))
    Hit [] -> Miss
    Miss -> Miss
    Moved -> Moved
  where
    (n0, n1) = e.sizes
    who = [tip, xpubId spec, num n0, num n1]
    -- the outputs of the transaction that a page starts at come first
    -- among those of its second
    first h = encode (OutPoint h 0)
    output [o, sc, kept] = do
      outpoint <- either (const Nothing) Just (decode o)
      s <- integer sc
      let (value, more) = C.break (== ' ') kept
          (size, rest) = C.break (== ' ') (B.drop 1 more)
          (a, script) = B.splitAt (number size) (B.drop 1 rest)
      address <- either (const Nothing) Just (decode a)
      path <- HashMap.lookup address e.paths
      return
        XPubUnspent
          { path,
            unspent = Unspent {block = position s, outpoint, value = number value, script, address = Just address}
          }
    output _ = Nothing

-- | The number of transactions of a cached key.
cachedCount :: Connection -> FleetKeys -> ByteString -> XPubSpec -> (Int, Int) -> IO (Cached (ByteString, Word32))
cachedCount conn keys tip spec (n0, n1) =
  ask conn keys "count" [tip, xpubId spec, num n0, num n1] <&> \case
    Hit [v, n] -> Hit (v, number n)
    Hit _ -> Miss
    Miss -> Miss
    Moved -> Moved

-- | Where a page starts, as the arguments of the script, given the bytes
-- that the members of the score of a transaction are to be no smaller
-- than.
place :: From -> (TxHash -> ByteString) -> [ByteString]
place from bytes = case from of
  Top -> ["top", "", ""]
  InMempool time h -> ["at", num (score (MemRef time)), bytes h]
  InChain ref -> ["score", num (score ref), ""]

-- | Ask the script something that it answers with rows.
--
-- An entry that a move of the tip is writing ('cachePieces') is read when
-- the move is done: the read waits a few milliseconds at a time, for two
-- seconds at most, and then says that the tip moved, which has the request
-- answered again.
ask :: Connection -> FleetKeys -> ByteString -> [ByteString] -> IO (Cached [ByteString])
ask conn keys what args = go (400 :: Int)
  where
    go tries =
      run conn (evalScript xpubScript [] (what : keys.prefix : "32" : args)) >>= \case
        "ok" : rows -> return (Hit rows)
        ["miss"] -> return Miss
        ["raced"] -> return Moved
        ["marked"]
          | tries > 0 -> threadDelay 5000 >> go (tries - 1)
          | otherwise -> return Moved
        other -> fail ("Unexpected answer of the xpub script: " <> show (take 1 other))

-------------------------------------------------------------------------------
-- Writing
-------------------------------------------------------------------------------

-- | What became of an attempt to build an entry.
data Built
  = -- | the entry has the addresses of the key now, of which so many were
    -- added
    Built !Int
  | -- | the fleet tip moved, the cache is not at the tip, or another store
    -- wrote the entry meanwhile: nothing more was written
    Abandoned
  | -- | a move of the tip is under way: the build is to be tried again
    -- when it is done
    Busy
  deriving (Eq, Show)

-- | The numbers of addresses that the cache has for a key in each of its
-- two chains, both zero if it does not have the key.
cacheHave :: Connection -> FleetKeys -> XPubSpec -> IO (Int, Int)
cacheHave conn keys spec =
  run conn (evalScript xpubScript [] ["have", keys.prefix, "32", xpubId spec]) >>= \case
    [n0, n1] -> return (fromInteger n0, fromInteger n1)
    other -> fail ("Unexpected answer of the xpub script: " <> show (other :: [Integer]))

-- | Delete the entry of a key.
cacheDrop :: Connection -> FleetKeys -> XPubSpec -> IO ()
cacheDrop conn keys spec = insist conn (X.drop (X.cacheKeys keys.prefix) (xpubId spec))

-- | Build the entry of a key, or add to it the addresses that the key has
-- gained: given the tip that the fleet is at, the database as it was at
-- that block, and the pool. A build is for a request that missed, and is a
-- use of the key: its priority in the cache becomes the level plus what the
-- entry cost to read, its addresses, transactions and outputs at the
-- prices of 'Haskoin.Store.XPub.indexCost', added up over its writes.
--
-- The addresses of the key are read as a request reads them, from the
-- chain with the pool on top. An entry that has some of them keeps what it
-- has and gets the others; one that has an address that the key no longer
-- has is deleted and built again. The chain's part of each new address is
-- read from the database, and sent a few addresses at a time, so that no
-- one write holds Redis for long: until the last is in, the entry has
-- fewer addresses than the key, and every read of it misses.
--
-- Each write is taken only while the fleet tip, the block of the cache
-- and the addresses of the entry are what this store found.
cacheBuild :: Connection -> FleetKeys -> ByteString -> DatabaseReader -> PoolReads -> XPubSpec -> IO Built
cacheBuild conn keys tip r pool spec = do
  bals <- (pooledRead r pool).xPubBals spec >>= lookahead r pool spec
  let wanted m = [b | b@XPubBal {path = m' : _} <- bals, m' == m]
      (w0, w1) = (length (wanted 0), length (wanted 1))
  (h0, h1) <- cacheHave conn keys spec
  (n0, n1) <-
    if h0 > w0 || h1 > w1
      then cacheDrop conn keys spec >> return (0, 0)
      else return (h0, h1)
  let new = drop n0 (wanted 0) <> drop n1 (wanted 1)
  go (n0, n1) 0 =<< mapM chainOf new
  where
    chain = pooledRead r emptyPool
    chainOf b = do
      let a = b.balance.address
      bal <- Q.getBalance r a
      txs <- chain.getAddressTxs a def
      outs <- chain.getAddressUnspents a def
      return (b, bal, txs, outs)
    weight (_, _, txs, outs) = 1 + length txs + length outs
    -- as many addresses as hold about 'chunk' rows between them
    batch _ [] = ([], [])
    batch room (x : xs)
      | room <= 0 = ([], x : xs)
      | otherwise = let (now', later) = batch (room - weight x) xs in (x : now', later)
    go _ added [] = return (Built added)
    go (n0, n1) added todo = do
      let (now', later) = batch chunk todo
          cost =
            indexCost
              (length now')
              (sum [length txs | (_, _, txs, _) <- now'])
              (sum [length outs | (_, _, _, outs) <- now'])
              0
          additions = map address now'
          count m = length [() | (XPubBal {path = m' : _}, _, _, _) <- now', m' == m]
          step :: (Monad m) => Atomic m X.Step
          step =
            X.build
              (X.cacheKeys keys.prefix)
              tip
              (xpubId spec)
              (toInteger n0, toInteger n1)
              (toInteger cost)
              additions
      -- one that other stores change the ground of five times is given up,
      -- as one that found the entry changed
      transact conn 5 step >>= \case
        Just X.StepAdded -> go (n0 + count 0, n1 + count 1) (added + length now') later
        Just X.StepBusy -> return Busy
        _ -> return Abandoned
    address (b, bal, txs, outs) =
      X.Addition
        { address = encode b.balance.address,
          path = path b.path,
          balance = maybe "" encode (bal >>= real),
          txs = [(encode t.txid, fromIntegral (score t.block)) | t <- txs],
          outs = [(encode u.outpoint, fromIntegral (score u.block), num u.value, u.script) | u <- outs]
        }
    real b = if nullBalance b then Nothing else Just b
    path = \case
      [m, n] -> runPut (putWord8 (fromIntegral m) >> putWord32be n)
      _ -> ""

-- | The addresses of a key as a scan gives them, with its lookahead: in
-- each chain, the next gap's worth of addresses after them, up to the first
-- that is used. Those are indexed with the others, so that a payment in the
-- pool to one of them is in the entry at once, and the entry still answers
-- when the payment moves the gap ('window'). A used address past the gap
-- is not one that the key has, and stops the lookahead.
lookahead :: DatabaseReader -> PoolReads -> XPubSpec -> [XPubBal] -> IO [XPubBal]
lookahead r pool spec bals = do
  Derived ext chg <- derivedAddrs r spec
  more <- forM [(0, ext), (1, chg)] $ \(m, derived) -> do
    let have = length [() | XPubBal {path = m' : _} <- bals, m' == m]
        next = take (fromIntegral r.maxGap) (drop have derived)
    found <- (pooledRead r pool).getBalances (map snd next)
    return
      . takeWhile ((== 0) . (.balance.txs))
      $ zipWith (\(n, _) b -> XPubBal {path = [m, n], balance = b}) next found
  return (bals <> concat more)

-- | About how many transactions and outputs one write of 'cacheBuild'
-- carries.
chunk :: Int
chunk = 5000

-- | What blocks did at an address: the transactions that they added to
-- it, the outputs that they paid to it, and its outputs that they spent.
data AddressChange = AddressChange
  { txs :: ![(TxHash, BlockRef)],
    paid :: ![(OutPoint, BlockRef, Word64, ByteString)],
    spent :: ![OutPoint]
  }
  deriving (Eq, Show)

instance Semigroup AddressChange where
  a <> b = AddressChange {txs = a.txs <> b.txs, paid = a.paid <> b.paid, spent = a.spent <> b.spent}

-- | What the transactions of some blocks, in their order, did at each
-- address that they touch. An output that one of them pays and a later one
-- spends is in both lists of its address ('tipMoves' looks each up at the
-- new tip).
blockChanges :: Network -> [TxData] -> HashMap Address AddressChange
blockChanges net tds = HashMap.fromListWith (flip (<>)) (concatMap one tds)
  where
    one td =
      let h = txHash td.tx
          address = either (const Nothing) Just . scriptToAddressBS net
          ins =
            [ (a, AddressChange {txs = [], paid = [], spent = [i.outpoint]})
            | (n, i) <- zip [0 ..] td.tx.inputs,
              i.outpoint /= nullOutPoint,
              Just prev <- [IntMap.lookup n td.prevs],
              Just a <- [address prev.script]
            ]
          outs =
            [ (a, AddressChange {txs = [], paid = [(OutPoint h n, td.block, o.value, o.script)], spent = []})
            | (n, o) <- zip [0 ..] td.tx.outputs,
              Just a <- [address o.script]
            ]
          touched = HashMap.keys (HashMap.fromList [(a, ()) | (a, _) <- ins <> outs])
       in [(a, AddressChange {txs = [(h, td.block)], paid = [], spent = []}) | a <- touched] <> ins <> outs

-- | Which of some addresses an entry of the cache has, in their order.
cacheTracked :: Connection -> FleetKeys -> [Address] -> IO [Bool]
cacheTracked conn keys as = concat <$> forM (pieces as) asked
  where
    asked some =
      map (== '1') . C.unpack
        <$> run conn (evalScript xpubScript [] ("tracked" : keys.prefix : "32" : map encode some))
    pieces [] = []
    pieces xs = let (now, later) = splitAt 4000 xs in now : pieces later

-- | What became of an attempt to begin a move of the cache.
data Begun
  = -- | the move is under way, under its token: builds wait for it
    Begun
  | -- | another store is moving the tip
    Moving
  | -- | the fleet tip is not the one that the store saw
    BeginRaced
  deriving (Eq, Show)

-- | Begin a move of the tip with the cache, under a token, from the tip
-- that the store saw: from here until the commit ('fleetCommit' in
-- "Haskoin.Store.Fleet.Redis") or 'cacheAbort', builds wait, so that every
-- entry is either one that the move brings along or one built after it. A
-- move whose store wrote nothing for ten seconds is aborted by whoever
-- comes across it.
cacheBegin :: Connection -> FleetKeys -> ByteString -> ByteString -> IO Begun
cacheBegin conn keys token seen =
  transact conn 5 (X.begin (X.cacheKeys keys.prefix) token seen) <&> \case
    Just X.BeganMove -> Begun
    Just X.TipElsewhere -> BeginRaced
    -- another store moves the tip, or others kept changing what this
    -- asked about: the store tries at its next turn
    _ -> Moving

-- | End a move of the cache that will not be committed, dropping the
-- entries that its pieces wrote, to be built again.
cacheAbort :: Connection -> FleetKeys -> ByteString -> IO ()
cacheAbort conn keys token = insist conn (X.abort (X.cacheKeys keys.prefix) token)

-- | What a move of the tip does to the chain's layer of an address that
-- entries have: its balance in the chain at the new tip, the transactions
-- of the abandoned blocks that touch it, those of the new blocks with
-- where they are, and, of the outputs that either touches, those that are
-- not unspent in the chain at the new tip and those that are.
data AddressMove = AddressMove
  { address :: !Address,
    balance :: !(Maybe Balance),
    abandoned :: ![TxHash],
    added :: ![(TxHash, BlockRef)],
    gone :: ![OutPoint],
    unspent :: ![Unspent]
  }
  deriving (Eq, Show)

-- | What a move of the tip does at the addresses that entries have, worked
-- out from the view of the database at the new tip, the transactions of
-- the abandoned blocks as the view at the old tip had them, and those of
-- the new blocks, both in the order of their blocks.
--
-- = Undone address by address
--
-- A reorganisation is undone at each address as it was done: the
-- transactions of the abandoned blocks come out, the new ones go in, and
-- every output that either touches is looked up at the new tip, so that
-- what the abandoned blocks paid goes, what they spent comes back unless
-- the new blocks spend it, and what the new blocks pay and do not spend
-- comes in. The balance is read at the new tip. Nothing else of an entry
-- changes, so no entry has to be emptied or built again.
tipMoves :: Connection -> FleetKeys -> DatabaseReader -> [TxData] -> [TxData] -> IO [AddressMove]
tipMoves conn keys r old new = do
  let undone = blockChanges r.net old
      done = blockChanges r.net new
      touched = HashMap.keys (HashMap.union (() <$ undone) (() <$ done))
  flags <- cacheTracked conn keys touched
  forM [a | (a, True) <- zip touched flags] $ \a -> do
    let none = AddressChange [] [] []
        u = HashMap.lookupDefault none a undone
        d = HashMap.lookupDefault none a done
        outs = nub' ([p | (p, _, _, _) <- u.paid <> d.paid] <> u.spent <> d.spent)
    bal <- Q.getBalance r a
    found <- forM outs $ \p -> (p,) <$> Q.getUnspent r p
    return
      AddressMove
        { address = a,
          balance = bal,
          abandoned = map fst u.txs,
          added = d.txs,
          gone = [p | (p, Nothing) <- found],
          unspent = [x | (_, Just x) <- found]
        }

-- | Write the moves of the addresses into their entries, a few at a time,
-- each piece marking the entries that it writes, which readers then wait
-- for until the commit. 'False' if the move was taken over or the tip
-- moved meanwhile: the caller aborts it.
cachePieces :: Connection -> FleetKeys -> ByteString -> ByteString -> [AddressMove] -> IO Bool
cachePieces conn keys token seen = go
  where
    go [] = return True
    go moves = do
      let (now', later) = batch chunk moves
          pieces = map one now'
          written :: (Monad m) => Atomic m Bool
          written = X.piece (X.cacheKeys keys.prefix) token seen pieces
      -- a piece that other stores change the ground of five times is not
      -- written, and the caller aborts the move
      transact conn 5 written >>= \case
        Just True -> go later
        _ -> return False
    weight x = 1 + length x.abandoned + length x.added + length x.gone + length x.unspent
    batch _ [] = ([], [])
    batch room (x : xs)
      | room <= 0 = ([], x : xs)
      | otherwise = let (now', later) = batch (room - weight x) xs in (x : now', later)
    one x =
      X.Piece
        { address = encode x.address,
          balance = maybe "" encode (x.balance >>= real),
          abandoned = map encode x.abandoned,
          added = [(encode h, fromIntegral (score ref)) | (h, ref) <- x.added],
          gone = map encode x.gone,
          unspent = [(encode o.outpoint, fromIntegral (score o.block), num o.value, o.script) | o <- x.unspent]
        }
    real b = if nullBalance b then Nothing else Just b

-- | Delete entries past so many by GreedyDual, those of the lowest priority
-- first, raising the level to each ('Haskoin.Store.Fleet.XPub.Decide.evict').
-- A use of an entry gave it the level plus its cost, so a large key
-- outlives small ones used as recently, and any key goes once it is not
-- used for long enough. Says how many it deleted.
cacheEvict :: Connection -> FleetKeys -> Int -> IO Int
cacheEvict conn keys most = fromInteger <$> insist conn (X.evict (X.cacheKeys keys.prefix) most)

-- | How large the cache is: the number of its entries, one for each
-- extended public key, and the number of addresses that they have between
-- them, which is what its memory in Redis goes with, together with the
-- transactions of those addresses.
cacheSize :: Connection -> FleetKeys -> IO (Int, Int)
cacheSize conn keys =
  run conn (evalScript xpubScript [] ["size", keys.prefix, "32"]) >>= \case
    [entries, addresses] -> return (fromInteger entries, fromInteger addresses)
    other -> fail ("Unexpected answer of the xpub script: " <> show (other :: [Integer]))

-------------------------------------------------------------------------------
-- Helpers
-------------------------------------------------------------------------------

num :: (Show a) => a -> ByteString
num = C.pack . show

number :: (Num a) => ByteString -> a
number = maybe 0 (fromInteger . fst) . C.readInteger

-- | A score as Redis prints it: a whole number, which every score of an
-- entry is, or, should Redis ever print a large one that way, a number in
-- the notation of floating point numbers. The first is read as a whole
-- number, since reading a floating point number from text takes several
-- microseconds, and a page has a score for each row.
integer :: ByteString -> Maybe Int64
integer bytes = case C.readInteger bytes of
  Just (n, rest) | B.null rest -> Just (fromInteger n)
  _ -> case reads (C.unpack bytes) :: [(Double, String)] of
    [(d, "")] -> Just (round d)
    _ -> Nothing

groups :: Int -> [a] -> [[a]]
groups n rows
  | null rows = []
  | otherwise = take n rows : groups n (drop n rows)

-- | Run Redis commands, throwing if Redis answers with an error.
run :: Connection -> Redis (Either Reply a) -> IO a
run conn f =
  R.runRedis conn f >>= \case
    Right x -> return x
    Left e -> fail ("Redis error: " <> show e)

-- | The script of the reads of the cache; see its header for what it
-- takes and gives. What writes the cache is
-- "Haskoin.Store.Fleet.XPub.Decide".
xpubScript :: Script
xpubScript = script (greedyLua <> $(makeRelativeToProject "src/Haskoin/Store/Fleet/lua/xpub.lua" >>= embedFile))
{-# NOINLINE xpubScript #-}
