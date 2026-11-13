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
-- addresses. It is built on the shared pool, and described in "The cache
-- of extended public keys of a fleet" in @docs\/layered-database.md@.
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
--   those addresses and the outputs that it pays to them. The script of the
--   pool puts them in and takes them out in the step in which it changes
--   the pool, so this layer is the pool, always.
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
--   back to it, as when a block is reverted and mined again, and on the
--   way the pool takes that block's transactions out of the entries and
--   puts them back as unconfirmed. When the tip moves, the store that moved it
--   brings the cache there, with what the blocks in between did to the
--   addresses that entries have ('cacheAdvance'). Until it has, every read
--   is a miss and the store answers from its database. If it never does, or
--   the chain was reorganised, or the pool was emptied, the cache is
--   emptied: the next move finds it at a block that it does not start from.
--
-- * __The addresses of a key depend on which are used__, and the pool
--   counts. So every read of balances runs the scan again over what the
--   entry has ('window'), and is a miss unless the scan reads exactly the
--   addresses of the entry. The other reads say how many addresses the
--   caller found, and miss if the entry has another number.
--
-- The alternative that this replaces, "Haskoin.Store.Cache", keeps one
-- store's view of each key and repairs it from that store's events. It is
-- still what a store that is not in a fleet uses.
--
-- = What it costs
--
-- An entry is built from the database by the store that missed, in the
-- background, a few addresses at a time ('cacheBuild'); when a key gains
-- addresses only those are read. Each block costs one question to Redis
-- about the addresses that it touches, and a write for those that entries
-- have.
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
    cacheDrop,
    cacheHave,
    CacheMoved (..),
    AddressChange (..),
    blockChanges,
    cacheTracked,
    cacheAdvance,
    cacheEvict,
    cacheTip,
    cacheSize,

    -- * Encoding
    xpubId,
    score,
    position,
    nothingPool,
  )
where

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
    Ctx,
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
import Haskoin.Store.Common (Limits (..), StoreRead (..), scanXPubBals)
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
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered (From (..))
import Haskoin.Store.Database.Query.Pooled (PoolReads (..), pooledRead)
import Haskoin.Store.Database.Reader (DatabaseReader (..))
import Haskoin.Store.Database.Types (MemSummary (..))
import Haskoin.Store.Fleet.Redis (FleetKeys (..))

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
-- It is the score of "Haskoin.Store.Cache", as a whole number: height and
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

-- | A pool that has nothing. The queries of
-- "Haskoin.Store.Database.Query.Pooled" with it answer from the chain
-- alone, which is how the chain's layer of an entry is read.
nothingPool :: PoolReads
nothingPool =
  PoolReads
    { summaries = return . map (const Nothing),
      addressTxs = \_ _ _ -> return [],
      addressOuts = \_ _ _ -> return [],
      spentOutputs = \_ ps -> return (map (const False) ps),
      bodies = return . map (const (Nothing, IntMap.empty)),
      spenders = return . map (const Nothing),
      page = \_ _ -> return [],
      startingWith = \_ -> return [],
      replaced = \_ -> return Nothing
    }

-------------------------------------------------------------------------------
-- Reading
-------------------------------------------------------------------------------

-- | What a read of the cache gave.
data Cached a
  = -- | the answer, which is the one that the database gives at the fleet
    -- tip with the pool on top
    Hit !a
  | -- | the cache cannot answer: ask the database
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
    paths :: HashMap Address [Word32]
  }
  deriving (Eq, Show)

-- | The balances of a key at the fleet tip, with the pool applied, if the
-- cache has the key with exactly the addresses that a scan of it reads
-- now. Arguments: the tip that the request answers from, the initial gap
-- and the gap of the store, and the time in milliseconds, by which the
-- cache knows which keys are in use.
cachedEntry :: Connection -> FleetKeys -> ByteString -> Word32 -> Word32 -> Word64 -> XPubSpec -> IO (Cached Entry)
cachedEntry conn keys tip igap gap now spec =
  ask conn keys "bals" [tip, xpubId spec, num now] <&> \case
    Hit rows -> maybe Miss Hit (entry (mapMaybe row (groups 3 rows)))
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
      return (fromIntegral m, n, maybe chain (M.applySummary chain) (summary sums))
    row _ = Nothing
    summary sums = case map number (C.words sums) of
      [txs, paid, paidCount, spent, spentCount, both, bothCount] -> Just MemSummary {..}
      _ -> Nothing
    entry rows = do
      balances <- window igap gap rows
      let count m = length [() | XPubBal {path = m' : _} <- balances, m' == m]
      return
        Entry
          { balances,
            sizes = (count 0, count 1),
            paths = HashMap.fromList [(b.balance.address, b.path) | b <- balances]
          }

-- | The balances of a key as a scan of it gives them, from the balances of
-- the addresses that an entry has, each with the chain that it is in and
-- its number there: if the scan, run over them, reads exactly those
-- addresses. 'Nothing' if it would read one that the entry does not have,
-- or would stop before one that it has.
--
-- This is what makes an entry safe against the gap. The addresses of a key
-- are those up to the last one that is used and so many more, and a
-- payment in the pool counts as use. An entry is built for the addresses
-- of one moment; after a payment to one of its unused addresses the key
-- has more, and after such a payment is dropped it has fewer. Either way
-- this gives 'Nothing', and the store answers from the database and
-- builds the entry again.
window :: Word32 -> Word32 -> [(Word32, Word32, Balance)] -> Maybe [XPubBal]
window igap gap rows
  | not (contiguous ext && contiguous chg) = Nothing
  | otherwise = do
      found <- scanXPubBals (traverse (`HashMap.lookup` known)) igap gap (ext <> [beyond]) (chg <> [beyond])
      if length found == HashMap.size known then Just found else Nothing
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
cachedTxs :: Connection -> FleetKeys -> ByteString -> XPubSpec -> (Int, Int) -> From -> Limits -> IO (Cached [TxRef])
cachedTxs conn keys tip spec (n0, n1) from limits =
  ask conn keys "txs" (who <> place from encode <> [num limits.offset, num limits.limit]) <&> fmap refs
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
  IO (Cached [XPubUnspent])
cachedUnspents conn keys tip spec e from limits =
  ask conn keys "outs" (who <> place from first <> [num limits.offset, num limits.limit]) <&> \case
    Hit rows -> maybe Miss Hit (mapM output (groups 3 rows))
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
cachedCount :: Connection -> FleetKeys -> ByteString -> XPubSpec -> (Int, Int) -> IO (Cached Word32)
cachedCount conn keys tip spec (n0, n1) =
  ask conn keys "count" [tip, xpubId spec, num n0, num n1] <&> \case
    Hit [n] -> Hit (number n)
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
ask :: Connection -> FleetKeys -> ByteString -> [ByteString] -> IO (Cached [ByteString])
ask conn keys what args =
  run conn (R.eval xpubScript [] (what : keys.prefix : "32" : args)) >>= \case
    "ok" : rows -> return (Hit rows)
    ["miss"] -> return Miss
    ["raced"] -> return Moved
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
  deriving (Eq, Show)

-- | The numbers of addresses that the cache has for a key in each of its
-- two chains, both zero if it does not have the key.
cacheHave :: Connection -> FleetKeys -> XPubSpec -> IO (Int, Int)
cacheHave conn keys spec =
  run conn (R.eval xpubScript [] ["have", keys.prefix, "32", xpubId spec]) >>= \case
    [n0, n1] -> return (fromInteger n0, fromInteger n1)
    other -> fail ("Unexpected answer of the xpub script: " <> show (other :: [Integer]))

-- | Delete the entry of a key.
cacheDrop :: Connection -> FleetKeys -> XPubSpec -> IO ()
cacheDrop conn keys spec = do
  _ <- run conn (R.eval xpubScript [] ["drop", keys.prefix, "32", xpubId spec]) :: IO [ByteString]
  return ()

-- | Build the entry of a key, or add to it the addresses that the key has
-- gained: given the tip that the fleet is at, the database as it was at
-- that block, the pool, and the time in milliseconds.
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
cacheBuild :: Connection -> FleetKeys -> ByteString -> DatabaseReader -> PoolReads -> Word64 -> XPubSpec -> IO Built
cacheBuild conn keys tip r pool now spec = do
  bals <- (pooledRead r pool).xPubBals spec
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
    chain = pooledRead r nothingPool
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
          args =
            [tip, xpubId spec, num n0, num n1, num now, num (length now')]
              <> concatMap address now'
          count m = length [() | (XPubBal {path = m' : _}, _, _, _) <- now', m' == m]
      run conn (R.eval xpubScript [] ("build" : keys.prefix : "32" : args)) >>= \case
        ["ok"] -> go (n0 + count 0, n1 + count 1) (added + length now') later
        [why] | why `elem` ["raced", "behind", "changed"] -> return Abandoned
        other -> fail ("Unexpected answer of the xpub script: " <> show (take 1 (other :: [ByteString])))
    address (b, bal, txs, outs) =
      [encode b.balance.address, path b.path, maybe "" encode (bal >>= real)]
        <> [num (length txs)]
        <> concat [[encode t.txid, num (score t.block)] | t <- txs]
        <> [num (length outs)]
        <> concat [[encode u.outpoint, num (score u.block), num u.value, u.script] | u <- outs]
    real b = if nullBalance b then Nothing else Just b
    path = \case
      [m, n] -> runPut (putWord8 (fromIntegral m) >> putWord32be n)
      _ -> ""

-- | About how many transactions and outputs one write of 'cacheBuild'
-- carries.
chunk :: Int
chunk = 5000

-- | What became of an attempt to bring the cache to a block.
data CacheMoved
  = -- | the cache is at the block now, with what the blocks did
    CacheMoved
  | -- | it was there already
    CacheStays
  | -- | the fleet tip is not that block any more: whoever moves it next
    -- sees to the cache
    CacheRaced
  | -- | the cache was emptied and starts again at the block
    CacheEmptied
  deriving (Eq, Show)

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
-- spends is in both lists of its address: 'cacheAdvance' takes out what
-- was spent after it puts in what was paid.
blockChanges :: Network -> Ctx -> [TxData] -> HashMap Address AddressChange
blockChanges net ctx tds = HashMap.fromListWith (flip (<>)) (concatMap one tds)
  where
    one td =
      let h = txHash td.tx
          address = either (const Nothing) Just . scriptToAddressBS net ctx
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
        <$> run conn (R.eval xpubScript [] ("tracked" : keys.prefix : "32" : map encode some))
    pieces [] = []
    pieces xs = let (now, later) = splitAt 4000 xs in now : pieces later

-- | Bring the cache from the block that it is taken to be at to the block
-- that the fleet tip has moved to: given what the blocks in between did at
-- the addresses that entries have, each with its balance in the chain at
-- the new block, or 'Nothing' to empty the cache instead, as a store does
-- that cannot say what they did.
--
-- The cache is also emptied when it is found at another block than the
-- one given: a store moved the tip and did not get to bring the cache
-- along, and what the blocks since did to it is not known here.
cacheAdvance ::
  Connection ->
  FleetKeys ->
  Maybe ByteString ->
  ByteString ->
  Maybe [(Address, Maybe Balance, AddressChange)] ->
  IO CacheMoved
cacheAdvance conn keys from to changes =
  run conn (R.eval xpubScript [] ("advance" : keys.prefix : "32" : args)) >>= \case
    ["moved"] -> return CacheMoved
    ["stays"] -> return CacheStays
    ["raced"] -> return CacheRaced
    ["emptied"] -> return CacheEmptied
    other -> fail ("Unexpected answer of the xpub script: " <> show (take 1 (other :: [ByteString])))
  where
    args =
      [fromMaybe "" from, to, maybe "1" (const "0") changes, num (maybe 0 length changes)]
        <> concatMap one (fromMaybe [] changes)
    one (a, bal, c) =
      [encode a, maybe "" encode (bal >>= real), num (length c.txs)]
        <> concat [[encode h, num (score ref)] | (h, ref) <- c.txs]
        <> [num (length c.paid)]
        <> concat [[encode p, num (score ref), num v, s] | (p, ref, v, s) <- c.paid]
        <> [num (length c.spent)]
        <> map encode c.spent
    real b = if nullBalance b then Nothing else Just b

-- | Delete the least recently used entries past so many. Says how many it
-- deleted.
cacheEvict :: Connection -> FleetKeys -> Int -> IO Int
cacheEvict conn keys most =
  fromInteger <$> run conn (R.eval xpubScript [] ["evict", keys.prefix, "32", num most])

-- | How large the cache is: the number of its entries, one for each
-- extended public key, and the number of addresses that they have between
-- them, which is what its memory in Redis goes with, together with the
-- transactions of those addresses.
cacheSize :: Connection -> FleetKeys -> IO (Int, Int)
cacheSize conn keys =
  run conn (R.eval xpubScript [] ["size", keys.prefix, "32"]) >>= \case
    [entries, addresses] -> return (fromInteger entries, fromInteger addresses)
    other -> fail ("Unexpected answer of the xpub script: " <> show (other :: [Integer]))

-- | The block that the cache is at, if it is where the fleet tip is: at
-- the same block, by the same move of the tip.
cacheTip :: Connection -> FleetKeys -> IO (Maybe ByteString)
cacheTip conn keys =
  run conn (R.eval xpubScript [] ["where", keys.prefix, "32"]) <&> \block ->
    if B.null block then Nothing else Just block

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

-- | The script of the cache; see its header for what it takes and gives.
xpubScript :: ByteString
xpubScript = $(makeRelativeToProject "src/Haskoin/Store/Fleet/lua/xpub.lua" >>= embedFile)
