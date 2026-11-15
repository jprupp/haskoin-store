-- |
-- Module      : Haskoin.Store.Database.Copy
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- A copy in memory of the mempool area of the database: every key and
-- value of its two column families, as RocksDB has them. The block store
-- answers its own reads of the area from it
-- ("Haskoin.Store.Database.Writer"). The API does not: its requests read
-- RocksDB as before.
--
-- = Why
--
-- A block at the tip of the chain takes its transactions out of the
-- mempool, and to do that it reads some fifty thousand keys of the area:
-- the body of each transaction, the mempool spender of each output, the
-- summary of each address. The area is small, less than a hundred
-- megabytes for 25,000 transactions, and all of it is in RocksDB's memory.
-- But a lookup in RocksDB still costs about eight microseconds of
-- processor time: the call into it, its memtables, its filters, a block to
-- find and to check. That was 180 to 460 ms of every block on a store of
-- mainnet, with the disk idle. A lookup here is a walk down a tree in the
-- heap, well under a microsecond.
--
-- = Why it can be right
--
-- The block store is the only writer of the database, and it writes one
-- batch at a time ('Haskoin.Store.Database.Writer.runWriterReport'). So
-- the area changes in one place, and that place changes the copy with it:
-- after a batch is written, its changes to the two column families are
-- applied to the copy ('applyChanges'), and the new copy takes the place
-- of the old in one step. The few other things that write to the area,
-- all of them when the store starts and before it takes anything from the
-- network (a migration, a wipe of the mempool, a repair of the
-- summaries), say that the copy is not known any more ('forgetCopy'), and
-- the next batch that needs it loads it again ('loadCopy').
--
-- = Bytes, and nothing about the keys
--
-- The copy holds the bytes of each key and of each value, and knows
-- nothing of what they mean. That keeps the statement that makes it right
-- short, and checkable: the copy has the keys and values that a scan of
-- the two column families gives. The tests check exactly that after every
-- operation of their random histories ('copyEntries'). A copy of decoded
-- values, a table for each kind of key, would answer some reads faster,
-- and would have to be kept right kind by kind.
--
-- = An ordered map, not changed in place
--
-- Each column family is a 'Map' from key to value, ordered by the bytes of
-- the keys as RocksDB orders them, so that the keys that start with some
-- bytes are found as a scan finds them ('prefixed'): the mempool spenders
-- of the outputs of a transaction, the list of the mempool by time. The
-- map is never changed in place. A change makes a new map that shares
-- most of the old one, and the reference to the copy is set to it. So a
-- reader takes the copy once and reads it from any number of threads,
-- with no lock, while it stays what it was.
--
-- A hash table of the keys, for the lookups by whole key, would have to
-- be kept next to the ordered map that the scans need. It was measured
-- and left out: @docs\/block-import.md@ has the numbers.
--
-- = Bytes that the collector may move
--
-- Keys and values are of the type 'ShortByteString': bytes in the
-- ordinary heap, with four words of overhead each. A 'ByteString' is
-- memory that the collector may not move, and has eight words of
-- overhead. Several hundred thousand small ones that live for hours,
-- made a few at a time between other allocations, keep whole blocks of
-- the heap from being freed for the sake of one value each. And a slice of
-- a larger buffer, such as the 256 KiB that a batch is serialised into,
-- would keep that whole buffer. So the bytes that the copy keeps are
-- always copied out of where they came from.
--
-- The price is that a value is copied once more when it is read, into a
-- 'ByteString' for its decoder, and that a full collection copies the
-- whole area, as it does everything else in the heap.
--
-- = The outputs that mempool transactions spend
--
-- The copy holds one more thing, which is not part of the area: for each
-- output that a mempool transaction spends and that the chain has as
-- unspent, the unspent output of the chain, as a t'SpentOutput'.
--
-- When a transaction enters the mempool, the store reads from the chain
-- the outputs that it spends. When a block confirms the transaction, the
-- import needs the same outputs again: their values and scripts are what
-- the balances are changed by, and their positions are in the keys that
-- it deletes. Reading them a second time was the largest part of the read
-- ahead of a block at the tip of the chain, 40 to 375 ms of each block on
-- a store of Bitcoin mainnet. So the batch that puts a transaction in the
-- mempool leaves what it read here, and the import of a block looks here
-- before it asks RocksDB
-- ('Haskoin.Store.Database.Writer.readAheadUnspents').
--
-- It is a cache, not a copy of anything: an output that is not here is
-- read from RocksDB, so having too few is slow and never wrong. Nothing is
-- kept across a restart: the table starts empty, and fills as the mempool
-- turns over. What must never happen is an entry that is not what the
-- chain has. Three rules see to that, all applied when a batch is written
-- ('applySpent'), in one step with the changes of the batch to the area:
--
-- * __An entry lives as long as the mempool spender of its output.__ It is
--   made when a batch gives the output a mempool spender, and it goes when
--   a batch takes the mempool spender away: the transaction was
--   confirmed, deleted or replaced. A block that spends an output takes
--   its mempool spender away in the same batch, as that is either a
--   transaction of the block or one that conflicts with it.
--
-- * __A batch that changes an unspent output of the chain changes the
--   entry__, or drops it if the output is gone. That is what a block does
--   that confirms a coinbase a second time, which moves the outputs of
--   the first to its own position while their mempool spenders stay.
--
-- * __A batch that reverts a block empties the table.__
--
-- And it is part of the copy, so whatever makes the copy forgotten
-- forgets the table: a write that failed, a wipe of the mempool, a
-- migration.
--
-- It lives here, with the copy, because it has the same owner and the
-- same life: one writer, changed when a batch is written, read by batches
-- and by nothing else. Unlike the area it is kept decoded, but for the
-- script, which is bytes that the collector may move, for the reason
-- above. The address is worked out from the script when an entry is
-- used: an address can hold a pinned slice of the script it came from.
-- An entry takes 162 bytes of heap, 7.4 MiB for a mempool of 25,000
-- transactions (@docs\/block-import.md@).
module Haskoin.Store.Database.Copy
  ( -- * The copy
    Copy,
    Family (..),
    emptyCopy,
    loadCopy,
    copyEntries,
    copySize,

    -- * Reading
    valueIn,
    prefixed,
    firstKey,
    shortKey,

    -- * Changing
    CopyChange (..),
    applyChanges,

    -- * The outputs that mempool transactions spend
    SpentOutput (..),
    spentOutput,
    spentUnspent,
    spentOutputs,
    SpentChange (..),
    applySpent,

    -- * Where a database keeps its copy
    CopyRef,
    newCopyRef,
    knownCopy,
    setCopy,
    forgetCopy,
  )
where

import Data.ByteString (ByteString)
import Data.ByteString.Builder.Extra (Next (..), runBuilder)
import Data.ByteString.Short (ShortByteString, fromShort, toShort)
import Data.ByteString.Short qualified as Short
import Data.Default (def)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (IORef, atomicWriteIORef, newIORef, readIORef)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Serialize (Serialize, encode, put)
import Data.Serialize.Put (execPut)
import Data.Word (Word32, Word64)
import Database.RocksDB
  ( ChunkConfig (..),
    ColumnFamily,
    DB,
    iterChunk,
    iterFirst,
    withIterCF,
  )
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (castPtr)
import Haskoin (Network, OutPoint, eitherToMaybe, scriptToAddressBS)
import Haskoin.Store.Data (BlockRef (..), Unspent (..))
import System.IO.Unsafe (unsafeDupablePerformIO)

-- | One of the two column families of the mempool area.
data Family
  = -- | 'Haskoin.Store.Database.Reader.mempoolCF': the list of the
    -- mempool, the bodies of its transactions and their spenders
    General
  | -- | 'Haskoin.Store.Database.Reader.mempoolAddrCF': what the mempool
    -- does at each address
    ByAddress
  deriving (Eq, Show)

-- | What the two column families of the mempool area hold, at one moment,
-- and what the block store remembers of the outputs that mempool
-- transactions spend. It is a value like any other: nothing changes it,
-- and a change gives another one ('applyChanges', 'applySpent').
data Copy = Copy
  { general :: !(Map ShortByteString ShortByteString),
    byAddress :: !(Map ShortByteString ShortByteString),
    -- | the chain's unspent output of outputs that mempool transactions
    -- spend: not part of the area, and empty when the copy is loaded
    spent :: !(HashMap OutPoint SpentOutput)
  }

-- | The copy of an area with nothing in it.
emptyCopy :: Copy
emptyCopy = Copy {general = Map.empty, byAddress = Map.empty, spent = HashMap.empty}

-- | The map of a column family.
family :: Family -> Copy -> Map ShortByteString ShortByteString
family General c = c.general
family ByAddress c = c.byAddress

-- | Read the whole mempool area of a database: a scan of the mempool
-- column family and of the one by address, if the database has it. A
-- database of the layout of version 20 does not.
--
-- The scan gives the keys in their order, so each map is built in one
-- pass, without a comparison. It took 0.3 s for a mempool of 25,000
-- transactions, 400,000 keys (@docs\/block-import.md@), most of it
-- RocksDB's iterator. It is done once for an open database, unless
-- something writes to the area behind the block store.
loadCopy :: DB -> ColumnFamily -> Maybe ColumnFamily -> IO Copy
loadCopy db generalCF byAddressCF = do
  general <- scan generalCF
  byAddress <- maybe (return Map.empty) scan byAddressCF
  -- nothing is remembered of what was read before the copy was loaded
  return Copy {general, byAddress, spent = HashMap.empty}
  where
    scan cf = withIterCF db cf $ \it -> iterFirst it >> go it []
    -- the entries so far, the last one first
    go it acc = do
      (entries, more) <- iterChunk it def {chunkEntries = 1024, chunkBytes = 4 * 1024 * 1024}
      let acc' = foldl' (\done (k, v) -> entry k v : done) acc entries
      if more then go it acc' else return $! Map.fromDistinctDescList acc'
    entry k v = let !key = toShort k; !value = toShort v in (key, value)

-- | Every key and value of a column family, in the order of the keys: what
-- a scan of the column family gives, if the copy is right. For the tests.
copyEntries :: Family -> Copy -> [(ByteString, ByteString)]
copyEntries f c = [(fromShort k, fromShort v) | (k, v) <- Map.toAscList (family f c)]

-- | How many keys the copy has, and how many bytes its keys and values
-- are. The heap that it takes is more: about 120 bytes for each key, for
-- the tree and the headers of the two byte strings. With 25,000
-- transactions in the mempool that was 85 MiB for 38 MiB of keys and
-- values.
copySize :: Copy -> (Int, Int)
copySize c = (Map.size c.general + Map.size c.byAddress, bytes c.general + bytes c.byAddress)
  where
    bytes = Map.foldlWithKey' (\n k v -> n + Short.length k + Short.length v) 0

-- | The value of a key, if the column family has it.
valueIn :: Family -> ShortByteString -> Copy -> Maybe ByteString
valueIn f k c = fromShort <$> Map.lookup k (family f c)

-- | The keys of a column family that start with some bytes, with their
-- values, in the order of the keys: what a scan from those bytes gives.
-- The list is made as it is read.
prefixed :: Family -> ShortByteString -> Copy -> [(ByteString, ByteString)]
prefixed f prefix c =
  [ (fromShort k, fromShort v)
  | (k, v) <- Map.toAscList (Map.takeWhileAntitone (prefix `Short.isPrefixOf`) from)
  ]
  where
    from = Map.dropWhileAntitone (< prefix) (family f c)

-- | The first key of a column family, if it has one.
firstKey :: Family -> Copy -> Maybe ByteString
firstKey f c = fromShort . fst <$> Map.lookupMin (family f c)

-- | Bytes that a key of the mempool area can have, with room to spare: the
-- longest, an output paid to an address, is about a hundred.
keyRoom :: Int
keyRoom = 192

-- | The bytes of a key, as 'encode' gives them, to look it up in the copy.
--
-- 'encode' takes a buffer of four kilobytes for each key, and a block
-- looks up tens of thousands. This serialises into a few bytes of scratch
-- space, and falls back on 'encode' for a key that does not fit, of which
-- the mempool area has none.
shortKey :: (Serialize k) => k -> ShortByteString
shortKey k = unsafeDupablePerformIO . allocaBytes keyRoom $ \buffer ->
  runBuilder (execPut (put k)) buffer keyRoom >>= \case
    (len, Done) -> Short.packCStringLen (castPtr buffer, len)
    _ -> return (toShort (encode k))

-- | A change that a batch made to the mempool area.
data CopyChange
  = -- | a key of a column family was set to a value
    CopySet !Family !ByteString !ByteString
  | -- | a key of a column family was deleted
    CopyDrop !Family !ByteString

-- | The copy after some changes, in their order.
--
-- The bytes of the keys and values that it keeps are copied, so that the
-- byte strings of the changes can be slices of the buffers that the batch
-- was serialised into, and those buffers still go when the batch is
-- written. Nothing of the result is left to be worked out later: the
-- copy that comes back refers to the changes no more.
applyChanges :: [CopyChange] -> Copy -> Copy
applyChanges changes c = foldl' apply c changes
  where
    apply done = \case
      CopySet f k v ->
        let !key = toShort k
            !value = toShort v
         in onFamily f (Map.insert key value) done
      CopyDrop f k -> onFamily f (Map.delete (toShort k)) done
    onFamily General g done = (done :: Copy) {general = g done.general}
    onFamily ByAddress g done = (done :: Copy) {byAddress = g done.byAddress}

-- | An unspent output of the chain that a mempool transaction spends, as
-- the block store remembers it between the batch that put the transaction
-- in the mempool and the block that confirms it: what
-- 'Haskoin.Store.Data.Unspent' has, but for the output itself, which is
-- the key, and the address, which the script gives.
--
-- The script is a 'ShortByteString', so that an entry that lives for
-- hours holds no memory that the collector cannot move.
data SpentOutput = SpentOutput
  { -- | height of the block of the transaction that made the output
    height :: !Word32,
    -- | position of that transaction in its block
    position :: !Word32,
    -- | satoshi
    value :: !Word64,
    -- | the output script
    script :: !ShortByteString
  }
  deriving (Eq, Show)

-- | What to remember of an unspent output of the chain. Nothing of one
-- that is not at a position in a block, which the chain does not have.
spentOutput :: Unspent -> Maybe SpentOutput
spentOutput u = case u.block of
  BlockRef {height, position} -> Just SpentOutput {height, position, value = u.value, script = toShort u.script}
  MemRef {} -> Nothing

-- | The unspent output that was remembered, as the database gives it: the
-- address is worked out from the script, as
-- 'Haskoin.Store.Database.Types.valToUnspent' does.
spentUnspent :: Network -> OutPoint -> SpentOutput -> Unspent
spentUnspent net outpoint s =
  Unspent
    { block = BlockRef {height = s.height, position = s.position},
      outpoint,
      value = s.value,
      script,
      address = eitherToMaybe (scriptToAddressBS net script)
    }
  where
    script = fromShort s.script

-- | Everything that is remembered: the chain's unspent output, by output,
-- of outputs that mempool transactions spend.
spentOutputs :: Copy -> HashMap OutPoint SpentOutput
spentOutputs c = c.spent

-- | A change to what is remembered of the outputs that mempool
-- transactions spend, which a batch makes when it is written.
data SpentChange
  = -- | a mempool transaction now spends this output, which the chain has
    -- as unspent
    Remember !OutPoint !SpentOutput
  | -- | no mempool transaction spends this output any more, or the chain
    -- does not have it as unspent
    Forget !OutPoint
  | -- | the batch changed this unspent output of the chain, to this or to
    -- nothing: what is remembered of it, if anything, follows. The new
    -- value is not worked out unless the output is remembered: a block
    -- changes thousands of unspent outputs that no mempool transaction
    -- spends
    Correct !OutPoint (Maybe SpentOutput)
  | -- | a block was reverted
    ForgetAll

-- | The copy after some changes to what is remembered, in their order.
-- Nothing of the result is left to be worked out later.
applySpent :: [SpentChange] -> Copy -> Copy
applySpent changes c = (c :: Copy) {spent = foldl' apply c.spent changes}
  where
    apply m = \case
      Remember p s -> HashMap.insert p s m
      Forget p -> HashMap.delete p m
      Correct p new
        | HashMap.member p m -> maybe (HashMap.delete p m) (\s -> HashMap.insert p s m) new
        | otherwise -> m
      ForgetAll -> HashMap.empty

-- | Where an open database keeps the copy of its mempool area: nothing
-- until a batch needs it, and nothing again whenever the copy can no
-- longer be taken for what the database has.
newtype CopyRef = CopyRef (IORef (Maybe Copy))

-- | A place for the copy of a database that was just opened: not loaded.
newCopyRef :: IO CopyRef
newCopyRef = CopyRef <$> newIORef Nothing

-- | The copy, if it is loaded and known to be what the database has.
knownCopy :: CopyRef -> IO (Maybe Copy)
knownCopy (CopyRef ref) = readIORef ref

-- | Set the copy: to what was just loaded, or to what a batch that was
-- just written made of it.
setCopy :: CopyRef -> Copy -> IO ()
setCopy (CopyRef ref) c = atomicWriteIORef ref (Just c)

-- | Say that the copy is not known to be what the database has: before a
-- write to the mempool area whose outcome is not known yet, and after any
-- write to it that did not go through a batch. The next batch that needs
-- the copy loads it again.
forgetCopy :: CopyRef -> IO ()
forgetCopy (CopyRef ref) = atomicWriteIORef ref Nothing
