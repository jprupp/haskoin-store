-- |
-- Module      : Haskoin.Store.Database.Merge
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The merge operators of the database: how RocksDB works out an address
-- balance and a group of spenders from the merge records that the import
-- of a block writes.
--
-- = The problem
--
-- A block changes the balance of every address that it pays or spends
-- from, and adds a spender to the group of every output that it spends.
-- Each was read, changed and written back. On a store of Bitcoin mainnet
-- those two kinds of read were most of the reads of a block: 300 to 850 ms
-- for the groups of spenders and 70 to 300 for the balances, of a read
-- ahead of 640 to 1,440 ms. And they were read only to be changed.
--
-- = Merge records
--
-- RocksDB has a third kind of write besides a put and a delete: a merge
-- record, which says how a value changes and is written without reading
-- anything. The /merge operator/ of the column family works out the value
-- later, from the value that the key had and the records written since:
-- when the key is read, and when a compaction brings them together, after
-- which the key holds a plain value again.
--
-- So the import of a block writes, for each address, the differences to
-- its balance, and for each group of outputs, the spenders to add
-- ('Haskoin.Store.Database.Writer.mergeBalance',
-- 'Haskoin.Store.Database.Writer.mergeSpender'), and reads neither.
--
-- = The two operators
--
-- They are C functions, in @cbits\/haskoin_store_merge.c@, because RocksDB
-- calls them from its own threads. A merge record has the format of a
-- value in both, so that applying records to a value and combining records
-- with each other are one operation:
--
-- * __Balances__ ('balanceOperator'): a record is five differences in the
--   forty bytes of a 'Haskoin.Store.Database.Types.BalVal', and merging
--   adds each of the five numbers modulo 2^64. A key without a value
--   counts from zero.
--
-- * __Groups of spenders__ ('spenderOperator'): a record is a
--   t'Haskoin.Store.Database.Types.SpenderVal' with the entries to add,
--   and merging is the union, the newer entry winning for an output that
--   two have.
--
-- = What it asks
--
-- * __Every program that opens the database sets the operators__: they are
--   in the configuration of the two column families
--   ('Haskoin.Store.Database.Reader.columnFamilyConfig'), so nothing in
--   this package can open it without. RocksDB itself does not refuse to:
--   it then drops the merge records that are still in its write-ahead log,
--   and cannot read the others. That is why a database that this version
--   has opened has a column family that older versions do not know of, and
--   why they can no longer open it
--   ('Haskoin.Store.Database.Reader.layeredFamilyConfig').
--
-- * __Readers pay for the merge.__ A read of a key with merge records
--   pending looks into every table file that has one of them, down to the
--   value, and merges. @haskoin-store-bench merge@ measures it, and
--   @docs\/one-fleet.md@ has the figures.
--
-- * __Reverts read.__ Only the import of a block writes merge records.
--   Taking a block out of the chain, and everything else that changes a
--   balance or a group of spenders, reads the value and writes it whole,
--   or deletes it: a put or a delete hides the merge records under it.
module Haskoin.Store.Database.Merge
  ( balanceOperator,
    spenderOperator,
  )
where

import Database.RocksDB (FullMerge, MergeOperator (..), PartialMerge)
import Foreign.Ptr (FunPtr)

foreign import ccall "&haskoin_store_balance_full_merge" balanceFullMerge :: FunPtr FullMerge

foreign import ccall "&haskoin_store_balance_partial_merge" balancePartialMerge :: FunPtr PartialMerge

foreign import ccall "&haskoin_store_spender_full_merge" spenderFullMerge :: FunPtr FullMerge

foreign import ccall "&haskoin_store_spender_partial_merge" spenderPartialMerge :: FunPtr PartialMerge

-- | The merge operator of the column family of address balances. A merge
-- record is a 'Haskoin.Store.Database.Types.BalVal' of five differences,
-- each added to its number modulo 2^64, so that a number goes down by
-- adding its two's complement. A balance that is not there counts from
-- zero.
balanceOperator :: MergeOperator
balanceOperator =
  MergeFunctions
    { mergeName = "haskoin-store-balance-1",
      fullMerge = balanceFullMerge,
      partialMerge = balancePartialMerge
    }

-- | The merge operator of the column family of the spenders of outputs. A
-- merge record is a t'Haskoin.Store.Database.Types.SpenderVal' with the
-- spenders to add to a group, and merging is the union of the group and
-- the records, the newest entry winning for an output that several have.
spenderOperator :: MergeOperator
spenderOperator =
  MergeFunctions
    { mergeName = "haskoin-store-spender-1",
      fullMerge = spenderFullMerge,
      partialMerge = spenderPartialMerge
    }
