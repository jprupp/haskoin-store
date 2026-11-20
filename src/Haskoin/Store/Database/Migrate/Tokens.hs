-- |
-- Module      : Haskoin.Store.Database.Migrate.Tokens
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Migration of a database of Bitcoin Cash from version 23 to 24: what the
-- tokens of Bitcoin Cash (CashTokens) need indexed (@docs\/cashtokens.md@).
--
-- = The problem
--
-- Until version 24 the store read scripts with haskoin-core 2.0, which did
-- not know the token prefix: an output that holds tokens had no address,
-- and was in no address's history, balance or unspent outputs.
-- haskoin-core 2.1 gives such an output the address of its locking script.
-- A database of version 23 is wrong for it in two ways. What is missing is
-- missing: the history of the addresses that held tokens. And what is
-- there would go wrong: spending an old output that holds tokens takes its
-- coins out of a balance that never had them.
--
-- = What it does
--
-- It walks the blocks of the main chain from the activation of CashTokens
-- to the tip, and for every transaction with an output or a spent output
-- that holds tokens, writes what version 24 would have written when the
-- block was imported:
--
-- * the address of each such output and spent output gets the transaction
--   in its history, and one more in the count of its balance, unless an
--   output or a spent output without tokens gave it the transaction
--   already;
--
-- * each output with tokens and an address adds its coins to what the
--   address received, and, if it is still unspent, to its balance and its
--   unspent outputs;
--
-- * the token balances, the holders, the transactions and the unspent
--   outputs of the categories, as the import of a block writes them
--   ("Haskoin.Store.Logic").
--
-- It writes the result, not the history: an output spent since counts as
-- received and not as held, whatever came between. A balance it did not
-- touch is the balance of the chain at the tip, so the sum is too.
--
-- = Safe to interrupt
--
-- It works in batches of blocks. Each batch is one atomic write, with the
-- height of the next block to do under t'MigrationKey' in the same write
-- ('Haskoin.Store.Database.Writer.writeRaw'), so a batch is done once or
-- not at all: the balances are added to, without reading, and a batch done
-- twice would count its outputs twice. A store that stopped carries on
-- from that height. 'Haskoin.Store.Database.Migrate.upgradeDatabase' then
-- sets version 24 and removes the marker.
--
-- = Where it starts
--
-- The rules of CashTokens apply to the blocks after the first whose median
-- time past is the time of the upgrade, on every network of Bitcoin Cash.
-- 'tokenActivation' finds that block from the timestamps of the blocks in
-- the database. An output that started with the token byte before then is
-- left as it was: the prefix meant nothing yet, and consensus makes such
-- an output unspendable, so nothing reads it again.
module Haskoin.Store.Database.Migrate.Tokens
  ( migrateTokens,
    tokenActivation,
    tokenUpgradeTime,
  )
where

import Control.Monad (forM, forM_, when)
import Data.HashSet qualified as HashSet
import Data.IntMap.Strict qualified as IntMap
import Data.List (sort)
import Data.Maybe (catMaybes, isJust, mapMaybe)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Word (Word32, Word64)
import Database.RocksDB.Query (insertOp, retrieve)
import Haskoin
  ( BlockHeader (..),
    BlockHeight,
    OutPoint (..),
    TokenData (..),
    Tx (..),
    TxOut (..),
    eitherToMaybe,
    scriptToAddressBS,
    txHash,
  )
import Haskoin.Node (LogLevel (..), Logger (..))
import Haskoin.Store.Data
  ( BlockData (..),
    Prev (..),
    TxData (..),
    TxRef (..),
    Unspent (..),
    scriptTokens,
  )
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader (DatabaseReader (..))
import Haskoin.Store.Database.Types
  ( BalVal (..),
    MigrationKey (..),
    TokBalVal (..),
  )
import Haskoin.Store.Database.Writer
  ( Writer,
    insertAddrTx,
    insertAddrUnspent,
    insertCatTx,
    insertCatUnspent,
    mergeBalance,
    mergeTokBal,
    runWriter,
    writeRaw,
  )

-- | The time of the upgrade of Bitcoin Cash that brought CashTokens, 15 May
-- 2023 at noon UTC: the rules apply after the first block whose median time
-- past is at least this.
tokenUpgradeTime :: Word64
tokenUpgradeTime = 1684152000

-- | Blocks in each write of the migration: a few thousand transactions on
-- Bitcoin Cash.
batchBlocks :: BlockHeight
batchBlocks = 200

-- | The height of the first block of the main chain under the rules of
-- CashTokens: the one after the first block whose median time past is
-- 'tokenUpgradeTime' or later. The height after the best block if there is
-- none yet.
--
-- The median time past only goes up along a chain, so it is found by
-- halving the range of heights, from the timestamps of the blocks in the
-- database: no headers are needed.
tokenActivation :: DatabaseReader -> IO BlockHeight
tokenActivation r = do
  tip <- bestHeight r
  let search lo hi
        | lo >= hi = return lo
        | otherwise = do
            let mid = (lo + hi) `div` 2
            reached <- (>= tokenUpgradeTime) <$> medianTimePast r mid
            if reached then search lo mid else search (mid + 1) hi
  -- the first block whose median time past reached the upgrade, and the
  -- rules start at the one after it
  (+ 1) <$> search 0 (tip + 1)

-- | Add what version 24 indexes to a database of Bitcoin Cash of version
-- 23, from a height or from where an earlier run stopped, telling a
-- function how far it is. Says how many transactions it wrote for. The
-- version is left to the caller.
migrateTokens :: (Text -> IO ()) -> BlockHeight -> DatabaseReader -> IO Int
migrateTokens report start r = do
  marker <- retrieve r.db MigrationKey :: IO (Maybe Word32)
  tip <- bestHeight r
  let from = maybe start id marker
  report $
    "Indexing the tokens of Bitcoin Cash from block "
      <> cs (show from)
      <> " to "
      <> cs (show tip)
      <> ": this reads every transaction since the upgrade of May 2023"
  counts <- forM [from, from + batchBlocks .. tip] $ \h -> do
    let to = min tip (h + batchBlocks - 1)
    n <- runWriter r.net r quiet $ \w -> do
      ns <- forM [h .. to] (migrateBlock r w)
      writeRaw w (insertOp MigrationKey (to + 1))
      return (sum ns)
    when ((to + 1) `mod` 10000 < batchBlocks) $
      report $
        "Tokens indexed up to block " <> cs (show to)
    return n
  return (sum counts)
  where
    quiet = Logger {level = LevelError, source = "", push = \_ -> return ()}

-- | Write what version 24 adds for the transactions of the block of the
-- main chain at a height. Says how many it wrote for.
migrateBlock :: DatabaseReader -> Writer -> BlockHeight -> IO Int
migrateBlock r w h = do
  hashes <- Q.getBlocksAtHeight r h
  blocks <- catMaybes <$> mapM (Q.getBlock r) hashes
  case filter (.main) blocks of
    bd : _ -> do
      tds <- catMaybes <$> mapM (Q.getTxData r) bd.txs
      length . filter id <$> mapM (migrateTx r w) tds
    [] -> return 0

-- | Write what version 24 adds for one confirmed transaction, if it has an
-- output or a spent output that holds tokens. Says whether it had.
migrateTx :: DatabaseReader -> Writer -> TxData -> IO Bool
migrateTx r w td = do
  let held = not (null tokenOuts && null tokenPrevs)
  when held $ do
    -- the history and the count of the addresses that only tokens gave it
    forM_ (HashSet.toList (HashSet.difference tokenAddrs plainAddrs)) $ \a -> do
      insertAddrTx w a ref
      mergeBalance w a (bal 0 0 1 0)
    forM_ tokenOuts $ \(i, o, t) -> do
      let p = OutPoint h i
          address = addr o.script
          u = Unspent {block = td.block, outpoint = p, value = o.value, script = o.script, address}
      unspent <- isJust <$> Q.getUnspent r p
      forM_ address $ \a -> do
        mergeBalance w a (bal 0 0 0 o.value)
        mergeTokBal w a t.category (tok 0 0 0 0 t.amount)
      when unspent $ do
        insertCatUnspent w t.category u
        forM_ address $ \a -> do
          insertAddrUnspent w a u
          mergeBalance w a (bal o.value 1 0 0)
          mergeTokBal w a t.category (tokensIn t)
    forM_ categories $ \c -> insertCatTx w c ref
    forM_ holders $ \(a, c) -> mergeTokBal w a c (tok 0 0 0 1 0)
  return held
  where
    net = r.net
    h = txHash td.tx
    ref = TxRef {block = td.block, txid = h}
    addr = eitherToMaybe . scriptToAddressBS net
    tokens = scriptTokens net
    outs = zip [0 ..] td.tx.outputs
    tokenOuts = [(i, o, t) | (i, o) <- outs, Just t <- [tokens o.script]]
    tokenPrevs = [(pv, t) | pv <- IntMap.elems td.prevs, Just t <- [tokens pv.script]]
    plainAddrs =
      HashSet.fromList $
        mapMaybe (addr . (.script)) [o | (_, o) <- outs, not (isJust (tokens o.script))]
          <> mapMaybe (addr . (.script)) [pv | pv <- IntMap.elems td.prevs, not (isJust (tokens pv.script))]
    tokenAddrs =
      HashSet.fromList $
        mapMaybe (\(_, o, _) -> addr o.script) tokenOuts <> mapMaybe (addr . (.script) . fst) tokenPrevs
    owned = [(addr o.script, t) | (_, o, t) <- tokenOuts] <> [(addr pv.script, t) | (pv, t) <- tokenPrevs]
    categories = HashSet.toList (HashSet.fromList [t.category | (_, t) <- owned])
    holders = HashSet.toList (HashSet.fromList [(a, t.category) | (Just a, t) <- owned])

-- | What an output that holds tokens adds to the token balance of its
-- address while it is unspent, as the import of a block adds it.
tokensIn :: TokenData -> TokBalVal
tokensIn t = tok t.amount (maybe 0 (const 1) t.nft) 1 0 0

-- | Differences to a balance: coins held, unspent outputs, transactions,
-- coins received. Nothing is unconfirmed in the chain.
bal :: Word64 -> Word64 -> Word64 -> Word64 -> BalVal
bal confirmed utxo txs received = BalVal {confirmed, unconfirmed = 0, utxo, txs, received}

-- | Differences to a token balance: tokens held, non-fungible tokens,
-- unspent outputs, transactions, tokens received.
tok :: Word64 -> Word64 -> Word64 -> Word64 -> Word64 -> TokBalVal
tok amount nfts outputs count received = TokBalVal {amount, nfts, outputs, count, received}

-- | The height of the best block.
bestHeight :: DatabaseReader -> IO BlockHeight
bestHeight r =
  Q.getBestBlock r >>= \case
    Nothing -> return 0
    Just b -> maybe 0 (.height) <$> Q.getBlock r b

-- | The median time past of the block of the main chain at a height: the
-- median of its timestamp and those of the ten blocks before it.
medianTimePast :: DatabaseReader -> BlockHeight -> IO Word64
medianTimePast r h = do
  times <- forM [h - min h 10 .. h] $ \k -> do
    hashes <- Q.getBlocksAtHeight r k
    blocks <- catMaybes <$> mapM (Q.getBlock r) hashes
    return [fromIntegral b.header.timestamp | b <- blocks, b.main]
  let sorted = sort (concat times)
  return $ if null sorted then 0 else sorted !! (length sorted `div` 2)
