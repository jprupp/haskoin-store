{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE NoFieldSelectors #-}
{-# OPTIONS_GHC -Wno-ambiguous-fields #-}

-- |
-- Module      : Haskoin.Store.Logic
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Rules to import and revert blocks and transactions.
--
-- These functions keep every index consistent: transactions, unspent outputs,
-- spenders, address transactions, address unspent outputs, balances and the
-- mempool. Each takes the 'Writer' of a batch as its first argument and runs
-- in 'IO'; the writer buffers all changes and 'runWriter' commits them in one
-- atomic batch, so a failure leaves the database untouched.
--
-- When a block is imported, transactions already in the mempool are confirmed,
-- new ones are imported, and mempool transactions that conflict with the block
-- are deleted together with their descendants. When a block is reverted, its
-- transactions go back to the mempool and its coinbase is deleted.
--
-- On networks with replace-by-fee, a new mempool transaction may replace the
-- unconfirmed transactions it conflicts with. Elsewhere a conflicting mempool
-- transaction is rejected as a double spend.
-- = Why these rules
--
-- The indices are only useful if they always agree with each other: a balance
-- must equal the sum of the unspent outputs of its address, a spent output must
-- name its spender, and a transaction must appear in the index of every address
-- it touches. Every function here changes all of them together, inside one
-- atomic batch, so the database moves from one consistent state to the next.
--
-- The store does not validate scripts or consensus rules. It follows the chain
-- with the most work, as reported by haskoin-node, and checks only what it needs
-- to keep the indices consistent: that inputs exist and are unspent, and that a
-- transaction does not spend more than it has. A block that fails these checks
-- is not imported, and the block store disconnects the peer that sent it.
--
-- The mempool is treated as provisional. A confirmed block always wins: mempool
-- transactions that conflict with it are deleted together with everything that
-- spends them. Between mempool transactions, replace-by-fee decides on networks
-- that support it; elsewhere the first one seen stays.
module Haskoin.Store.Logic
  ( ImportException (..),
    initBest,
    revertBlock,
    importBlock,
    newMempoolTx,
    deleteUnconfirmedTx,
  )
where

import Control.Monad
  ( forM,
    forM_,
    guard,
    unless,
    void,
    when,
    zipWithM_,
  )
import qualified Data.ByteString as B
import Data.Either (rights)
import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as H
import qualified Data.IntMap.Strict as I
import Data.List (nub)
import Data.Maybe
  ( catMaybes,
    fromMaybe,
    isJust,
    isNothing,
  )
import Data.Serialize (encode)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Word (Word32, Word64)
import Haskoin
  ( Address,
    Block (..),
    BlockHash,
    BlockHeader (..),
    BlockNode (..),
    Ctx,
    Network (..),
    OutPoint (..),
    Tx (..),
    TxHash,
    TxIn (..),
    TxOut (..),
    blockHashToHex,
    computeSubsidy,
    eitherToMaybe,
    genesisBlock,
    genesisNode,
    headerHash,
    isGenesis,
    nullOutPoint,
    scriptToAddressBS,
    txHash,
    txHashToHex,
  )
import Haskoin.Node (LogLevel (..), logAt)
import Haskoin.Store.Common (nub', sortTxs)
import Haskoin.Store.Data
import Haskoin.Store.Database.Writer
import UnliftIO (Exception, throwIO)

-- | Log a message with the logger of the writer.
logDebug, logError :: Writer -> Text -> IO ()
logDebug wr = logAt LevelDebug (writerLogger wr)
logError wr = logAt LevelError (writerLogger wr)

-- | Network of the database being written.
getNetwork :: Writer -> IO Network
getNetwork wr = return (writerNet wr)

-- | Secp256k1 context of the database being written.
getCtx :: Writer -> IO Ctx
getCtx wr = return (writerCtx wr)

-- | Data of a transaction that is in the database and not deleted.
getActiveTxData :: Writer -> TxHash -> IO (Maybe TxData)
getActiveTxData wr th =
  getTxData wr th >>= \case
    Just td | not td.deleted -> return (Just td)
    _ -> return Nothing

-- | Balance of an address, or a zero balance if it was never used.
getDefaultBalance :: Writer -> Address -> IO Balance
getDefaultBalance wr a = fromMaybe (zeroBalance a) <$> getBalance wr a

-- | Reasons why an import can fail.
data ImportException
  = -- | the block does not build on the best block
    PrevBlockNotBest
  | -- | the transaction spends an output that is unknown or already spent
    Orphan
  | -- | a coinbase transaction cannot go into the mempool
    UnexpectedCoinbase
  | -- | the database has no best block, or it cannot be read
    BestBlockNotFound
  | -- | only the best block can be reverted
    BlockNotBest
  | -- | a transaction that should be in the database is not
    TxNotFound
  | -- | the transaction conflicts with another and the network does not have
    -- replace-by-fee
    DoubleSpend
  | -- | a confirmed transaction cannot be deleted or replaced
    TxConfirmed
  | -- | the outputs of the transaction are worth more than its inputs
    InsufficientFunds
  | -- | the transaction spends the same output twice
    DuplicatePrevOutput
  | -- | a transaction cannot be deleted while another one spends it
    TxSpent
  deriving (Eq, Ord, Exception)

instance Show ImportException where
  show PrevBlockNotBest = "Previous block not best"
  show Orphan = "Orphan"
  show UnexpectedCoinbase = "Unexpected coinbase"
  show BestBlockNotFound = "Best block not found"
  show BlockNotBest = "Block not best"
  show TxNotFound = "Transaction not found"
  show DoubleSpend = "Double spend"
  show TxConfirmed = "Transaction confirmed"
  show InsufficientFunds = "Insufficient funds"
  show DuplicatePrevOutput = "Duplicate previous output"
  show TxSpent = "Transaction is spent"

-- | Import the genesis block if the database has no best block yet.
initBest :: Writer -> IO ()
initBest wr = do
  logDebug wr "Initializing best block"
  net <- getNetwork wr
  m <- getBestBlock wr
  when (isNothing m) . void $ do
    logDebug wr "Importing Genesis block"
    importBlock wr (genesisBlock net) (genesisNode net)

-- | Import a transaction into the mempool with the time it arrived. Returns
-- 'False' if it is already known.
--
-- Conflicting unconfirmed transactions are deleted together with their
-- descendants if the network has replace-by-fee; otherwise the transaction is
-- rejected with 'DoubleSpend'. Throws an 'ImportException' if the transaction is
-- invalid, for example if it spends unknown outputs.
newMempoolTx :: Writer -> Tx -> UnixTime -> IO Bool
newMempoolTx wr tx w =
  getActiveTxData wr (txHash tx) >>= \case
    Just _ ->
      return False
    Nothing -> do
      -- Check everything before writing, so that a rejected transaction
      -- leaves the mempool as it was.
      replaced <- getConflicts wr True True tx
      checkNewTx wr (map txHash replaced) tx
      mapM_ (deleteSingleTx wr . txHash) replaced
      rbf <- isRBF wr (MemRef w) tx
      _ <- importTx wr (MemRef w) w rbf tx
      return True

-- | Data of the best block. Throws 'BestBlockNotFound' if there is none.
bestBlockData :: Writer -> IO BlockData
bestBlockData wr = do
  h <-
    getBestBlock wr >>= \case
      Nothing -> do
        logError wr "Best block unknown"
        throwIO BestBlockNotFound
      Just h -> return h
  getBlock wr h >>= \case
    Nothing -> do
      logError wr "Best block not found"
      throwIO BestBlockNotFound
    Just b -> return b

-- | Revert the best block: make its parent the best block, mark it as not in the
-- main chain, move its transactions back to the mempool, and delete its
-- coinbase. Throws 'BlockNotBest' if the block is not the best block.
revertBlock :: Writer -> BlockHash -> IO ()
revertBlock wr bh = do
  bd <-
    bestBlockData wr >>= \b ->
      if headerHash b.header == bh
        then return b
        else do
          logError wr $
            "Cannot revert non-head block: " <> blockHashToHex bh
          throwIO BlockNotBest
  logDebug wr $
    "Obtained block data for " <> blockHashToHex bh
  tds <- mapM (getImportTxData wr) bd.txs
  logDebug wr $
    "Obtained import tx data for block " <> blockHashToHex bh
  setBest wr bd.header.prev
  logDebug wr $
    "Set parent as best block "
      <> blockHashToHex bd.header.prev
  insertBlock wr bd {main = False}
  logDebug wr $
    "Updated as not in main chain: " <> blockHashToHex bh
  forM_ (drop 1 tds) (unConfirmTx wr)
  logDebug wr $
    "Unconfirmed " <> cs (show (length tds)) <> " transactions"
  deleteConfirmedTx wr (txHash (tds !! 0).tx)
  logDebug wr $
    "Deleted coinbase: " <> txHashToHex (txHash (tds !! 0).tx)

-- | Check that a block builds on the best block, or that it is the genesis block
-- of an empty database.
checkNewBlock :: Writer -> Block -> BlockNode -> IO ()
checkNewBlock wr b n =
  getBestBlock wr >>= \case
    Nothing
      | isGenesis n -> return ()
      | otherwise -> do
          logError wr $
            "Cannot import non-genesis block: "
              <> blockHashToHex (headerHash b.header)
          throwIO BestBlockNotFound
    Just h
      | b.header.prev == h -> return ()
      | otherwise -> do
          logError wr $
            "Block does not build on head: "
              <> blockHashToHex (headerHash b.header)
          throwIO PrevBlockNotBest

-- | Import the transactions of a block, in dependency order. Mempool transactions
-- that conflict with the block are deleted first. Transactions already in the
-- mempool are confirmed; others are imported.
importOrConfirm :: Writer -> BlockNode -> [Tx] -> IO [TxData]
importOrConfirm wr bn txns = do
  logDebug wr "Freeing outputs..."
  mapM_ (freeOutputs wr True False . snd) (reverse txs)
  logDebug wr "Outputs freed"
  mapM (uncurry action) txs
  where
    txs = sortTxs txns
    br i = BlockRef {height = bn.height, position = i}
    bn_time = fromIntegral $ bn.header.timestamp
    action i tx =
      testPresent wr tx >>= \case
        False -> import_it i tx
        True -> confirm_it i tx
    confirm_it i tx =
      getActiveTxData wr (txHash tx) >>= \case
        Just t -> do
          logDebug wr $
            "Confirming tx: "
              <> txHashToHex (txHash tx)
          confirmTx wr t (br i)
        Nothing -> do
          logError wr $
            "Cannot find tx to confirm: "
              <> txHashToHex (txHash tx)
          throwIO TxNotFound
    import_it i tx = do
      logDebug wr $
        "Importing tx: " <> txHashToHex (txHash tx)
      importTx wr (br i) bn_time False tx

-- | Import a block that builds on the best block, and make it the best block.
-- Returns its data and the data of its transactions.
importBlock :: Writer -> Block -> BlockNode -> IO (BlockData, [TxData])
importBlock wr b n = do
  logDebug wr $
    "Checking new block: "
      <> blockHashToHex (headerHash n.header)
  checkNewBlock wr b n
  logDebug wr "Passed check"
  net <- getNetwork wr
  let subsidy = computeSubsidy net n.height
  bs <- getBlocksAtHeight wr n.height
  logDebug wr $
    "Inserting block entries for: "
      <> blockHashToHex (headerHash n.header)
  setBlocksAtHeight wr
    (nub (headerHash n.header : bs))
    n.height
  setBest wr (headerHash n.header)
  tds <- importOrConfirm wr n b.txs
  let bd =
        BlockData
          { height = n.height,
            main = True,
            work = n.work,
            header = n.header,
            size = fromIntegral (B.length (encode b)),
            txs = map txHash b.txs,
            weight = if net.segWit then w else 0,
            subsidy = subsidy,
            fee = sum $ map txDataFee tds,
            outputs = ts_out_val
          }
  insertBlock wr bd
  logDebug wr $
    "Finished importing block: "
      <> blockHashToHex (headerHash n.header)
  return (bd, tds)
  where
    ts_out_val =
      sum $ map (sum . map (.value) . (.outputs)) (drop 1 b.txs)
    w =
      let f t = (t :: Tx) {witness = []}
          b' = (b :: Block) {txs = map f b.txs}
          x = B.length (encode b)
          s = B.length (encode b')
       in fromIntegral $ s * 3 + x

-- | Check a transaction that will replace these ones. Outputs that they spend
-- count as unspent, and outputs that they create as missing.
checkNewTx :: Writer -> [TxHash] -> Tx -> IO ()
checkNewTx wr replaced tx = do
  when (unique_inputs < length tx.inputs) $ do
    logError wr $
      "Transaction spends same output twice: "
        <> txHashToHex (txHash tx)
    throwIO DuplicatePrevOutput
  prevs <- mapM prev_output (prevOuts tx)
  when (any isNothing prevs) $ do
    logError wr $
      "Orphan: " <> txHashToHex (txHash tx)
    throwIO Orphan
  when (isCoinbaseTx tx) $ do
    logError wr $
      "Coinbase cannot be imported into mempool: "
        <> txHashToHex (txHash tx)
    throwIO UnexpectedCoinbase
  when (outputs > sum (map (.value) (catMaybes prevs))) $ do
    logError wr $
      "Insufficient funds for tx: " <> txHashToHex (txHash tx)
    throwIO InsufficientFunds
  where
    prev_output op
      | op.hash `elem` replaced = return Nothing
      | otherwise = do
          parent <- getActiveTxData wr op.hash
          return (parent >>= getTxOut op.index . (.tx))
    outputs = sum (map (.value) tx.outputs)
    unique_inputs = length (nub' (map (.outpoint) tx.inputs))

-- | Unspent outputs spent by a transaction, or 'Nothing' for those that are missing.
getUnspentOutputs :: Writer -> Tx -> IO [Maybe Unspent]
getUnspentOutputs wr tx = mapM (getUnspent wr) (prevOuts tx)

-- | Data of a new transaction, from its replace-by-fee flag, position, time and
-- the outputs it spends.
prepareTxData :: Bool -> BlockRef -> Word64 -> Tx -> [Unspent] -> TxData
prepareTxData rbf br tt tx us =
  TxData
    { block = br,
      tx = tx,
      prevs = ps,
      deleted = False,
      rbf = rbf,
      timestamp = tt,
      spenders = I.empty
    }
  where
    mkprv Unspent {..} = Prev script value
    ps = I.fromList $ zip [0 ..] $ map mkprv us

-- | Import a transaction at a position in the chain or the mempool. Throws 'Orphan' if it spends a missing output.
importTx ::
  Writer ->
  BlockRef ->
  -- | unix time
  Word64 ->
  -- | RBF
  Bool ->
  Tx ->
  IO TxData
importTx wr br tt rbf tx = do
  mus <- getUnspentOutputs wr tx
  us <- forM mus $ \case
    Nothing -> do
      logError wr $
        "Attempted to import a tx missing UTXO: "
          <> txHashToHex (txHash tx)
      throwIO Orphan
    Just u -> return u
  let td = prepareTxData rbf br tt tx us
  commitAddTx wr td
  return td

-- | Move a confirmed transaction back to the mempool.
unConfirmTx :: Writer -> TxData -> IO TxData
unConfirmTx wr t = confTx wr t Nothing

-- | Confirm a mempool transaction at a position in a block.
confirmTx :: Writer -> TxData -> BlockRef -> IO TxData
confirmTx wr t br = confTx wr t (Just br)

-- | Move a transaction to a new position in the indices of all its addresses.
replaceAddressTx :: Writer -> TxData -> BlockRef -> IO ()
replaceAddressTx wr t new = do
  net <- getNetwork wr
  ctx <- getCtx wr
  forM_ (txDataAddresses net ctx t) $ \a -> do
    deleteAddrTx wr
      a
      TxRef
        { block = t.block,
          txid = txHash t.tx
        }
    insertAddrTx wr
      a
      TxRef
        { block = new,
          txid = txHash t.tx
        }

-- | Move an unspent output to a new position in the chain, updating the indices
-- and balance of its address.
adjustAddressOutput ::
  Writer ->
  OutPoint ->
  TxOut ->
  BlockRef ->
  BlockRef ->
  IO ()
adjustAddressOutput wr op o old new = do
  let pk = o.script
  getUnspent wr op >>= \case
    Nothing -> return ()
    Just u -> do
      unless (u.block == old) $
        error $
          "Existing unspent block bad for output: " <> show op
      replace_unspent pk
  where
    replace_unspent pk = do
      net <- getNetwork wr
      ctx <- getCtx wr
      let ma = eitherToMaybe (scriptToAddressBS net ctx pk)
      deleteUnspent wr op
      insertUnspent wr
        Unspent
          { block = new,
            outpoint = op,
            value = o.value,
            script = pk,
            address = ma
          }
      forM_ ma $ replace_addr_unspent pk
    replace_addr_unspent pk a = do
      deleteAddrUnspent wr
        a
        Unspent
          { block = old,
            outpoint = op,
            value = o.value,
            script = pk,
            address = Just a
          }
      insertAddrUnspent wr
        a
        Unspent
          { block = new,
            outpoint = op,
            value = o.value,
            script = pk,
            address = Just a
          }
      decreaseBalance wr (confirmed old) a o.value
      increaseBalance wr (confirmed new) a o.value

-- | Confirm a transaction at a position in a block, or move it back to the mempool
-- if no position is given.
confTx :: Writer -> TxData -> Maybe BlockRef -> IO TxData
confTx wr t mbr = do
  replaceAddressTx wr t new
  forM_ (zip [0 ..] t.tx.outputs) $ \(n, o) -> do
    let op = OutPoint (txHash t.tx) n
    adjustAddressOutput wr op o old new
  rbf <- isRBF wr new t.tx
  let td = (t :: TxData) {block = new, rbf = rbf}
  insertTx wr td
  updateMempool wr td
  updateSpendersRBF wr td
  return td
  where
    new = fromMaybe (MemRef t.timestamp) mbr
    old = t.block

-- | Spenders inherit BIP125 signalling from unconfirmed parents only, so their
-- flag can change when a parent is confirmed or unconfirmed.
updateSpendersRBF :: Writer -> TxData -> IO ()
updateSpendersRBF wr td =
  forM_ (nub' (map (.txid) (I.elems td.spenders))) $ \h ->
    getActiveTxData wr h >>= mapM_ update
  where
    update s = do
      rbf <- isRBF wr s.block s.tx
      when (rbf /= s.rbf) $ do
        let s' = (s :: TxData) {rbf = rbf}
        insertTx wr s'
        updateSpendersRBF wr s'

-- | Delete the transactions that spend the same outputs as this one, with their
-- descendants. The first flag refuses to delete confirmed transactions, and the
-- second refuses any deletion on networks without replace-by-fee.
freeOutputs :: Writer -> Bool -> Bool -> Tx -> IO ()
freeOutputs wr memonly rbfcheck tx =
  getConflicts wr memonly rbfcheck tx >>= mapM_ (deleteSingleTx wr . txHash)

-- | Transactions that spend the same outputs as this one, with their
-- descendants, in an order in which they can be deleted.
getConflicts ::
  Writer ->
  -- | only delete transaction if unconfirmed
  Bool ->
  -- | only delete if network has RBF
  Bool ->
  Tx ->
  IO [Tx]
getConflicts wr memonly rbfcheck tx = do
  logDebug wr $
    "Getting conflicts for tx " <> txHashToHex (txHash tx) <> "..."
  let prevs = prevOuts tx
  unspents <- mapM (getUnspent wr) prevs
  let spents = [p | (p, Nothing) <- zip prevs unspents]
  spndrs <- catMaybes <$> mapM (getSpender wr) spents
  let txids = H.fromList $ filter (/= txHash tx) $ map (.txid) spndrs
  chains <- mapM (getChain wr memonly rbfcheck) (H.toList txids)
  return . reverse . map snd . sortTxs . M.elems $
    M.fromList [(txHash t, t) | t <- concat chains]

-- | Delete a transaction and its descendants, confirmed or not.
deleteConfirmedTx :: Writer -> TxHash -> IO ()
deleteConfirmedTx wr = deleteTx wr False False

-- | Delete an unconfirmed transaction and its descendants. If the flag is set and
-- the network has no replace-by-fee, throw 'DoubleSpend' instead. Does nothing if
-- the transaction is not found.
deleteUnconfirmedTx :: Writer -> Bool -> TxHash -> IO ()
deleteUnconfirmedTx wr rbfcheck th =
  getActiveTxData wr th >>= \case
    Just _ -> deleteTx wr True rbfcheck th
    Nothing ->
      logDebug wr $
        "Not found or already deleted: " <> txHashToHex th

-- | Delete a transaction and its descendants.
deleteTx ::
  Writer ->
  -- | only delete transaction if unconfirmed
  Bool ->
  -- | only delete if network has RBF
  Bool ->
  TxHash ->
  IO ()
deleteTx wr memonly rbfcheck th = do
  chain <- getChain wr memonly rbfcheck th
  logDebug wr $
    "Deleting "
      <> cs (show (length chain))
      <> " txs from chain leading to "
      <> txHashToHex th
  mapM_ (\t -> let h = txHash t in deleteSingleTx wr h >> return h) chain

-- | A transaction and all its descendants, in an order in which they can be
-- deleted: spenders first.
getChain ::
  Writer ->
  -- | only delete transaction if unconfirmed
  Bool ->
  -- | only delete if network has RBF
  Bool ->
  TxHash ->
  IO [Tx]
getChain wr memonly rbfcheck th' = do
  logDebug wr $
    "Getting chain for tx " <> txHashToHex th'
  sort_clean <$> go M.empty (H.singleton th')
  where
    sort_clean = reverse . map snd . sortTxs
    get_tx th =
      getActiveTxData wr th >>= \case
        Nothing -> do
          logDebug wr $
            "Transaction not found: " <> txHashToHex th
          return Nothing
        Just td
          | memonly && confirmed td.block -> do
              logError wr $
                "Transaction already confirmed: "
                  <> txHashToHex th
              throwIO TxConfirmed
          | rbfcheck ->
              getNetwork wr >>= \net ->
                if net.replaceByFee
                  then return $ Just td
                  else do
                    logError wr $
                      "Double-spending transaction: "
                        <> txHashToHex th
                    throwIO DoubleSpend
          | otherwise -> return $ Just td
    go txm pdg = do
      let ths = filter (not . (`M.member` txm)) (H.toList pdg)
      tds <- catMaybes <$> mapM get_tx ths
      let txmn = M.fromList $ fmap (\d -> (txHash d.tx, d.tx)) tds
          spds = concatMap (map (.txid) . I.elems . (.spenders)) tds
          pdg' = H.fromList spds
          txm' = txmn <> txm
      if H.null pdg'
        then do
          logDebug wr $
            "Chain for tx "
              <> txHashToHex th'
              <> " contains "
              <> cs (show (M.size txm'))
              <> " txs"
          return $ M.elems txm'
        else go txm' pdg'

-- | Delete a transaction that nothing spends. Throws 'TxSpent' otherwise.
deleteSingleTx :: Writer -> TxHash -> IO ()
deleteSingleTx wr th =
  getActiveTxData wr th >>= \case
    Nothing -> do
      logError wr $
        "Already deleted: " <> txHashToHex th
      throwIO TxNotFound
    Just td ->
      if I.null td.spenders
        then do
          logDebug wr $
            "Deleting tx: " <> txHashToHex th
          commitDelTx wr td
        else do
          logError wr $
            "Tried to delete spent tx: "
              <> txHashToHex th
          throwIO TxSpent

-- | Remove a transaction from every index and mark it as deleted.
commitDelTx :: Writer -> TxData -> IO ()
commitDelTx wr = commitModTx wr False

-- | Add a transaction to every index.
commitAddTx :: Writer -> TxData -> IO ()
commitAddTx wr = commitModTx wr True

-- | Add a transaction to every index, or remove it if the flag is 'False'.
commitModTx :: Writer -> Bool -> TxData -> IO ()
commitModTx wr add tx_data = do
  net <- getNetwork wr
  ctx <- getCtx wr
  mapM_ mod_addr_tx (txDataAddresses net ctx td)
  mod_outputs
  mod_unspent
  insertTx wr td
  updateMempool wr td
  where
    tx = td.tx
    br = td.block
    td = (tx_data :: TxData) {deleted = not add}
    tx_ref = TxRef br (txHash tx)
    mod_addr_tx a
      | add = do
          insertAddrTx wr a tx_ref
          modAddressCount wr add a
      | otherwise = do
          deleteAddrTx wr a tx_ref
          modAddressCount wr add a
    mod_unspent
      | add = spendOutputs wr tx
      | otherwise = unspendOutputs wr tx
    mod_outputs
      | add = addOutputs wr br tx
      | otherwise = delOutputs wr br tx

-- | Add a transaction to the mempool if it is unconfirmed, or remove it otherwise.
updateMempool :: Writer -> TxData -> IO ()
updateMempool wr td@TxData {deleted = True} =
  deleteFromMempool wr (txHash td.tx)
updateMempool wr td@TxData {block = MemRef t} =
  addToMempool wr (txHash td.tx) t
updateMempool wr td@TxData {block = BlockRef {}} =
  deleteFromMempool wr (txHash td.tx)

-- | Mark the outputs spent by a transaction as spent.
spendOutputs :: Writer -> Tx -> IO ()
spendOutputs wr tx =
  zipWithM_ (spendOutput wr (txHash tx)) [0 ..] (prevOuts tx)

-- | Add the outputs of a transaction as unspent.
addOutputs :: Writer -> BlockRef -> Tx -> IO ()
addOutputs wr br tx =
  zipWithM_ (addOutput wr br . OutPoint (txHash tx)) [0 ..] tx.outputs

-- | Whether an unconfirmed transaction can be replaced by fee: it signals BIP125
-- in one of its inputs, or it spends an unconfirmed transaction that can be
-- replaced. Always 'False' for confirmed transactions and on networks without
-- replace-by-fee.
isRBF ::
  Writer ->
  BlockRef ->
  Tx ->
  IO Bool
isRBF wr br tx
  | confirmed br = return False
  | otherwise =
      getNetwork wr >>= \net ->
        if net.replaceByFee
          then go
          else return False
  where
    go
      | any ((< 0xffffffff - 1) . (.sequence)) tx.inputs = return True
      | otherwise = carry_on
    carry_on =
      let hs = nub' $ map (.outpoint.hash) tx.inputs
          ck [] = return False
          ck (h : hs') =
            getActiveTxData wr h >>= \case
              Nothing -> return False
              Just t
                | confirmed t.block -> ck hs'
                | t.rbf -> return True
                | otherwise -> ck hs'
       in ck hs

-- | Add an unspent output.
addOutput :: Writer -> BlockRef -> OutPoint -> TxOut -> IO ()
addOutput wr = modOutput wr True

-- | Delete an output.
delOutput :: Writer -> BlockRef -> OutPoint -> TxOut -> IO ()
delOutput wr = modOutput wr False

-- | Add an unspent output, or delete it if the flag is 'False', updating the indices and balance of its address.
modOutput ::
  Writer ->
  Bool ->
  BlockRef ->
  OutPoint ->
  TxOut ->
  IO ()
modOutput wr add br op o = do
  net <- getNetwork wr
  ctx <- getCtx wr
  let ma = eitherToMaybe (scriptToAddressBS net ctx o.script)
  mod_unspent ma
  forM_ ma $ \a -> do
    mod_addr_unspent a (u ma)
    modBalance wr (confirmed br) add a o.value
    modifyReceived wr a v
  where
    v
      | add = (+ o.value)
      | otherwise = subtract o.value
    u ma =
      Unspent
        { script = o.script,
          block = br,
          outpoint = op,
          value = o.value,
          address = ma
        }
    mod_unspent ma
      | add = insertUnspent wr (u ma)
      | otherwise = deleteUnspent wr op
    mod_addr_unspent
      | add = insertAddrUnspent wr
      | otherwise = deleteAddrUnspent wr

-- | Delete the outputs of a transaction.
delOutputs :: Writer -> BlockRef -> Tx -> IO ()
delOutputs wr br tx =
  forM_ (zip [0 ..] tx.outputs) $ \(i, o) -> do
    let op = OutPoint (txHash tx) i
    delOutput wr br op o

-- | Data of a transaction. Throws 'TxNotFound' if it is missing or deleted.
getImportTxData :: Writer -> TxHash -> IO TxData
getImportTxData wr th =
  getActiveTxData wr th >>= \case
    Nothing -> do
      logDebug wr $ "Tx not found: " <> txHashToHex th
      throwIO TxNotFound
    Just d -> return d

-- | Output of a transaction at an index, or 'Nothing' if out of range.
getTxOut :: Word32 -> Tx -> Maybe TxOut
getTxOut i tx = do
  guard (fromIntegral i < length tx.outputs)
  return $ tx.outputs !! fromIntegral i

-- | Record which input spends an output.
insertSpender :: Writer -> OutPoint -> Spender -> IO ()
insertSpender wr op s = do
  td <- getImportTxData wr op.hash
  let p = td.spenders
      p' = I.insert (fromIntegral op.index) s p
      td' = (td :: TxData) {spenders = p'}
  insertTx wr td'

-- | Forget which input spends an output.
deleteSpender :: Writer -> OutPoint -> IO ()
deleteSpender wr op = do
  td <- getImportTxData wr op.hash
  let p = td.spenders
      p' = I.delete (fromIntegral op.index) p
      td' = (td :: TxData) {spenders = p'}
  insertTx wr td'

-- | Spend an unspent output with an input of a transaction.
spendOutput :: Writer -> TxHash -> Word32 -> OutPoint -> IO ()
spendOutput wr th ix op = do
  u <-
    getUnspent wr op >>= \case
      Just u -> return u
      Nothing -> error $ "Could not find UTXO to spend: " <> show op
  deleteUnspent wr op
  insertSpender wr op (Spender th ix)
  let pk = u.script
  net <- getNetwork wr
  ctx <- getCtx wr
  forM_ (scriptToAddressBS net ctx pk) $ \a -> do
    decreaseBalance wr (confirmed u.block) a u.value
    deleteAddrUnspent wr a u

-- | Make the outputs spent by a transaction unspent again.
unspendOutputs :: Writer -> Tx -> IO ()
unspendOutputs wr = mapM_ (unspendOutput wr) . prevOuts

-- | Make an output unspent again.
unspendOutput :: Writer -> OutPoint -> IO ()
unspendOutput wr op = do
  net <- getNetwork wr
  ctx <- getCtx wr
  t <-
    getActiveTxData wr op.hash >>= \case
      Nothing -> error $ "Could not find tx data: " <> show op.hash
      Just t -> return t
  let o =
        fromMaybe
          (error ("Could not find output: " <> show op))
          (getTxOut op.index t.tx)
      m = eitherToMaybe (scriptToAddressBS net ctx o.script)
      u =
        Unspent
          { value = o.value,
            block = t.block,
            script = o.script,
            outpoint = op,
            address = m
          }
  deleteSpender wr op
  insertUnspent wr u
  forM_ m $ \a -> do
    insertAddrUnspent wr a u
    increaseBalance wr (confirmed u.block) a o.value

-- | Change the total received by an address.
modifyReceived :: Writer -> Address -> (Word64 -> Word64) -> IO ()
modifyReceived wr a f = do
  b <- getDefaultBalance wr a
  setBalance wr b {received = f b.received}

-- | Remove an output from the confirmed or unconfirmed balance of an address.
decreaseBalance :: Writer -> Bool -> Address -> Word64 -> IO ()
decreaseBalance wr conf = modBalance wr conf False

-- | Add an output to the confirmed or unconfirmed balance of an address.
increaseBalance :: Writer -> Bool -> Address -> Word64 -> IO ()
increaseBalance wr conf = modBalance wr conf True

-- | Add or remove an output from the confirmed or unconfirmed balance of an address, and its count of unspent outputs.
modBalance ::
  Writer ->
  -- | confirmed
  Bool ->
  -- | add
  Bool ->
  Address ->
  Word64 ->
  IO ()
modBalance wr conf add a val = do
  b <- getDefaultBalance wr a
  setBalance wr $ (g . f) b
  where
    g b = (b :: Balance) {utxo = m 1 b.utxo}
    f b
      | conf = (b :: Balance) {confirmed = m val b.confirmed}
      | otherwise = (b :: Balance) {unconfirmed = m val b.unconfirmed}
    m
      | add = (+)
      | otherwise = subtract

-- | Increase or decrease the number of transactions of an address.
modAddressCount :: Writer -> Bool -> Address -> IO ()
modAddressCount wr add a = do
  b <- getDefaultBalance wr a
  setBalance wr b {txs = f b.txs}
  where
    f
      | add = (+ 1)
      | otherwise = subtract 1

-- | Addresses of a list of outputs, without duplicates.
txOutAddrs :: Network -> Ctx -> [TxOut] -> [Address]
txOutAddrs net ctx =
  nub' . rights . map (scriptToAddressBS net ctx . (.script))

-- | Addresses of the outputs spent by a transaction, without duplicates.
txInAddrs :: Network -> Ctx -> [Prev] -> [Address]
txInAddrs net ctx =
  nub' . rights . map (scriptToAddressBS net ctx . (.script))

-- | Addresses that a transaction is indexed under. An output script counts
-- only when it has an address on the network.
txDataAddresses :: Network -> Ctx -> TxData -> [Address]
txDataAddresses net ctx t =
  nub' $ txInAddrs net ctx prevs <> txOutAddrs net ctx outs
  where
    prevs = I.elems t.prevs
    outs = t.tx.outputs

-- | Outputs spent by a transaction, without the null outpoint of a coinbase.
prevOuts :: Tx -> [OutPoint]
prevOuts tx = filter (/= nullOutPoint) (map (.outpoint) tx.inputs)

-- | Whether a transaction is in the database and not deleted.
testPresent :: Writer -> Tx -> IO Bool
testPresent wr tx = isJust <$> getActiveTxData wr (txHash tx)
