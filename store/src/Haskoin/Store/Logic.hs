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
-- mempool. They run in 'ImportM', which buffers all changes and commits them in
-- one atomic batch, so a failure leaves the database untouched.
--
-- When a block is imported, transactions already in the mempool are confirmed,
-- new ones are imported, and mempool transactions that conflict with the block
-- are deleted together with their descendants. When a block is reverted, its
-- transactions go back to the mempool and its coinbase is deleted.
--
-- On networks with replace-by-fee, a new mempool transaction may replace the
-- unconfirmed transactions it conflicts with. Elsewhere a conflicting mempool
-- transaction is rejected as a double spend.
module Haskoin.Store.Logic
  ( ImportException (..),
    ImportM,
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
import Haskoin.Store.Common
import Haskoin.Store.Data
import Haskoin.Store.Database.Writer (WriterT, writerLogger)
import UnliftIO (Exception, throwIO)

-- | Block and transaction import. Failures are thrown as 'ImportException'.
-- Writes are buffered and committed together when 'runWriter' finishes, so a
-- failed batch commits nothing.
type ImportM = WriterT IO

-- | Log a message with the logger of the writer.
logDebug, logError :: Text -> ImportM ()
logDebug msg = writerLogger >>= \l -> logAt LevelDebug l msg
logError msg = writerLogger >>= \l -> logAt LevelError l msg

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
initBest :: ImportM ()
initBest = do
  logDebug "Initializing best block"
  net <- getNetwork
  m <- getBestBlock
  when (isNothing m) . void $ do
    logDebug "Importing Genesis block"
    importBlock (genesisBlock net) (genesisNode net)

-- | Import a transaction into the mempool with the time it arrived. Returns
-- 'False' if it is already known.
--
-- Conflicting unconfirmed transactions are deleted together with their
-- descendants if the network has replace-by-fee; otherwise the transaction is
-- rejected with 'DoubleSpend'. Throws an 'ImportException' if the transaction is
-- invalid, for example if it spends unknown outputs.
newMempoolTx :: Tx -> UnixTime -> ImportM Bool
newMempoolTx tx w =
  getActiveTxData (txHash tx) >>= \case
    Just _ ->
      return False
    Nothing -> do
      -- Check everything before writing, so that a rejected transaction
      -- leaves the mempool as it was.
      replaced <- getConflicts True True tx
      checkNewTx (map txHash replaced) tx
      mapM_ (deleteSingleTx . txHash) replaced
      rbf <- isRBF (MemRef w) tx
      _ <- importTx (MemRef w) w rbf tx
      return True

-- | Data of the best block. Throws 'BestBlockNotFound' if there is none.
bestBlockData :: ImportM BlockData
bestBlockData = do
  h <-
    getBestBlock >>= \case
      Nothing -> do
        logError "Best block unknown"
        throwIO BestBlockNotFound
      Just h -> return h
  getBlock h >>= \case
    Nothing -> do
      logError "Best block not found"
      throwIO BestBlockNotFound
    Just b -> return b

-- | Revert the best block: make its parent the best block, mark it as not in the
-- main chain, move its transactions back to the mempool, and delete its
-- coinbase. Throws 'BlockNotBest' if the block is not the best block.
revertBlock :: BlockHash -> ImportM ()
revertBlock bh = do
  bd <-
    bestBlockData >>= \b ->
      if headerHash b.header == bh
        then return b
        else do
          logError $
            "Cannot revert non-head block: " <> blockHashToHex bh
          throwIO BlockNotBest
  logDebug $
    "Obtained block data for " <> blockHashToHex bh
  tds <- mapM getImportTxData bd.txs
  logDebug $
    "Obtained import tx data for block " <> blockHashToHex bh
  setBest bd.header.prev
  logDebug $
    "Set parent as best block "
      <> blockHashToHex bd.header.prev
  insertBlock bd {main = False}
  logDebug $
    "Updated as not in main chain: " <> blockHashToHex bh
  forM_ (drop 1 tds) unConfirmTx
  logDebug $
    "Unconfirmed " <> cs (show (length tds)) <> " transactions"
  deleteConfirmedTx (txHash (tds !! 0).tx)
  logDebug $
    "Deleted coinbase: " <> txHashToHex (txHash (tds !! 0).tx)

-- | Check that a block builds on the best block, or that it is the genesis block
-- of an empty database.
checkNewBlock :: Block -> BlockNode -> ImportM ()
checkNewBlock b n =
  getBestBlock >>= \case
    Nothing
      | isGenesis n -> return ()
      | otherwise -> do
          logError $
            "Cannot import non-genesis block: "
              <> blockHashToHex (headerHash b.header)
          throwIO BestBlockNotFound
    Just h
      | b.header.prev == h -> return ()
      | otherwise -> do
          logError $
            "Block does not build on head: "
              <> blockHashToHex (headerHash b.header)
          throwIO PrevBlockNotBest

-- | Import the transactions of a block, in dependency order. Mempool transactions
-- that conflict with the block are deleted first. Transactions already in the
-- mempool are confirmed; others are imported.
importOrConfirm :: BlockNode -> [Tx] -> ImportM [TxData]
importOrConfirm bn txns = do
  logDebug "Freeing outputs..."
  mapM_ (freeOutputs True False . snd) (reverse txs)
  logDebug "Outputs freed"
  mapM (uncurry action) txs
  where
    txs = sortTxs txns
    br i = BlockRef {height = bn.height, position = i}
    bn_time = fromIntegral $ bn.header.timestamp
    action i tx =
      testPresent tx >>= \case
        False -> import_it i tx
        True -> confirm_it i tx
    confirm_it i tx =
      getActiveTxData (txHash tx) >>= \case
        Just t -> do
          logDebug $
            "Confirming tx: "
              <> txHashToHex (txHash tx)
          confirmTx t (br i)
        Nothing -> do
          logError $
            "Cannot find tx to confirm: "
              <> txHashToHex (txHash tx)
          throwIO TxNotFound
    import_it i tx = do
      logDebug $
        "Importing tx: " <> txHashToHex (txHash tx)
      importTx (br i) bn_time False tx

-- | Import a block that builds on the best block, and make it the best block.
-- Returns its data and the data of its transactions.
importBlock :: Block -> BlockNode -> ImportM (BlockData, [TxData])
importBlock b n = do
  logDebug $
    "Checking new block: "
      <> blockHashToHex (headerHash n.header)
  checkNewBlock b n
  logDebug "Passed check"
  net <- getNetwork
  let subsidy = computeSubsidy net n.height
  bs <- getBlocksAtHeight n.height
  logDebug $
    "Inserting block entries for: "
      <> blockHashToHex (headerHash n.header)
  setBlocksAtHeight
    (nub (headerHash n.header : bs))
    n.height
  setBest (headerHash n.header)
  tds <- importOrConfirm n b.txs
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
  insertBlock bd
  logDebug $
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
checkNewTx :: [TxHash] -> Tx -> ImportM ()
checkNewTx replaced tx = do
  when (unique_inputs < length tx.inputs) $ do
    logError $
      "Transaction spends same output twice: "
        <> txHashToHex (txHash tx)
    throwIO DuplicatePrevOutput
  prevs <- mapM prev_output (prevOuts tx)
  when (any isNothing prevs) $ do
    logError $
      "Orphan: " <> txHashToHex (txHash tx)
    throwIO Orphan
  when (isCoinbaseTx tx) $ do
    logError $
      "Coinbase cannot be imported into mempool: "
        <> txHashToHex (txHash tx)
    throwIO UnexpectedCoinbase
  when (outputs > sum (map (.value) (catMaybes prevs))) $ do
    logError $
      "Insufficient funds for tx: " <> txHashToHex (txHash tx)
    throwIO InsufficientFunds
  where
    prev_output op
      | op.hash `elem` replaced = return Nothing
      | otherwise = do
          parent <- getActiveTxData op.hash
          return (parent >>= getTxOut op.index . (.tx))
    outputs = sum (map (.value) tx.outputs)
    unique_inputs = length (nub' (map (.outpoint) tx.inputs))

-- | Unspent outputs spent by a transaction, or 'Nothing' for those that are missing.
getUnspentOutputs :: (StoreReadBase m) => Tx -> m [Maybe Unspent]
getUnspentOutputs tx = mapM getUnspent (prevOuts tx)

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
  BlockRef ->
  -- | unix time
  Word64 ->
  -- | RBF
  Bool ->
  Tx ->
  ImportM TxData
importTx br tt rbf tx = do
  mus <- getUnspentOutputs tx
  us <- forM mus $ \case
    Nothing -> do
      logError $
        "Attempted to import a tx missing UTXO: "
          <> txHashToHex (txHash tx)
      throwIO Orphan
    Just u -> return u
  let td = prepareTxData rbf br tt tx us
  commitAddTx td
  return td

-- | Move a confirmed transaction back to the mempool.
unConfirmTx :: TxData -> ImportM TxData
unConfirmTx t = confTx t Nothing

-- | Confirm a mempool transaction at a position in a block.
confirmTx :: TxData -> BlockRef -> ImportM TxData
confirmTx t br = confTx t (Just br)

-- | Move a transaction to a new position in the indices of all its addresses.
replaceAddressTx :: TxData -> BlockRef -> ImportM ()
replaceAddressTx t new = do
  net <- getNetwork
  ctx <- getCtx
  forM_ (txDataAddresses net ctx t) $ \a -> do
    deleteAddrTx
      a
      TxRef
        { block = t.block,
          txid = txHash t.tx
        }
    insertAddrTx
      a
      TxRef
        { block = new,
          txid = txHash t.tx
        }

-- | Move an unspent output to a new position in the chain, updating the indices
-- and balance of its address.
adjustAddressOutput ::
  OutPoint ->
  TxOut ->
  BlockRef ->
  BlockRef ->
  ImportM ()
adjustAddressOutput op o old new = do
  let pk = o.script
  getUnspent op >>= \case
    Nothing -> return ()
    Just u -> do
      unless (u.block == old) $
        error $
          "Existing unspent block bad for output: " <> show op
      replace_unspent pk
  where
    replace_unspent pk = do
      net <- getNetwork
      ctx <- getCtx
      let ma = eitherToMaybe (scriptToAddressBS net ctx pk)
      deleteUnspent op
      insertUnspent
        Unspent
          { block = new,
            outpoint = op,
            value = o.value,
            script = pk,
            address = ma
          }
      forM_ ma $ replace_addr_unspent pk
    replace_addr_unspent pk a = do
      deleteAddrUnspent
        a
        Unspent
          { block = old,
            outpoint = op,
            value = o.value,
            script = pk,
            address = Just a
          }
      insertAddrUnspent
        a
        Unspent
          { block = new,
            outpoint = op,
            value = o.value,
            script = pk,
            address = Just a
          }
      decreaseBalance (confirmed old) a o.value
      increaseBalance (confirmed new) a o.value

-- | Confirm a transaction at a position in a block, or move it back to the mempool
-- if no position is given.
confTx :: TxData -> Maybe BlockRef -> ImportM TxData
confTx t mbr = do
  replaceAddressTx t new
  forM_ (zip [0 ..] t.tx.outputs) $ \(n, o) -> do
    let op = OutPoint (txHash t.tx) n
    adjustAddressOutput op o old new
  rbf <- isRBF new t.tx
  let td = (t :: TxData) {block = new, rbf = rbf}
  insertTx td
  updateMempool td
  updateSpendersRBF td
  return td
  where
    new = fromMaybe (MemRef t.timestamp) mbr
    old = t.block

-- | Spenders inherit BIP125 signalling from unconfirmed parents only, so their
-- flag can change when a parent is confirmed or unconfirmed.
updateSpendersRBF :: TxData -> ImportM ()
updateSpendersRBF td =
  forM_ (nub' (map (.txid) (I.elems td.spenders))) $ \h ->
    getActiveTxData h >>= mapM_ update
  where
    update s = do
      rbf <- isRBF s.block s.tx
      when (rbf /= s.rbf) $ do
        let s' = (s :: TxData) {rbf = rbf}
        insertTx s'
        updateSpendersRBF s'

-- | Delete the transactions that spend the same outputs as this one, with their
-- descendants. The first flag refuses to delete confirmed transactions, and the
-- second refuses any deletion on networks without replace-by-fee.
freeOutputs :: Bool -> Bool -> Tx -> ImportM ()
freeOutputs memonly rbfcheck tx =
  getConflicts memonly rbfcheck tx >>= mapM_ (deleteSingleTx . txHash)

-- | Transactions that spend the same outputs as this one, with their
-- descendants, in an order in which they can be deleted.
getConflicts ::
  -- | only delete transaction if unconfirmed
  Bool ->
  -- | only delete if network has RBF
  Bool ->
  Tx ->
  ImportM [Tx]
getConflicts memonly rbfcheck tx = do
  logDebug $
    "Getting conflicts for tx " <> txHashToHex (txHash tx) <> "..."
  let prevs = prevOuts tx
  unspents <- mapM getUnspent prevs
  let spents = [p | (p, Nothing) <- zip prevs unspents]
  spndrs <- catMaybes <$> mapM getSpender spents
  let txids = H.fromList $ filter (/= txHash tx) $ map (.txid) spndrs
  chains <- mapM (getChain memonly rbfcheck) (H.toList txids)
  return . reverse . map snd . sortTxs . M.elems $
    M.fromList [(txHash t, t) | t <- concat chains]

-- | Delete a transaction and its descendants, confirmed or not.
deleteConfirmedTx :: TxHash -> ImportM ()
deleteConfirmedTx = deleteTx False False

-- | Delete an unconfirmed transaction and its descendants. If the flag is set and
-- the network has no replace-by-fee, throw 'DoubleSpend' instead. Does nothing if
-- the transaction is not found.
deleteUnconfirmedTx :: Bool -> TxHash -> ImportM ()
deleteUnconfirmedTx rbfcheck th =
  getActiveTxData th >>= \case
    Just _ -> deleteTx True rbfcheck th
    Nothing ->
      logDebug $
        "Not found or already deleted: " <> txHashToHex th

-- | Delete a transaction and its descendants.
deleteTx ::
  -- | only delete transaction if unconfirmed
  Bool ->
  -- | only delete if network has RBF
  Bool ->
  TxHash ->
  ImportM ()
deleteTx memonly rbfcheck th = do
  chain <- getChain memonly rbfcheck th
  logDebug $
    "Deleting "
      <> cs (show (length chain))
      <> " txs from chain leading to "
      <> txHashToHex th
  mapM_ (\t -> let h = txHash t in deleteSingleTx h >> return h) chain

-- | A transaction and all its descendants, in an order in which they can be
-- deleted: spenders first.
getChain ::
  -- | only delete transaction if unconfirmed
  Bool ->
  -- | only delete if network has RBF
  Bool ->
  TxHash ->
  ImportM [Tx]
getChain memonly rbfcheck th' = do
  logDebug $
    "Getting chain for tx " <> txHashToHex th'
  sort_clean <$> go M.empty (H.singleton th')
  where
    sort_clean = reverse . map snd . sortTxs
    get_tx th =
      getActiveTxData th >>= \case
        Nothing -> do
          logDebug $
            "Transaction not found: " <> txHashToHex th
          return Nothing
        Just td
          | memonly && confirmed td.block -> do
              logError $
                "Transaction already confirmed: "
                  <> txHashToHex th
              throwIO TxConfirmed
          | rbfcheck ->
              getNetwork >>= \net ->
                if net.replaceByFee
                  then return $ Just td
                  else do
                    logError $
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
          logDebug $
            "Chain for tx "
              <> txHashToHex th'
              <> " contains "
              <> cs (show (M.size txm'))
              <> " txs"
          return $ M.elems txm'
        else go txm' pdg'

-- | Delete a transaction that nothing spends. Throws 'TxSpent' otherwise.
deleteSingleTx :: TxHash -> ImportM ()
deleteSingleTx th =
  getActiveTxData th >>= \case
    Nothing -> do
      logError $
        "Already deleted: " <> txHashToHex th
      throwIO TxNotFound
    Just td ->
      if I.null td.spenders
        then do
          logDebug $
            "Deleting tx: " <> txHashToHex th
          commitDelTx td
        else do
          logError $
            "Tried to delete spent tx: "
              <> txHashToHex th
          throwIO TxSpent

-- | Remove a transaction from every index and mark it as deleted.
commitDelTx :: TxData -> ImportM ()
commitDelTx = commitModTx False

-- | Add a transaction to every index.
commitAddTx :: TxData -> ImportM ()
commitAddTx = commitModTx True

-- | Add a transaction to every index, or remove it if the flag is 'False'.
commitModTx :: Bool -> TxData -> ImportM ()
commitModTx add tx_data = do
  net <- getNetwork
  ctx <- getCtx
  mapM_ mod_addr_tx (txDataAddresses net ctx td)
  mod_outputs
  mod_unspent
  insertTx td
  updateMempool td
  where
    tx = td.tx
    br = td.block
    td = (tx_data :: TxData) {deleted = not add}
    tx_ref = TxRef br (txHash tx)
    mod_addr_tx a
      | add = do
          insertAddrTx a tx_ref
          modAddressCount add a
      | otherwise = do
          deleteAddrTx a tx_ref
          modAddressCount add a
    mod_unspent
      | add = spendOutputs tx
      | otherwise = unspendOutputs tx
    mod_outputs
      | add = addOutputs br tx
      | otherwise = delOutputs br tx

-- | Add a transaction to the mempool if it is unconfirmed, or remove it otherwise.
updateMempool :: TxData -> ImportM ()
updateMempool td@TxData {deleted = True} =
  deleteFromMempool (txHash td.tx)
updateMempool td@TxData {block = MemRef t} =
  addToMempool (txHash td.tx) t
updateMempool td@TxData {block = BlockRef {}} =
  deleteFromMempool (txHash td.tx)

-- | Mark the outputs spent by a transaction as spent.
spendOutputs :: Tx -> ImportM ()
spendOutputs tx =
  zipWithM_ (spendOutput (txHash tx)) [0 ..] (prevOuts tx)

-- | Add the outputs of a transaction as unspent.
addOutputs :: BlockRef -> Tx -> ImportM ()
addOutputs br tx =
  zipWithM_ (addOutput br . OutPoint (txHash tx)) [0 ..] tx.outputs

-- | Whether an unconfirmed transaction can be replaced by fee: it signals BIP125
-- in one of its inputs, or it spends an unconfirmed transaction that can be
-- replaced. Always 'False' for confirmed transactions and on networks without
-- replace-by-fee.
isRBF ::
  (StoreReadBase m) =>
  BlockRef ->
  Tx ->
  m Bool
isRBF br tx
  | confirmed br = return False
  | otherwise =
      getNetwork >>= \net ->
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
            getActiveTxData h >>= \case
              Nothing -> return False
              Just t
                | confirmed t.block -> ck hs'
                | t.rbf -> return True
                | otherwise -> ck hs'
       in ck hs

-- | Add an unspent output.
addOutput :: BlockRef -> OutPoint -> TxOut -> ImportM ()
addOutput = modOutput True

-- | Delete an output.
delOutput :: BlockRef -> OutPoint -> TxOut -> ImportM ()
delOutput = modOutput False

-- | Add an unspent output, or delete it if the flag is 'False', updating the indices and balance of its address.
modOutput ::
  Bool ->
  BlockRef ->
  OutPoint ->
  TxOut ->
  ImportM ()
modOutput add br op o = do
  net <- getNetwork
  ctx <- getCtx
  let ma = eitherToMaybe (scriptToAddressBS net ctx o.script)
  mod_unspent ma
  forM_ ma $ \a -> do
    mod_addr_unspent a (u ma)
    modBalance (confirmed br) add a o.value
    modifyReceived a v
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
      | add = insertUnspent (u ma)
      | otherwise = deleteUnspent op
    mod_addr_unspent
      | add = insertAddrUnspent
      | otherwise = deleteAddrUnspent

-- | Delete the outputs of a transaction.
delOutputs :: BlockRef -> Tx -> ImportM ()
delOutputs br tx =
  forM_ (zip [0 ..] tx.outputs) $ \(i, o) -> do
    let op = OutPoint (txHash tx) i
    delOutput br op o

-- | Data of a transaction. Throws 'TxNotFound' if it is missing or deleted.
getImportTxData :: TxHash -> ImportM TxData
getImportTxData th =
  getActiveTxData th >>= \case
    Nothing -> do
      logDebug $ "Tx not found: " <> txHashToHex th
      throwIO TxNotFound
    Just d -> return d

-- | Output of a transaction at an index, or 'Nothing' if out of range.
getTxOut :: Word32 -> Tx -> Maybe TxOut
getTxOut i tx = do
  guard (fromIntegral i < length tx.outputs)
  return $ tx.outputs !! fromIntegral i

-- | Record which input spends an output.
insertSpender :: OutPoint -> Spender -> ImportM ()
insertSpender op s = do
  td <- getImportTxData op.hash
  let p = td.spenders
      p' = I.insert (fromIntegral op.index) s p
      td' = (td :: TxData) {spenders = p'}
  insertTx td'

-- | Forget which input spends an output.
deleteSpender :: OutPoint -> ImportM ()
deleteSpender op = do
  td <- getImportTxData op.hash
  let p = td.spenders
      p' = I.delete (fromIntegral op.index) p
      td' = (td :: TxData) {spenders = p'}
  insertTx td'

-- | Spend an unspent output with an input of a transaction.
spendOutput :: TxHash -> Word32 -> OutPoint -> ImportM ()
spendOutput th ix op = do
  u <-
    getUnspent op >>= \case
      Just u -> return u
      Nothing -> error $ "Could not find UTXO to spend: " <> show op
  deleteUnspent op
  insertSpender op (Spender th ix)
  let pk = u.script
  net <- getNetwork
  ctx <- getCtx
  forM_ (scriptToAddressBS net ctx pk) $ \a -> do
    decreaseBalance (confirmed u.block) a u.value
    deleteAddrUnspent a u

-- | Make the outputs spent by a transaction unspent again.
unspendOutputs :: Tx -> ImportM ()
unspendOutputs = mapM_ unspendOutput . prevOuts

-- | Make an output unspent again.
unspendOutput :: OutPoint -> ImportM ()
unspendOutput op = do
  net <- getNetwork
  ctx <- getCtx
  t <-
    getActiveTxData op.hash >>= \case
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
  deleteSpender op
  insertUnspent u
  forM_ m $ \a -> do
    insertAddrUnspent a u
    increaseBalance (confirmed u.block) a o.value

-- | Change the total received by an address.
modifyReceived :: Address -> (Word64 -> Word64) -> ImportM ()
modifyReceived a f = do
  b <- getDefaultBalance a
  setBalance b {received = f b.received}

-- | Remove an output from the confirmed or unconfirmed balance of an address.
decreaseBalance :: Bool -> Address -> Word64 -> ImportM ()
decreaseBalance conf = modBalance conf False

-- | Add an output to the confirmed or unconfirmed balance of an address.
increaseBalance :: Bool -> Address -> Word64 -> ImportM ()
increaseBalance conf = modBalance conf True

-- | Add or remove an output from the confirmed or unconfirmed balance of an address, and its count of unspent outputs.
modBalance ::
  -- | confirmed
  Bool ->
  -- | add
  Bool ->
  Address ->
  Word64 ->
  ImportM ()
modBalance conf add a val = do
  b <- getDefaultBalance a
  setBalance $ (g . f) b
  where
    g b = (b :: Balance) {utxo = m 1 b.utxo}
    f b
      | conf = (b :: Balance) {confirmed = m val b.confirmed}
      | otherwise = (b :: Balance) {unconfirmed = m val b.unconfirmed}
    m
      | add = (+)
      | otherwise = subtract

-- | Increase or decrease the number of transactions of an address.
modAddressCount :: Bool -> Address -> ImportM ()
modAddressCount add a = do
  b <- getDefaultBalance a
  setBalance b {txs = f b.txs}
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
testPresent :: (StoreReadBase m) => Tx -> m Bool
testPresent tx = isJust <$> getActiveTxData (txHash tx)
