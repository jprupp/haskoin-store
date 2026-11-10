{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -Wno-ambiguous-fields #-}

-- |
-- Module      : Haskoin.Store.Logic.Layered
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Rules to import and revert blocks and transactions in the layered layout
-- of the database (@docs\/layered-database.md@), in which the mempool is
-- kept apart from the chain. These are the rules that the block store
-- imports with. They do what "Haskoin.Store.Logic" did for the layout of
-- version 20, with the same functions, the same results and the same
-- errors, which is how the block store moved from one to the other, and
-- how the tests compare the two.
--
-- = Two sets of rules
--
-- __The chain__ has one door. 'importBlock' and 'revertBlock' are the only
-- functions here that write to the column families of the chain, and what
-- they write depends on the block and on those column families alone. A
-- block is imported the same way whether or not the store had seen its
-- transactions before, as during the initial sync: every output it spends
-- must be unspent in the chain, or the block is refused.
--
-- __The mempool__ is a set of transactions, each with its keys in the
-- mempool area ('mempoolRecords'). A transaction enters by having its keys
-- written and leaves by having them deleted; nothing else is read or
-- changed, and nothing is written to the chain. What is left of the rules
-- is the checks: that the outputs a transaction spends exist and are
-- unspent, in the chain or in the mempool, and what it replaces.
--
-- The two meet in three places. A block takes the transactions it confirms
-- out of the mempool, and deletes those that conflict with it. A reverted
-- block gives its transactions back to the mempool. And a mempool
-- transaction may spend outputs of the chain, which the chain goes on
-- listing as unspent: only the mempool area says otherwise.
--
-- = Same answers as version 20
--
-- For any sequence of blocks and transactions, these rules leave the
-- database that "Haskoin.Store.Logic" followed by
-- "Haskoin.Store.Database.Migrate" leaves, key for key, and each step gives
-- the same result. The tests check that for random sequences. There is one
-- difference by design. A block that spends an output of an unconfirmed
-- transaction is refused here, because that output is not in the chain;
-- version 20 imports it, since its indices do not tell the two apart.
module Haskoin.Store.Logic.Layered
  ( initBest,
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
import Data.ByteString qualified as B
import Data.Either (rights)
import Data.Functor ((<&>))
import Data.HashMap.Strict qualified as M
import Data.HashSet qualified as H
import Data.IntMap.Strict qualified as I
import Data.List (nub)
import Data.Maybe
  ( catMaybes,
    fromMaybe,
    isJust,
    isNothing,
    mapMaybe,
  )
import Data.Serialize (encode)
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
import Haskoin.Store.Database.Mempool (MemRecords (..), mempoolRecords)
import Haskoin.Store.Database.Types (MemAddrKey (..), MemAddrVal (..), MemSummary (..))
import Haskoin.Store.Database.Writer
import Haskoin.Store.Logic (ImportException (..))
import UnliftIO (throwIO)

-------------------------------------------------------------------------------
-- Reading
-------------------------------------------------------------------------------

-- | Log a message with the logger of the writer.
logDebug, logError :: Writer -> Text -> IO ()
logDebug wr = logAt LevelDebug (writerLogger wr)
logError wr = logAt LevelError (writerLogger wr)

-- | Address of an output script, if it has one on the network.
scriptAddress :: Writer -> B.ByteString -> Maybe Address
scriptAddress wr = eitherToMaybe . scriptToAddressBS (writerNet wr) (writerCtx wr)

-- | Data of a confirmed transaction, with the spenders of its outputs that
-- are in blocks. Version 20 kept the bodies of deleted transactions in the
-- same place; they do not count.
chainTx :: Writer -> TxHash -> IO (Maybe TxData)
chainTx wr h =
  getTxData wr h <&> \case
    Just td | not td.deleted -> Just td
    _ -> Nothing

-- | Data of a transaction that is in the mempool or in the chain, without
-- the mempool spenders of its outputs.
activeBody :: Writer -> TxHash -> IO (Maybe TxData)
activeBody wr h = getMempoolTx wr h >>= maybe (chainTx wr h) (return . Just)

-- | Data of a transaction that is in the mempool or in the chain, with every
-- spender of its outputs, in blocks and in the mempool.
activeTx :: Writer -> TxHash -> IO (Maybe TxData)
activeTx wr h = activeBody wr h >>= mapM spent
  where
    spent td = do
      ms <- getMempoolSpenders wr h (length td.tx.outputs)
      return (td :: TxData) {spenders = I.union td.spenders ms}

-- | An output that a new mempool transaction may spend: one that the chain
-- has as unspent, or one of a mempool transaction, that no mempool
-- transaction spends.
spendable :: Writer -> OutPoint -> IO (Maybe Unspent)
spendable wr p =
  getMempoolSpender wr p >>= \case
    Just _ -> return Nothing
    Nothing ->
      getUnspent wr p >>= \case
        Just u -> return (Just u)
        Nothing -> (>>= output) <$> getMempoolTx wr p.hash
  where
    output td = unspentOutput wr td.block p <$> getTxOut p.index td.tx

-- | An output as an unspent output, given the position of its transaction.
unspentOutput :: Writer -> BlockRef -> OutPoint -> TxOut -> Unspent
unspentOutput wr br p o =
  Unspent
    { block = br,
      outpoint = p,
      value = o.value,
      script = o.script,
      address = scriptAddress wr o.script
    }

-- | Input that spends an output, in the mempool or in a block.
spenderOf :: Writer -> OutPoint -> IO (Maybe Spender)
spenderOf wr p = getMempoolSpender wr p >>= maybe (getSpender wr p) (return . Just)

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

-------------------------------------------------------------------------------
-- Blocks
-------------------------------------------------------------------------------

-- | Import the genesis block if the database has no best block yet.
initBest :: Writer -> IO ()
initBest wr = do
  logDebug wr "Initializing best block"
  let net = writerNet wr
  m <- getBestBlock wr
  when (isNothing m) . void $ do
    logDebug wr "Importing Genesis block"
    importBlock wr (genesisBlock net) (genesisNode net)

-- | Check that a block builds on the best block, or that it is the genesis block
-- of an empty database.
checkNewBlock :: Writer -> Block -> BlockNode -> IO ()
checkNewBlock wr b n =
  getBestBlock wr >>= \case
    Nothing
      | isGenesis n -> return ()
      | otherwise -> do
          logError wr $ "Cannot import non-genesis block: " <> blockHashToHex (headerHash b.header)
          throwIO BestBlockNotFound
    Just h
      | b.header.prev == h -> return ()
      | otherwise -> do
          logError wr $ "Block does not build on head: " <> blockHashToHex (headerHash b.header)
          throwIO PrevBlockNotBest

-- | Import a block that builds on the best block, and make it the best block.
-- Returns its data and the data of its transactions.
--
-- The block goes into the chain's column families as it would into those of
-- a store with an empty mempool. Before that, the mempool gives way: the
-- transactions of the block that were in it leave it, and the mempool
-- transactions that spend an output that the block spends are deleted, with
-- their descendants.
importBlock :: Writer -> Block -> BlockNode -> IO (BlockData, [TxData])
importBlock wr b n = do
  checkNewBlock wr b n
  let net = writerNet wr
      subsidy = computeSubsidy net n.height
  bs <- getBlocksAtHeight wr n.height
  setBlocksAtHeight wr (nub (headerHash n.header : bs)) n.height
  setBest wr (headerHash n.header)
  tds <- importTxs wr n b.txs
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
  logDebug wr $ "Finished importing block: " <> blockHashToHex (headerHash n.header)
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

-- | Import the transactions of a block, in dependency order, after deleting
-- the mempool transactions that conflict with them.
importTxs :: Writer -> BlockNode -> [Tx] -> IO [TxData]
importTxs wr bn txns = do
  mapM_ (freeOutputs . snd) (reverse txs)
  mapM (uncurry action) txs
  where
    txs = sortTxs txns
    br i = BlockRef {height = bn.height, position = i}
    blockTime = fromIntegral bn.header.timestamp
    freeOutputs tx = getConflicts wr True False tx >>= mapM_ (deleteSingleTx wr . txHash)
    action i tx =
      getMempoolTx wr (txHash tx) >>= \case
        Just td -> do
          -- it leaves the mempool, and keeps the time it was first seen
          removeMempoolTx wr td
          td' <- importChainTx wr (br i) td.timestamp tx
          updateSpendersRBF wr tx
          return td'
        Nothing ->
          chainTx wr (txHash tx) >>= \case
            Just td -> moveChainTx wr td (br i)
            Nothing -> importChainTx wr (br i) blockTime tx

-- | Import a transaction of a block into the chain. Throws 'Orphan' if an
-- output that it spends is not unspent in the chain.
importChainTx :: Writer -> BlockRef -> Word64 -> Tx -> IO TxData
importChainTx wr br time tx = do
  us <- forM (prevOuts tx) $ \p ->
    getUnspent wr p >>= \case
      Just u -> return u
      Nothing -> do
        logError wr $ "Attempted to import a tx missing UTXO: " <> txHashToHex (txHash tx)
        throwIO Orphan
  let td = prepareTxData False br time tx us
  addChainTx wr td
  -- it may have been deleted from the mempool before
  getDeletedTx wr (txHash tx) >>= mapM_ (\_ -> deleteDeletedTx wr (txHash tx))
  return td

-- | Give a confirmed transaction a new position in the chain: a block has a
-- transaction that is already confirmed. Only two coinbases of 2010 are, on
-- Bitcoin; the later one takes the place of the earlier, with the outputs
-- that are still unspent, as in the layout of version 20.
moveChainTx :: Writer -> TxData -> BlockRef -> IO TxData
moveChainTx wr t new = do
  forM_ (txDataAddresses wr t) $ \a -> do
    deleteAddrTx wr a TxRef {block = t.block, txid = h}
    insertAddrTx wr a TxRef {block = new, txid = h}
  forM_ (zip [0 ..] t.tx.outputs) $ \(i, _) ->
    getUnspent wr (OutPoint h i) >>= mapM_ move
  let td = (t :: TxData) {block = new}
  insertTx wr td
  return td
  where
    h = txHash t.tx
    move u = do
      let u' = (u :: Unspent) {block = new}
      insertUnspent wr u'
      forM_ u.address $ \a -> deleteAddrUnspent wr a u >> insertAddrUnspent wr a u'

-- | Revert the best block: make its parent the best block, mark it as not in
-- the main chain, take its transactions out of the chain and give them to
-- the mempool, and delete its coinbase with whatever spends it. Throws
-- 'BlockNotBest' if the block is not the best block.
revertBlock :: Writer -> BlockHash -> IO ()
revertBlock wr bh = do
  bd <-
    bestBlockData wr >>= \b ->
      if headerHash b.header == bh
        then return b
        else do
          logError wr $ "Cannot revert non-head block: " <> blockHashToHex bh
          throwIO BlockNotBest
  tds <- mapM inChain bd.txs
  setBest wr bd.header.prev
  insertBlock wr bd {main = False}
  case tds of
    [] -> return ()
    coinbase : rest -> do
      let returned = M.fromList [(txHash td.tx, td) | td <- rest]
          -- the transactions that go back to the mempool, each after the
          -- transactions that it spends
          ordered = filter ((`M.member` returned) . txHash) (map snd (sortTxs (map (.tx) tds)))
      -- out of the chain, each after the transactions that spend it
      forM_ (reverse ordered) (leave . txHash)
      -- into the mempool, at the time each was first seen. The coinbase is
      -- still in the chain: a transaction that spends it has all its
      -- parents while its flag is worked out, as in the layout of version 20
      forM_ (mapMaybe ((`M.lookup` returned) . txHash) ordered) $ \td -> do
        rbf <- isRBF wr td.tx
        addMempoolTx wr td.timestamp (td :: TxData) {rbf = rbf}
        updateSpendersRBF wr td.tx
      -- a coinbase cannot be in the mempool: it is deleted, and so is
      -- everything that spends it
      spenders <- getMempoolSpenders wr (txHash coinbase.tx) (length coinbase.tx.outputs)
      forM_ (nub' (map (.txid) (I.elems spenders))) $ \h ->
        getChain wr True False h >>= mapM_ (deleteSingleTx wr . txHash)
      leave (txHash coinbase.tx)
      insertDeletedTx wr (coinbase :: TxData) {deleted = True, spenders = I.empty}
  where
    inChain h =
      chainTx wr h >>= \case
        Just td -> return td
        Nothing -> do
          logDebug wr $ "Tx not found: " <> txHashToHex h
          throwIO TxNotFound
    -- take a transaction that nothing in the chain spends out of the chain
    leave h = do
      td <- inChain h
      unless (I.null td.spenders) $ do
        logError wr $ "Tried to revert spent tx: " <> txHashToHex h
        throwIO TxSpent
      removeChainTx wr td

-------------------------------------------------------------------------------
-- The chain's column families
-------------------------------------------------------------------------------

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

-- | Change the balance of an address in the chain. An address whose balance
-- goes back to nothing, as when the only block that paid it is reverted, has
-- none.
modifyBalance :: Writer -> Address -> (Balance -> Balance) -> IO ()
modifyBalance wr a f = do
  b <- f . fromMaybe (zeroBalance a) <$> getBalance wr a
  if nullBalance b then deleteBalance wr a else setBalance wr b

-- | Subtract, or stop: no count or amount of the chain goes below zero.
less :: String -> Word64 -> Word64 -> Word64
less what x y
  | y > x = error $ "The " <> what <> " of an address would go below zero"
  | otherwise = x - y

-- | Add a confirmed transaction to every index of the chain. Every output
-- that it spends must be unspent in the chain.
addChainTx :: Writer -> TxData -> IO ()
addChainTx wr td = do
  forM_ (txDataAddresses wr td) $ \a -> do
    insertAddrTx wr a TxRef {block = td.block, txid = h}
    modifyBalance wr a $ \b -> (b :: Balance) {txs = b.txs + 1}
  forM_ (zip [0 ..] td.tx.outputs) $ \(i, o) -> do
    let u = unspentOutput wr td.block (OutPoint h i) o
    insertUnspent wr u
    forM_ u.address $ \a -> do
      insertAddrUnspent wr a u
      modifyBalance wr a $ \b ->
        (b :: Balance)
          { confirmed = b.confirmed + o.value,
            utxo = b.utxo + 1,
            received = b.received + o.value
          }
  zipWithM_ spend [0 ..] (prevOuts td.tx)
  insertTx wr td
  where
    h = txHash td.tx
    spend ix p = do
      u <-
        getUnspent wr p >>= \case
          Just u -> return u
          Nothing -> error $ "Could not find UTXO to spend: " <> show p
      deleteUnspent wr p
      parent <- chainTx wr p.hash >>= maybe (throwIO TxNotFound) return
      insertTx wr (parent :: TxData) {spenders = I.insert (fromIntegral p.index) (Spender h ix) parent.spenders}
      forM_ u.address $ \a -> do
        deleteAddrUnspent wr a u
        modifyBalance wr a $ \b ->
          (b :: Balance)
            { confirmed = less "confirmed balance" b.confirmed u.value,
              utxo = less "count of unspent outputs" b.utxo 1
            }

-- | Take a confirmed transaction that nothing in the chain spends out of
-- every index of the chain, and make the outputs that it spent unspent
-- again.
removeChainTx :: Writer -> TxData -> IO ()
removeChainTx wr td = do
  forM_ (txDataAddresses wr td) $ \a -> do
    deleteAddrTx wr a TxRef {block = td.block, txid = h}
    modifyBalance wr a $ \b -> (b :: Balance) {txs = less "count of transactions" b.txs 1}
  forM_ (zip [0 ..] td.tx.outputs) $ \(i, o) -> do
    let p = OutPoint h i
        u = unspentOutput wr td.block p o
    deleteUnspent wr p
    forM_ u.address $ \a -> do
      deleteAddrUnspent wr a u
      modifyBalance wr a $ \b ->
        (b :: Balance)
          { confirmed = less "confirmed balance" b.confirmed o.value,
            utxo = less "count of unspent outputs" b.utxo 1,
            received = less "amount received" b.received o.value
          }
  forM_ (prevOuts td.tx) $ \p -> do
    parent <-
      chainTx wr p.hash >>= \case
        Just t -> return t
        Nothing -> error $ "Could not find tx data: " <> show p.hash
    let o = fromMaybe (error ("Could not find output: " <> show p)) (getTxOut p.index parent.tx)
        u = unspentOutput wr parent.block p o
    insertTx wr (parent :: TxData) {spenders = I.delete (fromIntegral p.index) parent.spenders}
    insertUnspent wr u
    forM_ u.address $ \a -> do
      insertAddrUnspent wr a u
      modifyBalance wr a $ \b -> (b :: Balance) {confirmed = b.confirmed + o.value, utxo = b.utxo + 1}
  deleteTxData wr h
  where
    h = txHash td.tx

-------------------------------------------------------------------------------
-- The mempool area
-------------------------------------------------------------------------------

-- | Import a transaction into the mempool with the time it arrived. Returns
-- 'False' if it is already known.
--
-- Conflicting unconfirmed transactions are deleted together with their
-- descendants if the network has replace-by-fee; otherwise the transaction is
-- rejected with 'DoubleSpend'. Throws an 'ImportException' if the transaction is
-- invalid, for example if it spends unknown outputs.
newMempoolTx :: Writer -> Tx -> UnixTime -> IO Bool
newMempoolTx wr tx w =
  activeBody wr (txHash tx) >>= \case
    Just _ -> return False
    Nothing -> do
      -- Check everything before writing, so that a rejected transaction
      -- leaves the mempool as it was.
      replaced <- getConflicts wr True True tx
      checkNewTx wr (map txHash replaced) tx
      mapM_ (deleteSingleTx wr . txHash) replaced
      rbf <- isRBF wr tx
      us <- forM (prevOuts tx) $ \p ->
        spendable wr p >>= \case
          Just u -> return u
          Nothing -> do
            logError wr $ "Attempted to import a tx missing UTXO: " <> txHashToHex (txHash tx)
            throwIO Orphan
      addMempoolTx wr w (prepareTxData rbf (MemRef w) w tx us)
      return True

-- | Delete an unconfirmed transaction and its descendants. If the flag is set and
-- the network has no replace-by-fee, throw 'DoubleSpend' instead. Does nothing if
-- the transaction is not found.
deleteUnconfirmedTx :: Writer -> Bool -> TxHash -> IO ()
deleteUnconfirmedTx wr rbfcheck th =
  activeBody wr th >>= \case
    Just _ -> getChain wr True rbfcheck th >>= mapM_ (deleteSingleTx wr . txHash)
    Nothing -> logDebug wr $ "Not found or already deleted: " <> txHashToHex th

-- | Write the keys of a transaction to the mempool area, with the time it
-- entered the mempool. The body goes in at that mempool position and without
-- spenders, whatever it had.
addMempoolTx :: Writer -> UnixTime -> TxData -> IO ()
addMempoolTx wr time td = do
  insertMempoolTx wr r.body
  forM_ r.spends (uncurry (insertMempoolSpender wr))
  forM_ r.addresses (uncurry (insertMempoolAddr wr))
  adjustSummaries wr (+) r
  addToMempool wr r.txid r.time
  -- it may have been deleted from the mempool before
  getDeletedTx wr r.txid >>= mapM_ (\_ -> deleteDeletedTx wr r.txid)
  where
    r = mempoolRecords (writerNet wr) (writerCtx wr) time td

-- | Delete the keys of a mempool transaction, given its body, from the
-- mempool area.
removeMempoolTx :: Writer -> TxData -> IO ()
removeMempoolTx wr td = do
  deleteMempoolTx wr r.txid
  forM_ r.spends (deleteMempoolSpender wr . fst)
  forM_ r.addresses (deleteMempoolAddr wr . fst)
  adjustSummaries wr (less "mempool summary") r
  deleteFromMempool wr r.txid
  where
    time = case td.block of
      MemRef t -> t
      BlockRef {} -> td.timestamp
    r = mempoolRecords (writerNet wr) (writerCtx wr) time td

-- | Add what a transaction does at each address it touches to the summary
-- of the address, or take it off again, given how to combine a number of
-- the summary with one of the transaction.
--
-- Everything is in the entries of the transaction but one thing: whether an
-- output that it spends was made by the mempool, and whether an output that
-- it makes is spent by the mempool, which is what 'both' counts. Each is a
-- read by key of what another transaction wrote. An output is counted in
-- 'both' while both of its transactions are in the mempool, so it is added
-- by the second of them to arrive and taken off by the first to leave,
-- whichever that is.
adjustSummaries :: Writer -> (Word64 -> Word64 -> Word64) -> MemRecords -> IO ()
adjustSummaries wr op r = do
  deltas <- forM r.addresses $ \case
    (MemAddrTxKey a _ _, _) -> return (a, MemSummary 1 0 0 0 0 0 0)
    (MemAddrSpentKey a p, MemAddrSpentVal v) -> do
      made <- isJust <$> getMempoolTx wr p.hash
      return (a, if made then MemSummary 0 0 0 v 1 v 1 else MemSummary 0 0 0 v 1 0 0)
    (MemAddrOutKey a _ p, MemAddrOutVal v _) -> do
      taken <- isJust <$> getMempoolSpender wr p
      return (a, if taken then MemSummary 0 v 1 0 0 v 1 else MemSummary 0 v 1 0 0 0 0)
    (k, _) -> error ("Not an entry of a transaction: " <> show k)
  forM_ (M.toList (M.fromListWith plus deltas)) $ \(a, d) -> do
    old <-
      getMempoolAddr wr (MemAddrSumKey a) <&> \case
        Just (MemAddrSumVal s) -> s
        _ -> zero
    let new = combine op old d
    if new == zero
      then deleteMempoolAddr wr (MemAddrSumKey a)
      else insertMempoolAddr wr (MemAddrSumKey a) (MemAddrSumVal new)
  where
    zero = MemSummary 0 0 0 0 0 0 0
    plus = combine (+)
    combine f x y =
      MemSummary
        { txs = f x.txs y.txs,
          paid = f x.paid y.paid,
          paidCount = f x.paidCount y.paidCount,
          spent = f x.spent y.spent,
          spentCount = f x.spentCount y.spentCount,
          both = f x.both y.both,
          bothCount = f x.bothCount y.bothCount
        }

-- | Delete a mempool transaction that nothing spends, and keep its body as
-- that of a deleted transaction. Throws 'TxSpent' if something spends it.
deleteSingleTx :: Writer -> TxHash -> IO ()
deleteSingleTx wr th =
  getMempoolTx wr th >>= \case
    Nothing -> do
      logError wr $ "Already deleted: " <> txHashToHex th
      throwIO TxNotFound
    Just td -> do
      spenders <- getMempoolSpenders wr th (length td.tx.outputs)
      if I.null spenders
        then do
          logDebug wr $ "Deleting tx: " <> txHashToHex th
          removeMempoolTx wr td
          insertDeletedTx wr (td :: TxData) {deleted = True}
        else do
          logError wr $ "Tried to delete spent tx: " <> txHashToHex th
          throwIO TxSpent

-- | Check a transaction that will replace these ones. Outputs that they spend
-- count as unspent, and outputs that they create as missing.
checkNewTx :: Writer -> [TxHash] -> Tx -> IO ()
checkNewTx wr replaced tx = do
  when (unique_inputs < length tx.inputs) $ do
    logError wr $ "Transaction spends same output twice: " <> txHashToHex (txHash tx)
    throwIO DuplicatePrevOutput
  prevs <- mapM prev_output (prevOuts tx)
  when (any isNothing prevs) $ do
    logError wr $ "Orphan: " <> txHashToHex (txHash tx)
    throwIO Orphan
  when (isCoinbaseTx tx) $ do
    logError wr $ "Coinbase cannot be imported into mempool: " <> txHashToHex (txHash tx)
    throwIO UnexpectedCoinbase
  when (outputs > sum (map (.value) (catMaybes prevs))) $ do
    logError wr $ "Insufficient funds for tx: " <> txHashToHex (txHash tx)
    throwIO InsufficientFunds
  where
    prev_output op
      | op.hash `elem` replaced = return Nothing
      | otherwise = (>>= getTxOut op.index . (.tx)) <$> activeBody wr op.hash
    outputs = sum (map (.value) tx.outputs)
    unique_inputs = length (nub' (map (.outpoint) tx.inputs))

-- | Transactions that spend the same outputs as this one, with their
-- descendants, in an order in which they can be deleted. The first flag
-- refuses confirmed transactions, and the second refuses any on networks
-- without replace-by-fee.
getConflicts :: Writer -> Bool -> Bool -> Tx -> IO [Tx]
getConflicts wr memonly rbfcheck tx = do
  let prevs = prevOuts tx
  unspents <- mapM (spendable wr) prevs
  let spents = [p | (p, Nothing) <- zip prevs unspents]
  spndrs <- catMaybes <$> mapM (spenderOf wr) spents
  let txids = H.fromList $ filter (/= txHash tx) $ map (.txid) spndrs
  chains <- mapM (getChain wr memonly rbfcheck) (H.toList txids)
  return . reverse . map snd . sortTxs . M.elems $
    M.fromList [(txHash t, t) | t <- concat chains]

-- | A transaction and all its descendants, in an order in which they can be
-- deleted: spenders first.
getChain :: Writer -> Bool -> Bool -> TxHash -> IO [Tx]
getChain wr memonly rbfcheck th' =
  sort_clean <$> go M.empty (H.singleton th')
  where
    sort_clean = reverse . map snd . sortTxs
    get_tx th =
      activeTx wr th >>= \case
        Nothing -> do
          logDebug wr $ "Transaction not found: " <> txHashToHex th
          return Nothing
        Just td
          | memonly && confirmed td.block -> do
              logError wr $ "Transaction already confirmed: " <> txHashToHex th
              throwIO TxConfirmed
          | rbfcheck ->
              if (writerNet wr).replaceByFee
                then return $ Just td
                else do
                  logError wr $ "Double-spending transaction: " <> txHashToHex th
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
        then return $ M.elems txm'
        else go txm' pdg'

-- | Whether a mempool transaction can be replaced by fee: it signals BIP125
-- in one of its inputs, or it spends an unconfirmed transaction that can be
-- replaced. Always 'False' on networks without replace-by-fee.
isRBF :: Writer -> Tx -> IO Bool
isRBF wr tx
  | not (writerNet wr).replaceByFee = return False
  | any ((< 0xffffffff - 1) . (.sequence)) tx.inputs = return True
  | otherwise = ck (nub' $ map (.outpoint.hash) tx.inputs)
  where
    ck [] = return False
    ck (h : hs') =
      activeBody wr h >>= \case
        Nothing -> return False
        Just t
          | confirmed t.block -> ck hs'
          | t.rbf -> return True
          | otherwise -> ck hs'

-- | Mempool transactions inherit BIP125 signalling from unconfirmed parents
-- only, so their flag can change when a parent is confirmed or goes back to
-- the mempool. Correct the flags of the mempool transactions that spend the
-- outputs of a transaction, and of theirs in turn.
updateSpendersRBF :: Writer -> Tx -> IO ()
updateSpendersRBF wr tx = do
  spenders <- getMempoolSpenders wr (txHash tx) (length tx.outputs)
  forM_ (nub' (map (.txid) (I.elems spenders))) $ \h ->
    getMempoolTx wr h >>= mapM_ update
  where
    update s = do
      rbf <- isRBF wr s.tx
      when (rbf /= s.rbf) $ do
        insertMempoolTx wr (s :: TxData) {rbf = rbf}
        updateSpendersRBF wr s.tx

-------------------------------------------------------------------------------
-- Transactions
-------------------------------------------------------------------------------

-- | Output of a transaction at an index, or 'Nothing' if out of range.
getTxOut :: Word32 -> Tx -> Maybe TxOut
getTxOut i tx = do
  guard (fromIntegral i < length tx.outputs)
  return $ tx.outputs !! fromIntegral i

-- | Addresses that a transaction is indexed under: those of its outputs and
-- of the outputs that it spends. An output script counts only when it has
-- an address on the network.
txDataAddresses :: Writer -> TxData -> [Address]
txDataAddresses wr t =
  nub' . rights $
    map (scriptToAddressBS net ctx . (.script)) (I.elems t.prevs)
      <> map (scriptToAddressBS net ctx . (.script)) t.tx.outputs
  where
    net = writerNet wr :: Network
    ctx = writerCtx wr :: Ctx

-- | Outputs spent by a transaction, without the null outpoint of a coinbase.
prevOuts :: Tx -> [OutPoint]
prevOuts tx = filter (/= nullOutPoint) (map (.outpoint) tx.inputs)
