{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE NoFieldSelectors #-}
{-# OPTIONS_GHC -Wno-ambiguous-fields #-}

module Haskoin.Store.Logic
  ( ImportException (..),
    MonadImport,
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
import Control.Monad.Except (MonadError, throwError)
import Control.Monad.Logger
  ( MonadLoggerIO (..),
    logDebugS,
    logErrorS,
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
import Haskoin.Store.Common
import Haskoin.Store.Data
import UnliftIO (Exception)

type MonadImport m =
  ( MonadError ImportException m,
    MonadLoggerIO m,
    StoreReadBase m,
    StoreWrite m
  )

data ImportException
  = PrevBlockNotBest
  | Orphan
  | UnexpectedCoinbase
  | BestBlockNotFound
  | BlockNotBest
  | TxNotFound
  | DoubleSpend
  | TxConfirmed
  | InsufficientFunds
  | DuplicatePrevOutput
  | TxSpent
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

initBest :: (MonadImport m) => m ()
initBest = do
  ctx <- getCtx
  $(logDebugS) "BlockStore" "Initializing best block"
  net <- getNetwork
  m <- getBestBlock
  when (isNothing m) . void $ do
    $(logDebugS) "BlockStore" "Importing Genesis block"
    importBlock (genesisBlock net ctx) (genesisNode net)

newMempoolTx :: (MonadImport m) => Tx -> UnixTime -> m Bool
newMempoolTx tx w =
  getActiveTxData (txHash tx) >>= \case
    Just _ ->
      return False
    Nothing -> do
      freeOutputs True True tx
      rbf <- isRBF (MemRef w) tx
      checkNewTx tx
      _ <- importTx (MemRef w) w rbf tx
      return True

bestBlockData :: (MonadImport m) => m BlockData
bestBlockData = do
  h <-
    getBestBlock >>= \case
      Nothing -> do
        $(logErrorS) "BlockStore" "Best block unknown"
        throwError BestBlockNotFound
      Just h -> return h
  getBlock h >>= \case
    Nothing -> do
      $(logErrorS) "BlockStore" "Best block not found"
      throwError BestBlockNotFound
    Just b -> return b

revertBlock :: (MonadImport m) => BlockHash -> m ()
revertBlock bh = do
  bd <-
    bestBlockData >>= \b ->
      if headerHash b.header == bh
        then return b
        else do
          $(logErrorS) "BlockStore" $
            "Cannot revert non-head block: " <> blockHashToHex bh
          throwError BlockNotBest
  $(logDebugS) "BlockStore" $
    "Obtained block data for " <> blockHashToHex bh
  tds <- mapM getImportTxData bd.txs
  $(logDebugS) "BlockStore" $
    "Obtained import tx data for block " <> blockHashToHex bh
  setBest bd.header.prev
  $(logDebugS) "BlockStore" $
    "Set parent as best block "
      <> blockHashToHex bd.header.prev
  insertBlock bd {main = False}
  $(logDebugS) "BlockStore" $
    "Updated as not in main chain: " <> blockHashToHex bh
  forM_ (drop 1 tds) unConfirmTx
  $(logDebugS) "BlockStore" $
    "Unconfirmed " <> cs (show (length tds)) <> " transactions"
  deleteConfirmedTx (txHash (tds !! 0).tx)
  $(logDebugS) "BlockStore" $
    "Deleted coinbase: " <> txHashToHex (txHash (tds !! 0).tx)

checkNewBlock :: (MonadImport m) => Block -> BlockNode -> m ()
checkNewBlock b n =
  getBestBlock >>= \case
    Nothing
      | isGenesis n -> return ()
      | otherwise -> do
          $(logErrorS) "BlockStore" $
            "Cannot import non-genesis block: "
              <> blockHashToHex (headerHash b.header)
          throwError BestBlockNotFound
    Just h
      | b.header.prev == h -> return ()
      | otherwise -> do
          $(logErrorS) "BlockStore" $
            "Block does not build on head: "
              <> blockHashToHex (headerHash b.header)
          throwError PrevBlockNotBest

importOrConfirm :: (MonadImport m) => BlockNode -> [Tx] -> m [TxData]
importOrConfirm bn txns = do
  $(logDebugS) "BlockStore" "Freeing outputs..."
  mapM_ (freeOutputs True False . snd) (reverse txs)
  $(logDebugS) "BlockStore" "Outputs freed"
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
          $(logDebugS) "BlockStore" $
            "Confirming tx: "
              <> txHashToHex (txHash tx)
          confirmTx t (br i)
        Nothing -> do
          $(logErrorS) "BlockStore" $
            "Cannot find tx to confirm: "
              <> txHashToHex (txHash tx)
          throwError TxNotFound
    import_it i tx = do
      $(logDebugS) "BlockStore" $
        "Importing tx: " <> txHashToHex (txHash tx)
      importTx (br i) bn_time False tx

importBlock :: (MonadImport m) => Block -> BlockNode -> m (BlockData, [TxData])
importBlock b n = do
  $(logDebugS) "BlockStore" $
    "Checking new block: "
      <> blockHashToHex (headerHash n.header)
  checkNewBlock b n
  $(logDebugS) "BlockStore" "Passed check"
  net <- getNetwork
  let subsidy = computeSubsidy net n.height
  bs <- getBlocksAtHeight n.height
  $(logDebugS) "BlockStore" $
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
  $(logDebugS) "BlockStore" $
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

checkNewTx :: (MonadImport m) => Tx -> m ()
checkNewTx tx = do
  when (unique_inputs < length tx.inputs) $ do
    $(logErrorS) "BlockStore" $
      "Transaction spends same output twice: "
        <> txHashToHex (txHash tx)
    throwError DuplicatePrevOutput
  us <- getUnspentOutputs tx
  when (any isNothing us) $ do
    $(logErrorS) "BlockStore" $
      "Orphan: " <> txHashToHex (txHash tx)
    throwError Orphan
  when (isCoinbaseTx tx) $ do
    $(logErrorS) "BlockStore" $
      "Coinbase cannot be imported into mempool: "
        <> txHashToHex (txHash tx)
    throwError UnexpectedCoinbase
  when (length (prevOuts tx) > length us) $ do
    $(logErrorS) "BlockStore" $
      "Orphan: " <> txHashToHex (txHash tx)
    throwError Orphan
  when (outputs > unspents us) $ do
    $(logErrorS) "BlockStore" $
      "Insufficient funds for tx: " <> txHashToHex (txHash tx)
    throwError InsufficientFunds
  where
    unspents = sum . map (.value) . catMaybes
    outputs = sum (map (.value) tx.outputs)
    unique_inputs = length (nub' (map (.outpoint) tx.inputs))

getUnspentOutputs :: (StoreReadBase m) => Tx -> m [Maybe Unspent]
getUnspentOutputs tx = mapM getUnspent (prevOuts tx)

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

importTx ::
  (MonadImport m) =>
  BlockRef ->
  -- | unix time
  Word64 ->
  -- | RBF
  Bool ->
  Tx ->
  m TxData
importTx br tt rbf tx = do
  mus <- getUnspentOutputs tx
  us <- forM mus $ \case
    Nothing -> do
      $(logErrorS) "BlockStore" $
        "Attempted to import a tx missing UTXO: "
          <> txHashToHex (txHash tx)
      throwError Orphan
    Just u -> return u
  let td = prepareTxData rbf br tt tx us
  commitAddTx td
  return td

unConfirmTx :: (MonadImport m) => TxData -> m TxData
unConfirmTx t = confTx t Nothing

confirmTx :: (MonadImport m) => TxData -> BlockRef -> m TxData
confirmTx t br = confTx t (Just br)

replaceAddressTx :: (MonadImport m) => TxData -> BlockRef -> m ()
replaceAddressTx t new = do
  ctx <- getCtx
  forM_ (txDataAddresses ctx t) $ \a -> do
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

adjustAddressOutput ::
  (MonadImport m) =>
  OutPoint ->
  TxOut ->
  BlockRef ->
  BlockRef ->
  m ()
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
      ctx <- getCtx
      let ma = eitherToMaybe (scriptToAddressBS ctx pk)
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

confTx :: (MonadImport m) => TxData -> Maybe BlockRef -> m TxData
confTx t mbr = do
  replaceAddressTx t new
  forM_ (zip [0 ..] t.tx.outputs) $ \(n, o) -> do
    let op = OutPoint (txHash t.tx) n
    adjustAddressOutput op o old new
  rbf <- isRBF new t.tx
  let td = (t :: TxData) {block = new, rbf = rbf}
  insertTx td
  updateMempool td
  return td
  where
    new = fromMaybe (MemRef t.timestamp) mbr
    old = t.block

freeOutputs ::
  (MonadImport m) =>
  -- | only delete transaction if unconfirmed
  Bool ->
  -- | only delete RBF
  Bool ->
  Tx ->
  m ()
freeOutputs memonly rbfcheck tx = do
  $(logDebugS) "BlockStore" $
    "Freeing outputs for tx " <> txHashToHex (txHash tx) <> "..."
  let prevs = prevOuts tx
  unspents <- mapM getUnspent prevs
  let spents = [p | (p, Nothing) <- zip prevs unspents]
  spndrs <- catMaybes <$> mapM getSpender spents
  let txids = H.fromList $ filter (/= txHash tx) $ map (.txid) spndrs
  mapM_ (deleteTx memonly rbfcheck) $ H.toList txids

deleteConfirmedTx :: (MonadImport m) => TxHash -> m ()
deleteConfirmedTx = deleteTx False False

deleteUnconfirmedTx :: (MonadImport m) => Bool -> TxHash -> m ()
deleteUnconfirmedTx rbfcheck th =
  getActiveTxData th >>= \case
    Just _ -> deleteTx True rbfcheck th
    Nothing ->
      $(logDebugS) "BlockStore" $
        "Not found or already deleted: " <> txHashToHex th

deleteTx ::
  (MonadImport m) =>
  -- | only delete transaction if unconfirmed
  Bool ->
  -- | only delete RBF
  Bool ->
  TxHash ->
  m ()
deleteTx memonly rbfcheck th = do
  chain <- getChain memonly rbfcheck th
  $(logDebugS) "BlockStore" $
    "Deleting "
      <> cs (show (length chain))
      <> " txs from chain leading to "
      <> txHashToHex th
  mapM_ (\t -> let h = txHash t in deleteSingleTx h >> return h) chain

getChain ::
  (MonadImport m) =>
  -- | only delete transaction if unconfirmed
  Bool ->
  -- | only delete RBF
  Bool ->
  TxHash ->
  m [Tx]
getChain memonly rbfcheck th' = do
  $(logDebugS) "BlockStore" $
    "Getting chain for tx " <> txHashToHex th'
  sort_clean <$> go M.empty (H.singleton th')
  where
    sort_clean = reverse . map snd . sortTxs
    get_tx th =
      getActiveTxData th >>= \case
        Nothing -> do
          $(logDebugS) "BlockStore" $
            "Transaction not found: " <> txHashToHex th
          return Nothing
        Just td
          | memonly && confirmed td.block -> do
              $(logErrorS) "BlockStore" $
                "Transaction already confirmed: "
                  <> txHashToHex th
              throwError TxConfirmed
          | rbfcheck ->
              isRBF td.block td.tx >>= \case
                True -> return $ Just td
                False -> do
                  $(logErrorS) "BlockStore" $
                    "Double-spending transaction: "
                      <> txHashToHex th
                  throwError DoubleSpend
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
          $(logDebugS) "BlockStore" $
            "Chain for tx "
              <> txHashToHex th'
              <> " contains "
              <> cs (show (M.size txm'))
              <> " txs"
          return $ M.elems txm'
        else go txm' pdg'

deleteSingleTx :: (MonadImport m) => TxHash -> m ()
deleteSingleTx th =
  getActiveTxData th >>= \case
    Nothing -> do
      $(logErrorS) "BlockStore" $
        "Already deleted: " <> txHashToHex th
      throwError TxNotFound
    Just td ->
      if I.null td.spenders
        then do
          $(logDebugS) "BlockStore" $
            "Deleting tx: " <> txHashToHex th
          commitDelTx td
        else do
          $(logErrorS) "BlockStore" $
            "Tried to delete spent tx: "
              <> txHashToHex th
          throwError TxSpent

commitDelTx :: (MonadImport m) => TxData -> m ()
commitDelTx = commitModTx False

commitAddTx :: (MonadImport m) => TxData -> m ()
commitAddTx = commitModTx True

commitModTx :: (MonadImport m) => Bool -> TxData -> m ()
commitModTx add tx_data = do
  ctx <- getCtx
  mapM_ mod_addr_tx (txDataAddresses ctx td)
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

updateMempool :: (MonadImport m) => TxData -> m ()
updateMempool td@TxData {deleted = True} =
  deleteFromMempool (txHash td.tx)
updateMempool td@TxData {block = MemRef t} =
  addToMempool (txHash td.tx) t
updateMempool td@TxData {block = BlockRef {}} =
  deleteFromMempool (txHash td.tx)

spendOutputs :: (MonadImport m) => Tx -> m ()
spendOutputs tx =
  zipWithM_ (spendOutput (txHash tx)) [0 ..] (prevOuts tx)

addOutputs :: (MonadImport m) => BlockRef -> Tx -> m ()
addOutputs br tx =
  zipWithM_ (addOutput br . OutPoint (txHash tx)) [0 ..] tx.outputs

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

addOutput :: (MonadImport m) => BlockRef -> OutPoint -> TxOut -> m ()
addOutput = modOutput True

delOutput :: (MonadImport m) => BlockRef -> OutPoint -> TxOut -> m ()
delOutput = modOutput False

modOutput ::
  (MonadImport m) =>
  Bool ->
  BlockRef ->
  OutPoint ->
  TxOut ->
  m ()
modOutput add br op o = do
  ctx <- getCtx
  mod_unspent ctx
  forM_ (ma ctx) $ \a -> do
    mod_addr_unspent a (u ctx)
    modBalance (confirmed br) add a o.value
    modifyReceived a v
  where
    v
      | add = (+ o.value)
      | otherwise = subtract o.value
    ma ctx = eitherToMaybe (scriptToAddressBS ctx o.script)
    u ctx =
      Unspent
        { script = o.script,
          block = br,
          outpoint = op,
          value = o.value,
          address = ma ctx
        }
    mod_unspent ctx
      | add = insertUnspent (u ctx)
      | otherwise = deleteUnspent op
    mod_addr_unspent
      | add = insertAddrUnspent
      | otherwise = deleteAddrUnspent

delOutputs :: (MonadImport m) => BlockRef -> Tx -> m ()
delOutputs br tx =
  forM_ (zip [0 ..] tx.outputs) $ \(i, o) -> do
    let op = OutPoint (txHash tx) i
    delOutput br op o

getImportTxData :: (MonadImport m) => TxHash -> m TxData
getImportTxData th =
  getActiveTxData th >>= \case
    Nothing -> do
      $(logDebugS) "BlockStore" $ "Tx not found: " <> txHashToHex th
      throwError TxNotFound
    Just d -> return d

getTxOut :: Word32 -> Tx -> Maybe TxOut
getTxOut i tx = do
  guard (fromIntegral i < length tx.outputs)
  return $ tx.outputs !! fromIntegral i

insertSpender :: (MonadImport m) => OutPoint -> Spender -> m ()
insertSpender op s = do
  td <- getImportTxData op.hash
  let p = td.spenders
      p' = I.insert (fromIntegral op.index) s p
      td' = (td :: TxData) {spenders = p'}
  insertTx td'

deleteSpender :: (MonadImport m) => OutPoint -> m ()
deleteSpender op = do
  td <- getImportTxData op.hash
  let p = td.spenders
      p' = I.delete (fromIntegral op.index) p
      td' = (td :: TxData) {spenders = p'}
  insertTx td'

spendOutput :: (MonadImport m) => TxHash -> Word32 -> OutPoint -> m ()
spendOutput th ix op = do
  u <-
    getUnspent op >>= \case
      Just u -> return u
      Nothing -> error $ "Could not find UTXO to spend: " <> show op
  deleteUnspent op
  insertSpender op (Spender th ix)
  let pk = u.script
  ctx <- getCtx
  forM_ (scriptToAddressBS ctx pk) $ \a -> do
    decreaseBalance (confirmed u.block) a u.value
    deleteAddrUnspent a u

unspendOutputs :: (MonadImport m) => Tx -> m ()
unspendOutputs = mapM_ unspendOutput . prevOuts

unspendOutput :: (MonadImport m) => OutPoint -> m ()
unspendOutput op = do
  ctx <- getCtx
  t <-
    getActiveTxData op.hash >>= \case
      Nothing -> error $ "Could not find tx data: " <> show op.hash
      Just t -> return t
  let o =
        fromMaybe
          (error ("Could not find output: " <> show op))
          (getTxOut op.index t.tx)
      m = eitherToMaybe (scriptToAddressBS ctx o.script)
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

modifyReceived :: (MonadImport m) => Address -> (Word64 -> Word64) -> m ()
modifyReceived a f = do
  b <- getDefaultBalance a
  setBalance b {received = f b.received}

decreaseBalance :: (MonadImport m) => Bool -> Address -> Word64 -> m ()
decreaseBalance conf = modBalance conf False

increaseBalance :: (MonadImport m) => Bool -> Address -> Word64 -> m ()
increaseBalance conf = modBalance conf True

modBalance ::
  (MonadImport m) =>
  -- | confirmed
  Bool ->
  -- | add
  Bool ->
  Address ->
  Word64 ->
  m ()
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

modAddressCount :: (MonadImport m) => Bool -> Address -> m ()
modAddressCount add a = do
  b <- getDefaultBalance a
  setBalance b {txs = f b.txs}
  where
    f
      | add = (+ 1)
      | otherwise = subtract 1

txOutAddrs :: Ctx -> [TxOut] -> [Address]
txOutAddrs ctx = nub' . rights . map (scriptToAddressBS ctx . (.script))

txInAddrs :: Ctx -> [Prev] -> [Address]
txInAddrs ctx = nub' . rights . map (scriptToAddressBS ctx . (.script))

txDataAddresses :: Ctx -> TxData -> [Address]
txDataAddresses ctx t =
  nub' $ txInAddrs ctx prevs <> txOutAddrs ctx outs
  where
    prevs = I.elems t.prevs
    outs = t.tx.outputs

prevOuts :: Tx -> [OutPoint]
prevOuts tx = filter (/= nullOutPoint) (map (.outpoint) tx.inputs)

testPresent :: (StoreReadBase m) => Tx -> m Bool
testPresent tx = isJust <$> getActiveTxData (txHash tx)
