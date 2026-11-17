{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -Wno-ambiguous-fields #-}

-- |
-- Module      : Haskoin.Store.Logic
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Rules to import and revert blocks. The database holds the chain and
-- nothing else: the mempool is the pool that the fleet shares in Redis
-- ("Haskoin.Store.Fleet.Pool"), so these rules never meet an unconfirmed
-- transaction. 'importBlock' and 'revertBlock' are the only functions that
-- write to the column families of the chain, and what they write depends
-- on the block and on those column families alone: the same bytes on every
-- store that has the same blocks.
--
-- A reverted block takes its transactions out of the chain. They are not
-- kept anywhere in the database: the member that moves the fleet tip reads
-- them from its view of the database at the old tip, and gives them back to
-- the pool ("Haskoin.Store.Fleet.Member").
--
-- = Importing a block in stages
--
-- 'importBlock' works in four stages, so that a full block takes a second
-- or two and not eighteen, as it did on a store of mainnet whose disk and
-- cores were mostly idle. What cost the time was thirty thousand reads of
-- the database, one after another from one thread, many of them of a key
-- that had been read already.
--
-- 1. __Prepare__ ('prepareBlock'), on all cores, from the block alone: the
--    hash of each transaction, the address of each output, the sizes. They
--    are carried along from there.
--
-- 2. __Read ahead__: the outputs that the block spends are read with
--    multi-gets on all cores into the read cache of the batch
--    ("Haskoin.Store.Database.Writer").
--
-- 3. __Apply__: the rules, one transaction after another, without a read of
--    the database between them.
--
-- 4. __Write__: the batch.
--
-- Since database version 23 the balances of the addresses of a block and
-- the spenders of the outputs that it spends are not read at all: the
-- block writes how they change, as merge records, and RocksDB works out
-- the values when they are read ('addChainTx',
-- "Haskoin.Store.Database.Merge").
--
-- = A mined block is not validated
--
-- A block that gets here was mined: it passed proof of work, and the node
-- that the store follows took it. The store takes it that its
-- transactions are valid: every output that the block spends is unspent
-- in the chain or made by a transaction of the block, and none is spent
-- twice. Checking that is the work of the node, and each check here was a
-- read of the database for every input of every block. So the import
-- reads what it needs to write the block, and nothing to check it:
--
-- * __No spender of the chain is read.__ A block writes its spenders as
--   merge records and never reads a group of them.
--
-- * __An output that the block makes is not read from the database.__ The
--   rules find it in the changes of the batch, as each transaction is
--   applied after those that it spends ('readAheadBlock').
--
-- * __Only a coinbase is looked up in the chain__, in case it is confirmed
--   already. That happened to two coinbases of 2010, and it is data, not
--   a check: the later one takes the place of the earlier. A transaction
--   with an input cannot be confirmed twice, since its input is spent.
--
-- What stays is what the store's own data needs: the unspent outputs that
-- the block spends, whose values, scripts and positions the balances and
-- the indexes are made from, and the order in which transactions are taken
-- ('sortTxs', needed on Bitcoin Cash, whose blocks are in the order of the
-- hashes).
--
-- A block that breaks the assumption is promised one thing: it is written
-- whole or not at all, since a batch that throws writes nothing
-- ("Haskoin.Store.Database.Writer"). It is still refused when the one
-- read that stays finds nothing: an output that does not exist, that the
-- chain has as spent, or that an earlier transaction of the block spent,
-- is not there to be read, and the block is an 'Orphan'. A transaction
-- that spends one output twice stops the batch too, when it comes to
-- spend what it just spent ('addChainTx'): a look in the changes of the
-- batch, in memory.
module Haskoin.Store.Logic
  ( ImportException (..),
    initBest,
    revertBlock,
    importBlock,

    -- * What a block needs worked out once
    PreparedBlock (..),
    PreparedTx (..),
    prepareBlock,
  )
where

import Control.DeepSeq (NFData, force)
import Control.Exception (Exception, evaluate, throwIO)
import Control.Monad (forM, forM_, guard, unless, void, when, zipWithM_)
import Data.ByteString qualified as B
import Data.Default (def)
import Data.Either (rights)
import Data.Functor ((<&>))
import Data.HashSet qualified as H
import Data.IntMap.Strict qualified as I
import Data.List (nub)
import Data.Maybe (catMaybes, fromMaybe, isNothing, mapMaybe)
import Data.Serialize (encode)
import Data.Set qualified as S
import Data.Text (Text)
import Data.Word (Word32, Word64)
import GHC.Generics (Generic)
import Haskoin
  ( Address,
    Block (..),
    BlockHash,
    BlockHeader (..),
    BlockNode (..),
    Network (..),
    OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    VarInt (..),
    blockHashToHex,
    computeSubsidy,
    doubleSHA256,
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
import Haskoin.Store.Common (nub', replacedKept, sortTxs, sortTxsWith)
import Haskoin.Store.Data
import Haskoin.Store.Database.Types (BalVal (..))
import Haskoin.Store.Database.Writer

-- | Reasons why an import can fail.
data ImportException
  = -- | the block does not build on the best block
    PrevBlockNotBest
  | -- | the transaction spends an output that is unknown or already spent
    Orphan
  | -- | the database has no best block, or it cannot be read
    BestBlockNotFound
  | -- | only the best block can be reverted
    BlockNotBest
  | -- | a transaction that should be in the database is not
    TxNotFound
  | -- | a transaction cannot be taken out while another one spends it
    TxSpent
  deriving (Eq, Ord, Exception)

instance Show ImportException where
  show PrevBlockNotBest = "Previous block not best"
  show Orphan = "Orphan"
  show BestBlockNotFound = "Best block not found"
  show BlockNotBest = "Block not best"
  show TxNotFound = "Transaction not found"
  show TxSpent = "Transaction is spent"

-------------------------------------------------------------------------------
-- Reading
-------------------------------------------------------------------------------

-- | Log a message with the logger of the writer.
logDebug, logError :: Writer -> Text -> IO ()
logDebug wr = logAt LevelDebug (writerLogger wr)
logError wr = logAt LevelError (writerLogger wr)

-- | Address of an output script, if it has one on the network.
scriptAddress :: Writer -> B.ByteString -> Maybe Address
scriptAddress wr = eitherToMaybe . scriptToAddressBS (writerNet wr)

-- | Data of a confirmed transaction, with the spenders of its outputs that
-- are in blocks. Version 20 kept the bodies of deleted transactions in the
-- same place; they do not count.
chainTx :: Writer -> TxHash -> IO (Maybe TxData)
chainTx wr h =
  getTxData wr h <&> \case
    Just td | not td.deleted -> Just td
    _ -> Nothing

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
-- It works in the stages that the head of this module describes, and notes
-- how long each took ('noteBlockTimes').
importBlock :: Writer -> Block -> BlockNode -> IO (BlockData, [TxData])
importBlock wr b n = do
  checkNewBlock wr b n
  (pb, preparing) <- stopwatch (prepareBlock (writerNet wr) b)
  (kinds, reading) <- stopwatch (readAheadBlock wr pb)
  (done, applying) <- stopwatch (applyBlock wr b n pb)
  noteBlockTimes wr preparing reading kinds applying
  return done

-- | A transaction of a block with what its import needs of it, worked out
-- once, from the transaction alone.
data PreparedTx = PreparedTx
  { tx :: !Tx,
    -- | its hash
    txid :: !TxHash,
    -- | the outputs that it spends, in the order of its inputs: none for a
    -- coinbase
    spends :: ![OutPoint],
    -- | the address of each of its outputs, for those that have one
    paid :: ![Maybe Address],
    -- | bytes that it takes in a block, with its witness
    size :: !Int,
    -- | the same without its witness
    base :: !Int
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (NFData)

-- | A block with what its import needs of it, worked out once.
data PreparedBlock = PreparedBlock
  { -- | its transactions, in its order
    txs :: ![PreparedTx],
    -- | the same, each after those that it spends, with its position in
    -- the block ('sortTxs')
    ordered :: ![(Word32, PreparedTx)],
    -- | bytes of the block
    size :: !Int,
    -- | weight of the block, as BIP 141 defines it
    weight :: !Int
  }
  deriving (Eq, Show)

-- | Work out what the import of a block needs of it, on all cores.
--
-- Each transaction is serialised once without its witness: the hash is
-- taken of those bytes, and their length is its part of the size of the
-- block. One with a witness is serialised once more for its length. The
-- size and the weight of the block are sums of those, with the header and
-- the count of transactions, instead of two more serialisations of the
-- whole block.
prepareBlock :: Network -> Block -> IO PreparedBlock
prepareBlock net b = do
  txs <- concat <$> onAllCores 2 16 (evaluate . force . map (prepareTx net)) b.txs
  let count = length txs
      overhead = B.length (encode b.header) + B.length (encode (VarInt (fromIntegral count)))
      base = overhead + sum (map (.base) txs)
      full = overhead + sum (map (.size) txs)
      ordered = sortTxsWith (.txid) (.tx) txs
  -- the sort is part of this stage
  _ <- evaluate (length ordered)
  return PreparedBlock {txs, ordered, size = full, weight = 3 * base + full}

-- | Work out what the import of a block needs of one of its transactions.
prepareTx :: Network -> Tx -> PreparedTx
prepareTx net tx =
  PreparedTx
    { tx,
      txid = TxHash (doubleSHA256 stripped),
      spends = prevOuts tx,
      paid = map (eitherToMaybe . scriptToAddressBS net . (.script)) tx.outputs,
      size = if null tx.witness then B.length stripped else B.length (encode tx),
      base = B.length stripped
    }
  where
    -- what 'txHash' hashes
    stripped = encode (tx :: Tx) {witness = []}

-- | Read ahead what the rules will read from the database for a block,
-- into the read cache of the batch: the outputs that the block spends and
-- does not make, as unspent outputs of the chain, with multi-gets on all
-- cores. Their values, scripts and positions are what the balances and the
-- indexes of the block are made from.
--
-- An output that a transaction of the block makes is not asked for. The
-- database does not have it, and the rules find it in the changes of the
-- batch, since each transaction is applied after those that it spends. On
-- Bitcoin mainnet in November 2026 that was 3,000 to 6,500 of the inputs
-- of a block.
--
-- A key that is missing here costs a read when the rules ask for it, and
-- nothing else.
--
-- The balances of the addresses of the block, and the groups of spenders
-- of the outputs that it spends, are not read: they are changed without
-- being read ('addChainTx'). It says how long the read took, as
-- t'ReadTimes'.
readAheadBlock :: Writer -> PreparedBlock -> IO ReadTimes
readAheadBlock wr pb
  | not (readsAhead wr) = return noReads
  | otherwise = do
      (_, unspentReads) <- stopwatch (readAheadUnspents wr outside)
      return ReadTimes {unspentReads, spenderReads = 0, balanceReads = 0}
  where
    made = H.fromList (map (.txid) pb.txs)
    -- the outputs that the block spends and that no transaction of the
    -- block makes
    outside = filter (not . (`H.member` made) . (.hash)) (concatMap (.spends) pb.txs)

-- | The rules of the import of a block, given what was worked out of it.
applyBlock :: Writer -> Block -> BlockNode -> PreparedBlock -> IO (BlockData, [TxData])
applyBlock wr b n pb = do
  let net = writerNet wr
      subsidy = computeSubsidy net n.height
  bs <- getBlocksAtHeight wr n.height
  setBlocksAtHeight wr (nub (headerHash n.header : bs)) n.height
  setBest wr (headerHash n.header)
  tds <- importTxs wr n pb.ordered
  when (n.height >= replacedKept) $
    getReplacedAt wr (n.height - replacedKept) >>= mapM_ (deleteReplaced wr)
  let bd =
        BlockData
          { height = n.height,
            main = True,
            work = n.work,
            header = n.header,
            size = fromIntegral pb.size,
            txs = map (.txid) pb.txs,
            weight = if net.segWit then fromIntegral pb.weight else 0,
            subsidy = subsidy,
            fee = sum $ map txDataFee tds,
            outputs = sum $ map (sum . map (.value) . (.outputs)) (drop 1 b.txs)
          }
  insertBlock wr bd
  logDebug wr $ "Finished importing block: " <> blockHashToHex (headerHash n.header)
  return (bd, tds)

-- | Import the transactions of a block, in dependency order.
importTxs :: Writer -> BlockNode -> [(Word32, PreparedTx)] -> IO [TxData]
importTxs wr bn = mapM (uncurry action)
  where
    br i = BlockRef {height = bn.height, position = i}
    blockTime = fromIntegral bn.header.timestamp
    action i p
      -- only a transaction without inputs can be in the chain already:
      -- the inputs of any other would be spent
      | null p.spends =
          chainTx wr p.txid >>= \case
            Just td -> moveChainTx wr td (br i) blockTime
            Nothing -> importChainTx wr (br i) blockTime p
      | otherwise = importChainTx wr (br i) blockTime p

-- | Import a transaction of a block into the chain, with the time of the
-- block. Throws 'Orphan' if an output that it spends is not unspent in the
-- chain. That is not a check of the block: the output is read for what the
-- transaction needs of it, and there is nothing to go on with when it is
-- not there.
--
-- = Why the time of the block
--
-- What the chain's column families hold is to follow from the chain and
-- from nothing else: the same bytes on every store that has the same
-- blocks, whatever each of them saw before. The time of a transaction was
-- the one thing in them that did not. A store kept the time at which it
-- first saw a transaction in its mempool when a block confirmed it, so two
-- stores had two times for it, a store that synced later had the time of
-- the block, and the stores of a fleet gave different answers about the
-- same confirmed transaction. The time of first sight is what a
-- transaction has while it is in the pool.
importChainTx :: Writer -> BlockRef -> Word64 -> PreparedTx -> IO TxData
importChainTx wr br time p = do
  us <- forM p.spends $ \o ->
    getUnspent wr o >>= \case
      Just u -> return u
      Nothing -> do
        logError wr $ "Attempted to import a tx missing UTXO: " <> txHashToHex p.txid
        throwIO Orphan
  let td = prepareTxData False br time p.tx us
  addChainTx wr p td us
  return td

-- | Give a confirmed transaction a new position in the chain: a block has a
-- transaction that is already confirmed. Only two coinbases of 2010 are, on
-- Bitcoin; the later one takes the place of the earlier, with the outputs
-- that are still unspent, as in the layout of version 20, and the time of
-- the later block.
moveChainTx :: Writer -> TxData -> BlockRef -> Word64 -> IO TxData
moveChainTx wr t new time = do
  forM_ (txDataAddresses wr t) $ \a -> do
    deleteAddrTx wr a TxRef {block = t.block, txid = h}
    insertAddrTx wr a TxRef {block = new, txid = h}
  forM_ (zip [0 ..] t.tx.outputs) $ \(i, _) ->
    getUnspent wr (OutPoint h i) >>= mapM_ move
  let td = (t :: TxData) {block = new, timestamp = time}
  insertTx wr td
  return td
  where
    h = txHash t.tx
    move u = do
      let u' = (u :: Unspent) {block = new}
      insertUnspent wr u'
      forM_ u.address $ \a -> deleteAddrUnspent wr a u >> insertAddrUnspent wr a u'

-- | Revert the best block: make its parent the best block, mark it as not in
-- the main chain, and take its transactions out of the chain, each after
-- the transactions of the block that spend it. Throws 'BlockNotBest' if the
-- block is not the best block.
--
-- Nothing is kept of the transactions: the pool that the fleet shares gets
-- them back from the member that moves the tip, which reads them from its
-- view of the database at the old tip ("Haskoin.Store.Fleet.Member").
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
  -- what the block replaced is not replaced any more
  getReplacedAt wr bd.height >>= mapM_ (deleteReplaced wr)
  -- out of the chain, each after the transactions that spend it
  forM_ (reverse (sortTxs (map (.tx) tds))) (leave . txHash . snd)
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
      spent <- getChainSpenders wr h (length td.tx.outputs)
      unless (I.null spent) $ do
        logError wr $ "Tried to revert spent tx: " <> txHashToHex h
        throwIO TxSpent
      removeChainTx wr td

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

-- | Change the balance of an address in the chain, by reading it and
-- writing it whole. An address whose balance goes back to nothing, as when
-- the only block that paid it is reverted, has none.
--
-- The import of a block does not go through here: it changes a balance
-- without reading it ('addChainTx'). This is for taking a block out.
modifyBalance :: Writer -> Address -> (Balance -> Balance) -> IO ()
modifyBalance wr a f = do
  b <- f . fromMaybe (zeroBalance a) <$> getBalance wr a
  if nullBalance b then deleteBalance wr a else setBalance wr b

-- | Subtract, or stop: no count or amount of the chain goes below zero.
less :: String -> Word64 -> Word64 -> Word64
less what x y
  | y > x = error $ "The " <> what <> " of an address would go below zero"
  | otherwise = x - y

-- | Add a confirmed transaction to every index of the chain, given what
-- was worked out of it and the outputs that it spends, which must be
-- unspent in the chain.
--
-- Its addresses are those worked out before: of its outputs, and of the
-- outputs that it spends, which came with them. Each is taken once, by
-- comparing them: hashing an address, as 'nub'' does, costs more than the
-- few comparisons that a transaction needs. Each output is asked for
-- again as it is spent, which the batch answers from memory: it is how a
-- transaction that spends an output twice is stopped. A mined block is
-- not validated, and this look is the one thing here that does nothing
-- else. It stays because it costs nothing that could be measured: the
-- benchmark took as long without it (@docs\/block-import.md@).
--
-- = Nothing is read to be changed
--
-- A balance is changed by its differences ('mergeBalance') and a spender
-- is added to its group ('mergeSpender') without reading either: the batch
-- writes them as merge records, and RocksDB works out the values when they
-- are read ("Haskoin.Store.Database.Merge"). Reading the balances and the
-- groups of spenders was most of the read ahead of a block on a store of
-- mainnet, and the import only read them to write them back.
--
-- Two things follow. A count or an amount of a balance is not checked
-- against going below zero here, as it is in 'removeChainTx', since that
-- takes the value: an output that is spent was unspent in the chain, so
-- its amount is in the balance. And a balance is never deleted here: the
-- count of transactions of every address that the transaction touches
-- goes up by one, so none of them comes to nothing. Taking a block out
-- reads each value and writes it whole ('removeChainTx').
addChainTx :: Writer -> PreparedTx -> TxData -> [Unspent] -> IO ()
addChainTx wr p td us = do
  forM_ (S.toList (S.fromList (mapMaybe (.address) us <> catMaybes p.paid))) $ \a -> do
    insertAddrTx wr a TxRef {block = td.block, txid = h}
    mergeBalance wr a (def :: BalVal) {txs = 1}
  forM_ (zip3 [0 ..] td.tx.outputs p.paid) $ \(i, o, address) -> do
    let u = Unspent {block = td.block, outpoint = OutPoint h i, value = o.value, script = o.script, address}
    insertUnspent wr u
    forM_ address $ \a -> do
      insertAddrUnspent wr a u
      mergeBalance wr a (def :: BalVal) {confirmed = o.value, utxo = 1, received = o.value}
  zipWithM_ spend [0 ..] p.spends
  insertTxOf wr h td
  where
    h = p.txid
    spend ix o = do
      u <-
        getUnspent wr o >>= \case
          Just u -> return u
          Nothing -> error $ "Could not find UTXO to spend: " <> show o
      deleteUnspent wr o
      -- under the key of the output: the record of the transaction that
      -- made it is written once, when it is confirmed, and is not read
      -- here either
      mergeSpender wr o (Spender h ix)
      forM_ u.address $ \a -> do
        deleteAddrUnspent wr a u
        -- taken off by adding the two's complement
        mergeBalance wr a (def :: BalVal) {confirmed = negate u.value, utxo = negate 1}

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
    deleteSpender wr p
    insertUnspent wr u
    forM_ u.address $ \a -> do
      insertAddrUnspent wr a u
      modifyBalance wr a $ \b -> (b :: Balance) {confirmed = b.confirmed + o.value, utxo = b.utxo + 1}
  deleteTxData wr h
  where
    h = txHash td.tx

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
    map (scriptToAddressBS net . (.script)) (I.elems t.prevs)
      <> map (scriptToAddressBS net . (.script)) t.tx.outputs
  where
    net = writerNet wr :: Network

-- | Outputs spent by a transaction, without the null outpoint of a coinbase.
prevOuts :: Tx -> [OutPoint]
prevOuts tx = filter (/= nullOutPoint) (map (.outpoint) tx.inputs)
