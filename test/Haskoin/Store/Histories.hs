-- | Random histories of a store, for tests that compare two ways of keeping
-- its database: the layout in use today, where the chain and the mempool
-- share their indices, and the layered layout of @docs/layered-database.md@.
--
-- A history is a sequence of operations: transactions for the mempool,
-- blocks, deletions and reversals, some of them invalid. Each is chosen from
-- a number and from what a database of today's layout holds at that point,
-- so that the same history can be given to both sets of rules.
module Haskoin.Store.Histories
  ( -- * Databases and rules
    open20,
    openLayered,
    Rules (..),
    mixedRules,
    layeredRules,
    run,

    -- * Histories
    History (..),
    Op (..),
    Outcome (..),
    start,
    plan,
    apply,
    record,
    step,
    blockOn,
    coinOf,
    payments,
    anyNumber,
    onlyPayments,
    p2pkh,
    hashLock,
    addressesOf,

    -- * Answers
    Reads (..),
    mixed,
    layered,
    Answers (..),
    ask,

    -- * Column families
    Chain (..),
    chainOf,
    dump,
  )
where

import Control.Monad (void)
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Default (def)
import Data.Either (isLeft, rights)
import Data.Hashable (hashWithSalt)
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.List (sort, sortOn)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Serialize (decode, encode)
import Data.Word (Word16, Word8)
import Database.RocksDB (ColumnFamily, iterEntry, iterFirst, iterNext, withIterCF)
import Haskoin
  ( Address,
    Block (..),
    BlockHash,
    BlockHeader (..),
    BlockNode (..),
    Ctx,
    Network,
    OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    headerHash,
    nullOutPoint,
    scriptToAddressBS,
    sha256,
    txHash,
  )
import Haskoin.Node (LogLevel (..), Logger (..))
import Haskoin.Store.Common (sortTxs)
import Haskoin.Store.Data
  ( Balance (..),
    BlockData (..),
    BlockRef (..),
    Prev (..),
    Spender (..),
    TxData (..),
    TxRef (..),
    UnixTime,
    Unspent (..),
    confirmed,
    zeroBalance,
  )
import Haskoin.Store.Database.Mempool qualified as M
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    addrOutCF,
    addrTxCF,
    balanceCF,
    txCF,
    unspentCF,
    withDatabaseReader,
    withLayeredReader,
  )
import Haskoin.Store.Database.Types (BalVal (..))
import Haskoin.Store.Database.Writer (MempoolChanges (..), Writer, runWriterMempool)
import Haskoin.Store.Logic (ImportException)
import Haskoin.Store.Logic qualified as Mixed
import Haskoin.Store.Logic.Layered qualified as Layered
import Haskoin.Store.TestBlocks (nextBlock, script)
import Test.QuickCheck (Gen, choose)
import UnliftIO (try)

-------------------------------------------------------------------------------
-- Databases and rules
-------------------------------------------------------------------------------

-- | Open a database as the store does today, and as the layered layout
-- needs it.
open20, openLayered :: Network -> Ctx -> FilePath -> (DatabaseReader -> IO a) -> IO a
open20 net ctx dir = withDatabaseReader net ctx 20 32 dir False Nothing Nothing Nothing Nothing Nothing
openLayered net ctx dir = withLayeredReader net ctx 20 32 dir False Nothing Nothing Nothing Nothing Nothing

-- | The rules that import blocks and transactions, for one layout.
data Rules = Rules
  { initBest :: Writer -> IO (),
    importBlock :: Writer -> Block -> BlockNode -> IO (BlockData, [TxData]),
    revertBlock :: Writer -> BlockHash -> IO (),
    newMempoolTx :: Writer -> Tx -> UnixTime -> IO Bool,
    deleteUnconfirmedTx :: Writer -> Bool -> TxHash -> IO ()
  }

-- | The rules of the layout in use today.
mixedRules :: Rules
mixedRules =
  Rules
    { initBest = Mixed.initBest,
      importBlock = Mixed.importBlock,
      revertBlock = Mixed.revertBlock,
      newMempoolTx = Mixed.newMempoolTx,
      deleteUnconfirmedTx = Mixed.deleteUnconfirmedTx
    }

-- | The rules of the layered layout.
layeredRules :: Rules
layeredRules =
  Rules
    { initBest = Layered.initBest,
      importBlock = Layered.importBlock,
      revertBlock = Layered.revertBlock,
      newMempoolTx = Layered.newMempoolTx,
      deleteUnconfirmedTx = Layered.deleteUnconfirmedTx
    }

-- | Run a batch of writes, and give what it returned with how it changed
-- the mempool, or the reason why it wrote nothing.
run :: DatabaseReader -> (Writer -> IO a) -> IO (Either ImportException (a, MempoolChanges))
run db = try . runWriterMempool db.net db.ctx db quiet
  where
    quiet = Logger {level = LevelError, source = "", push = \_ -> return ()}

-------------------------------------------------------------------------------
-- Histories
-------------------------------------------------------------------------------

-- | What happened to a database: its main chain, newest block first, every
-- transaction ever made for it, taken or not, coinbases included, the
-- outputs made that have no address, which no index of addresses lists, and
-- the time to give the next mempool transaction.
data History = History
  { chain :: ![(Block, BlockNode)],
    made :: ![Tx],
    loose :: ![OutPoint],
    clock :: !UnixTime
  }

-- | Something that happens to a store.
data Op
  = -- | a transaction arrives for the mempool, at a time
    Pay !Tx !UnixTime
  | -- | a block arrives, on the best block
    Mine !Block !BlockNode
  | -- | a mempool transaction is deleted, with its descendants
    Delete !TxHash
  | -- | the best block is reverted
    Revert !BlockHash
  deriving (Show)

-- | What an operation did: what the rules returned or why they refused, and
-- the transactions that entered and left the mempool.
data Outcome = Outcome
  { result :: !(Either ImportException String),
    added :: ![TxHash],
    removed :: ![TxHash]
  }
  deriving (Eq, Show)

-- | Give an operation to a set of rules.
apply :: Rules -> DatabaseReader -> Op -> IO Outcome
apply rules db = \case
  Pay tx time -> outcome <$> run db (\w -> rules.newMempoolTx w tx time)
  -- the data of the transactions is left out: it comes with the spenders
  -- that each layout keeps in a body
  Mine block node -> outcome . fmap (first fst) <$> run db (\w -> rules.importBlock w block node)
  Delete h -> outcome <$> run db (\w -> rules.deleteUnconfirmedTx w False h)
  Revert h -> outcome <$> run db (\w -> rules.revertBlock w h)
  where
    outcome :: (Show a) => Either ImportException (a, MempoolChanges) -> Outcome
    outcome (Left e) = Outcome {result = Left e, added = [], removed = []}
    outcome (Right (x, c)) = Outcome {result = Right (show x), added = sort c.added, removed = sort c.removed}

-- | Note what an operation did in a history. Transactions are noted whether
-- they were taken or not, so that questions are asked about all of them.
record :: DatabaseReader -> History -> Op -> Outcome -> History
record db hist op out = case op of
  Pay tx _ -> noted [tx]
  Mine block node
    | isLeft out.result -> noted block.txs
    | otherwise -> (noted block.txs) {chain = (block, node) : hist.chain}
  Delete _ -> hist
  Revert _
    | isLeft out.result -> hist
    | otherwise -> hist {chain = drop 1 hist.chain}
  where
    known = map txHash hist.made
    noted txs =
      let new = [tx | tx <- txs, txHash tx `notElem` known]
          unlisted =
            [ OutPoint (txHash tx) i
            | tx <- new,
              (i, o) <- zip [0 ..] tx.outputs,
              isLeft (scriptToAddressBS db.net db.ctx o.script)
            ]
       in hist {made = new <> hist.made, loose = unlisted <> hist.loose}

-- | A database with the genesis block and one block with a coin, under a
-- set of rules, and its history so far.
start :: Rules -> DatabaseReader -> IO History
start rules db = do
  void (run db rules.initBest) :: IO ()
  let hist = History {chain = [], made = [], loose = [], clock = 1000}
  block <- blockOn db []
  record db hist block <$> apply rules db block

-- | A block with these transactions on the best block of a database.
blockOn :: DatabaseReader -> [Tx] -> IO Op
blockOn db txs = do
  best <- Q.getBestBlock db >>= maybe (fail "no best block") return
  parent <- Q.getBlock db best >>= maybe (fail "no data for the best block") return
  return (uncurry Mine (nextBlock db.net parent.height parent.header txs))

-- | The output of the coinbase of the best block.
coinOf :: History -> OutPoint
coinOf hist = case hist.chain of
  (Block {txs = coinbase : _}, _) : _ -> OutPoint (txHash coinbase) 0
  _ -> error "no block with a coinbase"

-- | Scripts that pay to five addresses: that of every coinbase, and four
-- more.
scripts :: [ByteString]
scripts = script : map p2pkh [1 .. 4]

p2pkh :: Word8 -> ByteString
p2pkh n = "\x76\xa9\x14" <> B.replicate 20 n <> "\x88\xac"

-- | A script that has no address on Bitcoin.
hashLock :: ByteString
hashLock = "\xaa\x20" <> B.replicate 32 0xbb <> "\x87"

-- | The addresses of the scripts of the tests that have one on the network
-- of a database.
addressesOf :: DatabaseReader -> [Address]
addressesOf db = rights (map (scriptToAddressBS db.net db.ctx) (hashLock : scripts))

-- | Eight payments, each from the change of the one before, starting from
-- a coin: a chain in the mempool.
payments :: OutPoint -> [Tx]
payments coin = take 8 (go coin 5000000000)
  where
    go from value =
      let left = value - 10000
          tx = Tx 2 [TxIn from "" maxBound] [TxOut left script, TxOut 9000 (p2pkh 3)] [] 0
       in tx : go (OutPoint (txHash tx) 0) left

-- | A number for 'plan', any of them as likely as any other. QuickCheck's
-- own generator of numbers starts with small ones, which would make the
-- first histories of every run nothing but the same payment.
anyNumber :: Gen Word16
anyNumber = choose (minBound, maxBound)

-- | Turn numbers into numbers that 'plan' reads as payments.
onlyPayments :: [Word16] -> [Word16]
onlyPayments ps = [p - p `mod` 16 + p `mod` 6 | p <- ps]

-- | Choose and carry out one operation under the rules of today's layout,
-- which is the layout that 'plan' reads.
step :: DatabaseReader -> History -> Word16 -> IO History
step db hist seed =
  plan db hist seed >>= \case
    (hist', Nothing) -> return hist'
    (hist', Just op) -> record db hist' op <$> apply mixedRules db op

-- | Choose the next operation of a history from a number, given a database
-- of today's layout that has gone through the history so far. Out of
-- sixteen: six are payments between the scripts of the tests, from outputs
-- that are unspent, in a block or in the mempool; one is a transaction that
-- must be refused; one conflicts with a mempool transaction; four are
-- blocks, with part of the mempool, with a transaction that the mempool has
-- not seen, or with one that conflicts with the mempool; two are deletions;
-- one is the reversal of the best block; and one sends an earlier
-- transaction again. The clock moves on with some of them, so that mempool
-- transactions share a time now and then.
plan :: DatabaseReader -> History -> Word16 -> IO (History, Maybe Op)
plan db hist0 seed = (hist,) <$> chosen
  where
    hist = hist0 {clock = hist0.clock + fromIntegral (seed `mod` 2)}
    kind = seed `mod` 16
    s = fromIntegral seed `div` 16 :: Int
    chosen
      | kind <= 5 = pay
      | kind == 6 = refused
      | kind == 7 = rival (\tx -> return (Just (Pay tx hist.clock)))
      | kind <= 9 = block . shuffled =<< some
      | kind == 10 = rival (\tx -> Just <$> blockOn db [tx]) >>= maybe (block []) (return . Just)
      | kind == 11 = unseen
      | kind == 12 = delete
      | kind == 13 = deleteOther
      | kind == 14 = revert
      | otherwise = again
    rotate k xs = let k' = k `mod` max 1 (length xs) in drop k' xs <> take k' xs
    target j = (hashLock : scripts) !! ((s `div` 11 + j) `mod` 6)
    spending us values =
      let outputs = [TxOut v (target j) | (j, v) <- zip [0 ..] values]
       in Tx 2 [TxIn u.outpoint "" signal | u <- us] outputs [] 0
    -- some signal that they can be replaced
    signal = if even (s `div` 13) then maxBound else 0
    block txs = Just <$> blockOn db txs
    shuffled = sortOn (hashWithSalt s . encode . txHash)
    pool = map snd <$> Q.getMempool db
    -- outputs that nothing spends, in a block or in the mempool
    unspent = do
      listed <- concat <$> mapM (\a -> Q.getAddressUnspents db a def) (addressesOf db)
      (listed <>) . catMaybes <$> mapM (Q.getUnspent db) hist.loose
    -- a payment from some of them, in equal shares
    payment us =
      let total = sum (map (.value) us)
          count = 1 + (s `div` 7) `mod` 3
          share = (total - 500) `div` fromIntegral count
       in if null us || total < 500 + 600 * fromIntegral count
            then Nothing
            else Just (spending us (replicate count share))
    pay = do
      us <- take (1 + s `mod` 3) . rotate (s `div` 3) <$> unspent
      return ((`Pay` hist.clock) <$> payment us)
    -- part of the mempool, in an order in which it can be mined
    some = do
      tds <- catMaybes <$> (pool >>= Q.getManyTxData db)
      let ordered = map snd (sortTxs (map (.tx) tds))
      return (take (s `mod` (length ordered + 1)) ordered)
    -- a block with part of the mempool and a payment that the mempool has
    -- not seen, from outputs of blocks
    unseen = do
      us <- take (1 + s `mod` 2) . rotate (s `div` 3) . filter (confirmed . (.block)) <$> unspent
      txs <- some
      block (shuffled (txs <> maybe [] pure (payment us)))
    -- a transaction that spends what a mempool transaction spends, when
    -- that is an output of a block
    rival :: (Tx -> IO (Maybe Op)) -> IO (Maybe Op)
    rival make =
      rotate s <$> pool >>= \case
        [] -> return Nothing
        h : _ ->
          Q.getTxData db h >>= \case
            Just td
              | i : _ <- td.tx.inputs,
                Just p <- IntMap.lookup 0 td.prevs,
                p.value > 2000 ->
                  Q.getTxData db i.outpoint.hash >>= \case
                    Just parent
                      | confirmed parent.block ->
                          let value = p.value - 700 - fromIntegral (s `mod` 50)
                           in make (Tx 2 [TxIn i.outpoint "" maxBound] [TxOut value (target 0)] [] 0)
                    _ -> return Nothing
            _ -> return Nothing
    -- a transaction or a block that the rules must refuse
    refused = do
      us <- rotate (s `div` 8) <$> unspent
      let spent = [i.outpoint | (b, _) <- hist.chain, mined <- drop 1 b.txs, i <- take 1 mined.inputs]
          unknown = Tx 2 [TxIn (OutPoint (TxHash (sha256 (encode seed))) 0) "" maxBound] [TxOut 1000 script] [] 0
          again' p = Tx 2 [TxIn p "" maxBound] [TxOut 1000 script] [] 0
          send tx = return (Just (Pay tx hist.clock))
      case (s `mod` 8, us, rotate s spent) of
        -- an output that a confirmed transaction has spent
        (1, _, p : _) -> send (again' p)
        -- the same output twice
        (2, u : _, _) -> send (Tx 2 (replicate 2 (TxIn u.outpoint "" maxBound)) [TxOut 1000 script] [] 0)
        -- more than it has
        (3, u : _, _) -> send (Tx 2 [TxIn u.outpoint "" maxBound] [TxOut (u.value + 1) script] [] 0)
        -- a coinbase
        (4, _, _) -> send (Tx 1 [TxIn nullOutPoint (encode seed) maxBound] [TxOut 1000 script] [] 0)
        -- a block that spends an output that does not exist
        (5, _, _) -> block [unknown]
        -- a block that spends an output that a confirmed transaction has
        -- spent
        (6, _, p : _) -> block [again' p]
        -- a block that does not build on the best block, but beside it
        (7, _, _) | (_, best) : _ <- hist.chain -> do
          parent <- Q.getBlock db best.header.prev >>= maybe (fail "no data for the parent") return
          return . Just $ uncurry Mine (nextBlock db.net parent.height parent.header [])
        -- an output that does not exist
        _ -> send unknown
    delete =
      fmap Delete . take 1 . rotate s <$> pool >>= \case
        [] -> return Nothing
        d : _ -> return (Just d)
    -- the deletion of a confirmed transaction, or of one that is not there
    deleteOther =
      let mined = [txHash tx | (b, _) <- hist.chain, tx <- b.txs]
       in return . Just . Delete $ case rotate s mined of
            h : _ | even s -> h
            _ -> TxHash (sha256 (encode seed))
    revert = return $ case hist.chain of
      (b, _) : _ : _ -> Just (Revert (headerHash b.header))
      _ -> Nothing
    -- an earlier transaction, which may be confirmed, in the mempool,
    -- deleted, or missing what it spends
    again = return $ case rotate s hist.made of
      tx : _ -> Just (Pay tx hist.clock)
      [] -> Nothing

-------------------------------------------------------------------------------
-- Answers
-------------------------------------------------------------------------------

-- | The queries of a layout.
data Reads = Reads
  { txData :: TxHash -> IO (Maybe TxData),
    spender :: OutPoint -> IO (Maybe Spender),
    unspent :: OutPoint -> IO (Maybe Unspent),
    balance :: Address -> IO (Maybe Balance),
    addressTxs :: Address -> IO [TxRef],
    addressUnspents :: Address -> IO [Unspent]
  }

-- | Queries of today's layout, where one set of indices has the chain and
-- the mempool mixed.
mixed :: DatabaseReader -> Reads
mixed db =
  Reads
    { txData = Q.getTxData db,
      spender = Q.getSpender db,
      unspent = Q.getUnspent db,
      balance = Q.getBalance db,
      addressTxs = \a -> Q.getAddressTxs db a def,
      addressUnspents = \a -> Q.getAddressUnspents db a def
    }

-- | Queries of the layered layout.
layered :: DatabaseReader -> Reads
layered db =
  Reads
    { txData = M.getTxData db,
      spender = M.getSpender db,
      unspent = M.getUnspent db,
      balance = M.getBalance db,
      addressTxs = M.getAddressTxs db,
      addressUnspents = M.getAddressUnspents db
    }

-- | Everything that the API could say about a history.
data Answers = Answers
  { balances :: ![(Address, Balance)],
    addressTxs :: ![(Address, [TxRef])],
    addressUnspents :: ![(Address, [Unspent])],
    txs :: ![(TxHash, Maybe TxData)],
    spenders :: ![(OutPoint, Maybe Spender)],
    unspents :: ![(OutPoint, Maybe Unspent)],
    mempool :: ![(UnixTime, TxHash)]
  }
  deriving (Eq, Show)

-- | Ask everything about the addresses of the tests and about every
-- transaction made, with each of its outputs and one that it does not have.
ask :: Reads -> DatabaseReader -> [Tx] -> IO Answers
ask queries db made = do
  balances <- mapM (\a -> (a,) . fromMaybe (zeroBalance a) <$> queries.balance a) as
  addressTxs <- mapM (\a -> (a,) <$> queries.addressTxs a) as
  addressUnspents <- mapM (\a -> (a,) <$> queries.addressUnspents a) as
  txs <- mapM (\h -> (h,) <$> queries.txData h) hashes
  spenders <- mapM (\p -> (p,) <$> queries.spender p) outpoints
  unspents <- mapM (\p -> (p,) <$> queries.unspent p) outpoints
  mempool <- Q.getMempool db
  return Answers {balances, addressTxs, addressUnspents, txs, spenders, unspents, mempool}
  where
    as = addressesOf db
    hashes = map txHash made
    outpoints = [OutPoint (txHash tx) i | tx <- made, i <- [0 .. fromIntegral (length tx.outputs)]]

-------------------------------------------------------------------------------
-- The chain's column families
-------------------------------------------------------------------------------

-- | What the chain's column families hold, but for what version 20 left
-- there that does not come from the chain: bodies marked as deleted, and
-- balances at zero. The time a transaction was first seen is apart from its
-- body, because a database that never saw the mempool has another.
data Chain = Chain
  { bodies :: ![(ByteString, (BlockRef, Tx, IntMap Prev, Bool, IntMap Spender))],
    firstSeen :: ![(ByteString, UnixTime)],
    addressTxs :: ![(ByteString, ByteString)],
    addressOutputs :: ![(ByteString, ByteString)],
    unspents :: ![(ByteString, ByteString)],
    balances :: ![(ByteString, BalVal)]
  }
  deriving (Eq, Show)

chainOf :: DatabaseReader -> IO Chain
chainOf db = do
  txs <- dump db (txCF db.db) >>= mapM (\(k, v) -> (k,) <$> decoded v)
  addressTxs <- dump db (addrTxCF db.db)
  addressOutputs <- dump db (addrOutCF db.db)
  unspents <- dump db (unspentCF db.db)
  balances <- dump db (balanceCF db.db) >>= mapM (\(k, v) -> (k,) <$> decoded v)
  let live = [(k, td :: TxData) | (k, td) <- txs, not td.deleted]
  return
    Chain
      { bodies = [(k, (td.block, td.tx, td.prevs, td.rbf, td.spenders)) | (k, td) <- live],
        firstSeen = [(k, td.timestamp) | (k, td) <- live],
        addressTxs,
        addressOutputs,
        unspents,
        balances = filter ((/= def) . snd) balances
      }
  where
    decoded v = either fail return (decode v)

-- | Every key and value of a column family.
dump :: DatabaseReader -> ColumnFamily -> IO [(ByteString, ByteString)]
dump db cf = withIterCF db.db cf $ \iter -> iterFirst iter >> go iter
  where
    go iter =
      iterEntry iter >>= \case
        Nothing -> return []
        Just e -> iterNext iter >> (e :) <$> go iter
