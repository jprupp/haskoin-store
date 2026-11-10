-- | Random histories of a store, for tests that compare two ways of keeping
-- its database: the layout in use today, where the chain and the mempool
-- share their indices, and the layered layout of @docs/layered-database.md@.
--
-- A history is a sequence of operations: transactions for the mempool,
-- blocks, deletions and reversals, some of them invalid. Each is chosen from
-- a number and from what a database of today's layout holds at that point,
-- so that the same history can be given to both sets of rules.
--
-- Payments go to a handful of fixed scripts and to a few addresses of an
-- extended public key, 'wallet', so that the queries about extended public
-- keys have something to find.
module Haskoin.Store.Histories
  ( -- * Databases and rules
    open20,
    openLayered,
    withShadow,
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
    wallet,

    -- * Answers
    mixed,
    layered,
    pooled,
    Answers (..),
    ask,

    -- * Pages
    Question (..),
    Reply (..),
    questions,
    answer,
    differences,
    startOf,
    startsKnown,
    size,

    -- * Column families
    Chain (..),
    chainOf,
    dump,
  )
where

import Control.Monad (void)
import Data.Bifunctor (first)
import Data.Bits (shiftR)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Default (def)
import Data.Either (isLeft, rights)
import Data.Hashable (hashWithSalt)
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.List (sort, sortOn)
import Data.Maybe (catMaybes, fromMaybe, mapMaybe)
import Data.Serialize (decode, encode, getWord64be, runGet)
import Data.Word (Word16, Word32, Word64, Word8)
import Database.RocksDB (ColumnFamily, iterEntry, iterFirst, iterNext, withIterCF)
import Haskoin
  ( Address,
    Block (..),
    BlockHash,
    BlockHeader (..),
    BlockHeight,
    BlockNode (..),
    Ctx,
    Network,
    OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    addressToScriptBS,
    deriveXPubKey,
    headerHash,
    makeXPrvKey,
    nullOutPoint,
    pubSubKey,
    scriptToAddressBS,
    sha256,
    txHash,
  )
import Haskoin.Node (LogLevel (..), Logger (..))
import Haskoin.Store.Common
  ( Limits (..),
    Start (..),
    StoreRead (..),
    deriveAddresses,
    deriveFunction,
    sortTxs,
  )
import Haskoin.Store.Data
  ( Balance (..),
    BlockData (..),
    BlockRef (..),
    DeriveType (..),
    Prev (..),
    Spender (..),
    TxData (..),
    TxRef (..),
    UnixTime,
    Unspent (..),
    XPubBal (..),
    XPubSpec (..),
    XPubUnspent (..),
    confirmed,
    zeroBalance,
  )
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered qualified as L
import Haskoin.Store.Database.Query.Pooled qualified as P
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    addrOutCF,
    addrTxCF,
    balanceCF,
    spenderCF,
    txCF,
    unspentCF,
    withDatabaseReader,
    withLayeredReader,
  )
import Haskoin.Store.Database.Types (BalVal (..), SpenderKey (..), SpenderVal (..))
import Haskoin.Store.Database.Writer (MempoolChanges (..), Writer, runWriterMempool)
import Haskoin.Store.Logic (ImportException)
import Haskoin.Store.Logic qualified as Mixed
import Haskoin.Store.Logic.Layered qualified as Layered
import Haskoin.Store.TestBlocks (nextBlock, script)
import Test.QuickCheck (Gen, choose, elements, frequency, vectorOf)
import UnliftIO (try, withSystemTempDirectory)

-------------------------------------------------------------------------------
-- Databases and rules
-------------------------------------------------------------------------------

-- | Open a database as the store does today, and as the layered layout
-- needs it.
open20, openLayered :: Network -> Ctx -> FilePath -> (DatabaseReader -> IO a) -> IO a
open20 net ctx dir = withDatabaseReader net ctx 20 32 dir False Nothing Nothing Nothing Nothing Nothing
openLayered net ctx dir = withLayeredReader net ctx 20 32 dir False Nothing Nothing Nothing Nothing Nothing

-- | Run an action with a new database of the layout of version 20 that has
-- the start of a history ('start'), in a directory of its own.
--
-- 'plan' chooses the next operation of a history by reading a database of
-- that layout: what is unspent, what the mempool has. Given a database of
-- the layered layout it finds almost none of the mempool, and its blocks
-- confirm next to nothing. So a test of the layered layout keeps one of
-- these beside its own database, plans from it, and applies each
-- operation to both, as 'mixedRules' and 'layeredRules' take the same
-- operations and give the same outcomes.
withShadow :: Network -> Ctx -> (DatabaseReader -> IO a) -> IO a
withShadow net ctx action =
  withSystemTempDirectory "haskoin-store-shadow-" $ \dir ->
    open20 net ctx dir $ \shadow -> start mixedRules shadow >> action shadow

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
-- outputs made that have no address, which no index of addresses lists, the
-- time to give the next mempool transaction, and the scripts that payments
-- go to.
data History = History
  { chain :: ![(Block, BlockNode)],
    made :: ![Tx],
    loose :: ![OutPoint],
    clock :: !UnixTime,
    scripts :: ![ByteString]
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
  let hist = History {chain = [], made = [], loose = [], clock = 1000, scripts = places db.ctx}
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

-- | The scripts that the payments of a history go to: one that has no
-- address on Bitcoin, that of every coinbase, four more of the same kind,
-- and those of five addresses of the 'wallet'.
places :: Ctx -> [ByteString]
places ctx = hashLock : script : map p2pkh [1 .. 4] <> mapMaybe (addressToScriptBS ctx) paid
  where
    spec = wallet ctx
    branch m = map snd (deriveAddresses (deriveFunction ctx spec.deriv) (pubSubKey ctx spec.key m) 0)
    -- with addresses that are never paid between them, so that whether the
    -- later ones are within the gap depends on which earlier ones are used
    paid = map (branch 0 !!) [0, 2, 5] <> map (branch 1 !!) [0, 2]

-- | An extended public key. Five of its addresses are paid in histories:
-- three that it gives out to be paid, and two for its change.
wallet :: Ctx -> XPubSpec
wallet ctx = XPubSpec {key = deriveXPubKey ctx (makeXPrvKey seed), deriv = DeriveNormal}
  where
    seed = "the wallet of the histories of the tests of haskoin-store" :: ByteString

p2pkh :: Word8 -> ByteString
p2pkh n = "\x76\xa9\x14" <> B.replicate 20 n <> "\x88\xac"

-- | A script that has no address on Bitcoin.
hashLock :: ByteString
hashLock = "\xaa\x20" <> B.replicate 32 0xbb <> "\x87"

-- | The addresses of the scripts of a history that have one on a network.
addressesOf :: Network -> Ctx -> History -> [Address]
addressesOf net ctx hist = rights (map (scriptToAddressBS net ctx) hist.scripts)

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
-- sixteen: six are payments between the scripts of the history, from outputs
-- that are unspent, in a block or in the mempool, a few of them with ten
-- outputs and the others with up to three; one is a transaction that
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
    target j = hist.scripts !! ((s `div` 11 + j) `mod` length hist.scripts)
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
      listed <- concat <$> mapM (\a -> Q.getAddressUnspents db a def) (addressesOf db.net db.ctx hist)
      (listed <>) . catMaybes <$> mapM (Q.getUnspent db) hist.loose
    -- a payment from some of them, in equal shares: now and then in ten of
    -- them, for a transaction with many outputs to spend
    payment us =
      let total = sum (map (.value) us)
          count = if s `mod` 13 == 5 && total >= 50000 then 10 else 1 + (s `div` 7) `mod` 3
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

-- | Run an action with the queries of a database: those of today's layout,
-- where one set of indices has the chain and the mempool mixed, or those of
-- the layered layout, on a view of the database as it is now.
mixed, layered, pooled :: DatabaseReader -> (StoreRead -> IO a) -> IO a
mixed db action = action (Q.databaseRead db)
layered db action = L.withView db (action . L.layeredRead)
-- the queries of a fleet, with the store's own mempool area as the pool
pooled db action = L.withView db $ \v ->
  let r = L.viewReader v in action (P.pooledRead r (P.localPool r))

-- | Everything that the API could say about a history, in whole lists.
data Answers = Answers
  { balances :: ![(Address, Balance)],
    addressTxs :: ![(Address, [TxRef])],
    addressUnspents :: ![(Address, [Unspent])],
    txs :: ![(TxHash, Maybe TxData)],
    numbered :: ![(TxHash, [TxData])],
    spenders :: ![(OutPoint, Maybe Spender)],
    unspents :: ![(OutPoint, Maybe Unspent)],
    mempool :: ![(UnixTime, TxHash)]
  }
  deriving (Eq, Show)

-- | Ask everything about the addresses of a history and about every
-- transaction made, by its hash and by its short number, with each of its
-- outputs and one that it does not have. An address that was never used and
-- one whose balance is zero are the same to the API, and are the same here.
ask :: History -> StoreRead -> IO Answers
ask hist queries = do
  balances <- mapM (\a -> (a,) . fromMaybe (zeroBalance a) <$> queries.getBalance a) as
  addressTxs <- mapM (\a -> (a,) <$> queries.getAddressTxs a def) as
  addressUnspents <- mapM (\a -> (a,) <$> queries.getAddressUnspents a def) as
  txs <- mapM (\h -> (h,) <$> queries.getTxData h) hashes
  numbered <- mapM (\h -> (h,) <$> queries.getNumTxData (shortNumber h)) hashes
  spenders <- mapM (\p -> (p,) <$> queries.getSpender p) outpoints
  unspents <- mapM (\p -> (p,) <$> queries.getUnspent p) outpoints
  mempool <- queries.getMempool
  return Answers {balances, addressTxs, addressUnspents, txs, numbered, spenders, unspents, mempool}
  where
    as = addressesOf queries.net queries.ctx hist
    hashes = map txHash hist.made
    outpoints = [OutPoint (txHash tx) i | tx <- hist.made, i <- [0 .. fromIntegral (length tx.outputs)]]

-------------------------------------------------------------------------------
-- Pages
-------------------------------------------------------------------------------

-- | A question whose answer is a page of a list, or is read in a way that
-- the questions of 'ask' do not use: many at a time, by number, or through
-- an extended public key.
data Question
  = -- | a page of the transactions of an address
    AddressTxs !Address !Limits
  | -- | a page of the transactions of some addresses
    AddressesTxs ![Address] !Limits
  | -- | a page of the unspent outputs of an address
    AddressUnspents !Address !Limits
  | -- | a page of the unspent outputs of some addresses
    AddressesUnspents ![Address] !Limits
  | -- | the balances of some addresses
    Balances ![Address]
  | -- | the data of some transactions
    Bodies ![TxHash]
  | -- | the transactions with a short number of the Blockchain.info API
    Numbered !Word64
  | -- | a page of the mempool
    Pool !Limits
  | -- | the balances of the addresses of the 'wallet'
    WalletBalances
  | -- | a page of the transactions of the 'wallet'
    WalletTxs !Limits
  | -- | a page of the unspent outputs of the 'wallet'
    WalletUnspents !Limits
  | -- | the number of transactions of the 'wallet'
    WalletCount
  | -- | a page of the transactions of some addresses, read as those of an
    -- extended public key are
    SetTxs ![Address] !Limits
  | -- | a page of the unspent outputs of some addresses, read as those of
    -- an extended public key are
    SetUnspents ![Address] !Limits
  | -- | the number of distinct transactions of some addresses
    SetCount ![Address]
  deriving (Eq, Show)

-- | The answer to a 'Question'.
data Reply
  = Refs ![TxRef]
  | Outputs ![Unspent]
  | Amounts ![Balance]
  | Data ![Maybe TxData]
  | Matches ![TxData]
  | Listed ![(UnixTime, TxHash)]
  | Paths ![XPubBal]
  | PathOutputs ![XPubUnspent]
  | Number !Word32
  deriving (Eq, Show)

-- | Ask a question.
answer :: StoreRead -> Question -> IO Reply
answer queries = \case
  AddressTxs a limits -> Refs <$> queries.getAddressTxs a limits
  AddressesTxs as limits -> Refs <$> queries.getAddressesTxs as limits
  AddressUnspents a limits -> Outputs <$> queries.getAddressUnspents a limits
  AddressesUnspents as limits -> Outputs <$> queries.getAddressesUnspents as limits
  Balances as -> Amounts <$> queries.getBalances as
  Bodies hs -> Data <$> queries.getManyTxData hs
  Numbered i -> Matches <$> queries.getNumTxData i
  Pool limits -> Listed <$> queries.getMempoolPage limits
  WalletBalances -> Paths <$> queries.xPubBals spec
  WalletTxs limits -> Refs <$> (queries.xPubBals spec >>= \bals -> queries.xPubTxs spec bals limits)
  WalletUnspents limits -> PathOutputs <$> (queries.xPubBals spec >>= \bals -> queries.xPubUnspents spec bals limits)
  WalletCount -> Number <$> (queries.xPubBals spec >>= queries.xPubTxCount spec)
  SetTxs as limits -> Refs <$> (set as >>= \bals -> queries.xPubTxs spec bals limits)
  SetUnspents as limits -> PathOutputs <$> (set as >>= \bals -> queries.xPubUnspents spec bals limits)
  SetCount as -> Number <$> (set as >>= queries.xPubTxCount spec)
  where
    spec = wallet queries.ctx
    -- the queries about an extended public key take the balances of its
    -- addresses, so any addresses can be given to them
    set as = zipWith (\i b -> XPubBal {path = [0, i], balance = b}) [0 ..] <$> queries.getBalances as

-- | Whether the page of a question starts somewhere that both layouts
-- know, given a database of version 20: not at a transaction that left
-- the mempool unconfirmed. Version 20 keeps the bodies of those, and
-- starts a page where such a transaction was; the layered layout keeps
-- nothing of them, and its page is empty.
startsKnown :: DatabaseReader -> Question -> IO Bool
startsKnown db question = case startOf question of
  Just (Just (AtTx h)) -> maybe True (not . (.deleted)) <$> Q.getTxRecord db h
  _ -> return True

-- | The questions that two databases answer differently, with the answers
-- of both.
differences :: [Question] -> [Reply] -> [Reply] -> [(Question, Reply, Reply)]
differences asked was now = [(q, a, b) | (q, a, b) <- zip3 asked was now, a /= b]

-- | How much there is in an answer.
size :: Reply -> Int
size = \case
  Refs xs -> length xs
  Outputs xs -> length xs
  Amounts xs -> length xs
  Data xs -> length (catMaybes xs)
  Matches xs -> length xs
  Listed xs -> length xs
  Paths xs -> length xs
  PathOutputs xs -> length xs
  Number n -> fromIntegral n

-- | Where the page of a question starts, if it has a page.
startOf :: Question -> Maybe (Maybe Start)
startOf = \case
  AddressTxs _ limits -> Just limits.start
  AddressesTxs _ limits -> Just limits.start
  AddressUnspents _ limits -> Just limits.start
  AddressesUnspents _ limits -> Just limits.start
  WalletTxs limits -> Just limits.start
  WalletUnspents limits -> Just limits.start
  SetTxs _ limits -> Just limits.start
  SetUnspents _ limits -> Just limits.start
  _ -> Nothing

-- | Questions about a history: pages with small limits and offsets, from the
-- top, from any transaction ever made, whatever became of it, from one that
-- was never made, and from heights up to above the best block; about the
-- addresses of the history, one at a time and several together, with one
-- that was never used and with repetitions; and about the 'wallet'.
questions :: Network -> Ctx -> History -> Int -> Gen [Question]
questions net ctx hist n = vectorOf n question
  where
    question =
      frequency
        [ (4, AddressTxs <$> address <*> limits),
          (4, AddressesTxs <$> several address <*> limits),
          (4, AddressUnspents <$> address <*> limits),
          (4, AddressesUnspents <$> several address <*> limits),
          (1, Balances <$> several address),
          (1, Bodies <$> several hash),
          (1, Numbered . shortNumber <$> hash),
          (1, Pool <$> limits),
          (1, pure WalletBalances),
          (3, WalletTxs <$> limits),
          (3, WalletUnspents <$> limits),
          (1, pure WalletCount),
          (3, SetTxs <$> several address <*> limits),
          (3, SetUnspents <$> several address <*> limits),
          (1, SetCount <$> several address)
        ]
    address = frequency [(12, elements (addressesOf net ctx hist)), (1, elements unused)]
    unused = rights [scriptToAddressBS net ctx (p2pkh 9)]
    hash = frequency [(12, elements (map txHash hist.made)), (1, pure (TxHash (sha256 ("never made" :: ByteString))))]
    several :: Gen a -> Gen [a]
    several one = frequency [(1, pure 0), (12, choose (1, 6))] >>= (`vectorOf` one)
    limits =
      Limits
        <$> frequency [(1, pure 0), (5, choose (1, 6))]
        <*> frequency [(3, pure 0), (2, choose (1, 3))]
        <*> frequency
          [ (3, pure Nothing),
            (6, Just . AtTx <$> hash),
            (2, Just . AtBlock <$> choose (0, best + 2))
          ]
    best :: BlockHeight
    best = case hist.chain of
      (_, node) : _ -> node.height
      [] -> 0

-- | The short number of a transaction in the Blockchain.info API: the first
-- 53 bits of its hash.
shortNumber :: TxHash -> Word64
shortNumber h = either error (`shiftR` 11) (runGet getWord64be (encode h))

-------------------------------------------------------------------------------
-- The chain's column families
-------------------------------------------------------------------------------

-- | What the chain's column families hold, but for what version 20 left
-- there that does not come from the chain: bodies marked as deleted, and
-- balances at zero. The time a transaction was first seen is apart from its
-- body, because a database that never saw the mempool has another.
data Chain = Chain
  { bodies :: ![(ByteString, (BlockRef, Tx, IntMap Prev, Bool))],
    firstSeen :: ![(ByteString, UnixTime)],
    addressTxs :: ![(ByteString, ByteString)],
    addressOutputs :: ![(ByteString, ByteString)],
    unspents :: ![(ByteString, ByteString)],
    balances :: ![(ByteString, BalVal)],
    -- | the inputs of blocks that spend outputs, each with the output that
    -- it spends, whether the database has it under a key of version 22 or
    -- in the record of the transaction that made the output, as the
    -- versions before did: where they are is not what this compares
    spenders :: ![(ByteString, ByteString)]
  }
  deriving (Eq, Show)

chainOf :: DatabaseReader -> IO Chain
chainOf db = do
  txs <- dump db (txCF db.db) >>= mapM (\(k, v) -> (k,) <$> decoded v)
  addressTxs <- dump db (addrTxCF db.db)
  addressOutputs <- dump db (addrOutCF db.db)
  unspents <- dump db (unspentCF db.db)
  balances <- dump db (balanceCF db.db) >>= mapM (\(k, v) -> (k,) <$> decoded v)
  spenders <- dump db (spenderCF db.db)
  let live = [(k, td :: TxData) | (k, td) <- txs, not td.deleted]
  return
    Chain
      { bodies = [(k, (td.block, td.tx, td.prevs, td.rbf)) | (k, td) <- live],
        firstSeen = [(k, td.timestamp) | (k, td) <- live],
        addressTxs,
        addressOutputs,
        unspents,
        balances = filter ((/= def) . snd) balances,
        spenders =
          sort $
            [ (encode (OutPoint h (fromIntegral i)), encode spender)
            | (k, v) <- spenders,
              Right (SpenderKey h _) <- [decode k],
              Right (SpenderVal m) <- [decode v],
              (i, spender) <- IntMap.toList m
            ]
              <> [ (encode (OutPoint (txHash td.tx) (fromIntegral i)), encode spender)
                 | (_, td) <- live,
                   (i, spender) <- IntMap.toList td.spenders
                 ]
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
