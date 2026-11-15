module Haskoin.Store.Logic.StagedSpec (spec) where

import Control.DeepSeq (force)
import Control.Exception (SomeException, evaluate, try)
import Control.Monad (foldM, forM, forM_, void, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Default (def)
import Data.Either (isRight)
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.IntMap.Strict qualified as IntMap
import Data.List (sort, (\\))
import Data.Maybe (catMaybes, isJust)
import Data.Serialize (encode)
import Data.Word (Word64)
import Database.RocksDB (columnFamilies, compactRange, flush, iterEntry, iterFirst, iterNext, withIter)
import Database.RocksDB.Query (insertCF)
import Haskoin
  ( Block (..),
    Network (..),
    OutPoint (..),
    Tx (..),
    TxHash (..),
    TxIn (..),
    TxOut (..),
    bchRegTest,
    btcRegTest,
    headerHash,
    nullOutPoint,
    sha256,
    txHash,
  )
import Haskoin.Node (LogLevel (..), Logger (..))
import Haskoin.Store.Common (sortTxs)
import Haskoin.Store.Data (BlockRef (..), Prev (..), TxData (..), Unspent (..), confirmed)
import Haskoin.Store.Database.Copy (spentOutputs)
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered qualified as L
import Haskoin.Store.Database.Reader
  ( DatabaseReader (..),
    forgetMempoolCopy,
    knownMempoolCopy,
    mempoolCF,
  )
import Haskoin.Store.Database.Types (MemDeletedKey (..))
import Haskoin.Store.Database.Views (closeViews, newViews, takeView, withViewAt)
import Haskoin.Store.Database.Writer (MempoolChanges (..), Writer, runWriterMempool)
import Haskoin.Store.Histories
import Haskoin.Store.Integration.Temp (withSystemTempDirectory)
import Haskoin.Store.Logic (ImportException (..))
import Haskoin.Store.Logic.Layered (PreparedBlock (..), PreparedTx (..), prepareBlock)
import Haskoin.Store.TestBlocks (coinbaseTx, script)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck
  ( Gen,
    Property,
    arbitrary,
    choose,
    elements,
    forAll,
    frequency,
    ioProperty,
    property,
    shuffle,
    sublistOf,
    tabulate,
    vectorOf,
  )
import Test.QuickCheck.Gen (unGen)
import Test.QuickCheck.Random (mkQCGen)

spec :: Spec
spec = describe "import of a block in stages" $ do
  forM_ [("Bitcoin", btcRegTest), ("Bitcoin Cash, without replace-by-fee", bchRegTest)] $ \(name, net) -> do
    modifyMaxSuccess (const 30) $
      it ("leaves every key that the sequential import leaves, for random histories on " <> name) $
        property . forAll (vectorOf 70 anyNumber) $ \seeds ->
          ioProperty (labelled <$> sameStages net [\db h -> plan db h seed | seed <- seeds] unchecked)
    modifyMaxSuccess (const 30) $
      it ("leaves every key that the sequential import leaves, for blocks that the mempool has not seen on " <> name) $
        property . forAll (vectorOf 50 (choose (0, 1000000))) $ \seeds ->
          ioProperty (labelled <$> sameStages net (map unseen seeds) unchecked)
    modifyMaxSuccess (const 30) $
      it ("leaves every key that the sequential import leaves, for blocks that leave and come back on " <> name) $
        property . forAll (vectorOf 60 (choose (0, 1000000))) $ \seeds -> ioProperty $ do
          undone <- newIORef []
          labelled <$> sameStages net (map (churn undone) seeds) unchecked
    modifyMaxSuccess (const 10) $
      it ("writes nothing of a block that could not be mined, after random histories on " <> name) $
        property . forAll (vectorOf 30 (choose (0, 1000000))) $ \seeds ->
          forAll (choose (0, 1000000)) $ \seed ->
            ioProperty (labelled <$> sameStages net (map unseen seeds <> [spendCoin]) (unminable seed))

  it "leaves the same keys when a batch has changes already, and beside what version 21 left in the mempool area" $ do
    -- two blocks in one batch, the second spending what the first made and
    -- what the first spent from; then a mempool transaction and the block
    -- that confirms it in one batch
    let leftover old new =
          forM_ [old, new] $ \db -> do
            insertCF db.db (mempoolCF db.db) (MemDeletedKey (TxHash (sha256 ("gone" :: ByteString)))) deletedBody
            -- written behind the batches: the copy of the area is to be
            -- loaded again
            forgetMempoolCopy db
        deletedBody =
          TxData
            { block = MemRef 5,
              tx = Tx 2 [TxIn (OutPoint (TxHash (sha256 ("none" :: ByteString))) 0) "" maxBound] [TxOut 1 script] [] 0,
              prevs = mempty,
              deleted = True,
              rbf = False,
              timestamp = 5,
              spenders = mempty
            }
        together shadow hist = do
          let coin = coinOf hist
              first = Tx 2 [TxIn coin "" maxBound] [TxOut 3000 script, TxOut 4000 (p2pkh 1), TxOut 5000 (p2pkh 2)] [] 0
              out i = OutPoint (txHash first) i
              second = Tx 2 [TxIn (out 0) "" maxBound, TxIn (out 2) "" maxBound] [TxOut 7000 (p2pkh 1)] [] 0
              third = Tx 2 [TxIn (out 1) "" maxBound] [TxOut 3500 (p2pkh 3)] [] 0
          Mine b1 n1 <- blockOn shadow [first]
          void (apply mixedRules shadow (Mine b1 n1))
          Mine b2 n2 <- blockOn shadow [second]
          void (apply mixedRules shadow (Mine b2 n2))
          Mine b3 n3 <- blockOn shadow [third]
          return
            [ \rules w -> show <$> (rules.importBlock w b1 n1 >> rules.importBlock w b2 n2),
              \rules w -> show <$> (rules.newMempoolTx w third 2000 >> rules.importBlock w b3 n3)
            ]
    withThree btcRegTest $ \shadow old new hist -> do
      leftover old new
      batches <- together shadow hist
      forM_ batches $ \batch -> do
        was <- batchOf old (batch sequentialRules)
        now <- batchOf new (batch layeredRules)
        now `shouldBe` was
        fst now `shouldSatisfy` isRight
        raw new >>= \mine -> raw old >>= \theirs -> mine `shouldBe` theirs
        copyIsArea "a batch with changes already" new

  it "refuses a block with a transaction that has inputs and is confirmed already, which the sequential import moved" $
    withThree btcRegTest $ \shadow old new hist -> do
      let paid = Tx 2 [TxIn (coinOf hist) "" maxBound] [TxOut 4000 (p2pkh 1)] [] 0
      Mine b1 n1 <- blockOn shadow [paid]
      void (apply mixedRules shadow (Mine b1 n1))
      Mine b2 n2 <- blockOn shadow [paid]
      forM_ [(old, sequentialRules), (new, layeredRules)] $ \(db, rules) ->
        fst <$> batchOf db (\w -> show <$> rules.importBlock w b1 n1) >>= (`shouldSatisfy` isRight)
      untouched <- raw new
      -- no block that is mined has it: its input is spent
      fst <$> batchOf old (\w -> show <$> sequentialRules.importBlock w b2 n2) >>= (`shouldSatisfy` isRight)
      fst <$> batchOf new (\w -> show <$> layeredRules.importBlock w b2 n2) `shouldReturn` Left "Orphan"
      raw new `shouldReturn` untouched

  describe "the outputs that mempool transactions spend, remembered" $ do
    it "are those of the chain that a transaction spends, until a block confirms it" $
      funded $ \give mine coins new -> do
        let a = spend 0 (take 2 coins)
            child = Tx 2 [TxIn (OutPoint (txHash a) 0) "" maxBound] [TxOut 500 script] [] 0
            other = spend 2 [coins !! 2]
            unseenTx = spend 3 [coins !! 3]
        give (Pay a 2000)
        keptBy new `shouldReturn` sort (take 2 coins)
        -- an output of a mempool transaction is not one of the chain
        give (Pay child 2001)
        keptBy new `shouldReturn` sort (take 2 coins)
        mine [a]
        keptBy new `shouldReturn` []
        -- a block with a transaction whose output is not remembered, as
        -- the chain did not have it when the transaction came, one whose
        -- output is, and one that the mempool has not seen
        give (Pay other 2002)
        keptBy new `shouldReturn` [coins !! 2]
        mine [child, other, unseenTx]
        keptBy new `shouldReturn` []

    it "are forgotten when the transaction is deleted, and when it is replaced" $
      funded $ \give mine coins new -> do
        let a = spend 0 (take 2 coins)
            rival = spend 1 [coins !! 1, coins !! 2]
        give (Pay a 2000)
        give (Delete (txHash a))
        keptBy new `shouldReturn` []
        give (Pay a 2001)
        -- the rival takes the place of the transaction: what both spend
        -- stays, with its new spender
        give (Pay rival 2002)
        keptBy new `shouldReturn` sort [coins !! 1, coins !! 2]
        mine [rival]
        keptBy new `shouldReturn` []

    it "are all forgotten when a block is reverted" $
      funded $ \give mine coins new -> do
        let a = spend 0 (take 2 coins)
        mine []
        give (Pay a 2000)
        keptBy new `shouldReturn` sort (take 2 coins)
        best <- Q.getBestBlock new >>= maybe (fail "no best block") return
        give (Revert best 2001)
        keptBy new `shouldReturn` []
        -- the transaction is confirmed from what the database has
        mine [a]

    it "follow a coinbase that is confirmed again while a mempool transaction spends it" $
      withThree btcRegTest $ \_ old new hist -> do
        -- the coinbase of the first block, whose output a mempool
        -- transaction spends, comes again in a later block: its unspent
        -- output moves to that block
        let twice = coinbaseTx 1
            coin = OutPoint (txHash twice) 0
            a = Tx 2 [TxIn coin "" maxBound] [TxOut 4000 (p2pkh 1)] [] 0
            give = sameStep old new
            position = fmap (.block) <$> Q.getUnspent new coin
        hist.chain `shouldSatisfy` any ((== [twice]) . (.txs) . fst)
        give (Pay a 2000)
        keptBy new `shouldReturn` [coin]
        position `shouldReturn` Just (BlockRef 1 0)
        give =<< blockOn old [twice]
        keptBy new `shouldReturn` [coin]
        position `shouldReturn` Just (BlockRef 2 1)
        give =<< blockOn old [a]
        keptBy new `shouldReturn` []

    it "start empty on a database that is opened again, which imports the same" $
      withSystemTempDirectory "haskoin-store-reopened-" $ \dirOnce ->
        withSystemTempDirectory "haskoin-store-reopened-" $ \dirTwice -> do
          let a coin = Tx 2 [TxIn coin "" maxBound] [TxOut 3000 (p2pkh 1), TxOut 4000 (p2pkh 2)] [] 0
              taken db op = fst <$> full layeredRules db op >>= (`shouldSatisfy` isRight)
              arrive db = do
                hist <- start layeredRules db
                taken db (Pay (a (coinOf hist)) 2000)
                keptBy db `shouldReturn` [coinOf hist]
                return (a (coinOf hist))
              confirm db tx = do
                blockOn db [tx] >>= taken db
                keptBy db `shouldReturn` []
                raw db
          -- one store stays open, and confirms the payment from what it
          -- remembers
          once <- openLayered btcRegTest dirOnce $ \db -> arrive db >>= confirm db
          -- the other is closed in between, and remembers nothing
          tx <- openLayered btcRegTest dirTwice arrive
          twice <- openLayered btcRegTest dirTwice $ \db -> do
            isJust <$> knownMempoolCopy db `shouldReturn` False
            confirm db tx
          twice `shouldBe` once

    it "cannot be reached through a view of the database, nor can the copy of the mempool area" $
      funded $ \give _ coins new -> do
        give (Pay (spend 0 (take 2 coins)) 2000)
        isJust <$> knownMempoolCopy new `shouldReturn` True
        L.withView new (fmap isJust . knownMempoolCopy . L.viewReader) `shouldReturn` False
        views <- newViews new
        takeView views ()
        withViewAt views () (fmap isJust . knownMempoolCopy) `shouldReturn` Just False
        closeViews views

  describe "prepareBlock" $
    modifyMaxSuccess (const 50) $
      it "gives the hash of each transaction, the order of sortTxs, and the size and weight of the block" $
        property . forAll (choose (0, 1000000)) $ \seed -> ioProperty $ do
          let coins = [(OutPoint (TxHash (sha256 (encode (i :: Int)))) 0, 100000) | i <- [1 .. 20]]
              txs = unGen (payments' btcRegTest [script, p2pkh 1, hashLock] coins) (mkQCGen seed) 30
              block = Block btcRegTest.genesisHeader (coinbaseTx 1 : txs)
              stripped = (block :: Block) {txs = map (\tx -> (tx :: Tx) {witness = []}) block.txs}
          pb <- prepareBlock btcRegTest block
          map (.txid) pb.txs `shouldBe` map txHash block.txs
          map (.tx) pb.txs `shouldBe` block.txs
          [(i, p.tx) | (i, p) <- pb.ordered] `shouldBe` sortTxs block.txs
          pb.size `shouldBe` B.length (encode block)
          pb.weight `shouldBe` 3 * B.length (encode stripped) + B.length (encode block)

-- | What happened in a history, for the tables that QuickCheck prints: how
-- many blocks were imported with an empty mempool and with a mempool that
-- had something, and how many were refused; and what the blocks that met
-- a mempool had to do with it ('met').
labelled :: [Done] -> Property
labelled done =
  tabulate "operations" [d.kind <> (if d.taken then ", taken" else ", refused") | d <- done]
    . tabulate "blocks taken on a mempool that is not empty" (concatMap (.met) done)
    . tabulate "outputs remembered after an operation" [bucket d.remembered | d <- done]
    $ property True
  where
    bucket n
      | n == 0 = "none"
      | n <= 3 = "1 to 3"
      | otherwise = "4 or more" :: String

-- | What an operation of a history was.
data Done = Done
  { -- | the kind of thing that it was
    kind :: !String,
    -- | whether it was taken
    taken :: !Bool,
    -- | for a block that was taken on a mempool that had something: what
    -- it had to do with the mempool, one entry for each thing that holds
    met :: ![String],
    -- | how many outputs that mempool transactions spend the store that
    -- imports in stages remembered after it ('spentIsChain')
    remembered :: !Int
  }

-- | What a block that was taken had to do with a mempool that was not
-- empty, given the transactions of the mempool before it and those that
-- left the mempool with it. These are the blocks that the import in
-- stages looks up conflicts for, so the histories must have plenty of
-- each kind.
meeting :: [TxHash] -> Block -> [TxHash] -> [String]
meeting pool block removed =
  ["(every such block)"]
    <> ["every transaction is in the mempool" | not (null txs) && null new]
    <> ["has transactions that the mempool has not seen" | not (null new)]
    <> ["one of those spends an output of the block" | any (any inside . (.inputs)) new]
    <> ["one of those spends an output of one of those" | any (any unseenParent . (.inputs)) new]
    <> ["conflicts with mempool transactions" | any (`notElem` hashes) removed]
  where
    txs = drop 1 block.txs
    hashes = map txHash txs
    new = [tx | tx <- txs, txHash tx `notElem` pool]
    inside i = i.outpoint.hash `elem` hashes
    unseenParent i = i.outpoint.hash `elem` map txHash new

-------------------------------------------------------------------------------
-- Two databases and a shadow
-------------------------------------------------------------------------------

-- | Run an action with three new databases that have the start of a
-- history: a shadow of the layout of version 20, which is what 'plan' and
-- 'blockOn' read, one for the sequential import and one for the import in
-- stages, and with the history.
withThree :: Network -> (DatabaseReader -> DatabaseReader -> DatabaseReader -> History -> IO a) -> IO a
withThree net action =
  withSystemTempDirectory "haskoin-store-shadow-" $ \dirShadow ->
    withSystemTempDirectory "haskoin-store-sequential-" $ \dirOld ->
      withSystemTempDirectory "haskoin-store-staged-" $ \dirNew ->
        open20 net dirShadow $ \shadow ->
          openLayered net dirOld $ \old ->
            openLayered net dirNew $ \new -> do
              hist <- start mixedRules shadow
              void (start sequentialRules old)
              void (start layeredRules new)
              action shadow old new hist

-- | Run an action with a store that imports the sequential way and one
-- that imports in stages, after a block that pays four outputs to as
-- many addresses. The action is given a way to give an operation to both
-- ('sameStep'), a way to give them a block of some transactions, those
-- four outputs, and the database that imports in stages.
funded :: ((Op -> IO ()) -> ([Tx] -> IO ()) -> [OutPoint] -> DatabaseReader -> IO a) -> IO a
funded action =
  withThree btcRegTest $ \_ old new hist -> do
    let fund = Tx 2 [TxIn (coinOf hist) "" maxBound] [TxOut 5000 (p2pkh n) | n <- [1 .. 4]] [] 0
        give = sameStep old new
        mine txs = blockOn old txs >>= give
    mine [fund]
    action give mine [OutPoint (txHash fund) i | i <- [0 .. 3]] new

-- | Give an operation that must be taken to a store that imports the
-- sequential way and to one that imports in stages. The two must give
-- the same outcome and be left with the same keys, and the second must
-- remember only what its database has ('spentIsChain').
sameStep :: DatabaseReader -> DatabaseReader -> Op -> IO ()
sameStep old new op = do
  was <- full sequentialRules old op
  now <- full layeredRules new op
  (show op, now) `shouldBe` (show op, was)
  (show op, fst now) `shouldSatisfy` isRight . snd
  mine <- raw new
  theirs <- raw old
  (show op, mine) `shouldBe` (show op, theirs)
  copyIsArea (show op) new
  void (spentIsChain (show op) new)

-- | The outputs that a database remembers that mempool transactions
-- spend, in order: none if it has no copy of its mempool area.
keptBy :: DatabaseReader -> IO [OutPoint]
keptBy db = maybe [] (sort . HashMap.keys . spentOutputs) <$> knownMempoolCopy db

-- | A payment of 1,000 from some outputs, told from others by a number.
spend :: Word64 -> [OutPoint] -> Tx
spend k ps = Tx 2 [TxIn p "" maxBound | p <- ps] [TxOut (1000 + k) script] [] 0

-- | Run a batch, and give what it returned or why it failed, whatever the
-- failure, and how it changed the mempool.
batchOf :: DatabaseReader -> (Writer -> IO String) -> IO (Either String String, ([TxHash], [TxHash]))
batchOf db action =
  try (runWriterMempool db.net db quiet (\w -> action w >>= evaluate . force)) >>= \case
    Left (e :: SomeException) -> return (Left (show e), ([], []))
    Right (x, c) -> return (Right x, (sort c.added, sort c.removed))
  where
    quiet = Logger {level = LevelError, source = "", push = \_ -> return ()}

-- | Give an operation to a set of rules. Unlike 'apply', the result of a
-- block has the data of its transactions, in the order that the rules
-- give them.
full :: Rules -> DatabaseReader -> Op -> IO (Either String String, ([TxHash], [TxHash]))
full rules db op = batchOf db $ \w -> case op of
  Pay tx time -> show <$> rules.newMempoolTx w tx time
  Mine block node -> show <$> rules.importBlock w block node
  Delete h -> show <$> rules.deleteUnconfirmedTx w False h
  Revert h time -> show <$> rules.revertBlock w time h

-- | Every key and value of a database: of its default column family, then
-- of each of the others.
raw :: DatabaseReader -> IO [[(ByteString, ByteString)]]
raw db = do
  main <- withIter db.db (\iter -> iterFirst iter >> go iter)
  (main :) <$> mapM (dump db) (columnFamilies db.db)
  where
    go iter =
      iterEntry iter >>= \case
        Nothing -> return []
        Just e -> iterNext iter >> (e :) <$> go iter

-- | Give the same history to a store that imports blocks one transaction
-- after another and reads the database as it goes ('sequentialRules'), and
-- to one that imports them in stages ('layeredRules'). Each operation is
-- chosen from a third database, of the layout of version 20. Every
-- operation must have the same outcome in both, and leave the same keys
-- and values in every column family.
--
--
-- = Where the merge records are
--
-- The import in stages writes merge records for balances and spenders,
-- which RocksDB combines with the values when they are read, and the
-- sequential import writes whole values. What is compared is what an
-- iterator returns, which is merged values: the two must be the same
-- bytes. A merge is not done the same way everywhere, though: in a
-- memtable, across several table files, or by a compaction. So the
-- memtables of the store that imports in stages are flushed after every
-- fifth operation, which leaves the merge records of a few blocks in files
-- of their own, with the reverts that put whole values among them, and
-- lets RocksDB compact them when it likes. At the end everything is
-- compacted, and compared once more.
--
-- = The copy of the mempool area
--
-- The store that imports in stages reads its mempool area from a copy in
-- memory ("Haskoin.Store.Database.Copy"). After every operation the copy
-- must be what a scan of the two column families gives ('copyIsArea'). The
-- sequential store runs every rule through 'withoutReadCache', which reads
-- RocksDB: it must never have a copy, or it would not be an independent
-- reference.
--
-- With the copy, the store that imports in stages remembers the unspent
-- outputs that mempool transactions spend, and its blocks take them from
-- there instead of reading them. After every operation each output that
-- it remembers must be what its database has, and have a mempool spender
-- ('spentIsChain'). The sequential store reads every output from RocksDB.
--
-- When the history is over, the last argument is run with the three
-- databases and the history, before the last comparison: for what a test
-- wants to try on stores that have gone through a history ('unminable').
--
-- Gives what each operation was ('Done').
sameStages ::
  Network ->
  [DatabaseReader -> History -> IO (History, Maybe Op)] ->
  (DatabaseReader -> DatabaseReader -> DatabaseReader -> History -> IO ()) ->
  IO [Done]
sameStages net choices ending =
  withThree net $ \shadow old new hist0 -> do
    (done, hist) <- foldM (next shadow old new) ([], hist0) (zip [1 :: Int ..] choices)
    ending shadow old new hist
    forM_ (columnFamilies new.db) $ \cf -> compactRange new.db (Just cf) Nothing Nothing
    mine <- raw new
    theirs <- raw old
    ("compacted", mine) `shouldBe` ("compacted", theirs)
    return (reverse done)
  where
    next shadow old new (done, hist) (count, chosen) =
      chosen shadow hist >>= \case
        (hist', Nothing) -> return (done, hist')
        (hist', Just op) -> do
          pool <- map snd <$> Q.getMempool shadow
          planned <- full mixedRules shadow op
          was <- full sequentialRules old op
          now <- full layeredRules new op
          (show op, now) `shouldBe` (show op, was)
          when (count `mod` 5 == 0) $
            forM_ (columnFamilies new.db) (flush new.db . Just)
          mine <- raw new
          theirs <- raw old
          (show op, mine) `shouldBe` (show op, theirs)
          -- the store that imports in stages answers its reads of the
          -- mempool area from a copy in memory, which must be the area:
          -- after an operation that was taken, and after one that was
          -- refused, which must have left it alone
          copyIsArea (show op) new
          -- what it remembers of the outputs that mempool transactions
          -- spend must be what the chain has for each of them
          remembered <- spentIsChain (show op) new
          -- and the sequential one has no copy to read: it is held to
          -- what RocksDB has
          sequentialCopy <- knownMempoolCopy old
          (show op, isJust sequentialCopy) `shouldBe` (show op, False)
          -- the shadow must go the same way, or the rest of the history
          -- is planned for a chain that the two do not have
          (show op, isRight (fst planned)) `shouldBe` (show op, isRight (fst was))
          let taken = isRight (fst was)
              kind = case op of
                Mine _ _ | null pool -> "block, mempool empty"
                Mine _ _ -> "block, mempool not empty"
                Pay _ _ -> "transaction"
                Delete _ -> "deletion"
                Revert _ _ -> "reversal"
              met = case op of
                Mine block _ | taken && not (null pool) -> meeting pool block (snd (snd now))
                _ -> []
              -- 'record' only looks at whether it was taken
              outcome = Outcome {result = either (const (Left Orphan)) Right (fst planned), added = [], removed = []}
          return (Done {kind, taken, met, remembered} : done, record shadow hist' op outcome)

-- | Nothing more to do when a history is over.
unchecked :: DatabaseReader -> DatabaseReader -> DatabaseReader -> History -> IO ()
unchecked _ _ _ _ = return ()

-------------------------------------------------------------------------------
-- Blocks that could not be mined
-------------------------------------------------------------------------------

-- | A block whose one payment spends the coinbase of the best block: the
-- last operation of a history that 'unminable' follows, so that the chain
-- has an output that a block has spent, and a coinbase that nothing spends.
spendCoin :: DatabaseReader -> History -> IO (History, Maybe Op)
spendCoin db hist = (hist,) . Just <$> blockOn db [paid]
  where
    paid = Tx 2 [TxIn (coinOf hist) "" maxBound] [TxOut 3000 (p2pkh 1), TxOut 4000 (p2pkh 2)] [] 0

-- | What the two imports do with a block that breaks what the import in
-- stages takes for granted of a mined block: that every output it spends
-- is unspent in the chain or made by the block, and that none is spent
-- twice. The stores have gone through a history that ends with
-- 'spendCoin', and each block has, around the transaction that is wrong,
-- transactions that could be mined ('unseenTxs'): some conflict with the
-- mempool, which gives way in the batch before the batch stops.
--
-- The import in stages does not check a block. All it promises of one
-- like these is that it is written whole or not at all. Each of these is
-- still refused, because what the import needs to read is not there, and
-- nothing is written: every key of every column family is as it was, and
-- so is the copy of the mempool area.
--
-- The reason that it gives is not always the one that the sequential
-- import gives, which looks in the chain for who spent an output.
unminable :: Int -> DatabaseReader -> DatabaseReader -> DatabaseReader -> History -> IO ()
unminable seed shadow old new hist = do
  spent <- case hist.chain of
    (Block {txs = [_, paid]}, _) : _ | [i] <- paid.inputs -> return i.outpoint
    _ -> fail "the history does not end with the block of spendCoin"
  (coins, taken, coinbases) <- ingredients shadow hist
  let coin = coinOf hist
      -- the coinbase of the best block is kept for the transactions that
      -- are wrong
      others = filter ((/= coin) . fst) coins
      valid = unGen (unseenTxs shadow.net hist.scripts others taken coinbases) (mkQCGen seed) 30
      parent = spend 0 [coin]
      orphan = Left "Orphan"
      stopped = Left "Could not find UTXO to spend"
      blocks =
        [ ("an input that does not exist", [spend 1 [unknown]], orphan, orphan),
          ("an input that a block has spent", [spend 1 [spent]], orphan, Left "Transaction confirmed"),
          ("an output of the chain spent by two transactions", [spend 1 [coin], spend 2 [coin]], orphan, orphan),
          ("an output of the block spent by two transactions", [parent, child 1, child 2], orphan, orphan),
          ("an output spent twice by one transaction", [spend 1 [coin, coin]], stopped, stopped)
        ]
      unknown = OutPoint (TxHash (sha256 (encode seed))) 0
      child k = spend k [OutPoint (txHash parent) 0]
  forM_ (zip [0 :: Int ..] blocks) $ \(k, (what, wrong, staged, reference)) -> do
    -- the wrong ones among the others, at a place that the seed chooses
    let (front, back) = splitAt ((seed + k) `mod` (length valid + 1)) valid
    op <- blockOn shadow (front <> wrong <> back)
    forM_ [(old, sequentialRules, reference), (new, layeredRules, staged)] $ \(db, rules, expected) -> do
      was <- raw db
      outcome <- fst <$> full rules db op
      -- the reason, without the output that it names
      (what, either (Left . takeWhile (/= ':')) Right outcome) `shouldBe` (what, expected)
      left <- raw db
      (what, left) `shouldBe` (what, was)
    copyIsArea what new
    void (spentIsChain what new)

-------------------------------------------------------------------------------
-- Blocks that leave the chain and come back
-------------------------------------------------------------------------------

-- | Histories in which the same blocks are reverted and imported again,
-- several times over. Three operations in ten revert the best block and
-- remember it, three import the block that was reverted last, again, and
-- the others are those of 'unseen': mostly new blocks, after which the
-- blocks that were reverted are forgotten, as they no longer fit.
--
-- This is where merge records meet whole values. A block that is imported
-- writes merge records for its balances and spenders. Its revert reads
-- them, merged, and writes whole values or deletes the keys. The block
-- comes back with merge records over those, and its transactions come out
-- of the mempool, where the revert put them.
--
-- No block of these histories has an earlier coinbase again, which those
-- of 'unseen' have now and then. The layered rules, sequential or not,
-- move such a coinbase to the later block, and take it out of the chain
-- when that block is reverted: the earlier block then cannot be reverted,
-- for want of its coinbase, while the rules of version 20, which the
-- history is planned from, revert it. With this many reverts the two would
-- part ways.
churn :: IORef [Op] -> Int -> DatabaseReader -> History -> IO (History, Maybe Op)
churn undone seed db hist = do
  reverted <- readIORef undone
  case (seed `mod` 10, hist.chain, reverted) of
    (k, (b, n) : _ : _, _) | k < 3 -> do
      writeIORef undone (Mine b n : reverted)
      return (hist, Just (Revert (headerHash b.header) hist.clock))
    (k, _, again : rest) | k >= 3 && k < 6 -> do
      writeIORef undone rest
      return (hist, Just again)
    _ -> do
      chosen <- unseen (seed `div` 10) db hist
      case snd chosen of
        Just (Mine b _)
          | any (any ((== nullOutPoint) . (.outpoint)) . (.inputs)) (drop 1 b.txs) -> return (fst chosen, Nothing)
          | otherwise -> writeIORef undone [] >> return chosen
        _ -> return chosen

-------------------------------------------------------------------------------
-- Blocks that the mempool has not seen
-------------------------------------------------------------------------------

-- | One operation in four is any of those of 'plan', so that the mempool
-- has something now and then. The others are blocks of transactions that
-- the mempool has not seen ('unseenTxs'), as a store that syncs or catches
-- up imports.
unseen :: Int -> DatabaseReader -> History -> IO (History, Maybe Op)
unseen seed db hist
  | seed `mod` 4 == 0 = plan db hist (fromIntegral (seed `div` 4))
  | otherwise = do
      (coins, taken, coinbases) <- ingredients db hist
      let txs = unGen (unseenTxs db.net hist.scripts coins taken coinbases) (mkQCGen seed) 30
      (hist,) . Just <$> blockOn db txs

-- | What 'unseenTxs' makes the transactions of a block from, read from a
-- database of the layout of version 20 that has gone through a history:
-- the outputs of blocks that nothing spends, with their values; those
-- that mempool transactions spend; and the earlier coinbases whose output
-- is still there.
ingredients :: DatabaseReader -> History -> IO ([(OutPoint, Word64)], [(OutPoint, Word64)], [Tx])
ingredients db hist = do
  listed <- concat <$> mapM (\a -> Q.getAddressUnspents db a def) (addressesOf db.net hist)
  others <- catMaybes <$> mapM (Q.getUnspent db) hist.loose
  -- outputs of blocks that mempool transactions spend: a block that
  -- spends one conflicts with the mempool
  pool <- catMaybes <$> (Q.getMempool db >>= mapM (Q.getTxData db . snd) . take 4)
  taken <- fmap concat . forM pool $ \td ->
    fmap catMaybes . forM (zip [0 ..] td.tx.inputs) $ \(i, input) ->
      Q.getTxData db input.outpoint.hash >>= \case
        Just parent
          | confirmed parent.block,
            Just p <- IntMap.lookup i td.prevs ->
              return (Just (input.outpoint, p.value))
        _ -> return Nothing
  let coins = [(u.outpoint, u.value) | u <- listed <> others, confirmed u.block]
      -- earlier coinbases whose output is still there. One whose output
      -- is spent can be confirmed again too, but then the block cannot
      -- be reverted under the layered rules, sequential or not, and can
      -- under those of version 20: the shadow would go another way
      coinbases =
        [ tx
        | (b, _) <- drop 1 hist.chain,
          tx <- take 1 b.txs,
          OutPoint (txHash tx) 0 `elem` map fst coins
        ]
  return (coins, taken, coinbases)

-- | Transactions for a block that could be mined: payments from outputs of
-- blocks and from each other, in an order in which each comes after those
-- that it spends or, for one block in three, in any order, as on Bitcoin
-- Cash. One block in four has transactions that spend what mempool
-- transactions spend, when there are any. And now and then a block has an
-- earlier coinbase again, as two blocks of 2010 had: that is not something
-- to refuse, but data that both imports must keep the same way.
--
-- What no mined block has is not here: an input that does not exist or is
-- spent, an output spent twice. The import in stages does not check a
-- block for them, and 'unminable' says what it does with one.
unseenTxs :: Network -> [ByteString] -> [(OutPoint, Word64)] -> [(OutPoint, Word64)] -> [Tx] -> Gen [Tx]
unseenTxs net scripts coins taken coinbases = do
  rivals <- frequency [(3, pure []), (1, sublistOf taken)]
  txs <- payments' net scripts (coins <> rivals)
  again <- frequency [(9, pure []), (1, if null coinbases then pure [] else pure <$> elements coinbases)]
  order <- frequency [(2, pure pure), (1, pure shuffle)]
  order (txs <> again)

-- | Up to a dozen payments from some coins and from each other, each after
-- those that it spends, with one to three inputs and one to three outputs.
-- Some have a witness, on a network that has them.
payments' :: Network -> [ByteString] -> [(OutPoint, Word64)] -> Gen [Tx]
payments' net scripts coins0 = choose (0, 12 :: Int) >>= \count -> go count coins0 []
  where
    go 0 _ acc = return (reverse acc)
    go _ [] acc = return (reverse acc)
    go n coins acc = do
      k <- choose (1, min 3 (length coins))
      picked <- take k <$> shuffle coins
      outs <- choose (1, 3 :: Int)
      targets <- vectorOf outs (elements scripts)
      signed <- (net.segWit &&) <$> arbitrary
      let total = sum (map snd picked)
          share = (total - 500) `div` fromIntegral outs
          witness = if signed then [[B.replicate 9 (fromIntegral n), "\x02"] | _ <- picked] else []
          tx = Tx 2 [TxIn p "" maxBound | (p, _) <- picked] [TxOut share s | s <- targets] witness 0
          made = [(OutPoint (txHash tx) i, share) | i <- take outs [0 ..]]
          left = coins \\ picked
      if total < 500 + 600 * fromIntegral outs
        then go (n - 1) left acc
        else go (n - 1) (left <> made) (tx : acc)
