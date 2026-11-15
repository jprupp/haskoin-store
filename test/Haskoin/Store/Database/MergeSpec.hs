-- | The merge operators of the database against what they are meant to
-- do, said in Haskell.
--
-- The operators are C functions that RocksDB calls on its own threads
-- ("Haskoin.Store.Database.Merge"). They are tested twice. First by
-- calling them directly, as RocksDB would, with random values and merge
-- records: the result must be the bytes of the Haskell fold of the same,
-- records combined beforehand must do what they did one by one, and input
-- that is not of the format must be refused. Then through a database:
-- merge records, puts and deletes written in any order, in memtables and
-- in table files, must read as the same fold, before and after a
-- compaction.
module Haskoin.Store.Database.MergeSpec (spec) where

import Control.Monad (forM_)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Default (def)
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Serialize (encode)
import Data.Word (Word32, Word64)
import Database.RocksDB
  ( FullMerge,
    MergeOperator (..),
    PartialMerge,
    columnFamilies,
    compactRange,
    deleteCF,
    flush,
    mergeCF,
    putCF,
  )
import Database.RocksDB.Query (retrieveCF)
import Foreign.C.String (CString)
import Foreign.C.Types (CInt (..), CSize (..), CUChar (..))
import Foreign.Marshal.Alloc (alloca, free)
import Foreign.Marshal.Array (withArray)
import Foreign.Marshal.Utils (withMany)
import Foreign.Ptr (FunPtr, Ptr, nullPtr)
import Foreign.Storable (peek)
import Haskoin
  ( Address,
    TxHash (..),
    btcRegTest,
    eitherToMaybe,
    scriptToAddressBS,
    sha256,
  )
import Haskoin.Store.Data (Spender (..))
import Haskoin.Store.Database.Merge (balanceOperator, spenderOperator)
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Reader (DatabaseReader (..), balanceCF, spenderCF)
import Haskoin.Store.Database.Types
  ( BalKey (..),
    BalVal (..),
    SpenderKey (..),
    SpenderVal (..),
    addBalVal,
    valToBalance,
  )
import Haskoin.Store.Histories (openLayered)
import Haskoin.Store.Integration.Temp (withSystemTempDirectory)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess, prop)
import Test.QuickCheck
  ( Gen,
    arbitrary,
    choose,
    elements,
    forAll,
    frequency,
    ioProperty,
    listOf,
    listOf1,
    oneof,
    vectorOf,
  )

spec :: Spec
spec = describe "the merge operators of the database" $ do
  describe "the formats that they read and write" $ do
    it "are those of the values: forty bytes for a balance, a count and forty bytes for each spender" $ do
      encode (BalVal 1 2 3 4 0x0102030405060708)
        `shouldBe` B.pack (concat [[0, 0, 0, 0, 0, 0, 0, n] | n <- [1 .. 4]] <> [1 .. 8])
      let spender = Spender (TxHash (sha256 ("spender" :: ByteString))) 0x0a0b0c0d
          two = SpenderVal (IntMap.fromList [(0x01020304, spender), (2, spender)])
      encode two
        `shouldBe` B.concat
          [ B.pack [2],
            B.pack [0, 0, 0, 2],
            encode (sha256 ("spender" :: ByteString)),
            B.pack [0x0a, 0x0b, 0x0c, 0x0d],
            B.pack [1, 2, 3, 4],
            encode (sha256 ("spender" :: ByteString)),
            B.pack [0x0a, 0x0b, 0x0c, 0x0d]
          ]
      B.length (encode two) `shouldBe` 1 + 2 * 40

  describe "of balances" $ do
    prop "adds the differences to the balance, each number modulo 2^64, from zero if there is none" $
      forAll ((,) <$> oneof [pure Nothing, Just <$> genBalVal] <*> listOf genBalVal) $ \(existing, records) ->
        ioProperty $
          fullOf balanceOperator (encode <$> existing) (map encode records)
            `shouldReturn` Just (encode (foldl addBalVal (fromMaybe def existing) records))
    prop "makes one record of several, which does what they did" $
      forAll ((,) <$> oneof [pure Nothing, Just <$> genBalVal] <*> listOf1 (listOf1 genBalVal)) $
        \(existing, parts) -> ioProperty $ do
          combined <- mapM (partialOf balanceOperator . map encode) parts
          map (fmap B.length) combined `shouldBe` map (const (Just 40)) parts
          combined `shouldBe` map (Just . encode . foldl1 addBalVal) parts
          one <- fullOf balanceOperator (encode <$> existing) (map encode (concat parts))
          fullOf balanceOperator (encode <$> existing) [r | Just r <- combined] `shouldReturn` one
    it "takes a number down by its two's complement" $ do
      let had = BalVal {confirmed = 5000, unconfirmed = 0, utxo = 2, txs = 7, received = 9000}
          spent = (def :: BalVal) {confirmed = negate 3000, utxo = negate 1, txs = 1}
      fullOf balanceOperator (Just (encode had)) [encode spent]
        `shouldReturn` Just (encode BalVal {confirmed = 2000, unconfirmed = 0, utxo = 1, txs = 8, received = 9000})
    it "refuses what is not forty bytes" $ do
      let good = encode (def :: BalVal)
      fullOf balanceOperator (Just good) [good] `shouldReturn` Just good
      fullOf balanceOperator (Just (B.init good)) [good] `shouldReturn` Nothing
      fullOf balanceOperator (Just good) [good, good <> "\0"] `shouldReturn` Nothing
      fullOf balanceOperator Nothing [""] `shouldReturn` Nothing
      fullOf balanceOperator (Just "") [good] `shouldReturn` Nothing
      partialOf balanceOperator [good, B.take 8 good] `shouldReturn` Nothing

  describe "of groups of spenders" $ do
    prop "joins the records with the group, the newest winning for an output, in the order of the outputs" $
      forAll ((,) <$> oneof [pure Nothing, Just <$> genGroup] <*> listOf genGroup) $ \(existing, records) ->
        ioProperty $
          fullOf spenderOperator (group <$> existing) (map group records)
            `shouldReturn` Just (group (joined (fromMaybe IntMap.empty existing) records))
    prop "makes one record of several, which does what they did" $
      forAll ((,) <$> oneof [pure Nothing, Just <$> genGroup] <*> listOf1 (listOf1 genGroup)) $
        \(existing, parts) -> ioProperty $ do
          combined <- mapM (partialOf spenderOperator . map group) parts
          combined `shouldBe` map (Just . group . joined IntMap.empty) parts
          one <- fullOf spenderOperator (group <$> existing) (map group (concat parts))
          fullOf spenderOperator (group <$> existing) [r | Just r <- combined] `shouldReturn` one
    it "refuses a group whose length is not that of its count" $ do
      let good = group (IntMap.fromList [(1, spenderOf 1), (2, spenderOf 2)])
      fullOf spenderOperator (Just good) [good] `shouldReturn` Just good
      fullOf spenderOperator (Just (B.init good)) [good] `shouldReturn` Nothing
      fullOf spenderOperator (Just good) [good <> "\0"] `shouldReturn` Nothing
      fullOf spenderOperator Nothing [""] `shouldReturn` Nothing
      fullOf spenderOperator Nothing [B.pack [3] <> B.drop 1 good] `shouldReturn` Nothing
      partialOf spenderOperator [good, B.take 41 good] `shouldReturn` Nothing
    it "takes a group with no entries for what it is" $
      fullOf spenderOperator (Just "\0") ["\0"] `shouldReturn` Just "\0"

  describe "in a database" $
    modifyMaxSuccess (const 25) $ do
      prop "give the balances that the same writes give when each is read and written whole" $
        forAll (listOf (genStep genBalVal)) $ \steps -> ioProperty . withDB $ \db -> do
          let cf = balanceCF db.db
              wanted = model addBalVal def steps
              agree = forM_ [0 .. keys - 1] $ \k -> do
                let a = address (fromIntegral k)
                Q.getBalance db a `shouldReturn` (valToBalance a <$> Map.lookup k wanted)
                retrieveCF db.db cf (BalKey a) `shouldReturn` Map.lookup k wanted
          forM_ steps $ \case
            Whole k v -> putCF db.db cf (encode (BalKey (address (fromIntegral k)))) (encode v)
            Gone k -> deleteCF db.db cf (encode (BalKey (address (fromIntegral k))))
            Change k v -> mergeCF db.db cf (encode (BalKey (address (fromIntegral k)))) (encode v)
            ToFile -> flush db.db (Just cf)
          agree
          flush db.db (Just cf)
          agree
          compactRange db.db (Just cf) Nothing Nothing
          agree
      prop "give the groups of spenders that the same writes give when each is read and written whole" $
        forAll (listOf (genStep genGroup)) $ \steps -> ioProperty . withDB $ \db -> do
          let cf = spenderCF db.db
              wanted = model (flip IntMap.union) IntMap.empty steps
              -- the groups of one transaction, sixteen outputs apart, and
              -- one of another
              key k = if k == 0 then SpenderKey (made 0) 7 else SpenderKey (made 1) (fromIntegral k)
              agree = do
                forM_ [0 .. keys - 1] $ \k ->
                  retrieveCF db.db cf (key k) `shouldReturn` (SpenderVal <$> Map.lookup k wanted)
                -- and as the queries read them, with a scan of the
                -- transaction
                Q.getChainSpenders db (made 0) `shouldReturn` fromMaybe IntMap.empty (Map.lookup 0 wanted)
                Q.getChainSpenders db (made 1)
                  `shouldReturn` IntMap.unions [m | (k, m) <- Map.toList wanted, k /= 0]
          forM_ steps $ \case
            Whole k v -> putCF db.db cf (encode (key k)) (group v)
            Gone k -> deleteCF db.db cf (encode (key k))
            Change k v -> mergeCF db.db cf (encode (key k)) (group v)
            ToFile -> flush db.db (Just cf)
          agree
          flush db.db (Just cf)
          agree
          compactRange db.db (Just cf) Nothing Nothing
          agree
      it "has the two operators on their column families, and the one that keeps older stores out" $
        withDB $
          \db -> length (columnFamilies db.db) `shouldBe` 11

-------------------------------------------------------------------------------
-- Calling the operators as RocksDB does
-------------------------------------------------------------------------------

foreign import ccall "dynamic" callFull :: FunPtr FullMerge -> FullMerge

foreign import ccall "dynamic" callPartial :: FunPtr PartialMerge -> PartialMerge

-- | The full merge of an operator: the value that a key has, if any, and
-- its merge records, oldest first. 'Nothing' if the function says that it
-- failed, which it must say as RocksDB's C API reads it: with no bytes.
fullOf :: MergeOperator -> Maybe ByteString -> [ByteString] -> IO (Maybe ByteString)
fullOf op existing records = case op of
  MergeFunctions {fullMerge = function} ->
    operands records $ \pointers lengths count success size -> do
      let call (value, n) =
            callFull function nullPtr nullPtr 0 value (fromIntegral n) pointers lengths count success size
      maybe (call (nullPtr, 0 :: Int)) (`B.useAsCStringLen` call) existing
  UInt64Add -> fail "not an operator of the store"

-- | The partial merge of an operator: merge records, oldest first.
partialOf :: MergeOperator -> [ByteString] -> IO (Maybe ByteString)
partialOf op records = case op of
  MergeFunctions {partialMerge = function} ->
    operands records $ \pointers lengths count success size ->
      callPartial function nullPtr nullPtr 0 pointers lengths count success size
  UInt64Add -> fail "not an operator of the store"

-- | Hand merge records to a function as RocksDB's C API does, and read
-- what it returns: memory from malloc, a length, and whether it worked.
operands ::
  [ByteString] ->
  (Ptr CString -> Ptr CSize -> CInt -> Ptr CUChar -> Ptr CSize -> IO CString) ->
  IO (Maybe ByteString)
operands records call =
  withMany B.useAsCStringLen records $ \rows ->
    withArray (map fst rows) $ \pointers ->
      withArray (map (fromIntegral . snd) rows :: [CSize]) $ \lengths ->
        alloca $ \success ->
          alloca $ \size -> do
            bytes <- call pointers lengths (fromIntegral (length rows)) success size
            ok <- peek success
            n <- peek size
            if ok == 0
              then do
                -- RocksDB copies this many bytes before it looks at the flag
                (bytes, n) `shouldBe` (nullPtr, 0)
                return Nothing
              else do
                result <- B.packCStringLen (bytes, fromIntegral n)
                free bytes
                return (Just result)

-------------------------------------------------------------------------------
-- What they are meant to do
-------------------------------------------------------------------------------

-- | A group with these records joined to it, oldest first: a newer entry
-- takes the place of an older one for the same output.
joined :: IntMap Spender -> [IntMap Spender] -> IntMap Spender
joined = foldl (flip IntMap.union)

-- | The bytes of a group of spenders.
group :: IntMap Spender -> ByteString
group = encode . SpenderVal

-- | A write to one of a few keys: a whole value, a delete, a merge record,
-- or a flush of the memtable to a table file.
data Step v
  = Whole Int v
  | Gone Int
  | Change Int v
  | ToFile
  deriving (Show)

-- | The keys that the steps write to.
keys :: Int
keys = 4

genStep :: Gen v -> Gen (Step v)
genStep value =
  frequency
    [ (2, Whole <$> key <*> value),
      (1, Gone <$> key),
      (6, Change <$> key <*> value),
      (2, pure ToFile)
    ]
  where
    key = choose (0, keys - 1)

-- | What each key holds after the steps, given how a merge record changes
-- a value and the value that a key without one counts from.
model :: (v -> v -> v) -> v -> [Step v] -> Map.Map Int v
model merge zero = foldl step Map.empty
  where
    step m (Whole k v) = Map.insert k v m
    step m (Gone k) = Map.delete k m
    step m (Change k v) = Map.insert k (merge (Map.findWithDefault zero k m) v) m
    step m ToFile = m

-- | Numbers of any size, so that sums wrap around, and small ones.
genBalVal :: Gen BalVal
genBalVal = BalVal <$> number <*> number <*> number <*> number <*> number
  where
    number = oneof [arbitrary, fromIntegral <$> choose (0, 3 :: Int), negate . fromIntegral <$> choose (1, 3 :: Int)]

-- | A few spenders, of outputs that other groups often have too.
genGroup :: Gen (IntMap Spender)
genGroup = do
  n <- choose (0, 5)
  IntMap.fromList <$> vectorOf n ((,) <$> output <*> (spenderOf <$> arbitrary))
  where
    -- most in the sixteen outputs of a group, and some whose number takes
    -- all four bytes
    output = frequency [(8, choose (0, 15)), (1, elements [255, 256, 65536, 16777216, 2000000000])]

spenderOf :: Word32 -> Spender
spenderOf n = Spender (TxHash (sha256 (encode n))) (n `mod` 7)

made :: Word64 -> TxHash
made n = TxHash (sha256 (encode n))

address :: Word32 -> Address
address i =
  fromMaybe (error "no address") . eitherToMaybe . scriptToAddressBS btcRegTest $
    "\x76\xa9\x14" <> B.take 20 (encode (sha256 (encode i))) <> "\x88\xac"

withDB :: (DatabaseReader -> IO a) -> IO a
withDB action = withSystemTempDirectory "haskoin-store-merge-" $ \dir -> openLayered btcRegTest dir action
