{-# LANGUAGE OverloadedStrings #-}

module Haskoin.Store.Web.PiecesSpec (spec) where

import Control.Concurrent (threadDelay)
import Control.Monad (forM_)
import Control.Exception (ErrorCall (..), throwIO, try)
import Data.Aeson (encode, toEncoding)
import Data.ByteString.Builder (Builder, toLazyByteString)
import Data.ByteString.Lazy qualified as L
import Data.List (sort)
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef)
import Haskoin.Store.Web.Pieces
import Network.Wai.Internal (Response (..))
import Test.Hspec hiding (around)

spec :: Spec
spec = describe "a response in pieces" $ do
  it "has the bytes of its pieces, in order" $ do
    answer [] `shouldReturn` ("", True)
    answer [plain "a"] `shouldReturn` ("a", True)
    answer (map plain ["a", "", "bc", "d"]) `shouldReturn` ("abcd", True)
  it "has the bytes of a list encoded at once, for pieces of its items" $ do
    let lists = [[], [[]], [[1]], [[1, 2], [3]], [[], [1], [], [2, 3], []], [[1 .. 100], [101 .. 150]]]
        listed :: [[Int]] -> IO (L.ByteString, Bool)
        listed l = listPieces toEncoding (map return l) >>= answer
    mapM_ (\l -> listed l `shouldReturn` (encode (concat l), True)) lists
  it "has the same bytes when its pieces take turns to finish" $ do
    -- later pieces finish first, so that what is settled in order, the
    -- commas, is settled after all of them are encoded
    let slow n items = threadDelay (1000 * (5 - n `mod` 5)) >> return items
        l = [[], [1, 2], [], [3], [4 .. 9], [], [10 :: Int]]
    pieces <- listPieces toEncoding (zipWith slow [0 ..] l)
    answer pieces `shouldReturn` (encode (concat l), True)
  it "cuts keys into pieces of the size, and the last of what is left" $ do
    read' <- newIORef []
    let fetch ks = atomicModifyIORef' read' (\r -> (ks : r, ())) >> return ks
    pieces <- listPieces toEncoding (pieced 3 fetch [1 .. 8 :: Int])
    answer pieces `shouldReturn` (encode [1 .. 8 :: Int], True)
    -- pieces are read at the same time, in no fixed order
    sort <$> readIORef read' `shouldReturn` [[1, 2, 3], [4, 5, 6], [7, 8]]
  it "puts bytes around pieces without a piece more" $ do
    let wrapped ps = around "<" ">" (map plain ps)
    let cases = [[], ["a"], ["a", "b"], ["a", "b", "c"]]
    mapM (fmap fst . answer . wrapped) cases `shouldReturn` ["<>", "<a>", "<ab>", "<abc>"]
    map (length . wrapped) cases `shouldBe` [1, 1, 2, 3]
  it "throws what the first piece throws, before there is a response" $ do
    ended <- newIORef False
    r <- try (piecesResponse 2 [] (mark ended) [piece broken, plain "b"])
    either (\(ErrorCall e) -> e) (const "") r `shouldBe` "gone"
  it "ends short, and says that it ended, when a later piece throws" $ do
    ended <- newIORef False
    res <- piecesResponse 2 [] (mark ended) [plain "[1", plain "2", piece broken, plain "3"]
    written <- newIORef mempty
    r <- try (body res (\b -> modifyIORef' written (<> b)) (return ()))
    either (\(ErrorCall e) -> e) (const "") r `shouldBe` "gone"
    (toLazyByteString <$> readIORef written) `shouldReturn` "[12"
    readIORef ended `shouldReturn` True
  it "reads as many pieces at once as it is told, and no more" $ do
    forM_ [1, 3] $ \n -> do
      now <- newIORef (0 :: Int)
      most <- newIORef 0
      let busy = do
            k <- atomicModifyIORef' now (\c -> (c + 1, c + 1))
            atomicModifyIORef' most (\m -> (max m k, ()))
            threadDelay 2000
            atomicModifyIORef' now (\c -> (c - 1, ()))
            return "x"
      res <- piecesResponse n [] (return ()) (map piece (replicate 10 busy))
      body res (const (return ())) (return ())
      readIORef most `shouldReturn` n
  it "reads no further ahead than it is told, of the piece that it writes" $ do
    events <- newIORef []
    let note e = atomicModifyIORef' events (\es -> (es <> [e], ()))
        read' n = note ("read " <> show n) >> return mempty
    res <- piecesResponse 2 [] (return ()) (map (piece . read') [1 .. 6 :: Int])
    body res (const (return ())) (note "flush")
    -- by the k-th flush, pieces up to k + 2 may have been read: the one
    -- written and the two after it
    seen <- readIORef events
    let readsBy k = length (filter (/= "flush") (before k seen))
        before k = map fst . takeWhile ((< k) . snd) . (zip <*> flushes)
        flushes = scanl (\c e -> if e == "flush" then c + 1 else c) (0 :: Int)
    map readsBy [1 .. 6] `shouldSatisfy` and . zipWith (>=) [3, 4, 5, 6, 6, 6]
  it "cancels the reads that are still running when a piece fails" $ do
    finished <- newIORef False
    let slow = threadDelay 200000 >> modifyIORef' finished (const True) >> return "late"
    res <- piecesResponse 3 [] (return ()) [plain "a", piece broken, piece slow]
    r <- try (body res (const (return ())) (return ()))
    either (\(ErrorCall e) -> e) (const "") r `shouldBe` "gone"
    threadDelay 300000
    readIORef finished `shouldReturn` False
  where
    broken :: IO Builder
    broken = throwIO (ErrorCall "gone")
    mark :: IORef Bool -> IO ()
    mark ended = modifyIORef' ended (const True)
    body (ResponseStream _ _ run) = run
    body _ = \_ _ -> expectationFailure "not a stream"
    -- the bytes of the response, and whether it said that it ended
    plain :: Builder -> Piece
    plain = piece . return
    answer :: [Piece] -> IO (L.ByteString, Bool)
    answer pieces = do
      ended <- newIORef False
      res <- piecesResponse 3 [] (mark ended) pieces
      written <- newIORef mempty
      body res (\b -> modifyIORef' written (<> b)) (return ())
      (,) . toLazyByteString <$> readIORef written <*> readIORef ended
