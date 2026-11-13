{-# LANGUAGE OverloadedStrings #-}

module Haskoin.Store.Web.PiecesSpec (spec) where

import Control.Exception (ErrorCall (..), throwIO, try)
import Data.Aeson (encode, toEncoding)
import Data.ByteString.Builder (Builder, toLazyByteString)
import Data.ByteString.Lazy qualified as L
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Haskoin.Store.Web.Pieces
import Network.Wai.Internal (Response (..))
import Test.Hspec hiding (around)

spec :: Spec
spec = describe "a response in pieces" $ do
  it "has the bytes of its pieces, in order" $ do
    answer [] `shouldReturn` ("", True)
    answer [return "a"] `shouldReturn` ("a", True)
    answer (map return ["a", "", "bc", "d"]) `shouldReturn` ("abcd", True)
  it "has the bytes of a list encoded at once, for pieces of its items" $ do
    let lists = [[], [[]], [[1]], [[1, 2], [3]], [[], [1], [], [2, 3], []], [[1 .. 100], [101 .. 150]]]
        listed :: [[Int]] -> IO (L.ByteString, Bool)
        listed l = listPieces toEncoding (map return l) >>= answer
    mapM_ (\l -> listed l `shouldReturn` (encode (concat l), True)) lists
  it "cuts keys into pieces of the size, and the last of what is left" $ do
    read' <- newIORef []
    let fetch ks = modifyIORef' read' (<> [ks]) >> return ks
    pieces <- listPieces toEncoding (pieced 3 fetch [1 .. 8 :: Int])
    answer pieces `shouldReturn` (encode [1 .. 8 :: Int], True)
    readIORef read' `shouldReturn` [[1, 2, 3], [4, 5, 6], [7, 8]]
  it "puts bytes around pieces without a piece more" $ do
    let wrapped ps = around "<" ">" (map return ps)
    let cases = [[], ["a"], ["a", "b"], ["a", "b", "c"]]
    mapM (fmap (toLazyByteString . mconcat) . sequence . wrapped) cases
      `shouldReturn` ["<>", "<a>", "<ab>", "<abc>"]
    map (length . wrapped) cases `shouldBe` [1, 1, 2, 3]
  it "throws what the first piece throws, before there is a response" $ do
    ended <- newIORef False
    r <- try (piecesResponse [] (mark ended) [broken, return "b"])
    either (\(ErrorCall e) -> e) (const "") r `shouldBe` "gone"
  it "ends short, and says that it ended, when a later piece throws" $ do
    ended <- newIORef False
    res <- piecesResponse [] (mark ended) [return "[1", broken]
    written <- newIORef mempty
    r <- try (body res (\b -> modifyIORef' written (<> b)) (return ()))
    either (\(ErrorCall e) -> e) (const "") r `shouldBe` "gone"
    (toLazyByteString <$> readIORef written) `shouldReturn` "[1"
    readIORef ended `shouldReturn` True
  it "reads one piece ahead of the one that it writes, and no further" $ do
    events <- newIORef []
    let note e = modifyIORef' events (<> [e])
        piece n = note ("read " <> show n) >> return mempty
    res <- piecesResponse [] (return ()) (map piece [1 .. 4 :: Int])
    body res (const (return ())) (note "flush")
    -- the next piece is read while one is written, so which of the two
    -- comes first is not fixed; what is fixed is that by the flush of a
    -- piece no piece past the next one has been asked for
    seen <- readIORef events
    filter (/= "flush") seen `shouldBe` ["read 1", "read 2", "read 3", "read 4"]
    let readsBy k = length (filter (/= "flush") (before k seen))
        before k = map fst . takeWhile ((< k) . snd) . (zip <*> flushes)
        flushes = scanl (\n e -> if e == "flush" then n + 1 else n) (0 :: Int)
    map readsBy [1, 2, 3] `shouldSatisfy` and . zipWith (>=) [2, 3, 4]
  where
    broken :: IO Builder
    broken = throwIO (ErrorCall "gone")
    mark :: IORef Bool -> IO ()
    mark ended = modifyIORef' ended (const True)
    body (ResponseStream _ _ run) = run
    body _ = \_ _ -> expectationFailure "not a stream"
    -- the bytes of the response, and whether it said that it ended
    answer :: [IO Builder] -> IO (L.ByteString, Bool)
    answer pieces = do
      ended <- newIORef False
      res <- piecesResponse [] (mark ended) pieces
      written <- newIORef mempty
      body res (\b -> modifyIORef' written (<> b)) (return ())
      (,) . toLazyByteString <$> readIORef written <*> readIORef ended
