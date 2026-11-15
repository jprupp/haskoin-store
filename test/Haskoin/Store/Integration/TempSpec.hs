-- | The temporary directories of the tests: there for the action, and gone
-- after it.
module Haskoin.Store.Integration.TempSpec (spec) where

import Control.Exception (ErrorCall (..), throwIO, try)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.List (isPrefixOf)
import Haskoin.Store.Integration.Temp
import System.Directory (doesDirectoryExist, doesFileExist)
import System.FilePath (takeFileName, (</>))
import Test.Hspec

spec :: Spec
spec = describe "a temporary directory" $ do
  it "is there for the action, and removed with what is in it afterwards" $ do
    dir <- withSystemTempDirectory "haskoin-store-temp" $ \dir -> do
      takeFileName dir `shouldSatisfy` isPrefixOf "haskoin-store-temp"
      doesDirectoryExist dir `shouldReturn` True
      writeFile (dir </> "file") "kept until the end"
      doesFileExist (dir </> "file") `shouldReturn` True
      return dir
    doesDirectoryExist dir `shouldReturn` False

  it "is removed when the action fails, and made where it is asked" $
    withSystemTempDirectory "haskoin-store-temp" $ \parent -> do
      made <- newIORef ""
      r <- try . withTempDirectory parent "inner" $ \dir -> do
        writeIORef made dir
        writeFile (dir </> "file") "lost"
        throwIO (ErrorCall "failed") :: IO ()
      r `shouldBe` Left (ErrorCall "failed")
      dir <- readIORef made
      dir `shouldSatisfy` isPrefixOf (parent </> "inner")
      doesDirectoryExist dir `shouldReturn` False
