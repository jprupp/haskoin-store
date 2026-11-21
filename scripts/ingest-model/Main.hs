{-# LANGUAGE OverloadedRecordDot #-}

-- The model checker of Haskoin.Store.Ingest as a program of its own, for
-- scripts/check-ingest.sh: it goes through the worlds of
-- Haskoin.Store.Ingest.Model, all of them or those numbered on the command
-- line, and says how many states each has, or what went wrong and after
-- which steps.
import Control.Monad (forM, unless)
import Data.List (isInfixOf)
import Data.Maybe (isJust)
import Haskoin.Store.Ingest.Model
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.IO

-- The program ends with a failure when a world fails, or when the world
-- that must fail, for it breaks an assumption, passes: a script or a
-- person who looks only at how it ended is told the truth.
main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  args <- getArgs
  let chosen = case args of
        [] -> zip [0 :: Int ..] scenarios
        ns -> [(i, sc) | (i, sc) <- zip [0 ..] scenarios, show i `elem` ns]
  failed <- forM chosen $ \(i, sc) -> do
    let result = explore sc
    putStrLn (show i <> ". " <> sc.name)
    putStrLn ("   " <> describeResult result)
    return (isJust result.failure)
  -- the world that breaks A5, when every world is asked for
  unseen <-
    if null args
      then do
        putStrLn ("without A5. " <> withoutOrder.name)
        case (explore withoutOrder).failure of
          Just (what, _) | "goes round for ever" `isInfixOf` what -> do
            putStrLn "   fails as it must: the store goes round for ever"
            return False
          other -> do
            putStrLn ("   should go round for ever, and " <> maybe "passes" fst other)
            return True
      else return False
  let bad = length (filter id failed)
  unless (bad == 0 && not unseen) $ do
    putStrLn (show bad <> " of " <> show (length chosen) <> " worlds failed")
    exitFailure
  putStrLn ("All " <> show (length chosen) <> " worlds pass")
