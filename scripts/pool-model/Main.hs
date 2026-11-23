{-# LANGUAGE OverloadedRecordDot #-}

-- The checker of the pool after a move of the tip as a program of its
-- own, for scripts/check-pool.sh: it goes through the worlds of
-- Haskoin.Store.Fleet.LeavingModel, all of them or those numbered on the
-- command line, and says how many pools, blocks and states each has, or
-- what did not hold.
import Control.Monad (forM)
import Data.Maybe (isJust)
import Haskoin.Store.Fleet.LeavingModel
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.IO

-- The program ends with a failure when a world fails, or when the world
-- with a fault put in passes: a script or a person who looks only at how
-- it ended is told the truth.
main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  args <- getArgs
  let chosen = case args of
        [] -> zip [0 :: Int ..] worlds
        ns -> [(i, w) | (i, w) <- zip [0 ..] worlds, show i `elem` ns]
  failed <- forM chosen $ \(i, w) -> do
    let report = explore w
    putStrLn (show i <> ". " <> w.name)
    putStrLn ("   " <> describeReport report)
    return (isJust report.failure)
  -- the first world with what a block is in conflict with left unmarked,
  -- when every world is asked for
  unseen <-
    if null args
      then do
        let report = unmarked (head worlds)
        putStrLn "with a fault. what a block is in conflict with is not marked"
        case report.failure of
          Just why -> putStrLn ("   fails as it must: " <> why) >> return False
          Nothing -> putStrLn "   PASSES, and must not: the checker does not see the fault" >> return True
      else return False
  if or failed || unseen
    then exitFailure
    else putStrLn ("All " <> show (length chosen) <> " worlds hold")
