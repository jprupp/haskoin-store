-- The model checker of Haskoin.Store.Ingest as a program of its own, for
-- scripts/check-ingest.sh: it goes through the worlds of
-- Haskoin.Store.Ingest.Model, all of them or those numbered on the command
-- line, and says how many states each has, or what went wrong and after
-- which steps.
import Control.Monad (forM_)
import Haskoin.Store.Ingest.Model
import System.Environment (getArgs)
import System.IO

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  args <- getArgs
  let chosen = case args of
        [] -> zip [0 :: Int ..] scenarios
        ns -> [(i, sc) | (i, sc) <- zip [0 ..] scenarios, show i `elem` ns]
  forM_ chosen $ \(i, sc) -> do
    putStrLn (show i <> ". " <> sc.name)
    putStrLn ("   " <> describeResult (explore sc))
