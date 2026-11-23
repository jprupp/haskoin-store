-- Every metric that the dashboard reads, sent through the store's own
-- StatsD library as a store started with --statsd-aggregates
-- --statsd-no-events sends them: statistics only, once a second.
--
--   Sender METRICS PORT SECONDS
--
-- METRICS is a file with a kind and a key on each line (sample.py). The
-- load is known, so that check.py can hold what the dashboard shows to
-- what was sent. A change of the load here is a change of the figures
-- there.
import Control.Concurrent (threadDelay)
import Control.Monad (forM, forM_, when)
import System.Environment (getArgs)
import System.Metrics.StatsD

main :: IO ()
main = do
  [file, port, duration] <- getArgs
  metrics <- map words . lines <$> readFile file
  let seconds = read duration :: Int
  withStats
    defStatConfig
      { statsdServer = "127.0.0.1",
        statsdPort = read port,
        namespace = "haskoin_store.btc",
        reportSamples = False,
        reportStats = True
      }
    $ \s -> do
      counters <- forM (zip [0 :: Int ..] [k | ["counter", k] <- metrics]) $ \(i, k) ->
        (,) (perSecond i k) <$> newStatCounter s k 1
      gauges <- forM (zip [0 :: Int ..] [k | ["gauge", k] <- metrics]) $ \(i, k) ->
        (,,) k (100 + i) <$> newStatGauge s k 0
      timings <- forM [k | ["timing", k] <- metrics] $ \k -> (,) k <$> newStatTiming s k 1
      -- twenty ticks a second
      forM_ [0 .. seconds * 20 - 1] $ \tick -> do
        let second = tick `div` 20
            within = tick `mod` 20
        forM_ counters $ \(n, c) -> when (within < n) (incrementCounter c 1)
        when (within == 0) . forM_ gauges $ \(k, v, g) -> setGauge g $ case k of
          "store.blocks" -> 978000 + second `div` 60
          "store.headers" -> 978001 + second `div` 60
          "web.cap.overloaded" -> if even (second `div` 30) then 0 else 1
          "db.write_delayed" -> 0
          _ -> v
        forM_ timings $ \(k, t) -> case k of
          -- twenty a second, of 10 to 200 ms: a mean of 105, a longest of
          -- 200, and 190 below which 95 in a hundred fall
          "web.address_balance" -> addTiming t (10 * (within + 1))
          -- one of a second in even seconds, nine of 100 ms in odd ones:
          -- a mean of 190, where the mean of the means of the seconds is
          -- 550
          "web.xpub_balances" -> do
            when (even second && within == 0) (addTiming t 1000)
            when (odd second && within < 9) (addTiming t 100)
          -- five a second of 50 ms
          _ -> when (within `mod` 4 == 0) (addTiming t 50)
        threadDelay 50000
  where
    -- how many a second a counter counts
    perSecond :: Int -> String -> Int
    perSecond i k = case k of
      "fleet.cache.hits" -> 3
      "fleet.cache.misses" -> 1
      "web.server_errors" -> 2
      _ -> i `mod` 7 + 1
