-- | What the move of the tip over a block costs to decide, without Redis.
--
-- > haskoin-store-bench decide [TXS [POOL [INPUTS [OUTPUTS [RIVALS]]]]]
--
-- A move of the tip is one decision ("Haskoin.Store.Fleet.Pool.Decide",
-- @advance@) that takes the transactions of a block out of the shared
-- mempool. Transactions that are offered wait for as long as it takes.
-- On a server it took thirteen to eighteen seconds for a block of three
-- to four thousand transactions, and the store said where the time went:
-- nine tenths of it deciding, in Haskell, and less than a tenth in Redis
-- and in the database together. This times the deciding alone, so that
-- what would shorten it can be tried and measured here, and not on a
-- server.
--
-- It fills the model of Redis ("Haskoin.Store.Fleet.Atomic", @Db@) with a
-- pool of POOL transactions (20,000) by the decision that offers one, and
-- then decides, and does not carry out, the move over a block that
-- confirms TXS of them (4,000), three times over, and the same over a
-- half and a quarter of them, to show how the cost grows. The model
-- answers from maps in memory, so what is timed is the decision, gone
-- through once for each time that it has questions to ask, as against a
-- Redis.
--
-- A transaction has INPUTS inputs (3) and OUTPUTS outputs (3), each at an
-- address of its own. One in five spends an output of the one before it,
-- which the block confirms too, and one in ten has a child that stays in
-- the pool, as blocks of Bitcoin have.
--
-- A block also confirms RIVALS transactions (0) that the pool does not
-- have, each spending what a transaction of the pool spends: that one
-- leaves with its child, and is kept as replaced. A node drops such a
-- transaction when the block comes, and a store that had it takes it out
-- in the move.
--
-- For each run it prints how long each pass over the decision took and
-- how many questions it ended with, the answers that the decision depends
-- on and its writes, and, with @+RTS -T@, how much it allocated and how
-- long the garbage collector ran. The time for each question or write of
-- a pass is the number to compare between two builds.
--
-- Run it with @+RTS -T -A64m@, as a store runs. With the allocation area
-- that GHC gives by default the collector runs three hundred times in a
-- move of 4,000 transactions, and each time goes over the model of Redis,
-- which is in the memory of this program and not of a store: the move
-- took 2.1 seconds then and 0.8 with it, on the same machine.
module Decide (decideBench) where

import Control.Exception (evaluate)
import Control.Monad (forM_, unless, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as C
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Serialize (encode)
import Data.Word (Word32, Word64)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Stats (RTSStats (..), getRTSStats, getRTSStatsEnabled)
import Haskoin.Store.Fleet.Atomic
  ( Atomic,
    Backend (..),
    Db,
    Op (..),
    Outcome (..),
    answer,
    emptyDb,
    perform,
    runAtomic,
    runPure,
    tidy,
  )
import Haskoin.Store.Fleet.Pool.Decide qualified as D
import Haskoin.Store.Fleet.Pool.Types
import Text.Printf (printf)

decideBench :: [String] -> IO ()
decideBench args = do
  let numbers = map read args <> drop (length args) [4000, 20000, 3, 3, 0] :: [Int]
      (txs, pool, ins, outs, rivals) = case numbers of
        a : b : c : d : e : _ -> (a, max a b, c, d, e)
        _ -> error "usage: haskoin-store-bench decide [TXS [POOL [INPUTS [OUTPUTS [RIVALS]]]]]"
      shape = Shape {ins, outs, rivals}
  printf "A pool of %d transactions of %d inputs and %d outputs, and a block of %d of them\n" pool ins outs txs
  when (rivals > 0) $
    printf "The block also confirms %d transactions that each spend what one of the pool spends\n" rivals
  began <- getMonotonicTimeNSec
  db <- evaluate (filled shape pool)
  -- all of it, and not what the first questions happen to look at
  _ <- evaluate (length (show db))
  ended <- getMonotonicTimeNSec
  printf "The pool was filled in %d ms\n\n" (millis (ended - began))
  forM_ [txs `div` 4, txs `div` 2, txs] $ \n -> do
    printf "A block of %d transactions:\n" n
    forM_ [1 .. 3 :: Int] $ \_ -> decided shape pool db n
    putStrLn ""

-- | How many inputs and outputs a transaction has, and how many
-- transactions of the pool a block is in conflict with.
data Shape = Shape {ins :: !Int, outs :: !Int, rivals :: !Int}

-- | The number of a transaction of the pool that a block conflicts with:
-- the last ones of the pool, which no block here confirms, those that
-- have a child.
rival :: Int -> Int -> Int
rival pool i = 10 * ((pool `div` 10) - i)

keys :: D.Keys
keys = D.poolKeys "hs3:btc:" 32

now :: Integer
now = 1795000000000

target :: ByteString
target = B.replicate 32 0xbb

-- | Thirty-two bytes for a number: the id of a transaction.
txid :: Int -> ByteString
txid c = B.replicate 24 0xaa <> encode (fromIntegral c :: Word64)

-- | An output of a transaction.
output :: ByteString -> Int -> ByteString
output t i = t <> encode (fromIntegral i :: Word32)

-- | An address as a store names it: a byte for its kind and twenty more.
address :: Int -> Int -> Int -> ByteString
address side c i =
  B.singleton (fromIntegral side) <> B.take 20 (C.pack (show c <> ":" <> show i) <> B.replicate 20 0x2e)

-- | A transaction of the pool. Its first input is an output of the
-- chain, or the first output of the transaction before it for one in
-- five.
poolTx :: Shape -> Int -> PoolTx
poolTx shape c =
  PoolTx
    { txid = t,
      time = 1795000000 + fromIntegral c,
      inputs = map fst spent,
      body = B.replicate 400 7,
      entries =
        [PoolTouches a | (_, a) <- spent]
          <> [PoolTouches (address 1 c i) | i <- [0 .. shape.outs - 1]]
          <> [PoolSpends a o 1000 | (o, a) <- spent]
          <> [PoolPays (address 1 c i) (output t i) 1000 script | i <- [0 .. shape.outs - 1]]
    }
  where
    t = txid c
    script = B.replicate 25 0x76
    spent =
      [ if i == 0 && chained c
          then (output (txid (c - 1)) 0, address 1 (c - 1) 0)
          else (output (txid (1000000000 + c)) i, address 0 c i)
      | i <- [0 .. shape.ins - 1]
      ]

-- | Whether a transaction spends from the one before it.
chained :: Int -> Bool
chained c = c > 1 && c `mod` 5 == 0

-- | Whether each input of a transaction is unspent in the chain: all but
-- the one that spends from the pool.
unspent :: Shape -> Int -> [Bool]
unspent shape c = [not (i == 0 && chained c) | i <- [0 .. shape.ins - 1]]

-- | The child of a transaction, which no block confirms here.
child :: Int -> PoolTx
child c =
  PoolTx
    { txid = t,
      time = 1795000900,
      inputs = [o],
      body = B.replicate 250 7,
      entries =
        [ PoolTouches (address 1 c 1),
          PoolTouches (address 2 c 0),
          PoolSpends (address 1 c 1) o 1000,
          PoolPays (address 2 c 0) (output t 0) 900 (B.replicate 25 0x76)
        ]
    }
  where
    t = txid (2000000000 + c)
    o = output (txid c) 1

-- | The model of Redis with one live store that reported the block, and
-- a pool of so many transactions, one in ten with a child.
filled :: Shape -> Int -> Db
filled shape pool = foldl' offered start [1 .. pool]
  where
    start =
      foldl'
        (flip perform)
        emptyDb
        [ HSet "hs3:btc:fleet:lease" "a" (C.pack (show (now + 10000))),
          HSet "hs3:btc:fleet:recent" "a" target
        ]
    offered db c =
      let with = offer (unspent shape c) (poolTx shape c) db
       in if shape.outs > 1 && c `mod` 10 == 0 then offer [False] (child c) with else with
    offer :: [Bool] -> PoolTx -> Db -> Db
    offer oks tx db = case runPure now (D.offer keys Nothing oks [] tx) db of
      Right (Taken, after) -> after
      Right (other, _) -> error ("A transaction was not taken: " <> show other)
      Left why -> error (show why)

-- | Decide the move over the first so many transactions of the pool, and
-- say what it cost.
decided :: Shape -> Int -> Db -> Int -> IO ()
decided shape pool db n = do
  marks <- newIORef []
  let fetch qs = do
        from <- getMonotonicTimeNSec
        answers <- evaluate (map (answer db) qs)
        -- every answer, all of it, before the clock is read again
        unless (and (zipWith (==) answers answers)) (error "An answer differs from itself")
        to <- getMonotonicTimeNSec
        answers <$ modifyIORef' marks ((from, to, length qs) :)
      -- the transactions of the block that the pool never had, each
      -- with the output that it spends and that one of the pool spends
      strangers =
        [ (txid (3000000000 + i), o)
        | i <- [1 .. shape.rivals],
          let r = rival pool i,
          r > n,
          o <- take 1 (poolTx shape r).inputs
        ]
      confirmed = map txid [1 .. n] <> map fst strangers
      change =
        PoolChange
          { returned = [],
            gone = [],
            confirmed,
            spent =
              [ o <> tx.txid <> encode (978000 :: Word32)
              | c <- [1 .. n],
                let tx = poolTx shape c,
                o <- tx.inputs
              ]
                <> [o <> t <> encode (978000 :: Word32) | (t, o) <- strangers],
            height = Just 978000,
            lost = []
          }
      move :: (Monad m) => Atomic m Moved
      move = D.advance keys Nothing target (Just now) 32 (D.Changing change) [] ""
  before <- stats
  began <- getMonotonicTimeNSec
  outcome <- runAtomic Backend {fetch, time = return now} move
  (answers, written, kept) <- case outcome of
    Decided (MovedTo _) depends writes _ -> do
      a <- evaluate (length depends)
      w <- evaluate (length writes)
      k <- evaluate (length (tidy writes))
      return (a, w, k)
    Decided other _ _ _ -> error ("The tip did not move: " <> show other)
    Stuck why -> error (show why)
  ended <- getMonotonicTimeNSec
  after <- stats
  passes <- reverse <$> readIORef marks
  let -- a pass goes from the end of the answers before it to its own
      -- questions, and the last to the end
      starts = began : [to | (_, to, _) <- passes]
      stops = [from | (from, _, _) <- passes] <> [ended]
      asked = [qs | (_, _, qs) <- passes] <> [0]
      answering = sum [to - from | (from, to, _) <- passes]
      deciding = (ended - began) - answering
      steps = (length passes + 1) * (answers + written)
  printf
    "  %d ms deciding and %d answering, %d passes, %d answers, %d writes (%d sent)\n"
    (millis deciding)
    (millis answering)
    (length passes + 1)
    answers
    written
    kept
  printf
    "    for each transaction %.0f us, for each answer or write of a pass %.2f us\n"
    (micros deciding / fromIntegral n)
    (micros deciding / fromIntegral (max 1 steps))
  putStrLn . ("    the passes, in ms, with the questions that each ended with: " <>) . unwords $
    [show (millis (stop - start)) <> " (" <> show qs <> ")" | (start, stop, qs) <- zip3 starts stops asked]
  forM_ ((,) <$> before <*> after) $ \(b, a) ->
    printf
      "    allocated %d MB, %.0f bytes for each answer or write of a pass; the collector ran %d ms in %d collections\n"
      ((a.allocated_bytes - b.allocated_bytes) `div` 1000000)
      (fromIntegral (a.allocated_bytes - b.allocated_bytes) / fromIntegral (max 1 steps) :: Double)
      ((a.gc_elapsed_ns - b.gc_elapsed_ns) `div` 1000000)
      (a.gcs - b.gcs)
  when (answers == 0) (error "A move that asked nothing")
  where
    stats =
      getRTSStatsEnabled >>= \case
        True -> Just <$> getRTSStats
        False -> return Nothing

millis :: Word64 -> Word64
millis = (`div` 1000000)

micros :: Word64 -> Double
micros n = fromIntegral n / 1000
