{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

-- |
-- Module      : Haskoin.Store.Web.Pieces
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- A response that is written while it is read.
--
-- A handler usually reads everything it answers with and then encodes it.
-- For a long list of large items, such as thousands of whole transactions,
-- that makes the client wait for the last item before it gets the first,
-- and makes the server hold the whole list for as long as the client takes
-- to read it. Here the list comes in pieces, each an action that reads some
-- of the items, and the response is written piece by piece.
--
-- = What it gives and what it does not
--
-- Measured on 5,000 transactions of one address, 15 MB of JSON: the first
-- byte left after 11 ms where it took 96 ms, and sixteen such requests at
-- once took 197 MB of memory where they took 1,014 MB. The whole response
-- took as long as before: most of that time goes into encoding JSON, which
-- reading in pieces does not change, but encoding them on several cores
-- does (see below). A short answer gains nothing, and should stay with the
-- routes that read first.
--
-- = Why it is safe
--
-- The order of things makes it so. All that can refuse the request, and the
-- first piece, come before the status line, so an error there is answered
-- as an error. A read that fails later can only cut the response short,
-- which a client sees as a response that does not end. So the pieces are
-- for what the database itself names, such as the transactions listed in a
-- block or under an address: those are there, and their reads are not
-- expected to fail. What the client names, such as a list of transaction
-- hashes, may not be there, and is read whole before anything is answered.
--
-- While a piece is written the next ones are read, so that neither the
-- database nor the client waits for the other. Nothing of the database is
-- held between pieces, so a slow client costs a few pieces of memory and no
-- more. The price is that the pieces are not read from one view of the
-- database: an item may change, or go, between two of them.
--
-- = Encoding on several cores
--
-- Most of the time of a long answer goes into encoding its JSON, and one
-- thread doing it for a whole answer leaves the other cores idle. So each
-- piece is encoded, down to its bytes, in the thread that reads it, and
-- several pieces are read and encoded at once, as many as 'piecesResponse'
-- is told. What has to be done in order, such as the comma between the
-- items of two pieces, which depends on whether the pieces before had any,
-- is left for the writer, and is cheap: a 'Piece' is an action in two
-- steps, the first one that may run beside the others, and the second one
-- that runs in order. A response holds at most that many pieces, and one
-- more being written.
--
-- Measured with @scripts/bench-web.sh@ on 5,000 transactions of one
-- address, eight cores, eight pieces at once: one request at a time took
-- 77 ms where it took 176 ms, for 16% more CPU time. With eight requests at
-- once, enough to keep every core busy without it, as many were answered,
-- 5% fewer. So it is for a server with cores to spare, which is the usual
-- case: it shortens the wait, and does not make more answers out of the
-- same cores.
--
-- = The alternative
--
-- Lazy input and output, a lazy 'Data.ByteString.Lazy.ByteString' whose
-- chunks read the database when the server asks for them, would do the same
-- with less code. It was rejected: an error would come out of pure code,
-- and nothing would say when a read happens or ends.
module Haskoin.Store.Web.Pieces
  ( -- * Responses
    piecesResponse,
    ahead,

    -- * Pieces
    Piece,
    piece,
    pieced,
    listPieces,
    around,
    hexOf,
  )
where

import Control.Exception (evaluate)
import Data.Aeson.Encoding (Encoding, fromEncoding)
import Data.ByteString.Builder (Builder, lazyByteString, toLazyByteString)
import Data.ByteString.Lazy qualified as L
import Data.Bytes.Put (runPutL)
import Data.Bytes.Serial (Serial (..))
import Haskoin.Util (hexBuilder)
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.List (intersperse)
import Data.Sequence (Seq (..), (|>))
import Network.HTTP.Types (ResponseHeaders, status200)
import Network.Wai (Response, responseStream)
import UnliftIO (asyncWithUnmask, cancel, finally, mask_, wait)

-- | A piece of a response. Its outer action reads what the piece is made of
-- and encodes it to bytes, and may run at the same time as those of other
-- pieces. The action that it gives runs in order, right before the piece is
-- written, and gives the bytes to write. It should be cheap: it is there
-- for what depends on the pieces before, such as a comma.
newtype Piece = Piece (IO (IO Builder))

-- | A piece from an action that reads its bytes. They are encoded in the
-- thread that reads them, so the action that it gives only hands them over.
piece :: IO Builder -> Piece
piece act = Piece $ do
  bytes <- rendered =<< act
  return (return (lazyByteString bytes))

-- | The bytes of a builder, all of them made, so that the work of making
-- them is done where this is run.
rendered :: Builder -> IO L.ByteString
rendered b = do
  let bytes = toLazyByteString b
  _ <- evaluate (L.length bytes)
  return bytes

-- | The response made of pieces: the bytes of each, in order. The first
-- piece is read before the response is made, and its errors are thrown
-- here. The others are read while the response is written, up to the
-- number given at once, as 'ahead' does. The action given runs when the
-- response ends, however it ends. The response has no @Content-Length@.
piecesResponse :: Int -> ResponseHeaders -> IO () -> [Piece] -> IO Response
piecesResponse n headers done pieces = do
  (first, rest) <- case pieces of
    [] -> return (mempty, [])
    Piece p : more -> do
      bytes <- p >>= id
      return (bytes, more)
  return . responseStream status200 headers $ \write flush ->
    let send (Piece p) = p
        out next = next >>= write >> flush
     in (write first >> flush >> ahead n (map send rest) out) `finally` done

-- | Run actions, up to the number given at once, and hand their results to
-- the last action in order, as each comes. A failure of one is thrown here,
-- in order too: the results before it are handed over first. Actions still
-- running when this ends, however it ends, are cancelled, and a result is
-- let go once it is handed over.
ahead :: Int -> [IO a] -> (a -> IO ()) -> IO ()
ahead n acts consume = do
  running <- newIORef Empty
  let start act = mask_ $ do
        a <- asyncWithUnmask (\unmask -> unmask act)
        modifyIORef' running (|> a)
      go pending =
        readIORef running >>= \case
          Empty -> return ()
          a :<| others -> do
            x <- wait a
            writeIORef running others
            case pending of
              [] -> consume x >> go []
              act : more -> start act >> consume x >> go more
      (first, later) = splitAt (max 1 n) acts
  (mapM_ start first >> go later) `finally` (readIORef running >>= mapM_ cancel)

-- | Cut a list of keys into pieces of a size, each read with the action
-- given: the pieces of items that are read by key.
pieced :: Int -> ([k] -> IO b) -> [k] -> [IO b]
pieced size fetch = map fetch . cut
  where
    cut [] = []
    cut ks = let (part, more) = splitAt (max 1 size) ks in part : cut more

-- | The pieces of a JSON array from pieces of its items: the same bytes as
-- the list of all the items encoded at once. A piece may have no items.
-- The items of each piece are encoded where it is read; whether a comma
-- comes before them, which depends on whether an item came in the pieces
-- before, is settled in order, as 'piecesResponse' runs the pieces.
listPieces :: (b -> Encoding) -> [IO [b]] -> IO [Piece]
listPieces enc pieces = do
  begun <- newIORef False
  let encoded part = Piece $ do
        items <- part
        bytes <- rendered (mconcat (intersperse "," (map (fromEncoding . enc) items)))
        return $ do
          was <- readIORef begun
          let has = not (null items)
          writeIORef begun (was || has)
          return ((if was && has then "," else mempty) <> lazyByteString bytes)
  return (around "[" "]" (map encoded pieces))

-- | Pieces with bytes before the first and after the last, without a piece
-- more: the first piece stays the one that is read before the response.
around :: Builder -> Builder -> [Piece] -> [Piece]
around open close = \case
  [] -> [piece (return (open <> close))]
  [only] -> [wrap open close only]
  first : more -> wrap open mempty first : ending more
  where
    ending [] = []
    ending [final] = [wrap mempty close final]
    ending (p : more) = p : ending more
    wrap before after (Piece p) = Piece (fmap (fmap (\b -> before <> b <> after)) p)

-- | The serialization of a value, in hexadecimal.
hexOf :: (Serial a) => a -> Builder
hexOf = hexBuilder . runPutL . serialize
