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
-- this does not change. A short answer gains nothing, and should stay with
-- the routes that read first.
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
-- While a piece is written the next one is read, so that neither the
-- database nor the client waits for the other, and no more than two pieces
-- are held. Nothing of the database is held between pieces, so a slow
-- client costs two pieces of memory and no more. The price is that the
-- pieces are not read from one view of the database: an item may change,
-- or go, between two of them.
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

    -- * Pieces
    pieced,
    listPieces,
    around,
    hexOf,
  )
where

import Data.Aeson.Encoding (Encoding, fromEncoding)
import Data.ByteString.Builder (Builder, lazyByteStringHex)
import Data.Bytes.Put (runPutL)
import Data.Bytes.Serial (Serial (..))
import Data.IORef (newIORef, readIORef, writeIORef)
import Network.HTTP.Types (ResponseHeaders, status200)
import Network.Wai (Response, responseStream)
import UnliftIO (finally, wait, withAsync)

-- | The response made of pieces: the bytes of each, in order. A piece is
-- an action that reads something and gives its bytes. The first piece is
-- read before the response is made, and its errors are thrown here; the
-- others are read while the response is written, each while the one before
-- it is written. The action given runs when the response ends, however it
-- ends. The response has no @Content-Length@.
piecesResponse :: ResponseHeaders -> IO () -> [IO Builder] -> IO Response
piecesResponse headers done pieces = do
  (first, rest) <- case pieces of
    [] -> return (mempty, [])
    piece : more -> (,more) <$> piece
  return . responseStream status200 headers $ \write flush ->
    let send bytes [] = write bytes
        -- the thread that reads the next piece is let go before going on,
        -- or it would keep its piece until the end of the response
        send bytes (piece : more) = do
          bytes' <- withAsync piece $ \next -> write bytes >> flush >> wait next
          send bytes' more
     in send first rest `finally` done

-- | Cut a list of keys into pieces of a size, each read with the action
-- given: the pieces of items that are read by key.
pieced :: Int -> ([k] -> IO b) -> [k] -> [IO b]
pieced size fetch = map fetch . cut
  where
    cut [] = []
    cut ks = let (piece, more) = splitAt (max 1 size) ks in piece : cut more

-- | The pieces of a JSON array from pieces of its items: the same bytes as
-- the list of all the items encoded at once. A piece may have no items.
-- The pieces have to be run in order, as 'piecesResponse' runs them: each
-- has to know whether an item came before it, to put a comma.
listPieces :: (b -> Encoding) -> [IO [b]] -> IO [IO Builder]
listPieces enc pieces = do
  begun <- newIORef False
  let bytes piece = do
        items <- piece
        was <- readIORef begun
        writeIORef begun (was || not (null items))
        return . mconcat $
          [(if comma then "," else mempty) <> fromEncoding (enc item) | (comma, item) <- zip (was : repeat True) items]
  return (around "[" "]" (map bytes pieces))

-- | Pieces with bytes before the first and after the last, without a piece
-- more: the first piece stays the one that is read before the response.
around :: Builder -> Builder -> [IO Builder] -> [IO Builder]
around open close = \case
  [] -> [return (open <> close)]
  [only] -> [(\b -> open <> b <> close) <$> only]
  first : more -> ((open <>) <$> first) : ending more
  where
    ending [] = []
    ending [final] = [(<> close) <$> final]
    ending (piece : more) = piece : ending more

-- | The serialization of a value, in hexadecimal.
hexOf :: (Serial a) => a -> Builder
hexOf = lazyByteStringHex . runPutL . serialize
