{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoFieldSelectors #-}

-- |
-- Module      : Haskoin.Store.WebClient
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Client for the REST API of Haskoin Store.
--
-- Build a request value, such as 'GetAddrBalance', and pass it to 'apiCall'.
-- The response type follows from the request type. Requests and responses use
-- the compact binary encoding of the API. Errors returned by the server are
-- thrown as 'Store.Except' in 'MonadError'.
--
-- @
-- import Control.Monad.Except (runExceptT)
-- import Data.Default (def)
--
-- balance :: Ctx -> Address -> IO (Either Except Balance)
-- balance ctx addr = runExceptT $ 'apiCall' ctx def ('GetAddrBalance' addr)
-- @
--
-- Requests that take long lists of addresses, transactions or blocks can be
-- split into several calls with 'apiBatch'.
-- The client always uses the binary encoding, which is smaller and faster than
-- JSON and decodes straight into the types of "Haskoin.Store.Data".
module Haskoin.Store.WebClient
  ( -- * Calls
    ApiConfig (..),
    apiCall,
    apiBatch,
    -- * Blocks
    GetBlock (..),
    GetBlocks (..),
    GetBlockRaw (..),
    GetBlockBest (..),
    GetBlockBestRaw (..),
    GetBlockLatest (..),
    GetBlockHeight (..),
    GetBlockHeights (..),
    GetBlockHeightRaw (..),
    GetBlockTime (..),
    GetBlockTimeRaw (..),
    -- * Transactions
    GetTx (..),
    GetTxs (..),
    GetTxRaw (..),
    GetTxsRaw (..),
    GetTxsBlock (..),
    GetTxsBlockRaw (..),
    GetTxAfter (..),
    PostTx (..),
    GetMempool (..),
    GetEvents (..),
    -- * Addresses
    GetAddrTxs (..),
    GetAddrsTxs (..),
    GetAddrTxsFull (..),
    GetAddrsTxsFull (..),
    GetAddrBalance (..),
    GetAddrsBalance (..),
    GetAddrUnspent (..),
    GetAddrsUnspent (..),
    -- * Extended public keys
    GetXPub (..),
    GetXPubTxs (..),
    GetXPubTxsFull (..),
    GetXPubBalances (..),
    GetXPubUnspent (..),
    DelCachedXPub (..),
    -- * Network
    GetPeers (..),
    GetHealth (..),
    -- * Parameters
    StartParam (..),
    OffsetParam (..),
    LimitParam (..),
    LimitsParam (..),
    HeightParam (..),
    HeightsParam (..),
    Store.DeriveType (..),
    NoCache (..),
    NoTx (..),
  )
where

import Control.Arrow (second)
import Control.Exception
import Control.Lens ((.~), (?~), (^.))
import Control.Monad.Except
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson qualified as A
import Data.Bytes.Get
import Data.Bytes.Put
import Data.Bytes.Serial
import Data.Default (Default, def)
import Data.Monoid (Endo (..), appEndo)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Text qualified as Text
import Haskoin.Crypto (Ctx)
import Haskoin.Network
import Haskoin.Store.Data qualified as Store
import Haskoin.Store.WebCommon
import Haskoin.Transaction
import Haskoin.Util
import Network.HTTP.Client (Request (..))
import Network.HTTP.Types (StdMethod (..))
import Network.HTTP.Types.Status
import Network.Wreq qualified as HTTP
import Network.Wreq.Types (ResponseChecker)
import Numeric.Natural (Natural)

-- | Configuration specifying the Network and the Host for API calls.
-- Default instance:
--
-- @
-- ApiConfig
-- { net = bch
-- , host = "https://api.haskoin.com/"
-- }
-- @
data ApiConfig = ApiConfig
  { -- | network to query; its name is appended to the host
    net :: !Network,
    -- | base URL of the API, ending in a slash
    host :: !String
  }
  deriving (Eq, Show)

instance Default ApiConfig where
  def =
    ApiConfig
      { net = bch,
        host = "https://api.haskoin.com/"
      }

-- | Make a call to the haskoin-store API.
--
-- Usage (default options):
--
-- > apiCall ctx def $ GetAddrsTxs addrs def
--
-- With options:
--
-- > apiCall ctx def $ GetAddrsUnspent addrs def {limit = Just 10}
--
-- Only @GET@ and @POST@ endpoints are supported.
apiCall ::
  (ApiResource a b, MonadIO m, MonadError Store.Except m) =>
  Ctx ->
  ApiConfig ->
  a ->
  m b
apiCall ctx (ApiConfig net apiHost) res = do
  args <- liftEither $ toOptions net ctx res
  let url = apiHost <> net.name <> cs (queryPath net ctx res)
  case resourceMethod $ asProxy res of
    GET -> liftEither =<< liftIO (getBinary args url)
    POST ->
      case resourceBody res of
        Just (PostBox val) ->
          liftEither =<< liftIO (postBinary args url val)
        _ -> throwError $ Store.StringError "Could not post resource"
    _ -> throwError $ Store.StringError "Unsupported HTTP method"

-- | Batch commands that have a large list of arguments:
--
-- > apiBatch ctx 20 def (GetAddrsTxs addrs def)
--
-- The list is split into chunks of the given size, one call is made per chunk,
-- and the results are concatenated.
apiBatch ::
  (Batchable a b, MonadIO m, MonadError Store.Except m) =>
  Ctx ->
  Natural ->
  ApiConfig ->
  a ->
  m b
apiBatch ctx i conf res =
  mconcat <$> mapM (apiCall ctx conf) (resourceBatch i res)

-- | Requests that can be split into several smaller ones whose results are
-- concatenated.
class (ApiResource a b, Monoid b) => Batchable a b where
  -- | Split a request into requests with lists of at most this size.
  resourceBatch :: Natural -> a -> [a]

instance Batchable GetBlocks (Store.SerialList Store.BlockData) where
  resourceBatch i (GetBlocks hs t) =
    (`GetBlocks` t) <$> chunksOf i hs

instance Batchable GetBlockHeights (Store.SerialList Store.BlockData) where
  resourceBatch i (GetBlockHeights (HeightsParam hs) n) =
    (`GetBlockHeights` n)
      <$> (HeightsParam <$> chunksOf i hs)

instance Batchable GetTxs (Store.SerialList Store.Transaction) where
  resourceBatch i (GetTxs ts) =
    GetTxs <$> chunksOf i ts

instance Batchable GetTxsRaw (Store.RawResultList Tx) where
  resourceBatch i (GetTxsRaw ts) =
    GetTxsRaw <$> chunksOf i ts

instance Batchable GetAddrsTxs (Store.SerialList Store.TxRef) where
  resourceBatch i (GetAddrsTxs as l) =
    (`GetAddrsTxs` l) <$> chunksOf i as

instance Batchable GetAddrsTxsFull (Store.SerialList Store.Transaction) where
  resourceBatch i (GetAddrsTxsFull as l) =
    (`GetAddrsTxsFull` l) <$> chunksOf i as

instance Batchable GetAddrsBalance (Store.SerialList Store.Balance) where
  resourceBatch i (GetAddrsBalance as) =
    GetAddrsBalance <$> chunksOf i as

instance Batchable GetAddrsUnspent (Store.SerialList Store.Unspent) where
  resourceBatch i (GetAddrsUnspent as l) =
    (`GetAddrsUnspent` l) <$> chunksOf i as

------------------
-- API Internal --
------------------

-- | HTTP options carrying the query string parameters of a request.
toOptions ::
  (ApiResource a b) =>
  Network ->
  Ctx ->
  a ->
  Either Store.Except (Endo HTTP.Options)
toOptions net ctx res =
  mconcat <$> mapM f (snd $ queryParams res)
  where
    f (ParamBox p) = toOption net ctx p

-- | HTTP option for one query string parameter.
toOption ::
  (Param a) =>
  Network ->
  Ctx ->
  a ->
  Either Store.Except (Endo HTTP.Options)
toOption net ctx a = do
  res <-
    maybeToEither (Store.UserError "Invalid Param") $
      encodeParam net ctx a
  return $ applyOpt (paramLabel a) res

-- | Set a query string parameter to comma-separated values.
applyOpt :: Text -> [Text] -> Endo HTTP.Options
applyOpt p t = Endo $ HTTP.param p .~ [Text.intercalate "," t]

-- | Make a @GET@ request and decode the binary response.
getBinary ::
  (Serial a) =>
  Endo HTTP.Options ->
  String ->
  IO (Either Store.Except a)
getBinary opts url = do
  resE <- try $ HTTP.getWith (binaryOpts opts) url
  return $ do
    res <- resE
    return . runGetL deserialize $ res ^. HTTP.responseBody

-- | Make a @POST@ request with a binary body and decode the binary response.
postBinary ::
  (Serial a, Serial r) =>
  Endo HTTP.Options ->
  String ->
  a ->
  IO (Either Store.Except r)
postBinary opts url body = do
  resE <- try $ HTTP.postWith (binaryOpts opts) url (runPutL (serialize body))
  return $ do
    res <- resE
    return . runGetL deserialize $ res ^. HTTP.responseBody

-- | Request binary responses, and turn error responses into exceptions.
binaryOpts :: Endo HTTP.Options -> HTTP.Options
binaryOpts opts =
  appEndo (opts <> accept <> stat) HTTP.defaults
  where
    accept = Endo $ HTTP.header "Accept" .~ ["application/octet-stream"]
    stat = Endo $ HTTP.checkResponse ?~ checkStatus

-- | Throw the error in the body of an unsuccessful response as 'Store.Except'.
-- A health check that answers 503 is not an error: its body says what is
-- wrong.
checkStatus :: ResponseChecker
checkStatus req res
  | statusIsSuccessful status = return ()
  | isHealthPath && code == 503 = return () -- Ignore health checks
  | otherwise = do
      e <- A.decodeStrict <$> res ^. HTTP.responseBody
      throwIO $
        case e of
          Just except -> except :: Store.Except
          Nothing -> Store.StringError "could not decode error"
  where
    code = res ^. HTTP.responseStatus . HTTP.statusCode
    message = res ^. HTTP.responseStatus . HTTP.statusMessage
    status = mkStatus code message
    isHealthPath = "/health" `Text.isInfixOf` cs (path req)

---------------
-- Utilities --
---------------

-- | Split a list into chunks of at most this size.
chunksOf :: Natural -> [a] -> [[a]]
chunksOf n xs
  | null xs = []
  | otherwise =
      uncurry (:) $ second (chunksOf n) $ splitAt (fromIntegral n) xs
