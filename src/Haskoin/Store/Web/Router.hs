-- |
-- Module      : Haskoin.Store.Web.Router
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Routing of requests to handlers, directly over WAI.
--
-- A 'Route' is a method, a path pattern such as @\/block\/:block@, and a
-- handler: a function from the request to a response in 'IO'. 'router' turns
-- a list of them into a WAI 'Application' that answers with the first route
-- whose method and path match. A handler that finds the request is not for
-- it after all, such as one whose path parameter does not parse as the kind
-- of value it serves, calls 'next', and the request goes to the next route
-- that matches.
--
-- = Why not a web framework
--
-- The server needs routing, parameters and responses; a framework like
-- Scotty adds them in a monad transformer over 'IO' that every handler has to
-- run in, with its own exceptions for control flow and its own way to return
-- a response. Here a handler is a plain function: it gets everything about
-- the request in a 'Req', returns a WAI 'Response', and stops early by
-- throwing, which 'router' and the server catch. The whole router is this
-- module.
module Haskoin.Store.Web.Router
  ( -- * Routes
    Route (..),
    route,
    get,
    post,
    router,

    -- * Requests
    Req (..),
    Next (..),
    next,
    capture,
    captureWith,
    param,
    header,
  )
where

import Control.Exception (Exception, catch, throwIO)
import Data.ByteString.Lazy qualified as L
import Data.CaseInsensitive qualified as CI
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (decodeUtf8Lenient, encodeUtf8)
import Network.HTTP.Types (Method, methodGet, methodPost, parseQueryText)
import Network.Wai

-- | A request being answered.
data Req = Req
  { -- | the request of WAI
    wai :: !Request,
    -- | the parameters captured from the path, by name
    captures :: ![(Text, Text)],
    -- | the parameters of the query string, then those of a form in the
    -- body; the first one of a name is the one that counts
    params :: ![(Text, Text)],
    -- | the body, read whole before the handler runs
    body :: !L.ByteString
  }

-- | Where a request should go.
data Route = Route
  { method :: !Method,
    -- | the segments of the path: literals, and captures by name
    pattern :: ![Either Text Text],
    handler :: Req -> IO Response
  }

-- | A route for a method and a path such as @\/block\/:block@, where a
-- segment that starts with a colon captures what is there under its name.
route :: Method -> Text -> (Req -> IO Response) -> Route
route m path = Route m [segment t | t <- T.splitOn "/" path, not (T.null t)]
  where
    segment t = maybe (Left t) Right (T.stripPrefix ":" t)

-- | A route for @GET@.
get :: Text -> (Req -> IO Response) -> Route
get = route methodGet

-- | A route for @POST@.
post :: Text -> (Req -> IO Response) -> Route
post = route methodPost

-- | What a handler throws to pass the request to the next route that
-- matches.
data Next = Next
  deriving stock (Show)

instance Exception Next

-- | Pass the request to the next route that matches.
next :: IO a
next = throwIO Next

-- | An application that answers with the first route whose method and path
-- match the request and whose handler does not pass it on, or with the
-- fallback if there is none. The body of the request is read whole first,
-- and parsed as a form if it is one; handlers that stream a response still
-- stream it.
router :: [Route] -> (Req -> IO Response) -> Application
router routes fallback request respond = do
  body <- strictRequestBody request
  let form
        | lookup "Content-Type" (requestHeaders request)
            == Just "application/x-www-form-urlencoded" =
            parseQueryText (L.toStrict body)
        | otherwise = []
      params = [(k, maybe "" id v) | (k, v) <- queryToText (queryString request) <> form]
      req cs = Req {wai = request, captures = cs, params = params, body = body}
      path = filter (not . T.null) (pathInfo request)
      go [] = fallback (req [])
      go (r : rs)
        | r.method == requestMethod request,
          Just cs <- match r.pattern path =
            r.handler (req cs) `catch` \Next -> go rs
        | otherwise = go rs
  go routes >>= respond
  where
    queryToText q = [(decodeUtf8Lenient k, decodeUtf8Lenient <$> v) | (k, v) <- q]

-- | The captures of a path, if it matches a pattern.
match :: [Either Text Text] -> [Text] -> Maybe [(Text, Text)]
match [] [] = Just []
match (Left l : ps) (t : ts) | l == t = match ps ts
match (Right n : ps) (t : ts) = ((n, t) :) <$> match ps ts
match _ _ = Nothing

-- | A parameter captured from the path. Passes the request on if there is
-- none of that name, which only a wrong pattern would cause.
capture :: Req -> Text -> IO Text
capture r n = maybe next return (lookup n r.captures)

-- | A parameter captured from the path, read with a parser. Passes the
-- request on if it does not parse: the path may be meant for another route,
-- such as an xpub where an address is expected.
captureWith :: Req -> (Text -> Maybe a) -> Text -> IO a
captureWith r f n = capture r n >>= maybe next return . f

-- | A parameter of the query string or of a form, if there is one.
param :: Req -> Text -> Maybe Text
param r n = lookup n r.params

-- | A header of the request, if there is one.
header :: Req -> Text -> Maybe Text
header r n = decodeUtf8Lenient <$> lookup (CI.mk (encodeUtf8 n)) (requestHeaders r.wai)
