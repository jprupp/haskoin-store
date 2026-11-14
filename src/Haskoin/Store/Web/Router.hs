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
-- A 'Route' is a method, a path pattern such as @\/block\/:block@, the names
-- of the parameters it accepts in the query string or a form, and a handler:
-- a function from the request to a response in 'IO'. 'router' turns a list of
-- them into a WAI 'Application' that answers with the first route whose
-- method and path match. A handler that finds the request is not for it after
-- all, such as one whose path parameter does not parse as the kind of value
-- it serves, calls 'next', and the request goes to the next route that
-- matches.
--
-- = Parameters are declared
--
-- A request with a parameter that its route does not accept is refused
-- ('Unknown'), rather than answered as if the parameter were not there. A
-- misspelt parameter, such as @limt=10@, or one that an endpoint does not
-- support, would otherwise be ignored, and the client would get an answer to
-- a question it did not ask, with nothing to tell it so. The check runs
-- before the handler, so a refused request has no effect, which matters for
-- a handler that does something, such as broadcasting a transaction.
--
-- The declaration and the handler cannot drift apart: reading a parameter
-- that the route does not declare ('param') throws 'Undeclared', an error of
-- the program that the tests of each endpoint find, rather than a parameter
-- that is accepted and never read, or read and never accepted.
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
    Miss (..),
    Undeclared (..),
    route,
    get,
    post,
    router,
    routerScoped,

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

import Control.Applicative ((<|>))
import Control.Exception (Exception, catch, throwIO)
import Data.ByteString.Lazy qualified as L
import Data.CaseInsensitive qualified as CI
import Data.List (nub)
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
    body :: !L.ByteString,
    -- | the names of the parameters that the route accepts
    accepted :: ![Text]
  }

-- | Where a request should go.
data Route = Route
  { method :: !Method,
    -- | the segments of the path: literals, and captures by name
    pattern :: ![Either Text Text],
    -- | the names of the parameters it accepts in the query string or a form
    accepts :: ![Text],
    handler :: Req -> IO Response,
    -- | whether it answers from memory alone, at next to no cost, like a
    -- health check: such a route is answered even by a server that is full
    -- ("Haskoin.Store.Web.Admission"). 'route' makes routes that are not.
    light :: !Bool
  }

-- | A route for a method, a path such as @\/block\/:block@, where a segment
-- that starts with a colon captures what is there under its name, and the
-- names of the parameters it accepts.
route :: Method -> Text -> [Text] -> (Req -> IO Response) -> Route
route m path accepts handler =
  Route
    { method = m,
      pattern = [segment t | t <- T.splitOn "/" path, not (T.null t)],
      accepts = accepts,
      handler = handler,
      light = False
    }
  where
    segment t = maybe (Left t) Right (T.stripPrefix ":" t)

-- | A route for @GET@.
get :: Text -> [Text] -> (Req -> IO Response) -> Route
get = route methodGet

-- | A route for @POST@.
post :: Text -> [Text] -> (Req -> IO Response) -> Route
post = route methodPost

-- | Why no route answered a request.
data Miss
  = -- | no route has its method and path
    NoRoute
  | -- | the routes with its method and path do not accept these parameters
    Unknown ![Text]
  deriving stock (Show)

-- | What 'param' throws for a parameter that the route does not declare: a
-- mistake in the program, not in the request.
newtype Undeclared = Undeclared Text
  deriving stock (Show)

instance Exception Undeclared

-- | What a handler throws to pass the request to the next route that
-- matches.
data Next = Next
  deriving stock (Show)

instance Exception Next

-- | Pass the request to the next route that matches.
next :: IO a
next = throwIO Next

-- | An application that answers with the first route whose method and path
-- match the request, that accepts all its parameters, and whose handler does
-- not pass it on; or with the fallback, told why none did. The body of the
-- request is read whole first, and parsed as a form if it is one; handlers
-- that stream a response still stream it.
router :: [Route] -> (Req -> Miss -> IO Response) -> Application
router routes = routerScoped routes (\_ answer -> answer routes)

-- | 'router' for handlers that need something that lasts as long as one
-- request does, such as a view of a database that must be the same for
-- everything the request reads and must be let go when it is answered.
--
-- The routes are given twice. The first list is used for its methods, paths
-- and parameters only, to find the route of a request. The second argument
-- is told which route matched, and runs an action with the same routes, in
-- the same order, made for the one request: the handler that answers is
-- taken from those. The response is
-- sent inside that action, so that a handler that streams its response can
-- go on reading while it is sent.
--
-- The alternative, to make the routes once and have each handler take what
-- it needs for itself, would put that in every handler; and making all the
-- routes again for each request, to match against, would cost more than
-- many requests do.
routerScoped ::
  [Route] ->
  (Route -> ([Route] -> IO (Maybe ResponseReceived)) -> IO (Maybe ResponseReceived)) ->
  (Req -> Miss -> IO Response) ->
  Application
routerScoped routes scoped fallback request respond = do
  body <- strictRequestBody request
  let form
        | lookup "Content-Type" (requestHeaders request)
            == Just "application/x-www-form-urlencoded" =
            parseQueryText (L.toStrict body)
        | otherwise = []
      params = [(k, maybe "" id v) | (k, v) <- queryToText (queryString request) <> form]
      req cs as = Req {wai = request, captures = cs, params = params, body = body, accepted = as}
      path = filter (not . T.null) (pathInfo request)
      -- the parameters that the first route matching the path refused, if any
      go unknown [] = fallback (req [] []) (maybe NoRoute Unknown unknown) >>= respond
      go unknown ((i, r) : rs)
        | r.method == requestMethod request,
          Just cs <- match r.pattern path =
            case nub [k | (k, _) <- params, k `notElem` r.accepts] of
              [] -> scoped r (answer i (req cs r.accepts)) >>= maybe (go unknown rs) return
              ks -> go (unknown <|> Just ks) rs
        | otherwise = go unknown rs
  go Nothing (zip [0 :: Int ..] routes)
  where
    -- the response of the handler of a route, sent, or nothing if the
    -- handler passed the request on
    answer i r scopedRoutes =
      ((Just <$> (scopedRoutes !! i).handler r) `catch` \Next -> return Nothing) >>= mapM respond
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

-- | A parameter of the query string or of a form, if there is one. Throws
-- 'Undeclared' if the route does not accept a parameter of that name.
--
-- It is an action rather than a pure function so that the error is thrown
-- when the parameter is read, inside the handler, where the server answers
-- it, and not whenever the value happens to be evaluated, which may be while
-- the response is being sent.
param :: Req -> Text -> IO (Maybe Text)
param r n
  | n `elem` r.accepted = return (lookup n r.params)
  | otherwise = throwIO (Undeclared n)

-- | A header of the request, if there is one.
header :: Req -> Text -> Maybe Text
header r n = decodeUtf8Lenient <$> lookup (CI.mk (encodeUtf8 n)) (requestHeaders r.wai)
