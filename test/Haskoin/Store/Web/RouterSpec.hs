module Haskoin.Store.Web.RouterSpec (spec) where

import Control.Exception (try)
import Data.ByteString.Builder (toLazyByteString)
import Data.ByteString.Lazy.Char8 qualified as L8
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Maybe (fromMaybe)
import Data.Text qualified as T
import Haskoin.Store.Web.Router
import Network.HTTP.Types (methodGet, parseQuery, status200, status404, statusCode)
import Network.Wai
import Network.Wai.Internal (Response (..), ResponseReceived (..))
import Test.Hspec

spec :: Spec
spec = describe "router" $ do
  it "answers with the route whose path matches, with its captures" $
    call "/block/abc" `shouldReturn` (200, "block abc")
  it "gives the parameters that the route accepts to its handler" $
    call "/block/abc?notx=true" `shouldReturn` (200, "block abc notx=true")
  it "refuses a parameter that the route does not accept, without running it" $
    call "/block/abc?limt=5" `shouldReturn` (404, "unknown limt")
  it "names every unknown parameter once" $
    call "/block/abc?a=1&b=2&a=3" `shouldReturn` (404, "unknown a,b")
  it "tries the next route when a handler passes, keeping its parameters" $
    call "/block/best?notx=false" `shouldReturn` (200, "best notx=false")
  it "falls back when no route has the path" $
    call "/nothing" `shouldReturn` (404, "no route")
  it "fails when a handler reads a parameter that its route does not declare" $ do
    r <- try (call "/sloppy")
    either (\(Undeclared n) -> n) (const "") r `shouldBe` "secret"

  it "answers inside the scope of a request, and makes one for each route that it tries" $ do
    events <- newIORef []
    let note e = modifyIORef' events (<> [e])
        scoped answer = note "open" >> answer routes <* note "close"
        req = defaultRequest {requestMethod = methodGet, pathInfo = ["block", "best"]}
    _ <- routerScoped routes scoped (\_ _ -> fail "no route") req $ \_ ->
      note "respond" >> return ResponseReceived
    -- the first route passes the request on, the second answers it
    readIORef events `shouldReturn` ["open", "close", "open", "respond", "close" :: String]

-- | Routes for the tests: the first matches any block but passes on
-- "best", for the second to answer.
routes :: [Route]
routes =
  [ get "/block/:block" ["notx"] $ \req -> do
      b <- capture req "block"
      if b == "best" then next else notx req >>= \n -> ok (T.unwords (["block", b] <> n)),
    get "/block/best" ["notx"] $ \req -> notx req >>= \n -> ok (T.unwords ("best" : n)),
    get "/sloppy" [] $ \req -> param req "secret" >>= ok . fromMaybe ""
  ]
  where
    notx req = maybe [] (\v -> ["notx=" <> v]) <$> param req "notx"
    ok t = return (responseLBS status200 [] (L8.pack (T.unpack t)))

-- | Status and body of the answer to a GET of a path with a query string.
call :: String -> IO (Int, String)
call url = do
  let (path, query) = break (== '?') url
      req =
        defaultRequest
          { requestMethod = methodGet,
            pathInfo = filter (not . T.null) (T.splitOn "/" (T.pack path)),
            queryString = parseQuery (L8.toStrict (L8.pack query))
          }
  out <- newIORef Nothing
  _ <- router routes miss req $ \res -> writeIORef out (Just res) >> return ResponseReceived
  readIORef out >>= \case
    Just (ResponseBuilder s _ b) -> return (statusCode s, L8.unpack (toLazyByteString b))
    _ -> fail "no response"
  where
    miss _ NoRoute = return (responseLBS status404 [] "no route")
    miss _ (Unknown ks) =
      return (responseLBS status404 [] (L8.pack ("unknown " <> T.unpack (T.intercalate "," ks))))
