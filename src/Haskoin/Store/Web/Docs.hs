{-# LANGUAGE TemplateHaskell #-}

-- |
-- Module      : Haskoin.Store.Web.Docs
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The reference of the API, served by the server that it describes: the
-- page at the root of the server, and the description that the page reads.
--
-- = Why the server carries its own reference
--
-- @swagger.yaml@ is the reference of the API. A copy on a web site
-- describes whatever version that site was last given, and someone who
-- runs a server wants the reference of the server in front of them. So the
-- file is compiled into the program ('swaggerYaml'), and the root of the
-- server answers with a page that shows it ('swaggerPage'), with the
-- server itself first in its list of servers, so that requests tried from
-- the page go to it.
--
-- = What the page needs
--
-- The page is Swagger UI, loaded from a public CDN at a fixed version, as
-- the page of api.haskoin.com does. A browser without access to the
-- Internet shows an empty page; @\/swagger.yaml@ is served either way. The
-- program is not made a megabyte and a half larger to carry a copy of it.
module Haskoin.Store.Web.Docs
  ( swaggerYaml,
    swaggerPage,
  )
where

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.FileEmbed (embedFile, makeRelativeToProject)

-- | @swagger.yaml@ as it was when the program was built, with this server
-- at the head of its list of servers: a relative address, which a reader of
-- the description resolves against where it found the description.
swaggerYaml :: ByteString
swaggerYaml = case B.breakSubstring marker file of
  (before, rest)
    | B.null rest -> file
    | otherwise -> before <> marker <> "  - url: './'\n    description: This server\n" <> B.drop (B.length marker) rest
  where
    marker = "servers:\n"
    file = $(makeRelativeToProject "swagger.yaml" >>= embedFile)

-- | The page at the root of the server: Swagger UI, showing 'swaggerYaml',
-- which it asks this server for, next to wherever the page itself is.
swaggerPage :: ByteString
swaggerPage =
  B.intercalate
    "\n"
    [ "<!DOCTYPE html>",
      "<html lang=\"en\">",
      "<head>",
      "  <meta charset=\"UTF-8\">",
      "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
      "  <title>Haskoin Store API</title>",
      "  <link rel=\"stylesheet\" href=\"" <> cdn <> "/swagger-ui.css\">",
      "</head>",
      "<body>",
      "  <div id=\"swagger-ui\"></div>",
      "  <noscript>The reference of this API is at <a href=\"swagger.yaml\">swagger.yaml</a>.</noscript>",
      "  <script src=\"" <> cdn <> "/swagger-ui-standalone-preset.js\"></script>",
      "  <script src=\"" <> cdn <> "/swagger-ui-bundle.js\" charset=\"UTF-8\"></script>",
      "  <script>",
      "    window.onload = function () {",
      "      // next to this page, whether or not its address ends in a slash",
      "      const here = window.location.href.split(/[?#]/)[0].replace(/\\/?$/, '/');",
      "      window.ui = SwaggerUIBundle({",
      "        url: new URL('swagger.yaml', here).href,",
      "        dom_id: '#swagger-ui',",
      "        presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],",
      "        layout: 'StandaloneLayout'",
      "      });",
      "    };",
      "  </script>",
      "</body>",
      "</html>",
      ""
    ]
  where
    cdn = "https://unpkg.com/swagger-ui-dist@5.17.14"
