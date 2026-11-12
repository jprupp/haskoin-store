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
-- server answers with a page that shows it ('swaggerPage').
--
-- The file names one server, @.\/@, a relative address that a reader
-- resolves against the address it read the file from. Requests tried from
-- the page go to the server that served it, whether it answers at the root
-- of a host or behind a proxy under a path such as @\/btc\/@. The file
-- names no public servers: those would describe someone else's servers,
-- at whatever version they run. The page has no list of servers to choose
-- from and no box to load another description, for the same reason.
--
-- = What the page needs
--
-- The page is Swagger UI, loaded from a public CDN at a fixed version. A
-- browser without access to the Internet shows an empty page;
-- @\/swagger.yaml@ is served either way. The program is not made a
-- megabyte and a half larger to carry a copy of it.
module Haskoin.Store.Web.Docs
  ( swaggerYaml,
    swaggerPage,
  )
where

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.FileEmbed (embedFile, makeRelativeToProject)

-- | @swagger.yaml@ as it was when the program was built.
swaggerYaml :: ByteString
swaggerYaml = $(makeRelativeToProject "swagger.yaml" >>= embedFile)

-- | The page at the root of the server: Swagger UI, showing 'swaggerYaml',
-- which it asks this server for, next to wherever the page itself is. It
-- leaves out the bar of Swagger UI that loads another description, and the
-- list of servers, which holds only this one.
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
      "  <style>",
      "    /* the list of servers, which holds only this one */",
      "    .swagger-ui .scheme-container { display: none; }",
      "  </style>",
      "</head>",
      "<body>",
      "  <div id=\"swagger-ui\"></div>",
      "  <noscript>The reference of this API is at <a href=\"swagger.yaml\">swagger.yaml</a>.</noscript>",
      "  <script src=\"" <> cdn <> "/swagger-ui-bundle.js\" charset=\"UTF-8\"></script>",
      "  <script>",
      "    window.onload = function () {",
      "      // next to this page, whether or not its address ends in a slash",
      "      const here = window.location.href.split(/[?#]/)[0].replace(/\\/?$/, '/');",
      "      window.ui = SwaggerUIBundle({",
      "        url: new URL('swagger.yaml', here).href,",
      "        dom_id: '#swagger-ui',",
      "        presets: [SwaggerUIBundle.presets.apis],",
      "        layout: 'BaseLayout'",
      "      });",
      "    };",
      "  </script>",
      "</body>",
      "</html>",
      ""
    ]
  where
    cdn = "https://unpkg.com/swagger-ui-dist@5.17.14"
