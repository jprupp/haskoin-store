{-# LANGUAGE LambdaCase #-}

-- |
-- Module      : Haskoin.Store.Startup
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Errors that stop the store from starting, said in words that tell an
-- operator what is wrong and what to do about it.
--
-- = Why
--
-- A store needs a few things from its machine before it can run: a Redis
-- (or Valkey) to share with its fleet, a data directory that it can write,
-- a database that no other process holds, a port or a Unix socket to listen
-- on, and the certificate and key of HTTPS if it is to speak it. When one
-- is missing, the library that finds out throws an exception about its own
-- part, such as @Network.Socket.connect: \<socket: 123\>: does not exist
-- (Connection refused)@ for a Redis that is not running, or a RocksDB
-- @LOCK@ file for a second store on the same directory. Those say what
-- failed, deep down, but not what it was for: the operator has to guess
-- which of the store's connections it was, and how to fix it.
--
-- This module turns each of those failures into a 'StartupError': one
-- paragraph that names the thing that is missing, the setting that points
-- at it (option and environment variable), and the usual fix. The program
-- prints it and exits with status 1, without the backtrace of an uncaught
-- exception, which is noise for a mistake of configuration.
--
-- = Where the checks are
--
-- Each check sits where the thing is first used, rather than in a list of
-- checks run up front: a check made in advance can pass, and the real use
-- fail a moment later for the same reason, with the old message.
--
-- * Redis: 'connectRedis', the first thing that 'Haskoin.Store.withStore'
--   does, before it opens the database or the network, so that a store
--   without Redis stops at once instead of after the database opened.
-- * The database: 'explainDatabase', around the opening of RocksDB.
-- * The data directory: 'makeDataDirectory'.
-- * The web server: 'explainListen', around the binding of its port, and
--   'checkReadable' for the files of HTTPS.
--
-- Errors while the store runs are not stopped here: a Redis that goes
-- away later is logged by the health check with its reason
-- ('redisProblem'), and the store answers 503 until it is back.
module Haskoin.Store.Startup
  ( -- * The error
    StartupError (..),

    -- * Redis
    connectRedis,
    redisProblem,

    -- * Files and directories
    makeDataDirectory,
    checkReadable,

    -- * Explanations of other failures
    explainDatabase,
    explainListen,
  )
where

import Control.Exception (Exception (..), IOException, SomeException, catch, handle, throwIO, try)
import Data.ByteString.Char8 qualified as C
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.List (isInfixOf)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Database.Redis qualified as R
import System.Directory (createDirectoryIfMissing)
import System.IO (IOMode (ReadMode), withFile)
import System.IO.Error
  ( ioeGetErrorString,
    isAlreadyInUseError,
    isDoesNotExistError,
    isPermissionError,
  )
import System.Timeout (timeout)

-- | A reason why the store cannot start, in words for the operator. The
-- program prints the text alone and exits with status 1 ('displayException'
-- is the text).
newtype StartupError = StartupError Text
  deriving (Show, Eq)

instance Exception StartupError where
  displayException (StartupError t) = T.unpack t

-- | Throw a 'StartupError' made of these lines, joined with spaces.
failWith :: [Text] -> IO a
failWith = throwIO . StartupError . T.unwords

-- | What every message about Redis ends with: what it is for, and how to
-- point the store at it.
redisHint :: Text
redisHint =
  T.unwords
    [ "haskoin-store needs a Redis or Valkey server to run: it keeps the tip,",
      "the mempool and the xpub cache that the stores of a fleet share.",
      "Start one (for example `valkey-server` or `redis-server`), or point the store",
      "at a running one with --redis-url or REDIS_URL (redis://[:password@]host:port[/db])."
    ]

-- | Connect to the Redis of the fleet at this URL (empty for the default,
-- @redis:\/\/localhost:6379@), and make sure that it answers: a ping must
-- come back as @PONG@. Throws a 'StartupError' that says what went wrong:
-- the URL is not one of Redis, nothing listens there, the name of the host
-- is unknown, Redis wants a password or another one, or it does not answer
-- within five seconds.
--
-- The check of the answer is the point: @checkedConnect@ of hedis only
-- makes sure that a reply came, and a Redis that wants a password replies
-- to a ping with an error, so a store connected to it without one started
-- and then answered 503 to every request, with nothing in its logs but
-- \"Redis does not answer\".
connectRedis :: String -> IO R.Connection
connectRedis url = do
  info <-
    if null url
      then return R.defaultConnectInfo
      else either badURL return (R.parseConnectInfo url)
  -- hedis waits for ever by default for a host that drops the packets
  let info' = info {R.connectTimeout = Just (fromMaybe 5 info.connectTimeout)}
  conn <- R.connect info' `catch` \(e :: IOException) -> unreachable (show e)
  answer <-
    try (timeout 6000000 (R.runRedis conn R.ping)) >>= \case
      Left e -> failed e
      Right r -> return r
  case answer of
    Nothing ->
      failWith ["Redis at", shown, "did not answer within five seconds.", redisHint]
    Just (Right R.Pong) -> return conn
    Just (Right other) ->
      failWith ["Redis at", shown, "answered a ping with", T.pack (show other) <> ".", redisHint]
    Just (Left reply) -> replied reply
  where
    shown = T.pack (if null url then "redis://localhost:6379" else url)
    badURL :: String -> IO b
    badURL why =
      failWith
        [ "--redis-url (or REDIS_URL)",
          T.pack (show url),
          "is not a Redis URL:",
          T.pack why <> ".",
          "It should look like redis://localhost:6379, or redis://:password@host:port/db."
        ]
    unreachable :: String -> IO b
    unreachable why
      | "refused" `isInfixOf` why =
          failWith ["Cannot connect to Redis at", shown <> ": nothing is listening there.", redisHint]
      | "does not exist" `isInfixOf` why || "Name or service not known" `isInfixOf` why =
          failWith ["Cannot connect to Redis at", shown <> ": the host name is unknown.", redisHint]
      | otherwise =
          failWith ["Cannot connect to Redis at", shown <> ":", T.pack why <> ".", redisHint]
    failed :: SomeException -> IO a
    failed e
      | Just (io :: IOException) <- fromException e = unreachable (show io)
      | Just (R.ConnectTimeout _) <- fromException e =
          failWith ["Redis at", shown, "did not accept a connection within five seconds.", redisHint]
      | Just (R.ConnectAuthError reply) <- fromException e = replied reply
      | Just (R.ConnectSelectError reply) <- fromException e =
          failWith
            [ "Redis at",
              shown,
              "refused the database number in the URL:",
              replyText reply <> ".",
              "Use a database that the server has (0 to 15 by default)."
            ]
      | otherwise = throwIO e
    replied :: R.Reply -> IO b
    replied reply
      | any (`T.isPrefixOf` msg) ["NOAUTH", "WRONGPASS", "ERR AUTH", "ERR invalid password"] =
          failWith
            [ "Redis at",
              shown,
              "refused the store:",
              msg <> ".",
              "It needs a password, or another one: put it in the URL,",
              "as redis://:password@host:port, or redis://user:password@host:port."
            ]
      | otherwise =
          failWith ["Redis at", shown, "answered with an error:", msg <> ".", redisHint]
      where
        msg = replyText reply

-- | The text of a reply of Redis, for a message.
replyText :: R.Reply -> Text
replyText =
  T.dropWhileEnd (== '.') . T.strip . \case
    R.Error e -> T.pack (C.unpack e)
    R.SingleLine s -> T.pack (C.unpack s)
    other -> T.pack (show other)

-- | Why Redis does not answer a ping right now, if it does not: for the
-- health check of a store that runs, which logs it. Nothing if it answers
-- @PONG@ within two seconds.
redisProblem :: R.Connection -> IO (Maybe Text)
redisProblem conn =
  handle (\(e :: SomeException) -> return (Just (described e))) $
    timeout 2000000 (R.runRedis conn R.ping) >>= \case
      Nothing -> return (Just "no answer within two seconds")
      Just (Right R.Pong) -> return Nothing
      Just (Right other) -> return (Just ("it answered " <> T.pack (show other)))
      Just (Left reply) -> return (Just (replyText reply))

-- | A failure to reach Redis in a few words: the reason of a refused
-- connection or an unknown host, rather than the text of the socket's
-- exception.
described :: SomeException -> Text
described e
  | "refused" `isInfixOf` why = "connection refused"
  | "does not exist" `isInfixOf` why || "Name or service not known" `isInfixOf` why = "host name unknown"
  | Just R.ConnectionLost <- fromException e = "connection lost"
  | otherwise = T.pack why
  where
    why = displayException e

-- | Make the data directory of the store, and its parents, if they are not
-- there. Throws a 'StartupError' that names the directory and the option
-- (@--dir@, @DIR@) if it cannot be made.
makeDataDirectory :: FilePath -> IO ()
makeDataDirectory dir =
  createDirectoryIfMissing True dir `catch` \(e :: IOException) ->
    failWith
      [ "Cannot create the data directory",
        T.pack dir <> ":",
        T.pack (reason e) <> ".",
        "Set --dir (or DIR) to a directory that the user running haskoin-store can write."
      ]
  where
    reason e
      | isPermissionError e = "permission denied"
      | isDoesNotExistError e = "a parent directory does not exist and cannot be made"
      | otherwise = ioeGetErrorString e

-- | Make sure that a file named by an option can be read, before a library
-- tries and fails with a message that does not name the option. The first
-- argument is the option, such as @--tls-cert@.
checkReadable :: Text -> FilePath -> IO ()
checkReadable option path =
  withFile path ReadMode (\_ -> return ()) `catch` \(e :: IOException) ->
    failWith
      [ option,
        T.pack path <> ":",
        T.pack (reason e) <> "."
      ]
  where
    reason e
      | isDoesNotExistError e = "no such file"
      | isPermissionError e = "the user running haskoin-store may not read it"
      | otherwise = ioeGetErrorString e

-- | Open the database in this directory with a function in the style of
-- @withX@, and run an action with it, explaining the usual failures of the
-- opening: another process holds the database (most often a store that is
-- still running, with the same @--dir@ and network), or the user may not
-- write it. Only the opening is explained: once the action has started,
-- an error of the action goes through as it is, since it is not about
-- opening the database.
explainDatabase :: FilePath -> ((r -> IO a) -> IO a) -> (r -> IO a) -> IO a
explainDatabase dir open action = do
  started <- newIORef False
  open (\r -> writeIORef started True >> action r) `catch` \(e :: IOException) ->
    readIORef started >>= \case
      True -> throwIO e
      False -> explain e
  where
    explain e
      | "LOCK" `isInfixOf` why && ("unavailable" `isInfixOf` why || "lock" `isInfixOf` why) =
          failWith
            [ "The database in",
              T.pack dir,
              "is in use by another process, most likely another haskoin-store",
              "with the same --dir and --net. Stop that one first,",
              "or give this one a data directory of its own with --dir."
            ]
      | "Permission denied" `isInfixOf` why =
          failWith
            [ "Cannot open the database in",
              T.pack dir <> ": permission denied.",
              "Run haskoin-store as the user that owns the directory, or set --dir to another one."
            ]
      | otherwise =
          failWith ["Cannot open the database in", T.pack dir <> ":", T.pack (ioeGetErrorString e)]
      where
        why = show e

-- | Run an action that binds the web server to this address (as the
-- operator gave it, such as @127.0.0.1:3000@ or a socket's path), and
-- explain why it could not: the address is in use, the port needs root,
-- or the host is not an address of this machine. The action gets an
-- action to run as soon as it listens (Warp's @setBeforeMainLoop@): only
-- errors before then are explained, and those after go through as they
-- are, since they are not about binding.
explainListen :: Text -> (IO () -> IO a) -> IO a
explainListen address action = do
  listening <- newIORef False
  action (writeIORef listening True) `catch` \(e :: IOException) ->
    readIORef listening >>= \case
      True -> throwIO e
      False -> explain e
  where
    explain e
      | isAlreadyInUseError e || "in use" `isInfixOf` show e =
          failWith
            [ "Cannot listen on",
              address <> ": another program is using it, perhaps another haskoin-store.",
              "Stop it, or choose another port with --port (or PORT), or another socket with --socket."
            ]
      | isPermissionError e =
          failWith
            [ "Cannot listen on",
              address <> ": permission denied.",
              "Ports below 1024 need root: use a higher --port, or a proxy in front of the store."
            ]
      | "Cannot assign requested address" `isInfixOf` show e || isDoesNotExistError e =
          failWith
            [ "Cannot listen on",
              address <> ": the host is not an address of this machine.",
              "Set --host (or HOST) to one of its addresses, or to * for all of them."
            ]
      | otherwise =
          failWith ["Cannot listen on", address <> ":", T.pack (ioeGetErrorString e)]
