{-# LANGUAGE LambdaCase #-}

-- |
-- Module      : Haskoin.Store.Web.Listen
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Where the web server listens when it is not a TCP port: a Unix socket,
-- made here or handed over by systemd.
--
-- A store behind a proxy on the same machine needs no TCP port at all. A
-- Unix socket is a file, so who may connect is set by its owner and mode,
-- not by firewall rules or the address it is bound to, and nothing on the
-- network can reach it by mistake. It also skips the TCP stack, which a
-- proxy on the same machine gains nothing from.
--
-- There are two ways to get one, and the store takes either:
--
-- * __Socket activation__ ('systemdSocket'). A systemd @.socket@ unit
--   creates the socket, owns its path and permissions, and passes it to the
--   service as file descriptor 3, with @LISTEN_PID@ and @LISTEN_FDS@ in its
--   environment. This is the better way under systemd: the socket exists
--   before the store starts, so clients that connect during a restart wait
--   instead of failing, and the unit file says who may use it.
--
-- * __A path__ ('withUnixSocket'). The store creates the socket itself, for
--   machines without systemd, containers and tests.
--
-- A socket from systemd wins over a path, and a path over a TCP port, so a
-- unit file that adds a @.socket@ unit needs no change to the store's own
-- options.
--
-- Over a Unix socket a client has no address: the access log shows @unix@
-- for it, unless a proxy names the client in @X-Forwarded-For@. Nothing
-- else in the store looks at the client's address; the congestion control
-- of "Haskoin.Store.Web.Admission" goes by how busy the cores are, not by
-- who asks.
module Haskoin.Store.Web.Listen
  ( systemdSocket,
    withUnixSocket,
    parseMode,
  )
where

import Control.Exception (bracket, bracketOnError, throwIO, try)
import Control.Monad (unless, when)
import Foreign.C.String (peekCAString)
import Network.Socket
  ( Family (AF_UNIX),
    SockAddr (SockAddrUnix),
    Socket,
    SocketType (Stream),
    bind,
    close,
    connect,
    defaultProtocol,
    listen,
    maxListenQueue,
    mkSocket,
    setNonBlockIfNeeded,
    socket,
  )
import Numeric (readOct)
import System.Directory (removeFile)
import System.Environment (lookupEnv, unsetEnv)
import System.IO.Error (isDoesNotExistError)
import System.Posix.Files (getFileStatus, isSocket, setFileMode)
import System.Posix.Internals (withFilePath)
import System.Posix.Process (getProcessID)
import System.Posix.Types (FileMode)
import Text.Read (readMaybe)

-- | The first socket that systemd passed to this process, if it passed any.
--
-- systemd sets @LISTEN_PID@ to the process that the sockets are for, and
-- @LISTEN_FDS@ to how many there are, starting at file descriptor 3. The
-- variables are checked against this process, so that a child that
-- inherited them does not take the sockets, and are then removed, so that
-- no child of this one does. Only the first socket is used; a unit with
-- several @ListenStream@ lines should list one.
systemdSocket :: IO (Maybe Socket)
systemdSocket = do
  pid <- lookupEnv "LISTEN_PID"
  fds <- lookupEnv "LISTEN_FDS"
  me <- getProcessID
  case (readMaybe =<< pid, readMaybe =<< fds) of
    (Just p, Just n)
      | p == toInteger me && n >= (1 :: Int) -> do
          mapM_ unsetEnv ["LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"]
          -- systemd leaves the descriptor blocking; GHC's I/O manager needs
          -- it not to be, or an accept would block a whole capability
          setNonBlockIfNeeded 3
          Just <$> mkSocket 3
    _ -> return Nothing

-- | Run an action with a Unix socket that listens at a path, and remove the
-- path when the action ends.
--
-- A socket file left at the path by a store that did not stop cleanly is
-- removed first, but only if nothing answers on it: a second store started
-- by mistake on the same path fails instead of taking the socket from the
-- first. A file at the path that is not a socket is never removed.
--
-- The mode is set after the socket is bound and before it listens, so there
-- is no moment in which a client could connect with the wrong permissions.
--
-- The path may be at most 107 bytes long, a limit of the system; a relative
-- path is taken from the working directory, and is one way around it.
withUnixSocket :: FilePath -> FileMode -> (Socket -> IO a) -> IO a
withUnixSocket path mode act = do
  -- the kernel keeps the path in 108 bytes, ending in a zero; the length is
  -- that of the path's bytes in the encoding of the file system
  pathBytes <- withFilePath path (fmap length . peekCAString)
  when (pathBytes > 107) . throwIO . userError $
    "The path of the Unix socket is longer than 107 bytes, which the system allows: "
      <> path
  removeStale path
  bracket open (\s -> close s >> removeFile path) act
  where
    open = bracketOnError (socket AF_UNIX Stream defaultProtocol) close $ \s -> do
      bind s (SockAddrUnix path)
      setFileMode path mode
      listen s maxListenQueue
      return s

-- | Remove a socket file that nothing listens on.
removeStale :: FilePath -> IO ()
removeStale path =
  try (getFileStatus path) >>= \case
    Left e
      | isDoesNotExistError e -> return ()
      | otherwise -> throwIO e
    Right st -> do
      unless (isSocket st) $
        throwIO . userError $
          path <> " exists and is not a socket"
      live <- bracket (socket AF_UNIX Stream defaultProtocol) close $ \s ->
        either (const False) (const True)
          <$> (try (connect s (SockAddrUnix path)) :: IO (Either IOError ()))
      when live $
        throwIO . userError $
          path <> " is in use by another server"
      removeFile path

-- | Read a file mode written in octal, such as @0660@ or @660@.
parseMode :: String -> Maybe FileMode
parseMode s = case readOct s of
  [(m, "")] | m <= 0o7777 -> Just m
  _ -> Nothing
