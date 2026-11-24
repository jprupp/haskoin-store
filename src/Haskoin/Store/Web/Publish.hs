{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# OPTIONS_GHC -Wno-ambiguous-fields #-}

-- |
-- Module      : Haskoin.Store.Web.Publish
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- Publishing a transaction that a client posted: @POST \/transactions@.
--
-- = What it does
--
-- 1. __Check__ what the store can decide from its own index ('judge'): the
--    coins, the amounts, the lock time, the rules of relay that do not
--    change, the form of the input scripts, the tokens of Bitcoin Cash, and
--    the signature of every coin locked to one key. A transaction that the
--    store has in a block is answered at once, and nothing is sent.
--
-- 2. __Send over RPC__, where the store was given the RPC of a full node
--    ('Publishing'.@direct@). The node checks everything, and says at once
--    whether it took the transaction and why not. It is the only way to
--    publish a spend of a coin that the store cannot verify.
--
-- 3. __Hand it to every peer__ otherwise, if the store verified every
--    input: to all at once, each on a connection of its own ('deliver'),
--    and answer that it was handed over as soon as they took it. What the
--    network does with it is the network's to decide. Bitcoin Core refuses
--    without saying so; Bitcoin Cash Node says why, and if every peer that
--    answered refused it, the answer is that reason.
--
-- 4. __It comes back__ as any transaction does: a peer that takes it
--    announces it on the connections that the store syncs from, which did
--    not send it, and the store imports it. That takes a moment with a node
--    that whitelists the store, and seconds with one that does not. The
--    store also asks for it a few times ('Asking'), in case none does.
--
-- A transaction that the store has unconfirmed is checked and sent again in
-- the same way. The store does not hold its mempool to its node's, so it
-- may list a transaction that the nodes forgot: after a reorganisation, or
-- when a node lost its mempool. Posting it again is how a wallet gets it
-- back to them.
--
-- = Why it is this way
--
-- * __No waiting for the echo.__ The request could wait for a node to
--   announce the transaction back, and say what came of it. But any limit
--   on that wait would be arbitrary, Bitcoin Core does not say
--   when it refuses, and the store cannot see the network: a transaction
--   that one node refuses may be taken by others. The client is told what
--   the store knows, that it was handed over, and finds out the rest the
--   way it finds out about any transaction.
--
-- * __A connection of its own.__ A node never announces a transaction to
--   the connection that gave it, and gives it back there only once it
--   relayed it to another peer, or announced something else there. Sent on
--   the connections that the store syncs from, a transaction did not come
--   back to a store whose node had no other peer until a block confirmed
--   it. On a connection of its own it comes back as any other.
--
-- * __Every peer.__ A peer that is sent a transaction it finds invalid may
--   close its connections to the sender, and Bitcoin Cash Node closes them
--   all. So the store sends a peer only what it verified, and checks the
--   rules whose breach was measured to cost it its connections: the form of
--   the input scripts and the tokens (@docs\/publishing.md@ has what was
--   measured). A transaction that only breaks a rule of relay, such as one
--   that pays too little, is refused by the nodes without harm.
--
-- * __The RPC first.__ A node that the store can ask says no with a reason,
--   from Bitcoin Core too, and runs every script.
--
-- = What it cannot do
--
-- Only a full node runs a script. The store refuses a spend of a coin that
-- a script locks unless it has the RPC of a node to send it over. It does
-- not know whether the network took a transaction until it sees it: a
-- client that wants to know watches for it, or posts it again.
module Haskoin.Store.Web.Publish
  ( -- * Publishing
    Publishing (..),
    Answer (..),
    Direct (..),
    Held (..),
    publish,

    -- * The courier
    Delivery (..),
    Courier (..),
    deliver,

    -- * Asking for what was handed over
    Asking,
    newAsking,
    askFor,
    askLoop,
    askAgain,
    due,
    asked,

    -- * What the store can check

    -- | The checks themselves are in haskoin-core
    -- ("Haskoin.Transaction.Judge"), which other programs use too: they
    -- say nothing of a store. They are exported from here as they were.
    Coin (..),
    Refusal (..),
    Rules (..),
    judge,
    tokenRules,
    isDust,
    maxStandardSize,
    coinOf,
    maxTxSize,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently, race, waitCatch, withAsync)
import Control.Concurrent.STM (TVar, atomically, modifyTVar', newTVarIO, readTVarIO, writeTVar)
import Control.Monad (forM_, forever, unless, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.List (partition)
import Data.Maybe (isJust, listToMaybe)
import Data.Serialize (decode, encode)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Time.Clock (NominalDiffTime, UTCTime, addUTCTime, getCurrentTime)
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import Data.Word (Word32, Word64, Word8)
import Haskoin
  ( BlockHeight,
    InputVerdict (..),
    Message (..),
    MessageCommand (..),
    NFToken (..),
    Network (..),
    NetworkAddress (..),
    OutPoint (..),
    Ping (..),
    Pong (..),
    Reject (..),
    Script (..),
    TokenData (..),
    Tx (..),
    TxHash,
    TxIn (..),
    TxOut (..),
    VarString (..),
    genesisNode,
    isMinimalPush,
    isPushOp,
    nullOutPoint,
    sockToHostAddress,
    splitTokenData,
    txHash,
    txHashToHex,
  )
import Haskoin.Node
  ( LogLevel (..),
    Logger,
    PeerConfig (..),
    PeerEvent (..),
    WithConnection,
    buildVersion,
    expectsReject,
    newPeer,
    peer,
    peerMessageDone,
    sendMessage,
  )
import Haskoin.Node.Address (peerHostAddress, seenAs)
import Haskoin.Node.Peer.Remote (newRemote)
import Haskoin.Store.Common (StoreRead (..))
import Haskoin.Store.Data (BlockRef (..), Spender (..), TxData (..), isCoinbaseTx)
import Haskoin.Transaction.Judge
  ( Coin (..),
    Refusal (..),
    Rules (..),
    isDust,
    judge,
    maxStandardSize,
    maxTxSize,
    tokenRules,
  )
import NQE (newMailbox, receive, withPublisher, withSubscription)
import Network.Socket (SockAddr (..))
import System.Random (randomIO)
import System.Timeout (timeout)

-- | Whether the store has a transaction, and where.
data Held
  = -- | the store does not have it
    Absent
  | -- | in its mempool: the network may have forgotten it, so it is sent
    -- again
    Pending
  | -- | in a block: there is nothing to send
    Confirmed
  deriving (Eq, Show)

-- | What came of giving a transaction to a full node over its RPC.
data Direct
  = -- | the node took it into its mempool, or had it
    Sent
  | -- | the node refused it, with this reason
    Declined !Text
  | -- | there is no such node, or it did not answer
    Unavailable
  deriving (Eq, Show)

-- | The answer to a request to publish.
data Answer
  = -- | the store has it in a block: nothing was sent
    InBlock
  | -- | a full node took it over its RPC
    ToNode
  | -- | handed to this many peers
    ToPeers !Int
  | -- | a full node refused it over its RPC and said why, or every peer
    -- that answered refused it
    Refused !Text
  | -- | refused by the store, before anything was sent
    Invalid !Refusal
  | -- | not sent: the store could not verify the signature of this input,
    -- and had no node to give the transaction to over RPC
    Unverified !Int
  | -- | not handed over: the store has no peer, or reached none
    NoPeers
  deriving (Eq, Show)

-- | The coin of an output, from the queries of a request.
coinOf :: StoreRead -> OutPoint -> IO Coin
coinOf q o =
  q.getTxData o.hash >>= \case
    Just td
      | not td.deleted,
        out : _ <- drop (fromIntegral o.index) td.tx.outputs ->
          q.getSpender o >>= \case
            Nothing -> return (coin td out)
            Just s ->
              q.getTxData s.txid >>= \case
                Just by | not by.deleted, Just _ <- confirmed by.block -> return (Spent s.txid)
                _ -> return (coin td out)
    _ -> return Missing
  where
    coin td out = Coin {output = out, mined = confirmed td.block, coinbase = isCoinbaseTx td.tx}
    confirmed BlockRef {height} = Just height
    confirmed MemRef {} = Nothing

-- | What publishing needs. The peers and the courier are functions, so
-- that the rules can be tested without a network.
data Publishing = Publishing
  { -- | the chain as the store has it now
    rules :: !(IO Rules),
    -- | give the transaction to a full node over its RPC, where the store
    -- was given the RPC of one
    direct :: !(Tx -> IO Direct),
    -- | whether the store has the transaction, and where, from the
    -- queries of the request
    holds :: !(TxHash -> IO Held),
    -- | the coin of an output, from the queries of the request ('coinOf')
    coin :: !(OutPoint -> IO Coin),
    -- | the addresses of the peers that the store has now, that take
    -- transactions
    peers :: !(IO [SockAddr]),
    -- | hand a transaction to a peer on a connection of its own ('deliver')
    courier :: !(SockAddr -> Tx -> IO Delivery),
    -- | ask for a transaction that was handed over, after the answer: see
    -- 'Asking'. It must not wait.
    fetch :: !(TxHash -> IO ()),
    note :: !(LogLevel -> Text -> IO ())
  }

-- | Publish a transaction, and say what came of it. See the module header.
publish :: Publishing -> Tx -> IO Answer
publish p tx =
  p.holds txid >>= \case
    Confirmed -> return InBlock
    _ ->
      (judge <$> p.rules <*> pure tx <*> mapM (p.coin . (.outpoint)) tx.inputs) >>= \case
        Left r -> return (Invalid r)
        Right unverified ->
          p.direct tx >>= \case
            Sent -> do
              p.note LevelDebug ("A node took " <> name <> " over its RPC")
              p.fetch txid
              return ToNode
            Declined why -> do
              p.note LevelInfo ("A node refused " <> name <> ": " <> why)
              return (Refused why)
            Unavailable -> case unverified of
              Just i -> do
                p.note LevelInfo ("Not sent to a peer, input " <> T.pack (show i) <> " not verified: " <> name)
                return (Unverified i)
              Nothing -> do
                addresses <- p.peers
                delivered <- mapConcurrently (\a -> p.courier a tx) addresses
                let handed = length [() | d <- delivered, d `elem` [Took, Kept, NoAnswer]]
                    reasons = [why | Rejected why <- delivered]
                if
                  | handed > 0 -> do
                      p.note LevelDebug ("Handed " <> name <> " to " <> T.pack (show handed) <> " peers")
                      p.fetch txid
                      return (ToPeers handed)
                  | why : _ <- reasons -> do
                      p.note LevelInfo ("Every peer refused " <> name <> ", the first: " <> why)
                      return (Refused why)
                  | Closed `elem` delivered -> do
                      p.note LevelWarn ("A peer closed the connection on receiving " <> name)
                      return (Refused "a peer closed the connection on receiving it")
                  | otherwise -> do
                      p.note LevelWarn ("No peer to hand " <> name <> " to")
                      return NoPeers
  where
    txid = txHash tx
    name = txHashToHex txid

-- | The transactions that were handed over and that the store asks for
-- until it has them, each with when to ask next and the delays after that
-- ('askAgain').
--
-- The store asks its peers as if they had announced the transaction, so
-- that it is requested and imported as any other. A peer gives it back
-- once it would announce it: Bitcoin Cash Node in a second or two, Bitcoin
-- Core once it has announced something to the store since it took the
-- transaction, which on a busy network is a matter of seconds.
--
-- One thread of the web server asks ('askLoop'), and stops with it: a
-- thread for each transaction could outlive the database that it reads.
newtype Asking = Asking (TVar (HashMap TxHash (UTCTime, [NominalDiffTime])))

-- | Nothing to ask for.
newAsking :: IO Asking
newAsking = Asking <$> newTVarIO HashMap.empty

-- | The most transactions asked for at once. Past it a transaction is not
-- asked for, and comes to the store when a peer announces it or a block
-- confirms it.
askingLimit :: Int
askingLimit = 10000

-- | Ask for a transaction from now on, on the schedule of 'askAgain'.
askFor :: Asking -> TxHash -> IO ()
askFor (Asking var) h = do
  now <- getCurrentTime
  atomically . modifyTVar' var $ \m -> case askAgain of
    first : rest | HashMap.size m < askingLimit -> HashMap.insert h (addUTCTime first now, rest) m
    _ -> m

-- | The transactions whose time to be asked for has come, and what is left
-- to ask for after them: each at its next delay, or no more after its last.
due ::
  UTCTime ->
  HashMap TxHash (UTCTime, [NominalDiffTime]) ->
  ([TxHash], HashMap TxHash (UTCTime, [NominalDiffTime]))
due now m = ([h | (h, (t, _)) <- HashMap.toList m, t <= now], HashMap.mapMaybe next m)
  where
    next (t, ds)
      | t > now = Just (t, ds)
      | otherwise = case ds of
          d : rest -> Just (addUTCTime d now, rest)
          [] -> Nothing

-- | The schedule after a transaction whose time had come was asked for:
-- at its next delay, or no more after its last ('due').
asked ::
  UTCTime ->
  TxHash ->
  HashMap TxHash (UTCTime, [NominalDiffTime]) ->
  HashMap TxHash (UTCTime, [NominalDiffTime])
asked now h = HashMap.update next h
  where
    next (t, ds)
      | t > now = Just (t, ds)
      | otherwise = case ds of
          d : rest -> Just (addUTCTime d now, rest)
          [] -> Nothing

-- | Every second, ask for each transaction whose time has come, unless the
-- store has it now. Runs until it is cancelled.
--
-- The schedule of a transaction moves on only once it was asked for, one
-- at a time ('asked'). Moved on for all of them first, as it once was, an
-- error in the middle lost the turn of the others, and a transaction at its
-- last delay was never asked for at all.
askLoop :: Asking -> (TxHash -> IO Bool) -> (TxHash -> IO ()) -> IO ()
askLoop (Asking var) present ask = forever $ do
  threadDelay 1000000
  now <- getCurrentTime
  ready <- fst . due now <$> readTVarIO var
  forM_ ready $ \h ->
    present h >>= \case
      True -> atomically (modifyTVar' var (HashMap.delete h))
      False -> ask h >> atomically (modifyTVar' var (asked now h))

-- | When to ask for a transaction that was handed over: soon, for Bitcoin
-- Cash Node, and then every 25 seconds for two minutes and a half. The
-- delays are longer than the 20 seconds that the block store waits for a
-- transaction that it requested before it asks again.
askAgain :: [NominalDiffTime]
askAgain = [2, 25, 25, 25, 25, 25, 25]

-- | What a node did with a transaction handed to it.
data Delivery
  = -- | it answered the ping that followed: it is done with the
    -- transaction, and said nothing against it, as Bitcoin Core never does
    Took
  | -- | it answered the ping that followed without a @reject@, and it is a
    -- node that answers a transaction it refuses with one, as Bitcoin Cash
    -- Node does: it did not refuse it
    Kept
  | -- | it sent a @reject@ message for the transaction, with this reason
    Rejected !Text
  | -- | it closed the connection after getting the transaction and
    -- before answering the ping
    Closed
  | -- | it kept the connection and did not answer in time
    NoAnswer
  | -- | no connection, or it ended before the transaction was sent
    NoConnection
  deriving (Eq, Show)

-- | What the courier needs.
data Courier = Courier
  { net :: !Network,
    connect :: !(SockAddr -> WithConnection),
    -- | seconds for all of it
    patience :: !NominalDiffTime,
    logger :: !Logger
  }

-- | Hand a transaction to a node on a connection made for it, and closed
-- after: the version handshake, the transaction, a @ping@, and what the
-- node answers until its @pong@.
--
-- The connection is a peer of "Haskoin.Node.Peer" with a publisher of its
-- own, so nothing of it reaches the peer manager or the block store. It has
-- a model of the node, as every peer does ("Haskoin.Node.Peer.Remote"),
-- which judges each message before it goes. The model also says whether the
-- node answers a refusal with a @reject@ ('expectsReject'): if it does, a
-- @pong@ with no @reject@ before it means that the node kept the
-- transaction ('Kept'); if it never does, only that the node is done with
-- it ('Took').
deliver :: Courier -> SockAddr -> Tx -> IO Delivery
deliver c sa tx =
  withPublisher $ \pub -> withSubscription pub $ \sub -> do
    (inbox, mailbox) <- newMailbox
    sent <- newTVarIO False
    refused <- newTVarIO Nothing
    nonce <- randomIO
    token <- randomIO
    seed <- randomIO
    now <- getCurrentTime
    let (addrNet, local) = seenAs sa
        opened = floor (utcTimeToPOSIXSeconds now * 1000000)
        config =
          PeerConfig
            { pub = pub,
              net = c.net,
              label = "courier " <> cs (show sa),
              connect = c.connect sa,
              logger = c.logger,
              lookupBlock = const (return Nothing),
              bestBlock = return (genesisNode c.net),
              remote = newRemote c.net mempty addrNet local opened seed
            }
        nowhere = NetworkAddress 0 (sockToHostAddress (SockAddrInet 0 0))
        -- the zero address for a peer on a Unix socket, which has none
        there = NetworkAddress 0 (peerHostAddress sa)
        version = buildVersion c.net nonce 0 nowhere there (floor (utcTimeToPOSIXSeconds now))
        took = if expectsReject c.net version then Kept else Took
    node <- newPeer config mailbox
    let next =
          receive sub >>= \case
            PeerMessage _ m -> peerMessageDone node >> return m
            _ -> next
        handshake theirs ours
          | theirs && ours = return ()
          | otherwise =
              next >>= \case
                MVersion _ -> MVerAck `sendMessage` node >> handshake True ours
                MVerAck -> handshake theirs True
                _ -> handshake theirs ours
        answer =
          next >>= \case
            MPong (Pong n) | n == token -> return ()
            MPing (Ping n) -> MPong (Pong n) `sendMessage` node >> answer
            MReject Reject {message = MCTx, reason} -> do
              atomically (writeTVar refused (Just (T.strip (T.decodeUtf8Lenient reason.get))))
              answer
            _ -> answer
        talk = do
          MVersion version `sendMessage` node
          handshake False False
          atomically (writeTVar sent True)
          MTx tx `sendMessage` node
          MPing (Ping token) `sendMessage` node
          answer
    withAsync (peer config node inbox) $ \a -> do
      -- when the connection ends, what the node sent before it did may
      -- still be on its way to 'answer': give it a moment
      r <- timeout (seconds c.patience) (race (waitCatch a >> threadDelay 200000) talk)
      was <- readTVarIO sent
      why <- readTVarIO refused
      return $ case (why, r) of
        (Just reason, _) -> Rejected reason
        (Nothing, Just (Right ())) -> took
        (Nothing, Just (Left ())) -> if was then Closed else NoConnection
        (Nothing, Nothing) -> if was then NoAnswer else NoConnection

seconds :: NominalDiffTime -> Int
seconds t = max 0 (round (t * 1000000))
