{-# LANGUAGE DuplicateRecordFields #-}
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
-- = The problem
--
-- The store does not run scripts. It can check the signature of a coin
-- locked to one key, and cannot tell whether a script of another kind is
-- satisfied, so it cannot tell every valid transaction from a forged one.
-- Two things follow. It must not put a
-- transaction from a client into its mempool, or anyone could show a
-- payment that will never confirm. And it must not send one over the
-- connections it syncs from: a node that finds a transaction invalid may
-- drop every connection from the address that sent it, and Bitcoin Cash
-- Node does.
--
-- = What it does
--
-- The store does not broadcast. It hands the transaction to one full node
-- and listens for it to come back:
--
-- 1. __Check__ what the store can decide from its own index ('judge'): the
--    coins, the amounts, the lock time, the rules of relay that do not
--    change, and the signature of every coin locked to one key. A
--    transaction that the store has already is answered at once.
--
-- 2. __Send over RPC__, where the store was given the RPC of a full node
--    ('Publishing'.@direct@). The node checks everything, says why it
--    refuses, and what it refuses costs the store nothing. The next step
--    is for when there is no such node, or it does not answer.
--
-- 3. __Hand over__ ('deliver'), only a transaction whose every signature
--    the store verified: on a connection of its own, the courier, to
--    a node that the store syncs from. The transaction, then a @ping@. A
--    node handles the messages of a connection in order, so the @pong@
--    says that it is done with the transaction.
--
-- 4. __Listen__: the connections that the store syncs from never mentioned
--    the transaction, so a node that accepted it announces it there as it
--    does any other, and the store imports it as it does any other. That
--    announcement, the echo, is the only way into the mempool.
--
-- 5. __Answer__ when the transaction is in the mempool, or when the node
--    said that it refused it, or after 'Limits'.@wait@ seconds.
--
-- 6. __Remember__ ('Outbox'): a transaction on its way, and one that was
--    refused, are kept for a while by the hash of their bytes. The same
--    bytes posted again join the first request, or get its answer, and are
--    not sent again.
--
-- = Why it is this way
--
-- * __A connection of its own.__ A node never announces a transaction to
--   the connection that gave it, and answers @notfound@ when asked for it
--   there until its timer for announcements has run. So on the connection
--   that sent it there is no telling what the node made of it. On any
--   other connection the store is a peer that does not have the
--   transaction, and hears of it by the ordinary relay, which every node
--   does. Bitcoin Core does the same for its own transactions when told
--   to broadcast them privately.
--
-- * __The transaction, not an announcement of it.__ A node waits two
--   seconds before asking a peer it did not choose for what it announced,
--   and does not ask at all for what it knows or has refused.
--
-- * __One node.__ What is sent to one node reaches the others if it is
--   good. Sending it to many spreads what is bad.
--
-- * __No breaker.__ A node that closes the courier on a transaction has
--   found it invalid, and may have closed the other connections from this
--   address too. That transaction is not sent again. The store does not
--   stop publishing the others: someone who posts forged transactions
--   would then keep everyone else from publishing, which is worse than
--   what it guards against. The store connects to its node again, and a
--   node that whitelists the store closes nothing.
--
-- The alternatives: sending through a node's RPC would make the store
-- need an account on a node, where it needs only to be a peer; the RPC is
-- an option, to ask before sending. Running scripts here would need an
-- interpreter of every script of both chains, held to the nodes rule for
-- rule; the store checks a signature where there is no script to run.
-- Giving the transaction to
-- half the peers and listening on the others needs two peers, and puts the
-- connections that the store syncs from at risk.
--
-- = What it cannot do
--
-- Only a full node runs a script. A peer that is sent a transaction it
-- finds invalid may drop every connection of the sender, so the store
-- sends a peer only what it verified, and refuses a spend of a coin
-- that a script locks unless it has the RPC of a node to send it over. A node that
-- whitelists the store never does, and announces without delay, so a
-- store should be whitelisted by the nodes it is run next to. Bitcoin
-- Core does not say to a peer when it refuses a transaction: the client
-- learns it when the wait is over, unless the store asks over RPC.
module Haskoin.Store.Web.Publish
  ( -- * Publishing
    Publishing (..),
    Limits (..),
    Answer (..),
    Verdict (..),
    Direct (..),
    publish,

    -- * What the store can check
    Coin (..),
    Refusal (..),
    Rules (..),
    judge,
    isDust,
    maxStandardSize,
    coinOf,
    maxTxSize,

    -- * What is on its way
    Outbox,
    Slot (..),
    Ticket (..),
    emptyOutbox,
    enter,
    settle,

    -- * The courier
    Delivery (..),
    Courier (..),
    deliver,
  )
where

import Control.Monad (unless, when)
import Data.ByteString qualified as B
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.Maybe (listToMaybe)
import Data.Serialize (encode)
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Time.Clock (NominalDiffTime, UTCTime, addUTCTime, diffUTCTime, getCurrentTime)
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import Data.Word (Word32, Word64)
import Haskoin
  ( Hash256,
    Message (..),
    MessageCommand (..),
    Network (..),
    NetworkAddress (..),
    OutPoint (..),
    Ping (..),
    Pong (..),
    Reject (..),
    Tx (..),
    TxHash,
    BlockHeight,
    InputVerdict (..),
    TxIn (..),
    TxOut (..),
    VarString (..),
    doubleSHA256,
    nullOutPoint,
    sockToHostAddress,
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
    peer,
    peerMessageDone,
    sendMessage,
    wrapPeer,
  )
import Haskoin.Node.Peer.Lock (Lock (..))
import Haskoin.Store.Common (StoreEvent (..), StoreRead (..))
import Haskoin.Store.Data (BlockRef (..), Spender (..), TxData (..), isCoinbaseTx)
import NQE (Publisher, newMailbox, receive, withPublisher, withSubscription)
import Network.Socket (SockAddr (..))
import System.Random (randomIO)
import UnliftIO
  ( STM,
    TVar,
    atomically,
    newTVarIO,
    onException,
    race,
    readTVar,
    readTVarIO,
    retrySTM,
    stateTVar,
    timeout,
    waitCatch,
    withAsync,
    writeTVar,
  )
import UnliftIO.Concurrent (threadDelay)

-- | How long to wait, and how much to remember.
data Limits = Limits
  { -- | seconds that a request waits for the transaction to come back
    wait :: !NominalDiffTime,
    -- | seconds that the courier may take to connect and hand over
    handover :: !NominalDiffTime,
    -- | nodes to try when one cannot be reached
    tries :: !Int,
    -- | transactions that the outbox holds; more are turned away
    capacity :: !Int,
    -- | seconds that an answer other than success is remembered
    memory :: !NominalDiffTime
  }
  deriving (Eq, Show)

-- | What a full node, or the lack of one, made of a transaction.
data Verdict
  = -- | a node announced it back, or the store has it
    Accepted
  | -- | a node refused it and said why
    Refused !Text
  | -- | a node closed the connection on receiving it
    Dropped
  | -- | no node could be reached
    Unreachable
  | -- | a node took it and nothing came back in time
    Silent
  | -- | not sent: the store could not verify the signature of this input,
    -- and had no node to give the transaction to over RPC
    Unverified !Int
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
  = -- | what came of handing it to a node
    Verdict !Verdict
  | -- | refused by the store, before anything was sent
    Invalid !Refusal
  | -- | the outbox is full
    Busy
  deriving (Eq, Show)

-- | An output that a transaction spends, as the store knows it.
data Coin
  = -- | it is there to spend, or only an unconfirmed transaction spends
    -- it
    Coin
      { -- | the output, as it is in the transaction that made it
        output :: !TxOut,
        -- | the height of the block that confirmed it, if one did
        mined :: !(Maybe BlockHeight),
        -- | whether a coinbase made it
        coinbase :: !Bool
      }
  | -- | a confirmed transaction has spent it
    Spent !TxHash
  | -- | the store does not know it
    Missing
  deriving (Eq, Show)

-- | Why the store refuses a transaction without sending it.
data Refusal
  = TooLarge !Int
  | NoInputs
  | NoOutputs
  | IsCoinbase
  | DuplicateInput !OutPoint
  | UnknownOutput !OutPoint
  | SpentOutput !OutPoint !TxHash
  | Overspent
  | Immature !OutPoint
  | NotFinal !Word32
  | TooHeavy !Int
  | Dust !Int
  | Forged !Int
  deriving (Eq)

instance Show Refusal where
  show (TooLarge n) = "The transaction is too large: " <> show n <> " bytes"
  show NoInputs = "The transaction has no inputs"
  show NoOutputs = "The transaction has no outputs"
  show IsCoinbase = "A coinbase transaction cannot be published"
  show (DuplicateInput o) = "The transaction spends the same output twice: " <> showOutPoint o
  show (UnknownOutput o) = "The transaction spends an output that this server does not know: " <> showOutPoint o
  show (SpentOutput o by) =
    "The transaction spends an output that a confirmed transaction has spent: "
      <> showOutPoint o
      <> " by "
      <> cs (txHashToHex by)
  show Overspent = "The outputs of the transaction are worth more than its inputs"
  show (Immature o) = "The transaction spends a coinbase that is not a hundred blocks old: " <> showOutPoint o
  show (NotFinal lock) = "The lock time of the transaction has not passed: " <> show lock
  show (TooHeavy n) = "The transaction is larger than nodes relay: " <> show n
  show (Dust i) = "Output " <> show i <> " is worth less than it costs to spend, and nodes do not relay it"
  show (Forged i) = "The signature of input " <> show i <> " does not verify"

showOutPoint :: OutPoint -> String
showOutPoint o = cs (txHashToHex o.hash) <> ":" <> show o.index

-- | The largest transaction that a block of the network can hold, in
-- bytes. With SegWit a block weighs four million units at most, and a byte
-- of witness weighs one, so a transaction that is nearly all witness comes
-- close to four million bytes. Without it, the rule of Bitcoin Cash is a
-- million bytes for a transaction.
--
-- Nodes relay far smaller ones only, a hundred thousand bytes or so. The
-- store does not hold their rule, which is theirs to change: this is the
-- size past which no node can take the transaction.
maxTxSize :: Network -> Int
maxTxSize net
  | net.segWit = 4000000
  | otherwise = 1000000

-- | What the store knows of the chain and the network when it looks at a
-- transaction.
data Rules = Rules
  { net :: !Network,
    -- | what can be said of the signature of each input, given the
    -- outputs they spend: 'judgeInputs' of haskoin-core
    inputs :: !(Tx -> [TxOut] -> [InputVerdict]),
    -- | height of the best block
    height :: !BlockHeight,
    -- | median of the times of the last eleven blocks, which is the time
    -- that lock times are held to (BIP 113)
    medianTime :: !Word64
  }

-- | What the store can say of a transaction from its own index, given the
-- coin of each input in order ('coinOf').
--
-- Every check here is one whose answer a node would give too, and that
-- does not depend on what else is in a mempool:
--
-- * __What makes it no transaction__: no inputs or outputs, an input
--   twice, a coinbase, longer than a block can hold ('maxTxSize').
-- * __The coins__: each input spends an output that the store knows, that
--   no confirmed transaction has spent, and a coinbase only after a
--   hundred blocks. The outputs are not worth more than the inputs.
-- * __The lock time__ has passed, by height or by the median time, unless
--   every input waives it.
-- * __What nodes do not relay__: more than 'maxStandardSize', and an
--   output so small that spending it costs more than it holds ('isDust').
--   These are rules of relay and not of the chain; they are the ones that
--   have not changed in years.
-- * __Signatures__, where there is no script to run: an input that spends
--   a coin locked to one key with a signature that does not verify
--   ('judgeInputs' of haskoin-core). An input that is a script, or that
--   the store cannot read, is left to the node.
--
-- A conflict with an unconfirmed transaction is not a reason to refuse:
-- whether one may replace the other is for the node to say, and the
-- mempool of the store may hold what the network has dropped. The fee is
-- not checked either: what a node asks for changes with its mempool.
--
-- The answer for a transaction that passes says whether every signature
-- was verified, or names the first input that the store could not judge:
-- a script, a hash type other than all. Such a transaction may be good,
-- and only a full node can say. See 'publish' for what becomes of it.
judge :: Rules -> Tx -> [Coin] -> Either Refusal (Maybe Int)
judge rules tx coins = do
  when (size > maxTxSize rules.net) $ Left (TooLarge size)
  when (null tx.inputs) $ Left NoInputs
  when (null tx.outputs) $ Left NoOutputs
  when (any ((== nullOutPoint) . (.outpoint)) tx.inputs) $ Left IsCoinbase
  mapM_ (Left . DuplicateInput) (firstDuplicate (map (.outpoint) tx.inputs))
  spent <- mapM known (zip (map (.outpoint) tx.inputs) coins)
  -- sums of 64-bit values: compared as integers, which do not wrap
  unless (sum (map (toInteger . (.value)) spent) >= sum (map (toInteger . (.value)) tx.outputs)) $ Left Overspent
  mapM_ mature (zip (map (.outpoint) tx.inputs) coins)
  unless final $ Left (NotFinal tx.locktime)
  when (heavy > maxStandardSize rules.net) $ Left (TooHeavy heavy)
  mapM_ (Left . Dust) (take 1 [i | (i, o) <- zip [0 ..] tx.outputs, isDust rules.net o])
  let verdicts = zip [0 ..] (rules.inputs tx spent)
  mapM_ (Left . Forged) (take 1 [i | (i, InputForged) <- verdicts])
  return (listToMaybe [i | (i, InputUndecided) <- verdicts])
  where
    size = B.length (encode tx)
    -- what the limit of relay counts: weight with SegWit, bytes without
    heavy
      | rules.net.segWit = 3 * B.length (encode (tx {witness = []} :: Tx)) + size
      | otherwise = size
    known (_, Coin {output}) = Right output
    known (o, Spent by) = Left (SpentOutput o by)
    known (o, Missing) = Left (UnknownOutput o)
    -- a coinbase can be spent in the hundredth block after its own
    mature (o, Coin {coinbase = True, mined = Just h})
      | rules.height + 1 < h + 100 = Left (Immature o)
    mature _ = Right ()
    -- IsFinalTx of Bitcoin Core, for the block after the best one
    final
      | tx.locktime == 0 = True
      | tx.locktime < 500000000 = tx.locktime < rules.height + 1 || waived
      | otherwise = fromIntegral tx.locktime < rules.medianTime || waived
    waived = all ((== maxBound) . (.sequence)) tx.inputs
    firstDuplicate = go HashMap.empty
      where
        go _ [] = Nothing
        go seen (o : os)
          | HashMap.member o seen = Just o
          | otherwise = go (HashMap.insert o () seen) os

-- | The most that nodes relay: 400,000 units of weight with SegWit, which
-- is 100,000 bytes of a transaction without witness, and 100,000 bytes on
-- Bitcoin Cash.
maxStandardSize :: Network -> Int
maxStandardSize net
  | net.segWit = 400000
  | otherwise = 100000

-- | Whether an output is dust: worth less than three times what it costs,
-- at the lowest fee nodes relay, to make it and to spend it. Nodes do not
-- relay a transaction with one. This is @IsDust@ of Bitcoin Core at its
-- default of 3,000 satoshi a kilobyte, and of Bitcoin Cash Node: 546
-- satoshi for an output to a key hash, 294 to a witness key hash.
--
-- An output that cannot be spent, which starts with @OP_RETURN@, holds
-- data and may hold no value.
isDust :: Network -> TxOut -> Bool
isDust net o
  | unspendable = False
  | otherwise = toInteger o.value < 3 * toInteger (made + spent)
  where
    s = o.script
    unspendable = (not (B.null s) && B.head s == 0x6a) || B.length s > 10000
    made = 8 + lengthOfLength + B.length s
    lengthOfLength
      | B.length s < 0xfd = 1
      | B.length s <= 0xffff = 3
      | otherwise = 5
    -- what an input that spends it adds: less when its signature is in
    -- the witness
    spent
      | net.segWit && witnessProgram = 67
      | otherwise = 148
    witnessProgram =
      B.length s >= 4
        && B.length s <= 42
        && (B.head s == 0 || (B.head s >= 0x51 && B.head s <= 0x60))
        && fromIntegral (B.index s 1) == B.length s - 2

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

-- | A transaction of the outbox.
data Slot
  = -- | a request is publishing it
    Flying
  | -- | what came of it
    Done !Verdict
  deriving (Eq, Show)

-- | The transactions on their way and those that failed not long ago, by
-- the hash of their bytes.
--
-- The hash is of the bytes as posted, witness included. By the
-- transaction id, someone could post a transaction of another with its
-- witness spoiled, and have the good one answered as refused.
data Outbox = Outbox
  { slots :: !(HashMap Hash256 (UTCTime, Slot))
  }
  deriving (Eq, Show)

-- | An outbox with nothing on its way.
emptyOutbox :: Outbox
emptyOutbox = Outbox HashMap.empty

-- | What a request is told when it comes with a transaction.
data Ticket
  = -- | publish it: it has a slot now
    Fresh
  | -- | another request is publishing it: wait for that one
    Joined
  | -- | it failed not long ago, this way
    Known !Verdict
  | -- | no room
    Full
  deriving (Eq, Show)

-- | Come with a transaction. What is too old is forgotten first.
enter :: Limits -> UTCTime -> Hash256 -> Outbox -> (Ticket, Outbox)
enter limits now key old =
  case HashMap.lookup key box.slots of
    Just (_, Flying) -> (Joined, box)
    Just (_, Done v) -> (Known v, box)
    Nothing
      | HashMap.size box.slots >= limits.capacity -> (Full, box)
      | otherwise -> (Fresh, box {slots = HashMap.insert key (now, Flying) box.slots})
  where
    box = Outbox {slots = HashMap.filter kept old.slots}
    -- a request that died without settling does not hold its slot for ever
    kept (t, Flying) = diffUTCTime now t < limits.wait + 2 * limits.handover * fromIntegral limits.tries + 60
    kept (t, Done _) = diffUTCTime now t < limits.memory

-- | Say what came of a transaction. Success and a node that could not be
-- reached are not remembered: the first is in the database, and the
-- second is worth trying again at once. They stay only long enough for
-- the requests that joined to read them ('enter' forgets them).
settle :: UTCTime -> Hash256 -> Verdict -> Outbox -> Outbox
settle now key v box =
  box
    { slots = HashMap.insert key (stamp, Done v) box.slots
    }
  where
    stamp = case v of
      Accepted -> long
      Unreachable -> long
      -- the node may answer over RPC the next time
      Unverified _ -> long
      _ -> now
    -- older than any memory
    long = addUTCTime (-1000000000) now

-- | What publishing needs. The courier and the nodes are functions, so
-- that the rules can be tested without a network.
data Publishing = Publishing
  { limits :: !Limits,
    outbox :: !(TVar Outbox),
    -- | the chain as the store has it now
    rules :: !(IO Rules),
    -- | give the transaction to a full node over its RPC, where the store
    -- was given the RPC of one
    direct :: !(Tx -> IO Direct),
    -- | whether the store has the transaction, from the queries of the
    -- request
    has :: !(TxHash -> IO Bool),
    -- | the coin of an output, from the queries of the request ('coinOf')
    coin :: !(OutPoint -> IO Coin),
    -- | whether the store has the transaction now, read anew: the queries
    -- of a request see the database as it was when the request came
    present :: !(TxHash -> IO Bool),
    -- | what the store tells its clients
    events :: !(Publisher StoreEvent),
    -- | whether the store has a mempool to see the transaction come into
    mempool :: !Bool,
    -- | the nodes to hand a transaction to, the first first
    nodes :: !(IO [SockAddr]),
    -- | hand a transaction to a node
    courier :: !(SockAddr -> Tx -> IO Delivery),
    note :: !(LogLevel -> Text -> IO ())
  }

-- | Publish a transaction, and say what came of it. See the module header.
publish :: Publishing -> Tx -> IO Answer
publish p tx = do
  known <- p.has txid
  if known
    then return (Verdict Accepted)
    else
      (judge <$> p.rules <*> pure tx <*> mapM (p.coin . (.outpoint)) tx.inputs) >>= \case
        Left r -> return (Invalid r)
        Right unverified -> do
          now <- getCurrentTime
          atomically (stateTVar p.outbox (enter p.limits now key)) >>= \case
            Full -> p.note LevelWarn "Outbox full: transaction turned away" >> return Busy
            Joined -> Verdict <$> joined
            -- sent already, and the node may only have been slow to announce
            Known Silent -> Verdict <$> listening (return Silent)
            Known v -> return (Verdict v)
            Fresh -> do
              v <- listening (send unverified) `onException` done Unreachable
              done v
              say v
              return (Verdict v)
  where
    txid = txHash tx
    key = doubleSHA256 (encode tx)
    name = txHashToHex txid
    done v = do
      now <- getCurrentTime
      atomically (stateTVar p.outbox (\b -> ((), settle now key v b)))
    joined =
      timeout (seconds (p.limits.wait + 2 * p.limits.handover)) (atomically (slotDone p.outbox key)) >>= \case
        Just (Just v) -> return v
        -- forgotten already: it was accepted, or no node was reached
        Just Nothing -> (\has -> if has then Accepted else Unreachable) <$> p.present txid
        Nothing -> return Silent
    -- Listen before sending, so that the echo cannot come unheard, and
    -- look again for the transaction once listening: it may have come
    -- since the request read the database.
    listening send =
      withSubscription p.events $ \sub ->
        p.present txid >>= \case
          True -> return Accepted
          False ->
            send >>= \case
              Silent
                | not p.mempool -> return Accepted
                | otherwise ->
                    timeout (seconds p.limits.wait) (echo sub) >>= \case
                      Just () -> return Accepted
                      -- an event may have been missed, never a transaction
                      Nothing -> (\has -> if has then Accepted else Silent) <$> p.present txid
              v -> return v
    echo sub =
      receive sub >>= \case
        StoreMempoolNew h | h == txid -> return ()
        _ -> echo sub
    -- Over the RPC of a node if the store has one that answers: the node
    -- checks everything, says why it refuses, and nothing it refuses
    -- costs the store its connections. Else to a peer, and then only what
    -- the store verified itself.
    send unverified =
      p.direct tx >>= \case
        Sent -> return Silent
        Declined why -> return (Refused why)
        Unavailable -> maybe handOver (return . Unverified) unverified
    -- 'Silent' here: the node has it, and has not said that it refused it
    handOver = p.nodes >>= go . take p.limits.tries
      where
        go [] = return Unreachable
        go (sa : more) =
          p.courier sa tx >>= \case
            Took -> return Silent
            NoAnswer -> return Silent
            Rejected why -> return (Refused why)
            Closed -> return Dropped
            NoConnection -> go more
    say Accepted = p.note LevelDebug ("Published " <> name)
    say (Refused why) = p.note LevelInfo ("A node refused " <> name <> ": " <> why)
    say Dropped = p.note LevelWarn ("A node closed the connection on receiving " <> name)
    say Unreachable = p.note LevelWarn ("No node to publish " <> name <> " to")
    say (Unverified i) =
      p.note LevelInfo ("Not sent to a peer, input " <> T.pack (show i) <> " not verified: " <> name)
    say Silent = p.note LevelInfo ("No node announced " <> name <> " after taking it")

-- | Wait for a transaction that another request is publishing.
slotDone :: TVar Outbox -> Hash256 -> STM (Maybe Verdict)
slotDone var key =
  HashMap.lookup key . (.slots) <$> readTVar var >>= \case
    Just (_, Flying) -> retrySTM
    Just (_, Done v) -> return (Just v)
    Nothing -> return Nothing

seconds :: NominalDiffTime -> Int
seconds t = max 0 (round (t * 1000000))

-- | What a node did with a transaction handed to it.
data Delivery
  = -- | it answered the ping that followed: it is done with the
    -- transaction, and said nothing against it
    Took
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
-- own, so nothing of it reaches the peer manager or the block store.
deliver :: Courier -> SockAddr -> Tx -> IO Delivery
deliver c sa tx =
  withPublisher $ \pub -> withSubscription pub $ \sub -> do
    (inbox, mailbox) <- newMailbox
    busy <- newTVarIO Free
    pending <- newTVarIO 0
    sent <- newTVarIO False
    refused <- newTVarIO Nothing
    nonce <- randomIO
    token <- randomIO
    now <- getCurrentTime
    let config =
          PeerConfig
            { pub = pub,
              net = c.net,
              label = "courier " <> cs (show sa),
              connect = c.connect sa,
              logger = c.logger,
              lookupBlock = const (return Nothing)
            }
        node = wrapPeer config busy pending mailbox
        nowhere = NetworkAddress 0 (sockToHostAddress (SockAddrInet 0 0))
        there = NetworkAddress 0 (sockToHostAddress sa)
        version = buildVersion c.net nonce 0 nowhere there (floor (utcTimeToPOSIXSeconds now))
        next =
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
        (Nothing, Just (Right ())) -> Took
        (Nothing, Just (Left ())) -> if was then Closed else NoConnection
        (Nothing, Nothing) -> if was then NoAnswer else NoConnection
