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
-- * __No waiting for the echo.__ Until 1.12.1 the request waited, up to
--   @--publish-timeout@, for a node to announce the transaction back, and
--   said what came of it. The wait was arbitrary, Bitcoin Core does not say
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
--   the input scripts and the tokens ("docs/publishing.md" has what was
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

    -- * What the store can check
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

import Control.Monad (forM_, forever, unless, when)
import Data.Bits (shiftL, (.&.), (.|.))
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
    Network (..),
    NetworkAddress (..),
    OutPoint (..),
    Ping (..),
    Pong (..),
    Reject (..),
    Script (..),
    Tx (..),
    TxHash,
    TxIn (..),
    TxOut (..),
    VarString (..),
    isMinimalPush,
    isPushOp,
    nullOutPoint,
    sockToHostAddress,
    splitTokenPrefix,
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
import Haskoin.Store.Common (StoreRead (..))
import Haskoin.Store.Data (BlockRef (..), Spender (..), TxData (..), isCoinbaseTx)
import NQE (newMailbox, receive, withPublisher, withSubscription)
import Network.Socket (SockAddr (..))
import System.Random (randomIO)
import UnliftIO
  ( TVar,
    atomically,
    mapConcurrently,
    modifyTVar',
    newTVarIO,
    race,
    readTVarIO,
    stateTVar,
    timeout,
    waitCatch,
    withAsync,
    writeTVar,
  )
import UnliftIO.Concurrent (threadDelay)

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
  | BadVersion !Word32
  | NotPushOnly !Int
  | NotMinimal !Int
  | BadTokens !Int
  | TokensUnbacked !String
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
  show (BadVersion v) = "Nodes do not take a transaction of version " <> show v
  show (NotPushOnly i) = "The script of input " <> show i <> " does more than push data"
  show (NotMinimal i) = "The script of input " <> show i <> " pushes data in a longer form than it needs"
  show (BadTokens i) = "The tokens of output " <> show i <> " are not written as tokens are"
  show (TokensUnbacked c) = "The transaction gives tokens of category " <> c <> " that its inputs do not hold"

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
-- * __Input scripts__ only push data, each push in its shortest form. On
--   Bitcoin Cash that is a rule of the chain, and a node drops every
--   connection of a peer that sends a transaction that breaks it.
-- * __The version__: 1 or 2 on Bitcoin Cash, a rule of the chain there; 1
--   to 3 on Bitcoin, which nodes relay.
-- * __Tokens__ of Bitcoin Cash ('tokenRules'): written as tokens are, and
--   given only as the inputs hold them. A node drops every connection of a
--   peer that sends a transaction that breaks them.
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
  unless (tx.version `elem` versions) $ Left (BadVersion tx.version)
  mapM_ pushes (zip [0 ..] tx.inputs)
  spent <- mapM known (zip (map (.outpoint) tx.inputs) coins)
  when cash $ tokenRules tx spent
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
    cash = isJust rules.net.sigHashForkId
    -- Bitcoin Cash takes versions 1 and 2 only, by its rules since May
    -- 2023; Bitcoin Core relays those and version 3
    versions
      | cash = [1, 2]
      | otherwise = [1, 2, 3]
    -- an input script that only pushes data, each push in its shortest
    -- form: rules of Bitcoin Cash, relay rules of Bitcoin Core
    pushes (i, input) = case decode input.script of
      Right (Script ops)
        | not (all isPushOp ops) -> Left (NotPushOnly i)
        | not (all isMinimalPush ops) -> Left (NotMinimal i)
        | otherwise -> Right ()
      Left _ -> Left (NotPushOnly i)
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
                let handed = length [() | d <- delivered, d `elem` [Took, NoAnswer]]
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

-- | Every second, ask for each transaction whose time has come, unless the
-- store has it now. Runs until it is cancelled.
askLoop :: Asking -> (TxHash -> IO Bool) -> (TxHash -> IO ()) -> IO ()
askLoop (Asking var) present ask = forever $ do
  threadDelay 1000000
  now <- getCurrentTime
  ready <- atomically (stateTVar var (due now))
  forM_ ready $ \h ->
    present h >>= \case
      True -> atomically (modifyTVar' var (HashMap.delete h))
      False -> ask h

-- | When to ask for a transaction that was handed over: soon, for Bitcoin
-- Cash Node, and then every 25 seconds for two minutes and a half. The
-- delays are longer than the 20 seconds that the block store waits for a
-- transaction that it requested before it asks again.
askAgain :: [NominalDiffTime]
askAgain = [2, 25, 25, 25, 25, 25, 25]

-- | The tokens that an output holds: their category, a non-fungible token
-- with its capability and commitment, and an amount of fungible ones.
data Tokens = Tokens
  { category :: !ByteString,
    nft :: !(Maybe (Word8, ByteString)),
    amount :: !Word64
  }

-- | The tokens at the front of the script of an output, if it has any, or
-- 'Left' if they are not written as tokens are ('splitTokenPrefix').
readTokens :: ByteString -> Either () (Maybe Tokens)
readTokens script = case splitTokenPrefix script of
  Nothing -> Left ()
  Just (prefix, _)
    | B.null prefix -> Right Nothing
    | otherwise -> Right (Just (parse prefix))
  where
    -- the prefix is well formed: 'splitTokenPrefix' said so
    parse prefix =
      let flags = B.index prefix 33
          rest0 = B.drop 34 prefix
          (commitment, rest1)
            | flags .&. 0x40 /= 0 = let (n, r) = compact rest0 in B.splitAt (fromIntegral n) r
            | otherwise = (B.empty, rest0)
          held
            | flags .&. 0x10 /= 0 = fst (compact rest1)
            | otherwise = 0
       in Tokens
            { category = B.take 32 (B.drop 1 prefix),
              nft = if flags .&. 0x20 /= 0 then Just (flags .&. 0x0f, commitment) else Nothing,
              amount = held
            }
    compact :: ByteString -> (Word64, ByteString)
    compact b = case B.uncons b of
      Just (w, r)
        | w < 0xfd -> (fromIntegral w, r)
        | w == 0xfd -> sized 2 r
        | w == 0xfe -> sized 4 r
        | otherwise -> sized 8 r
      Nothing -> (0, B.empty)
    sized :: Int -> ByteString -> (Word64, ByteString)
    sized k r = (B.foldr (\w acc -> acc `shiftL` 8 .|. fromIntegral w) 0 (B.take k r), B.drop k r)

-- | The rules of the tokens of Bitcoin Cash (CashTokens, since May 2023),
-- given the outputs that the inputs spend, in order. A node drops every
-- connection of a peer that sends it a transaction that breaks them, so
-- the store checks them before it sends anything.
--
-- * The tokens of every output are written as tokens are.
-- * A category is new when it is the id of a transaction whose first
--   output an input spends: its tokens may be made at will.
-- * Of any other category, the outputs hold no more fungible tokens than
--   the inputs, and no more than the largest amount, in all.
-- * And its non-fungible tokens come from the inputs: any, when an input
--   holds one that can mint; else each output's equals one of an input
--   that cannot be changed, or takes the place of one that can be changed,
--   and none can mint.
tokenRules :: Tx -> [TxOut] -> Either Refusal ()
tokenRules tx spent = do
  outs <- mapM readOut (zip [0 ..] tx.outputs)
  let ins = [t | Right (Just t) <- map (readTokens . (.script)) spent]
      made = [t | Just t <- outs]
      new = [encode i.outpoint.hash | i <- tx.inputs, i.outpoint.index == 0]
      categories = HashMap.keys (HashMap.fromList [(t.category, ()) | t <- made])
  forM_ categories $ \c -> do
    let given = [t | t <- made, t.category == c]
        held = [t | t <- ins, t.category == c]
        total = sum (map (toInteger . (.amount)) given)
    when (total > 0x7fffffffffffffff) $ Left (TokensUnbacked (named c))
    unless (c `elem` new) $ do
      when (total > sum (map (toInteger . (.amount)) held)) $ Left (TokensUnbacked (named c))
      unless (nftsBacked [n | Just n <- map (.nft) given] [n | Just n <- map (.nft) held]) $
        Left (TokensUnbacked (named c))
  where
    readOut (i, o) = either (const (Left (BadTokens i))) Right (readTokens o.script)
    -- the id of a category as people write it: the bytes reversed
    named = concatMap hex . B.unpack . B.reverse
    hex w = [digits !! fromIntegral (w `div` 16), digits !! fromIntegral (w `mod` 16)]
    digits = "0123456789abcdef"
    -- capabilities: 0 none, 1 mutable, 2 minting
    nftsBacked given held
      | any ((== 2) . fst) held = True
      | any ((== 2) . fst) given = False
      | otherwise =
          let (fixed, changing) = partition ((== 0) . fst) given
              left = foldl matched ([n | n <- held, fst n == 0], []) fixed
              mutable = length (filter ((== 1) . fst) held)
           in length (snd left) + length changing <= mutable
    -- an immutable token given takes an equal one held, or waits for a
    -- mutable one
    matched (pool, unmatched) n = case break (== n) pool of
      (before, _ : after) -> (before <> after, unmatched)
      _ -> (pool, n : unmatched)

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

seconds :: NominalDiffTime -> Int
seconds t = max 0 (round (t * 1000000))
