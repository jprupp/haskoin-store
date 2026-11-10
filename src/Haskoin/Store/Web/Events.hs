{-# LANGUAGE MultiWayIf #-}

-- |
-- Module      : Haskoin.Store.Web.Events
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- What the clients of @\/events@ are told: a log of the last events that
-- every client reads at its own pace, and the choice that a client makes of
-- the transactions it wants to hear about.
--
-- = One log instead of a queue for each client
--
-- Every client used to have a queue of its own, filled as events came. A
-- queue knows nothing of what came before it was made, so a client that
-- lost its connection for a second had no way to ask for what it missed,
-- and had to read everything again.
--
-- Here the events are kept once, numbered, in an t'EventLog' of the last
-- 'logLength'. A client is a number, the next event that it has not read
-- ('readEvents'). A client that connects starts at the end
-- ('eventsEnd'), or after an event that it names by its hash
-- ('eventsAfter'), which is how it takes up where it left. A client that
-- is slower than the events come falls off the start of the log and is
-- told so, as it was when its queue filled.
--
-- An event is named by its hash and not by its number, because the number
-- is that of one server since it started. In a fleet every server reads
-- the same stream of events from Redis, from the start of what Redis
-- keeps, so the logs of the servers hold the same events in the same
-- order, and a client may come back to another server than the one it
-- left.
--
-- A transaction can be in the log twice, when it entered the mempool and
-- when it left. A client that names it is taken to have heard the first,
-- and hears everything after it again: an event told twice costs a
-- request, and an event not told is a payment that a wallet does not show.
--
-- = Choosing transactions
--
-- A client of the WebSocket can say which transactions it cares for, with
-- the commands of t'EventCommand': addresses, extended public keys, and a
-- Bloom filter as in BIP 37. What it chose is its t'Watch', and 'relevant'
-- decides for each transaction. So that it can, the log keeps the
-- transaction and its addresses with the event (see t'Seen'), read once when
-- the event comes, whatever the number of clients.
--
-- When the server does not know the transaction of an event, every client
-- is told of it. That happens when a transaction leaves the mempool long
-- after the log forgot that it entered, and it errs on the side of telling
-- too much.
--
-- An extended public key is watched as a wallet uses it: the addresses in
-- use and the gap after them on each of its two chains. When a transaction
-- touches one, the gap moves on ('relevant' returns the new t'Watch'), so
-- the client goes on hearing of payments to addresses it had not been
-- given yet when it subscribed. The same key may be watched under each
-- type of address, with a command for each.
--
-- The Bloom filter is tested as a node tests it for an SPV client
-- ('bloomRelevantUpdate'), except that the server does not add the
-- outputs of a matching transaction to the filter: the filter stays as the
-- client made it, and the client adds what it wants to follow
-- ('EventFilterAdd'). A filter that changed behind the back of the client
-- could not be told apart from its own after a few matches.
module Haskoin.Store.Web.Events
  ( -- * Log
    EventLog,
    Happened (..),
    Entry (..),
    Seen (..),
    seenOf,
    newEventLog,
    appendEvent,
    eventsEnd,
    eventsAfter,
    readEvents,
    logLength,
    logWeight,

    -- * Choice of a client
    Watch (..),
    everything,
    nothing,
    relevant,
    command,
    maxWatchAddresses,
    maxWatchXPubs,
  )
where

import Control.Concurrent.STM
  ( STM,
    TVar,
    atomically,
    modifyTVar',
    newTVarIO,
    readTVar,
    retry,
    stateTVar,
  )
import Data.ByteString qualified as B
import Data.Either (rights)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.IntMap.Strict qualified as IntMap
import Data.Maybe (isJust, mapMaybe)
import Data.Sequence (Seq, ViewL (..), (|>))
import Data.Sequence qualified as Seq
import Data.Word (Word32, Word64)
import Haskoin
  ( Address,
    BlockHash,
    BloomFilter,
    Ctx,
    Network,
    Tx (..),
    TxHash,
    TxOut (..),
    bloomInsert,
    bloomRelevantUpdate,
    pubSubKey,
    scriptToAddressBS,
  )
import Haskoin.Store.Common (deriveFunction)
import Haskoin.Store.Data
  ( Event (..),
    EventCommand (..),
    Prev (..),
    TxData (..),
    XPubBal (..),
    XPubSpec (..),
    nullBalance,
  )

-- | What the server knows of a transaction that an event is about: enough
-- to say whether a client wants to hear of it.
data Seen = Seen
  { -- | the transaction, for the Bloom filter
    tx :: !Tx,
    -- | the addresses that it pays to and spends from
    addresses :: !(HashSet Address),
    -- | roughly the bytes that this takes in memory
    size :: !Int
  }
  deriving (Eq, Show)

-- | A transaction as the log keeps it.
seenOf :: Network -> Ctx -> TxData -> Seen
seenOf net ctx td =
  Seen
    { tx = td.tx,
      addresses = HashSet.fromList (rights (map (scriptToAddressBS net ctx) scripts)),
      size = 256 + sum (map B.length scripts) + sum (map (sum . map B.length) td.tx.witness)
    }
  where
    scripts = map (.script) (IntMap.elems td.prevs) <> map (.script) td.tx.outputs

-- | Something that the clients are told.
data Happened
  = -- | the best block changed to this one
    BlockArrived !BlockHash
  | -- | a transaction entered the mempool, with what is known of it
    TxEntered !TxHash !(Maybe Seen)
  | -- | a transaction left the mempool unconfirmed
    TxLeft !TxHash
  deriving (Eq, Show)

-- | An event in the log.
data Entry = Entry
  { -- | its place: one more than that of the event before
    number :: !Word64,
    -- | what the client is sent
    event :: !Event,
    -- | the transaction, when the event is about one that the server knows
    seen :: !(Maybe Seen),
    -- | the bytes that this entry is counted for in the log
    weight :: !Int
  }
  deriving (Eq, Show)

-- | The last events, oldest first.
data Log = Log
  { entries :: !(Seq Entry),
    -- | the number of the next event
    next :: !Word64,
    -- | the sum of the weights of the entries
    held :: !Int,
    -- | the transactions of the entries that say that one entered, to
    -- know them again when they leave
    bodies :: !(HashMap TxHash Seen)
  }

-- | The log of events that the clients of a server read.
newtype EventLog = EventLog (TVar Log)

-- | The most events that the log keeps: how far back a client can take up
-- from, and how far behind it may fall.
logLength :: Int
logLength = 10000

-- | The most bytes of transactions that the log keeps. Events of large
-- transactions make the log shorter instead of making the server larger.
logWeight :: Int
logWeight = 64 * 1024 * 1024

-- | An empty log.
newEventLog :: IO EventLog
newEventLog = EventLog <$> newTVarIO Log {entries = Seq.empty, next = 0, held = 0, bodies = HashMap.empty}

-- | Add an event at the end of the log, and drop from the start what the
-- log has no room for.
appendEvent :: EventLog -> Happened -> STM ()
appendEvent (EventLog var) what = modifyTVar' var $ \l ->
  let entry = case what of
        BlockArrived h -> Entry {number = l.next, event = EventBlock h, seen = Nothing, weight = 0}
        TxEntered h s -> Entry {number = l.next, event = EventTx h, seen = s, weight = maybe 0 (.size) s}
        TxLeft h -> Entry {number = l.next, event = EventTx h, seen = HashMap.lookup h l.bodies, weight = 0}
      bodies = case what of
        TxEntered h (Just s) -> HashMap.insert h s l.bodies
        _ -> l.bodies
   in trim Log {entries = l.entries |> entry, next = l.next + 1, held = l.held + entry.weight, bodies}
  where
    trim l = case Seq.viewl l.entries of
      e :< rest
        | Seq.length l.entries > logLength || l.held > logWeight ->
            trim Log {entries = rest, next = l.next, held = l.held - e.weight, bodies = forget e l.bodies}
      _ -> l
    forget e = case e.event of
      EventTx h | e.weight > 0 -> HashMap.delete h
      _ -> id

-- | Where a client that wants only what comes from now on starts reading.
eventsEnd :: EventLog -> STM Word64
eventsEnd (EventLog var) = (.next) <$> readTVar var

-- | Where a client that has heard of an event starts reading, given the
-- hash of the block or of the transaction: after the first event about
-- it. 'Nothing' when the log has no such event, because it never had or
-- because it is too long ago; the client then has to read what it follows
-- again and start from the end.
eventsAfter :: EventLog -> Maybe BlockHash -> Maybe TxHash -> STM (Maybe Word64)
eventsAfter (EventLog var) block tx = do
  l <- readTVar var
  let is e = case e.event of
        EventBlock h -> Just h == block
        EventTx h -> Just h == tx
  return $ (\i -> (Seq.index l.entries i).number + 1) <$> Seq.findIndexL is l.entries

-- | The events from a number on, waiting for one when there is none yet.
-- 'Nothing' when the log no longer has that number: the client fell too
-- far behind.
readEvents :: EventLog -> Word64 -> STM (Maybe (Seq Entry))
readEvents (EventLog var) from = do
  l <- readTVar var
  let first = l.next - fromIntegral (Seq.length l.entries)
  if
    | from < first -> return Nothing
    | from >= l.next -> retry
    | otherwise -> return (Just (Seq.drop (fromIntegral (from - first)) l.entries))

-- | What a client of the WebSocket chose to hear about.
data Watch = Watch
  { -- | whether it chose at all; a client that did not hears everything
    filtered :: !Bool,
    -- | the addresses that it named
    plain :: !(HashSet Address),
    -- | the keys that it named, with the number of addresses derived on
    -- the external and on the change chain
    keys :: !(HashMap XPubSpec (Word32, Word32)),
    -- | the addresses derived from the keys: the key, the chain and the
    -- index of each
    derived :: !(HashMap Address (XPubSpec, Word32, Word32)),
    -- | its Bloom filter
    bloom :: !(Maybe BloomFilter)
  }
  deriving (Eq, Show)

-- | The choice of a client that has not chosen: every transaction.
everything :: Watch
everything =
  Watch {filtered = False, plain = HashSet.empty, keys = HashMap.empty, derived = HashMap.empty, bloom = Nothing}

-- | The choice of a client that wants blocks alone, until it names
-- something to watch.
nothing :: Watch
nothing = everything {filtered = True}

-- | The most addresses that a client may name. Those derived from keys do
-- not count.
maxWatchAddresses :: Int
maxWatchAddresses = 100000

-- | The most extended public keys that a client may name. Each is read
-- from the database when it is named, as a request about it is.
maxWatchXPubs :: Int
maxWatchXPubs = 100

-- | Whether a client is told of an event, given what the log knows of its
-- transaction, and its choice afterwards: the gap of a key moves on when
-- the transaction touches one of its addresses. The gap is the number of
-- unused addresses to watch after the last one used.
--
-- A client that has not chosen is told of everything, and every client is
-- told of a block and of a transaction that the server does not know.
relevant :: Ctx -> Word32 -> Maybe Seen -> Watch -> (Bool, Watch)
relevant _ _ Nothing w = (True, w)
relevant ctx gap (Just s) w
  | not w.filtered = (True, w)
  | otherwise = (named || not (null hits) || matched, foldl' moved w hits)
  where
    touched = HashSet.toList s.addresses
    named = any (`HashSet.member` w.plain) touched
    hits = mapMaybe (`HashMap.lookup` w.derived) touched
    matched = maybe False (\f -> isJust (bloomRelevantUpdate ctx f s.tx)) w.bloom
    moved now (spec, chain, index) = extend ctx spec chain (index + 1 + gap) now

-- | Derive the addresses of a watched key on one chain up to a number of
-- them, if there are fewer.
extend :: Ctx -> XPubSpec -> Word32 -> Word32 -> Watch -> Watch
extend ctx spec chain upto w = case HashMap.lookup spec w.keys of
  Just (external, change)
    | upto > have ->
        w
          { keys = HashMap.insert spec (if chain == 0 then (upto, change) else (external, upto)) w.keys,
            derived = foldl' (\m i -> HashMap.insert (derive i) (spec, chain, i) m) w.derived [have .. upto - 1]
          }
    where
      have = if chain == 0 then external else change
      derive = deriveFunction ctx spec.deriv (pubSubKey ctx spec.key chain)
  _ -> w

-- | Do what a client asked with a command, and say why not when it cannot
-- be done, in which case the connection is to be closed. The function
-- reads the balances of an extended public key from the database.
command :: Ctx -> Word32 -> (XPubSpec -> IO [XPubBal]) -> TVar Watch -> EventCommand -> IO (Either String ())
command ctx gap balances var = \case
  EventWatch as -> change $ \w ->
    let plain = foldl' (flip HashSet.insert) w.plain as
     in if HashSet.size plain > maxWatchAddresses
          then Left "Too many addresses"
          else Right (chosen w) {plain}
  EventUnwatch as -> change $ \w -> Right (chosen w) {plain = foldl' (flip HashSet.delete) w.plain as}
  EventWatchXPub spec -> do
    -- read before the choice changes: nothing is held while the database
    -- is read
    used <- balances spec
    let upto chain = gap + maximum (0 : [i + 1 | XPubBal {path = [c, i], balance = b} <- used, c == chain, inUse b])
    change $ \w ->
      if
        | HashMap.member spec w.keys -> Right (chosen w)
        | HashMap.size w.keys >= maxWatchXPubs -> Left "Too many extended public keys"
        | otherwise ->
            let named = (chosen w) {keys = HashMap.insert spec (0, 0) w.keys}
             in Right (extend ctx spec 1 (upto 1) (extend ctx spec 0 (upto 0) named))
  EventUnwatchXPub spec -> change $ \w ->
    Right
      (chosen w)
        { keys = HashMap.delete spec w.keys,
          derived = HashMap.filter (\(s, _, _) -> s /= spec) w.derived
        }
  EventFilterLoad f -> change $ \w -> Right (chosen w) {bloom = Just f}
  EventFilterAdd d -> change $ \w -> case w.bloom of
    Nothing -> Left "No filter to add to"
    Just f -> Right w {bloom = Just (bloomInsert f d)}
  EventFilterClear -> change $ \w -> Right (chosen w) {bloom = Nothing}
  EventAll -> change (const (Right everything))
  EventNone -> change (const (Right nothing))
  where
    chosen w = w {filtered = True}
    inUse b = not (nullBalance b)
    change :: (Watch -> Either String Watch) -> IO (Either String ())
    change f = atomically . stateTVar var $ \w -> case f w of
      Left why -> (Left why, w)
      Right new -> (Right (), new)
