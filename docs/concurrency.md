# Concurrency in haskoin-store

A store is a few dozen threads, a RocksDB database that one of them writes,
and a Redis that every store of a fleet shares. This document says, for each
concurrent protocol, who owns which state, which pure machine decides what
happens, and why no interleaving of threads, or of stores, can break what the
others rely on. It ends with what an audit of November 2026 found, what
changed, and what is left as a known limit. The threads of haskoin-node, on
which the store runs, are in that package's own `docs/concurrency.md`.

Read the part about a protocol before you change it, and keep this document
in step with the code.

## The rule

Every concurrent protocol is a pure state machine: its transitions are
functions of a module of their own, tested with QuickCheck through arbitrary
sequences of events, or a Lua script that Redis runs atomically, tested
against a model of it. The threads store the state and carry out what the
transitions decide. A race is ruled out in one of four ways, and each piece
of state below names which:

- **A single owner.** One thread changes the state; others at most read it.
- **One transaction per decision.** The state is read, the decision made and
  the new state written in one STM transaction.
- **One script per decision.** In Redis, a decision and its writes are one
  Lua script, which checks again what it depends on (the tip, the agreed
  block, the token of a move) and refuses with `raced` if it changed.
- **The order of one queue.** Events that must keep an order go through one
  mailbox that one thread reads.

## The block store

One thread, the block store's, reads one mailbox: node events, blocks and
transactions from peers, pongs, a clock tick, refill requests. It owns:

| State | Machine | Rule |
| --- | --- | --- |
| `sync`, the block download | `Haskoin.Store.BlockStore.Sync` | single owner |
| `pending`, transactions on their way | `Haskoin.Store.BlockStore.Pending` | one transaction per change |
| `refill`, `knows` | flags read and written by the owner | single owner |
| the reservation of the syncing peer | haskoin-node's `Lock`, by the store's own `owner` | single owner |
| the database | the writer, one RocksDB batch per block | single owner |

The rules of `Sync`, each a property of `SyncSpec`:

- At most one syncing peer; only `start` and `pass` set one.
- A block is accepted only from a peer that was asked for it.
- A peer that was asked for a block, and has not disconnected, is never
  taken for a peer that was not asked: it is the syncing peer, or among
  those passed over, until it delivers or the timeout runs out. `forget` is
  for a peer that disconnected; `finish` for one that is still connected,
  which keeps what it owes.
- A peer owes each block once, and a block is wanted once.

Readers of the database never see a block half written: a batch is one
write, and the web reads through a snapshot taken right after the commit
(`noteBest`), on the block store's thread.

**Transactions announced by peers** wait, merged by peer, for a thread of
their own (`askForTxs`), which takes all that wait in one transaction, asks
Redis once whether the fleet has seen them, and requests the rest. It reads
whether the store is in sync without changing anything (`inSyncNow`), and
notes its requests in `pending` in the transaction that checks there is
room for them: at most 2,000 wait for an answer (`requestWindow`). At most 100,000
ids wait; more are dropped with a warning, since a node announces a
transaction to a peer once.

## The fleet

Each store is a member of a fleet, alone or not. The state in Redis (the
tip, the pool, the leases and reports, the seen set, the cache of xpubs, the
events) is written only by the Lua scripts of `src/Haskoin/Store/Fleet/lua`,
each a single decision that checks again what it depends on. The pure model
is `Haskoin.Store.Fleet` (tip agreement, leases, who may empty the pool) and
`Haskoin.Store.Fleet.Pool` (the pool), and `RedisSpec` runs the scripts
against the model step by step.

The invariants the fleet relies on:

| | Invariant | Why it holds |
| --- | --- | --- |
| I1 | The tip, the pool and the cache's tip change only together | one script, `pool.lua` advance |
| I2 | Each pool input spends an output of the pool or one unspent at the tip; one spender per output; the pool is closed under parents | the offer checks the tip it was judged at, in the script |
| I3 | The tip moves only to a block that every live store has a view of | `report.lua` takes a report only from a store with a view of the tip, or alone; the advance checks the agreed block |
| I4 | The pool is emptied only when no live store could say the way | the advance empties for a way that the caller does not know only if the caller is the one live store (`mayEmptyAlone`), and only a store whose report was taken moves the tip |
| I5 | A store keeps a view of the fleet tip while it is live | `prune` decides what to keep in the transaction that lets views go, and a block joins the recent ones before its view is taken |
| I6 | A transaction is in the seen set only if some store will not need to fetch it again | a transaction in conflict stays waiting until the pool decides on it; expiry chooses and withdraws in one script |

The member's own state:

| State | Owner | Rule |
| --- | --- | --- |
| `views`, with `recent`, `reported`, `tip` | the member and the block store both prune | one transaction per decision (`keepViews`) |
| `waiting`, transactions to offer | written by the block store, read by the member | one transaction per change; at most `mostWaiting` |
| `built`, announcements of builds | the listener and the requests that wait | one transaction per change; a key only while a request waits |
| `healthy`, `owed`, `newBest`, `fresh`: what wakes the member | set by the web server's health check (`setHealthy`), the block store (`noteBest`) and whoever gives a transaction (`memberReceived`); cleared by the member | `memberLoop` reads them and clears what its turn sees to in one transaction, so a signal that comes during a turn wakes the next |

The member's turns follow `Haskoin.Store.Fleet.turn`: none while the store
is unwell, a whole turn at a new block, every two seconds and as soon as
the store is well again, and an offer alone as soon as a transaction comes.
`FleetSpec` replays any sequence of blocks, transactions, health changes
and timers through it, and checks that a well store leaves nothing waiting.

## The web server

| Protocol | Machine | Rule |
| --- | --- | --- |
| The xpub indexer: jobs, queue, waiters | `Haskoin.Store.XPub.Indexer`, one `queue` TVar | one transaction per change; a reservation is undone if its request is interrupted |
| Admission: slots and line | `Haskoin.Store.Web.Admission` | one transaction per change, masked |
| Events: the log and each client's place | `Haskoin.Store.Web.Events` | append-only log; a client's start is taken before its filters |
| Asking for published transactions | `Haskoin.Store.Web.Publish.due` and `asked` | each transaction's schedule moves on once it was asked for |
| Whether Redis answers (`cacheUp`) | `reach` | three slow answers in a row clear it, any quick one sets it |
| Background loops | `supervised` | a loop that fails is logged and started again |

## What the audit found, and what changed

Four readers went through the code in parallel, one for each part; every
finding was checked against the code before it was fixed.

**Confirmed, and fixed:**

1. **A good peer was disconnected after a stall.** When the store came in
   sync, the syncing peer was forgotten with what it owed; its late copy of a
   block then looked unrequested, and the peer was disconnected. Now
   `Sync.finish` keeps it among the peers passed over.
2. **The web thread changed the sync state.** The refill ran the in-sync
   check, which ends the syncing peer's turn, on the web server's thread. It
   now goes through the block store's mailbox.
3. **A peer could owe a block twice**, after `forgetRequests`, and be
   disconnected for the copy it no longer owed. `request` now checks what the
   peer owes.
4. **A block of an abandoned branch could be imported** if it arrived after
   the node moved to another chain, and the fleet moved to it until the store
   reverted it. The block store now checks that a block is on the node's main
   chain before it imports it, and a revert publishes the new best block.
5. **Two threads pruning views could let go of the view of the newest
   block**, deciding on lists read before the other changed them. Now the
   lists are read in the transaction that lets views go.
6. **A store that was not live could empty the pool of the fleet** while one
   other store was live: the script counted the live stores, not which.
   Now only the one live store may, and only a store whose report was taken
   moves the tip.
7. **A store emptied the pool where the documentation said it waits**, when
   it could not read the blocks of a reorganisation. That is now
   `Unreadable`, which waits, until the way is too long for every store.
8. **A transaction in conflict could be lost for the fleet**, which counts it
   as seen, when the second offer was not decided. It now waits for the next
   round.
9. **Expiry could withdraw a transaction that had just come back.** The
   script now chooses and withdraws in one call (`poolExpire`).
10. **A request interrupted while it took the lock of an xpub left its job
    reserved for good**, and every later request for the key waited in vain.
    `ask` now runs masked and undoes the reservation.
11. **The listener of announcements spun with no pause while Redis was
    down**, its map of keys grew without bound, and a reconnection or an
    abandoned build woke waiting requests as if a build were done, which
    made them answer 503. Now it backs off, counts only keys that requests
    wait for, wakes nobody at a reconnection, and announces only builds that
    are in the cache; a build that is not fails its job.
12. **One slow answer from Redis made every store answer 503** for up to
    five seconds. It now takes three in a row.
13. **Background loops of the web server could die in silence**, and the
    health check could hang on a connection to Redis that did. Every loop is
    supervised, and the read of the newest transaction has a timeout.
14. **An error while asking for published transactions skipped the others**,
    and could drop one at its last delay. Each one's schedule now moves on
    after it was asked for.
15. **Redis calls on the block store's thread held up blocks** behind an
    unbounded mailbox of announcements. They now run on their own thread,
    behind a bounded queue.
16. **The transactions a member held to offer had no bound** while Redis was
    away. At most 100,000 wait now.
17. **A store that a block made well took no turn for up to two seconds.**
    The member woke at the block, read the health the check had not yet
    updated, and slept until its timer; and a transaction waited for the
    next turn before it was in the pool, so listed. The member now waits on
    all its signals in one transaction (`Fleet.turn`), takes a whole turn
    when the store is well again, and offers each transaction as it comes.
18. **Chains of transactions entered the pool minutes late.** The member
    offered what waited oldest first, by the second, and in the order of the
    ids within a second: a chain of 24 Runes mints that a node took in one
    second was offered children first, and took up to 23 rounds, each trying
    the 2,000 oldest, orphans first. On groot's btc, after the restart into
    2.0.0, it fell 9,000 transactions behind its node. Each transaction now
    comes after its parents (`offerOrder`); an offer at an arrival offers
    only what arrived, and orphans wait for the whole turns. Announcements
    of a peer are merged instead of queued one by one, where a full queue of
    1,000 dropped them for good, and a store asks its peers for their
    mempools when it starts, as their nodes do not announce again what the
    store missed while it was away.

**Known limits, left as they are:**

- **Redis that fails over to a replica that lags**, or restarts from an old
  snapshot, keeps its epoch: the stores do not notice, and what was offered
  since is gone from the pool and from the seen set. The pool fills again
  from new announcements. Run Redis without replicas that can be promoted
  behind, or flush it, which the stores notice.
- **The stream of events keeps 10,000 entries.** A listener further behind
  loses what was trimmed, without an error.
- **A move of the tip that takes more than ten seconds** before its first
  piece can be aborted by another store. That costs time, never the state:
  the commit checks the token of the move.
- **The fleet tip follows its slowest healthy store**, whose own node may be
  hours behind the network.
- **`fleetMove`** sets the tip without the pool. It is for the tests; the
  stores move the tip with `fleetCommit` only.

## Checking a change

- A change to a machine changes its spec: add the property that the change
  keeps.
- A new piece of shared state names its owner and its rule in the tables
  above.
- A change to a Lua script is tested against Redis (`Haskoin.Store.Fleet.*`
  specs), and the pure model changes with it.
