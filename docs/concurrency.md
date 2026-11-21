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
sequences of events; or a decision about the state in Redis, made in
Haskell and carried out in one step; or a short Lua script that Redis runs
atomically, tested against a model of it. The threads store the state and
carry out what the transitions decide. A race is ruled out in one of four
ways, and each piece of state below names which:

- **A single owner.** One thread changes the state; others at most read it.
- **One transaction per decision.** The state is read, the decision made and
  the new state written in one STM transaction.
- **One step per decision.** In Redis, a decision and its writes are one
  step: a Lua script that checks again what it depends on (the tip, the
  agreed block, the token of a move) and refuses with `raced` if it
  changed; or, for the shared mempool, a decision made in Haskell from
  what Redis said, which a script carries out only if Redis still says
  all of it ("Decisions in one step", below).
- **The order of one queue.** Events that must keep an order go through one
  mailbox that one thread reads.

## The block store

One thread, the block store's, reads one mailbox: node events, blocks from
peers, pongs, a clock tick. It owns:

| State | Machine | Rule |
| --- | --- | --- |
| `sync`, the block download | `Haskoin.Store.BlockStore.Sync` | single owner |
| `knows`, what each peer said it has | read and written by the owner | single owner |
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

## The path of a transaction into the pool

**What peers name and send** is no business of the block store. One
thread, the runner of `Haskoin.Store.Ingest.Run`, owns one pure machine,
`Haskoin.Store.Ingest`, and gives it everything that happens, in the order
of one queue: a peer that comes or goes, what it names, a transaction, a
`notfound`, the clock, whether the store is in sync, and the answers of
the pool to what the machine asked. The machine decides; the runner
carries out.

The RPC of the node, when the store has it, is a source of the path with a
thread of its own (`ingestFeed`). To the machine it is a peer: one that
comes when the web server starts, goes when a call fails, and comes again,
as a new peer, when it is tried again. The thread decides nothing either:
it makes the calls that the machine asked for and puts the answers in the
queue of events.

`docs/mempool-ingestion.md` has what the machine guarantees and how each
guarantee is established, by a proof, by a model checker that goes through
every order of events, or by a test.

| State | Owner | Rule |
| --- | --- | --- |
| the machine: where each id is, the window of each peer | the runner's thread | single owner; every change is `step` |
| the queue of events | written by the thread that reads the node's events, read by the runner | events of peers are dropped past 20,000, and the machine is told that something was lost |
| `state`, what `/health` reads | written by the runner after each step | one transaction per write |
| what is asked of the RPC of the node | written by the runner, read by the thread of the feed (`ingestFeed`) | a queue for each time the RPC is reached, named in the event that tells the machine of it; what the feed answers goes into the queue of events, and waits for room there |
| the number of the next feed | the thread of the feed | read and added to in the transaction that tells the machine of the feed, so no number is used twice (U1) |

## The fleet

Each store is a member of a fleet, alone or not. The state in Redis (the
tip, the pool, the leases and reports, the seen set, the cache of xpubs, the
events) is written only in single steps, each a decision that checks again
what it depends on. The pool, the seen set, the events of the pool and the
move of the tip with the pool are written by the decisions of
`Haskoin.Store.Fleet.Pool.Decide`; the reports, the move of the tip alone
and the cache of xpubs by the Lua scripts of `src/Haskoin/Store/Fleet/lua`.
The pure model is `Haskoin.Store.Fleet` (tip agreement, leases, who may
empty the pool) and `Haskoin.Store.Fleet.Pool` (the pool), and `RedisSpec`
runs both against the model step by step.

### Decisions in one step

The changes to the pool were one Lua script of eight hundred lines. Redis
ran it alone, which made each change one step, and it had two faults. A
script that fails after its first write keeps what it wrote, since Redis
undoes nothing: a field that was not there, or a number that Redis did not
take, left the pool half changed. And the rules were kept twice, in Lua to
run and in Haskell to test against, joined by tests that draw sequences
at random; a bug of 19 November was in that gap.

Now a change is a **decision** (`Haskoin.Store.Fleet.Atomic`):

1. It is a computation that can only ask Redis questions and add to a
   list of writes. It runs in any monad, so its type says that it does
   nothing else. Every question is kept with its answer.
2. One small script (`lua/atomic.lua`) is given the questions with their
   answers, and the writes. It asks every question again. If one answer
   differs it writes nothing and says so, and the decision is made again.
   If all are the same it carries out the writes.

Why that is one step. Redis runs the script alone (R1), so the answers
are all true at one moment, that of the script. The decision is a
function of its answers and of nothing else. So the writes are what the
decision would have written had it run inside Redis at that moment.

Why it cannot fail half way. The script compares every answer before its
first write, and then runs plain commands with nothing to decide between
them. A fault in a decision is an exception in Haskell, before Redis is
told anything.

What it rests on, and how each part is established:

| Part | How |
| --- | --- |
| A decision does nothing but ask and write | by its type: it is run as `forall m. Monad m => Atomic m a` |
| The script compares before it writes, and decides nothing | by reading it: a hundred lines, with no rule of the fleet in them |
| A decision sees its own writes as Redis would say them (`replay`) | tested: 5,000 drawn cases against the pure model of Redis, and 300 against a Valkey; where it cannot tell, it stops with an error and guesses nothing |
| The pure model of Redis says what Redis says | tested against a Valkey: any writes, then every question |
| A decision whose answers changed writes nothing | tested against a Valkey, with a set and a hash of 200 members in whatever order Redis lists them |
| No change is lost when many decide at once | tested: eight threads, four hundred additions to one number |
| Each key has one kind, so a plain write cannot be refused | by convention of `Haskoin.Store.Fleet.Pool.Decide`, not by a type |
| The decisions do what the script did | tested: the script and the decisions side by side under two prefixes of one Valkey, 300 drawn sequences of offers, withdrawals, expiries and moves of the tip of each kind, all that Redis has compared after every step (`Haskoin.Store.Fleet.DecideSpec`) |

None of this is proved. What changed is where a fault can be: in a pure
function, where it is an exception before any write, and no longer in
the middle of a script.

What it costs. An offer that is taken is two calls to Redis where it was
one, and Redis spends about 137 microseconds on it where the script took
74 (Valkey's own count, 2,000 offers). A decision is gone through more
than once to learn what it asks, so that Redis is asked in a few calls
and not once for each question.

Stores that keep the pool in this way have keys that start with `hs3:`,
and those that ran the script `hs2:`: on one Redis they are two fleets.

The invariants the fleet relies on:

| | Invariant | Why it holds |
| --- | --- | --- |
| I1 | The tip, the pool and the cache's tip change only together | one decision, `advance`, carried out in one step |
| I2 | Each pool input spends an output of the pool or one unspent at the tip; one spender per output; the pool is closed under parents | the offer checks the tip it was judged at, in the same step |
| I3 | The tip moves only to a block that every live store has a view of | `report.lua` takes a report only from a store with a view of the tip, or alone; the advance checks the agreed block |
| I4 | The pool is emptied only when no live store could say the way | the advance empties for a way that the caller does not know only if the caller is the one live store (`mayEmptyAlone`), and only a store whose report was taken moves the tip |
| I5 | A store keeps a view of the fleet tip while it is live | `prune` decides what to keep in the transaction that lets views go, and a block joins the recent ones before its view is taken |
| I6 | A transaction is in the seen set only if some store will not need to fetch it again | a transaction in conflict stays waiting until the pool decides on it; expiry chooses and withdraws in one step |

The member's own state:

| State | Owner | Rule |
| --- | --- | --- |
| `views`, with `recent`, `reported`, `tip` | the member and the block store both prune | one transaction per decision (`keepViews`) |
| `built`, announcements of builds | the listener and the requests that wait | one transaction per change; a key only while a request waits |
| `healthy`, `owed`, `newBest`: what wakes the member | set by the web server's health check (`setHealthy`) and the block store (`noteBest`); cleared by the member | `memberLoop` reads them and clears what its turn sees to in one transaction, so a signal that comes during a turn wakes the next |

The member's turns follow `Haskoin.Store.Fleet.turn`: none while the store
is unwell, one at a new block, every two seconds and as soon as the store
is well again. `FleetSpec` replays any sequence of blocks, health changes
and timers through it, and checks that a well store leaves nothing
waiting. A turn offers no transaction: the path into the pool does, each
as it comes (`memberOffer`).

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
19. **The path of a transaction into the pool had no statement of what it
    must do**, and items 15 to 18 were patches to five stages that each
    lost transactions in their own way: the fixes of item 18 themselves
    asked a node for 57,000 transactions at once, and lost its answer when
    a header came first. The stages are gone. One machine,
    `Haskoin.Store.Ingest`, replaces the queue of announcements, the set
    of requests, the member's list of what waits and the question to the
    peers at the start (`docs/mempool-ingestion.md`).

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
- A change to a decision of the pool is tested against the pure model
  (`RedisSpec`). `DecideSpec` then fails, since the script that it
  compares with has not changed: say there what differs and why, or drop
  the comparison once the script is no longer what the stores should do.
- A decision writes to a key only as the kind that the key has.
