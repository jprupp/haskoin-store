# Xpubs: answered from the fleet's cache

How the store answers a question about an extended public key (xpub).
The architecture around it is in [one-fleet.md](one-fleet.md).

## In short

A wallet asks about one key that stands for all of its addresses.
Reading each of them from the database for every request is slow. So
every answer about an xpub comes from one cache that the fleet shares in
Redis (`Haskoin.Store.Fleet.XPub`), never straight from the database.
Each xpub is read from the database once, by one store, in background
threads, and every store answers from that one entry. So two stores
never give two answers. The cost: the first request for an xpub waits,
or is told to come back later.

There is no other cache: none in memory, and none of a store alone.

## Words used here

The shared words (store, fleet, the chain, the pool, the tip, view,
xpub, the cache, entry, decision) are in [README.md](README.md).

| Word | What it means here |
| --- | --- |
| **branch** | One of the two rows of addresses of an xpub: receiving (0) and change (1). The code says chain. |
| **gap** | The run of unused addresses that ends a branch: its addresses are those up to the last used one, and a gap more. |
| **scan**, **window** | A scan reads a branch in order, up to the gap. What it reads is the window. |
| **lookahead** | A gap's worth of unused addresses after the window of each branch, kept in the entry too. |
| **the chain's layer** | The part of an entry that comes from the chain, as it is at one block. |
| **the pool's layer** | The part of an entry that comes from the pool. |
| **the cache's tip** | The block that the chain's layer of every entry is at. |
| **version** | A number on an entry, new at every change to it. |
| **marked** | Said of an entry that a move of the tip is writing. |
| **leaving** | Said of a transaction that the newest block confirmed or made invalid, still in the pool for a moment. [tip-first.md](tip-first.md) says marked. |
| **hit**, **miss** | A read that the cache answers, and one that it cannot. |
| **build**, **job** | A build reads an xpub from the database and writes its entry. A job is one build, waiting or running. |
| **the indexer** | The background threads of a store that run the builds. |
| **the door** | The code between a request and the cache. |
| **lock**, **holder** | The lock is a key in Redis that says which store builds an entry, and when it expects to be done. That store is the holder. The code also says dibs. |
| **evict** | To delete an entry because the cache is full. |

## An entry

An entry has, for the addresses that a scan of the key reads:

- **the chain's layer**: balances, transactions and unspent outputs at
  the cache's tip, which is the fleet tip;
- **the pool's layer**: what the pool does at those addresses. The pool
  edits it in the same step that changes the pool;
- **lookahead**: derived and indexed when the entry is built. So the
  pool finds a payment to the next address of a wallet, and puts it in
  the entry at once.

For example, with a gap of 32 (the default of `--xpub-gap`), a wallet
has used receiving addresses 0 to 2. The window is 0 to 34, and the
lookahead is 35 to 66. A payment to address 10 arrives in the pool. The
window is now 0 to 42. The entry has all of it, and goes on answering.

## Exact reads

**A read answers only where the cache is.** The request's tip, the fleet
tip and the cache's tip must be the same block. And the scan must need
no address that the entry lacks.

**A request reads one version of each entry.** Every change to an entry
gives it a new version. If one changes, the request is answered again.

**A leaving transaction is never counted.** After a block, the
transactions that the block took out of the mempool are leaving: they
are removed a moment later ([tip-first.md](tip-first.md)). A read of an
entry that has a leaving transaction removes it first, and then answers
(`ask` in `Haskoin.Store.Fleet.XPub`, and the check of `pool:leaving` in
`lua/xpub.lua`). So no answer counts, from the pool, a transaction that
the block confirmed or made invalid.

## The tip move

The store that moves the fleet tip brings the cache along:

1. It writes the cache's part first, in pieces (`cacheBegin`, then
   `cachePieces`). Each piece marks the entries that it touches.
2. One step moves the pool, the fleet tip and the cache's tip together
   (`fleetCommit`, the decision `advance`). It gives every marked entry
   a new version and clears the marks. When the move only adds blocks,
   the pool's part of the step is to say which transactions are leaving.

**No request sees half a move.** A read of a marked entry waits up to
two seconds. Then the request is answered again.

**A move that goes quiet is aborted.** The store says every two seconds
that it is at its move (`cacheBeat`). A move whose store said nothing
for ten seconds is aborted by whoever finds it.

**A reorganisation is undone address by address.** It is read from the
store's views of its database at recent blocks (`tipMoves`): what the
abandoned blocks did comes out, the outputs that they spent come back,
and the new blocks go in. A store without a view of the old tip does not
move the tip, and waits for a store that has one.

**A reorganisation never empties the cache.** Only a restart of the pool
does: the way between the two tips is too long, or the only live store
has no view of the old tip.

**Nothing has to catch up.** This upkeep is atomic with the pool, and
exact by construction. So there is nothing to check afterwards. It
cannot share a rule with anything that works from events.

## The indexer

`Haskoin.Store.XPub.Indexer` runs the builds in threads of its own,
`--xpub-workers` of them (one per core by default). So no request thread
reads an xpub.

Jobs wait in one queue, first in, first out, with no key in it twice. A
request for a key already queued joins its job.

Before it queues a job, the store estimates when the job would be done:
its place in the queue, over the threads, times the median time of
recent jobs. With 40 jobs ahead, 8 threads and a median of 2 s, that is
(40 ÷ 8 + 1) × 2 s = 12 s. The estimate is rough, since the size of an
xpub is unknown until it is read. It only has to tell soon from later.

A job is refused at once, and nothing is taken, when the estimate is
past `--xpub-horizon` (30 s) or the queue is full (`--xpub-queue`).

## The door

`Haskoin.Store.XPub.Service` is the door. `xpubQuery` in the web server
puts it in front of the queries.

| Case | What happens |
| --- | --- |
| A hit | The cache answers. The other reads of the request use the same version of the entry. If it changed or is gone, the request is answered again, up to three times, and then gets 503. |
| A miss | The door asks the indexer. One store of the fleet builds the entry: the store that queues the job takes the lock, which carries the estimate of when it will be done, by the clock of Redis (`Haskoin.Store.XPub.Lock`). |
| The job is expected later than `--xpub-wait` (10 s) | The request is refused at once with 429, and the same `Retry-After` on every store. |
| The job is expected sooner | The request waits until the build is announced (`fleet:built`). It gets 429 if the wait runs out. |
| The holder dies | Its lock expires within 30 s. The next store that asks takes the lock and builds. |
| A request says `nocache` | The parameter is ignored. An entry is never stale, so there is nothing to read again. |

**Every store makes the same decision**, from the one estimate in the
lock.

**What is announced.** Only a build that put its entry in the cache. A
build that was abandoned fails its job. A store counts announcements
only for the keys that its requests wait for, and forgets a key when the
last of those requests is done. A reconnection to Redis wakes nobody: a
build announced while the connection was down is found when the wait
runs out.

**The cache cannot be turned off.** Its size is all that is set.

## What the cache keeps

**Eviction.** Past `--redis-max-xpubs` entries, the cache evicts by the
GreedyDual rule: by what an entry would cost to build again, and by how
recently it was used.

**Extending the lookahead.** When payments use up more than half of the
lookahead, the indexer is asked to extend it. Reads go on answering
meanwhile. A read that needs an address the entry lacks is a miss.

**After Redis loses its data**, entries are built again as requests ask
for them. The first requests wait or get 429.
