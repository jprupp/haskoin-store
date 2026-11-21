# Xpubs: answered from the fleet's cache

Every answer about an extended public key (xpub) comes from the fleet's cache
in Redis (`Haskoin.Store.Fleet.XPub`), never straight from the database.
There is no other cache: no cache in memory, and no cache of a store alone.
Each xpub is indexed once, by one store, in background threads, and every
store of the fleet answers from that one entry. So two stores never give
two answers, and no request reads an xpub from the database itself. The
architecture around it is in [one-fleet.md](one-fleet.md).

## An entry

An entry has, for the addresses that a scan of the key reads:

- **the chain's layer**: balances, transactions and unspent outputs at the
  block that the cache is at, which is the fleet tip;
- **the pool's layer**: what the shared pool does at those addresses.
  `pool.lua` edits it in the same script that changes the pool;
- **lookahead**: a gap's worth of unused addresses after the window of each
  chain, derived and indexed at build, so that `pool.lua` finds a payment
  to the next address of a wallet and puts it in the entry at once.

A read answers only when the request's tip, the fleet tip and the cache's
tip are the same block, and when the scan needs no address that the entry
lacks. Every script that edits an entry stamps a new version; a request
reads one version of each entry, and is answered again if one changes.

## The tip move

The member that moves the fleet tip writes the cache's part first, in
pieces (`cacheBegin`, then `cachePieces`), each marking the entries that it
touches. Then one script (`fleetCommit`, in `pool.lua`) moves the pool, the
fleet tip and the cache tip together, stamps the marked entries and clears
the marks. A read of a marked entry waits up to two seconds, then has the
request answered again: no request sees half a move. A move with no piece
for ten seconds is aborted by whoever finds it.

A reorganisation is undone address by address, read from the store's views
of its database at recent blocks (`tipMoves`): what the abandoned blocks
did comes out, the outputs that they spent come back, and the new branch
goes in. A store without a view of the old tip does not move the tip, and
waits for one that has it. A reorganisation never empties the cache; only a
restart of the pool does (a way too long, or the only live store without
the old tip).

This upkeep cannot share a rule with anything that works from events: it is
atomic with the pool, and exact by construction, so there is nothing to
check or catch up.

## The indexer

`Haskoin.Store.XPub.Indexer` runs the builds in threads of their own,
`--xpub-workers` of them (one per core by default), so that no request
thread reads an xpub. Jobs wait in one queue, first in, first out, with no
key in it twice; a request for a key already queued joins its job.

Before it queues a job, the store estimates when it would be done: its
place in the queue, over the threads, times the median of recent jobs. A
job past `--xpub-horizon` (30 s), or a full queue (`--xpub-queue`), is
refused at once and nothing is taken. The estimate is rough, since the
size of an xpub is unknown until it is read, but it only has to tell soon
from later.

## The door

`Haskoin.Store.XPub.Service` is the door, and `xpubQuery` in the web server
puts it in front of the queries:

- **A hit answers.** The other reads of the request use the same version of
  the entry; if it changed or is gone, the request is answered again (up
  to five times), then 503.
- **A miss asks the indexer** (`Haskoin.Store.XPub.Indexer`). One store of
  the fleet builds the entry: the store that queues it takes its dibs, a lock
  in Redis that carries the estimate of when it will be done, by the clock of
  Redis (`Haskoin.Store.XPub.Lock`).
- **The same decision on every store.** A job expected past `--xpub-wait`
  (10 s) is refused at once with 429 and the same `Retry-After` on every
  store. Otherwise the request waits until the build is announced
  (`fleet:built`), and gets 429 if the wait runs out. Only a build that put
  its entry in the cache is announced; one that was abandoned fails its job.
  A store counts announcements only for the keys that its requests wait
  for, and forgets a key when the last of them is done, and a reconnection
  to Redis wakes nobody: a build announced while the connection was down is
  found when the wait runs out.
- **A dead holder.** Its lock expires within 30 s, and the next store that
  asks takes the dibs and builds.
- **No `nocache`.** An entry is never stale, so there is nothing to read
  again; the parameter is ignored. Nor can the cache be turned off: its
  size is all that is set.

Entries are evicted by GreedyDual past `--redis-max-xpubs`, by the cost of
building them again and how recently they were used.

When payments use up more than half of the lookahead, the indexer is
asked to extend it, and reads go on answering meanwhile. A read that needs
an address the entry lacks is a miss.

After Redis loses its data, entries are built again as requests ask for
them: cold requests wait or get 429.
