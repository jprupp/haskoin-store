# One fleet

What the store is built around, and why. It describes the code; it is not
part of the package.

## In short

Stores behind one load balancer must give the same answer to the same
question. So everything that changes between blocks is kept once, in Redis,
where every store reads it: the mempool, the answers about xpubs, and the
block to answer from. Each store keeps only the chain, in its own database.
A store alone is a fleet of one. The cost is a Redis on every install, and
a store that is behind the others does not answer.

## Words used here

The shared words (store, fleet, the pool, the tip, view, entry, turn and
others) are in [README.md](README.md). This page adds:

| Word | What it means here |
| --- | --- |
| **live** | A store whose report was accepted in the last ten seconds. |
| **report** | At each turn, a store tells Redis which recent blocks it has. |
| **offer** | A store hands a transaction to the pool ([fleet-mempool.md](fleet-mempool.md)). |
| **intake** | The path of a transaction from a peer to an offer. |
| **door** | Where a request about an xpub arrives. It answers from the entry (a hit), or asks for one to be built (a miss). |
| **indexer** | The background threads of a store that build entries. |

## The design in five rules

1. **Redis is mandatory.** The store does not start without the Redis of
   `--redis-url` (default `redis://localhost:6379`).
2. **Every store is a member of a fleet**, even a fleet of one. A fleet is
   the stores that share one Redis.
3. **The mempool lives only in Redis**, in one pool that the fleet shares.
   No store keeps a mempool of its own, in RocksDB or in memory.
4. **The chain lives only in RocksDB.** Each store imports blocks into its
   own database, which holds confirmed data and nothing else.
5. **Xpubs live only in Redis.** Each xpub is indexed once, by one store,
   into an entry that every store answers from.

## Why

Two stores that each kept their own mempool and their own xpub cache could
disagree. One has heard of a payment and the other has not: a wallet that
asks twice sees the payment come and go. Or one has a balance from the
last block, and the other from the block before.

So the fleet agrees on one block to answer from, the tip: the newest block
that every live store has. A request reads the chain from its store's own
database at the tip, and the pool on top of it.

A single store follows the same path. That costs a Redis on every install,
but it leaves one design to build and test, not two. A store alone runs
exactly the code that a fleet of ten runs.

## How a request is answered

```
peers ──tx──▶ intake ──checks at tip + pool──▶ offer ──▶ pool, xpub entries, events
peers ──block──▶ block store ──▶ RocksDB (chain only) ──▶ report ──▶ tip move
request ──▶ RocksDB view at the fleet tip + pool (Query.Pooled) ──▶ answer
xpub request ──▶ door ──hit──▶ entry in Redis ──▶ answer
                     └─miss──▶ indexer (one store, in the background) ──▶ entry
```

- **Transactions** come from peers. A store checks each input against its
  view at the tip and against the pool, and offers the transaction to the
  pool. The pool takes it, refuses it as a conflict (another transaction of
  the pool spends the same output), or holds it back as an orphan (a parent
  is missing). See [fleet-mempool.md](fleet-mempool.md).
- **Blocks** go into RocksDB only ([block-import.md](block-import.md)).
  Each store reports the blocks it has. When all live stores have a newer
  block, one of them moves the tip: the pool, the tip and the xpub entries
  change in one step ([tip-first.md](tip-first.md)).
- **A reorganisation** gives back the transactions of the abandoned blocks
  in that same step, and not before. Until the tip moves, they are
  confirmed at the tip and not in the pool. After it, they are in the pool
  and not confirmed. No answer has a transaction both confirmed and
  unconfirmed.
- **A store that is reverting blocks** may find that its chain moved
  between its report and its move of the tip. It leaves the tip for its
  next turn. Moving it then would empty the pool, and lose what no node has
  any more.
- **Requests** take the tip when they start. A read that finds the tip
  moved has the request answered again from the new tip. No answer mixes
  two blocks.
- **Xpubs** are answered only from their entries in Redis. A miss asks the
  indexer, which runs in background threads. One store builds the entry
  while the others wait for it. See [xpub-indexing.md](xpub-indexing.md).

## Health

A store is unhealthy when Redis does not answer, or when it has no view of
the tip, for example while it syncs or after it fell behind. It then
answers 503 to `/health` and to every request that needs the pool or an
xpub, so the load balancer sends the request elsewhere. Any answer of its
own could differ from those of the other stores, which is what a fleet is
there to prevent.

## When Redis loses its data

Redis keeps a random epoch key. A store that finds it gone or changed
forgets its tip, and is unhealthy until the fleet has a tip again. Its next
turn sets one. The pool then fills again from what the peers announce, and
from the node's own mempool over RPC where the store has `--node-rpc`. Xpub
entries are built again on demand.

## What this design gives up

- **A Redis on every install**, even for one store on a laptop.
- **Redis must never evict** the pool or the fleet's keys. Run it with
  `maxmemory-policy noeviction`. Xpub entries are evicted by the store
  itself, not by Redis.
- **A store behind the fleet does not answer** until it catches up, even
  where its own database could.
- **A request is one block, but not one moment of the pool.** A transaction
  can enter between two reads of the same request. Reads of one xpub are
  exact: they see one version of its entry.
