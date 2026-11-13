# Layered database: the chain below, the mempool on top

The database of the store has two areas, and a query reads both.

- **Chain area:** blocks, transactions, unspent outputs, indices by address
  and balances, with nothing from the mempool in them. It changes only when
  a block is imported or reverted, one block at a time, atomically.
- **Mempool area:** everything about unconfirmed transactions, kept as
  changes to apply on top of the chain when a query is answered. Emptying
  it leaves a store with an empty mempool and an untouched chain.

This is database version 22. A database of version 20 or 21 is migrated when
the store opens it, and [cannot go back](#upgrading-from-version-20-or-21).

## Where things are

| Part | Where |
|---|---|
| Keys of both areas | `Haskoin.Store.Database.Types` |
| Keys of one mempool transaction; summaries | `Haskoin.Store.Database.Mempool` |
| Rules that import blocks and transactions | `Haskoin.Store.Logic.Layered` |
| Queries over both areas, on a view | `Haskoin.Store.Database.Query.Layered` |
| Migrations from versions 20 and 21 | `Haskoin.Store.Database.Migrate` |
| A view of the database for each recent block | `Haskoin.Store.Database.Views` |
| Fleet tip and shared pool: rules | `Haskoin.Store.Fleet`, `Haskoin.Store.Fleet.Pool` |
| The same, carried out in Redis | `Haskoin.Store.Fleet.Redis` |
| Queries on a block with the pool on top | `Haskoin.Store.Database.Query.Pooled` |
| What a store does as a member of a fleet | `Haskoin.Store.Fleet.Member` |
| Cache of extended public keys of a fleet | `Haskoin.Store.Fleet.XPub`, `xpub.lua` |

## The problem that the layout solves

Version 20 wrote a mempool transaction into the same indices as the chain,
down to its name as spender in the records of confirmed transactions. A
query read one place. The price:

- **Nothing was immutable.** Any index, and the record of any confirmed
  transaction, could be changed by a transaction that was gone a minute
  later. A bug in the mempool rules damaged data that only a resync fixes.
- **Two stores at the same block held different databases.** A fleet gave
  different answers, and the Redis cache had to hear of every change from
  each of them: see `docs/cache-consistency.md`.

## The layout

### The chain area

The column families and encodings of version 20, with other contents:

- **`tx`:** the record of each confirmed transaction,
  [written once](#records-of-confirmed-transactions-are-written-once).
- **`spender`:** which confirmed transaction spends each output.
- **`unspent`, `addr-out`:** outputs of confirmed transactions that no
  confirmed transaction spends.
- **`addr-tx`:** confirmed transactions by address.
- **`balance`:** balances of the chain alone. The unconfirmed part is zero.

### The mempool area

Two column families. `mempool` holds what is read by transaction or output:

- `MemTxKey` txid, and `MemTimeKey` time and txid: the list of the mempool.
- `TxKey` txid: the body of a mempool transaction, without spenders.
- `MemSpenderKey` outpoint: the mempool transaction that spends an output.

`mempool-addr` holds what is read by address:

- `MemAddrTxKey` address, time, txid: a mempool transaction touches it.
- `MemAddrOutKey` address, time, outpoint: the mempool pays it this output.
- `MemAddrSpentKey` address, outpoint: the mempool spends this output of it.
- `MemAddrSumKey` address: the summary, below.

**Every key but the summary belongs to one transaction** and follows from
its body alone (`mempoolRecords`). A transaction enters the mempool by
writing its keys and leaves by deleting them. No key says whether a spent
output is of the chain or of the mempool, so when a block confirms a parent
and not its child, the keys of the child do not change.

**Why entries and not stored results.** A stored balance with the mempool
applied is wrong as soon as a block pays the address, as in version 20.
Entries that say what the mempool *does* stay right whatever the chain does.

**The summary** (`MemSummary`) is what the entries of one address add up
to: `txs`, the transactions that touch it; `paid` and `paidCount`, the
outputs that the mempool pays to it; `spent` and `spentCount`, the outputs
of it that the mempool spends; `both` and `bothCount`, those that it both
pays and spends. The balance of an address is then two reads by key:

```
confirmed   = chain.confirmed - (spent - both)
unconfirmed = paid - both
utxo        = chain.utxo - (spentCount - bothCount) + (paidCount - bothCount)
txs         = chain.txs + txs
received    = chain.received + paid
```

- **It does not go stale either.** Every field counts what mempool
  transactions do, and `both` changes only when one of the two transactions
  enters or leaves the mempool, which is when the rules run.
- **Why it exists.** Without it a balance read every entry of the address,
  35 milliseconds for one with 6,216 entries. And finding that an address
  had nothing was a scan through a Bloom filter, which after a block
  stepped over every key that the block had deleted: 1.6 milliseconds.
- **It bounds every scan.** Its counts say how many entries to read, and it
  is the last key of its address, so a scan stops at a live key.

**Deleted transactions are not kept.** A transaction that leaves the mempool
unconfirmed is forgotten, and the API does not return it. Bodies that
version 20 marked as deleted stay in `tx` of a migrated database and are
ignored: finding them means reading the whole column family. So that it
does not fetch from its peers what it just deleted, a store remembers the
last hundred thousand in memory, until it is started again.

### Invariants

1. The chain area holds what follows from the main chain up to the best
   block, and nothing else, apart from the bodies that version 20 left.
2. The mempool area is the keys of the transactions of the mempool, and for
   each address that they touch a summary of its entries, which can be
   written again from them (`rebuildSummaries`).
3. Every output that a mempool transaction spends exists: the chain has it
   as unspent, or another mempool transaction creates it.
4. No two mempool transactions spend the same output.
5. No transaction is both confirmed and in the mempool.

**What can still differ between two stores at the same block**, with or
without a mempool:

- **Blocks that left the main chain.** A store that was running during a
  reorganisation keeps the abandoned block, marked as not in the main
  chain, and lists it at its height. A store that synced later never saw
  it. The blocks at a height, and a block asked for by that hash, can
  therefore differ. Nothing else in the chain area depends on when a store
  synced.
- **What version 20 left in a migrated database**, none of which the API
  shows: bodies of transactions that it deleted, and balances of zero for
  addresses whose only transactions were deleted.
- **How RocksDB lays the data out**: table files, and which of them have
  filters.

## Reading: the mempool applied on top

Every query reads the chain, reads what the mempool area has for the same
thing, and combines them (`layeredRead`).

- **Lists by address:** the mempool's part, then the chain's: the API
  lists newest first, and unconfirmed transactions are newest.
- **Unspent outputs:** those that the mempool pays and does not spend, then
  the chain's without those that the mempool spends. The summary says when
  there is nothing to leave out.
- **A transaction:** the mempool area is asked first, for all the
  transactions of a request at once, then `tx`. Mempool spenders are
  looked up only for outputs that no block spends.
- **Extended public keys:** built from the queries by address. An address
  that only the mempool has used counts as used for the gap.

**Pages.** A page that starts at the top or at a mempool transaction reads
the mempool's part from there, then the chain's. A page that starts at a
confirmed transaction or at a height reads the chain alone, so every page
after the first of a long history costs what it did. A height never includes
the mempool. A page that starts at a transaction that is not known, a
deleted one included, is empty.

**One view for each request.** Two areas read at different moments could
show a transaction twice or not at all. So the queries take a `View`, both
areas through one RocksDB snapshot (`withView`). The web server holds one
around each request and the sending of its response, so a slow client
holds old data back from compaction. Nothing made from a view may be used
after it is released, and the types do not prevent it.

## Writing

Each event is one atomic RocksDB batch across both areas.

- **A transaction enters the mempool:** its keys are written, and the
  summaries of its addresses. It is replaced, dropped or too old
  (`--mempool-timeout`): its keys are deleted.
- **A block arrives:** it is imported into the chain area. The keys of the
  mempool transactions that it confirms or conflicts with are deleted.
- **A block is reverted:** its transactions leave the chain area and their
  keys go to the mempool area.
- **The mempool is wiped (`--wipe-mempool`):** every key of the mempool
  area is deleted. That is safe at any time.

**One door into the chain.** Only importing and reverting a block write to
the chain area, and both check the block against the chain area alone,
whether or not the store had seen its transactions.

- **A block that spends an output of an unconfirmed transaction is
  refused.** Version 20 imported it, and left a confirmed transaction whose
  parent was in the mempool.
- **The replace-by-fee flag** of a mempool transaction is inherited from
  unconfirmed ancestors, and corrected, in the mempool area only, when an
  ancestor is confirmed or comes back.

## Records of confirmed transactions are written once

Version 22 took two things out of the record of a confirmed transaction, so
that two stores at the same block hold the same record.

- **Its time is the time of its block.** Before, a store that had seen the
  transaction in its mempool kept the time of first sight, so two stores
  gave two values of `time`. A transaction that goes back to the mempool
  when its block is reverted has the time of the revert there. It had the
  time of its block until 1.12.1, and the mempool, which deletes what is
  older than `--mempool-timeout`, deleted a transaction of an old block as
  soon as it came back, while the nodes kept it.
- **Its spenders are under keys of their own**, in `spender`
  (`SpenderKey`). Before, each block that spent one of its outputs read the
  whole record and wrote it back. Now the record is written when a block
  confirms the transaction and deleted if that block is reverted. The one
  exception is the two coinbases of 2010 that have the hash of an earlier
  one: the later takes its place.
- **A key holds the spenders of a group of sixteen outputs.** A key for
  each output would need a read for each output whenever a transaction is
  returned. One value for a transaction would be rewritten whole at every
  spend: half a megabyte, ten thousand times, for a payout to ten
  thousand. With sixteen, nearly every transaction has one key.
- **Whoever reads a record by its hash passes the hash along** to read the
  spenders. Working it out from the record cost more than both reads.

## A fleet that answers as one

Stores started with `--fleet` that share a Redis give the same answer to the
same request at the same time. They agree on the state to answer from, not
on the moment at which each commits a block.

- **The fleet tip** is in Redis: the newest block that every live store has
  imported. Each store imports at its own pace, keeps a view of its
  database at each recent block, and reports those blocks in a key that
  expires, every two seconds and at every block.
- **One Lua script moves the tip** to the newest block that all live stores
  reported. It is a compare-and-set: nothing is locked, and two stores that
  try at once do the same thing.
- **The shared pool** is the mempool that answers requests. Each store
  offers its transactions with their entries by address. The script keeps
  the same lists and summaries as the local mempool area, and the time at
  which each transaction was first offered, so every store lists it alike.
- **Moving the tip changes the pool in the same step.** The store that
  moves it says what the blocks in between confirmed and spent, and across
  a reorganisation what comes back.
- **When no store can say, the pool starts again.** A store goes through
  50 blocks at most to say what they did. Past that it moves the tip and
  empties the pool, and a counter next to the tip tells every store to
  offer its mempool again.
  - Any store may do this. The way is as long for all of them, and a tip
    that nobody may leave stays for good: two stores that came back
    together after half a day away answered nothing from then on.
  - A store that does not have the old tip at all may do it only when it
    is alone. Another store might know the way.
- **Every request** is answered from the view at the fleet tip with the pool
  on top. Each read of the pool checks that the tip has not moved. If it
  has, the request is answered again, unless its response is on its way.
- **Events:** the script that changes the pool or moves the tip appends
  what it did to a stream in Redis, in the same step: a transaction that
  entered, one that left unconfirmed, a block that the tip moved to or
  through. Every store serves `/events` from that stream, so all tell the
  same, in the same order. A store's own events would tell of a
  transaction that the next server does not have yet.
- **Conflicts:** of two transactions that spend the same output, the first
  one in stays, unless the store that offers the other deleted the first
  from its own mempool, as it does when it replaces it by fee. A store
  knows what it deleted since it started, not before: after a restart it
  leaves such a conflict as it is.
- **A store does not withdraw what it deletes from its own mempool.** A
  replacement reaches the pool with the offer of what replaces, a conflict
  with a block when the tip moves, and age is counted on the pool's times.
- **A store ahead of the tip holds back** a transaction until each of its
  inputs spends an output of the pool or one unspent in its view at the tip.
- **The replace-by-fee flag is kept apart from the body.** It changes when
  a parent is confirmed or comes back, and a body given to the pool once
  went stale (`replaceable`).

- **Why not a lock and a commit in step:** a store that stalls would block
  the others, and stores would still commit milliseconds apart.

How the shared mempool works step by step, and its weak points, are in
`docs/fleet-mempool.md`.

**What a fleet cannot say, and what it costs.**

- **A request sees one block, but not one moment of the pool.** A
  transaction can enter the pool between two reads of one request, so a
  balance and a list can differ by it. A store alone has no such gap.
- **Redis is in the way of every request**, about 80 microseconds a round
  trip when measured, and the fleet is one import behind its fastest
  store. With Redis down each store answers from its own database, and
  `/health` answers 203 instead of 200.
- **Redis must not lose writes.** A failover that loses the last second
  loses mempool transactions and can move the tip back.
- **A store that falls behind** drops out after ten seconds without a
  report, or at once when its health check fails. It answers 503 on
  `/health`, and 503 with `behind-fleet` at a tip that it has no view of.

## The cache of extended public keys of a fleet

`Haskoin.Store.Fleet.XPub` caches wallets in the Redis of the fleet: a
request is a read or two of Redis instead of a read of the database for
each address. A store alone keeps `Haskoin.Store.Cache`.

- **An entry has two layers.** The chain's: the balance of each address,
  and the transactions and unspent outputs of all of them, at one block.
  The pool's: the transactions of the pool that touch those addresses and
  the outputs that it pays them. Both are in sorted sets in the order of
  the API, so a page is a range.
- **The pool's layer is kept by the script of the pool**, so an entry
  cannot have a mempool that the pool does not.
- **The cache is valid for one move of the tip.** Every move is counted,
  and the cache is at a count and a block. The store that moves the tip
  sends what the blocks changed for the cached addresses, usually nothing.
  Until then every read misses and is answered from the database. A block
  alone is not enough: mined, reverted and mined again, it looks current
  while the pool has moved its transactions out and back as unconfirmed.
- **Nothing is repaired.** A cache that was not brought along is emptied,
  and so is the cache at a reorganisation. Entries are built again.
- **The gap is checked at every read.** The scan of the key runs over the
  balances of the entry, pool included, and the read is a miss unless it
  reads exactly the addresses of the entry (`window`). A key that grew
  gets its new addresses. One that shrank is built again.
- **Entries are built in the background** by the store that missed, for
  keys with at least `--redis-min-addrs` addresses or `--redis-min-txs`
  transactions. The least recently used go beyond `--redis-max-xpubs`.

## Upgrading from version 20 or 21

The store migrates when it opens a database (`upgradeDatabase`). **There is
no way back**: a store of version 20 cannot open a database that has the
`mempool-addr` column family. Take a copy or a file system snapshot first.

**From version 20: moving the mempool.** For each mempool transaction,
descendants first, the migration takes it out of the chain's indices as
deleting it did in version 20, removes its body from `tx`, and writes its
keys to the mempool area. Summaries are written at the end.

- It reads and writes only what the mempool touches: 5.0 seconds for 30,000
  transactions in memory, where wiping them took 4.2. Slower on a disk.
- It reads everything before it writes, and refuses a database that is not
  as version 20 says, untouched. It works in atomic batches and marks the
  database, so it can be interrupted and carried on.
- **If it stops with an error**, the database stays marked as being
  migrated. No way out of that is written yet.

**From version 21: rewriting the records** (`migrateRecords`). Nothing lists
the records that need a change, so it reads every record of `tx` and writes
nearly every one, with the time of its block and without its spenders,
which go to their keys in the same write.

- **Plan for many hours on a database of Bitcoin**, during which the store
  does not start. It read 210,000 records a second in memory when only
  times had to change. Bitcoin has more than a billion, on a disk.
- **It needs room for a second copy of `tx`**, the largest column family,
  until RocksDB has compacted the old one away.
- Interrupted, it starts again and skips what is done. Leaving old records
  and reading around them was rejected: stores would differ for ever.

## What it costs

The programs and their raw output are in `docs/layered-database/`.
`QueryCost.hs` writes one history with the rules of each layout and asks
both databases the same questions: 211 blocks, 200,000 confirmed
transactions, 40,000 in the mempool, in memory, on one thread, with
`rocksdb-haskell-jprupp` 2.6.4. The busiest address has 6,216 entries in
the mempool area, the cold wallet none and the busy wallet 16,368.

**Queries**, in microseconds, without Bloom filters on the chain:

| | Version 20 | Layered |
|---|---|---|
| Balance of an address | 4.3 | 11.5 |
| Balance of the busiest address | 3.1 | 11.7 |
| Balances of 100 addresses | 244 | 345 |
| A confirmed transaction | 5.7 | 18.0 |
| 100 confirmed transactions | 352 | 823 |
| A mempool transaction | 5.2 | 16.4 |
| Transactions of an address | 19 | 31 |
| Busiest address: first 100 transactions | 175 | 217 |
| Busiest address: first 100 unspent outputs | 365 | 660 |
| Cold wallet: balances | 1,381 | 1,628 |
| Cold wallet: 100 transactions, far back | 6,969 | 4,469 |
| Busy wallet: first 100 transactions | 4,440 | 7,544 |
| Busy wallet: first 100 unspent outputs | 7,756 | 15,946 |

- **A question by key costs about 7 microseconds more.** Half of that is
  the snapshot, which the web server takes once for a request and this
  program for each question.
- **A busy address costs what any other does**, right after a block too.
- **Transactions in full cost the most**: about twice as much for a page of
  confirmed ones, for the second read of each.
- **A wallet with nothing or little in the mempool** takes 1.0 to 1.2 times
  as long. One whose addresses all have mempool transactions takes up to
  twice as long for its first page: two streams to open for each address.
  A page far back is faster: its start is looked up once for all addresses.

**Bloom filters are on by default** (`--no-bloom` turns them off), for
everything that is read by key. The column families of the mempool always
have them. With them the balances of the cold wallet took 1,318
microseconds instead of 1,628.

- They take 10 bits for each key and live in the block cache, 1 GiB by
  default. On a chain with billions of keys that is gigabytes: size the
  cache for it, or turn them off. The indices by address have none.

**The web server**, measured at version 21 with an empty mempool
(`scripts/bench-web.sh`), uses 19% more CPU for a request about one
transaction or one balance, and 47% more for the balances of a wallet.
Plain lists by address cost the same or less, because version 2.6.4 of the
binding reads rows three times as fast.

**Writing**, in microseconds for each transaction:

| | Version 20 | Layered |
|---|---|---|
| Importing a chain, nothing in the mempool | 189 | 173 |
| Taking a transaction into the mempool | 294 | 243 |
| Importing a block that confirms mempool transactions | 205 | 397 |

Work moves from the arrival of a transaction to its block. A block of 3,000
transactions that the mempool had seen took 1.3 seconds instead of 0.6.

**A fleet** (`FleetCost.hs`), one store alone and as the only member of a
fleet, Redis in a container on the same machine, in microseconds:

| | Alone | Fleet |
|---|---|---|
| Balance of an address | 20 | 105 |
| Balances of 100 addresses | 300 | 577 |
| Busiest address: first 100 unspent outputs | 669 | 1,313 |
| A mempool transaction | 27 | 108 |
| Offering a transaction to the pool | | 334 |

Most requests are one round trip. A list of many addresses is one read for
all the summaries and one for each address that has something.

**The cache**, in milliseconds for a request, which reads the balances of
the wallet and then what it was asked for:

| | Alone | Fleet, no cache | Fleet, cached |
|---|---|---|---|
| Cold wallet: balances | 1.7 | 4.8 | 1.6 |
| Cold wallet: first 100 transactions | 6.7 | 10.5 | 1.5 |
| Busy wallet: first 100 transactions | 9.6 | 64.6 | 3.1 |
| Busy wallet: first 100 unspent outputs | 17.1 | 72.3 | 3.9 |
| Small wallet, 84 addresses: first 100 unspent outputs | 1.06 | 2.02 | 1.44 |

A small wallet gains little, which is what the two thresholds are for.
Building the entry of a large wallet took under half a second.

## How it is tested

The rules and queries of version 20 (`Haskoin.Store.Logic`,
`Haskoin.Store.Database.Query`) stay in the library as the reference, and
nothing else calls them. Random histories of payments, replacements, blocks
and reorganisations are compared with them after every operation.

- **Rules:** three stores get the same history, one on the rules of
  version 20, one on the new rules, one migrated at a random point.
  Results, errors and answers must agree at every step, and the databases
  key for key at the end.
- **Migration:** a migrated database answers as before, and its chain area
  equals that of a store that never saw a mempool.
- **Queries:** pages with random limits, offsets and starts, for addresses
  and an extended public key under two gaps. Every summary is worked out
  again from its entries, and the bytes of a confirmed record never change.
- **Fleet:** Redis is compared step by step with the rules of the tip and
  of the pool, which are pure functions. A member runs random histories
  against a real Redis, with the cache on and off, each answer held to the
  store's own. Integration tests run two stores against a real node.
  One test builds a tip that is too far for both of two live stores,
  which used to stay where it was for good.
- **Mutations were tried**, one at a time, in the migration, the rules, the
  queries (thirteen), the records (four), the cache (twelve) and the way
  out of a tip that is too far (two). Each made the tests fail.

Not shown: load, and a database of real size.

## What is open or not done

- **The mempool in memory.** A mirror of the mempool area in memory was
  considered and not built: what cost too much was reading all the entries
  of an address, which the summary removes, not the price of a lookup.
- **Recovery from a migration that stops with an error** is not written.
- **Spenders next to their record**, read with one seek, would bring a
  page of full transactions back to about what it cost.
- **Bloom filters on a full database** were not measured for memory.
- **How long a store may lag** before it leaves the fleet is not a setting.
