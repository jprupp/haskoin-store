# Layered database: the chain below, the mempool on top

This is a design for splitting the database of the store into two areas, and
a report on how far it has been taken. It was written on 2026-11-09 against
the database layout of version 20.

- **Chain area:** blocks, transactions, unspent outputs, address indices
  and balances, with nothing from the mempool in them. It changes only when
  a block is imported or reverted, one block at a time, atomically.
- **Mempool area:** everything about unconfirmed transactions, as changes to
  apply on top of the chain when a query is answered. Nothing that happens
  in the mempool writes to the chain area. Emptying the mempool area leaves a
  store with an empty mempool.

The API answers as it does today. No resync is needed: a migration moves the
mempool, and everything it did to the chain's indices, into its own area.

## Contents

- [Where this stands](#where-this-stands)
- [The problem](#the-problem)
- [The design](#the-design)
- [Reading: the mempool applied on top](#reading-the-mempool-applied-on-top)
- [Writing](#writing)
- [What it costs: measurements](#what-it-costs-measurements)
- [Keeping the mempool in memory](#keeping-the-mempool-in-memory)
- [The cache under the same rules](#the-cache-under-the-same-rules)
- [Migration](#migration)
- [Evidence](#evidence)
- [Order of work](#order-of-work)
- [Alternatives considered](#alternatives-considered)
- [Open questions](#open-questions)

## Where this stands

The store still runs on version 20. What exists on this branch is the part
that could be built and proved ahead of the switch.

| Piece | Where | State |
|---|---|---|
| Keys of the mempool area | `Haskoin.Store.Database.Types` | written |
| The keys of one transaction, and the merge of the two areas | `Haskoin.Store.Database.Mempool` | written, whole lists only |
| Migration from version 20 | `Haskoin.Store.Database.Migrate` | written, tested, not called by the store |
| Tests against today's database | `test/Haskoin/Store/Database/MigrateSpec.hs` | passing |
| Cost of the extra lookups | `docs/layered-database/` | measured |
| Rules that import blocks and transactions | `Haskoin.Store.Logic` | not started |
| Paged queries over both areas, the web server on them | `Haskoin.Store.Database.Query` | not started |
| The cache on two layers | `Haskoin.Store.Cache` | designed below, not started |

Three findings came out of the work, in order of how soon they matter:

1. **Bloom filters were never built.** `--bloom` has no effect with the
   RocksDB binding as released, on RocksDB 6.24 or later. The fix is
   [rocksdb-haskell pull request 21](https://github.com/jprupp/rocksdb-haskell/pull/21).
   See [Bloom filters](#bloom-filters-were-not-being-built).
2. **The migration works, and the merge gives today's answers.** For 350
   random histories of blocks, payments, replacements, deletions and
   reorganisations, the migrated database answers every query as the
   version 20 database did, and its chain area is that of a database that
   never saw a mempool. See [Evidence](#evidence).
3. **The extra lookups are cheap with filters and almost free in memory.**
   Between 0.5 and 1.1 microseconds to find that a transaction, an output or
   an address has nothing in the mempool area, against 0.05 to 0.1 in
   memory. See [What it costs](#what-it-costs-measurements).

## The problem

### How the mempool leaks into the chain today

In version 20 a transaction in the mempool is written into the same indices
as the block chain. `Haskoin.Store.Logic` does this when it imports one
(`commitAddTx`):

| Column family | What a mempool transaction writes there |
|---|---|
| `tx` | Its body, at a mempool position. |
| `tx` | The body of every transaction whose output it spends, a confirmed one included, rewritten to name it as the spender (`insertSpender`). |
| `unspent`, `addr-out` | Its outputs, as unspent. The outputs it spends are deleted (`spendOutput`). |
| `addr-tx` | An entry under every address it touches. |
| `balance` | The balance of every address it touches: an output it spends leaves the confirmed balance at once, its own outputs go to the unconfirmed balance, and the counts of transactions and outputs and the amount received all change. |
| `mempool` | Its place in the list of the mempool and in the index by time. |

When a block confirms the transaction, all of that is written a second
time, at the new position, and the first copy is deleted (`confTx`). When the
transaction is replaced or dropped, all of it is undone, and the body is
kept, marked as deleted (`commitDelTx`).

This is done for speed. A query reads one place and gets the answer.

### What that costs

- **Nothing is immutable.** Every index, and the body of every confirmed
  transaction with an unspent output, can be changed by an unconfirmed
  transaction that may be gone a minute later. No part of the database can
  be said to follow from the block chain alone.
- **The cache inherits it.** The Redis cache copies these merged structures
  and has to follow every change to them, from several stores whose mempools
  differ. Every hole in `docs/cache-consistency.md` is a change that the
  cache did not hear of.
- **Mistakes in the mempool reach the chain's data.** The rules that import
  a mempool transaction write to balances and to bodies of confirmed
  transactions. A bug there, or a mempool transaction that should not have
  been taken, damages data that only a resync restores.
- **Everything is written twice.** A transaction first seen in the mempool is
  written to the large column families, deleted, and written again when it
  is confirmed.
- **Wiping the mempool is slow and delicate.** `--wipe-mempool` has to undo
  every transaction against the chain's indices, in the right order.

## The design

### Two areas

**The chain area** is the column families that exist today, with the same
keys and the same encodings. Only their contents change:

| Column family | Today | Layered |
|---|---|---|
| `tx` | Confirmed, unconfirmed and deleted bodies; spenders of any kind | Confirmed bodies; spenders in blocks only |
| `unspent`, `addr-out` | Outputs that neither a block nor the mempool spends, those of the mempool included | Outputs of confirmed transactions that no confirmed transaction spends |
| `addr-tx` | Confirmed and unconfirmed transactions | Confirmed transactions |
| `balance` | With the mempool applied | Of the chain alone; the unconfirmed balance is always zero |
| `block`, `height`, best block | No mempool data | Unchanged |

Because the encodings do not change, the chain's data is not rewritten, and
no resync is needed.

**The mempool area** is two column families. `mempool` exists today and
holds the list of the mempool; `mempool-addr` is new.

| Column family | Key | Value | Meaning |
|---|---|---|---|
| `mempool` | `MemTxKey` txid | time | A transaction is in the mempool (as today). |
| `mempool` | `MemTimeKey` time, txid | nothing | The mempool by time (as today). |
| `mempool` | `TxKey` txid | body | Body of a mempool transaction, without spenders. |
| `mempool` | `MemSpenderKey` outpoint | spender | A mempool input spends this output, of a confirmed or an unconfirmed transaction. |
| `mempool` | `MemDeletedKey` txid | body | A transaction that left the mempool unconfirmed. |
| `mempool-addr` | `MemAddrTxKey` address, time, txid | nothing | A mempool transaction touches the address. |
| `mempool-addr` | `MemAddrSpentKey` address, outpoint | amount | The mempool spends this output of the address. |
| `mempool-addr` | `MemAddrOutKey` address, time, outpoint | amount, script | The mempool pays this output to the address. |

These are the same kinds of data as the chain has, apart from blocks:
bodies, spenders, address transactions, address outputs. Balances are the
exception. They are not stored for the mempool; they are worked out from the
entries of the address, for the reason given next.

The address entries are in a column family of their own so that it can be
filtered by address, like `addr-tx` and `addr-out`. A scan of an address
with nothing in the mempool, which is nearly every address, then stops at
the filter. The measurements below show the difference.

### Every key belongs to one transaction

Each key of the mempool area depends on one mempool transaction alone: on
its inputs, with the outputs they spend, which the body carries, and on its
outputs (`mempoolRecords`). The mempool area is the union of the keys of its
transactions.

- A transaction enters the mempool by writing its keys, and leaves by
  deleting them. No other key is read or changed.
- No key says whether a spent output was created by a block or by the
  mempool, nor whether a created output has been spent. So when a block
  confirms a transaction and not the ones that spend it, the keys of those
  stay exactly as they are.
- The area can be checked, or rebuilt, from the bodies alone.

What a query needs is worked out when it is answered, from the entries of
one address:

| The mempool... | Then the output is... |
|---|---|
| creates it and does not spend it | an unspent output of the mempool |
| spends it and did not create it | one that the chain has as unspent, to be left out |
| creates it and spends it | in neither list; it only counts as received |

**Why not store the mempool's balances and unspent outputs as values?** That
is the obvious reading of "the same types of data as the chain", and it is
what version 20 does in place. A stored value with the mempool applied
depends on the chain under it. It is wrong as soon as a block pays the
address, so every block would have to find and correct the values it made
stale. That is `confTx` again, with the same room for error. Entries that say
what the mempool *does*, instead of what the result *is*, stay right whatever
the chain does.

The one field that depends on other transactions is the replace-by-fee flag
of a body, which a transaction inherits from unconfirmed ancestors. It is
corrected when an ancestor is confirmed, as today (`updateSpendersRBF`), and
only in the mempool area.

### Deleted transactions

The store keeps the body of a transaction that was replaced or dropped,
marked as deleted, so that the API can say what became of it. Version 20
keeps it in `tx`, which means that deleting a mempool transaction writes to
the chain area. In the layered layout it goes under `MemDeletedKey`, in the
mempool area.

The coinbase of a block that is reverted goes there too. Emptying the whole
mempool area forgets these bodies along with the mempool; an orderly wipe can
keep them, since they have a prefix of their own.

Bodies that version 20 already marked as deleted stay in `tx`. Finding them
would take a scan of the whole column family. Nothing refers to them, and
the lookup of a transaction falls back to them, so the API answers the same.

### The time a transaction was first seen

One thing in the chain area still comes from the mempool: a confirmed body
keeps the time the transaction was first seen, if it was in the mempool
before its block, and the API returns it. It is written once, when the block
is imported, and never changed. Using the time of the block instead would
make the chain area a pure function of the chain, at the cost of a small
change to the API. This design keeps the first-seen time.

### Invariants

1. The chain's column families hold what follows from the blocks of the main
   chain up to the best block, and nothing else, except the two things
   above: first-seen times, and bodies deleted by version 20.
2. The mempool area is the union of the keys of the mempool's transactions.
3. Every output that a mempool transaction spends exists: the chain has it
   as unspent, or another mempool transaction creates it.
4. No two mempool transactions spend the same output.
5. No transaction is both confirmed and in the mempool.

Given these, the merge below gives exactly what version 20 stores. The
migration checks 3 and 5 for every transaction it moves, and 4 holds because
the spender of an output is a single key.

### One door into the chain

In the layered layout only two operations write to the chain area: importing
a block and reverting one. Both check the block against the chain area alone.
Nothing about the mempool is read to decide what the chain area holds, and
nothing the mempool does can change it.

- A block is imported the same way whether or not the store had seen its
  transactions before, as during the initial sync. The second path of today,
  which confirms transactions already in the mempool by moving their entries
  (`confTx`, `replaceAddressTx`, `adjustAddressOutput`), goes away.
- A mempool area that is wrong for any reason can be emptied. The chain area
  is untouched, and the mempool fills again from the peers.

## Reading: the mempool applied on top

`Haskoin.Store.Database.Mempool` has the merge. Each query reads the chain,
reads what the mempool area has for the same thing, and combines them.

| Query | Chain area | Mempool area | Combined |
|---|---|---|---|
| Transaction | body, spenders in blocks | body if unconfirmed; mempool spenders of its outputs | the body from where it is, with both sets of spenders |
| Spender of an output | from the body of its transaction | `MemSpenderKey` | the mempool's, or else the chain's |
| Unspent output | `unspent` | spender; body of its transaction | none if the mempool spends it; else the chain's, or an output of a mempool body |
| Transactions of an address | `addr-tx` | `MemAddrTxKey` entries | the mempool's, then the chain's |
| Unspent outputs of an address | `addr-out` | the address entries | the mempool's unspent ones, then the chain's that the mempool does not spend |
| Balance of an address | `balance` | the address entries | the formula below |

**Balance.** With *hidden* the outputs that the mempool spends and did not
create, and *fresh* those it creates and does not spend:

```
confirmed   = chain.confirmed - sum of hidden
unconfirmed = sum of fresh
utxo        = chain.utxo - count of hidden + count of fresh
txs         = chain.txs + count of mempool transactions of the address
received    = chain.received + sum of everything the mempool pays to it
```

**An example.** A miner's address M has one coin of 5,000,000,000 from a
block. T1, in the mempool, spends it: 3,000 to A and 2,000 change to M. T2,
also in the mempool, spends the change: 1,500 to B.

| | Version 20 stores for M | Layered: chain | Layered: mempool entries of M |
|---|---|---|---|
| confirmed | 0 | 5,000,000,000 | spends the coin: hidden |
| unconfirmed | 0 | 0 | creates T1:1 and spends it: neither |
| utxo | 0 | 1 | |
| txs | 3 | 1 | T1, T2 |
| received | 5,000,002,000 | 5,000,000,000 | T1:1 pays 2,000 |

The formula gives 0, 0, 0, 3 and 5,000,002,000, as stored today.

Now a block confirms T1 and not T2. T1's keys are deleted from the mempool
area and the block is imported into the chain, where M now has 2,000
confirmed in T1:1. T2's keys have not changed. Its entry "spends T1:1" used
to cancel against T1's "creates T1:1"; now nothing creates T1:1 in the
mempool, so it is hidden from the chain's 2,000. The answer for M is the same
as before the block, as it should be, and nothing had to be converted.

**Order.** The API lists newest first, and unconfirmed transactions are
newer than any block. Today's indices keep mempool entries before block
entries for that reason. So the merged list is the mempool's part followed
by the chain's part, and it is the same sequence that the single index gives
today. Every rule about starts, offsets and limits applies to it unchanged.
Ties between mempool transactions of the same second are broken by the same
bytes as today, because the keys have the same layout.

**A consistent view.** Reading two areas at different moments can show a
transaction twice, or not at all, if a block is imported in between; a
balance could even take the same payment off twice. Today's queries read
without a snapshot and get away with it, because each key is whole in
itself. The layered queries must read both areas through one RocksDB
snapshot. `withSnapshot` gives a database handle that the existing query
functions can use as they are; creating and releasing one took about 1.1
microseconds in the measurements. One snapshot for each request also ends
the mixed reads that version 20 can produce while a block is imported.

## Writing

| Event | Version 20 writes | Layered writes |
|---|---|---|
| A transaction enters the mempool | `tx`, `addr-tx`, `addr-out`, `unspent`, `balance`, bodies of the transactions it spends, `mempool` | its keys in the mempool area |
| A mempool transaction is replaced or dropped | all of that undone; its body kept as deleted in `tx` | its keys deleted; its body under `MemDeletedKey` |
| A block confirms a mempool transaction | its entries moved: deleted and written again in every index | its keys deleted from the mempool area; the block imported into the chain area |
| A block has a transaction never seen | imported into every index | the same |
| A block conflicts with a mempool transaction | that transaction and its descendants undone in every index, then the block imported | their keys deleted; the block imported |
| A block is reverted | its transactions moved back to mempool positions | its transactions removed from the chain area and their keys added to the mempool area |
| The mempool is wiped | every transaction undone, in order | every key of the mempool area deleted |

Each event is still one atomic RocksDB batch across both areas.

- **A transaction first seen in the mempool is written to the large column
  families once**, when its block comes, instead of written, deleted and
  written again.
- **A block at the tip costs what it costs during the initial sync**, where
  no transaction is in the mempool. Today part of that work was done when
  the transactions arrived.
- **Mempool rules become adding and removing keys.** What is left of them is
  the checks: that the outputs exist and are unspent, in the chain or in the
  mempool, and what a new transaction replaces.

## What it costs: measurements

The cost of the design is that a query visits two places. I measured the
second visit. `docs/layered-database/OverlayCost.hs` fills the two column
families as laid out above with synthetic mempool transactions, 13 keys
each, and times each kind of lookup through the Haskell binding; the output
is in `docs/layered-database/overlay-cost.txt`.

Microseconds for one lookup, read through a snapshot, with the data in
table files, RocksDB 11.1.1 configured as the store configures it:

| Lookup | No filters, 50,000 txs | No filters, 200,000 txs | With filters, 50,000 txs | With filters, 200,000 txs | In memory |
|---|---|---|---|---|---|
| Is this transaction in the mempool? (one of 100 in a multi-get) | 2.5 | 4.0 | 0.5 | 0.6 | 0.04–0.06 |
| Does the mempool spend this output? (one of 100 in a multi-get) | 2.4 | 4.0 | 0.5 | 0.7 | 0.05–0.09 |
| Address with nothing in the mempool, `mempool-addr` | 2.8 | 6.6 | 0.8 | 1.1 | 0.05–0.12 |
| Same, if the entries were in `mempool`, unfiltered by address | 3.9 | 6.7 | 4.0 | 6.3 | |
| Address with one mempool transaction | 6.3 | 9.8 | 6.3 | 6.5 | 0.3–0.5 |
| Mempool spenders of a transaction, by scan | 3.7 | 5.5 | 3.9 | 5.6 | |

"No filters" is the binding as released. "With filters" is the binding with
the fix described below. Other measurements: a snapshot costs about 1.1
microseconds to create and release, and an iterator 1.4 to 2.0.

What that means for a request, with filters and 200,000 transactions in the
mempool:

| Request | Extra lookups | Extra time in RocksDB | In memory |
|---|---|---|---|
| Balance of an address | 1 scan | 1 µs | 0.1 µs |
| Page of 100 transactions with spenders | 100 gets and 100 scans | 0.6 ms | about 0.02 ms |
| Balances of an xpub with 1,000 addresses | 1,000 scans | 1.1 ms | 0.1 ms |
| The same, without filters | 1,000 scans | 6.6 ms | 0.1 ms |

These are the lookups alone, in a small benchmark. What a whole request
gains or loses has to be measured on the real code with
`scripts/bench-web.sh`, once the queries exist.

- **Without filters the design would be slow** for wallets with many
  addresses. The mempool's column families must have them.
- **With filters it costs about a microsecond to find that the mempool has
  nothing**, which is the usual answer, and 6 for an address or a
  transaction where it has something. That is small against reading the
  chain's own data, which is the same work as today.
- **Memory is about ten times faster again**, should the lookups ever need
  to be; see the next section.

### Bloom filters were not being built

Lookups of keys that are not there cost as much as lookups of keys that are,
which is what a Bloom filter is meant to prevent. The table files had none:
RocksDB reported `filter block size=0; # entries for filter=0`.

- **Cause.** `rocksdb_filterpolicy_create_bloom_full` takes its bits per key
  as a `double` since RocksDB 6.24. `rocksdb-haskell-jprupp` imports it with
  a `CInt`. The two travel in different registers, so RocksDB reads
  something else as the bits per key and builds no filter. Nothing fails.
- **Effect on the store.** `--bloom` does nothing when the store is built
  against RocksDB 6.24 or later. It is off by default, so most stores were
  not expecting filters anyway. I have not checked which RocksDB the
  production build links against.
- **Fix.** [Pull request 21](https://github.com/jprupp/rocksdb-haskell/pull/21)
  on the binding. With it, the same column family has 3,250,201 bytes of
  filters for 2,600,000 keys, the 10 bits per key asked for, and a lookup of
  an absent key takes 1.5 microseconds instead of 5.3 to 5.9.
- **Also found.** Comparing all 72 foreign imports of the binding with the C
  header showed one more that had drifted: `approximateSize` called RocksDB
  with one argument too few. The store does not use it. The pull request
  fixes it too.
- **Before turning `--bloom` on.** Once the store is built with the fixed
  binding, `--bloom` will build filters for the chain's column families, in
  the table files written from then on. They take about 1.25 bytes for each
  key, and the store keeps them in its block cache. On a chain with billions
  of keys that is gigabytes competing with data for a cache of 1 GiB by
  default. It should be sized before it is enabled.
- **The mempool's column families always have filters** in the layered
  layout, whatever `--bloom` says. They are small: 10 bits for each key, and
  a mempool transaction has about a dozen keys.

### What it does to the code

- **Simpler: writing.** Blocks are imported one way. The rules that move a
  transaction's entries when it is confirmed or sent back to the mempool
  (`confTx`, `replaceAddressTx`, `adjustAddressOutput`, `updateMempool`) go
  away, and the mempool rules come down to checks and to adding or removing
  the keys of a transaction. Wiping is deleting keys.
- **More complex: reading.** Every query has two sources and needs a
  snapshot, and paging has to run across both. The merge itself is small:
  the five functions that apply the mempool to a balance, a list of unspent
  outputs and a list of transactions are about forty lines.
- **More complex: the transition.** Two layouts, a migration, and a cache
  protocol to specify again.

## Keeping the mempool in memory

The mempool is bounded by age, not by size: `--mempool-timeout` deletes
transactions after 14 days by default. There are three ways to hold it.

| | What it is | For | Against |
|---|---|---|---|
| **RocksDB only** | Both column families, read through the request's snapshot. | Nothing new to keep right. Consistent by construction. No memory. | About a microsecond for each lookup. |
| **Mirror in memory** | The indices of the mempool area, decoded, in immutable maps, updated with every write. Bodies stay in RocksDB. | 40 to 120 nanoseconds for each lookup. | 1.8 KB for each transaction as measured: 87 MiB for 50,000, 345 MiB for 200,000. All of it live data for the garbage collector. |
| **Memory first** | The mempool lives in memory and is saved now and then. | No write for a mempool transaction. | A restart forgets transactions that the API already showed. After a crash the saved mempool is older than the chain area and has to be checked against it. |

**Recommendation: RocksDB only at first, the mirror if measurements ask for
it, and not memory first.**

- **Write through always.** A write to a small column family is cheap, and a
  batch that covers both areas is what keeps them in step. Saving the
  mempool later gives that up for little.
- **The mirror is an accelerator, never the truth.** It is built by decoding
  the column families at start, and kept up to date by applying to it the
  same keys that each batch writes. A counter written with every batch says
  which state a snapshot belongs to. A request uses the mirror when its
  counter matches the snapshot's, and reads RocksDB otherwise, so it can
  never answer from a mirror that is ahead or behind.
- **Cached xpubs do not need it.** In the cache design below, both layers of
  a cached xpub are in Redis, so a cached request does not read the store's
  own mempool area for the xpub's addresses. The mirror would help the
  queries that read the database: addresses, pages of transactions, and
  xpubs that are not cached. There the lookups add about a millisecond for
  1,000 addresses, to a query that already reads 1,000 balances of the
  chain. Whether that calls for a mirror is for `scripts/bench-web.sh` to
  say, once the queries exist.
- **1.8 KB for each transaction is a first measurement**, with ordinary maps
  and lists. It can be made smaller. Since the mempool has no bound on its
  size, a mirror should come with a measurement of the heap under load,
  which the stress test already records.

## The cache under the same rules

This is the reason for the whole change. The design here is less far along
than the database's: nothing of it is built, and it should be specified in
`Haskoin.Store.Cache.Protocol`, with its model tests, before it is.

### Two layers in Redis

An entry of a cached xpub has two layers, like the database.

- **Chain layer.** The xpub's confirmed transactions, its unspent outputs
  in the chain and the chain balances of its addresses, with the hash and
  height of the block they reflect. It changes only when a block is applied
  to it.
- **Mempool layer.** The entries of the mempool for the xpub's addresses:
  the same three kinds as `mempool-addr` has. Bodies of mempool
  transactions go in the shared store of bodies, as today.

A request reads both layers from Redis and merges them with the same
functions as the database. Every store sharing the Redis reads the same two
layers, so they give the same answer for a cached xpub, mempool included,
whatever their own mempools hold.

### Why this is easier than today

- **The chain layer is the same whoever writes it.** It follows from the
  chain at a block and the xpub's addresses. Two stores at the same block
  compute the same data, apart from the time a transaction was first seen,
  so it does not matter which one writes. "A store never replaces an entry
  that knows more" becomes "an entry only moves forward along the chain",
  decided by height and hash.
- **The chain is the change log.** An entry at block B is brought to block
  B' by applying the blocks in between, read from the store's own chain
  area. Nothing depends on having heard every event. This is what I ruled
  out in `docs/cache-consistency.md` as impossible with private logs: the
  chain is a log that every store shares.
- **A missed block cannot hide.** In the chain area, every confirmed
  transaction that touches an address adds one to the count of transactions
  in its chain balance. Comparing the chain balances of the xpub's addresses
  with the entry's is an exact test of whether the entry has every block.
  The cache already makes this comparison; today it has blind spots because
  the mempool moves the same numbers.
- **Mempool entries commute.** They belong to one transaction each, and any
  store writes the same entries for the same transaction. Adding them twice
  changes nothing.

### Rules for the shared mempool layer

- **Add.** A store that has a transaction in its mempool adds its entries to
  the entry of every cached xpub it touches, provided the store is at the
  entry's block or later.
- **Conflicts.** Two transactions that spend the same output cannot both be
  in an entry. The one already there stays, unless the store adding the
  other has replaced it by its own rules.
- **Remove for a reason only.** A store removes a transaction's entries when
  a block it applies confirms the transaction or conflicts with it, when it
  replaced the transaction, or when the transaction is too old. A store
  that simply does not have the transaction leaves it alone: another store
  does have it.
- **A block is applied to an entry in one script.** The chain layer's
  changes, the removal of what the block confirmed or made invalid, and the
  new block hash, together. A reader never sees half of it.
- **A store that is behind does not write.** It serves the entry as it is,
  which is how it gives the same answers as the stores that are ahead.
- **Any store in sync can repair an entry.** It can compare both layers with
  its own database at its block: add what it has and the entry lacks, and
  remove what it knows to be confirmed or invalid. Lost events then only
  delay an entry; they cannot leave it wrong.

### What this corrects

| Problem in `docs/cache-consistency.md` | What happens to it |
|---|---|
| A spend that confirms without changing a balance | In the chain layer a confirmation always changes chain balances. |
| A reorganisation that keeps every balance | The entry carries the hash of its block. |
| A mempool replacement with the same amounts | The mempool layer is compared entry by entry, not by balance. |
| Spenders of cached bodies never repaired | Spenders in blocks are applied block by block from a marker in Redis, and caught up by chain if a block is missed. Mempool spenders are not in cached bodies at all. |
| Scheduled full reloads and their memory peaks | Not needed: the check is exact and reads only balances. |
| Claims, and "knows more" by an unseen transaction | Chain work is ordered by block; mempool entries need no order. |

### What stays open

- **The gap.** The addresses of an xpub depend on which are used, and a
  mempool payment counts as use. So the set of addresses of an entry can
  grow or shrink with the mempool, and with it the chain data that belongs
  in the entry. This is rare, and the entry can be rebuilt when it happens,
  as today.
- **Mempool spenders of outputs outside the xpub.** A cached transaction
  that pays someone else shows that output as spent when the other party
  spends it. For unconfirmed spends, each store would answer from its own
  mempool, so stores can differ on that one field until the spend confirms.
- **Stores that disagree.** If two stores each took one of two conflicting
  transactions, the entry shows the one that reached Redis first. That is a
  rule, not a truth, in the same way that a node keeps the first of two
  conflicting transactions that it sees.
- **Sharing transactions as well.** A store could import from Redis the
  mempool transactions it does not have, through its own rules. Then the
  stores' own mempools converge, and uncached queries agree too. This is an
  addition, not a requirement.

## Migration

`Haskoin.Store.Database.Migrate` moves a database of version 20 to the
layered layout, version 21.

### What it does

For each transaction in the mempool, descendants before the transactions
they spend:

1. Take it out of the chain's indices, exactly as deleting it does in
   version 20: its address entries, its outputs, its place as spender in the
   bodies of the transactions it spends, the outputs it spent made unspent
   again, and the balances.
2. Remove its body from `tx`. It is not kept as deleted: it moves.
3. Write its keys to the mempool area.

When it is done the chain area holds what it would hold if the mempool had
been wiped, and the mempool area holds the mempool, with the same arrival
times and flags.

### Why it is safe

- **It only reads and writes what the mempool touches.** The time it takes
  is in proportion to the mempool, not to the chain.
- **It refuses before it writes.** Everything the mempool lists is read
  first. A database that is not as version 20 says is refused untouched,
  and a store of version 20 can still open it.
- **It works in batches, each atomic.** After any batch the database is a
  valid mix: transactions already moved are in the mempool area, the others
  still in the chain's indices. It can be interrupted and carried on.
- **It marks the database.** The first write sets version 21 and a marker.
  From then on a store of version 20 refuses the database, and a store that
  knows the layout carries the migration on. The last write removes the
  marker.
- **It refuses rather than guesses.** A balance that would go below zero, or
  a transaction that spends something that is not there, stops the migration
  before the batch is written.
- **It checks its result.** Every transaction moved is looked up again: it
  must be in the mempool area, have left nothing in the chain's indices, and
  spend only outputs that exist.
- **It has its own copy of the rules.** It does not call
  `Haskoin.Store.Logic`, whose rules will change with the layout. The tests
  check its rules against `Haskoin.Store.Logic` while both exist.

### How long it takes

`docs/layered-database/MigrateBench.hs`, with 30,000 mempool transactions on
this machine, database on a RAM disk:

| | Seconds |
|---|---|
| Wiping the mempool with the rules of version 20 | 4.2 |
| Migrating it | 5.0 |

About 6,000 transactions a second, the check at the end included. A mempool
of 300,000 would take under a minute at that rate. On a large database on
disk it will be slower, as wiping is, because the bodies of the confirmed
transactions that the mempool spends have to be read.

### Running it

It is not wired in. `withLayeredReader` opens a database with the new column
family, and `migrateLayered` does the rest. When the store moves to version
21, `initRocksDB` will call it for a database of version 20, as it calls the
migrations from versions 18 and 19 today.

- **There is no way back.** RocksDB opens a database only when given all its
  column families, so a store of version 20 cannot open one that has
  `mempool-addr`. Take a copy or a file system snapshot of a production
  database before upgrading.
- **If it stops with an error,** the database stays marked as being
  migrated. The way out that needs no old binary is to empty the mempool
  area, which the layout makes safe at any time, and to wipe what is left of
  the mempool in the chain's indices with the migration's own rules. That
  path is not written yet.

## Evidence

`test/Haskoin/Store/Database/MigrateSpec.hs` builds databases of version 20
with the store's own rules, migrates them, and compares.

- **Random histories.** 50 histories each time the suite runs, each of 52
  steps chosen at random among: a payment from outputs that are unspent, in
  a block or in the mempool, between five addresses and a script that has no
  address, some signalling replace-by-fee; a block with part of the mempool;
  a transaction that replaces one in the mempool; the deletion of one; and
  the reversal of the best block. The histories differ from run to run: 350
  of them passed over seven runs. In the last of those runs, all 50 ended
  with ten or more mempool transactions and with ten or more of them
  spending another mempool transaction, and 74% had deleted ten or more
  along the way.
- **Every answer is kept.** After the migration, the merged queries give
  what version 20 gave: the balance, transactions and unspent outputs of
  every address, the data of every transaction ever made with its spenders,
  the spender and the unspent state of every output, and the mempool.
- **The chain area is clean.** Its column families are compared key by key
  with those of a second database that imported the same blocks and never
  saw a mempool. They are equal, apart from first-seen times and what
  version 20 left behind: deleted bodies and balances of zero.
- **The mempool area is exact.** Its two column families hold the keys of
  the mempool's transactions, each worked out from that transaction alone,
  and nothing else.
- **Wiping is safe.** With every key of the mempool area deleted, the
  answers are those of that second database.
- **Interruption.** The migration is stopped after each of the first six
  writes of a chain of eight transactions, and carried on; the result is
  the same.
- **Refusals.** A database of another version, and one whose mempool lists a
  transaction with no body, are refused with nothing written.
- **The tests can fail.** With one rule of the migration deliberately wrong,
  not taking a payment off the amount received, three of the four fail.

What this does not show: the rules that will import blocks and transactions
in the new layout do not exist yet, so nothing here tests them. The
migration gives the way to: for any history, the database that the new rules
build must be key for key the one that the old rules build and the migration
then converts.

## Order of work

Each step ends with something that can be tested against today's behaviour.

1. **Build the store with the fixed RocksDB binding.** Needed for the read
   costs above. Independent of everything else.
2. **A snapshot for each request.** No change of layout. It also ends the
   mixed reads of today.
3. **New rules, beside the old.** Chain rules that import and revert blocks
   in the chain area alone; mempool rules that add and remove keys. Test:
   for random histories, the same database, key for key, as the old rules
   followed by the migration.
4. **Paged queries over both areas.** Test: the same pages as version 20,
   for random limits, offsets and starts, on migrated databases.
5. **The switch.** Version 21: the store opens with `mempool-addr`, runs the
   migration, uses the new rules and queries; `--wipe-mempool` empties the
   mempool area; the old paths are deleted. Then measure with
   `scripts/bench-web.sh` and the stress test.
6. **The mirror in memory**, if the measurements of step 5 call for it.
7. **The cache on two layers**, specified and model-tested in
   `Haskoin.Store.Cache.Protocol` first.

Steps 1 and 2 are small and useful by themselves. Steps 3 to 5 are the
database change. Step 7 is where the concurrency problems of the cache are
actually removed.

## Alternatives considered

- **Keep the mempool's merged values in the mempool area**, as a
  copy-on-write layer over the chain's keys. The simplest reads: look in the
  mempool area, then in the chain. Rejected because the values go stale when
  the chain changes; see
  [Every key belongs to one transaction](#every-key-belongs-to-one-transaction).
- **A stored balance delta for each address.** One point lookup for each
  balance instead of a scan. Rejected for now: the split between confirmed
  and unconfirmed depends on whether a spent output's transaction is still
  in the mempool, so deltas have to be converted when a block confirms a
  parent and not its child. A marker for "this address has mempool
  entries" would get most of the gain without that, and the filter by
  address already gives it.
- **One column family for the whole mempool area.** Simpler to wipe.
  Rejected because address scans could not use a filter: 4.0 to 6.3
  microseconds against 0.8 to 1.1 for an address with nothing in the
  mempool.
- **Fix the cache only**, by reading spenders from the database when
  serving, as `docs/cache-consistency.md` first proposed. It closes one hole
  and leaves the chain's data as changeable as before.
- **Anchor the chain area some blocks below the tip**, so that
  reorganisations never touch it. Not needed in the database: blocks are
  reverted atomically, one at a time, as today, and what was in them goes
  back to the mempool area in the same write.
- **Move the bodies deleted by version 20 out of `tx`.** It would make the
  chain area cleaner, at the cost of a scan of the largest column family.

## Open questions

1. **Deleted transactions.** Kept for ever, as today, they make the mempool
   area grow without bound. Deleting them after `--mempool-timeout` would
   bound it, and change what the API says about a transaction dropped weeks
   ago, from deleted to unknown. And should an orderly wipe record the
   wiped transactions as deleted, as `--wipe-mempool` does today? Today a
   deleted transaction is not requested again when a peer announces it, so a
   wipe that records them keeps them out until they confirm.
2. **First-seen time or block time** for confirmed transactions.
3. **`--bloom` for the chain**, once it works: how much memory it needs on
   a full database, and whether it should stay off by default.
4. **The conflict rule of the shared mempool layer**: first to reach Redis
   wins.
5. **A bound on the size of the mempool**, if the mirror in memory is built.
