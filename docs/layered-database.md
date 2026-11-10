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
- [What has to change before the switch](#what-has-to-change-before-the-switch)
- [Keeping the mempool in memory](#keeping-the-mempool-in-memory)
- [The cache under the same rules](#the-cache-under-the-same-rules)
- [A fleet that answers as one](#a-fleet-that-answers-as-one)
- [Migration](#migration)
- [Evidence](#evidence)
- [Order of work](#order-of-work)
- [Alternatives considered](#alternatives-considered)
- [Open questions](#open-questions)

## Where this stands

The store runs on the layered layout, version 21. A database of version 20
is migrated when the store opens it. The rules and queries of version 20
are still in the library, as the reference that the tests hold the new ones
to.

| Piece | Where | State |
|---|---|---|
| Keys of the mempool area | `Haskoin.Store.Database.Types` | written |
| The keys of one transaction, and what the mempool does to a balance | `Haskoin.Store.Database.Mempool` | written |
| Migration from version 20 | `Haskoin.Store.Database.Migrate` | written, tested, run when the store opens a database (`upgradeDatabase`) |
| Rules that import blocks and transactions | `Haskoin.Store.Logic.Layered` | written, tested against the rules of version 20, used by the block store |
| Every query of the store over both areas, pages included, on a snapshot | `Haskoin.Store.Database.Query.Layered` | written, tested against today's queries, measured: right, and too slow in two cases; not used by the store |
| Tests against today's database, rules and queries | `test/Haskoin/Store/Database/MigrateSpec.hs`, `test/Haskoin/Store/Logic/LayeredSpec.hs`, `test/Haskoin/Store/Database/Query/LayeredSpec.hs` | passing |
| Cost of lookups, of whole queries and of writing | `docs/layered-database/` | measured |
| A summary for each address, and lists read by the page | `MemSummary`, `Haskoin.Store.Database.Mempool`, `Haskoin.Store.Database.Query.Layered` | written, tested: each summary is what its entries add up to after every operation |
| The web server on the new queries, one snapshot for each request | `Haskoin.Store.Web`, `routerScoped` | done |
| The cache on two layers | `Haskoin.Store.Cache` | designed below, not started |

Six findings came out of the work, in order of how soon they matter:

1. **Bloom filters were never built.** `--bloom` had no effect on RocksDB
   6.24 or later, because of a wrong type in the RocksDB binding. It is
   fixed, in
   [rocksdb-haskell pull request 21](https://github.com/jprupp/rocksdb-haskell/pull/21),
   and the store is built with the fixed binding. See
   [Bloom filters](#bloom-filters-were-not-being-built).
2. **The migration works, and the merge gives today's answers.** For
   hundreds of random histories of blocks, payments, replacements, deletions
   and reorganisations, the migrated database answers every query as the
   version 20 database did, and its chain area is that of a database that
   never saw a mempool. See [Evidence](#evidence).
3. **The new rules do what today's rules do.** Given the same random
   history, the rules of the layered layout and today's rules followed by
   the migration leave the same database, key for key, and every operation
   has the same result on the way. So does a store that is upgraded in the
   middle of the history, which is what a store in production will be. See
   [Evidence](#evidence).
4. **The new queries give today's pages.** Every query that the web server
   uses is written for the two areas, on a snapshot of the database. Asked
   for pages with random limits, offsets and starts, about addresses and an
   extended public key, they answer as today's queries do, on migrated
   databases and on databases that the new rules built. See
   [Evidence](#evidence).
5. **The queries as built are not ready for the switch.** They cost 10 to 15
   microseconds more for a request when the mempool has nothing for what is
   asked, which is fine. But they read all that the mempool has for an
   address on every question about it, 35 milliseconds for a busy one, and
   right after a block they take 1.6 milliseconds to find that an address
   has nothing. See [What it costs](#what-it-costs-measurements) and
   [What has to change before the switch](#what-has-to-change-before-the-switch).
6. **Two things in the RocksDB layer hold back today's store as well.** A
   scan through a Bloom filter can step over every key deleted lately, so
   `--bloom` no longer puts filters on the indices by address. And the
   binding reads rows three times slower than it could.

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

The coinbase of a block that is reverted goes there too. A deleted body is
removed when its transaction comes back, into the mempool or in a block, so
a transaction is in one place at most. Emptying the whole mempool area
forgets these bodies along with the mempool; an orderly wipe can keep them,
since they have a prefix of their own.

Bodies that version 20 already marked as deleted stay in `tx`. Finding them
would take a scan of the whole column family. Nothing refers to them, and
the lookup of a transaction falls back to them, so the API answers the same.

Such a leftover can outlive its use. If its transaction comes back into the
mempool after the upgrade, the new body goes to the mempool area, and the
old one stays in `tx` until a block confirms the transaction and writes over
it, or for good if the transaction is deleted again. So a lookup takes the
first body it finds in this order: the mempool's, a confirmed one, one
deleted since the upgrade, one deleted before it.

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
2. The mempool area is the union of the keys of the mempool's transactions,
   and for each address that they touch a
   [summary](#1-a-summary-for-each-address-that-the-mempool-touches) that
   is a function of the entries of that address.
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
- A block that spends an output of an unconfirmed transaction is refused,
  because that output is not in the chain. Today such a block is imported:
  the indices do not tell a confirmed output from an unconfirmed one, and the
  result is a confirmed transaction whose parent is still in the mempool.
  This is the one place where the new rules differ from today's on purpose.
- A mempool area that is wrong for any reason can be emptied. The chain area
  is untouched, and the mempool fills again from the peers.

## Reading: the mempool applied on top

`Haskoin.Store.Database.Query.Layered` has every query of
`Haskoin.Store.Database.Query` for the two areas, with the same names and the
same arguments. Each reads the chain, reads what the mempool area has for the
same thing, and combines them. `layeredRead` gives them as the record of
queries that the web server answers with, as `databaseRead` gives today's.

| Query | Chain area | Mempool area | Combined |
|---|---|---|---|
| Transaction | body, spenders in blocks | body if unconfirmed; mempool spenders of its outputs | the body from where it is, with both sets of spenders |
| Transactions with a short number | scan of `tx` by the start of the hash | the same scan of its bodies and of the deleted ones | all of them, in the order of their hashes |
| Spender of an output | from the body of its transaction | `MemSpenderKey` | the mempool's, or else the chain's |
| Unspent output | `unspent` | spender; body of its transaction | none if the mempool spends it; else the chain's, or an output of a mempool body |
| Transactions of an address | `addr-tx` | `MemAddrTxKey` entries | the mempool's, then the chain's |
| Unspent outputs of an address | `addr-out` | the address entries | the mempool's unspent ones, then the chain's that the mempool does not spend |
| Balance of an address | `balance` | the address entries | the formula below |
| Number of transactions of some addresses | distinct keys of `addr-tx` | distinct transactions in the address entries | the sum: no transaction is in both |

The queries about an extended public key are built from these: its balances
are those of its addresses, so an address that only the mempool has used
counts as used when the gap is measured, as today; its lists are those of
its addresses, merged.

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

### Pages

A page is cut from the merged list by today's rules: where it starts, then
the offset, then the limit.

| The page starts at | Mempool's part | Chain's part |
|---|---|---|
| the top | all of it | from its newest entry |
| a mempool transaction | from that transaction on, by time and then by hash | from its newest entry |
| a confirmed transaction | none | from the position of the transaction |
| a height | none | from the last position in the block at that height |
| a deleted transaction | where the transaction was: one of the two rows above | |
| a transaction that is not known | none | none |

- **The mempool's part is read whole.** It is the few entries of the
  address, read with the one scan that its balance needs too. For an
  address with thousands of entries that is far too much; see
  [What has to change before the switch](#what-has-to-change-before-the-switch). The chain's
  part is streamed from its index only as far as the page needs, as today.
- **Several addresses are merged as today.** For a set of addresses or an
  extended public key, the list of each address is its mempool's part
  followed by its chain's part, and the lists are merged by the functions
  that merge them today (`joinDescStreams`, `mergeDescGroups`).
- **A list of transactions that starts in the chain does not read the
  mempool area.** That is every page after the first of a long history.
- **Outputs that the mempool spends are dropped as the chain's part is
  streamed**, before the offset and the limit are counted.
- **A height never includes the mempool**, not even a height above the best
  block. That is what today's queries do. The comment on `AtBlock` said the
  opposite, and has been corrected.

### Reading transactions

A body can be in three places: the mempool area, for a transaction in the
mempool; `tx`, for a confirmed one, and for those that version 20 deleted;
and the mempool area again, under `MemDeletedKey`, for those deleted since.

- **The mempool area is asked first**, for all the transactions of a request
  in one multi-get. Most are confirmed, but with its filter the mempool area
  says so for a fraction of the cost of reading a body, and `tx` is then
  asked only for the others. The deleted ones are looked up last, and only
  for what is still missing.
- **Mempool spenders are looked up only for outputs that no block spends.**
  The mempool cannot spend the others. For an old transaction that is few
  outputs or none. All of them, for all the transactions of the request, go
  in one multi-get.
- **A transaction with more than eight such outputs is scanned instead.**
  Its mempool spenders share a prefix, and a scan costs about as much as
  eight lookups whatever it finds.

### A consistent view

Reading two areas at different moments can show a transaction twice, or not
at all, if a block is imported in between; a balance could even count the
same payment twice. Today's queries read without a snapshot and get away
with it, because each of their answers is one key or one scan.

So the new queries do not take the database. They take a `View`: both areas
through one RocksDB snapshot. `withView` is the only way to make one, so a
query cannot be run on the live database by mistake. Taking and releasing a
snapshot takes 3.7 microseconds with the ten column families of the store.

- **One view for each request** is how the web server is meant to use it.
  The queries of one request then agree with each other as well: the
  balances of an xpub with its unspent outputs, a list of transactions with
  their bodies. Today a block can land between them.
- **The view has to last as long as the response.** A streamed response
  reads while it is sent, so the view must be held around the sending, not
  only around the handler. RocksDB keeps what a view can see for as long as
  it is open, so a slow client holds old data back from compaction until its
  response ends.
- **Nothing made from a view may be used after it is released.** The
  snapshot is freed then, and RocksDB would be handed a dangling pointer.
  The types do not prevent it; the comment on `withView` warns of it.

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

`Haskoin.Store.Logic.Layered` has these rules, with the functions of
`Haskoin.Store.Logic`: `importBlock`, `revertBlock`, `newMempoolTx`,
`deleteUnconfirmedTx` and `initBest`. They give the same results and the same
errors, so the block store can move from one module to the other.
`Haskoin.Store.Database.Writer` gained what a batch needs to write to the
mempool area; a batch of today's rules writes nothing there.

Two details were kept as they are today, so that nothing changes for a
store that syncs from the first block:

- **A transaction that is already confirmed.** Two coinbases of 2010 appear
  twice on Bitcoin. The later one takes the position of the earlier, with
  the outputs that are still unspent.
- **Replace-by-fee flags.** A mempool transaction inherits the flag from
  unconfirmed ancestors, and it is corrected when one of them is confirmed
  or goes back to the mempool. The comparison with today's rules found one
  case that depended on the order of the steps of a reversal; it is the
  same now.

## What it costs: measurements

Three programs in `docs/layered-database/` measured it, each with its output
next to it:

- `OverlayCost.hs`, written before any of the design was built, times single
  lookups in column families laid out like the mempool area.
- `QueryCost.hs` writes one history into a database of each layout, with
  each set of rules, and times both: the rules, every kind of query, and the
  first step of a scan before and after a block.
- `MigrateBench.hs` times the migration; see [Migration](#migration).

What they found, in short, in microseconds:

| | Today | Layered, as built |
|---|---|---|
| Importing a chain, for each transaction | 189 | 182 |
| A mempool transaction, from its arrival to its block | 499 | 570 |
| A request about something that the mempool does not touch | | 10 to 15 more |
| Each address read, when it has nothing in the mempool | 2.7 | 6 more |
| The balance of an address with 6,216 entries in the mempool | 3 | 35,000 |
| The first step of a scan of an address that has nothing, after a block | 14 | 1,638 |

The first four lines are the price that the design expected to pay. The
last two are not acceptable. They do not come from keeping the mempool
apart, but from two things in how the queries read it: they read all that
the mempool has for an address on every question about the address, and
they find out that it has nothing by scanning, through a filter.
[What has to change before the switch](#what-has-to-change-before-the-switch)
says what to do about each.

### Single lookups

`OverlayCost.hs` fills two column families laid out as the mempool area
with synthetic mempool transactions, 13 keys each, and times each kind of
lookup through the Haskell binding; the output is in
`docs/layered-database/overlay-cost.txt`.

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

"No filters" is the binding as it was released then. "With filters" is the
binding with the fix described below.

- **Reads by key hold up in the whole queries.** A key that is not there
  costs 0.5 to 0.7 microseconds through its filter, here and below.
- **Scans do not.** The 0.8 to 1.1 microseconds for an address with
  nothing were measured on a database that nothing had been deleted from.
  The store deletes from the mempool area all the time, and that changes
  the cost of a scan through a filter completely; see
  [below](#a-scan-through-a-filter-can-step-over-every-deleted-key).
- **A snapshot costs more with more column families**: 1.1 microseconds
  with the two of this program, 3.6 with the ten of the store.

### Writing, measured

`QueryCost.hs make` writes a history with the rules of one layout and times
the store's part of it: 200,000 transactions in 200 blocks, 50,000
transactions taken into the mempool, then ten blocks that confirm the
oldest 10,000 of those. RocksDB 11.1.1 with the store's default settings,
database on tmpfs, one thread. Microseconds for each transaction:

| | Today's rules | Layered rules |
|---|---|---|
| Importing a chain, with nothing in the mempool | 189 | 182 |
| Taking a transaction into the mempool | 294 | 203 |
| Importing a block that confirms mempool transactions | 205 | 367 |
| A transaction over its life: the mempool, then a block | 499 | 570 |

- **Syncing costs the same.** No transaction is in the mempool then.
- **Work moves from the arrival of a transaction to its block**, as the
  design says. Taking a transaction costs a third less. Confirming it costs
  four fifths more, because the block writes it to the chain's indices for
  the first time, and deletes its keys from the mempool area.
- **Over its life a transaction costs 14% more.** It is written to the
  large column families once instead of twice, as hoped, but the keys of the
  mempool area and their deletion cost more than that saves.
- **Blocks at the tip take longer to import**, by the same four fifths, for
  the part of a block that the mempool had seen. A block of 3,000 such
  transactions takes about 1.1 seconds instead of 0.6 on this machine.

### Whole queries, measured

`QueryCost.hs time` opens the two databases that `make` wrote, asks both
the same questions, checks that the answers are the same, and times them.
The state: 211 blocks and 40,000 transactions in the mempool. What is asked
about:

- **Addresses**: 1,000 that the mempool does not touch, 1,000 with one or
  two mempool transactions, 1,000 that were never used, and the busiest
  one, with 17,342 transactions and 6,216 entries in the mempool area.
- **Wallets**: extended public keys with 200 addresses in use on each
  chain, 464 read with the gap. The *cold* one has nothing in the mempool,
  the *quiet* one five payments, and the *busy* one 16,368 entries.
- **A small wallet**: 20 addresses in use, 84 read, 399 transactions, two
  of them in the mempool.

Microseconds for each question, the best of three means, the chain's
column families without filters as by default:

| Nothing in the mempool for what is asked about | Today | Layered | Times as long |
|---|---|---|---|
| Balance of an address | 4.6 | 19 | 4.2 |
| Balance of an address that was never used | 4.3 | 19 | 4.4 |
| Balances of 100 addresses | 265 | 937 | 3.5 |
| A confirmed transaction | 6.3 | 16 | 2.6 |
| 100 confirmed transactions | 376 | 522 | 1.39 |
| Spender of an output | 6.0 | 13 | 2.1 |
| An unspent output | 5.0 | 15 | 3.0 |
| Transactions of an address | 46 | 65 | 1.40 |
| Cold wallet: balances | 1,366 | 3,768 | 2.8 |
| Cold wallet: first 100 transactions | 7,311 | 10,438 | 1.43 |
| Cold wallet: 100 transactions, far back | 10,545 | 8,246 | 0.78 |
| Cold wallet: first 100 unspent outputs | 10,915 | 13,989 | 1.28 |
| Cold wallet: number of transactions | 80,133 | 87,632 | 1.09 |

| A little in the mempool | Today | Layered | Times as long |
|---|---|---|---|
| Balance of an address with a mempool transaction or two | 4.5 | 37 | 8.3 |
| Its transactions | 57 | 86 | 1.52 |
| A mempool transaction | 5.0 | 19 | 3.7 |
| 100 mempool transactions | 308 | 688 | 2.2 |
| Quiet wallet: balances | 1,390 | 4,837 | 3.5 |
| Quiet wallet: first 100 transactions | 7,467 | 11,761 | 1.58 |
| Quiet wallet: first 100 unspent outputs | 11,259 | 15,694 | 1.39 |
| Small wallet: balances | 229 | 592 | 2.6 |
| Small wallet: first 100 transactions | 863 | 1,011 | 1.17 |
| Small wallet: first 100 unspent outputs | 1,174 | 1,320 | 1.12 |

| Much in the mempool | Today | Layered | Times as long |
|---|---|---|---|
| Busiest address: balance | 3.1 | 35,284 | 11,382 |
| Busiest address: first 100 transactions | 563 | 34,414 | 61 |
| Busiest address: 100 transactions, far back | 573 | 608 | 1.06 |
| Busiest address: first 100 unspent outputs | 769 | 34,663 | 45 |
| Busy wallet: balances | 1,046 | 110,348 | 105 |
| Busy wallet: first 100 transactions | 7,470 | 166,818 | 22 |
| Busy wallet: 100 transactions, far back | 11,104 | 8,477 | 0.76 |
| Busy wallet: first 100 unspent outputs | 11,296 | 166,202 | 15 |
| Busy wallet: number of transactions | 179,809 | 315,424 | 1.75 |

- **Nothing in the mempool: a fixed cost for each request, and a small one
  for each thing read.** About 10 to 15 microseconds more for a request,
  of which 3.6 are the snapshot; about 6 more for each address;
  about 1.5 more for each transaction body. Lists come out between
  1.1 and 1.6 times as long.
- **A page far back in a wallet is faster than today.** Today's query looks
  the transaction that the page starts at up once for each address of the
  wallet; the new one looks it up once.
- **Much in the mempool: the time grows with what the mempool has.** Every
  question about an address reads all the entries of the address, at about
  5.5 microseconds each. For the busiest address that is 35 milliseconds,
  whether the question is its balance, which today is one read of 3
  microseconds, or a page of 100.
- **Counting the transactions of a wallet is slow in both layouts**: 80
  milliseconds for 19,499 transactions, 4.1 microseconds for each key read.

Where the time of the layered queries goes, each part timed alone in a
snapshot:

| Part | Microseconds |
|---|---|
| A snapshot, taken and released | 3.7 |
| Balances of 100 addresses: the chain's part | 273 |
| Balances of 100 addresses: the mempool's part | 640 |
| Of that: 100 seeks of one iterator that find nothing | 477 |
| Instead: 100 keys that are not there, in one multi-get | 56 |
| Busiest address: its mempool entries, read and decoded | 34,021 |
| The same entries, read and not decoded | 29,737 |

- **Finding that an address has nothing costs 4.8 microseconds by seek and
  0.56 by key.** The queries seek.
- **Reading an entry costs 4.8 microseconds before any of it is decoded.**
  Decoding adds 0.7. So the cost of a scan is in getting the rows out of
  RocksDB, not in what the store does with them; see
  [Reading rows through the binding](#reading-rows-through-the-binding).

### A scan through a filter can step over every deleted key

A scan by address asks RocksDB for the first key at or after the address,
and stops when the key it gets belongs to another address. That first step
is a seek. In the mempool area, at rest, it took about 10 microseconds for
nearly every address that has nothing, and up to three quarters of a
millisecond for a few. Right after a block it took 1.6 milliseconds on
average, for all of them.

- **Why.** RocksDB keeps the newest writes in memory and the rest in table
  files. With a filter on the prefix of the key, a seek skips every table
  file that has nothing for the address, which is the point of the filter.
  The newest writes have no filter, and after a block most of them are
  deletions: the keys of the transactions that the block confirmed. The seek
  lands on the first deleted key after the address, steps past it to the
  next, and goes on until it finds a key that is alive. The keys that are
  alive are in the files it skipped. So it steps over every deletion to the
  end.
- **Without a filter it cannot happen.** Every file takes part in the seek,
  and the first live key after the address ends it at once.
- **At rest it happens to a seek now and then.** A filter says "maybe" for
  about 1% of the addresses that a file does not have, and when that file
  holds mostly deletions it is walked in the same way.
- **RocksDB has an answer to it**: an iterator can be told to stay within
  the prefix it was sent to, or be given an upper bound. The Haskell binding
  has no way to say either.

`QueryCost.hs seeks` measures it: 2,000 seeks for addresses that have
nothing, before and after 5,000 of the 40,000 mempool transactions are
confirmed by a block, or deleted. Mean time in microseconds:

| Index scanned | As it was left | After a block | After deletions |
|---|---|---|---|
| Today's layout, transactions by address, no filters | 13 | 14 | 14 |
| The same, with the filters that `--bloom` asked for until now | 9 | 10 | 810 |
| Layered layout, entries of the mempool by address | 13 | 1,638 | 1,647 |

The longest seek at rest was 48 microseconds without filters and 756 in the
mempool area.

What follows from it:

- **`--bloom` no longer asks for filters on the two indices by address** of
  today's store. It always did, and none was ever built. With the fixed
  binding they would have been, and each round of the store's own deletion
  of old mempool transactions, a thousand at a time, would have made every
  scan of an unused address slow until the next flush. A block does not do
  that in today's layout, because it writes the confirmed entries of an
  address next to the ones it deletes. `--bloom` still builds filters for
  everything that is read by key.
- **The mempool area cannot find out that an address has nothing by
  scanning it.** It has to be a read by key.
- **The binding should offer bounded iterators.** With them the filters can
  come back for scans.

### Reading rows through the binding

Every scan in the store gets a row out of RocksDB with six calls: is the
iterator valid, the key, valid again, the value, valid again, next. The
binding makes all six as *safe* foreign calls, the kind that lets other
Haskell threads run meanwhile, which cost about 0.4 microseconds each here.
Five of the six only read a field of the iterator and return at once.

Walking the rows of a column family of the measured databases, reading the
key and the value of each, in nanoseconds for each row:

| | `mempool-addr`, 319,908 rows | `addr-tx`, 699,474 rows |
|---|---|---|
| Binding as released | 2,814 | 2,839 |
| With `valid`, `key` and `value` as unsafe calls | 914 | 803 |

Three times as fast, for three words changed in the binding. This is not
about the layered layout: it is what every page of a list and every count
costs in the store today. A page of 100 transactions of an address takes
563 microseconds, 5.6 for each row, and the count of a wallet above, 80
milliseconds. The change was tried in a copy of the binding; it has not
been proposed to it yet.

### Bloom filters were not being built

Lookups of keys that are not there cost as much as lookups of keys that are,
which is what a Bloom filter is meant to prevent. The table files had none:
RocksDB reported `filter block size=0; # entries for filter=0`.

- **Cause.** `rocksdb_filterpolicy_create_bloom_full` takes its bits per key
  as a `double` since RocksDB 6.24. `rocksdb-haskell-jprupp` imports it with
  a `CInt`. The two travel in different registers, so RocksDB reads
  something else as the bits per key and builds no filter. Nothing fails.
- **Effect on the store.** `--bloom` did nothing when the store was built
  against RocksDB 6.24 or later. The Docker image is: its Debian release has
  RocksDB 9.10. The option is off by default, so most stores were not
  expecting filters anyway.
- **Fix.** [Pull request 21](https://github.com/jprupp/rocksdb-haskell/pull/21)
  on the binding, released as `rocksdb-haskell-jprupp` 2.6.2. With it, the
  same column family has 3,250,201 bytes of filters for 2,600,000 keys, the
  10 bits per key asked for, and a lookup of an absent key takes 1.5
  microseconds instead of 5.3 to 5.9. The store is built with 2.6.3 now.
- **Also found.** Comparing all 72 foreign imports of the binding with the C
  header showed one more that had drifted: `approximateSize` called RocksDB
  with one argument too few. The store does not use it. The pull request
  fixes it too.
- **Before turning `--bloom` on.** With the fixed binding, `--bloom` builds
  filters for the chain's column families, in the table files written from
  then on. They take about 1.25 bytes for each key, and the store keeps them
  in its block cache. On a chain with billions of keys that is gigabytes
  competing with data for a cache of 1 GiB by default. It should be sized
  before it is enabled, and a store that already runs with the option will
  start building them when it is upgraded.
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
  snapshot, and paging runs across both. It came out smaller than feared:
  `Haskoin.Store.Database.Query.Layered` is about as long as the module of
  today's queries, and uses its streams and merges for the chain's part.
  What is new is where a page starts, the mempool's part of each list, and
  the three places a body can be in.
- **More complex: the transition.** Two layouts, a migration, and a cache
  protocol to specify again.

## What has to change before the switch

The queries as built give the right answers, and take far too long in two
cases: about an address that the mempool touches a lot, and about any
address right after a block. Both come from one choice: to read everything
that the mempool has for an address, with a scan, for every question about
that address. Four changes follow from the measurements. The first two are
built, and the queries measured above are the ones from before them; the
other two are changes to the binding.

### 1. A summary for each address that the mempool touches

One more key for an address, next to its entries, with what they add up to:

| Field | What it counts |
|---|---|
| `txs` | mempool transactions that touch the address |
| `paid`, `paidCount` | outputs that the mempool pays to the address: their amount and number |
| `spent`, `spentCount` | outputs of the address that the mempool spends |
| `both`, `bothCount` | outputs that the mempool both pays and spends |

The balance is then the chain's balance and one read by key:

```
confirmed   = chain.confirmed - (spent - both)
unconfirmed = paid - both
utxo        = chain.utxo - (spentCount - bothCount) + (paidCount - bothCount)
txs         = chain.txs + txs
received    = chain.received + paid
```

- **It answers "nothing" without a scan.** An address that the mempool does
  not touch has no summary, and a key that is not there costs 0.56
  microseconds to read through its filter, whatever was deleted lately.
  That is the usual case, which the queries as built pay between 4 and 1,600
  microseconds for.
- **It makes a busy address cost what any other does.**
- **It does not go stale when the chain changes**, which is why this
  document first rejected stored balances for the mempool. Every field
  counts things that mempool transactions do, and none says what the chain
  holds. What depends on the chain, whether an output that the mempool
  spends is one of the chain or one of the mempool, is the difference
  between `spent` and `both`; and `both` changes only when one of the two
  transactions enters or leaves the mempool. The rules are there when that
  happens, and what they need to know then is a read by key: whether the
  transaction that an input spends from is in the mempool, and whether a
  mempool transaction spends an output.
- **It is not the key of one transaction.** The invariant becomes: the
  mempool area is the keys of its transactions, and for each address a
  summary that is a function of the entries of that address. A test can
  work every summary out again from the entries after every operation, and
  the migration can write them the same way.
- **Its counts bound every scan.** A query that knows how many entries of
  each kind an address has reads that many rows and no more. It never asks
  RocksDB for the key after the last one, which is the step that walks over
  deletions.
- **It costs the rules a read and a write for each address** that a mempool
  transaction touches, as today's rules pay for its balance.

Built: the summary is `MemSummary`, under `MemAddrSumKey`, the last key of
an address in `mempool-addr`. The rules of `Haskoin.Store.Logic.Layered`
change it with every transaction that enters or leaves the mempool
(`adjustSummaries`), and the migration writes all of them at the end from
the entries that it moved (`rebuildSummaries`), which is also how they
would be put right. The tests work every summary out again from the entries
after each operation of their random histories (`wrongSummaries`), and
compare the summaries that the rules kept with those that the migration
wrote for the same history, byte for byte. Being the last key of an
address, the summary is also what a scan of the address's transactions or
outputs runs into when they end, instead of the deleted keys of the next
address.

### 2. Lists read in proportion to the page

- **Transactions.** The entries of an address are already in the order of
  the list. A page reads as many as it needs, from the top or from the
  transaction it starts at.
- **Unspent outputs.** The outputs that the mempool pays are in order too.
  Whether the mempool spends one of them, or an output of the chain, is a
  read by key, and the summary says when there is no need to ask: `both` of
  zero means that no output of the mempool is spent, and `spent` equal to
  `both` that nothing of the chain is hidden.
- **Several addresses.** The summaries, read together, say which addresses
  have anything in the mempool. Only those get a stream.

Built, as described. The reads of hidden outputs stop when as many were
found as the summary counts. The number of transactions of several
addresses is the sum of the chain's count and the summaries' when at most
one of the addresses has transactions in the mempool, and otherwise the
mempool transactions of those that have are read to count each once.

### 3. Bounded iterators in the binding

`rocksdb-haskell-jprupp` creates iterators with no bound, and RocksDB lets
an iterator be told to stay within the prefix that it was sent to. With
that, a scan through a filter stops where it should, and the filters can go
back on the indices by address of the chain, and stay on those of the
mempool area. It needs a new function in the binding, and the scans of the
store have to ask for it. The summary makes the layered layout safe without
it; today's store with `--bloom` is safe without it because the filters are
off where it matters.

### 4. Cheaper row reads in the binding

The three accessors of an iterator as unsafe foreign calls: three times as
fast for every scan, in today's store as much as in this layout. See
[Reading rows through the binding](#reading-rows-through-the-binding).

### What it should cost then

Worked out from the parts that were measured, not measured itself:

| | As built | With the summary and streamed lists |
|---|---|---|
| Balance of an address with nothing in the mempool | 19 | about 9: the snapshot, the chain's balance, a key that is not there |
| Each address of a wallet, with nothing in the mempool | 6.4 on top of the chain's 2.7 | 0.6 on top of it |
| Balance of the busiest address | 35,000 | about 10, as any other |
| First 100 transactions of the busiest address | 34,000 | about what today's 563 is: 100 rows |
| An address with nothing, right after a block | 1,600 | as at rest |

## Keeping the mempool in memory

The mempool is bounded by age, not by size: `--mempool-timeout` deletes
transactions after 14 days by default. There are three ways to hold it.

| | What it is | For | Against |
|---|---|---|---|
| **RocksDB only** | Both column families, read through the request's snapshot. | Nothing new to keep right. Consistent by construction. No memory. | About a microsecond for each lookup. |
| **Mirror in memory** | The indices of the mempool area, decoded, in immutable maps, updated with every write. Bodies stay in RocksDB. | 40 to 120 nanoseconds for each lookup. | 1.8 KB for each transaction as measured: 87 MiB for 50,000, 345 MiB for 200,000. All of it live data for the garbage collector. |
| **Memory first** | The mempool lives in memory and is saved now and then. | No write for a mempool transaction. | A restart forgets transactions that the API already showed. After a crash the saved mempool is older than the chain area and has to be checked against it. |

**Recommendation: RocksDB only, with the summary of the section before,
and not memory first.** The measurements asked for something, but not for
the mirror. What costs too much is not the price of a lookup, which a
mirror would cut, but reading all the entries of an address and scanning to
find that there are none; a mirror would have to keep the same summary to
avoid the first. Kept in RocksDB, the summary needs no memory, nothing to
load at start, and no way to tell whether it matches a snapshot.

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
  xpubs that are not cached. With the summary, those pay 0.6 microseconds
  for each address on top of the 2.7 of its balance in the chain. A mirror
  could take that to 0.1. It is not worth its memory for that.
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

## A fleet that answers as one

This section is a proposal, written on 2026-11-10. Its first part is
specified: `Haskoin.Store.Fleet` has the rules for the fleet tip as pure
decisions, and `test/Haskoin/Store/FleetSpec.hs` runs three stores through
random blocks, reorganisations, reports and lapses of time against them.
`Haskoin.Store.Fleet.Redis` carries them out against Redis, in two Lua
scripts, and a test compares Redis with the rules step by step.
`Haskoin.Store.Fleet.Pool` has the rules of the shared mempool, also as
pure decisions with a model test, and a third script carries those out,
moving the tip and changing the pool in one step. No store uses any of it.

On the store's side, `Haskoin.Store.Database.Views` keeps a snapshot of the
database for each recent block and releases those that the rules no longer
ask for. The rest is not started, and comes after the switch to the layered
layout: the snapshot of a block in today's layout has the store's own
mempool mixed into the chain's indices, and nothing can be laid on top of
that. In order: the read path of the mempool area fixed as
[the measurements ask](#what-has-to-change-before-the-switch); the switch;
the entries and the summary of each address in the shared pool, which a
query about an address needs and the pool does not have yet; a request
answered from the view at the tip with the pool on top; and the process
that reports, moves the tip, and offers the store's mempool to the pool.

The aim is stronger than the cache design above: stores that share a Redis
should give the same answer to the same request at the same time, always,
and not only once they have all caught up. Two decisions come with it:

- **Deleted transactions are no longer returned.** Some clients do not
  check the flag. A transaction that was replaced or dropped is unknown to
  the API, and a page that starts at one is empty. The bodies that version
  20 left in `tx` stay there and are filtered out; the layered layout keeps
  none. What has to be kept is the fact that the store saw the transaction,
  so that it does not fetch it again when a peer announces it.
- **The mempool that answers requests is the one in Redis.** A store's own
  mempool area is then what it offers to Redis and relays to peers, not what
  it answers from.

### The idea: agree on the state, not on the moment

Stores cannot commit a block at the same instant, and they do not need to.
A response is consistent across the fleet if every store answers from the
same block and the same mempool. So:

- **Redis holds the fleet tip**: the hash and height of the newest block
  that every live store has imported.
- **Each store imports blocks at its own pace**, with no lock, and keeps a
  view of its database at each block until the fleet tip has moved past it.
- **Each store reports its recent blocks in a key that expires**: those it
  still has a view of, newest first. A store that dies or falls behind drops
  out by itself, and answers 503 on `/health` until it has caught up, so
  that the load balancer leaves it alone. A store reports only when it has a
  view of the fleet tip, so that one that is catching up cannot pull the tip
  back.
- **A store keeps the views of what it last reported**, and of the tip that
  it read after reporting. The tip only moves to a block that every live
  store reported, so a live store always has the view that a request needs.
- **One script moves the fleet tip**, to the newest block that every live
  store reported. A Lua script sets the new tip and, in the same step, takes out of
  the shared mempool what that block confirmed or made invalid. It is a
  compare-and-set on the tip: nothing is held, and two stores that try at
  once do the same thing.
- **Every request reads the fleet tip**, answers from the view at that
  block, and applies the shared mempool on top, read in the same script.

A reorganisation is the same: the tip is a hash, and the script that moves
it to a block of another branch puts back what the old branch had confirmed.

### Why not a lock and a commit in step

The first suggestion was a lock in Redis for each block, the changes to
RocksDB worked out once and shipped through Redis, and every store pausing
its API to commit together. That is a two-phase commit, and it was set
aside for these reasons:

- **It needs to know who is in the fleet.** A store that stalls after it
  was counted blocks all the others. A timeout unblocks them and breaks the
  guarantee.
- **A lock with an expiry does not exclude.** A holder that pauses past the
  expiry carries on as if it held the lock.
- **Stores still commit milliseconds apart**, with requests in flight on
  each. The pause narrows the window and does not close it, and makes each
  block a stall of the whole fleet as long as its slowest import.
- **Shipped changes are right only for identical databases.** In the
  layered layout each store can work the same chain area out by itself; the
  one thing that differs between stores is the time a transaction was first
  seen, which this design takes from Redis or drops.

### What it costs, and what is open

- **A round trip to Redis for every request**, and Redis in the way of all
  of them. With Redis down each store answers from its own tip and its own
  mempool, so the fleet is no longer consistent, and says so: `/health`
  answers 203 instead of 200. Today's store already does this for its cache.
- **The fleet is one import behind its fastest store.**
- **Redis must not lose writes.** A failover that loses the last second
  loses mempool transactions and can move the tip back.
- **The slow reads measured above move to Redis.** The summary for each
  address belongs with the shared mempool there, and the local mempool area
  needs none.
- **Conflicts between stores' mempools** are settled when a transaction is
  offered to Redis: the first one in stays, unless the store that offers the
  other names it as replaced, having replaced it by its own rules.
- **A store that is ahead of the tip holds back** what only makes sense
  after its newer blocks: it offers a transaction when each input spends an
  output of the pool or one that is unspent in its view at the tip.
- **The store that moves the tip tells the pool what the blocks did**: what
  they confirmed and spent, and across a reorganisation what comes back and
  what is gone for good. So it needs both tips and the blocks between them.
- **How long a store may lag** before it is dropped from the fleet.

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

The store runs it when it opens a database (`upgradeDatabase`, called by
`Haskoin.Store.Manager`): a new database gets version 21, one of version 18
or 19 is first brought to version 20 by the migrations that those always
had, and one of version 20 is migrated, from the start or from where an
earlier run stopped. The log says how many transactions were moved.

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

### The migration and the merge

`test/Haskoin/Store/Database/MigrateSpec.hs` builds databases of version 20
with the store's own rules, migrates them, and compares.

- **Random histories.** 50 histories each time the suite runs, each of 57
  operations chosen at random: payments from outputs that are unspent, in a
  block or in the mempool, between ten addresses, five of them of an
  extended public key, and a script that has no address, some signalling
  replace-by-fee, a few with ten outputs; transactions that replace one in
  the mempool; blocks with part of the mempool, in or out of order, with
  transactions the mempool has not seen, or with one that conflicts with
  it; deletions; reversals of the best block; and operations that are
  refused. The histories differ from run to run: 250 of them passed
  over five runs with the choices as they are now, after 650 with
  narrower sets. In the last run, every history ended with ten or more
  mempool transactions, and 86% with ten or more of them spending
  another mempool transaction.
- **Every answer is kept.** After the migration, the new queries give what
  version 20 gave: the balance, transactions and unspent outputs of every
  address, the data of every transaction ever made with its spenders, asked
  for by hash and by short number, the spender and the unspent state of
  every output, and the mempool.
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
  not taking a payment off the amount received, three of the four failed.

### The new rules against today's

`test/Haskoin/Store/Logic/LayeredSpec.hs` gives the same history to three
stores, each with a database of its own, on Bitcoin and on Bitcoin Cash,
which has no replace-by-fee. One runs today's rules. One runs the new rules.
The third is upgraded on the way: it runs today's rules up to a point
chosen at random, its database is migrated, and it runs the new rules from
there on, as a store in production will.

- **Histories.** 40 on each network every time the suite runs, each of 70
  operations chosen at random as above. Those that must be refused are
  transactions with an unknown output, an output already spent in a block,
  the same output twice, more than their inputs, or no inputs but a
  coinbase's; blocks with an unknown output, an output already spent, or
  beside the best block; and deletions of confirmed transactions. Earlier
  transactions are sent again too, whatever became of them. Over five
  runs, 200 histories passed on each network with the
  choices as they are now, after 240 with a narrower set and two stores.
- **Every operation has the same outcome.** The same result or the same
  error, and the same transactions entering and leaving the mempool, which
  is what the store publishes as events.
- **Every answer is the same after every operation**, the queries of the
  layered layout against today's.
- **The databases are the same at the end.** Today's database is migrated
  and compared with the one that the new rules built: every column family,
  key for key and value for value. The only difference allowed is where the
  bodies of deleted transactions are kept, and the bodies themselves must be
  equal.
- **The upgraded store ends the same too**, but for the deleted bodies. It
  keeps in `tx` those that version 20 deleted before the upgrade, even when
  the transaction came back afterwards and has a body elsewhere. The
  queries never answer with such a leftover, which is what the comparison
  after every operation checks.
- **The tests can fail.** With the correction of flags left out, with
  deleted bodies not kept, or with conflicting mempool transactions not
  deleted by a block, the comparison failed each time. It also found a real
  difference while the rules were written: the flag kept in the deleted body
  of a transaction that spent the coinbase of a reverted block.
- **So can the comparison with the upgraded store.** Three faults that make
  no difference to a database built by the new rules, and so pass the other
  two stores, were caught by the third: the rules taking a body that version
  20 deleted for a confirmed transaction; and the queries answering with
  such a body when the transaction was deleted again since, or, asked by
  short number, when it is back in the mempool.
- **Particular cases.** A block that spends an output of the mempool is
  refused by the new rules and leaves the database untouched. When a block
  confirms a transaction, the keys of a mempool transaction that spends it
  are found unchanged. And a transaction that a second block confirms again,
  as two coinbases of 2010 are, ends in the same place under both sets of
  rules, through the reversal of both blocks too.

### The new queries against today's

`test/Haskoin/Store/Database/Query/LayeredSpec.hs` asks today's queries and
the new ones the same questions.

- **Questions.** Pages of the transactions and of the unspent outputs of
  one address, of several addresses, and of an extended public key, with
  limits from 1 to 6 or none and offsets up to 3. Pages start at the top, at
  any transaction ever made in the history, whatever became of it, at a
  transaction that was never made, and at heights from 0 to two above the
  best block. Also: the balances of several addresses, the bodies of several
  transactions, transactions by short number, pages of the mempool, and the
  balances and the number of transactions of the extended public key.
  Addresses repeat, and one of them was never used.
- **On migrated databases.** 30 histories on each network every time the
  suite runs. A database is built by today's rules, 150 questions are asked
  of it, it is migrated, and the questions are asked again. Over five
  runs, 150 histories passed on each network.
- **On databases that the new rules built, and on upgraded ones.** The
  comparison of the rules, above, ends by asking 60 such questions of all
  three of its databases.
- **The gap of the extended public key.** Histories pay five of its
  addresses: the first, third and sixth that it gives out, and the first
  and third of its change. Each question is asked twice: with the gaps that
  the tests open their databases with, 20 and 32, which reach all five, and
  with an initial gap of 2 and a gap of 3, under which the later ones are
  found only if earlier ones are used, by a block or by the mempool alone.
- **What the pages covered.** In one run on Bitcoin, 3,576 pages: 29%
  started at the top, 16% at a mempool transaction, 22% at a confirmed one,
  10% at a deleted one, 18% at a height, and 6% at a transaction that was
  never made, which gives an empty page. 54% had something in them, and each
  of the other kinds of start had 180 or more that did.
- **One moment.** A view is taken; a block confirms two mempool
  transactions and two more arrive; the view still answers every question
  as before, and a view taken afterwards does not.
- **An example with numbers.** The example of this document, M and its two
  payments, page by page, before and after a block confirms the first
  payment.
- **The tests can fail.** Thirteen faults were put in the queries, one at
  a time, and the tests caught each:
  - a view that takes no snapshot;
  - a page that starts at a mempool transaction and keeps what is newer, of
    transactions and of outputs, or that loses the transaction itself;
  - a page that starts in the chain and keeps the mempool's part;
  - outputs that the mempool spends left in the list;
  - the mempool spenders of a transaction with many outputs not read;
  - mempool transactions not counted, and balances without the mempool;
  - deleted transactions not found by short number, and not accepted as the
    start of a page;
  - a height that starts at the first transaction of its block, not the
    last;
  - an address read too short when a page has an offset.

  Two changes that must make no difference made none: reading every mempool
  spender by scan, and every one by multi-get.

What this does not show:

- **Load.** The rules and the queries were timed in one thread, on a
  database of 200,000 transactions, not in the web server and not on a
  database of real size, where reading the chain costs more and the mempool
  area the same.
- **A database of real size.** The integration tests run the whole store
  on the layout, against real nodes on regtest chains.
- **Bloom filters in the tests.** The store is built with the binding that
  builds them, but the tests keep nearly all their data in memory, where no
  filter is used. `QueryCost.hs` reads through filters in table files and
  compares every answer that it times.

## Order of work

Each step ends with something that can be tested against today's behaviour.

1. **Build the store with the fixed RocksDB binding.** Done: the store is
   built with `rocksdb-haskell-jprupp` 2.6.3, and its whole test suite
   passes with it.
2. **A snapshot for each request.** Done: the new queries cannot be asked
   without one (`withView`), and the web server takes it around each
   request and the sending of its response.
3. **New rules, beside the old.** Done: `Haskoin.Store.Logic.Layered`.
   Chain rules that import and revert blocks in the chain area alone;
   mempool rules that add and remove keys. Tested: for random histories, the
   same database, key for key, as the old rules followed by the migration.
4. **Paged queries over both areas.** Done:
   `Haskoin.Store.Database.Query.Layered`. Tested: the same pages as today's
   queries, for random limits, offsets and starts, on migrated databases and
   on databases built by the new rules.
5. **The switch.** Done. Version 21: the store opens with `mempool-addr`,
   runs the migration, and uses the new rules and queries;
   `--wipe-mempool` empties the mempool area. What it took in the web
   server and the cache:
   - the record of queries was built once, when the server started
     (`webQuery`); it is built for each request, from its view. The router
     finds the route of a request in a list made once, and takes the
     handler from the same list made for the request (`routerScoped`), so
     that no handler had to change and the response is sent, streamed or
     not, while the view is open;
   - the reads that went to the database beside that record go through the
     same view: the bodies that the cache does not have, the queries of a
     request with `nocache=true`, and the count of transactions of
     `/blockchain/multiaddr`, which is now one of the queries;
   - `Haskoin.Store.Cache` reads the database through a view for each of
     its reads.

   The rules and queries of version 20 (`Haskoin.Store.Logic`,
   `Haskoin.Store.Database.Query`) are not deleted: the tests run every
   random history through both and compare, which is the evidence that the
   new layout answers as the old one did. Nothing in the store calls them.
6. **The cache on two layers**, specified and model-tested in
   `Haskoin.Store.Cache.Protocol` first.

Before step 5, the read path had to change as
[the measurements ask](#what-has-to-change-before-the-switch): a summary for
each address, and lists read by the page. That is done for the mempool that
a store keeps in RocksDB, which is what it answers from alone and when
Redis is down; the shared pool of a fleet needs the same in Redis. The two
changes to the binding are independent and help today's store. A mirror in
memory is no longer planned.

Steps 1 to 5 are done. Step 6 is where the concurrency problems of the cache
are actually removed.

## Alternatives considered

- **Keep the mempool's merged values in the mempool area**, as a
  copy-on-write layer over the chain's keys. The simplest reads: look in the
  mempool area, then in the chain. Rejected because the values go stale when
  the chain changes; see
  [Every key belongs to one transaction](#every-key-belongs-to-one-transaction).
- **A stored balance delta for each address.** One point lookup for each
  balance instead of a scan. Rejected at first: the split between confirmed
  and unconfirmed depends on whether a spent output's transaction is still
  in the mempool, so deltas have to be converted when a block confirms a
  parent and not its child; and the filter by address seemed to make the
  scan cheap enough. The measurements showed that it does not, and
  [the summary](#1-a-summary-for-each-address-that-the-mempool-touches) is
  this alternative come back in a form that needs no conversion: it keeps
  what the mempool spends and what it both pays and spends apart, so that
  nothing in it says what the chain holds.
- **One column family for the whole mempool area.** Simpler to wipe.
  Rejected because address scans could not use a filter: 4.0 to 6.3
  microseconds against 0.8 to 1.1 for an address with nothing in the
  mempool. That reason is weaker now: a scan through a filter turned out to
  be unsafe, and the summary is read by key. The entries by address still
  sort together in a column family of their own, which is worth keeping.
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
3. **`--bloom` for the chain.** It works now, for what is read by key. On
   the measured database it took a never-used address's balance from 4.3
   to 2.0 microseconds, 100 balances from 265 to 185, and the balances of a
   wallet of 464 addresses from 1.37 to 0.96 milliseconds. How much memory
   it needs on a full database, and whether it should stay off by default,
   are still open.
4. **The conflict rule of the shared mempool layer**: first to reach Redis
   wins.
