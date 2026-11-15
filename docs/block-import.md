# Importing a block in stages

How the store imports a block after version 1.15.1, why, and what it gained.
Written on 2026-11-15. The code is `Haskoin.Store.Logic.Layered`
(`importBlock`), `Haskoin.Store.Database.Writer` (the read cache and the
reads ahead) and `Haskoin.Store.Database.Copy` (the copy of the mempool
area in memory, and the outputs that mempool transactions spend).

## The short answer

A block is imported in four stages:

1. **Prepare.** From the block alone, on all cores: the hash of each
   transaction, the address of each output, the sizes.
2. **Read ahead.** Every key of the chain that the rules will read is read
   from RocksDB with multi-gets, many at a time, into a cache that belongs
   to the batch. What the mempool has for the block is not read from
   RocksDB: the store keeps [a copy of it in
   memory](#the-mempool-area-from-memory).
3. **Apply.** The rules run as before, one transaction after another. They
   find everything in memory.
4. **Write.** All the changes go to RocksDB in one atomic batch, as before.

The database that comes out is the same, byte for byte.

A block that was mined is taken to be valid. The import reads what it
needs to write the block, and nothing to check it: see [what a mined block
is not checked for](#what-a-mined-block-is-not-checked-for).

## The problem

A store of Bitcoin mainnet took 13 to 18 seconds to import a full block:
4,000 transactions, 1.6 MB. Its disk was nearly idle and half of its four
cores too.

- **The time went into reads, one at a time.** A block read about 13,800
  unspent outputs and 11,000 balances from RocksDB, each with its own call,
  from one thread. Each cost 0.8 ms on that machine.
- **Many were read twice or more.** The output that an input spends was read
  when the mempool was asked about conflicts, again to build the
  transaction, and again to spend it. Nothing was remembered between reads.
- **Some were never needed.** Every transaction was looked up in the
  mempool, in the chain, and for conflicts, also while syncing, when the
  mempool is empty.
- **One more read was not counted:** the spenders of the transaction that
  made each output, read again for every input.
- **The rest was repeated work.** The hash of a transaction, which is two
  rounds of SHA-256 over all of it, was worked out five or six times. The
  block was serialised three times to get its size and weight.

Part of the 0.8 ms was a problem of its own: RocksDB's cache was too small
for the index and filter blocks of the table files. That was fixed
elsewhere, and brought a block down to 3.3 seconds at best, still one read
at a time. On that disk, random reads go at 5,800 a second one at a time
and 31,000 a second 32 at a time.

## The stages

### 1. Prepare

`prepareBlock` splits the transactions over the cores. For each one it
serialises the transaction once without its witness, hashes those bytes,
and keeps their length. A transaction with a witness is serialised once
more for its full length. It also works out the address of each output.

The size and weight of the block are sums of those lengths. The order in
which the transactions will be applied is worked out here too, from the
hashes that are now known.

### 2. Read ahead

What a block reads is known before any rule runs:

- the outputs that it spends and does not make itself, from the block
  itself. An output that one transaction of the block makes and another
  spends is not in the database: the rules find it in the batch. And an
  output that a mempool transaction spent is not read either, if the
  store [remembers it](#the-outputs-that-the-mempool-spends-remembered)
  from when the transaction came;
- when the mempool is not empty: what the mempool has for each of those
  outputs and for each transaction of the block, the summaries of the
  addresses, and a few more keys for the transactions that leave the
  mempool. These were read from RocksDB too, some fifty thousand keys
  for a block at the tip. They now come from [a copy of the mempool area
  in memory](#the-mempool-area-from-memory), and only two kinds of them
  are still taken ahead of the rules.

Two more kinds of key were read here until database version 23, and were
most of the read ahead on a store of mainnet: the spenders of the
transactions that made those outputs, and the balances of the addresses
that the block pays or spends from. They were read only to be changed, and
are now [changed without being read](#balances-and-spenders-without-a-read).
No spender of the chain is read any more, ahead or by the rules: the reads
that were left after version 23 were there to check the block, and [a
mined block is not checked](#what-a-mined-block-is-not-checked-for).

Each of these is one list of keys. The list is cut in pieces, and each
piece is one RocksDB multi-get. Eight of them run at a time for each core.
A multi-get does not hold a core while it waits for the disk, so on a cold
database that is 32 reads in flight on four cores, where it was one.

The answers go into the **read cache** of the batch, those that say "not
there" too.

### 3. Apply

The rules are the ones the store had. They ask the batch for an unspent
output or a balance, as before. The batch looks in three places, in this
order:

1. the changes that the batch has made so far;
2. the read cache;
3. the database, and then it remembers the answer.

The changes come first, so the cache can never hide one. A key that was not
read ahead is read when it is asked for, so reading ahead is never needed
to be right. It only moves the reads to where they can be done together.

For a key of the mempool area the third place is not the database but the
copy of the area in memory.

The database does not change while a batch is open. The block store is its
only writer, and it writes one batch at a time.

### 4. Write

One batch, one write to the write-ahead log, as before. That is what makes
a block all or nothing.

The keys and values are serialised on all cores, into buffers of 256 KiB.
What goes into a buffer is the batch itself, packed: for each key a header
of 17 bytes, then the key, then the value. RocksDB is handed a buffer in
one call, some forty calls for a block, where it was a call for each key
(`writePacked` of the RocksDB binding). Nothing is kept in memory for a
single key.

A batch of fewer than 256 keys, as that of a mempool transaction, is
handed over key by key as before.

## Balances and spenders, without a read

A block changes the balance of every address that it pays or spends from,
and adds a spender to the group of every output that it spends. Until
database version 23 it read each of them to write it back. On a store of
Bitcoin mainnet, four blocks at the tip spent this long reading ahead, in
milliseconds by kind of key:

| Unspent outputs | Groups of spenders | Balances | Mempool |
| ---: | ---: | ---: | ---: |
| 259 | 719 | 94 | 306 |
| 87 | 304 | 70 | 180 |
| 98 | 852 | 300 | 186 |
| 47 | 764 | 194 | 232 |

Now the block writes how they change and reads neither:

- **A balance** gets a merge record with five differences: so much more
  confirmed, one more unspent output, one more transaction. All that a
  block does to an address is summed into one record.
- **A group of spenders** gets a merge record with the spenders to add.
- **RocksDB works out the values** when they are read, and when a
  compaction brings the records and the value together, with two merge
  operators that are part of the store.
- **The answers are the same.** A reader gets the merged value, and the
  tests compare every key and value of this import, as RocksDB returns
  them, with those of the sequential import, which still reads and writes
  whole values.
- **A revert reads**, as before: taking a block out checks that nothing
  goes below zero and deletes a balance that goes back to nothing.

What it costs is paid by readers, a few microseconds for a key that has
merge records which RocksDB has not compacted yet, and by upgrades, which
are one way. `docs/layered-database.md` has the formats, the cost to
readers and why an older store cannot open the database.

## The mempool area, from memory

At the tip of the chain the transactions of a block are in the mempool,
and the block takes them out. For that it read some fifty thousand keys
of the mempool area, the two column families of the unconfirmed
transactions. On a store of mainnet that took 180 to 460 ms of a block:
the last column of the table above has four such blocks.

- **None of that time was the disk.** The area is 64 and 20 MB for 25,000
  transactions, and all of it is in RocksDB's memory. A lookup still
  costs about 8 microseconds of processor time: the call into RocksDB,
  its memtables, its filters, a block to find and to check.
- **So the store keeps a copy of the area in memory**, every key and
  value of the two column families as bytes, in an ordered map for each
  (`Haskoin.Store.Database.Copy`). A batch answers every read of the area
  from it. A lookup is a walk down a tree, 0.2 to 0.3 microseconds, and
  half a microsecond with the key serialised and the value decoded.
- **It is right because the area has one writer.** The block store writes
  one batch at a time, and nothing else writes to the area while it runs.
  When a batch has been written, its changes to the two column families
  are applied to the copy, and the new copy takes the place of the old in
  one step.
- **Requests of the API do not use it.** They read RocksDB, on a snapshot,
  as before. The copy is what the database has only between two batches
  and for the batch that is open.

### How it is kept right

- **Loaded when a batch first needs it**, with one scan of the two column
  families: 0.3 s for 25,000 transactions. A store that syncs has an
  empty area and an empty copy.
- **Changed when a batch is written.** The changes of a batch to the two
  column families are noted while the batch is serialised, as slices of
  its buffers. The copy takes the bytes out of them, so that the buffers
  can go.
- **A batch that fails changes nothing**: its function threw, nothing was
  written, and the copy was not touched.
- **A write that fails forgets the copy.** Before RocksDB is handed the
  batch the copy is marked as not known, and it is set again only when
  the write has returned. If the write throws, or the thread is killed in
  between, the next batch loads the copy again from the column families:
  RocksDB may have kept a part of a batch that it refused. The three
  steps are also masked, so that a batch that was written is not followed
  by a scan for nothing.
- **Whatever else writes to the area says so**, and the copy is loaded
  again by the next batch: the migration from version 20, a wipe of the
  mempool at the start (`--wipe-mempool`) and the repair of the summaries.
  All of them run when the store starts, before it takes anything from
  the network.
- **The new copy is worked out while RocksDB writes.** A block at the tip
  deletes sixty thousand keys from the copy. Another thread does it
  during the write, which RocksDB makes in one thread of its own, and on
  another core of the program than the one that the write was started
  from. The write of such a block, in milliseconds, medians of three runs:

  | | Write |
  | --- | ---: |
  | Without a copy | 169 |
  | The copy changed after the write | 210 |
  | During the write, on the same core | 202 |
  | During the write, on another core | 170 |

  On the same core it gains next to nothing: the thread that comes back
  from RocksDB wants that core again.

### What it is made of, and what was measured

Each column family is a map from key to value, ordered by the bytes of
the keys as RocksDB orders them, and never changed in place: a change
makes a new map that shares most of the old one. Scans need the order:
the mempool spenders of the outputs of a transaction are the keys that
start with its hash. And a map that does not change can be read from any
number of threads without a lock.

Measured by themselves, on a map of 48,000 keys, about what the benchmark
has, and of 360,000, about what a mempool of 25,000 transactions has, in
nanoseconds:

| | 48,000 keys | 360,000 keys |
| --- | ---: | ---: |
| Serialise the key of a mempool spender | 150 | 100 |
| The same with `encode`, which takes 4 kB each time | 610 | 990 |
| Look it up in the ordered map | 210 | 320 |
| Look it up in a hash map | 70 | 170 |
| Decode the spender | 140 | 140 |
| The whole read, with the ordered map | 420 | 520 |
| Delete a key from the ordered map | 320 | 710 |
| Add a key to the ordered map | 530 | 990 |

- **A hash table for the lookups by key was left out.** It would take
  100 to 150 ns off a read of 420 to 520, some 5 ms of a block of the
  benchmark, and would have to be kept next to the ordered map that the
  scans need: twice the changes at every batch, and 50 bytes more for
  each key. The lookup is not most of a read: the key and the value are.
- **The key of an address is the slow one**: 410 ns to serialise, where
  a key with a transaction hash takes 100. Its serialiser is in
  haskoin-core.
- **Keys and values are byte strings that the collector may move**
  (`ShortByteString`). The pinned kind would keep a block of the heap for
  every few values that outlive their neighbours, and a slice of a
  buffer of the batch would keep the buffer.

### What is still read ahead

With every read of the area a look in memory, most of them need not be
made ahead. Each kind was tried both ways in the benchmark: taken from
the copy ahead of the rules, on all cores, into the tables of the batch,
or asked of the copy by the rules as they come to it. The read ahead and
the rules together, for a block at the tip, in milliseconds, medians of
three runs of one program that an environment variable told what to take
ahead:

| What is taken ahead | Read ahead | Rules | Together |
| --- | ---: | ---: | ---: |
| Everything, as before | 48.7 | 104.7 | 153.4 |
| Nothing (one run) | 15.4 | 146.2 | 161.6 |
| The bodies | 20.8 | 137.9 | 159.0 |
| The bodies and the summaries of the addresses | 34.5 | 114.2 | 148.9 |

The read ahead of the first column has the 16 ms of the unspent outputs,
which are read from RocksDB. The bodies with the spenders of the outputs
that the block spends, in one more run, came to 159.2 together: no better
than the bodies alone.

- **The bodies pay**: a body is a transaction, which has to be decoded.
  Ahead, on four cores, the 3,500 of a block cost 4 ms, and save the
  rules 8.
- **The summaries pay**: 13,650 keys of addresses, each slow to
  serialise. Ahead they cost 14 ms, and save the rules 24.
- **The others do not.** The mempool spender of each output, the scan
  for the spenders of each transaction and the entry of each transaction
  in the list cost 14 ms ahead and save the rules 10: putting them into
  the tables of the batch, which one thread does, costs as much as
  looking them up when the rules ask.

So the bodies and the summaries are taken ahead, and nothing else. What
is timed as the read ahead of the mempool (`store.block_read_mempool`) is
those two.

### One read that is not made

When a transaction leaves the mempool, the summary of each address that
it spends from is corrected, and for that the store asked, for each
output that the transaction spends, whether the transaction that made it
is in the mempool. It no longer asks when the chain has the output as
unspent, which the batch knows from memory: the block read every output
that it spends.

- **It is the same answer.** An output that is unspent in the chain was
  made by a confirmed transaction, and a confirmed transaction is not in
  the mempool. A mempool transaction writes to the mempool area only. A
  block takes a transaction out of the mempool before it puts it in the
  chain, and a revert takes it out of the chain, with its unspent
  outputs, before it gives it to the mempool.
- **Where it could break:** on a database that had a transaction both
  listed in the mempool and with an unspent output in the chain. No
  version of the rules writes that, and the migration from version 20
  checks every mempool transaction for it.
- **The sequential import still asks every time**, and the tests compare
  the two.

### What it costs

Measured with 25,000 transactions in the mempool
(`haskoin-store-bench import 1000000 25000 1 +RTS -N4 -A64m -T`):

| | |
| --- | ---: |
| Keys in the two column families | 401,125 |
| Bytes of their keys and values | 38.5 MiB |
| Heap that the copy takes | 84.6 MiB |
| Heap for each mempool transaction | 3,549 bytes |
| Loading it from the database | 264 and 280 ms |
| A full collection of the heap, with it | 27 ms |
| The same without it | 16 ms |

- **Memory:** 3.5 kB of heap for each transaction in the mempool, 85 MiB
  for a mempool of 25,000. A little more than half of it is the tree
  and the headers of the byte strings, about 120 bytes for each key.
- **Collections:** GHC's collector copies what is alive, so a full
  collection copies the copy: 11 ms more for each, on four cores. Small
  collections do not look at it.
- **The load** is a scan of the two column families, most of it RocksDB's
  iterator. It happens once, when the first batch after the start needs
  the copy.
- **When syncing, nothing:** the area is empty, and so is the copy.

## The outputs that the mempool spends, remembered

When a transaction enters the mempool, the store reads from the chain the
unspent outputs that it spends, to check it. When a block confirms the
transaction, the import needs the same outputs again:

- their values and scripts, to change the balances and to store the
  transaction with what its inputs were worth;
- the positions of the transactions that made them, which are in the keys
  of the address index that it deletes.

So this is not a check, and it stays. But it was a second read of what
the store had read once, and with everything else out of the way it was
the largest part of the read ahead of a block at the tip: 90 to 375 ms a
block on a store of mainnet with a small cache, 40 to 220 ms with a large
one.

Now the store keeps what it read. Next to the copy of the mempool area
there is a table from an output to the chain's unspent output, for the
outputs that mempool transactions spend
(`Haskoin.Store.Database.Copy`):

- **Filled when a batch is written**, for the outputs that the batch gave
  a mempool spender: the batch has them in its read cache.
- **Used by the read ahead of a block.** An output that is in the table
  goes into the read cache of the batch from there. The others are read
  from RocksDB as before.
- **A cache, not a copy.** An output that is not in the table is read
  from RocksDB, so too few entries is slow and never wrong. After a
  restart the table is empty, and it fills as the mempool turns over.
- **Only the block store reads it**, through its batches. Requests of the
  API read RocksDB. The sequential import does not use it.

### What it costs

Measured with 25,000 transactions in the mempool, which spend 47,500
outputs of the chain
(`haskoin-store-bench import 1000000 25000 1 +RTS -N4 -A64m -T`):

| | |
| --- | ---: |
| Outputs remembered | 47,500 |
| Heap that they take | 7.4 MiB |
| Heap for each output | 162 bytes |

- **Memory:** 162 bytes for each output, a tenth of what the copy of the
  mempool area takes for the same mempool. An entry has the position,
  the value and the script, and its key is the output. The address is
  worked out from the script when the entry is used.
- **Nothing that the collector cannot move.** The script is kept as bytes
  in the ordinary heap, as the copy keeps its keys and values. An
  address can hold a slice of the script that it came from, which would
  pin it, so none is kept.
- **A block that is written** has each unspent output that it changes
  looked up in the table, and each mempool spender that it deletes: a
  look in a hash table for each, in the thread that changes the copy
  while RocksDB writes.
- **When syncing, nothing:** the table is empty, and is not looked at.

### Why an entry is never stale

An entry must be what the chain has for its output, or a block would
write a wrong balance. Three rules keep it so, all applied when a batch
is written, in one step with the copy:

1. **An entry lives as long as the mempool spender of its output.** It is
   dropped when a batch takes that spender away: the transaction was
   confirmed, deleted or replaced. A block that spends the output takes
   its mempool spender away in the same batch, because that spender is a
   transaction of the block, or else conflicts with one and is deleted.
2. **A batch that reverts a block empties the table.**
3. **A batch that changes an unspent output of the chain changes its
   entry with it**, or drops it if the output is gone.

The first two were the design. Checking them against the rules found one
case that they miss: a coinbase that is confirmed a second time, as two
of 2010 were. Its unspent outputs move to the position of the later
block, and a mempool transaction that spends one stays in the mempool. No
spender leaves and nothing is reverted, yet the unspent output has
changed. The third rule covers it, and anything like it: only batches
write unspent outputs, and every unspent output that a batch writes is
looked up in the table. With it, the first two are not needed to be
right. They keep the table small, and empty after a reorganisation.

Where it could still break:

- **Something that writes unspent outputs outside a batch.** Only the
  migration from version 20 does, when the store starts, and it says
  that the copy is not known any more, which forgets the table.
- **A write that fails.** The copy is forgotten before a batch is handed
  to RocksDB and set again when the write has returned, and the table is
  part of the copy.

## What a mined block is not checked for

A block that reaches the store was mined: it passed proof of work, and the
node took it. The store takes it that its transactions are valid:

- every output that the block spends is unspent in the chain, or made by
  a transaction of the block itself;
- no output is spent twice.

Checking that is the work of the node. Here each check was a read of the
database for every input of every block. So the import reads what it
needs to write the block, and nothing to check it.

**What went, and what it cost:**

- **Who spends an output in the chain.** With a mempool, the import looked
  for what each transaction of the block conflicts with, and for an output
  that was not there to be spent it asked the chain who had spent it. An
  output that the block itself makes is not there yet, so every input that
  spends one was a lookup of a key that does not exist, answered by a
  Bloom filter that a store with a small cache reads from the disk. Blocks
  of Bitcoin mainnet have 3,000 to 6,500 such inputs, when the mempool has
  not seen their transactions: one block took ten seconds over them. With
  an empty mempool the same keys were read ahead, to see that the chain
  had no spender for them. Now the import of a block never reads a spender
  of the chain.
- **The outputs that the block makes, read from the chain.** With a
  mempool, every output that the block spends was read ahead as an unspent
  output of the chain, also those that a transaction of the same block
  makes, which the chain cannot have. Now only the others are.
- **The conflicts of a transaction that is in the mempool.** Every input
  of every transaction of the block was asked about. But the mempool has
  one spender for an output at most, so a transaction that is in the
  mempool is the mempool spender of all that it spends, and conflicts
  with nothing. Now only a transaction that the mempool has not seen is
  asked about, and only the mempool is asked. This one does not rest on
  the block being valid, but on the rules of the mempool: a transaction
  enters it only when no mempool transaction spends what it spends, and
  takes the keys of its own inputs with it when it leaves.
- **Is this transaction confirmed already?** Each transaction was looked up
  in the chain. Now only a transaction without inputs is: a coinbase. Two
  coinbases of 2010 were confirmed twice, which is why the lookup exists,
  and it is data, not a check: the later one takes the place of the
  earlier. A transaction with an input cannot be confirmed twice, since
  its input is spent the first time.
- **Does the mempool conflict with it, when it is empty?** Nothing can.
  The store sees that the mempool is empty with one look at its copy, and
  that the batch has not touched it, and then asks the mempool nothing
  about any input.

**What a block that is not valid does now.** It is written whole or not at
all: a batch that throws writes nothing, and that is all that is promised.
Such a block is still refused whenever the import cannot read what it
needs:

| The block has | Before | Now |
| --- | --- | --- |
| An input that does not exist | Refused, as an orphan | The same |
| An input that a block has spent | Refused: the spender is confirmed | Refused, as an orphan |
| An output spent by two of its transactions | Refused, as an orphan | The same |
| An output spent twice by one transaction | Stops: the output is gone | The same |
| A confirmed transaction with inputs, again | Refused, as an orphan | The same |

The last two rows are looks in the changes of the batch, in memory. A
coinbase that is confirmed again is not in the table: it is imported, as
it always was.

## What stays

Everything that keeps the store's own data right:

- **The unspent outputs that the block spends** are read from the chain:
  their values, scripts and positions are what the balances and the
  indexes of addresses are made from.
- **The order of the transactions.** Each is applied after those it spends.
  Bitcoin blocks come in such an order, but Bitcoin Cash orders them by
  hash. The order also decides which transaction of the block a replaced
  mempool transaction is said to conflict with. So the same sort is used,
  now with the hashes it is given. It takes 3 ms.
- **The mempool giving way.** Transactions of the block leave the mempool.
  Mempool transactions that spend what the block spends are deleted, with
  their descendants, and remembered as replaced.
- **Reverts, deletions and the rules of the mempool** check all that they
  did. Nothing vouches for an unconfirmed transaction.
- **That the block builds on the best block.** That is about the store's
  own chain, not about the block.

## The proof

`importBlockSequential` is the import as it was until 1.15.1, kept in the
library for this. `withoutReadCache` gives a batch that reads the database
every time, as before.

The tests (`Haskoin.Store.Logic.StagedSpec`) give the same history to two
stores, one that imports the old way and one the new way, and compare
after every operation:

- what the operation returned or why it was refused;
- which transactions entered and left the mempool;
- every key and value of every column family, as an iterator returns
  them. The new import writes merge records for balances and spenders and
  the old one whole values: what is compared is the values that RocksDB
  makes of the records, which must be the same bytes.

After every operation, the store that imports the new way must also have
a copy of its mempool area that is what a scan of the two column families
gives, byte for byte, and the store that imports the old way must have
no copy at all: it reads RocksDB, and is what the copy is held to.

The histories are random, and their blocks are ones that could be mined.
They have blocks that spend outputs made in the same block, in any order,
several payments to an address, mempool transactions that a block confirms
or conflicts with, deletions and reverts. A second set of histories is
mostly blocks that the mempool has not seen, as when syncing, with chains
of transactions in the block, on a mempool that is empty or not, some of
them conflicting with it, and now and then an earlier coinbase again. In a
third set, blocks are reverted and the same blocks imported again, several
times over: that is where merge records meet the whole values and the
deletes of a revert. All run on Bitcoin and Bitcoin Cash. QuickCheck
prints how many blocks of each kind the histories had.

Blocks that could not be mined have a test of their own, after random
histories: each row of the table above, with transactions that could be
mined around the one that is wrong, some of them conflicting with the
mempool. It checks how each import refuses the block, and that every key
of every column family and the copy of the mempool area are as they were.

A merge is done in more than one place in RocksDB: in a memtable, across
table files, in a compaction. So the store that imports the new way has
its memtables flushed after every fifth operation, and everything is
compacted and compared once more at the end of a history.

The older tests still hold both imports to the rules of version 20.

After every operation, too, each output that the store which imports
the new way remembers must be, in its database, the unspent output that
it remembers, and have a mempool spender. Tests of their own go through
each way in and out of the table: a transaction confirmed, deleted,
replaced; a block reverted; a coinbase confirmed again while a mempool
transaction spends it; a block with outputs that are remembered and
others that are not; a database opened again, which remembers nothing
and imports the same. QuickCheck prints how many outputs were remembered
after each operation.

The copy of the mempool area has tests of its own
(`Haskoin.Store.Database.WriterSpec`):

- every read of the area gives the same through a batch, which reads the
  copy, and through the same batch without its read cache, which reads
  RocksDB, before and after changes of the batch;
- a batch that throws leaves the copy as it was;
- a write that RocksDB refuses is followed by a reload;
- a database that is opened with a mempool in it loads it;
- a batch of thousands of changes, packed on all cores, with values
  larger than a buffer, leaves the copy equal to the column families.

## Measurements

`haskoin-store-bench import` builds a database with a million unspent
outputs at half a million addresses, closes it so that RocksDB writes it to
table files, and copies it. It then imports the same blocks into each copy,
the old way and the new way. A block has 3,500 transactions, 7,000 inputs
and 7,000 outputs. Its output is in `docs/block-import/import-bench.txt`,
for the tables below. The first four tables have two kinds of block, in
which one input in twenty spends an output of the same block. The fifth
and the sixth have two more, in which three in four do, as on Bitcoin
mainnet.

On an AMD Ryzen 9 3900X with four cores given to the store (`+RTS -N4`),
the database on tmpfs, RocksDB 11.1.1, milliseconds a block:

| | 1.15.1 | Sequential, today | In stages |
| --- | ---: | ---: | ---: |
| **Syncing: the mempool is empty** | **1,381** | **1,074** | **382** |
| Prepare | | | 36 |
| Read ahead | | | 75 |
| Apply the rules (before: with their reads) | 1,046 | 845 | 63 |
| Write the batch: 65,410 keys, 6.6 MB | 335 | 230 | 208 |
| **At the tip: every transaction is in the mempool** | **2,321** | **1,915** | **841** |
| Prepare | | | 28 |
| Read ahead | | | 196 |
| Apply the rules (before: with their reads) | 1,739 | 1,515 | 240 |
| Write the batch: 124,514 keys, 9.8 MB | 582 | 400 | 377 |

- **"1.15.1"** is the sequential import measured before the hash tables
  and the write were made faster: what 1.15.1 ran. **"Sequential, today"**
  is `importBlockSequential` in today's library, which shares the faster
  hash tables and the faster write.
- **A store of mainnet gains more than this.** In the benchmark a read
  costs 30 microseconds, all of it CPU: the files are in memory. On the
  production machine a read waits for a SATA disk, and reads done 32 at a
  time went five times faster there. The benchmark shows the least that
  the change is worth.

### The batch, packed

The table above is from before the batch was packed (stage 4), when
RocksDB was handed its keys one call at a time. To see what packing
gained, the benchmark was run on the code before it and after it, in turn,
six times each. Medians of the six, in milliseconds a block, in stages:

| | Key by key | Packed |
| --- | ---: | ---: |
| **Syncing: the mempool is empty** | **250** | **198** |
| Write the batch: 65,410 keys, 6.6 MB | 134 | 86 |
| **At the tip: every transaction is in the mempool** | **522** | **447** |
| Write the batch: 124,514 keys, 9.8 MB | 257 | 183 |

- **The gain is in the write, and only there.** The other stages took the
  same on both sides, within a few milliseconds. Whichever code ran first
  of a pair tended to be a little slower in them, so four pairs were run
  in one order and two in the other.
- **Compare within a table, not across.** These runs were made later the
  same day, and every stage was faster in them than in the table above,
  also the stages that packing does not touch: prepare took 18 ms where
  the table has 36. Among themselves the runs agree: the write of the
  first kind of block took 130 to 140 ms before and 85 to 93 ms after.
- **Where the 48 ms went.** Timed inside the write of the first kind of
  block, in one more run on each side, medians of its five blocks:

  | | Key by key | Packed |
  | --- | ---: | ---: |
  | List the changes of the batch | 2 | 2 |
  | Serialise them, on all cores | 24 | 24 |
  | Hand them to RocksDB, and RocksDB's write | 102 | 59 |
  | Count the keys and the bytes, for the metrics | 4 | 0 |

  The calls cost more here than the 16 ms that 65,000 of them cost when
  timed by themselves. What else they waited for was not taken apart.
  Each handed over a key and a value that another core had just written,
  out of a list of 65,000 operations.
- **A busy store should gain more.** A thread that comes back from such a
  call needs a core of the program again, and waits for one when the
  other threads are busy. The benchmark imports with nothing else to do.
  In the benchmark of the binding, a batch of this size took 420 ms to
  write key by key next to eight busy threads on four cores, and 28 ms
  packed.

### Balances and spenders, merged

The benchmark was run on the code before the block wrote merge records
for its balances and spenders (database version 22) and after it (version
23), in turn, six times each, the order swapped from one pair to the
next. Medians of the six, in milliseconds a block, in stages:

| | Read and written whole | Merged |
| --- | ---: | ---: |
| **Syncing: the mempool is empty** | **189** | **148** |
| Prepare | 15 | 11 |
| Read ahead | 56 | 21 |
| of it: unspent outputs | 17 | 17 |
| of it: groups of spenders | 13 | 2 |
| of it: balances | 24 | 0 |
| Apply the rules | 33 | 30 |
| Write the batch: 65,410 keys, 6.6 and 6.3 MB | 86 | 87 |
| **At the tip: every transaction is in the mempool** | **433** | **398** |
| Prepare | 12 | 13 |
| Read ahead | 126 | 92 |
| of it: unspent outputs | 16 | 14 |
| of it: groups of spenders | 12 | 0 |
| of it: balances | 24 | 0 |
| of it: the mempool | 73 | 77 |
| Apply the rules | 121 | 115 |
| Write the batch: 124,514 keys, 9.8 and 9.0 MB | 175 | 179 |

- **The reads that are gone were cheap here**, 30 microseconds each from
  memory, and they were 35 ms of a block. On the store of mainnet whose
  timings are above they were 370 to 1,150 ms of a block: this benchmark
  shows the least that the change is worth.
- **The write takes 1 to 4 ms longer**, with as many keys and fewer
  bytes: a merge record for a group of spenders has the spenders that the
  block adds, where the whole group was written before. What RocksDB does
  more for a merge record than for a put was not looked into.
- **At the tip no spenders are read at all** in these blocks, whose
  transactions are all in the mempool: the read ahead that is left is for
  when the mempool is empty.
- **Readers pay instead**, a few microseconds for a key with merge
  records that are not compacted yet: `docs/layered-database.md`.

### The mempool area, copied

The benchmark was run on the code before the copy (commit 9be69e06) and
on the code with it, in turn, six times each, the order swapped from one
pair to the next. Medians of the six, in milliseconds a block, in stages:

| | Read from RocksDB | From the copy |
| --- | ---: | ---: |
| **Syncing: the mempool is empty** | **137** | **139** |
| Prepare | 9 | 9 |
| Read ahead | 20 | 19 |
| of it: unspent outputs | 17 | 17 |
| of it: groups of spenders | 2 | 2 |
| of it: balances | 0 | 0 |
| Apply the rules | 30 | 31 |
| Write the batch: 65,410 keys, 6.3 MB | 79 | 80 |
| **At the tip: every transaction is in the mempool** | **371** | **337** |
| Prepare | 10 | 10 |
| Read ahead | 88 | 35 |
| of it: unspent outputs | 14 | 17 |
| of it: groups of spenders | 0 | 0 |
| of it: balances | 0 | 0 |
| of it: the mempool | 73 | 18 |
| Apply the rules | 111 | 121 |
| Write the batch: 124,514 keys, 9.0 MB | 163 | 171 |

- **At the tip a block takes 34 ms less**, a tenth. The mempool was 73 ms
  of its read ahead and is 18: the bodies and the summaries, from the
  copy. The reads that the rules now make themselves are 10 ms of the
  rules.
- **The write takes 8 ms longer** at the tip: the changes to the area are
  noted for the copy, the entries of the list are looked up in the copy
  where a table of the batch had them, and a core changes the copy
  meanwhile.
- **The unspent outputs take 3 ms longer** to read at the tip, in every
  run. Nothing in how they are read has changed, and why was not looked
  into.
- **Syncing is as it was**, within what two runs of the same program
  differ by: 2 ms.
- **A store of mainnet should gain more than a tenth.** These are the
  reads that take 180 to 460 ms of its blocks, at 8 microseconds each,
  where the benchmark's take 73 ms. From the copy they should take about
  as long there as here: a read costs half a microsecond in a map of
  that size.

Every stage was faster in these runs than in the table before: the code
before the copy already had the serialisers of haskoin-core that write
to a buffer directly. Compare within a table.

### A mined block, not validated

The benchmark got two more kinds of block for this: blocks of chains of
transactions, in which 5,180 of 7,001 inputs spend an output of the same
block, on an empty mempool and on a mempool of 3,500 other transactions
that has not seen the block. The second is what took ten seconds on a
store of mainnet, and no kind of block measured it. The benchmark was
then run on the code that still checked a block (commit f82516ea) and on
the code that does not, in turn, four times each. Medians of the four,
in milliseconds a block, in stages:

| | Checked | Not checked |
| --- | ---: | ---: |
| **Syncing: the mempool is empty** | **136** | **135** |
| Read ahead | 19 | 17 |
| of it: unspent outputs | 17 | 17 |
| of it: groups of spenders | 2 | 0 |
| Apply the rules | 29 | 31 |
| Write the batch: 65,410 keys, 6.3 MB | 79 | 78 |
| **The same, with a block of chains** | **115** | **106** |
| Read ahead | 14 | 6 |
| of it: unspent outputs | 8 | 6 |
| of it: groups of spenders | 5 | 0 |
| Apply the rules | 27 | 27 |
| Write the batch: 49,240 keys, 5.0 MB | 64 | 63 |
| **At the tip: every transaction is in the mempool** | **351** | **339** |
| Read ahead | 44 | 38 |
| of it: unspent outputs | 16 | 20 |
| of it: groups of spenders | 10 | 0 |
| of it: the mempool | 19 | 18 |
| Apply the rules | 121 | 114 |
| Write the batch: 124,515 keys, 9.0 MB | 176 | 179 |
| **A mempool that has not seen the block, of chains** | **161** | **138** |
| Read ahead | 36 | 16 |
| of it: unspent outputs | 13 | 5 |
| of it: groups of spenders | 12 | 0 |
| of it: the mempool | 11 | 11 |
| Apply the rules | 35 | 32 |
| Write the batch: 49,239 keys, 5.0 MB | 79 | 79 |

- **The block that the mempool has not seen takes 23 ms less**, a seventh.
  Its 5,180 outputs made in the block are not read from the chain, and no
  spender is looked for: 8 and 12 ms. The rules ask about conflicts with
  one look in the copy of the mempool area for each input, where they
  made several, and one in the chain.
- **This is the least that it is worth.** Here a key that is not there
  is answered in a microsecond or two, from files in memory. On the store
  of mainnet that this was made for, each waited for a filter to be read
  from the disk, and thousands of them took ten seconds of one block.
- **At the tip a block takes 11 ms less.** The 10 ms under "groups of
  spenders" was not reads but the looks that chose which groups to read,
  which the commit before this one added. The rules take 7 ms less: a
  transaction that is in the mempool is no longer asked what it conflicts
  with.
- **The unspent outputs take 4 ms longer to read at the tip**, in every
  run, with 350 keys fewer to read. Telling the outputs that the block
  makes from the others is part of that time now, where a block that met
  a mempool read them all; how much of the 4 ms it is was not measured.
- **Syncing is as it was**, but for the 2 ms of the spenders.
- **The second look at an output as it is spent stays.** It is a look in
  the changes of the batch, and what stops a transaction that spends one
  output twice. A third program without it ran in turn with the two: its
  blocks took 135, 108, 336 and 144 ms, no difference that the runs can
  tell.

### The outputs that the mempool spends, remembered

The benchmark was run on the code that read every output of a block from
RocksDB (commit ec4774ec) and on the code that remembers those that
mempool transactions spend, in turn, four times each. Medians of the
four, in milliseconds a block, in stages:

| | Read again | Remembered |
| --- | ---: | ---: |
| **Syncing: the mempool is empty** | **137** | **135** |
| **The same, with a block of chains** | **106** | **106** |
| **At the tip: every transaction is in the mempool** | **329** | **322** |
| Prepare | 9 | 9 |
| Read ahead | 37 | 26 |
| of it: unspent outputs | 19 | 6 |
| of it: the mempool | 17 | 20 |
| Apply the rules | 106 | 111 |
| Write the batch: 124,515 keys, 9.0 MB | 178 | 176 |
| **A mempool that has not seen the block, of chains** | **138** | **138** |

- **At the tip no output is read from RocksDB**: the unspent outputs take
  6 ms where they took 19. All 6,650 that the block spends from the
  chain are remembered.
- **What the 6 ms are.** Telling the outputs that the block makes from
  the others, looking each of the others up in the table, making an
  unspent output of it with its address, and putting it in the read cache
  of the batch: a microsecond an output, most of it in one thread. It was
  not taken apart further.
- **The block gains 7 ms, not 13.** The read ahead of the mempool and the
  rules take 3 and 5 ms longer, in every run, with the same work to do
  on the same values. Why was not found.
- **This is the least that it is worth.** Here an unspent output is read
  in three microseconds, from files in memory. On a store of mainnet
  these reads were 40 to 375 ms of a block at the tip, and from memory
  they should take there what they take here.
- **The other kinds of block are as they were.** With an empty mempool
  nothing is remembered and the table is not looked at, and the block
  that the mempool has not seen spends nothing that is remembered.
- **Writing a batch takes no longer.** What a batch does to the table is
  applied by the thread that brings the copy of the mempool area up to
  date, while RocksDB writes.

### Where the time goes now

With the reads out of the way, a block of the first kind spent its 148
ms like this, in the runs of the third table:

- **Write, 87 ms.** About 60 ms is RocksDB: it is handed 34 buffers and
  adds their keys to its memtables, in one thread. The rest is
  serialising them, on all cores.
- **Apply, 30 ms.** The rules and the hash tables of the batch.
- **Read ahead, 21 ms.** The outputs that the block spends: RocksDB
  finding, decompressing and checking blocks for 7,000 keys, on four
  cores.
- **Prepare, 11 ms.** Serialising and hashing each transaction.

A block at the tip does more: its transactions leave the mempool, which
is as many keys to look up in the copy of the mempool area and to delete
again, from RocksDB and from the copy.

Things that were measured and changed on the way:

- **Hash tables.** The tables of a batch are keyed by outputs and
  addresses. Their generic hash took 0.4 to 0.8 microseconds a key, and an
  insertion up to 3.4. `Haskoin.Store.Database.Table` hashes the bytes
  that these keys already hold, and the rules went from 180 to 60 ms a
  block.
- **Serialising the batch.** `encode` takes a buffer of four kilobytes for
  each key and each value. A block was a gigabyte of them, in one thread.
  Now each core serialises its share into a few large buffers.
- **Threads bound to cores.** Work of a few milliseconds that is split
  over threads gains little if the threads are left to find a core:
  sixteen pieces of serialising took 35 ms that way, 26 ms with each
  thread started on its core, and 80 ms in one thread.
- **Handing the batch over.** RocksDB was given the keys with a foreign
  call for each, of the kind that lets other threads run meanwhile: the
  thread gives up its core of the program for the call and has to get one
  back. Now the cores serialise the batch as packed records, and RocksDB
  is given a buffer of them in a call. See "The batch, packed" above.

## What was not done

`docs/block-import-next.md` goes through each of these and a few more: what
it would gain, what it would cost, and what it would do to reads.

- **Reading ahead for the next blocks** while one is written. A value read
  for a block that is not imported yet can be changed by the block before
  it, so it cannot go into the cache of its batch. Only reading the keys to
  warm RocksDB's own cache would be safe. That is a possible next step for
  a store that is catching up.
- **Serialising faster.** Writing a transaction hash through the
  serialisers of haskoin-core allocates 680 bytes, and a transaction of 370
  bytes takes 6 microseconds and 18 kB. Half of the prepare stage and of
  the serialising in the write stage is that. It belongs in haskoin-core.
- **The write itself.** RocksDB writes a batch in one thread. A batch
  cannot be split without losing that a block is all or nothing.
- **A hash table next to the copy of the mempool area**, for the lookups
  by whole key: 70 to 170 ns where the ordered map takes 210 to 320, of a
  read of some 500. See [what the copy is made
  of](#what-it-is-made-of-and-what-was-measured).
- **A faster key for an address.** Serialising the key of the summary of
  an address takes 410 ns, four times what a key with a transaction hash
  takes, and a block at the tip makes 13,650 of them. The serialiser of
  an address is in haskoin-core.
