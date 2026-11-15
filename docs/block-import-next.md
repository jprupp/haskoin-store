# Block import below a second: what is left

What could still be done to import a full block of Bitcoin mainnet well
below a second, what each step would gain and cost, and what it would do
to the speed of reads. Written on 2026-11-15. It follows
`docs/block-import.md`, which says how a block is imported in stages.

The steps are those that the staged import left behind as suggestions.
Each was checked against the code, against RocksDB's source, and against
a morning of figures from production. One of them turned out to be done
already. [Where it stands](#where-it-stands) says which are done.

## The short answer

A full block of mainnet now takes about one second at the tip. Half of
that is reading, a quarter is writing.

| | Step | Stage it cuts | Gain a block | Effect on reads |
| --- | --- | --- | --- | --- |
| 1 | RocksDB merges balances and spender groups | Read ahead | About 350 ms | Slower for keys changed lately |
| 2 | Warm the cache for the next blocks | Read ahead | None at the tip | A little slower while catching up |
| 3 | Cheaper serialisers in haskoin-core | Prepare, write | About 50 ms | About 3 times faster to decode |
| 4 | Hand RocksDB the batch in one piece | Write | 50 to 75 ms, measured | None |
| 5 | Batched multi-get, asynchronous reads | Read ahead | None | None |

- **The gains are estimates**, not measurements, except where a figure is
  given with its source below.
- **Steps 1, 3 and 4 together** would bring the median block from about
  1,000 ms to about 500 ms.
- **Only step 1 changes the database.** It is the largest gain and the
  only one that can slow a read.
- **The reads were measured first.** The groups of spenders are more than
  half of the read ahead, and the balances come next: see
  [The read ahead by kind of key](#the-read-ahead-by-kind-of-key). So
  step 1 is worth doing.

## Where a block's time goes today

The store of mainnet, on the commit `8ae74310`, from 04:00 to 08:42 UTC on
2026-11-15: 35 blocks at the tip, from the stage timings that the store
sends to StatsD. Milliseconds a block.

| Stage | Median | Least | Most |
| --- | ---: | ---: | ---: |
| Prepare | 24 | 5 | 33 |
| Read ahead | 505 | 62 | 1,439 |
| Apply | 156 | 17 | 232 |
| Write | 275 | 42 | 408 |
| **Whole import** | **998** | **126** | **1,872** |

- **18 of the 35 blocks took less than a second.** One in ten took more
  than 1.5 seconds, each time because of the read ahead.
- **The machine:** four cores, 62 GiB, the database on a SATA disk,
  RocksDB 10.10.1 with a block cache of 24 GiB. The cache had filled to 15
  GiB by the end, so these are not yet the figures of a full cache.
- **Bitcoin Cash** on the same machine: 112 ms a block at the median, 81
  of them reading. Two blocks of 29 took 2.0 and 3.8 seconds, all of it
  reading from a cold part of the database.
- **The write is CPU work, not the disk.** The benchmark writes a batch of
  the same kind in 377 ms with its database in memory. The log of changes
  is not forced to the disk at each block.

## The read ahead by kind of key

The store now times the read ahead for each kind of key that it reads
(`store.block_read_unspents` and three more, for StatsD). Four blocks of
mainnet at the tip, on 2026-11-15 between 10:00 and 10:25 UTC.
Milliseconds a block.

| Block | Unspent outputs | Groups of spenders | Balances | Mempool | Read ahead |
| --- | ---: | ---: | ---: | ---: | ---: |
| 10:00:54 | 259 | 719 | 94 | 306 | 1,380 |
| 10:03:05 | 87 | 304 | 70 | 180 | 644 |
| 10:15:08 | 98 | 852 | 300 | 186 | 1,438 |
| 10:25:05 | 47 | 764 | 194 | 232 | 1,240 |

- **The groups of spenders are more than half of the read ahead.** They
  are keyed by the transaction that made each output, so they are spread
  all over a large table, and nothing reads them before the block comes.
- **The balances are next,** a tenth to a fifth.
- **The unspent outputs are cheap,** as expected: each was read when its
  transaction came into the mempool.
- **The mempool reads take about 200 ms,** which was not expected: that
  part of the database is small. It has not been looked into yet.
- **These blocks are slower than the morning's.** A backup was reading
  the same disk at 100 to 140 MB/s, so the read ahead took about twice as
  long as in the table above. The shares are what matters here. The first
  block after the restart is left out; later blocks were imported during
  a load test and are left out too.

Step 1 removes the reads of the second and third columns: about 70% of
the read ahead.

## How RocksDB keeps data

Step 1 changes how the store uses RocksDB, so this section says how
RocksDB works, for a reader who has never used it. The other steps do not
need it.

RocksDB is the database inside the store. It keeps **keys** and their
**values**, both plain bytes, sorted by key. The store has several tables
in it, which RocksDB calls column families: one for balances, one for
unspent outputs, one for the spenders of outputs, and so on.

### A write never changes a file

When the store sets a key, RocksDB does two things:

1. It adds the write to the end of a **log** on the disk, so that it is
   not lost if the store stops.
2. It puts the key and value in a **table in memory**, sorted.

That is all. It does not look for the old value, and it reads nothing
from the disk. This is why writes are cheap.

When the table in memory is full, RocksDB writes it to the disk as one
**file**, sorted by key, and starts an empty table. A file is never
changed after that.

### Levels and compaction

Files pile up, so RocksDB keeps them in **levels**. New files go in at
the top. Each level below holds about ten times more than the one above
it, and the last level holds nearly all the data. There are seven.

A job that runs in the background, the **compaction**, keeps this in
order. It takes files of one level and the files of the next level that
cover the same keys, and writes them out again as new files of the lower
level, sorted. Then it deletes the old files.

```
   newest     table in memory
     |        level 0   a few small files
     |        level 1
     |        ...
   oldest     level 6   nearly all the data
```

### A key can be in several places

Since nothing is changed in place, a key that was set three times can be
there three times: the oldest value in a low level, a newer one above it,
the newest in memory. **The newest wins.** The compaction is what throws
the old ones away: when two versions of a key meet in it, it keeps the
newer one.

A delete works the same way. It is a write of a marker that says "this
key is deleted", which hides the older versions until a compaction drops
them all.

### A read looks from the newest to the oldest

To read a key, RocksDB looks in the table in memory, then in the files of
level 0, then in each level below. **It stops at the first version it
finds**, since that is the newest.

Three things keep this fast:

- **A filter** for each file says "this key is certainly not here" or
  "maybe". It is small and is kept in memory, so most files are passed
  over without being read.
- **An index** for each file says which block of the file could hold the
  key. A block is a few kilobytes.
- **The block cache** keeps in memory the blocks that were read lately.

So a read costs one block from the disk at most, most of the time, and
nothing if the block is in the cache. Reads are where the disk is waited
for. That is the reverse of a database that changes its files in place,
where the writes are the costly part.

### A batch

Many writes can be given to RocksDB as one **batch**. It is all or
nothing: either every write of it is in the database or none is. The
store writes a block as one batch.

## How a merge works

### The problem it solves

Some values are changed, not replaced. To add 5,000 to a balance, the
store must know the balance: it reads it, adds, and sets the result. The
write is cheap, as above. The read is not, and a block of mainnet makes
some 25,000 of them, for balances and for groups of spenders.

### A third kind of write

RocksDB has a third kind of write next to "set" and "delete": the
**merge record**. It does not say what the value is. It says how to
change it: "add 5,000 to this balance".

A merge record is written like any other write: to the log and to the
table in memory. Nothing is read. The work of finding the old value is
put off until someone asks for it.

### The merge function

RocksDB only sees bytes, so it has to be told what "change" means. The
program that opens the database gives each table a **merge function**,
which RocksDB calls a merge operator. It does two things:

- **Apply:** given the old value, or nothing if there is none, and a list
  of merge records from the oldest to the newest, give the new value.
- **Combine:** given two merge records that follow each other, give one
  record that does the same as both. For balances that is adding the
  amounts.

The function has to be there every time the database is opened, by the
store or by any tool. Without it RocksDB cannot say what the value of a
key with merge records is.

### What a read does

A read still looks from the newest to the oldest. The difference is that
a merge record does not end the search. RocksDB keeps the record and goes
on down, until it finds a whole value, a delete marker, or the bottom.
Then it calls the function with what it found.

An address that was paid in three blocks, with the balance in satoshi:

```
   table in memory    merge: + 5,000      (the block just imported)
   level 0            merge: - 2,000
   level 3            merge: + 1,000
   level 6            value: 100,000
```

A read finds the three records and then the value, and answers 104,000.
The program that asked sees a plain value. It cannot tell that merges
were used.

Without merges, the same key would have its newest whole value in the
table in memory, and the read would stop there. That is the cost of a
merge to a reader: it has to go as far down as the last whole value, and
read one block for each level where it finds a record.

### What a compaction does

The compaction is where the putting off ends. When the versions of a key
meet in it:

- two merge records become one, by "combine": `- 2,000` and `+ 1,000`
  become `- 1,000`;
- a merge record that meets the whole value becomes a new whole value, by
  "apply": `- 1,000` on `100,000` becomes the value `99,000`.

So the records of a key do not pile up without end. There is one for each
level at most, and a key that no block has touched for a while is one
plain value at the bottom, exactly as today.

### With sets and deletes

All three kinds of write can be used on one key. A set or a delete hides
everything older, merge records too. A merge record that comes after a
delete, or for a key that was never set, starts from nothing: the
function is told there is no old value.

## 1. Let RocksDB merge balances and spender groups

### What it is

Two tables of the store hold values that a block changes and does not
replace:

- **Balances.** One key for each address. The value is five numbers:
  confirmed, unconfirmed, the count of unspent outputs, the count of
  transactions, the total received.
- **Groups of spenders.** One key for each group of sixteen outputs of a
  transaction. The value is a small list that says, for each output of
  the group that is spent, which input spent it.

Today the import reads each of them, changes it and sets it. With this
step it writes a merge record and reads nothing:

- for a balance, **five differences**, such as "confirmed + 5,000,
  unspent outputs + 1, transactions + 1, received + 5,000";
- for a group of spenders, **one entry**: "output 3 was spent by input 1
  of transaction X".

Each table gets its merge function. For balances it adds the differences
to the five numbers. For groups of spenders it puts the entry in the
list. The values at rest keep the format that they have today.

### What it gains

A block of mainnet reads about 13,800 unspent outputs, 11,000 balances
and up to 13,800 groups of spenders from the chain. Only the unspent
outputs would be left: about a third of those reads. In the benchmark it
is a quarter: 7,000 of 28,000.

The reads that go are also the cold ones. An unspent output was read once
already, when its transaction came into the mempool, so it is likely to be
in the cache when the block comes. The balances and the groups of spenders
in the chain were not.

They are the slow reads: measured, they are about 70% of the read ahead
([The read ahead by kind of key](#the-read-ahead-by-kind-of-key)). So the
read ahead should lose about 350 of its 505 ms.

### What it costs

- **Code in C.** RocksDB calls the merge from its own threads, during
  reads and compactions, so it cannot be a Haskell function. It goes in
  rocksdb-haskell, with the rest of its C. Two merges are needed: add five
  numbers, and add an entry to a small map.
- **A change of format, one way: database version 23.** The values that
  are stored today stay as they are, so nothing is rewritten and the
  upgrade takes no time. But once this version has opened a database, an
  older version cannot open it again.
- **An older store must be stopped at the door.** This was found while
  building it: a RocksDB that opens the database without the merge
  functions does not fail. It quietly drops the merge records that are
  still in its log, so a balance would lose a payment. So the database
  gets one more table, empty, called `merge-operators`. An older store
  does not know it, and RocksDB refuses to open a database when a table
  is not asked for: "Column families not opened: merge-operators".
  Nothing is lost.
- **A revert stays as it is.** It is rare, and it must delete a balance
  that goes back to nothing, which a merge cannot do. It keeps reading and
  writing whole values. RocksDB allows both on the same key.
- **A check goes from the import:** that no count or amount of an address
  goes below zero. It needs the value. It stays in the revert.
- **The test of equality changes.** It compares every key and value of two
  databases. For these two families it would compare what a read returns.
- **A few reads of spender groups stay:** the check that an output made in
  the block is not spent already. It looks for keys that are not there,
  which RocksDB answers from its filters.

### What it gained in the benchmark

The benchmark of the import, database version 22 against 23, six runs
each in turn. Milliseconds a block, medians.

| | Syncing, 22 | Syncing, 23 | At the tip, 22 | At the tip, 23 |
| --- | ---: | ---: | ---: | ---: |
| Read ahead | 56.2 | 20.6 | 125.5 | 91.8 |
| of which groups of spenders | 13.1 | 2.2 | 12.1 | 0.0 |
| of which balances | 24.2 | 0.0 | 23.7 | 0.0 |
| Whole block | 189.2 | 148.2 | 433.0 | 398.0 |
| Bytes in the batch | 6.6 MB | 6.3 MB | 9.8 MB | 9.0 MB |

- **The two kinds of read are gone,** as meant. What is left of the read
  ahead at the tip is the unspent outputs and the mempool.
- **The benchmark shows the least of it.** Its database is in memory, so
  a read there is processor time only. In production those reads wait for
  the disk, and were about 70% of the read ahead.
- **The batch is a little smaller:** a merge record for a group of
  spenders holds the new entries only, not the whole group.

### What it does to reads

It can make a read slower, for a key that changed lately. For most keys
nothing changes.

- **Today** a read stops at the newest value of a key. For a busy address
  that value is often still in memory, in the table that RocksDB fills
  before it writes a file.
- **With merges** a read collects every merge record of the key, down to
  the last whole value, and adds them up. The whole value of a busy
  address can be several levels down. That is one block to read from each
  level that has a record, where today it is one block or none.
- **It is bounded.** A compaction folds the records of a key into one for
  each level, and into the whole value at the last level. So a read finds
  about as many records as there are levels, which is seven, and one more
  for each of the few newest files.
- **An address or transaction that no recent block touched** reads as
  fast as today: one whole value, at the bottom.
- **What reads them:** the balance of an address or of an extended key,
  and every transaction that is returned with the spenders of its
  outputs.

Measured with a small benchmark that comes with the store: a key with a
number of merge records, each in a table file of its own, against the
same key as one whole value. Microseconds a read, one thread.

| Merge records on the key | 0 | 1 | 3 | 6 | One whole value |
| --- | ---: | ---: | ---: | ---: | ---: |
| Balance, blocks in the cache | 10.6 | 13.5 | 18.2 | 25.2 | 7.5 |
| Spenders of a transaction, blocks in the cache | 17.9 | 22.5 | 26.9 | 35.4 | 13.2 |
| Balance, cache of 1 MiB | 20.3 | 27.4 | 38.1 | 53.2 | 11.1 |
| Spenders of a transaction, cache of 1 MiB | 29.8 | 37.5 | 49.8 | 67.5 | 19.7 |

- **Each merge record costs a read about 2.5 microseconds** when its
  block is in the cache, and about 6 when the cache is too small.
- **The files cost more than the records.** The column "0" has the six
  files and no record in them for the key: it is already slower than one
  whole value. That part is there today too, whenever a key is spread
  over several files.
- **On a disk it is another matter.** A record whose block is not in the
  cache is one more read from the disk, a fraction of a millisecond. That
  is the case to measure on a copy of the database of mainnet, with the
  address and transaction requests of the load test, before this goes to
  production.
- **Nothing puts a limit on the records of a very busy key** between two
  compactions, other than the compactions themselves.

## 2. Warm the cache for the next blocks

### What it is

While a store catches up, the next blocks are downloaded before they are
imported. The store could read the keys of the next few blocks while it
writes the current one, and throw the values away. The point is only to
bring those parts of the files into RocksDB's cache, so that the read
ahead of the next block finds them in memory.

The values cannot be kept: the block in between may change them. That is
why this warms RocksDB's cache and does not fill the cache of the batch.

### What it gains

Nothing at the tip, where the next block does not exist yet. It helps a
store that is catching up or syncing from nothing, where the read ahead
is nearly all of a block's time and the disk has room for more reads.

### What it does to reads

- **At the tip:** nothing.
- **While catching up:** a little slower. The warming reads share the disk
  with the requests, and push other data out of the cache. A store that is
  catching up serves old data anyway, and catches up sooner for it.

## 3. Cheaper serialisers in haskoin-core

### What it is

The store turns transactions, hashes and outputs into bytes, and back,
with the serialisers of haskoin-core, through a library called cereal.
Measured in the benchmark of the import, before this step:

- writing one transaction hash allocated 680 bytes, for 32 bytes of
  output;
- a transaction of 370 bytes took 6 microseconds and allocated 18 kB.

### What the cause was

Not what was suspected. The guess was that the compiler did not make
direct code for the types that are used most. It did. The cost was in
how cereal works, and in the hashing:

- **cereal builds a small function for each field** that it writes or
  reads, and runs them afterwards: 10.7 kB of memory to write a
  transaction of 387 bytes.
- **cereal takes a buffer of four kilobytes** every time something is
  serialised, even one byte: 200 nanoseconds each time.
- **The hashing library** allocated 1.5 kB and spent 400 nanoseconds
  around each round of SHA-256, more than a short input takes to hash.

### How it was done

A new module of haskoin-core, `Haskoin.Util.Direct`, runs the same
serialisers in three small ways of its own: count the bytes, write them
into a buffer of exactly that size, read them from a buffer at an
offset. The types that the store uses most go through it, in haskoin-core
and in haskoin-store-data. The hash functions call the C code of the
hashing library on memory that they hold for the call.

No byte that is written changed, and no error message. A parse that
fails is run again through cereal, so that the error is the one that
cereal gives.

### What it gained

From the benchmarks of the two libraries, before and after.

| | Before | After |
| --- | ---: | ---: |
| Hash of a transaction (`txHash`), SegWit | 2.87 µs | 1.07 µs |
| Write a SegWit transaction of 387 bytes | 1.58 µs | 0.52 µs |
| Read it | 1.03 µs | 0.42 µs |
| Weight of a transaction | 2.74 µs | 0.08 µs |
| Read a block of 3,000 transactions | 5.8 ms | 1.6 ms |
| Write it | 5.0 ms | 0.5 ms |
| Read a transaction of the database (`TxData`) | 1.87 µs | 0.64 µs |
| Write it | 2.44 µs | 0.63 µs |
| Read an unspent output of the database | 228 ns | 64 ns |
| Read the record of a block with 3,000 transactions | 246 µs | 50 µs |

- **In the store,** with the benchmark of the import, a block went from
  148 to 138 ms while syncing and from 398 to 372 ms at the tip. The
  prepare stage went from 12 to 9 ms and the write from 179 to 163 ms.
  The two sets of runs were an hour apart on the same machine, not in
  turn, so take a few milliseconds either way.
- **More is left in the store itself.** It asks for the size of a
  transaction by serialising it and counting, in a dozen places, and
  every key of a single read still takes cereal's buffer of four
  kilobytes. Both have a direct way now.

### What it does to reads

**It makes them faster.** This is the only step here that does. Every
transaction that the store returns is read from the database with these
serialisers: about three times faster now. A raw block reads and writes
every transaction in it. A block that comes from a peer decodes in 5.7 ms
where it took 9.2.

What it does to a whole request is not measured yet.

## 4. Hand RocksDB the batch in one piece

### What it is

The store gives RocksDB the keys of a block one call at a time: 65,000 to
125,000 calls for a block of the benchmark. Each call is made the safe
way, which lets other Haskell threads run but costs about 250
nanoseconds. Two fixes, the second better:

- **Make those calls the fast way.** They only copy bytes in memory.
  Measured: 65,000 calls take 7 ms where they took 16.
- **Build the batch in Haskell,** in RocksDB's own format, on all cores,
  and hand it over with one call. RocksDB has a function for that.

### How it was done

The second way, with one difference. The cores serialise the changes
straight into buffers of 256 KiB as a list of records: which table, set
or delete, the key, the value. A small C function that comes with
rocksdb-haskell reads a buffer and adds its records to the batch. A block
is 34 calls where it was 65,000, and still one batch.

The records are in a format of the binding's own and not in the one that
RocksDB keeps a batch in, which is internal to RocksDB and could change.

### What it gained

Measured with the benchmark, the code before and after in turn, six runs
each. Milliseconds a block, medians.

| | Before | After |
| --- | ---: | ---: |
| Syncing: write | 134 | 86 |
| Syncing: whole block | 250 | 198 |
| At the tip: write | 257 | 183 |
| At the tip: whole block | 522 | 447 |

- **More than the 20 to 30 ms that was expected.** The calls cost more in
  place than when they were timed alone.
- **A busy store should gain more.** In the binding's own benchmark,
  65,000 operations took 48 ms key by key and 22 ms packed. Next to eight
  busy threads they took 420 ms key by key and 28 ms packed. That has not
  been measured in the store.

The rest of the write is not touched: about half of it is RocksDB adding
the keys to its tables in memory, in one thread, and nothing here makes
that faster.

### What it does to reads

Nothing. One thing to keep: the call that writes the batch must stay a
safe call. Made the fast way, it would stop every request for the 200 ms
that it takes.

## 5. Batched multi-get and asynchronous reads

### What it was

RocksDB has a newer multi-get that sorts its keys and reads those that are
in one file together, and an option to read from several files at once.
The suggestion was to use both, so that one call would do what the store
now does with 32 threads.

### What I found

- **The newer multi-get is already in use.** In RocksDB 10.10 and 11.1 the
  function that the binding calls is the batched one underneath.
- **Reading several files at once needs a RocksDB built for it,** with
  coroutines, which the library on the production machine is not. Without
  them the option does nothing for a multi-get.

So there is nothing to gain here as things stand, for blocks or for
reads. The 32 threads already give the disk as many reads at a time as it
can use.

## The mempool reads

### What was found

The read ahead of a block at the tip spent about 200 ms on the mempool,
which was not expected: that part of the database is 64 MB and 20 MB for
25,000 transactions, all of it in memory.

It is not one slow read. A block of mainnet makes about 50,000 small
reads there, of six kinds, and each costs about 8 microseconds of
processor time: RocksDB looks in up to four places for a key, and each
read crosses from Haskell into RocksDB and back. On real blocks the
largest kind is a scan for each transaction of the block, to find the
mempool transactions that spend its outputs: 66 to 150 ms a block.

These reads exist because a block at the tip empties part of the
mempool. Each of its transactions has to leave it, with its entry in the
list, its spenders and the summary of each address that it touches.

### What was done

- **The mempool area is kept in memory as well.** The block store is the
  only writer of the database. It loads the two mempool tables once, and
  applies each batch to its copy when the batch is written. Every read
  that it makes of the mempool is answered from the copy. Requests of the
  API still read RocksDB.
- **One read is no longer made:** whether the parent of a mempool
  transaction is in the mempool too, when the output that is spent is
  unspent in the chain. A transaction with such an output is confirmed.
- **Not done: merge records for the summaries of addresses.** With the
  copy in memory the store has the old summary already, so there is no
  read left for a merge record to save.

The copy is held to the database by the tests: after every operation of
the random histories, it must equal the two tables byte for byte.

### What it costs and gains

- **Memory:** 85 MiB for 25,000 mempool transactions, 3.5 kB each.
- **Start:** 0.3 seconds to load them.
- **In the benchmark,** a block at the tip went from 371 to 337 ms. Its
  mempool reads went from 73 to 18 ms.
- **In production** it should gain more, since the scans cost six times
  more there than in the benchmark. That is not measured yet.

## What a mined block is read for

The store looks up every output that a block spends. That is not to
check the block. It needs three things from each: its value, to take it
off the balance of its address; its script, to know the address; and the
position of the transaction that made it, which is part of the key of
the address's unspent output, to delete it. A node only has to know that
an output is unspent. The store has to know whose it was.

Three changes followed from asking which reads are checks and which are
data. A mined block is taken to be valid: every output that it spends is
unspent in the chain or made by the block itself.

### The checks that went

- **Looking in the chain for a spender of each output,** to be sure that
  it was not spent already. A block of mainnet has 3,000 to 6,500 inputs
  that spend an output made in the same block, and a store whose mempool
  had not seen those transactions looked each up by itself. On the trial
  one block spent 10 seconds on it.
- **Looking for conflicts of a block transaction that is in the
  mempool.** The mempool holds one spender for an output, so such a
  transaction is the spender of all its inputs, and nothing else in the
  mempool can conflict with it.
- **Reading from the chain the outputs that the block itself makes.**
  They cannot be there yet.

What stays is the mempool giving way: a block transaction that the
mempool has not seen can make a mempool transaction invalid, and that one
is found and deleted with what spends it.

A block that breaks the assumption is still not half written: its batch
throws, and a batch that throws writes nothing.

### The outputs that are remembered

When a transaction comes into the mempool, the store reads the outputs
that it spends. When a block confirms it, the store read them again. Now
it keeps them in memory in between, for as long as a mempool transaction
spends them, and a block at the tip takes them from there.

It is a cache and not a copy: an output that is not in it is read from
RocksDB, so an empty table after a restart is right, only slower. Every
unspent output that a batch writes is checked against it, so an entry
equals the database as long as it is there. It takes 162 bytes an output:
7 MiB for 25,000 mempool transactions.

### In the benchmark

Milliseconds a block, the whole import. The benchmark got two new kinds
of block, with chains of transactions inside the block as on mainnet.

| Kind of block | Before | No checks | Outputs remembered |
| --- | ---: | ---: | ---: |
| Mempool empty | 136 | 135 | 135 |
| Mempool empty, chains in the block | 115 | 106 | 106 |
| At the tip | 351 | 339 | 322 |
| The mempool has not seen the block, chains | 161 | 138 | 138 |

In the benchmark every read is from memory, so these are the least that
the changes are worth. The reads that went are those that waited for the
disk in production.

### Fleets

The servers of a fleet answer from one mempool in Redis. Each still has
its own mempool in its database, which is what it offers to the fleet.

- **A fleet never writes a server's own mempool,** so the copy in memory
  stays equal to the database.
- **A fleet does not read the copy or the remembered outputs.** It reads
  the database, and its views of the database at an earlier block get no
  copy at all.
- **For a valid block the database is the same key for key** with and
  without the checks, the records of what a block replaced included, from
  which every server of a fleet answers alike.

The tests of fleets ran after each change, a reorganisation with a
replaced payment among them.

## The trial on a copy of the database of mainnet

Database version 23 was tried on groot before going to production, on
2026-11-15 from 14:04 UTC.

### How

A writable btrfs snapshot of the database of mainnet, 1.6 TB, which takes
no space until it changes, and a second store on it, on its own port, next
to the store in production. Both followed the chain from the same block.
The trial had a block cache of 8 GB, production 24 GB. A backup was
reading the same disk throughout.

### The upgrade

The store logged the upgrade from version 22 to 23, set the two merge
functions and made the guard table. It took no time. This was RocksDB
10.10, where the tests had only been run on 11.1 and 8.9.

### Import

The same blocks on both stores, milliseconds a block.

| Block | Production, 22 | Trial, 23 | Read ahead, production | Read ahead, trial |
| --- | ---: | ---: | ---: | ---: |
| 14:08, the trial just opened | 2,392 | 1,647 | 1,819 | 1,096 |
| 14:33 | 2,440 | 741 | 1,685 | 249 |
| 14:37 | 1,662 | 918 | 1,152 | 490 |
| 14:49 | 2,197 | 848 | 1,593 | 332 |
| 14:56 | 2,122 | 856 | 1,621 | 432 |
| 15:40 | 1,706 | 1,017 | 974 | 351 |
| 15:49 | 1,723 | 887 | 1,101 | 364 |
| 16:02 | 2,264 | 1,100 | 1,483 | 543 |

- **The trial read no balance and no group of spenders** in any block.
  Production spent 350 to 1,580 ms a block on them.
- **The trial also has the batch in one piece and the cheaper
  serialisers,** so this is the three steps together.
- **Five blocks are left out:** two were imported while the read test
  below ran, and three while the trial was held up by a limit on its
  memory that was set too low for the test, not by the store.

### Reads

4,000 requests on each store: balances of addresses and transactions
that a block had changed since the snapshot, which have merge records on
the trial, and older ones to compare with.

- **Every answer was equal,** but for five where the two mempools
  differed.
- **A first read of a balance that changed lately** took 1.30 ms on the
  trial at the median against 0.75 in production. Older balances took
  1.53 against 1.22: so about 0.2 to 0.3 ms of it is the merge, and the
  rest is the smaller cache of the trial.
- **A repeated read** took the same on both: 0.78 against 0.73 ms.
- **With eight clients at once** both stores gave the same: about 2,700
  balances and 2,000 transactions a second.

### What the trial found

One block took 10 seconds in the apply stage. It was the chain lookup of
[The checks that went](#the-checks-that-went), which version 23 had left
to single reads, and which the benchmark could not show: its block at the
tip had every transaction in the mempool. With that fixed, and the
mempool area in memory, the same blocks on both stores:

| Block | Production, 22 | Trial, 23 | Read ahead, trial | Apply, trial | Write, trial |
| --- | ---: | ---: | ---: | ---: | ---: |
| 17:29 | 1,332 | 718 | 85 | 295 | 313 |
| 17:46 | 1,667 | 807 | 130 | 274 | 379 |
| 17:49 | 1,229 | 682 | 133 | 218 | 302 |

The mempool reads of a block went from 165 to 449 ms in production to 21
to 37 ms on the trial.

With the checks of a mined block gone and the outputs of mempool
transactions remembered, the next seven blocks, after two to warm up:

| Block | Production, 22 | Trial, 23 | Read ahead, trial | Apply, trial | Write, trial |
| --- | ---: | ---: | ---: | ---: | ---: |
| 19:04 | 1,176 | 470 | 41 | 170 | 236 |
| 19:07 | 1,287 | 512 | 38 | 170 | 283 |
| 19:22 | 1,383 | 681 | 64 | 220 | 379 |
| 19:36 | 1,280 | 628 | 51 | 227 | 339 |
| 19:54 | 2,533 | 706 | 69 | 225 | 399 |
| 19:55 | 1,328 | 546 | 51 | 188 | 270 |
| 20:00 | 1,247 | 622 | 38 | 232 | 330 |

- **Every block took less than a second,** 470 to 706 ms, with a third of
  the cache that production has.
- **The read ahead is 38 to 69 ms,** where production spends 660 to 1,560.
  The unspent outputs take 10 to 31 ms of it: most come from memory.
- **The write is now the largest stage,** about half of a block. Most of
  it is RocksDB adding the keys to its tables in memory, in one thread.
- **After 41 blocks,** 2,000 more reads gave the same answers as
  production, but for three where the two mempools differed.

### What it does not show

Fourteen blocks is a short time. The merge records of a busy key were
still mostly in memory, in one or two files at most. A key with records
in several levels, after days, is the case of the table in
[What it does to reads](#what-it-does-to-reads), and was not seen here.

## What none of this touches

- **RocksDB's own part of the write,** about half of it, so about 150 ms
  a block: adding the keys to its tables in memory, in one thread.
- **The apply stage at the tip,** about 150 ms: the rules, and the
  transactions of the block leaving the mempool.

With steps 1, 3 and 4 done, these two would be half of what is left.

## Where it stands

The steps were taken in this order, on 2026-11-15.

1. **Time the read ahead by kind of key: done,** and running in
   production. The groups of spenders and the balances are the slow
   reads, so step 1 goes ahead.
2. **Step 4, the batch in one piece: done,** not yet in production.
3. **Step 3, cheaper serialisers: done,** in haskoin-core and
   haskoin-store-data, and the store takes them. Not in production.
4. **Step 1, merges: done,** not in production. It makes the database
   version 23, which no older store can open. It was tried on a copy of
   the database of mainnet: see
   [The trial](#the-trial-on-a-copy-of-the-database-of-mainnet).
5. **The mempool reads: done,** not in production: see
   [The mempool reads](#the-mempool-reads).
6. **No checks of a mined block, and the outputs of mempool
   transactions remembered: done,** not in production: see
   [What a mined block is read for](#what-a-mined-block-is-read-for).
7. **Step 2, warming the cache while catching up: not started.** It does
   nothing at the tip, so it waits for a decision that catching up
   matters.

### The benchmark, step by step

The same block at the tip in `haskoin-store-bench import`, on a database
in memory, after each step. Milliseconds a block. The runs were hours
apart, so each figure is good to a few per cent.

| | Whole block | Read ahead | Write |
| --- | ---: | ---: | ---: |
| Version 1.15.1, one read at a time | 2,321 | | 582 |
| In stages | 522 | 126 | 257 |
| Step 4: the batch in one piece | 447 | 126 | 183 |
| Step 1: merges | 398 | 92 | 179 |
| Step 3: cheaper serialisers | 372 | 88 | 163 |
| The mempool area in memory | 337 | 35 | 171 |

In production the read ahead waits for a disk, which the benchmark does
not have: there steps 1 and 4 should gain more than this, and that is
still to be measured.

There was one more idea for the slow reads that needs no change of
format: read the balances and the groups of spenders when a transaction
comes into the mempool, as its unspent outputs already are, so that the
block finds them in the cache. It would gain less than step 1, since the
reads are still made, from memory. It is not needed if step 1 is done.
