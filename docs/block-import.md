# Importing a block in stages

How the store imports a block, and why it is built this way. The code is
`Haskoin.Store.Logic` (`importBlock`) and `Haskoin.Store.Database.Writer`
(the read cache and the reads ahead). It describes the code; it is not
part of the package.

The database holds the chain and nothing else. The mempool is the pool in
Redis ([one-fleet.md](one-fleet.md)), so the import never meets an
unconfirmed transaction. When the fleet tip moves past a block, the pool
gives way to it ([fleet-mempool.md](fleet-mempool.md)).

## The short answer

A block is imported in four stages:

1. **Prepare.** From the block alone, on all cores: the hash of each
   transaction, the address of each output, the sizes.
2. **Read ahead.** The unspent outputs that the block spends are read from
   RocksDB with multi-gets, many at a time, into a cache that belongs to
   the batch.
3. **Apply.** The rules run one transaction after another. They find
   everything in memory.
4. **Write.** All the changes go to RocksDB in one atomic batch.

Balances and spenders are not read at all: the block writes how they
change, as merge records. And a block that was mined is taken to be valid:
the import reads what it needs to write the block, and nothing to check it.

## The problem

A store of Bitcoin mainnet once took 13 to 18 seconds to import a full
block of 4,000 transactions, while its disk and half its cores sat idle.
The time went into some thirty thousand reads, one at a time from one
thread, many of them of a key read already. Hashes were worked out five or
six times, and the block was serialised three times to get its size.

On that disk random reads go at 5,800 a second one at a time, and 31,000 a
second 32 at a time. So the design reads less, reads each key once, and
reads many keys at once.

## The stages

### 1. Prepare

`prepareBlock` splits the transactions over the cores. For each one it
serialises the transaction once without its witness, hashes those bytes,
and keeps their length. A transaction with a witness is serialised once
more for its full length. It also works out the address of each output.

The size and weight of the block are sums of those lengths. The order in
which the transactions are applied is worked out here too, from the
hashes. Each is applied after those it spends: Bitcoin blocks come in such
an order, but Bitcoin Cash orders them by hash.

### 2. Read ahead

What a block reads is known before any rule runs: the outputs that it
spends and does not make itself (`readAheadBlock`). An output that one
transaction of the block makes and another spends is not in the database;
the rules find it in the batch.

The keys are cut in pieces, and each piece is one RocksDB multi-get. Eight
run at a time for each core. A multi-get does not hold a core while it
waits for the disk, so on a cold database that is 32 reads in flight on
four cores, where it was one.

The answers go into the **read cache** of the batch, those that say "not
there" too.

### 3. Apply

The rules ask the batch for an unspent output. The batch looks in three
places, in this order:

1. the changes that the batch has made so far;
2. the read cache;
3. the database, and then it remembers the answer.

The changes come first, so the cache can never hide one. A key that was
not read ahead is read when it is asked for, so reading ahead is never
needed to be right. It only moves the reads to where they can be done
together.

The database does not change while a batch is open. The block store is
its only writer, and it writes one batch at a time.

### 4. Write

One batch, one write to the write-ahead log. That is what makes a block
all or nothing.

The keys and values are serialised on all cores, into buffers of 256 KiB:
for each key a header of 17 bytes, then the key, then the value. RocksDB
is handed a buffer in one call, some forty calls for a block, not one call
for each key (`writePacked` of the RocksDB binding). A call for each key
gives up a core of the program and must win one back, which costs most
when the store is busy. A batch of fewer than 256 keys is handed over key
by key.

## Balances and spenders, without a read

A block changes the balance of every address that it pays or spends from,
and adds a spender to the group of every output that it spends. Reading
each of them only to write it back was most of the reads of a block on a
store of mainnet: 370 to 1,150 ms of a block.

So the block writes how they change and reads neither:

- **A balance** gets a merge record with the differences: so much more
  confirmed, one more unspent output, one more transaction. All that a
  block does to an address is summed into one record.
- **A group of spenders** gets a merge record with the spenders to add.
- **RocksDB works out the values** when they are read, and when a
  compaction brings the records and the value together, with two merge
  operators written in C (`Haskoin.Store.Database.Merge`).
- **A revert reads.** Taking a block out checks that nothing goes below
  zero, and deletes a balance that goes back to nothing.

The cost: a reader pays a few microseconds for a key with merge records
that are not compacted yet, and an older store cannot open the database.
The formats are in the Haddock of `Haskoin.Store.Database.Merge`.

## What a mined block is not checked for

A block that reaches the store was mined: it passed proof of work, and the
node took it. The store takes it that its transactions are valid:

- every output that the block spends is unspent in the chain, or made by
  a transaction of the block itself;
- no output is spent twice.

Checking that is the work of the node. Here each check was a read of the
database for every input. On blocks of mainnet whose inputs mostly spend
outputs of the same block, such lookups of missing keys took up to ten
seconds of one block. So:

- **No spender of the chain is read.** Spenders are written as merge
  records.
- **An output that the block makes is not read from the database.**
- **Only a coinbase is looked up in the chain**, in case it is confirmed
  already. That happened to two coinbases of 2010; the later one takes the
  place of the earlier. A transaction with an input cannot be confirmed
  twice, since its input is spent.

A block that breaks the assumption is promised one thing: it is written
whole or not at all. It is still refused whenever the import cannot read
what it needs. An input that does not exist, that the chain has spent, or
that an earlier transaction of the block spent, is not there to read, and
the block is an orphan. A transaction that spends one output twice stops
the batch when it comes to spend what it just spent.

## What stays

Everything that keeps the store's own data right:

- **The unspent outputs that the block spends** are read: their values,
  scripts and positions make the balances and the address indexes.
- **The order of the transactions**, as above. The sort takes 3 ms.
- **Reverts** check all that they did. A reverted block's transactions
  leave the database; the tip move gives them back to the pool.
- **That the block builds on the best block**, which is about the store's
  own chain, not about the block.

## How it is tested

- `Haskoin.Store.LogicSpec`: a revert takes out exactly what the import of
  a block put in, key for key, on random chains; and `prepareBlock` gives
  the hashes, addresses and sizes that the block gives.
- `Haskoin.Store.Database.WriterSpec`: the read cache of a batch never
  hides a change, and a batch answers the same without it.
- `Haskoin.Store.Database.MergeSpec`: merge records add up to the whole
  values.

## The benchmark

`haskoin-store-bench import` builds a database with a million unspent
outputs at half a million addresses, closes it so that RocksDB writes it
to table files, and copies it. It then imports the same blocks into each
copy: with the read cache and without it (`withoutReadCache`), which reads
every key when the rules ask for it. A block has 3,500 transactions, 7,000
inputs and 7,000 outputs. Some blocks spend outputs spread over the set;
others are chains, where three inputs in four spend an output of the same
block, as on mainnet.

On four cores, with the files in memory, a block of the first kind takes
about 150 ms:

- **Write, about 85 ms.** About 60 ms is RocksDB adding the keys to its
  memtables, in one thread. The rest is serialising, on all cores.
- **Apply, about 30 ms.** The rules and the hash tables of the batch.
- **Read ahead, about 20 ms.** The 7,000 outputs that the block spends.
- **Prepare, about 10 ms.** Serialising and hashing each transaction.

The benchmark shows the least that the design is worth. Its reads cost 30
microseconds, all CPU; on a store with a SATA disk, reads done 32 at a
time went five times faster than one at a time.

## Choices made on the way

- **Hash tables.** The tables of a batch are keyed by outputs and
  addresses. `Haskoin.Store.Database.Table` hashes the bytes that these
  keys already hold; the generic hash took up to 0.8 microseconds a key.
- **Serialising the batch.** `encode` takes a buffer of four kilobytes for
  each key and each value. Each core serialises its share into a few large
  buffers instead.
- **Threads bound to cores.** Work of a few milliseconds split over
  threads gains little if the threads are left to find a core, so each
  thread starts on its core.

## What was not done

- **Reading ahead for the next blocks** while one is written. A value read
  for a block not imported yet can be changed by the block before it, so
  it cannot go into the cache of its batch. Only warming RocksDB's own
  cache would be safe.
- **Serialising faster.** Writing a transaction hash through the
  serialisers of haskoin-core allocates 680 bytes. Half of the prepare
  stage and of the serialising is that. It belongs in haskoin-core.
- **The write itself.** RocksDB writes a batch in one thread. A batch
  cannot be split without losing that a block is all or nothing.
- **A faster key for an address.** Serialising the key of an address
  summary takes four times what a key with a transaction hash takes. The
  serialiser of an address is in haskoin-core.
