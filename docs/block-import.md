# Importing a block in stages

How the store writes a block to its database, and why it is built this
way. The code is `Haskoin.Store.Logic` (`importBlock`) and
`Haskoin.Store.Database.Writer` (the read cache and the reads ahead).
This page describes the code; it is not part of the package.

## In short

A full block used to take a store many seconds to write to its database,
while its disk and half its cores sat idle. The time went into tens of
thousands of small reads, made one at a time. Now a block goes through
four stages: work out what the block alone tells, read what it needs
many keys at a time, apply the rules in memory, and write every change
at once. Balances and spenders are changed without being read, and a
mined block is not checked again: the node did that. The cost: a balance
that recent blocks changed takes a few microseconds more to read, an
older store cannot open the database, and a block that is not valid is
promised only that it is written whole or not at all.

The database holds the chain and nothing else. The mempool is the pool in
Redis ([one-fleet.md](one-fleet.md)), so the import never meets an
unconfirmed transaction. When the tip moves past a block, the pool gives
way to it ([fleet-mempool.md](fleet-mempool.md),
[tip-first.md](tip-first.md)).

## Words used here

The shared words (store, node, the chain, the pool, the tip) are in
[README.md](README.md).

| Word | What it means here |
| --- | --- |
| **the rules** | The code that says what a block changes in the database: outputs made and spent, balances, the indexes of addresses. |
| **batch** | All the changes of one block, collected in memory and then written to RocksDB together: all of them or none. |
| **stage** | One of the four steps of an import: prepare, read ahead, apply, write. |
| **multi-get** | One call to RocksDB that reads many keys. |
| **read ahead** | To read with multi-gets, before the rules run, the keys that the rules will ask for. |
| **read cache** | What a batch has read from the database, kept in memory until the batch is written. It remembers "not there" too. |
| **spender** | The transaction and input that spend an output. The store keeps the spenders of neighbouring outputs of one transaction under one key: a **group**. |
| **merge record** | A note in RocksDB that says how a value changes ("5,000 more"), not what it becomes. Writing one needs no read. |
| **merge operator** | The function that RocksDB runs to turn a value and its merge records into the new value. |
| **compaction** | RocksDB's own tidying of its files, in the background. |
| **write-ahead log** | The file where RocksDB puts a batch first. A batch that is in it is written. |
| **memtable** | A table that RocksDB keeps in memory, where new keys go before they reach a file. |
| **revert** | To take the newest block out of the database again, as a reorganisation needs. |
| **orphan** | A block that spends an output which the import cannot find. It is refused. |

## The problem

Measured on a store of Bitcoin mainnet, before this design:

| What | Figure |
| --- | --- |
| Importing a full block of 4,000 transactions | 13 to 18 s, with the disk and half the cores idle |
| Reads of the database for that block | some 30,000, one at a time from one thread, many of a key read already |
| Times each hash was worked out | five or six |
| Times the block was serialised to get its size | three |
| Random reads on that disk, one at a time | 5,800 a second |
| Random reads on that disk, 32 at a time | 31,000 a second |

So the design reads less, reads each key once, and reads many keys at
once.

## The stages

### 1. Prepare

The store works out what the block alone tells, on all cores
(`prepareBlock`). For each transaction:

- it serialises the transaction once without its witness, hashes those
  bytes, and keeps their length;
- if the transaction has a witness, it serialises it once more for its
  full length;
- it works out the address of each output.

The size and weight of the block are sums of those lengths.

The order in which the transactions are applied is worked out here too,
from the hashes. Each transaction is applied after those it spends.
Bitcoin blocks already come in such an order. Bitcoin Cash orders them
by hash, so they must be sorted.

### 2. Read ahead

What a block reads is known before any rule runs: the outputs that it
spends and does not make itself (`readAheadBlock`). An output that one
transaction of the block makes and another spends is not in the
database. The rules find it in the batch.

The keys are cut in pieces, and each piece is one multi-get. Eight
multi-gets run at a time for each core. A multi-get does not hold a core
while it waits for the disk. So when the disk has to be read, four cores
have 32 reads in flight, where there was one.

The answers go into the read cache, those that say "not there" too.

### 3. Apply

The rules run one transaction after another. When they ask the batch for
an unspent output, the batch looks in three places, in this order:

1. the changes that the batch has made so far;
2. the read cache;
3. the database, and then it remembers the answer.

The changes come first, so the cache can never hide one. A key that was
not read ahead is read when it is asked for. So reading ahead is never
needed to be right: it only moves the reads to where they can be done
together.

The database does not change while a batch is open. The block store is
its only writer, and it writes one batch at a time.

### 4. Write

The batch is one write to RocksDB's write-ahead log. That is what makes
a block all or nothing.

1. The keys and values are serialised on all cores, into buffers of
   256 KiB. Each key gets a header of 17 bytes, then the key, then the
   value.
2. RocksDB is handed one buffer in one call (`writePacked` of the
   RocksDB binding): some forty calls for a block.

The other way is one call for each key. Each such call gives up a core
of the program and must win one back, which costs most when the store is
busy. Only a batch of fewer than 256 keys is handed over key by key.

## Balances and spenders, without a read

A block changes the balance of every address that it pays or spends
from. It also adds a spender to the group of every output that it
spends. Reading each of them only to write it back was most of the reads
of a block on a store of mainnet: 370 to 1,150 ms of a block.

So the block writes how they change, and reads neither.

- **A balance** gets one merge record with the differences. A block that
  pays 5,000 satoshis to an address writes: 5,000 more confirmed, one
  more unspent output, one more transaction. All that a block does to an
  address is summed into that record.
- **A group of spenders** gets a merge record with the spenders to add.
- **RocksDB works out the values** when they are read, and when a
  compaction brings the records and the value together. It uses two
  merge operators written in C (`Haskoin.Store.Database.Merge`).
- **A revert reads.** Taking a block out checks that nothing goes below
  zero, and deletes a balance that goes back to nothing.

The cost: a reader pays a few microseconds for a key whose merge records
are not compacted yet (`haskoin-store-bench merge` measures it), and an
older store cannot open the database. The formats are in the Haddock of
`Haskoin.Store.Database.Merge`.

## What a mined block is not checked for

A block that reaches the store was mined: it passed proof of work, and
the node took it. So the store assumes that its transactions are valid:

- every output that the block spends is unspent in the chain, or made by
  a transaction of the block itself;
- no output is spent twice.

Checking that is the work of the node. Here, each check was a read of
the database for every input. The worst were blocks of mainnet whose
inputs mostly spend outputs of the same block. Those outputs are not in
the database, and looking for them took up to ten seconds of one block.
So:

- **No spender of the chain is read.** Spenders are written as merge
  records.
- **An output that the block makes is not read from the database.**
- **Only a coinbase is looked up in the chain**, in case it is confirmed
  already. That happened to two coinbases of 2010; the later one takes
  the place of the earlier. A transaction with an input cannot be
  confirmed twice, since its input is spent.

A block that breaks the assumption is promised one thing: it is written
whole or not at all. It is still refused whenever the import cannot read
what it needs:

- An input that does not exist, that the chain has spent, or that an
  earlier transaction of the block spent, is not there to read. The
  block is an orphan.
- A transaction that spends one output twice stops the batch when it
  comes to spend what it just spent. That look is in memory, and the
  benchmark took as long without it.

## What stays

Everything that keeps the store's own data right:

- **The unspent outputs that the block spends** are read. Their values,
  scripts and positions make the balances and the address indexes.
- **The order of the transactions**, as in stage 1. The sort takes 3 ms.
- **Reverts** check all that they did. A reverted block's transactions
  leave the database, and the move of the tip gives them back to the
  pool.
- **That the block builds on the best block.** This is about the store's
  own chain, not about the block.

## How it is tested

| Test | What it holds |
| --- | --- |
| `Haskoin.Store.LogicSpec` | A revert takes out exactly what the import of a block put in, key for key, on random chains. And `prepareBlock` gives the hashes, addresses and sizes that the block gives. |
| `Haskoin.Store.Database.WriterSpec` | The read cache of a batch never hides a change, and a batch answers the same without it. |
| `Haskoin.Store.Database.MergeSpec` | Merge records add up to the whole values. |

## The benchmark

`haskoin-store-bench import` works like this:

1. It builds a database with a million unspent outputs at half a million
   addresses.
2. It closes the database, so that RocksDB writes it to table files, and
   copies it.
3. It imports the same blocks into each copy: once with the read cache,
   and once without it (`withoutReadCache`), which reads every key when
   the rules ask for it.

A block has 3,500 transactions, 7,000 inputs and 7,000 outputs. Blocks
are of two kinds. Some spend outputs spread over the set. Others are
chains, where three inputs in four spend an output of the same block, as
on mainnet.

On four cores, with the files in memory, a block of the first kind takes
about 150 ms:

| Stage | Time | What the time goes into |
| --- | --- | --- |
| Write | about 85 ms | About 60 ms is RocksDB adding the keys to its memtables, in one thread. The rest is serialising, on all cores. |
| Apply | about 30 ms | The rules and the hash tables of the batch. |
| Read ahead | about 20 ms | The 7,000 outputs that the block spends. |
| Prepare | about 10 ms | Serialising and hashing each transaction. |

The benchmark shows the least that the design is worth. Its reads cost
30 microseconds each, all of it CPU. On a store with a SATA disk, reads
done 32 at a time went five times faster than one at a time.

## Choices made on the way

- **Hash tables.** The tables of a batch are keyed by outputs and
  addresses. `Haskoin.Store.Database.Table` hashes the bytes that these
  keys already hold. The generic hash took up to 0.8 microseconds a key.
- **Serialising the batch.** `encode` takes a buffer of four kilobytes
  for each key and each value. So each core serialises its share into a
  few large buffers instead.
- **Threads bound to cores.** Each thread starts on a core of its own.
  Work of a few milliseconds split over threads gains little if the
  threads are left to find a core.

## What was not done

- **Reading ahead for the next blocks** while one is written. A value
  read for a block not imported yet can be changed by the block before
  it. So it cannot go into the read cache of its batch. Only warming
  RocksDB's own cache would be safe.
- **Serialising faster.** Writing a transaction hash through the
  serialisers of haskoin-core allocates 680 bytes. That is half of the
  prepare stage and half of the serialising. The fix belongs in
  haskoin-core.
- **The write itself.** RocksDB writes a batch in one thread. A batch
  cannot be split without losing that a block is all or nothing.
- **A faster key for an address.** Serialising the key of an address
  summary takes four times what a key with a transaction hash takes. The
  serialiser of an address is in haskoin-core.
