# Concurrency in haskoin-store

What runs at the same time in a store and across a fleet, who may change
what, and why no order of events breaks it. It ends with the rules that
an audit of November 2026 left, and with the known limits. The threads
of haskoin-node, on which the store runs, are in that package's own
`docs/concurrency.md`.

Read the part about a protocol before you change it, and keep this page
in step with the code.

## In short

A store is a few dozen threads, a RocksDB database that one of them
writes, and a Redis that every store of a fleet shares. Such a program
breaks when two parts change the same thing at once, or when one acts on
what another has just changed.

So whatever runs beside something else is decided by a pure function,
by a change to Redis that is carried out in one step, or by a short
script that Redis runs alone. Every piece of shared state has one owner,
or changes in one step with what the change depends on. A thread only
carries out what a rule says, so the rules are tested without threads.

It has a cost. A change to Redis takes two calls where a script took
one. And the longest change, a move of the tip, has to be given the
right to go first: new transactions wait meanwhile.

## Words used here

The shared words (store, fleet, the pool, the tip, view, the cache,
entry, turn) are in [README.md](README.md). This page adds these.

| Word | What it means here |
| --- | --- |
| **machine** | A pure function: from what it knew and what just happened, it says what to do. A thread carries that out. |
| **owner** | The one thread that may change a piece of state. |
| **mailbox** | A queue of messages that one thread reads, in order. |
| **decision** | As in the README: one change to Redis, worked out in Haskell and applied in one step. |
| **question** | One command that reads Redis and changes nothing. |
| **answer** | What Redis said to a question. |
| **write** | One command that changes Redis. A decision only lists its writes. |
| **the script** | `lua/atomic.lua`: it carries out the writes of a decision if every answer is still the same. Any other Lua script is named as such. |
| **refused** | A decision whose answers changed: nothing was written, and it is made again. |
| **pass** | One run through the code of a decision, from its start. |
| **the model of Redis** | The part of Redis that the fleet uses, as a pure value. The tests run decisions against it. |
| **the model of the fleet** | The rules of the fleet as pure functions. |
| **Valkey** | The Redis that the tests run, in a container. |
| **cell** | A field of a hash, a member of a set, the score of a member, or what a key holds. |
| **lease** | The time until which a store is **live**. The store renews it at every turn, for ten seconds. |
| **report** | What a store tells Redis at every turn: its newest blocks. |
| **the agreed block** | The newest block that every live store reported. The tip moves only there. |
| **move** | Taking the tip to the agreed block, with the pool and the cache (`moveTip`). The store that does it is the **mover**. |
| **the way** | The blocks between the old tip and the new one, and what they did. |
| **token** | A random number that a mover picks for one move. |
| **piece** | A part of what a move writes to the cache before the tip moves. |
| **the floor** | The right of the mover to go first (the keys `fx:moving` and `fx:beat`). |
| **beat** | The time that the mover writes every two seconds, to say that it is still at its move. |
| **gives way** | What a decision does when it has to wait: it writes nothing. |
| **mark** | A field of the hash `pool:leaving`: its transaction has to leave the pool, and is still in. |
| **take out** | Remove a marked transaction from the pool and from all that mentions it, with its mark. |
| **offer** | A store asks the pool to take one transaction. |
| **withdrawal**, **expiry** | Removing a transaction from the pool with all that spends from it; an expiry is that of the old ones. |
| **build**, **drop**, **eviction** | Writing the entry of an xpub into the cache; deleting one; deleting those of lowest priority. |
| **member** | The part of a store that takes its turns in the fleet (`Haskoin.Store.Fleet.Member`). |

## The rule

Whatever runs at the same time as something else is one of three things:

1. A **machine**, in a module of its own, tested with QuickCheck on
   random sequences of events.
2. A **decision** about what Redis holds.
3. A short **Lua script** that Redis runs alone, tested against a model
   of it.

The threads keep the state and carry out what these say.

A race is ruled out in one of four ways. Each piece of state below names
its way.

| Way | What it means |
| --- | --- |
| **A single owner** | One thread changes the state. Others at most read it. |
| **One STM transaction per change** | The state is read, the change worked out and the new state written in one STM transaction. |
| **One step in Redis** | A change to Redis and what it depends on are one step. A Lua script reads again what it depends on (the tip, the agreed block, the token of a move) and answers `raced` if that changed. For the pool and the cache it is a decision ("Decisions in one step", below). |
| **The order of one queue** | Events that must keep their order go through one mailbox that one thread reads. |

## The block store

One thread, the block store's, reads one mailbox. Node events, blocks
from peers, pongs and a clock tick all come through it. That thread
owns:

| State | Machine | Rule |
| --- | --- | --- |
| `sync`, the block download | `Haskoin.Store.BlockStore.Sync` | single owner |
| `knows`, what each peer said it has | read and written by the owner | single owner |
| the reservation of the syncing peer | haskoin-node's `Lock`, by the store's own `owner` | single owner |
| the database | the writer, one RocksDB batch per block | single owner |

The store asks one peer at a time for blocks, the syncing peer. A peer
that is too slow is passed over, and the next is asked. The rules of the
machine (`Sync`), each a property of `SyncSpec`:

- **One syncing peer at most.** Only `start` and `pass` set one.
- **A block is accepted only from a peer that was asked for it.**
- **A peer that was asked is not forgotten.** While it is connected, it
  is never taken for a peer that was not asked. It is the syncing peer,
  or among those passed over, until it delivers or the timeout runs out.
  `forget` is for a peer that disconnected. `finish` is for one that is
  still connected, and keeps what it owes.
- **Once each.** A peer owes each block once, and a block is wanted once.

A reader of the database never sees half a block: a batch is one write,
and the web server reads through a snapshot that the block store's
thread takes right after the write (`noteBest`).

## The path of a transaction into the pool

The block store has nothing to do with the transactions that peers
announce and send. One thread, the runner (`Haskoin.Store.Ingest.Run`),
owns one machine, `Haskoin.Store.Ingest`. The runner gives the machine
everything that happens, in the order of one queue:

- a peer that comes or goes;
- what a peer announces, a transaction, a `notfound`;
- the clock, and whether the store is in sync;
- the answers of the pool to what the machine asked.

The machine decides. The runner carries out.

The runner can also be given a feed (`ingestFeed`): something that is
no peer and that a whole mempool can be read from, with a thread of its
own. The store starts none since it has the node of its RPC as a peer
(`Haskoin.Store.NodePeer`). To the machine a feed is a peer. It comes
when it is started, goes when a call fails, and comes again, as a new
peer, when it is tried again. The feed decides nothing either: it makes the
calls that the machine asked for, and puts the answers in the queue of
events.

[mempool-ingestion.md](mempool-ingestion.md) says what the machine
guarantees, and whether each guarantee is proved, checked or tested.

| State | Owner | Rule |
| --- | --- | --- |
| the machine: where each id is, and the window of each peer (the most requests out to it) | the runner | single owner; every change is `step` |
| the queue of events | written by the thread that reads the node's events, read by the runner | events of peers are dropped past 20,000, and the machine is told that something was lost |
| `state`, what `/health` reads | written by the runner after each step | one STM transaction per write |
| what is asked of the RPC of the node | written by the runner, read by the feed | a queue for each time the RPC is reached, named in the event that tells the machine of it; what the feed answers goes into the queue of events, and waits for room there |
| the number of the next feed | the feed | read and added to in the STM transaction that tells the machine of the feed, so no number is used twice (U1) |

## The fleet

Every store is a member of a fleet, alone or not. The fleet keeps in
Redis the tip, the pool, the leases and reports, the cache and the
events. All of it is written only in single steps, and each step reads
again what it depends on.

| What is written | By what |
| --- | --- |
| the pool, its events, and the move of the tip with the pool | the decisions of `Haskoin.Store.Fleet.Pool.Decide` |
| the cache | the decisions of `Haskoin.Store.Fleet.XPub.Decide` |
| the reports, and the move of the tip alone | two short Lua scripts of `src/Haskoin/Store/Fleet/lua` |
| only the note that an entry of the cache was used | the reads of the pool and of the cache, which are Lua scripts too |

The model of the fleet is `Haskoin.Store.Fleet` (which block the stores
agree on, leases, who may empty the pool) and `Haskoin.Store.Fleet.Pool`
(the pool). `RedisSpec` runs what is done in Redis against it, step by
step.

### Decisions in one step

**The problem.** The changes to the pool were one Lua script of eight
hundred lines. Redis ran it alone, which made each change one step. It
had two faults:

- **Redis undoes nothing.** A script that fails after its first write
  keeps what it wrote. A field that was not there, or a number that
  Redis did not take, left the pool half changed.
- **The rules were kept twice**, in Lua to run and in Haskell to test
  against, joined only by tests on random sequences. A bug of
  19 November was in that gap.

**What is done now.** A change is a decision
(`Haskoin.Store.Fleet.Atomic`):

1. The decision runs in Haskell. It can only ask Redis questions and
   add to a list of writes. It runs in any monad, so its type says that
   it does nothing else. Every question is kept with its answer.
2. The script (`lua/atomic.lua`) is given the questions with their
   answers, and the writes. It asks every question again.
3. If one answer differs, the script writes nothing and says so. The
   decision is refused, and is made again.
4. If all are the same, the script carries out the writes.

For example, the offer of a transaction asks "is the tip still block
B?" and "does the pool spend this coin already?", and lists the writes
that put the transaction in. If the tip moves before the script runs,
the first answer differs: nothing is written, and the offer is decided
again.

**Why that is one step.** Redis runs the script alone (R1 of
[mempool-ingestion.md](mempool-ingestion.md)). So the answers are all
true at one moment, that of the script. The decision depends on its
answers and on nothing else. So the writes are what the decision would
have written, had it run inside Redis at that moment.

**Why it cannot fail half way.** The script compares every answer before
its first write, and then runs plain commands with nothing to decide
between them. A fault in a decision is an exception in Haskell, before
Redis is told anything.

**What it rests on**, and how each part is known:

| Part | How it is known |
| --- | --- |
| A decision does nothing but ask and write | By its type: it is run as `forall m. Monad m => Atomic m a`. |
| The script compares before it writes, and decides nothing | By reading it: a hundred lines, with no rule of the fleet in them. |
| A decision sees its own writes as the model of Redis says them (`replay`) | What a write does to one cell is written once, in `Haskoin.Store.Fleet.Atomic.Cells`, and both use it. **Proved** by Liquid Haskell (`proofs/`) for every cell function, key and list of writes: the cell of a key in the model after the writes is the answer that the decision works out from the cell before. The proof assumes four facts of `Data.Map`, and does not look into the arithmetic of `INCRBY`. Six bugs planted in it are each rejected. For all the fields of a hash and all the members of a set, the decision changes its answer by the function that changes the model. |
| The rest of what a decision sees of its own writes: the names of the fields of a hash, a range and a count of a sorted set, whether a key is there, and that a write goes to the map of its own kind | Not proved. **Sampled** on 5,000 random cases against the model of Redis, and **tested** on 300 against a Valkey. Where a decision cannot tell, it stops with an error and guesses nothing. |
| The model of Redis says what Redis says | Tested against a Valkey: any writes, then every question. |
| A decision whose answers changed writes nothing | Tested against a Valkey, with a set and a hash of 200 members in whatever order Redis lists them. |
| No change is lost when many decide at once | Tested: eight threads, four hundred additions to one number. |
| A decision has one clock, that of Redis when it was first asked, however long the decision takes | Tested in the model of Redis: Redis is asked three times, with a clock ten seconds later each time, and the decision reads the first. The test fails with a clock for each time. |
| A decision of any size is carried out | Tested against a Valkey: 280,000 writes, more words than Redis takes arguments in a command (1,048,576). Questions and writes go a thousand to an argument, packed, and the script unpacks one argument at a time. |
| The writes that are sent do what all the writes do | Writes that a later delete undoes are not sent (`tidy`): sampled on 5,000 random cases in the model of Redis. The rest become commands with many members: tested against a Valkey on 300 random lists of writes, and by the two comparisons with the old Lua scripts, below. |
| No write fails in Redis for the kind of its key | By type. A key has its kind in its type (`Haskoin.Store.Fleet.Atomic.Keys`), given once where the key is named, and each write takes keys of one kind. A hash written to a set, or an addition to what is no number, does not compile; three such writes were tried. The decisions of the pool and of the cache import that module, and not the writes without kinds. A key that both write is named once: the cache takes it from the record of the pool. |
| The decisions of the pool do what the old Lua script of the pool did | Sampled: the old script and the decisions side by side, under two prefixes of one Valkey. 300 random sequences of offers, withdrawals, expiries and moves of the tip of each kind, with all that Redis holds compared after every step (`Haskoin.Store.Fleet.DecideSpec`). |
| The decisions of the cache do what its old Lua script did | Sampled the same way: 300 random sequences of builds, of moves begun, written in pieces, given up, gone quiet and committed, and of entries dropped and evicted, with the pool changing under them (`Haskoin.Store.Fleet.XPubDecideSpec`). |

One row is proved. The rest is by type, by reading, sampled or tested.
What changed is where a fault can be: in a pure function, where it is an
exception before any write, and no longer in the middle of a script.

**What it costs.** Two calls to Redis where there was one:

| | Old Lua script | Decision |
| --- | --- | --- |
| An offer that is taken (Valkey's own count, 2,000 offers) | 1 call, 74 microseconds of Redis | 2 calls, about 137 |
| A build of an xpub with 20 addresses and 300 rows | 1 call, 1.0 milliseconds of Redis | 2 calls, about 1.5 |

A decision takes more than one pass, to learn what it asks. So Redis is
asked in a few calls, and not once for each question.

**One clock.** A decision judges time by one clock, read at its start.
With the clock read again at each call to Redis, a move that took ten
seconds to decide judged the ten-second lease of its own store ten
seconds late: it found no store live and gave up, at every turn
(item 17). An early clock is safe for what stays true: a lease that had
run out has run out, and one that had not is what a script that ran at
the start would have found.

**One change to the cache.** A move whose store wrote nothing for ten
seconds is ended by whichever decision comes across it. A read ended it
too, and no longer does: to a read, an entry that the pieces of such a
move wrote to is a miss, and the build that follows the miss ends the
move.

**Two fleets on one Redis.** The keys of stores that keep the pool in
this way start with `hs3:`, and those of stores that ran the old script
with `hs2:`. On one Redis they are two fleets.

### The floor: a long decision gets its turn

**The problem.** A decision is carried out only if nothing that it read
has changed. That makes it right, and does not make it happen. A short
decision is refused now and then, and is made again. A long one can be
refused every time.

The long one is a move that removes the transactions of a block from the
pool. It removes thousands of them, reads the answers to tens of
thousands of questions, and takes a second or more. Transactions are
offered several times a second, and each offer changes an answer or two.
So on a network with traffic the move was refused at every try.

That happened on a server on 22 November 2026, at the first block that a
store of this design met on Bitcoin. The store imported every block and
went on answering from the one before, while its health, which looks at
the database, said that all was well. Nothing had shown it: Redis ran
the old Lua script alone, and the tests moved the tip over a dozen
transactions with nothing else writing.

**The rule.** The mover has the floor, as it already had for the cache
(`Haskoin.Store.Fleet.XPub.Decide.begin`):

1. The mover writes its token to `fx:moving`, and the time beside it,
   to `fx:beat`.
2. It writes the time again every two seconds, for as long as it works
   at the move (`beat`, from `moveTip`).
3. Whatever else writes gives way while that time is less than ten
   seconds old (`Haskoin.Store.Fleet.Pool.Decide.floorHeld`).

| Decision | While another store has the floor |
| --- | --- |
| an offer | is late, and is made again by the path into the pool |
| a withdrawal, an expiry | removes nothing; the next turn does |
| a build of an entry of the cache | is busy, as before |
| a drop, an eviction | leaves the entries; whoever asked finds them there |
| another move of the tip | does not begin |

Each of those reads `fx:moving`. So one that was decided before the
floor was taken, and would be carried out after, is refused by the
script and decided again. From the moment the token is written, nothing
but the mover changes what the move reads.

Two things still write under a floor, and the move does not read them in
a way that they change. A store renews its lease at every turn: the
move asks which leases still run, and not when each runs out (`HAbove`),
so a renewal is the same answer. And a read of the cache notes that an
xpub was used, in a list that the move does not read.

**The floor lasts as long as the move**, and not ten seconds from when
it was taken. With the time written only at the beginning and at each
piece, a move that took more than ten seconds to decide lost the floor:
the offers that it had held back were taken, and it was refused
(item 17). So the beat is a key of its own, written from a thread of its
own:

- **The move reads the token and never the beat.** So a beat changes no
  answer of the move: the store cannot race itself.
- **The decisions of the mover** (`piece`, `abort`, `advance`) **ask
  only whether the token is theirs.** A move is its store's until
  another decision ends it, which takes the token away.
- **Everyone else reads both**, and ends a move whose store wrote no
  beat for ten seconds. A store that stops loses the floor ten seconds
  later.

**What a store says of its moves.** A move that is not over in ten
minutes has gone wrong, and its store stops the beat. A store whose move
is refused five turns in a row says so in its log, as a warning, with
how long the last took and which answer had changed. One whose move took
a second or more says how long, since transactions wait meanwhile. Both
say where the time went: the blocks read from the database, the cache
read and written, the decision with how much of it was calls to Redis,
the call that carries it out, and how long the garbage collector ran
meanwhile. What would shorten a move depends on which of those is long,
and that is measured on the server, not guessed.

**The cost grows with the block, not with its square.** A decision sees
its own writes. For a question about one cell it goes only through the
writes to that cell (`Haskoin.Store.Fleet.Atomic.Cells.skipOthers`,
proved). Going through every write to the key grew with the square of
the block.

**The block is gone through once, whatever its size.** A decision
starts again from its top each time Redis is asked, and what the move
asks comes in steps that each need the answers of the one before: which
transactions are in the pool, what each does at its addresses, which
cached xpubs have those addresses. Left to find those as it went, the
move went through the whole block five times, and on a server nine.
Three of those times were for a few hundred questions about the children
of its transactions: whether each could still be replaced by fee. The
pool no longer works that out: on Bitcoin every unconfirmed transaction
can be, and on Bitcoin Cash none.

So the move names what it will ask before it starts, in those steps
(`Haskoin.Store.Fleet.Pool.Decide.foreseen`), and leaves at once a pass
in which Redis has not answered all of it
(`Haskoin.Store.Fleet.Atomic.ahead`). A step then costs a call to Redis,
and not a pass over the block. Naming a question changes nothing of what
is decided: a decision depends on a question only by asking it. A
question of the move that is not named costs a pass over the block
again, which a test counts.

`haskoin-store-bench decide` (`bench/Decide.hs`) measures where the time
of deciding goes, without Redis. For a block of 4,000 transactions on a
desktop:

| | Passes | Through the block | Deciding | Collector |
| --- | --- | --- | --- | --- |
| Finding the questions as it went | 8 | 5 | 4.4 s | 1.8 s |
| Naming them ahead | 5 | 1 | 1.3 s | 0.4 s |
| And not working out which can be replaced by fee | 4 | 1 | 1.1 s | 0.3 s |
| The same block in conflict with 60 transactions of the pool, found as it went | 9 | 6 | 5.4 s | |
| And named ahead | 6 | 1 | 1.2 s | |

The last two rows are what a server showed once the rest was in place:
of four blocks, two moved in under five seconds, and two took nine and
fifteen, asking Redis seven times. A block of Bitcoin confirms
transactions that the pool never had, and that spend what transactions
of the pool spend. Those of the pool leave, with all that spends from
them, and each step of that was found by going through the block again.

The first two rows have the same answers and writes, 103,741 and
212,540, and the third 94,409 and 205,207. A pass costs about a
microsecond and a kilobyte allocated for each of them, and a server took
four times as long for each. Compiling the decisions specialised to the
monad that runs them was tried, and changed nothing: the cost was in how
many passes there were, and is now in what one pass does.

How this is known:

| Claim | How it is known |
| --- | --- |
| While its store keeps the beat, no other decision changes an answer that the move depends on, however long that lasts. Neither does the beat | Sampled in the model of Redis, 2,000 random cases: a pool, a move decided, then any mix of offers, withdrawals, expiries, trims, builds, drops, evictions, another move begun, written and given up, and a lease renewed. Each comes seven seconds after the one before, with a beat a second before it. Every answer of the move is the same after them. With the floor taken and no beat after, the same property fails, and it fails without the floor: both are sampled too. |
| A move that is carried out twelve seconds after it was decided is carried out, with leases of ten seconds | Tested against a Valkey: a move that removes 200 transactions from the pool, offers that spend from them every 5 ms, a beat every two seconds. With no beat the same move is refused, which is tested too. |
| The move goes through its block once, whatever the size | Tested against a Valkey. Redis is asked three times for 40 unrelated transactions and for 400. It is asked five times when a quarter have children in the pool, a cached xpub has one of their addresses, and the block is in conflict with ten transactions of the pool that have children; nine times without the conflicts named ahead. The test fails when one question of the move is not named ahead. That naming changes nothing of what is decided is by construction, and the two comparisons with the old Lua scripts hold it to that. |
| A move over a full block is carried out at its first try while others write | Tested against a Valkey: a move that removes 800 transactions from the pool, with offers that spend from them, another store's reports, and builds at their addresses, every 5 ms. It fails if nobody gives way, and if the move depends on when a lease runs out. |
| A store answers from each new block while its node never stops taking transactions | Tested against Bitcoin Core and Bitcoin Cash Node in Podman (`Haskoin.Store.MempoolSpec`): a block that confirms 2,400 transactions of the pool while about sixty a second keep coming. It fails without the floor. |

Not known:

- **That a move always wins within a bound** when the stores of a fleet
  report different blocks at once. That changes the agreed block, and
  so, rightly, has the move decided again.
- **That the member keeps the beat while it works** is tested only where
  a move takes about a second (`Haskoin.Store.MempoolSpec`). In the test
  of the long move, the test writes the beat.
- **How long a move takes on a server** is not measured here. The tests
  run without optimisation. There, a move over 5,400 transactions of
  three inputs and three outputs takes about eight seconds, and one over
  9,000 about thirteen, both carried out at the first try under offers.
  Transactions wait for that long at each block.

### What the fleet relies on

| | Invariant | Why it holds |
| --- | --- | --- |
| I1 | The tip, the pool and the cache's tip change only together | one decision, `advance`, carried out in one step |
| I2 | Each input of a transaction of the pool spends an output of the pool, or one unspent at the tip. An output has one spender. Every unconfirmed parent of a transaction of the pool is in the pool | the offer reads the tip that it was judged at, in the same step |
| I3 | The tip moves only to a block that every live store has a view of | `report.lua` takes a report only from a store with a view of the tip, or alone; `advance` reads the agreed block |
| I4 | The pool is emptied only when no live store could say the way | `advance` empties it for a way that the caller does not know only if the caller is the one live store (`mayEmptyAlone`), and only a store whose report was taken moves the tip |
| I5 | A store keeps a view of the tip while it is live | `prune` decides what to keep in the STM transaction that lets views go, and a block joins the recent ones before its view is taken |
| I6 | Whether a transaction that a node announces is fetched is whether the pool has it | the stores ask the pool alone (`fleetSettled`). A set of what the fleet had seen is no longer kept: a transaction that the pool let go and a node took again stayed out ([mempool-ingestion.md](mempool-ingestion.md), T7) |

### The member's own state

| State | Who changes it | Rule |
| --- | --- | --- |
| `views`, with `recent`, `reported`, `tip` | the member and the block store both prune | one STM transaction per change (`keepViews`) |
| `built`, announcements of builds | the listener and the requests that wait | one STM transaction per change; an xpub is in it only while a request waits |
| `healthy`, `owed`, `newBest`: what wakes the member | set by the web server's health check (`setHealthy`) and the block store (`noteBest`); cleared by the member | `memberLoop` reads them and clears what its turn sees to in one STM transaction, so a signal that comes during a turn wakes the next |

When the member takes a turn is a pure function,
`Haskoin.Store.Fleet.turn`: none while the store is unwell, one at a new
block, one every two seconds, and one as soon as the store is well
again. `FleetSpec` replays any sequence of blocks, health changes and
timers through it, and holds that a well store leaves nothing waiting. A
turn offers no transaction. The path into the pool does, each as it
comes (`memberOffer`).

### After the move: the pool lets go

A move over new blocks alone does not take out of the pool what the
blocks confirmed or made invalid. It marks those transactions, and they
are taken out afterwards, a few at a time. [tip-first.md](tip-first.md)
walks through it, and [fleet-mempool.md](fleet-mempool.md), "The tip
first", has the rest. This section says who owns what.

So the common move reads a fifth of what it read, and writes a
twentieth. The long decision that the floor is for is now the move that
undoes a block, which still removes everything in its own step.

**Who owns what**, between the move and the last transaction out:

- **The move writes the marks** (`pool:leaving`), in the step that moves
  the tip, and only when there are none. A move that finds the marks of
  the move before it waits (`Leaving`).
- **Only taking out removes a mark** (`takeOut`), in the step that takes
  its transaction out. Anyone may do it, for any marked transactions:
  the store that moved the tip, another store at its turn, a request
  that has to read one. It does nothing for a transaction that is not
  marked. So two stores that take the same transaction out take it out
  once: the second finds no mark.
- **No write gets by a mark.** Every key of the pool and of the cache
  is named with the key of the marks (`guarded`, in `poolKeys`), and
  every write to such a key asks first whether Redis has a mark
  (`guarding`). With a mark, the decision stops before it writes.
  Without one, it depends on there being none, as on any answer. Only
  the taking out and the emptying of the pool write among marks
  (`unguarded`). This is by construction: a decision has no other
  function to write with.
- **Every other writer gives way** (`leavingHeld`). Whatever else
  writes to the pool or to the cache asks whether there is a mark, and
  does nothing if there is: the offer, the withdrawal, the expiry, the
  build, the drop, the eviction, and the next move. The next move waits
  from its first step: the move of the cache that comes before it does
  not begin. Each is carried out only if that answer still holds, as
  with any answer of a decision.
- **A read goes by no marked transaction** (`read.lua`, `xpub.lua`). It
  names the marked transactions in its way, they are taken out, and it
  reads again.

**Why no order of events breaks it.**

1. Between the move and the last transaction out, the only writes to the
   pool and to the cache are takings out.
2. Each takes whole transactions out, exactly as the move removed them
   when it did so itself, and the order does not matter.
   `Haskoin.Store.Fleet.DecideSpec` holds this to the old Lua script,
   state by state, with the order and the batches changing.
3. A read sees the pool before a taking out or after it, never half of
   one. And what it reads is written only by transactions that it has
   had taken out (tip-first.md, "Why a reader only has to look at its
   own addresses").
4. A store that stops leaves marks, and the next turn of any store takes
   them out. Nothing waits for a store that is gone.

**How it is known.** That no write gets by a mark: by construction.
That any order of taking out leaves the same pool: proved, for the pool
as a pure value (`anyOrder` in `Haskoin.Store.Fleet.Pool.Order`). For
the pool in Redis: checked in small worlds, sampled and tested
(tip-first.md, "How it is known to be right", has the sizes and the
limits of each).
The checker (`scripts/check-pool.sh`) goes through every pool, every
block and every order of taking out of its worlds, with the decisions
themselves. In every state it checks that writers wait, that every read
is exact, and that any order ends as the move in one step. The
decisions also: random histories, sampled in the model of Redis and
tested against a Redis. In
the model, each of the other writers is tried between two takings out,
and the property fails when one of them is made not to wait. The reads
of the pool: random histories against a Redis, tested. The reads of the
cache: two cases, tested.

The order does matter to one thing that nobody reads: a count can end as
a zero in one order and as nothing in another
([fleet-mempool.md](fleet-mempool.md), "How it is tested").

## The web server

The web server answers each request in a thread of its own. What those
threads share:

| Protocol | Machine | Rule |
| --- | --- | --- |
| The xpub indexer: jobs, queue, waiters | `Haskoin.Store.XPub.Indexer`, one `queue` TVar | one STM transaction per change; a reservation is undone if its request is interrupted |
| Admission: slots and line | `Haskoin.Store.Web.Admission` | one STM transaction per change, masked |
| Events: the log and each client's place | `Haskoin.Store.Web.Events` | append-only log; a client's start is taken before its filters |
| Asking for published transactions | `Haskoin.Store.Web.Publish.due` and `asked` | each transaction's schedule moves on once it was asked for |
| Whether Redis answers (`cacheUp`) | `reach` | three slow answers in a row clear it, any quick one sets it |
| Background loops | `supervised` | a loop that fails is logged and started again |

## What the audit found, and what changed

Each item is a rule that the audit of November 2026 left, and the fault
that the rule prevents.

1. **A peer that still owes blocks is kept when the store comes in
   sync**, among the peers passed over (`Sync.finish`). Forgotten with
   what it owed, its late copy of a block looked unrequested, and a good
   peer was disconnected.
2. **The in-sync check goes through the block store's mailbox.** It ends
   the syncing peer's turn, and the refill ran it on the web server's
   thread.
3. **`request` looks at what a peer owes.** After `forgetRequests` a
   peer could owe a block twice, and be disconnected for the copy that
   it no longer owed.
4. **A block is imported only if it is on the node's main chain, and a
   revert publishes the new best block.** A block of an abandoned branch
   that arrived after the node moved to another chain was imported, and
   the fleet moved to it until the store reverted it.
5. **The lists that pruning goes by are read in the STM transaction that
   lets views go.** Two threads that pruned from lists read earlier
   could let go of the view of the newest block.
6. **Only the one live store may empty the pool, and only a store whose
   report was taken moves the tip.** The old Lua script counted the live
   stores, not which: a store that was not live could empty the pool
   while one other store was live.
7. **A store that cannot read the blocks of a reorganisation waits**
   (`Unreadable`), until the way is too long for every store. It emptied
   the pool, where the documentation said that it waits.
8. **Expiry chooses and withdraws in one step** (`poolExpire`). Chosen
   in a step before, a transaction that had just come back could be
   withdrawn.
9. **`ask` runs masked and undoes its reservation.** A request that was
   interrupted while it took the lock of an xpub left its job reserved
   for good, and every later request for that xpub waited in vain.
10. **The listener of announcements backs off while Redis is down,
    counts only xpubs that requests wait for, wakes nobody at a
    reconnection, and announces only builds that are in the cache**; a
    build that is not fails its job. It spun with no pause, its map grew
    without bound, and a reconnection or an abandoned build woke waiting
    requests as if a build were done, which made them answer 503.
11. **It takes three slow answers from Redis in a row for a store to
    answer 503.** One made every store answer 503 for up to five
    seconds.
12. **Every background loop of the web server is supervised, and the
    read of the newest transaction has a timeout.** Loops could die in
    silence, and the health check could hang on a connection to Redis
    that hung.
13. **The schedule of each published transaction moves on once it was
    asked for.** An error while asking for one skipped the others, and
    could drop one at its last delay.
14. **The member waits on all its signals in one STM transaction
    (`Fleet.turn`), and takes a whole turn when the store is well again.**
    A store that a block made well took no turn for up to two seconds: the
    member woke at the block, read a health that the check had not updated
    yet, and slept until its timer.
15. **The path of a transaction into the pool is one machine,
    `Haskoin.Store.Ingest`, with a statement of what it must do**
    ([mempool-ingestion.md](mempool-ingestion.md)). It replaced five
    separate stages, each of which lost transactions in its own way: the
    queue of announcements, the set of requests, the member's list of what
    waits, the offer at each turn, and the question to the peers at the
    start.
16. **The mover has the floor, and what else writes gives way** ("The
    floor", above). With transactions offered all the time, a move was
    refused for ever. A test moves the tip while others write, as no
    test did.
17. **A decision has one clock, the mover keeps the floor for as long as
    it works, questions and writes go a thousand to an argument, and a
    store whose move is refused says which answer had changed.** A move
    over a full block was still refused at every turn, for three faults
    of a move that takes ten seconds. Two are told above ("One clock",
    "The floor lasts as long as the move"). The third: the move went to
    Redis as one command of 1,008,352 arguments, four in a hundred under
    the most that Redis takes, and a block a little larger would have
    closed the connection. No test had seen them: the tests moved 800
    small transactions in under a second, with leases of a minute. Each
    now has a test at the time or the size that it failed at.

### Known limits, left as they are

- **Redis that fails over to a replica that lags**, or restarts from an
  old snapshot, keeps its epoch, the number by which stores notice that
  Redis lost its data. So the stores do not notice, and what was offered
  since is gone from the pool. The pool fills again from new
  announcements and from the next reading of the nodes' mempools. Run
  Redis without replicas that can be promoted behind, or flush it, which
  the stores notice.
- **The stream of events keeps 10,000 entries.** A listener further
  behind loses what was trimmed, without an error.
- **A store that stops for more than ten seconds in the middle of a
  move**, as a machine that swaps can, loses the floor. Its move can be
  ended by another store, or refused. That costs time, never the state:
  the last decision of the move reads the token and every answer.
- **Transactions wait after a block**, while its transactions are taken
  out of the pool: one to two seconds for a full block on a server. The
  move of the tip itself is under a second. A move that undoes a block
  still takes seconds, and transactions wait for all of it.
- **The tip follows its slowest healthy store**, whose own node may be
  hours behind the network.
- **`fleetMove` sets the tip without the pool.** It is for the tests.
  The stores move the tip with `fleetCommit` only.

## Checking a change

- **A change to a machine changes its spec.** Add the property that the
  change keeps.
- **A new piece of shared state** names its owner and its rule in the
  tables above.
- **A change to a Lua script** is tested against Redis (the
  `Haskoin.Store.Fleet.*` specs), and the model of the fleet changes
  with it.
- **A change to a decision of the pool** is tested against the model of
  the fleet (`RedisSpec`), and one to a decision of the cache against
  the stores that read it (`CacheSpec`). `DecideSpec` or
  `XPubDecideSpec` then fails, since the old Lua script that it compares
  with has not changed. Say there what differs and why, or drop the
  comparison once the old script is no longer what the stores should do.
- **A new question in the move** is named in `foreseen`, or the move of
  a full block goes through the block once more for it. `asks Redis no
  more times` in `DecideSpec` fails then, and
  `haskoin-store-bench decide` says what it costs.
- **A new key of the pool or of the cache** is named in its record of
  keys, with its kind. A decision imports
  `Haskoin.Store.Fleet.Atomic.Keys`, never the writes of
  `Haskoin.Store.Fleet.Atomic`.
