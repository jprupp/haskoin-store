# The tip first

How a block's transactions leave the shared mempool, in plain terms. This
is the page to read first about what happens at a block.
[fleet-mempool.md](fleet-mempool.md) has the whole pool, and
[concurrency.md](concurrency.md) says who may write what.

## The setting

A store (a server of haskoin-store) keeps two things:

- **the chain**, in its own database: every block up to the newest it has;
- **the pool**, in Redis: the transactions that no block has confirmed yet,
  shared by all the stores of a fleet.

Redis also holds **the tip**: the block that every request is answered
from. A request reads the chain as it was at the tip, and lays the pool on
top. So the tip is what the outside world sees as "the latest block".

## The problem

When a block arrives, the transactions that it confirmed have to leave the
pool. They are in the chain now, and counting them in both would show
every payment twice.

Taking one transaction out of the pool is more than deleting it. It is
taking it out of everything that mentions it:

- the list and the sums of each address it pays or spends from;
- the entry of every cached extended public key that has one of those
  addresses.

For a full block that is tens of thousands of questions to Redis and more
than a hundred thousand writes.

It used to be one step: the tip moved, and all of that happened, together
or not at all. That is simple and safe. But the tip waited for it:

- several seconds on a server at every full block;
- nearly one of them inside Redis, which answers nobody else while it
  carries one step out;
- more with every cached key.

## The idea

Move the tip at once. Only **mark** the transactions that have to leave.
Take them out afterwards, a few at a time.

## What happens, step by step

1. **A block arrives.** The store reads what the block did: which
   transactions it confirmed, and which coins it spent.

2. **One step in Redis moves the tip and marks.** In that single step:
   - each transaction of the pool that the block confirmed is marked;
   - each transaction of the pool that spends a coin the block spent
     another way is marked, with everything that spends from it;
   - the tip is set to the new block.

   From this moment every request is answered from the new block.

3. **The marked transactions are taken out**, two hundred to a step, by
   the store that moved the tip. Each leaves exactly as it left before,
   and its mark goes in the same step.

4. **When no mark is left**, the pool is as it was after the old single
   step.

## What is marked

Two kinds of transaction:

| Kind | Why it leaves |
| --- | --- |
| One that the block confirmed | it is in the chain now |
| One that the block made invalid: it spends a coin that a transaction of the block spent, or spends from one that does | it can never be confirmed |

The second kind matters most. The chain says where a confirmed
transaction went. It says nothing of one that became invalid: only the
mark does.

## The three rules that make it right

Between the move and the last transaction out, a marked transaction is
still in the pool. Nobody may see it.

**1. Writers wait.** While any mark exists, nothing else changes the pool:
a new transaction is told "late" and offered again, nothing is withdrawn
or expired, no cached key is built or dropped, and the next block's move
does not begin.

**2. Readers help.** A request that would read something a marked
transaction touches is told which transactions those are, and is given
nothing. The store takes them out at once, and reads again.

**3. Anyone may take out, in any order.** Taking out does nothing for a
transaction that is not marked. So two stores, or a store and a request,
never take one out twice. A store that stops leaves marks, and the next
store clears them at its next turn.

Together: every answer is that of a pool that has let go of everything,
whether or not it has yet.

## Why a reader only has to look at its own addresses

One fact about the data carries the whole design:

> What the pool keeps for an address is written only by the transactions
> at that address.

So the answer for an address can be wrong only if a marked transaction is
at that address. A read looks at the transactions of its own address,
finds the marked ones among them, has them taken out, and is then exact.
It does not need to wait for the rest of the block.

The same holds for a category of tokens, and for a cached key, whose entry
lists every transaction at its addresses.

## What does not use marks

A block that undoes another (a reorganisation). Transactions of the
abandoned block come back into the pool, and they must find the new
blocks' effects already applied. So that move still does everything in one
step, as all moves did.

## What it costs now

In a benchmark, for a block of 4,000 transactions out of a pool of 70,000:

| | One step | Mark, then take out |
| --- | --- | --- |
| Moving the tip | 0.8 s | 0.12 s |
| The same with a cached key at every address | 2.7 s | 0.12 s |
| Largest single call to Redis | about 130,000 writes | about 7,000 |

The taking out that follows takes about a second, in twenty steps of 65
milliseconds or less.

On a server, the same night, for full blocks:

| | Moving the tip |
| --- | --- |
| Before | 9 to 12 s |
| With the old step made as fast as it would go | 3.1 to 3.5 s |
| Mark, then take out | under 1 s, every block |

New transactions wait one to two seconds after a block, while its
transactions are taken out. Before, they waited for the whole move.

## Where it is in the code

| Piece | Where |
| --- | --- |
| The marks | the hash `pool:leaving` in Redis: one field for each marked transaction |
| The move that marks | `marked`, in `advance` of `Haskoin.Store.Fleet.Pool.Decide` |
| Taking out | `takeOut`, same module |
| "Is anything marked?", which every writer asks | `leavingHeld`, same module |
| The loop that takes out after a move | `clearLeaving` in `Haskoin.Store.Fleet.Member` |
| The check that a read makes | `lua/read.lua` and `lua/xpub.lua` in `src/Haskoin/Store/Fleet` |
| The benchmark | `haskoin-store-bench decide` (`bench/Decide.hs`) |

## How it is known to be right

It is **checked in small worlds, sampled and tested. It is not proved**
([README.md](README.md), "How sure we are").

**Checked.** A program goes through small worlds with the store's own
code (`scripts/check-pool.sh`, half a minute). A world is five or six
transactions that could be in a pool: some spend from others, two spend
the same coin, some move tokens, and cached keys may have their
addresses. In each world it tries:

- every pool: each set of those transactions, offered;
- every block: each set of them, confirmed;
- every order of taking out, one transaction at a time, and all at once.

That is 7,168 pools and blocks and 22,866 states in between. In every
one of them:

1. the move marks exactly what the old single step took out;
2. no writer changes anything while a transaction is marked: an offer, a
   withdrawal, an expiry, a build, a drop and an eviction of a cached
   key, the start of the next move's work on the cache, and the next
   move itself;
3. for each address, category, transaction, output and cached key: if
   no marked transaction is among those that its read goes by, what the
   read reads is already final; and taking out those that are makes it
   so;
4. with nothing marked, everything is as after the old single step, and
   the pool takes a new transaction again.

One limit: the checker says in Haskell what the two reading scripts read
and which transactions they name. That the scripts do the same is
sampled against a real Redis, below, not checked.

**Sampled and tested.**

| Check | Size |
| --- | --- |
| Mark and take out, in a random order and in random pieces, against the old single step, in a model of Redis. Others try to write in between, and must change nothing. (`marking` in `DecideSpec`) | 10,000 random histories once, 3,000 at every run |
| The same against a real Redis, held to the old script (`both` in `DecideSpec`) | 300 histories |
| Every kind of read from a pool that is only marked, against the same pool with everything out (`reading` in `DecideSpec`) | 300 histories |
| A cached key read right after a store moved the tip and stopped: once with a confirmed transaction, once with one made invalid (`leavingTest`, `conflictTest` in `CacheSpec`) | 2 cases |
| Planted faults: a read that does not look for marks, a cache that does not, a move that does not mark conflicts, and three writers that do not wait | all caught |

Every new rule comes with a broken version that its test must catch. A
check that has never failed is not known to work. The checker was run
with ten faults put in, one at a time: each of seven writers made not to
wait, and a read by address, by category and of a cached key made not
to look for marks. It failed on each. It also runs, every time, one
world in which what a block made invalid is left unmarked, and that
world must fail.

## What the tests found

The model found one thing that three hundred histories against Redis had
missed. A counter can end stored as "0" where the old single step left no
counter at all, depending on the order in which two related transactions
leave. Nothing reads the difference: every read takes a missing counter
for zero. The comparisons treat the two as equal, and the documents say
so.

## What it does not prove

- Nothing about a pool of thousands. The worlds are small, on the bet
  that a wrong rule already shows with five transactions. Thousands of
  random histories are a second line behind that bet, not a proof.
- Nothing about speed on a machine whose drive stalls. Small steps help
  there, since no single call holds Redis for long, but a frozen process
  is frozen.
