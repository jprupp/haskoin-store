# The shared mempool of a fleet

How the servers of a fleet keep one mempool in Redis, and whether the
design holds. Written on 2026-11-11, against version 1.9.1. It describes
the code; it is not part of the package.

## The short answer

The design is sound for what it promises: every server answers from the
same mempool, that mempool is valid on top of the block the fleet answers
from, and the two always change together. Each rule is a Lua script that
Redis runs in one step, and the tests hold the scripts to a pure model.

It has weak points, listed at the end. One is a real gap: **if Redis loses
its data, the servers do not offer their mempools again**, and unconfirmed
transactions disappear from every answer until they confirm.

## The problem

Each server has its own mempool, filled in the order its peers sent
transactions. No two are alike. Servers that answer from the same block
still disagree if each lays its own mempool on top: one lists a payment,
the next does not.

So the fleet keeps one pool in Redis, and every server answers from it.
A server's own mempool stays. It is what the server offers to the pool,
what it relays to peers, and what it answers from when Redis is down.

## What Redis holds

- **The fleet tip**: the block that every server answers from.
- **The pool**: the transactions, with what each spends, what spends each
  output, the children of each transaction, its body, the time it first
  entered a mempool, and, for every address, its transactions, its outputs
  and their sums.
- **Counters** next to the tip: how many times the tip moved, and how many
  times the pool was started again.
- **A stream of events**: what entered, what left, which blocks the tip
  went through.

The pool and the tip are written only by `pool.lua`, so one script call
changes them together or not at all.

## Getting in: the offer

Every two seconds each server takes a turn. In it, the server offers the
transactions of its mempool that it has not offered yet
(`offerMempool`).

1. The server reads the fleet tip, and opens its own view of its database
   at that block. A server without that view offers nothing.
2. For each input of the transaction it checks, in that view, whether the
   output is unspent at the tip. It sends one flag per input.
3. The script checks that the tip is still the one the server read. If
   not, the offer is late and the round stops.
4. The script refuses the transaction if:
   - an input spends an output that something in the pool already spends
     (a **conflict**), unless the server names that rival as replaced;
   - an input spends an output that is neither unspent at the tip (by the
     flag) nor made by a transaction of the pool (an **orphan**).
5. Otherwise it goes in, with the time the offering server first saw it,
   and an event says it entered.

What the server does with each answer:

| Answer | What the server does |
| --- | --- |
| Taken, or already known | Marks it offered, and never offers it again |
| Orphan | Keeps it waiting, and tries again next round |
| Conflict | Offers it again naming the rivals as replaced, if it deleted all of them itself; else waits |
| Late | Stops; the next round reads the whole mempool |

**Replacement by fee** works through the conflict rule. A server's node
replaces X with Y by its own rules and deletes X. When the server offers Y
and the pool still has X, the server names X as replaced, and the script
takes X out with everything that spends it, then puts Y in. A server that
never saw Y cannot do this, so X stays. The first of two conflicting
transactions wins unless a server that saw both says otherwise.

A server **ahead of the tip** may have transactions that spend outputs of
its newer blocks. Their flags say "not unspent at the tip", so they come
back as orphans and wait until the tip catches up.

## Getting out

A transaction leaves the pool only for a reason:

- it was replaced (above);
- it is older than the mempool timeout, counted on the pool's own times
  (`expirePool`, run by every server each turn);
- the tip moved to a block that confirms it, or that spends what it spends.

Whatever leaves takes along everything that spends it, since those would
spend outputs that no longer exist. A confirmed transaction leaves alone:
what spends it now spends a confirmed output.

A server **does not withdraw** what it deletes from its own mempool. It may
simply not have a transaction that another server has.

## When the tip moves

A server whose turn finds that all live servers have a newer block in
common moves the tip there (`moveTip`). It reads from its own database
what the blocks between the old tip and the new one did, and hands the
script all of it in one call:

- the transactions they confirmed, which leave quietly;
- the outputs they spent, whose spenders in the pool leave with their
  descendants;
- across a reorganisation, the transactions of the abandoned blocks, which
  come back into the pool, and those that cannot (coinbases, and any the
  server no longer has), whose spenders leave.

The script checks that the tip is still the one the server read, and that
the new block is still the one the live servers agree on. Then it changes
the pool, moves the tip, counts the move, and tells the blocks as events.
Two servers that try at once do it once: the second finds the tip changed.

### What the blocks replaced

The pool also keeps what the blocks replaced, so that every server answers
410 alike for a transaction that lost to a transaction of a block. A
server's own record of it depends on its own mempool, and is not used in a
fleet.

- **A transaction of the pool** that leaves because it, or an ancestor,
  spent an output that the new blocks spend is kept with the transaction of
  the block that spent it, and the height of its block. The server gives
  both with each output (`pool:rep`, with `pool:repby:` by winner and
  `pool:repat` by height).
- **A payment of an abandoned block** that a new block replaced is not in
  the pool, and the server no longer has it: it has imported the new blocks
  and deleted it. Every server had it confirmed and keeps the same record
  of it, so the server that moves the tip names it, and it waits
  (`pool:lost`) until a move confirms the transaction that replaced it.
  The tip may stop at the fork before it goes on, when the servers reach
  the new blocks at different times; the test of the fleet ran into that.
- **It is forgotten** when the winner comes back from an abandoned block,
  2,016 blocks after its height, and when the pool starts again.

Not kept: a transaction of the pool replaced by fee, or withdrawn for its
age. Those answer 404.

## When nobody can say what the blocks did

A server goes through 50 blocks at most. Two cases are past that:

- **It does not have the old tip** at all. It may then empty the pool and
  move the tip, but only when no other server is live: another might know.
- **The way is longer than 50 blocks.** That is true for every server, so
  any of them may empty the pool and move the tip. Before version 1.9.1
  only a server alone could, and two servers that came back together after
  a long outage stopped the fleet for good.

Either way the script adds one to the restart counter. Every server checks
that counter each turn (`noteRestarts`), and when it changed, forgets what
it offered and offers its whole mempool again, 2,000 transactions a turn.
Until those offers arrive the pool is empty or partly filled: a turn for a
small mempool, a minute or so for a large one.

## Reading

A request takes the fleet tip, opens the server's own view of its database
at that block, and lays the pool on top (`Haskoin.Store.Database.Query.Pooled`).
Every read of the pool names the tip, and `read.lua` refuses it if the tip
moved. The request is then answered again from the new tip. So no answer
mixes a block with a pool that belongs to another block.

A request is one block, but not one moment of the pool: a transaction can
enter between two reads of the same request. A balance and a list can
differ by it. A server alone has no such gap.

## Why it holds

These hold after every script call:

- **Valid at the tip.** Each input of each pooled transaction spends
  either an output made by another pooled transaction or an output that is
  unspent at the tip. An offer checks it; a tip move removes what the new
  blocks confirmed or spent, and what depended on what they abandoned.
- **No double spends.** At most one pooled transaction spends each output.
  The script checks the spender of each input inside the same call that
  inserts.
- **Closed under parents.** Removing a transaction removes its
  descendants, except when it was confirmed.
- **Tip and pool together.** Only `pool.lua` changes either, in one call,
  after checking the tip it was given. Reads check the tip too.

The check of the chain (the flags) happens in the server, outside Redis.
That is safe because the flags are about the chain at one block, which
never changes, and the script refuses the offer if the tip is no longer
that block.

## Weak points

**1. Redis losing data is not noticed. A real gap.** A server remembers
what it offered, and never offers it again unless it deletes it or sees the
restart counter change. If Redis is flushed, or fails over to a replica
that missed the last writes, the pool loses transactions and nothing tells
the servers. Those transactions vanish from every answer until they
confirm or expire. A flushed Redis also resets the restart counter to
zero, which a server that last saw zero does not notice. The design
document already says Redis must not lose writes; nothing checks it.
*Fix:* a random epoch in Redis, set if missing (`SET NX`). A server that
finds a different epoch from its last turn offers its whole mempool again,
as after a restart. A few lines in `noteRestarts`.

**2. Redis eviction would corrupt the pool.** With a memory limit and a
policy that evicts keys, Redis can drop one key of the pool and keep the
others: a spender without its transaction, sums that do not add up. The
pool has to live in a Redis with `maxmemory-policy noeviction`. Nothing
says so or checks it. *Fix:* check `CONFIG GET maxmemory-policy` at start
and refuse to join a fleet otherwise, or say it in the README.

**3. A restart is abrupt for clients.** When the pool is started again,
the events do not say that its transactions left, and only the new block
is told, not the ones skipped. A client of `/events` should read again
what it follows when it sees a block it cannot connect. The emptying uses
`KEYS`, which blocks Redis for as long as it walks the whole key space.
*Fix, if it matters:* keep the pool's keys in a set, or under one hash
tag, and delete those.

**4. One peer's transaction reaches every server.** The store does not
check scripts; it trusts its peers, as it always has. A server alone shows
a bad transaction from one of its peers only on that server. In a fleet it
reaches the pool and every server shows it, until it conflicts with a
block or expires (14 days by default). *Fix:* none needed if the servers
connect to nodes you run, which is the advice already.

**5. The pool keeps what the network dropped.** A transaction that nodes
evict for a low fee stays in the pool until it expires, as it does in a
server alone. Same behaviour, now shared.

**6. "First seen" is the pool's, not the miners'.** Of two conflicting
transactions, the pool keeps the first that any server offered, unless a
server that saw both replaced one. Miners may confirm the other. The pool
then corrects itself when that block comes. This is the same rule a node
uses, and cannot be better without knowing what miners will do.

**7. Small ones.**
- A server that expired a transaction earlier than the pool, by its own
  clock, can replace it in the pool with a conflicting one that other
  servers have not seen.
- A server that was restarted forgets what it deleted, and cannot replace
  anything in the pool it deleted before.
- A tip move across many large blocks is one long script call, which holds
  Redis while it runs. In normal running it is one block.
- Pool times are the offering servers' clocks. Skewed clocks skew the
  order of the mempool list by as much.

## How it is tested

- `Haskoin.Store.Fleet.PoolSpec`: the rules as a pure model, and a
  property that the pool stays valid on top of the chain whatever is
  offered, mined and unwound (3,000 random histories).
- `Haskoin.Store.Fleet.RedisSpec`: the scripts against a real Redis, step
  by step against the model, plus the emptying rules and the events.
- `Haskoin.Store.Fleet.MemberSpec`: a member through random histories of
  payments, replacements, blocks and reorganisations, each answer held to
  the server's own; and two members stuck past 50 blocks, which must move
  on and fill the pool again.
- `IntegrationSpec`, the fleet test: two servers against a real node and
  Redis, answering alike with a payment in the pool and after its block.

Not tested: Redis losing data, Redis evicting keys, more than two servers,
and a pool of real size.

## Where the code is

| Part | Code |
| --- | --- |
| The rules, as pure functions | `src/Haskoin/Store/Fleet/Pool.hs`, `src/Haskoin/Store/Fleet.hs` |
| Offer, withdraw, move the tip | `src/Haskoin/Store/Fleet/lua/pool.lua` |
| Reads that check the tip | `src/Haskoin/Store/Fleet/lua/read.lua` |
| Calling the scripts | `src/Haskoin/Store/Fleet/Redis.hs` |
| Offering, moving, restarting, expiring | `src/Haskoin/Store/Fleet/Member.hs` |
| Answering from view and pool | `src/Haskoin/Store/Database/Query/Pooled.hs` |
