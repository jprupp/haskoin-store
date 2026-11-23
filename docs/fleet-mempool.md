# The mempool: one pool in Redis

How the stores of a fleet keep one mempool in Redis, and why the design
holds. The design around it is in [one-fleet.md](one-fleet.md). This page
describes the code; it is not part of the package.

## In short

Stores that answer from the same block must also answer from the same
mempool. So the fleet keeps one mempool in Redis, the pool, and no store
keeps one of its own, in RocksDB or in memory. The pool is always valid on
top of the tip, and the two change together: each change is a decision,
applied in one step or not at all ([concurrency.md](concurrency.md),
"Decisions in one step"). The cost: a store without Redis knows nothing of
the mempool, and is unhealthy. The tests hold the decisions to a pure
model, and to the Lua script that they replaced. The [weak
points](#weak-points) are near the end.

## Words used here

The shared words (store, fleet, the pool, the tip, view, decision, turn and
others) are in [README.md](README.md).

| Word | What it means here |
| --- | --- |
| **offer** | A store hands one transaction to the pool, with what it checked. |
| **flag** | Sent with an offer, one for each input: is the output that it spends unspent at the tip? |
| **taken**, **known** | Answers to an offer: the pool put the transaction in, or had it already. |
| **late** | Answer to an offer: not now. The tip moved, or the pool is busy with a move. The store offers again. |
| **conflict** | The transaction spends an output that a transaction of the pool already spends. |
| **rival** | A transaction of the pool that spends an output that another one also spends: one being offered, or one of a block. |
| **orphan** | A transaction that spends an output that is neither unspent at the tip nor made in the pool. A parent is missing. |
| **replaced** | Lost its output to another transaction: one offered in its place (by fee), or one of a block. |
| **withdraw** | Remove a transaction from the pool unconfirmed, with everything that spends from it. |
| **expire** | Withdraw a transaction that is older than the mempool timeout. |
| **marked** | Still in the pool after a move of the tip, but listed as leaving (`pool:leaving`). |
| **take out** | Remove a marked transaction from the pool and from all that mentions it (`takeOut`). |
| **settled** | The pool has the transaction, or a block at the tip confirms it. No store fetches it again. |
| **live** | A store whose report of its blocks was accepted in the last ten seconds. |
| **the floor** | The right of one store to change the pool alone while it moves the tip ([concurrency.md](concurrency.md), "The floor"). |
| **abandoned block** | A block that a reorganisation dropped from the chain. |
| **source** | A way to read a node's whole mempool: its RPC, or a peer whose node answers when asked. |
| **restart** | The pool is emptied and the tip moved, when no store can say what the blocks did. |
| **the epoch** | A random number in Redis (`fleet:epoch`). Gone or changed, it says that Redis lost its data. |

## What Redis holds

- **The tip** (`fleet:tip`): the block that every store answers from.
- **The pool**: the transactions, with what each spends, what spends each
  output, the children of each transaction, its body, the time it first
  entered a mempool, and, for every address, its transactions, its outputs
  and their sums.
- **The tokens of the pool** (CashTokens, on Bitcoin Cash): for each
  category, its transactions and the outputs that hold it, and for each
  address and category, the sums of a token balance. The store reads the
  tokens of each output and sends them with the transaction, so nothing in
  Redis reads a token prefix ([cashtokens.md](cashtokens.md)).
- **Two counters** next to the tip: how many times the tip moved
  (`fleet:moves`), and how many times the pool was restarted
  (`fleet:restarts`).
- **A stream of events** (`fleet:events`): what entered, what left, which
  blocks the tip went through.
- **No list of what the fleet saw.** A store checks an announcement against
  the pool alone, in one call (`ZMSCORE`), and against what it holds to
  offer ([mempool-ingestion.md](mempool-ingestion.md), T7). A list of the
  newest 100,000 transactions that the pool had decided on (`fleet:seen`)
  is no longer kept. A store fetched nothing that was in it, so a
  transaction that the pool had let go and that a node still held was never
  fetched again.

Only the decisions of the pool (`Haskoin.Store.Fleet.Pool.Decide`) write
the pool and the tip. Each is applied in one step: the two change together
or not at all, and a decision that fails has written nothing. While a store
moves the tip it has the floor: the offers of the others are late, and are
made again a moment after.

## Getting in: the offer

How a transaction gets from a peer to an offer is a machine of its own,
`Haskoin.Store.Ingest`, with a document of its own
([mempool-ingestion.md](mempool-ingestion.md)). In brief: the store asks
the pool about what a peer names, requests what the pool does not have, and
offers what arrives at once (`memberOffer`). An offer is of one
transaction. It comes after the offers of the transactions that this one
spends from and that the store is still getting.

The offer, in order:

1. The store takes the tip, and opens its own view of its database at that
   block. A store without that view offers nothing.
2. It reads each output that the transaction spends, from that view or from
   the pool. If one is in neither, the transaction is an orphan, and
   nothing is sent.
3. It sends one flag for each input: whether the output is unspent at the
   tip, in that view.
4. The decision (`offer`) answers **late** if the tip is no longer the one
   that the store read, if a store has the floor, or if a transaction is
   marked.
5. It answers **known** if the pool has the transaction.
6. It answers **conflict** if an input spends an output that a transaction
   of the pool already spends, unless the store names that rival as
   replaced.
7. It answers **orphan** if an input spends an output that is neither
   unspent at the tip (by the flag) nor made by a transaction of the pool.
8. Otherwise the transaction is **taken**. It goes in with the time at
   which the offering store first saw it, and an event says it entered.

What the store does with each answer:

| Answer | What the store does |
| --- | --- |
| Taken, or known | Lets it go. The pool has it, so no store fetches it again. |
| Orphan | Asks its peers for the transactions that it spends from and the pool lacks. Keeps it, and offers it again when the missing one is in the pool, or after two seconds, four, eight, for 20 minutes at most. |
| Conflict | Offers it again, naming the rivals as replaced: a peer relayed it, so its node took it in their place. If that offer is not decided either, as when a new rival came, it is offered again later. |
| Late | Stops, and offers it again later, at the new tip. |

**Replacement by fee** works through the conflict rule. For example, X is
in the pool and spends output A:0. A peer relays Y, which also spends A:0.
A peer relays Y only once its node took Y in place of X. The store offers Y
and is told of a conflict with X. It offers Y again, naming X as replaced.
The decision withdraws X, then puts Y in.

**A store ahead of the tip** may have transactions that spend outputs of
its newer blocks. Their flags say "not unspent at the tip". So they come
back as orphans, and wait until the tip catches up.

## Getting out

A transaction leaves the pool only for a reason:

- **It was replaced** by fee (above).
- **It expired**: it is older than the mempool timeout, counted on the
  pool's own times. Every store sees to that at each turn in which it did
  not move the tip (`expirePool`). One decision chooses the old
  transactions and withdraws them. Chosen in a step before, a transaction
  could leave, come back with a new time, and be withdrawn for its old one.
- **The tip moved** to a block that confirms it, or that spends what it
  spends.

Whatever leaves takes along everything that spends from it, since those
would spend outputs that no longer exist. A confirmed transaction leaves
alone: what spends from it now spends a confirmed output.

A store has no mempool of its own, so there is nothing for it to withdraw.
What it holds and has not offered yet is in the machine
(`Haskoin.Store.Ingest`), with the orphans. An orphan is let go 20 minutes
after the store had it, as Bitcoin Core lets go of its own: a parent that
has not come by then is not coming.

## When the tip moves

A store whose turn finds that all live stores have a newer block in common
moves the tip there (`moveTip`).

1. **It reads what the blocks did** between the old tip and the new one,
   from its views of its database:
   - the transactions that they confirmed, which leave quietly, with no
     event;
   - the outputs that they spent, whose rivals in the pool leave with
     everything that spends from them;
   - across a reorganisation, the transactions of the abandoned blocks,
     read from the view at the old tip. They come back into the pool.
     Coinbases cannot, and what spends from those leaves. A store without a
     view of the old tip does not move the tip: another store may have it.
2. **It hands all of it to one decision** (`advance`), in one call, with
   the xpub entries ([xpub-indexing.md](xpub-indexing.md)).
3. **The decision checks** that the tip is still the one that the store
   read, and that the new block is still the one that the live stores agree
   on. Then it changes the pool, moves the tip, counts the move, and tells
   the blocks as events.

Two stores that try at once move the tip once: the second finds the tip
changed.

For example, P is in the pool and spends output A:0. A block confirms Q,
which also spends A:0. P is a rival of Q and can never be confirmed. It
leaves, with everything that spends from it.

### The tip first, and the pool lets go after it

Nearly every move is over new blocks alone. Such a move takes nothing out
of the pool. It only marks what has to leave, and the tip is at the new
block. [tip-first.md](tip-first.md) walks through it, with the reasons, the
costs and the tests. In summary:

- **What is marked** (`pool:leaving`, one field for each transaction): a
  transaction that a block confirmed, with nothing in its field; and a
  rival of a transaction of a block, with everything that spends from it,
  each with the transaction that replaced it and the height.
- **Taking out** comes afterwards, a few at a time (`takeOut`, two hundred
  to a decision). Each transaction leaves as it left when the move did
  everything. The store that moved the tip does it, and so does any store
  at its next turn, for a store that stops in between.
- **Why.** Taking a full block's transactions out in the move made the tip
  wait: the tip of a server was five to fifteen seconds behind its node at
  every full block, and once, on a machine short of memory, Redis answered
  nobody for twenty-three seconds. Marking is one write for each
  transaction, decided in a tenth of the time whatever the number of cached
  xpubs, and no step of the taking out is long.

What makes it right is that nobody sees a marked transaction:

1. **Writers wait** (`leavingHeld`). While a transaction is marked, an
   offer is late, a withdrawal takes nothing, no entry of the cache is
   built, dropped or evicted, and the next move waits. The same things wait
   while a store has the floor.
2. **Readers help.** A read that would go by a marked transaction takes it
   out first ([Reading](#reading)). So every answer is that of a pool that
   has let go of everything.
3. **Anyone may take out, in any order.** A transaction that is no longer
   marked is left alone. So two stores, or a store and a request, take
   nothing out twice.

A move that undoes a block still takes everything out in its own step, as
all moves once did. A transaction that comes back to the pool has to find
what the new blocks did already done.

### What the blocks replaced

The pool also keeps what the blocks replaced, so that every store answers
410 alike for a transaction that lost to one of a block.

- **A transaction of the pool** leaves when it, or one that it spends from,
  spent an output that the new blocks spend. The pool keeps it with the
  transaction of the block that spent that output, and the height of its
  block. The store that moves the tip gives both with each output that the
  blocks spent (`pool:rep`, with `pool:repby:` by the transaction that
  replaced it and `pool:repat` by height).
- **A payment of an abandoned block** that a new block replaced is not in
  the pool, and the store no longer has it: it has imported the new blocks
  and deleted it. Every store had it confirmed, and keeps the same record
  of it. So the store that moves the tip names it, and it waits
  (`pool:lost`) until a move confirms the transaction that replaced it. The
  tip may stop at the fork before it goes on, when the stores reach the new
  blocks at different times.
- **It is forgotten** in three cases: the transaction that replaced it
  comes back from an abandoned block; the tip is 2,016 blocks past its
  height; the pool is restarted.

Not kept: a transaction of the pool that was replaced by fee, or that
expired. Those answer 404.

## When nobody can say what the blocks did

A store goes through 50 blocks at most. Two cases are past that:

- **It does not have the old tip** at all. It may then restart the pool and
  move the tip, but only when it is the one store that is live: another
  might know. A store whose report was refused is not live, and moves no
  tip at all. A reorganisation from a tip that it has but cannot read is
  not this case: it waits.
- **The way is longer than 50 blocks.** That is true for every store, so
  any of them may restart the pool and move the tip.

Either way the decision also empties the xpub cache, whose pool layer is
gone, and counts the restart (`fleet:restarts`). Each store sees the count
change at its turn (`noteRestarts`), and fills the pool again as after a
loss of Redis, below.

## When Redis loses its data

Redis keeps the epoch (`fleet:epoch`, set if missing). A store that finds
it gone or changed at its turn (`noteEpoch`) forgets its tip, and is
unhealthy until the fleet has a tip again. The first turn sets one, since
every live store reports. Then the store fills the pool again.

## Filling the pool again

A store reads the whole mempool of its sources, again and again. Its
sources are the RPC of its node, if it was given one (`--node-rpc`), and
the peers whose node gives its mempool when asked. It reads them:

- when a source connects, since a node announces a transaction to a peer
  once, and never one that it took before the peer connected;
- whenever it may have missed something, for want of room, of an answer or
  of Redis;
- when the pool lost what it had, after a loss of Redis or a restart;
- thirty seconds after a reading, if a peer named a transaction since
  then that the pool did not have; otherwise after twice the last wait,
  up to ten minutes ([mempool-ingestion.md](mempool-ingestion.md)).

The store asks the pool about each id first, and does not fetch what the
pool has. So reading a mempool that the pool already has costs the list of
its ids and no more. [mempool-ingestion.md](mempool-ingestion.md) has the
rules, and why they make every loss temporary.

**The RPC** (`getrawmempool`, then `getrawtransaction` in batches) always
answers its account, whatever the node allows its peers. The store asks it
as it asks a peer: for as many transactions at once as a window allows. The
window grows while the node answers in time, and halves when it does not. A
node that does not answer is tried again after a second, then two, up to a
minute. It is never taken for a node with an empty mempool.

**Which peers are sources.** Both nodes answer a peer that they whitelist
with the permission `mempool`, or any peer when they serve Bloom filters
(`-peerbloomfilters`, off by default on Bitcoin Core, on by default on
Bitcoin Cash Node). Any other peer that asks, they disconnect. A store that
asked the seeds and the discovered peers of Bitcoin, and had no other
peers, lost them all at once, and answered 503 until it found new ones. So
a store asks only a peer whose node takes the request.

**How a store knows.** Every peer runs a model of its node (haskoin-node's
`Haskoin.Node.Peer.Remote`, on the state machines that haskoin-core
translates from both nodes). The model reads from the version message of
the node whether it serves Bloom filters. It has the permissions that the
operator says the nodes of `--peer` grant (`--peer-permission mempool`, as
`whitebind=mempool@…` grants).

**A fleet with no source** has no `--node-rpc`, no peer that serves Bloom
filters and no `--peer` that grants `mempool`. It gets back only what its
peers announce from then on: new transactions, and those that wallets
broadcast again. Each of its stores says in `/health` (`mempool`) that it
has no source.

## Reading

A request takes the tip, opens the store's own view of its database at that
block, and lays the pool on top (`Haskoin.Store.Database.Query.Pooled`).
Every read of the pool names the tip, and `read.lua` refuses it if the tip
moved. The request is then answered again from the new tip. So no answer
mixes a block with a pool that belongs to another block.

**One block, but not one moment of the pool.** A transaction can enter
between two reads of the same request, and a balance and a list can differ
by it. Reads of an xpub are exact: they see one version of its entry, and
the request is answered again if the entry changes.

**Marked transactions are never read.** After a move, the chain at the tip
has the transactions that the blocks confirmed, and the pool still has
those that are marked. A read that would go by one of them says so, names
them, and reads nothing. The store takes those out and reads again
(`poolRead`, and the reads of the cache).

Which those are is known without reading every marked transaction:

- What the pool keeps for an address is written by the transactions at that
  address and by no other. The same holds for a category of tokens.
- An entry of the cache lists every transaction at its addresses.
- A transaction and an output are read with what spends them.

A read that goes by no marked transaction reads the pool as it will be once
they are all out. The list of the whole pool is read without them.

## What a fleet reads of a store's database

RocksDB holds the chain alone. A fleet reads:

- **The blocks between two tips** (`moveTip`): the blocks, and the records
  of the transactions that they confirmed, with their inputs. Across a
  reorganisation it also reads the transactions of the abandoned blocks,
  from the view at the old tip, since a reverted block's transactions leave
  the database.
- **The chain at the tip**, through a view of the database at that block,
  for the offer and for every request.

The block store imports a block [in stages](block-import.md), and takes a
mined block to be valid without checking it. That changes nothing here: for
a block that could be mined, the import leaves the chain's column families
as they always were.

## Why it holds

These hold after every decision:

- **Valid at the tip.** Each input of each transaction of the pool spends
  an output made by another transaction of the pool, or an output that is
  unspent at the tip. An offer checks it. A move of the tip removes what
  the new blocks confirmed or spent, and what depended on what they
  abandoned.
- **No double spends.** At most one transaction of the pool spends each
  output. The decision that puts a transaction in checks the spender of
  each input in the same step.
- **Closed under parents.** Removing a transaction removes everything that
  spends from it, except when it was confirmed.
- **Tip and pool together.** Only a decision of the pool changes either, in
  one step, after checking the tip that it was given. Reads check the tip
  too.
- **What a block took out is out, or marked.** After a move, each
  transaction that the blocks confirmed or replaced is out of the pool or
  marked, in the same step as the tip. Nothing but the taking out changes
  the pool while one is marked, and no read goes by one. So the first three
  rules hold of the pool without the marked transactions, which is the pool
  that every request sees.

The flags are worked out in the store, outside Redis. That is safe: they
are about the chain at one block, which never changes, and the decision
refuses the offer if the tip is no longer that block.

## Weak points

**1. Redis losing data.** The epoch tells the stores (see above). Until the
peers and the node have sent the transactions again, the pool lacks them,
and xpub entries are built again on demand.

**2. Redis eviction would corrupt the pool.** With a memory limit and a
policy that evicts keys, Redis can drop one key of the pool and keep the
others: a spender without its transaction, sums that do not add up. The
pool has to live in a Redis with `maxmemory-policy noeviction`. The store
does not check it.

**3. A restart is abrupt for clients.** When the pool is restarted, the
events do not say that its transactions left. Only the new block is told,
not the ones skipped. A client of `/events` should read again what it
follows when it sees a block that it cannot connect. The emptying uses
`KEYS`, which blocks Redis for as long as it walks the whole key space.
Keeping the pool's keys in a set, or under one hash tag, would avoid it.

**4. One peer's transaction reaches every store.** The store does not check
scripts; it trusts its peers. A bad transaction from one store's peer
reaches the pool, and every store shows it until it conflicts with a block
or expires (14 days by default). That is no risk when the stores connect to
nodes you run, which is the advice.

**5. The pool keeps what the network dropped.** A transaction that nodes
evict for a low fee stays in the pool until it expires.

**6. "First seen" is the pool's, not the miners'.** Of two transactions in
conflict, the pool keeps the first that any store offered, unless a store
that saw both replaced one. Miners may confirm the other. The pool then
corrects itself when that block comes. This is the same rule a node uses,
and cannot be better without knowing what miners will do.

**7. Small ones.**
- Two stores whose peers announce a transaction at the same moment both
  fetch it, since neither has offered it yet. The offer of the second finds
  it known.
- A replacement relayed by a peer replaces its rivals in the pool, on the
  word of that peer's node.
- The pool's part of a move of the tip that undoes a block is one long
  call, which holds Redis while it runs: such a move takes everything
  out in its own step. A move over new blocks only marks, and is short
  ([tip-first.md](tip-first.md)). The cache's part is written in pieces
  before it.
- Pool times are the clocks of the offering stores. Skewed clocks skew the
  order of the mempool list by as much.

## How it is tested

Random histories are **sampled**, particular cases are **tested**, and
one row is **checked**: every case of its small worlds
([README.md](README.md), "How sure we are"). Nothing here is proved.

| Where | What is held to what | How sure |
| --- | --- | --- |
| `Haskoin.Store.Fleet.LeavingSpec`, and `scripts/check-pool.sh` | The pool after a move of the tip: every pool, block and order of taking out of four small worlds, with the decisions themselves. Writers wait, every read is exact, and any order ends as the move in one step ([tip-first.md](tip-first.md)). | checked: 7,168 pools and blocks, 22,866 states |
| `Haskoin.Store.Fleet.PoolSpec` | The rules as a pure model. The pool stays valid on top of the chain, whatever is offered, mined and unwound. | sampled: 3,000 histories |
| `Haskoin.Store.Fleet.RedisSpec` | The pool in a real Redis, step by step against the model, the tokens included. | sampled: 300 histories |
| The same | The rules for emptying the pool, the events, and what counts as settled. | tested |
| `Haskoin.Store.Fleet.DecideSpec` | The decisions against the script that they replaced, in a real Redis. A move that marks, with the marked taken out afterwards in an order and in pieces that change from step to step, leaves Redis as the script that took everything out in one step did. Nothing is offered or withdrawn meanwhile. | sampled: 300 histories |
| The same | The same in the model of Redis. Offers, withdrawals, expiries and changes to the cache are tried between two takings out, and change nothing. | sampled: 3,000 histories at each run, 10,000 when it was written |
| The same | Every kind of read, of every address, transaction and output of a history, from a pool that has only been marked: it is the read of the same pool with everything out. It fails with the guard of one kind of read taken away. | sampled: 300 histories |
| `Haskoin.Store.Fleet.CacheSpec`: two stores on one chain, and a real Redis | Histories of payments, blocks, reorganisations and builds, with each cached entry held to the queries at the tip and the pool. | sampled: ten histories |
| The same | A reorganisation that keeps the cache. The pool filling again after Redis lost its data. A transaction that one store received, fetched by the other until it is offered, and not after. The queries of the token endpoints on Bitcoin Cash, with tokens moved twice in the pool and then confirmed. | tested |
| The same | A store that stops right after it moved the tip. Its cached xpub is then read without what the block confirmed: the read takes it out. It fails when the cache is made not to look for marks. | tested |
| The same | The same with a transaction that the block made invalid, by confirming another that spends the same coin and that the pool never had. The chain says nothing of that one; only the mark does. The xpub is read without what it paid. It fails when the cache is made not to look for marks, and when the move is made not to mark the rivals of a block. | tested |
| `IntegrationSpec`, the fleet test | Two stores against a real node and Redis answer alike, with a payment in the pool and after its block. | tested |

The two ends of a move that marks differ in one thing, which nobody reads.
A count that two transactions leaving together both change can end as a
zero where it would have ended as nothing, and every read takes the two for
the same. The model found that; three hundred histories against Redis had
not.

Not tested: Redis evicting keys, more than two stores, and a pool of real
size.

## Where the code is

| Part | Code |
| --- | --- |
| The rules, as pure functions | `src/Haskoin/Store/Fleet/Pool.hs`, `src/Haskoin/Store/Fleet.hs` |
| Offer, withdraw, expire, move the tip | `src/Haskoin/Store/Fleet/Pool/Decide.hs` |
| Decisions carried out in one step | `src/Haskoin/Store/Fleet/Atomic.hs`, `Atomic/Redis.hs`, `lua/atomic.lua` |
| Reads that check the tip | `src/Haskoin/Store/Fleet/lua/read.lua` |
| Calling them | `src/Haskoin/Store/Fleet/Redis.hs` |
| The script that the decisions replace, for the tests | `test/Haskoin/Store/Fleet/pool.lua`, `Oracle.hs` |
| Offering, moving, restarting, expiring | `src/Haskoin/Store/Fleet/Member.hs` |
| Answering from view and pool | `src/Haskoin/Store/Database/Query/Pooled.hs` |
