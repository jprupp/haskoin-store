# The mempool: one pool in Redis

How the servers of a fleet keep one mempool in Redis, and why the design
holds. The architecture around it is in [one-fleet.md](one-fleet.md). It
describes the code; it is not part of the package.

## In short

Every server answers from the same mempool, that mempool is valid on top
of the block the fleet answers from, and the two always change together.
Each change to the pool is a decision made in Haskell and carried out in
Redis in one step, or not at all (`docs/concurrency.md`, "Decisions in one
step"). The tests hold the decisions to a pure model, and to the Lua
script that they replace. The weak points are listed at the end.

## The problem

Servers that answer from the same block must also answer from the same
mempool. So the mempool is in Redis, the pool, and nowhere else: a server
keeps no mempool of its own, in RocksDB or in memory. A server without Redis
knows nothing of the mempool, and is unhealthy.

## What Redis holds

- **The fleet tip**: the block that every server answers from.
- **The pool**: the transactions, with what each spends, what spends each
  output, the children of each transaction, its body, the time it first
  entered a mempool, and, for every address, its transactions, its outputs
  and their sums.
- **The tokens of the pool** (CashTokens, on Bitcoin Cash): for each
  category, its transactions and the outputs that hold it, and for each
  address and category, the sums of a token balance. The server reads the
  tokens of each output and sends them with the transaction, so the
  scripts never read a token prefix (`docs/cashtokens.md`).
- **Counters** next to the tip: how many times the tip moved, and how many
  times the pool was started again.
- **A stream of events**: what entered, what left, which blocks the tip
  went through.
- **No list of what the fleet saw.** There was one (`fleet:seen`), the
  newest 100,000 transactions that the pool had decided on, and a server
  fetched nothing that was in it: a transaction that the pool had let go
  and that a node still held was then never fetched again. A server now
  checks an announcement against the pool alone, in one call
  (`ZMSCORE`), and against what it holds to offer
  (`docs/mempool-ingestion.md`, T7), and the list is no longer kept.

The pool and the tip are written only by the decisions of the pool
(`Haskoin.Store.Fleet.Pool.Decide`), each carried out in one step: they
change together or not at all, and a decision that fails has written
nothing. While a server moves the tip, the others' offers are late and
are made again a moment after: the move has the floor
(`docs/concurrency.md`, "The floor").

## Getting in: the offer

How a transaction gets from a peer to an offer is a machine of its own,
`Haskoin.Store.Ingest`, with a document of its own,
`docs/mempool-ingestion.md`: what a peer names is asked of the pool, what
the pool has not decided on is requested, and what arrives is offered at
once (`memberOffer`), one transaction to an offer, each after those that
it spends from and that the server is still getting. The offer reads the
outputs that a transaction spends from the server's view at the tip or
from the pool. One whose outputs are in neither is an orphan: the machine
holds it for the transaction that it lacks, and offers it again when that
one is in the pool.

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
| Taken, or already known | Lets it go; the pool has it, so no server fetches it again |
| Orphan | Asks its peers for the transactions that it spends from and the pool lacks, keeps it waiting for them, and tries again after two seconds, four, eight, for 20 minutes at most |
| Conflict | Offers it again naming the rivals as replaced: a peer relayed it, so its node took it in their place. If that offer is not decided either, as when a new rival came, it waits for the next round |
| Late | Stops; the next round offers it again at the new tip |

**Replacement by fee** works through the conflict rule. A peer relays Y
only once its node took Y in place of X, so a server that is told of a
conflict offers Y again naming X as replaced, and the script takes X out
with everything that spends it, then puts Y in.

A server **ahead of the tip** may have transactions that spend outputs of
its newer blocks. Their flags say "not unspent at the tip", so they come
back as orphans and wait until the tip catches up.

## Getting out

A transaction leaves the pool only for a reason:

- it was replaced (above);
- it is older than the mempool timeout, counted on the pool's own times
  (`expirePool`, run by every server each turn): one decision chooses the
  old transactions and withdraws them, so that one that left
  and came back with a new time is not withdrawn for its old one;
- the tip moved to a block that confirms it, or that spends what it spends.

Whatever leaves takes along everything that spends it, since those would
spend outputs that no longer exist. A confirmed transaction leaves alone:
what spends it now spends a confirmed output.

A server has no mempool of its own, so there is nothing for it to
withdraw. What it holds and has not offered yet is in the machine
(`Haskoin.Store.Ingest`), with the orphans. An orphan is let go 20 minutes
after the server had it, as Bitcoin Core lets go of its own: a parent that
has not come by then is not coming.

## When the tip moves

A server whose turn finds that all live servers have a newer block in
common moves the tip there (`moveTip`). It reads from its views of its
database what the blocks between the old tip and the new one did, and
hands the script all of it in one call, with the xpub entries
([xpub-indexing.md](xpub-indexing.md)):

- the transactions they confirmed, which leave quietly;
- the outputs they spent, whose spenders in the pool leave with their
  descendants;
- across a reorganisation, the transactions of the abandoned blocks, which
  come back into the pool, read from the server's view at the old tip, and
  those that cannot (coinbases), whose spenders leave. A server without a
  view of the old tip does not move the tip: another server may have it.

The script checks that the tip is still the one the server read, and that
the new block is still the one the live servers agree on. Then it changes
the pool, moves the tip, counts the move, and tells the blocks as events.
Two servers that try at once do it once: the second finds the tip changed.

### The tip first, and the pool lets go after it

A move over new blocks alone, which nearly every move is, does not take
out of the pool what the blocks confirmed or replaced. It **marks** each
of those transactions (`pool:leaving`, one field for each: nothing for
one that a block confirmed, and for one that a block is in conflict
with, the transaction that replaced it and the height). That is one write
for each, and the tip is at the new block. The transactions then leave a
few at a time (`takeOut`, two hundred to a decision), each as it left
before: the store that moved the tip does it, and so does any store at
its next turn, for one that stops in between.

Why. Taking a transaction out is taking it out of all that mentions it:
the lists and the sums of each of its addresses, and the entry of every
cached key that has one of them. For a full block that was tens of
thousands of questions and more than a hundred thousand writes in one
decision, for which the tip waited: seconds of it in Haskell, and nearly
a second in Redis on a server, where Redis answers nobody else while it
carries a decision out. It grew with the number of cached keys. The tip
of a server was five to fifteen seconds behind its node at every full
block, and once, on a machine short of memory, Redis answered nobody for
twenty-three seconds. Marking is decided in a tenth of the time whatever
the number of cached keys, and no step of the taking out is long.

What makes it right is that nobody sees a marked transaction:

- **Nothing else changes the pool or the cache meanwhile.** While a
  transaction is marked, an offer is late, a withdrawal takes nothing,
  no entry of the cache is built, dropped or evicted, and the next move
  waits (`leavingHeld`), as all of them do while a store has the floor.
- **A read that would go by a marked transaction takes it out first**
  (below). So every answer is the answer of a pool that has let go of
  everything.
- **Taking out is the same in any order and by anyone.** One that is no
  longer marked is left alone, so two stores, or a store and a request,
  take nothing out twice.

A move that undoes a block takes everything out in its own step, as all
moves did: a transaction that comes back to the pool has to find what
the new blocks did already done.

### What the blocks replaced

The pool also keeps what the blocks replaced, so that every server answers
410 alike for a transaction that lost to a transaction of a block.

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
  the new blocks at different times.
- **It is forgotten** when the winner comes back from an abandoned block,
  2,016 blocks after its height, and when the pool starts again.

Not kept: a transaction of the pool replaced by fee, or withdrawn for its
age. Those answer 404.

## When nobody can say what the blocks did

A server goes through 50 blocks at most. Two cases are past that:

- **It does not have the old tip** at all. It may then empty the pool and
  move the tip, but only when it is the one server that is live: another
  might know. A server whose report was refused is not live, and moves no
  tip at all.
  A reorganisation from a tip that it has but cannot read is not this case:
  it waits.
- **The way is longer than 50 blocks.** That is true for every server, so
  any of them may empty the pool and move the tip.

Either way the decision also empties the xpub cache, whose pool layer is
gone, and counts the restart (`fleet:restarts`). Each server sees the count
change at its turn (`noteRestarts`) and fills the pool again as after a
loss of Redis, below.

## When Redis loses its data

Redis keeps a random epoch (`fleet:epoch`, set if missing). A server that
finds it gone or changed at its turn (`noteEpoch`) forgets its tip, and is
unhealthy until the fleet has a tip again; the first turn sets one, since
every live server reports. Then it fills the pool again.

## Filling the pool again

A server reads the whole mempool of its **sources** again and again: the
RPC of its node, if it was given one (`--node-rpc`), and the peers whose
node gives its mempool when asked. It does so when a source connects,
since a node announces a transaction to a peer once and never one that it
took before the peer connected; whenever it may have missed something,
for want of room, of an answer or of Redis; when the pool lost what it
had, after a loss of Redis or a restart of the pool; and every ten minutes
besides. What the pool has is not fetched again: the server asks the pool
about each id first, so a reading of a mempool that the pool already has
costs the list of its ids and no more. `docs/mempool-ingestion.md` has
the rules, and why they make every loss temporary.

**The RPC** (`getrawmempool`, then `getrawtransaction` in batches) always
answers its account, whatever the node allows its peers. The server asks
it as it asks a peer: for as many transactions at once as a window that
grows while the node answers in time and halves when it does not. A node
that does not answer is tried again after a second, then two, up to a
minute, and is never taken for a node with an empty mempool.

**Which peers are sources.** Both nodes answer a peer that they whitelist
with the permission `mempool`, or any
peer when they serve Bloom filters (`-peerbloomfilters`, off by default
on Bitcoin Core, on by default on Bitcoin Cash Node). Any other peer that
asks, they disconnect. Asking the seeds and the discovered peers of
Bitcoin once lost them all: a store with no other peers dropped to none,
all at once, and answered 503 until it found new ones. So only a peer
whose node takes the request is asked. Every peer runs a model of its
node (haskoin-node's `Haskoin.Node.Peer.Remote`, on the state machines
that haskoin-core translates from both nodes). The model reads from the
version message of the node whether it serves Bloom filters, and has the
permissions that the operator says the nodes of `--peer` grant
(`--peer-permission mempool`, as `whitebind=mempool@…` grants).

A fleet without `--node-rpc`, and without a peer that serves Bloom filters
or a `--peer` that grants `mempool`, has no source. It gets back only what
its peers announce from then on: new transactions, and those wallets
broadcast again. Each of its servers says in `/health`
(`mempool`) that it has no source.

## Reading

A request takes the fleet tip, opens the server's own view of its database
at that block, and lays the pool on top (`Haskoin.Store.Database.Query.Pooled`).
Every read of the pool names the tip, and `read.lua` refuses it if the tip
moved. The request is then answered again from the new tip. So no answer
mixes a block with a pool that belongs to another block.

A request is one block, but not one moment of the pool: a transaction can
enter between two reads of the same request. A balance and a list can
differ by it. Reads of an xpub are exact: they see one version of its
entry, and the request is answered again if it changes.

After a move, the chain at the tip has the transactions that the blocks
confirmed, and the pool still has those that are marked as leaving. A
read that would go by one of them says so, with which, and reads
nothing: the store takes those out and reads again (`poolRead`, and the
reads of the cache). Which those are is known without reading them all.
What the pool keeps for an address is written by the transactions at
that address and by no other, and the same for a category of tokens; an
entry of the cache lists every transaction at its addresses; a
transaction and an output are read with what spends them. A read that
goes by none of them reads the pool as it will be once they are all out.
The list of the whole pool is read without them.

## What a fleet reads of a server's database

RocksDB holds the chain alone. A fleet reads:

- **The blocks between two tips** (`moveTip`): the blocks, the records of
  the transactions that they confirmed, with their inputs, and, across a
  reorganisation, the transactions of the abandoned blocks from the view
  at the old tip. A reverted block's transactions leave the database.
- **The chain at the fleet tip**, through a view of the database at that
  block, for the offer and for every request.

The block store imports a block [in stages](block-import.md), and takes a
mined block to be valid without checking it. That changes nothing here:
for a block that could be mined, the import leaves the chain's column
families as they always were.

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
- **Tip and pool together.** Only a decision of the pool changes either,
  in one step, after checking the tip it was given. Reads check the tip
  too.
- **What a block took out is out, or marked.** After a move, each
  transaction that the blocks confirmed or replaced is out of the pool
  or marked as leaving, in the same step as the tip. Nothing but the
  taking out changes the pool while one is marked, and no read goes by
  one. So the three rules above hold of the pool without the marked
  transactions, which is the pool that every request sees.

The check of the chain (the flags) happens in the server, outside Redis.
That is safe because the flags are about the chain at one block, which
never changes, and the script refuses the offer if the tip is no longer
that block.

## Weak points

**1. Redis losing data.** An epoch key tells the servers (see above). Until
the peers and the node have sent the transactions again, the pool lacks
them, and xpub entries are built again on demand.

**2. Redis eviction would corrupt the pool.** With a memory limit and a
policy that evicts keys, Redis can drop one key of the pool and keep the
others: a spender without its transaction, sums that do not add up. The
pool has to live in a Redis with `maxmemory-policy noeviction`. The store
does not check it.

**3. A restart is abrupt for clients.** When the pool is started again,
the events do not say that its transactions left, and only the new block
is told, not the ones skipped. A client of `/events` should read again
what it follows when it sees a block it cannot connect. The emptying uses
`KEYS`, which blocks Redis for as long as it walks the whole key space.
Keeping the pool's keys in a set, or under one hash tag, would avoid it.

**4. One peer's transaction reaches every server.** The store does not
check scripts; it trusts its peers. A bad transaction
from one server's peer reaches the pool and every server shows it, until
it conflicts with a block or expires (14 days by default). That is no
risk when the servers connect to nodes you run, which is the advice.

**5. The pool keeps what the network dropped.** A transaction that nodes
evict for a low fee stays in the pool until it expires.

**6. "First seen" is the pool's, not the miners'.** Of two conflicting
transactions, the pool keeps the first that any server offered, unless a
server that saw both replaced one. Miners may confirm the other. The pool
then corrects itself when that block comes. This is the same rule a node
uses, and cannot be better without knowing what miners will do.

**7. Small ones.**
- Two servers whose peers announce a transaction at the same moment both
  fetch it, since neither has offered it yet. The offer of the second
  finds it known.
- A replacement relayed by a peer replaces its rivals in the pool, on the
  word of that peer's node.
- The pool's part of a tip move across many large blocks is one long
  script call, which holds Redis while it runs; the cache's part is
  written in pieces before it. In normal running it is one block.
- Pool times are the offering servers' clocks. Skewed clocks skew the
  order of the mempool list by as much.

## How it is tested

- `Haskoin.Store.Fleet.PoolSpec`: the rules as a pure model, and a
  property that the pool stays valid on top of the chain whatever is
  offered, mined and unwound (3,000 random histories).
- `Haskoin.Store.Fleet.RedisSpec`: the scripts against a real Redis, step
  by step against the model, the tokens included, plus the emptying
  rules, the events, and what counts as settled.
- `Haskoin.Store.Fleet.DecideSpec`: the decisions held to the scripts
  that they replaced, over random histories against a real Redis. A move
  that marks, with the marked taken out afterwards in an order and in
  pieces that change from step to step, leaves Redis as the script that
  took everything out in one step did, and nothing is offered or
  withdrawn meanwhile. The same in the model of Redis, over three
  thousand histories at each run and ten thousand when it was written,
  with offers, withdrawals, expiries and changes to the cache tried
  between two takings out, which change nothing. The two ends are the
  same but for this: a count that two transactions leaving together
  both change can end as a zero where it would have ended as nothing,
  which every read takes for the same. The model found that; three
  hundred histories against Redis had not. And every kind of read, of every address,
  transaction and output of a history, from a pool that has only been
  marked, is the read of the same pool with everything out: it fails
  with the guard of one kind of read taken away.
- `Haskoin.Store.Fleet.CacheSpec`: two members on one chain and a real
  Redis: random histories of payments, blocks, reorganisations and builds,
  each cached entry held to the queries at the tip and the pool; a
  reorganisation that keeps the cache; the pool filling again after Redis
  lost its data; a transaction that one member received fetched by the
  other until it is offered, and not after; and the token endpoints'
  queries on Bitcoin Cash, with tokens moved twice in the pool and then
  confirmed; and a store that stops right after moving the tip, whose
  cached key is then read without what the block confirmed, the read
  taking it out. The same with a transaction that the block made
  invalid, by confirming another that spends the same coin and that the
  pool never had: the chain says nothing of that one, only the mark
  does, and the key is read without what it paid. Both fail when the
  cache is made not to look for marks, and the second when the move is
  made not to mark what a block is in conflict with.
- `IntegrationSpec`, the fleet test: two servers against a real node and
  Redis, answering alike with a payment in the pool and after its block.

Not tested: Redis evicting keys, more than two servers, and a pool of real
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
