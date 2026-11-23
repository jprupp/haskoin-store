# How transactions reach the shared mempool: what must be true

What the path from a node's mempool to the pool in Redis has to
guarantee, what it may assume, and how each guarantee is known to hold.
[fleet-mempool.md](fleet-mempool.md) has the pool itself.

## In short

A node tells a peer about a new transaction once, and never again. So a
store that misses one announcement lacks that transaction until a block
confirms it. The store therefore asks a node for its whole mempool, and
asks again whenever it may have missed something. Every decision on the
way is made by one pure function (`step` in `Haskoin.Store.Ingest`), so
that it can be proved, checked and tested. The promise: once the node's
mempool stops changing, the pool comes to hold all of it, whatever state
the store was in. The cost: whole mempools are read again, every thirty
seconds while transactions come.

## Words used here

The shared words (store, node, peer, fleet, the pool, the tip) and the
five words for how sure we are (by construction, proved, checked,
sampled, tested) are in [README.md](README.md).

| Word | What it means here |
| --- | --- |
| **name**, **announce** | A node names a transaction when it sends its id (`inv`). It announces one when it names it unasked, as it takes it. |
| **source** | A node that gives its whole mempool when asked, again and again: a peer that answers `mempool`, or the node's RPC (`--node-rpc`). |
| **reading** | Asking a source for its whole mempool, once. The code calls it a sweep (`Sweep`, `sweep`). |
| **owed** | What a node may have and the store may lack is owed. A source that owes a reading is read again. |
| **the machine** | The pure function that makes every decision of the path, with what it remembers (`step` and `Ingest` in `Haskoin.Store.Ingest`). |
| **the runner** | The thread that tells the machine what happened and does what it says (`Haskoin.Store.Ingest.Run`). It decides nothing. |
| **the ledger** | The map from each transaction id to its place, with the only five functions that change it (`Haskoin.Store.Ingest.Ledger`). |
| **place** | Where an id is: **wanted** of the peers that named it, **asked** of one peer, or held. |
| **held** | The store has the transaction itself in memory, and the pool has not decided on it. |
| **offer** | To give a held transaction to the pool. Until the pool answers, the transaction is **offering**. |
| **verdict** | The pool's answer to an offer: settled, an orphan, or "ask again". |
| **settled** | The pool has decided on the transaction, or a block of the store's database confirms it. |
| **orphan** | A held transaction (the **child**) that spends from one that the pool lacks (its **parent**). |
| **seek** | To ask every peer for a parent that the pool lacks (`seek`). |
| **given up** | Let go by the machine before it is settled, with a reason. |
| **dirty** | Something was given up that a node may still have, and no source was read since. |
| **room** | The most ids that the machine keeps a place for, and the most transactions that it holds. |
| **window** | The most requests that the store has out to one peer at once. |
| **world** | A tiny imaginary network that the model checker goes through in every order of events. |
| **planted bug** | Code broken on purpose, which a check has to catch. |

Which peers are sources: Bitcoin Core answers `mempool` to a peer that it
whitelists with that permission, or when it serves Bloom filters. Bitcoin
Cash Node serves them by default.

## Why

The contents of the pool have a model and invariants
([concurrency.md](concurrency.md), I1 to I6). The path into the pool had
neither.

It was five stages joined by IO code: an announcement, a request, a wait,
a transaction held, an offer. Each stage could lose a transaction in a
way of its own: a queue that was full, a request forgotten after 20
seconds, a store not in sync for a moment, a cap. Each loss was excused
by "a peer announces again". That is false: a node announces a
transaction to a peer once (A1 below). So any loss lasted until a block.

On 21 November 2026 the btc store of a server fell 17,000 transactions
behind its node, and three fixes in two hours each broke something else.
Each stage passed its own tests. Every bug was in how the stages fit
together, or in whether a transaction arrives at all. Nothing said what
had to hold from end to end, so nothing could be proved.

## What is assumed

These are the axioms: what the proofs take from the world. Each rule of
the nodes comes from the nodes' source, as the protocol notes of
haskoin-core translate it (`doc/protocol/bitcoin-core.md`,
`bitcoin-cash-node.md`). A1, A2, A3, A5, A7, A8 and F1 have a test
against both nodes in Podman (`Haskoin.Store.NodeAxiomsSpec`), so that a
release of a node that changes one of them is noticed.

| | Assumption |
| --- | --- |
| A1 | A node announces a transaction to a peer **at most once**, and never one that it took before the peer connected. |
| A2 | A source, asked for its mempool, names **every** transaction in it at that moment, in messages of at most 50,000 names. A node that is no source closes the connection of a peer that asks. |
| A3 | Asked for a transaction that it announced, or that an answer to `mempool` named, a node sends it, or says `notfound` if it no longer has it. It reads at most 50,000 requests in a message. |
| A4 | A node sends in order, and stops sending to a peer that stops reading. It never asks again by itself. |
| A5 | Within one answer, a node names a transaction after the transactions that it spends from. Across answers and across peers there is no order. |
| A6 | A node drops transactions from its mempool without saying so: confirmed, replaced, evicted, expired. |
| A7 | A node that has a transaction has those that it spends from, unless a block has confirmed them. Asked for one of them that it never named to the peer, Bitcoin Core sends it, and Bitcoin Cash Node says `notfound`, though it has it. Neither drops the peer for asking. |
| A8 | A node names transactions to a peer no faster than it chooses to. Bitcoin Core 32 names about five hundred at once and then seventy every five seconds, fourteen a second, even to a peer that it whitelists. Bitcoin Cash Node 29 names 800 at once to a peer that it whitelists. Asked for its mempool, a node names all of it at once (A2). |
| F1 | The RPC of a node is a source as a peer is one. `getrawmempool` gives the id of **every** transaction in the mempool, each after those that it spends from. `getrawtransaction` gives a transaction, or the error -5 for one that the node does not have. Any other answer, or none, says nothing of what the node has. |
| R1 | A Redis script runs alone, start to end. |
| R2 | Redis may lose all its data, and the store finds out (the epoch). It may be away for any time. |
| U1 | The runner gives every connection a name of its own, never used again. In haskoin-node a connection is a peer of its own. |
| U2 | The runner gives the machine its inputs one at a time, and those of one peer in the order in which they came. |

**An example of A1.** A node takes a transaction at noon. A store
connects to it a second later. The node never announces that transaction
to the store. Only a reading finds it.

**What A5 is needed for.** The machine fetches and offers a chain in any
order of arrival. So with room to spare, the order of the names does not
matter. At the bound it does: the ids that are named first get the room.
Take a store with room for one id, and a node that holds a parent and
its child. A source that named the child first would give the room to
the child, which cannot be offered without its parent. A store with less
room than its node's mempool has transactions would go round for ever.
The model checker has a world that shows this, and that world must fail.

## What must be true

1. **T1, nothing is lost in silence.** Every transaction id that the
   store learns of is in exactly one place until it leaves. It leaves in
   one of two ways only: **settled**, or **given up for a counted
   reason**. The reasons are a closed list (`Reason`):
   - the node says that it no longer has it;
   - a request got no answer;
   - there was no room;
   - the peers that named it are gone;
   - what it spends from did not come;
   - the pool could not say whether it had it.

   A transaction given up while a node may still have it makes the store
   **dirty**. An id that a peer names, and that the store has no place
   for, is asked of the pool at once. So it is never only in the store's
   hands.
2. **T2, everything is bounded.** Three things each have a bound: the
   ids that the store holds, the requests that it has out to each peer,
   and the transactions that it holds in memory. So has every index that
   the store keeps of them, whether the clock ticks or not. At a bound
   the store gives up with the reason "no room" and is dirty. It never
   blocks the reading of its peers.
3. **T3, the pool converges.** Take a store with a source, in sync, with
   Redis answering. If its node's mempool and chain stop changing, then
   after a bounded number of steps the pool holds every transaction of
   the node's mempool that is valid at the tip, and it keeps them. This
   holds **from any state that the store can reach**: after a loss of
   Redis, a peer dropped, a full queue, a restart. A dirty store reads
   its source again. It holds with any room, down to one id and one
   transaction held: a store with less room than its node's mempool has
   transactions takes it in rounds.
4. **T4, the store keeps its node.** It asks a peer only for what that
   peer named, or for a parent: what a transaction that the store holds
   spends from and the pool lacks (A7). It never asks for more at once
   than the peer's window, which follows what the peer delivers. It
   sends nothing that the model of the node (`Haskoin.Node.judgeMessage`)
   says gets it dropped.
5. **T5, what the store says is true.** The count of pending
   transactions in `/health` is the number of requests out.
6. **T6, a transaction does not wait for what can be asked for.** When
   the pool says that it lacks a parent of a held transaction, the store
   asks its peers for it at once, if it has a peer and room. It does not
   wait for the parent to come by itself.
7. **T7, the pool takes what the node has.** Whether a transaction that
   a node names is settled is whether the pool has it, and nothing else:
   not whether the fleet saw it before. A store never turns away a
   transaction that its own node holds for something that it remembers.

## What is not promised

- **The RPC as the only source, and no peer connected to that node.** A
  transaction that the node takes is in the pool at the next reading of
  the RPC, every ten minutes, and not before. The RPC announces nothing,
  and no peer is there to be told. With a peer connected, T3 holds: the
  peer is told of what the node takes, and what the node took before the
  peer connected is read from the RPC when the peer connects.
- **No source.** T3 does not hold, and cannot: by A1 a transaction that
  the store missed is not announced again. A store whose peers are all
  strangers on Bitcoin, and that was not given the RPC of a node, has no
  source. T1 and T2 still hold. The store says that it has no source in
  its log, for each peer that is none, and in `/health` (`mempool`). A
  test shows it: such a store lacks what its node took before it
  connected, and has it once it is given the RPC.
- **The pool may hold what the node dropped** (A6). A transaction
  evicted or expired at the node stays in the pool until a block
  conflicts with it, or the pool's own limit of age takes it out. Each
  store of a fleet has a node of its own, so the pool is the union of
  what they hold. No single node can say that a transaction is gone.
- **Nodes that disagree.** Two stores of a fleet whose nodes hold
  transactions in conflict cannot both have theirs in the pool. It holds
  one, the one offered last, and each store offers its own again at its
  next reading. The fleet used to remember what its pool had let go, and
  did not fetch it again (`fleet:seen`). That kept a store from taking
  back what its own node held, which is worse, so the set is no longer
  kept (T7). A fleet with one node for each network, as most are, has no
  such conflict.
- **A transaction with no children that the store lost** without knowing
  it is back at the next reading of the source, every ten minutes, and
  not before. Nothing spends from it, so nothing asks for it (T6).
- **Time.** T3 bounds steps, not seconds. A node or a Redis that answers
  slowly makes the store slow, never wrong. One bound in seconds is
  kept, by a test and not by a proof: while transactions come, a source
  is read every thirty seconds. So a store is half a minute behind its
  node at most, plus the time of a reading, however fast the node takes
  transactions and however slowly it names them (A8).

## The design that makes this provable

**One map from transaction id to its place.** A place is wanted, asked
of a peer, or held, with what it waits for. An id has one value in a
map, so "in exactly one place" needs no proof. An id leaves the map
through one function, which says why. Every index beside the map is
exact: it is what the map says and nothing else, with no entry left to
clean later.

**A place only for what the pool has not decided on.** What a peer names
is asked of the pool first. It gets a place when the pool says that it
is not settled. What is settled never takes room.

**The last room for a transaction held is for one that can be offered.**
A transaction that has to wait for another never takes it.

**The sources are read again.** This is what makes every loss temporary,
and T3 true from any state. A source is read:

1. at the start, when it connects;
2. when the store is dirty;
3. thirty seconds after a reading, if a peer named a transaction that
   the pool lacked since that reading;
4. after twice the last wait if no peer did, up to ten minutes.

The period used to be ten minutes whatever happened. A server was then
thousands of transactions behind its node for minutes whenever the node
took more than fourteen a second, which is all that Bitcoin Core names
to a peer (A8). The store cannot tell whether transactions come fast: it
sees what the node names, at the node's pace. So it reads while they
come at all. The machine does not measure what a reading costs. The
runner says in the log when going through what a node named at once took
a second or more.

**A source that is no peer is a peer to the machine.** The RPC of the
node (`Haskoin.Store.NodeRpc`) comes as a peer that is a source. It is
asked for its mempool and for transactions by the same outputs, within
the same window, and it answers with the same inputs. A call that fails
is a peer that went, and a call tried again is a new peer (U1). What the
store asks a node at once over RPC follows what the node delivers, with
no number written down for it. One thing is particular to the RPC: it
announces nothing. What its node takes is announced to a peer, on
another connection, which begins at another time. The model has this
since a test lost transactions to it.

**A peer that connects owes what its node took before.** A node names to
a peer nothing that it took before the peer connected (A1). So a peer
that is a source is asked for its mempool when it connects. When a peer
that is no source connects, every source owes a reading: one of them may
be the RPC of that node, read a moment before.

**No room is a reason, not a wait.** An announcement is an id of 32
bytes, cheap to drop when the source is read again. Blocking a peer's
reader instead would stop its blocks too.

**A window for each peer.** It grows while the peer answers and shrinks
when it does not, in place of a fixed number.

**A transaction held for a parent is found by that parent.** It is tried
when the parent settles, in place of every held transaction at every
turn.

**A parent that the pool lacks is asked for** (`seek`), of every peer,
when the pool says so. The pool has just said that it lacks the parent,
so the parent gets a place without the pool being asked. The store used
to wait for the parent to come by itself, and to try the child again
after two seconds, four, eight, up to twenty minutes. But a node names a
transaction to a peer once (A1): a parent that the store had lost was
never named again, and everything that spent from it stayed out with it.

**A pure step function and a runner.** Every decision is in
`step :: Input -> Ingest -> (Ingest, [Output])`. The runner reads an
input, calls `step`, and carries out the outputs. It decides nothing.

## How each claim is established

There are five levels of evidence, from strongest to weakest: the five
words of [README.md](README.md). Each part
below says which claim has which today. "Tested" is never called proved,
and what is not done yet is said.

### By construction: the types leave no other possibility

- An id is in one place (T1): the places are the values of one map.
- The machine cannot change that map. It is inside an abstract type,
  `Ledger`, of a module of its own (`Haskoin.Store.Ingest.Ledger`),
  which gives the machine five functions and nothing else to change it
  with.

### Proved for every state and every input

By Liquid Haskell (`proofs/`, `cabal build` there, eighty seconds and
1.3 GiB of memory at most). CI runs it at every push, as the job
"Liquid Haskell".

- **Nothing leaves in silence (T1).** Of the five functions of the
  ledger, four keep every id in its place. The fifth, `leave`, takes one
  id away and puts the output that says so at the head of the outputs,
  in the same call. So whatever the machine does with them, an id that
  it had and no longer has was said to be gone.
- **The two bounds on memory (T2).** A ledger never has more ids than
  its room, and never counts more transactions held than its room for
  those. `put` is the one function that adds, and it refuses when there
  is no room.
- **`step` is total and always ends.** It has an answer for every input
  in every state, well formed or not, and no loop of it depends on the
  indexes being right. Its loops are folds over a snapshot: the first
  version had three that would have spun for ever with a wrong index.
- **Four planted bugs** in the ledger, one for each of its claims, are
  each rejected.

What those proofs assume: seven facts about `Data.Map`, written as
`assume` lines in the ledger module, such as that an insert adds its key
and at most one to the size.

**Not proved, and resting on the level below, the checks:** that the
count of transactions held is the number of places that hold one; that
the other indexes say what the map says; the windows (T4); and
convergence (T3), which is a property of the machine in a world and not
of a function.

### Checked for every order of events, in small worlds

A model checker in the test suite (`Haskoin.Store.Ingest.Model`) runs
`step` itself in worlds of one to three transactions and one or two
peers, with nodes that follow A1 to A7, through every order of events:
19 worlds, about 990,000 states. [the-19-worlds.md](the-19-worlds.md)
says in plain terms what a world is and how it is gone through.

- **In every state:**
  - nothing leaves in silence, and what a peer names has a place or is
    asked of the pool (T1);
  - the invariant, with the bounds and the exact indexes (T2);
  - a peer is asked only for what it named or for what a transaction
    spends from, and a source only for its mempool (T4);
  - the count of requests out (T5);
  - a parent that the pool lacks has a place, to be asked for, when
    there is a peer and room (T6).
- **From every state, with nothing more going wrong:** every run ends,
  and ends with the mempool of every connected source in the pool (T3).
- **Five planted bugs** in `step`, one for each kind of check, are each
  found. A check that has never failed is not known to work. The fifth
  is a machine that does not ask for what the pool lacks: it fails in
  every world that has a chain.
- **Parents.** The nodes of these worlds send what they have when asked,
  named or not (A7 as Bitcoin Core does it). That a Bitcoin Cash Node
  says `notfound` for what it did not name is not in them. The store
  then waits for the next reading, as it did before it asked for
  parents, and the worlds passed without that rule.
- **The RPC.** Two worlds have one node with its RPC and a peer that is
  no source, one mempool between them, the RPC announcing nothing. In
  them, a machine that does not read its sources when a peer connects
  comes to rest without what the node took between the reading of the
  RPC and the connection of the peer, in seven steps. That is the sixth
  planted bug, and the one that was in the store.
- **A world that must fail.** It breaks an assumption: a source that
  names a child first, with room for one id (A5). The search finds the
  store going round for ever there. The program of
  `scripts/check-ingest.sh` ends with a failure if that world passes, or
  if any other fails.

### Sampled, which is weaker

2,000 runs of a larger world (two chains of three, three peers, every
disturbance) keep the same rules and converge.

### Tested against the real programs

The real programs are Bitcoin Core and Bitcoin Cash Node, in Podman.

**The store end to end** (`Haskoin.Store.MempoolSpec`). In nine
situations, on each node. In the first eight a store must end with
exactly the mempool of its node:

1. it starts from nothing;
2. it starts at the tip with an empty pool and a large mempool;
3. it was away while transactions came, without a block;
4. the same, with a block;
5. its Valkey is stopped and started again under it (R2);
6. its node takes chains while it finds blocks;
7. its pool lost transactions that its node then takes children of (T6);
8. its pool let go of transactions that its node still has (T7).

In the ninth its node never stops taking transactions while blocks come,
and the store must answer from each new block within seconds of it.

The seventh and the eighth fail without their rule. On Bitcoin Core there are two
more:

- The node replaces transactions by ones that pay more. The store has
  the replacements and nothing of what they replaced.
- The node takes 2,500 transactions in a few seconds. The store has them
  within a minute, 31 seconds after the first where it was measured, and
  not after the two minutes and a half that the node takes to name them.
  This one fails without the reading every thirty seconds.

Three of these tests fail on the version that fell behind on the server,
so they can see that failure. One failed one run in three on the path
before this one: a race. The tests do not decide the order of events, so
they cannot find a race by themselves. That is what the model checker is
for.

**A store whose only source is the RPC** (the same spec), on a Bitcoin
Core that whitelists nobody:

- The store was away while transactions came, with and without a block.
  It has them when it is back. Without the RPC the same test fails, on
  what the node took before the store connected.
- The node closes its connections, takes transactions, and takes
  connections again. The store has them once it is connected, by reading
  the RPC again. This one fails without the rule that a peer that
  connects makes the sources owe a reading. It then ends as the server's
  stores did: at rest, without what its node has, and not knowing it.

**The assumptions** (`Haskoin.Store.NodeAxiomsSpec`), with a peer of the
tests' own that keeps what a node sends:

- A1: a node names a transaction to a peer once, and never one that it
  took before the peer connected.
- A2, A3, A5: asked for its mempool by a peer that it allows, a node
  names all of it, each transaction after those it spends from. It sends
  one that is asked for, and says `notfound` for one that it never had.
- A2: asked by a peer with no permission, Bitcoin Core closes the
  connection. Bitcoin Cash Node, which serves Bloom filters, answers.
- A7: asked for a parent that it never named, of a transaction that it
  did name, Bitcoin Core sends it and Bitcoin Cash Node says `notfound`.
- F1: over its RPC, a node gives the id of every transaction of its
  mempool, each after those it spends from, the transactions asked for,
  and its word for one that it never had. A node that nobody listens
  for, or that does not know the account, is an exception and never an
  empty mempool.
- A8: given 800 transactions at once, Bitcoin Core has not named them
  all to a peer that it whitelists ten seconds on, and has within
  forty-five. Bitcoin Cash Node has at once.

**Not tested:** that a node stops sending to a peer that stops reading
(A4), and that it says nothing of what it drops (A6), which is an
absence. The limit of 50,000 names to a message is not reached by a
test.

## What the checks have found

Before any of this ran on a server, these were found in the machine as
first written: the first three by going through every order of events,
the others as each says. Each is a failure of the kind that the stores
had, and none showed in an example.

1. **Indexes that grew for ever.** The queues of a peer were cleaned
   when the clock ticked. With no tick they had no bound: the smallest
   world had no end. They are exact now (T2).
2. **A mempool larger than the room was never taken.** Each reading
   named the settled transactions first. They took the room, and the
   store went round for ever. An id now gets a place only once the pool
   says that it is not settled (T3).
3. **An answer of the pool, about what a peer named, that came after the
   peer had gone**, put the peer back among those to ask (T4).
4. **With two sources, reading one cleared what the other owed.** Found
   by reading the code, before the checker ran. Each source owes its own
   reading now.
5. **A call of the RPC that failed was an empty mempool**, in the loop
   that read the RPC before it was a source of this path. And a
   transaction that the node did not send was no different from one that
   it does not have. Found by reading the code. The two are now an
   exception and the node's word (F1).
6. **The program of the model checker ended as a success after a world
   had failed**, though it printed the failure. Found by breaking A5 on
   purpose.

Two more were found on a server, by comparing the mempool of the node
with that of the store, id by id, and neither by a check here:

7. **A transaction that the fleet remembered and the pool did not have
   was never fetched again.** A server lacked one that its node held for
   hours, through every reading: the pool had let it go once, and
   "settled" meant "in the pool, or seen before". The model took the
   pool's word for what is settled, and the store asked something else.
   It asks the pool alone now (T7).
8. **Transactions waited twenty minutes for a parent that nobody was
   fetching.** A test ended with 84 held behind four that the store had
   lost. The parent is asked for now (T6).
9. **What a node took between the reading of its RPC and the connection
   of the store as its peer was in neither.** That is how the four of
   finding 8 were lost, in one run of about forty. The model had the RPC
   as a peer that announces, so it could not show it. With a source that
   does not announce it does, in seven steps. A peer that connects now
   makes the sources owe a reading.

The same search showed where the first model of the world was wrong: it
used the name of a peer again for a later connection, which haskoin-node
never does (U1).

## What stays outside the proofs

- The axioms.
- The runner (`Haskoin.Store.Ingest.Run`). It decides nothing: it turns
  messages into inputs and carries out outputs. It drops events of peers
  past a bound, and tells the machine so.
- The thread that asks the RPC of the node, and the calls that it makes
  (`ingestFeed`, `nodeFeed`, tested against both nodes).
- The two functions of the member that ask the pool and offer to it
  (`memberSettled`, `memberOffer`).
- The pool itself. [concurrency.md](concurrency.md), "Decisions in one
  step", says how its changes are made one step and how that is checked.
- GHC's runtime.
