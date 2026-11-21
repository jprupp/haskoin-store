# How transactions reach the shared mempool: what must be true

What the path from a node's mempool to the pool in Redis has to guarantee,
what it may assume, and how each guarantee is established. The path is one
pure machine, `Haskoin.Store.Ingest`, run by `Haskoin.Store.Ingest.Run`.
`docs/fleet-mempool.md` has the pool itself.

## Why

The contents of the pool have a model and invariants
(`docs/concurrency.md`, I1 to I6). The path into the pool had neither. It
was five stages joined by IO code: an announcement, a request, a wait, a
transaction held, an offer. Each stage could lose a transaction on terms
of its own: a queue that was full, a request forgotten after 20 seconds, a
store not in sync for a moment, a cap. Each loss was excused by "a peer
announces again", which is false: a node announces a transaction to a peer
once (A1 below). So any loss lasted until a block.

On 21 November 2026 the btc store of a server fell 17,000 transactions
behind its node. Three fixes in two hours each broke something else. Every
one of those bugs was in how the stages compose, or in whether a
transaction arrives at all; each stage passed its own tests. Nothing said
what had to hold from end to end, so nothing could be proved.

## Words

- **The node** is a full node that the store is a peer of. **Its mempool**
  is the set of transactions it holds unconfirmed.
- **The pool** is the shared mempool of the fleet, in Redis.
- **The tip** is the fleet tip, the block that the pool is valid on.
- **A source** is a way to read a node's whole mempool, again and again:
  a peer that answers `mempool`, or the node's RPC (`--node-rpc`). Bitcoin
  Core answers `mempool` to a peer it whitelists with that permission, or
  when it serves Bloom filters; Bitcoin Cash Node serves them by default.
- **Settled** is a transaction that the pool has decided on, or that a
  block of the store's database confirms.

## What is assumed

These are the axioms: what the proofs take from the world. Each is a rule
of the nodes that haskoin-core's protocol notes translate from their
source (`doc/protocol/bitcoin-core.md`, `bitcoin-cash-node.md`). A1, A2,
A3 and A5 have a test against both nodes in Podman
(`Haskoin.Store.NodeAxiomsSpec`), so that a release of a node that
changes one of them is noticed.

| | Assumption |
| --- | --- |
| A1 | A node announces a transaction to a peer **at most once**, and never one that it took before the peer connected. |
| A2 | A source, asked for its mempool, names **every** transaction in it at that moment, in announcements of at most 50,000. A node that is no source closes the connection of a peer that asks. |
| A3 | Asked for a transaction that it announced, or that an answer to `mempool` named, a node sends it, or says `notfound` if it no longer has it. It reads at most 50,000 requests in a message. |
| A4 | A node sends in order, and stops sending to a peer that stops reading. It never asks again by itself. |
| A5 | A node names a transaction after the transactions that it spends from, within one answer. Across answers and across peers there is no order. |
| A6 | A node drops transactions from its mempool without saying so: confirmed, replaced, evicted, expired. |
| R1 | A Redis script runs alone, start to end. |
| R2 | Redis may lose all its data, and the store finds out (the epoch). It may be away for any time. |
| U1 | The runner gives every connection a name of its own, never used again: in haskoin-node a connection is a peer of its own. |
| U2 | The runner gives the machine its inputs one at a time, those of one peer in the order in which they came. |

## What must be true

1. **T1, nothing is lost in silence.** Every transaction id that the store
   learns of is in exactly one place until it leaves, and it leaves in one
   of two ways only: **settled**, or **given up for a counted reason**.
   The reasons are a closed list (`Reason`): the node says that it has
   it no longer, a request got no answer, there was no room, the peers
   that named it are gone, what it spends from did not come, the pool
   could not say whether it had it. A transaction given up while a node
   may still have it marks the store **dirty**. What a peer names and the
   store has no place for is asked of the pool at once, so it is never
   only in the store's hands.
2. **T2, everything is bounded.** The ids the store holds, the requests it
   has out to each peer, and the transactions it holds in memory each have
   a bound, and so has every index that the store keeps of them, whether
   the clock ticks or not. At a bound the store gives up with the reason
   "no room" and is dirty. It never blocks the reading of its peers.
3. **T3, the pool converges.** Take a store with a source, in sync, with
   Redis answering. If its node's mempool and chain stop changing, then
   after a bounded number of steps the pool holds every transaction of the
   node's mempool that is valid at the tip, and it keeps them. This holds
   **from any state that the store can reach**: after a loss of Redis, a
   peer dropped, a full queue, a restart. A dirty store reads its source
   again. It holds with any room, down to one id and one body: a store
   with less room than its node's mempool has transactions takes it in
   rounds.
4. **T4, the store keeps its node.** It asks a peer only for what that
   peer named, never for more at once than a window that follows what the
   peer delivers, and it sends nothing that the model of the node
   (`Haskoin.Node.judgeMessage`) says gets it dropped.
5. **T5, what the store says is true.** The count of pending
   transactions in `/health` is the number of requests out.

## What is not promised

- **Without a source**, T3 does not hold, and cannot: by A1 a transaction
  that the store missed is not announced again. A store whose peers are
  all strangers on Bitcoin has no source. T1 and T2 still hold, and the
  store says at its start, and in `/health`, that it has no source.
- **The pool may hold what the node dropped** (A6): a transaction evicted
  or expired at the node stays in the pool until a block conflicts with
  it or the pool's own limit of age takes it out. Each store of a fleet
  has a node of its own, so the pool is the union of what they hold, and
  no single node can say that a transaction is gone.
- **What the pool let go.** The fleet remembers what the pool decided on,
  the newest 100,000 (`fleet:seen`), and the store does not fetch those
  again. A transaction that the pool let go for its age, or turned away
  for one that conflicts with it, stays out while it is remembered, even
  if a node still has it.
- **Time.** T3 bounds steps, not seconds: a node or a Redis that answers
  slowly makes it slow, never wrong.

## The design that makes this provable

- **One map from transaction id to its place**: wanted, asked of a peer,
  or held, with what it waits for. An id has one value in a map, so "in
  exactly one place" needs no proof. An id leaves the map through one
  function, which says why. Every index beside the map is exact: it is
  what the map says and nothing else, with no entry left to clean later.
- **A place only for what the pool has not decided on.** What a peer names
  is asked of the pool first, and gets a place when the pool says that it
  is not settled. What is settled never takes room.
- **The last room for a body is for one that can be offered.** A
  transaction that has to wait for another never takes it.
- **Reading the source again**, at the start, when dirty, and at a period
  that follows what a reading costs and what it finds. This is what makes
  every loss temporary, and T3 true from any state.
- **No room is a reason, not a wait.** Announcements are ids of 32 bytes
  and cheap to drop when the source is read again; blocking a peer's
  reader would stop its blocks too.
- **A window for each peer** that grows while the peer answers and
  shrinks when it does not, in place of a fixed number.
- **A transaction held for a parent is found by that parent**, and tried
  when the parent settles, in place of every held transaction at every
  turn.
- **A pure step function and a runner.** Every decision is in
  `step :: Input -> Ingest -> (Ingest, [Output])`. The runner reads an
  input, calls `step`, and carries out the outputs. It decides nothing.

## How each claim is established

Four levels of evidence, and which claim has which today. "Tested" is
never called proved, and what is not done yet is said.

**By construction: the types leave no other possibility.**

- An id is in one place (T1): the places are the values of one map.
- The machine cannot change that map. It is inside an abstract type,
  `Ledger`, of a module of its own (`Haskoin.Store.Ingest.Ledger`), which
  gives the machine five functions and nothing else to change it with.

**Proved for every state and every input**, by Liquid Haskell
(`proofs/`, `cabal build` there, two minutes):

- **Nothing leaves in silence (T1).** Of the five functions of the
  ledger, four keep every id in its place, and the fifth, `leave`, takes
  one id away and puts the output that says so at the head of the
  outputs, in the same call. So whatever the machine does with them, an
  id that it had and no longer has was said to be gone.
- **The two bounds on memory (T2).** A ledger never has more ids than its
  room, and never counts more transactions held than its room for those.
  `put` is the one function that adds, and it refuses when there is no
  room.
- **`step` is total and always ends.** It has an answer for every input
  in every state, well formed or not, and no loop of it depends on the
  indexes being right. The first version had three loops that did: with a
  wrong index they would have spun for ever. They are folds over a
  snapshot now.
- Four bugs planted in the ledger, one for each of its claims, are each
  rejected.

What those proofs assume: seven facts about `Data.Map`, written as
`assume` lines in the ledger module, such as that an insert adds its key
and at most one to the size.

**Not proved, and resting on the level below:** that the count of
transactions held is the number of places that hold one; that the other
indexes say what the map says; the windows (T4); and convergence (T3),
which is a property of the machine in a world and not of a function.

**Checked for every order of events, in small worlds**: a model checker
in the test suite (`Haskoin.Store.Ingest.Model`) runs `step` itself in
worlds of one to three transactions and one or two peers, with nodes that
follow A1 to A6, through every order of events: 17 worlds, about 790,000
states.

- In every state: nothing leaves in silence and what a peer names has a
  place or is asked of the pool (T1); the invariant, with the bounds and
  the exact indexes (T2); a peer is asked only for what it named, a
  source only for its mempool (T4); the count of requests out (T5).
- From every state, with nothing more going wrong: every run ends, and
  ends with the mempool of every connected source in the pool (T3).
- Four bugs planted in `step`, one for each kind of check, are each
  found. A check that has never failed is not known to work.

**Sampled**, which is weaker: 3,000 runs of a larger world (two chains of
three, three peers, every disturbance) keep the same rules and converge.

**Tested against the real programs**, Bitcoin Core and Bitcoin Cash Node
in Podman:

- **The store end to end** (`Haskoin.Store.MempoolSpec`): six situations
  in which a store must end with exactly the mempool of its node, on each
  node. It starts from nothing; it starts at the tip with an empty pool
  and a large mempool; it was away while transactions came, with and
  without a block; its Valkey is stopped and started again under it (R2);
  its node takes chains while it finds blocks. Three of them fail on the
  version that fell behind on the server, so they can see that failure,
  and one failed one run in three on the path before this one, a race.
  They do not decide the order of events, and so cannot find a race by
  themselves: that is what the model checker is for.
- **The assumptions** (`Haskoin.Store.NodeAxiomsSpec`), with a peer of the
  tests' own that keeps what a node sends: a node names a transaction to
  a peer once, and never one that it took before the peer connected (A1);
  asked for its mempool by a peer that it allows, it names all of it,
  each transaction after those it spends from, sends one that is asked
  for, and says `notfound` for one that it never had (A2, A3, A5); asked
  by a peer with no permission, Bitcoin Core closes the connection and
  Bitcoin Cash Node, which serves Bloom filters, answers (A2).
- **Not tested:** that a node stops sending to a peer that stops reading
  (A4), and that it says nothing of what it drops (A6), which is an
  absence. The limit of 50,000 names to a message is not reached by a
  test.

## What the checks have found

Before any of this ran on a server, going through every order of events
found these in the machine as first written. Each is a failure of the
kind that the stores had, and none showed in an example.

1. **Indexes that grew for ever.** The queues of a peer were cleaned when
   the clock ticked. With no tick they had no bound: the smallest world
   had no end. They are exact now (T2).
2. **A mempool larger than the room was never taken.** Each reading of
   the source named the settled transactions first, they took the room,
   and the others never got in: the store went round for ever. An id now
   gets a place only once the pool says it is not settled (T3).
3. **An answer of the pool, about what a peer named, that came after the
   peer had gone**, put the peer back among those to ask (T4).
4. **With two sources, reading one cleared what the other owed.** Found
   by reading, before the checker ran; each source owes its own reading
   now.

The same search showed where the first model of the world was wrong: it
used the name of a peer again for a later connection, which haskoin-node
never does (U1).

What stays outside the proofs is named: the axioms, the runner
(`Haskoin.Store.Ingest.Run`, which decides nothing: it turns messages
into inputs and carries out outputs, and drops events of peers past a
bound while telling the machine so), the two functions of the member that
ask the pool and offer to it (`memberSettled`, `memberOffer`), the Lua
scripts of the pool (`docs/concurrency.md` says how those are checked,
and that gap is the next piece of work), and GHC's runtime.
