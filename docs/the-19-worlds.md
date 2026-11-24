# The worlds of the checker

How the path of a transaction into the shared mempool is checked, in plain
terms. [mempool-ingestion.md](mempool-ingestion.md) has the path itself,
what it promises and what it assumes. The checker is
`scripts/check-ingest.sh`, and its worlds are in
`test/Haskoin/Store/Ingest/Model.hs`.

## The thing being tested

One piece of the store decides what to do with every transaction between
"my node mentioned it" and "it is in the shared mempool". It is written as a
single function: given what just happened and what it knows, it says what to
do next. Call it the machine.

## The problem with ordinary tests

The bugs this replaced were never about one event. They were about two
events arriving in an unlucky order: a block that came between a request and
its answer, a peer that connected just after a read. An ordinary test tries
one order, or a few random ones, and misses the unlucky one.

## What a "world" is

A tiny imaginary network, small enough that a program can try **every**
possible order of events in it, not a sample. A world has:

- one or two nodes, each with a mempool;
- one to three transactions, some spending from others;
- the machine itself, the real code that the store runs;
- a pretend pool that answers its questions;
- a list of things that can go wrong.

## What "going through a world" means

The checker starts with everything empty and asks: what could happen next? A
node takes a transaction, a message arrives, a peer drops, the pool loses its
data, a request times out. It tries each of those, and from each result asks
the same question again, until it has visited every situation that can be
reached.

A world has between 500 and 430,000 such situations. All 26 together have
about two and a quarter million, and going through them takes three
minutes and a half. There were 19 until the store told its own nodes from strangers,
which is where the name of this page comes from.

## What is checked in every situation

1. No transaction disappears without the machine saying why.
2. Its memory stays within its bounds.
3. It only asks a node for things that it is allowed to ask for.
4. From this situation, if nothing more goes wrong, the store always ends up
   with everything its node has. It never gets stuck and never goes round in
   circles.
5. A forged transaction is never offered to the pool as one that a node of
   the operator took.

## Why 26

Each world pairs a small set of transactions with one or two kinds of
trouble, so that every kind of trouble meets every other somewhere.

| Worlds | What they are for |
| --- | --- |
| 1 | one transaction, with every kind of trouble at once |
| 9 | a chain of two transactions, each with one kind of trouble: blocks, a peer that drops, requests with no answer, a pool that fails, a pool that is wiped, a store out of sync, clients, another store |
| 3 | very little memory, and chains that arrive child first |
| 3 | two kinds of trouble together |
| 2 | two peers, or two sources |
| 2 | one node reached both by its RPC and as a peer |
| 4 | a stranger and a node of the operator, with a transaction that the store cannot verify: alone, with blocks, with one that spends from it, and with nobody to vouch for it |
| 3 | a stranger that has forged transactions, with and without a node of the operator beside it |

There is also one more world that is **supposed to fail**. It breaks one of
the assumptions about nodes on purpose, to prove that the checker can see a
failure at all.

## Why the worlds are so small

Trying every order is only possible when there are few things to order. The
bet, which is the usual one for this technique, is that a wrong rule already
shows with two transactions and one peer.

It has paid off. A store that read its node's RPC and then connected to the
node as a peer lost whatever the node took in between, since a node announces
to a peer nothing that it took before the peer connected. A test hit it once
in about forty runs. With the right ingredients in a world, the checker shows
it in seven steps, every time.

## What it does not prove

- Nothing about a hundred thousand transactions, or about speed.
- It is only as good as its worlds. Until a world had an RPC that stays
  silent, the bug above got through.

## Two habits that keep it honest

- **The checker runs the store's real code**, not a description of it. A pass
  is a statement about what is deployed, within those small worlds.
- **Every new rule comes with a deliberately broken version that the checker
  must catch.** A check that has never failed is not known to work.

## Where it sits among the other evidence

| Level | What it covers |
| --- | --- |
| By construction | a transaction id is in exactly one place: the places are the values of one map |
| Proved, for every state and input | nothing leaves in silence; the two bounds on memory; every step ends |
| Checked, every order of events in small worlds | the 26 worlds: the rules above, and that the store always converges |
| Sampled | thousands of random runs of a larger world, which is weaker |
| Tested against real nodes | a store that must end with exactly its node's mempool, in a dozen situations |

"Tested" is never called proved.
