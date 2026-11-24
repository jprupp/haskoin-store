# The documents of haskoin-store

These pages say how the store works inside, and why. They describe the
code; they are not part of the package. The [README](../README.md) is
for running and using the store.

Each page starts with the problem it solves, in a few sentences, and
with the words it uses. Read as far as you need.

## Where to start

Read these three, in this order. Together they are the whole design.

1. [one-fleet.md](one-fleet.md): what the store is built around, in five
   rules.
2. [tip-first.md](tip-first.md): what happens when a block arrives.
3. [fleet-mempool.md](fleet-mempool.md): the shared mempool, all of it.

## Every page

| Page | The question it answers |
| --- | --- |
| [one-fleet.md](one-fleet.md) | Why does every store need Redis, and what do the stores of a fleet share? |
| [tip-first.md](tip-first.md) | What happens to the mempool when a block arrives, and how is it checked? (`scripts/check-pool.sh`) |
| [fleet-mempool.md](fleet-mempool.md) | How does a transaction get into the shared mempool, stay there, and leave? |
| [mempool-ingestion.md](mempool-ingestion.md) | How does a store end up with exactly the mempool of its node, and how do we know? |
| [node-peer.md](node-peer.md) | How can a store take everything from my own node, through its RPC and its ZeroMQ socket, with no peer-to-peer connection? |
| [the-19-worlds.md](the-19-worlds.md) | How is that path checked for every order in which things can happen? (`scripts/check-ingest.sh`) |
| [concurrency.md](concurrency.md) | What runs at the same time, who may change what, and why does nothing break? |
| [xpub-indexing.md](xpub-indexing.md) | How are the questions about an extended public key answered fast? |
| [block-import.md](block-import.md) | How is a block written to the database, and what does it cost? |
| [publishing.md](publishing.md) | What does `POST /transactions` do with a transaction? |
| [congestion-control.md](congestion-control.md) | How many heavy requests does a store answer at once? |
| [cashtokens.md](cashtokens.md) | How does the store read, index and serve the tokens of Bitcoin Cash? |
| [hsq.md](hsq.md) | How do I ask a store something from the command line? |
| [grafana.md](grafana.md) | How do I see a store's metrics on a dashboard? |

## Words used on every page

A page lists the words of its own subject. These are the ones they share.

| Word | What it means here |
| --- | --- |
| **store** | One running `haskoin-store`: a database, a web server, and connections to nodes. |
| **node** | A full node of the network that a store connects to: Bitcoin Core or Bitcoin Cash Node. |
| **peer** | A node, seen from the store, over the peer-to-peer protocol. |
| **fleet** | The stores that share one Redis. A store alone is a fleet of one. |
| **the chain** | The blocks, in a store's own database (RocksDB). Nothing else is in it. |
| **mempool** | The transactions that no block has confirmed yet. Each node has one. |
| **the pool** | The mempool of a fleet: one, in Redis, shared by its stores. |
| **the tip** | The block that requests are answered from. It is kept in Redis, so every store of a fleet answers from the same block. It is also called the fleet tip. |
| **view** | A store's database as it was at one block. A request reads the view at the tip, and lays the pool on top. |
| **xpub** | An extended public key: one key from which a wallet derives all its addresses. |
| **the cache** | The answers about xpubs that the fleet keeps in Redis. Each cached xpub has an **entry**. |
| **decision** | One change to what Redis holds, worked out in Haskell and then applied in one step, only if nothing it depended on has changed meanwhile. |
| **script** | A small Lua program that Redis runs in one go, without doing anything else meanwhile. |
| **turn** | One round of a store's upkeep, every two seconds or so: it reports its newest blocks, moves the tip if it should, and cleans up. |

## How sure we are

The pages use five words for how a claim is known, from strongest to
weakest. They are never mixed up.

| Word | What was done |
| --- | --- |
| **by construction** | The code cannot be written any other way: the types, or the one function that everything goes through, leave no other possibility. |
| **proved** | A tool (Liquid Haskell) checked it for every possible state and input. |
| **checked** | A program went through every possible order of events in a small example. |
| **sampled** | Random cases were tried, hundreds or thousands of them. The page says how many. |
| **tested** | Particular cases were tried, often against a real node or a real Redis. |
