# A node as a peer, through its RPC and its ZeroMQ socket

How a store can take everything from a full node of its operator without
the peer-to-peer protocol (`--node-rpc`, with `--node-zmq`). The code is
`Haskoin.Store.NodePeer` and `Haskoin.Store.NodeZmq`.

## In short

A store takes blocks and transactions from peers, over the peer-to-peer
protocol. That protocol is made for strangers. A node tells a peer what
it chooses to, at the pace that it chooses.

Somebody who runs the node has two better ways to it:

- its **RPC** answers anything, at once;
- its **ZeroMQ socket** says what the node takes, as it takes it.

Together they give all that a peer gives. So a store that is given the
RPC treats that node as one more peer. The store asks it what it asks
any peer, and a small thread answers from the RPC and from what the
node publishes.

Such a store needs no peer-to-peer connection at all: headers, blocks,
transactions and the mempool come over the RPC, and what is new is
heard over ZeroMQ. It may have peers all the same, to that node or to
others.

The two are meant to be given together. A store that has the RPC and
not the ZeroMQ socket still has the node as a peer. It then asks the
node every five seconds for what the socket would have said at once,
and it warns of it, in its log and in `/health`.

## Words used here

The shared words (store, node, peer, the pool) are in
[README.md](README.md). This page adds these.

| Word | What it means here |
| --- | --- |
| **RPC** | The interface of a node that answers questions over HTTP, with a password (`--node-rpc`). |
| **ZeroMQ** | A way for a program to publish messages that others subscribe to. Both Bitcoin Core and Bitcoin Cash Node can publish this way. |
| **publish** | To send a message to whoever subscribed, without knowing who did. Nothing can be asked back. |
| **topic** | The kind of a published message: `hashtx` for a transaction, `hashblock` for a block. |
| **counter** | A number in each published message, one more than in the last of its topic. A gap says that messages were lost. |
| **the node peer** | The peer of the store that is a node through its RPC and its ZeroMQ socket. |
| **a node of the operator** | A node that whoever runs the store also runs: the node peer, and the nodes of `--peer` unless `--foreign-peers` says otherwise. Its word is taken. |
| **a stranger** | Any other peer. What only strangers name is verified first, or waits. |
| **pipe** | The connection of the node peer: two queues in memory, with the store at one end and the thread that answers at the other. |

## What a node publishes

Each message has a topic, a body and a counter.

| Topic | Body | Bitcoin Core 32 | Bitcoin Cash Node 29 |
| --- | --- | --- | --- |
| `hashtx` | the id of a transaction | yes | yes |
| `hashblock` | the hash of a block | yes | yes |
| `rawtx`, `rawblock` | the transaction or the block itself | yes | yes |
| `sequence` | every change of the mempool and the tip | yes | no |
| `hashds`, `rawds` | a double-spend proof | no | yes |

A transaction is published when the node takes it, and again when a
block that has it becomes the tip. A block is published when it becomes
the tip.

The store subscribes to `hashtx` and `hashblock` only: the two that
both nodes have. It takes ids and not whole transactions, since a block
would otherwise send every one of its transactions again in full.

## What ZeroMQ does not give

- **Nothing can be asked.** The socket only publishes.
- **Nothing is kept.** A subscriber that connects late hears nothing of
  before, and a node drops what a slow subscriber does not read, a
  thousand messages behind by default.
- **Nothing is checked.** There is no password and no encryption.
  Whoever reaches the socket hears it. Keep it on the machine of the
  node, or on a network that you trust.

So ZeroMQ is never the only way that the store hears. When the counter
skips, or the subscription is made again, the node peer names the whole
mempool of the node, as an answer to a request for it does.

## How the node peer answers

| The store asks, as it asks any peer | The thread answers from |
| --- | --- |
| the handshake | `getblockcount` |
| headers after a block | `getblockheader`, `getblockhash` |
| a block | `getblock` |
| a transaction | `getrawtransaction` |
| the whole mempool | `getrawmempool` |
| a ping | any call that the node answers |
| (a new transaction, a new block) | ZeroMQ: `hashtx`, `hashblock` |

Because it is a peer, nothing else of the store changes:

- it counts as a peer in `/health`;
- it is a node of the operator, whose word is taken
  ([mempool-ingestion.md](mempool-ingestion.md), T8);
- it is a source: the store reads its whole mempool when it needs to.

The store could have learned a second way to take blocks, beside peers.
That would be a second path to get right and to keep right. A peer
costs the translation of some ten messages, and everything that the
store does with a peer applies as it is.

## When the node peer is there

Both connections together are the peer.

1. If the RPC fails, or the ZeroMQ socket closes, the peer is gone. The
   store sees a peer that disconnected, and connects again as to any
   peer.
2. If the node hangs, the pipe does not break: it is in memory. So the
   thread answers a ping only once the node answered a call. The store
   then finds its peer silent, and lets it go.
3. A ZeroMQ socket can go quiet without closing. So every thirty
   seconds the thread asks the node for its best block, and announces
   it if nothing did.

A store whose node is down is a store with no peer, and says so in
`/health`.

## Without the ZeroMQ socket

The RPC announces nothing. So without the socket the thread asks, every
five seconds:

1. **For the best block of the node.** It announces it if it is new.
2. **For the ids of the node's mempool.** It names those that were not
   there the last time, as a node names what it just took.

It asks for the mempool only while **no other node of the operator is a
peer of the store**. Such a peer announces what the node takes already.
While every other peer is a stranger, or there is none, this reading is
the only word of a node of the operator that the store gets, and a
transaction that only strangers named waits for it. So the store is
kept at most five seconds behind a node that it trusts.

The store warns of the missing socket in two places:

| Where | What |
| --- | --- |
| The log | A warning when the store starts, and each time that the node peer connects |
| `/health` | A sentence in `warnings`, and in `text`. The store is as healthy as without it: `ok` and the status do not change |

What it costs: the node is asked for the ids of its whole mempool every
five seconds. For sixty thousand transactions that is some four
megabytes of JSON each time. This cost was **not measured** on a
mempool of that size. The socket costs nothing of the kind: give it.

## Setting it up

Start the node with the two topics, at one address:

```
-zmqpubhashtx=tcp://127.0.0.1:28332 -zmqpubhashblock=tcp://127.0.0.1:28332
```

A Unix socket works too, and the two sides write it differently:

| Who | How the address is written |
| --- | --- |
| The store (`--node-zmq`) | `ipc:///run/node/zmq.sock` or `unix:/run/node/zmq.sock`: both mean the same |
| Bitcoin Core 32 (`-zmqpub…`) | `unix:/run/node/zmq.sock`. Given `ipc:///…` it says "Invalid port specified" and does not start |
| Bitcoin Cash Node 29 (`-zmqpub…`) | `ipc:///run/node/zmq.sock` |

This is **tested**: in the tests each node publishes on a Unix socket
that it makes itself, written as above, and Bitcoin Core was started
with `ipc://` to see it refuse. Neither node listens on a Unix socket
for its RPC or for its peers (both were tried), so for those the tests
join a TCP port inside the container to a socket.

Bitcoin Core has
ZeroMQ only if it was built with it (`-DWITH_ZMQ=ON`); Bitcoin Cash
Node has it unless it was built without.

Then give the store both. The RPC alone works, with the warning and the
cost said above:

```
haskoin-store --net btc --node-rpc http://user:password@127.0.0.1:8332/ \
  --node-zmq tcp://127.0.0.1:28332
```

With no `--peer` and no `--discover`, that node is its only peer. The
store speaks the protocol of ZeroMQ itself and needs no library for it.

## What it costs

- Blocks come one call each, as hexadecimal in JSON. That is slower than
  the peer-to-peer protocol for the first sync of a chain. For a store
  that follows the tip it does not matter.
- Each ping costs the node a call.

## How it is known

All of it is **tested** against Bitcoin Core and Bitcoin Cash Node in
Podman. Nothing here is proved or checked by a model: the machine of the
path into the pool did not change, and the node peer is to it a peer
like any other.

| Test | What it shows |
| --- | --- |
| `publishes` in `Haskoin.Store.NodeAxiomsSpec`, both nodes | The store's own subscriber hears what a node publishes: the id of each transaction, the hash of each block, the counters, and each transaction again when a block has it |
| `alone` in `Haskoin.Store.MempoolSpec`, both nodes | A store with no other peer has the node's 150 blocks, its mempool, what the node takes next and its next block, and says that it has one peer |
| `nodeStops`, both nodes | When the node hangs the store has no peer and says so, and has its node again when the node goes on |
| `hears`, Bitcoin Core | With the node as a peer both ways, the store has 800 transactions that the node took in seconds within five seconds of the last, with no reading of the mempool; with the peer-to-peer protocol alone it is late |
| `polls`, both nodes | A store with the RPC alone and no other peer has one peer, is healthy, and warns of the missing socket. Three batches of transactions thirteen seconds apart, and then a block, are each in the store within twelve seconds. With the thread asking every sixty seconds the test fails |
| `strangers True`, Bitcoin Core | A store told that its peer is a stranger, with the RPC of its own node, takes on the word of its node what it cannot verify of the stranger |
| `fresh`, a pure function | What is named is what the mempool has that it had not the last time, in the order of the node |

Not tested: a first sync of a real chain this way, a node that is down
and not only hanging, a ZeroMQ socket that goes quiet, the cost of
asking a large mempool every five seconds, and that the mempool is
**not** asked for while another node of the operator is a peer.
