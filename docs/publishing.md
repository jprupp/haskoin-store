# Publishing a transaction

What `POST /transactions` does to get a transaction to the network, and
why. The code is `Haskoin.Store.Web.Publish`. This page describes the
code; it is not part of the package.

## In short

A client posts a transaction, and the store has to get it to the
network. The store does not run scripts, so it cannot always tell whether
a transaction is valid. So it checks what it can, and hands the
transaction to every peer only if it verified every signature. Each peer
gets it on a connection made for that, and the store answers 202,
"handed over", as soon as they took it. The transaction enters the pool
only when a node announces it back to the store. The cost: a 202 does not
say that the network took the transaction, and without the RPC of a node
the store publishes only what it can verify.

## Words used here

The shared words, such as store, peer and the pool, are in
[README.md](README.md).

| Word | What it means here |
| --- | --- |
| **hand over** | Give the transaction to a node. It says nothing of whether the node keeps it. |
| **check** | A test of the transaction that the store does itself, before it sends anything. |
| **verified, forged, cannot judge** | What the store says of the signatures of one input: they are right, one is wrong, or the store cannot tell. |
| **courier** | A connection to one peer, made to hand over one transaction and closed after. |
| **sync connection** | One of the lasting connections on which the store gets blocks and transactions from a peer. The part of the store that reads them is the **block store**. |
| **echo** | A node announces, on a sync connection, a transaction that the store handed over. Only the echo brings it into the pool. |
| **relay** | A node passes a transaction on to its own peers. A *rule of relay* is a node's own rule for what it passes on. A *rule of the chain* is one that every block must keep. |
| **announce**, `inv` | `inv` is the message "I have this transaction". To announce is to send it. The other side then asks for the transaction if it wants it. |
| `ping`, `pong`, `reject` | `ping` is the message "are you there?", and `pong` is its answer. With `reject`, Bitcoin Cash Node says that it refuses a transaction, and why. |
| **whitelisted peer** | A peer that a node was told to trust (`-whitelist`). The node announces to it without delay, and never closes its connections over a transaction. |
| **the RPC** | The interface of a node for its operator, with a password. The store has it only with `--node-rpc`. |

## The problem

The store does not run scripts. It can check the signature of a coin that
is locked to one key, which is most coins. It cannot tell whether a
script of any other kind is satisfied. So:

- **A client's transaction must not go straight into the pool.** Anyone
  could then show a payment, to any address, that will never confirm.
- **A peer must not be sent what it finds invalid.** Bitcoin Cash Node
  drops every connection from the address that sent it.
- **A transaction sent on a sync connection does not come back.** A node
  never announces a transaction to the connection that gave it. The store
  would not see it, sometimes until a block confirms it.

## How it works

1. **Read the body, up to a limit.** The body is the transaction in
   hexadecimal. The limit is twice the largest size of a transaction (in
   the table below), plus 4,096 bytes for white space. A longer body is
   answered 413: by its length when the client gave it, or else as soon
   as that much was read.
2. **Check.** A transaction that the store has in a block is answered
   200, and nothing is sent. Any other goes through the checks in the
   table below. That includes one that the pool has: it is checked and
   sent again. A failed check is a 400 with the reason.
3. **Send it over the RPC, if the store has one** (`--node-rpc`). The
   store gives the transaction to that node (`sendrawtransaction`), and
   step 4 is skipped. The node runs every script. It says at once whether
   it took the transaction, and why not: over the RPC, Bitcoin Core says
   so too. If the node does not answer, the log has a warning, and the
   transaction goes on to step 4.
4. **Hand it to every peer, only if it was verified.** If the store
   cannot judge an input, the answer is 400 and nothing is sent.
   Otherwise the store makes a courier to each peer that takes
   transactions, all at once. Each courier sends the transaction and then
   a `ping`. A node handles the messages of a connection in order, so its
   `pong` says that it is done with the transaction. That takes
   milliseconds.
5. **Answer.** 202 with the id and a message, once a peer took it.
   Bitcoin Cash Node says when it refuses (`reject`), so a `pong` with no
   `reject` before it means that it kept the transaction. Bitcoin Core
   refuses without a word, so its `pong` means only that it is done with
   the transaction. The courier knows which node does which from the
   model of the node (`expectsReject` of haskoin-node). When every peer
   that answered refused, the answer is 400 with the first reason.
6. **It comes back.** The sync connections never had the transaction. So
   a node that took it announces it there, and the block store asks for
   it and offers it to the pool as any other. In case no node does, the
   store also asks for it, as if a peer had announced it: after two
   seconds, then every 25 seconds for two minutes and a half.

Posting the same transaction again is always safe. Each time, it is
checked and handed over again.

### The checks of step 2

The store answers them from the chain at the tip and from the pool. A
node would give each of these answers too, and none depends on what else
is in a mempool.

| Check | The rule |
| --- | --- |
| It is a transaction | It has inputs and outputs, is not a coinbase, and has no input twice. |
| Largest size | No longer than a block of the network can hold: 4,000,000 bytes with SegWit, 1,000,000 on Bitcoin Cash. |
| Coins | Every input spends an output that the store knows, and that no confirmed transaction has spent. A coinbase is spent only after a hundred blocks. |
| Amounts | The outputs are not worth more than the inputs. |
| Lock time | It has passed, by height or by the median time of the last eleven blocks, unless every input waives it. |
| Version | 1 or 2 on Bitcoin Cash, a rule of the chain there. 1 to 3 on Bitcoin, which nodes relay. |
| Input scripts | Every input script only pushes data, and each push is in its shortest form. |
| Tokens | On Bitcoin Cash, the tokens are written in the format of tokens, and the outputs give no tokens that the inputs do not hold ([Tokens](#tokens)). |
| Size of relay | Not larger than nodes relay: 400,000 units of weight with SegWit, 100,000 bytes on Bitcoin Cash. |
| Dust | No output is dust, worth less than three times what it costs to make and spend it: 546 satoshi to a key hash, 294 to a witness key hash. |
| **Signatures** | **No signature is forged**, where there is no script to run ([Signatures](#signatures)). |

A conflict with an unconfirmed transaction is left to the node, which
knows the rules of replacement.

### A transaction the store has unconfirmed

The pool is not held to the nodes' mempools. It may list a transaction
that the nodes forgot: after a reorganisation that left it out, or when a
node lost its mempool. Posting it again hands it to the peers again, as
any other, and gets it back to them. A node that has it ignores it.

## What would cost the store its peers

Bitcoin Cash Node drops every connection of a peer that sends it a
transaction that breaks a rule of the chain, unless it whitelists that
peer. Bitcoin Core 32 drops none. Bitcoin Core 29 and older drop a peer
for a transaction that breaks a rule of the chain, by their source.

Each kind of transaction below was posted to a store whose node did not
whitelist it. The node's peer ids were compared before and after:

| Transaction | Bitcoin Core 32.1 | Bitcoin Cash Node 29.2.0 |
| --- | --- | --- |
| No fee | refused in silence | `reject`, connection kept |
| A double spend of a payment the node has, paying less | refused in silence | `reject`, connection kept |
| Version 0 or 7 | refused in silence | `reject`, connection kept |
| Version 3 | taken | `reject`, connection kept |
| A signature pushed in a longer form than it needs | refused in silence | **every connection closed** |
| More tokens out than in | | **every connection closed** |

So the store refuses the last two itself. It refuses the versions too,
which no node relays anyway. Two more things are never sent to a peer. A
signature on Bitcoin Cash without the fork id cannot be verified:
haskoin-core does not make its digest. And an input script with more than
a signature and a key cannot be judged.

### Tokens

The rules of CashTokens, which a node holds every transaction to:

- The tokens of every output are written in the format of tokens.
- A category is new when it is the id of a transaction whose first output
  an input spends. Its tokens may be made at will.
- Of any other category, the outputs hold no more fungible tokens than
  the inputs, and no more than the largest amount in all.
- And its non-fungible tokens come from the inputs. When an input holds
  one that can mint, the outputs may hold any. Else none can mint, and
  each one equals one held that cannot be changed, or takes the place of
  one held that can.

## Signatures

The store refuses a transaction with a forged signature, for the coins it
can judge: those locked to one key and spent by that key.

| Coin | Network |
| --- | --- |
| Key hash (P2PKH), key (P2PK) | both; on Bitcoin Cash with ECDSA or Schnorr signatures, and with tokens |
| Witness key hash (P2WPKH), plain or nested in a script hash | Bitcoin |
| Taproot, spent by its key | Bitcoin |

**Forged** is said only when all of this holds: the input has exactly the
items such a coin takes, the hash type is "all", and the key is not the
one the coin names or the signature does not verify. A wrong "forged"
would turn away a good transaction.

**Verified** is said of more than that. An input of a standard kind whose
signatures verify is verified, whatever its hash type. The standard kinds
are the coins of the table above, and multisig with as many signatures as
it asks for:

| Multisig | Network |
| --- | --- |
| Bare, in a script hash, or in a witness script hash, with ECDSA signatures | both, the witness forms on Bitcoin |
| The same with Schnorr signatures, and a field of bits that says which keys signed | Bitcoin Cash |
| Of Taproot: a script with a Schnorr signature for each key that signs (`multi_a`) | Bitcoin |
| Of Taproot, with the keys made into one (MuSig) | Bitcoin: it is a spend by one key |

**Cannot judge** is the rest: a script that is not one of those, and a
multisig that does not verify. Such a transaction is not sent to a peer.
It is published over the RPC, where the store has one, and refused where
it has none.

This is `judgeInputs` of haskoin-core, held to what Bitcoin Cash Node
does: its test vectors, and transactions signed by its wallet.

## The answers

| Answer | When |
| --- | --- |
| 202, the id and a message | Handed over: a node took it over RPC, or one peer or more kept the connection and did not refuse it. A peer that had not answered after the courier's ten seconds counts among those |
| 200 and the id | The store has it in a block: nothing was sent |
| 400 | Refused at step 2; or not verifiable, with no RPC; or refused by the node over RPC; or no peer kept it, and one sent a `reject` or closed the connection on receiving it |
| 413 | The body is longer than a transaction can be |
| 500 `server-error` | No peer to hand it to, or none could be reached |

A 202 does not say that the network took the transaction. The client finds
out as for any transaction: it shows in the store when a node announces
it, and in a block.

## Against abuse

- **Nothing a client wrote is in the pool** before a node announced it.
- **Only what is verified reaches a peer**, and nothing that was measured
  to cost the store its connections.
- **Transactions the store has in a block, and spends of unknown or spent
  outputs, cost no traffic.**
- **Asking for what was handed over is bounded**: 10,000 transactions at
  once. Past that, a transaction comes to the store when a node announces
  it or a block confirms it.
- **There is no breaker.** A node that closes the courier on a transaction
  may have closed the store's other connections too. The store does not
  stop publishing because of it. If it did, whoever posts such
  transactions would keep everyone else from publishing. The store
  connects to its peers again within seconds.

## What was measured

Against Bitcoin Core 32.1 and Bitcoin Cash Node 29.2.0 on regtest. The
node's only peer was the store, unless said otherwise.

How long the store takes to have a transaction that it handed over:

| Node | Courier, a connection of its own | On the sync connections |
| --- | --- | --- |
| Whitelists the store | under a second | 3.3 s, when it came back at all |
| Bitcoin Core, does not whitelist it | seconds: the timer of announcements, five seconds on average | 28 s |
| Bitcoin Cash Node, does not whitelist it | about a second | not in 200 s |

The reason: a node gives a transaction back to the peer that sent it only
after something else happened. Bitcoin Cash Node waits until it relayed
the transaction to another peer. Bitcoin Core waits until it announced
something else to that peer. With no other peer and nothing else going
on, that may not happen before a block. Hence the courier.

With a program that speaks the peer-to-peer protocol:

| | Core, not whitelisted | BCHN, not whitelisted | Either, whitelisted |
| --- | --- | --- | --- |
| `pong` after the transaction | under 1 ms | under 1 ms | under 1 ms |
| Echo on another connection | 3 to 13 s | 0.05 to 1.2 s | under 1 ms |
| Bad signature | nothing said, connection kept | `reject`, every connection closed | Core: nothing; BCHN: `reject` |
| Announced first (`inv`) | asked for after 2 s | asked for after 2 s | not measured |

## What it cannot do

- **Without the RPC, the store publishes only what it can verify**: coins
  locked to one key, and multisig. It refuses a spend of a script of
  another kind. On Bitcoin Cash it also refuses a signature with the hash
  type that covers every coin spent (`SIGHASH_UTXOS`).
- **It does not know whether the network took a transaction.** Bitcoin
  Core does not say when it refuses, and a node that refuses may not be
  the only one.
- **The child of a transaction that has not come back yet is refused** at
  step 2: the store does not know the outputs it spends. With a node that
  whitelists the store that is a matter of a second.
- **The rules of relay are the node's to change.** The store holds the
  size and the dust rule at the values nodes have had for years. A node
  set otherwise takes transactions that the store refuses.
- **The fee is not checked** by the store. A node tells its peers the
  lowest fee rate it relays, but Bitcoin Core rounds that number at
  random. A check against it would refuse some transactions that the node
  takes.

**Whitelist the store on the nodes you run it next to** (`-whitelist` in
Bitcoin Core and Bitcoin Cash Node), and say what they grant with
`--peer-permission`. The node then announces without delay, and never
closes the store's connections over a transaction.

## What was not chosen

| Not chosen | Why |
| --- | --- |
| Needing the RPC of a node | The store speaks to nodes as a peer, and that is all it needs to run. The RPC is an option, and the better way when it is there. |
| Running scripts in the store | That takes an interpreter of every script of both chains, held to the nodes rule for rule. The store checks the signature where there is no script to run, and leaves the rest. |
| Sending on the sync connections | The transaction does not come back, as measured above. And a peer that closes the connection takes the store's sync with it. |
| Announcing the transaction (`inv`) and waiting to be asked | It costs two seconds, and a node that knows the transaction, or refused it, does not ask. |

## How it is tested

| Where | What |
| --- | --- |
| haskoin-core, `VerdictSpec` and `SignatureSpec` | The Schnorr signatures of Bitcoin Cash Node and transactions signed by its wallet. Inputs of each kind in the tables above: verified, forged and unseen. |
| `Haskoin.Store.Web.PublishSpec` | The checks of step 2 and the rules of tokens. Publishing to peers that do as they are told: take it, refuse it, close the connection, cannot be reached. Publishing over RPC. A transaction the store has in a block or unconfirmed. The schedule of asking for it back. |
| `IntegrationSpec`, "hands what it verified to its peers" | Against Bitcoin Core and Bitcoin Cash Node. A good transaction is answered 202, is in the node's mempool, and comes to the store. A forged one, an unknown or spent output and a body that is too long are refused. One that pays no fee is refused with the reason on Bitcoin Cash, handed over on Bitcoin, and never shows. One in a block is answered 200. |
| `IntegrationSpec`, "that does not whitelist the store" | Three things are refused: what the store cannot verify, a push in a longer form than it needs and, on Bitcoin Cash, more tokens out than in. The store keeps the connection it had to its node all along, and hands over the next good one. |
| `IntegrationSpec`, four more tests | "publishes over the RPC of its node", "publishes spends of a multisig", "publishes transactions that make and spend tokens" and "sends again a payment that the nodes forgot". |

Not tested: a store with several peers.
