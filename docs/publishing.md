# Publishing a transaction

How `POST /transactions` gets a transaction to the network, why, and how
it differs from what the store did before. Written on 2026-11-12, and
rewritten on 2026-11-13 when the wait for the echo went. The code is
`Haskoin.Store.Web.Publish`.

## The short answer

The store checks the transaction, hands it to every peer it has, each on
a connection made for that (the **courier**), and answers 202, "handed
over", as soon as they took it. It does not wait to see what the network
makes of it. A peer that takes it announces it on the connections that the
store syncs from, as it does any transaction, and the store imports it
then. Only that announcement, the **echo**, brings it into the mempool.

## The problem

The store does not run scripts. It can check the signature of a coin that
is locked to one key, which is most coins, and cannot tell whether a
script of any other kind is satisfied. So:

- It must not put a client's transaction into its mempool. Anyone could
  then show a payment, to any address, that will never confirm.
- It must not send a peer what the peer finds invalid. Bitcoin Cash Node
  drops every connection from the address that sent it.
- A node never announces a transaction to the connection that gave it.
  Sent on the connections that the store syncs from, a transaction does
  not come back to the store, sometimes until a block confirms it.

## How it works

1. **Read no more than a transaction can be long.** The body is the
   transaction in hexadecimal, so twice those sizes, and 4,096 bytes for
   white space. A longer body is answered 413: by its length when the
   client gave it, and else as soon as that much was read.
2. **Check.** The store answers from its own index what it can. Each of
   these is an answer a node would give too, and does not depend on what
   else is in a mempool:
   - a transaction it has in a block is answered 200 and nothing is sent;
     one it has unconfirmed is checked and sent again, as below;
   - no coinbase, no input twice, and no longer than a block of the network
     can hold: 4,000,000 bytes with SegWit, 1,000,000 on Bitcoin Cash;
   - every input spends an output the store knows, that no confirmed
     transaction has spent, and a coinbase only after a hundred blocks;
   - the outputs are not worth more than the inputs;
   - the lock time has passed, by height or by the median time of the
     last eleven blocks, unless every input waives it;
   - the version is 1 or 2 on Bitcoin Cash, a rule of the chain there,
     and 1 to 3 on Bitcoin, which nodes relay;
   - every input script only pushes data, and each push is in its shortest
     form;
   - on Bitcoin Cash, the tokens are written as tokens are, and the outputs
     give no tokens that the inputs do not hold (see below);
   - it is not larger than nodes relay: 400,000 units of weight with
     SegWit, 100,000 bytes on Bitcoin Cash;
   - no output is dust, worth less than three times what it costs to make
     and spend it: 546 satoshi to a key hash, 294 to a witness key hash;
   - **no signature is forged**, where there is no script to run: see
     below.

   A conflict with an unconfirmed transaction is left to the node, which
   knows the rules of replacement. A failure is a 400 with the reason.
3. **Send it over RPC, if the store was given the RPC of a node.** With
   `--node-rpc` the store gives the transaction to that node
   (`sendrawtransaction`), and the next step is skipped. The node runs
   every script, and says at once whether it took the transaction and why
   not, from Bitcoin Core too. If the node does not answer, the
   transaction goes the next way, and the log has a warning.
4. **Hand it to every peer, only what was verified.** A transaction goes
   to the peers only if the store verified the signature of every input;
   one with an input that the store cannot judge is refused with 400. The
   courier connects to each peer that takes transactions, all at once,
   and sends the transaction and then a `ping`. A node handles the
   messages of a connection in order, so its `pong` says that it is done
   with the transaction, which takes milliseconds.
5. **Answer.** 202 with the id and a message, once a peer took it. Bitcoin
   Cash Node says when it refuses (`reject`), and when every peer that
   answered refused the transaction, the answer is 400 with the first
   reason. Bitcoin Core refuses without a word.
6. **It comes back.** The connections that the store syncs from never had
   the transaction. A node that took it announces it there, and the block
   store asks for it and imports it as any other. In case none does, the
   store also asks for it a few times over two minutes and a half, as if
   a peer had announced it.

Posting the same transaction again is always safe. Each time, it is
checked and handed over again.

### A transaction the store has unconfirmed

The store does not hold its mempool to its node's. It may list a
transaction that the nodes forgot: after a reorganisation that left it
out, or when a node lost its mempool. Posting it again hands it to the
peers again, as any other, and gets it back to them. A node that has it
ignores it.

## What would cost the store its peers

Bitcoin Cash Node drops every connection of a peer that sends it a
transaction that breaks a rule of the chain, unless it whitelists that
peer. Bitcoin Core 32 drops none; Bitcoin Core 29 and older drop a peer
for a transaction that breaks a rule of the chain, by their source.

On 2026-11-13 each kind of transaction below was posted to a store whose
node did not whitelist it, and the node's peer ids were compared before
and after. These are the transactions that passed the store's checks of
1.12.1:

| Transaction | Bitcoin Core 32.1 | Bitcoin Cash Node 29.2.0 |
| --- | --- | --- |
| No fee | refused in silence | `reject`, connection kept |
| A double spend of a payment the node has, paying less | refused in silence | `reject`, connection kept |
| Version 0 or 7 | refused in silence | `reject`, connection kept |
| Version 3 | taken | `reject`, connection kept |
| A signature pushed in a longer form than it needs | refused in silence | **every connection closed** |
| More tokens out than in | | **every connection closed** |

So the store refuses the last two itself now, and the versions, which no
node relays anyway. A signature on Bitcoin Cash without the fork id
cannot be verified (haskoin-core does not make its digest), and an input
script with more than a signature and a key cannot be judged: neither is
sent to a peer.

### Tokens

The rules of CashTokens, which a node holds every transaction to:

- the tokens of every output are written as tokens are;
- a category is new when it is the id of a transaction whose first output
  an input spends, and its tokens may be made at will;
- of any other category, the outputs hold no more fungible tokens than
  the inputs, and no more than the largest amount in all;
- and its non-fungible tokens come from the inputs: any of them when an
  input holds one that can mint; else each one equals one held that cannot
  be changed, or takes the place of one held that can, and none can mint.

## Signatures

The store refuses a transaction with a forged signature, for the coins it
can judge: those locked to one key and spent by that key.

| Coin | Network |
| --- | --- |
| Key hash (P2PKH), key (P2PK) | both; on Bitcoin Cash with ECDSA or Schnorr signatures, and with tokens |
| Witness key hash (P2WPKH), plain or nested in a script hash | Bitcoin |
| Taproot, spent by its key | Bitcoin |

It says forged only when the input has exactly the items such a coin
takes, the hash type is "all", and the key is not the one the coin names
or the signature does not verify. A wrong "forged" would turn away a
good transaction.

It verifies more than it calls forged. An input of a standard kind whose
signatures verify is verified, whatever its hash type: the coins of the
table, and multisig with as many signatures as it asks for:

| Multisig | Network |
| --- | --- |
| Bare, in a script hash, or in a witness script hash, with ECDSA signatures | both, the witness forms on Bitcoin |
| The same with Schnorr signatures, and a field of bits that says which keys signed | Bitcoin Cash |
| Of Taproot: a script with a Schnorr signature for each key that signs (`multi_a`) | Bitcoin |
| Of Taproot, with the keys made into one (MuSig) | Bitcoin: it is a spend by one key |

What it can do neither with is the rest: a script that is not one of
those, and a multisig that does not verify. This is `judgeInputs` of
haskoin-core, held to what Bitcoin Cash Node does: its test vectors, and
transactions signed by its wallet.

What the store cannot judge it does not send to a peer. Such a
transaction is published over the RPC of a node, where the store has one,
and refused where it has none.

## The answers

| Answer | When |
| --- | --- |
| 202, the id and a message | Handed over: a node took it over RPC, or one peer or more took it |
| 200 and the id | The store has it in a block: nothing was sent |
| 400 | The store refused it at step 2, or could not verify it and has no RPC to send it over, or a node refused it over RPC, or every peer that answered refused it |
| 413 | The body is longer than a transaction can be |
| 500 `server-error` | No peer to hand it to, or none could be reached |

A 202 does not say that the network took the transaction. The client finds
out as for any transaction: it shows in the store when a node announces
it, and in a block.

## Against abuse

- **Nothing a client wrote is in the mempool** before a node announced it.
- **Only what is verified reaches a peer**, and nothing that was measured
  to cost the store its connections.
- **Transactions the store has in a block, and spends of unknown or spent
  outputs, cost no traffic.**
- **Asking for what was handed over is bounded**: 10,000 transactions at
  once; past that, a transaction comes to the store when a node announces
  it or a block confirms it.
- **There is no breaker.** A node that closes the courier on a transaction
  may have closed the store's other connections too. The store does not
  stop publishing because of it: whoever posts such transactions would
  then keep everyone else from publishing. The store connects to its
  peers again within seconds.

## What was measured

Against Bitcoin Core 32.1 and Bitcoin Cash Node 29.2.0 on regtest, the
node's only peer being the store, unless said otherwise.

How long the store takes to have a transaction that it handed over:

| Node | Courier, a connection of its own | On the connections the store syncs from |
| --- | --- | --- |
| Whitelists the store | under a second | 3.3 s, when it came back at all |
| Bitcoin Core, does not whitelist it | seconds: the timer of announcements, five seconds on average | 28 s |
| Bitcoin Cash Node, does not whitelist it | about a second | not in 200 s |

Bitcoin Cash Node gives a peer a transaction that the peer sent it only
once it relayed it to another peer; Bitcoin Core only once it announced
something else to that peer. With no other peer and nothing else going
on, that may not happen before a block. Hence the courier.

From the measurements of 1.11.0, with a program that speaks the
peer-to-peer protocol:

| | Core, not whitelisted | BCHN, not whitelisted | Either, whitelisted |
| --- | --- | --- | --- |
| `pong` after the transaction | under 1 ms | under 1 ms | under 1 ms |
| Echo on another connection | 3 to 13 s | 0.05 to 1.2 s | under 1 ms |
| Bad signature | nothing said, connection kept | `reject`, then every connection from the address closed | Core: nothing; BCHN: `reject` |
| Announced first (`inv`) | asked for after 2 s | asked for after 2 s | not measured |

## What it cannot do

- **Without the RPC of a node, the store publishes only what it can
  verify**: coins locked to one key, and multisig. A spend of a script
  of another kind is refused, and on Bitcoin Cash a signature with the
  hash type that covers every coin spent (`SIGHASH_UTXOS`).
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
  lowest fee rate it relays, but Bitcoin Core rounds that number at random,
  and a check against it would refuse some transactions the node takes.

**Whitelist the store on the nodes you run it next to** (`-whitelist` in
Bitcoin Core and Bitcoin Cash Node). The node then announces without
delay, and never closes the store's connections over a transaction.

## What the store did before

| | Before 1.11.0 | 1.11.0 to 1.12.1 | Now |
| --- | --- | --- | --- |
| Where it is sent | Half the peers, on the connections the store syncs from | One node, on a connection of its own | Every peer, each on a connection of its own |
| What the client is told | 200 at once, whatever happens | What came of it, after up to `--publish-timeout` | 202, handed over, at once |
| How it comes back | The store asks the same peers once, five seconds later | A node announces it | A node announces it; the store asks too |
| What the store checks | That the body is one transaction | The inputs, the amounts, the signatures | Also the input scripts, the version and the tokens |

- **Before 1.11.0**, a forged transaction went to half the peers on the
  connections the store syncs from, and Bitcoin Cash Node closed every
  connection from the address: one request cut the store off from half
  its peers. And the transaction often never came back, as above.
- **From 1.11.0**, the request waited for the echo, up to 30 seconds. The
  wait was arbitrary, and the answer said less than it seemed: Bitcoin Core
  refuses in silence, so a timeout meant nothing for sure, and one node is
  not the network.

## What was not chosen

- **Needing the RPC of a node.** The store speaks to nodes as a peer, and
  that is all it needs to run. The RPC is an option, and the better way
  when it is there.
- **Running scripts in the store.** That takes an interpreter of every
  script of both chains, held to the nodes rule for rule. The store checks
  the signature where there is no script to run, and leaves the rest.
- **Sending on the connections the store syncs from.** The transaction
  does not come back, as measured above, and a peer that closes the
  connection takes the store's sync with it.
- **Announcing the transaction** (`inv`) and waiting to be asked. It costs
  two seconds, and a node that knows the transaction, or refused it, does
  not ask.

## How it is tested

- haskoin-core, `VerdictSpec` and `SignatureSpec`: the Schnorr signatures
  of Bitcoin Cash Node and transactions signed by its wallet; inputs of
  each kind in the tables above, verified, forged and unseen.
- `Haskoin.Store.Web.PublishSpec`: the checks of step 2, the rules of
  tokens, and publishing to peers that do as they are told: take it,
  refuse it, close the connection, cannot be reached; over RPC; a
  transaction the store has in a block or unconfirmed; and the schedule of
  asking for it back.
- `IntegrationSpec`, "hands what it verified to its peers": against
  Bitcoin Core and Bitcoin Cash Node, a good transaction is answered 202,
  is in the node's mempool, and comes to the store. A forged one, an
  unknown or spent output and a body that is too long are refused. One
  that pays no fee is refused with the reason on Bitcoin Cash, handed over
  on Bitcoin, and never shows. One in a block is answered 200.
- `IntegrationSpec`, "that does not whitelist the store": what the store
  cannot verify, a push in a longer form than it needs and, on Bitcoin
  Cash, more tokens out than in are refused; the store keeps the
  connection it had to its node all along, and hands over the next good
  one.
- `IntegrationSpec`, "publishes over the RPC of its node", "publishes
  spends of a multisig", "publishes transactions that make and spend
  tokens" and "sends again a payment that the nodes forgot".

Not tested: a store with several peers.
