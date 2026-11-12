# Publishing a transaction

How `POST /transactions` gets a transaction to the network since version
1.11.0, why, and how it differs from what the store did before. Written on
2026-11-12. The code is `Haskoin.Store.Web.Publish`.

## The short answer

The store does not broadcast. It hands the transaction to one full node on
a connection made for that, the **courier**, and listens on the connections
it syncs from for a node to announce the transaction back, the **echo**.
Only the echo brings the transaction into the mempool. The request is
answered when that has happened, or when it is known that it will not.

## The problem

The store does not run scripts. It can check the signature of a coin that
is locked to one key, which is most coins, and cannot tell whether a
script of any other kind is satisfied. So:

- It must not put a client's transaction into its mempool. Anyone could
  then show a payment, to any address, that will never confirm.
- It should not send one over the connections it syncs from. A node that
  finds a transaction invalid may drop every connection from the address
  that sent it.
- It cannot know by itself whether the network took the transaction, and
  the client wants to know.

## How it works

1. **Read no more than a transaction can be long.** The body is the
   transaction in hexadecimal, so twice those sizes, and 4,096 bytes for
   white space. A longer body is answered 413: by its length when the
   client gave it, and else as soon as that much was read.
2. **Check.** The store answers from its own index what it can. Each of
   these is an answer a node would give too, and does not depend on what
   else is in a mempool:
   - a transaction it has already, confirmed or not, is answered 200 and
     nothing is sent;
   - no coinbase, no input twice, and no longer than a block of the network
     can hold: 4,000,000 bytes with SegWit, 1,000,000 on Bitcoin Cash;
   - every input spends an output the store knows, that no confirmed
     transaction has spent, and a coinbase only after a hundred blocks;
   - the outputs are not worth more than the inputs;
   - the lock time has passed, by height or by the median time of the
     last eleven blocks, unless every input waives it;
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
   every script and holds the transaction to its own rules and mempool.
   Its no comes with a reason, from Bitcoin Core too, and costs the store
   nothing: a node does not close the connections of who it refuses over
   RPC. If the node does not answer, the transaction goes the next way,
   and the log has a warning.
4. **Hand over to a peer, only what was verified.** A transaction goes
   to a peer only if the store verified the signature of every input.
   One with an input that the store cannot judge is refused with 400: a
   peer that finds it invalid may close every connection of the store.
   The courier connects to a node the store syncs from, and
   sends the transaction and then a `ping`. A node handles the messages of
   a connection in order, so its
   `pong` says that it is done with the transaction. If the node cannot be
   reached, the next one is tried.
5. **Listen.** The connections that the store syncs from never mentioned
   the transaction. A node that accepted it announces it there, as it does
   any transaction, and the block store asks for it and imports it, as it
   does any transaction.
6. **Answer.** The request waits until the store says that the transaction
   entered the mempool, for `--publish-timeout` seconds (30) at most. In a
   fleet that is what the fleet says.
7. **Remember.** A transaction on its way, and one that failed, are kept
   for ten minutes by the hash of their bytes. The same bytes posted again
   join the first request, or get its answer, and are not sent again.

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
those, and a multisig that does not verify, of which it does not say
that no node takes it.

This is `judgeInputs` of haskoin-core. On Bitcoin Cash it needed two
things that haskoin-core did not have: the Schnorr signatures that most
wallets there make, which are not those of BIP 340, and the digest of a
coin that holds tokens. Both are held to what Bitcoin Cash Node does: its
test vectors, and transactions signed by its wallet.

What the store cannot judge it does not send to a peer. A transaction
with such an input is published over the RPC of a node, where the store
has one, and refused where it has none. So a store without the RPC of a
node publishes payments from coins of one key and from multisig, and not
from a script of another kind: give it the RPC of a node to publish
those.

That is the price of not being cut off. A peer that is sent a transaction
it finds invalid may close every connection of the sender, and Bitcoin
Cash Node does. If the store sent on what it could not judge, anyone
could make it lose its peers with one request, again and again.

## The answers

| Answer | When |
| --- | --- |
| 200 and the id | The store has the transaction: a node announced it back, or it was there already |
| 400 | The store refused it at step 2, or could not verify it and has no RPC to send it over, or a node refused it and said why: over RPC, or Bitcoin Cash Node as a peer |
| 413 | The body is longer than a transaction can be |
| 500 `server-timeout` | A node took it and none announced it in time: most likely it was refused |
| 500 `server-error` | No node could be reached, or a node closed the connection on receiving it |

Posting the same transaction again is always safe. If it came in the
meantime the answer is 200.

## Against abuse

- **Nothing a client wrote is in the mempool** before a node announced it.
- **One node gets a transaction, once.** Repeats, transactions the store
  has, and spends of unknown or spent outputs cost no traffic.
- **The outbox holds 1,000 transactions.** More are turned away.
- **A transaction that failed is not sent again** for ten minutes: one
  that a node refused, one that a node closed the connection on, and one
  that was not announced.
- **There is no breaker.** A node that closes the courier on a transaction
  has found it invalid, and may have closed the store's other connections
  too. The store does not stop publishing other transactions because of
  it. Whoever posts forged transactions would then keep everyone else
  from publishing, and that is worse than what a breaker guards against:
  the store connects to its node again within seconds. See the limits.

## What was measured

Against Bitcoin Core 32.1 and Bitcoin Cash Node 29.2.0 on regtest, with a
program that speaks the peer-to-peer protocol over two connections: one
that hands a transaction over and one that listens.

| | Core, not whitelisted | BCHN, not whitelisted | Either, whitelisted |
| --- | --- | --- | --- |
| `pong` after the transaction | under 1 ms | under 1 ms | under 1 ms |
| Echo on the other connection | 3 to 13 s | 0.05 to 1.2 s | under 1 ms |
| Asked for it back on the same connection | `notfound` at 5 s in 3 runs of 4 | given after 1 to 3 s | given at 1 s |
| Bad signature | nothing said, connection kept | `reject`, then every connection from the address closed | Core: nothing; BCHN: `reject` |
| No fee | nothing said | `reject` | the same |
| Announced first (`inv`) | asked for after 2 s | asked for after 2 s | not measured |

Bitcoin Core announces to a peer it did not choose on a timer of five
seconds on average, for privacy, and will not give a transaction on a
connection before that timer has run for it. Bitcoin Core 29 and older
close the connections of a peer that sends an invalid transaction, as
Bitcoin Cash Node does; this is from their source, not measured.

## What it cannot do

- **Without the RPC of a node, the store publishes only what it can
  verify**: coins locked to one key, and multisig. A spend of a script
  of another kind is refused, and on Bitcoin Cash a signature with the
  hash type that covers every coin spent (`SIGHASH_UTXOS`).
- **Bitcoin Core does not say to a peer that it refused.** The client
  learns it when the wait is over, unless the store has the RPC of the
  node.
- **The rules of relay are the node's to change.** The store holds the
  size and the dust rule at the values nodes have had for years. A node
  set otherwise takes transactions that the store refuses.
- **A node that does not whitelist the store is slow to announce**: five
  seconds on average for Bitcoin Core.
- **The child of a transaction that is still on its way is refused** at
  step 2, until the parent has come back.
- **The fee is not checked** by the store. A node tells its peers the
  lowest fee rate it relays, but Bitcoin Core rounds that number at random,
  and a check against it would refuse some transactions the node takes.

**Whitelist the store on the nodes you run it next to** (`-whitelist` in
Bitcoin Core and Bitcoin Cash Node). The node then announces without
delay, so the answer takes milliseconds, and never closes the store's
connections over a transaction.

## What the store did before

The principle was the same: the store sent the transaction to nodes and
never to its own mempool, and took it in only when a node gave it back.
Three things were different, and each one failed in a way that was
measured or read in the code.

| | Before 1.11.0 | Now |
| --- | --- | --- |
| Where it is sent | To half the peers, on the connections the store syncs from | To one node, on a connection of its own |
| How it comes back | The store asks the same peers for it once, five seconds later | A node announces it on a connection that never sent it |
| What the client is told | 200 at once, whatever happens | What came of it |
| What the store checks | That the body is one transaction | Also the inputs and the amounts |
| The same transaction again | Sent again each time | Sent once |
| No peer | 200 | 500 |

- **Asking the same peer.** A node does not announce a transaction to the
  peer that gave it, and Bitcoin Core answers `notfound` to a request for
  it until its timer has run. Three times in four the one request at five
  seconds was too early, and the store did not ask again. With one node
  that does not whitelist the store, the transaction was not seen until a
  block confirmed it.
- **Half the peers.** A forged transaction went to half the peers on the
  connections the store syncs from. Bitcoin Cash Node closed every
  connection from the address at once. One request could cut a store off
  from half its peers, and a second from half the rest.
- **200 whatever happens.** The reference promised errors for a
  transaction that was refused, for a timeout and for no peers. The code
  could give none of them.

## What was not chosen

- **Needing the RPC of a node.** The store speaks to nodes as a peer, and
  that is all it needs to run. The RPC is an option, and the better way
  when it is there.
- **Asking the node first and sending as a peer after**
  (`testmempoolaccept`). Two steps where one does, and the transaction
  could be refused between them.
- **Running scripts in the store.** That takes an interpreter of every
  script of both chains, held to the nodes rule for rule. The store checks
  the signature where there is no script to run, and leaves the rest.
- **Half the peers send, the others listen**, as bitcoinj does. It needs
  two peers, and puts the connections that the store syncs from at risk.
- **Announcing the transaction** (`inv`) and waiting to be asked. It costs
  two seconds, and a node that knows the transaction, or refused it, does
  not ask.

Bitcoin Core has the same design for the transactions of its own wallet
when told to broadcast them privately: a short connection that hands the
transaction over, a `ping` to know that it arrived, and the transaction is
taken for sent when it comes back from the network.

## How it is tested

- haskoin-core, `VerdictSpec` and `SignatureSpec`: the thirteen test
  vectors of Schnorr signatures of Bitcoin Cash Node; two transactions
  signed by its wallet, one of them spending tokens; inputs of each kind
  in the table above, verified, forged and unseen; and the property that
  a signed transaction is never called forged.
- `Haskoin.Store.Web.PublishSpec`: the checks of step 2, the outbox, and
  publishing with a courier that does as it is told: the echo, a refusal,
  a closed connection, silence, a node that cannot be reached, two
  requests at once, and two transactions with one id.
- `IntegrationSpec`, "publishes through a full node": a store against
  Bitcoin Core and Bitcoin Cash Node. A good transaction is answered when
  the store has it and is in the node's mempool. A forged one is refused
  by the store. One that pays no fee, which only a node refuses, is
  refused with the node's reason or times out, never shows, and the next
  good one is published. An unknown output, an output spent in a block
  and a body that is too long are refused with the reason.
- `IntegrationSpec`, "publishes over the RPC of its node": a store given
  the RPC of its node refuses the transaction that pays no fee with the
  node's reason, on Bitcoin Core too. A good transaction that the store
  cannot judge is published over RPC, and refused by a store without it.
  A store whose RPC does not answer publishes what it verified through a
  peer.
- `IntegrationSpec`, "publishes spends of a multisig": spends of a coin
  that two of three keys open, made with keys of the test and published
  through a peer, so verified by the store and taken by the node. On
  Bitcoin Core, a multisig in a witness script hash and a multisig of
  Taproot. On Bitcoin Cash Node, a multisig in a script hash with ECDSA
  signatures, and the same with Schnorr signatures. Each paying one
  satoshi less is refused.
- `IntegrationSpec`, "publishes transactions that make and spend tokens":
  on Bitcoin Cash Node, a transaction of the wallet that makes tokens and
  one that spends them are published, and a forged spend of them is
  refused.

- `IntegrationSpec`, "that does not whitelist the store": a store against
  each node started without `-whitelist`, which is how a node treats any
  peer it did not choose.
  A good transaction is answered 200 once the node has announced it,
  seconds later on Bitcoin Core, and the store has it. A spend of a coin
  that a script locks, which the store cannot tell from one that the node
  would close its connections over, is refused by the store: the store
  keeps its node, and publishes the next good one.

Not tested: a store with several peers, where the echo comes from another
node than the one that got the transaction.
