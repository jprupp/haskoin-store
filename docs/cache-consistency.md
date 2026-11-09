# Keeping the Redis cache exact

This is an analysis of how a cached xpub can come to differ from the
database without any check noticing, and of what would rule that out. It was
written on 2026-11-09, after the incremental reloads and their catch-up
(`Haskoin.Store.Cache`, `cacheRefresh`). The holes described here come from
reading the code; none has been reproduced in a test yet.

## The error

The cache is meant to answer exactly as the database would. An entry is kept
up to date by events: when a block or a mempool transaction touches a cached
xpub, a store applies the change to the entry, and checks the result against
the balances of the xpub's addresses in the database. A balance that the
change does not explain means a change was missed; the store then looks for
it (the catch-up), and reads the xpub whole if it cannot find it. In
addition, every entry is read whole on a schedule, every five to ten
minutes.

An entry goes wrong, and stays wrong, only when two things happen together:
a change is missed, **and** the checks of the next reload cannot see that it
was.

### How a change gets missed

- **A store restarts.** The cache forgets the last block it saw (`lastBest`
  starts empty), so the next block event does not collect the transactions
  of blocks that a reorganisation removed. Mempool events while the store was
  down are lost.
- **A store dies holding a claim.** A change of an entry is claimed by the
  first store to see it, and the others skip it. The claim expires after two
  minutes, but by then the other stores have moved on, and nobody applies
  the change.
- **An error in the worker.** It is logged, and the batch of events it was
  applying is dropped.

### What the checks cannot see

1. **The confirmation of a transaction that only spends from the xpub.**
   Spending a confirmed output takes it off the confirmed balance as soon as
   the spending transaction enters the mempool (`decreaseBalance` in
   `Haskoin.Store.Logic` goes by the block of the output, not of the
   spender). When the spender is then confirmed, no balance moves. The
   catch-up would find it among the mempool transactions of the entry, but
   the catch-up only runs when some balance is off.
2. **A reorganisation that leaves every balance as it was.** If the block
   that replaces another confirms the same transactions, only block hashes
   and positions change. The entry is stamped with a height, not a block
   hash, and the catch-up only looks above that height.
3. **A replacement with the same effect on the xpub.** Take a fee bump of a
   payment to the xpub: the xpub receives the same amount, so its number of
   transactions, amounts and number of outputs stay the same. Only the
   transaction ID differs.
4. **Spenders in the cached bodies of transactions.** This one needs a
   longer explanation, below. In short: it is the only hole that the
   scheduled full reload never repairs, and it can make any endpoint that
   returns transactions answer wrongly, not only the xpub endpoints.

## Why spenders are a problem

### What a spender is, and where the cache keeps it

Every output of a transaction is either unspent, or spent by an input of a
later transaction. The API shows this with every output, as `spent` and
`spender` (the transaction and input that spend it) in the native API, and as
`spent` in the Blockchain.info API. So a transaction as the API returns it is
not fixed: its outputs change whenever a later transaction spends one of
them.

The cache keeps, for every transaction that a cached xpub has, its body
under `tx:<hash>`, and its spenders apart, in a hash `sp:<hash>` from output
index to spender. They are kept apart because the body never changes once
confirmed, while the spenders do.

These bodies are not only used for xpubs. The web server reads every
transaction from Redis when Redis has it, and from the database otherwise
(`webQuery` in `Haskoin.Store.Web`). That is so that a store that is behind
can still serve every transaction of a cached xpub. As a result,
`GET /transaction/<hash>`, `GET /transactions`, the Blockchain.info endpoints
and the xpub endpoints all show the spenders that Redis has, for any
transaction that a cached xpub has.

### How spenders are kept up to date

When a transaction enters the mempool, is confirmed, or is deleted, every
store that sees the event updates the spenders of the outputs that the
transaction spends (`updateSpenders`, with `spends.lua`):
- the spender is set when the transaction spends the output;
- it is cleared when the transaction is deleted, if it is still the spender
  recorded.

This happens for every transaction, whether it touches a cached xpub or not,
and it is not part of the reload of any entry.

### A worked example

Alice's wallet is a cached xpub.
1. Alice pays Bob 0.1 BTC in transaction T1. T1 has two outputs: output 0 to
   Bob, an address outside Alice's xpub, and output 1, Alice's change.
2. T1 is in Alice's entry, so Redis has `tx:T1` and `sp:T1`, both outputs
   unspent.
3. Days later Bob spends output 0, in transaction T2. T2 touches no address
   of Alice's xpub.

In the database, T1 has changed: its output 0 is now spent by T2. In Redis,
`sp:T1` is updated by the event of T2, as above.

If that event is missed, for one of the reasons listed earlier, `sp:T1` keeps
output 0 unspent. From then on every store sharing that Redis answers that
Bob's output is unspent: in Alice's transactions, in `GET /transaction/T1`,
and in Blockchain.info's `rawtx` and `multiaddr`. The database of every one of
those stores says it is spent.

The reverse happens too. Suppose T2 is dropped from the mempool and its
deletion is missed. Then `sp:T1` keeps T2 as the spender, and the API shows
output 0 spent by a transaction that does not exist.

### Why no check notices

- **The checks only look at the xpub.** They compare its balances, and
  catch up from changes at its addresses. Output 0 belongs to Bob, so
  nothing of Alice's moves when Bob spends it: no balance, no transaction of
  Alice's history, no unspent output of Alice's.
- **No reload runs.** T2 touches no cached xpub, so no reload is triggered
  by it at all.

### Why even a full reload does not repair it

A reload writes bodies, with their spenders read from the database, only for
some transactions:
- those that are new to the entry;
- those whose position in it changed, such as a confirmation;
- those that the change touched.

T1 is long confirmed and its position does not change. So it is not
rewritten, by an incremental reload or by a full one. The stale spender lasts
until one of these happens:
- T1 itself changes, which for a confirmed transaction means a
  reorganisation;
- no cached xpub has T1 any more, so its body is deleted, and it is later
  cached again.

### It is not only outputs outside the xpub

Outputs at addresses of the xpub have the same weakness, with one difference.
Say Alice spends output 1 of T1, her change, in T3, and the event of T3 is
missed:
- **The miss is noticed.** Alice's balance moved, so the next reload of her
  entry notices, finds T3, and fixes her unspent outputs and history.
- **But T1's body is not repaired.** The reload writes the body of T3, not
  that of T1, and it does not update the spenders of the outputs that T3
  spends. So T1 still shows output 1 unspent, even though Alice's list of
  unspent outputs no longer has it.

So the problem is spenders of any output of a cached body, whenever the
event that changes them is missed. Outputs outside the xpub are the worse
case, because nothing notices at all.

## What could be done

### A. Never miss a change

Each store would keep a durable log of its changes, and a reload would read
everything since the entry's position in that log. This does not work with
a Redis shared by several stores:
- every store has its own database, so a position in one store's log means
  nothing to another;
- entries are written by whichever store claims a change first.

Saving the last block seen in Redis would fix the case of the reorganisation
after a restart. It would not fix lost mempool events or claims held by a
store that died.

### B. Close each hole

Each fix is small:
- **Hole 1 and 3:** check the mempool transactions of the entry on every
  reload, not only when a balance is off. There are few, and the catch-up
  already gives up beyond 1,000. For each one that was deleted, also scan
  its addresses, which finds its replacement.
- **Hole 2:** stamp the entry with the hash of its block as well as the
  height. If that block is no longer in the main chain of the store, catch
  up from the fork, or read the xpub whole.
- **Hole 4, outputs inside the xpub:** have the commit also update the
  spenders of the outputs that the transactions it applies spend, as the
  events do. This works for changes that the reload found itself.
- **Hole 4, outputs outside the xpub:** stop caching spenders, and read them
  from the database of the store that answers. The database has an index of
  spenders, read one key per output, and is always right for that store.
  - **What is lost:** a store that is behind would not show spends it has not
    imported yet, for a transaction it does serve from Redis. That is what it
    already does for transactions that are not cached.
  - **What is gained:** the whole class of stale spenders goes away, and so
    does `updateSpenders` with its script.

The limit of this approach: it is complete only for the holes that are
listed. A new kind of change, for example from future code, could open
another one silently.

### C. An exact check

Keep in RocksDB, for every address, a digest of its index: a sum of hashes
of each of its entries, (transaction, block) and (unspent output, block).
The digest is updated in the same write batch that already updates the
address's balance. Each cache entry records the digest of the database it
was written against. A reload then checks:

> the database's digest now, minus the entry's digest, equals the digest of
> the changes that the reload applies.

The old blocks of the changes come from the scores in Redis. A missed change
of any kind to the indexed data breaks the equation, and the reload catches
up or reads the xpub whole.

- **What it covers:** holes 1, 2 and 3, and any future hole in the same data,
  without relying on events. It does not cover spenders, which are not in
  the index of the xpub's addresses; those need the fix of B.
- **Several stores:** the digest is a function of what a database holds, so
  stores whose databases agree compute the same one. Mempool times are left
  out of it, because each store sees a transaction at a different time.
- **Costs:**
  - a new column family, so that haskoin-store-data and its `Balance`
    encoding stay as they are;
  - a pass over every address index for existing data, which would take
    hours on mainnet, or computing it lazily the first time an address is
    used;
  - a small extra cost on each write.
- **The hash:** it must resist someone crafting transactions whose hashes
  cancel each other out. That means 256-bit hashes summed modulo a large
  prime, not a plain XOR.
- **What it allows:** with an exact check, the scheduled full reload is no
  longer needed, and with it go the memory peaks it causes (see
  `stress/results/2026-11-09-full-catch-up`).

## Recommendation

1. **Do B first.** It makes the cache safe against every hole known today,
   at almost no cost while running. Its biggest gain is reading spenders from
   the database, which removes the one hole that nothing repairs.
2. **Do C if the guarantee must hold by construction**, not by a list of
   holes. It is the only option that rules out this kind of error entirely
   for the data of an xpub.

A timer-driven full reload, rather than one on the first change after its
time, would only shorten how long an error lasts. It would not prevent it,
and it would not repair spenders at all.
