# CashTokens

Since May 2023 (block 792,772), an output on Bitcoin Cash can hold tokens
(CHIP-2022-02). The tokens are written at the front of the output's script
field, in a token prefix, before the script that locks the coins. This page
says how the store reads, indexes and serves them.

## What the store did before

haskoin-core 2.0 did not know the prefix. An output with tokens started
with the byte `0xef`, which matches no script template, so it had no
address. The store kept it as an output without an address: it was in no
address's history, balance or unspent outputs. Transaction
e3abdbdc…f5a809 of block 978,089, for example, had no address for four of
its five outputs.

The publishing checks (`Haskoin.Store.Web.Publish`) already read the
prefix, with a parser of their own, to refuse transactions that break the
token rules before they are sent to a node.

## What changes

haskoin-core 3.0 takes the prefix off before it matches the templates, on
the Bitcoin Cash networks only. An output with tokens now has the address
of its locking script, in the plain CashAddr form (type 0 or 1), as Bitcoin
Cash Node shows it. So the coins of one script stay under one address,
with or without tokens.

On top of that, the store:

1. **Shows the tokens of every output.** Outputs, inputs (the output they
   spend) and unspent outputs get an optional `token` field: the category,
   the fungible amount, and the non-fungible token with its capability and
   commitment. The JSON is that of the CashTokens test vectors and of
   Bitcoin Cash Node's RPC, with the amount as a string. The `script` field
   keeps the whole script field, prefix included, as it is in the
   transaction.
2. **Keeps token balances.** For each address and category: the fungible
   amount, confirmed and unconfirmed, the number of non-fungible tokens,
   and the number of unspent outputs that hold the category.
   The balance in satoshis does not change: it still counts the coins of
   every output, with or without tokens.
3. **Indexes categories.** For each category: its transactions, its
   unspent outputs, and its holders (the addresses with a token balance).

The publishing checks use haskoin-core's `splitTokenData` in place of their
own parser.

## API

New endpoints, on every network; on Bitcoin they answer with nothing:

| Endpoint | Answer |
| --- | --- |
| `GET /address/{address}/tokens` | the token balances of an address, one per category |
| `GET /token/{category}/holders` | the token balances of a category, one per address, with `limit` and `offset` |
| `GET /token/{category}/unspent` | the unspent outputs that hold the category, newest first, with `limit` and `height`/`time` |
| `GET /token/{category}/transactions` | the transactions that create, move or burn tokens of the category, newest first, like an address's |

The JSON of `StoreOutput`, `StoreInput` and `Unspent` gets the `token`
field on the Bitcoin Cash networks; on Bitcoin it is not there, and the
JSON is as it was. A client that does not know the field ignores it.

The Haskell types do not get a field: the tokens are in the script field,
which they carry already, and haskoin-store-data 4.0's `scriptTokens` reads
them. So the types keep their binary encoding, which the store writes in
its database and its caches, and nothing that builds them changes. The new
`TokenBalance` type and the requests of the new endpoints are in
haskoin-store-data 4.0 too.

## Database

The layout goes from version 23 to 24, with three new column families:

| Column family | Key | Value |
| --- | --- | --- |
| `tok-bal` | address, category | token balance, with a merge operator as `balance` |
| `cat-tx` | category, block position, transaction | nothing, as `addr-tx` |
| `cat-out` | category, block position, outpoint | the output, as `addr-out` |

The holders of a category are read from `tok-bal` through a second key,
category then address, written with the same merge records. Writing a
balance twice costs little next to scanning every address for a category.

The tokens of an output are read from its script whenever they are needed.
They are not stored apart: the script is already in the transaction.

## The shared mempool

The token endpoints count the mempool, as an address's balance does. The
fleet's pool in Redis keeps, as transactions come and go:

| Key | What it holds |
| --- | --- |
| `pool:ctx:{category}` | the transactions that move the category, newest first |
| `pool:cout:{category}`, `pool:cval:{category}` | the outputs of the pool that hold it, with value and script |
| `pool:csum:{category}` | how many transactions, free outputs and spent outputs |
| `pool:tsum:{category}{address}` | the sums of the address's token balance |
| `pool:acat:{address}`, `pool:chold:{category}` | the categories of an address, the addresses of a category |

The fleet member sends the tokens of every output that a transaction makes
or uses up with its other entries, so the Lua scripts never read a prefix.

**Free and spent, not paid and spent.** The summary of an address adds up
what the pool pays and what it spends, and takes off what it does both
to. Token sums cannot work that way. A category can have 2^63 − 1 tokens,
often all in one output, and moving them twice in the pool would push a
sum of what was paid past what Redis can hold. The script would then fail
halfway. So each output that holds tokens counts once at most: as free,
while the pool makes it and doesn't use it up, or as spent, while the pool
uses it up and the chain made it. Each sum is then the tokens of outputs
that exist together, never more than the supply. Only the amount received
counts every output, as it does in the chain.

**Amounts in two parts.** Lua's numbers are exact only up to 2^53. The
script splits each amount into the digits above the last nine and the
last nine, adds the two parts apart, and the store joins them again.

A token balance with the pool is the chain's balance minus what the pool
spends, plus what is free: `confirmed` is the chain's amount minus the
spent, and `unconfirmed` is the free amount. The holders of a category are
the chain's and the pool's together, in the order of their addresses.
Transactions and unspent outputs list the pool's first, as an address's do.

## Migration of an existing database

A database of version 23 on Bitcoin Cash was built without the addresses
of token outputs. haskoin-core 3.0 finds them when such an output is
spent: without a migration, the store would take out of a balance coins
that were never put in.

The migration to version 24 walks the blocks from 792,772, CashTokens'
activation, to the tip. For every transaction with an output or an input
that holds tokens, it adds what version 24 would have written: the
addresses' entries and balances for the outputs that now have an address,
the token balances, and the category indexes. Like the earlier migrations,
it works in batches, each one atomic, and keeps its place in the
database, so it can be stopped and carried on. On Bitcoin and its test
networks there are no tokens, and the migration only writes the new
version.

The mempool is in Redis, not in the database, so the migration does not
touch it. The scripts of the new version read the entries that the old
one kept, so the pool carries on. The transactions that were already in
it have no token entries, though: their tokens count once a block
confirms them, a few minutes after the upgrade. Every store of a fleet
has to be upgraded together, since an older store's script does not know
the token entries.

## Where it stands

All of it ships in **haskoin-store 2.0.0**, with haskoin-core 3.0,
haskoin-store-data 4.0 and haskoin-node 2.7:

- the `token` field of the JSON, and `TokenBalance` and the requests of the
  new endpoints, in haskoin-store-data 4.0;
- database version 24: the four column families, written by the import of
  a block and taken out by a revert ("Haskoin.Store.Logic"), and read by
  the four endpoints;
- the migration from version 23
  ("Haskoin.Store.Database.Migrate.Tokens"). Its test imports the same
  blocks as version 23 did and as version 24 does, migrates the first, and
  finds every column family of the two the same;
- the publishing checks on haskoin-core's parser;
- the shared mempool: the token entries of the pool in Redis, their reads,
  and the endpoints with the pool on top. The test of the pool holds the
  script to a model over random histories, and an integration test on a
  regtest chain of Bitcoin Cash Node asks the four endpoints through the
  client of haskoin-store-data, with the transactions in the mempool and
  then confirmed.

Until those releases are on Hackage, each package pins a commit of the one
below on GitHub.

## Not done

- Token metadata (BCMR names, symbols, decimals and icons). It lives off
  the chain, and a client can fetch it by category.
- Token totals per category, such as the supply. A supply needs to know
  which outputs can still mint, which the holders' balances already show.
