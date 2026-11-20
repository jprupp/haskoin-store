# CashTokens

Since May 2023 (block 792,772), an output on Bitcoin Cash can hold tokens
(CHIP-2022-02). The tokens are written at the front of the output's script
field, in a token prefix, before the script that locks the coins. This page
says how the store reads, indexes and serves them. It is the plan for the
`cashtokens` branches of haskoin-node, haskoin-store-data and this
repository, and it becomes the reference once the work is done.

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

haskoin-core 2.1 takes the prefix off before it matches the templates, on
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
which they carry already, and haskoin-store-data 3.1's `scriptTokens` reads
them. So the types keep their binary encoding, which the store writes in
its database and its caches, and nothing that builds them changes. The new
`TokenBalance` type and the requests of the new endpoints are in
haskoin-store-data 3.1 too.

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

The fleet's pool in Redis keeps a summary of each address that its
transactions touch (`pool:asum:*`). It gets the same token parts as the
database: per address and category, the fungible amount and the number of
non-fungible tokens, and per category the transactions and unspent outputs
in the pool. The fleet member sends the token entries of a transaction
with its other entries, so that the Lua scripts never read a prefix.

## Migration of an existing database

A database of version 23 on Bitcoin Cash was built without the addresses
of token outputs. haskoin-core 2.1 finds them when such an output is
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
touch it. The fleet's pool is emptied once at the upgrade, and refills
from the nodes and the peers.

## Where it stands

Done, on the `cashtokens` branches of haskoin-node, haskoin-store-data and
this repository, each pinned to the commit of the one below:

- the `token` field of the JSON, and `TokenBalance` and the requests of the
  new endpoints, in haskoin-store-data 3.1;
- database version 24: the four column families, written by the import of
  a block and taken out by a revert ("Haskoin.Store.Logic"), and read by
  the four endpoints;
- the migration from version 23
  ("Haskoin.Store.Database.Migrate.Tokens"). Its test imports the same
  blocks as version 23 did and as version 24 does, migrates the first, and
  finds every column family of the two the same;
- the publishing checks on haskoin-core's parser.

Still to do:

- the shared mempool: the token entries of the pool in Redis and their
  reads, so that the endpoints count unconfirmed transactions, as the
  balances of addresses do. Until then the token endpoints answer from the
  blocks alone;
- haskoin-core 2.1 and haskoin-store-data 3.1 on Hackage, and the pins back
  to Hackage versions.

## Not done

- Token metadata (BCMR names, symbols, decimals and icons). It lives off
  the chain, and a client can fetch it by category.
- Token totals per category, such as the supply. A supply needs to know
  which outputs can still mint, which the holders' balances already show.
