# CashTokens

How the store reads, indexes and serves the tokens of Bitcoin Cash
(CHIP-2022-02).

## In short

Since May 2023 (block 792,772), an output on Bitcoin Cash can hold
tokens as well as coins. A store that does not know tokens gives such an
output no address, so its coins are missing from every balance. Now the
store reads the tokens of every output, files the output under the
address of its script, keeps a token balance for each address, and
indexes each kind of token. Four endpoints serve this, and they count
the mempool, as an address's balance does. The cost: each token balance
is written twice, an existing database of Bitcoin Cash must be migrated,
and every store of a fleet must be upgraded together.

## Words used here

The shared words (store, fleet, the chain, the pool) are in
[README.md](README.md). Merge records and merge operators are in
[block-import.md](block-import.md).

| Word | What it means here |
| --- | --- |
| **locking script** | The script of an output that says who may spend it. |
| **token prefix** | Bytes at the front of an output's script field, before the locking script. They start with `0xef` and say which tokens the output holds. |
| **category** | One kind of token. Its name is the id of the transaction whose output 0 created it. An output holds tokens of one category. |
| **fungible** | Said of tokens that are all alike. An output holds an amount of them. |
| **non-fungible token** | A single token that is a thing of its own. |
| **capability** | What the holder of a non-fungible token can do: `none`, `mutable` (change its commitment) or `minting` (make new tokens of the category). |
| **commitment** | The bytes that a non-fungible token carries. |
| **token balance** | What one address has of one category: the fungible amount, the number of non-fungible tokens, and the number of unspent outputs that hold the category. |
| **holder** | An address with a token balance of a category. |
| **column family** | A table of its own inside RocksDB. |
| **free**, **spent** | The two ways in which the pool counts an output that holds tokens ("The shared mempool" below). |

## What the store did before

haskoin-core 2.0 did not know the token prefix. An output with tokens
starts with the byte `0xef`, which matches no script template. So it had
no address, and the store kept it as an output without one. It was in no
address's history, balance or unspent outputs. For example, transaction
e3abdbdc…f5a809 of block 978,089 had no address for four of its five
outputs.

The publishing checks (`Haskoin.Store.Web.Publish`) already read the
prefix, with a parser of their own. They refuse a transaction that
breaks the token rules before it is sent to a node.

## What changes

haskoin-core 3.0 takes the prefix off before it matches the templates,
on the Bitcoin Cash networks only. An output with tokens now has the
address of its locking script, in the plain CashAddr form (type 0 or 1),
as Bitcoin Cash Node shows it. So the coins of one script stay under one
address, with or without tokens.

On top of that, the store does three things:

1. **It shows the tokens of every output.** Outputs, inputs (the output
   they spend) and unspent outputs get an optional `token` field. It has
   the category, the fungible amount, and the non-fungible token with
   its capability and commitment. The JSON is that of the CashTokens
   test vectors and of Bitcoin Cash Node's RPC, with the amount as a
   string. The `script` field keeps the whole script field, prefix
   included, as it is in the transaction.
2. **It keeps token balances.** For each address and category: the
   fungible amount, confirmed and unconfirmed, the number of
   non-fungible tokens, and the number of unspent outputs that hold the
   category. The balance in satoshis does not change. It still counts
   the coins of every output, with or without tokens.
3. **It indexes categories.** For each category: its transactions, its
   unspent outputs, and its holders.

The publishing checks now use haskoin-core's `splitTokenData` in place
of their own parser.

## API

The new endpoints are on every network. On Bitcoin they answer with
nothing.

| Endpoint | Answer |
| --- | --- |
| `GET /address/{address}/tokens` | the token balances of an address, one per category |
| `GET /token/{category}/holders` | the token balances of a category, one per address, with `limit` and `offset` |
| `GET /token/{category}/unspent` | the unspent outputs that hold the category, newest first, with `limit` and `height`/`time` |
| `GET /token/{category}/transactions` | the transactions that create, move or burn tokens of the category, newest first, like an address's |

**The JSON gets a field.** `StoreOutput`, `StoreInput` and `Unspent` get
the `token` field on the Bitcoin Cash networks. On Bitcoin it is not
there, and the JSON is as it was. A client that does not know the field
ignores it.

**The Haskell types do not.** The tokens are in the script field, which
the types carry already, and haskoin-store-data 4.0's `scriptTokens`
reads them. So the types keep their binary encoding, which the store
writes in its database and its caches. Nothing that builds them changes.

The new `TokenBalance` type and the requests of the new endpoints are in
haskoin-store-data 4.0 too.

## Database

The layout goes from version 23 to 24, with four new column families:

| Column family | Key | Value |
| --- | --- | --- |
| `tok-bal` | address, category | token balance, with a merge operator as `balance` |
| `holder` | category, address | the same token balance, written with the same merge records |
| `cat-tx` | category, block position, transaction | nothing, as `addr-tx` |
| `cat-out` | category, block position, outpoint | the output, as `addr-out` |

**Each token balance is written twice.** The holders of a category are
read through the second key, category then address. Writing a balance
twice costs little next to scanning every address for a category.

**The tokens of an output are not stored apart.** They are read from its
script whenever they are needed. The script is already in the
transaction.

## The shared mempool

The token endpoints count the mempool, as an address's balance does. The
pool keeps these as transactions come and go:

| Key | What it holds |
| --- | --- |
| `pool:ctx:{category}` | the transactions that move the category, newest first |
| `pool:cout:{category}`, `pool:cval:{category}` | the outputs of the pool that hold it, with value and script |
| `pool:csum:{category}` | how many transactions, free outputs and spent outputs |
| `pool:tsum:{category}{address}` | the sums of the address's token balance |
| `pool:acat:{address}`, `pool:chold:{category}` | the categories of an address, the addresses of a category |

The store sends the tokens of every output that a transaction makes or
uses up, with the transaction's other entries. So nothing in Redis ever
reads a prefix.

### Free and spent, not paid and spent

For coins, the pool's summary of an address adds up what the pool pays
and what it spends, and takes off what it does both to. Token sums
cannot work that way.

For example, a category has 2^63 − 1 tokens, all in one output. One
transaction of the pool moves them, and a second moves them again. A sum
of what was paid would be twice 2^63 − 1. That is more than a number in
Redis can hold, and Redis would refuse the addition.

So each output that holds tokens counts once at most:

| Counted as | When |
| --- | --- |
| **free** | the pool makes the output and does not use it up |
| **spent** | the chain made the output and the pool uses it up |

Each sum is then the tokens of outputs that exist together, never more
than the supply. Only the amount received counts every output, as it
does in the chain.

### Amounts in two parts

The pool keeps each amount as two numbers: the digits above the last
nine, and the last nine. It adds the two parts apart, and the store
joins them again. The reason is Lua, whose numbers are exact only up to
2^53. The scripts that read the pool still add the parts.

### What a request gets

| Answer | How the chain and the pool are put together |
| --- | --- |
| Token balance | `confirmed` is the chain's amount minus the spent. `unconfirmed` is the free amount. |
| Holders of a category | the chain's and the pool's together, in the order of their addresses |
| Transactions, unspent outputs | the pool's first, as an address's do |

## Migration of an existing database

A database of version 23 on Bitcoin Cash was built without the addresses
of token outputs. haskoin-core 3.0 finds those addresses when such an
output is spent. Without a migration, the store would take out of a
balance coins that were never put in.

The migration to version 24 goes like this:

1. It walks the blocks from 792,772, the activation of CashTokens, to
   the tip.
2. For every transaction with an output or an input that holds tokens,
   it adds what version 24 would have written: the addresses' entries
   and balances for the outputs that now have an address, the token
   balances, and the category indexes.
3. Like the earlier migrations, it works in batches, each one atomic,
   and keeps its place in the database. So it can be stopped and carried
   on.

On Bitcoin and its test networks there are no tokens, and the migration
only writes the new version.

**The mempool is not migrated.** It is in Redis, not in the database.
The scripts of the new version read the entries that the old one kept,
so the pool carries on. The transactions that were already in it have no
token entries, though. Their tokens count once a block confirms them, a
few minutes after the upgrade.

**Every store of a fleet has to be upgraded together.** An older store's
script does not know the token entries.

## Where it stands

All of it ships in **haskoin-store 2.0.0**, with haskoin-core 3.0,
haskoin-store-data 4.0 and haskoin-node 2.7:

- **The JSON and the types**: the `token` field, `TokenBalance` and the
  requests of the new endpoints, in haskoin-store-data 4.0.
- **Database version 24**: the four column families. The import of a
  block writes them and a revert takes them out
  (`Haskoin.Store.Logic`). The four endpoints read them.
- **The migration from version 23**
  (`Haskoin.Store.Database.Migrate.Tokens`). Its test imports the same
  blocks as version 23 did and as version 24 does, migrates the first,
  and finds every column family of the two the same.
- **The publishing checks**, on haskoin-core's parser.
- **The shared mempool**: the token entries of the pool, their reads,
  and the endpoints with the pool on top. Two tests cover it. The test
  of the pool holds the script to a model over random histories. An
  integration test on a regtest chain of Bitcoin Cash Node asks the four
  endpoints through the client of haskoin-store-data, with the
  transactions in the mempool and then confirmed.

Until those releases are on Hackage, each package pins a commit of the
one below on GitHub.

## Not done

- **Token metadata** (BCMR names, symbols, decimals and icons). It lives
  off the chain, and a client can fetch it by category.
- **Token totals per category**, such as the supply. A supply needs to
  know which outputs can still mint, which the holders' balances already
  show.
