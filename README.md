# Haskoin Store

[![CI](https://github.com/jprupp/haskoin-store/actions/workflows/ci.yml/badge.svg)](https://github.com/jprupp/haskoin-store/actions/workflows/ci.yml)
[![Hackage haskoin-store](https://img.shields.io/hackage/v/haskoin-store.svg?label=haskoin-store)](https://hackage.haskell.org/package/haskoin-store)
[![Hackage haskoin-store-data](https://img.shields.io/hackage/v/haskoin-store-data.svg?label=haskoin-store-data)](https://hackage.haskell.org/package/haskoin-store-data)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/jprupp/haskoin-store/blob/master/haskoin-store/LICENSE)

A Bitcoin (BTC) and Bitcoin Cash (BCH) block chain indexer with a REST API,
written in Haskell. It connects to the peer-to-peer network directly, so it
does not need a full node next to it, and keeps its indices in RocksDB.

- [API reference](https://github.com/jprupp/haskoin-store/blob/master/swagger.yaml)
- [Library documentation](https://hackage.haskell.org/package/haskoin-store)
  and [client documentation](https://hackage.haskell.org/package/haskoin-store-data)
- [Changelog](https://github.com/jprupp/haskoin-store/blob/master/CHANGELOG.md)
- [Report a bug](https://github.com/jprupp/haskoin-store/issues)

## Contents

- [Why Haskoin Store](#why-haskoin-store)
- [Design principles](#design-principles)
- [Features](#features)
- [Quick start](#quick-start)
- [Building from source](#building-from-source)
- [Running](#running)
- [Tutorial: following a wallet](#tutorial-following-a-wallet)
- [Configuration](#configuration)
- [API](#api)
- [How it works](#how-it-works)
- [Monitoring](#monitoring)
- [Using it from Haskell](#using-it-from-haskell)
- [Packages](#packages)
- [Development](#development)
- [Related projects](#related-projects)
- [License](#license)

## Why Haskoin Store

A Bitcoin node can tell you what is in block 800000, but not what the balance
of an address is, which transactions belong to a wallet, or which outputs it
can spend. Wallets, block explorers, payment processors and accounting tools
all need exactly those answers, for many users at once, in milliseconds.

The usual way to get them is to run a full node and an indexer next to it, and
operate both. Haskoin Store is one program instead. It connects to the
peer-to-peer network itself, downloads blocks and unconfirmed transactions,
keeps indices by address, transaction and extended public key in RocksDB, and
serves them over a REST API. It has been used to serve public Bitcoin and
Bitcoin Cash APIs such as api.haskoin.com.

## Design principles

- **Index for the questions people ask.** Every index is laid out so that an
  API query is one seek and a short scan in RocksDB, newest first, which keeps
  response times flat as the chain grows.
- **Never half-write.** Each block, and each batch of mempool transactions, is
  one atomic database write. A crash leaves the store at the previous block,
  with no repair step on restart.
- **Unconfirmed transactions matter.** The mempool is indexed and persisted like
  the chain, ordered before confirmed history, and handles replace-by-fee on
  Bitcoin. A wallet sees a payment the moment the network does.
- **Extended public keys are first class.** Wallets are mostly xpubs, so the
  store derives their addresses up to an exact gap, remembers the derivations,
  and can keep large wallets ready in a Redis shared by several stores.
- **Bounded resources.** API limits stop one request from monopolising the
  server, slow event clients are dropped, and RocksDB memory has a budget.
- **Operable.** A health endpoint that tells a load balancer when an instance is
  behind, StatsD metrics for everything, configuration through flags or
  environment variables, and a Docker image.
- **Follow the work, not a node.** The store trusts the chain with the most
  proof of work, as reported by [haskoin-node](https://github.com/jprupp/haskoin-node),
  and checks what it needs to keep its indices consistent. It does not
  re-validate scripts; run it against the network you trust, or next to your
  own node with `--peer`.

## Features

| Area            | What you get                                                                                |
| --------------- | ------------------------------------------------------------------------------------------- |
| Networks        | Bitcoin and Bitcoin Cash: mainnet, testnets and regtest                                     |
| Addresses       | Balances, transactions and unspent outputs (UTXO) of any address, alone or many at once      |
| Xpubs           | Balances, transactions and UTXO of extended public keys, for legacy, nested SegWit and native SegWit addresses, with an optional Redis cache for large wallets |
| Blocks and txs  | Lookup by hash, height or time; raw serialized forms; transactions of a block               |
| Mempool         | Persistent, indexed by arrival time, with replace-by-fee on BTC and pruning of old entries   |
| Formats         | JSON, pretty JSON, or a compact binary encoding                                              |
| Live events     | New blocks and transactions as a JSON stream or over a WebSocket                            |
| Compatibility   | Endpoints in the style of Blockchain.info under `/blockchain/`                               |
| Operations      | Health check, StatsD metrics, RocksDB memory limits, Docker image                           |

### What is not included

- No wallet: it never holds keys or signs anything.
- No full validation of scripts and consensus rules. It follows the chain with
  the most work and indexes what peers send.
- No address or transaction search by anything but its hash; no indices on
  scripts without an address.

## Quick start

With [Docker](https://www.docker.com/):

```sh
git clone https://github.com/jprupp/haskoin-store.git
cd haskoin-store
docker build -t haskoin-store .
docker run -d --name haskoin-store -p 3000:3000 \
  -e NET=btc -e DISCOVER=true \
  -v haskoin-store-data:/home/haskoin/.haskoin-store \
  haskoin-store
curl 'http://localhost:3000/block/best?pretty=true'
```

The first sync downloads and indexes the whole chain, which takes a long
time and a lot of disk. The API answers while it syncs; `/health` reports
whether the node is in sync.

## Building from source

You need [Stack](https://docs.haskellstack.org/) and three C libraries:
[zlib](https://zlib.net/),
[libsecp256k1](https://github.com/bitcoin-core/secp256k1) and
[RocksDB](https://rocksdb.org/). Stack installs the right GHC by itself.

**Debian or Ubuntu**

```sh
sudo apt install git pkg-config zlib1g-dev libsecp256k1-dev librocksdb-dev haskell-stack
```

**Fedora**

```sh
sudo dnf install git pkgconf zlib-devel libsecp256k1-devel rocksdb-devel stack
```

Then build and install the `haskoin-store` binary to `~/.local/bin`:

```sh
git clone https://github.com/jprupp/haskoin-store.git
cd haskoin-store
stack build --copy-bins
~/.local/bin/haskoin-store --help
```

**Any distribution, with [Nix](https://nixos.org/)**: Stack fetches the C
libraries listed in `stack.yaml` from Nix instead of the system.

```sh
stack --nix build --copy-bins
```

## Running

```sh
haskoin-store --net btc --discover
```

Give it either `--discover` or at least one `--peer`; with neither it turns
discovery on by itself. Data goes in a directory named after the network inside
`--dir`, so one data directory can hold several networks.

For a big public deployment, also set `MALLOC_ARENA_MAX=2` (the Docker image
does) to keep glibc from fragmenting RocksDB's memory.

## Tutorial: following a wallet

This walks through what a wallet back end typically does with the API, on a
store that is already running on `localhost:3000`.

### 1. Wait for the store to be ready

```sh
curl -s -o /dev/null -w '%{http_code}\n' localhost:3000/health
```

`200` means the index is caught up with the network. While it syncs you get
`503`, and the body says which check fails.

### 2. Get the balance of a wallet

Most wallets are an extended public key. Ask for its summary, and say which
kind of addresses it derives (`legacy`, the default, `compat` for nested
SegWit, `segwit` for native SegWit):

```sh
curl 'localhost:3000/xpub/xpub6C...?derive=segwit&pretty=true'
```

The answer has confirmed and unconfirmed balances, and the index of the last
used receiving and change address, so the wallet knows where to derive the
next address.

If you get `derive` wrong, the store says so: a wallet with no transactions
of the kind you asked for, but some of another kind, gets a 400 whose message
names the right one, instead of an empty wallet that looks like a zero
balance.

### 3. List its transactions, page by page

```sh
curl 'localhost:3000/xpub/xpub6C.../transactions?derive=segwit&limit=50'
```

Results are newest first, unconfirmed ones on top. To get the next page, start
from the last transaction you received instead of using an offset:

```sh
curl 'localhost:3000/xpub/xpub6C.../transactions?derive=segwit&limit=50&offset=1&height=<last txid>'
```

`offset=1` skips the transaction you already have. Paging by starting point
stays fast however deep you go, and does not shift when new transactions
arrive.

### 4. Find spendable outputs

```sh
curl 'localhost:3000/xpub/xpub6C.../unspent?derive=segwit'
```

Each output comes with its derivation path, so the wallet knows which key signs
it.

### 5. Broadcast a payment

```sh
curl -X POST --data-binary @tx.hex localhost:3000/transactions
```

The body is the signed transaction in hex. The store sends it to its peers and
returns the transaction id. It appears in the mempool, and in the wallet's
history, as soon as a peer relays it back.

### 6. Stay up to date without polling

```sh
curl -N localhost:3000/events
```

Each line is a new block or a mempool change. A wallet back end keeps this open
and refreshes the affected wallets when an event arrives, instead of asking
every few seconds.

### 7. Make big wallets fast

Start the store with `--redis` and a Redis (or Valkey) server. Xpubs with at
least `--redis-min-addrs` used addresses, or at least `--redis-min-txs`
transactions in their addresses, are then cached after their first
query and answered from Redis, transactions in full included. Several stores
can share one Redis: they keep each other's cache up to date, and a store that
has not imported a transaction yet still serves it from the cache. Add
`nocache=true` to a query to read the database instead.

Servers behind a load balancer should also be started with `--fleet`
([A fleet of servers](#a-fleet-of-servers)): they then answer every request
alike, and share a cache of xpubs that is built for that.

## Configuration

Every option has an environment variable that sets its default, which is handy
in containers. Booleans in variables accept `true`/`false`, `yes`/`no`,
`on`/`off` and `1`/`0`. `haskoin-store --help` shows the current values.

### Network and peers

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `-n`, `--net` | `NET` | `bch` | Network: `btc`, `bch`, or a test network (see `--help`) |
| `-d`, `--dir` | `DIR` | `~/.haskoin-store` | Data directory |
| `--discover` | `DISCOVER` | off | Discover peers through DNS seeds and relayed addresses |
| `-p`, `--peer` | `PEER` | none | Connect to `HOST[:PORT]`; repeat for more, space-separated in `PEER` |
| `--max-peers` | `MAX_PEERS` | `20` | Most peers to connect to |
| `--peer-timeout` | `PEER_TIMEOUT` | `120` | Seconds before an unresponsive peer is dropped |
| `--max-peer-life` | `MAX_PEER_LIFE` | `172800` | Seconds after which a peer is dropped, so connections rotate |
| `--block-window` | `BLOCK_WINDOW` | `64` | Blocks to request from the syncing peer at a time |

### Mempool

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--no-mempool` | `NO_MEMPOOL` | off | Do not index unconfirmed transactions |
| `--sync-mempool` | `SYNC_MEMPOOL` | off | Ask each peer for its whole mempool once in sync |
| `--wipe-mempool` | `WIPE_MEMPOOL` | off | Empty the mempool area of the database at start |
| `--mempool-timeout` | `MEMPOOL_TIMEOUT` | `14` | Days after which unconfirmed transactions are deleted (0 = never) |

### Web server and limits

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--host` / `--port` | `HOST` / `PORT` | `*` / `3000` | Address the API listens on |
| `--noweb` | `NO_WEB` | off | Index only, no API |
| `--no-xpub` | `NO_XPUB` | off | Disable the xpub endpoints |
| `--no-blockchain-info` | `NO_BLOCKCHAIN_INFO` | off | Disable the `/blockchain/` endpoints and price lookups |
| `--max-item-count` | `MAX_ITEM_COUNT` | `200000` | Most items in a list (0 = no limit) |
| `--max-full-item-count` | `MAX_FULL_ITEM_COUNT` | `5000` | Most full transactions in a list (0 = no limit) |
| `--max-offset` | `MAX_OFFSET` | `50000` | Largest `offset` (0 = no limit) |
| `--def-item-count` | `DEF_ITEM_COUNT` | `100` | Items in a list when no `limit` is given |
| `--xpub-gap` | `XPUB_GAP` | `32` | Unused addresses kept after the last used one of each chain of an xpub; payments further away are not seen |
| `--xpub-gap-init` | `XPUB_GAP_INIT` | `20` | Addresses looked at for an xpub with no transactions; an xpub with none used among them counts as unused |
| `--ticker-url` | `TICKER_URL` | Blockchain.info ticker | Source of current prices |
| `--price-history-url` | `PRICE_HISTORY_URL` | Blockchain.info price index | Source of historical prices |
| `--ticker-refresh` | `TICKER_REFRESH` | `90000000` | Microseconds between price updates |

### Redis cache

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--redis` | `REDIS` | off | Cache large xpubs in Redis |
| `--redis-url` | `REDIS_URL` | local Redis | Redis URL, such as `redis://host:6379` |
| `--redis-min-addrs` | `REDIS_MIN_ADDRS` | `100` | Cache xpubs with at least this many used addresses |
| `--redis-min-txs` | `REDIS_MIN_TXS` | `1000` | Or with at least this many transactions in their addresses |
| `--redis-max-xpubs` | `REDIS_MAX_XPUBS` | `10000` | Evict the least recently used xpubs past this many |

Every key of the cache starts with `hs2:<network>:`, so a Redis can be shared
by stores of several networks, and by stores of older versions, whose cache
uses other keys. `--redis-max-keys` and `--redis-sync-interval` of older
versions are gone.

### A fleet of servers

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--fleet` | `FLEET` | off | Answer as one of the servers that share the Redis of `--redis` |

Servers behind a load balancer import the same blocks and transactions at
slightly different times, so the same request can get different answers
depending on which of them it reaches. Start every one of them with
`--redis`, the same `--redis-url` and `--fleet`, and they all answer from
the same block and the same mempool:

- **The block** is the newest that every live server has. Each server
  imports blocks when it gets them and keeps its database as it was at each
  of its last six, and a request is answered from the block that the fleet
  agreed on. The fleet moves to a new block in one step, when the last
  server has it.
- **The mempool** is one pool in Redis, which every server offers its
  unconfirmed transactions to and all answer from. A transaction has one
  time of arrival there, so lists come in the same order everywhere.
- **A server that falls behind** answers 503 with `behind-fleet` rather
  than something that the others would not say, and stops counting after
  ten seconds without a report, so that it holds nobody up. A server whose
  health check fails stops reporting.
- **Without Redis** each server answers from its own database, and
  `/health` says 203, as it does for the cache.
- **Large xpubs are cached for the whole fleet**, under the same
  `--redis-min-addrs`, `--redis-min-txs` and `--redis-max-xpubs` as the
  cache of a server alone, which should be the same on every server. An
  entry holds the chain's part of a wallet at the block that the fleet
  answers from, and the pool's part is kept by the pool itself, so a
  cached answer is the one that the database gives, on every server. The
  server that moves the fleet to a new block brings the cache there; in
  the moment before it has, and for a wallet that just gained or lost an
  address, requests are answered from the database. A reorganisation
  empties the cache. With it a request about a wallet of 464 addresses
  and 19,499 transactions takes 1.3 to 2.2 milliseconds, where it took 5
  to 38.
- **Deleted transactions** are not known to a fleet: a page that starts at
  one is empty.

The rules, and the tests that hold Redis to them, are described in "A fleet
that answers as one" in `docs/layered-database.md`.

### Health check

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--max-lagging-blocks` | `MAX_LAGGING_BLOCKS` | `3` | Most blocks the index may lag behind the headers |
| `--max-pending-txs` | `MAX_PENDING_TXS` | `10000` | Most transactions waiting to be imported |
| `--min-peers` | `MIN_PEERS` | `1` | Fewest connected peers |
| `--block-timeout` | `BLOCK_TIMEOUT` | `14400` | Most seconds since the last block |
| `--tx-timeout` | `TX_TIMEOUT` | `1800` | Most seconds since the last transaction |
| `--health-check-interval` | `HEALTH_CHECK_INTERVAL` | `30` | Seconds between checks |

### Metrics and logging

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--statsd` | `STATSD` | off | Send metrics to StatsD |
| `--statsd-host` / `--statsd-port` | `STATSD_HOST` / `STATSD_PORT` | `localhost` / `8125` | StatsD server |
| `--statsd-prefix` | `STATSD_PREFIX` | `haskoin_store` | Prefix of every metric |
| `--statsd-no-events` | `STATSD_NO_EVENTS` | off | Do not send individual samples |
| `--statsd-aggregates` | `STATSD_AGGREGATES` | off | Also send aggregated statistics every second |
| `--debug` | `DEBUG` | off | Verbose logging |

### RocksDB

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--bloom` | `BLOOM` | off | Bloom filters on what is read by key, for faster lookups at the cost of memory |
| `--max-open-files` | `MAX_OPEN_FILES` | `2000` | Open files; bounds index and filter memory (0 = unlimited) |
| `--write-buffer-mb` | `WRITE_BUFFER_MB` | `256` | Memtables across all column families (0 = unlimited) |
| `--max-wal-mb` | `MAX_WAL_MB` | `1024` | Write-ahead log size that forces flushes (0 = RocksDB default) |
| `--block-cache-mb` | `BLOCK_CACHE_MB` | `1024` | Shared block cache, holding index and filter blocks too (0 = RocksDB default) |

Memory used by RocksDB is roughly the block cache plus the memtables, on top of
the Haskell heap. `/dbstats` shows where it goes.

`--bloom` builds Bloom filters for the table files written while it is on.
They need about 1.25 bytes for each key and are held in the block cache: on a
chain with billions of keys, size that before turning the option on.

The filters are for the data that is read by key: transactions, unspent
outputs, blocks, balances and the mempool. The two indices by address, which
are scanned, never get them. With a filter RocksDB skips the files that
cannot have an address, and then a scan of an address that has nothing can
no longer see the keys that would stop it: it steps over every key deleted
since the last flush. With 5,000 mempool transactions just deleted, such a
scan took 810 microseconds with filters on those indices and 13 without.
`docs/layered-database.md` has the measurements.

Up to and including version 1.8.1 the option had no effect when the store
was built against RocksDB 6.24 or later, as the Docker image is:
`rocksdb-haskell-jprupp` passed the size of the filter to RocksDB with the
wrong type, and RocksDB built none, without an error. The store is now built
with `rocksdb-haskell-jprupp` 2.6.4, which has the fix since 2.6.3. A store that already
runs with `--bloom` therefore starts building filters, and using that memory,
when it is upgraded; turn the option off to keep it as it was.

## API

Full reference: [`swagger.yaml`](swagger.yaml), also served at
[api.haskoin.com](https://api.haskoin.com/). It explains the conventions of
the API (amounts, ordering, paging, formats, errors, unconfirmed transactions
and reorganisations) and the reasons for them, as well as every endpoint.

```sh
curl localhost:3000/address/bc1q…/balance
curl localhost:3000/xpub/xpub6C…/balances
curl localhost:3000/transaction/<txid>
curl -N localhost:3000/events              # stream of new blocks and txs
```

- Add `?pretty=true` for indented JSON.
- Send `Accept: application/octet-stream` for the binary encoding, which
  the Haskell client in `haskoin-store-data` decodes.
- Lists are paged with `limit` and `offset`, newest first. `height` begins
  a page at a transaction hash, a block hash, a block height or a Unix time
  instead.
- Parameters are checked: a parameter that an endpoint does not accept, or
  a value it cannot read, gives 400 with a message that names it, instead of
  being ignored. The Blockchain.info endpoints accept `format=json`, which
  clients written for them often send.
- Errors come back as JSON with an HTTP status: 400 for bad input, 404 for
  unknown things, 409 when a short transaction number is ambiguous, 413 for
  requests that ask for too much.

### Endpoints at a glance

| Area | Endpoints |
| --- | --- |
| Blocks | `/block/best`, `/block/latest`, `/block/:hash`, `/blocks`, `/block/height/:h`, `/block/heights`, `/block/time/:t`, `/block/mtp/:t`, each with a `/raw` variant where it makes sense |
| Transactions | `/transaction/:txid`, `/transactions`, `/transactions/block/:hash`, `/transaction/:txid/after/:height`, `POST /transactions`, `/raw` variants |
| Mempool | `/mempool` |
| Addresses | `/address/:addr/{transactions,transactions/full,balance,unspent}` and the multi-address forms `/address/{transactions,transactions/full,balances,unspent}?addresses=…` |
| Xpubs | `/xpub/:xpub`, `/xpub/:xpub/{transactions,transactions/full,balances,unspent}`, `DELETE /xpub/:xpub` (drop from cache); `derive=legacy\|compat\|segwit` picks the address type (`normal`, the old name of `legacy`, still works) |
| Network | `/peers`, `/health` (503 when unhealthy, 203 without its Redis), `/dbstats` |
| Events | `/events`, as JSON lines over HTTP or as a WebSocket |
| Blockchain.info | `/blockchain/multiaddr`, `/blockchain/balance`, `/blockchain/rawaddr/:addr`, `/blockchain/unspent`, `/blockchain/rawtx/:txid`, `/blockchain/rawblock/:hash`, `/blockchain/latestblock`, `/blockchain/unconfirmed-transactions`, `/blockchain/export-history`, `/blockchain/q/…` |

Turn off the xpub endpoints with `--no-xpub` and the Blockchain.info ones
with `--no-blockchain-info`.

## How it works

```
peers ──► haskoin-node ──► block store ──► RocksDB ──► web server ──► clients
                │               │             ▲
                └─ headers      └─ events ──► cache ◄──► Redis ◄──► other stores
```

- **haskoin-node** keeps a pool of peers and syncs block headers.
- The **block store** (`Haskoin.Store.BlockStore`) downloads blocks from one
  peer at a time and mempool transactions from all of them, and imports them
  with the rules in `Haskoin.Store.Logic.Layered`. Each import is one atomic
  RocksDB batch, so a crash never leaves a half-written block.
- Reorganisations are handled by reverting blocks until the best block is
  back on the main chain of headers.
- The **cache** (`Haskoin.Store.Cache`) keeps xpubs with many used addresses
  or many transactions in Redis: their balances, transactions, unspent
  outputs and the bodies of their transactions. It never computes an entry
  from what it guesses changed: when a block, a mempool transaction or a
  reorganisation touches a cached xpub, the store reads from RocksDB, with
  the same queries that answer uncached requests, the transactions that
  changed and the outputs they create and spend, and writes the differences
  in one Lua script, together with every transaction body the entry needs
  that Redis does not have yet (bodies it has, such as those of the
  transactions a block confirms, are rewritten just before, in small
  scripts, so that a large block does not hold Redis for long).
  The time of a reload no longer grows with the size of the xpub. The store
  reads the whole xpub instead when it first caches it, and whenever it
  cannot be sure the entry has every earlier change: when the entry is more
  than a block behind, when an address with transactions enters or leaves
  the gap, when the balance of an address that no change touched differs
  from Redis's and its newest transactions do not explain why, and on a
  schedule, every five to ten minutes. The interval depends on the xpub, so
  that xpubs cached together are not all read whole at the same moment,
  which would hold them all in memory at once. Under load the database is
  often a little ahead of the events being applied, so a reload first adds
  the transactions that the database has for such an address since the
  block the entry was written at, and the mempool transactions of the entry
  that have since been confirmed or dropped. The script
  writes nothing if the entry changed since it was read, so the entry is
  always what the database says; and Redis, which answers one client at a
  time, is held for the size of the change, not of the xpub. A page of a
  cached xpub is one round trip to Redis. The Lua scripts are in
  `src/Haskoin/Store/Cache/lua/`, one file each, embedded in the library
  when it is compiled. Stores sharing a Redis claim each change, so it
  is applied once, by the first store to see it; a store never replaces an
  entry that knows more than its database (a higher block, or a transaction
  it has not seen), and a store that is catching up does no cache work.
  Without Redis, xpub queries read RocksDB directly. The ways an entry can
  still come to differ from the database unnoticed, such as a missed spend of
  an output of a cached transaction, and what would rule them out, are in
  `docs/cache-consistency.md`.
- A **fleet** (`Haskoin.Store.Fleet.Member`, with `--fleet`) has a cache of
  xpubs of another kind (`Haskoin.Store.Fleet.XPub`), because its servers
  do not answer from their own databases but from one block and one shared
  mempool. An entry has two layers: what the chain has for the addresses of
  the xpub at the block that the fleet answers from, and what the shared
  pool has for them, which the script of the pool writes as it changes the
  pool. The first follows from the chain, so any server writes the same,
  and nothing has to be claimed or repaired: the server that moves the
  fleet to a new block applies what the block did, and a cache that was
  not brought along is emptied. `docs/layered-database.md` has the design,
  the tests and the measurements.
- The **web server** (`Haskoin.Store.Web`) answers from RocksDB, and from the
  cache for cached xpubs and for transaction bodies that Redis has.

Xpub addresses are derived up to an exact gap: every address up to the last
used one of each chain, then exactly `--xpub-gap` unused ones, and always the
first `--xpub-gap-init` addresses. A payment further away is not seen, as a
wallet following BIP 44 with the same gap would not see it; raise the gap to
see it.

An xpub does not record which kind of address its wallet derives, so a
request names it with `derive`. When an xpub has no transactions with the
kind asked for, the store reads the first `--xpub-gap-init` addresses of the
other kinds, and if one of them is used it refuses the request with 400 and
names that kind: an empty answer would look like a wallet with no money, a
mistake that is easy to miss. The extra reads happen only for wallets that
look empty, and not at all on networks without SegWit, which have one kind of
address.

The database layout is described in `Haskoin.Store.Database.Types`.

Unconfirmed transactions are kept in an area of their own, and applied on
top of the chain when a query is answered, so that nothing the mempool does
changes the chain's data. This is version 21 of the database, the layered
layout of `docs/layered-database.md`, which has the reasons, the
measurements and the evidence. Up to version 20 they were written into the
same indices as the block chain. A database of version 20 is migrated the
first time this version of the store opens it: its mempool is moved to the
new area, in batches that can be interrupted, and the log says how many
transactions were moved. **There is no way back**: an older store refuses
the database afterwards, so take a copy of a production database first.

Version 22 makes the chain's data the same on every server that has the
same blocks: a confirmed transaction has the time of its block. Until then
a server that had seen a transaction in its mempool kept the time of that
when a block confirmed it, so two servers gave two times for one confirmed
transaction, and a server that synced later gave a third. A database of
version 21, or of version 20 once its mempool is moved, gets the new times
the first time this version of the store opens it. Nothing lists the
transactions that need it, so **the store reads every transaction that it
has before it starts**, and the log says how far it is every million. It
read 210,000 a second of a small database that was in memory; a database
of Bitcoin has more than a billion, and a disk to read them from, so plan
for hours. It can be interrupted, and starts again from the first
transaction, skipping what is done.
The parts:

- the migration from version 20 (`Haskoin.Store.Database.Migrate`);
- the keys that a mempool transaction writes, the summary that is kept for
  each address they touch, and what the mempool does to the balance and the
  unspent outputs of an address (`Haskoin.Store.Database.Mempool`);
- the rules that import blocks and transactions in that layout
  (`Haskoin.Store.Logic.Layered`), which the tests hold to the results of
  the rules of version 20, operation by operation and key by key;
- the queries over both areas (`Haskoin.Store.Database.Query.Layered`), each
  on a snapshot of the database, one for each request of the web server,
  which the tests hold to the answers of the queries of version 20, page by
  page.

The rules and queries of version 20 (`Haskoin.Store.Logic`,
`Haskoin.Store.Database.Query`) stay in the library for those tests, and
nothing in the store calls them.

## Monitoring

- **`/health`** returns the result of the latest check as JSON, with status 200
  when every check passes and 503 otherwise. The checks are: index lag behind
  the headers, age of the last block and of the last transaction, transactions
  waiting to be imported, and connected peers. Point your load balancer at it.
- **Without Redis** a store that has a cache goes on answering, from its own
  database, and `/health` says 203 instead of 200, with the header
  `Haskoin-Degraded: redis` and `"redis": {"ok": false}` in its JSON. A request waits at most two seconds for Redis
  before the store gives up on it, and the store tries again at every health
  check. 203 is in the range that load balancers take for healthy, so the
  store stays in rotation; alert on anything but 200 to hear of it.
- **StatsD**: with `--statsd`, the store reports the timing of every kind of
  request (`web.*`), database reads (`db.*`), cache hits, misses, reloads and
  full reloads (`cache.*`),
  and block store gauges such as block and header heights and mempool size
  (`store.*`). A server of a fleet reports what it does with the fleet's
  cache of xpubs under `fleet.cache.*`:

  | Metric | Kind | Meaning |
  | --- | --- | --- |
  | `fleet.cache.hits` | counter | Requests about an xpub whose balances the cache gave |
  | `fleet.cache.misses` | counter | Requests about an xpub that the database answered |
  | `fleet.cache.builds` | counter | Entries that this server built, or added addresses to |
  | `fleet.cache.build` | timing | Milliseconds to build an entry |
  | `fleet.cache.abandoned` | counter | Builds given up because the fleet or the entry moved on |
  | `fleet.cache.evictions` | counter | Entries deleted for being the least recently used |
  | `fleet.cache.emptied` | counter | Times that this server emptied the cache |

  Misses include small xpubs, which are never cached, so the share of hits
  says how much of the load the cache takes, not how well it holds what it
  has. `fleet.cache.emptied` should move only at a reorganisation or when a
  server stops at the wrong moment: the log of the server says why each
  time, at the level of information. With `--debug` it also has a line for
  each entry built and each block that the cache was brought to.
- **`/dbstats`** returns RocksDB memory use per column family and RocksDB's own
  statistics, as text.
- **In a fleet** (`--fleet`), any request can be answered 503 with
  `{"error": "behind-fleet"}` by a server that does not have the block that
  the fleet answers from. Have the load balancer try another server, and
  alert if one keeps saying it: its `/health` will say why.
- **Memory.** The resident memory of the store is its GHC heap plus RocksDB:
  the block cache and memtables fill up to `--block-cache-mb` and
  `--write-buffer-mb`, then stay there. The executable is built with
  `--disable-delayed-os-memory-return`, so memory that the heap no longer
  needs goes back to the system at once. Without it, Linux keeps counting
  returned memory as resident (`LazyFree` in `/proc/<pid>/smaps`) until it
  needs it, and the heap looks as if it never shrinks after a peak, such as
  the full reload of a large cached xpub. Pass `+RTS
  --disable-delayed-os-memory-return -RTS` yourself if you build the store
  some other way. To tell such memory from a leak, see
  `stress/results/2026-11-09-memory/README.md`: how to read `smaps`, and a
  heap profile of a busy store, with live data flat at about 10 MiB.
- **Logs** go to standard error, one line per message, tagged with the
  component, such as `[Info#BlockStore] Best block: ...`.

## Using it from Haskell

Query a running server with `haskoin-store-data`:

```haskell
import Control.Monad.Except (runExceptT)
import Data.Default (def)
import Haskoin (btc, textToAddr, withContext)
import Haskoin.Store.WebClient

main :: IO ()
main = withContext $ \ctx -> do
  let cfg = def {net = btc} -- https://api.haskoin.com/btc/...
      Just addr = textToAddr btc "bc1q…"
  r <- runExceptT $ apiCall ctx cfg (GetAddrBalance addr)
  print r
```

The client appends the network name to `host`, as in the layout of
api.haskoin.com. To reach your own server, put it behind a proxy that maps
`/btc/` (or the name of your network) to it.

Or embed the indexer in your own program with `haskoin-store`: `withStore`
starts the node, the block store and the cache from a `StoreConfig`, and the
functions of `Haskoin.Store.Database.Query.Layered` query its database in
`IO`, on a view of it taken with `withView`, such as
`withView store.db (\v -> getBalance v addr)`.
Subscribe to `Store.pub` to receive `StoreEvent`s.

Every module has Haddock documentation; build it with `stack haddock`.

## Packages

Haskoin Store is two Haskell packages, each in its own repository:

- **`haskoin-store`**, this repository: the indexer, the web server and the
  executable.
- **[`haskoin-store-data`](https://github.com/jprupp/haskoin-store-data)**:
  the data types, their JSON and binary encodings, and a client for the API
  (`Haskoin.Store.WebClient`). Use this one to talk to a Haskoin Store server
  from Haskell. It is small to depend on, as it has no database or node.

They were released together from this repository up to 1.8.1. A change to
the wire format is made in haskoin-store-data, released, and then picked up
here; `swagger.yaml` stays here and describes the server.

## Development

```sh
stack build --test --no-run-tests
stack test                     # includes a 6000-block heap-growth test
scripts/format                 # ormolu, the code style used here
scripts/bench-web.sh           # load-test the API with wrk
scripts/stress-cache.sh        # stress the Redis cache, with a report
scripts/check-local-deps.sh    # what CI checks: no local paths in stack.yaml
scripts/install-system-libraries.sh  # the C libraries, on Ubuntu or Debian
```

`stack.yaml` must pin released versions of every dependency. Pointing
`extra-deps` at a sibling checkout such as `../haskoin-core` is fine for
local testing, but CI rejects it.

The tests use [hspec](https://hspec.github.io/). The unit tests need no network
access. The integration tests (`Haskoin.Store.IntegrationSpec`) run regtest
chains of Bitcoin Core and Bitcoin Cash Node, and Valkey, in
[Podman](https://podman.io/) containers, and check the store against them: the
exact gap, spends and change, mempool order, multiaddr counts, reorganisations,
and that the cache answers exactly like the database on every xpub endpoint.

- They are skipped if Podman cannot run containers, or if
  `HASKOIN_STORE_NO_PODMAN` is set.
- The first run builds the image `localhost/haskoin-store-test`, which needs
  network access: Ubuntu 26.04 pinned by digest, Valkey from a fixed snapshot of
  the Ubuntu archive, and the release tarballs of both nodes, checked against
  SHA-256 hashes pinned in `test/Haskoin/Store/Integration/Image.hs`. No
  third-party image is used.
- Containers are removed when the tests end, data lives in tmpfs mounts and
  under `/dev/shm`, and every container has a memory limit.
- Set `HASKOIN_TEST_LOG=1` to print the log of the store under test.

The test suite runs with at most four cores and a 4 GiB heap, so that a
runaway test fails instead of exhausting the machine.

`package.yaml` is the source of the package description. Change it rather
than `haskoin-store.cabal`, which
[hpack](https://github.com/sol/hpack) generates.

### Stress test of the cache

`scripts/stress-cache.sh [OUT_DIR] [-- OPTIONS]` measures the cache while it
works hard. It runs a regtest chain of Bitcoin Core, Valkey and wrk in Podman
containers from the test image, and the `haskoin-store` binary with the
cache on. A few wallets, whose keys the stress program (`stress/Main.hs`)
holds, make hundreds of thousands of transactions between them, signed by
the program and sent to the node in batches, with a block after each batch.
Every transaction changes two cached xpubs, so the cache reloads them over
and over, while wrk asks the server about the xpubs on five endpoints
(summary, balances, transactions, unspent outputs and multiaddr) the whole
time. At the end it compares the cached answers with those of the database,
and `scripts/stress-report.py` writes `OUT_DIR/report.md`: how fast the
transactions went in, how many reloads the cache did, how many of them read
the whole xpub, and how long they took as the xpubs grew, how far the server
fell behind, its CPU and memory, the
memory of Valkey, the commands that blocked Redis for 10 ms or more (from
its slow log, by script, with the number of arguments of each, which grows
with the size of a commit), and the latencies of each endpoint over time.

- The defaults are 200,000 transactions between 4 wallets of 200 receiving
  and 200 change addresses, 2,000 per block; pass `-- --help` for the
  options, and `-- --txs 20000` for a run of a few minutes. A default run
  takes about half an hour and up to about 7 GiB of memory.
- The raw measurements stay in `OUT_DIR` as JSON lines (`rounds.jsonl`,
  `samples.jsonl`, `wrk.jsonl`), with `summary.json` and the log of the
  server.
- No container is left behind, even if the script is interrupted or
  killed: the program removes its containers when it ends, and the script
  removes any that carry its label and stops the server.
- Results of past runs are kept in `stress/results/<date>/` for comparison:
  the report, the measurements (without the server log), and a README with
  the commits, the machine and what the run found. A new run worth keeping
  goes there too.
- To look for a leak, run the server with a heap profile by type, which needs
  no profiling build, and summarise it: the live heap over time and the types
  of closure that grew most.

  ```sh
  scripts/stress-cache.sh /tmp/run -- --txs 60000 \
    --server-rts '-N4 -M2g -T -hT -i10 -po/tmp/heap'
  scripts/heap-profile-summary.py /tmp/heap.hp
  ```

### Measurements for the layered database

`docs/layered-database/` has the programs behind the numbers in
`docs/layered-database.md`, with their output. They are not part of the
package; each says in its header how to build and run it.

- `OverlayCost.hs` times the lookups that reading the mempool from an area
  of its own adds to a query, in RocksDB and in memory.
- `MigrateBench.hs` times the migration to the layered layout against
  wiping the same mempool.
- `QueryCost.hs` writes the same synthetic history into a database of each
  layout, with each set of rules, and times both: importing blocks and
  mempool transactions, and every kind of query, for addresses and wallets
  that have nothing, a little and much in the mempool. It checks that both
  databases give the same answers.
- `FleetCost.hs` asks one store the same questions alone and as a member
  of a fleet, with its mempool in a pool in Redis, checks that the answers
  are the same, and times both. It asks about wallets a third way too,
  through the fleet's cache of xpubs.

### Contributing

Pull requests are welcome.

- Format the code with `scripts/format` (Ormolu), and keep every line of
  code shorter than 120 columns.
- Put scripts in files of their own rather than embedding them: Lua for
  Redis under `src/Haskoin/Store/Cache/lua/`, shell under `scripts/`, called
  from CI rather than written into the workflow.
- Add tests for the behaviour you change.
- Describe the change under a new version in `CHANGELOG.md`, which follows
  [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
- Update `swagger.yaml` when you change the API.

## Related projects

- [haskoin-core](https://github.com/jprupp/haskoin-core) has the Bitcoin data
  types and protocol.
- [haskoin-node](https://github.com/jprupp/haskoin-node) connects to peers and
  syncs headers.
- [nqe](https://github.com/jprupp/nqe),
  [rocksdb-haskell-jprupp](https://github.com/jprupp/rocksdb-haskell),
  [rocksdb-query](https://github.com/jprupp/rocksdb-query) and
  [statsd-rupp](https://github.com/jprupp/statsd-rupp) provide concurrency,
  storage and metrics.

## License

Haskoin Store is released under the
[MIT License](https://github.com/jprupp/haskoin-store/blob/master/haskoin-store/LICENSE).
