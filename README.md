# Haskoin Store

[![CI](https://github.com/jprupp/haskoin-store/actions/workflows/ci.yml/badge.svg)](https://github.com/jprupp/haskoin-store/actions/workflows/ci.yml)
[![Hackage haskoin-store](https://img.shields.io/hackage/v/haskoin-store.svg?label=haskoin-store)](https://hackage.haskell.org/package/haskoin-store)
[![Hackage haskoin-store-data](https://img.shields.io/hackage/v/haskoin-store-data.svg?label=haskoin-store-data)](https://hackage.haskell.org/package/haskoin-store-data)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/jprupp/haskoin-store/blob/master/LICENSE)

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
| Formats         | JSON                                                                                         |
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
curl -s 'http://localhost:3000/block/best' | jq
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
curl -s 'localhost:3000/xpub/xpub6C...?derive=segwit' | jq
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

The body is the signed transaction in hex. The store hands it to one full
node and answers with the transaction id once a node has announced it back
and the store has it: the wallet's history shows it on the next request.

A transaction that cannot be valid, or that a node refuses with a reason, is
answered 400 with the reason. A 500 means that the store could not find out:
post the same transaction again, which sends nothing twice.
[Publishing](#publishing) has the rest.

### 6. Stay up to date without polling

```sh
curl -N localhost:3000/events
```

Each line is a new block or a mempool change. A wallet back end keeps this open
and refreshes the affected wallets when an event arrives, instead of asking
every few seconds.

After a lost connection, come back with the hash of the last event you
heard, and the stream starts with what came after it:

```sh
curl -N "localhost:3000/events?after=9a5c…"
```

The server keeps its last 10,000 events. It answers 404 when it no longer
has the one you name: read your wallets again, and listen without `after`.

Over a WebSocket at the same path, a client can ask for only the
transactions it cares about. It sends one or more of these, as text:

```json
{"type":"watch","addresses":["bc1q…","1A1z…"]}
{"type":"watch","xpub":"xpub6C…","derive":"segwit"}
{"type":"filterload","filter":"b50f","functions":11,"tweak":0,"flags":0}
```

From then on it hears of a transaction only when it touches one of the
addresses, an address of the key, or matches the Bloom filter (BIP 37, as
SPV wallets use). Blocks are always sent. A key is followed as a wallet
uses it: the gap of unused addresses moves on as payments come. For a key
with several types of address, send one message for each type.

A client that connects and then sends its messages gets every transaction
until the first message is read. To start with a choice, put it in the URL,
of the WebSocket or of the HTTP stream:

```sh
curl -N "localhost:3000/events?quiet=true"                  # blocks only
curl -N "localhost:3000/events?addresses=bc1q…,1A1z…"
curl -N "localhost:3000/events?xpub=xpub6C…&derive=legacy,segwit"
```

`quiet=true` sends blocks alone until the client watches something; a
WebSocket client with a long list connects with it and sends the list in
messages. The [API reference](swagger.yaml) has every message and
parameter.

### 7. Make big wallets fast

Start the store with `--redis` and a Redis (or Valkey) server.

- **Large xpubs are cached** after their first query and answered from
  Redis, transactions in full included. Large means at least
  `--redis-min-addrs` used addresses, or at least `--redis-min-txs`
  transactions.
- **Several stores can share one Redis.** They keep each other's cache up
  to date.
- Add `nocache=true` to a query to read the database instead.

Servers behind a load balancer should also be started with `--fleet`
([A fleet of servers](#a-fleet-of-servers)). They then answer every request
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
| `--discover` | `DISCOVER` | off | Discover peers through DNS seeds and relayed addresses; on regtest it finds none, so name a `--peer` |
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
| `--tls-cert` / `--tls-key` | `TLS_CERT` / `TLS_KEY` | none | Serve HTTPS with this certificate chain and private key, both files in PEM |
| `--no-xpub` | `NO_XPUB` | off | Disable the xpub endpoints |
| `--no-blockchain-info` | `NO_BLOCKCHAIN_INFO` | off | Disable the `/blockchain/` endpoints and price lookups |
| `--max-item-count` | `MAX_ITEM_COUNT` | `200000` | Most items in a list (0 = no limit) |
| `--max-full-item-count` | `MAX_FULL_ITEM_COUNT` | `5000` | Most full transactions in a list (0 = no limit) |
| `--max-offset` | `MAX_OFFSET` | `50000` | Largest `offset` (0 = no limit) |
| `--slow-request` | `SLOW_REQUEST` | `5` | Seconds a request may take before the log warns of it (0 = never) |
| `--publish-timeout` | `PUBLISH_TIMEOUT` | `30` | Seconds that `POST /transactions` waits for a node to announce the transaction back |
| `--node-rpc` | `NODE_RPC` | none | JSON-RPC URL of a full node, such as `http://user:password@127.0.0.1:8332/`, to publish transactions through; without it, or when it does not answer, they go to a peer |
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
- **After a long outage** the fleet starts its shared mempool again. When
  the servers come back more than 50 blocks past the block that the fleet
  last answered from, the first of them moves the fleet there, empties
  the shared mempool, and all offer their unconfirmed transactions again.
  The log has a warning when it happens.
- **`/events` is the same stream on every server.** What changes the
  shared mempool or moves the fleet to a block writes an event to Redis
  in the same step, and every server sends those events, in that order,
  instead of what its own node saw. A block is told when the whole fleet
  can answer about it. Each server keeps the same last events, so a
  client that reconnects with `after=` may land on any of them.
- **Without Redis** each server answers from its own database, and
  `/health` says 203, as it does for the cache.
- **Large xpubs are cached for the whole fleet**, under the same
  `--redis-min-addrs`, `--redis-min-txs` and `--redis-max-xpubs` as the
  cache of a server alone. Set them the same on every server.
  - A cached answer is the one that the database gives, on every server.
  - The server that moves the fleet to a new block brings the cache along.
    In the moment before it has, and for a wallet that just gained or lost
    an address, requests are answered from the database.
  - A reorganisation empties the cache.
  - A request about a wallet of 464 addresses and 19,499 transactions
    takes 1.3 to 2.2 milliseconds with it, where it took 5 to 38.

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
| `--health-check-interval` | `HEALTH_CHECK_INTERVAL` | `1` | Seconds between checks; there is one at each new block too |

### Metrics and logging

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--statsd` | `STATSD` | off | Send metrics to StatsD |
| `--statsd-host` / `--statsd-port` | `STATSD_HOST` / `STATSD_PORT` | `localhost` / `8125` | StatsD server |
| `--statsd-prefix` | `STATSD_PREFIX` | `haskoin_store` | Prefix of every metric |
| `--statsd-no-events` | `STATSD_NO_EVENTS` | off | Do not send individual samples |
| `--statsd-aggregates` | `STATSD_AGGREGATES` | off | Also send aggregated statistics every second |
| `--access-log` | `ACCESS_LOG` | off | Log every request, with the address of the client and the time it took |
| `--debug` | `DEBUG` | off | Verbose logging |

### RocksDB

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--no-bloom` | `BLOOM` | on | No Bloom filters on what is read by key; `BLOOM=false` is the same |
| `--max-open-files` | `MAX_OPEN_FILES` | `2000` | Open files; bounds index and filter memory (0 = unlimited) |
| `--write-buffer-mb` | `WRITE_BUFFER_MB` | `256` | Memtables across all column families (0 = unlimited) |
| `--max-wal-mb` | `MAX_WAL_MB` | `1024` | Write-ahead log size that forces flushes (0 = RocksDB default) |
| `--block-cache-mb` | `BLOCK_CACHE_MB` | `1024` | Shared block cache, holding index and filter blocks too (0 = RocksDB default) |

Memory used by RocksDB is roughly the block cache plus the memtables, on top of
the Haskell heap. `/dbstats` shows where it goes.

Bloom filters are on by default. They let RocksDB skip the table files that
cannot have a key, which is most of what a lookup of a transaction, an
output or a balance would otherwise read.

- **What has them:** the data that is read by key: transactions, spenders,
  unspent outputs, blocks, balances and the mempool. The two indices by
  address, which are scanned, do not.
- **What they cost:** about 1.25 bytes for each key, held in the block
  cache. On a chain with billions of keys that is gigabytes: size
  `--block-cache-mb` for it, or turn them off with `--no-bloom`.
- **An existing database** gets filters for the table files that are
  written from now on, so their memory comes gradually, as RocksDB
  compacts. The migration to database version 22 rewrites nearly every
  transaction, and with it most of the filters arrive at once.
- **`--bloom`** was the way to turn them on, and is still accepted. Up to
  version 1.8.1 it built no filters at all on RocksDB 6.24 or later,
  because of a wrong type in the binding, fixed in
  `rocksdb-haskell-jprupp` 2.6.3.

## API

Full reference: [`swagger.yaml`](swagger.yaml), also served by every
server about itself (below). It explains the conventions of
the API (amounts, ordering, paging, formats, errors, unconfirmed transactions
and reorganisations) and the reasons for them, as well as every endpoint.

```sh
curl localhost:3000/address/bc1q…/balance
curl localhost:3000/xpub/xpub6C…/balances
curl localhost:3000/transaction/<txid>
curl -N localhost:3000/events              # stream of new blocks and txs
```

- The root of the server, `/`, is the reference of its API as a page that
  can try requests, and `/swagger.yaml` the file behind it. The file names
  one server, `./`, so requests tried from the page go to the server that
  served it, also behind a proxy that mounts it under a path such as
  `/btc/`. The reference always matches the version that is running.
- Answers are JSON, without spaces. Pipe them through `jq` to read them
  indented.
- Lists are paged with `limit` and `offset`, newest first. `height` begins
  a page at a transaction hash, a block hash, a block height or a Unix time
  instead.
- Parameters are checked: a parameter that an endpoint does not accept, or
  a value it cannot read, gives 400 with a message that names it, instead of
  being ignored. The Blockchain.info endpoints accept `format=json`, which
  clients written for them often send.
- Errors come back as JSON with an HTTP status: 400 for bad input, 404 for unknown things, 409 when a short transaction
  number is ambiguous, 413 for requests that ask for too much.

### HTTP and HTTPS

- **`HEAD` and `OPTIONS`** are answered, the second with what a browser
  may send from another site.
- **Answers are compressed** for a client that sends
  `Accept-Encoding: gzip`, but for the stream of events.
- **Long answers are sent while they are read.** The endpoints that answer
  with all the transactions that the database lists somewhere read them a
  hundred at a time and send each hundred while they read the next: the
  full transactions of addresses and of an xpub (`.../transactions/full`),
  the transactions of a block (`/transactions/block/:block` and its `/raw`),
  the raw blocks (`/block/.../raw`), and the blocks of the Blockchain.info
  API (`/blockchain/rawblock/:block`, `/blockchain/block-height/:height`).
  The first byte leaves sooner and the server holds two hundred
  transactions, not the whole answer. These answers have no
  `Content-Length`. What can refuse a request is checked before the first
  byte; a failure after it ends the answer before it is complete. Lists
  that the client names, such as `/transactions?txids=`, are read whole
  first, because what is asked for may not be there.
- **Long lists go in a body.** The endpoints that take a list of
  addresses, blocks, heights or transactions also take it under `POST`,
  as a form (`application/x-www-form-urlencoded`), so that it does not
  have to fit in a URL. `/transactions` does not: `POST` there publishes
  a transaction.
- **A transaction to publish has a largest size**: the body of
  `POST /transactions` may be as long as the longest transaction of the
  network in hexadecimal, 8 MB with SegWit and 2 MB on Bitcoin Cash. More
  is answered 413 without being read whole.
- **A 503 says when to try again**, in `Retry-After`.
- **HTTPS is optional and off by default.** With `--tls-cert` and
  `--tls-key` the server speaks HTTPS on its port, and nothing else. A
  public service usually has a proxy in front that holds the certificate;
  this is for a server by itself. The files are read at start, so a
  renewed certificate takes a restart.

### Endpoints at a glance

| Area | Endpoints |
| --- | --- |
| Blocks | `/block/best`, `/block/latest`, `/block/:hash`, `/blocks`, `/block/height/:h`, `/block/heights`, `/block/time/:t`, `/block/mtp/:t`, each with a `/raw` variant where it makes sense |
| Transactions | `/transaction/:txid`, `/transactions`, `/transactions/block/:hash`, `/transaction/:txid/after/:height`, `POST /transactions`, `/raw` variants |
| Mempool | `/mempool` |
| Addresses | `/address/:addr/{transactions,transactions/full,balance,unspent}` and the multi-address forms `/address/{transactions,transactions/full,balances,unspent}?addresses=…` |
| Xpubs | `/xpub/:xpub`, `/xpub/:xpub/{transactions,transactions/full,balances,unspent}`, `DELETE /xpub/:xpub` (drop from cache); `derive=legacy\|compat\|segwit` picks the address type (`normal`, the old name of `legacy`, still works) |
| Network | `/peers`, `/health` (503 when unhealthy, 203 without its Redis), `/dbstats` |
| Events | `/events`, as JSON lines over HTTP or as a WebSocket; `after=` resumes, and a WebSocket client can filter by address, xpub or Bloom filter |
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
- The **block store** downloads blocks from one peer at a time and mempool
  transactions from all of them, and imports them. Each import is one
  atomic RocksDB batch, so a crash never leaves a half-written block.
- **Reorganisations** are handled by reverting blocks until the best block
  is back on the main chain of headers.
- The **web server** answers from RocksDB, and from Redis for what is
  cached there.

### Publishing

The store does not check scripts, so a transaction that a client posts
never goes into its mempool directly: anyone could show a payment that will
never confirm. It is handed to one full node, and comes in when a node
announces it back.

- **A connection of its own.** The transaction goes to a node on a
  connection made for it, not on those the store syncs from. A node does
  not announce a transaction to the connection that gave it. On the others
  the store hears of it as of any transaction, and imports it the same way.
- **The answer waits for that**, up to `--publish-timeout` seconds. A 200
  means that a full node accepted the transaction and the store has it.
- **The store checks what it can first**: that the inputs exist and are
  not spent, that a coinbase is old enough, the amounts, the lock time,
  the size and the dust rule of relay, and the signature of every coin
  that is locked to one key, on both networks. A transaction that fails
  is refused with the reason, and nothing is sent. A script of any other
  kind is left to the node.
- **Over the RPC of a node, when the store has it.** With `--node-rpc`
  the store gives the transaction to a full node over its RPC. The node
  checks everything and says why it refuses. If the node does not
  answer, the transaction goes to a peer instead.
- **To a peer, only what the store verified.** A peer that is sent a
  transaction it finds invalid may close every connection of the store.
  So a transaction with an input that the store cannot verify, a script
  that is not a key or a multisig, is refused unless there is an RPC to
  send it over. Give the store the RPC of a node to publish those.
- **The same transaction is sent once**, however often it is posted.
- **Whitelist the store on your nodes** (`-whitelist` in Bitcoin Core and
  Bitcoin Cash Node). A node announces to a whitelisted peer without
  delay, so the answer takes milliseconds; otherwise Bitcoin Core takes
  five seconds on average. And a node that finds a transaction invalid may
  close every connection of the peer that sent it, which it never does to
  a whitelisted one. The store then connects again, does not send that
  transaction a second time, and goes on publishing the others.

`docs/publishing.md` has the design, what was measured on Bitcoin Core and
Bitcoin Cash Node, and how it differs from versions before 1.11.0.

### The database

The chain and the mempool are kept apart.

- **The chain's data is written once and is the same on every server** that
  has the same blocks. A confirmed transaction has the time of its block.
  What spends its outputs is under keys of its own, so its record never
  changes.
- **Unconfirmed transactions live in an area of their own**, and are applied
  on top of the chain when a query is answered. Nothing the mempool does
  changes the chain's data.
- **A transaction that leaves the mempool unconfirmed is forgotten**: it was
  replaced, double-spent by a block, or too old. The API answers 404 for
  it.
- **Each request reads one snapshot** of the database, so everything in an
  answer is of the same moment.

`docs/layered-database.md` has the reasons, the measurements and the tests.
The layout of the keys is described in `Haskoin.Store.Database.Types`.

### Upgrading an existing database

This is version 22 of the database. The store migrates an older database
the first time it opens it. **There is no way back**: an older store
refuses the database afterwards, so take a copy first.

| From | What the migration does | How long |
| --- | --- | --- |
| 20 | Moves the mempool to its own area | Seconds to minutes |
| 21 | Reads every transaction and writes nearly every one again | Many hours for Bitcoin |

The second step is the long one. Nothing lists the records that need a
change, and nearly all do.

- **Time:** a database of Bitcoin has more than a billion transactions.
  The store does not start until it is done. The log says how far it is
  every million transactions.
- **Disk:** it needs room for a second copy of the transactions, until
  RocksDB has compacted the first away.
- **Interrupted,** it starts again and skips what is done.
- **Deleted transactions** that an old database still holds are left where
  they are and ignored. Finding them would mean reading everything again.

What changed for clients in version 22:

- A confirmed transaction that the server first saw in its mempool used to
  keep that time. Now every server gives the time of the block.
- Reading one confirmed transaction in full takes a second key: 18
  microseconds where it took 11.5. Lists by address and by xpub cost the
  same as before.

### Extended public keys

**The gap.** Addresses are derived up to an exact gap: every address up to
the last used one of each chain, then exactly `--xpub-gap` unused ones, and
always the first `--xpub-gap-init`. A payment further away is not seen, as
a wallet following BIP 44 with the same gap would not see it. Raise the gap
to see it.

**The type of address.** An xpub does not record which kind of address its
wallet derives, so a request names it with `derive`.

- If the xpub has no transactions of the kind asked for, the store looks at
  the first `--xpub-gap-init` addresses of the other kinds.
- If one of them is used, it refuses the request with 400 and names that
  kind. An empty answer would look like a wallet with no money, a mistake
  that is easy to miss.
- The extra reads happen only for wallets that look empty, and never on
  networks without SegWit, which have one kind of address.

### The cache of a server alone

With `--redis`, the cache (`Haskoin.Store.Cache`) keeps large xpubs in
Redis: their balances, transactions, unspent outputs and the bodies of
their transactions. Without Redis, xpub queries read RocksDB directly.

- **It reads the database, it does not guess.** When a block, a mempool
  transaction or a reorganisation touches a cached xpub, the store reads
  what changed from RocksDB, with the same queries that answer uncached
  requests, and writes the differences to Redis in one Lua script.
- **An update costs the size of the change**, not of the xpub. Redis
  answers one client at a time, so this is what keeps it responsive.
- **The script writes nothing if the entry changed since it was read**, so
  an entry is always what the database says.
- **It reads the whole xpub when it cannot be sure** that the entry has
  every earlier change: when it first caches it, when the entry is more
  than a block behind, when an address enters or leaves the gap, when a
  balance does not add up, and every five to ten minutes in any case.
- **Stores that share a Redis** claim each change, so it is applied once. A
  store never replaces an entry that knows more than its own database, and
  a store that is catching up does no cache work.
- **A page of a cached xpub** is one round trip to Redis.

The Lua scripts are in `src/Haskoin/Store/Cache/lua/`, one file each,
compiled into the library. The ways an entry could still come to differ
from the database unnoticed, and what would rule them out, are in
`docs/cache-consistency.md`.

### The cache of a fleet

A fleet (`--fleet`) has a cache of another kind
(`Haskoin.Store.Fleet.XPub`), because its servers do not answer from their
own databases but from one block and one shared mempool.

An entry has two layers:

- what the chain has for the addresses of the xpub, at the block that the
  fleet answers from;
- what the shared pool has for them, which the pool's own script writes as
  it changes the pool.

The first follows from the chain, so any server writes the same. Nothing
has to be claimed or repaired: the server that moves the fleet to a new
block applies what the block did, and a cache that was not brought along
is emptied. `docs/layered-database.md` has the design and the tests.

### Where the code is

| Module | What it does |
| --- | --- |
| `Haskoin.Store.BlockStore` | Downloads and imports blocks and mempool transactions |
| `Haskoin.Store.Logic.Layered` | The rules that import blocks and transactions |
| `Haskoin.Store.Database.Mempool` | The keys of the mempool area, and what it does to balances |
| `Haskoin.Store.Database.Query.Layered` | The queries over both areas, each on a snapshot |
| `Haskoin.Store.Database.Migrate` | The migrations from versions 20 and 21 |
| `Haskoin.Store.Cache` | The Redis cache of a server alone |
| `Haskoin.Store.Fleet.Member`, `Haskoin.Store.Fleet.XPub` | A fleet, and its cache |
| `Haskoin.Store.Web`, `Haskoin.Store.Web.Events` | The web server, and `/events` |
| `Haskoin.Store.Web.Pieces` | Answers that are sent while they are read |
| `Haskoin.Store.Web.Publish` | Publishing a transaction through a full node |

The rules and queries of version 20 (`Haskoin.Store.Logic`,
`Haskoin.Store.Database.Query`) stay in the library as a reference. The
tests hold the new ones to their results, operation by operation and page
by page. Nothing in the store calls them.

## Monitoring

### Health

`/health` returns the result of the latest check as JSON. Point your load
balancer at it.

| Status | Meaning |
| --- | --- |
| 200 | Every check passes |
| 203 | Healthy, but its Redis does not answer |
| 503 | A check fails; the body says which |

The check runs every second, and at once when a block arrives, so what
`/health` answers is at most a second old. Redis is asked every five
seconds.

The checks are: how far the index is behind the headers, the age of the
last block and of the last transaction, the transactions waiting to be
imported, and the connected peers.

**Without its Redis**, a store that has a cache goes on answering from its
own database:

- `/health` says 203, with the header `Haskoin-Degraded: redis` and
  `"redis": {"ok": false}` in its JSON.
- A request waits at most two seconds for Redis before the store gives up
  on it. The store tries Redis again at every health check.
- Load balancers take 203 for healthy, so the store stays in rotation.
  Alert on anything but 200 to hear of it.

**In a fleet** (`--fleet`), a server that does not have the block that the
fleet answers from can answer any request with 503 and
`{"error": "behind-fleet"}`. Have the load balancer try another server, and
alert if one keeps saying it: its `/health` will say why.

### StatsD

With `--statsd`, the store reports:

| Prefix | What |
| --- | --- |
| `web.*` | Timing of every kind of request |
| `db.*` | Database reads |
| `cache.*` | Cache hits, misses, reloads and full reloads |
| `store.*` | Block and header heights, mempool size and other gauges |
| `fleet.cache.*` | What a server of a fleet does with the fleet's cache of xpubs |

The metrics of the fleet's cache:

| Metric | Kind | Meaning |
| --- | --- | --- |
| `fleet.cache.hits` | counter | Requests about an xpub whose balances the cache gave |
| `fleet.cache.misses` | counter | Requests about an xpub that the database answered |
| `fleet.cache.builds` | counter | Entries that this server built, or added addresses to |
| `fleet.cache.build` | timing | Milliseconds to build an entry |
| `fleet.cache.abandoned` | counter | Builds given up because the fleet or the entry moved on |
| `fleet.cache.evictions` | counter | Entries deleted for being the least recently used |
| `fleet.cache.emptied` | counter | Times that this server emptied the cache |
| `fleet.cache.xpubs` | gauge | Xpubs in the cache, whichever server cached them |
| `fleet.cache.addresses` | gauge | Addresses that those xpubs have between them |

- **Misses include small xpubs**, which are never cached. So the share of
  hits says how much of the load the cache takes, not how well it holds
  what it has.
- **`fleet.cache.emptied`** should move only at a reorganisation or when a
  server stops at the wrong moment. The log says why each time.
- With `--debug` the log also has a line for each entry built and each
  block that the cache was brought to.

### Database statistics

`/dbstats` returns the memory that RocksDB uses for each column family, and
RocksDB's own statistics, as text.

### Memory

The resident memory of the store is its GHC heap plus RocksDB.

- **RocksDB:** the block cache and memtables fill up to `--block-cache-mb`
  and `--write-buffer-mb`, then stay there.
- **The heap** gives memory back to the system as soon as it no longer
  needs it. The executable is built with
  `--disable-delayed-os-memory-return` for that. Without the flag, Linux
  keeps counting returned memory as resident until it needs it, and the
  heap looks as if it never shrinks after a peak.
- If you build the store some other way, pass
  `+RTS --disable-delayed-os-memory-return -RTS` yourself.
- **To tell such memory from a leak**, see
  `stress/results/2026-11-09-memory/README.md`: how to read `smaps`, and a
  heap profile of a busy store, with live data flat at about 10 MiB.

### Logs

Logs go to standard error, one line per message, tagged with the component,
such as `[Info#BlockStore] Best block: ...`.

| Level | What it is for |
| --- | --- |
| Error | Something wrong with the store or its database. Alert on these |
| Warn | Something unusual that the store dealt with: a misbehaving peer, no peers left, Redis or the price service not answering, a block reverted, the shared mempool of a fleet started again |
| Info | Milestones: start-up, each new best block, migrations, the mempool pruned |
| Debug | Everything else, with `--debug`: each peer, each transaction, each request |

What the log says about the server itself, without `--debug`:

- **Health.** One line when `/health` changes: `No longer healthy: 5 blocks
  behind the headers` as a warning, and `Healthy again`. A store that is
  still syncing says `Not healthy yet` once, which is not a warning.
- **Slow requests.** A warning for a request that took `--slow-request`
  seconds or more to answer, with the request and how long it took. The
  stream of events is left out.
- **Every request**, with `--access-log`, which is off by default:
  `203.0.113.7 GET /block/best HTTP/1.1: 200 OK, 0.4 ms`. A public
  server usually has a proxy in front that keeps this log better. The
  address is the first of `X-Forwarded-For` when a proxy sent one.

An outage of Redis is one warning when it starts, `Could not … Going on
without`, and one line when it is over, `Can … again`. The attempts in
between are logged for debugging.

Nothing that a peer or a client does is an error. A transaction that the
store refuses (an orphan, a double spend), a request with a bad parameter
or for something unknown, and a peer that hangs up are all ordinary on a
public network, and are logged for debugging.

## Using it from Haskell

Query a running server with `haskoin-store-data`:

```haskell
import Data.Default (def)
import Haskoin (btc, textToAddr, withContext)
import Haskoin.Store.WebClient

main :: IO ()
main = withContext $ \ctx -> do
  let cfg = def {net = btc} -- https://api.haskoin.com/btc/...
      Just addr = textToAddr btc "bc1q…"
  r <- apiCall ctx cfg (GetAddrBalance addr)
  print r
```

`apiCall` answers `Right` the result or `Left` an error. `apiEvents`
listens to `/events`, and `apiEventsAfter` takes up from an event.

The client appends the network name to `host`, as in the layout of
api.haskoin.com. To reach your own server, put it behind a proxy that maps
`/btc/` (or the name of your network) to it.

Or embed the indexer in your own program with `haskoin-store`:

- `withStore` starts the node, the block store and the cache from a
  `StoreConfig`.
- The functions of `Haskoin.Store.Database.Query.Layered` query its
  database in `IO`, on a view taken with `withView`, such as
  `withView store.db (\v -> getBalance v addr)`.
- Subscribe to `Store.pub` to receive `StoreEvent`s.

Every module has Haddock documentation; build it with `stack haddock`.

## Packages

Haskoin Store is two Haskell packages, each in its own repository:

- **`haskoin-store`**, this repository: the indexer, the web server and the
  executable.
- **[`haskoin-store-data`](https://github.com/jprupp/haskoin-store-data)**:
  the data types, their JSON, and a client for the API
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
works hard.

What it runs:

1. A regtest chain of Bitcoin Core, Valkey and wrk, in Podman containers
   from the test image, and the `haskoin-store` binary with the cache on.
2. A few wallets, whose keys the stress program (`stress/Main.hs`) holds,
   make hundreds of thousands of transactions between them. The program
   signs them and sends them to the node in batches, with a block after
   each batch.
3. Every transaction changes two cached xpubs, so the cache reloads them
   over and over. Meanwhile wrk asks the server about the xpubs on five
   endpoints: summary, balances, transactions, unspent outputs and
   multiaddr.
4. At the end it compares the cached answers with those of the database.

`scripts/stress-report.py` then writes `OUT_DIR/report.md`, with:

- how fast the transactions went in, and how far the server fell behind;
- how many reloads the cache did, how many read the whole xpub, and how
  long they took as the xpubs grew;
- the CPU and memory of the server, and the memory of Valkey;
- the commands that blocked Redis for 10 ms or more, from its slow log;
- the latencies of each endpoint over time.

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

Haskoin Store is released under the [MIT License](LICENSE).
