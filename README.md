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
- **Never half-write.** Each block is one atomic database write, and each
  change of the shared pool one Redis script. A crash leaves the store at the previous block,
  with no repair step on restart.
- **Unconfirmed transactions matter.** The mempool is one pool in Redis that
  every store of a fleet shares, indexed by address, ordered before
  confirmed history, with replace-by-fee on Bitcoin. A wallet sees a
  payment the moment the network does.
- **Extended public keys are first class.** Wallets are mostly xpubs, so the
  store derives their addresses up to an exact gap, remembers the derivations,
  and keeps wallets ready in a cache in the Redis that the stores of a
  fleet share.
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
| Xpubs           | Balances, transactions and UTXO of extended public keys, for legacy, nested SegWit and native SegWit addresses, from the fleet's cache in Redis |
| Tokens          | CashTokens on Bitcoin Cash: the tokens of every output, token balances of addresses, and the holders, transactions and UTXO of each category |
| Blocks and txs  | Lookup by hash, height or time; raw serialized forms; transactions of a block               |
| Mempool         | One pool in Redis shared by the fleet, by arrival time, with replace-by-fee on BTC and expiry |
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
docker network create haskoin
docker run -d --name valkey --network haskoin valkey/valkey
docker run -d --name haskoin-store --network haskoin -p 3000:3000 \
  -e NET=btc -e DISCOVER=true -e REDIS_URL=redis://valkey:6379 \
  -v haskoin-store-data:/home/haskoin/.haskoin-store \
  haskoin-store
curl -s 'http://localhost:3000/block/best' | jq
```

The store needs a Redis or Valkey server, here in a container of its own:
without one it does not start. The first sync downloads and indexes the
whole chain, which takes a long time and a lot of disk. The API answers while it syncs; `/health` reports
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

It needs a Redis or Valkey server, at `redis://localhost:6379` unless
`--redis-url` names another, and stops at start with a message if there is
none ([Redis: the fleet](#redis-the-fleet)).

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

The body is the signed transaction in hex. The store checks it, hands it to
its peers and answers 202 with the transaction id at once. When a node
announces it, usually within a second, the wallet's history shows it.

A transaction that cannot be valid, or that a node refuses with a reason, is
answered 400 with the reason. Posting the same transaction again is safe: it
is handed over again, which gets a payment that the network forgot back to
it.
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

Wallets are answered from the fleet's cache in Redis, which follows every
block and the shared mempool.

- **Every xpub is cached** on its first query. The query waits while one
  store of the fleet reads the wallet, ten seconds at most (`--xpub-wait`).
  If the wallet is not expected that soon, or the wait runs out, the store
  answers 429 with `Retry-After`, the same on every store: ask again then.
- **The cache keeps the costly ones longest**, up to `--redis-max-xpubs`.
  The cache cannot be turned off; that option sets only its size, at least
  1. `nocache` is accepted and ignored: an answer from the cache is always
  exact.

`docs/xpub-indexing.md` has the details.

## Configuration

Every option has an environment variable that sets its default, which is handy
in containers. Booleans in variables accept `true`/`false`, `yes`/`no`,
`on`/`off` and `1`/`0`. `haskoin-store --help` shows the current values.

### Network and peers

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `-n`, `--net` | `NET` | `bch` | Network: `btc`, `bch`, the testnets `btctest4`, `bchtest4`, `bchtest3` and `btctest` (Bitcoin testnet3, deprecated), or regtest `btcreg`, `bchreg` |
| `-d`, `--dir` | `DIR` | `~/.haskoin-store` | Data directory |
| `--discover` | `DISCOVER` | off | Discover peers through DNS seeds and relayed addresses; on regtest it finds none, so name a `--peer` |
| `-p`, `--peer` | `PEER` | none | Connect to `HOST[:PORT]`, or to the Unix socket `unix:PATH`; repeat for more, space-separated in `PEER` |
| `--peer-permission` | `PEER_PERMISSIONS` | none | A permission that the nodes of `--peer` grant the store with `-whitelist` or `-whitebind`, by its name there: `mempool`, `noban`, `relay`, `forcerelay`, `download`, `bloomfilter`, `addr`; repeat for more, comma- or space-separated in `PEER_PERMISSIONS`. With `mempool`, the store asks them for their mempools when it starts and when the pool fills again |
| `--max-peers` | `MAX_PEERS` | `20` | Most peers to connect to |
| `--peer-timeout` | `PEER_TIMEOUT` | `30` | Seconds before a peer that is silent, or that owes blocks, is dropped |
| `--block-stall` | `BLOCK_STALL` | `10` | Longest wait, in seconds, for a block from the syncing peer before another peer is asked too; a quick peer gets four of its recent gaps between blocks, at least 2 s |
| `--block-ping` | `BLOCK_PING` | `5` | Seconds that a peer has to answer a ping to be asked for blocks, unless none answers in time |
| `--max-peer-life` | `MAX_PEER_LIFE` | `172800` | Seconds after which a peer is dropped, so connections rotate |
| `--block-window` | `BLOCK_WINDOW` | `64` | Blocks to request from the syncing peer at a time |

### Mempool

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--mempool-timeout` | `MEMPOOL_TIMEOUT` | `14` | Days after which unconfirmed transactions are deleted (0 = never) |

### Web server and limits

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--host` / `--port` | `HOST` / `PORT` | `*` / `3000` | Address the API listens on |
| `--socket` | `SOCKET` | none | Listen on a Unix socket at this path instead of `--host` and `--port` ([Unix sockets](#unix-sockets)) |
| `--socket-mode` | `SOCKET_MODE` | `0660` | Permissions of that socket, in octal |
| `--noweb` | `NO_WEB` | off | Index only, no API |
| `--tls-cert` / `--tls-key` | `TLS_CERT` / `TLS_KEY` | none | Serve HTTPS with this certificate chain and private key, both files in PEM |
| `--no-blockchain-info` | `NO_BLOCKCHAIN_INFO` | off | Disable the `/blockchain/` endpoints and price lookups |
| `--max-item-count` | `MAX_ITEM_COUNT` | `200000` | Most items in a list (0 = no limit) |
| `--max-full-item-count` | `MAX_FULL_ITEM_COUNT` | `5000` | Most full transactions in a list (0 = no limit) |
| `--max-offset` | `MAX_OFFSET` | `50000` | Largest `offset` (0 = no limit) |
| `--slow-request` | `SLOW_REQUEST` | `5` | Seconds a request may take before the log warns of it (0 = never) |
| `--max-requests` | `MAX_REQUESTS` | 16 a core | Most long requests (lists, whole blocks, xpubs) answered at once, the ceiling of a limit that adapts; see [Overload](#overload) (0 = no limit) |
| `--min-requests` | `MIN_REQUESTS` | 2 a core | Fewest long requests answered at once, where the limit starts |
| `--target-delay` | `TARGET_DELAY` | `25` | Milliseconds that a thread ready to run may wait for a core before the limit comes down |
| `--request-queue` | `REQUEST_QUEUE` | `64` | Long requests that may wait for a slot; more are refused with 429 |
| `--request-wait` | `REQUEST_WAIT` | `1` | Seconds that a long request may wait for a slot; then it is refused with 429 |
| `--node-rpc` | `NODE_RPC` | none | JSON-RPC URL of a full node, such as `http://user:password@127.0.0.1:8332/`, to publish transactions through, and to read the node's whole mempool from, as a source of the shared mempool; without it, or when it does not answer, transactions go to the peers, and the store reads the mempools of the peers that allow it |
| `--node-rpc-socket` | `NODE_RPC_SOCKET` | none | Unix socket to send the requests of `--node-rpc` through, in place of the host and port of its URL; the URL still gives the user and password. Neither node listens on one itself: it is something that joins it to the node, such as socat |
| `--def-item-count` | `DEF_ITEM_COUNT` | `100` | Items in a list when no `limit` is given |
| `--xpub-gap` | `XPUB_GAP` | `32` | Unused addresses kept after the last used one of each chain of an xpub; payments further away are not seen |
| `--xpub-gap-init` | `XPUB_GAP_INIT` | `20` | Addresses looked at for an xpub with no transactions; an xpub with none used among them counts as unused |
| `--ticker-url` | `TICKER_URL` | Blockchain.info ticker | Source of current prices |
| `--price-history-url` | `PRICE_HISTORY_URL` | Blockchain.info price index | Source of historical prices |
| `--ticker-refresh` | `TICKER_REFRESH` | `90000000` | Microseconds between price updates |

### Cache of xpubs

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--xpub-workers` | `XPUB_WORKERS` | `0` | Threads that read xpubs into the cache (0 = one per core) |
| `--xpub-wait` | `XPUB_WAIT` | `10` | Most seconds that a request waits for its xpub to be read; one that would wait longer gets 429 |
| `--xpub-horizon` | `XPUB_HORIZON` | `30` | Seconds: an xpub that the store expects to read later than this is not queued at all |
| `--xpub-queue` | `XPUB_QUEUE` | `1000` | Most xpubs that requests wait for in the queue; more are refused with 429 |

### Redis: the fleet

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--redis-url` | `REDIS_URL` | `redis://localhost:6379` | Redis of the fleet, which every store needs: its tip, mempool and xpub cache; `unix:PATH` for a Redis on a Unix socket |
| `--redis-max-xpubs` | `REDIS_MAX_XPUBS` | `10000` | Size of the xpub cache, at least 1; evicted by cost to index again and by use |

Redis is how stores agree, and every store is a member of a fleet, even a
fleet of one. Every key starts with `hs2:<network>:`.

- **The block** is the newest that every live store has. A request is
  answered from it, from the store's view of its database at that block.
- **The mempool** is one pool in Redis, and only there: a store keeps no
  mempool of its own. Each store offers the transactions that its peers
  send it (`docs/fleet-mempool.md`).
- **A store that falls behind**, or loses Redis, answers 503 and reports
  unhealthy, and stops counting after ten seconds without a report, so
  that it holds nobody up.
- **When Redis loses its data**, the stores notice it by an epoch key that
  is gone or changed. Each forgets the fleet tip, reports unhealthy until
  the fleet has a tip again, and fills the pool again. Xpub entries are
  built again as requests ask for them.
- **When the pool starts again**, after a reorganisation deeper than 50
  blocks, the stores fill it again too.
- **Filling the pool again** is reading the whole mempool of a
  **source**, which a store does when it starts, whenever it may have
  missed a transaction, when the pool lost what it had, and every ten
  minutes. With `--node-rpc` the RPC of the node is a source, whatever the
  node allows its peers. A peer is one when its node takes the request,
  which a node closes the connection over unless it offers Bloom filters
  or whitelists the store with `mempool`. The store knows which from a
  model of each peer's node (`docs/fleet-mempool.md`): tell it what your
  nodes grant with `--peer-permission`. A store with no source says so in
  `/health`.
- **`/events` is the same stream on every store**, written to Redis with
  each change of the pool and each move of the tip.
- **Xpubs** are cached for the whole fleet (`docs/xpub-indexing.md`).

### Health check

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--max-lagging-blocks` | `MAX_LAGGING_BLOCKS` | `3` | Most blocks the index may lag behind the headers |
| `--max-pending-txs` | `MAX_PENDING_TXS` | `10000` | Most transactions requested from peers and not received yet |
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
| `--max-open-files` | `MAX_OPEN_FILES` | `0` | Table files kept open (0 = all of them) |
| `--write-buffer-mb` | `WRITE_BUFFER_MB` | `256` | Memtables across all column families (0 = unlimited) |
| `--max-wal-mb` | `MAX_WAL_MB` | `1024` | Write-ahead log size that forces flushes (0 = RocksDB default) |
| `--background-jobs` | `BACKGROUND_JOBS` | one a core, 2 to 16 | Threads for flushes and compactions together (0 = RocksDB default of two) |
| `--block-cache-mb` | `BLOCK_CACHE_MB` | `1024` | Shared block cache for data blocks; indexes and filters stay in memory outside it (0 = RocksDB default) |

Memory used by RocksDB is roughly the block cache plus the memtables, on top of
the Haskell heap. `/dbstats` shows where it goes.

Every table file stays open by default, and keeps its index and its
filter in memory while it is open.

- **Why all files open:** a lookup in a file that is not open opens it
  first, which reads its footer and properties again. A store of Bitcoin
  Cash with 8,357 files and the old limit of 2,000 opened some 70,000 files
  to send one block of 166,739 transactions. An open file costs a file
  descriptor and a few kilobytes, so raise the limit on open files of the
  process (`LimitNOFILE` in systemd) above the number of table files:
  about one for each 64 MiB of database.
- **Indexes and filters stay in memory**, with their open files and
  outside the block cache, so that data blocks cannot push them out: when
  they were in the cache and a busy store evicted them, nearly every
  lookup read megabytes back, 0.8 ms of processor time each and 18 seconds
  to import a block of Bitcoin. They come to about 0.9% of the database:
  16 GB for Bitcoin (a database of 1.7 TB) and 4 GB for Bitcoin Cash
  (461 GB) on api.haskoin.com, on Thu 19 Nov 2026 (`/dbstats`, table
  readers). Keep all files open
  (above) so that they are read once, when the store starts, which takes
  longer on a large database. The block cache (`--block-cache-mb`) holds
  data blocks only.

RocksDB flushes and compacts in the background. The store gives it one
thread a core, at least 2 and at most 16, a quarter of them for flushes
and the rest for compactions. `--background-jobs` sets another number, and
0 leaves RocksDB's own default of two: one for each.

- **Why not RocksDB's two:** a fresh sync writes faster than one
  compaction thread keeps up with. RocksDB then delays writes once 64 GiB
  of compaction is pending in a column family, and stops them at 256 GiB.
  A sync of Bitcoin Cash on 24 cores froze at height 436,814 that way, with
  209 GB pending, and moved again at once with 12 jobs. The store now logs
  when RocksDB delays or stops writes, and `/health` reports it (see
  [Health](#health)).
- **Why not more than 16:** past that the disk is the limit, and more
  threads only make reads wait longer for it.
- **Subcompactions** would let one large compaction use several threads,
  but `rocksdb-haskell-jprupp` 2.8.0 cannot set `max_subcompactions`, so
  each compaction has one thread.

- **When it helps:** a backlog of compactions on a machine whose disk and
  cores have room: the first sync, or the hours after a migration (see
  [Upgrading an existing database](#upgrading-an-existing-database)). One
  thread compacts about 90 MiB a second if the disk keeps up.
- **When it does not:** on a disk that is already the limit. Measured on a
  store of Bitcoin on a SATA SSD, the one compaction thread was on a core a
  third of the time and waiting for the disk the rest, with the machine at
  75% I/O wait. A second thread there would have made every request wait
  longer for the same disk.
- **What it costs:** cores and disk that the requests would otherwise have.
  On a slow disk that already limits a store serving traffic, set fewer.

Bloom filters are on by default. They let RocksDB skip the table files that
cannot have a key, which is most of what a lookup of a transaction, an
output or a balance would otherwise read.

- **What has them:** the data that is read by key: transactions, spenders,
  unspent outputs, blocks and balances. The two indices by
  address, which are scanned, do not.
- **What they cost:** about 1.25 bytes for each key, held in memory with
  the indexes, outside the block cache (above). On a chain with billions
  of keys that is gigabytes: plan memory for it, or turn them off with
  `--no-bloom`.
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
- **A 503 or a 429 says when to try again**, in `Retry-After`.
- **HTTPS is optional and off by default.** With `--tls-cert` and
  `--tls-key` the server speaks HTTPS on its port, and nothing else. A
  public service usually has a proxy in front that holds the certificate;
  this is for a server by itself. The files are read at start, so a
  renewed certificate takes a restart.

### Unix sockets

A store with its proxy on the same machine needs no TCP port. On a Unix
socket, who may connect is set by the file's owner and mode, and nothing
on the network can reach the store by mistake. There are two ways to get
one:

- **`--socket PATH`**: the store makes the socket, with the mode of
  `--socket-mode` (`0660` by default: owner and group), and removes it when
  it stops. A socket left by a store that did not stop cleanly is removed
  at start if nothing answers on it; a store already listening there, or a
  file that is not a socket, stops the new one instead. The path may be at
  most 107 bytes long.
- **Socket activation by systemd**: a `.socket` unit makes the socket and
  owns its path and permissions, and the store takes it from systemd
  (`LISTEN_FDS`). Clients that connect while the store restarts wait
  instead of failing. A socket from systemd wins over `--socket`, which
  wins over `--host` and `--port`, so adding the unit needs no change to
  the store's options. For a user service:

  ```ini
  # ~/.config/systemd/user/haskoin-store-bch.socket
  [Socket]
  ListenStream=%t/haskoin-store/bch.sock
  SocketMode=0660

  [Install]
  WantedBy=sockets.target
  ```

  A `.socket` unit starts the `.service` of the same name.

Test it with `curl --unix-socket /run/user/1000/haskoin-store/bch.sock
http://localhost/health`. nginx reaches it with
`server unix:/run/user/1000/haskoin-store/bch.sock;` in an `upstream`. A
client on a Unix socket has no address, so the access log shows `unix` for
it unless the proxy sends `X-Forwarded-For`. Nothing else in the store
looks at the client's address, and WebSocket events work the same.

### Endpoints at a glance

| Area | Endpoints |
| --- | --- |
| Blocks | `/block/best`, `/block/latest`, `/block/:hash`, `/blocks`, `/block/height/:h`, `/block/heights`, `/block/time/:t`, `/block/mtp/:t`, each with a `/raw` variant where it makes sense |
| Transactions | `/transaction/:txid`, `/transactions`, `/transactions/block/:hash`, `/transaction/:txid/after/:height`, `POST /transactions`, `/raw` variants |
| Mempool | `/mempool` |
| Addresses | `/address/:addr/{transactions,transactions/full,balance,unspent}` and the multi-address forms `/address/{transactions,transactions/full,balances,unspent}?addresses=…` |
| Xpubs | `/xpub/:xpub`, `/xpub/:xpub/{transactions,transactions/full,balances,unspent}`, `DELETE /xpub/:xpub` (drop from cache); `derive=legacy\|compat\|segwit` picks the address type (`normal`, the old name of `legacy`, still works) |
| Tokens | `/address/:addr/tokens`, `/token/:category/{holders,unspent,transactions}`; outputs, inputs and UTXO have a `token` field on Bitcoin Cash (`docs/cashtokens.md`) |
| Network | `/peers`, `/health` (503 when unhealthy, Redis included), `/dbstats` |
| Events | `/events`, as JSON lines over HTTP or as a WebSocket; `after=` resumes, and a WebSocket client can filter by address, xpub or Bloom filter |
| Blockchain.info | `/blockchain/multiaddr`, `/blockchain/balance`, `/blockchain/rawaddr/:addr`, `/blockchain/unspent`, `/blockchain/rawtx/:txid`, `/blockchain/rawblock/:hash`, `/blockchain/latestblock`, `/blockchain/unconfirmed-transactions`, `/blockchain/export-history`, `/blockchain/q/…` |

Turn off the Blockchain.info endpoints with `--no-blockchain-info`. The xpub
endpoints are always on: they answer from the fleet's cache in Redis.

### Tokens of Bitcoin Cash

Since May 2023 an output on Bitcoin Cash can hold tokens (CashTokens): a
fungible amount of one category, a non-fungible token, or both. The store
reads them from every output, keeps a token balance for each address and
category, and indexes each category. On Bitcoin there are no tokens: the
token endpoints answer with empty lists, and the JSON has no `token` field.

The token balances of an address, one per category:

```sh
curl http://localhost:3000/address/bitcoincash:qz.../tokens
```

```json
[
  {
    "address": "bitcoincash:qz...",
    "category": "084c...93ae",
    "confirmed": "1000",
    "unconfirmed": "0",
    "nfts": 0,
    "utxo": 1,
    "txs": 2,
    "received": "2000"
  }
]
```

- **Amounts are strings**, since a category can have up to 2^63 − 1 tokens,
  more than a JSON number holds exactly in many languages.
- **They count the mempool**, as the balance of an address does: `confirmed`
  is what blocks hold and the mempool does not spend, and `unconfirmed` what
  the mempool adds. Their sum is what the address holds now.
- **`nfts`** is the number of non-fungible tokens, **`utxo`** the number of
  unspent outputs that hold the category, **`txs`** the number of
  transactions that moved it to or from the address, and **`received`**
  every fungible token that the address ever received, spent ones included.

The category is the id of the transaction whose output 0 the first
transaction of the tokens spent. Its endpoints:

```sh
curl 'http://localhost:3000/token/084c...93ae/holders?limit=10&offset=0'
curl 'http://localhost:3000/token/084c...93ae/unspent?limit=10'
curl 'http://localhost:3000/token/084c...93ae/transactions?limit=10'
```

- **`/holders`**: a token balance for each address that holds the
  category, in the order of the addresses' bytes, a page at a time with
  `limit` and `offset`. Tokens in outputs without an address are in no
  one's balance.
- **`/unspent`**: the unspent outputs that hold it, newest first: those of
  the mempool, then those of blocks that the mempool does not spend. Pages
  as an address's unspent outputs, with `limit`, `offset` and `height`.
- **`/transactions`**: the transactions that created, moved or burned it,
  newest first, those of the mempool first, paged the same way.

Every output, input (the output it spends) and unspent output has a `token`
field on Bitcoin Cash: `null` without tokens, and otherwise the tokens as the
CashTokens test vectors and Bitcoin Cash Node's RPC write them:

```json
"token": {
  "category": "084c...93ae",
  "amount": "1000",
  "nft": {"capability": "minting", "commitment": "01"}
}
```

`nft` is there only for an output that holds a non-fungible token. The
tokens are written at the front of an output's script, so its `pkscript`
has them too. The Haskell client of
[haskoin-store-data](https://github.com/jprupp/haskoin-store-data) has a
request for each endpoint (`GetAddrTokens`, `GetTokenHolders`,
`GetTokenUnspent` and `GetTokenTxs`), and `scriptTokens` reads the tokens of
a script. `swagger.yaml` describes every field, with an example of each
answer.

## How it works

```
peers ──► haskoin-node ──► block store ──► RocksDB ──► web server ──► clients
                │               │             ▲
                └─ headers      └─ events ──► cache ◄──► Redis ◄──► other stores
```

- **haskoin-node** keeps a pool of peers and syncs block headers.
- The **block store** downloads blocks from one peer at a time, and
  transactions from all of them, which it hands to the fleet's pool. Each
  block is one atomic RocksDB batch, so a crash never leaves a half-written block.
- **A block is imported in stages**, so that a full block takes a second
  or two: what it needs is worked out on all cores, everything it will
  read is read from RocksDB ahead of time, many keys at once, and then the
  rules run in memory. See [Importing a block](#importing-a-block).
- **The database holds the chain alone.** A transaction from a peer goes
  to the pool that the fleet shares in Redis, which is the mempool;
  RocksDB has no mempool of its own.
- **A peer is asked only for blocks that it has announced:** it sent the
  header of the block or of a later one, or gave a height at least that of
  the block when it connected. A peer asked for a block that it does not
  have yet answers nothing.
- **A peer is pinged before it is asked.** The peers that have the block
  are pinged at once, and the first to answer is asked. One that takes
  longer than `--block-ping` seconds is asked only if none answers in
  time. This costs each new block one round trip to the quickest peer.
- **A peer that does not deliver is passed over, not waited for.** If the
  peer that was asked sends nothing for four of its recent gaps between
  blocks, at least two seconds and at most `--block-stall` seconds,
  another is found the same way and asked for the same blocks, and
  whichever delivers first is taken. A store that imports hundreds of
  blocks a second so notices a quiet peer in two seconds, while a peer
  that takes seconds over each large block is not passed over for it. The stall doubles each time, up to
  `--peer-timeout`, so that a slow link still gets its blocks; a peer that
  still owes blocks after `--peer-timeout` is disconnected.
- **Reorganisations** are handled by reverting blocks until the best block
  is back on the main chain of headers.
- The **web server** answers from the view of RocksDB at the fleet tip,
  with the fleet's pool in Redis on top, and xpubs from their cache in
  Redis.

### Importing a block

A full block of Bitcoin, 4,000 transactions, took a store of mainnet 13 to
18 seconds to import, with its disk and half its cores idle. The time went
into some 30,000 reads of RocksDB, made one after another from one thread,
many of them of a key that had just been read.

Now a block goes through four stages:

1. **Prepare:** the hash of each transaction, the address of each output
   and the sizes, from the block alone, on all cores.
2. **Read ahead:** every key of the chain that the rules will read, with
   RocksDB multi-gets, eight at a time for each core, into a cache that
   belongs to the batch. Each key is read once.
3. **Apply:** the rules, one transaction after another, as before. They
   find everything in memory.
4. **Write:** one atomic batch, as before, serialised on all cores and
   handed to RocksDB a buffer of 256 KiB at a time.

The balances of the addresses of a block and the spenders of the outputs
that it spends are in none of the stages: since database version 23 they
are not read. The block writes how they change, as RocksDB merge records,
and RocksDB works out the values when they are read. On a store of mainnet
they were more than half of the read ahead.

- **A mined block is not validated.** It passed proof of work and the
  node took it, so the store takes it that every output it spends is
  unspent in the chain or made by the block itself, and that none is
  spent twice. The import reads the outputs that the block spends, which
  it needs to write the block, and nothing to check it: no spender of the
  chain and no output that the block itself makes. Only a coinbase is looked up
  in the chain in case it is confirmed already, as two of 2010 were. A
  block that is not valid is written whole or not at all, and is refused
  when what the import needs to read is not there.
- **What keeps the store's own data right stays:** the order in which
  transactions are applied. Reverts check what they always did.

Measured with `haskoin-store-bench import`, on four cores of an AMD Ryzen
9 3900X, with a database of a million unspent outputs on tmpfs and blocks
of 3,500 transactions, milliseconds a block:

| | 1.15.1 | 1.16.0 |
| --- | ---: | ---: |
| Syncing: the mempool is empty | 1,381 | 382 |
| At the tip: every transaction is in the mempool first | 2,321 | 841 |

Of the 382 ms, 36 prepare, 75 read ahead, 63 apply the rules and 208
write a batch of 65,410 keys and 6.6 MB.

That was with the batch given to RocksDB one key at a time. Packed into
buffers, its write took 86 ms where it took 134, in later runs of the same
benchmark in which the whole block went from 250 to 198 ms, and from 522
to 447 ms at the tip. Without the reads of balances and spenders, in a
third set of runs, the read ahead went from 56 to 21 ms and the block from
189 to 148 ms, and from 433 to 398 ms at the tip. Without the reads that
checked a mined block, in a later set, a block of chains of transactions
went from 161 to 138 ms. These were measured while the database still had
a mempool of its own, which it no longer has. `docs/block-import.md` has
the sets of numbers.

A store of mainnet gains more than the benchmark shows. In the benchmark
a read is 30 microseconds of CPU, as the files are in memory. On the
store of mainnet that this was made for, a read waits for a disk, which
gave five times more reads a second when asked for 32 at a time than one
at a time: its reads are slower than the benchmark's, and gain more from
being made together.

`docs/block-import.md` has the design, what was measured, and what is left.
`docs/block-import.md` says what could bring a block well below a
second, and what each step would do to reads.
The stages are timed for StatsD: see [StatsD](#statsd).

### Publishing

The store does not check scripts, so a transaction that a client posts
never goes into its mempool directly: anyone could show a payment that will
never confirm. It is handed over, and comes in when a node announces it.

- **Checked first**: that the inputs exist and are not spent, that a
  coinbase is old enough, the amounts, the lock time, the version, that
  the input scripts only push data in its shortest form, the tokens of
  Bitcoin Cash, the size and the dust rule of relay, and the signature of
  every coin that is locked to one key. A transaction that fails is
  refused with the reason, and nothing is sent.
- **Handed to every peer, each on a connection of its own**, and answered
  202 as soon as they took it. The store does not wait to see what the
  network makes of it. A node does not announce a transaction to the
  connection that gave it; on those the store syncs from it hears of it as
  of any transaction, and imports it the same way.
- **Over the RPC of a node, when the store has it.** With `--node-rpc`
  the store gives the transaction to a full node over its RPC instead. The
  node checks everything and says why it refuses. If the node does not
  answer, the transaction goes to the peers.
- **To a peer, only what the store verified.** A peer that is sent a
  transaction it finds invalid may close every connection of the store,
  and Bitcoin Cash Node does. So a transaction with an input that the store
  cannot verify, a script that is not a key or a multisig, is refused
  unless there is an RPC to send it over; and so are the transactions that
  were measured to cost the store its connections.
- **Whitelist the store on your nodes** (`-whitelist` in Bitcoin Core and
  Bitcoin Cash Node), and tell the store what they grant
  (`--peer-permission`). A node announces to a whitelisted peer without
  delay, so the transaction shows in the store at once; otherwise Bitcoin
  Core takes five seconds on average. And a node never closes the
  connections of a whitelisted peer.

`docs/publishing.md` has the design, what was measured on Bitcoin Core and
Bitcoin Cash Node, and how it differs from earlier versions.

### The database

RocksDB holds the chain alone. The mempool is the fleet's pool in Redis.

- **The chain's data is written once and is the same on every server** that
  has the same blocks. A confirmed transaction has the time of its block.
  What spends its outputs is under keys of its own, so its record never
  changes.
- **Unconfirmed transactions live in Redis**, in the pool that the fleet
  shares, and are applied on top of the chain when a query is answered.
  A reverted block's transactions leave RocksDB; the tip move hands them
  back to the pool.
- **A transaction that leaves the mempool unconfirmed is forgotten**: it was
  replaced, double-spent by a block, or too old. The API answers 404 for
  it, or 410, with the transaction that won, for one that a block replaced
  in the last 2016 blocks.
- **Each request reads one snapshot** of the database, at the fleet tip, so
  everything in an answer is of the same moment.

`docs/one-fleet.md` has the reasons, and `docs/fleet-mempool.md` the pool
in detail and its tests. `docs/concurrency.md` says, for every protocol
that runs at once, in a store and across a fleet, which pure machine or
Lua script decides it, who owns what, and why no interleaving breaks it.
The layout of the keys is described in `Haskoin.Store.Database.Types`.

### Upgrading an existing database

This is version 24 of the database. The store migrates an older database
the first time it opens it. **There is no way back**: an older store
cannot open the database afterwards, so take a copy first.

**Upgrading to 2.0.0**, which brings the tokens of Bitcoin Cash:

- **Take a copy of the database first.** The migration from version 23 is
  the one below; on Bitcoin Cash it reads every block since May 2023, which
  takes hours, and carries on where it was if it is stopped.
- **Upgrade every store of a fleet together.** The scripts of the shared
  pool in Redis take token entries, which an older store's scripts do not
  know. The transactions that are in the pool at the upgrade have no token
  entries: their tokens count once a block confirms them.
- **Clients** that do not know the `token` field ignore it; on Bitcoin the
  JSON is as it was.

| From | What the migration does | How long |
| --- | --- | --- |
| 18, 19 | Not migrated: sync again | |
| 20 | Takes the unconfirmed transactions out of the chain's indices; the pool gets them again from the peers | Seconds to minutes |
| 21 | Reads every transaction and writes nearly every one again | Many hours for Bitcoin |
| 22 | Writes the new version | At once |
| 23 | Bitcoin Cash: indexes the outputs that hold tokens, under their addresses and in the token indexes, from the upgrade of May 2023 (`docs/cashtokens.md`). Others: writes the new version | Bitcoin Cash: hours, it reads every transaction since then. Others: at once |

**From 23 to 24 on Bitcoin Cash, outputs with tokens get their
addresses.** Until version 24 an output that holds tokens had no address,
so the addresses that held tokens were missing their history and part of
their balance. The migration walks the blocks since CashTokens began and
adds them, with the token balances and the indexes of each category. It
works in batches of 200 blocks, each written at once with its place, so
it can be stopped and carries on where it was. The four new column
families keep an older store out, as `merge-operators` does.

**From 22 to 23 nothing in the database changes, and it is still one
way.** From version 23 a block writes the balances of its addresses and
the spenders of the outputs that it spends as RocksDB merge records, which
only a store with the merge operators can read (see
[Importing a block](#importing-a-block)). An older haskoin-store does not
have them:

- **It fails to open the database**, with a RocksDB error that names a
  column family that it does not know, `merge-operators`. That column
  family is empty, and is there for this: without it, RocksDB would open
  the database for the older store and silently drop the merge records
  that were still in its write-ahead log.
- **The database is unchanged by the attempt**, and this version opens it
  as before.
- **To go back to an older version,** restore the copy, or sync again.

The step from 21 is the long one. Nothing lists the records that need a
change, and nearly all do.

- **Time:** a database of Bitcoin has more than a billion transactions.
  The store does not start until it is done. The log says how far it is
  every million transactions.
- **Disk:** it needs room for a second copy of the transactions, until
  RocksDB has compacted the first away.
- **Interrupted,** it starts again and skips what is done.
- **Afterwards** RocksDB writes the spenders once more, in the background,
  while the store runs. The migration writes them in the order of their
  keys, so their files reach the last level without being compacted, and
  RocksDB compacts each such file by itself once the store starts. On a
  store of Bitcoin that was 196 GiB in about two hours on a SATA SSD, with
  the machine waiting for its disk most of that time: the store answered
  and imported blocks slowly until it was over. It needs no more room, and
  nothing has to be done about it. On a faster disk `--background-jobs`
  shortens it.
- **Deleted transactions** that an old database still holds are left where
  they are and ignored. Finding them would mean reading everything again.

What changed for clients in version 23: nothing in the answers. A balance
or the spenders of a transaction that recent blocks changed take a few
microseconds more to read until RocksDB has compacted them:
`docs/block-import.md` explains why.

What changed for clients in version 22:

- A confirmed transaction that the server first saw in its mempool used to
  keep that time. Now every server gives the time of the block.
- Reading one confirmed transaction in full takes a second key: 18
  microseconds where it took 11.5. Lists by address and by xpub cost the
  same as before.

### Tokens

An output with tokens has the address of its locking script, read by
haskoin-core 3.0 after it takes off the token prefix, so the coins of one
script stay under one address with or without tokens. Importing a block
writes what its transactions do to tokens next to what they do to coins:
the token balance of each address and category, and for each category its
transactions and its unspent outputs, in column families of their own. The
holders of a category are a second key of the same balances, so that
listing them reads no other address. The shared mempool in Redis keeps the
same token entries for the transactions of the pool, which the endpoints
lay over those of the blocks. `docs/cashtokens.md` has the details, and why
each part is built as it is.

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

### The cache of a fleet

The fleet has a cache of xpubs
(`Haskoin.Store.Fleet.XPub`), because its servers do not answer from their
own databases but from one block and one shared mempool.

An entry has two layers:

- what the chain has for the addresses of the xpub, at the block that the
  fleet answers from;
- what the shared pool has for them, which the pool's own script writes as
  it changes the pool.

The first follows from the chain, so any server writes the same. Nothing
has to be claimed or repaired: the server that moves the fleet to a new
block applies what the block did, in the same Redis commit that moves the
tip and the pool. A big move is written in pieces under the entries'
version marks, so a request waits or is answered again and never sees half
of it. A reorganisation is undone address by address, from the store's
views of its database at recent blocks: it never empties the cache.

An entry also holds a gap's worth of addresses after the window, derived
ahead of time, so a payment to the next address of a wallet is in its
entry as soon as the pool takes it. `docs/xpub-indexing.md` has the design.

### Where the code is

| Module | What it does |
| --- | --- |
| `Haskoin.Store.BlockStore` | Downloads and imports blocks, and hands transactions from peers to the fleet's pool |
| `Haskoin.Store.BlockStore.Sync` | The state of the block download, as a pure state machine |
| `Haskoin.Store.BlockStore.Pending` | Transactions that peers announce, on their way to the pool |
| `Haskoin.Store.Logic` | The rules that import and revert blocks |
| `Haskoin.Store.Database.Writer` | A batch of writes, with its read cache and its reads ahead |
| `Haskoin.Store.Database.Table` | The hash tables of a batch, whose keys are hashed quickly |
| `Haskoin.Store.Database.Merge` | The merge operators of balances and spenders, which are C functions (`cbits/`) |
| `Haskoin.Store.Database.Query` | The queries of the chain's column families |
| `Haskoin.Store.Database.Reader` | Opening the database, with its migration, column families and metrics |
| `Haskoin.Store.Database.Query.Pooled` | The queries of the chain with the fleet's pool on top |
| `Haskoin.Store.Database.Views` | Views of the database at recent blocks, one of them the fleet tip |
| `Haskoin.Store.Database.Slices` | Stored blocks and transactions read in part, for what an answer needs |
| `Haskoin.Store.Database.Migrate` | The migrations from versions 20, 21 and 22 |
| `Haskoin.Store.Database.Stalls` | The default number of RocksDB background jobs, and the watch on write stalls |
| `Haskoin.Store.XPub` | The vocabulary of the xpub cache: its reads and the budget of a job |
| `Haskoin.Store.XPub.Indexer` | The threads that read xpubs into the cache, and the queue of their jobs |
| `Haskoin.Store.XPub.Lock` | The lock in Redis that keeps two stores from reading the same xpub |
| `Haskoin.Store.XPub.Service` | The door to the fleet's cache of xpubs: a hit answers, a miss asks the indexer |
| `Haskoin.Store.Fleet.Member`, `Haskoin.Store.Fleet.XPub` | A fleet, and its cache |
| `Haskoin.Store.Fleet`, `Haskoin.Store.Fleet.Pool` | The rules of the fleet tip and of the pool, as pure decisions |
| `Haskoin.Store.Fleet.Redis` | Those rules carried out in Redis, one Lua script per step |
| `Haskoin.Store.Web`, `Haskoin.Store.Web.Events` | The web server, and `/events` |
| `Haskoin.Store.Web.Router` | Routing of requests to handlers, directly over WAI |
| `Haskoin.Store.Web.Docs` | The API reference that the server serves at its root |
| `Haskoin.Store.Web.Pieces` | Answers that are sent while they are read |
| `Haskoin.Store.Web.Listen` | A Unix socket to listen on, made by the store or passed by systemd |
| `Haskoin.Store.Web.Admission` | The limit on long requests answered at once, and the line in front of it (CoDel) |
| `Haskoin.Store.Web.Congestion` | The delay of GHC's scheduler, and the controller that moves the limit with it |
| `Haskoin.Store.Runtime` | GHC's garbage collector, as StatsD metrics |
| `Haskoin.Store.Exception` | Handlers that let a thread be stopped, and cleanups that cannot be cut short |
| `Haskoin.Store.Manager` | Wires a store together: Redis, RocksDB, the node, the block store and the fleet |
| `Haskoin.Store.Startup` | Errors that stop the store from starting, said so an operator can act |
| `Haskoin.Store.Outage` | Logs once that Redis or another dependency stopped working, and once that it is back |
| `Haskoin.Store.Web.Publish` | Checking a transaction and handing it to the peers, or to a full node over RPC |

The tests hold the cache to the queries of `Query.Pooled` at the fleet tip
with the pool: every cached entry equals what they give, after random
histories of payments, blocks, reorganisations and builds.

## Monitoring

`tools/hsq/hsq` is a small client for people: `hsq` prints a summary of a
store's health, and `hsq /block/best` any answer of the API, laid out and
coloured, over a Unix socket, HTTP or HTTPS. See `docs/hsq.md`.

### Health

A store is unhealthy when Redis does not answer within two seconds, when
it has no view of the fleet tip, or when its lease in the fleet has
lapsed: it then knows nothing of the mempool, answers no xpub query, and
answers 503. A load balancer should stop sending it requests until
`/health` passes again. A store that syncs from scratch is unhealthy until
it reaches the fleet tip.

`/health` returns the result of the latest check as JSON. Point your load
balancer at it.

| Status | Meaning |
| --- | --- |
| 200 | Every check passes |
| 503 | A check fails, Redis included; the body says which |

The check runs every second, and at once when a block arrives, so what
`/health` answers is at most a second old. Redis is asked every five
seconds.

The checks are: how far the index is behind the headers, the age of the
last block and of the last transaction, the transactions waiting to be
imported, the connected peers, the database, and Redis (`"redis"` in the
JSON).

**Each check says in words what it found**, in its `"text"`, and the
`"text"` at the top says what that means for the whole store, such as:

> The store is not healthy. The database is 4 blocks behind the best
> header, at height 688,798, more than the 2 allowed: the store is
> syncing, or cannot import blocks fast enough.

They are for people looking at a store; a program should read `"ok"` and
the numbers, as the wording may change.

**The mempool of the nodes** is in `"mempool"`: `"sources"` is how many
things the store can read a whole mempool from again, `"dirty"` whether it
may have missed transactions and has not read a source since, and `"known"`
and `"held"` how many transaction ids are on their way to the shared
mempool and how many are held. It is no check and has no `"ok"`: a store
with no source is healthy. But a node announces a transaction to a peer
once, so such a store shows fewer unconfirmed transactions than its node
after it missed some, until a block has them, and the `"text"` at the top
says so. A source is the RPC of a node (`--node-rpc`), or a peer whose node
gives its mempool when asked: for Bitcoin Core, one that whitelists the
store with the permission `mempool`, named with `--peer` and declared with
`--peer-permission mempool` (`docs/mempool-ingestion.md`).

**The database** is in `"database"`, as RocksDB reports it every five
seconds: `"writes"` is `flowing`, `delayed` or `stopped`, with
`"pending-compaction-bytes"` and `"delayed-write-rate"` (bytes a second, 0
when not delayed), and the work that explains it: `"running-compactions"`,
`"running-flushes"`, `"immutable-memtables"` (full tables in memory waiting
to be flushed), `"size-bytes"` and `"live-data-bytes"` of the files, and
`"background-errors"` since the database opened. RocksDB delays writes and
then stops them when its compactions fall behind ([RocksDB](#rocksdb)).
Only stopped writes fail the check, as the store then imports nothing. The
store logs a warning when writes are delayed or stopped and a line when
they recover.

- **Stopped writes make the store unhealthy:** it imports nothing until
  they flow again.
- **Delayed writes do not.** RocksDB slows writes for seconds at a time
  during a sync, and the store answers requests at full speed meanwhile.
  If the delay makes it fall behind, the check of the blocks says so.

**A server that is not synchronized with the network**, because it does
not have the block to answer from yet or cannot reach its Redis, answers
any request with 503 and `{"error": "behind-fleet"}`, with the message
"This server is not synchronized with the network". Have the load balancer
try another server, and alert if one keeps saying it: its `/health` and
its log will say why.

### Overload

A store asked for more than its cores can do still answers the simple
requests at once. Only so many long requests run at once, and the limit
follows what the cores keep up with.
[docs/congestion-control.md](docs/congestion-control.md) explains the
design.

- **What is limited:** the long routes, those that answer lists, whole
  blocks and xpubs. Each holds a slot until its last byte is sent.
  Requests for one thing by its key, such as a transaction or a balance,
  and those that read nothing from the database, such as `/health`,
  `/peers` and `/events`, are not limited.
- **The limit adapts.** A probe on each core notes how late a woken
  thread runs, less the pauses of GHC's collector. Every half second, if
  the slowest tenth of those delays was over `--target-delay` (25 ms)
  and the cores were used at 90%, the limit comes down by a quarter, at most once in the time a request
  holds its slot. If it was under, every slot was in use and the cores
  were used under 90%, the limit grows as TCP CUBIC's window does: fast
  back toward the level of the last cut, then past it. It stays between
  `--min-requests` (two a core) and `--max-requests` (sixteen a core).
  Requests that wait for the disk give their cores back, so the limit
  grows for them; busy cores stop it growing.
- **A request for an xpub that is not cached** gives its slot back while
  it waits for the indexer, which bounds that work itself.
- **The line** in front of the slots holds up to `--request-queue` (64)
  requests for up to `--request-wait` (1 second). It is managed by CoDel:
  when even the shortest wait in 100 ms was over 5 ms, the line is
  overloaded, and a request that finds no slot free is refused at once.
  While the cores have room and the limit is under its ceiling, the line
  waits for the limit to grow instead.
- **A refused request** gets 429, `{"error": "server-busy"}` and
  `Retry-After: 1`, as nginx's `limit_conn_status 429` does: clients and
  proxies take it to mean "come back later", where some retry a 503 at
  once. Have the load balancer try another server on a 429 (with nginx,
  `proxy_next_upstream error timeout http_429`).
- **Xpubs:** reading an xpub into the cache is the work of the indexer,
  in threads of its own, not of a request. A request that would wait for
  the indexer too long gets 429, with when to come back
  ([Extended public keys](#extended-public-keys)).
- **Behind nginx:**
  - **Keep connections to the store open:** `keepalive` in the
    `upstream`, with `keepalive_timeout` under Warp's idle timeout of 30
    seconds, and `proxy_http_version 1.1` with
    `proxy_set_header Connection ""` in the `location`. A new connection
    for each request makes the slowest requests much slower under load.
  - **Keep clients apart in the proxy.** The store does not tell clients
    apart. `limit_conn` per client address and `limit_req` do.
- **Off:** `--max-requests 0` answers every request as it comes.
- **Watch** `web.cap.limit`, `web.cap.delay_us` and the refusals in
  StatsD ([StatsD](#statsd)). A limit stuck at the ceiling with long waits
  points at the disk; one stuck at the floor with a high delay points at
  work that is not limited.

### StatsD

With `--statsd`, the store reports:

| Prefix | What |
| --- | --- |
| `web.*` | Timing of every kind of request |
| `web.reruns` | Requests answered again because a cached xpub that they read changed meanwhile |
| `db.*` | Database reads |
| `store.*` | Block and header heights and other gauges |
| `fleet.cache.*` | What a server of a fleet does with the fleet's cache of xpubs |
| `rts.*` | GHC's garbage collector |

The metrics of the block store:

| Metric | Kind | Meaning |
| --- | --- | --- |
| `store.blocks` | gauge | Height of the best block |
| `store.headers` | gauge | Height of the best header |
| `store.peers` | gauge | Peers online |
| `store.block` | timing | Milliseconds to import a block, with publishing its events |
| `store.block_prepare` | timing | Of those, working out what the import needs of the block |
| `store.block_prefetch` | timing | Reading ahead what the rules will read |
| `store.block_read_unspents` | timing | Of the read ahead: the unspent outputs that the block spends |
| `store.block_read_spenders` | timing | Zero: a block no longer reads spenders, as a mined block is not validated |
| `store.block_read_balances` | timing | Zero since database version 23: a block no longer reads balances |
| `store.block_apply` | timing | The rules, in memory |
| `store.block_write` | timing | Writing the batch to RocksDB |
| `store.last_block_ms`, `store.last_block_prepare_ms`, `store.last_block_prefetch_ms`, `store.last_block_apply_ms`, `store.last_block_write_ms` | gauge | The same times of the latest block, kept until the next one, so that a chart averaging seconds shows them right at any zoom |
| `db.pending_compaction_bytes` | gauge | Bytes RocksDB has to compact, over every column family, every five seconds |
| `db.write_delayed` | gauge | Writes to RocksDB: 0 flowing, 1 delayed, 2 stopped |

- **The four stages add up to nearly all of `store.block`.** A slow
  `store.block_prefetch` is the disk or RocksDB's cache, and the four
  `store.block_read_*` timings say which kind of key it waited for. The
  other three stages are mostly CPU: the write too, which serialises the
  batch and has RocksDB add it to its tables in memory.
- **Every sample of these timings is sent**: there is one a block. Until
  version 1.15.1 `store.block` was made with a sampling rate of zero and
  never reached the StatsD server.

The metrics of the fleet's cache:

| Metric | Kind | Meaning |
| --- | --- | --- |
| `ingest.settled` | counter | Transactions that peers named and that the pool decided on, or already had |
| `ingest.given_up.REASON` | counter | Transactions given up on their way to the pool, by reason: `node_has_not`, `unanswered`, `no_room`, `peer_gone`, `waited_too_long`, `unjudged` (`docs/mempool-ingestion.md`). All but the first make the server read its sources again |
| `ingest.dropped` | counter | Messages of peers dropped because the path into the pool was behind; the sources are read again |
| `ingest.ids` | gauge | Transaction ids on their way to the pool: wanted, asked for, or held |
| `ingest.pending` | gauge | Transactions asked of peers and not received, what `/health` counts |
| `ingest.held` | gauge | Transactions held: being offered, or waiting for one that they spend from |
| `fleet.cache.hits` | counter | Requests about an xpub whose balances the cache gave |
| `fleet.cache.misses` | counter | Requests about an xpub that the cache did not have |
| `fleet.cache.builds` | counter | Entries that this server built, or added addresses to |
| `fleet.cache.build` | timing | Milliseconds to build an entry |
| `fleet.cache.abandoned` | counter | Builds given up because the fleet or the entry moved on |
| `fleet.cache.evictions` | counter | Entries evicted, those of the lowest priority first (GreedyDual) |
| `fleet.cache.emptied` | counter | Times that this server emptied the cache |
| `fleet.cache.xpubs` | gauge | Xpubs in the cache, whichever server cached them |
| `fleet.cache.addresses` | gauge | Addresses that those xpubs have between them |

- **Misses include xpubs being built**, and those built for other
  addresses than they have now. So the share of hits says how much of the
  load the cache takes, not how well it holds what it has.
- **`fleet.cache.emptied`** moves only when the pool starts again: the way
  to the new tip is longer than the stores go through, or a store that is
  alone does not know it. A reorganisation never empties the cache. The
  log says why each time.
- With `--debug` the log also has a line for each entry built and each
  block that the cache was brought to.

The metrics of the garbage collector, sent once a second:

| Metric | Kind | Meaning |
| --- | --- | --- |
| `rts.gcs` | counter | Collections |
| `rts.major_gcs` | counter | Collections of the whole heap |
| `rts.gc_pause_us` | counter | Microseconds that every thread was stopped for the collector |
| `rts.gc_pause_mean_us` | gauge | Mean pause of a collection in the last second |
| `rts.gc_pause_percent` | gauge | Share of the last second that every thread was stopped |
| `rts.gc_cpu_us` | counter | Microseconds of CPU that the collector used, on all cores |
| `rts.nonmoving_sync_us` | counter | Microseconds stopped to end a mark of the non-moving collector |
| `rts.nonmoving_mark_us` | counter | Microseconds that the non-moving collector marked, while the store ran |
| `rts.allocated_bytes` | counter | Bytes allocated |
| `rts.live_bytes` | gauge | Bytes live after the last collection |
| `rts.heap_bytes` | gauge | Bytes that the heap holds from the system |

- **Every collection stops every thread**, the small ones too. While it is
  stopped the store answers nothing, not even `/health`, so a high
  `rts.gc_pause_percent` shows up as slow answers everywhere.
- **The executable runs with `+RTS -T`**, which GHC needs to keep these
  statistics. Without it, as in a program that uses the library, the log
  says so and there are no `rts.*` metrics.
- **The `rts.nonmoving_*` metrics** stay at zero unless the store runs
  with GHC's non-moving collector, as with `GHCRTS=--nonmoving-gc` in its
  environment. The executable uses GHC's default collector (see
  [Memory](#memory)).

The metrics of admission control ([Overload](#overload)):

| Metric | Kind | Meaning |
| --- | --- | --- |
| `web.cap.limit` | gauge | The limit on long requests, every half second |
| `web.cap.delay_us` | gauge | 90th percentile of the scheduler's delay in the last half second, without the collector's pauses, in microseconds |
| `web.cap.lateness_us` | gauge | The same with the collector's pauses |
| `web.cap.used` | gauge | Use of the cores that the controller sees, smoothed over two seconds, in hundredths of the capabilities |
| `web.cap.round_trip_us` | gauge | Time that a long request holds its slot, smoothed, in microseconds: the controller cuts at most once in it |
| `web.cap.overloaded` | gauge | 1 while CoDel finds the line overloaded |
| `web.cap.sojourn` | timing | Milliseconds that an admitted long request waited for its slot |
| `web.cap.refused_full` | counter | Requests refused because the line was full |
| `web.cap.refused_waited` | counter | Requests refused after `--request-wait` |
| `web.cap.refused_codel` | counter | Requests refused by CoDel |
| `web.requests_refused` | counter | Requests refused with `server-busy`, for any reason |
| `web.requests_running` | gauge | Long requests being answered |
| `web.requests_waiting` | gauge | Long requests waiting for a slot |

And for each long route, named by the fixed segments of its path, such as
`address_transactions_full` or `blockchain_multiaddr`:

| Metric | Kind | Meaning |
| --- | --- | --- |
| `web.cap.<route>.running` | gauge | Slots that it holds |
| `web.cap.<route>.waited` | timing | Milliseconds that a request waited for its slot |
| `web.cap.<route>.held` | timing | Milliseconds that a request held its slot, to the last byte |
| `web.cap.<route>.refused` | counter | Requests refused |

### Database statistics

`/dbstats` returns the memory that RocksDB uses for each column family, and
RocksDB's own statistics, as text.

### Memory

The resident memory of the store is its GHC heap plus RocksDB.

- **RocksDB:** the block cache and memtables fill up to `--block-cache-mb`
  and `--write-buffer-mb`, then stay there. The indexes and filters of the
  open table files come on top, about 0.9% of the database (see
  [RocksDB](#rocksdb)).
- **The heap** gives memory back to the system as soon as it no longer
  needs it. The executable is built with
  `--disable-delayed-os-memory-return` for that. Without the flag, Linux
  keeps counting returned memory as resident until it needs it, and the
  heap looks as if it never shrinks after a peak.
- **The heap collects less often**, with 64 MiB of new objects for each
  core between collections (`-A64m`), where GHC's default is 4. Every
  collection stops every thread. Measured under load on a store of mainnet
  and one of testnet, collections fell from 50 to 110 a second to 2 to 4,
  the time that the stores spent stopped fell by half (11% to 6%, and 24%
  to 14%), and the testnet store answered 19% more requests.
- **The price** is about 60 MiB more memory for each core, and pauses of
  about 30 ms where they were 2 to 4 ms. To change it, set for example
  `GHCRTS=-A16m` in the environment of the store.
- **The collector is GHC's default**, which the executable does not
  choose: in GHC 9.12 it copies. Version 1.15.1 chose the non-moving
  collector; in the same measurements it gained nothing, and with `-A64m`
  the copying one did a little better. `GHCRTS=--nonmoving-gc` selects it.
- If you build the store some other way, pass
  `+RTS -A64m --disable-delayed-os-memory-return -RTS` yourself.

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
  seconds or more to answer, with the request and how long it took, until
  the last byte was sent. A client or proxy that reads slowly can make a
  request slow too. A response cut short ends in `, cut short`. The stream
  of events is left out.
- **Every request**, with `--access-log`, which is off by default:
  `203.0.113.7 GET /block/best HTTP/1.1: 200 OK, 0.4 ms`. A public
  server usually has a proxy in front that keeps this log better. The
  address is the first of `X-Forwarded-For` when a proxy sent one, and
  `unix` for a client on a [Unix socket](#unix-sockets) without one.

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
{-# LANGUAGE OverloadedStrings #-}

import Data.Default (def)
import Haskoin (btc, textToAddr)
import Haskoin.Store.WebClient

main :: IO ()
main = do
  let cfg = def {net = btc} -- https://api.haskoin.com/btc/...
      Just addr = textToAddr btc "bc1q…"
  r <- apiCall cfg (GetAddrBalance addr)
  print r
```

`apiCall` answers `Right` the result or `Left` an error. `apiEvents`
listens to `/events`, and `apiEventsAfter` takes up from an event.

The tokens of Bitcoin Cash, by address and by category:

```haskell
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

import Data.Default (def)
import Haskoin (bch, hexToTxHash, textToAddr)
import Haskoin.Store.Data (SerialList (..), TokenBalance (..))
import Haskoin.Store.WebClient

main :: IO ()
main = do
  let cfg = def {net = bch}
      Just addr = textToAddr bch "bitcoincash:qz…"
      Just category = hexToTxHash "084c…93ae"
  -- the fungible tokens of each category that the address holds now
  Right (SerialList balances) <- apiCall cfg (GetAddrTokens addr)
  mapM_ (\b -> print (b.category, b.confirmedTokens + b.unconfirmedTokens)) balances
  -- the first page of the holders of a category
  Right (SerialList holders) <- apiCall cfg (GetTokenHolders category Nothing (OffsetParam 0))
  print (length holders)
```

The client appends the network name to `host`, as in the layout of
api.haskoin.com. To reach your own server, put it behind a proxy that maps
`/btc/` (or the name of your network) to it.

Or embed the indexer in your own program with `haskoin-store`:

- `withStore` starts the node, the block store and the cache from a
  `StoreConfig`.
- The functions of `Haskoin.Store.Database.Query` query the chain in its
  database in `IO`, such as `getBalance store.db addr`, and `chainRead`
  gives them as a `StoreRead`.
- The token queries of a `StoreRead` (`getAddressTokens`,
  `getTokenHolders`, `getTokenTxs` and `getTokenUnspents`) answer for the
  tokens of Bitcoin Cash.
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
haskell-format                 # ormolu, the code style used here (jprupp/tools)
scripts/bench-web.sh           # load-test the API with wrk
stack bench haskoin-store:bench:haskoin-store-bench \
  --ba 'import +RTS -N4 -A64m' # time the import of full blocks
scripts/heap-profile-summary.py FILE.hp  # what grew in a heap profile
check-stack-paths              # what CI checks: no local paths in stack.yaml
scripts/install-system-libraries.sh  # the C libraries, on Ubuntu or Debian
haskell-release --dry-run      # build and check a release, upload nothing
```

`haskell-format`, `check-stack-paths`, `haskell-release` and `haskell-dev`
are scripts of [jprupp/tools](https://github.com/jprupp/tools), shared by the
Haskell packages of this family. Clone that repository once and put its
folder on your `PATH`, for example in `~/.bashrc`; a terminal opened before
the change needs `source ~/.bashrc`.

```sh
git clone https://github.com/jprupp/tools.git ~/Source/tools
echo 'export PATH="$HOME/Source/tools:$PATH"' >> ~/.bashrc
```

The Stack commands build the store as it is released, with optimisation,
which takes minutes. For the time between two commits there is a faster
loop, `haskell-dev`. Run it from the top folder of haskoin-store:

```sh
haskell-dev test                     # build, then run all the tests
haskell-dev test Fleet.Redis         # the specs with Fleet.Redis in their module name
haskell-dev test Fleet.Redis Web     # the specs of several modules
haskell-dev test --match "/Haskoin.Store.Integration/Regtest Bitcoin Core/"
                                     # one path, in one process
haskell-dev test --seed 12345        # run a failure again
haskell-dev test --jobs 4            # fewer processes, to save memory
haskell-dev build                    # build without optimisation
haskell-dev docs                     # build the documentation and check its comments
```

`test` builds first, so a separate `build` is not needed. A run of all the
tests takes about 70 seconds in eight processes, against about four minutes
for `stack test`. `haskell-dev.toml` names the slow spec modules,
`Haskoin.Store.Integration` (`test/Haskoin/Store/IntegrationSpec.hs`), whose
five environments then run at the same time, each with its own node, Valkey
and store in Podman, and `Haskoin.Store.Download`, the downloads of a store
from real nodes, one of them frozen with `SIGSTOP` to hang as a bad peer
does.

The tests name no address or port of the machine. The nodes, Valkey, and
openssl and curl for the test of HTTPS, run in containers of
[podman-test](https://github.com/jprupp/podman-test), reached through Unix
sockets of their pods; the web servers of the store listen on Unix sockets
of their own, which the tests and the client of haskoin-store-data reach;
and what stands for a peer in memory is named by a Unix socket. Nothing in
the tests uses a Bitcoin Core or a Bitcoin Cash Node of the machine.

When a test fails, the output gives the seed: run the
test again alone with `--seed` and `--match` on its path, which prints the
usual output of hspec.

The integration tests, and those of the fleet, need rootless Podman (see
below); without it they are pending, and the others run. Now and then a test
fails with `InsecureConnectionDenied`, an exception of the HTTP client, and
passes when run again with the same seed: its cause is not known yet, so run
such a failure again before taking it for a fault of a change.

Do not mix the two kinds of build in one folder. When no source has changed,
Stack does not see that the flags have, and keeps the objects of the other
build: a plain `stack build` or `stack test` right after `haskell-dev build`
uses code without optimisation. Only what runs from this folder uses it, such
as the tests and `stack exec haskoin-store`: a package that depends on
haskoin-store builds its own copy. After the next change to a source,
everything compiles again. `stack bench` configures the package again and
compiles it with optimisation. Never time the store with `haskell-dev`: the
benchmarks run with `stack bench`.

A release is made with `haskell-release`, once the version in
`package.yaml` is bumped, its CHANGELOG entry written and the commit pushed.
It builds and tests the package, builds the source and documentation
archives, and only if all of that works tags the commit, pushes the tag and
uploads both archives to Hackage. It uploads the documentation itself,
because `stack upload --documentation` fails here: the internal library
`integration` gets a documentation file named `haskoin-store:integration.txt`,
and Hackage refuses a colon in a file name. The script leaves the
documentation of the internal libraries out, as they are only for the tests.

`stack.yaml` must pin released versions, or git commits, of every
dependency. Pointing
`extra-deps` at a sibling checkout such as `../haskoin-core` is fine for
local testing, but CI rejects it.

The tests use [hspec](https://hspec.github.io/). The unit tests need no network
access. The integration tests (`Haskoin.Store.IntegrationSpec`) run regtest
chains of Bitcoin Core and Bitcoin Cash Node, and Valkey, in
[Podman](https://podman.io/) containers, and check the store against them: the
exact gap, spends and change, mempool order, multiaddr counts, reorganisations,
and that the cache answers exactly like the database on every xpub endpoint.

- The containers run through
  [podman-test](https://github.com/jprupp/podman-test), in a pod for each
  environment. The store reaches Valkey, the RPC of the node and the node
  itself through Unix sockets of the pod (`--redis-url unix:PATH`,
  `--node-rpc-socket` and the peer `unix:PATH`), so the pod has no network,
  and environments that run at once never meet.
- They are skipped if Podman cannot run containers, or if
  `PODMAN_TEST_SKIP` is set.
- The first run builds the image `localhost/haskoin-store-test`, which needs
  network access: Ubuntu 26.04 pinned by digest, Valkey from a fixed snapshot of
  the Ubuntu archive, and the release tarballs of both nodes, checked against
  SHA-256 hashes pinned in `integration/Haskoin/Store/Integration/Image.hs`. No
  third-party image is used.
- Pods are removed when the tests end, and those of a run that was killed
  by the next run. Data lives in tmpfs mounts and under `/dev/shm`, and every
  container has a memory limit.
- Set `HASKOIN_TEST_LOG=1` to print the log of the store under test.

The test suite runs with at most four cores and a 4 GiB heap, so that a
runaway test fails instead of exhausting the machine.

`package.yaml` is the source of the package description. Change it rather
than `haskoin-store.cabal`, which
[hpack](https://github.com/sol/hpack) generates.

### Looking for a leak

Run the server with a heap profile by type, which needs no profiling build,
for example `haskoin-store ... +RTS -hT -i10 -po/tmp/heap`, and summarise it
with `scripts/heap-profile-summary.py /tmp/heap.hp`: the live heap over time,
and the types of closure that grew most.

### Measurements for importing blocks

`haskoin-store-bench import [OUTPUTS [TXS [BLOCKS [CACHE_MIB [DIR]]]]]`
(`bench/Import.hs`) times the import of full blocks, in stages with the
reads made ahead, and with every read made when the rules come to it.

1. It builds a regtest database with a million unspent outputs at half a
   million addresses, closes it so that RocksDB writes it to table files,
   and copies it.
2. It imports the same blocks of 3,500 transactions into each copy, five
   of each of two kinds:
   - with inputs that spend outputs spread over the set;
   - with chains of transactions in the block, so that three inputs in
     four spend an output of the same block, as on Bitcoin mainnet today.
3. It prints milliseconds a block for each stage, and the number of keys
   and bytes of the batch of a block.

Give it the cores of the machine you care about with `+RTS -N4`, and the
allocation area of the store with `-A64m`.

`haskoin-store-bench merge [KEYS [READS [CACHE_MIB [DIR]]]]`
(`bench/Merge.hs`) times what the merge records of blocks cost the readers
of the database: the balance of an address, the spender of an output and
the spenders of a transaction, for keys with no merge record, one, three
and six, each in a table file of its own, and then the same keys once
compacted.

### Contributing

Pull requests are welcome.

- Format the code with `haskell-format` (Ormolu, from jprupp/tools), and keep every line of
  code shorter than 120 columns.
- Put scripts in files of their own rather than embedding them: Lua for
  Redis under `src/Haskoin/Store/Fleet/lua/` or `src/Haskoin/Store/XPub/lua/`,
  shell under `scripts/`, called from CI rather than written into the
  workflow.
- Catch any error with `catchAny`, `handleAny` or `tryAny` of
  `Haskoin.Store.Exception`, and clean up with its `bracket`, `finally` and
  `onException`, not with those of `Control.Exception`. A request that
  times out and a loop that is cancelled are stopped by an exception: a
  handler for `SomeException` would take it for an error and go on, and a
  cleanup of `Control.Exception` can be cut short, with a snapshot not
  released or a gauge left too high. The module says more.
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
