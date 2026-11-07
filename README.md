# Haskoin Store

[![CI](https://github.com/jprupp/haskoin-store/actions/workflows/ci.yml/badge.svg)](https://github.com/jprupp/haskoin-store/actions/workflows/ci.yml)
[![Hackage haskoin-store](https://img.shields.io/hackage/v/haskoin-store.svg?label=haskoin-store)](https://hackage.haskell.org/package/haskoin-store)
[![Hackage haskoin-store-data](https://img.shields.io/hackage/v/haskoin-store-data.svg?label=haskoin-store-data)](https://hackage.haskell.org/package/haskoin-store-data)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/jprupp/haskoin-store/blob/master/store/LICENSE)

A Bitcoin (BTC) and Bitcoin Cash (BCH) block chain indexer with a REST API,
written in Haskell. It connects to the peer-to-peer network directly, so it
does not need a full node next to it, and keeps its indices in RocksDB.

- [API reference](https://github.com/jprupp/haskoin-store/blob/master/swagger.yaml)
- [Library documentation](https://hackage.haskell.org/package/haskoin-store)
  and [client documentation](https://hackage.haskell.org/package/haskoin-store-data)
- [Changelog](https://github.com/jprupp/haskoin-store/blob/master/CHANGELOG.md)
- [Report a bug](https://github.com/jprupp/haskoin-store/issues)

## Contents

- [Features](#features)
- [Quick start](#quick-start)
- [Building from source](#building-from-source)
- [Running](#running)
- [Configuration](#configuration)
- [API](#api)
- [How it works](#how-it-works)
- [Monitoring](#monitoring)
- [Using it from Haskell](#using-it-from-haskell)
- [Packages](#packages)
- [Development](#development)
- [Related projects](#related-projects)
- [License](#license)

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
| `--wipe-mempool` | `WIPE_MEMPOOL` | off | Delete the indexed mempool at start |
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
| `--xpub-gap` | `XPUB_GAP` | `32` | Unused addresses after which xpub derivation stops |
| `--xpub-gap-init` | `XPUB_GAP_INIT` | `20` | Addresses to look at for an xpub with no transactions |
| `--ticker-url` | `TICKER_URL` | Blockchain.info ticker | Source of current prices |
| `--price-history-url` | `PRICE_HISTORY_URL` | Blockchain.info price index | Source of historical prices |
| `--ticker-refresh` | `TICKER_REFRESH` | `90000000` | Microseconds between price updates |

### Redis cache

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--redis` | `REDIS` | off | Cache large xpubs in Redis |
| `--redis-url` | `REDIS_URL` | local Redis | Redis URL, such as `redis://host:6379` |
| `--redis-min-addrs` | `REDIS_MIN_ADDRS` | `100` | Cache xpubs with at least this many used addresses |
| `--redis-max-keys` | `REDIS_MAX_KEYS` | `100000000` | Evict least recently used xpubs past this many keys |
| `--redis-sync-interval` | `REDIS_SYNC_INTERVAL` | `30` | Seconds between mempool syncs to Redis |

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
| `--bloom` | `BLOOM` | off | Bloom filters, for faster lookups at the cost of memory |
| `--max-open-files` | `MAX_OPEN_FILES` | `2000` | Open files; bounds index and filter memory (0 = unlimited) |
| `--write-buffer-mb` | `WRITE_BUFFER_MB` | `256` | Memtables across all column families (0 = unlimited) |
| `--max-wal-mb` | `MAX_WAL_MB` | `1024` | Write-ahead log size that forces flushes (0 = RocksDB default) |
| `--block-cache-mb` | `BLOCK_CACHE_MB` | `1024` | Shared block cache, holding index and filter blocks too (0 = RocksDB default) |

Memory used by RocksDB is roughly the block cache plus the memtables, on top of
the Haskell heap. `/dbstats` shows where it goes.

## API

Full reference: [`swagger.yaml`](swagger.yaml), also served at
[api.haskoin.com](https://api.haskoin.com/).

```sh
curl localhost:3000/address/bc1q…/balance
curl localhost:3000/xpub/xpub6C…/balances
curl localhost:3000/transaction/<txid>
curl -N localhost:3000/events              # stream of new blocks and txs
```

- Add `?pretty=true` for indented JSON.
- Send `Accept: application/octet-stream` for the binary encoding, which
  the Haskell client in `haskoin-store-data` decodes.
- Lists are paged with `limit` and `offset`, newest first. `start` begins
  a page at a transaction hash, a block height or a Unix time instead.
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
| Xpubs | `/xpub/:xpub`, `/xpub/:xpub/{transactions,transactions/full,balances,unspent}`, `DELETE /xpub/:xpub` (drop from cache); `derive=normal\|compat\|segwit` picks the address type |
| Network | `/peers`, `/health` (503 when unhealthy), `/dbstats` |
| Events | `/events`, as JSON lines over HTTP or as a WebSocket |
| Blockchain.info | `/blockchain/multiaddr`, `/blockchain/balance`, `/blockchain/rawaddr/:addr`, `/blockchain/unspent`, `/blockchain/rawtx/:txid`, `/blockchain/rawblock/:hash`, `/blockchain/latestblock`, `/blockchain/unconfirmed-transactions`, `/blockchain/export-history`, `/blockchain/q/…` |

Turn off the xpub endpoints with `--no-xpub` and the Blockchain.info ones
with `--no-blockchain-info`.

## How it works

```
peers ──► haskoin-node ──► block store ──► RocksDB ──► web server ──► clients
                │               │             ▲
                └─ headers      └─ events ──► cache writer ──► Redis
```

- **haskoin-node** keeps a pool of peers and syncs block headers.
- The **block store** (`Haskoin.Store.BlockStore`) downloads blocks from one
  peer at a time and mempool transactions from all of them, and imports them
  with the rules in `Haskoin.Store.Logic`. Each import is one atomic RocksDB
  batch, so a crash never leaves a half-written block.
- Reorganisations are handled by reverting blocks until the best block is
  back on the main chain of headers.
- The **cache writer** (`Haskoin.Store.Cache`) copies xpubs with many used
  addresses to Redis and keeps them current as blocks and transactions
  arrive. Without Redis, xpub queries read RocksDB directly.
- The **web server** (`Haskoin.Store.Web`) answers from RocksDB, through the
  cache when there is one.

The database layout is described in `Haskoin.Store.Database.Types`.

## Monitoring

- **`/health`** returns the result of the latest check as JSON, with status 200
  when every check passes and 503 otherwise. The checks are: index lag behind
  the headers, age of the last block and of the last transaction, transactions
  waiting to be imported, and connected peers. Point your load balancer at it.
- **StatsD**: with `--statsd`, the store reports the timing of every kind of
  request (`web.*`), database reads (`db.*`), cache hits and misses (`cache.*`),
  and block store gauges such as block and header heights and mempool size
  (`store.*`).
- **`/dbstats`** returns RocksDB memory use per column family and RocksDB's own
  statistics, as text.
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
starts the node, the block store and the cache from a `StoreConfig`, and
`withDB store.db` runs queries such as `getBalance` or `xPubTxs` against
it. Subscribe to `Store.pub` to receive `StoreEvent`s.

Every module has Haddock documentation; build it with `stack haddock`.

## Packages

This repository holds two Haskell packages:

- **`store/`** — `haskoin-store`: the indexer, the web server and the
  executable.
- **`data/`** — `haskoin-store-data`: the data types, their JSON and binary
  encodings, and a client for the API (`Haskoin.Store.WebClient`). Use this
  one to talk to a Haskoin Store server from Haskell.

## Development

```sh
stack build --test --no-run-tests
stack test haskoin-store-data
stack test haskoin-store       # includes a 6000-block heap-growth test
scripts/format                 # ormolu, the code style used here
scripts/bench-web.sh           # load-test the API with wrk
```

`stack.yaml` must pin released versions of every dependency. Pointing
`extra-deps` at a sibling checkout such as `../haskoin-core` is fine for
local testing, but CI rejects it.

The tests use [hspec](https://hspec.github.io/) and need no network access.

`package.yaml` in `store/` and `data/` is the source of each package
description. Change it rather than the `.cabal` files, which
[hpack](https://github.com/sol/hpack) generates.

### Contributing

Pull requests are welcome.

- Format the code with `scripts/format` (Ormolu).
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
[MIT License](https://github.com/jprupp/haskoin-store/blob/master/store/LICENSE).
