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
kind of addresses it derives (`normal`, `compat` for nested SegWit, `segwit`
for native SegWit):

```sh
curl 'localhost:3000/xpub/xpub6C...?derive=segwit&pretty=true'
```

The answer has confirmed and unconfirmed balances, and the index of the last
used receiving and change address, so the wallet knows where to derive the
next address.

### 3. List its transactions, page by page

```sh
curl 'localhost:3000/xpub/xpub6C.../transactions?derive=segwit&limit=50'
```

Results are newest first, unconfirmed ones on top. To get the next page, start
from the last transaction you received instead of using an offset:

```sh
curl 'localhost:3000/xpub/xpub6C.../transactions?derive=segwit&limit=50&offset=1&start=<last txid>'
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
least `--redis-min-addrs` used addresses are then cached after their first
query and answered from Redis, transactions in full included. Several stores
can share one Redis: they keep each other's cache up to date, and a store that
has not imported a transaction yet still serves it from the cache. Add
`nocache=true` to a query to read the database instead.

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
| `--redis-max-xpubs` | `REDIS_MAX_XPUBS` | `10000` | Evict the least recently used xpubs past this many |

Every key of the cache starts with `hs2:<network>:`, so a Redis can be shared
by stores of several networks, and by stores of older versions, whose cache
uses other keys. `--redis-max-keys` and `--redis-sync-interval` of older
versions are gone.

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
                └─ headers      └─ events ──► cache ◄──► Redis ◄──► other stores
```

- **haskoin-node** keeps a pool of peers and syncs block headers.
- The **block store** (`Haskoin.Store.BlockStore`) downloads blocks from one
  peer at a time and mempool transactions from all of them, and imports them
  with the rules in `Haskoin.Store.Logic`. Each import is one atomic RocksDB
  batch, so a crash never leaves a half-written block.
- Reorganisations are handled by reverting blocks until the best block is
  back on the main chain of headers.
- The **cache** (`Haskoin.Store.Cache`) keeps xpubs with many used addresses
  in Redis: their balances, transactions, unspent outputs and the bodies of
  their transactions. It never patches an entry: when a block, a mempool
  transaction or a reorganisation touches a cached xpub, the store reads the
  whole xpub again from RocksDB, with the same queries that answer uncached
  requests, and replaces the entry in one Lua script, together with every
  transaction body it needs. Stores sharing a Redis claim each change, so it
  is applied once, by the first store to see it; a store never replaces an
  entry that knows more than its database (a higher block, or a transaction
  it has not seen), and a store that is catching up does no cache work.
  Without Redis, xpub queries read RocksDB directly.
- The **web server** (`Haskoin.Store.Web`) answers from RocksDB, and from the
  cache for cached xpubs and for transaction bodies that Redis has.

Xpub addresses are derived up to an exact gap: every address up to the last
used one of each chain, then exactly `--xpub-gap` unused ones, and always the
first `--xpub-gap-init` addresses. A payment further away is not seen, as a
wallet following BIP 44 with the same gap would not see it; raise the gap to
see it.

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
starts the node, the block store and the cache from a `StoreConfig`, and the
functions of `Haskoin.Store.Database.Query`, such as `getBalance store.db
addr` or `xPubTxs store.db bals limits`, query its database in `IO`.
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
transactions went in, how many reloads the cache did and how long they took
as the xpubs grew, how far the server fell behind, its CPU and memory, the
memory of Valkey, and the latencies of each endpoint over time.

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
[MIT License](https://github.com/jprupp/haskoin-store/blob/master/haskoin-store/LICENSE).
