# Haskoin Store

[![CI](https://github.com/jprupp/haskoin-store/actions/workflows/ci.yml/badge.svg)](https://github.com/jprupp/haskoin-store/actions/workflows/ci.yml)

A Bitcoin (BTC) and Bitcoin Cash (BCH) block chain indexer with a REST API,
written in Haskell. It connects to the peer-to-peer network directly, so it
does not need a full node next to it, and keeps its indices in RocksDB.

- Indices for address balances, transactions and unspent outputs (UTXO).
- Balances, transactions and UTXO for extended public keys (xpub), with an
  optional Redis cache for large wallets.
- Persistent mempool, indexed by arrival time. RBF replacement on BTC.
- JSON or compact binary responses, and a live event stream of new blocks
  and transactions.
- Compatibility endpoints in the style of Blockchain.info under
  `/blockchain/`.
- Metrics over StatsD.

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

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `-n`, `--net` | `NET` | `bch` | Network: `btc`, `bch`, or a test network (see `--help`) |
| `-d`, `--dir` | `DIR` | `~/.haskoin-store` | Database directory |
| `--host` / `--port` | `HOST` / `PORT` | `*` / `3000` | Address the API listens on |
| `--discover` | `DISCOVER` | off | Discover peers automatically |
| `-p`, `--peer` | `PEER` | none | Connect to `HOST[:PORT]` (repeat for more; space-separated in `PEER`) |
| `--redis` / `--redis-url` | `REDIS` / `REDIS_URL` | off | Cache xpubs in Redis |
| `--no-mempool` | `NO_MEMPOOL` | off | Do not index unconfirmed transactions |
| `--noweb` | `NO_WEB` | off | Index only, no API |
| `--debug` | `DEBUG` | off | Verbose logging |

Every option can also be set through the environment variable next to it,
which is handy in containers. `haskoin-store --help` lists the rest: API
page limits, xpub gap, timeouts, peer counts, StatsD and RocksDB tuning.

Give it either `--discover` or at least one `--peer`, or it has nobody to
sync from. For a big public deployment, also set `MALLOC_ARENA_MAX=2` (the
Docker image does) to keep glibc from fragmenting RocksDB's memory.

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

Changes are listed in [CHANGELOG.md](CHANGELOG.md).

## License

MIT. See [store/LICENSE](store/LICENSE).
