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
- Lists are paged with `limit` and `offset`.

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
