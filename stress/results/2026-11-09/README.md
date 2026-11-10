# Full default stress run, 2026-11-09

The results of `scripts/stress-cache.sh` with its default options, kept for
comparison with later runs. See [report.md](report.md) for the results.

- **Code:** haskoin-store `c34ed3d3` (web server directly on WAI), haskoin-store-data
  `91eaeb2e` (1.9.0), haskoin-node `10d6a3ce`.
- **Machine:** AMD Ryzen 9 3900X (12 cores, 24 threads), 32 GiB of memory, Fedora,
  rootless Podman. The server, Bitcoin Core, Valkey and wrk shared the machine.
- **Options:** see [config.json](config.json): 4 wallets of 200 receiving and 200 change
  addresses, 200,000 transactions, 2,000 per block, server with `+RTS -N4 -M2g`.

## Files

| File | What it holds |
|---|---|
| `report.md` | the report written by `scripts/stress-report.py` |
| `summary.json` | the totals that the stress tool measured |
| `config.json` | the options of the run and the xpubs of the wallets |
| `rounds.jsonl` | one line per block: timings of signing, sending and catching up |
| `wrk.jsonl` | one line per wrk run: requests, latency percentiles, errors |
| `samples.jsonl.gz` | the StatsD samples and resource measurements, compressed |

The server log (2.9 MB) is left out. To write the report again, decompress
`samples.jsonl.gz` in a copy of this directory and run
`python3 scripts/stress-report.py <copy>`.

## Findings

- All 200,000 transactions accepted, every cached answer checked matched the
  database, no errors in the server log and no failed requests.
- Redis was never blocked for long: 32 scripts of 10 ms or more, the longest 18 ms.
- Reload time grows linearly with the size of the xpub, from 0.6 s to 4.7 s on average
  (8.6 s at most) at about 100,000 transactions per xpub. Almost all of it is the
  server reading the whole xpub from RocksDB; reading only what changed would remove
  that growth.
- The server was never more than one block behind the node.
- Server memory grew steadily from 367 MiB to 1,339 MiB over the run; the cause is
  not known yet.
- Bitcoin Core was the bottleneck at about 93 transactions per second.
