# Full default stress run, 2026-11-10

The results of `scripts/stress-cache.sh` with its default options on
version 1.9.0, kept for comparison with other runs. See
[report.md](report.md) for the results, and
[the run of 2026-11-09](../2026-11-09/README.md) for version 1.8.1 on the
same machine with the same options.

- **Code:** haskoin-store `5c8c4f24` (1.9.0), haskoin-store-data 1.11.0,
  haskoin-node 2.5.0, rocksdb-haskell-jprupp 2.7.1.
- **Machine:** AMD Ryzen 9 3900X (12 cores, 24 threads), 32 GiB of memory,
  Fedora, rootless Podman. The server, Bitcoin Core, Valkey and wrk shared
  the machine.
- **Options:** see [config.json](config.json): 4 wallets of 200 receiving
  and 200 change addresses, 200,000 transactions, 2,000 per block, server
  with `+RTS -N4 -M2g`.

## Files

| File | What it holds |
|---|---|
| `report.md` | the report written by `scripts/stress-report.py` |
| `summary.json` | the totals that the stress tool measured |
| `config.json` | the options of the run and the xpubs of the wallets |
| `rounds.jsonl` | one line per block: timings of signing, sending and catching up |
| `wrk.jsonl` | one line per wrk run: requests, latency percentiles, errors |
| `samples.jsonl.gz` | the StatsD samples and resource measurements, compressed |

The server log is left out. To write the report again, decompress
`samples.jsonl.gz` in a copy of this directory and run
`python3 scripts/stress-report.py <copy>`.

## Findings

- All 200,000 transactions accepted, every cached answer checked matched
  the database, no errors in the server log and no failed requests.
- The server was never more than one block behind the node.

Against the run of 2026-11-09:

| | 1.8.1 | 1.9.0 |
|---|---|---|
| Mean cache reload | 2,448 ms | 234 ms |
| Longest cache reload | 8,640 ms | 4,861 ms |
| Mean reload in the last twelfth of the run | 4,666 ms | 154 ms |
| Server memory at the end | 1,339 MiB | 728 MiB |
| Requests answered from the cache | 1.74 million | 2.29 million |
| Worst p99 latency, by endpoint | 360 to 477 ms | 64 to 167 ms |
| Scripts that held Redis 10 ms or more | 32, longest 18 ms | 160, longest 27 ms |

- **Reload time no longer grows with the xpub.** Only 18 of 2,117 reloads
  read the whole xpub; the rest read what changed. The reloads of several
  seconds that remain are those 18.
- **Tail latencies are a third to a fifth of what they were**, since
  requests no longer wait behind long reloads.
- **Server memory still grows over the run**, to about half the earlier
  peak. The GHC heap is 100 to 300 MiB of it and goes up and down.
- **Redis is held more often, briefly.** An update now writes more in one
  script. It is the number to watch with several servers on one Redis.
- Bitcoin Core was the bottleneck, at about 103 transactions per second.

This run is of the Redis cache of a server alone. It does not exercise a
fleet or the filters of `/events`.
