# Full default stress run with incremental reloads, 2026-11-09

The same run as [2026-11-09](../2026-11-09/README.md), after the cache learnt to
bring an entry up to date from what changed instead of reading the whole xpub
on every change. See [report.md](report.md) for the results.

- **Code:** haskoin-store `bd22ad11`, haskoin-store-data `b304acd2`,
  haskoin-node `10d6a3ce`.
- **Machine and options:** as in the first run: AMD Ryzen 9 3900X, 32 GiB,
  rootless Podman; 4 wallets of 200 receiving and 200 change addresses,
  200,000 transactions, 2,000 per block, server with `+RTS -N4 -M2g`. The
  server ran without `--disable-delayed-os-memory-return`, which came later.

The files are those of the first run; the server log is left out.

## Compared with the first run

| | First run | Incremental |
|---|---|---|
| Reloads | 2,030 | 2,387, of which 168 full |
| Mean milliseconds per reload | 2,448 | 452 |
| Longest reload, milliseconds | 8,640 | 7,561 |
| Requests per second, summary / transactions / unspent | 308 / 189 / 158 | 443 / 280 / 229 |
| Requests per second, balances / multiaddr | 96 / 70 | 131 / 98 |
| Median milliseconds, summary / transactions / multiaddr | 4.8 / 8.3 / 20.6 | 3.5 / 5.5 / 17.1 |
| Scripts that held Redis 10 ms or more | 32, longest 18 ms | 229, longest 27 ms |
| Cached answers equal to the database's | 16 of 16 | 16 of 16 |

## Findings

- A reload that reads only what changed takes as long however large the xpub;
  the mean is now set by the full reloads, which still grow with it.
- 168 reloads of 2,387 were full, many more than the four first cachings and
  the one every ten minutes per xpub. At this rate of transactions, the
  balances that a reload reads often already include transactions that
  arrived after the events it applies, which the check takes for a missed
  change. Applying those transactions, rather than reading the xpub whole, is
  the next improvement.
- More scripts held Redis for 10 ms or more, up to 27 ms. Not yet looked into.
- The memory of the server grew as in the first run. A heap profile showed
  10 MiB of live data throughout; the growth was memory that the GHC heap had
  given back with `MADV_FREE`, still counted as resident, and RocksDB caches
  filling up to their limits. The executable now returns memory at once
  (`--disable-delayed-os-memory-return`).
