# Full default stress run after the catch-up, 2026-11-09

The default run of [2026-11-09](../2026-11-09/README.md) and
[2026-11-09-incremental](../2026-11-09-incremental/README.md), repeated after
three changes:
- reloads catch up with changes that the database has before their events
  ([2026-11-09-catch-up](../2026-11-09-catch-up/README.md));
- bodies that Redis has are rewritten before the commit
  ([2026-11-09-bodies](../2026-11-09-bodies/README.md));
- the server returns memory at once
  ([2026-11-09-memory](../2026-11-09-memory/README.md)).

See [report.md](report.md) for the results.

- **Code:** haskoin-store `a1275bcc`, haskoin-store-data `b304acd2`.
- **Machine and options:** as in the earlier full runs: AMD Ryzen 9 3900X,
  32 GiB, rootless Podman.
  - Workload: 4 wallets of 200 receiving and 200 change addresses, 200,000
    transactions, 2,000 per block.
  - Server: `+RTS -N4 -M2g`, plus `--disable-delayed-os-memory-return` from
    its default RTS options.
- **Files:** those of every run, without the server log.

## Compared with the earlier full runs

| | First run | Incremental | After the catch-up |
|---|---|---|---|
| Reloads | 2,030, all full | 2,387, of which 168 full | 2,098, of which 16 full |
| Mean milliseconds per reload | 2,448 | 452 | 305 |
| Longest reload, milliseconds | 8,640 | 7,561 | 5,628 |
| Requests per second, summary / transactions / unspent | 308 / 189 / 158 | 443 / 280 / 229 | 459 / 284 / 235 |
| Requests per second, balances / multiaddr | 96 / 70 | 131 / 98 | 135 / 101 |
| Worst p99 of a run, milliseconds, across endpoints | 360–477 | 414–562 | 244–401 |
| Commands that held Redis 10 ms or more | 32, longest 18 ms | 229, longest 27 ms | 130, longest 18.5 ms |
| Whole server at the end, MiB | – | – | 513, GHC heap 65 |
| Cached answers equal to the database's | 16 of 16 | 16 of 16 | 16 of 16 |

## Findings

**Full reloads are only the scheduled ones.** Of the 16 full reloads, 4 were
the first caching of each xpub. The other 12 were the reload of every entry
every ten minutes (`fullReloadEvery`), which happened at 605, 1,212 and
1,817 seconds. None was a fallback.

**Reloads are faster.** The mean reload was 305 ms, against 452 ms before the
catch-up and 2,448 ms in the first run. The worst p99 of a run of wrk was
244–401 ms across endpoints, against 414–562 ms before.

**Slow commands are fewer and shorter.** 130 commands took 10 ms or more,
against 229 before the catch-up, and the longest took 18.5 ms, against 27.
All but one were incremental commits with a median of 7,378 arguments,
the size of a block's confirmation in this workload, as in the shorter runs.
The first run had fewer (32). Its reloads took seconds, so it may have
combined more changes into each commit, but this run cannot tell.

**The scheduled full reloads cost memory, in bursts.** The GHC heap stayed
between about 50 and 100 MiB, except during two of the scheduled full
reloads:

| Seconds | GHC heap, MiB | Whole server, MiB |
|---|---|---|
| 1,208–1,211 | up to 539 | up to 890 |
| 1,813–1,817 | up to 785 | up to 1,215 |

Each burst lasted a few seconds, and the memory went back to the system at
once, so the server ended at 513 MiB. The four xpubs were cached together,
so they reloaded together; by then each had tens of thousands of
transactions, which a full reload holds in memory from both the database
and Redis. In production this is the likely source of a large heap: one
large xpub, or several reloading at the same time.

The burst could be made smaller in two ways, neither of them done yet:
- spread the scheduled full reloads of different entries over time;
- have a full reload compare the database and Redis in chunks rather than
  whole.
