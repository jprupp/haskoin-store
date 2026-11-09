# Stress run with the catch-up of incremental reloads, 2026-11-09

The run with the fix of [2026-11-09-memory](../2026-11-09-memory/README.md)
(`dontneed`), after incremental reloads learnt to find changes that the
database has before their events, instead of reading the whole xpub. See
[report.md](report.md) for the results.

- **Code:** haskoin-store with the catch-up (the commit that adds this
  directory), haskoin-store-data `b304acd2`.
- **Machine and options:** AMD Ryzen 9 3900X, 32 GiB, rootless Podman.
  - Workload: 4 wallets of 200 receiving and 200 change addresses, 60,000
    transactions, 2,000 per block.
  - Server: `+RTS -N4 -M2g`, plus `--disable-delayed-os-memory-return`, which
    is now in its default RTS options.
- **Files:** those of every run, without the server log. The slow log now
  records which script each slow command ran and how many arguments it had.

## Compared with the run before the catch-up

| | Before (`dontneed`) | With the catch-up |
|---|---|---|
| Reloads | 460 | 416 |
| Of which full | 48 | 4, the first caching of each xpub |
| Mean milliseconds per reload | 387 | 386 |
| Longest reload, milliseconds | 3,761 | 4,831 |
| Worst p99 of a run, milliseconds, across endpoints | 107–181 | 42–149 |
| Requests per second, summary / transactions / unspent | 435 / 271 / 223 | 445 / 277 / 227 |
| Commands that held Redis 10 ms or more | 84 | 81 |
| Cached answers equal to the database's | 16 of 16 | 16 of 16 |

## Findings

**The fallbacks are gone.** Before, about one reload in ten read the whole
xpub. That happened whenever the balances showed changes that the reload
had not been told of: under load the database runs ahead of the events
being applied. Now the reload finds those transactions from the addresses
whose balances moved, and from the mempool transactions of the entry. The
only full reloads left were the first caching of each xpub.

**Reload times did not change at this size.** The xpubs of this run end
with about 15,000 transactions each, so a full reload of one is cheap. The
time of a reload is mostly the confirmation of a block, which brings about
1,000 transactions of each xpub to read whichever path runs. The saving
grows with the size of the xpub, as the full runs showed: there a full
reload took seconds at 100,000 transactions.

**The slow scripts are the confirmations of blocks.** All 81 commands that
took 10 ms or more were incremental commits:
- **Size:** they had a median of 7,950 arguments, up to 8,236.
- **What they are:** a block of 2,000 transactions between four wallets
  confirms about 1,000 transactions of each xpub, and its commit rewrote,
  in one script, each transaction's score, its body with its new block,
  its spenders, and the outputs it created.
- **Before the catch-up:** the earlier runs recorded only "EVAL" in the slow
  log, but the count of slow commands then was 84, about the same.
- **Next step:** this led to rewriting bodies that Redis already has in
  small scripts before the commit (see the CHANGELOG).
