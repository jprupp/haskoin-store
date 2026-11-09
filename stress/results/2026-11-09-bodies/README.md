# Stress run with bodies rewritten before the commit, 2026-11-09

The run of [2026-11-09-catch-up](../2026-11-09-catch-up/README.md), with the
same options, after bodies that Redis already has started being rewritten
in small scripts of 200 before the commit of a reload, instead of inside
it. See [report.md](report.md) for the results.

- **Code:** haskoin-store with the change (the commit that adds this
  directory), haskoin-store-data `b304acd2`.
- **Machine and options:** as in the catch-up run. The files are those of
  every run, without the server log.

## Compared with the catch-up run

| | Catch-up | Bodies before the commit |
|---|---|---|
| Reloads | 416, of which 4 full | 467, of which 4 full |
| Mean milliseconds per reload | 386 | 328 |
| Commands that held Redis 10 ms or more | 81 | 53 |
| Total milliseconds of those commands | 936 | 610 |
| Longest command, milliseconds | 19.3 | 17.7 |
| Median arguments of a slow commit | 7,950 | 7,495 |
| Worst p99 of a run, milliseconds, across endpoints | 42–149 | 55–94 |
| Cached answers equal to the database's | 16 of 16 | 16 of 16 |

## Findings

- **Fewer slow commits.** A third fewer commands held Redis for 10 ms or
  more, and none of the scripts that rewrite bodies did. Reloads were faster
  on average too, though a single run of each is not enough to say by how
  much.
- **The slow commits that remain did not get much smaller.** Bodies were a
  small part of them: the median size fell by only 455 arguments.
- **What the rest is.** From the code: a block confirms about 1,000
  transactions of each xpub. Each one moves to its block in the sorted set of
  transactions (2 arguments), its outputs at the xpub move to their new
  scores (3 each), and its inputs from the xpub are removed again (2 each).
  That is about 7 arguments a transaction. This is the estimate that fits
  the slow log, not a measurement of each part.
- **What would shorten them further.** All of those moves are made in one
  script so that a reader never sees half a block confirmed. Splitting the
  commit of a large block into several scripts would shorten them, at the
  cost of that guarantee for the moment between the scripts. Skipping
  removals of outputs that the mempool already removed would save the 2
  arguments per input, but the store would have to read Redis first to know
  which those are.
- **The workload is extreme.** On a live chain, one xpub rarely has a
  thousand transactions in a single block.
