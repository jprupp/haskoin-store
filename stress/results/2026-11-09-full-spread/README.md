# Full default stress run with spread full reloads, 2026-11-09

The run of [2026-11-09-full-catch-up](../2026-11-09-full-catch-up/README.md),
with the same options, after the scheduled full reload of each xpub got its
own interval, between five and ten minutes, instead of ten minutes for all.
See [report.md](report.md) for the results.

- **Code:** haskoin-store with the change (the commit that adds this
  directory), haskoin-store-data `b304acd2`.
- **Machine and options:** as in the earlier full runs. The files are those
  of every run, without the server log.

## Compared with the run before

| | Same interval for all | Spread |
|---|---|---|
| Full reloads | 16, at 4 moments | 18, at 15 moments |
| Highest GHC heap, MiB | 785 | 250 |
| Highest whole server, MiB | 1,215 | 693 |
| Whole server at the end, MiB | 513 | 502 |
| Mean milliseconds per reload | 305 | 332 |
| Longest reload, milliseconds | 5,628 | 11,873 |
| Worst p99 of a run, milliseconds, across endpoints | 244–401 | 52–106 |
| Commands that held Redis 10 ms or more | 130, longest 18.5 ms | 135, longest 20.1 ms |
| Cached answers equal to the database's | 16 of 16 | 16 of 16 |

## Findings

- **Full reloads no longer come together.** The xpubs were cached together
  but drifted apart from their first scheduled full reload. After that, each
  xpub was read whole at its own time. In the run before, all four xpubs
  were read whole at 605, 1,212 and 1,817 seconds.
- **Memory peaks are a third of what they were.** The GHC heap peaked at
  250 MiB instead of 785, and the whole server at 693 MiB instead of
  1,215: one xpub's full reload at a time instead of four.
- **The worst latencies of the web server fell.** The worst p99 of a run of
  wrk dropped from 244–401 ms to 52–106 ms across endpoints. This fits the
  server no longer reading four large xpubs at once, but it is one run of
  each.
- **One full reload took 11.9 seconds**, at 1,647 seconds. It was the only
  reload to finish in that second, together with a full reload, so it was
  almost certainly that full reload. The others took up to about 5 seconds,
  as in earlier runs. This run does not show why. One possible cause is a
  commit retried after a transaction body went missing, which reads the
  xpub again. Every transaction of this test touches two xpubs, so a commit
  of one can remove a body that a commit of the other expects. But retries
  are not recorded, so this is a guess. A reader of that xpub got the entry
  as it was before the reload while it ran, so no answer was wrong; the
  entry was just late.
- **Mean reload time was a little higher**, 332 ms against 305 ms. The long
  reload above accounts for some of that; one run of each cannot say more.
