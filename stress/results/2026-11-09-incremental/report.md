# Stress test of the Redis cache

200,000 transactions between 4 wallets of 200 receiving and 200 change addresses, 2,000 per block, in 2,378.3 seconds, while wrk asked the server about their xpubs on five endpoints with 2 connections each.

## Transactions

| | |
|---|---|
| Sent | 200,000 |
| Refused by the node | 0 |
| Blocks | 100 |
| Seconds to make them | 2,362.6 |
| Transactions per second | 84.7 |
| Median milliseconds to sign a block of them | 165.0 |
| Median milliseconds for the node to accept them | 24,954.0 |
| Seconds for the server to catch up at the end | 2.4 |

## Cache

| | |
|---|---|
| Reloads of an xpub | 2,387 |
| Of which full, reading the whole xpub | 168 |
| Reloads per second while making transactions | 1.0 |
| Mean milliseconds per reload | 452.3 |
| Longest reload, milliseconds | 7,561 |
| Reads answered from the cache | 2,746,524 |
| Reads that missed the cache | 4 |
| Reloads given up because Redis knew more | 0 |
| Evictions | 0 |

## Commands that blocked Redis

Redis runs one command or script at a time, so a long one makes every client wait, every server sharing the cache included. These are the commands that took 10 ms or more.

| Command | Count | Total ms | Median ms | Longest ms |
|---|---|---|---|---|
| EVAL | 229 | 2,702.5 | 11.1 | 27.1 |

## Over time

The run split into equal parts of time. Reload times grow with the number of transactions of the xpubs; the lag is how many blocks the server was behind the node.

| Seconds | Reloads | Mean reload ms | Longest reload ms | Most blocks behind | Server CPU % | Server MiB | Valkey MiB |
|---|---|---|---|---|---|---|---|
| 1–187 | 241 | 364.2 | 5,918 | 1 | 488.4 | 334.5 | 41.8 |
| 201–394 | 196 | 486.9 | 4,668 | 1 | 533.7 | 469.5 | 65.5 |
| 395–589 | 192 | 267.7 | 2,684 | 1 | 636.2 | 520.7 | 85.4 |
| 592–788 | 192 | 263.4 | 4,538 | 1 | 514.5 | 628.6 | 105.4 |
| 792–984 | 216 | 571.1 | 5,277 | 1 | 604.1 | 773.7 | 125.7 |
| 994–1,182 | 204 | 540.9 | 4,672 | 1 | 615.1 | 840.3 | 147.8 |
| 1,198–1,379 | 180 | 331.3 | 3,237 | 1 | 570.1 | 873.1 | 162.5 |
| 1,381–1,575 | 184 | 334.0 | 4,532 | 1 | 533.3 | 989.6 | 178.1 |
| 1,578–1,774 | 204 | 281.1 | 6,066 | 1 | 607.3 | 1,057.9 | 194.2 |
| 1,786–1,970 | 182 | 744.3 | 7,561 | 1 | 569.3 | 1,141.4 | 210.7 |
| 1,972–2,168 | 196 | 597.8 | 4,468 | 1 | 535.2 | 1,291.5 | 226.4 |
| 2,169–2,366 | 200 | 652.0 | 4,286 | 1 | 501.5 | 1,304.2 | 243.4 |

```
reloads          █▆▆▆▇▆▆▆▆▆▆▆
mean reload ms   ▄▅▃▃▆▆▄▄▃█▆▇
blocks behind    ████████████
server CPU       ▆▆█▆▇▇▇▆▇▇▆▆
```

## Web server under load

Each line sums up the runs of wrk on one endpoint, each rotating over every xpub. Latencies are in milliseconds: the median of the medians of the runs, the worst 99th percentile of a run, and the slowest request.

| Endpoint | Runs | Requests | Requests per second | Median | Worst p99 | Slowest | Not 2xx | Errors |
|---|---|---|---|---|---|---|---|---|
| transactions | 232 | 650,464 | 280.2 | 5.5 | 455.4 | 636.7 | 0 | 0 |
| balances | 232 | 304,837 | 131.3 | 13.5 | 562.4 | 795.8 | 0 | 0 |
| multiaddr | 232 | 226,639 | 97.6 | 17.1 | 480.8 | 615.9 | 0 | 0 |
| summary | 231 | 1,024,693 | 443.4 | 3.5 | 413.8 | 558.3 | 0 | 0 |
| unspent | 232 | 530,544 | 228.6 | 6.6 | 461.4 | 598.3 | 0 | 0 |

The 99th percentile of each run, in order:

```
transactions   ▁▁▂▁▁▁▁▁▁▁▁▁▂▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▂▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▃▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▄▁▃▁▂▁▁▁▁▁▁▁▁▁▃▃▁▄▁▁▂▁▁▁▁▃▁▁▁▁▁▁▁▁▁▁▃▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▄▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁█▁▃▁▁▁▁▁▁▄▁▁▁▁▂▁▂▁▁▁▁▅▁▁▁▁▁▅▁▁▆▆▁▁▁▆▁▄▁▁▁▁▁▁▁▆▁▁▄▁▁▁▇▁▁  (up to 455.4 ms)
balances       ▁▁▂▁▁▁▁▁▁▂▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▃▁▁▁▁▁▃▁▁▁▂▃▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▃▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▄▁▄▁▃▁▁▁▁▁▁▁▁▁▅▄▁▄▁▁▂▁▁▁▁▅▁▁▁▁▁▁▁▁▁▁▅▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▄▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁█▁▄▁▁▁▁▁▁▄▁▁▁▁▃▁▂▁▁▁▁▅▁▁▁▁▁▅▃▁▆▅▁▁▁▅▁▄▁▁▁▁▁▁▁▆▁▁▆▁▁▁▇▁▁  (up to 562.4 ms)
multiaddr      ▂▂▂▁▂▂▂▁▁▂▁▁▂▁▂▁▂▁▂▁▁▂▁▁▁▁▁▁▂▁▁▁▃▁▁▃▁▃▂▂▁▁▂▃▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▃▁▁▁▁▂▁▁▁▁▁▂▂▁▂▁▁▁▁▁▁▅▁▄▁▃▁▁▁▁▁▂▁▁▁▄▅▁▄▁▁▂▁▁▁▁▄▂▁▁▂▁▂▁▂▁▁▄▁▁▁▁▁▂▁▁▁▁▂▁▁▂▁▁▁▁▁▁▁▅▁▂▁▁▁▁▁▁▁▁▂▁▁▂▂▁▁▂▁▂▁▁▁▁▁▁▁▁▂▇▂▄▂▂▁▁▁▄▁▁▁▁▂▃▂▂▁▁▂▅▁▁▁▁▁▂▆▁▂▇▁▁▁▆▂▆▁▁▁▁▁▁▁▂▆▁▅▁▁▁▂█▁▁  (up to 480.8 ms)
summary        ▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▂▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▄▁▄▁▂▁▁▁▁▁▁▁▁▁▃▃▁▄▁▁▂▁▁▁▁▃▁▁▁▁▁▁▁▁▁▁▃▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▄▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▇▁▃▁▁▁▁▁▄▁▁▁▁▁▂▂▁▁▁▁▅▁▁▁▁▁▁▄▁▁▇▁▁▁▆▁▅▁▁▁▁▁▁▁▇▁▁▅▁▁▁█▁▁  (up to 413.8 ms)
unspent        ▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▂▁▁▂▁▁▁▁▃▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▃▁▂▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▄▁▃▁▃▁▁▁▁▁▁▁▁▃▄▁▁▄▁▁▂▁▁▁▁▃▁▁▁▁▁▁▁▁▁▁▄▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▅▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▇▃▁▁▂▁▁▁▄▁▁▁▁▂▂▂▁▁▁▁▅▁▁▁▁▁▁▆▁▂▆▁▁▁▆▁▅▁▁▁▁▁▁▁▁▂▁▅▁▁▁█▁▁▁  (up to 461.4 ms)
```

## Cached answers

All 16 answers checked at the end (summary, balances, first page of transactions and of unspent outputs of every xpub) were the same with and without the cache.

The server logged 0 errors (apart from health checks while it started); see `server.log`.
