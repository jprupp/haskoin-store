# Stress test of the Redis cache

60,000 transactions between 4 wallets of 200 receiving and 200 change addresses, 2,000 per block, in 398.8 seconds, while wrk asked the server about their xpubs on five endpoints with 2 connections each.

## Transactions

| | |
|---|---|
| Sent | 60,000 |
| Refused by the node | 0 |
| Blocks | 30 |
| Seconds to make them | 379.5 |
| Transactions per second | 158.1 |
| Median milliseconds to sign a block of them | 166.1 |
| Median milliseconds for the node to accept them | 9,910.5 |
| Seconds for the server to catch up at the end | 5.0 |

## Cache

| | |
|---|---|
| Reloads of an xpub | 467 |
| Of which full, reading the whole xpub | 4 |
| Reloads per second while making transactions | 1.2 |
| Mean milliseconds per reload | 327.6 |
| Longest reload, milliseconds | 5,799 |
| Reads answered from the cache | 461,078 |
| Reads that missed the cache | 4 |
| Reloads given up because Redis knew more | 0 |
| Evictions | 0 |

## Commands that blocked Redis

Redis runs one command or script at a time, so a long one makes every client wait, every server sharing the cache included. These are the commands that took 10 ms or more.

| Command | Count | Total ms | Median ms | Longest ms | Median arguments | Most arguments |
|---|---|---|---|---|---|---|
| commitScript (incremental) | 53 | 609.6 | 11.0 | 17.7 | 7,495 | 8,006 |

## Over time

The run split into equal parts of time. Reload times grow with the number of transactions of the xpubs; the lag is how many blocks the server was behind the node.

| Seconds | Reloads | Mean reload ms | Longest reload ms | Most blocks behind | Server CPU % | Server MiB | GHC heap MiB | Valkey MiB |
|---|---|---|---|---|---|---|---|---|
| 1–32 | 43 | 373.0 | 2,587 | 1 | 370.6 | 169.7 | 85.1 | 11.4 |
| 33–64 | 44 | 123.1 | 498 | 1 | 403.8 | 184.9 | 84.5 | 19.3 |
| 65–96 | 44 | 358.0 | 2,618 | 1 | 396.0 | 218.5 | 93.1 | 26.2 |
| 97–127 | 40 | 171.6 | 726 | 1 | 424.1 | 229.4 | 87.0 | 33.5 |
| 128–159 | 29 | 300.8 | 5,531 | 1 | 380.3 | 256.3 | 95.9 | 41.4 |
| 160–192 | 39 | 979.4 | 5,799 | 1 | 532.8 | 261.4 | 96.9 | 46.0 |
| 193–216 | 48 | 335.6 | 2,100 | 1 | 708.8 | 267.1 | 91.2 | 52.8 |
| 231–251 | 44 | 233.7 | 1,209 | 1 | 608.4 | 284.4 | 92.0 | 60.4 |
| 265–285 | 40 | 102.7 | 513 | 1 | 688.6 | 262.0 | 67.7 | 65.1 |
| 301–320 | 20 | 114.5 | 489 | 1 | 397.7 | 270.9 | 61.9 | 67.5 |
| 321–352 | 32 | 96.7 | 538 | 1 | 740.0 | 298.7 | 85.9 | 70.0 |
| 353–385 | 44 | 593.2 | 3,816 | 1 | 589.2 | 307.9 | 89.9 | 79.6 |

```
reloads          ▇▇▇▆▅▆█▇▆▃▅▇
mean reload ms   ▃▁▃▂▃█▃▂▁▁▁▅
blocks behind    ████████████
server CPU       ▄▄▄▅▄▆▇▆▇▄█▆
```

## Web server under load

Each line sums up the runs of wrk on one endpoint, each rotating over every xpub. Latencies are in milliseconds: the median of the medians of the runs, the worst 99th percentile of a run, and the slowest request.

| Endpoint | Runs | Requests | Requests per second | Median | Worst p99 | Slowest | Not 2xx | Errors |
|---|---|---|---|---|---|---|---|---|
| transactions | 38 | 105,315 | 277.0 | 5.5 | 54.7 | 182.5 | 0 | 0 |
| unspent | 38 | 88,522 | 232.8 | 6.4 | 73.4 | 182.5 | 0 | 0 |
| balances | 38 | 51,559 | 135.6 | 13.2 | 93.7 | 210.9 | 0 | 0 |
| multiaddr | 38 | 36,868 | 97.0 | 17.2 | 88.8 | 202.9 | 0 | 0 |
| summary | 38 | 172,729 | 454.3 | 3.4 | 58.7 | 172.9 | 0 | 0 |

The 99th percentile of each run, in order:

```
transactions   ▄▆▅▇▆▅█▄▄▅▄▅▄▄▅▄▅▄▄▄▅▄▄▄▅▄▄▅▆▄▄▄▄▄▄▅▄▅  (up to 54.7 ms)
unspent        ▃█▃▅▅▄▇▃▆▄▄▆▃▄▄▃▄▄▃▄▄▃▃▃▄▃▃▄▄▃▃▃▃▃▄▄▃▅  (up to 73.4 ms)
balances       ▃▆▃▆▅▄█▃▃▃▄▄▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▂▄  (up to 93.7 ms)
multiaddr      █▇▅▆▆▇▇▆▅▆▅▆▅▇▆▅▅▆▅▅▅▅▅▅▅▅▅▆▆▅▅▆▅▅▆▅▅▇  (up to 88.8 ms)
summary        ▃▆▃▆▅▃▆▃▃▃█▄▃▃▃▃▄▃▃▄▃▃▃▅▄▃▃▃▃▃▃▃▃▃▄▃▃▄  (up to 58.7 ms)
```

## Cached answers

All 16 answers checked at the end (summary, balances, first page of transactions and of unspent outputs of every xpub) were the same with and without the cache.

The server logged 0 errors (apart from health checks while it started); see `server.log`.
