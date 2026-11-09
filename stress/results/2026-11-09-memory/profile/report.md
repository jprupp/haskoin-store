# Stress test of the Redis cache

60,000 transactions between 4 wallets of 200 receiving and 200 change addresses, 2,000 per block, in 461.1 seconds, while wrk asked the server about their xpubs on five endpoints with 2 connections each.

## Transactions

| | |
|---|---|
| Sent | 60,000 |
| Refused by the node | 0 |
| Blocks | 30 |
| Seconds to make them | 445.1 |
| Transactions per second | 134.8 |
| Median milliseconds to sign a block of them | 164.6 |
| Median milliseconds for the node to accept them | 11,469.8 |
| Seconds for the server to catch up at the end | 2.7 |

## Cache

| | |
|---|---|
| Reloads of an xpub | 520 |
| Of which full, reading the whole xpub | 42 |
| Reloads per second while making transactions | 1.2 |
| Mean milliseconds per reload | 383.8 |
| Longest reload, milliseconds | 5,722 |
| Reads answered from the cache | 517,109 |
| Reads that missed the cache | 4 |
| Reloads given up because Redis knew more | 0 |
| Evictions | 0 |

## Commands that blocked Redis

Redis runs one command or script at a time, so a long one makes every client wait, every server sharing the cache included. These are the commands that took 10 ms or more.

| Command | Count | Total ms | Median ms | Longest ms |
|---|---|---|---|---|
| EVAL | 67 | 797.6 | 11.3 | 20.2 |

## Over time

The run split into equal parts of time. Reload times grow with the number of transactions of the xpubs; the lag is how many blocks the server was behind the node.

| Seconds | Reloads | Mean reload ms | Longest reload ms | Most blocks behind | Server CPU % | Server MiB | Valkey MiB |
|---|---|---|---|---|---|---|---|
| 1–37 | 48 | 514.6 | 4,701 | 1 | 371.4 | 178.4 | 11.4 |
| 38–74 | 48 | 286.4 | 2,198 | 1 | 441.8 | 206.6 | 21.2 |
| 75–112 | 40 | 1,112.7 | 5,722 | 1 | 370.6 | 265.4 | 28.6 |
| 113–148 | 48 | 107.0 | 708 | 1 | 693.4 | 279.2 | 36.2 |
| 153–175 | 36 | 157.6 | 962 | 1 | 609.9 | 288.8 | 41.8 |
| 187–212 | 52 | 394.2 | 2,294 | 1 | 723.1 | 366.7 | 46.0 |
| 228–249 | 40 | 226.9 | 2,139 | 1 | 703.6 | 383.5 | 51.0 |
| 265–299 | 36 | 513.0 | 4,702 | 1 | 491.8 | 391.6 | 56.1 |
| 300–336 | 44 | 523.2 | 4,026 | 1 | 658.7 | 429.1 | 60.7 |
| 337–371 | 40 | 215.3 | 2,721 | 1 | 595.7 | 437.2 | 65.5 |
| 376–411 | 52 | 418.7 | 3,090 | 1 | 393.0 | 512.9 | 72.2 |
| 412–448 | 36 | 120.7 | 722 | 1 | 676.0 | 518.4 | 79.6 |

```
reloads          ▇▇▆▇▅█▆▅▆▆█▅
mean reload ms   ▄▂█▁▁▃▂▄▄▂▃▁
blocks behind    ████████████
server CPU       ▄▅▄▇▆█▇▅▇▆▄▇
```

## Web server under load

Each line sums up the runs of wrk on one endpoint, each rotating over every xpub. Latencies are in milliseconds: the median of the medians of the runs, the worst 99th percentile of a run, and the slowest request.

| Endpoint | Runs | Requests | Requests per second | Median | Worst p99 | Slowest | Not 2xx | Errors |
|---|---|---|---|---|---|---|---|---|
| transactions | 44 | 121,757 | 276.6 | 5.5 | 128.9 | 211.9 | 0 | 0 |
| balances | 44 | 57,713 | 131.1 | 13.6 | 274.8 | 371.3 | 0 | 0 |
| multiaddr | 44 | 41,692 | 94.7 | 17.5 | 165.6 | 309.7 | 0 | 0 |
| summary | 44 | 194,156 | 441.1 | 3.5 | 125.9 | 191.2 | 0 | 0 |
| unspent | 44 | 99,365 | 225.7 | 6.5 | 149.8 | 272.8 | 0 | 0 |

The 99th percentile of each run, in order:

```
transactions   ▂▃▄▃▂▃▄▂▂▂▂▂▂▃▂▂▃▂▃▃▂▄▂▃▃▂▃▂▂▃▃▂▃▂▂▃▂▂▂█▃▂▃▂  (up to 128.9 ms)
balances       ▁▂▂▁▁▂▃▁▁▁▁▂▂▃▁▁▁▁▃▂▁▂▁▂▂▂▃▁▁▂▁▁▂▁▂▂▁▁▂█▃▁▁▁  (up to 274.8 ms)
multiaddr      ▅▄▄▄▃▃▄▃▄▃▃▃▄▄▃▃▃▅▄▅▃▃▃▄▄▄▄▃▃▅▃▃▃▃▃▄▃▃▄█▄▃▃▃  (up to 165.6 ms)
summary        ▂▂▃▂▂▂▃▂▂▂▂▂▂▃▂▂▂▂▂▃▂▂▂▃▃▂▃▂▂▂▂▂▂▂▃▃▂▂▂█▃▂▂▂  (up to 125.9 ms)
unspent        ▂▃▄▂▂▃▄▂▂▃▂▂▂▄▂▂▂▃▃▃▂▂▂▅▃▂▃▂▂▃▂▂▂▂▂▃▂▂▃█▃▂▃▂  (up to 149.8 ms)
```

## Cached answers

All 16 answers checked at the end (summary, balances, first page of transactions and of unspent outputs of every xpub) were the same with and without the cache.

The server logged 0 errors (apart from health checks while it started); see `server.log`.
