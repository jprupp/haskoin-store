# Stress test of the Redis cache

60,000 transactions between 4 wallets of 200 receiving and 200 change addresses, 2,000 per block, in 381.6 seconds, while wrk asked the server about their xpubs on five endpoints with 2 connections each.

## Transactions

| | |
|---|---|
| Sent | 60,000 |
| Refused by the node | 0 |
| Blocks | 30 |
| Seconds to make them | 365.2 |
| Transactions per second | 164.3 |
| Median milliseconds to sign a block of them | 164.1 |
| Median milliseconds for the node to accept them | 10,022.2 |
| Seconds for the server to catch up at the end | 3.1 |

## Cache

| | |
|---|---|
| Reloads of an xpub | 416 |
| Of which full, reading the whole xpub | 4 |
| Reloads per second while making transactions | 1.1 |
| Mean milliseconds per reload | 386.0 |
| Longest reload, milliseconds | 4,831 |
| Reads answered from the cache | 427,105 |
| Reads that missed the cache | 4 |
| Reloads given up because Redis knew more | 0 |
| Evictions | 0 |

## Commands that blocked Redis

Redis runs one command or script at a time, so a long one makes every client wait, every server sharing the cache included. These are the commands that took 10 ms or more.

| Command | Count | Total ms | Median ms | Longest ms | Median arguments | Most arguments |
|---|---|---|---|---|---|---|
| commitScript (incremental) | 81 | 935.8 | 11.1 | 19.3 | 7,950 | 8,236 |

## Over time

The run split into equal parts of time. Reload times grow with the number of transactions of the xpubs; the lag is how many blocks the server was behind the node.

| Seconds | Reloads | Mean reload ms | Longest reload ms | Most blocks behind | Server CPU % | Server MiB | GHC heap MiB | Valkey MiB |
|---|---|---|---|---|---|---|---|---|
| 1–30 | 40 | 105.7 | 387 | 1 | 389.9 | 168.4 | 87.8 | 11.5 |
| 31–61 | 36 | 385.4 | 1,927 | 1 | 388.6 | 192.0 | 86.8 | 19.2 |
| 62–90 | 36 | 629.3 | 4,506 | 1 | 396.1 | 206.5 | 92.0 | 26.0 |
| 93–122 | 40 | 327.8 | 2,654 | 1 | 411.4 | 226.0 | 87.9 | 33.6 |
| 123–151 | 32 | 528.3 | 1,975 | 1 | 398.6 | 237.1 | 85.8 | 41.5 |
| 154–183 | 48 | 104.5 | 674 | 1 | 390.8 | 254.6 | 91.5 | 48.0 |
| 185–215 | 32 | 206.0 | 1,317 | 1 | 431.3 | 266.4 | 89.8 | 52.7 |
| 216–245 | 28 | 625.0 | 3,929 | 1 | 597.3 | 264.9 | 82.9 | 58.1 |
| 246–276 | 24 | 1,195.6 | 4,831 | 1 | 749.4 | 282.4 | 90.9 | 62.4 |
| 277–306 | 40 | 355.7 | 3,124 | 0 | 737.0 | 281.1 | 80.3 | 67.3 |
| 310–330 | 40 | 373.6 | 3,340 | 1 | 638.6 | 277.3 | 76.1 | 72.0 |
| 346–368 | 20 | 141.8 | 589 | 1 | 378.5 | 293.9 | 84.8 | 77.2 |

```
reloads          ▆▆▆▆▅█▅▅▄▆▆▃
mean reload ms   ▁▃▄▂▄▁▂▄█▃▃▁
blocks behind    █████████▁██
server CPU       ▄▄▄▄▄▄▅▆█▇▆▄
```

## Web server under load

Each line sums up the runs of wrk on one endpoint, each rotating over every xpub. Latencies are in milliseconds: the median of the medians of the runs, the worst 99th percentile of a run, and the slowest request.

| Endpoint | Runs | Requests | Requests per second | Median | Worst p99 | Slowest | Not 2xx | Errors |
|---|---|---|---|---|---|---|---|---|
| unspent | 36 | 81,827 | 227.1 | 6.6 | 62.3 | 186.1 | 0 | 0 |
| transactions | 36 | 99,583 | 276.5 | 5.5 | 59.2 | 175.5 | 0 | 0 |
| summary | 36 | 160,152 | 444.5 | 3.4 | 42.2 | 146.9 | 0 | 0 |
| multiaddr | 36 | 34,810 | 96.6 | 17.4 | 149.3 | 359.5 | 0 | 0 |
| balances | 36 | 47,818 | 132.8 | 13.6 | 83.3 | 191.5 | 0 | 0 |

The 99th percentile of each run, in order:

```
unspent        ▄▅▅▄▅▅▄▆▆▅▅▄▄▄▅▆▆▄▆▅▄▅▄▄▄▄▄▅▄▄▄█▄▄▄▄  (up to 62.3 ms)
transactions   ▄▅▇▄▅▅▄▅▆▆▄▄▆▄▅▆▅▄▆▆▄▅▄▅▄▄▄▄▄▄▄█▄▄▄▄  (up to 59.2 ms)
summary        ▄▄▆▄▅▅▄▅▆▆▅▄▄▄▅▆▆▄▆▅▄▅▄▄▄▄▄▄▃▄▄█▄▄▄▄  (up to 42.2 ms)
multiaddr      ▅▄▄▃▃▄▃▄▅▄▄▃▄▄▄▅▄█▄▄▃▄▃▄▃▃▃▃▃▃▃▄▃▃▃▃  (up to 149.3 ms)
balances       ▄▇▄▄▄▅▃▆▆▇▄▄▃▃▄▅▆▃▆▃▃▄▃▃▄▃▃▄▃▃▃█▄▃▃▅  (up to 83.3 ms)
```

## Cached answers

All 16 answers checked at the end (summary, balances, first page of transactions and of unspent outputs of every xpub) were the same with and without the cache.

The server logged 0 errors (apart from health checks while it started); see `server.log`.
