# Stress test of the Redis cache

60,000 transactions between 4 wallets of 200 receiving and 200 change addresses, 2,000 per block, in 390.5 seconds, while wrk asked the server about their xpubs on five endpoints with 2 connections each.

## Transactions

| | |
|---|---|
| Sent | 60,000 |
| Refused by the node | 0 |
| Blocks | 30 |
| Seconds to make them | 376.2 |
| Transactions per second | 159.5 |
| Median milliseconds to sign a block of them | 163.0 |
| Median milliseconds for the node to accept them | 10,001.8 |
| Seconds for the server to catch up at the end | 1.1 |

## Cache

| | |
|---|---|
| Reloads of an xpub | 460 |
| Of which full, reading the whole xpub | 48 |
| Reloads per second while making transactions | 1.2 |
| Mean milliseconds per reload | 387.1 |
| Longest reload, milliseconds | 3,761 |
| Reads answered from the cache | 425,052 |
| Reads that missed the cache | 4 |
| Reloads given up because Redis knew more | 0 |
| Evictions | 0 |

## Commands that blocked Redis

Redis runs one command or script at a time, so a long one makes every client wait, every server sharing the cache included. These are the commands that took 10 ms or more.

| Command | Count | Total ms | Median ms | Longest ms |
|---|---|---|---|---|
| EVAL | 84 | 968.7 | 11.1 | 20.7 |

## Over time

The run split into equal parts of time. Reload times grow with the number of transactions of the xpubs; the lag is how many blocks the server was behind the node.

| Seconds | Reloads | Mean reload ms | Longest reload ms | Most blocks behind | Server CPU % | Server MiB | GHC heap MiB | Valkey MiB |
|---|---|---|---|---|---|---|---|---|
| 1–31 | 40 | 318.3 | 3,306 | 1 | 364.5 | 164.4 | 82.9 | 11.5 |
| 32–62 | 44 | 660.2 | 3,761 | 1 | 369.3 | 206.1 | 95.9 | 18.9 |
| 63–94 | 48 | 271.0 | 1,366 | 1 | 395.7 | 227.3 | 103.3 | 26.3 |
| 95–125 | 36 | 673.2 | 3,001 | 1 | 444.2 | 235.4 | 84.7 | 33.4 |
| 126–157 | 40 | 149.4 | 533 | 1 | 455.7 | 252.5 | 85.9 | 41.6 |
| 158–189 | 36 | 426.5 | 3,752 | 1 | 501.7 | 267.4 | 86.9 | 46.0 |
| 190–220 | 32 | 778.1 | 3,473 | 1 | 390.7 | 278.6 | 89.9 | 52.6 |
| 221–249 | 44 | 288.7 | 1,440 | 1 | 847.8 | 430.7 | 231.3 | 58.2 |
| 252–283 | 24 | 95.8 | 440 | 1 | 652.8 | 256.7 | 50.9 | 60.6 |
| 284–300 | 40 | 76.3 | 367 | 1 | 712.8 | 298.7 | 85.9 | 65.2 |
| 316–335 | 48 | 600.1 | 3,556 | 1 | 621.2 | 460.9 | 240.0 | 69.8 |
| 352–377 | 28 | 209.8 | 1,024 | 1 | 381.6 | 310.2 | 71.9 | 76.8 |

```
reloads          ▆▇█▆▆▆▅▇▄▆█▅
mean reload ms   ▃▆▃▇▂▄█▃▁▁▆▂
blocks behind    ████████████
server CPU       ▄▄▄▄▄▅▄█▆▆▆▄
```

## Web server under load

Each line sums up the runs of wrk on one endpoint, each rotating over every xpub. Latencies are in milliseconds: the median of the medians of the runs, the worst 99th percentile of a run, and the slowest request.

| Endpoint | Runs | Requests | Requests per second | Median | Worst p99 | Slowest | Not 2xx | Errors |
|---|---|---|---|---|---|---|---|---|
| unspent | 36 | 80,271 | 222.9 | 6.8 | 128.1 | 230.8 | 0 | 0 |
| summary | 36 | 156,546 | 434.5 | 3.6 | 130.8 | 260.7 | 0 | 0 |
| multiaddr | 36 | 34,367 | 95.4 | 17.6 | 180.7 | 306.0 | 0 | 0 |
| balances | 36 | 47,655 | 132.3 | 13.5 | 173.8 | 317.7 | 0 | 0 |
| transactions | 36 | 97,593 | 270.9 | 5.6 | 107.1 | 214.2 | 0 | 0 |

The 99th percentile of each run, in order:

```
unspent        ▂▂▃▂▃▂▄▂▂▂▃▃▄▃▅▂▂▂▂▂▃▂▅▂█▂▂▂▅▃▂▂▃▇▂▂  (up to 128.1 ms)
summary        ▂▂▂▂▃▂▃▂▂▂▂▂▂▂▄▁▂▂▂▂▂▂▄▁▆▂▂▂▃▃▁▂▂█▂▂  (up to 130.8 ms)
multiaddr      ▄▃▃▃▄▃▅▃▄▃▃▃▄▃▅▃▃▃▃▃▃▃▅▃█▃▃▃▄▃▃▃▄▇▃▃  (up to 180.7 ms)
balances       ▂▂▂▂▄▂▃▂▂▂▂▃▄▂▅▂▂▂▂▂▃▂▅▂█▂▂▂▃▂▂▂▂▇▂▂  (up to 173.8 ms)
transactions   ▂▃▃▃▄▂▄▃▃▃▂▄▄▃▅▂▂▂▂▃▃▂▆▂█▂▂▂▅▅▂▂▃▆▂▂  (up to 107.1 ms)
```

## Cached answers

All 16 answers checked at the end (summary, balances, first page of transactions and of unspent outputs of every xpub) were the same with and without the cache.

The server logged 0 errors (apart from health checks while it started); see `server.log`.
