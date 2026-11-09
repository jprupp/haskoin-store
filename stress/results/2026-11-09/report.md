# Stress test of the Redis cache

200,000 transactions between 4 wallets of 200 receiving and 200 change addresses, 2,000 per block, in 2,166.9 seconds, while wrk asked the server about their xpubs on five endpoints with 2 connections each.

## Transactions

| | |
|---|---|
| Sent | 200,000 |
| Refused by the node | 0 |
| Blocks | 100 |
| Seconds to make them | 2,152.4 |
| Transactions per second | 92.9 |
| Median milliseconds to sign a block of them | 162.1 |
| Median milliseconds for the node to accept them | 21,343.8 |
| Seconds for the server to catch up at the end | 1.2 |

## Cache

| | |
|---|---|
| Reloads of an xpub | 2,030 |
| Reloads per second while making transactions | 0.9 |
| Mean milliseconds per reload | 2,447.7 |
| Longest reload, milliseconds | 8,640 |
| Reads answered from the cache | 1,741,297 |
| Reads that missed the cache | 4 |
| Reloads given up because Redis knew more | 0 |
| Evictions | 0 |

## Commands that blocked Redis

Redis runs one command or script at a time, so a long one makes every client wait, every server sharing the cache included. These are the commands that took 10 ms or more.

| Command | Count | Total ms | Median ms | Longest ms |
|---|---|---|---|---|
| EVAL | 32 | 382.5 | 10.9 | 18.0 |

## Over time

The run split into equal parts of time. Reload times grow with the number of transactions of the xpubs; the lag is how many blocks the server was behind the node.

| Seconds | Reloads | Mean reload ms | Longest reload ms | Most blocks behind | Server CPU % | Server MiB | Valkey MiB |
|---|---|---|---|---|---|---|---|
| 1–179 | 246 | 587.3 | 2,391 | 1 | 467.6 | 367.1 | 36.2 |
| 180–358 | 188 | 1,228.4 | 3,673 | 1 | 649.9 | 505.9 | 56.5 |
| 359–538 | 192 | 1,527.3 | 4,549 | 1 | 673.6 | 621.1 | 75.9 |
| 539–718 | 172 | 1,754.5 | 4,467 | 1 | 569.0 | 712.3 | 94.3 |
| 719–897 | 172 | 2,253.1 | 4,836 | 1 | 577.1 | 811.5 | 107.8 |
| 898–1,077 | 160 | 2,740.8 | 6,866 | 1 | 585.5 | 864.8 | 125.1 |
| 1,078–1,254 | 152 | 2,972.2 | 7,349 | 1 | 616.8 | 941.6 | 140.0 |
| 1,257–1,436 | 160 | 3,025.6 | 7,419 | 1 | 563.9 | 1,019.7 | 156.4 |
| 1,440–1,613 | 160 | 2,922.9 | 7,703 | 1 | 615.3 | 1,118.5 | 172.1 |
| 1,627–1,787 | 140 | 3,771.5 | 8,095 | 1 | 631.0 | 1,209.9 | 187.5 |
| 1,797–1,970 | 156 | 3,915.3 | 7,621 | 1 | 491.1 | 1,262.3 | 202.6 |
| 1,975–2,154 | 132 | 4,666.2 | 8,640 | 1 | 472.2 | 1,338.5 | 213.6 |

```
reloads          █▆▆▅▅▅▅▅▅▄▅▄
mean reload ms   ▁▂▃▃▄▅▅▅▅▆▆█
blocks behind    ████████████
server CPU       ▅▇█▆▆▇▇▆▇▇▆▅
```

## Web server under load

Each line sums up the runs of wrk on one endpoint, each rotating over every xpub. Latencies are in milliseconds: the median of the medians of the runs, the worst 99th percentile of a run, and the slowest request.

| Endpoint | Runs | Requests | Requests per second | Median | Worst p99 | Slowest | Not 2xx | Errors |
|---|---|---|---|---|---|---|---|---|
| multiaddr | 211 | 146,813 | 69.5 | 20.6 | 438.3 | 527.6 | 0 | 0 |
| unspent | 211 | 333,727 | 158.1 | 9.6 | 386.1 | 494.7 | 0 | 0 |
| summary | 212 | 653,975 | 308.3 | 4.8 | 359.5 | 506.2 | 0 | 0 |
| transactions | 211 | 399,312 | 189.1 | 8.3 | 397.2 | 530.0 | 0 | 0 |
| balances | 211 | 203,009 | 96.2 | 15.3 | 477.1 | 607.0 | 0 | 0 |

The 99th percentile of each run, in order:

```
multiaddr      ▂▂▂▁▂▂▂▂▂▂▂▂▂▂▂▂▃▂▂▂▂▂▂▂▂▂▂▂▃▃▃▂▃▂▄▂▂▃▃▃▂▃▃▃▃▃▂▃▄▄▃▃▄▃▃▄▅▃▃▄▄▄▃▄▂▄▅▅▄▃▄▅▄▃▃▄▃▃▄▄▃▄▄▄▅▄▄▄▃▄▅▃▅▂▄▂▃▄▅▃▂▄▂▄▆▅▃▄▃▄▄▂▄▂▄▄▆▅▄▃▅▄▄▆▄▃▄▄▃▆▅▅▃▃▅▄▆▄▅▅▆▆▆▅▅▆▅▆▆▅▅▇▆▅▇▅▅▅▅▂▅▆▂▅▄▆▆▅▆▇▅▆▅▆▅▆▅▅▆▅▆▆▄▆▆▅▅▇▄▆▅▅▃▆▆█▅▂▆▅▂▄▇▃▆▅▆▄▄▄▇  (up to 438.3 ms)
unspent        ▁▁▁▁▁▂▂▁▁▂▂▂▁▂▁▁▂▂▁▂▁▁▂▂▂▁▂▂▃▃▂▂▃▂▄▂▂▃▃▃▁▃▃▃▃▂▂▂▄▃▃▃▄▃▂▄▄▃▂▄▄▄▃▄▃▄▄▄▄▂▄▄▄▄▃▃▃▂▅▄▃▄▅▄▃▃▄▅▃▅▅▃▄▂▄▂▃▄▄▄▂▄▂▄▅▅▂▅▂▄▅▂▄▁▄▃▆▄▄▂▅▄▄▅▄▃▄▄▃▄▄▅▂▂▆▅▆▅▄▅▅▅▆▅▆▆▅▅▆▆▅▆▆▆▇▆▅▅▅▂▅▆▂▅▄▆▇▅▆▆▄▅▅▆▅▆▅▅▇▆▆▆▄▇▆▅▅▅▅▆▆▅▂▆▆█▅▂▆▅▂▅▆▂▅▇▅▅▅▄▆  (up to 386.1 ms)
summary        ▁▁▁▁▁▂▁▁▁▂▁▁▁▂▁▁▂▂▁▁▁▁▂▁▂▁▁▂▂▃▁▁▃▁▃▂▂▂▃▃▃▃▃▂▃▂▂▂▃▃▂▂▄▃▂▂▅▂▁▄▃▃▂▄▃▃▃▃▃▃▃▃▄▃▂▃▃▂▅▄▃▃▄▄▃▄▃▅▃▄▄▃▄▁▄▁▃▃▃▄▃▄▂▃▄▄▂▄▁▃▄▁▄▁▃▃▅▄▄▂▅▅▃▄▄▂▄▄▃▄▄▅▃▁▅▄▆▃▅▅▅▅▆▃▅▅▅▅▅▆▄▄▆▅▆▆▅▅▅▁▄▆▂▅▂▆▇▅▆▇▄▅▅▆▅▇▄▅▆▆▅▆▄▄▆▂▅▅▆▇▅█▂▆▆▇▅▂▆▆▂▅▅▂▃▆▅▆▅▂▆▅  (up to 359.5 ms)
transactions   ▁▁▁▁▂▁▁▁▁▂▁▁▁▂▁▁▂▂▁▁▁▁▂▁▂▁▂▁▃▂▁▂▂▂▃▂▂▂▃▃▁▃▃▃▃▂▂▂▄▂▂▂▄▃▂▃▃▂▁▃▃▄▂▄▃▃▃▃▃▂▃▃▃▃▂▃▃▂▄▄▃▃▄▃▃▃▃▄▃▄▄▃▄▁▃▁▃▃▄▃▂▄▂▃▄▄▂▄▂▃▃▂▄▁▃▃▅▄▄▂▄▄▄▄▄▂▄▄▃▄▄▄▂▁▄▅▅▅▃▄▅▅▅▄▅▅▄▅▅▆▅▅▅▅▅▆▅▅▅▂▅▆▂▅▃▆▆▅▆█▄▅▅▅▄▅▄▄▆▅▅▆▄▆▆▂▄▄▄▆▄▆▂▆▆▇▅▂▅▅▂▄▅▂▅▆▄▅▄▃▆  (up to 397.2 ms)
balances       ▁▁▁▁▂▂▂▁▂▂▂▁▁▂▁▁▃▁▁▂▁▂▁▁▂▁▃▁▃▄▁▂▃▂▄▂▂▃▄▄▁▄▄▃▃▃▂▂▅▃▃▃▄▄▂▄▄▃▃▄▅▄▃▄▁▄▄▅▅▁▄▅▄▄▃▃▃▂▅▅▄▄▄▅▄▄▄▅▄▅▄▃▅▁▅▂▄▅▅▄▁▄▂▄▅▆▂▅▂▄▄▂▅▁▄▄▆▅▄▁▆▅▅▅▅▃▄▄▅▅▅▅▂▁▆▅▆▅▄▅▅▆▆▅▇▆▆▆▆▆▆▆▆▆▆▇▅▆▆▁▄▆▂▅▅▇█▆▅▇▅▅▅▆▆▅▆▅▆▅▅▆▄▅▆▅▅▆▄▇▇▅▂▆▅▇▅▂▆▅▂▅▇▁▅▆▆▄▅▄▆  (up to 477.1 ms)
```

## Cached answers

All 16 answers checked at the end (summary, balances, first page of transactions and of unspent outputs of every xpub) were the same with and without the cache.

The server logged 0 errors (apart from health checks while it started); see `server.log`.
