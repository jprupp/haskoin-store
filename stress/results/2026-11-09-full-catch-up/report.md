# Stress test of the Redis cache

200,000 transactions between 4 wallets of 200 receiving and 200 change addresses, 2,000 per block, in 1,998.7 seconds, while wrk asked the server about their xpubs on five endpoints with 2 connections each.

## Transactions

| | |
|---|---|
| Sent | 200,000 |
| Refused by the node | 0 |
| Blocks | 100 |
| Seconds to make them | 1,981.2 |
| Transactions per second | 100.9 |
| Median milliseconds to sign a block of them | 161.6 |
| Median milliseconds for the node to accept them | 16,948.5 |
| Seconds for the server to catch up at the end | 4.2 |

## Cache

| | |
|---|---|
| Reloads of an xpub | 2,098 |
| Of which full, reading the whole xpub | 16 |
| Reloads per second while making transactions | 1.1 |
| Mean milliseconds per reload | 304.7 |
| Longest reload, milliseconds | 5,628 |
| Reads answered from the cache | 2,377,364 |
| Reads that missed the cache | 4 |
| Reloads given up because Redis knew more | 0 |
| Evictions | 0 |

## Commands that blocked Redis

Redis runs one command or script at a time, so a long one makes every client wait, every server sharing the cache included. These are the commands that took 10 ms or more.

| Command | Count | Total ms | Median ms | Longest ms | Median arguments | Most arguments |
|---|---|---|---|---|---|---|
| commitScript (incremental) | 129 | 1,482.9 | 11.0 | 18.5 | 7,378 | 8,096 |
| commitScript (full reload) | 1 | 10.2 | 10.2 | 10.2 | 7,332 | 7,332 |

## Over time

The run split into equal parts of time. Reload times grow with the number of transactions of the xpubs; the lag is how many blocks the server was behind the node.

| Seconds | Reloads | Mean reload ms | Longest reload ms | Most blocks behind | Server CPU % | Server MiB | GHC heap MiB | Valkey MiB |
|---|---|---|---|---|---|---|---|---|
| 1–165 | 220 | 258.0 | 2,811 | 1 | 400.1 | 251.0 | 89.9 | 43.4 |
| 166–330 | 188 | 235.7 | 4,075 | 1 | 587.1 | 285.9 | 93.9 | 67.3 |
| 331–496 | 172 | 432.8 | 5,628 | 1 | 674.2 | 341.1 | 95.9 | 91.6 |
| 497–660 | 172 | 350.3 | 3,761 | 1 | 643.6 | 365.6 | 91.9 | 113.6 |
| 663–825 | 168 | 290.1 | 4,994 | 1 | 542.4 | 390.6 | 85.9 | 129.5 |
| 828–985 | 180 | 139.2 | 2,505 | 1 | 641.3 | 399.1 | 84.9 | 149.0 |
| 998–1,158 | 179 | 394.4 | 4,989 | 1 | 627.1 | 423.7 | 88.2 | 166.3 |
| 1,163–1,323 | 144 | 303.7 | 3,481 | 1 | 487.3 | 889.5 | 538.9 | 175.4 |
| 1,324–1,480 | 184 | 319.1 | 3,446 | 1 | 602.1 | 469.1 | 92.9 | 195.0 |
| 1,491–1,654 | 163 | 165.2 | 2,285 | 1 | 509.3 | 481.4 | 84.9 | 211.3 |
| 1,655–1,820 | 160 | 337.2 | 4,157 | 1 | 508.0 | 1,214.7 | 784.9 | 224.3 |
| 1,821–1,986 | 168 | 451.0 | 4,760 | 1 | 583.8 | 530.7 | 92.9 | 239.7 |

```
reloads          █▆▆▆▆▆▆▅▆▆▆▆
mean reload ms   ▅▄▇▆▅▃▇▅▅▃▆█
blocks behind    ████████████
server CPU       ▅▇█▇▆▇▇▆▇▆▆▇
```

## Web server under load

Each line sums up the runs of wrk on one endpoint, each rotating over every xpub. Latencies are in milliseconds: the median of the medians of the runs, the worst 99th percentile of a run, and the slowest request.

| Endpoint | Runs | Requests | Requests per second | Median | Worst p99 | Slowest | Not 2xx | Errors |
|---|---|---|---|---|---|---|---|---|
| transactions | 195 | 553,678 | 283.8 | 5.4 | 244.1 | 384.2 | 0 | 0 |
| summary | 195 | 895,625 | 459.0 | 3.4 | 302.3 | 482.1 | 0 | 0 |
| balances | 195 | 264,133 | 135.4 | 13.3 | 400.6 | 505.7 | 0 | 0 |
| multiaddr | 195 | 196,646 | 100.8 | 16.6 | 378.2 | 497.9 | 0 | 0 |
| unspent | 195 | 459,216 | 235.4 | 6.4 | 367.4 | 493.8 | 0 | 0 |

The 99th percentile of each run, in order:

```
transactions   ▁▂▁▁▂▁▂▁▂▂▂▁▂▂▁▁▂▁▁▁▁▂▂▁▂▁▁▁▁▂▁▂▂▁▁▁▂▁▂▁▂▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▂▄▁▁▁▁▁▁▁▁▁▁▂▁▁▁▂▁▂▁▂▁▁▁▁▁▁▂▁▁▁▁▂▁▁▁▂▁▁▁▁▂▁▁▁▁▁▂▁▂▁▁▁▁▁▁▁▁▁▁▁▆▁▁▂▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▂▁▁▁▁▂▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▂▁▁▁▁▁█▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂  (up to 244.1 ms)
summary        ▁▁▁▁▂▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▂▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▃▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▆▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁█▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁  (up to 302.3 ms)
balances       ▁▁▁▁▂▁▂▁▁▁▁▁▂▂▁▁▂▁▁▁▁▂▂▁▂▁▁▁▁▂▁▂▂▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▂▁▂▁▁▁▁▁▄▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▂▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▂▁▁▁▁▂▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▇▁▁▁▁▁▁▁▂▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁█▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁  (up to 400.6 ms)
multiaddr      ▃▂▂▂▂▂▂▂▂▂▃▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▁▂▂▂▂▂▂▂▁▂▂▃▂▂▂▂▂▂▂▁▂▂▂▂▂▂▄▂▁▂▂▂▂▂▂▂▁▂▂▂▂▂▂▂▂▂▁▂▂▂▂▂▂▂▃▂▂▂▁▂▁▂▂▂▂▂▂▂▂▂▁▂▂▂▂▂▂▂▂▁▂▁▂▂▂▁▇▁▂▂▂▁▂▁▂▂▁▂▂▁▂▂▂▂▂▂▁▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▁▂▂▂▁▂▂▂▂▂▂▂▂▂▂▂▁▂▂▁▂▁▂█▂▂▂▂▂▂▂▂▁▂▁▂▂▁▁▂  (up to 378.2 ms)
unspent        ▁▁▁▁▂▂▂▁▁▁▁▁▁▂▁▁▁▁▁▁▁▂▂▁▂▁▁▁▁▁▁▂▂▁▁▁▁▁▁▁▂▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▃▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▂▁▁▁▁▁▆▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁█▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁  (up to 367.4 ms)
```

## Cached answers

All 16 answers checked at the end (summary, balances, first page of transactions and of unspent outputs of every xpub) were the same with and without the cache.

The server logged 0 errors (apart from health checks while it started); see `server.log`.
