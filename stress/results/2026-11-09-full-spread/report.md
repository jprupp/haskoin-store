# Stress test of the Redis cache

200,000 transactions between 4 wallets of 200 receiving and 200 change addresses, 2,000 per block, in 2,020.4 seconds, while wrk asked the server about their xpubs on five endpoints with 2 connections each.

## Transactions

| | |
|---|---|
| Sent | 200,000 |
| Refused by the node | 0 |
| Blocks | 100 |
| Seconds to make them | 2,005.0 |
| Transactions per second | 99.8 |
| Median milliseconds to sign a block of them | 163.2 |
| Median milliseconds for the node to accept them | 18,080.2 |
| Seconds for the server to catch up at the end | 1.1 |

## Cache

| | |
|---|---|
| Reloads of an xpub | 2,164 |
| Of which full, reading the whole xpub | 18 |
| Reloads per second while making transactions | 1.1 |
| Mean milliseconds per reload | 332.2 |
| Longest reload, milliseconds | 11,873 |
| Reads answered from the cache | 2,416,571 |
| Reads that missed the cache | 4 |
| Reloads given up because Redis knew more | 0 |
| Evictions | 0 |

## Commands that blocked Redis

Redis runs one command or script at a time, so a long one makes every client wait, every server sharing the cache included. These are the commands that took 10 ms or more.

| Command | Count | Total ms | Median ms | Longest ms | Median arguments | Most arguments |
|---|---|---|---|---|---|---|
| commitScript (incremental) | 135 | 1,548.3 | 10.9 | 20.1 | 7,324 | 8,105 |

## Over time

The run split into equal parts of time. Reload times grow with the number of transactions of the xpubs; the lag is how many blocks the server was behind the node.

| Seconds | Reloads | Mean reload ms | Longest reload ms | Most blocks behind | Server CPU % | Server MiB | GHC heap MiB | Valkey MiB |
|---|---|---|---|---|---|---|---|---|
| 1–165 | 211 | 699.2 | 5,489 | 1 | 411.1 | 250.3 | 97.3 | 43.4 |
| 174–333 | 179 | 349.6 | 5,427 | 1 | 558.7 | 279.0 | 91.9 | 65.1 |
| 336–488 | 192 | 217.9 | 2,641 | 1 | 620.8 | 349.1 | 112.7 | 87.3 |
| 503–669 | 188 | 328.2 | 3,865 | 1 | 589.9 | 352.8 | 111.5 | 111.5 |
| 672–832 | 192 | 308.0 | 6,181 | 1 | 619.6 | 416.8 | 115.5 | 129.4 |
| 841–1,003 | 180 | 388.4 | 4,665 | 1 | 563.4 | 485.8 | 170.4 | 151.0 |
| 1,007–1,169 | 183 | 119.5 | 3,403 | 1 | 600.3 | 425.1 | 99.9 | 166.7 |
| 1,171–1,330 | 178 | 312.3 | 4,431 | 1 | 548.8 | 480.7 | 125.9 | 179.4 |
| 1,343–1,496 | 156 | 453.5 | 8,345 | 1 | 666.3 | 545.6 | 159.1 | 195.3 |
| 1,506–1,665 | 171 | 311.5 | 11,873 | 1 | 577.9 | 643.8 | 236.9 | 213.5 |
| 1,677–1,838 | 180 | 161.2 | 2,965 | 1 | 587.3 | 528.0 | 88.9 | 226.8 |
| 1,842–2,007 | 154 | 296.1 | 4,630 | 1 | 612.1 | 693.4 | 250.1 | 241.6 |

```
reloads          █▆▇▇▇▆▇▆▆▆▆▆
mean reload ms   █▄▃▄▄▄▂▄▅▄▂▃
blocks behind    ████████████
server CPU       ▅▆▇▇▇▆▇▆█▇▇▇
```

## Web server under load

Each line sums up the runs of wrk on one endpoint, each rotating over every xpub. Latencies are in milliseconds: the median of the medians of the runs, the worst 99th percentile of a run, and the slowest request.

| Endpoint | Runs | Requests | Requests per second | Median | Worst p99 | Slowest | Not 2xx | Errors |
|---|---|---|---|---|---|---|---|---|
| unspent | 197 | 467,267 | 237.1 | 6.4 | 72.9 | 169.8 | 0 | 0 |
| summary | 197 | 915,893 | 464.7 | 3.3 | 52.0 | 169.1 | 0 | 0 |
| multiaddr | 196 | 198,665 | 101.3 | 16.5 | 99.8 | 246.0 | 0 | 0 |
| balances | 196 | 268,922 | 137.1 | 13.2 | 105.6 | 196.4 | 0 | 0 |
| transactions | 196 | 556,658 | 283.8 | 5.4 | 88.2 | 199.2 | 0 | 0 |

The 99th percentile of each run, in order:

```
unspent        ▄▄█▄▅▅▄▃▃▃▃▅▃▄▇▃▄▆▃▃▃▃▃▃▃▃▃▆▃▃▅▃▃▄▄▃▃▅▃▄▃▃▃▃▃▃▄▅▃▄▄▃▃▄▄▅▃▅▃▃▄▃▄▃▅▃▃▅▄▃▆▃▃▃▄▃▃▄▃▃▄▃▃▃▃▃▆▃▃▃▃▄▃▃▄▃▄▃▃▄▄▃▄▃▃▄▃▄▃▄▃▅▃▃▃▃▃▃▃▆▃▄▃▃▃▄▃▃▃▃▅▃▅▃▃▃▃▃▃▃▄▃▃▃▄▃▄▃▃▅▃▄▃▃▃▅▃▃▃▅▄▄▄▄▃▃▅▃▃▃▃▄▃▃▄▃▃▃▃▄▃▃▃▃▃▃▅▄▅▃▃▅▃▃▃▃▃  (up to 72.9 ms)
summary        ▃▃█▃▄▅▃▃▃▄▃▄▄▄▅▃▄▃▃▄▃▃▃▃▃▃▃▅▃▃▅▆▃▄▄▃▃▅▄▄▄▃▄▄▃▃▄▅▃▅▃▃▃▄▄▅▃▅▃▃▄▃▄▃▅▃▃▅▃▃▆▃▃▃▄▃▃▄▄▄▃▃▃▃▃▃▅▃▄▃▄▄▃▃▃▃▄▃▃▃▄▃▃▃▃▄▃▄▃▄▃▄▃▃▃▃▃▄▃▇▃▃▃▃▃▄▃▃▃▃▅▃▄▃▃▃▃▃▃▃▅▃▃▃▄▃▃▃▃▅▄▃▃▃▃▅▃▃▃▇▃▄▄▄▃▃▅▃▃▄▃▅▃▃▄▃▃▃▃▅▃▃▃▃▃▄▅▄▃▃▃▆▃▃▃▃▃  (up to 52.0 ms)
multiaddr      ▆▅▇▄▆▆▅▅▄▆▅▄▅▅▆▅▅▅▄▅▅▅▄▅▅▄▅▆▄▅▆▄▄▄▅▄▄▆▆▄▅▅▄▅▄▄▆▆▄▅▅▅▄▅▄▆▄▅▄▄▆▄▅▆▄▄▅▄▄▅▆▄▅▄▆▄▄▅▅▅▄▄▄▅▄▄▇▅▄▄▅▅▅▆▄▄▆▄▄▅▄▄▅▅▄▅▅▅▄▅▄▅▄▄▄▅▄▅▅▅▄▅▄▄▄▅▄▅▄▅▅▄▅▄▄▅▄▄▅▄▇▅▅▄▅▄▅▄▅▄▅▄▅▄▄▅▄▄▄█▅▆▄▅▄▄▆▄▄▅▅▆▄▅▄▄▅▄▆▅▄▄▄▄▅▄▅▅▆▅▇▄▄▄▄▄  (up to 99.8 ms)
balances       ▃▃█▃▃▅▃▃▃▃▂▃▂▂▃▂▃▃▃▂▃▃▂▃▃▂▂▅▃▂▅▃▂▃▃▂▂▅▃▃▃▂▃▃▃▂▄▅▂▄▃▃▂▃▃▇▃▅▂▃▄▂▃▅▂▂▅▂▂▃▆▃▃▂▃▂▂▄▃▃▃▂▂▃▂▂▅▃▂▂▃▃▃▃▂▃▅▂▃▃▄▃▃▃▂▃▂▃▃▃▃▄▂▂▂▃▂▃▃▄▃▃▂▂▃▂▃▃▂▂▅▂▄▂▃▂▂▃▂▃▃▂▃▃▃▂▃▂▅▃▃▃▂▂▂▅▂▃▂▆▃▃▂▄▂▂▅▂▂▂▂▄▃▃▃▂▂▃▅▂▂▂▂▂▃▂▅▄▃▂▆▂▂▃▂▂  (up to 105.6 ms)
transactions   ▃▃▆▃▄▄▃▃▃▄▃▅▃▃▄▃▃▃▃▃▃▃▃▄▃▃▃▄▃▃▄▃▃▃▃▃▃▄▃▃▃▃▃▃▃▃▃▅▃▄▃▃▃▃▃▄▃▄▃▃▄▃▃▄▃▃▄▃▃▃▄▃▃▃▄▃▃▄▃▃▃▃▃▃▃▃▄▃▃▃▃▃▃▃▃▃█▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▃▄▃▃▃▃▃▃▃▃▃▃▃▄▃▄▃▃▃▃▃▃▃▄▃▃▃▃▃▃▃▄▃▃▃▃▃▃▃▃▃▃▅▃▃▃▃▃▃▄▃▃▃▃▄▃▃▃▃▃▂▄▃▃▃▃▃▃▃▄▃▃▃▃▃▃▃▃▃  (up to 88.2 ms)
```

## Cached answers

All 16 answers checked at the end (summary, balances, first page of transactions and of unspent outputs of every xpub) were the same with and without the cache.

The server logged 0 errors (apart from health checks while it started); see `server.log`.
