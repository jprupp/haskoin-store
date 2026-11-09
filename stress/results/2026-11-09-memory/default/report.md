# Stress test of the Redis cache

60,000 transactions between 4 wallets of 200 receiving and 200 change addresses, 2,000 per block, in 399.6 seconds, while wrk asked the server about their xpubs on five endpoints with 2 connections each.

## Transactions

| | |
|---|---|
| Sent | 60,000 |
| Refused by the node | 0 |
| Blocks | 30 |
| Seconds to make them | 383.5 |
| Transactions per second | 156.5 |
| Median milliseconds to sign a block of them | 165.2 |
| Median milliseconds for the node to accept them | 9,946.6 |
| Seconds for the server to catch up at the end | 2.9 |

## Cache

| | |
|---|---|
| Reloads of an xpub | 432 |
| Of which full, reading the whole xpub | 22 |
| Reloads per second while making transactions | 1.1 |
| Mean milliseconds per reload | 296.7 |
| Longest reload, milliseconds | 4,830 |
| Reads answered from the cache | 452,578 |
| Reads that missed the cache | 4 |
| Reloads given up because Redis knew more | 0 |
| Evictions | 0 |

## Commands that blocked Redis

Redis runs one command or script at a time, so a long one makes every client wait, every server sharing the cache included. These are the commands that took 10 ms or more.

| Command | Count | Total ms | Median ms | Longest ms |
|---|---|---|---|---|
| EVAL | 74 | 868.9 | 11.3 | 20.0 |

## Over time

The run split into equal parts of time. Reload times grow with the number of transactions of the xpubs; the lag is how many blocks the server was behind the node.

| Seconds | Reloads | Mean reload ms | Longest reload ms | Most blocks behind | Server CPU % | Server MiB | GHC heap MiB | Valkey MiB |
|---|---|---|---|---|---|---|---|---|
| 1–32 | 52 | 158.0 | 968 | 1 | 383.6 | 178.8 | 92.9 | 11.2 |
| 33–64 | 40 | 157.9 | 751 | 1 | 396.7 | 201.2 | 93.1 | 19.1 |
| 65–96 | 40 | 224.8 | 1,501 | 1 | 404.8 | 249.4 | 119.9 | 26.2 |
| 97–128 | 36 | 422.9 | 1,827 | 1 | 384.6 | 289.1 | 145.9 | 33.6 |
| 129–161 | 40 | 455.9 | 2,767 | 1 | 441.9 | 335.2 | 174.9 | 41.7 |
| 162–188 | 32 | 759.5 | 4,830 | 1 | 593.0 | 346.8 | 174.9 | 47.9 |
| 194–225 | 28 | 378.9 | 2,355 | 1 | 540.1 | 355.4 | 174.9 | 52.7 |
| 226–257 | 36 | 84.3 | 588 | 1 | 653.1 | 360.7 | 173.5 | 56.1 |
| 258–285 | 36 | 498.0 | 2,227 | 1 | 752.0 | 464.0 | 261.2 | 60.6 |
| 290–316 | 32 | 214.2 | 1,144 | 1 | 519.2 | 470.3 | 261.2 | 65.2 |
| 326–349 | 32 | 108.1 | 530 | 1 | 587.1 | 476.9 | 261.2 | 69.7 |
| 363–386 | 28 | 179.1 | 1,416 | 1 | 385.1 | 487.3 | 261.2 | 77.3 |

```
reloads          █▆▆▅▆▅▄▅▅▅▅▄
mean reload ms   ▂▂▃▄▅█▄▁▅▂▁▂
blocks behind    ████████████
server CPU       ▄▄▄▄▅▆▆▇█▅▆▄
```

## Web server under load

Each line sums up the runs of wrk on one endpoint, each rotating over every xpub. Latencies are in milliseconds: the median of the medians of the runs, the worst 99th percentile of a run, and the slowest request.

| Endpoint | Runs | Requests | Requests per second | Median | Worst p99 | Slowest | Not 2xx | Errors |
|---|---|---|---|---|---|---|---|---|
| unspent | 38 | 87,133 | 229.2 | 6.6 | 83.7 | 195.4 | 0 | 0 |
| balances | 38 | 50,765 | 133.5 | 13.5 | 117.2 | 209.3 | 0 | 0 |
| multiaddr | 38 | 36,829 | 96.9 | 17.3 | 122.2 | 231.2 | 0 | 0 |
| summary | 38 | 173,626 | 456.7 | 3.4 | 47.7 | 183.4 | 0 | 0 |
| transactions | 38 | 104,079 | 273.7 | 5.6 | 65.5 | 205.1 | 0 | 0 |

The 99th percentile of each run, in order:

```
unspent        ▃▅▃█▃▃▃▄▄▄▃▅▅▃▅▃▃▄▃▃▃▄▃▃▃▃▅▄▃▃▃▃▄▃▃▃▄▅  (up to 83.7 ms)
balances       ▃▅▂█▂▃▂▃▃▄▃▄▅▂▃▃▃▄▂▂▂▅▂▃▂▂▃▃▂▃▃▂▃▃▂▂▃▅  (up to 117.2 ms)
multiaddr      ▆▅▄▇▄▄▄▅▄▄▅▆▅▄▅▄▄▆▄▄▅▄▄▅▄▄█▅▄▄▄▄▄▄▄▄▄▅  (up to 122.2 ms)
summary        ▄▇▄█▄▄▃▅▄▅▄▇▇▄▄▃▅▆▄▃▄▅▃▄▃▄▅▅▃▄▄▃▄▄▃▃▄▆  (up to 47.7 ms)
transactions   ▄▆▅█▄▆▄▅▄▅▄▇▆▄▅▃▄▅▄▄▄▅▄▄▃▃▇▅▃▄▄▃▄▇▃▄▄▅  (up to 65.5 ms)
```

## Cached answers

All 16 answers checked at the end (summary, balances, first page of transactions and of unspent outputs of every xpub) were the same with and without the cache.

The server logged 0 errors (apart from health checks while it started); see `server.log`.
