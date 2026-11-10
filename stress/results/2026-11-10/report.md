# Stress test of the Redis cache

200,000 transactions between 4 wallets of 200 receiving and 200 change addresses, 2,000 per block, in 1,958.2 seconds, while wrk asked the server about their xpubs on five endpoints with 2 connections each.

## Transactions

| | |
|---|---|
| Sent | 200,000 |
| Refused by the node | 0 |
| Blocks | 100 |
| Seconds to make them | 1,941.8 |
| Transactions per second | 103.0 |
| Median milliseconds to sign a block of them | 161.7 |
| Median milliseconds for the node to accept them | 16,855.0 |
| Seconds for the server to catch up at the end | 0.1 |

## Cache

| | |
|---|---|
| Reloads of an xpub | 2,117 |
| Of which full, reading the whole xpub | 18 |
| Reloads per second while making transactions | 1.1 |
| Mean milliseconds per reload | 233.9 |
| Longest reload, milliseconds | 4,861 |
| Reads answered from the cache | 2,288,705 |
| Reads that missed the cache | 4 |
| Reloads given up because Redis knew more | 0 |
| Evictions | 0 |

## Commands that blocked Redis

Redis runs one command or script at a time, so a long one makes every client wait, every server sharing the cache included. These are the commands that took 10 ms or more.

| Command | Count | Total ms | Median ms | Longest ms | Median arguments | Most arguments |
|---|---|---|---|---|---|---|
| commitScript (incremental) | 159 | 1,897.7 | 11.1 | 26.9 | 7,423 | 8,084 |
| commitScript (full reload) | 1 | 10.9 | 10.9 | 10.9 | 7,384 | 7,384 |

## Over time

The run split into equal parts of time. Reload times grow with the number of transactions of the xpubs; the lag is how many blocks the server was behind the node.

| Seconds | Reloads | Mean reload ms | Longest reload ms | Most blocks behind | Server CPU % | Server MiB | GHC heap MiB | Valkey MiB |
|---|---|---|---|---|---|---|---|---|
| 1–162 | 214 | 303.6 | 2,725 | 1 | 400.1 | 256.6 | 99.9 | 41.8 |
| 163–320 | 200 | 283.4 | 4,326 | 1 | 635.6 | 314.4 | 112.6 | 65.3 |
| 337–486 | 176 | 203.0 | 2,840 | 1 | 550.3 | 412.4 | 174.9 | 87.1 |
| 487–645 | 176 | 171.2 | 3,831 | 1 | 591.0 | 408.9 | 166.9 | 111.1 |
| 649–810 | 176 | 209.1 | 2,822 | 1 | 657.9 | 451.5 | 152.9 | 129.3 |
| 825–972 | 168 | 398.1 | 3,753 | 1 | 487.4 | 543.5 | 225.9 | 150.8 |
| 973–1,134 | 176 | 249.9 | 2,993 | 1 | 678.1 | 491.8 | 156.9 | 168.0 |
| 1,138–1,296 | 175 | 172.8 | 2,470 | 1 | 581.4 | 589.6 | 223.2 | 179.4 |
| 1,299–1,451 | 180 | 281.0 | 4,345 | 1 | 673.2 | 531.2 | 158.6 | 195.1 |
| 1,467–1,620 | 151 | 159.6 | 3,082 | 1 | 601.8 | 669.1 | 281.9 | 213.2 |
| 1,623–1,782 | 181 | 182.3 | 2,718 | 1 | 612.1 | 590.1 | 194.6 | 228.4 |
| 1,794–1,945 | 144 | 154.2 | 4,861 | 1 | 517.7 | 728.3 | 295.9 | 241.3 |

```
reloads          █▇▆▆▆▆▆▆▆▅▆▅
mean reload ms   ▆▅▄▄▄█▅▄▅▃▄▃
blocks behind    ████████████
server CPU       ▅▇▆▇▇▆█▇▇▇▇▆
```

## Web server under load

Each line sums up the runs of wrk on one endpoint, each rotating over every xpub. Latencies are in milliseconds: the median of the medians of the runs, the worst 99th percentile of a run, and the slowest request.

| Endpoint | Runs | Requests | Requests per second | Median | Worst p99 | Slowest | Not 2xx | Errors |
|---|---|---|---|---|---|---|---|---|
| unspent | 192 | 439,801 | 228.9 | 6.6 | 84.3 | 178.8 | 0 | 0 |
| multiaddr | 191 | 182,937 | 95.7 | 17.4 | 167.3 | 346.1 | 0 | 0 |
| summary | 191 | 868,392 | 454.4 | 3.4 | 64.1 | 166.4 | 0 | 0 |
| transactions | 192 | 539,469 | 280.8 | 5.5 | 74.8 | 183.4 | 0 | 0 |
| balances | 191 | 251,450 | 131.6 | 13.6 | 115.8 | 207.7 | 0 | 0 |

The 99th percentile of each run, in order:

```
unspent        ▃▇▇▃▇▄▃▆▆▃▃▄▄▄▆▃▅▃▃▄▃▄▃▃▆▃▃▅▃▃▅▃▃▃▄▃▃▃▆▄▄▃▆▃▃▅▅▆▅▃▃▅▅▇▃▅▃▃▇▃▃▅▃▆▃▄▃▃▃▃▄▆▃▃▃▃▃▃▅█▃▃▅▃▃▃▄▃▄▃▃▃▃▄▄▄▆▃▃▄▅▃▃▃▄▃▄▃▅▄▃▃▄▃▃▆▃▇▃▄▃▅▃▃▃▄▃▃▃▃▄▃▄▃▃▅▃▃▄▃▄▄▃▅▃▃▄▃▃▅▃▄▃▃▃▃▃▄▃▄▃▄▄▃▃▃▃▃▃▃▄▃▃▃▃▃▄▃▄▃▃▅▃▃▃▃▃▅▃▄▃▅  (up to 84.3 ms)
multiaddr      ▅▅▄▃▅▃▃▅▅▃▃▄▄▃▅▃▅▄▃▄▃▃▃▃▅▃▃▄▃▃▄▃▃▃▄█▃▃▅▃▃▃▅▃▃▄▄▄▅▃▃▅▄▄▃▄▃▃▇▃▃▃▃▄▃▄▃▄▃▃▄▅▃▃▃▃▃▃▅▃▃▃▄▃▃▃▄▃▃▃▃▃▃▃▄▄▄▃▃▄▅▃▃▃▃▃▃▃▄▄▃▃▃▃▃▄▃▅▃▄▃▄▃▃▃▄▃▃▃▃▄▃▃▃▃▄▃▃▃▃▃▄▃▃▃▃▃▃▄▃▃▃▃▃▃▃▄▃▃▅▃▃▃▃▃▃▃▃▃▃▃▃▅▃▃▃▃▃▄▃▃▄▃▃▃▃▃▃▃▃▃  (up to 167.3 ms)
summary        ▃▇▆▃▇▃▃█▃▃▄▄▃▃▆▃▆▃▃▄▃▃▃▃▅▃▂▄▃▃▄▃▃▃▅▃▃▄▆▃▃▂▆▄▃▅▄▅▅▃▃▄▅▅▃▅▃▃▄▃▃▄▃▄▃▄▃▃▃▂▄▆▃▃▃▃▃▃▃▄▃▃▅▃▃▃▄▃▄▃▃▃▃▃▃▅▅▃▃▄▆▃▃▃▃▃▄▃▄▄▃▃▄▃▃▅▃▆▃▄▃▅▃▃▃▄▃▃▃▃▄▂▅▃▃▅▃▃▅▃▄▅▅▃▃▃▃▃▄▃▃▄▃▃▃▃▄▃▃▃▃▄▃▃▃▃▃▃▃▃▃▃▃▃▆▃▃▃▃▃▃▅▃▃▃▃▃▄▃▃▃  (up to 64.1 ms)
transactions   ▄▇▅▃▇▄▃█▄▄▃▄▄▃▆▃▆▄▃▄▃▄▃▄▆▃▃▄▃▃▅▃▃▃▄▃▃▅▆▄▃▃▆▆▃▅▅▆▅▃▃▆▅▅▃▅▃▃▆▃▃▄▃▄▃▄▃▃▃▃▅▆▃▄▃▃▃▃▃▄▃▃▅▃▃▃▅▃▄▃▃▃▄▄▅▅▅▃▃▅▅▃▃▃▅▃▅▃▄▅▃▃▄▃▃▆▃▆▃▄▃▅▃▃▃▄▃▃▃▃▅▃▃▃▃▅▃▃▄▃▄▇▃▄▃▄▄▃▃▅▃▄▃▃▃▄▃▅▃▄▃▄▄▄▃▃▄▃▃▃▄▃▄▃▃▃▄▃▄▃▃▅▃▃▄▃▃▄▃▄▃▅  (up to 74.8 ms)
balances       ▃█▄▃▆▃▃▆▅▃▄▄▅▃▄▂▆▃▂▃▃▃▂▃▅▂▂▄▃▂▄▂▃▃▄▃▃▂▅▃▃▂▆▃▂▅▅▅▄▂▃▅▄▅▂▅▂▂▅▂▂▄▂▄▂▃▂▃▂▂▄▆▂▃▂▃▂▂▃▅▃▃▄▃▂▂▄▃▃▂▂▂▂▄▃▄▅▂▂▄▆▂▂▂▅▃▃▃▅▄▂▂▃▃▂▆▂▆▂▃▂▅▃▂▂▄▂▂▃▂▄▂▃▃▂▅▃▂▄▂▄▄▂▃▂▃▃▂▃▂▂▃▂▂▃▃▄▂▂▄▂▃▃▃▂▂▃▃▂▂▃▂▃▂▅▃▂▂▃▂▃▅▂▂▃▂▃▃▂▃▂  (up to 115.8 ms)
```

## Cached answers

All 16 answers checked at the end (summary, balances, first page of transactions and of unspent outputs of every xpub) were the same with and without the cache.

The server logged 0 errors (apart from health checks while it started); see `server.log`.
