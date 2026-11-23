# A Grafana dashboard for the store

`tools/grafana/haskoin-store.json` is a dashboard for Grafana. It shows
what a store says of itself through its metrics, read from InfluxDB.

## In short

A store can send figures about itself as it runs: how many requests it
answers, how long they take, how far it is behind its node, what its
database and its memory do. The README lists them under "StatsD". This
dashboard draws them: nine rows, 46 panels.

The figures go from the store to Telegraf, and from Telegraf to
InfluxDB. On the way they are summed up twice: once by the store, every
second, and once by Telegraf, every ten seconds. Some figures survive
that and some do not. The dashboard reads only those that do, and works
the others out from them. Where a figure cannot be had exactly, the
panel says what it shows instead.

It was checked against a real Telegraf and a real InfluxDB with a known
load, not yet against a store in production.

## Words used here

The shared words (store, node, the pool, xpub, the cache) are in
[README.md](README.md). This page adds these.

| Word | What it means here |
| --- | --- |
| **StatsD** | A simple way for a program to send figures out, one small message for each. The store speaks it with `--statsd`. |
| **Telegraf** | A program that takes in figures, StatsD messages among them, and writes them to a database every so often. |
| **InfluxDB** | A database of figures over time. Grafana reads it. |
| **InfluxQL** | The language in which InfluxDB is asked, like SQL. The dashboard's queries are written in it. |
| **measurement** | InfluxDB's word for one named series of figures, like a table. |
| **metric** | One figure that the store measures, with a name, such as `store.peers`. |
| **counter**, **gauge**, **timing** | The three kinds of metric: a count of things that happened, a value now, and how long something took. |
| **statistic** | What the store says of a metric for one second: a count, a sum, the longest, a percentile. |
| **percentile** | Of many timings, the one below which a share of them fall: at 90, nine in ten were faster. |
| **prefix** | What the name of every metric of a store starts with: `--statsd-prefix`, then the network. By default `haskoin_store.btc`. |
| **kind of request** | One endpoint of the API, as the metrics name it: `address_balance`, `xpub_balances`, `transaction`. |

## What it needs

1. **A store that sends its own statistics and no single samples.**
   Start it with `--statsd --statsd-aggregates --statsd-no-events`, and
   point `--statsd-host` and `--statsd-port` at Telegraf. The store then
   sends, every second, the statistics of that second.
2. **Telegraf with its `statsd` input, as it comes.** These settings
   matter, and the dashboard expects their defaults:

   | Setting | Default | Why it matters |
   | --- | --- | --- |
   | `metric_separator` | `"_"` | The dots of a name become underscores in the name of the measurement |
   | `templates` | none | A template would name the measurements otherwise |
   | `delete_counters`, `delete_timings`, `delete_gauges` | `true` | Each point is then of one interval, and not a running total |
   | `datadog_extensions` | `false` | |

   Telegraf's own interval may be any. Ten seconds is its default.
3. **InfluxDB that answers InfluxQL**, and Grafana's InfluxDB data
   source set to InfluxQL. InfluxDB 1.x does; 2.x and 3.x do when set up
   for it.
4. **One Telegraf for each machine that runs a store.** Telegraf marks
   what it writes with the name of its machine (`host`), which is how
   the dashboard tells two stores apart. Two stores of one network
   behind one Telegraf need a `--statsd-prefix` each.

## Installing it

In Grafana: Dashboards, New, Import, and upload
`tools/grafana/haskoin-store.json`. Then choose, at the top of the
dashboard:

| Variable | What to choose |
| --- | --- |
| Data source | The InfluxDB that Telegraf writes to |
| Metric prefix | The store's prefix with dots as underscores: `haskoin_store_btc` for Bitcoin, `haskoin_store_bch` for Bitcoin Cash |
| Host | The machine or machines to show (default: all) |
| Kind of request | The one that the row "One kind of request" is about |
| Percentile | 90 or 95, for the panels of percentiles |
| Percentile of | A typical second, or the worst second (see below) |

## What it shows

| Row | Panels |
| --- | --- |
| Overview | Block height, blocks behind, peers, requests, server errors and refused requests a second |
| Requests | By kind of request: requests a second, time spent answering, a percentile of the response time, the longest response; errors; clients of the event stream |
| One kind of request | For the kind chosen at the top: requests a second, the mean response time, percentiles, the longest, and how busy the kind is |
| Overload | The limit on long requests, those running and waiting; refusals by reason; scheduler delay; cores in use; waits; the same by route ([congestion-control.md](congestion-control.md)) |
| Blocks and peers | Heights, the import time of the latest block by stage, peers |
| Database | Reads by kind, open iterators, pending compaction, whether writes are delayed or stopped |
| Mempool ingestion | Transactions settled, given up by reason, and on their way ([mempool-ingestion.md](mempool-ingestion.md)) |
| Fleet cache of xpubs | Hits and misses, the size of the cache, builds and evictions, the time to build an entry |
| Runtime | GHC's garbage collector: pauses, collections, CPU, allocation, heap |

Every panel has a description, under the "i" beside its title. It names
the metrics that the panel reads, and says whether what it shows is
exact. The last four rows start closed.

## The figures are summed up twice

This is the one thing to understand about this dashboard.

The store does not send each request. Every second it sends statistics
of that second: so many requests of this kind, their times added up,
the longest, a percentile. It sends them in the StatsD protocol, a
count as a counter and a time as a timing.

Telegraf does not know that these are statistics already. It treats
them as it treats anything in StatsD, and sums them up again over its
own ten seconds. So what InfluxDB holds is a statistic, over ten
seconds, of a statistic of one second.

Some statistics come through that unharmed:

| The store's statistic | What Telegraf makes of ten of them | Is it right? |
| --- | --- | --- |
| A count | Their sum | Yes: the count of the ten seconds |
| A sum of times | Their sum | Yes: the sum of the ten seconds |
| The longest | The longest of them | Yes |
| The shortest | The shortest of them | Yes |
| A gauge | The last one | Yes: the value at the end |
| A mean | The mean of the ten means | **No** |
| A rate a second | The sum of the ten rates | **No**: ten times the rate |
| A percentile | The mean, or the highest, of the ten | **Not a percentile of the ten seconds** |

Why a mean of means is wrong. Take a kind of request that gets one
request of a second in one second, and nine requests of a tenth of a
second in the next. The mean of all ten is 190 milliseconds. The means
of the two seconds are 1,000 and 100, and their mean is 550. Telegraf
says 550. A second with one request counts as much as a second with a
thousand.

So the dashboard does this:

- **A rate** is the count, over the time. It never reads the store's
  own rate.
- **A mean** is the sum of the times over the count. InfluxQL cannot
  divide one measurement by another, so Grafana does it, from two
  queries. That works for one series at a time. This is why the mean is
  in the row "One kind of request", for the kind chosen at the top, and
  not in a panel of all kinds.
- **The longest** is read as it is.
- **A percentile** is shown as what can be had: that of a **typical
  second**, the mean of the percentiles of the seconds, or that of the
  **worst second**, the highest of them. Choose at the top. Neither is
  the percentile of the whole time, which nothing downstream of the
  store can work out.
- **Time spent answering** is the sum of the times, for each second
  that passes. At 1, one core is busy with that kind of request all the
  time. It is exact, and it adds up across kinds, which a response time
  does not.

One more thing about percentiles. The store takes the 90th percentile
of a second as the longest of the fastest 90% of the requests of that
second. With one request in a second, the fastest 90% of one is none,
and the store sends nothing. So for a kind of request that is asked a
few times a second, the percentile is of few requests, or missing. The
panel "Seconds with requests" says how busy a kind is. For a quiet
kind, look at the longest and at the mean.

## Good to know

- **The last point of a rate can be low.** On a long time range, a
  point of the graph covers several of Telegraf's intervals, and the
  last point's are not all in yet.
- **It is written for the metrics of haskoin-store 2.0.** A panel whose
  metric an older store does not send shows "No data". The rest works.
- **If Telegraf keeps running totals** (`delete_counters = false`), the
  rates are wrong: the dashboard takes each point for the count of one
  interval.
- **If Telegraf has templates, or another separator**, the measurements
  have other names than the dashboard asks for.
- **A store that also sends single samples** (without
  `--statsd-no-events`) writes more measurements, with the names of the
  metrics themselves. The dashboard does not read those, and is not
  disturbed by them.
- **Times are in milliseconds**, as the store sends them. Those that
  the store measures in microseconds say so in their name (`_us`).

## How it was checked

- **Against the store's code**: every metric that the dashboard reads
  is one that the store sends, and each is read as its kind.
- **On a real pipeline**: the store's own StatsD library sending its
  statistics every second, Telegraf 1.33 with its `statsd` input as it
  comes, InfluxDB 1.8 and Grafana 11.4. A known load was sent for every
  metric, and every one of the 90 queries was run through Grafana. None
  failed, and none came back empty. `tools/grafana/check/run.sh` does
  all of this again, in three minutes, with Podman.
- **Held to the load**, with what was sent in brackets: 19.9 requests a
  second (20); 2.09 seconds spent a second (2.1); a mean of 104.7 ms
  (105); for the kind of request of the example above, a mean of 190 ms
  (190, where Telegraf's own mean says 550), and a longest of 1,000 ms
  (1,000); a 95th percentile of 189.9 ms (190).
- **That the check can fail**: given a dashboard that reads Telegraf's
  own mean for a kind of request, it says 550 where 190 was sent, and
  fails.
- **Looked at**: eight panels were drawn and looked at, among them one
  of each thing that Grafana does after the query: naming the series by
  kind of request, adding them up, a division of two queries, a band of
  states, and the two choices of percentile.
- **Not tested**: against a store in production; on InfluxDB 2.x or
  3.x; and the other 38 panels were not looked at, though their queries
  ran. Figures that look wrong are worth an issue.

## Changing it

The file is generated. Change `tools/grafana/haskoin_grafana.py` and
run it; never edit the JSON by hand.

```sh
python3 tools/grafana/haskoin_grafana.py
python3 tools/grafana/haskoin_grafana.py --metrics
```

It needs Python 3 and nothing else. The first writes
`tools/grafana/haskoin-store.json`, or the file named on its command
line. The second lists the metrics that the dashboard reads. The JSON
has lines longer than the 120 columns of this repository, since each
query is on one line.

A new metric of the store gets a panel there, and its line in the
README's tables. A new panel reads a count, a sum, the longest or a
gauge, and says in its description what it shows.

After a change, run the check:

```sh
tools/grafana/check/run.sh         # every query, and the figures
tools/grafana/check/run.sh keep    # the same, and leave it running to look at
```

It needs Podman, and Stack with the store's dependencies built. A new
panel whose figure can be known gets a line in `EXPECTED` of
`tools/grafana/check/check.py`, and the load for it in `Sender.hs`
beside it.
