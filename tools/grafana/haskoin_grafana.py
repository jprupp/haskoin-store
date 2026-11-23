#!/usr/bin/env python3
"""Build the Grafana dashboard for haskoin-store: haskoin-store.json, beside this file.

  haskoin_grafana.py [OUT.json]     write the dashboard
  haskoin_grafana.py --metrics      list the metrics that it reads, a kind and a key on each line

The dashboard is generated: change this script and run it, never the JSON. docs/grafana.md says
what the dashboard needs, how to install it, and why its queries are written as they are.

What it reads
-------------

A store started with --statsd-aggregates --statsd-no-events sends no single samples. Every second
it sends statistics of the second, in the StatsD protocol, named as a StatsD daemon names them:

  PREFIX.NET.stats.counters.KEY.count          increments in the second
  PREFIX.NET.stats.gauges.KEY                  the value
  PREFIX.NET.stats.timers.KEY.count            timings in the second
  PREFIX.NET.stats.timers.KEY.sum              their sum, in milliseconds
  PREFIX.NET.stats.timers.KEY.upper, .lower    the longest and the shortest
  PREFIX.NET.stats.timers.KEY.upper_90, _95    the longest of the fastest 90%, 95%

Telegraf's statsd input takes them and, with its default settings, writes one measurement for each
to InfluxDB every ten seconds, the dots as underscores, with the tags host and metric_type.

The statistics are aggregated twice
-----------------------------------

The store sends a count as a StatsD counter and a timing statistic as a StatsD timing. Telegraf
aggregates what it is sent over its own interval. So a measurement holds a statistic, over ten
seconds, of a statistic of one second. Some survive that, and some do not:

  count       field `value`: the sum of the counts. Exact.
  sum         field `sum`: the sum of the sums. Exact.
  upper       field `upper`: the longest of the longest. Exact.
  lower       field `lower`: the shortest of the shortest. Exact.
  gauge       field `value`: the last value sent. Exact.
  mean        field `mean`: the mean of the means of the seconds, each second counting the same
              whether it had one timing or a thousand. Wrong. The mean is the sum over the count.
  rate,       field `value`: the sum of ten rates, ten times the rate. Wrong. A rate is the count
  count_ps    over the time.
  upper_90    no percentile of ten seconds can be made of ten percentiles of a second. What can
              be said is the mean of them, a typical second, or the highest, the worst second.
              And a second with a single timing has no upper_90 at all: the fastest 90% of one
              is none.

So this dashboard reads counts, sums, the longest, and gauges, and works the rest out from those.
A percentile is shown as what it is: of a typical second, or of the worst second.

How the queries are written
---------------------------

A rate a second is `derivative(cumulative_sum(sum("value")), 1s)`: the running total, and how fast
it grows. Dividing a sum by the width of a bucket would be wrong whenever Grafana's buckets are
narrower than Telegraf's interval, which they are on a short time range.

InfluxQL cannot divide one measurement by another, and a mean is a sum over a count, each in a
measurement of its own. So a mean is a Grafana expression over two queries. That works for one
series and not for one of each kind of request: the row "One kind of request" has the mean of
the kind chosen at the top.

The names of the series of a panel by kind of request are measurements, and the kind is taken out
of the name by a transformation.
"""
import json
import sys
from pathlib import Path

OUT = Path(__file__).resolve().parent / "haskoin-store.json"
DS = {"type": "influxdb", "uid": "${datasource}"}
EXPR = {"type": "__expr__", "uid": "__expr__"}
P = "${prefix}_stats"
WHERE = 'WHERE "host" =~ /^$host$/ AND $timeFilter'
BY = "GROUP BY time($__interval)"

# A kind of request that is not one of the admission control: no lookahead in InfluxDB's regexes.
KIND = "([^c]|c[^a]|ca[^p]|cap[^_])[a-z0-9_]*"

# Every metric that the dashboard reads, a kind and a key: for --metrics, and for whoever checks
# them against the store.
METRICS = set()


def counter(key):
    METRICS.add(("counter", key))
    return f'"{P}_counters_{key}_count"'


def gauge(key):
    METRICS.add(("gauge", key))
    return f'"{P}_gauges_{key}"'


def timer(key, stat):
    METRICS.add(("timing", key))
    return f'"{P}_timers_{key}_{stat}"'


def counters(pattern):
    """The counters whose key matches a pattern with one group, and what takes the group out."""
    METRICS.add(("counter", pattern))
    return f"/^{P}_counters_{pattern}_count$/", f"^.*_stats_counters_{pattern}_count$"


def gauges(pattern):
    METRICS.add(("gauge", pattern))
    return f"/^{P}_gauges_{pattern}$/", f"^.*_stats_gauges_{pattern}$"


def timers(pattern, stat):
    METRICS.add(("timing", pattern))
    return f"/^{P}_timers_{pattern}_{stat}$/", f"^.*_stats_timers_{pattern}_{stat.replace('$pct', '[0-9]+')}$"


def rate(measurement, field="value", scale=None):
    """So many a second: how fast the running total of a count, or of a sum, grows."""
    over = f" / {scale}" if scale else ""
    return f'SELECT derivative(cumulative_sum(sum("{field}")), 1s){over} FROM {measurement} {WHERE} {BY} fill(none)'


def total(measurement, field="value"):
    """The total in each bucket: for a ratio of two totals."""
    return f'SELECT sum("{field}") FROM {measurement} {WHERE} {BY} fill(none)'


def last(measurement, scale=None):
    """A gauge, for each host."""
    over = f" / {scale}" if scale else ""
    return f'SELECT last("value"){over} FROM {measurement} {WHERE} {BY}, "host" fill(none)'


def longest(measurement):
    return f'SELECT max("upper") FROM {measurement} {WHERE} {BY} fill(none)'


def typical(measurement):
    """A percentile of a typical second: the mean of the percentiles of the seconds."""
    return f'SELECT mean("mean") FROM {measurement} {WHERE} {BY} fill(none)'


def chosen(measurement):
    """A percentile as chosen at the top: of a typical second, or of the worst."""
    return f"SELECT ${{how:raw}} FROM {measurement} {WHERE} {BY} fill(none)"


class Board:
    def __init__(self):
        self.panels, self.id, self.x, self.y, self.h, self.row = [], 0, 0, 0, 0, None

    def _place(self, w, h):
        if self.x + w > 24:
            self.x, self.y = 0, self.y + self.h
            self.h = 0
        pos = {"h": h, "w": w, "x": self.x, "y": self.y}
        self.x += w
        self.h = max(self.h, h)
        return pos

    def start_row(self, title, collapsed=False):
        self.x, self.y, self.h = 0, self.y + self.h, 0
        self.id += 1
        row = {"type": "row", "id": self.id, "title": title, "collapsed": collapsed,
               "gridPos": {"h": 1, "w": 24, "x": 0, "y": self.y}, "panels": []}
        self.y += 1
        self.panels.append(row)
        self.row = row if collapsed else None

    def add(self, panel, w, h):
        self.id += 1
        panel = {"id": self.id, "datasource": DS, "gridPos": self._place(w, h), **panel}
        (self.row["panels"] if self.row else self.panels).append(panel)


def query(text, alias, hide=False):
    return {"datasource": DS, "rawQuery": True, "query": text, "alias": alias, "resultFormat": "time_series",
            "hide": hide}


def part(text, alias="part"):
    """A query that only an expression reads. The parts of an expression have the same name: Grafana
    pairs the series of two queries by their names and tags, and warns of those that it cannot pair."""
    return query(text, alias, hide=True)


def math(expression, hide=False):
    """A Grafana expression over the queries before it, named by their letters: $A / $B."""
    return {"datasource": EXPR, "type": "math", "expression": expression, "hide": hide}


def lettered(series):
    return [{"refId": chr(65 + i), **s} for i, s in enumerate(series)]


def renamed(regex):
    """Name each series by the group of a regex over the name of its measurement."""
    return {"id": "renameByRegex", "options": {"regex": regex, "renamePattern": "$1"}}


def summed(name):
    """One series: the sum of all of them."""
    return {"id": "calculateField", "options": {"mode": "reduceRow", "reduce": {"reducer": "sum"},
                                                "replaceFields": True, "alias": name}}


def graph(title, description, unit, *series, stack=False, legend="list", minimum=0, step=False, decimals=None,
          transformations=None, names=None):
    defaults = {"unit": unit, "color": {"mode": "palette-classic"},
                "custom": {"drawStyle": "line", "lineInterpolation": "stepAfter" if step else "linear",
                           "lineWidth": 1, "fillOpacity": 25 if stack else 8, "showPoints": "never",
                           "spanNulls": True, "stacking": {"mode": "normal" if stack else "none", "group": "A"}}}
    if minimum is not None:
        defaults["min"] = minimum
    if decimals is not None:
        defaults["decimals"] = decimals
    table = legend == "table"
    # An expression is named by its letter: give it the name that the legend should show.
    overrides = [{"matcher": {"id": "byFrameRefID", "options": ref},
                  "properties": [{"id": "displayName", "value": name}]} for ref, name in (names or {}).items()]
    return {"type": "timeseries", "title": title, "description": description,
            "fieldConfig": {"defaults": defaults, "overrides": overrides},
            "options": {"legend": {"displayMode": "table" if table else "list",
                                   "placement": "right" if table else "bottom",
                                   "calcs": ["mean", "max"] if table else [], "showLegend": True,
                                   **({"sortBy": "Mean", "sortDesc": True} if table else {})},
                        "tooltip": {"mode": "multi", "sort": "desc"}},
            "transformations": transformations or [],
            "targets": lettered(series)}


def stat(title, description, unit, *series, steps=None, decimals=None, transformations=None):
    steps = steps or [("green", None)]
    defaults = {"unit": unit, "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": [{"color": c, "value": v} for c, v in steps]}}
    if decimals is not None:
        defaults["decimals"] = decimals
    return {"type": "stat", "title": title, "description": description,
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "value", "graphMode": "area", "justifyMode": "auto",
                        "textMode": "auto", "orientation": "auto"},
            "transformations": transformations or [],
            "targets": lettered(series)}


def states(title, description, measurement, names):
    """A band of states over time; `names` maps each value to (text, colour)."""
    return {"type": "state-timeline", "title": title, "description": description,
            # Not the thresholds colour mode: with it Grafana turns numbers into threshold ranges and
            # ignores the names.
            "fieldConfig": {"defaults": {"color": {"mode": "fixed", "fixedColor": "text"},
                                         "custom": {"fillOpacity": 80, "lineWidth": 0},
                                         "mappings": [{"type": "value", "options": {
                                             str(v): {"text": t, "color": c, "index": i}
                                             for i, (v, (t, c)) in enumerate(names.items())}}]},
                            "overrides": []},
            "options": {"mergeValues": True, "showValue": "auto", "alignValue": "left", "rowHeight": 0.9,
                        "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "single", "sort": "none"}},
            "transformations": [],
            "targets": lettered([query(last(measurement), "$tag_host")])}


EXACT = " Exact: Telegraf adds the store's counts up."
PERCENTILE = (" The store works a percentile out for each second, and no percentile of a longer time can "
              "be made of those. Shown is the one chosen at the top: of a typical second (the mean of the "
              "seconds) or of the worst second. A second with fewer than ten requests of a kind has no "
              "90th percentile to speak of: look at the longest then.")


def build():
    b = Board()
    web_counts, web_count_name = timers(f"web_({KIND})", "count")
    web_sums, web_sum_name = timers(f"web_({KIND})", "sum")
    web_uppers, web_upper_name = timers(f"web_({KIND})", "upper")
    web_pcts, web_pct_name = timers(f"web_({KIND})", "upper_$pct")

    # ---- Overview
    b.start_row("Overview")
    b.add(stat("Block height", "Height of the best block that the store has imported (store.blocks).", "none",
               query(last(gauge("store_blocks")), "$tag_host")), 4, 4)
    b.add(stat("Blocks behind", "Best header minus best block (store.headers - store.blocks). More than a few "
               "for long means that the store is not keeping up with the chain.", "none",
               part(last(gauge("store_headers")), "$tag_host"), part(last(gauge("store_blocks")), "$tag_host"),
               math("$A - $B"),
               steps=[("green", None), ("yellow", 2), ("red", 6)]), 4, 4)
    b.add(stat("Peers", "Peers online (store.peers). At zero the store hears of no new blocks or transactions.",
               "none", query(last(gauge("store_peers")), "$tag_host"), steps=[("red", None), ("green", 1)]), 4, 4)
    b.add(stat("Requests", "Requests answered a second, every kind together: the counts of the web.* timings, "
               "added up." + EXACT, "reqps", query(rate(web_counts), "$measurement"), decimals=1,
               transformations=[summed("Requests")]), 4, 4)
    b.add(stat("Server errors", "Requests that failed with a server error, a second (web.server_errors).", "reqps",
               query(rate(counter("web_server_errors")), "Server errors"),
               steps=[("green", None), ("red", 0.001)], decimals=2), 4, 4)
    b.add(stat("Requests refused", "Requests refused with server-busy, a second, for any reason "
               "(web.requests_refused). See the Overload row for why.", "reqps",
               query(rate(counter("web_requests_refused")), "Refused"),
               steps=[("green", None), ("yellow", 0.001)], decimals=2), 4, 4)

    # ---- Requests
    b.start_row("Requests")
    b.add(graph("Requests by kind", "Requests answered a second, by kind of request (the count of each web.* "
                "timing)." + EXACT, "reqps", query(rate(web_counts), "$measurement"), stack=True, legend="table",
                transformations=[renamed(web_count_name)]), 12, 9)
    b.add(graph("Time spent answering, by kind", "Seconds spent answering each kind of request, for each second "
                "that passes: 1 is one core busy with it all the time. It says where the time of the store goes, "
                "which neither the rate nor the response time does alone. Exact: Telegraf adds the store's sums "
                "up.", "none", query(rate(web_sums, "sum", 1000), "$measurement"), stack=True, legend="table",
                decimals=2, transformations=[renamed(web_sum_name)]), 12, 9)
    b.add(graph("Response time by kind, percentile $pct",
                "The time within which $pct in a hundred requests of each kind were answered (web.* timings)."
                + PERCENTILE, "ms", query(chosen(web_pcts), "$measurement"), legend="table",
                transformations=[renamed(web_pct_name)]), 12, 9)
    b.add(graph("Longest response by kind", "The longest that a request of each kind took. Exact: the longest "
                "of the longest of each second.", "ms", query(longest(web_uppers), "$measurement"),
                legend="table", transformations=[renamed(web_upper_name)]), 12, 9)
    b.add(graph("Errors and reruns", "Requests that failed, a second: server errors (web.server_errors) and client "
                "errors (web.client_errors). Reruns (web.reruns) are requests answered again because a cached xpub "
                "that they read changed meanwhile.", "reqps",
                query(rate(counter("web_server_errors")), "Server errors"),
                query(rate(counter("web_client_errors")), "Client errors"),
                query(rate(counter("web_reruns")), "Reruns")), 12, 8)
    b.add(graph("Event stream clients", "Clients connected to the stream of events (web.events_connected).",
                "none", query(last(gauge("web_events_connected")), "$tag_host"), step=True, decimals=0), 12, 8)

    # ---- One kind of request
    b.start_row("One kind of request: $route")
    one = "web_${route}"
    b.add(graph("Requests", "Requests of the kind chosen at the top, a second." + EXACT, "reqps",
                query(rate(timer(one, "count")), "Requests")), 8, 8)
    b.add(graph("Response time", "Of the kind chosen at the top. The mean is exact: the sum of the times over "
                "the count of the requests, and not the mean that Telegraf makes of the means of the seconds, "
                "which counts a second with one request as much as one with a thousand. The percentiles are "
                "those of a typical second. The longest is exact.", "ms",
                part(total(timer(one, "sum"), "sum")), part(total(timer(one, "count"))), math("$A / $B"),
                query(typical(timer(one, "upper_90")), "90%, typical second"),
                query(typical(timer(one, "upper_95")), "95%, typical second"),
                query(longest(timer(one, "upper")), "Longest"), names={"C": "Mean"}), 8, 8)
    b.add(graph("Seconds with requests", "Of each ten seconds, in how many the store answered a request of this "
                "kind. Below ten, the kind is quiet, and its percentiles are of few requests.", "none",
                query(f'SELECT mean("count") FROM {timer(one, "upper")} {WHERE} {BY} fill(none)', "Seconds"),
                decimals=1), 8, 8)

    # ---- Overload
    b.start_row("Overload: admission control of long requests")
    b.add(graph("Long requests: limit, running and waiting",
                "The limit on long requests that the controller set (web.cap.limit), those being answered "
                "(web.requests_running) and those waiting for a slot (web.requests_waiting). A limit stuck at its "
                "ceiling with long waits points at the disk; one stuck at its floor with a high delay points at "
                "work that is not limited.", "none",
                query(last(gauge("web_cap_limit")), "Limit $tag_host"),
                query(last(gauge("web_requests_running")), "Running $tag_host"),
                query(last(gauge("web_requests_waiting")), "Waiting $tag_host"), decimals=0), 12, 8)
    b.add(graph("Requests refused, by reason", "Requests refused a second: because the line was full "
                "(web.cap.refused_full), after waiting too long (web.cap.refused_waited), or by CoDel "
                "(web.cap.refused_codel).", "reqps",
                query(rate(counter("web_cap_refused_full")), "Line full"),
                query(rate(counter("web_cap_refused_waited")), "Waited too long"),
                query(rate(counter("web_cap_refused_codel")), "CoDel"), stack=True), 12, 8)
    b.add(graph("Scheduler delay", "90th percentile of the scheduler's delay in the last half second, without the "
                "garbage collector's pauses (web.cap.delay_us) and with them (web.cap.lateness_us). These are "
                "gauges: the last value of each ten seconds.", "µs",
                query(last(gauge("web_cap_delay_us")), "Delay $tag_host"),
                query(last(gauge("web_cap_lateness_us")), "With GC pauses $tag_host")), 6, 7)
    b.add(graph("Cores in use", "Use of the cores that the controller sees, smoothed over two seconds "
                "(web.cap.used, here in cores).", "none", query(last(gauge("web_cap_used"), 100), "$tag_host"),
                decimals=1), 6, 7)
    b.add(graph("Wait for a slot, and time holding it", "How long an admitted long request waited for its slot "
                "(web.cap.sojourn: the 90th percentile of a typical second, and the longest), and the smoothed "
                "time that a long request holds its slot (web.cap.round_trip_us).", "ms",
                query(typical(timer("web_cap_sojourn", "upper_90")), "Wait, 90%, typical second"),
                query(longest(timer("web_cap_sojourn", "upper")), "Wait, longest"),
                query(last(gauge("web_cap_round_trip_us"), 1000), "Slot held $tag_host")), 6, 7)
    b.add(states("Overloaded", "Whether CoDel finds the line of long requests overloaded (web.cap.overloaded).",
                 gauge("web_cap_overloaded"), {0: ("No", "green"), 1: ("Overloaded", "red")}), 6, 7)
    running, running_name = gauges("web_cap_(.+)_running")
    waited, waited_name = timers("web_cap_(.+)_waited", "upper_$pct")
    held, held_name = timers("web_cap_(.+)_held", "upper_$pct")
    refused, refused_name = counters("web_cap_(.+)_refused")
    b.add(graph("Slots held, by long route", "Slots that each long route holds (web.cap.<route>.running).", "none",
                query(f'SELECT last("value") FROM {running} {WHERE} {BY} fill(none)', "$measurement"), stack=True,
                decimals=0, transformations=[renamed(running_name)]), 6, 8)
    b.add(graph("Wait for a slot by long route, percentile $pct", "The time that a request of each long route "
                "waited for its slot (web.cap.<route>.waited)." + PERCENTILE, "ms",
                query(chosen(waited), "$measurement"), transformations=[renamed(waited_name)]), 6, 8)
    b.add(graph("Slot held by long route, percentile $pct", "The time that a request of each long route held its "
                "slot, to the last byte (web.cap.<route>.held)." + PERCENTILE, "ms",
                query(chosen(held), "$measurement"), transformations=[renamed(held_name)]), 6, 8)
    b.add(graph("Requests refused, by long route", "Requests of each long route refused a second "
                "(web.cap.<route>.refused).", "reqps", query(rate(refused), "$measurement"), stack=True,
                transformations=[renamed(refused_name)]), 6, 8)

    # ---- Blocks
    b.start_row("Blocks and peers")
    b.add(graph("Heights", "Height of the best block (store.blocks) and of the best header (store.headers).", "none",
                query(last(gauge("store_blocks")), "Blocks $tag_host"),
                query(last(gauge("store_headers")), "Headers $tag_host"), minimum=None, decimals=0), 8, 8)
    b.add(graph("Import of the latest block, by stage", "Milliseconds that the latest block took to import, by "
                "stage: working out what the import needs (prepare), reading ahead what the rules will read "
                "(prefetch), the rules in memory (apply) and writing the batch to RocksDB (write). A slow prefetch "
                "is the disk or RocksDB's cache; the other stages are mostly CPU. (store.last_block_*_ms)", "ms",
                query(last(gauge("store_last_block_prepare_ms")), "Prepare $tag_host"),
                query(last(gauge("store_last_block_prefetch_ms")), "Prefetch $tag_host"),
                query(last(gauge("store_last_block_apply_ms")), "Apply $tag_host"),
                query(last(gauge("store_last_block_write_ms")), "Write $tag_host"), stack=True, step=True), 8, 8)
    b.add(graph("Peers", "Peers online (store.peers).", "none", query(last(gauge("store_peers")), "$tag_host"),
                step=True, decimals=0), 8, 8)

    # ---- Database
    b.start_row("Database", collapsed=True)
    reads = [("best_block", "Best block"), ("blocks", "Blocks"), ("txs", "Transactions"), ("balances", "Balances"),
             ("unspents", "Unspent outputs"), ("address_txs", "Address transactions"),
             ("xpub_balances", "Xpub balances"), ("xpub_unspents", "Xpub unspent outputs"),
             ("xpub_txs", "Xpub transactions"), ("xpub_tx_count", "Xpub transaction counts")]
    b.add(graph("Database reads by kind", "Reads of the database a second, by what was read (db.*).", "ops",
                *[query(rate(counter("db_" + k)), label) for k, label in reads], stack=True, legend="table"), 12, 8)
    b.add(graph("Open iterators", "Iterators open on RocksDB (db.iterators).", "none",
                query(last(gauge("db_iterators")), "$tag_host"), decimals=0), 4, 8)
    b.add(graph("Pending compaction", "Bytes that RocksDB has to compact, over every column family "
                "(db.pending_compaction_bytes).", "bytes",
                query(last(gauge("db_pending_compaction_bytes")), "$tag_host")), 4, 8)
    b.add(states("Writes", "Writes to RocksDB: flowing, delayed or stopped (db.write_delayed). Delayed or stopped "
                 "writes mean that compaction is not keeping up.", gauge("db_write_delayed"),
                 {0: ("Flowing", "green"), 1: ("Delayed", "yellow"), 2: ("Stopped", "red")}), 4, 8)

    # ---- Mempool ingestion
    b.start_row("Mempool ingestion", collapsed=True)
    b.add(graph("Transactions settled and messages dropped", "Transactions that peers named and that the pool "
                "decided on, or already had (ingest.settled), and messages of peers dropped because the path into "
                "the pool was behind (ingest.dropped), a second.", "ops",
                query(rate(counter("ingest_settled")), "Settled"),
                query(rate(counter("ingest_dropped")), "Dropped")), 8, 8)
    given, given_name = counters("ingest_given_up_(.+)")
    b.add(graph("Transactions given up, by reason", "Transactions given up on their way to the pool, a second, by "
                "reason (ingest.given_up.*). Every reason but node_has_not makes the store read its sources "
                "again.", "ops", query(rate(given), "$measurement"), stack=True,
                transformations=[renamed(given_name)]), 8, 8)
    b.add(graph("Transactions on their way", "Transaction ids on their way to the pool (ingest.ids), those asked of "
                "peers and not received, which /health counts (ingest.pending), and those held (ingest.held).",
                "none", query(last(gauge("ingest_ids")), "On their way $tag_host"),
                query(last(gauge("ingest_pending")), "Asked, not received $tag_host"),
                query(last(gauge("ingest_held")), "Held $tag_host"), decimals=0), 8, 8)

    # ---- Fleet cache
    b.start_row("Fleet cache of xpubs", collapsed=True)
    hits, misses = counter("fleet_cache_hits"), counter("fleet_cache_misses")
    b.add(graph("Hits and misses", "Requests about an xpub whose balances the cache gave (fleet.cache.hits) and that "
                "the cache did not have (fleet.cache.misses), a second. Misses include xpubs being built.", "reqps",
                query(rate(hits), "Hits"), query(rate(misses), "Misses"), stack=True), 8, 8)
    b.add(graph("Share of hits", "Hits over hits and misses: how much of the load the cache takes. Exact: a ratio "
                "of two counts.", "percentunit", part(total(hits)), part(total(misses)),
                math("$A / ($A + $B)"), names={"C": "Hits"}), 8, 8)
    b.add(graph("Cache size", "Xpubs in the cache, whichever store cached them (fleet.cache.xpubs), and the "
                "addresses that they have between them (fleet.cache.addresses).", "none",
                query(last(gauge("fleet_cache_xpubs")), "Xpubs $tag_host"),
                query(last(gauge("fleet_cache_addresses")), "Addresses $tag_host"), decimals=0), 8, 8)
    b.add(graph("Cache builds and evictions", "A second: entries that this store built or added addresses to "
                "(fleet.cache.builds), builds given up (fleet.cache.abandoned), entries evicted "
                "(fleet.cache.evictions) and times that this store emptied the cache (fleet.cache.emptied).", "ops",
                query(rate(counter("fleet_cache_builds")), "Builds"),
                query(rate(counter("fleet_cache_abandoned")), "Abandoned"),
                query(rate(counter("fleet_cache_evictions")), "Evictions"),
                query(rate(counter("fleet_cache_emptied")), "Emptied")), 12, 8)
    b.add(graph("Time to build an entry", "The time to build an entry (fleet.cache.build). The mean is exact: the "
                "sum of the times over the count of the builds. The longest is exact. Builds are few, so no "
                "percentile is shown.", "ms",
                part(total(timer("fleet_cache_build", "sum"), "sum")),
                part(total(timer("fleet_cache_build", "count"))), math("$A / $B"),
                query(longest(timer("fleet_cache_build", "upper")), "Longest"), names={"C": "Mean"}), 12, 8)

    # ---- Runtime
    b.start_row("Runtime: GHC's garbage collector", collapsed=True)
    b.add(graph("Time stopped for the collector", "Share of each second that every thread was stopped "
                "(rts.gc_pause_percent). While stopped the store answers nothing, so a high share shows up as slow "
                "answers everywhere. A gauge: the last second of each ten.", "percent",
                query(last(gauge("rts_gc_pause_percent")), "$tag_host")), 6, 8)
    b.add(graph("Mean pause", "Mean pause of a collection in the last second (rts.gc_pause_mean_us).", "µs",
                query(last(gauge("rts_gc_pause_mean_us")), "$tag_host")), 6, 8)
    b.add(graph("Collections", "Collections a second (rts.gcs), and those of the whole heap (rts.major_gcs).", "ops",
                query(rate(counter("rts_gcs")), "All"), query(rate(counter("rts_major_gcs")), "Whole heap")), 6, 8)
    b.add(graph("CPU of the collector", "Cores that the collector uses (the rate of rts.gc_cpu_us).", "none",
                query(rate(counter("rts_gc_cpu_us"), scale=1000000), "Cores"), decimals=2), 6, 8)
    b.add(graph("Allocation", "Bytes allocated a second (rts.allocated_bytes).", "binBps",
                query(rate(counter("rts_allocated_bytes")), "Allocated")), 12, 8)
    b.add(graph("Heap", "Bytes live after the last collection (rts.live_bytes) and bytes that the heap holds from "
                "the system (rts.heap_bytes).", "bytes",
                query(last(gauge("rts_live_bytes")), "Live $tag_host"),
                query(last(gauge("rts_heap_bytes")), "Heap $tag_host")), 12, 8)

    def custom(name, label, choices, default):
        """A choice at the top of the dashboard: each a text and a value."""
        options = [{"selected": text == default, "text": text, "value": value} for text, value in choices]
        return {"name": name, "label": label, "type": "custom", "hide": 0, "includeAll": False, "multi": False,
                "query": ",".join(f"{text} : {value}" for text, value in choices),
                "current": next(o for o in options if o["selected"]), "options": options}

    route_query = f"SHOW MEASUREMENTS WITH MEASUREMENT =~ /^{P}_timers_web_{KIND}_count$/"
    host_query = f'SHOW TAG VALUES FROM "{P}_gauges_store_blocks" WITH KEY = "host"'
    return {
        "title": "Haskoin Store", "uid": "haskoin-store", "tags": ["haskoin", "haskoin-store"],
        "description": "haskoin-store's metrics, read from InfluxDB as Telegraf's statsd input writes them, for "
                       "a store that sends its own statistics (--statsd-aggregates --statsd-no-events). See "
                       "docs/grafana.md of haskoin-store.",
        "schemaVersion": 39, "version": 1, "editable": True, "graphTooltip": 1,
        "time": {"from": "now-6h", "to": "now"}, "refresh": "30s", "timezone": "",
        "links": [{"title": "Metrics reference (haskoin-store README)", "type": "link", "icon": "doc",
                   "url": "https://github.com/jprupp/haskoin-store#statsd", "targetBlank": True}],
        "annotations": {"list": [{"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                                  "enable": True, "hide": True, "iconColor": "rgba(0, 211, 255, 1)",
                                  "name": "Annotations & Alerts", "type": "dashboard"}]},
        "templating": {"list": [
            {"name": "datasource", "label": "Data source", "type": "datasource", "query": "influxdb",
             "current": {}, "hide": 0, "includeAll": False, "multi": False, "options": [], "refresh": 1, "regex": ""},
            {"name": "prefix", "label": "Metric prefix", "type": "textbox", "query": "haskoin_store_btc", "hide": 0,
             "current": {"selected": True, "text": "haskoin_store_btc", "value": "haskoin_store_btc"},
             "options": [{"selected": True, "text": "haskoin_store_btc", "value": "haskoin_store_btc"}]},
            {"name": "host", "label": "Host", "type": "query", "datasource": DS, "query": host_query,
             "definition": host_query, "includeAll": True, "allValue": ".*", "multi": True, "refresh": 2, "sort": 1,
             "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "options": [], "hide": 0},
            {"name": "route", "label": "Kind of request", "type": "query", "datasource": DS, "query": route_query,
             "definition": route_query, "regex": "/^.*_stats_timers_web_(.*)_count$/", "includeAll": False,
             "multi": False, "refresh": 2, "sort": 1, "current": {}, "options": [], "hide": 0},
            custom("pct", "Percentile", [("90", "90"), ("95", "95")], "95"),
            custom("how", "Percentile of", [("a typical second", 'mean("mean")'),
                                            ("the worst second", 'max("upper")')], "a typical second"),
        ]},
        "panels": b.panels,
    }


if __name__ == "__main__":
    board = build()
    if sys.argv[1:] == ["--metrics"]:
        for kind, key in sorted(METRICS):
            print(kind, key)
    else:
        out = Path(sys.argv[1]) if len(sys.argv) > 1 else OUT
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(board, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Wrote {out}")
