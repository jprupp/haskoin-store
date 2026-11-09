#!/usr/bin/env python3
"""Report of a stress test of the Redis cache, from the files that the stress
program (stress/Main.hs) writes to its output directory.

    scripts/stress-report.py OUT_DIR

Writes OUT_DIR/report.md and prints its path. Only the standard library is
used.

The counts and times of the cache come from the aggregates that statsd-rupp
sends every second (stats.counters.*.count, stats.timers.*), which cover
every event; the individual samples that it also sends are one in ten.
"""

import json
import os
import statistics
import sys

BARS = "▁▂▃▄▅▆▇█"


def lines(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def load(path, default=None):
    if not os.path.exists(path):
        return default
    with open(path) as f:
        return json.load(f)


def spark(values):
    """A line of block characters for a series, scaled to its maximum."""
    vs = [v for v in values if v is not None]
    if not vs:
        return ""
    top = max(vs) or 1
    return "".join(
        " " if v is None else BARS[min(len(BARS) - 1, int(v / top * (len(BARS) - 1)))]
        for v in values
    )


def buckets(samples, n):
    """Split samples into n consecutive groups of about the same duration."""
    if not samples:
        return []
    end = samples[-1]["t"] or 1
    groups = [[] for _ in range(n)]
    for s in samples:
        groups[min(n - 1, int(s["t"] / end * n))].append(s)
    return groups


def counter(s, key):
    return s.get("counters", {}).get(key, 0)


def timing(s, key, field):
    t = s.get("timings", {}).get(key)
    return t.get(field) if t else None


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def fmt(x, digits=1):
    if x is None:
        return "–"
    if isinstance(x, float):
        return f"{x:,.{digits}f}"
    return f"{x:,}"


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    out = sys.argv[1]
    config = load(os.path.join(out, "config.json"), {})
    summary = load(os.path.join(out, "summary.json"), {})
    rounds = lines(os.path.join(out, "rounds.jsonl"))
    samples = lines(os.path.join(out, "samples.jsonl"))
    wrk = lines(os.path.join(out, "wrk.jsonl"))
    errors = 0
    log = os.path.join(out, "server.log")
    if os.path.exists(log):
        with open(log, errors="replace") as f:
            errors = sum(1 for l in f if "[Error" in l and "/health" not in l)

    r = []
    w = r.append
    w("# Stress test of the Redis cache\n")
    w(
        f"{fmt(summary.get('transactions_sent'))} transactions between "
        f"{config.get('wallets')} wallets of {config.get('addresses')} receiving and "
        f"{config.get('addresses')} change addresses, {fmt(config.get('block_txs'))} per block, "
        f"in {fmt(summary.get('seconds'))} seconds, while wrk asked the server about "
        f"their xpubs on five endpoints with {config.get('connections')} connections each.\n"
    )

    # Transactions
    gen_end = rounds[-1]["t"] if rounds else 0
    w("## Transactions\n")
    w("| | |\n|---|---|")
    w(f"| Sent | {fmt(summary.get('transactions_sent'))} |")
    w(f"| Refused by the node | {fmt(summary.get('transactions_refused'))} |")
    w(f"| Blocks | {len(rounds)} |")
    w(f"| Seconds to make them | {fmt(gen_end)} |")
    if gen_end:
        w(f"| Transactions per second | {fmt(summary.get('transactions_sent', 0) / gen_end)} |")
    if rounds:
        sign = statistics.median(x["sign_ms"] for x in rounds)
        submit = statistics.median(x["submit_ms"] for x in rounds)
        w(f"| Median milliseconds to sign a block of them | {fmt(sign)} |")
        w(f"| Median milliseconds for the node to accept them | {fmt(submit)} |")
    w(f"| Seconds for the server to catch up at the end | {fmt(summary.get('catch_up_seconds'))} |")
    w("")

    # Cache
    reloads = sum(counter(s, "stats.counters.cache.reloads.count") for s in samples)
    full = sum(counter(s, "stats.counters.cache.full_reloads.count") for s in samples)
    count_ms = sum(counter(s, "stats.timers.cache.reload.count") for s in samples)
    sum_ms = sum(timing(s, "stats.timers.cache.reload.sum", "sum") or 0 for s in samples)
    maxes = [timing(s, "stats.timers.cache.reload.upper", "max") for s in samples]
    maxes = [m for m in maxes if m is not None]
    hits = sum(counter(s, "stats.counters.cache.hits.count") for s in samples)
    misses = sum(counter(s, "stats.counters.cache.misses.count") for s in samples)
    stale = sum(counter(s, "stats.counters.cache.stale.count") for s in samples)
    evictions = sum(counter(s, "stats.counters.cache.evictions.count") for s in samples)
    w("## Cache\n")
    w("| | |\n|---|---|")
    w(f"| Reloads of an xpub | {fmt(int(reloads))} |")
    w(f"| Of which full, reading the whole xpub | {fmt(int(full))} |")
    if gen_end:
        w(f"| Reloads per second while making transactions | {fmt(reloads / gen_end)} |")
    w(f"| Mean milliseconds per reload | {fmt(sum_ms / count_ms if count_ms else None)} |")
    w(f"| Longest reload, milliseconds | {fmt(max(maxes) if maxes else None)} |")
    w(f"| Reads answered from the cache | {fmt(int(hits))} |")
    w(f"| Reads that missed the cache | {fmt(int(misses))} |")
    w(f"| Reloads given up because Redis knew more | {fmt(int(stale))} |")
    w(f"| Evictions | {fmt(int(evictions))} |")
    w("")

    # Redis
    slow = summary.get("redis_slow")
    if slow is not None:
        w("## Commands that blocked Redis\n")
        w(
            "Redis runs one command or script at a time, so a long one makes every "
            "client wait, every server sharing the cache included. These are the "
            "commands that took 10 ms or more.\n"
        )
        if not slow:
            w("None.\n")
        else:
            by = {}
            for x in slow:
                by.setdefault(x["command"] or "?", []).append(x["ms"])
            w("| Command | Count | Total ms | Median ms | Longest ms |")
            w("|---|---|---|---|---|")
            for cmd, ms in sorted(by.items(), key=lambda kv: -sum(kv[1])):
                w(f"| {cmd} | {len(ms)} | {fmt(sum(ms))} | {fmt(statistics.median(ms))} | {fmt(max(ms))} |")
            w("")

    # Over time
    n = min(12, max(1, len(samples)))
    groups = buckets(samples, n)
    w("## Over time\n")
    w(
        "The run split into equal parts of time. Reload times grow with the "
        "number of transactions of the xpubs; the lag is how many blocks the "
        "server was behind the node.\n"
    )
    columns = [
        "Seconds",
        "Reloads",
        "Mean reload ms",
        "Longest reload ms",
        "Most blocks behind",
        "Server CPU %",
        "Server MiB",
        "Valkey MiB",
    ]
    w("| " + " | ".join(columns) + " |")
    w("|" + "---|" * len(columns))
    series = {"reloads": [], "mean": [], "lag": [], "cpu": []}
    for g in groups:
        if not g:
            continue
        rl = sum(counter(s, "stats.counters.cache.reloads.count") for s in g)
        c = sum(counter(s, "stats.timers.cache.reload.count") for s in g)
        sm = sum(timing(s, "stats.timers.cache.reload.sum", "sum") or 0 for s in g)
        mx = [timing(s, "stats.timers.cache.reload.upper", "max") for s in g]
        mx = [m for m in mx if m is not None]
        lags = [
            s["node_height"] - s["server_height"]
            for s in g
            if s.get("node_height") is not None and s.get("server_height") is not None
        ]
        cpu = [s["cpu_percent"] for s in g]
        rss = [s["rss_mb"] for s in g]
        vm = [s["valkey_mb"] for s in g if s.get("valkey_mb") is not None]
        mean = sm / c if c else None
        series["reloads"].append(rl)
        series["mean"].append(mean)
        series["lag"].append(max(lags) if lags else None)
        series["cpu"].append(statistics.mean(cpu))
        w(
            f"| {fmt(g[0]['t'], 0)}–{fmt(g[-1]['t'], 0)} | {fmt(int(rl))} | {fmt(mean)} | "
            f"{fmt(max(mx) if mx else None)} | {fmt(max(lags) if lags else None)} | "
            f"{fmt(statistics.mean(cpu))} | {fmt(max(rss))} | {fmt(max(vm) if vm else None)} |"
        )
    w("")
    w("```")
    w(f"reloads          {spark(series['reloads'])}")
    w(f"mean reload ms   {spark(series['mean'])}")
    w(f"blocks behind    {spark(series['lag'])}")
    w(f"server CPU       {spark(series['cpu'])}")
    w("```\n")

    # Web
    w("## Web server under load\n")
    w(
        "Each line sums up the runs of wrk on one endpoint, each rotating over "
        "every xpub. Latencies are in milliseconds: the median of the medians "
        "of the runs, the worst 99th percentile of a run, and the slowest "
        "request.\n"
    )
    w("| Endpoint | Runs | Requests | Requests per second | Median | Worst p99 | Slowest | Not 2xx | Errors |")
    w("|---|---|---|---|---|---|---|---|---|")
    names = []
    for x in wrk:
        if x["endpoint"] not in names:
            names.append(x["endpoint"])
    trends = {}
    for name in names:
        runs = [x for x in wrk if x["endpoint"] == name and not x.get("failed")]
        failed = sum(1 for x in wrk if x["endpoint"] == name and x.get("failed"))
        if not runs:
            w(f"| {name} | {failed} failed | | | | | | | |")
            continue
        req = sum(x["requests"] for x in runs)
        dur = sum(x["duration_us"] for x in runs) / 1e6
        trends[name] = [x["p99_us"] / 1000 for x in runs]
        w(
            f"| {name} | {len(runs)}{f' ({failed} failed)' if failed else ''} | {fmt(req)} | "
            f"{fmt(req / dur if dur else None)} | "
            f"{fmt(statistics.median(x['p50_us'] for x in runs) / 1000)} | "
            f"{fmt(max(x['p99_us'] for x in runs) / 1000)} | "
            f"{fmt(max(x['max_us'] for x in runs) / 1000)} | "
            f"{fmt(sum(x['non2xx'] for x in runs))} | {fmt(sum(x['errors'] for x in runs))} |"
        )
    w("")
    if trends:
        w("The 99th percentile of each run, in order:\n")
        w("```")
        for name, t in trends.items():
            w(f"{name:<14} {spark(t)}  (up to {fmt(max(t))} ms)")
        w("```\n")

    # Consistency
    checks = summary.get("consistency", [])
    bad = [c for c in checks if not c.get("same")]
    w("## Cached answers\n")
    if not checks:
        w("Not checked: the run did not finish.\n")
    elif not bad:
        w(
            f"All {len(checks)} answers checked at the end (summary, balances, first "
            "page of transactions and of unspent outputs of every xpub) were the same "
            "with and without the cache.\n"
        )
    else:
        w(f"{len(bad)} of {len(checks)} answers differed with and without the cache:\n")
        for c in bad:
            w(f"- `{c['path'] or '/'}` of `{c['xpub']}`")
        w("")
    w(f"The server logged {errors} errors (apart from health checks while it started); see `server.log`.\n")

    path = os.path.join(out, "report.md")
    with open(path, "w") as f:
        f.write("\n".join(r))
    print(path)


if __name__ == "__main__":
    main()
