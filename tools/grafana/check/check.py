#!/usr/bin/env python3
"""Run every query of the dashboard through Grafana, and hold some to the load of Sender.hs.

  check.py DASHBOARD.json GRAFANA_URL

Each query is run as Grafana runs it, through its own API, with the variables of the dashboard
filled in. It fails if a query gives an error, if one that is shown gives nothing, or if a figure
is not what Sender.hs sent. What a panel does in the browser after its queries, naming and adding
up the series, is not run here: look at the dashboard for that.
"""
import json
import sys
import time
import urllib.error
import urllib.request

VARIABLES = {"${datasource}": "influx-check", "${prefix}": "haskoin_store_btc", "$host": ".*",
             "${route}": "xpub_balances", "$pct": "95", "${how:raw}": 'mean("mean")'}

# What Sender.hs sends, as the dashboard should show it: the row and the title of a panel, the
# letter of a query, a word of the name of its series or nothing for the only one, and the figure.
EXPECTED = [
    ("Requests", "Requests by kind", "A", "address_balance", 20),
    ("Requests", "Requests by kind", "A", "xpub_balances", 5),
    ("Requests", "Time spent answering, by kind", "A", "address_balance", 2.1),
    ("Requests", "Longest response by kind", "A", "xpub_balances", 1000),
    ("Requests", "Longest response by kind", "A", "address_balance", 200),
    ("Requests", "Response time by kind, percentile $pct", "A", "address_balance", 190),
    ("One kind of request: $route", "Requests", "A", None, 5),
    # the mean of the mean of the seconds is 550: the mean is 190
    ("One kind of request: $route", "Response time", "C", None, 190),
    ("One kind of request: $route", "Response time", "F", None, 1000),
    ("Overview", "Blocks behind", "C", None, 1),
    ("Overview", "Server errors", "A", None, 2),
    ("Fleet cache of xpubs", "Share of hits", "C", None, 0.75),
]


def fill(x):
    """The variables of the dashboard, as Grafana fills them in before it sends a query."""
    if isinstance(x, str):
        for name, value in VARIABLES.items():
            x = x.replace(name, value)
        return x
    if isinstance(x, dict):
        return {k: fill(v) for k, v in x.items()}
    if isinstance(x, list):
        return [fill(v) for v in x]
    return x


def panels(items, row=""):
    for p in items:
        if p["type"] == "row":
            row = p["title"]
            yield from panels(p.get("panels", []), row)
        else:
            yield row, p


def run(grafana, targets, minutes=3):
    now = int(time.time() * 1000)
    body = {"from": str(now - minutes * 60000), "to": str(now),
            "queries": [{**t, "intervalMs": 10000, "maxDataPoints": 500} for t in targets]}
    request = urllib.request.Request(grafana + "/api/ds/query", json.dumps(body).encode(),
                                     {"Content-Type": "application/json"})
    try:
        return json.load(urllib.request.urlopen(request, timeout=30))
    except urllib.error.HTTPError as e:
        return json.load(e)


def series(result, ref):
    """Each series of the answer to one query: its name, and its values that are not null."""
    out = []
    for frame in result.get("results", {}).get(ref, {}).get("frames", []):
        last = frame["schema"]["fields"][-1]
        name = frame["schema"].get("name") or last.get("config", {}).get("displayNameFromDS") or last["name"]
        values = frame["data"]["values"]
        out.append((name, [v for v in values[-1] if v is not None] if values else []))
    return out


def main():
    board, grafana = json.load(open(sys.argv[1])), sys.argv[2].rstrip("/")
    seen, failures, count = {}, [], 0
    for row, panel in panels(board["panels"]):
        targets = fill(panel["targets"])
        result = run(grafana, targets)
        for target in targets:
            count += 1
            where = f"{row} / {panel['title']} [{target['refId']}]"
            answer = result.get("results", {}).get(target["refId"], {})
            if "error" in answer or "message" in result:
                failures.append(f"{where}: {(answer.get('error') or result.get('message'))[:200]}")
                continue
            found = series(result, target["refId"])
            # a query that only an expression reads is not sent back
            if not target.get("hide") and not any(values for _, values in found):
                failures.append(f"{where}: nothing came back")
            seen[(row, panel["title"], target["refId"])] = found
    print(f"{count} queries run through Grafana")
    for row, title, ref, word, want in EXPECTED:
        chosen = [values for name, values in seen.get((row, title, ref), []) if word is None or word in name]
        # the middle of the range: its ends are of intervals that are not whole
        middle = chosen[0][2:-2] if chosen and len(chosen[0]) > 6 else []
        if not middle:
            failures.append(f"{row} / {title} [{ref}] {word or ''}: too little to compare")
            continue
        got = sum(middle) / len(middle)
        ok = abs(got - want) <= 0.03 * want
        print(f"  {'ok  ' if ok else 'WRONG'} {title} [{ref}] {word or ''}: {got:.2f}, sent {want}")
        if not ok:
            failures.append(f"{row} / {title} [{ref}] {word or ''}: {got:.2f}, and {want} was sent")
    for failure in failures:
        print("FAILED", failure)
    print("All queries run, and the figures are those that were sent" if not failures else f"{len(failures)} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
