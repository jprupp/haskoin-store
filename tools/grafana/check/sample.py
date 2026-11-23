#!/usr/bin/env python3
"""The metrics for Sender.hs to send: a kind and a key on each line.

They are those that the dashboard reads (haskoin_grafana.py --metrics), as the store names them,
with dots. Where the dashboard reads every kind of request, every long route or every reason,
a few of each stand for all.
"""
import subprocess
import sys
from pathlib import Path

GENERATOR = Path(__file__).resolve().parent.parent / "haskoin_grafana.py"
KINDS = ["address_balance", "xpub_balances", "transaction", "block", "mempool"]
LONG_ROUTES = ["xpub_balances", "blockchain_multiaddr"]
REASONS = ["unanswered", "node_has_not"]
# The first words of a name, which the dashboard has with underscores and the store with dots.
STARTS = [("web_cap_", "web.cap."), ("web_", "web."), ("store_", "store."), ("db_", "db."),
          ("ingest_given_up_", "ingest.given_up."), ("ingest_", "ingest."), ("fleet_cache_", "fleet.cache."),
          ("rts_", "rts.")]


def dotted(key):
    for underscored, with_dots in STARTS:
        if key.startswith(underscored):
            return with_dots + key[len(underscored):]
    return key


def main():
    listed = subprocess.run([sys.executable, str(GENERATOR), "--metrics"], capture_output=True, text=True,
                            check=True).stdout
    metrics = set()
    for line in listed.strip().split("\n"):
        kind, key = line.split(" ", 1)
        if "${route}" in key:
            continue
        if key.startswith("web_("):
            metrics.update((kind, "web." + k) for k in KINDS)
        elif key.startswith("web_cap_(.+)_"):
            metrics.update((kind, f"web.cap.{r}.{key[len('web_cap_(.+)_'):]}") for r in LONG_ROUTES)
        elif key.startswith("ingest_given_up_("):
            metrics.update((kind, "ingest.given_up." + r) for r in REASONS)
        else:
            metrics.add((kind, dotted(key)))
    for kind, key in sorted(metrics):
        print(kind, key)


if __name__ == "__main__":
    main()
