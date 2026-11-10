#!/usr/bin/env python3
"""Summarise a GHC heap profile (.hp, from +RTS -hT): the live heap over time,
and the closure types that grew most between the start and the end of the run.

A leak shows as a heap that keeps growing and a few closure types that grow
with it. A flat profile while the process grows means the growth is outside
the live heap: memory the heap holds but does not use, or memory that is not
the heap's at all. stress/results/2026-11-09-memory/README.md is an example.

Usage: scripts/heap-profile-summary.py FILE.hp
"""

import sys
from collections import defaultdict


def read_samples(path):
    """The samples of a profile, as (time, {closure type: bytes})."""
    samples = []
    cur = None
    t = 0.0
    with open(path, errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("BEGIN_SAMPLE"):
                t = float(line.split()[1])
                cur = {}
            elif line.startswith("END_SAMPLE"):
                if cur:
                    samples.append((t, cur))
                cur = None
            elif cur is not None and "\t" in line:
                name, val = line.rsplit("\t", 1)
                cur[name] = cur.get(name, 0) + int(val)
    return samples


def average(samples):
    acc = defaultdict(float)
    for _, d in samples:
        for k, v in d.items():
            acc[k] += v / len(samples)
    return acc


def main():
    samples = read_samples(sys.argv[1])
    if not samples:
        sys.exit("no samples")
    print(f"{len(samples)} samples; times are seconds of CPU of the mutator, not of the clock")
    step = max(1, len(samples) // 15)
    for tm, d in samples[::step] + [samples[-1]]:
        print(f"t={tm:7.0f}s  heap={sum(d.values()) / 2**20:8.1f} MiB")
    # averages of a fifth of the samples, so that one busy moment does not
    # pass for growth
    n = max(1, len(samples) // 5)
    first, last = average(samples[:n]), average(samples[-n:])
    growth = sorted(((last[k] - first.get(k, 0), k) for k in last), reverse=True)
    print("\ngrowth from the first fifth to the last fifth of the run (MiB):")
    for g, k in growth[:25]:
        print(f"{g / 2**20:9.1f}  last {last[k] / 2**20:8.1f}  {k[:90]}")


if __name__ == "__main__":
    main()
