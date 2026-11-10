# Memory of the server under the stress test, 2026-11-09

The memory of a production store went up after the cache changes of this
branch: on one server the "GHC heap" went from 828 to 2,326 MB, and the whole
process from 4,018 to 4,926 MB. These runs looked for a leak. There is none.
Most of the growth was memory the GHC heap had already given back, which
Linux still counted as the process's. The rest was RocksDB filling its caches
up to their configured limits.

- **Code:** haskoin-store `bd22ad11` (the incremental reloads), haskoin-store-data
  `b304acd2`.
- **Machine:** AMD Ryzen 9 3900X, 32 GiB, rootless Podman.
- **Run:** a shorter stress run than the full ones: 4 wallets of 200 receiving and
  200 change addresses, 60,000 transactions, 2,000 per block, with wrk on the
  xpubs throughout. Each run took about seven minutes.

| Directory | Server options | What it is for |
|---|---|---|
| [profile](profile/report.md) | `+RTS -N4 -M2g -T -hT -i10` | heap profile by type, [heap.hp.gz](profile/heap.hp.gz) |
| [default](default/report.md) | `+RTS -N4 -M2g` | memory as the server ran until now |
| [dontneed](dontneed/report.md) | `+RTS -N4 -M2g --disable-delayed-os-memory-return` | memory with the fix |

Each directory has the files of a stress run except the server log.
`samples.jsonl.gz` has the memory of the server once a second:
- `rss_mb` is the whole process;
- `heap_mb` is the part of it in the address range of the GHC heap;
- `heap_lazy_free_mb` is how much of that the heap has already given back.

## No leak: the live heap stays flat

The heap profile counts the bytes of every live closure every ten seconds of
CPU. Summarised with `scripts/heap-profile-summary.py`:

- the live heap stayed between 6 and 13 MiB for the whole run, with no trend;
- no closure type grew by more than 0.1 MiB between the first fifth of the run
  and the last;
- the largest type at the end was byte arrays (`ARR_WORDS`, 4.2 MiB, the
  bytes of serialized data and buffers), then thread stacks (0.7 MiB); the
  balances of addresses were under 0.2 MiB.

A leak would show as a heap that keeps growing, carried by a few types. Here
the live data at the end was the size it was at the start.

## Where the memory went

In the default run, the part of the process in the GHC heap's address range
grew from 45 to 261 MiB while the live heap stayed at 10 MiB. Read from
`/proc/<pid>/smaps`:

| Seconds | Whole process MiB | GHC heap range MiB | of which given back (`LazyFree`) |
|---|---|---|---|
| 1 | 100 | 45 | 0 |
| 59 | 197 | 93 | 40 |
| 146 | 333 | 174 | 113 |
| 296 | 463 | 261 | 198 |
| 386 | 487 | 261 | 186 |

**Why freed memory still counts.** When the garbage collector no longer needs
a block of memory, GHC by default returns it to the system with `MADV_FREE`.
That tells the kernel it may take the pages whenever it runs short. Until it
does, the pages stay mapped and count as resident, in `ps`, `top`, cgroup
statistics and monitoring. So after one large burst of allocation, the heap
looks as large as that burst for as long as the machine has memory to spare.

**What the bursts are.** They come from full reloads of the cache, which hold
a whole xpub in memory for a moment: its transactions and unspent outputs,
from the database and from Redis. The heap range grew in the windows with
reloads of seconds (see "Over time" in [default/report.md](default/report.md)),
though not in every such window: it only grows when a burst is larger than
every burst before it. Larger wallets than these make larger bursts. That
would explain a 2.3 GB "GHC heap" on a production server with a
modest live heap.

**The rest of the process.** The memory outside the GHC heap's range also grew,
by about 170 MiB in the default run and about 185 MiB in the run with the fix.
That is RocksDB's block cache and memtables, which grow until they reach the
limits set in its options and then stop. It is not something this branch
changed.

## The fix: return memory at once

`--disable-delayed-os-memory-return` makes GHC return memory with
`MADV_DONTNEED` instead. The pages leave the process at once, and its resident
size is what it uses.

| Seconds | Whole process MiB | GHC heap range MiB | of which given back |
|---|---|---|---|
| 1 | 101 | 47 | 0 |
| 59 | 192 | 77 | 0 |
| 156 | 222 | 50 | 0 |
| 289 | 260 | 51 | 0 |
| 377 | 310 | 72 | 0 |

The heap still grows during a large full reload. In the means over windows
of about 30 seconds in [dontneed/report.md](dontneed/report.md), it reached
231 and 240 MiB twice. But each time it fell back to 50–90 MiB once the
reload was over, instead of staying at the high-water mark. At the end the
whole process was 310 MiB against 487 MiB with the default.

The flag costs the kernel a little more work when the heap grows again,
because the pages have to be mapped afresh. Server CPU and reload times in
the two runs were within the noise of each other. The executable now has the
flag in its default RTS options (`package.yaml`), so a store needs no option
to get it.

## How to check a running store

Add up the `Rss` and `LazyFree` lines of the mappings in the GHC heap's range,
which starts at address `4200000000`, in `/proc/<pid>/smaps`. If most of a
large heap is `LazyFree`, it is this effect, not a leak. A store built before
the fix can get the same behaviour with
`+RTS --disable-delayed-os-memory-return -RTS`. The README's section on
monitoring has more on reading the memory of a store.
