# Congestion control for the requests of a store

How a store decides how many long requests to answer at once. The code is in
`Haskoin.Store.Web.Admission` (the slots and the line) and
`Haskoin.Store.Web.Congestion` (the signal and the controller).

## The short answer

- **The problem.** A store asked for more than its cores can do slows down
  for everyone at once, the quick requests included. Letting in only so many
  long requests at a time fixes that. The question is how many.
- **The answer.** The store measures how long a thread that is ready to run
  waits for a core, and how much of its cores it uses. While that wait is
  short, the cores have room and every slot is in use, it lets in more long
  requests. When the wait grows too long and the cores are busy, it lets in a
  quarter fewer, at most once a round trip. This is how TCP shares links
  (CUBIC).
- **The line** in front of the slots is managed the way Linux manages the
  queues of its network links (CoDel): it absorbs bursts, and refuses
  requests at once when it stops emptying.

## What is limited

Each route has a cost (`Haskoin.Store.Web.Router.Cost`):

- **Light** routes answer from memory, like `/health`. They are never held.
- **Short** routes look up one thing by its key: a transaction, a balance.
  They are not held either: they are what most clients ask for, and they
  are quick.
- **Long** routes answer lists, whole blocks and xpubs. Each takes a slot
  while it runs, until its last byte is sent.

## Why a fixed number is wrong

A request that waits for RocksDB on the disk gives its core back while it
waits. The disk of a production store gave its most with 32 reads in
flight, so such requests want many slots. A request that computes wants
about one slot a core: more of them only make every thread wait longer for
its turn, the health check's included. No fixed number suits both, and the
mix changes with the clients.

## The signal: how late a woken thread runs

A probe on each capability of GHC waits for a bell that rings every 10 ms
from outside GHC, through an eventfd, the way a connection waits for its
socket. It notes how late it ran. That lateness is the length of the
capability's queue of ready threads, in time, as a request woken by the
network finds it.

- It rises only when the cores have more to do than they can.
- A thread that waits for the disk in a safe foreign call is not in the
  queue, so it does not raise the signal.
- The pauses of GHC's collector are taken out: they stop idle cores as much
  as busy ones, and fewer slots would not shorten them.

Every half second the controller takes the 90th percentile of the lateness,
weighted by time so that every capability counts alike.

The latency of the requests is not the signal. The long routes differ by a
factor of a thousand in cost, so a slow request may just be a big one.

## The controller

Every half second the controller looks at the delay and at the use of the
cores: the CPU time of the store, all its threads included, over that of
its capabilities, smoothed over about two seconds.

- **Over the target** (25 ms) **with the cores at 90%:** the limit is cut
  by a quarter, at most once a round trip. The round trip is how long a request holds its slot,
  smoothed. A cut takes effect as requests end; a second one before that
  would answer one congestion twice.
- **Under the target, every slot used, and the cores under 90%:** the limit
  grows along the curve of TCP CUBIC. It climbs fast back toward the level
  it was last cut from, slows down near it, then speeds up past it. The
  clock of the curve runs only while growth is allowed.
- **Otherwise** it stays. An idle store does not drift up to the ceiling,
  and a store whose cores are busy gains nothing from more slots.

The delay alone does not cut. Requests that wait for the disk come back
to the cores in bursts, and the probes read over the target for a moment
while the cores are idle the rest of the time. Cutting on those bursts held
the limit under the number of clients on a slow disk, and cost answers. A
store that is really behind uses its cores to the full.

The limit stays between a floor (two a core) and a ceiling (sixteen a
core). A floor equal to the ceiling is a fixed cap.

The first controller added one slot a step and cut whenever the delay was
over the target. On two cores of a production store it answered a fifth to
a third fewer requests than no limit to sixteen clients that asked one
after the other: each cut left the cores idle, and one slot a step took
seconds to fill them again.

The target cannot be much lower than GHC's time slice of 20 ms: a thread
behind one other that computes waits that long, which is normal scheduling.

## Requests that wait for the xpub indexer

A request for an xpub that is not in the cache waits for the indexer, up to
`--xpub-wait`. It gives its slot back while it waits, and takes one again
when the xpub is in the cache, past the limit if need be, without the line.
The indexer's own threads bound that work. Holding the slot would leave a
core idle, and a few such waits of seconds would fill every slot: on a
production store, plain requests then waited about a second in the line.

## The line, and CoDel

A good queue absorbs a burst and empties again. A bad one stands, and every
request waits for nothing. CoDel tells the two apart by the shortest wait of
the requests let in during an interval of 100 ms. If even that was over
5 ms, the line stood all along, and it is *overloaded* until the end of the
next interval. While it is overloaded, a request that finds no free slot is
refused at once, and one that has waited over 10 ms is refused.

CoDel holds its fire while the limit is free to rise: the cores under
90% and the limit under its ceiling. A line that stands then waits for the
limit to catch up with the load, as after a quiet spell; it does not mean
that the store is behind. Refused, each client would wait a second for
nothing.

Otherwise a request waits until it gets a slot, for at most
`--request-wait`, and a line of `--request-queue` takes no more.

A refused request gets 429 with `{"error":"server-busy"}` and
`Retry-After: 1`. It costs the store almost nothing, and a load balancer
learns at once that this store is full. It is 429, as for an xpub that is
not ready and as nginx's `limit_conn_status 429`, because clients and
proxies take a 429 with `Retry-After` to mean "come back later". A load
generator on a production store retried each 503 at once, and the refusals
became a storm of six thousand a second.

A request in line sets no timer. GHC's timer manager was seen blocked for
seconds under load, so the waits are enforced by a sweep ten times a second.
A slot that comes free goes straight to the first in line.

## Configuration

| Option | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--max-requests` | `MAX_REQUESTS` | 16 a core | Ceiling of the limit; 0 turns the control off |
| `--min-requests` | `MIN_REQUESTS` | 2 a core | Floor of the limit, where it starts |
| `--target-delay` | `TARGET_DELAY` | 25 ms | Delay of the scheduler that counts as congestion |
| `--request-queue` | `REQUEST_QUEUE` | 64 | Most requests that may wait |
| `--request-wait` | `REQUEST_WAIT` | 1 s | Longest wait while the line moves |

The step of the controller (half a second, a quarter down, CUBIC's curve
up, 90% of the cores) and CoDel's figures (5 ms over 100 ms) are fixed in the code. They are the usual
values of these algorithms, and none depends on the machine.

## What to watch

StatsD gauges and counters under `web.cap.`:

- `limit`: a sawtooth under load, the floor when idle;
- `delay_us` and `lateness_us`: the signal, without and with the
  collector's pauses;
- `used` and `round_trip_us`: the use of the cores and the round trip
  that the controller sees;
- `overloaded`: 1 while CoDel refuses;
- `sojourn`: how long admitted requests waited;
- `refused_full`, `refused_waited`, `refused_codel`: refusals by reason;
- for each long route, `web.cap.<route>.running`, `waited`, `held` and
  `refused`.

A limit stuck at the ceiling with long waits points at the disk. One stuck
at the floor with a high delay points at work that is not limited, such as
short requests or another process.

## What it does not do

- **Short requests are not limited.** A flood of balances is answered as it
  comes.
- **A full disk is not seen** by the signal. The ceiling bounds it.
- **It does not keep clients apart.** Limits per client belong in the proxy
  in front, such as nginx.

## How it performs

`haskoin-store-bench admission` compares no limit, a fixed limit and the
adaptive one, with requests that compute, wait for a disk, or switch. On
four busy cores, under overload, a light request took 53 ms at the 99th
percentile against 940 ms with no limit, for a fifth fewer long requests
that compute. With requests that wait for a disk, the limit grew and
answered within 2% of no limit.

The controller against no limit, on a store of the benchmark
(`haskoin-store-bench`, three alternated pairs). In memory, on four
capabilities:

| Load | Control on | Control off |
| --- | --- | --- |
| Overload, 64 heavy connections: simple p50 / p99 | 18–19 / 53–69 ms | 105–109 / 159–164 ms |
| Overload: heavy answered | 54/s, plus 42–43 refusals (429) a second | 50–52/s |
| Closed loop, 16 connections, mixed | 108–111/s | 107–111/s |
| Moderate, 4 heavy connections: simple p50 / p99 | 4 / 15–16 ms | 4 / 15–16 ms |

Bound by the disk, on two capabilities: a block cache of 1 MB, the
database dropped from the page cache every 50 ms, and 3,500 reads a second:

| Load | Control on | Control off |
| --- | --- | --- |
| Closed loop, 16 connections, mixed | 38–43/s | 40–43/s |
| Overload: simple p50 / p99 | 30–32 / 144–152 ms | 85–88 / 318–487 ms |
| Overload: heavy answered | 24–26/s, plus 6–7 refusals a second | 26–27/s |

The heavy load waited `Retry-After` on each 429. Before the delay needed
busy cores to cut and CoDel waited for the limit to rise, the disk-bound
closed loop answered 33–42/s, with the cores at 63%: the limit was under
the number of clients a third of the time, and the line refused about two
requests a second, each of which kept its client away for a second.

## Further reading

- Van Jacobson, *Congestion Avoidance and Control*, 1988.
- Nichols and Jacobson, *Controlling Queue Delay*, ACM Queue, 2012 (CoDel).
- RFC 6817 (LEDBAT) and RFC 7661 (congestion window validation).
