# Congestion control for the requests of a store

How a store decides how many long requests to answer at once. The code is
in `Haskoin.Store.Web.Admission` (the slots and the line) and
`Haskoin.Store.Web.Congestion` (the signal and the controller).

## In short

A store that is asked for more than its cores can do slows down for
everyone at once, the quick requests included. So it answers only so many
long requests at a time, and the rest wait in a line. No fixed number is
right, so the store measures two things: how long a thread that is ready
to run waits for a core, and how much of its cores it uses. It lets in
more while the cores have room, and a quarter fewer when they fall
behind, the way TCP shares a link (CUBIC). The line absorbs a burst, and
refuses requests at once when it stops emptying, the way Linux manages
the queues of its network links (CoDel). The cost: under overload some
long requests are refused, and fewer of those that compute are answered.

## Words used here

The shared words, such as store and xpub, are in [README.md](README.md).

| Word | What it means here |
| --- | --- |
| **long request** | A request for a list, a whole block or an xpub. Only these are limited. |
| **slot** | The right to run one long request. A request holds its slot until its last byte is sent. |
| **the limit** | How many slots there are now. It moves between the **floor** and the **ceiling**. |
| **the line** | The long requests that wait for a slot, first come first. |
| **admission** | Letting a long request in: it gets a slot, at once or after a wait in the line. |
| **capability** | GHC's word for a core that runs Haskell threads. |
| **probe** | A small thread on each capability that notes how late it is woken: its **lateness**. |
| **the signal**, **the delay** | How long a thread that is ready to run waits for a core, as the probes measure it. |
| **the target** | The delay that counts as congestion: 25 ms. |
| **the controller** | The loop that looks at the signal every half second and moves the limit. |
| **round trip** | How long a request holds its slot, smoothed. |
| **CUBIC** | The rule by which TCP sets how much it sends. After a loss it sends less, climbs fast back toward the old rate, slows down near it, then tries more. |
| **CoDel** | A rule for queues. A queue that does not empty in a while is a bad one, and what waits in it is turned away. |
| **overloaded** | What the line is when CoDel finds that it does not empty. |

## What is limited

Each route has a cost (`Haskoin.Store.Web.Router.Cost`):

| Cost | Routes | Held? |
| --- | --- | --- |
| **Light** | Answer from memory, like `/health`. | Never. |
| **Short** | Look up one thing by its key: a transaction, a balance. | No. They are what most clients ask for, and they are quick. |
| **Long** | Answer lists, whole blocks and xpubs. | Each takes a slot while it runs, until its last byte is sent. |

## Why a fixed number is wrong

| A request that | Wants | Why |
| --- | --- | --- |
| waits for RocksDB on the disk | many slots | It gives its core back while it waits. The disk of a production store gave its most with 32 reads in flight. |
| computes | about one slot a core | More of them only make every thread wait longer for its turn, the health check's included. |

No fixed number suits both, and the mix changes with the clients.

## An example

A store on four cores. Its limit starts at the floor, 8 slots, and may
reach the ceiling, 64. The figures follow from the rules below. They are
an illustration, not a measurement.

1. Thirty clients ask for long lists that mostly wait for the disk. Eight
   run, and 22 wait in the line.
2. The cores have room and the delay is short, so the limit grows: 8
   after one second, 11 after two, 18 after three and 33 after four. Now
   all thirty run, and the limit stays.
3. The clients switch to requests that compute. The cores are at 100%,
   and the delay goes over 25 ms. The limit is cut by a quarter, from 33
   to 25. One round trip later, if the delay is still over 25 ms, it is
   cut to 18. And so on, down to 8 at the lowest.
4. The requests over the limit wait in the line. If the line stops
   emptying, the next request that finds no free slot gets 429 at once.
5. All along, `/health` and a request for one transaction take no slot.

## The signal: how late a woken thread runs

Each capability of GHC has a probe that waits for a bell. The bell rings
every 10 ms. It is rung from outside GHC, through an eventfd, so the
probe waits as a connection waits for its socket. The probe notes how
late it ran. That lateness is the length of the capability's queue of
ready threads, in time, as a request woken by the network finds it.

- It rises only when the cores have more to do than they can.
- A thread that waits for the disk in a safe foreign call is not in the
  queue, so it does not raise the signal.
- The pauses of GHC's collector are taken out. They stop idle cores as
  much as busy ones, and fewer slots would not shorten them.

Every half second the controller takes the 90th percentile of the
lateness. It is weighted by time, so that every capability counts alike.
That is the delay.

The latency of the requests is not the signal. The long routes differ by a
factor of a thousand in cost, so a slow request may just be a big one.

## The controller

Every half second the controller looks at the delay and at the use of the
cores. The use of the cores is the CPU time of the store, all its threads
included, over that of its capabilities, smoothed over about two seconds.

| What it sees | What the limit does | Why |
| --- | --- | --- |
| The delay over the target (25 ms), with the cores at 90% or more | It is cut by a quarter, at most once a round trip. | A cut takes effect as requests end. A second one before that would answer one congestion twice. |
| The delay under the target, every slot used, and the cores under 90% | It grows along the curve of TCP CUBIC: fast back toward the level it was last cut from, slower near it, then faster past it. | The store finds again the level that worked. The clock of the curve runs only while growth is allowed. |
| Anything else | It stays. | An idle store does not drift up to the ceiling, and a store whose cores are busy gains nothing from more slots. |

**The delay alone does not cut.** Requests that wait for the disk come
back to the cores in bursts. The probes then read over the target for a
moment, while the cores are idle the rest of the time. Cutting on those
bursts held the limit under the number of clients on a slow disk, and
cost answers. A store that is really behind uses its cores to the full.

**The bounds.** The limit stays between a floor (two a core) and a
ceiling (sixteen a core). A floor equal to the ceiling is a fixed cap.

**The target** cannot be much lower than GHC's time slice of 20 ms. A
thread behind one other that computes waits that long, which is normal
scheduling.

**Rejected: one slot more a step, and a cut whenever the delay is over
the target.** That controller was tried on two cores of a production
store, with sixteen clients that each sent one request after another. It
answered a fifth to a third fewer requests than no limit. Each cut left
the cores idle, and one slot a step took seconds to fill them again.

## Requests that wait for the xpub indexer

A request for an xpub that is not in the cache waits for the indexer, up
to `--xpub-wait`. It gives its slot back while it waits. When the xpub is
in the cache it takes a slot again, past the limit if need be, without
the line.

The indexer's own threads bound that work. Holding the slot would leave a
core idle, and a few such waits of seconds would fill every slot. That
happened on a production store: plain requests waited about a second in
the line.

## The line, and CoDel

A good queue absorbs a burst and empties again. A bad one stands, and
every request waits for nothing. CoDel tells the two apart:

1. It looks at intervals of 100 ms.
2. In each, it takes the shortest wait of the requests let in.
3. If even that was over 5 ms, the line stood all along. It is
   *overloaded* until the end of the next interval.

While the line is overloaded, a request that finds no free slot is
refused at once, and one that has waited over 10 ms is refused.

**CoDel holds its fire while the limit is free to rise**: the cores under
90% and the limit under its ceiling. A line that stands then waits for
the limit to catch up with the load, as after a quiet spell. It does not
mean that the store is behind. Refused, each client would wait a second
for nothing.

**Otherwise a request waits until it gets a slot**, for at most
`--request-wait`, and a line of `--request-queue` takes no more.

**A refused request gets 429** with `{"error":"server-busy"}` and
`Retry-After: 1`. It costs the store almost nothing, and a load balancer
learns at once that this store is full. It is 429 because clients and
proxies take a 429 with `Retry-After` to mean "come back later". It is
the answer for an xpub that is not ready too, and that of nginx with
`limit_conn_status 429`. A load generator on a production store retried
each 503 at once, and the refusals became a storm of six thousand a
second.

**A request in line sets no timer.** GHC's timer manager was seen blocked
for seconds under load. So a sweep enforces the waits, ten times a
second. A slot that comes free goes straight to the first in line.

## Configuration

| Option | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--max-requests` | `MAX_REQUESTS` | 16 a core | Ceiling of the limit; 0 turns the control off |
| `--min-requests` | `MIN_REQUESTS` | 2 a core | Floor of the limit, where it starts |
| `--target-delay` | `TARGET_DELAY` | 25 ms | Delay of the scheduler that counts as congestion |
| `--request-queue` | `REQUEST_QUEUE` | 64 | Most requests that may wait |
| `--request-wait` | `REQUEST_WAIT` | 1 s | Longest wait while the line moves |

Two sets of figures are fixed in the code: the step of the controller
(half a second, a quarter down, CUBIC's curve up, 90% of the cores) and
CoDel's (5 ms over 100 ms). They are the usual values of these
algorithms, and none depends on the machine.

## What to watch

StatsD gauges and counters under `web.cap.`:

| Name | What it shows |
| --- | --- |
| `limit` | The limit: a sawtooth under load, the floor when idle |
| `delay_us`, `lateness_us` | The signal, without and with the collector's pauses |
| `used`, `round_trip_us` | The use of the cores and the round trip that the controller sees |
| `overloaded` | 1 while CoDel refuses |
| `sojourn` | How long admitted requests waited |
| `refused_full`, `refused_waited`, `refused_codel` | Refusals by reason |
| `<route>.running`, `<route>.waited`, `<route>.held`, `<route>.refused` | For each long route: slots held now, the wait for a slot, the time a slot was held, and refusals |

- A limit stuck at the ceiling with long waits points at the disk.
- A limit stuck at the floor with a high delay points at work that is not
  limited, such as short requests or another process.

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
percentile, against 940 ms with no limit. The price was a fifth fewer
long requests that compute. With requests that wait for a disk, the limit
grew, and answered within 2% of no limit.

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

The heavy load waited `Retry-After` on each 429.

Two rules above matter here: the delay alone does not cut, and CoDel
waits while the limit may rise. Without them, the disk-bound closed loop
answered 33–42/s, with the cores at 63%. The limit was under the number
of clients a third of the time. The line refused about two requests a
second, and each refusal kept its client away for a second.

## Further reading

- Van Jacobson, *Congestion Avoidance and Control*, 1988.
- Nichols and Jacobson, *Controlling Queue Delay*, ACM Queue, 2012 (CoDel).
- RFC 6817 (LEDBAT) and RFC 7661 (congestion window validation).
