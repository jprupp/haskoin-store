# Congestion control for the requests of a store

How a store decides how many long requests to answer at once. The code is in
`Haskoin.Store.Web.Admission` (the slots and the line) and
`Haskoin.Store.Web.Congestion` (the signal and the controller).

## The short answer

- **The problem.** A store asked for more than its cores can do slows down
  for everyone at once, the quick requests included. Letting in only so many
  long requests at a time fixes that. The question is how many.
- **The answer.** The store measures how long a thread that is ready to run
  waits for a core. While that wait is short and every slot is in use, it
  lets in one more long request. When the wait grows too long, it lets in a
  quarter fewer. This is how TCP has shared links since 1988.
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

Additive increase, multiplicative decrease, as in TCP and LEDBAT:

- **Over the target** (25 ms): the limit is cut by a quarter. There is no
  second cut while more requests run than the limit allows, so one
  congestion is answered once.
- **Under the target, with every slot used:** one more slot. An idle store
  does not drift up to the ceiling.

The limit stays between a floor (one a core) and a ceiling (sixteen a
core). A floor equal to the ceiling is a fixed cap.

The target cannot be much lower than GHC's time slice of 20 ms: a thread
behind one other that computes waits that long, which is normal scheduling.

## The line, and CoDel

A good queue absorbs a burst and empties again. A bad one stands, and every
request waits for nothing. CoDel tells the two apart by the shortest wait of
the requests let in during an interval of 100 ms. If even that was over
5 ms, the line stood all along, and it is *overloaded* until the end of the
next interval. While it is overloaded, a request that finds no free slot is
refused at once, and one that has waited over 10 ms is refused.

Otherwise a request waits until it gets a slot, for at most
`--request-wait`, and a line of `--request-queue` takes no more.

A refused request gets 503 with `{"error":"server-busy"}` and
`Retry-After: 1`. It costs the store almost nothing, and a load balancer
learns at once that this store is full.

A request in line sets no timer. GHC's timer manager was seen blocked for
seconds under load, so the waits are enforced by a sweep ten times a second.
A slot that comes free goes straight to the first in line.

## Configuration

| Option | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--max-requests` | `MAX_REQUESTS` | 16 a core | Ceiling of the limit; 0 turns the control off |
| `--min-requests` | `MIN_REQUESTS` | 1 a core | Floor of the limit, where it starts |
| `--target-delay` | `TARGET_DELAY` | 25 ms | Delay of the scheduler that counts as congestion |
| `--request-queue` | `REQUEST_QUEUE` | 64 | Most requests that may wait |
| `--request-wait` | `REQUEST_WAIT` | 1 s | Longest wait while the line moves |

The step of the controller (half a second, one up, a quarter down) and
CoDel's figures (5 ms over 100 ms) are fixed in the code. They are the usual
values of these algorithms, and none depends on the machine.

## What to watch

StatsD gauges and counters under `web.cap.`:

- `limit`: a sawtooth under load, the floor when idle;
- `delay_us` and `lateness_us`: the signal, without and with the
  collector's pauses;
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

## Further reading

- Van Jacobson, *Congestion Avoidance and Control*, 1988.
- Nichols and Jacobson, *Controlling Queue Delay*, ACM Queue, 2012 (CoDel).
- RFC 6817 (LEDBAT) and RFC 7661 (congestion window validation).
