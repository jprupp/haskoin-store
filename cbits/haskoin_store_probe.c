/*
 * The bell of the probes of Haskoin.Store.Web.Congestion: a thread of the
 * system, outside of GHC, that writes to one eventfd for each probe every
 * period, and notes when it wrote and how long GHC's collector had run by
 * then. The probe, a Haskell thread, waits for its eventfd in
 * threadWaitRead, as a connection waits for its socket: GHC's manager of
 * input and output sees the eventfd in epoll and wakes the probe, which
 * then waits in its capability's queue of threads that are ready, as a
 * request does. From the write to the moment the probe runs is how long a
 * request that the network woke would have waited.
 */
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/eventfd.h>
#include <time.h>
#include <unistd.h>

#include "Rts.h"

struct haskoin_probe {
    int fd;
    pthread_mutex_t lock;
    /* whether a write is waiting for the probe to take it */
    int pending;
    /* when the oldest write that the probe did not take was made, in
     * nanoseconds of the monotonic clock */
    int64_t written;
    /* nanoseconds that GHC's collector had run by then */
    int64_t collected;
};

struct haskoin_probes {
    int n;
    int period_us;
    /* whether GHC keeps its statistics (+RTS -T) */
    int stats;
    int stop;
    pthread_t thread;
    struct haskoin_probe probe[];
};

static int64_t monotonic(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}

static void add_nanos(struct timespec *t, int64_t ns)
{
    int64_t s = t->tv_nsec + ns;
    t->tv_sec += s / 1000000000;
    t->tv_nsec = s % 1000000000;
}

static void *ring(void *arg)
{
    struct haskoin_probes *ps = arg;
    struct timespec next;
    clock_gettime(CLOCK_MONOTONIC, &next);
    while (!__atomic_load_n(&ps->stop, __ATOMIC_ACQUIRE)) {
        add_nanos(&next, (int64_t)ps->period_us * 1000);
        /* a bell that fell behind starts again from now, rather than
         * ringing many times at once to catch up */
        int64_t now = monotonic();
        if ((int64_t)next.tv_sec * 1000000000 + next.tv_nsec < now) {
            next.tv_sec = now / 1000000000;
            next.tv_nsec = now % 1000000000;
        }
        while (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &next, NULL) == EINTR) {
        }
        int64_t collected = 0;
        if (ps->stats) {
            RTSStats s;
            getRTSStats(&s);
            collected = (int64_t)s.gc_elapsed_ns;
        }
        for (int i = 0; i < ps->n; i++) {
            struct haskoin_probe *p = &ps->probe[i];
            pthread_mutex_lock(&p->lock);
            if (!p->pending) {
                p->pending = 1;
                p->written = monotonic();
                p->collected = collected;
            }
            pthread_mutex_unlock(&p->lock);
            uint64_t one = 1;
            ssize_t w = write(p->fd, &one, sizeof one);
            (void)w;
        }
    }
    return NULL;
}

static void release(struct haskoin_probes *ps, int made)
{
    for (int i = 0; i < made; i++) {
        pthread_mutex_destroy(&ps->probe[i].lock);
        close(ps->probe[i].fd);
    }
    free(ps);
}

/* Make n probes that ring every period, with their eventfds in fds, and
 * start the bell; NULL if the system refused. */
struct haskoin_probes *haskoin_probes_start(int n, int period_us, int stats, int *fds)
{
    struct haskoin_probes *ps = calloc(1, sizeof *ps + (size_t)n * sizeof(struct haskoin_probe));
    if (ps == NULL)
        return NULL;
    ps->n = n;
    ps->period_us = period_us;
    ps->stats = stats;
    for (int i = 0; i < n; i++) {
        int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (fd < 0) {
            release(ps, i);
            return NULL;
        }
        ps->probe[i].fd = fd;
        pthread_mutex_init(&ps->probe[i].lock, NULL);
        fds[i] = fd;
    }
    if (pthread_create(&ps->thread, NULL, ring, ps) != 0) {
        release(ps, n);
        return NULL;
    }
    return ps;
}

/* Stop the bell and let go of everything but the eventfds, which the caller
 * closes through GHC's manager of input and output. */
void haskoin_probes_stop(struct haskoin_probes *ps)
{
    __atomic_store_n(&ps->stop, 1, __ATOMIC_RELEASE);
    pthread_join(ps->thread, NULL);
    for (int i = 0; i < ps->n; i++)
        pthread_mutex_destroy(&ps->probe[i].lock);
    free(ps);
}

/* Take the oldest write that probe i did not take, emptying its eventfd:
 * 1 with when it was made and the collector's time then, or 0 if there is
 * none, which happens when two writes were taken at once. */
int haskoin_probe_take(struct haskoin_probes *ps, int i, int64_t *written, int64_t *collected)
{
    struct haskoin_probe *p = &ps->probe[i];
    uint64_t count;
    ssize_t r = read(p->fd, &count, sizeof count);
    (void)r;
    pthread_mutex_lock(&p->lock);
    int had = p->pending;
    if (had) {
        *written = p->written;
        *collected = p->collected;
        p->pending = 0;
    }
    pthread_mutex_unlock(&p->lock);
    return had;
}
