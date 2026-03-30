#ifndef __HFAIRLOCK__
#define __HFAIRLOCK__

#ifdef __cplusplus
extern "C" {
#endif

#define _GNU_SOURCE
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sched.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <linux/futex.h>
#include <pthread.h>
#include "../rdtsc.h"
#include "../hscl_common.h"

typedef unsigned long long ull;

#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define MIN(a, b) ((a) < (b) ? (a) : (b))

#ifndef MAX_DEPTH
#define MAX_DEPTH 8
#endif

#ifndef FAIRLOCK_GRANULARITY
#define FAIRLOCK_GRANULARITY (CYCLE_PER_MS * 20L)
#endif

#ifdef DEBUG
typedef struct stats {
    ull reenter;
    ull banned_time;
    ull start;
    ull next_runnable_wait;
    ull prev_slice_wait;
    ull own_slice_wait;
    ull runnable_wait;
    ull succ_wait;
    ull release_succ_wait;
} stats_t;
#endif

typedef struct node {
    int id;
    ull cs;
    int parent;
    int weight;
    ull slice;
    ull banned_until;
} node_t;

/*
 * BUG FIX 1 — path[] initialisation sentinel.
 * path[] is filled by set_path() right-to-left. Slots that are not
 * written (depth < MAX_DEPTH) were left as 0, causing every loop that
 * iterates the full MAX_DEPTH to visit node 0 multiple times and
 * double-charge its weight and ban.  We add a depth field so callers
 * can iterate only the valid prefix.
 */
typedef struct flthread_info {
    ull banned_until;
    ull weight;
    ull slice;
    ull start_ticks;
    int banned;
    int parent;
    int depth;          /* FIX 1: actual number of valid entries in path[] */
#ifdef DEBUG
    stats_t stat;
#endif
    int path[MAX_DEPTH];
} flthread_info_t;

enum qnode_state {
    INIT = 0,
    NEXT,
    RUNNABLE,
    RUNNING
};

typedef struct qnode {
    int state __attribute__ ((aligned (CACHELINE)));
    struct qnode *next __attribute__ ((aligned (CACHELINE)));
} qnode_t __attribute__ ((aligned (CACHELINE)));

typedef struct hfairlock_t {
    qnode_t *qtail __attribute__ ((aligned (CACHELINE)));
    qnode_t *qnext __attribute__ ((aligned (CACHELINE)));
    ull slice __attribute__ ((aligned (CACHELINE)));
    int slice_valid __attribute__ ((aligned (CACHELINE)));
    pthread_key_t flthread_info_key;
    ull total_weight;
    node_t *hierarchy;
} hfairlock_t __attribute__ ((aligned (CACHELINE)));

static inline qnode_t *flqnode(hfairlock_t *lock) {
    return (qnode_t *) ((char *) &lock->qnext - offsetof(qnode_t, next));
}

static inline int futex(int *uaddr, int futex_op, int val,
                        const struct timespec *timeout) {
    return syscall(SYS_futex, uaddr, futex_op, val, timeout, NULL, 0);
}

int hfairlock_init(hfairlock_t *lock, node_t *hierarchy);
flthread_info_t *flthread_info_create(hfairlock_t *lock, int weight);
void set_path(hfairlock_t *lock, int path[], int *depth_out,
              int parent, int weight, ull banned_until);
void hfairlock_thread_init(hfairlock_t *lock, int weight, int parent);
int hfairlock_destroy(hfairlock_t *lock);
ull get_updated_ban(hfairlock_t *lock, int parent, ull banned_until);
int is_reacquired(hfairlock_t *lock, int parent);
ull set_slice(hfairlock_t *lock, flthread_info_t *info);
void hfairlock_acquire(hfairlock_t *lock);
ull hfairlock_release(hfairlock_t *lock);

#ifdef __cplusplus
}
#endif

#endif /* __HFAIRLOCK__ */