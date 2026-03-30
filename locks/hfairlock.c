#include <hfairlock.h>

/* ============================================================
 * BUG SUMMARY — fixes applied in this file
 *
 * BUG 1 — set_path() double-visits node 0 (root)
 *   set_path() fills path[] right-to-left stopping at parent==0,
 *   then writes 0 at position i.  Slots left of i stay 0 from
 *   malloc, so every full-depth loop visits node 0 multiple times,
 *   double-charging its weight and ban.
 *   FIX: initialise all slots to -1 (sentinel), walk left-to-right
 *   after building the path, and record the actual depth.
 *   All loops now iterate info->depth, not MAX_DEPTH.
 *
 * BUG 2 — node->banned_until RMW is not atomic in hfairlock_release()
 *   node->banned_until += ... is a plain read-modify-write.  Two
 *   sibling threads releasing simultaneously can lose an update.
 *   FIX: use __atomic_fetch_add + __ATOMIC_RELAXED.
 *
 * BUG 3 — node->cs RMW is not atomic in hfairlock_release()
 *   Same problem as BUG 2 for the CS accumulator.
 *   FIX: use __atomic_fetch_add.
 *
 * BUG 4 — pmax not used to floor info->banned_until
 *   After computing pmax (max of all ancestor bans), the thread's
 *   own ban is computed independently.  A thread can reacquire
 *   while an ancestor node is still banned, defeating node-level
 *   fairness.
 *   FIX: uncomment and apply  info->banned_until = MAX(pmax, ...)
 *
 * BUG 5 — info->banned set to wrong value after give-away path
 *   In the slow path, after a thread wins the queue but decides it
 *   is node-banned and gives the lock to its successor, it sets
 *   info->banned = reacquire_slice.  reacquire_slice is 0 when the
 *   thread IS banned (no valid reacquire), so this clears the ban
 *   flag and the subsequent ban sleep is skipped.
 *   FIX: info->banned = !reacquire_slice  (invert the boolean).
 *
 * BUG 6 — get_updated_ban() short-circuit skips ancestor check
 *   If hierarchy[parent].slice > now, the function returns early
 *   without walking ancestors.  An ancestor could be banned by a
 *   sibling's release while the child's slice is still live.
 *   FIX: remove the short-circuit; always walk the full path.
 * ============================================================ */

int hfairlock_init(hfairlock_t *lock, node_t *hierarchy) {
    int rc;
    lock->qtail      = NULL;
    lock->qnext      = NULL;
    lock->total_weight = 0;
    lock->slice      = 0;
    lock->slice_valid = 0;
    lock->hierarchy  = hierarchy;
    if (0 != (rc = pthread_key_create(&lock->flthread_info_key, NULL)))
        return rc;
    return 0;
}

flthread_info_t *flthread_info_create(hfairlock_t *lock, int weight) {
    flthread_info_t *info = malloc(sizeof(flthread_info_t));
    info->banned_until = rdtsc();
    if (weight == 0) {
        int prio = getpriority(PRIO_PROCESS, 0);
        weight = prio_to_weight[prio + 20];
    }
    info->weight = weight;
    __sync_add_and_fetch(&lock->total_weight, weight);
    __sync_add_and_fetch(&lock->hierarchy[0].weight, weight);
    info->banned      = 0;
    info->slice       = 0;
    info->start_ticks = 0;
    info->depth       = 0;                      /* FIX 1 */
    /* FIX 1: initialise all path slots to sentinel -1 */
    for (int i = 0; i < MAX_DEPTH; ++i)
        info->path[i] = -1;
#ifdef DEBUG
    memset(&info->stat, 0, sizeof(stats_t));
    info->stat.start = info->banned_until;
#endif
    return info;
}

/*
 * FIX 1: set_path() — rewritten to fill path[] left-to-right and
 * record the actual depth so loops never visit uninitialised slots.
 *
 * The path is built root→leaf order:
 *   path[0] = root (node 0)
 *   path[1] = first intermediate
 *   ...
 *   path[depth-1] = direct parent of the thread
 *
 * We collect the ancestor chain first (parent→root), then reverse
 * it so the array reads root→leaf.
 */
void set_path(hfairlock_t *lock, int path[], int *depth_out,
              int parent, int weight, ull banned_until) {
    /* collect chain from thread's parent up to root */
    int chain[MAX_DEPTH];
    int len = 0;
    int p = parent;
    while (p != 0 && len < MAX_DEPTH) {
        chain[len++] = p;
        __sync_add_and_fetch(&lock->hierarchy[p].weight, weight);
        lock->hierarchy[p].banned_until = banned_until;
        p = lock->hierarchy[p].parent;
    }
    /* always include the root (node 0) */
    chain[len++] = 0;

    /* reverse into path[] so index 0 = root, index depth-1 = direct parent */
    for (int i = 0; i < len; ++i)
        path[i] = chain[len - 1 - i];

    *depth_out = len;
}

void hfairlock_thread_init(hfairlock_t *lock, int weight, int parent) {
    flthread_info_t *info =
        (flthread_info_t *) pthread_getspecific(lock->flthread_info_key);
    if (NULL != info)
        free(info);
    info = flthread_info_create(lock, weight);
    info->parent = parent;
    /* FIX 1: pass depth_out so path length is recorded */
    set_path(lock, info->path, &info->depth, parent, weight, info->banned_until);
    pthread_setspecific(lock->flthread_info_key, info);
}

int hfairlock_destroy(hfairlock_t *lock) {
    return 0;
}

/*
 * FIX 6: remove the early-exit short-circuit that skipped the
 * ancestor walk when the child node's slice was still live.
 * An ancestor can be banned by a sibling release while the child
 * slice is live — we must always walk the full path.
 */
ull get_updated_ban(hfairlock_t *lock, int parent, ull banned_until) {
    int p = parent;
    while (p != 0) {
        banned_until = MAX(banned_until, lock->hierarchy[p].banned_until);
        p = lock->hierarchy[p].parent;
    }
    /* also check root */
    banned_until = MAX(banned_until, lock->hierarchy[0].banned_until);
    return banned_until;
}

int is_reacquired(hfairlock_t *lock, int parent) {
    ull now = rdtsc();
    int p = parent;
    while (p != 0) {
        if (now > lock->hierarchy[p].slice)
            return 0;
        p = lock->hierarchy[p].parent;
    }
    return 1;
}

/*
 * FIX 1: iterate info->depth (actual path length) instead of MAX_DEPTH
 * so we never process uninitialised (-1) path entries.
 * Slice sizes decrease from root to leaf: root gets the longest slice.
 */
ull set_slice(hfairlock_t *lock, flthread_info_t *info) {
    ull now = rdtsc();
    ull min_slice = now + FAIRLOCK_GRANULARITY;
    int depth = info->depth;                     /* FIX 1 */

    for (int i = 0; i < depth; ++i) {
        int nid = info->path[i];
        if (nid < 0) continue;                   /* FIX 1: skip sentinel */
        int offset = depth - 1 - i;             /* root gets largest offset */
        if (lock->hierarchy[nid].slice <= now) {
            lock->hierarchy[nid].slice = now + (FAIRLOCK_GRANULARITY << offset);
        }
        min_slice = MIN(lock->hierarchy[nid].slice, min_slice);
    }
    info->start_ticks = now;
    return min_slice;
}

void hfairlock_acquire(hfairlock_t *lock) {
    flthread_info_t *info;
    ull now;
    ull banned_until;

    info = (flthread_info_t *) pthread_getspecific(lock->flthread_info_key);
    if (NULL == info) {
        info = flthread_info_create(lock, 0);
        pthread_setspecific(lock->flthread_info_key, info);
    }

    if (readvol(lock->slice_valid)) {
        ull curr_slice = lock->slice;
        if (curr_slice == info->slice && (now = rdtsc()) < curr_slice) {
            qnode_t *succ = readvol(lock->qnext);
            if (NULL == succ) {
                if (__sync_bool_compare_and_swap(&lock->qtail, NULL, flqnode(lock)))
                    goto reenter;
                spin_then_yield(SPIN_LIMIT,
                    (now = rdtsc()) < curr_slice &&
                    NULL == (succ = readvol(lock->qnext)));
#ifdef DEBUG
                info->stat.own_slice_wait += rdtsc() - now;
#endif
                if (now >= curr_slice)
                    goto begin;
            }
            if (succ->state < RUNNABLE ||
                __sync_bool_compare_and_swap(&succ->state, RUNNABLE, NEXT)) {
reenter:
#ifdef DEBUG
                info->stat.reenter++;
#endif
                info->start_ticks = now;
                return;
            }
        }
    }

begin:
    /* FIX 6: get_updated_ban now always walks the full ancestor chain */
    banned_until = get_updated_ban(lock, info->parent, info->banned_until);
    if (banned_until > (now = rdtsc()))
        info->banned_until = banned_until;

    if (info->banned) {
        if ((now = rdtsc()) < banned_until) {
            ull banned_time = banned_until - now;
#ifdef DEBUG
            info->stat.banned_time += banned_time;
#endif
            while (banned_time > CYCLE_PER_US * SLEEP_GRANULARITY) {
                struct timespec req = {
                    .tv_sec  = banned_time / CYCLE_PER_S,
                    .tv_nsec = (banned_time % CYCLE_PER_S /
                                CYCLE_PER_US / SLEEP_GRANULARITY) *
                               SLEEP_GRANULARITY * 1000,
                };
                nanosleep(&req, NULL);
                if ((now = rdtsc()) >= banned_until)
                    break;
                banned_time = banned_until - now;
            }
            spin_then_yield(SPIN_LIMIT, (now = rdtsc()) < banned_until);
        }
    }

    qnode_t n = { 0 };
    while (1) {
        qnode_t *prev = readvol(lock->qtail);
        if (__sync_bool_compare_and_swap(&lock->qtail, prev, &n)) {
            if (NULL == prev) {
                n.state   = RUNNABLE;
                lock->qnext = &n;
            } else {
                if (prev == flqnode(lock)) {
                    n.state      = NEXT;
                    prev->next   = &n;
                } else {
                    prev->next = &n;
#ifdef DEBUG
                    now = rdtsc();
#endif
                    do {
                        futex(&n.state, FUTEX_WAIT_PRIVATE, INIT, NULL);
                    } while (INIT == readvol(n.state));
#ifdef DEBUG
                    info->stat.next_runnable_wait += rdtsc() - now;
#endif
                }
            }

            int slice_valid;
            ull curr_slice;
            while ((slice_valid = readvol(lock->slice_valid)) &&
                   (now = rdtsc()) + SLEEP_GRANULARITY <
                   (curr_slice = readvol(lock->slice))) {
                ull slice_left = curr_slice - now;
                struct timespec timeout = {
                    .tv_sec  = 0,
                    .tv_nsec = (slice_left / (CYCLE_PER_US * SLEEP_GRANULARITY)) *
                               SLEEP_GRANULARITY * 1000,
                };
                futex(&lock->slice_valid, FUTEX_WAIT_PRIVATE, 0, &timeout);
#ifdef DEBUG
                info->stat.prev_slice_wait += rdtsc() - now;
#endif
            }
            if (slice_valid) {
                spin_then_yield(SPIN_LIMIT,
                    (slice_valid = readvol(lock->slice_valid)) &&
                    rdtsc() < readvol(lock->slice));
                if (slice_valid)
                    lock->slice_valid = 0;
            }

#ifdef DEBUG
            now = rdtsc();
#endif
            spin_then_yield(SPIN_LIMIT,
                RUNNABLE != readvol(n.state) ||
                0 == __sync_bool_compare_and_swap(&n.state, RUNNABLE, RUNNING));
#ifdef DEBUG
            info->stat.runnable_wait += rdtsc() - now;
#endif

            qnode_t *succ = readvol(n.next);
            if (NULL == succ) {
                lock->qnext = NULL;
                if (0 == __sync_bool_compare_and_swap(&lock->qtail, &n, flqnode(lock))) {
                    spin_then_yield(SPIN_LIMIT, NULL == (succ = readvol(n.next)));
#ifdef DEBUG
                    info->stat.succ_wait += rdtsc() - now;
#endif
                    lock->qnext = succ;
                }
            } else {
                lock->qnext = succ;
            }

            now = rdtsc();
            /* FIX 6: full ancestor walk inside get_updated_ban */
            banned_until = get_updated_ban(lock, info->parent, info->banned_until);
            int reacquire_slice = is_reacquired(lock, info->parent);

            if (reacquire_slice || banned_until <= now) {
                info->slice      = set_slice(lock, info);
                lock->slice      = info->slice;
                lock->slice_valid = 1;
                if (succ) {
                    succ->state = NEXT;
                    futex(&succ->state, FUTEX_WAKE_PRIVATE, 1, NULL);
                }
                return;
            } else {
                /*
                 * FIX 5: invert the boolean.
                 * reacquire_slice == 0 means the thread IS node-banned.
                 * The original code set info->banned = reacquire_slice (= 0),
                 * which cleared the ban flag and caused the ban sleep to be
                 * skipped on the next loop iteration, creating a busy spin.
                 */
                info->banned = !reacquire_slice;

                if (NULL == succ) {
                    if (__sync_bool_compare_and_swap(&lock->qtail, flqnode(lock), NULL))
                        goto begin;
                    spin_then_yield(SPIN_LIMIT, NULL == (succ = readvol(lock->qnext)));
                }
                succ->state = RUNNABLE;
                futex(&succ->state, FUTEX_WAKE_PRIVATE, 1, NULL);
                goto begin;
            }
        }
    }
}

ull hfairlock_release(hfairlock_t *lock) {
    ull now, cs;
#ifdef DEBUG
    ull succ_start = 0, succ_end = 0;
#endif
    flthread_info_t *info =
        (flthread_info_t *) pthread_getspecific(lock->flthread_info_key);

    now = rdtsc();
    cs  = now - info->start_ticks;

    ull total_w = __atomic_load_n(&lock->total_weight, __ATOMIC_RELAXED);
    ull pmax    = 0;

    /*
     * FIX 1: iterate info->depth, not MAX_DEPTH, to avoid visiting
     *         uninitialised path slots.
     * FIX 2: node->banned_until updated atomically to prevent lost
     *         updates when two sibling threads release concurrently.
     * FIX 3: node->cs updated atomically for the same reason.
     */
    for (int i = 0; i < info->depth; ++i) {
        int nid = info->path[i];
        if (nid < 0) continue;                       /* FIX 1: skip sentinel */
        node_t *node = &lock->hierarchy[nid];

        /* FIX 3: atomic CS accumulation */
        __atomic_fetch_add(&node->cs, cs, __ATOMIC_RELAXED);

        /* FIX 2: atomic ban update */
        ull penalty = cs * (total_w / (ull)node->weight);
        ull old_ban, new_ban;
        do {
            old_ban = __atomic_load_n(&node->banned_until, __ATOMIC_RELAXED);
            new_ban = old_ban + penalty;
        } while (!__atomic_compare_exchange_n(
                     &node->banned_until, &old_ban, new_ban,
                     0, __ATOMIC_RELAXED, __ATOMIC_RELAXED));

        pmax = MAX(pmax, new_ban);
    }

    /* Thread-level ban */
    ull thread_penalty = cs * (total_w / info->weight);
    info->banned_until += thread_penalty;

    /*
     * FIX 4: floor info->banned_until against pmax so a thread cannot
     * reacquire while any ancestor node is still banned.
     * (The original line was commented out, allowing this bypass.)
     */
    info->banned_until = MAX(pmax, info->banned_until);

    info->banned = now < info->banned_until;
    if (info->banned) {
        if (__sync_bool_compare_and_swap(&lock->slice_valid, 1, 0)) {
            futex(&lock->slice_valid, FUTEX_WAKE_PRIVATE, 1, NULL);
        }
    }

    qnode_t *succ = lock->qnext;
    if (NULL == succ) {
        if (__sync_bool_compare_and_swap(&lock->qtail, flqnode(lock), NULL))
            return info->slice;
#ifdef DEBUG
        succ_start = rdtsc();
#endif
        spin_then_yield(SPIN_LIMIT, NULL == (succ = readvol(lock->qnext)));
#ifdef DEBUG
        succ_end = rdtsc();
#endif
    }
    succ->state = RUNNABLE;

#ifdef DEBUG
    info->stat.release_succ_wait += succ_end - succ_start;
#endif
    return info->slice;
}