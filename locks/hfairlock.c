

#include <hfairlock.h>
int hfairlock_init(hfairlock_t *lock, node_t *hierarchy) {
    int rc;

    lock->qtail = NULL;
    lock->qnext = NULL;
    lock->total_weight = 0;
    lock->slice = 0;
    lock->slice_valid = 0;
    lock->hierarchy = hierarchy;
    if (0 != (rc = pthread_key_create(&lock->flthread_info_key, NULL))) {
        return rc;
    }
    return 0;
}

flthread_info_t *flthread_info_create(hfairlock_t *lock, int weight) {
    flthread_info_t *info;
    info = malloc(sizeof(flthread_info_t));
    info->banned_until = rdtsc();
    if (weight == 0) {
        int prio = getpriority(PRIO_PROCESS, 0);
        weight = prio_to_weight[prio+20];
    }
    info->weight = weight;
    __sync_add_and_fetch(&lock->total_weight, weight);
    __sync_add_and_fetch(&lock->hierarchy[0].weight, weight);
    info->banned = 0;
    info->slice = 0;
    info->start_ticks = 0;
    info->parent = 0; 
    memset(info->path, 0, sizeof(info->path));
#ifdef DEBUG
    memset(&info->stat, 0, sizeof(stats_t));
    info->stat.start = info->banned_until;
#endif
    return info;
}

void set_path(hfairlock_t *lock, int path[], int parent, int weight, ull banned_until) {
    int i = MAX_DEPTH - 1;
    while (parent != 0) {
        path[i] = parent;
        __sync_add_and_fetch(&lock->hierarchy[parent].weight, weight);
        lock->hierarchy[parent].banned_until = banned_until;
        // printf("%d\n", lock->hierarchy[parent].weight);
        parent = lock->hierarchy[parent].parent;
        --i;
    }
    path[i] = parent;
}

void hfairlock_thread_init(hfairlock_t *lock, int weight, int parent) {
    flthread_info_t *info;
    info = (flthread_info_t *) pthread_getspecific(lock->flthread_info_key);
    if (NULL != info) {
        free(info);
    }
    info = flthread_info_create(lock, weight);
    info->parent = parent;
    set_path(lock, info->path, parent, weight, info->banned_until);
    // lock->hierarchy[0].weight = lock->total_weight;
    pthread_setspecific(lock->flthread_info_key, info);
}

int hfairlock_destroy(hfairlock_t *lock) {
    // return pthread_key_delete(lock->flthread_info_key);
    return 0;
}


ull get_updated_ban(hfairlock_t *lock, int parent, ull banned_until) {
    // printf("%d %llu\t", parent, lock->hierarchy[parent].banned_until);
    if (lock->hierarchy[parent].slice > rdtsc()) {
    	return  banned_until;
    }
    while (parent != 0) {
        banned_until = MAX(banned_until, lock->hierarchy[parent].banned_until);
        parent = lock->hierarchy[parent].parent;
    }
    return banned_until;
}

int is_reacquired(hfairlock_t *lock, int parent) {
    ull now = rdtsc();
    while (parent != 0) {
        if (now > lock->hierarchy[parent].slice) {
            return 0;
        }
        parent = lock->hierarchy[parent].parent;
    }
    return 1;
}

ull set_slice(hfairlock_t *lock, flthread_info_t *info) {
    int offset = MAX_DEPTH;
    ull now = rdtsc();
    ull min = now + FAIRLOCK_GRANULARITY;
    for (int i = 0; i < MAX_DEPTH; ++i)
    {
        --offset;
        if (lock->hierarchy[info->path[i]].slice <= now) {
            lock->hierarchy[info->path[i]].slice = now + (FAIRLOCK_GRANULARITY << offset);
        }
        min = MIN(lock->hierarchy[info->path[i]].slice, min);
    }
    info->start_ticks = now;
    return min;
}

void hfairlock_acquire(hfairlock_t *lock) {
    flthread_info_t *info;
    ull now;
    ull banned_until;

    info = (flthread_info_t *) pthread_getspecific(lock->flthread_info_key);
    if (NULL == info) {
        info = flthread_info_create(lock, 0);
        info->parent = 0;
        pthread_setspecific(lock->flthread_info_key, info);
        set_path(lock, info->path, 0, info->weight, info->banned_until);
    }

    if (readvol(lock->slice_valid)) {
        ull curr_slice = lock->slice;
        // If owner of current slice, try to reenter at the beginning of the queue
        if (curr_slice == info->slice && (now = rdtsc()) < curr_slice) {
            qnode_t *succ = readvol(lock->qnext);
            if (NULL == succ) {
                if (__sync_bool_compare_and_swap(&lock->qtail, NULL, flqnode(lock)))
                    goto reenter;
                spin_then_yield(SPIN_LIMIT, (now = rdtsc()) < curr_slice && NULL == (succ = readvol(lock->qnext)));
#ifdef DEBUG
                info->stat.own_slice_wait += rdtsc() - now;
#endif
                // let the succ invalidate the slice, and don't need to wake it up because slice expires naturally
                if (now >= curr_slice)
                    goto begin;
            }
            // if state < RUNNABLE, it won't become RUNNABLE unless someone releases lock,
            // but as no one is holding the lock, there is no race
            if (succ->state < RUNNABLE || __sync_bool_compare_and_swap(&succ->state, RUNNABLE, NEXT)) {
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
    banned_until = get_updated_ban(lock, info->parent, info->banned_until);
    if (banned_until > (now = rdtsc())) {
        info->banned_until = banned_until;
    }
    if (info->banned) {
        if ((now = rdtsc()) < banned_until) {
            ull banned_time = banned_until - now;
#ifdef DEBUG
            info->stat.banned_time += banned_time;
#endif
            // sleep with granularity of SLEEP_GRANULARITY us
            while (banned_time > CYCLE_PER_US * SLEEP_GRANULARITY) {
                struct timespec req = {
                    .tv_sec = banned_time / CYCLE_PER_S,
                    .tv_nsec = (banned_time % CYCLE_PER_S / CYCLE_PER_US / SLEEP_GRANULARITY) * SLEEP_GRANULARITY * 1000,
                };
                nanosleep(&req, NULL);
                if ((now = rdtsc()) >= banned_until)
                    break;
                banned_time = banned_until - now;
            }
            // spin for the remaining (<SLEEP_GRANULARITY us)
            spin_then_yield(SPIN_LIMIT, (now = rdtsc()) < banned_until);
        }
    }

    qnode_t n = { 0 };
    while (1) {
        qnode_t *prev = readvol(lock->qtail);
        if (__sync_bool_compare_and_swap(&lock->qtail, prev, &n)) {
            // enter the lock queue
            if (NULL == prev) {
                n.state = RUNNABLE;
                lock->qnext = &n;
            } else {
                if (prev == flqnode(lock)) {
                    n.state = NEXT;
                    prev->next = &n;
                } else {
                    prev->next = &n;
                    // wait until we become the next runnable
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
            // invariant: n.state >= NEXT

            // wait until the current slice expires
            int slice_valid;
            ull curr_slice;
            while ((slice_valid = readvol(lock->slice_valid)) && (now = rdtsc()) + SLEEP_GRANULARITY < (curr_slice = readvol(lock->slice))) {
                ull slice_left = curr_slice - now;
                struct timespec timeout = {
                    .tv_sec = 0, // slice will be less then 1 sec
                    .tv_nsec = (slice_left / (CYCLE_PER_US * SLEEP_GRANULARITY)) * SLEEP_GRANULARITY * 1000,
                };
                futex(&lock->slice_valid, FUTEX_WAIT_PRIVATE, 0, &timeout);
#ifdef DEBUG
                info->stat.prev_slice_wait += rdtsc() - now;
#endif
            }
            if (slice_valid) {
                spin_then_yield(SPIN_LIMIT, (slice_valid = readvol(lock->slice_valid)) && rdtsc() < readvol(lock->slice));
                if (slice_valid)
                    lock->slice_valid = 0;
            }
            // invariant: rdtsc() >= curr_slice && lock->slice_valid == 0

#ifdef DEBUG
            now = rdtsc();
#endif
            // spin until RUNNABLE and try to grab the lock
            spin_then_yield(SPIN_LIMIT, RUNNABLE != readvol(n.state) || 0 == __sync_bool_compare_and_swap(&n.state, RUNNABLE, RUNNING));
            // invariant: n.state == RUNNING
#ifdef DEBUG
            info->stat.runnable_wait += rdtsc() - now;
#endif

            // record the successor in the lock so we can notify it when we release
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
            // invariant: NULL == succ <=> lock->qtail == flqnode(lock)

            now = rdtsc();
            banned_until = get_updated_ban(lock, info->parent, info->banned_until);
            int reacquire_slice = is_reacquired(lock, info->parent);
            if (reacquire_slice || banned_until <= now) {
                // info->start_ticks = now;
                // printf("There %x", succ);
                info->slice = set_slice(lock, info);
                lock->slice = info->slice;
                lock->slice_valid = 1;
                // wake up successor if necessary
                if (succ) {
                    succ->state = NEXT;
                    futex(&succ->state, FUTEX_WAKE_PRIVATE, 1, NULL);
                }
                return;
            } else {
                // record the successor in the lock so we can notify it when we release
                info->banned = reacquire_slice
                ;
                if (NULL == succ) {
                    if (__sync_bool_compare_and_swap(&lock->qtail, flqnode(lock), NULL))
                        goto begin;
                    spin_then_yield(SPIN_LIMIT, NULL == (succ = readvol(lock->qnext)));
                }
                succ->state = RUNNABLE;
                if (succ) {
                    futex(&succ->state, FUTEX_WAKE_PRIVATE, 1, NULL);
                }
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
    flthread_info_t *info;
    // invariant: NULL == succ || succ->state = RUNNABLE
    info = (flthread_info_t *) pthread_getspecific(lock->flthread_info_key);
    now = rdtsc();
    cs = now - info->start_ticks;
    
    ull pmax = 0;
    for (int i = 0; i < MAX_DEPTH; ++i) {
        node_t *node = &lock->hierarchy[info->path[i]];
        node->cs += cs;
        node->banned_until += cs * (__atomic_load_n(&lock->total_weight, __ATOMIC_RELAXED) / node->weight);
        node->banned_until = (pmax = MAX(pmax, node->banned_until));
    }

    info->banned_until += cs * (__atomic_load_n(&lock->total_weight, __ATOMIC_RELAXED) / info->weight);
    // info->banned_until = MAX(pmax, info->banned_until);
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