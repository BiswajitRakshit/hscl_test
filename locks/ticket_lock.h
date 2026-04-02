#ifndef __TICKET_LOCK_H__
#define __TICKET_LOCK_H__

/*
 * Ticket lock — guarantees FIFO acquisition order.
 * Uses fetch-and-add for ticket assignment.
 * Each thread spins on its own cache line (padded) to reduce coherence traffic.
 */

#include <stdint.h>
#include <sched.h>
#include "../hscl_common.h"

typedef struct {
    volatile uint64_t next_ticket __attribute__((aligned(CACHELINE)));
    volatile uint64_t now_serving __attribute__((aligned(CACHELINE)));
} ticket_lock_t __attribute__((aligned(CACHELINE)));

static inline void ticket_lock_init(ticket_lock_t *l) {
    l->next_ticket = 0;
    l->now_serving = 0;
}

static inline void ticket_lock_acquire(ticket_lock_t *l) {
    uint64_t my_ticket = __atomic_fetch_add(&l->next_ticket, 1, __ATOMIC_SEQ_CST);
    int spins = 0;
    while (__atomic_load_n(&l->now_serving, __ATOMIC_ACQUIRE) != my_ticket) {
        if (++spins > 100) {
            sched_yield();
            spins = 0;
        }
    }
}

static inline void ticket_lock_release(ticket_lock_t *l) {
    __atomic_fetch_add(&l->now_serving, 1, __ATOMIC_RELEASE);
}

static inline void ticket_lock_destroy(ticket_lock_t *l) {
    (void)l;
}

#endif /* __TICKET_LOCK_H__ */