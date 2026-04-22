# ── HSCL_TEST_1 Makefile ──────────────────────────────────────────────────────
UPSCALEDB   ?= $(HOME)/upscaledb
DURATION    ?= 100
FIND_PCT    ?= 100
THREAD_COUNTS ?= 4 8 16 32 64 128 256

CYCLE_PER_US := $(shell cat /proc/cpuinfo | grep "cpu MHz" | head -1 | awk '{printf "%d", $$4}')
MAX_DEPTH    := 2
GRANULARITY  := ($(CYCLE_PER_US)L*1000L*2L)

CXX  = g++
CC   = gcc
OPTS = -g -O2 -pthread
BENCH_INC = -I. -Ilocks/ \
            -I$(UPSCALEDB)/tools/ups_bench \
            -I$(UPSCALEDB)/tools \
            -I$(UPSCALEDB)/src \
            -I/usr/local/include
LIBS = -L/usr/local/lib -lupscaledb \
       -lboost_thread -lboost_system -lboost_filesystem -lboost_chrono \
       -lpthread
COMMON_DEF = -DCYCLE_PER_US=$(CYCLE_PER_US)L \
             -DMAX_DEPTH=$(MAX_DEPTH) \
             -DFAIRLOCK_GRANULARITY='$(GRANULARITY)'
BENCH_SRCS = $(UPSCALEDB)/tools/ups_bench
BENCH_OBJS = getopts.o common.o database.o generator_parser.o \
             generator_runtime.o berkeleydb.o
BENCH_ARGS = --distribution=random --key=uint32 \
             --find-pct=$(FIND_PCT) --no-progress \
             --stop-seconds=$(DURATION)

.PHONY: all info build-objs clean run run-flat run-two-level run-privileged \
        run-all-hierarchies fairness gen-hierarchies

all: info build-objs \
     ups_bench_boost_mutex ups_bench_pthread_mutex \
     ups_bench_pthread_spin ups_bench_ticket ups_bench_hscl
	@echo "" && echo "All binaries built:" && ls -lh ups_bench_*

info:
	@echo "============================================"
	@echo "  CPU MHz       : $(CYCLE_PER_US)"
	@echo "  CYCLE_PER_US  : $(CYCLE_PER_US)L"
	@echo "  Thread counts : $(THREAD_COUNTS)"
	@echo "  Duration      : $(DURATION)s per run"
	@echo "  Find pct      : $(FIND_PCT)%"
	@echo "============================================"

build-objs: $(BENCH_OBJS) hfairlock.o

hfairlock.o: locks/hfairlock.c locks/hfairlock.h hscl_common.h rdtsc.h
	$(CC) $< -c $(OPTS) -Ilocks/ -I. $(COMMON_DEF) -o $@

getopts.o: $(UPSCALEDB)/tools/getopts.cc
	$(CXX) $< -c $(OPTS) -I$(UPSCALEDB)/tools -I/usr/local/include -o $@

common.o: $(UPSCALEDB)/tools/ups_bench/common.c
	$(CC) $< -c $(OPTS) -I$(BENCH_SRCS) -I$(UPSCALEDB)/tools -o $@

database.o: $(BENCH_SRCS)/database.cc
	$(CXX) $< -c $(OPTS) $(BENCH_INC) -DBOOST_TIMER_ENABLE_DEPRECATED -o $@

generator_parser.o: $(BENCH_SRCS)/generator_parser.cc
	$(CXX) $< -c $(OPTS) $(BENCH_INC) -DBOOST_TIMER_ENABLE_DEPRECATED -o $@

generator_runtime.o: $(BENCH_SRCS)/generator_runtime.cc
	$(CXX) $< -c $(OPTS) $(BENCH_INC) -DBOOST_TIMER_ENABLE_DEPRECATED -o $@

berkeleydb.o: $(BENCH_SRCS)/berkeleydb.cc
	$(CXX) $< -c $(OPTS) $(BENCH_INC) -DBOOST_TIMER_ENABLE_DEPRECATED -o $@ \
	    2>/dev/null || touch $@

ups_bench_boost_mutex: main.cc upscaledb.cc $(BENCH_OBJS)
	@echo "Building boost_mutex..."; \
	$(CXX) $^ -o $@ $(OPTS) $(BENCH_INC) $(LIBS) $(COMMON_DEF) -DLOCK_BOOST_MUTEX

ups_bench_pthread_mutex: main.cc upscaledb.cc $(BENCH_OBJS)
	@echo "Building pthread_mutex..."; \
	$(CXX) $^ -o $@ $(OPTS) $(BENCH_INC) $(LIBS) $(COMMON_DEF) -DLOCK_PTHREAD_MUTEX

ups_bench_pthread_spin: main.cc upscaledb.cc $(BENCH_OBJS)
	@echo "Building pthread_spin..."; \
	$(CXX) $^ -o $@ $(OPTS) $(BENCH_INC) $(LIBS) $(COMMON_DEF) -DLOCK_PTHREAD_SPIN

ups_bench_ticket: main.cc upscaledb.cc $(BENCH_OBJS)
	@echo "Building ticket_lock..."; \
	$(CXX) $^ -o $@ $(OPTS) $(BENCH_INC) $(LIBS) $(COMMON_DEF) -DLOCK_TICKET

ups_bench_hscl: main.cc upscaledb.cc hfairlock.o $(BENCH_OBJS)
	@echo "Building H-SCL..."; \
	$(CXX) $^ -o $@ $(OPTS) $(BENCH_INC) $(LIBS) $(COMMON_DEF) -DHFAIRLOCK


# ── gen-hierarchies: generate per-thread-count hierarchy files ────────────────
# Format: 3 nodes (root=0, insert_group=1, find_group=2), then T lines (one
# per thread) assigning it to node 1 (inserts) or node 2 (finds).
# This matches MAX_DEPTH=2 and the 3-node init in run_single_test().
gen-hierarchies:
	@mkdir -p hierarchy
	@python3 -c "\
counts = [4, 8, 16, 32, 64, 128, 256]; \
[open('hierarchy/t%d.txt' % T, 'w').write( \
    '3\n0\n0\n' + \
    ''.join('1\n' if i < T//2 else '2\n' for i in range(T))) \
 for T in counts]; \
print('Generated hierarchy/t{4,8,16,32,64,128,256}.txt') \
"
	@echo "  3 nodes: root=0, insert_group=1, find_group=2"
	@echo "  First T/2 threads -> node 1 (inserts), last T/2 -> node 2 (finds)"


# ── run: benchmark all locks ──────────────────────────────────────────────────
# IMPORTANT: all loop logic is kept in ONE @-prefixed shell block joined by ;\
# to prevent "unexpected end of file" from make splitting recipe lines into
# separate shells.
run: all
	@echo "Pinning CPU..."
	@mkdir -p results
	sudo cpupower frequency-set -g performance 2>/dev/null || true
	@NCORES=$$(nproc); \
	export LD_LIBRARY_PATH=/usr/local/lib:$$LD_LIBRARY_PATH; \
	for T in $(THREAD_COUNTS); do \
	    for LOCK in boost_mutex pthread_mutex pthread_spin ticket hscl; do \
	        if [ "$$LOCK" = "pthread_spin" ] && [ "$$T" -ge "$$NCORES" ]; then \
	            echo ""; \
	            echo "=== $$LOCK  threads=$$T -- SKIPPED (spinlock unsafe: threads>=cores) ==="; \
	            continue; \
	        fi; \
	        echo ""; echo "=== $$LOCK  threads=$$T ==="; \
	        timeout $$(($(DURATION) + 30)) ./ups_bench_$$LOCK --num-threads=$$T $(BENCH_ARGS) \
	            2>&1 | tee results/$${LOCK}_t$${T}.txt; \
	        rm -f test-ham.db test-ham.db.jrn0 test-ham.db.jrn1; \
	        sleep 2; \
	    done; \
	done; \
	echo ""; \
	echo "Results saved to results/. Running analysis..."; \
	python3 fairness_analysis.py --results-dir=results --auto


# ── run-flat / run-two-level / run-privileged ─────────────────────────────────
run-flat: ups_bench_hscl
	@echo "=== H-SCL with FLAT hierarchy ==="
	@mkdir -p results/flat
	sudo cpupower frequency-set -g performance 2>/dev/null || true
	@export LD_LIBRARY_PATH=/usr/local/lib:$$LD_LIBRARY_PATH; \
	for T in $(THREAD_COUNTS); do \
	    echo ""; echo "--- hscl flat  threads=$$T ---"; \
	    timeout $$(($(DURATION) + 30)) ./ups_bench_hscl --num-threads=$$T $(BENCH_ARGS) \
	        2>&1 | tee results/flat/hscl_t$${T}.txt; \
	    rm -f test-ham.db test-ham.db.jrn0 test-ham.db.jrn1; \
	    sleep 2; \
	done; \
	echo "Done -- results in results/flat/"

run-two-level: ups_bench_hscl
	@echo "=== H-SCL with TWO-LEVEL hierarchy ==="
	@mkdir -p results/two_level
	sudo cpupower frequency-set -g performance 2>/dev/null || true
	@export LD_LIBRARY_PATH=/usr/local/lib:$$LD_LIBRARY_PATH; \
	for T in $(THREAD_COUNTS); do \
	    echo ""; echo "--- hscl two_level  threads=$$T ---"; \
	    timeout $$(($(DURATION) + 30)) ./ups_bench_hscl --num-threads=$$T $(BENCH_ARGS) \
	        2>&1 | tee results/two_level/hscl_t$${T}.txt; \
	    rm -f test-ham.db test-ham.db.jrn0 test-ham.db.jrn1; \
	    sleep 2; \
	done; \
	echo "Done -- results in results/two_level/"

run-privileged: ups_bench_hscl
	@echo "=== H-SCL with PRIVILEGED hierarchy ==="
	@mkdir -p results/privileged
	sudo cpupower frequency-set -g performance 2>/dev/null || true
	@export LD_LIBRARY_PATH=/usr/local/lib:$$LD_LIBRARY_PATH; \
	for T in $(THREAD_COUNTS); do \
	    echo ""; echo "--- hscl privileged  threads=$$T ---"; \
	    timeout $$(($(DURATION) + 30)) ./ups_bench_hscl --num-threads=$$T $(BENCH_ARGS) \
	        2>&1 | tee results/privileged/hscl_t$${T}.txt; \
	    rm -f test-ham.db test-ham.db.jrn0 test-ham.db.jrn1; \
	    sleep 2; \
	done; \
	echo "Done -- results in results/privileged/"

# ── run-all-hierarchies ───────────────────────────────────────────────────────
run-all-hierarchies: ups_bench_hscl
	$(MAKE) run-flat
	$(MAKE) run-two-level
	$(MAKE) run-privileged
	@echo ""
	@echo "=== Flat hierarchy ==="
	@python3 fairness_analysis.py --results-dir=results/flat --auto 2>/dev/null || true
	@echo ""
	@echo "=== Two-level hierarchy ==="
	@python3 fairness_analysis.py --results-dir=results/two_level --auto 2>/dev/null || true
	@echo ""
	@echo "=== Privileged hierarchy ==="
	@python3 fairness_analysis.py --results-dir=results/privileged --auto 2>/dev/null || true

fairness:
	@python3 plot_figures.py --results-dir=results

clean:
	rm -f *.o ups_bench_* test-ham.db* 2>/dev/null || true