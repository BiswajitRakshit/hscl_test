# ── HSCL_TEST_1 Makefile ──────────────────────────────────────────────────────
UPSCALEDB   ?= $(HOME)/upscaledb
DURATION    ?= 30
FIND_PCT    ?= 50
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

.PHONY: all info build-objs clean run fairness

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


run: all
	@echo "Pinning CPU..."
	@mkdir -p results
	sudo cpupower frequency-set -g performance 2>/dev/null || true
	@NCORES=$$(nproc); \
	export LD_LIBRARY_PATH=/usr/local/lib:$$LD_LIBRARY_PATH; \
	for T in $(THREAD_COUNTS); do \
	    for LOCK in boost_mutex pthread_mutex pthread_spin ticket hscl; do \
	        if [ "$$LOCK" = "pthread_spin" ] && [ "$$T" -ge "$$NCORES" ]; then \
	            echo ""; echo "=== $$LOCK  threads=$$T — SKIPPED (spinlock unsafe: threads>=cores) ==="; \
	            continue; \
	        fi; \
	        echo ""; echo "=== $$LOCK  threads=$$T ==="; \
	        timeout 60 ./ups_bench_$$LOCK --num-threads=$$T $(BENCH_ARGS) \
	            2>&1 | tee results/$${LOCK}_t$${T}.txt; \
	        rm -f test-ham.db test-ham.db.jrn0 test-ham.db.jrn1; \
	        sleep 2; \
	    done; \
	done


	@echo ""; echo "Results saved to results/. Running analysis..."
	@python3 fairness_analysis.py --results-dir=results --auto

fairness:
	@python3 plot_figures.py --results-dir=results 

clean:
	rm -f *.o ups_bench_* test-ham.db* 2>/dev/null || true