# ── HSCL_TEST_1 Makefile ────────────────────────────────────────────────────
# Usage:
#   make hscl    → builds ups_bench_hscl
#   make mutex   → builds ups_bench_mutex
#   make all     → builds both
#   make run     → pins CPU, runs both, prints results
#   make clean   → removes .o files and binaries
#
# Set UPSCALEDB to your upscaledb source root if different.
# CYCLE_PER_US is auto-detected from /proc/cpuinfo at build time.
# ────────────────────────────────────────────────────────────────────────────

UPSCALEDB   ?= $(HOME)/upscaledb
CYCLE_PER_US := $(shell cat /proc/cpuinfo | grep "cpu MHz" | head -1 | \
                awk '{printf "%d", $$4}')

MAX_DEPTH    = 2
SLICE_MS     = 2
GRANULARITY  = ($(CYCLE_PER_US)L*1000L*$(SLICE_MS)L)

CC   = gcc
CXX  = g++
OPTS = -g -O2

BENCH_INC = -I. -Ilocks/ \
            -I$(UPSCALEDB)/tools/ups_bench \
            -I$(UPSCALEDB)/tools \
            -I$(UPSCALEDB)/src \
            -I/usr/local/include

HSCL_DEF  = -DHFAIRLOCK \
             -DMAX_DEPTH=$(MAX_DEPTH) \
             -DCYCLE_PER_US=$(CYCLE_PER_US)L \
             -DFAIRLOCK_GRANULARITY='$(GRANULARITY)'

LIBS = -L/usr/local/lib -lupscaledb \
       -lboost_thread -lboost_system -lboost_filesystem -lboost_chrono \
       -lpthread

BENCH_OBJS = getopts.o common.o berkeleydb.o database.o \
             generator_parser.o generator_runtime.o

.PHONY: all hscl mutex run clean info

all: hscl mutex

info:
	@echo "CYCLE_PER_US = $(CYCLE_PER_US)"
	@echo "MAX_DEPTH    = $(MAX_DEPTH)"
	@echo "GRANULARITY  = $(GRANULARITY)"

# ── hfairlock object ─────────────────────────────────────────────────────────
hfairlock.o: locks/hfairlock.c locks/hfairlock.h hscl_common.h
	$(CC) $< -c $(OPTS) -Ilocks/ -I. \
	    -DCYCLE_PER_US=$(CYCLE_PER_US)L \
	    -DMAX_DEPTH=$(MAX_DEPTH) \
	    -DFAIRLOCK_GRANULARITY='$(GRANULARITY)' \
	    -o $@

# ── upstream bench objects ───────────────────────────────────────────────────
getopts.o: $(UPSCALEDB)/tools/getopts.cc
	$(CXX) $< -c $(OPTS) -I$(UPSCALEDB)/tools -I/usr/local/include -o $@

common.o: $(UPSCALEDB)/tools/ups_bench/common.c
	$(CC) $< -c $(OPTS) \
	    -I$(UPSCALEDB)/tools/ups_bench \
	    -I$(UPSCALEDB)/tools \
	    -o $@

database.o: $(UPSCALEDB)/tools/ups_bench/database.cc
	$(CXX) $< -c $(OPTS) $(BENCH_INC) -DBOOST_TIMER_ENABLE_DEPRECATED -o $@

generator_parser.o: $(UPSCALEDB)/tools/ups_bench/generator_parser.cc
	$(CXX) $< -c $(OPTS) $(BENCH_INC) -DBOOST_TIMER_ENABLE_DEPRECATED -o $@

generator_runtime.o: $(UPSCALEDB)/tools/ups_bench/generator_runtime.cc
	$(CXX) $< -c $(OPTS) $(BENCH_INC) -DBOOST_TIMER_ENABLE_DEPRECATED -o $@

berkeleydb.o: $(UPSCALEDB)/tools/ups_bench/berkeleydb.cc
	$(CXX) $< -c $(OPTS) $(BENCH_INC) -DBOOST_TIMER_ENABLE_DEPRECATED -o $@ \
	    2>/dev/null || (echo "berkeleydb skipped (not installed)" && touch $@)

# ── H-SCL binary ─────────────────────────────────────────────────────────────
hscl: ups_bench_hscl

ups_bench_hscl: main.cc upscaledb.cc hfairlock.o $(BENCH_OBJS)
	@echo "Building H-SCL with CYCLE_PER_US=$(CYCLE_PER_US)..."
	$(CXX) $^ -o $@ $(HSCL_DEF) $(BENCH_INC) $(LIBS) $(OPTS)
	@echo "Built: $@"

# ── Mutex baseline binary ─────────────────────────────────────────────────────
mutex: ups_bench_mutex

ups_bench_mutex: $(UPSCALEDB)/tools/ups_bench/main.cc \
                 $(UPSCALEDB)/tools/ups_bench/upscaledb.cc \
                 $(BENCH_OBJS)
	@echo "Building mutex baseline..."
	$(CXX) $^ -o $@ $(BENCH_INC) $(LIBS) $(OPTS)
	@echo "Built: $@"

# ── Run experiment ────────────────────────────────────────────────────────────
run: ups_bench_hscl ups_bench_mutex
	@echo "Pinning CPU to performance mode..."
	sudo cpupower frequency-set -g performance || true
	export LD_LIBRARY_PATH=/usr/local/lib:$$LD_LIBRARY_PATH && \
	echo "=== Running H-SCL ===" && \
	./ups_bench_hscl \
	    --num-threads=8 --stop-seconds=100 \
	    --distribution=random --key=uint32 \
	    --find-pct=50 --no-progress \
	    2>&1 | tee results_hscl.txt && \
	echo "=== Running Mutex ===" && \
	./ups_bench_mutex \
	    --num-threads=8 --stop-seconds=100 \
	    --distribution=random --key=uint32 \
	    --find-pct=50 --no-progress \
	    2>&1 | tee results_mutex.txt
	@echo ""
	@echo "=== H-SCL ===" && grep -E "insert_#ops|find_#ops|insert_latency|find_latency" results_hscl.txt
	@echo "=== MUTEX ===" && grep -E "insert_#ops|find_#ops|insert_latency|find_latency" results_mutex.txt
	sudo cpupower frequency-set -g powersave || true

# ── Clean ─────────────────────────────────────────────────────────────────────
clean:
	rm -f *.o ups_bench_hscl ups_bench_mutex results_*.txt test-ham.db*