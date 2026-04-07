#!/usr/bin/env python3
"""
fairness_analysis.py  — dynamic fairness comparison for H-SCL experiment
=========================================================================
Usage:
    python3 fairness_analysis.py                     # reads ./results/
    python3 fairness_analysis.py --results-dir DIR   # reads DIR/
    python3 fairness_analysis.py --auto              # no prompts, just print

Result file naming convention (produced by Makefile):
    <lock_name>_t<threads>.txt
    e.g.  hscl_t8.txt  pthread_mutex_t16.txt  boost_mutex_t4.txt

Each file is raw ups_bench stdout. The script parses:
    upscaledb insert_#ops    N (R/sec)
    upscaledb insert_latency (min, avg, max)
    upscaledb find_#ops      N (R/sec)
    upscaledb find_latency   (min, avg, max)
"""

import os
import re
import sys
import glob
import argparse
from collections import defaultdict

# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_result_file(path):
    """Extract metrics from a single ups_bench output file."""
    data = {}
    try:
        text = open(path).read()
    except Exception as e:
        print(f"  [WARN] cannot read {path}: {e}")
        return None

    def find(pattern):
        m = re.search(pattern, text)
        return float(m.group(1)) if m else None

    data["insert_ops"]  = find(r"insert_#ops\s+(\d+)")
    data["find_ops"]    = find(r"find_#ops\s+(\d+)")
    data["insert_rate"] = find(r"insert_#ops\s+\d+\s+\(([0-9.]+)/sec\)")
    data["find_rate"]   = find(r"find_#ops\s+\d+\s+\(([0-9.]+)/sec\)")

    lat = re.search(r"insert_latency.*?([0-9.]+),\s*([0-9.]+),\s*([0-9.]+)", text)
    if lat:
        data["insert_avg"] = float(lat.group(2))
        data["insert_max"] = float(lat.group(3))

    lat2 = re.search(r"find_latency.*?([0-9.]+),\s*([0-9.]+),\s*([0-9.]+)", text)
    if lat2:
        data["find_avg"] = float(lat2.group(2))
        data["find_max"] = float(lat2.group(3))

    # Validate required fields
    required = ["insert_ops","find_ops","insert_rate","find_rate",
                "insert_avg","find_avg","insert_max","find_max"]
    for k in required:
        if data.get(k) is None:
            print(f"  [WARN] {os.path.basename(path)}: missing field '{k}'")
            return None
    return data


def discover_results(results_dir):
    """
    Scan results_dir for files matching <lock>_t<N>.txt.
    Returns dict: { lock_name -> { thread_count -> parsed_data } }
    """
    pattern = os.path.join(results_dir, "*.txt")
    files   = sorted(glob.glob(pattern))
    if not files:
        print(f"No result files found in '{results_dir}'.")
        print("Run 'make run' first to generate results.")
        sys.exit(1)

    found = defaultdict(dict)
    for f in files:
        base = os.path.basename(f).replace(".txt","")
        # match <lock>_t<N>
        m = re.match(r"^(.+)_t(\d+)$", base)
        if not m:
            print(f"  [SKIP] unrecognised filename: {base}.txt")
            continue
        lock_name  = m.group(1)
        thread_cnt = int(m.group(2))
        data = parse_result_file(f)
        if data:
            found[lock_name][thread_cnt] = data
    return found


# ── Statistics ────────────────────────────────────────────────────────────────

def jains(values):
    n  = len(values)
    s  = sum(values)
    sq = sum(v*v for v in values)
    return (s*s) / (n*sq) if sq > 0 else 0.0


def compute_metrics(data, nthreads):
    """Given parsed data dict and total thread count, return metrics dict."""
    n  = nthreads // 2   # threads per group (insert / find split 50/50)
    if n == 0: n = 1

    ih = (data["insert_ops"] / n) * data["insert_avg"]
    fh = (data["find_ops"]   / n) * data["find_avg"]
    io = data["insert_ops"]  / n
    fo = data["find_ops"]    / n

    return {
        "jain_hold": jains([ih]*n + [fh]*n),
        "jain_ops" : jains([io]*n + [fo]*n),
        "jain_tput": jains([data["insert_rate"], data["find_rate"]]),
        "hold_ratio": ih / fh if fh > 0 else 0,
        "ops_ratio" : io / fo if fo > 0 else 0,
        "ih": ih, "fh": fh, "io": io, "fo": fo,
        "total_ops" : data["insert_ops"] + data["find_ops"],
        "insert_rate": data["insert_rate"],
        "find_rate"  : data["find_rate"],
        "insert_avg" : data["insert_avg"] * 1000,
        "find_avg"   : data["find_avg"]   * 1000,
        "insert_max" : data["insert_max"] * 1000,
        "find_max"   : data["find_max"]   * 1000,
    }


# ── Display ───────────────────────────────────────────────────────────────────

SEP  = "=" * 90
SEP2 = "-" * 90

LOCK_ORDER = [
    "boost_mutex",
    "pthread_mutex",
    "pthread_spin",
    "ticket",
    "hscl",
]

LOCK_LABELS = {
    "boost_mutex"   : "Boost mutex (UPS native)",
    "pthread_mutex" : "pthread_mutex",
    "pthread_spin"  : "pthread_spinlock",
    "ticket"        : "Ticket lock (FIFO)",
    "hscl"          : "H-SCL (hierarchical fair)",
}


def ordered_locks(found):
    """Return lock names in preferred display order."""
    ordered = [l for l in LOCK_ORDER if l in found]
    extras  = [l for l in sorted(found) if l not in LOCK_ORDER]
    return ordered + extras


def print_thread_table(found, thread_counts):
    """Print one comparison table per thread count."""
    for T in sorted(thread_counts):
        print(f"\n{SEP}")
        print(f"  THREAD COUNT = {T}  (50% insert / 50% find, 100s)")
        print(SEP)

        locks = ordered_locks(found)
        # header
        col = 28
        print(f"\n  {'Lock':<{col}} {'Jain(hold)':>10} {'Jain(ops)':>10} "
              f"{'hold ratio':>11} {'Total ops':>11} {'Find/sec':>10} "
              f"{'Ins max(ms)':>12} {'Fnd max(ms)':>12}")
        print(f"  {SEP2}")

        baseline_ops = None
        rows = []
        for lock in locks:
            if T not in found[lock]:
                continue
            m = compute_metrics(found[lock][T], T)
            if baseline_ops is None:
                baseline_ops = m["total_ops"]
            rows.append((lock, m))

        for lock, m in rows:
            label  = LOCK_LABELS.get(lock, lock)
            diff   = (m["total_ops"] - baseline_ops) / baseline_ops * 100 if baseline_ops else 0
            marker = f"({diff:+.1f}%)" if baseline_ops and lock != list(ordered_locks(found))[0] else "(baseline)"
            best_jain = m["jain_hold"] == max(r["jain_hold"] for _,r in rows)
            star = "*" if best_jain else " "
            print(f"  {star}{label:<{col-1}} {m['jain_hold']:>10.4f} {m['jain_ops']:>10.4f} "
                  f"{m['hold_ratio']:>11.3f} {m['total_ops']:>9,} {marker:>3}  "
                  f"{m['find_rate']:>10,.0f} "
                  f"{m['insert_max']:>12.2f} {m['find_max']:>12.2f}")

        print(f"  * = best Jain (hold time) for this thread count")


def print_scaling_table(found):
    """Print how each lock scales with thread count."""
    print(f"\n\n{SEP}")
    print(f"  SCALING — Total ops across thread counts")
    print(SEP)

    all_threads = sorted({T for lock_data in found.values() for T in lock_data})
    col = 28

    header = f"  {'Lock':<{col}}" + "".join(f" {T:>10}T" for T in all_threads)
    print("\n" + header)
    print(f"  {'-'*len(header)}")

    for lock in ordered_locks(found):
        label = LOCK_LABELS.get(lock, lock)
        row   = f"  {label:<{col}}"
        for T in all_threads:
            if T in found[lock]:
                ops = found[lock][T]["insert_ops"] + found[lock][T]["find_ops"]
                row += f" {ops:>10,}"
            else:
                row += f" {'N/A':>10}"
        print(row)

    print(f"\n  {'Lock':<{col}}" + "".join(f" {T:>10}T" for T in all_threads))
    print(f"  {'-'*len(header)}")
    # fairness row
    for lock in ordered_locks(found):
        label = LOCK_LABELS.get(lock, lock)
        row   = f"  {label:<{col}}"
        for T in all_threads:
            if T in found[lock]:
                m = compute_metrics(found[lock][T], T)
                row += f" {m['jain_hold']:>10.4f}"
            else:
                row += f" {'N/A':>10}"
        print(row)
    print(f"  (rows above = Jain hold-time fairness index)")


def print_summary(found):
    """Print the best lock for each metric."""
    print(f"\n\n{SEP}")
    print(f"  SUMMARY — Best lock per metric (averaged across thread counts)")
    print(SEP)

    lock_avgs = {}
    for lock in ordered_locks(found):
        if not found[lock]:
            continue
        jh_list, ops_list, tput_list = [], [], []
        for T, data in found[lock].items():
            m = compute_metrics(data, T)
            jh_list.append(m["jain_hold"])
            ops_list.append(m["total_ops"])
            tput_list.append(m["find_rate"])
        lock_avgs[lock] = {
            "jain_hold": sum(jh_list)/len(jh_list),
            "total_ops": sum(ops_list)/len(ops_list),
            "find_rate": sum(tput_list)/len(tput_list),
        }

    if not lock_avgs:
        return

    best_jain = max(lock_avgs, key=lambda l: lock_avgs[l]["jain_hold"])
    best_tput = max(lock_avgs, key=lambda l: lock_avgs[l]["total_ops"])
    best_find = max(lock_avgs, key=lambda l: lock_avgs[l]["find_rate"])

    print(f"\n  {'Metric':<35} {'Best lock':<28} {'Value'}")
    print(f"  {'-'*70}")
    print(f"  {'Jain fairness (hold time)':<35} {LOCK_LABELS.get(best_jain,best_jain):<28} {lock_avgs[best_jain]['jain_hold']:.4f}")
    print(f"  {'Total throughput (ops)':<35} {LOCK_LABELS.get(best_tput,best_tput):<28} {lock_avgs[best_tput]['total_ops']:,.0f}")
    print(f"  {'Find ops/sec':<35} {LOCK_LABELS.get(best_find,best_find):<28} {lock_avgs[best_find]['find_rate']:,.0f}")

    print(f"\n  {'Lock':<28} {'Avg Jain(hold)':>15} {'Avg total ops':>15} {'Avg find/sec':>14}")
    print(f"  {'-'*74}")
    for lock in ordered_locks(found):
        if lock not in lock_avgs:
            continue
        a = lock_avgs[lock]
        label = LOCK_LABELS.get(lock, lock)
        print(f"  {label:<28} {a['jain_hold']:>15.4f} {a['total_ops']:>15,.0f} {a['find_rate']:>14,.0f}")

    print(f"\n  Jain formula : J = (Σxi)² / (n·Σxi²)   xi = hold_time_per_thread")
    print(f"  LOT Jain     : xi = acquisitions_per_thread  (SCL paper metric)")
    print(SEP)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="H-SCL fairness analysis")
    parser.add_argument("--results-dir", default="results",
                        help="Directory containing <lock>_t<N>.txt result files")
    parser.add_argument("--auto", action="store_true",
                        help="Non-interactive mode")
    args = parser.parse_args()

    results_dir = args.results_dir
    if not os.path.isdir(results_dir):
        print(f"Results directory '{results_dir}' not found.")
        print("Run 'make run' first.")
        sys.exit(1)

    print(f"\n{SEP}")
    print(f"  H-SCL FAIRNESS ANALYSIS")
    print(f"  Reading results from: {os.path.abspath(results_dir)}")
    print(SEP)

    found = discover_results(results_dir)
    if not found:
        sys.exit(1)

    print(f"\n  Found {len(found)} lock types:")
    thread_counts = set()
    for lock, tdata in found.items():
        threads_str = ", ".join(str(t) for t in sorted(tdata.keys()))
        print(f"    {LOCK_LABELS.get(lock,lock):<30}  threads: {threads_str}")
        thread_counts.update(tdata.keys())

    print_thread_table(found, thread_counts)
    print_scaling_table(found)
    print_summary(found)


if __name__ == "__main__":
    main()