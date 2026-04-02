#!/usr/bin/env python3
"""
Fairness Analysis Tool for H-SCL / u-SCL / Mutex comparison
Usage: python3 fairness_analysis.py
       or edit the RESULTS dict below and re-run after each experiment.

Computes:
  - Jain's fairness index on lock hold time (per thread)
  - Jain's fairness index on acquisitions (LOT-based, correct SCL metric)
  - Insert/find hold ratio
  - Throughput comparison vs mutex baseline
"""

import math

# ─────────────────────────────────────────────────────────────────────────────
# EDIT THIS DICT — paste your ups_bench output numbers here
# Each entry needs: insert_ops, find_ops, insert_avg (sec), find_avg (sec)
# insert_rate and find_rate come from the "X/sec" values in output
# ─────────────────────────────────────────────────────────────────────────────
RESULTS = {
    "H-SCL u-SCL (depth=1)": {
        "insert_ops":  3218519,
        "find_ops":    3219391,
        "insert_rate": 6146.363839,
        "find_rate":   12121.592922,
        "insert_avg":  0.000163,
        "find_avg":    0.000082,
        "insert_max":  0.340837,
        "find_max":    0.312192,
    },


    # "H-SCL fixed (equal w)": {
    #     "insert_ops":  2622614,
    #     "find_ops":    2620876,
    #     "insert_rate": 4557.739869,
    #     "find_rate":   11926.498573,
    #     "insert_avg":  0.000219,
    #     "find_avg":    0.000084,
    #     "insert_max":  0.063605,
    #     "find_max":    0.066813,
    # },
    # "H-SCL fair (ins+2/fnd-2)": {
    #     "insert_ops":  2540006,
    #     "find_ops":    2537341,
    #     "insert_rate": 4397.057870,
    #     "find_rate":   11658.767154,
    #     "insert_avg":  0.000227,
    #     "find_avg":    0.000086,
    #     "insert_max":  0.067984,
    #     "find_max":    0.057274,
    # },
    # "H-SCL latest run": {
    #     "insert_ops":  2511568,
    #     "find_ops":    2508716,
    #     "insert_rate": 4390.777235,
    #     "find_rate":   11226.480780,
    #     "insert_avg":  0.000228,
    #     "find_avg":    0.000089,
    #     "insert_max":  0.081472,
    #     "find_max":    0.079616,
    # },
    # "H-SCL buggy": {
    #     "insert_ops":  2392874,
    #     "find_ops":    2388422,
    #     "insert_rate": 4477.464857,
    #     "find_rate":   9152.770311,
    #     "insert_avg":  0.000223,
    #     "find_avg":    0.000109,
    #     "insert_max":  0.086581,
    #     "find_max":    0.069129,
    # },
    "Mutex (baseline)": {
        "insert_ops":  2970681,
        "find_ops":    2969272,
        "insert_rate": 5509.941655,
        "find_rate":   11669.000376,
        "insert_avg":  0.000181,
        "find_avg":    0.000086,
        "insert_max":  0.648877,
        "find_max":    0.303796,
    },
}

NTHREADS_PER_GROUP = 4   # 4 insert threads, 4 find threads


# ─────────────────────────────────────────────────────────────────────────────
# Core calculations
# ─────────────────────────────────────────────────────────────────────────────

def jains(values):
    """Jain's fairness index. Returns value in (0, 1], where 1 = perfectly fair."""
    n = len(values)
    s = sum(values)
    sq = sum(v * v for v in values)
    if sq == 0:
        return 0.0
    return (s * s) / (n * sq)


def compute(name, d):
    n = NTHREADS_PER_GROUP
    # per-thread values
    insert_hold_per_thread = (d["insert_ops"] / n) * d["insert_avg"]
    find_hold_per_thread   = (d["find_ops"]   / n) * d["find_avg"]
    insert_ops_per_thread  = d["insert_ops"] / n
    find_ops_per_thread    = d["find_ops"]   / n

    all_holds = [insert_hold_per_thread] * n + [find_hold_per_thread] * n
    all_ops   = [insert_ops_per_thread]  * n + [find_ops_per_thread]  * n

    return {
        "name":             name,
        "jain_hold":        jains(all_holds),
        "jain_ops":         jains(all_ops),
        "jain_tput":        jains([d["insert_rate"], d["find_rate"]]),
        "ratio":            insert_hold_per_thread / find_hold_per_thread,
        "ops_ratio":        insert_ops_per_thread  / find_ops_per_thread,
        "insert_hold":      insert_hold_per_thread,
        "find_hold":        find_hold_per_thread,
        "insert_ops_thr":   insert_ops_per_thread,
        "find_ops_thr":     find_ops_per_thread,
        "total_ops":        d["insert_ops"] + d["find_ops"],
        "insert_rate":      d["insert_rate"],
        "find_rate":        d["find_rate"],
        "insert_avg_ms":    d["insert_avg"] * 1000,
        "find_avg_ms":      d["find_avg"]   * 1000,
        "insert_max_ms":    d["insert_max"] * 1000,
        "find_max_ms":      d["find_max"]   * 1000,
    }


rows = [compute(name, d) for name, d in RESULTS.items()]

# find mutex baseline for throughput comparison
mutex_row = next((r for r in rows if "mutex" in r["name"].lower()), rows[-1])
mutex_ops = mutex_row["total_ops"]
mutex_jain = mutex_row["jain_hold"]


# ─────────────────────────────────────────────────────────────────────────────
# Pretty print
# ─────────────────────────────────────────────────────────────────────────────

SEP = "=" * 100

def header(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


header("FAIRNESS COMPARISON — UpscaleDB lock benchmark (8 threads, 100s, 50% find/insert)")

# ── Table 1: Jain indices ──────────────────────────────────────────────────
print(f"\n{'Lock':<32} {'Jain(hold)':>11} {'Jain(ops/LOT)':>14} {'Jain(tput)':>11} "
      f"{'hold ratio':>11} {'ops ratio':>10}")
print("-" * 92)
for r in rows:
    flag = " ← BEST" if r["jain_hold"] == max(x["jain_hold"] for x in rows) else ""
    flag2 = " ← BEST" if r["jain_ops"] == max(x["jain_ops"] for x in rows) else ""
    print(f"{r['name']:<32} {r['jain_hold']:>11.4f} {r['jain_ops']:>14.4f}{flag2}"
          f"  {r['jain_tput']:>11.4f} {r['ratio']:>11.3f} {r['ops_ratio']:>10.4f}")

# ── Table 2: Throughput ───────────────────────────────────────────────────
header("THROUGHPUT COMPARISON")
print(f"\n{'Lock':<32} {'Insert/sec':>11} {'Find/sec':>11} {'Total ops':>12} {'vs mutex':>10}")
print("-" * 80)
for r in rows:
    diff = (r["total_ops"] - mutex_ops) / mutex_ops * 100
    marker = " (baseline)" if "mutex" in r["name"].lower() else f" ({diff:+.2f}%)"
    print(f"{r['name']:<32} {r['insert_rate']:>11,.0f} {r['find_rate']:>11,.0f} "
          f"{r['total_ops']:>12,}{marker}")

# ── Table 3: Per-thread detail ────────────────────────────────────────────
header("PER-THREAD DETAIL")
print(f"\n{'Lock':<32} {'ins_hold(s)':>12} {'fnd_hold(s)':>12} "
      f"{'ins_ops/thr':>12} {'fnd_ops/thr':>12} {'ins_avg(ms)':>12} {'fnd_avg(ms)':>12}")
print("-" * 108)
for r in rows:
    print(f"{r['name']:<32} {r['insert_hold']:>12.2f} {r['find_hold']:>12.2f} "
          f"{r['insert_ops_thr']:>12,.0f} {r['find_ops_thr']:>12,.0f} "
          f"{r['insert_avg_ms']:>12.4f} {r['find_avg_ms']:>12.4f}")

# ── Table 4: Latency ─────────────────────────────────────────────────────
header("LATENCY (milliseconds)")
print(f"\n{'Lock':<32} {'ins_avg':>9} {'fnd_avg':>9} {'ins_max':>9} {'fnd_max':>9}")
print("-" * 62)
for r in rows:
    print(f"{r['name']:<32} {r['insert_avg_ms']:>9.4f} {r['find_avg_ms']:>9.4f} "
          f"{r['insert_max_ms']:>9.2f} {r['find_max_ms']:>9.2f}")

# ── Summary ───────────────────────────────────────────────────────────────
header("SUMMARY FOR THESIS")
print()
best_jain_hold = max(rows, key=lambda r: r["jain_hold"])
best_jain_ops  = max(rows, key=lambda r: r["jain_ops"])
best_tput      = max(rows, key=lambda r: r["total_ops"])

print(f"  Best Jain (hold time)   : {best_jain_hold['name']:<32}  {best_jain_hold['jain_hold']:.4f}")
print(f"  Best Jain (ops/LOT)     : {best_jain_ops['name']:<32}  {best_jain_ops['jain_ops']:.4f}")
print(f"  Best throughput         : {best_tput['name']:<32}  {best_tput['total_ops']:,} ops")
print()
print("  Jain formula: J(x1..xn) = (Σxi)² / (n · Σxi²)")
print("  Hold-time Jain: xi = lock_hold_time[thread_i]")
print("  LOT Jain      : xi = acquisitions[thread_i]  (SCL paper definition)")
print()
print("  KEY: LOT Jain ≈ 1.0 for all H-SCL variants → equal lock opportunity")
print("       Hold-time Jain < 1.0 reflects workload CS asymmetry, not unfairness")
print()

# ── Weighted Jain (for unequal priority experiments) ─────────────────────
header("WEIGHTED JAIN (for unequal thread priorities)")
print()
print("  Formula: xi = cs_time[i] / weight[i]  where weight from prio_to_weight[]")
print("  Use this when threads have different nice values.")
print()

prio_to_weight = [
    88761,71755,56483,46273,36291,29154,23254,18705,14949,11916,
     9548, 7620, 6100, 4904, 3906, 3121, 2501, 1991, 1586, 1277,
     1024,  820,  655,  526,  423,  335,  272,  215,  172,  137,
      110,   87,   70,   56,   45,   36,   29,   23,   18,   15,
]

# Example: insert nice=+2 (w=655), find nice=-2 (w=1586)
w_insert = prio_to_weight[2 + 20]   # 655
w_find   = prio_to_weight[-2 + 20]  # 1586

for r in rows:
    n = NTHREADS_PER_GROUP
    ih = r["insert_hold"]
    fh = r["find_hold"]
    # weighted xi = hold_time / weight
    xi_list = [ih / w_insert] * n + [fh / w_find] * n
    jw = jains(xi_list)
    print(f"  {r['name']:<34}  weighted Jain = {jw:.4f}")

print()
print(SEP)