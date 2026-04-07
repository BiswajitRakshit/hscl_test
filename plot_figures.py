#!/usr/bin/env python3
"""
plot_figures.py — Reproduces Figure 4.1 and Figure 4.2 from thesis
====================================================================
Figure 4.1: Lock Hold Time summation across locks over threads (log scale)
Figure 4.2: Jain's fairness index comparison between locks over threads

Usage:
    python3 plot_figures.py                      # reads ./results/
    python3 plot_figures.py --results-dir DIR    # reads DIR/
    python3 plot_figures.py --no-pinning         # title says "without pinning"

Output:
    figure_4_1_lock_hold_time.png
    figure_4_2_jains_fairness.png
    figure_4_1_and_4_2_combined.png
"""

import os
import re
import sys
import glob
import argparse
import math
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    import numpy as np
except ImportError:
    print("Installing matplotlib...")
    os.system("pip3 install matplotlib numpy --break-system-packages -q")
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    import numpy as np


# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_file(path):
    try:
        text = open(path).read()
    except Exception:
        return None

    def get(pattern):
        m = re.search(pattern, text)
        return float(m.group(1)) if m else None

    insert_ops  = get(r"insert_#ops\s+(\d+)")
    find_ops    = get(r"find_#ops\s+(\d+)")
    insert_rate = get(r"insert_#ops\s+\d+\s+\(([0-9.]+)/sec\)")
    find_rate   = get(r"find_#ops\s+\d+\s+\(([0-9.]+)/sec\)")

    lat_i = re.search(r"insert_latency.*?([0-9.]+),\s*([0-9.]+),\s*([0-9.]+)", text)
    lat_f = re.search(r"find_latency.*?([0-9.]+),\s*([0-9.]+),\s*([0-9.]+)", text)

    if not all([insert_ops, find_ops, lat_i, lat_f]):
        return None

    return {
        "insert_ops":  insert_ops,
        "find_ops":    find_ops,
        "insert_rate": insert_rate or 0,
        "find_rate":   find_rate   or 0,
        "insert_avg":  float(lat_i.group(2)),
        "find_avg":    float(lat_f.group(2)),
        "insert_max":  float(lat_i.group(3)),
        "find_max":    float(lat_f.group(3)),
    }


def load_results(results_dir):
    """Returns { lock_name -> { threads -> parsed_data } }"""
    found = defaultdict(dict)
    for f in sorted(glob.glob(os.path.join(results_dir, "*.txt"))):
        base = os.path.basename(f).replace(".txt", "")
        m = re.match(r"^(.+)_t(\d+)$", base)
        if not m:
            continue
        lock, T = m.group(1), int(m.group(2))
        data = parse_file(f)
        if data:
            found[lock][T] = data
    return found


# ── Metrics ───────────────────────────────────────────────────────────────────

def jains(values):
    n = len(values); s = sum(values); sq = sum(v*v for v in values)
    return (s*s)/(n*sq) if sq > 0 else 0.0


def total_lock_hold_ms(data, nthreads):
    """Total lock hold time across ALL threads in milliseconds."""
    n = max(nthreads // 2, 1)
    ih = (data["insert_ops"] / n) * data["insert_avg"] * 1000   # ms
    fh = (data["find_ops"]   / n) * data["find_avg"]   * 1000   # ms
    return (ih + fh) * n * 2   # sum across all threads


def jain_hold(data, nthreads):
    n = max(nthreads // 2, 1)
    ih = (data["insert_ops"] / n) * data["insert_avg"]
    fh = (data["find_ops"]   / n) * data["find_avg"]
    return jains([ih]*n + [fh]*n)


# ── Plot styling (matches thesis figures) ────────────────────────────────────

# Lock display names and marker/line styles matching thesis Figure 4.1 legend
LOCK_STYLE = {
    "hscl":          {"label": "hfairlock", "marker": "o",  "color": "#1f77b4", "ls": "-"},
    "boost_mutex":   {"label": "mutex",     "marker": "--", "color": "#7f7f7f", "ls": "--"},
    "pthread_mutex": {"label": "pthread mutex","marker":"s","color": "#ff7f0e", "ls": "-"},
    "pthread_spin":  {"label": "spinlock",  "marker": "^",  "color": "#2ca02c", "ls": "-"},
    "ticket":        {"label": "ticket",    "marker": "D",  "color": "#d62728", "ls": "-"},
    # Placeholders for other NUMA locks from thesis (if you run them later)
    "cptkt":         {"label": "cptkt",     "marker": "<",  "color": "#9467bd", "ls": "-"},
    "cttkt":         {"label": "cttkt",     "marker": ">",  "color": "#8c564b", "ls": "-"},
    "hmcs":          {"label": "hmcs",      "marker": "v",  "color": "#e377c2", "ls": "-"},
    "htepfl":        {"label": "htepfl",    "marker": "p",  "color": "#17becf", "ls": "-"},
    "hymcs":         {"label": "hymcs",     "marker": "h",  "color": "#bcbd22", "ls": "-"},
}

# Preferred ordering (hfairlock first, mutex last — matches thesis)
LOCK_ORDER = ["hscl", "cptkt", "cttkt", "hmcs", "htepfl", "hymcs",
              "ticket", "pthread_spin", "pthread_mutex", "boost_mutex"]


def ordered_locks(found):
    present = set(found.keys())
    ordered = [l for l in LOCK_ORDER if l in present]
    extras  = [l for l in sorted(present) if l not in LOCK_ORDER]
    return ordered + extras


def get_style(lock):
    default = {"label": lock, "marker": "x", "color": "#333333", "ls": "-"}
    return LOCK_STYLE.get(lock, default)


# ── Figure 4.1 — Lock Hold Time ───────────────────────────────────────────────

def plot_fig41(found, output_path, pinning=True):
    fig, ax = plt.subplots(figsize=(5.5, 4.2))

    all_threads = sorted({T for ld in found.values() for T in ld})
    x_vals = all_threads
    x_ticks = [2**i for i in range(
        int(math.log2(min(x_vals))), int(math.log2(max(x_vals)))+1
    )] if x_vals else x_vals

    for lock in ordered_locks(found):
        style = get_style(lock)
        ys = []
        xs = []
        for T in sorted(found[lock].keys()):
            hold = total_lock_hold_ms(found[lock][T], T)
            ys.append(hold)
            xs.append(T)
        if not xs:
            continue
        marker = style["marker"] if style["marker"] != "--" else "o"
        ax.plot(xs, ys,
                label=style["label"],
                marker=marker,
                color=style["color"],
                linestyle=style["ls"],
                linewidth=1.5,
                markersize=5)

    ax.set_yscale("log")
    ax.set_xscale("log", base=2)
    ax.set_xticks(x_ticks)
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.set_xlabel("No of threads", fontsize=10)
    ax.set_ylabel("Lock Hold time (in ms)", fontsize=10)
    pin_str = "with pinning" if pinning else "without pinning"
    ax.set_title(f"Lock Hold summation across locks over threads {pin_str}", fontsize=9)
    ax.legend(fontsize=7, loc="upper right", framealpha=0.8)
    ax.grid(True, which="both", alpha=0.3)
    ax.tick_params(labelsize=8)

    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


# ── Figure 4.2 — Jain's Fairness Index ───────────────────────────────────────

def plot_fig42(found, output_path, pinning=True):
    fig, ax = plt.subplots(figsize=(5.5, 4.2))

    all_threads = sorted({T for ld in found.values() for T in ld})
    x_ticks = [2**i for i in range(
        int(math.log2(min(all_threads))), int(math.log2(max(all_threads)))+1
    )] if all_threads else all_threads

    for lock in ordered_locks(found):
        style = get_style(lock)
        xs, ys = [], []
        for T in sorted(found[lock].keys()):
            jf = jain_hold(found[lock][T], T)
            xs.append(T)
            ys.append(jf)
        if not xs:
            continue
        marker = style["marker"] if style["marker"] != "--" else "o"
        ax.plot(xs, ys,
                label=style["label"],
                marker=marker,
                color=style["color"],
                linestyle=style["ls"],
                linewidth=1.5,
                markersize=5)

    ax.set_xscale("log", base=2)
    ax.set_xticks(x_ticks)
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.set_ylim(0, 1.05)
    ax.set_xlabel("No of threads", fontsize=10)
    ax.set_ylabel("Jain's Fairness Index", fontsize=10)
    pin_str = "with pinning" if pinning else "without pinning"
    ax.set_title(f"Jain's fairness index comparision between locks {pin_str}", fontsize=9)
    ax.legend(fontsize=7, loc="lower left", framealpha=0.8)
    ax.grid(True, which="both", alpha=0.3)
    ax.tick_params(labelsize=8)

    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


# ── Combined side-by-side (thesis layout) ────────────────────────────────────

def plot_combined(found, output_path, pinning=True):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
    pin_str = "with pinning" if pinning else "without pinning"

    all_threads = sorted({T for ld in found.values() for T in ld})
    if not all_threads:
        print("  No data to plot.")
        return
    x_ticks = [2**i for i in range(
        int(math.log2(min(all_threads))), int(math.log2(max(all_threads)))+1
    )]

    for lock in ordered_locks(found):
        style = get_style(lock)
        marker = style["marker"] if style["marker"] != "--" else "o"
        xs = sorted(found[lock].keys())

        # Fig 4.1
        ys1 = [total_lock_hold_ms(found[lock][T], T) for T in xs]
        ax1.plot(xs, ys1, label=style["label"], marker=marker,
                 color=style["color"], linestyle=style["ls"],
                 linewidth=1.5, markersize=5)

        # Fig 4.2
        ys2 = [jain_hold(found[lock][T], T) for T in xs]
        ax2.plot(xs, ys2, label=style["label"], marker=marker,
                 color=style["color"], linestyle=style["ls"],
                 linewidth=1.5, markersize=5)

    # Axes 1 — hold time
    ax1.set_yscale("log")
    ax1.set_xscale("log", base=2)
    ax1.set_xticks(x_ticks)
    ax1.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax1.set_xlabel("No of threads", fontsize=10)
    ax1.set_ylabel("Lock Hold time (in ms)", fontsize=10)
    ax1.set_title(f"Lock Hold summation across locks\nover threads {pin_str}", fontsize=9)
    ax1.legend(fontsize=7, loc="upper right", framealpha=0.8)
    ax1.grid(True, which="both", alpha=0.3)
    ax1.tick_params(labelsize=8)

    # Axes 2 — fairness
    ax2.set_xscale("log", base=2)
    ax2.set_xticks(x_ticks)
    ax2.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax2.set_ylim(0, 1.05)
    ax2.set_xlabel("No of threads", fontsize=10)
    ax2.set_ylabel("Jain's Fairness Index", fontsize=10)
    ax2.set_title(f"Jain's fairness index comparision\nbetween locks {pin_str}", fontsize=9)
    ax2.legend(fontsize=7, loc="lower left", framealpha=0.8)
    ax2.grid(True, which="both", alpha=0.3)
    ax2.tick_params(labelsize=8)

    fig.tight_layout(pad=2.0)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


# ── Data summary ──────────────────────────────────────────────────────────────

def print_data_table(found):
    all_threads = sorted({T for ld in found.values() for T in ld})
    print("\n  Lock Hold Time (ms total) per thread count:")
    print(f"  {'Lock':<25}" + "".join(f"  T={T:>3}" for T in all_threads))
    print("  " + "-"*(25 + 8*len(all_threads)))
    for lock in ordered_locks(found):
        style = get_style(lock)
        row = f"  {style['label']:<25}"
        for T in all_threads:
            if T in found[lock]:
                h = total_lock_hold_ms(found[lock][T], T)
                row += f"  {h:>6,.0f}"
            else:
                row += f"  {'N/A':>6}"
        print(row)

    print(f"\n  Jain's Fairness Index per thread count:")
    print(f"  {'Lock':<25}" + "".join(f"  T={T:>3}" for T in all_threads))
    print("  " + "-"*(25 + 8*len(all_threads)))
    for lock in ordered_locks(found):
        style = get_style(lock)
        row = f"  {style['label']:<25}"
        for T in all_threads:
            if T in found[lock]:
                j = jain_hold(found[lock][T], T)
                row += f"  {j:>6.4f}"
            else:
                row += f"  {'N/A':>6}"
        print(row)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--output-dir",  default=".")
    parser.add_argument("--no-pinning",  action="store_true")
    args = parser.parse_args()

    rdir = args.results_dir
    if not os.path.isdir(rdir):
        print(f"Results directory '{rdir}' not found. Run 'make run' first.")
        sys.exit(1)

    print(f"Loading results from: {os.path.abspath(rdir)}")
    found = load_results(rdir)
    if not found:
        print("No valid result files found.")
        sys.exit(1)

    pinning = not args.no_pinning
    locks_found = list(found.keys())
    thread_counts = sorted({T for ld in found.values() for T in ld})
    print(f"Locks found   : {', '.join(locks_found)}")
    print(f"Thread counts : {thread_counts}")

    print_data_table(found)

    odir = args.output_dir
    os.makedirs(odir, exist_ok=True)

    print("\nGenerating plots...")
    plot_fig41(found,
               os.path.join(odir, "figure_4_1_lock_hold_time.png"),
               pinning=pinning)
    plot_fig42(found,
               os.path.join(odir, "figure_4_2_jains_fairness.png"),
               pinning=pinning)
    plot_combined(found,
                  os.path.join(odir, "figure_4_1_and_4_2_combined.png"),
                  pinning=pinning)

    print("\nDone. Files created:")
    for f in ["figure_4_1_lock_hold_time.png",
              "figure_4_2_jains_fairness.png",
              "figure_4_1_and_4_2_combined.png"]:
        fp = os.path.join(odir, f)
        if os.path.exists(fp):
            print(f"  {fp}")


if __name__ == "__main__":
    main()