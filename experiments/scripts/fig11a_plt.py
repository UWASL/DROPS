#!/usr/bin/env python3
"""
"""

import argparse
from pathlib import Path
import re
import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import to_rgb
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import FuncFormatter

# Default hatches for cost components
def default_hatches():
    return {
        "User time":       ".",
        "System overhead": "",
        "Idle containers": "//",
        "Idle VMs":        "\\\\",
    }

# ──────────────────────── utilities ────────────────────────
PREFERRED = ["P100", "P99.99", " P99.9", "P99", "P95"]

def ordered_percentiles(series: pd.Series) -> list[str]:
    """Return percentiles in preferred order, then remaining high→low."""
    available = list(series.dropna().unique())

    def pop_ci(target):
        for s in available:
            if s.lower() == target.lower():
                available.remove(s)
                return s
        return None

    ordered = [pop_ci(p) for p in PREFERRED]
    ordered = [p for p in ordered if p]

    def numeric(s):
        m = re.search(r"\d+(?:\.\d+)?", s)
        return float(m.group()) if m else -1.0

    ordered += sorted(available, key=numeric, reverse=True)
    return ordered

# ──────────────────────────── main ────────────────────────────

def parse_cli():
    ap = argparse.ArgumentParser(
        description="Plot failure rate vs. stacked cost components by percentile"
    )
    ap.add_argument(
        "--cost_dir", default=".",
        help="Directory containing *_DROPS-average_*_average_core_time.csv files"
    )
    ap.add_argument(
        "--failure_csv", default="./cost.csv",
        help="CSV file containing failure‑rate data"
    )
    ap.add_argument(
        "--failure_metric", default="Failure Rate",
        help="Column for failure‑rate metric"
    )
    ap.add_argument(
        "--out", default="fig11a.pdf",
        help="Output PDF filename"
    )
    ap.add_argument(
        "--fontsize", type=int, default=11,
        help="Base font size for all text in the plot"
    )
    return ap.parse_args()


def lighten_color(color, amount: float) -> tuple:
    """
    Lighten the given RGB color by blending with white.
    amount=0 returns original, amount=1 returns white.
    """
    r, g, b = to_rgb(color)
    return (1 - amount) * r + amount, (1 - amount) * g + amount, (1 - amount) * b + amount


def main():
    args = parse_cli()
    EXP = "DROPS"

    # Apply font size
    plt.rcParams.update({
        'font.size': args.fontsize,
        'hatch.linewidth': 0.2,
    })
        # Set global font size
    plt.rcParams.update({
        'axes.titlesize': args.fontsize,
        'axes.labelsize': args.fontsize,
        'xtick.labelsize': args.fontsize,
        'ytick.labelsize': args.fontsize,
    })

    # 1) load and prepare failure data
    df_fail = pd.read_csv(Path(args.failure_csv))
    df_fail = df_fail[df_fail["Exp"] == EXP]
    if df_fail.empty:
        raise ValueError(f"No rows for Exp={EXP} in {args.failure_csv}")

    # 2) discover cost files
    pattern = f"*-{EXP}-*-average_core_time.csv"
    files = sorted(glob.glob(str(Path(args.cost_dir) / pattern)))
    if not files:
        raise ValueError(f"No cost files matching {pattern} in {args.cost_dir}")

    # 3) parse each cost file
    pctiles = []
    cost_components = {comp: [] for comp in default_hatches().keys()}

    for fn in files:
        # fn = fn.split("/")[-1]
        m = re.search(rf"-{EXP}-([\d\.]+)-average_core_time\.csv$", fn)
        if not m:
            continue
        p = m.group(1)
        pctiles.append(p)

        df = pd.read_csv(fn)
        last = df.iloc[-1]
        # raw component core-hours
        user      = last["P-User"] / 3600
        sys_over  = (last["H-Boot"] + last["P-Create"] + last["P-Alloc"] + last["P-Deleted"] + last["P-Recycled"] + last["P-Pending"] )/ 3600
        idle_cont = last["P-Ready"] / 3600
        idle_vm   = last["H-Idle"] / 3600
        
        

        cost_components["User time"].append(user)
        cost_components["System overhead"].append(sys_over)
        cost_components["Idle containers"].append(idle_cont)
        cost_components["Idle VMs"].append(idle_vm)

    # determine ordering of percentiles
    ordered = ordered_percentiles(pd.Series(pctiles))
    idx = [pctiles.index(p) for p in ordered]
    for k in cost_components:
        cost_components[k] = [cost_components[k][i] for i in idx]
    fail_vals = (
        df_fail.set_index("Percentile")[args.failure_metric]
        .values
    )

    # prepare colors: single base color lightening
    base_color = "#325d6e"
    n_comp = len(cost_components)
    # lighter shades from darkest at bottom to lightest at top
    shades = [lighten_color(base_color, i/(n_comp-1)) for i in range(n_comp)]

    # 4) plot
    x = np.arange(len(ordered))
    width = 0.3
    with PdfPages(args.out) as pdf:
        fig, ax1 = plt.subplots(figsize=(3.7, 2.5))
        ax2 = ax1.twinx()

        # failure bars
        bars1 = ax1.bar(
            x - width/2,
            fail_vals,
            width,
            label="Failure Rate",
            facecolor="#fcc404",
            edgecolor="black",
        )

        # stacked cost bars (core‑hours)
        bottom = np.zeros_like(x, dtype=float)
        hatches = default_hatches()
        for (comp, vals), shade in zip(cost_components.items(), shades):
            bars = ax2.bar(
                x + width/2,
                vals,
                width,
                bottom=bottom,
                label=comp,
                facecolor=shade,
                edgecolor="black",
                hatch=hatches.get(comp, "")
            )
            bottom += np.array(vals)

        # annotate failure
        for bar in bars1:
            h = bar.get_height()
            ax1.annotate(f"{h:.1f}",
                         (bar.get_x()+bar.get_width()/2, h),
                         xytext=(0, 2), textcoords="offset points",
                         ha="center", va="bottom", rotation=90)
        
                    
            

        ax1.set_xlabel("SLO")
        ax1.set_ylabel("Failure Rate (%)")
        ax1.set_ylim(0, 20)
        ax2.set_ylabel("Core Hours")
        ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{x/100000:.0f}"))
        ax2.text(1.0, 1, "1e5", transform=ax2.transAxes, ha="right", va="bottom")
        ax1.set_xticks(x)
        ax1.set_xticklabels(PREFERRED, ha="center")
        # ax1.grid(axis="y", linestyle=":", linewidth=0.4, alpha=0.7)
        ax1.set_axisbelow(True)
        ax1.margins(y=0, x=0.03)

        fig.tight_layout()
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

    print(f"Saved generated figure to {args.out}")


if __name__ == "__main__":
    main()
