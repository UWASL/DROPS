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

# Default hatches for cost components
def default_hatches():
    return {
        "User time":       "....",
        "System overhead": "",
        "Idle containers": "//",
        "Idle VMs":        "\\\\",
    }

# ───────────────────────── utilities ─────────────────────────
def parse_cli():
    ap = argparse.ArgumentParser(
        description="Plot failure rate vs. stacked cost components by scale-down factor"
    )
    ap.add_argument(
        "--cost_dir", default=".",
        help="Directory containing *-reactive-*-*-*-average_core_time.csv files"
    )
    ap.add_argument(
        "--failure_csv", default="./cost.csv",
        help="CSV file containing failure-rate data"
    )
    ap.add_argument(
        "--failure_metric", default="Failure Rate",
        help="Column for failure-rate metric"
    )
    ap.add_argument(
        "--out", default="fig10b.pdf",
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
    return ((1 - amount) * r + amount,
            (1 - amount) * g + amount,
            (1 - amount) * b + amount)


def main():
    args = parse_cli()

    # 1) discover cost files matching reactive experiment
    pattern = "*-Reactive-*-*-*-average_core_time.csv"
    files = sorted(glob.glob(str(Path(args.cost_dir) / pattern)))
    if not files:
        raise ValueError(f"No cost files matching {pattern} in {args.cost_dir}")

    scaledowns = []
    cost_components = {comp: [] for comp in default_hatches().keys()}

    for fn in files:
        # capture second number as scale-down
        m = re.search(r"-Reactive-[0-9\.]+-([0-9\.]+)-\d+-average_core_time\.csv$", fn)
        if not m:
            continue
        sd = float(m.group(1))
        scaledowns.append(sd)

        df = pd.read_csv(fn)
        last = df.iloc[-1]

        # raw component core-hours
        user      = last["P-User"] / 3600
        sys_over  = (last["H-Boot"] + last["P-Create"]
                     + last["P-Alloc"] + last["P-Deleted"]) / 3600
        idle_cont = last["P-Ready"] / 3600
        idle_vm   = last["H-Idle"]  / 3600

        cost_components["User time"].append(user)
        cost_components["System overhead"].append(sys_over)
        cost_components["Idle containers"].append(idle_cont)
        cost_components["Idle VMs"].append(idle_vm)

    # derive requests per second = 1 / scaledown
    rates = [1.0/sd for sd in scaledowns]
    # order by rate ascending
    order_idx = list(np.argsort(rates))
    rates = [rates[i] for i in order_idx]
    for comp in cost_components:
        cost_components[comp] = [cost_components[comp][i] for i in order_idx]

    # 2) load failure data from pool-cost CSV
    df_fail = pd.read_csv(Path(args.failure_csv))
    if "Exp" not in df_fail.columns:
        raise ValueError("failure CSV must have 'Exp' column")
    # filter for 'reactive' experiments
    df_fail = df_fail[df_fail["Exp"].str.lower().str.startswith("reactive-")]
    # extract scale-down from Exp
    df_fail["scaledown"] = (
        df_fail["Exp"].str.extract(r"Reactive-[0-9\.]+-([0-9\.]+)")[0]
        .astype(float)
    )
    df_fail["rate"] = 1.0 / df_fail["scaledown"]

    # map rate -> failure metric
    fail_map = df_fail.set_index("rate")[args.failure_metric]
    # align failure values to ordered rates
    fail_vals = [fail_map.loc(r) for r in rates] if False else [fail_map.loc[r] for r in rates]

    # 3) plotting
    plt.rcParams.update({
        'font.size': args.fontsize,
        'hatch.linewidth': 0.2,
        'axes.titlesize': args.fontsize,
        'axes.labelsize': args.fontsize,
        'xtick.labelsize': args.fontsize,
        'ytick.labelsize': args.fontsize,
    })

    x = np.arange(len(rates))
    width = 0.4
    base_color = "#325d6e"
    n_comp = len(cost_components)
    shades = [lighten_color(base_color, i/(n_comp-1)) for i in range(n_comp)]

    with PdfPages(args.out) as pdf:
        fig, ax1 = plt.subplots(figsize=(3.5, 2.5))
        ax2 = ax1.twinx()

        # failure bars
        bars_fail = ax1.bar(
            x - width/2,
            fail_vals,
            width,
            label="Failure Rate",
            facecolor="lightgray",
            edgecolor="black",
        )

        # stacked cost bars
        bottom = np.zeros_like(x, dtype=float)
        hatches = default_hatches()
        for (comp, vals), shade in zip(cost_components.items(), shades):
            ax2.bar(
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
        # annotate failure bars
        for bar in bars_fail:
            h = bar.get_height()
            ax1.annotate(f"{h:.1f}",
                         (bar.get_x() + bar.get_width()/2, h),
                         xytext=(0, 2), textcoords="offset points",
                         ha="center", va="bottom", rotation=90)
            
        # Print cost values for debugging
        for i, rate in enumerate(rates):
            sum = 0
            for comp, vals in cost_components.items():
                sum += vals[i]

        ax1.set_xlabel("Scale-down Factor")
        ax1.set_ylabel("Failure Rate (%)")
        ax2.set_ylabel("Cost (Core Hours)")
        ax1.set_ylim(0, 30)
        ax1.set_xticks(x)
        ax1.set_xticklabels([f"{r:.1f}" for r in rates])
        # ax1.grid(axis="y", linestyle=":", linewidth=0.4, alpha=0.7)
        ax1.set_axisbelow(True)
        ax1.legend(
            loc="upper right",
            ncol=2,
            fontsize=args.fontsize - 2,
            frameon=False,
        )

        fig.tight_layout()
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

    print(f"Saved generated figure to {args.out}")

if __name__ == "__main__":
    main()
