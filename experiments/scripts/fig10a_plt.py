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
        description="Plot failure rate vs. stacked cost components by scale factor"
    )
    ap.add_argument(
        "--cost_dir", default=".",
        help="Directory containing *-Reactive-*-*-*-average_core_time.csv files"
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
        "--out", default="fig10a.pdf",
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

    scale_factors = []
    cost_components = {comp: [] for comp in default_hatches().keys()}

    for fn in files:
        m = re.search(r"-Reactive-([\d\.]+)-\d+-\d+-average_core_time\.csv$", fn)
        if not m:
            continue
        sf = m.group(1)
        scale_factors.append(sf)

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

    # Order by numeric scale factor ascending
    ordered = sorted(scale_factors, key=lambda x: float(x))
    idx = [scale_factors.index(sf) for sf in ordered]
    for comp in cost_components:
        cost_components[comp] = [cost_components[comp][i] for i in idx]

    # 2) load failure data from pool-cost CSV
    df_fail = pd.read_csv(Path(args.failure_csv))
    if "Exp" not in df_fail.columns:
        raise ValueError("failure CSV must have 'Exp' column")
    # filter for 'reactive' experiments
    df_fail = df_fail[df_fail["Exp"].str.lower().str.startswith("reactive-")]
    # extract scale_factor from Exp (e.g., 'reactive-1.5-1')
    df_fail["scale_factor"] = (
        df_fail["Exp"].str.extract(r"Reactive-([0-9\.]+)-")[0]
        .astype(float)
    )
    # build mapping of scale -> failure metric
    fail_map = df_fail.set_index("scale_factor")[args.failure_metric]
    # align failure values to ordered scale factors
    fail_vals = [fail_map.loc[float(sf)] for sf in ordered]

    # 3) plotting
    plt.rcParams.update({
        'font.size': args.fontsize,
        'hatch.linewidth': 0.2,
        'axes.titlesize': args.fontsize,
        'axes.labelsize': args.fontsize,
        'xtick.labelsize': args.fontsize,
        'ytick.labelsize': args.fontsize,
    })

    x = np.arange(len(ordered))
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

        ax1.set_xlabel("Scale-up Factor")
        ax1.set_ylabel("Failure Rate (%)")
        ax1.set_ylim(0,30)
        ax2.set_ylabel("Cost (Core Hours)")
        ax1.set_xticks(x)
        ax1.set_xticklabels(ordered)
        ax1.set_axisbelow(True)
        ax1.legend(
            loc="upper left",
            fontsize=args.fontsize - 2,
            frameon=False,
        )
        fig.tight_layout()
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

    print(f"Saved generated figure to {args.out}")

if __name__ == "__main__":
    main()
