#!/usr/bin/env python3
import argparse
import glob
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Patch
import matplotlib.colors as mcolors
from matplotlib.ticker import FuncFormatter

# Adjust hatch line width
plt.rcParams['hatch.linewidth'] = 0.2



LABEL_MAP = {
    "DROPS": ("Non-Aggressive", "#66b2b2", ""),
    "DROPS-Aggressive": ("Aggressive", "#4472c4", ""),
}


# ─── cost components and their hatches ────────────────────────
COST_FUNCS = {
    "User time":       lambda r: r["P-User"]/3600,
    "System overhead": lambda r: (r["H-Boot"] + r["P-Create"] + r["P-Pending"] + r["P-Alloc"] + r["P-Deleted"] + r["P-Recycled"])/3600,
    "Idle containers": lambda r: r["P-Ready"]/3600,
    "Idle VMs":        lambda r: r["H-Idle"]/3600,
}

COST_HATCHES = {
    "User time":       ".",
    "System overhead": "",
    "Idle containers": "//",
    "Idle VMs":        "\\\\",
}

def lighten_color(color, amount=0.5):
    """
    Lightens the given color by mixing it with white.
    Amount should be between 0 and 1. 0 returns original color, 1 returns white.
    """
    c = np.array(mcolors.to_rgb(color))
    return tuple(c + (1.0 - c) * amount)


def parse_args():
    ap = argparse.ArgumentParser(
        description="Two-cluster: left=failure, right=stacked cost"
    )
    ap.add_argument("--failure_csv", default="cost.csv")
    ap.add_argument("--cost_dir",    default=".")
    ap.add_argument("--percentile",  default="P95",
                    help="Which SLO percentile to plot (e.g. P99)")
    ap.add_argument("--metric",      default="Failure Rate")
    ap.add_argument("--out",         default="fig12a.pdf")
    ap.add_argument("--font_size",   type=int, default=11,
                    help="Base font size for plot")
    return ap.parse_args()


def gather_failure(df_fail, exps, pctile, metric):
    df = df_fail[
        (df_fail["Exp"].isin(exps)) &
        (df_fail["Percentile"] == pctile)
    ]
    return [float(df.loc[df["Exp"]==e, metric]) for e in exps]


def gather_cost_breakdown(cost_dir, exps, pctile):
    breakdown = {comp: [] for comp in COST_FUNCS}
    lookup = pctile.replace('P', '0.') if pctile != "P100" else "1"
    for exp in exps:
        pattern = f"*{exp}-{lookup}-average_core_time.csv"
        files = sorted(glob.glob(str(Path(cost_dir)/pattern)))
        if not files:
            raise FileNotFoundError(f"No cost file for {exp}@{pctile}")
        last = pd.read_csv(files[-1]).iloc[-1]
        for comp, fn in COST_FUNCS.items():
            value = fn(last)
            breakdown[comp].append(fn(last))
    return breakdown


def main():
    args = parse_args()

    # Set global font size
    plt.rcParams.update({
        'font.size': args.font_size,
        'axes.titlesize': args.font_size,
        'axes.labelsize': args.font_size,
        'xtick.labelsize': args.font_size,
        'ytick.labelsize': args.font_size,
    })

    df_fail = pd.read_csv(Path(args.failure_csv))
    exps = [e for e in LABEL_MAP if e in df_fail["Exp"].unique()]
    if not exps:
        raise ValueError("No matching LABEL_MAP keys in failure CSV")

    # collect data
    fail_vals = gather_failure(df_fail, exps, args.percentile, args.metric)
    cost_break = gather_cost_breakdown(args.cost_dir, exps, args.percentile)

    # layout
    m          = len(exps)
    separation = m + 1
    width      = 0.8

    xs_fail = np.arange(m)
    xs_cost = xs_fail + separation

    fig, ax1 = plt.subplots(figsize=(3.5, 2.5))
    ax2 = ax1.twinx()

    # 1) Failure cluste
    for i, exp in enumerate(exps):
        _, color, hatch = LABEL_MAP[exp]
        ax1.bar(xs_fail[i], fail_vals[i],
                width, facecolor=color,
                edgecolor='black', hatch=hatch)
        ax1.annotate(f"{fail_vals[i]:.1f}",
                     (xs_fail[i], fail_vals[i]),
                     xytext=(0,2), textcoords="offset points",
                     ha="center", va="bottom",
                     rotation=90, fontsize=(args.font_size)-1)
    # Add legend outside for colors
    plt.legend(handles=[Patch(facecolor=LABEL_MAP[e][1], edgecolor='black', label=LABEL_MAP[e][0]) for e in exps],
               fontsize=args.font_size-2, frameon=False,
               loc="upper center", bbox_to_anchor=(0.5, -0.28), ncol=len(exps))

    # 2) Cost cluster (stacked, per component) with lightening
    bottom = np.zeros(m)
    num_comps = len(COST_FUNCS)
    bars = []
    for idx, comp in enumerate(COST_FUNCS):
        vals = cost_break[comp]
        hatch = COST_HATCHES[comp]
        # compute lighten amount (up to 50% lighter at top)
        amount = idx / (num_comps - 1) * 0.5 if num_comps > 1 else 0
        # generate lighter colors per experiment base color
        base_colors = [LABEL_MAP[e][1] for e in exps]
        
        colors = [lighten_color(col, amount) for col in base_colors]
        bars = ax2.bar(xs_cost, vals, width,
                bottom=bottom,
                label=comp,
                color=colors,
                edgecolor='black',
                hatch=hatch)
        bottom += np.array(vals)
        
    # for bar in bars:
        # height = bar.get_height()
        # if height > bottom.max() * 0.2:
        # ax1.text(bar.get_x() + bar.get_width() / 2, 88, f'{  height*1.0/1000000000.0:.1f}',
        #          ha='center', va='bottom', color='black', rotation=90)
    

    # styling
    ax1.set_xticks([xs_fail.mean(), xs_cost.mean()])
    ax1.set_xticklabels(["Failure Rate", "Cost"])
    ax1.set_ylabel("Failure Rate (%)")
    ax2.set_ylabel("Cost (Core Hours)")
    ax1.grid(axis='y', linestyle=':', linewidth=0.4, alpha=0.7)
    ax1.set_axisbelow(True)
    ax1.set_ylim(0, 5)
    
    ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{x/100000:.0f}"))
    ax2.text(1.0, 1, "1e5", transform=ax2.transAxes, ha="right", va="bottom")
    ax2.set_ylim(0, bottom.max() * 1.1)
    ax1.set_xlabel("     ")
    fig.tight_layout()
    
    

    # save to PDF
    with PdfPages(args.out) as pdf:
        pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved generated figure to {args.out}")

if __name__ == "__main__":
    main()