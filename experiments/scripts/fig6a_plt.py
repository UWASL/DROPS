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

# Adjust hatch line width
plt.rcParams['hatch.linewidth'] = 0.2


LABEL_MAP = {
    "Production": ("Production", "#D62728", ""),       
    "DROPS": ("DROPS", "#FFC107", ""),              
    "model-PredictionConcentratedLoad-3600-sec": ("Predictive", "#4E79A7", ""),  
    "Reactive-1.35-1": ("Reactive", "#59A14F", ""),                    
    "model-PredictiveReactive-3600-sec": ("Predictive-reactive", "#9C755F", ""),  
}

# ─── cost components and their hatches ────────────────────────
COST_FUNCS = {
    "User time":       lambda r: r["P-User"]/3600,
    "System overhead": lambda r: (r["H-Boot"] + r["P-Create"] + r["P-Pending"] + r["P-Alloc"] + r["P-Deleted"] + r["P-Recycled"])/3600,
    "Idle containers": lambda r: r["P-Ready"]/3600,
    "Idle VMs":        lambda r: r["H-Idle"]/3600,
}

COST_HATCHES = {
    "User time":       "....",
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
    ap.add_argument("--failure_csv", default="./cost.csv")
    ap.add_argument("--cost_dir",    default=".")
    ap.add_argument("--percentile",  default="P100",
                    help="Which SLO percentile to plot (e.g. P99)")
    ap.add_argument("--metric",      default="Failure Rate")
    ap.add_argument("--out",         default="fig6a.pdf")
    ap.add_argument("--font-size",   type=int, default=11,
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
    lookup = pctile if pctile != "P100" else "1"
    for exp in exps:
        pattern = f"*{exp}-{lookup}-average_core_time.csv"
        files = sorted(glob.glob(str(Path(cost_dir)/pattern)))
        if not files:
            raise FileNotFoundError(f"No cost file for {exp}@{pctile}")
        last = pd.read_csv(files[-1]).iloc[-1]
        sum = 0
        for comp, fn in COST_FUNCS.items():
            value = fn(last)
            sum += value
            breakdown[comp].append(fn(last))
        syear = sum / 7 
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

    fig, ax1 = plt.subplots(figsize=(3.5, 3))
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
    handles = [Patch(facecolor=LABEL_MAP[exp][1], edgecolor='black', label=LABEL_MAP[exp][0]) for exp in exps]
    ax1.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.5, 1.35), ncol=3, fontsize=8, frameon=False)

    # 2) Cost cluster (stacked, per component) with lightening
    bottom = np.zeros(m)
    num_comps = len(COST_FUNCS)
    bars = []
    for idx, comp in enumerate(COST_FUNCS):
        vals = cost_break[comp]
        hatch = COST_HATCHES[comp]
        amount = idx / (num_comps - 1) * 0.5 if num_comps > 1 else 0
        base_colors = [LABEL_MAP[e][1] for e in exps]
        
        colors = [lighten_color(col, amount) for col in base_colors]
        bars = ax2.bar(xs_cost, vals, width,
                bottom=bottom,
                label=comp,
                color=colors,
                edgecolor='black',
                hatch=hatch)
        bottom += np.array(vals)
        
    # styling
    ax1.set_xticks([xs_fail.mean(), xs_cost.mean()])
    ax1.set_xticklabels(["Failure Rate", "Cost"])
    ax1.set_ylabel("Failure Rate (%)")
    ax2.set_ylabel("Cost (Core Hours)")
    ax1.set_axisbelow(True)
    ax1.set_ylim(0, 30)
    
    ax1.set_xlabel("     ")
    fig.tight_layout()
    
    # save to PDF
    with PdfPages(args.out) as pdf:
        pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved generated figure to {args.out}")

if __name__ == "__main__":
    main()