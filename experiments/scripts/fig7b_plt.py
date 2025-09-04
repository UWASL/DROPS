#!/usr/bin/env python3
"""
"""

import argparse
from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Patch
import matplotlib.colors as mcolors

plt.rcParams["hatch.linewidth"] = 0.2

plt.rcParams.update({
        'font.size': 11,
        'axes.titlesize': 11,
        'axes.labelsize': 11,
        'xtick.labelsize': 11,
        'ytick.labelsize': 11,
    })

LABEL_MAP = {
    "perfect-PredictionConstantLoad-3600-sec":   ("Perfect‑Uniform‑60‑Min",   "#008080", "//"),
    "perfect-PredictionPoissonLoad-3600-sec":   ("Perfect‑Poisson‑60‑Min",   "#1f4e79", "//"),
    "perfect-PredictionConcentratedLoad-3600-sec": ("Perfect-Reactive-Pred-10-Sec", "#008080", "//"),
}

PREFERRED_ORDER = ["P1000", "P9999", "P999", "P990", "P950"]
XMAP = ["P100", "P99.99", "P99.9", "P99", "P95"]


DIST_STYLE_MAP = {
    "PredictionConstantLoad": ("Constant",     "#4E79A7", ""), 
    "PredictionPoissonLoad": ("Poisson",      "#F28E2B", ""),  
    "PredictionConcentratedLoad":   ("Concentrated", "#76B7B2", ""), 
}

INTERVALS = [
    (10,    "10 s"),
    (60,    "1 min"),
    (600,   "10 min"),
    (3600,  "1 h"),
]

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

def draw_cost_interval_page(df: pd.DataFrame,
                            percentile: str,
                            cost_dir: Path,
                            pdf: PdfPages):

    rows = df[(df["Exp"].str.startswith("perfect-")) &
              (df["Percentile"].str.casefold() == percentile.casefold())].copy()
    if rows.empty:
        raise ValueError(f"No perfect-predictor rows for {percentile}")

    parsed = rows["Exp"].apply(_parse_exp)
    rows[["dist", "secs"]] = pd.DataFrame(parsed.tolist(), index=rows.index)
    rows = rows.dropna(subset=["dist", "secs"]).astype({"secs": int})

    # ----- x-axis -----
    xloc   = np.arange(len(INTERVALS))
    n_bars = len(DIST_STYLE_MAP)
    width  = 0.8 / n_bars
    fig, ax = plt.subplots(figsize=(3.5, 2.5))

# ----- plot every distribution -----
    for i, (dist, (label, colour, dist_hatch)) in enumerate(DIST_STYLE_MAP.items()):
        bottoms = np.zeros(len(INTERVALS))
        base_color = colour  # base color for this distribution
        for idx, (comp, fn) in enumerate(COST_FUNCS.items()):
            amount = idx / (len(COST_FUNCS) - 1) * 0.5 if len(COST_FUNCS) > 1 else 0
            y = []
            for secs, _ in INTERVALS:
                exp_name = f"perfect-{dist}-{secs}-sec"
                try:
                    row_cost = read_core_time(exp_name, percentile, cost_dir)
                    y.append(fn(row_cost))
                except FileNotFoundError:
                    print(f"Warning: no cost data for {exp_name}@{percentile}")
                    y.append(0.0)  # keep bar placeholders aligned

            xpos = xloc - 0.4 + i * width + width / 2
            color = lighten_color(base_color, amount)  # vary color within group

            ax.bar(xpos, y, width,
                bottom=bottoms,
                color=color,
                edgecolor="black",
                linewidth=0.7,
                hatch=COST_HATCHES[comp] or dist_hatch,
                label=label if (comp == "User time") else None)  # once
            bottoms += np.array(y)


    # ----- cosmetics -----
    ax.set_xlabel("Prediction Interval")
    ax.set_ylabel("Cost (Core Hours)")
    ax.set_xticks(xloc)
    ax.set_xticklabels([lbl for _, lbl in INTERVALS])
    ax.grid(axis="y", linestyle=":", linewidth=0.4, alpha=0.7)
    ax.set_axisbelow(True)
    ax.margins(y=0.15)

    # build a combined legend: dists first, cost hatches second
    dist_handles = [Patch(facecolor=v[1], edgecolor="black", hatch=v[2], label=v[0])
                    for v in DIST_STYLE_MAP.values()]
    cost_handles = [Patch(facecolor="white", edgecolor="black",
                          hatch=COST_HATCHES[c], label=c)
                    for c in COST_FUNCS]
    ax.legend(dist_handles,
              [h.get_label() for h in dist_handles ],
              fontsize=10, frameon=False,
              ncol=1)
    plt.tight_layout()
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)
    
def read_core_time(exp: str, percentile: str, cost_dir: Path) -> pd.Series:
    """
    Return the *last* row of the core-time CSV that matches <exp> & <percentile>.
    File names look like “…{exp}-{lookup}-average_core_time.csv”
    where lookup is e.g. “P99” or “1” for P100.
    """
    lookup   = percentile if percentile != "P100" else "1"
    pattern  = f"*{exp}-{lookup}-average_core_time.csv"
    matches  = sorted(cost_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"no cost file for {exp}@{percentile}")
    return pd.read_csv(matches[-1]).iloc[-1]  

# ───────────────────────────────── CLI ─────────────────────────────────────────
def parse_cli() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--failure_csv", default="./cost.csv")
    ap.add_argument("--metric", default="Failure Rate")
    ap.add_argument("--percentile", default="P100",
                    help="Percentile row to plot on the interval page.")
    ap.add_argument("--cost_dir", default=".",
                    help="Directory that holds *core_time.csv files")
    ap.add_argument("--out", default="fig7b.pdf")
    return ap.parse_args()

# ───────────────────────── Percentile helpers/plot ────────────────────────────
def ordered_percentiles(series: pd.Series) -> list[str]:
    """Return percentiles in the preferred order, then remaining high→low."""
    available = list(series.unique())

    def pop_case_insensitive(target):
        for s in available:
            if s.lower() == target.lower():
                available.remove(s)
                return s
        return None

    ordered = [pop_case_insensitive(p) for p in PREFERRED_ORDER]
    ordered = [p for p in ordered if p]

    def numeric(s):
        m = re.search(r"\d+", s)
        return int(m.group()) if m else -1

    ordered += sorted(available, key=numeric, reverse=True)
    return ordered


def draw_page(df: pd.DataFrame, metric: str, pdf: PdfPages):
    """Original percentile‑grouped plot (kept for completeness)."""
    percentiles = ordered_percentiles(df["percentile"])
    exps = list(LABEL_MAP.keys())
    n = len(exps)

    fig, ax = plt.subplots(figsize=(3.5, 2.5))
    width = 0.8 / n
    xloc = np.arange(len(percentiles))

    for i, exp in enumerate(exps):
        label, colour, hatch = LABEL_MAP[exp]
        y = (
            df[df["Exp"] == exp]
            .set_index("percentile")[metric]
            .reindex(percentiles)
            .values
        )
        xpos = xloc - 0.4 + i * width + width / 2
        bars = ax.bar(
            xpos, y, width,
            label=label, facecolor=colour,
            edgecolor="black", linewidth=0.7, hatch=hatch,
        )
        for bar, val in zip(bars, y):
            ax.annotate(f"{val:.1f}",
                        (bar.get_x() + bar.get_width() / 2, val),
                        xytext=(0, 2), textcoords="offset points",
                        ha="center", va="bottom", rotation=90, fontsize=9)

    ax.set_xlabel("SLO")
    ax.set_ylabel("Failure Rate (%)")
    ax.set_xticks(xloc)
    ax.set_xticklabels(XMAP)
    ax.grid(axis="y", linestyle=":", linewidth=0.4, alpha=0.7)
    ax.set_axisbelow(True)
    # ax.legend(fontsize=8, frameon=False,
            #   loc="upper center", bbox_to_anchor=(0.5, -0.15), ncol=2)
    ax.margins(y=0.15)
    ax.set_ylim(0, 80)
    plt.tight_layout()

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)

# ───────────────────────── Interval helpers/plot ──────────────────────────────
_EXPRE = re.compile(r".*perfect-(?P<dist>[A-Za-z]+)-(?P<secs>\d+)-sec")

def _parse_exp(exp: str) -> tuple[str, int] | None:
    """Return (distribution, seconds) or None if the pattern does not match."""
    m = _EXPRE.match(exp)
    if m:
        return m.group("dist"), int(m.group("secs"))
    return None


def draw_prediction_interval_page(df: pd.DataFrame,
                                  metric: str,
                                  percentile: str,
                                  pdf: PdfPages):
    """New page: 4 interval groups × 3 distributions (perfect predictors)."""
    # Filter only “perfect-*” rows and the requested percentile
    rows = df[
        (df["Exp"].str.startswith("perfect-")) &
        (df["percentile"].str.casefold() == percentile.casefold())
    ].copy()

    if rows.empty:
        raise ValueError(
            f"No rows matched percentile {percentile!r} with perfect predictors."
        )
        

    # Decode EXP‑names into extra columns
    parsed = rows["Exp"].apply(_parse_exp)
    rows[["dist", "secs"]] = pd.DataFrame(parsed.tolist(), index=rows.index)

    rows = rows.dropna(subset=["dist", "secs"])
    rows["secs"] = rows["secs"].astype(int)

    # Build fixed order for x‑axis
    interval_labels = [label for _, label in INTERVALS]
    xloc = np.arange(len(INTERVALS))
    n_bars = len(DIST_STYLE_MAP)
    width = 0.8 / n_bars

    fig, ax = plt.subplots(figsize=(3.5, 2.5))

    for i, (dist, (label, colour, hatch)) in enumerate(DIST_STYLE_MAP.items()):
        # Map each interval to its metric value (NaN → 0)
        y = []
        for secs, _label in INTERVALS:
            val = rows.loc[
                (rows["dist"] == dist) & (rows["secs"] == secs),
                metric
            ]
            y.append(val.iloc[0] if not val.empty else np.nan)

        xpos = xloc - 0.4 + i * width + width / 2
        bars = ax.bar(
            xpos, y, width,
            label=label, facecolor=colour,
            edgecolor="black", linewidth=0.7, hatch=hatch,
        )
        for bar, val in zip(bars, y):
            if not np.isnan(val):
                ax.annotate(f"{val:.1f}",
                            (bar.get_x() + bar.get_width() / 2, val),
                            xytext=(0, 2), textcoords="offset points",
                            ha="center", va="bottom", rotation=90, fontsize=9)

    ax.set_xlabel("Prediction Interval")
    ax.set_ylabel("Failure Rate (%)")
    ax.set_xticks(xloc)
    ax.set_xticklabels(interval_labels)
    ax.grid(axis="y", linestyle=":", linewidth=0.4, alpha=0.7)
    ax.set_axisbelow(True)
    # ax.legend(fontsize=8, frameon=False,
    #           loc="upper center", bbox_to_anchor=(0.5, -0.19), ncol=1)
    ax.margins(y=0.15)
    ax.set_ylim(0, 80)
    ax.set_title("Invisible", color='none' ,fontsize= 5)

    plt.tight_layout()

    # pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)

# ───────────────────────────────── main ────────────────────────────────────────
def main():
    args = parse_cli()
    df = pd.read_csv(Path(args.failure_csv))

    # ── Page 1 – original percentile chart (same filters as before)
    page1_df = df[df["Exp"].isin(LABEL_MAP)]
    if page1_df.empty:
        raise ValueError("LABEL_MAP filtered out every row – check the keys.")
    if args.metric not in df.columns:
        raise KeyError(f"Column {args.metric!r} not found in the CSV.")

    with PdfPages(args.out) as pdf:
        # draw_page(page1_df, args.metric, pdf)
        # draw_prediction_interval_page(df, args.metric, args.percentile, pdf)
        draw_cost_interval_page(df,
                            args.percentile,
                            Path(args.cost_dir),
                            pdf)


    print(f"Saved generated figure to {args.out}")


if __name__ == "__main__":
    main()
