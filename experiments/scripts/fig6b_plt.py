#!/usr/bin/env python3
"""
Usage example::

    python fig6b_plt.py --latency_dir ./results --percentile 0.95 --out latency.pdf
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd
import matplotlib.pyplot as plt

LABEL_MAP = {
    "model-PredictiveReactive-3600-sec": ("Predictive-reactive", "#9C755F","solid"),
    "Reactive-1.35-1": ("Reactive", "#59A14F", "dashed"),               
    "model-PredictionConcentratedLoad-3600-sec":      ("Predictive",      "#4E79A7", ":"),               
}


# legend order: keys here first, then others discovered
EXP_ORDER: List[str] = list(LABEL_MAP.keys())
# ──────────────────────────────────────────────────────────────────────────────

# filename parts:  <conc>-<exp>-<maybe -0-average>-<perc>-request_latency.csv
FILENAME_RE = re.compile(
r"^(?P<conc>\d+)-(?P<exp>.+)-(?P<perc>\d+)-request_latency(?:\.csv)?$"
)

# -------- helpers -------------------------------------------------------------

def normalise_percentile(p: str) -> str:
    p = p.strip()
    if p.startswith("."):
        p = "0" + p
    return p


def _match(filename: str):
    m = FILENAME_RE.match(filename)
    if not m:
        return None
    if m.group("perc"):
        exp = m.group("exp")
        perc = m.group("perc")
    else:
        exp = m.group("exp2")
        perc = m.group("perc2")
    # strip trailing "-0" if present (before average removal already handled)
    exp = re.sub(r"-0$", "", exp)
    return exp, perc


def iter_matching_files(directory: Path, percentile: str) -> Iterable[Path]:
    for path in directory.iterdir():
        if not path.is_file():
            continue
        res = _match(path.name)
        if not res:
            continue
        _, perc = res
        if normalise_percentile(perc) == percentile:
            yield path


def discover_percentiles(directory: Path) -> List[str]:
    percents: set[str] = set()
    for p in directory.iterdir():
        res = _match(p.name)
        if res:
            _, perc = res
            percents.add(normalise_percentile(perc))
    return sorted(percents)


def experiment_key(filename: str) -> str:
    res = _match(Path(filename).name)
    if not res:
        raise ValueError(f"Filename {filename} does not match expected pattern")
    exp, _ = res
    return exp

# -------- plotting ------------------------------------------------------------

def draw_figure(files: List[Path], out_file: str, logx: bool = False) -> None:
        # Set global font size
    plt.rcParams.update({
        'font.size': 11,
        'axes.titlesize': 11,
        'axes.labelsize': 11,
        'xtick.labelsize': 11,
        'ytick.labelsize': 11,
    })

    # legend order: EXP_ORDER first, then any new ones in discovery order
    ordered_keys: list[str] = []
    [ordered_keys.append(k) for k in EXP_ORDER if any(k == experiment_key(f.name) for f in files)]
    for f in files:
        k = experiment_key(f.name)
        if k not in ordered_keys:
            ordered_keys.append(k)

    fig, ax = plt.subplots(figsize=(3.5, 2.5))

    for f in files:
        key = experiment_key(f.name)
        legend_label, colour, hatch = LABEL_MAP.get(key, (None, None, None))
        
        if legend_label is None:
            continue

        df = pd.read_csv(f, sep=r",", engine="python", header=None, names=["lat", "cnt", "other"])
        if df.empty:
            continue
        cdf = df["cnt"].cumsum() / df["cnt"].sum()
        ax.plot(df["lat"], cdf, label=legend_label, linewidth=3, color=colour, linestyle=hatch)
    ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.7)

    ax.set_xlabel("Latency (Seconds)")
    ax.set_ylabel("CDF")
    # ax.set_title("Latency CDFs")
    if logx:
        ax.set_xscale("log")

    ax.set_axisbelow(True)
    ax.set_ylim(0, 1.1)
    
    legend = ax.legend(loc="lower right", fontsize=8, frameon=True, ncol=1)
    handles, labels = ax.get_legend_handles_labels()
    sorted_handles_labels = sorted(zip(labels, handles), key=lambda x: x[0])
    sorted_labels, sorted_handles = zip(*sorted_handles_labels)
    legend = ax.legend(sorted_handles, sorted_labels, loc="lower right", borderpad=0.01, frameon=False, fontsize=9, ncol=1)
    legend.get_frame().set_alpha(1.0)  # ✅ fully opaque
    ax.set_ylim(0.95, 1.01)  # Set y-axis limits to show the legend

    legend.set_zorder(10)  # Make sure legend sits on top of everything
    ax.set_title("Invisible", color='none' ,fontsize= 5)

    fig.tight_layout()
    fig.savefig(out_file)
    plt.close(fig)

    print(f"Saved generated figure to {out_file}")

# -------- CLI -----------------------------------------------------------------

def parse_cli() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--latency_dir", default=".", help="Directory containing the latency CSV files")
    ap.add_argument("--percentile", default="1", help="Percentile string to match (e.g. 0.95, .95)")
    ap.add_argument("--out", default="fig6b.pdf")
    ap.add_argument("--logx", action="store_true", help="Use log scale on X‑axis")
    return ap.parse_args()

# -------- main ----------------------------------------------------------------

def main() -> None:
    args = parse_cli()
    directory = Path(args.latency_dir)

    pct = normalise_percentile(args.percentile)
    files = list(iter_matching_files(directory, pct))
    if not files:
        avail = discover_percentiles(directory)
        raise FileNotFoundError(
            f"No '*-{pct}-request_latency.csv' files found in {directory}\n"
            f"Available percentiles in that folder: {', '.join(avail) if avail else 'none'}"
        )

    draw_figure(files, args.out, logx=args.logx)

if __name__ == "__main__":
    main()
