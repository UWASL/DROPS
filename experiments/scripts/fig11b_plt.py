#!/usr/bin/env python3
"""
"""

from __future__ import annotations

import argparse
import re
from itertools import cycle
from pathlib import Path
from typing import Dict

import pandas as pd
import matplotlib.pyplot as plt


# Set global font size
plt.rcParams.update({
        'font.size': 11,
        'axes.titlesize': 11,
        'axes.labelsize': 11,
        'xtick.labelsize': 11,
        'ytick.labelsize': 11,
    })

# ──────────────────────────────────────────────────────────────────────────────
# Regex to pull out: concurrency, experiment, percentile
FILENAME_RE = re.compile(
    r"^(?P<conc>\d+)-(?P<exp>.+?)-0(?P<perc>[\d.]+)-request_latency(?:\.csv)?$"
)

def normalise_percentile(p: str) -> str:
    p = p.strip()
    return p if not p.startswith(".") else "0" + p

def discover_statistical_files(dirpath: Path) -> Dict[str, Path]:
    """
    Return a mapping percentile -> first-found Path for that percentile,
    among files whose <exp> == "DROPS".
    """
    files: Dict[str, Path] = {}
    for path in sorted(dirpath.iterdir()):
        if not path.is_file():
            continue
        m = FILENAME_RE.match(path.name)
        if not m:
            continue
        exp = re.sub(r"-0$", "", m.group("exp"))
        if exp != "DROPS":
            continue
        pct = normalise_percentile(m.group("perc"))
        if pct not in files:
            files[pct] = path
    return files

def plot_all_percentiles(
    files_by_pct: Dict[str, Path],
    out_file: str,
    logx: bool = False
) -> None:
    """
    Read each CSV in `files_by_pct`, compute its CDF, and overlay all
    on a single matplotlib Axes, cycling through linestyles.
    """
    # define a palette of linestyles to cycle through
    linestyles = cycle(["-", "--", "-.", ":", (0, (1,1)), (0, (5,1)), (0, (3,5,1,5))])

    fig, ax = plt.subplots(figsize=(3.5, 2.5))

    for pct, path in sorted(files_by_pct.items(), key=lambda kv: float(kv[0])):
        df = pd.read_csv(path, header=None, names=["lat", "cnt", "_"])
        if df.empty:
            print(f"[!] skipping empty file {path.name}")
            continue

        cdf = df["cnt"].cumsum() / df["cnt"].sum()
        style = next(linestyles)
        label = f"P{float(pct) * 100:.0f}" if float(float(pct)*100).is_integer() else f"P{float(pct) * 100:.2f}"
        ax.plot(
            df["lat"],
            cdf,
            label=label,
            linewidth=3,
            linestyle=style,
        )

    ax.set_xlabel("Latency (s)")
    ax.set_ylabel("CDF")
    if logx:
        ax.set_xscale("log")
    ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.7)
    ax.set_ylim(0.95, 1.016)
    legend = ax.legend(
        frameon=True,
        fontsize=8,
        loc="lower right",
        edgecolor="white",
        facecolor="white",
        fancybox=True,
    )
    ax.set_title("Invisible", color='none' ,fontsize= 5)

    fig.tight_layout()
    fig.savefig(out_file)
    plt.close(fig)
    print(f"[+] saved combined CDF to {out_file}")

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Plot CDFs for every DROPS percentile in one figure"
    )
    p.add_argument(
        "-d", "--latency_dir",
        default=".",
        help="Directory containing DROPS-<pct>-request_latency.csv files"
    )
    p.add_argument(
        "-o", "--out",
        default="fig11b.pdf",
        help="Output filename (PDF/PNG)"
    )
    p.add_argument(
        "--logx",
        action="store_true",
        help="Use log scale on the X axis"
    )
    return p.parse_args()

def main() -> None:
    args = parse_args()
    directory = Path(args.latency_dir)
    files_by_pct = discover_statistical_files(directory)

    if not files_by_pct:
        raise FileNotFoundError(f"No ‘DROPS’ CSVs found in {directory}")

    plot_all_percentiles(files_by_pct, args.out, logx=args.logx)

if __name__ == "__main__":
    main()
