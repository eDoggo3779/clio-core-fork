#!/usr/bin/env python3
"""
Plot the CLIO-vs-Zarr-vs-rawput S3 write benchmark sweep results.csv.

Reads the results.csv produced by s3_write_bench_full.yaml (36 rows: 2 blob
sizes x 6 concurrencies x repeat 3) and writes clustered bar charts putting the
write stacks side by side in each cluster:

    clio_s3     - CLIO CTE -> S3 bdev tier (Poco + SigV4, in the runtime daemon)
    raw_put     - fork+exec `cae_s3_tool put`, the wire-speed FLOOR
    zarr_none   - Zarr v3 over s3fs, uncompressed

A fourth stack, zarr_zstd, is still declared in STACKS but is no longer part of
the grid: CLIO gets its own compression later this year, and until it does, a
compressed zarr write compared against an uncompressed CLIO one measures zstd
rather than either system. Stacks with no data in the CSV are dropped at load,
so this script keeps working unchanged on the older sweeps that do carry it.

Layout is transposed relative to plot_s3_read_bench.py: there is one subplot per
blob/chunk size and the x-axis is concurrency K, because on the write side the
question is how each stack scales with K -- and the answer differs by size,
which a K-faceted layout would hide.

Five figures are emitted:

  * wire_bw_mbps  - on-the-wire MB/s. THE cross-stack comparison. It stays the
                    headline even now that every stack in the grid is
                    uncompressed: on an older CSV carrying zarr_zstd, agg_bw is
                    not comparable across stacks, because zstd moves ~half the
                    bytes for the same logical payload and reads as ~2x faster.
  * agg_bw_mbps   - LOGICAL (uncompressed) MB/s: application-visible throughput.
                    Equal to the wire figure for every uncompressed stack; the
                    gap on a compressed one is exactly what compression bought,
                    so the pair has to be read together.
  * ratio_to_floor - each stack's wire bandwidth divided by the raw-PUT floor
                    measured in the SAME row. This is the honest headline: when
                    every stack is pinned to the link, absolute MB/s says
                    nothing and only the ratio distinguishes them. A dashed line
                    marks parity with the floor.
  * agg_ops_per_sec - objects completed per second. This is where a
                    per-operation ceiling shows up: a stack whose MB/s rises
                    with object size but whose ops/s is flat is paying a fixed
                    per-object cost, not a bandwidth cost.
  * max_rss_mb    - client process peak memory. CLIO streams through a K-slot
                    SHM window and grows as K x object_size; Zarr materializes the
                    whole array in-process and grows with the dataset. rawput is
                    flat because the bytes live in a temp file, not in RAM --
                    its cost is on disk instead (temp_file_bytes).

CAVEATS THE PLOTS ENCODE, so nobody has to rediscover them:

  1. The raw-PUT floor is a floor for SUSTAINED throughput, not for single-op
     latency. It forks one process and stages one temp file PER OBJECT, so at
     K=1 it is SLOWER than CLIO and the ratio-to-floor exceeds 1.0. Those K=1
     bars are hatched in the ratio figure. At K>=4 the fork+exec overhead
     pipelines across the concurrent processes and the floor becomes honest.
  2. Any cell where requested K exceeds the object count is hatched in every
     figure: effective_concurrency caps at the object count there, so the cell
     silently duplicates a lower-K result. (Does not occur in the 36-row sweep,
     where num_objects=256 > 64, but a smaller smoke can trip it.)

Usage:
    python plot_s3_write_bench.py <results.csv> [output_dir]

If output_dir is omitted, PNGs are written next to the input CSV.
Requires: pandas, matplotlib  (pip install pandas matplotlib)
"""
import os
import re
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")            # file-writing backend; no GUI needed (WSL-friendly)
import matplotlib.patches        # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

# Sweep variable columns the runner records for every row.
SIZE_COL = "clio_s3.object_size"
CONC_COL = "clio_s3.concurrency"
NUM_OBJ_COL = "clio_s3.num_objects"

# Observed Ares -> S3 uplink ceiling. Every stack that manages to pipeline its
# per-object work lands on 10.8-11.1 MB/s regardless of size or concurrency, and
# the raw-PUT floor stops climbing there too (K=32 -> K=64 moves it +0.3% at 1
# MiB, +2.2% at 4 MiB). That flatness is what rules out a per-connection
# concurrency limit and pins the ceiling on the link itself.
LINK_CEILING_MBPS = 11.1

# The raw-PUT floor's column prefix. Singled out because it is both a plotted
# stack and the denominator of the ratio-to-floor figure.
FLOOR_PREFIX = "raw_put.rawput"

# The four write stacks, in cluster order. Each entry is
#   (legend label, column prefix, bar color).
# The column for metric M is "<prefix>.<M>".
STACKS = [
    ("CLIO",        "clio_s3.write",      "#1f77b4"),
    ("Raw PUT",     FLOOR_PREFIX,         "#7f7f7f"),
    ("Zarr none",   "zarr_s3.write",      "#ff7f0e"),
    ("Zarr zstd",   "zarr_s3.writezstd",  "#2ca02c"),
]

# Metric suffix -> (axis label, output filename, log_y, show_link_ceiling,
# share_y). One figure per metric, faceted by blob size.
#
# share_y is per-metric, not global: MB/s, the ratio, and RSS are all directly
# comparable between the two panels and sharing the axis is what makes the
# comparison readable. Objects/s is NOT -- a 4 MiB object at the same bandwidth
# is a quarter the object rate, so sharing there squashes the 4 MiB panel into
# an unreadable strip against the 1 MiB panel's 20 ops/s.
METRICS = [
    ("wire_bw_mbps",    "Wire bandwidth (MB/s)",                "s3_write_wire_bw.png",  False, True,  True),
    ("agg_bw_mbps",     "Aggregate bandwidth, logical (MB/s)",  "s3_write_agg_bw.png",   False, True,  True),
    ("ratio_to_floor",  "Wire bandwidth / raw-PUT floor",       "s3_write_ratio.png",    False, False, True),
    ("agg_ops_per_sec", "Throughput (objects/s)",               "s3_write_ops.png",      False, False, False),
    ("max_rss_mb",      "Client peak RSS (MB)",                 "s3_write_max_rss.png",  True,  False, True),
]


def size_to_bytes(value):
    """Parse an object-size string ('1m', '4m', '512k') into bytes."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kKmMgG]?)\s*", str(value))
    if not m:
        return float("inf")
    mult = {"": 1, "k": 1024, "m": 1024 ** 2, "g": 1024 ** 3}[m.group(2).lower()]
    return float(m.group(1)) * mult


def human_bytes(n):
    """Compact byte label for panel titles ('1M', '4M', '256M')."""
    n = float(n)
    for unit in ("B", "K", "M", "G", "T"):
        if n < 1024 or unit == "T":
            return f"{n:.0f}{unit}" if n == int(n) else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.0f}T"


def load_success(csv_path):
    """Load the CSV, keep successful rows, coerce metrics, derive columns.

    Two metrics are derived rather than parsed, because neither is emitted by
    the benchmark itself:
      * max_rss_mb      - max_rss_kb / 1024, so the axis reads in MB.
      * ratio_to_floor  - wire_bw_mbps / the raw-PUT wire_bw_mbps of the SAME
                          row. Computed per row, not from column means, so the
                          repeat-to-repeat spread of the ratio is real: both
                          numerator and denominator saw the same link weather.

    Also narrows the module-level STACKS to the stacks this CSV actually
    carries -- see the note at the bottom of the function.
    """
    global STACKS
    df = pd.read_csv(csv_path)
    if "status" in df.columns:
        df = df[df["status"] == "success"].copy()
    if df.empty:
        raise SystemExit(f"No successful rows in {csv_path}")
    if NUM_OBJ_COL in df.columns:
        df[NUM_OBJ_COL] = pd.to_numeric(df[NUM_OBJ_COL], errors="coerce")

    for _, prefix, _ in STACKS:
        for col in (f"{prefix}.wire_bw_mbps", f"{prefix}.agg_bw_mbps",
                    f"{prefix}.agg_ops_per_sec", f"{prefix}.max_rss_kb"):
            if col in df.columns:
                # Blanks (a missing stat) become NaN and drop out of the mean
                # rather than crashing the plot.
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if f"{prefix}.max_rss_kb" in df.columns:
            df[f"{prefix}.max_rss_mb"] = df[f"{prefix}.max_rss_kb"] / 1024.0

    floor = df.get(f"{FLOOR_PREFIX}.wire_bw_mbps")
    for _, prefix, _ in STACKS:
        col = f"{prefix}.wire_bw_mbps"
        if floor is not None and col in df.columns:
            df[f"{prefix}.ratio_to_floor"] = df[col] / floor.replace(0, np.nan)

    # Drop stacks this sweep did not run. `Zarr zstd` left the grid once CLIO's
    # own compression landed on the roadmap -- comparing a compressed zarr
    # write against an uncompressed CLIO one measures zstd, not either system.
    # Filtering on the data rather than deleting the entry keeps the older
    # CSVs that DO carry those columns plottable, and stops a departed stack
    # from rendering as a phantom bar and a column of NaN.
    STACKS = [s for s in STACKS
              if f"{s[1]}.wire_bw_mbps" in df.columns
              and df[f"{s[1]}.wire_bw_mbps"].notna().any()]
    return df


def capped_concurrencies(sub, conc_vals):
    """Concurrency levels where requested K exceeds the object count.

    At those cells effective_concurrency == num_objects < K, so the run is not
    actually exercising K-way concurrency -- it duplicates a lower-K cell.
    """
    capped = set()
    if NUM_OBJ_COL not in sub.columns:
        return capped
    for k in conc_vals:
        nobj = sub[sub[CONC_COL] == k][NUM_OBJ_COL].dropna()
        if len(nobj) and float(k) > float(nobj.iloc[0]):
            capped.add(k)
    return capped


def plot_metric(df, suffix, ylabel, fname, log_y, show_ceiling, share_y, out_dir):
    """One figure for `suffix`: a subplot per blob size, clustered by stack."""
    size_vals = sorted(df[SIZE_COL].dropna().unique(), key=size_to_bytes)
    conc_vals = sorted(df[CONC_COL].dropna().unique(), key=float)
    if not conc_vals or not size_vals:
        print(f"  skip {fname}: no size/concurrency values")
        return

    is_ratio = suffix == "ratio_to_floor"
    # The floor divided by itself is a constant 1.0 -- it is the reference line
    # in the ratio figure, not a bar. Drop it from the cluster BEFORE computing
    # bar widths and offsets, or the ratio figure leaves a phantom gap where it
    # would have been and reads as a missing measurement.
    stacks = [s for s in STACKS if not (is_ratio and s[1] == FLOOR_PREFIX)]

    n_panels = len(size_vals)
    fig, axes = plt.subplots(
        1, n_panels, figsize=(max(6.0 * n_panels, 8), 5.4), sharey=share_y,
        squeeze=False)
    axes = axes[0]

    x = np.arange(len(conc_vals))
    width = 0.8 / len(stacks)
    any_data = False
    any_capped = False
    any_unfair_floor = False

    for panel, (ax, sz) in enumerate(zip(axes, size_vals)):
        sub = df[df[SIZE_COL] == sz]
        capped = capped_concurrencies(sub, conc_vals)
        for i, (label, prefix, color) in enumerate(stacks):
            col = f"{prefix}.{suffix}"
            if col not in sub.columns:
                continue
            means, errs = [], []
            for k in conc_vals:
                vals = sub[sub[CONC_COL] == k][col].dropna()
                means.append(vals.mean() if len(vals) else np.nan)
                errs.append(vals.std(ddof=0) if len(vals) > 1 else 0.0)
            means = np.array(means, dtype=float)
            if np.isnan(means).all():
                continue
            any_data = True
            offset = (i - (len(stacks) - 1) / 2) * width
            bars = ax.bar(x + offset, np.nan_to_num(means), width,
                          yerr=errs, capsize=3, label=label, color=color,
                          error_kw=dict(lw=1, alpha=0.6))
            for j, k in enumerate(conc_vals):
                # Hatch a bar when its cell is not measuring what it claims:
                # either K exceeded the object count, or (ratio only) the floor
                # in the denominator was itself fork+exec-bound at low K.
                unfair = is_ratio and float(k) <= 1
                if k in capped or unfair:
                    bars.patches[j].set_hatch("////")
                    bars.patches[j].set_edgecolor("black")
                    bars.patches[j].set_alpha(0.6)
                    any_capped = any_capped or (k in capped)
                    any_unfair_floor = any_unfair_floor or unfair

        if show_ceiling:
            ax.axhline(LINK_CEILING_MBPS, color="crimson", ls="--", lw=1.2, alpha=0.7,
                       label="Ares -> S3 link ceiling" if panel == 0 else None)
        if is_ratio:
            ax.axhline(1.0, color="crimson", ls="--", lw=1.2, alpha=0.7,
                       label="parity with raw-PUT floor" if panel == 0 else None)

        ax.set_title(f"{human_bytes(size_to_bytes(sz))} blobs / chunks")
        ax.set_xticks(x)
        ax.set_xticklabels([str(int(float(k))) for k in conc_vals])
        ax.set_xlabel("Concurrency K")
        if log_y:
            ax.set_yscale("log")
        ax.grid(axis="y", ls=":", alpha=0.4)
        if panel == 0 or not share_y:
            ax.set_ylabel(ylabel)

    if not any_data:
        plt.close(fig)
        print(f"  skip {fname}: all columns empty")
        return

    handles, labels = axes[0].get_legend_handles_labels()
    if any_capped:
        p = matplotlib.patches.Patch(
            facecolor="white", edgecolor="black", hatch="////", alpha=0.6,
            label="requested K > object count (duplicates a lower-K cell)")
        handles.append(p)
        labels.append(p.get_label())
    if any_unfair_floor:
        p = matplotlib.patches.Patch(
            facecolor="white", edgecolor="black", hatch="////", alpha=0.6,
            label="K=1: floor is fork+exec-bound, not a floor")
        handles.append(p)
        labels.append(p.get_label())
    # Figure-level legend along the bottom: an in-axes legend has nowhere safe
    # to sit here, because whichever corner is empty in one panel is occupied in
    # the other (low bars at low K, link-ceiling line across the top).
    fig.legend(handles, labels, fontsize=8, framealpha=0.9,
               loc="lower center", ncol=min(len(handles), 3),
               bbox_to_anchor=(0.5, 0.0))
    fig.suptitle(ylabel, fontsize=12)
    fig.tight_layout(rect=(0, 0.10 if len(handles) > 3 else 0.06, 1, 0.95))
    out_path = os.path.join(out_dir, fname)
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    print(f"  wrote {out_path}")


def print_summary(df):
    """Print the wire-bandwidth table the figures are drawn from.

    Cheap insurance against reading a bar chart wrong, and it makes the
    ratio-to-floor headline quotable without opening a PNG.
    """
    floor_col = f"{FLOOR_PREFIX}.wire_bw_mbps"
    for sz in sorted(df[SIZE_COL].dropna().unique(), key=size_to_bytes):
        sub = df[df[SIZE_COL] == sz]
        print(f"\n  {human_bytes(size_to_bytes(sz))} blobs -- wire MB/s (mean of repeats)")
        header = f"    {'K':>4}" + "".join(f"{lbl:>12}" for lbl, _, _ in STACKS)
        print(header + f"{'CLIO/floor':>12}")
        for k in sorted(sub[CONC_COL].dropna().unique(), key=float):
            r = sub[sub[CONC_COL] == k]
            cells = []
            for _, prefix, _ in STACKS:
                col = f"{prefix}.wire_bw_mbps"
                cells.append(r[col].mean() if col in r.columns else float("nan"))
            ratio = r[f"clio_s3.write.wire_bw_mbps"].mean() / r[floor_col].mean()
            print(f"    {int(float(k)):>4}" + "".join(f"{c:>12.2f}" for c in cells)
                  + f"{ratio:>12.2f}")


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    csv_path = sys.argv[1]
    out_dir = sys.argv[2] if len(sys.argv) > 2 else os.path.dirname(
        os.path.abspath(csv_path))
    os.makedirs(out_dir, exist_ok=True)

    df = load_success(csv_path)
    print(f"Loaded {len(df)} successful rows from {csv_path}")
    print_summary(df)
    print()
    for suffix, ylabel, fname, log_y, show_ceiling, share_y in METRICS:
        plot_metric(df, suffix, ylabel, fname, log_y, show_ceiling, share_y, out_dir)


if __name__ == "__main__":
    main()
