#!/usr/bin/env python3
"""Print a per-tier summary table for one or more run_local_bench.py --tag
results, side by side -- the local, no-plotting equivalent of pointing
unaryPlots()/batchPlots() at two artifact directories. UNARY_* workloads
show mean latency (ms); BATCH_*/AGG_* workloads show throughput (rows/sec).
With 2+ tags, each column after the first also shows %change vs. the first
tag, so you can tell at a glance whether a code change helped or hurt.
"""
import argparse
import csv
import os

import bench_common as bc


def load_csv(path):
    rows = []
    with open(path) as fh:
        for row in csv.DictReader(fh):
            rows.append((int(row["tier"]), int(row["client_id"]), int(row["batch_size"]), float(row["time_taken_ms"])))
    return rows


def print_workload(name, tag_to_rows):
    is_unary = bc.is_unary_workload(name)
    unit = "ms" if is_unary else "rows/sec"
    tags = list(tag_to_rows.keys())
    summaries = {tag: bc.summarize_rows(name, rows) for tag, rows in tag_to_rows.items()}
    tiers = sorted({tier for s in summaries.values() for tier in s})

    print(f"\n=== {name} ({unit}) ===")
    header = f"{'tier':>6}" + "".join(f"{tag:>16}" for tag in tags)
    if len(tags) > 1:
        header += "".join(f"{'Δ ' + tag + ' vs ' + tags[0]:>22}" for tag in tags[1:])
    print(header)

    for tier in tiers:
        base = summaries[tags[0]].get(tier)
        line = f"{tier:>6}" + "".join(f"{summaries[tag].get(tier, float('nan')):>16,.2f}" for tag in tags)
        if len(tags) > 1:
            for tag in tags[1:]:
                val = summaries[tag].get(tier)
                if base in (None, 0) or val is None:
                    line += f"{'n/a':>22}"
                else:
                    pct = (val - base) / base * 100
                    better = (pct > 0) if not is_unary else (pct < 0)  # higher throughput / lower latency = better
                    arrow = "better" if better else "worse"
                    line += f"{pct:>+15.1f}% {arrow:>6}"
        print(line)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", default=os.path.join(os.path.dirname(__file__), "results"))
    ap.add_argument("--tags", required=True, help="comma list of tags to compare, e.g. baseline,cache-v2")
    ap.add_argument("--workloads", default="all")
    args = ap.parse_args()

    tags = args.tags.split(",")
    names = list(bc.WORKLOADS.keys()) if args.workloads == "all" else args.workloads.split(",")

    for name in names:
        tag_to_rows = {}
        for tag in tags:
            path = os.path.join(args.results_dir, tag, f"{name}.csv")
            if os.path.exists(path):
                tag_to_rows[tag] = load_csv(path)
        if not tag_to_rows:
            continue
        print_workload(name, tag_to_rows)


if __name__ == "__main__":
    main()
