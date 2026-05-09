#!/bin/bash
# Wrapper around `jarvis ppl run yaml` that timestamps the result files.
#
# After jarvis writes results.csv / results.yaml into the output directory,
# this script renames them to results_<YYYYMMDD_HHMMSS>.{csv,yaml} so multiple
# runs in the same output directory don't overwrite each other.
#
# Usage:
#   ./run_bench.sh <pipeline.yaml> <output_dir>
#
# Example:
#   ./run_bench.sh \
#     jarvis_iowarp/pipelines/performance/microbench_cte_timed_ares.yaml \
#     /home/anaik18/clio-core-fork/results/cte_bench_timed_ares_results

set -e

if [ $# -ne 2 ]; then
    echo "Usage: $0 <pipeline.yaml> <output_dir>" >&2
    exit 1
fi

PIPELINE="$1"
OUTPUT_DIR="$2"
TS=$(date +%Y%m%d_%H%M%S)

jarvis ppl run yaml "$PIPELINE"

if [ -f "$OUTPUT_DIR/results.csv" ]; then
    mv "$OUTPUT_DIR/results.csv" "$OUTPUT_DIR/results_${TS}.csv"
    echo "Renamed: results.csv -> results_${TS}.csv"
fi

if [ -f "$OUTPUT_DIR/results.yaml" ]; then
    mv "$OUTPUT_DIR/results.yaml" "$OUTPUT_DIR/results_${TS}.yaml"
    echo "Renamed: results.yaml -> results_${TS}.yaml"
fi
