#!/bin/bash
# Slurm batch script: run one CTE grid pipeline on a single Ares node.
# Pipeline path is passed as $1 so the same script drives both the shm and
# tcp single-node sweeps.
#
# Submit examples:
#   sbatch run_ares_cte_grid_1node.sbatch \
#     jarvis_iowarp/pipelines/performance/microbench_cte_grid_1node_shm.yaml
#   sbatch run_ares_cte_grid_1node.sbatch \
#     jarvis_iowarp/pipelines/performance/microbench_cte_grid_1node_tcp.yaml
#
# Sweep size: 135 runs at ~38s each ≈ 90 min plus startup overhead.
# 4 h time budget leaves comfortable slack for any single-iteration outliers.

#SBATCH --nodes=1
#SBATCH --partition=compute
#SBATCH --exclusive
#SBATCH --time=4:00:00
#SBATCH --job-name=cte_grid_1n
#SBATCH --output=cte_grid_1n_%j.log

set -e

PIPELINE="${1:?Usage: sbatch $0 <pipeline.yaml>}"

source ~/miniconda3/etc/profile.d/conda.sh
conda activate iowarp

cd ~/clio-core-fork

# Derive results dir from pipeline filename so each grid YAML lands in its
# own directory and downstream rsync/analysis stays unambiguous.
PIPELINE_STEM="$(basename "$PIPELINE" .yaml)"
# Strip the leading "microbench_" so directory names mirror the existing
# convention (cte_grid_1node_shm_results, not microbench_cte_grid_..._results).
RESULTS_DIR_NAME="${PIPELINE_STEM#microbench_}_results"
RESULTS_DIR="/home/anaik18/clio-core-fork/results/${RESULTS_DIR_NAME}"

./run_bench.sh "$PIPELINE" "$RESULTS_DIR"
