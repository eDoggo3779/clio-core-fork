#!/bin/bash
# Slurm batch script: run one CTE grid pipeline across 4 Ares nodes.
# Same shape as the 2-node variant; only --nodes and the matched pipeline differ.
#
# The pipeline YAML's nprocs must equal --nodes * ppn. With --nodes=4 and
# ppn=1, the matched YAML has nprocs=4.
#
# Submit with:
#   sbatch run_ares_cte_grid_4node.sbatch \
#     jarvis_iowarp/pipelines/performance/microbench_cte_grid_4node_tcp.yaml

#SBATCH --nodes=4
#SBATCH --partition=compute
#SBATCH --exclusive
#SBATCH --time=4:00:00
#SBATCH --job-name=cte_grid_4n
#SBATCH --output=cte_grid_4n_%j.log

set -e

PIPELINE="${1:?Usage: sbatch $0 <pipeline.yaml>}"

source ~/miniconda3/etc/profile.d/conda.sh
conda activate iowarp

cd ~/clio-core-fork

HOSTFILE="$HOME/.jarvis/hostfile_${SLURM_JOB_ID}.txt"
mkdir -p "$(dirname "$HOSTFILE")"
scontrol show hostnames "$SLURM_JOB_NODELIST" > "$HOSTFILE"

echo "=== Allocated nodes ($SLURM_JOB_NUM_NODES) ==="
cat "$HOSTFILE"
echo "=============================================="

jarvis hostfile set "$HOSTFILE"
trap 'rm -f "$HOSTFILE"' EXIT

PIPELINE_STEM="$(basename "$PIPELINE" .yaml)"
RESULTS_DIR_NAME="${PIPELINE_STEM#microbench_}_results"
RESULTS_DIR="/home/anaik18/clio-core-fork/results/${RESULTS_DIR_NAME}"

./run_bench.sh "$PIPELINE" "$RESULTS_DIR"
