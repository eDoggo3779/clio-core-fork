#!/bin/bash
# Slurm batch script: run the CTE timed benchmark sweep across multiple Ares nodes.
# Submit with:  sbatch run_ares_cte_multinode.sbatch
# Monitor with: squeue -u $USER
# Output goes to cte_bench_multinode_<jobid>.log in the working directory.
#
# What this script does (vs the single-node variant):
#   1. Allocates N compute nodes (default: 4) with --exclusive.
#   2. Generates a per-allocation Jarvis hostfile from $SLURM_JOB_NODELIST and
#      registers it via `jarvis hostfile set`. This is what tells the runtime,
#      CTE, and the benchmark to fan out via PSSH instead of running locally.
#   3. Hands off to ./run_bench.sh for timestamped result files.
#   4. Cleans up the per-job hostfile so ~/.jarvis doesn't accumulate cruft.
#
# IMPORTANT: the multi-node pipeline YAML's `nprocs` (and `ppn`) must match
# `--nodes` here. Default: 4 nodes * ppn=1 = nprocs=4.

#SBATCH --nodes=4
#SBATCH --partition=compute
#SBATCH --exclusive
#SBATCH --time=4:00:00
#SBATCH --job-name=cte_bench_multinode
#SBATCH --output=cte_bench_multinode_%j.log

set -e

source ~/miniconda3/etc/profile.d/conda.sh
conda activate iowarp

cd ~/clio-core-fork

# --- Generate Jarvis hostfile from the Slurm allocation ---
HOSTFILE="$HOME/.jarvis/hostfile_${SLURM_JOB_ID}.txt"
mkdir -p "$(dirname "$HOSTFILE")"
scontrol show hostnames "$SLURM_JOB_NODELIST" > "$HOSTFILE"

echo "=== Allocated nodes ($SLURM_JOB_NUM_NODES) ==="
cat "$HOSTFILE"
echo "=============================================="

jarvis hostfile set "$HOSTFILE"

# Cleanup hostfile on exit (success or failure) so ~/.jarvis stays tidy
trap 'rm -f "$HOSTFILE"' EXIT

./run_bench.sh \
    jarvis_iowarp/pipelines/performance/microbench_cte_timed_ares_multinode.yaml \
    /home/anaik18/clio-core-fork/results/cte_bench_timed_ares_multinode_results
