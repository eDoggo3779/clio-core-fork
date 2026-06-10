#!/bin/bash
# Mofka multi-node FULL SWEEP — 2 Ares nodes, TCP, 45-run in-process sweep.
# Run the 2n smoke variant first (run_ares_mofka_multinode_2n_smoke.sbatch).
#
# Topology: bedrock on the batch/head node; jarvis hostfile = both nodes;
# mofka_bench drops the bedrock host so producer/consumer run only on the
# client node. Each iteration restarts bedrock + runs producer/consumer over
# PSSH (~5 min/run), so 45 runs ~= 3.75 h plus PSSH/startup slack; 6 h ceiling.
# To resume a timed-out run, drop the `rm -f results.csv` line below (jarvis
# skips already-completed rows) and resubmit.
#
# Submit from the repo root:
#   sbatch jarvis_clio_core/pipelines/mofka/run_ares_mofka_multinode_2n.sbatch
# Monitor: squeue -u $USER     Output: mofka_bench_2n_<jobid>.log

#SBATCH --job-name=mofka_bench_2n
#SBATCH --partition=compute
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH --time=06:00:00
#SBATCH --output=mofka_bench_2n_%j.log
#SBATCH --error=mofka_bench_2n_%j.err

set -e

source ~/miniconda3/etc/profile.d/conda.sh && conda activate iowarp
source ~/spack/share/spack/setup-env.sh && spack env activate ~/mofka-spack-env

cd "${SLURM_SUBMIT_DIR:-$HOME/clio-core-fork}"

HOSTFILE="$HOME/.jarvis/hostfile_${SLURM_JOB_ID}.txt"
mkdir -p "$(dirname "$HOSTFILE")"
scontrol show hostnames "$SLURM_JOB_NODELIST" > "$HOSTFILE"
echo "=== Allocated nodes (bedrock runs on the first; clients are the rest) ==="
cat "$HOSTFILE"
echo "========================================================================"
jarvis hostfile set "$HOSTFILE"
trap 'rm -f "$HOSTFILE"' EXIT

rm -f "$HOME/mofka_bench_ares_multinode_2n_results/results.csv"

jarvis ppl run yaml jarvis_clio_core/pipelines/mofka/microbench_mofka_ares_multinode_2n.yaml
