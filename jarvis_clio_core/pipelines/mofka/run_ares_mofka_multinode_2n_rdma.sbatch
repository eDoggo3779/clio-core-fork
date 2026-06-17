#!/bin/bash
# Mofka multi-node FULL SWEEP — 2 Ares nodes, RDMA (ofi+verbs), 45-run in-process
# sweep. Run the 2n RDMA smoke variant first
# (run_ares_mofka_multinode_2n_rdma_smoke.sbatch) — it is the cross-node verbs
# gate; do not submit this until it passes.
#
# Topology: bedrock on the batch/head node; jarvis hostfile = both nodes;
# mofka_bench drops the bedrock host so producer/consumer run only on the
# client node, connecting to the bedrock-bound ofi+verbs address over the RoCE
# fabric. Each iteration restarts bedrock + runs producer/consumer over PSSH
# (~5 min/run), so 45 runs ~= 3.75 h plus PSSH/startup slack; 6 h ceiling (the
# node-count axis sets runtime — keep 6 h, NOT the RDMA-1n 4 h). To resume a
# timed-out run, drop the `rm -f results.csv` line below (jarvis skips
# already-completed rows) and resubmit.
#
# No FI_* env exports are set here intentionally — keep the diff minimal and let
# real failures surface. If the smoke hangs cross-node, FI_VERBS_IFACE /
# FI_PROVIDER are the first remediation levers (a fabric/QP misconfig, distinct
# from the known 4n fan-in saturation).
#
# Submit from the repo root:
#   sbatch jarvis_clio_core/pipelines/mofka/run_ares_mofka_multinode_2n_rdma.sbatch
# Monitor: squeue -u $USER     Output: mofka_bench_2n_rdma_<jobid>.log

#SBATCH --job-name=mofka_bench_2n_rdma
#SBATCH --partition=compute
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH --time=06:00:00
#SBATCH --output=mofka_bench_2n_rdma_%j.log
#SBATCH --error=mofka_bench_2n_rdma_%j.err

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

rm -f "$HOME/mofka_bench_ares_multinode_2n_rdma_results/results.csv"

jarvis ppl run yaml jarvis_clio_core/pipelines/mofka/microbench_mofka_ares_multinode_2n_rdma.yaml
