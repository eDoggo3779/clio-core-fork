#!/bin/bash
# Mofka single-node FULL SWEEP — one Ares node, RDMA (ofi+verbs), 45-run
# in-process sweep. Run the smoke variant first (run_ares_mofka_rdma_smoke.sbatch)
# to confirm bedrock binds ofi+verbs before submitting this.
#
# Pattern A: one allocation; the 45-run sweep runs in-process via
# `jarvis ppl run yaml`. Each iteration restarts bedrock (start, wait for
# mofka.json, create topic, run, teardown) at ~5 min/run, so 45 runs ~= 3.75 h;
# 4 h ceiling. To resume a timed-out run, drop the `rm -f results.csv` line below
# (jarvis skips already-completed rows) and resubmit.
#
# No FI_* env exports are set here intentionally — keep the diff minimal and let
# real failures surface. Add them only if a specific failure signature appears.
#
# Submit from the repo root:
#   sbatch jarvis_clio_core/pipelines/mofka/run_ares_mofka_rdma.sbatch
# Monitor: squeue -u $USER     Output: mofka_bench_rdma_<jobid>.log

#SBATCH --job-name=mofka_bench_rdma
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH --time=04:00:00
#SBATCH --output=mofka_bench_rdma_%j.log
#SBATCH --error=mofka_bench_rdma_%j.err

set -e

source ~/miniconda3/etc/profile.d/conda.sh && conda activate iowarp
source ~/spack/share/spack/setup-env.sh && spack env activate ~/mofka-spack-env

cd "${SLURM_SUBMIT_DIR:-$HOME/clio-core-fork}"

HOSTFILE="$HOME/.jarvis/hostfile_${SLURM_JOB_ID}.txt"
mkdir -p "$(dirname "$HOSTFILE")"
scontrol show hostnames "$SLURM_JOB_NODELIST" > "$HOSTFILE"
jarvis hostfile set "$HOSTFILE"
trap 'rm -f "$HOSTFILE"' EXIT

rm -f "$HOME/mofka_bench_ares_rdma_results/results.csv"

jarvis ppl run yaml jarvis_clio_core/pipelines/mofka/microbench_mofka_ares_rdma.yaml
