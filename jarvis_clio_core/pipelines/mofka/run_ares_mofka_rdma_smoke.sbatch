#!/bin/bash
# Mofka single-node SMOKE — one Ares node, RDMA (ofi+verbs), in-process 2-run
# sweep. Confirms bedrock binds ofi+verbs and the stack runs end-to-end before
# the full 45-run RDMA sweep.
#
# Pattern A: one allocation, the whole sweep runs in-process via
# `jarvis ppl run yaml` (the YAML has no jarvis scheduler block, so each
# iteration takes PipelineTest._run_single's in-process start()/stop() path).
#
# Submit from the repo root:
#   sbatch jarvis_clio_core/pipelines/mofka/run_ares_mofka_rdma_smoke.sbatch
# Monitor: squeue -u $USER     Output: mofka_rdma_smoke_<jobid>.log

#SBATCH --job-name=mofka_rdma_smoke
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH --time=00:30:00
#SBATCH --output=mofka_rdma_smoke_%j.log
#SBATCH --error=mofka_rdma_smoke_%j.err

set -e

# Activate the environments the benchmark needs. conda owns jarvis (its CLI
# uses a conda-python shebang); the spack env owns bedrock + the mochi.mofka
# bindings (mofka_server prepends the spack view at configure time).
source ~/miniconda3/etc/profile.d/conda.sh && conda activate iowarp
source ~/spack/share/spack/setup-env.sh && spack env activate ~/mofka-spack-env

cd "${SLURM_SUBMIT_DIR:-$HOME/clio-core-fork}"

# Set the jarvis hostfile to the allocated node(s). Single node here → a
# 1-host hostfile → mofka_bench._is_multinode() is False (needs >1 host) →
# producer/consumer run locally alongside bedrock.
HOSTFILE="$HOME/.jarvis/hostfile_${SLURM_JOB_ID}.txt"
mkdir -p "$(dirname "$HOSTFILE")"
scontrol show hostnames "$SLURM_JOB_NODELIST" > "$HOSTFILE"
jarvis hostfile set "$HOSTFILE"
trap 'rm -f "$HOSTFILE"' EXIT

# Clear any prior results so jarvis doesn't "resume" past old/failed rows
# (its resume logic counts failed rows as completed and skips them).
rm -f "$HOME/mofka_bench_ares_rdma_smoke_results/results.csv"

jarvis ppl run yaml jarvis_clio_core/pipelines/mofka/microbench_mofka_ares_rdma_smoke.yaml
