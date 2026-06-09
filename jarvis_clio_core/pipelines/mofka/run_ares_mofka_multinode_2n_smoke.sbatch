#!/bin/bash
# Mofka multi-node SMOKE — 2 Ares nodes, TCP, in-process 2-run sweep.
#
# Topology: bedrock runs on the batch/head node (the node this script lands on,
# via mofka_server's LocalExecInfo). The jarvis hostfile is set to BOTH nodes;
# mofka_bench drops the bedrock host (MOFKA_SERVER_HOST) so producer/consumer
# run only on the 1 client node via PSSH.
#
# Submit from the repo root:
#   sbatch jarvis_clio_core/pipelines/mofka/run_ares_mofka_multinode_2n_smoke.sbatch
# Monitor: squeue -u $USER     Output: mofka_smoke_2n_<jobid>.log

#SBATCH --job-name=mofka_smoke_2n
#SBATCH --partition=compute
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH --time=00:30:00
#SBATCH --output=mofka_smoke_2n_%j.log
#SBATCH --error=mofka_smoke_2n_%j.err

set -e

source ~/miniconda3/etc/profile.d/conda.sh && conda activate iowarp
source ~/spack/share/spack/setup-env.sh && spack env activate ~/mofka-spack-env

cd "${SLURM_SUBMIT_DIR:-$HOME/clio-core-fork}"

# Hostfile = ALL allocated nodes. mofka_bench excludes the bedrock head node
# internally (idempotent server-host filter), so producer/consumer hit only the
# client node(s). The bench's workdir is on shared NFS ($HOME) so the client
# node can read mofka.json.
HOSTFILE="$HOME/.jarvis/hostfile_${SLURM_JOB_ID}.txt"
mkdir -p "$(dirname "$HOSTFILE")"
scontrol show hostnames "$SLURM_JOB_NODELIST" > "$HOSTFILE"
echo "=== Allocated nodes (bedrock runs on the first; clients are the rest) ==="
cat "$HOSTFILE"
echo "========================================================================"
jarvis hostfile set "$HOSTFILE"
trap 'rm -f "$HOSTFILE"' EXIT

rm -f "$HOME/mofka_bench_ares_multinode_2n_smoke_results/results.csv"

jarvis ppl run yaml jarvis_clio_core/pipelines/mofka/microbench_mofka_ares_multinode_2n_smoke.yaml
