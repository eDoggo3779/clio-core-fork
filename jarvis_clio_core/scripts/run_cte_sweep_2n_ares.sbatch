#!/bin/bash
# Queue the two-node CLIO Core (CTE) TCP performance sweep on Ares.
#
# Companion launcher to the Mofka two-node sweep
# (jarvis_clio_core/pipelines/mofka/run_ares_mofka_multinode_2n.sbatch). Grabs
# TWO compute nodes, activates the iowarp conda env, points the jarvis hostfile
# at both nodes, then runs the 45-run sweep in-process via `jarvis ppl run yaml`
# (Pattern A — no scheduler block). The runtime + CTE pool come up on BOTH nodes
# (PSSH), and clio_cte_bench runs one Put rank per node (nprocs=2, ppn=1), each
# rank serving its own LOCAL tier over TCP. Output lands in
# $HOME/cte_sweep_2n_results/results.csv.
#
# Quick queue:
#   sbatch jarvis_clio_core/scripts/run_cte_sweep_2n_ares.sbatch
#
# Override any path/env at submit time, e.g.:
#   sbatch --export=ALL,CONDA_ENV=iowarp,REPO=$HOME/clio-core-fork \
#          jarvis_clio_core/scripts/run_cte_sweep_2n_ares.sbatch
#
# Watch it:   squeue -u $USER ; tail -f cte_sweep_2n-<jobid>.out
#
#SBATCH --job-name=cte_sweep_2n
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
# ConstrainCores=yes on Ares hands an ntasks=1 job a single core unless we ask
# for more; chimaera's 4 workers + up to 4 bench threads need real cores.
#SBATCH --cpus-per-task=40
#SBATCH --partition=compute
#SBATCH --exclusive
# 45 runs x (~60 s + 2-node runtime/CTE PSSH start+stop) ~= 1.5-2 h; 4 h headroom.
#SBATCH --time=04:00:00
#SBATCH --output=cte_sweep_2n-%j.out
#SBATCH --error=cte_sweep_2n-%j.err

set -euo pipefail

# ---- knobs (override via --export=ALL,KEY=VAL) ----------------------------
CONDA_SH="${CONDA_SH:-$HOME/miniconda3/etc/profile.d/conda.sh}"
CONDA_ENV="${CONDA_ENV:-iowarp}"
REPO="${REPO:-$HOME/clio-core-fork}"
PIPELINE="${PIPELINE:-$REPO/jarvis_clio_core/pipelines/ares/cte_sweep_2n.yaml}"
RESULTS="${RESULTS:-$HOME/cte_sweep_2n_results/results.csv}"

echo "=== CTE 2n TCP sweep on $(hostname) — job ${SLURM_JOB_ID:-local} ==="
echo "conda: $CONDA_ENV ($CONDA_SH)"
echo "repo:  $REPO"
echo "yaml:  $PIPELINE"

# ---- activate the IOWarp/Jarvis conda env --------------------------------
# shellcheck disable=SC1090
source "$CONDA_SH"
conda activate "$CONDA_ENV"

# ---- register the repo with jarvis (idempotent) --------------------------
jarvis repo add "$REPO/jarvis_clio_core" 2>/dev/null || true

# ---- point the jarvis hostfile at BOTH allocated nodes so the runtime, CTE
#      compose, and bench all PSSH-fan-out to both. --------------------------
HOSTFILE="$HOME/.jarvis/hostfile_${SLURM_JOB_ID:-local}.txt"
mkdir -p "$(dirname "$HOSTFILE")"
scontrol show hostnames "${SLURM_JOB_NODELIST:-$(hostname)}" > "$HOSTFILE"
echo "=== Allocated nodes (CTE runs symmetrically on all of them) ==="
cat "$HOSTFILE"
echo "==============================================================="
jarvis hostfile set "$HOSTFILE"
trap 'rm -f "$HOSTFILE"' EXIT

# ---- stale-state guard on EVERY node: free port 9413 / kill leftover
#      daemons. Best-effort; non-fatal if a node has nothing to kill. -------
srun --ntasks-per-node=1 bash -c 'pkill -f clio_run 2>/dev/null; rm -f /dev/shm/chi_* 2>/dev/null; true' || true

# ---- clean slate: remove any stale results.csv so all 45 combinations re-run
#      (jarvis sweep-resume otherwise skips rows already present, incl. failed).
rm -f "$RESULTS"

# ---- run the 45-run sweep (runner auto-detects vars:+loop:) ---------------
jarvis ppl run yaml "$PIPELINE"

# ---- report ---------------------------------------------------------------
echo "=== done — results at $RESULTS ==="
if [ -f "$RESULTS" ]; then
  n=$(grep -c success "$RESULTS" 2>/dev/null || echo 0)
  echo "successful rows: $n / 45"
  head -1 "$RESULTS"
  grep success "$RESULTS" | head -3
  [ "$n" -ge 45 ] && echo "ALL 45 GREEN" || echo "INCOMPLETE — inspect $RESULTS / the .err log"
else
  echo "NO results.csv produced — check cte_sweep_2n-${SLURM_JOB_ID:-local}.err"
fi
