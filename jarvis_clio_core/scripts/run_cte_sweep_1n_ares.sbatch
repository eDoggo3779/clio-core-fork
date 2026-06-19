#!/bin/bash
# Queue the single-node CLIO Core (CTE) TCP performance sweep on Ares.
#
# Companion launcher to the Mofka single-node sweep
# (jarvis_clio_core/pipelines/mofka/run_ares_mofka.sbatch). Wraps
# `jarvis ppl run yaml .../ares/cte_sweep_1n.yaml` in a SLURM batch job: grabs
# one compute node, activates the iowarp conda env, (re)registers the repo, and
# runs the 45-run sweep (io_size {256,1024,4096,16384,65536} x threads {1,2,4}
# x 3 repeats, 60 s each, Put, TCP). Output lands in
# $HOME/cte_sweep_1n_results/results.csv.
#
# This is "Pattern A": one allocation, the whole sweep runs in-process via
# `jarvis ppl run yaml` (no jarvis scheduler block / no per-iteration sbatch),
# matching the Mofka launchers. See pipelines/mofka/architecture_decisions.md.
#
# Quick queue:
#   sbatch jarvis_clio_core/scripts/run_cte_sweep_1n_ares.sbatch
#
# Override any path/env at submit time, e.g.:
#   sbatch --export=ALL,CONDA_ENV=iowarp,REPO=$HOME/clio-core-fork \
#          jarvis_clio_core/scripts/run_cte_sweep_1n_ares.sbatch
#
# Watch it:   squeue -u $USER ; tail -f cte_sweep_1n-<jobid>.out
#
#SBATCH --job-name=cte_sweep_1n
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
# ConstrainCores=yes on Ares hands an ntasks=1 job a single core unless we ask
# for more; chimaera's 4 workers + up to 4 bench threads need real cores.
#SBATCH --cpus-per-task=40
#SBATCH --partition=compute
#SBATCH --exclusive
# 45 runs x (~60 s + runtime/CTE start+stop) ~= 1-1.5 h; 4 h is comfortable.
#SBATCH --time=04:00:00
#SBATCH --output=cte_sweep_1n-%j.out
#SBATCH --error=cte_sweep_1n-%j.err

set -euo pipefail

# ---- knobs (override via --export=ALL,KEY=VAL) ----------------------------
CONDA_SH="${CONDA_SH:-$HOME/miniconda3/etc/profile.d/conda.sh}"
CONDA_ENV="${CONDA_ENV:-iowarp}"
REPO="${REPO:-$HOME/clio-core-fork}"
PIPELINE="${PIPELINE:-$REPO/jarvis_clio_core/pipelines/ares/cte_sweep_1n.yaml}"
RESULTS="${RESULTS:-$HOME/cte_sweep_1n_results/results.csv}"

echo "=== CTE 1n TCP sweep on $(hostname) — job ${SLURM_JOB_ID:-local} ==="
echo "conda: $CONDA_ENV ($CONDA_SH)"
echo "repo:  $REPO"
echo "yaml:  $PIPELINE"

# ---- activate the IOWarp/Jarvis conda env --------------------------------
# shellcheck disable=SC1090
source "$CONDA_SH"
conda activate "$CONDA_ENV"

# ---- register the repo with jarvis (idempotent) --------------------------
jarvis repo add "$REPO/jarvis_clio_core" 2>/dev/null || true

# ---- stale-state guard: free the runtime port from any prior run ---------
pkill -f 'clio_run' 2>/dev/null || true

# ---- clean slate: jarvis sweep-resume counts FAILED rows as done and skips
#      them, so a stale results.csv silently no-ops a re-run. Remove it so all
#      45 combinations re-run fresh (matches the Mofka launchers). ----------
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
  echo "NO results.csv produced — check cte_sweep_1n-${SLURM_JOB_ID:-local}.err"
fi
