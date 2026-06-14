#!/bin/bash
# One-shot launcher: runs rdma_preflight.sh on a compute node via srun,
# captures output, prints exit code + tail. Run from the Ares login node.
#
#   bash jarvis_clio_core/pipelines/mofka/run_preflight.sh
set -u
REPO_ROOT="${REPO_ROOT:-$HOME/clio-core-fork}"
PREFLIGHT_SH="$REPO_ROOT/jarvis_clio_core/pipelines/mofka/rdma_preflight.sh"
PREFLIGHT_LOG="$REPO_ROOT/rdma_preflight_$(date +%Y%m%d_%H%M%S).log"
echo "PREFLIGHT_LOG=$PREFLIGHT_LOG"
srun --nodes=1 --partition=compute --exclusive --time=0:30:00 \
     --immediate=300 -J rdma_preflight \
     bash "$PREFLIGHT_SH" \
     > "$PREFLIGHT_LOG" 2>&1
EC=$?
echo "srun exit: $EC"
tail -120 "$PREFLIGHT_LOG"
exit $EC
