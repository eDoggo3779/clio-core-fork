#!/bin/bash
# Submit the full CTE gridsearch in one shot.
#
# Grid: {nodes: [1, 2, 4]} x {test_case: [Put, Get, PutGet]} x {ipc_mode: [shm, tcp]}
# shm is single-node-only (it cannot extend past one OS image), so the grid is:
#   - 1 node x shm
#   - 1 node x tcp
#   - 2 nodes x tcp
#   - 4 nodes x tcp
# Total: 4 jobs, 540 runs combined (135 per job).
#
# Each pipeline YAML sweeps test_case x io_size x num_threads internally,
# so this script just needs to dispatch one sbatch per (nodes, ipc_mode) combo.
# Slurm will queue the jobs and run them as resources become available.

set -e

cd "$(dirname "$0")"

PIPELINES_DIR="jarvis_iowarp/pipelines/performance"

echo "Submitting CTE grid jobs..."
echo

sbatch run_ares_cte_grid_1node.sbatch "$PIPELINES_DIR/microbench_cte_grid_1node_shm.yaml"
sbatch run_ares_cte_grid_1node.sbatch "$PIPELINES_DIR/microbench_cte_grid_1node_tcp.yaml"
sbatch run_ares_cte_grid_2node.sbatch "$PIPELINES_DIR/microbench_cte_grid_2node_tcp.yaml"
sbatch run_ares_cte_grid_4node.sbatch "$PIPELINES_DIR/microbench_cte_grid_4node_tcp.yaml"

echo
echo "All 4 grid jobs submitted. Monitor with:"
echo "  squeue -u \$USER"
echo
echo "Logs will appear at the repo root as cte_grid_{1n,2n,4n}_<jobid>.log."
