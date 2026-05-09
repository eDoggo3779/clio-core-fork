#!/bin/bash
# Slurm batch script: run the CTE timed benchmark sweep on a single Ares node.
# Submit with:  sbatch run_ares_cte.sbatch
# Monitor with: squeue -u $USER
# Output goes to cte_bench_<jobid>.log in the working directory.

#SBATCH --nodes=1
#SBATCH --partition=compute
#SBATCH --exclusive
#SBATCH --time=4:00:00
#SBATCH --job-name=cte_bench
#SBATCH --output=cte_bench_%j.log

set -e

source ~/miniconda3/etc/profile.d/conda.sh
conda activate iowarp

cd ~/clio-core-fork

./run_bench.sh \
    jarvis_iowarp/pipelines/performance/microbench_cte_timed_ares.yaml \
    /home/anaik18/clio-core-fork/results/cte_bench_timed_ares_results
