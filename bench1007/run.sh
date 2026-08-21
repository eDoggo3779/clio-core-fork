#!/bin/bash
# Issue #1007 sieve A/B: PutStreamDefer, sieve on vs CLIO_CTE_PUT_SIEVE=0.
# Usage: run.sh <embed|client> <sieve 1|0> <io_size> <threads> <io_count>
set -u
WT=/home/iowarp/wt-1007-sieve
BIN=$WT/build-cpu/bin/clio_cte_bench
export CLIO_SERVER_CONF=$WT/bench1007/cte_ram.yaml
export CLIO_BIND_ADDR=127.0.0.1
MODE=$1; SIEVE=$2; IOSZ=$3; THREADS=$4; COUNT=$5
DEPTH=${6:-128}
export CLIO_CTE_PUT_SIEVE=$SIEVE
if [ "$MODE" = "embed" ]; then
  export CLIO_BENCH_SELF_RUN=1
else
  export CLIO_BENCH_SELF_RUN=0
fi
$BIN --op PutStreamDefer --threads "$THREADS" --depth "$DEPTH" \
     --io-size "$IOSZ" --io-count "$COUNT" 2>&1 |
  grep -E "Throughput|IOPS|ops/s|Total|Result|MB/s" || true
