#!/bin/bash
# PutStreamDefer sieve A/B matrix (embedded runtime). Results to stdout CSV.
# Env: CLIO_BUILD_DIR (default <repo>/build-cpu), CLIO_PREFAULT recommended 0.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
BUILD=${CLIO_BUILD_DIR:-$REPO/build-cpu}
BIN=$BUILD/bin/clio_cte_bench
export CLIO_SERVER_CONF=$HERE/cte_ram.yaml
export CLIO_BIND_ADDR=127.0.0.1
export CLIO_BENCH_SELF_RUN=1
export CLIO_PREFAULT=${CLIO_PREFAULT:-0}
echo "sieve,io_size,threads,run,time_ms"
for sieve in 1 0; do
  for iosz in 512 4096; do
    for thr in 1 8; do
      if [ "$iosz" = 512 ]; then cnt=200000; else cnt=50000; fi
      if [ "$thr" = 8 ]; then cnt=$((cnt / 4)); fi
      for run in 1 2 3; do
        t=$(CLIO_CTE_PUT_SIEVE=$sieve "$BIN" --op PutStreamDefer \
              --threads "$thr" --depth 1024 --io-size "$iosz" \
              --io-count "$cnt" 2>&1 |
            grep -a "Time (avg)" | grep -oE "\(([0-9.]+) ms\)" |
            grep -oE "[0-9.]+")
        echo "$sieve,$iosz,$thr,$run,$t"
        sleep 1
      done
    done
  done
done
