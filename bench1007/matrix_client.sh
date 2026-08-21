#!/bin/bash
# #1007 A/B matrix: PutStreamDefer CLIENT mode (external clio_run).
set -u
WT=/home/iowarp/wt-1007-sieve
BIN=$WT/build-cpu/bin/clio_cte_bench
RUNBIN=$WT/build-cpu/bin/clio_run
export CLIO_SERVER_CONF=$WT/bench1007/cte_ram.yaml
export CLIO_BIND_ADDR=127.0.0.1
OUT=$WT/bench1007/results_client.csv
echo "mode,sieve,io_size,threads,run,time_ms" > "$OUT"
for sieve in 1 0; do
  for iosz in 512 4096; do
    for thr in 1 8; do
      if [ "$iosz" = 512 ]; then cnt=200000; else cnt=50000; fi
      if [ "$thr" = 8 ]; then cnt=$((cnt / 4)); fi
      for run in 1 2 3; do
        "$RUNBIN" start > /tmp/1007-runtime.log 2>&1 &
        RPID=$!
        sleep 3
        log=$(CLIO_BENCH_SELF_RUN=0 CLIO_CTE_PUT_SIEVE=$sieve \
          "$BIN" --op PutStreamDefer --threads "$thr" --depth 128 \
          --io-size "$iosz" --io-count "$cnt" 2>&1)
        tms=$(echo "$log" | grep -a "Time (avg)" | grep -oE "\(([0-9.]+) ms\)" | grep -oE "[0-9.]+")
        err=$(echo "$log" | grep -ac "completion failures" || true)
        echo "client,$sieve,$iosz,$thr,$run,$tms" >> "$OUT"
        if [ "$err" != "0" ]; then echo "client,$sieve,$iosz,$thr,$run,ERRORS" >> "$OUT"; fi
        "$RUNBIN" stop > /dev/null 2>&1
        wait $RPID 2>/dev/null
        sleep 2
      done
    done
  done
done
echo DONE
