#!/bin/bash
# #1007 A/B matrix: PutStreamDefer embed mode, sieve on/off, sizes, threads.
set -u
WT=/home/iowarp/wt-1007-sieve
BIN=$WT/build-cpu/bin/clio_cte_bench
export CLIO_SERVER_CONF=$WT/bench1007/cte_ram.yaml
export CLIO_BIND_ADDR=127.0.0.1
OUT=$WT/bench1007/results_embed.csv
echo "mode,sieve,io_size,threads,run,iops,mbps,time_ms" > "$OUT"
for sieve in 1 0; do
  for iosz in 512 4096; do
    for thr in 1 8; do
      if [ "$iosz" = 512 ]; then cnt=200000; else cnt=50000; fi
      if [ "$thr" = 8 ]; then cnt=$((cnt / 4)); fi
      for run in 1 2 3; do
        log=$(CLIO_BENCH_SELF_RUN=1 CLIO_CTE_PUT_SIEVE=$sieve \
          "$BIN" --op PutStreamDefer --threads "$thr" --depth 128 \
          --io-size "$iosz" --io-count "$cnt" 2>&1)
        iops=$(echo "$log" | grep -a "Aggregate IOPS" | grep -oE "[0-9.]+e?[+0-9]*$")
        mbps=$(echo "$log" | grep -a "Aggregate bandwidth" | grep -oE "[0-9.]+" | tail -1)
        tms=$(echo "$log" | grep -a "Time (avg)" | grep -oE "\(([0-9.]+) ms\)" | grep -oE "[0-9.]+")
        err=$(echo "$log" | grep -ac "completion failures" || true)
        echo "embed,$sieve,$iosz,$thr,$run,$iops,$mbps,$tms" >> "$OUT"
        if [ "$err" != "0" ]; then echo "embed,$sieve,$iosz,$thr,$run,ERRORS" >> "$OUT"; fi
        sleep 1
      done
    done
  done
done
echo DONE
