#!/bin/bash
# #1007 native-FS comparison: sequential buffered writes matching the
# PutStreamDefer op counts exactly. psync engine, --thread mandatory (WSL),
# end_fsync=1 so the timed region includes the flush (drain analog);
# no-op on tmpfs, real on disk.
set -u
OUT=/home/iowarp/wt-1007-sieve/bench1007/results_native.csv
echo "target,bs,threads,run,iops,runtime_ms" > "$OUT"
for target in /dev/shm/fio1007 /home/iowarp/fio1007; do
  mkdir -p "$target"
  for cfg in "512 1 102400000" "512 8 25600000" "4k 1 204800000" "4k 8 51200000"; do
    set -- $cfg
    bs=$1; jobs=$2; size=$3
    for run in 1 2 3; do
      rm -f "$target"/f.*
      res=$(fio --name=w --directory="$target" --rw=write --bs="$bs" \
        --size="$size" --numjobs="$jobs" --thread --ioengine=psync \
        --end_fsync=1 --group_reporting --output-format=json 2>/dev/null)
      iops=$(echo "$res" | python3 -c "import sys,json; j=json.load(sys.stdin)['jobs'][0]; print(int(j['write']['iops']))")
      rt=$(echo "$res" | python3 -c "import sys,json; j=json.load(sys.stdin)['jobs'][0]; print(j['write']['runtime'])")
      echo "$target,$bs,$jobs,$run,$iops,$rt" >> "$OUT"
    done
  done
  rm -rf "$target"
done
echo DONE
