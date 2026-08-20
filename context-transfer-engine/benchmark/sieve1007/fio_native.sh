#!/bin/bash
# Native-FS comparison: sequential buffered writes, op counts matching
# matrix.sh. --thread is mandatory; end_fsync=1 = drain analog.
# Usage: fio_native.sh [target_dir ...]  (default: /dev/shm/fio1007 and $HOME/fio1007)
set -u
TARGETS=${@:-"/dev/shm/fio1007 $HOME/fio1007"}
echo "target,bs,threads,run,iops,runtime_ms"
for target in $TARGETS; do
  mkdir -p "$target"
  for cfg in "512 1 102400000" "512 8 25600000" "4k 1 204800000" "4k 8 51200000"; do
    set -- $cfg
    bs=$1; jobs=$2; size=$3
    for run in 1 2 3; do
      rm -f "$target"/w.*
      fio --name=w --directory="$target" --rw=write --bs="$bs" \
          --size="$size" --numjobs="$jobs" --thread --ioengine=psync \
          --end_fsync=1 --group_reporting --output-format=json 2>/dev/null |
        python3 -c "import sys,json; j=json.load(sys.stdin)['jobs'][0]['write']; print('$target,$bs,$jobs,$run,%d,%s' % (int(j['iops']), j['runtime']))"
    done
  done
  rm -rf "$target"
done
