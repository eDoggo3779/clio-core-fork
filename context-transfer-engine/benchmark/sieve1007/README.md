# Issue #1007 sieve benchmark harness

Reproduces the client-side data-sieving measurements from PR #1009.

- `cte_ram.yaml` — CTE-only compose (no CAE), 6 GB RAM tier.
- `matrix.sh` — PutStreamDefer A/B matrix (sieve on vs CLIO_CTE_PUT_SIEVE=0),
  embedded mode, 512 B / 4 KiB x 1 / 8 threads.
- `fio_native.sh` — native-filesystem comparison: identical op counts via fio
  (psync, buffered, end_fsync). Enlarge /dev/shm first in containers:
  `sudo mount -o remount,size=2G /dev/shm`.
- `thread_prof.py` — /proc-based per-thread CPU + voluntary-context-switch
  profiler (for hosts where perf is unavailable): time-sliced utilization and
  a peak-window per-thread breakdown.

Conventions that materially change results:
- `CLIO_PREFAULT=0` pre-faults the WHOLE RAM tier at compose ("0" = whole
  mapping, not "off"); unset = incremental populate, which is fault-bound on
  fresh streams.
- `CLIO_BENCH_STREAM_REGION=<bytes>` wraps each thread's stream so later
  passes REWRITE offsets (warm-extent path, no fresh-block allocation).
- The pacing wall is depth * io-size of SHIPPED bytes; open sieve pages are
  exempt.
- Interleave A/B binaries within one batch; this class of box shows 20-30%
  batch-to-batch noise.
