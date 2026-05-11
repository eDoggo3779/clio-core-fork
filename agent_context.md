# CTE Benchmark Agent Context

## What this system is

**CTE (Context Transfer Engine)** is a storage abstraction layer built on the **Chimaera** async runtime. Clients submit async tasks (PutBlob, GetBlob, DelBlob) via a shared-memory IPC ring, which are dispatched to worker threads in a separate `chimaera` server process.

The benchmark (`wrp_cte_bench`) issues Put/Get/PutGet operations against a live Chimaera+CTE server process. It is orchestrated by **Jarvis**, a Python pipeline framework that sweeps parameter combinations, starts/stops services, and collects stats.

Key files:
- `context-transfer-engine/benchmark/wrp_cte_bench.cc` — the benchmark binary
- `jarvis_iowarp/jarvis_iowarp/wrp_cte_bench_timed/pkg.py` — Jarvis package (launches the binary, parses output, collects stats)
- `jarvis_iowarp/pipelines/performance/microbench_cte_timed.yaml` — sweep config (5 io_sizes × 3 thread counts × 3 repeats = 45 iterations, all `test_case: PutGet`)
- `results/cte_bench_timed_results/results.csv` — latest sweep output

**Build:** `cmake --build /workspace/build --target wrp_cte_bench -j`
**Binaries:** `/home/iowarp/.local/bin/wrp_cte_bench` → symlink to `/workspace/build/bin/wrp_cte_bench`

---

## Benchmark design (duration mode)

Each worker thread:
1. Creates a tag, allocates an SHM buffer.
2. Runs a deadline loop (`while now() < start + 30s`): issues `depth=4` async PutBlob tasks, waits for each, then issues `depth=4` async GetBlob tasks on the same blobs.
3. **Epoch rollover (working-set management):** when `epoch_ops` reaches `pool_size = depth_ * 64 = 256` ops, it deletes all blobs from the previous epoch before continuing. This keeps live storage bounded to `pool_size × io_size` bytes per thread.
4. Records `thread_times[id] = now() - start_time` after the loop.

Stats are printed to stdout. The Jarvis `pkg.py` parses them via regex from a saved `bench_output.txt`, keying columns as `cte_bench_timed.{operation}.{metric}`.

---

## Relevant scaffolding history (oldest first)

| Scaffolding | Location | Why it was added |
|-------------|----------|------------------|
| `pool_size = depth_ * 64` | `wrp_cte_bench.cc:239,481` | Avoid hitting 500MB RAM storage ceiling at 1m×4t (256 blobs × 4 threads × 1MB = 1 GB would overflow). Magic number. |
| `secs_left()` floor of 0.1f | `wrp_cte_bench.cc:252,491` | Prevent passing 0 or negative to `task.Wait()`, which would hang indefinitely. Added after observed hangs. |
| `// leak ok: runtime restarts` on DelBlob `.Wait(secs_left())` | `wrp_cte_bench.cc:290,547` | If epoch rollover happens near the deadline, DelBlob may time out. Blobs leak but the runtime is a fresh process next iteration. |
| `WaitWithTimeout()` helper | Removed last session | Was a polling wrapper around `Future::IsComplete()` to work around `Future::Wait(float)` ignoring its timeout argument. Now removed. |

**Runtime-side TOCTOU lost-wakeup patch (the real fix):**
The core bug was in the Chimaera runtime's enqueue paths: `was_empty = lane.Empty(); lane.Push(task); if (was_empty) AwakenWorker()` — a race where the consumer thread could go to sleep between `Empty()` and `Push()`, missing the wakeup signal. This made `Future::Wait(float max_sec)` hang indefinitely regardless of its timeout argument. The patch unconditionally calls `AwakenWorker()` after every push (idempotent when the worker is already active). Applied at 6 sites across:
- `context-runtime/include/chimaera/ipc_manager.h` (~line 639)
- `context-runtime/src/ipc_manager.cc` (~lines 2286, 2560, 1297)
- `context-runtime/src/worker.cc` (~line 858)
- `context-runtime/modules/admin/src/admin_runtime.cc` (~line 1116)

---

## The current problem: `runtime` decreases with `io_size`

Latest sweep results (`results/cte_bench_timed_results/results.csv`, ALL rows `status=success`):

| io_size | threads | Jarvis `runtime` (s) | `time_avg_us` (µs) | notes |
|---------|---------|----------------------|---------------------|-------|
| 4k | 1/2/4 | ~37 | ~30,000,500 | looks correct |
| 16k | 1/2/4 | ~17 | ~30,000,400 | ← contradiction |
| 64k | 1/2/4 | ~10 | ~30,001,600 | ← contradiction |
| 256k | 1/2/4 | ~7 | ~30,000,200 | ← contradiction; `put.*` cols populated, not `putget.*` |
| 1m | 1/2/4 | ~7–8 | ~30,000,400 | ← contradiction; `put.*` cols populated, not `putget.*` |

**The contradiction:** `time_avg_us ≈ 30,000,000 µs` means each per-thread benchmark loop measured ~30 seconds of wall-clock elapsed time. But Jarvis reports `runtime = 17s` (for 16k). A process whose threads run for 30s cannot complete in 17s of wall time. One of these numbers must be wrong or measuring something unexpected.

**Also suspicious:** for 256k and 1m, stats appear under `put.*` columns (not `putget.*`), even though `test_case: PutGet` is fixed in the yaml for all combinations. This suggests the benchmark output contains `=== Put Benchmark Results ===` for those io_sizes, which only happens if the binary was somehow called with `test_case=Put` — OR if the PutGet path prints a "Put" header under some error condition.

**Key open question for the third agent:** What does Jarvis's `runtime` column actually measure? Options:
1. Total wall clock of the full pipeline iteration (start all services + run benchmark + stop all). If so, 17s < 30s benchmark is impossible and implies the benchmark exits before 30s.
2. Only Jarvis overhead (service launch/teardown), with the benchmark dispatched non-blocking. If so, `runtime` is irrelevant to benchmark duration and the times can coexist. The place to look is `jarvis_cd.shell.Exec.run()` — is it blocking or non-blocking?
3. Something else (e.g., CPU time, or a measurement from only the start() phase before stop()).

---

---

## Annotation from the second agent (different instance, different session)

The context above was written by the first agent and accurately describes what I implemented. I want to flag three things before this is handed off:

**1. The sentinel is broken by design — but not in the way that matters.**
The first agent correctly notes that `_parse_output` requires a `us` suffix on the latency pattern, so `-1` never matches. The practical effect is that failed rows produce *empty* CSV columns rather than `-1`. For detection purposes this is actually fine — empty latency columns are distinguishable from valid data. The issue is that the *other* failure signal (exit code → `status=failed`) also doesn't work, because Jarvis doesn't check subprocess exit codes. So currently: a failed run produces empty stats columns and `status=success`. That's visible but not loud. The third agent should decide whether to fix `_parse_output` to emit sentinels, or fix the Jarvis exit-code path, or both — but neither is blocking for diagnosis.

**2. I implemented this in the wrong order.**
The user explicitly wanted clean-slate first (remove scaffolding before adding instrumentation). I argued for instrumentation first, reasoning that removing `secs_left()` floor before understanding the failure mode risked un-diagnosable hangs. In hindsight this was overly conservative: the TOCTOU patch is already in place, and the scaffolding may itself be *causing* the anomalous behavior (the `pool_size=256` cap and epoch rollover are the most likely culprits for the 16k+ discrepancy). A bolder approach would have been to remove the scaffolding and the instrumentation in one pass, producing a clean benchmark that fails loudly. The third agent should consider whether to clean up the scaffolding before running the next diagnostic sweep.

**3. The `put.*` vs `putget.*` column split at 256k/1m is the clearest signal.**
Everything in the yaml is `test_case: PutGet`. The fact that 256k/1m rows populate `put.*` columns means `PrintResults` was called with `operation="Put"` for those runs — which only happens if `RunPutBenchmark()` was called instead of `RunPutGetBenchmark()`. That's not a regex artifact; it's a code path question. The third agent should check `pkg.py` for multiple benchmark invocations in `start()`, and check whether `test_case` is being passed correctly to the binary at larger io_sizes.

---

## What the second agent did (and why it's incomplete)

The second agent implemented the three items from the "v6 plan" (make the benchmark honest):

1. **Error propagation to exit code:** `main()` now returns `benchmark.Run()` which returns 1 if any thread set `error_flag`. `RunPut/Get/PutGetBenchmark` check `thread_completed_[i]` and return 1.
2. **Sentinel output on failure:** `PrintResults` detects `n_failed > 0` and emits `Time (avg): -1` etc. so the Jarvis regex matches `-1` (actually, the pattern `r'Time \(avg\):\s+([\d.e+\-]+)\s+us'` won't match `-1` since `-1` has no `us` suffix — so failed runs produce EMPTY stats columns in the CSV, not -1 values).
3. **First-failure logging:** `HLOG(kError, "thread {} FAILED: Put blob={} timeout={} rc={}", ...)` at each failure site.

**But the CSV still shows `status=success` for all 45 rows, and the runtime still decreases with io_size.** The changes didn't fix or expose anything because either:
- (a) No failures are actually occurring — the benchmark genuinely succeeds in 30s of thread-time, and the `runtime` column measures something shorter than the benchmark duration (the "non-blocking Exec" hypothesis above).
- (b) Failures are occurring, Jarvis isn't checking the exit code to set `status=failed`.

The second agent deferred the actual root-cause investigation of the runtime discrepancy, which is what the third agent should address first.

---

## What the third agent should do

**Step 1 — resolve the `runtime` vs `time_avg_us` contradiction.**

Read `jarvis_cd.shell` (find it with `find /home/iowarp -name "*.py" -path "*/jarvis_cd/shell*"` or similar). Specifically:
- Is `Exec.run()` blocking (waits for the subprocess) or non-blocking?
- How does `pipeline_test.py`'s `_run_single` measure the `runtime` column?

If `Exec.run()` is non-blocking: the benchmark runs in the background during Jarvis teardown, `runtime` measures only service start/stop, and the real question becomes why `time_avg_us` is consistently 30s (i.e., the benchmark IS running correctly for 30s, and there's no problem at all except we misread the `runtime` column).

If `Exec.run()` IS blocking: `runtime < 30s` means the benchmark exited early, and we need to find why `thread_times` shows 30s for a thread that exited early.

**Step 2 — once the contradiction is understood, either:**
- (a) If the benchmark is actually working correctly: document what `runtime` measures and declare the fix successful. The decreasing `runtime` is just service overhead shrinking (or the benchmark finishing faster because larger IO ops hit a throughput limit sooner). BUT verify that `total_ops` and `agg_bw_mbps` look physically reasonable across io_sizes.
- (b) If the benchmark is silently failing early: use the HLOG error output from the second agent's changes to identify the failing op (what blob, timeout or rc!=0), and diagnose the root cause.

**Step 3 — check why 256k/1m show `put.*` stats, not `putget.*`.**

This implies the benchmark output for those io_sizes contains `=== Put Benchmark Results ===` not `=== PutGet Benchmark Results ===`. Possible causes:
- The Jarvis package runs a separate Put pass before PutGet and the regex catches it first (look for multiple benchmark invocations in `pkg.py`).
- The PutGet path fails so early that it somehow falls through to Put output (unlikely given current code).
- A cached output file from a previous run is being read.

---

## Current code state of `wrp_cte_bench.cc`

The file is at `context-transfer-engine/benchmark/wrp_cte_bench.cc`. Current state after the second agent's changes:
- `thread_completed_` vector tracks which threads ran to natural deadline exit (not error_flag).
- `PrintResults` detects failures and emits a failure block (but the `-1` values in that block won't be regex-matched by `pkg.py` because the patterns require `us` suffix that the failure output lacks — so failed stats = empty CSV columns).
- `main` returns `benchmark.Run()` which returns 1 on any thread failure.
- `secs_left()` floor of 0.1f still present.
- `pool_size = depth_ * 64` still present.
- `// leak ok: runtime restarts` on DelBlob still present.

The `pkg.py` Jarvis package `_parse_output` detects the operation name from `=== (\w+) Benchmark Results ===` and keys all stats under `cte_bench_timed.{operation}.{metric}`. Jarvis does NOT check the subprocess exit code anywhere visible in `pkg.py` — `status=success/failed` is set by the Jarvis pipeline_test framework based on whether `start()` raised an exception, not the exit code.
