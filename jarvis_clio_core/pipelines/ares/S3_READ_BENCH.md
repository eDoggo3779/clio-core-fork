# CLIO vs Zarr — S3 read benchmark on Ares (Issue #968)

Measures how CLIO performs as an intermediary for reading array data out of real
Amazon S3, against the incumbent cloud-native array format, Zarr.

**Read-only.** CTE→S3 *write* is deliberately out of scope: the S3 block device
links the AWS SDK in-process, which corrupts `clio_run` startup. That is tracked
separately (reimplement the S3 bdev on Poco + SigV4, mirroring the working GCS
transport). CLIO's S3 *read* path — the CAE assimilator — works today and is
what this benchmark exercises.

Unlike the Issue #526 pipelines, this one is **non-containerized**: bare-metal
binaries from a spack view, no SIF, no apptainer.

---

## What gets compared

| | CLIO | Zarr |
|---|---|---|
| Driver | `clio_s3_read_bench` (C++) | `zarr_s3_read.py` (zarr-python + s3fs) |
| Jarvis package | `clio_s3_bench` (`mode: read`) | `zarr_s3_bench` (`mode: read`) |
| Path | `ParseOmni` → `S3FileAssimilator` → fork+exec `cae_s3_tool get` → CTE `PutBlob` | `zarr.open` over `FsspecStore` → `arr[:]` |
| Reads | N flat objects, whole-object GETs | N chunks of a Zarr v3 store |
| Ends up | bytes in a distributed CTE tag | a NumPy array in process memory |
| Compression | none | none **and** zstd, both in the same row |

Both stacks move **the same 2 GiB of logical data** in every row. Across the
granularity axis only the *request count* changes (4096 / 512 / 64 / 8), because
each raw object set is the same 2 GiB buffer re-split.

### Read the fairness columns, not just the headline

Each driver emits a `Fairness` block alongside its throughput block, so every
results.csv row carries the caveats:

- `agg_bw_mbps` — throughput over **logical** (uncompressed) bytes on both
  sides. This is the directly comparable number.
- `wire_bw_mbps` / `bytes_moved` — what actually crossed the network. For the
  zstd store these are ~20-26× smaller than logical. This is the single biggest
  confound in the comparison.
- `get_count`, `objects_read` — request-rate vs bandwidth regime.
- `compression`, `decode_step` — Zarr burns CPU decompressing; CLIO does not.
- `subprocess_spawns`, `temp_file_bytes` — CLIO forks `cae_s3_tool` once per
  object and stages every object whole through node-local disk before it
  reaches CTE. Both are 0 for Zarr. These are structural costs of the current
  implementation, not measurement noise.
- `runtime_worker_threads` — see the concurrency caveat below.
- `max_rss_kb` — Zarr materializes the whole array in process; CLIO streams it
  in 1 MiB chunks. Absent if `/usr/bin/time` is not installed.

**Two things that belong in any writeup:**

1. **The end states differ.** CLIO lands bytes in a distributed, tiered CTE tag
   addressable by other CLIO clients; Zarr lands a NumPy array in one process's
   heap. CLIO does strictly more work. Zarr's number is *not* "CLIO minus
   overhead."
2. **CLIO's internal pipelining is not tunable.** `kMaxChunkSize` (1 MiB) and
   `kMaxParallelTasks` (32) are `static constexpr` inside
   `S3FileAssimilator::Schedule`, so object size is the only granularity control
   on the CLIO side.

### The concurrency caveat (read before interpreting any result)

`S3FileAssimilator` downloads via `fork()` + **blocking `waitpid()`** on a
runtime worker thread — not a `CLIO_CO_AWAIT`. The worker is held for the entire
S3 GET. Effective concurrency is therefore `min(K, available workers)`, not `K`.

That is why `runtime.num_threads` is swept in lockstep with the concurrency
axis, why `cpus_per_task` is 48, and why the smoke test exists: it compares K=1
against K=32 before you commit to the full grid. If they come out the same, the
worker pool is the ceiling — raise `runtime.num_threads`, or switch to the
multi-process fallback (`clio_s3.nprocs > 1`, which partitions the key space via
`--object-stride` / `--object-offset`).

---

## One-time setup

### 0. Build IOWarp with S3 support and expose it as a view

```bash
# The spack recipe ships a version tracking this benchmark's branch.
spack install iowarp@968-s3-bench +cae +cte +s3
spack view -d false symlink -i "/mnt/common/$USER/iowarp-s3-view" iowarp@968-s3-bench
export IOWARP_VIEW="/mnt/common/$USER/iowarp-s3-view"
```

Alternatively, to build a local checkout without a recipe edit:
```bash
spack develop -p "$HOME/clio-core" iowarp@dev
spack install iowarp@dev +cae +s3
```

Verify the S3 gate actually took — the root `CMakeLists.txt` silently turns
`CAE_ENABLE_S3` back **off** if `find_package(AWSSDK)` fails, in which case the
build succeeds with no S3 support at all:

```bash
ls "$IOWARP_VIEW/bin/clio_s3_read_bench" "$IOWARP_VIEW/bin/cae_s3_tool"
```
Both must exist. Note `aws-sdk-cpp` is unpinned in the recipe and builds all
components by default — expect 30–60 minutes.

### 1. Zarr venv

```bash
python3 -m venv "$HOME/zarr-venv"
"$HOME/zarr-venv/bin/pip" install 'zarr>=3' s3fs numpy
export ZARR_VENV="$HOME/zarr-venv"
```
Do **not** try to reuse `~/zarr_benchmarks`'s environment: it pins
`requires-python >=3.13` and an unresolvable local path dependency.

### 2. AWS credentials

Long-lived IAM keys in `~/.aws/credentials`, mode 600, under a named profile.
This is the one mechanism both the AWS **C++** SDK (`cae_s3_tool`) and
**botocore** (`s3fs`) honor with no code changes:

```ini
[clio-bench]
aws_access_key_id = ...
aws_secret_access_key = ...
```
```bash
chmod 600 ~/.aws/credentials
export S3_BENCH_PROFILE=clio-bench S3_BENCH_REGION=us-east-1
```
The pipeline exports only the **profile and region names** — never secrets.
Short-lived STS/SSO tokens are a poor fit: the full grid runs longer than a
typical 1-hour token lifetime.

### 3. Stage the dataset (once, ~17 GiB, from a host with egress)

```bash
export S3_BENCH_BUCKET=my-bucket
"$ZARR_VENV/bin/python3" \
  "$CLIO_REPO/jarvis_clio_core/scripts/stage_s3_read_bench_data.py" \
  --bucket "$S3_BENCH_BUCKET" --prefix clio-s3-read-bench \
  --region "$S3_BENCH_REGION"
```

Writes a 1024³ uint16 array (2 GiB) as 8 Zarr v3 stores (chunk edges
64/128/256/512 × none/zstd) plus 4 flat-object sets at matching sizes, and a
`manifest.json`. Idempotent — the manifest is written last, so a re-run skips
completed work and redoes only partial uploads. Useful flags: `--dry-run`,
`--only zarr|raw`, `--only-granularity 256`, `--force`.

Sanity-check it end-to-end against a local S3-compatible store first if you
like — both the staging script and the Zarr reader accept `--endpoint-url` (or
`S3_ENDPOINT`), and so does `cae_s3_tool`.

The default `--pattern smooth` compresses ~20–26× with zstd, close to the
zarr_benchmarks reference dataset's 24×. It is synthetic; report the ratio
(recorded per store in `manifest.json`) rather than presenting it as a property
of real scientific data. `--pattern random` is incompressible and reduces the
compression axis to a measurement of zstd's CPU cost.

---

## Running

### Smoke test first (2 rows, ~10 min, ~4 GiB egress)

```bash
export S3_BENCH_BUCKET=my-bucket S3_BENCH_PROFILE=clio-bench
export IOWARP_VIEW=/mnt/common/$USER/iowarp-s3-view ZARR_VENV=$HOME/zarr-venv
export CLIO_REPO=$HOME/clio-core JARVIS_VENV=$HOME/jarvis-venv
jarvis ppl submit "$CLIO_REPO/jarvis_clio_core/pipelines/ares/s3_read_bench_smoke.yaml"
```

`pre_cmds` expand when the **job** runs, so export overrides *before*
submitting. Then check `$HOME/s3_read_bench_smoke_results/results.csv`:
both rows green, every `*.agg_bw_mbps` populated, and compare
`clio_s3.read.agg_bw_mbps` between K=1 and K=32.

### Full grid (36 rows, ~1.6 h, ~155 GiB egress)

```bash
jarvis ppl submit "$CLIO_REPO/jarvis_clio_core/pipelines/ares/s3_read_bench.yaml"
```

Grid: bytes-per-request {512 KiB, 4 MiB, 32 MiB, 256 MiB} × concurrency
{1, 8, 32} = 12 combinations × `repeat: 3`. Compression is not a sweep axis —
both Zarr variants run inside each row.

**Cost:** ~155 GiB of egress ≈ $14 at $0.09/GB. Raising `repeat` scales that
linearly.

---

## Verifying a run

1. `successful rows: N / 36` in the `.out` log.
2. **Check the numbers, not just the color.** The `post_cmds` print
   `GREEN ROWS WITH BLANK THROUGHPUT (== FAILURES):` — it must say `none`. A
   green row with a blank `agg_bw_mbps` is a failure: `_get_stat` is called
   inside a try/except that logs a warning and continues, so a parse failure
   drops the columns silently rather than failing the row.
3. Cross-check one row by hand: `logical_bytes` should be 2147483648 on both
   stacks, and `objects_read` should equal `get_count` for CLIO.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `clio_s3_read_bench not on PATH` | IOWarp built without `+s3`, or `AWSSDK` was not found at configure time and `CAE_ENABLE_S3` silently reverted to OFF |
| `Preflight GET failed` | bad credentials/profile, wrong region, wrong bucket, or the dataset was never staged at that prefix |
| CLIO rows blank, Zarr rows fine | the **runtime** could not find `cae_s3_tool`; it forks the helper, so `CAE_S3_TOOL` must be exported in `pre_cmds` (the package's own env does not reach the daemon) |
| `zarr venv broken` | `$ZARR_VENV` missing `zarr`/`s3fs`/`numpy` |
| Raising concurrency changes nothing | the blocking-`waitpid` worker ceiling — raise `runtime.num_threads` or use `clio_s3.nprocs > 1` |
| No `max_rss_kb` column | `/usr/bin/time` not installed; throughput columns are unaffected |
| Disk full under `/tmp` | `TMPDIR` needs `concurrency × object_size` (32 × 256 MiB = 8 GiB) |
