# CLIO vs Zarr — S3 write benchmark on Ares (Issue #968)

Measures how CLIO performs as an intermediary for **writing** array data into
real Amazon S3, against Zarr and against a raw-PUT wire-speed floor.

This is the counterpart to [S3_READ_BENCH.md](S3_READ_BENCH.md). That benchmark
was read-only because CLIO's S3 *write* path — the `kS3` bdev — linked the AWS
SDK in-process, which stack-smashes `clio_run` startup. **That is now fixed**:
the bdev was reimplemented on Poco + SigV4, mirroring the working GCS transport,
and the AWS SDK is gone from `libclio_bdev_runtime.so` entirely. Writing is
therefore in scope for the first time.

Like the read benchmark, this pipeline is **non-containerized**: bare-metal
binaries from a spack view, no SIF, no apptainer.

---

## What gets compared

| | CLIO | Zarr | Raw floor |
|---|---|---|---|
| Driver | `clio_s3_write_bench` (C++) | `zarr_s3_write.py` (zarr-python + s3fs) | `s3_raw_put.py` |
| Path | `AsyncPutBlob` → CTE → `kS3` bdev `WriteBlocks` → signed PUT from the runtime daemon | `zarr.create_array` over `FsspecStore` → `arr[:] = data` | K concurrent `cae_s3_tool put` |
| Writes | N blobs, split into `block_<offset>` objects | N chunks of a Zarr v3 store | N flat objects |
| Starts from | bytes in a CLIO shared-memory buffer | a NumPy array in process memory | pre-staged local files |
| Compression | none | none **and** zstd, both in the same row | none |

All three stacks move **the same 256 MiB of logical data** in every row, in the
same 4 MiB unit.

### Read the raw floor first

The `rawput` row is not a competitor — it is the **bound**. It does the least
possible work: no CTE, no chunking layer, no metadata, no compression, just
concurrent PUTs of files that were staged before the clock started. Nothing in
the comparison should beat it.

Without it a poor CLIO number is uninterpretable. If CLIO is slow *and* rawput
is slow, the bottleneck is the link or the bucket, and no amount of CLIO work
will move it. Only a gap between them is a CLIO finding.

> **Future work, deliberately not done here:** the *read* benchmark has no
> equivalent floor, which is the same interpretability gap in the other
> direction. Back-porting a raw-GET floor to `s3_read_bench.yaml` would make
> those numbers attributable the same way. Do not touch the read bench for it —
> it is a separate change.

### Read the fairness columns, not just the headline

Each driver emits a `Fairness` block alongside its throughput block, so every
results.csv row carries the caveats:

- `agg_bw_mbps` — throughput over **logical** (uncompressed) bytes on all three
  sides. This is the directly comparable number.
- `wire_bw_mbps` / `bytes_moved` — what actually crossed the network. **This
  confound runs the opposite way from the read benchmark.** On reads,
  compression meant Zarr fetched fewer bytes. On writes it means Zarr *sends*
  fewer bytes, while CLIO's bdev sends every byte uncompressed. A zstd row that
  looks faster may simply have transferred less.
- `objects_written`, `put_count` — request-rate vs bandwidth regime. Note CLIO's
  count is derived from block geometry (`blob_size / block_size`), not from the
  blob count.
- `compression`, `decode_step` — for writes, `decode_step` is really the
  *encode* pass. It is kept under the read-side name so one parser key serves
  both benchmarks.
- `subprocess_spawns`, `temp_file_bytes` — **both are 0 for CLIO here**, which
  is the sharpest contrast with the read path: reading forks `cae_s3_tool` once
  per object and stages each object whole through node-local disk, while
  writing signs and PUTs directly from the runtime worker. The raw floor pays
  one spawn per object by construction.
- `runtime_worker_threads` — see the concurrency caveat below.
- `max_rss_kb` — Zarr materializes the whole array in process. Absent if
  `/usr/bin/time` is not installed.

**Two things that belong in any writeup:**

1. **The starting states differ.** CLIO writes from a CLIO shared-memory buffer
   through a distributed, tiered CTE; Zarr writes from one process's heap. CLIO
   does strictly more work. Zarr's number is *not* "CLIO minus overhead."
2. **Source entropy is an input, not an accident.** `compressibility` (default
   0.5) sets how compressible the Zarr source data is. At `0.0` zstd cannot
   compress at all and in fact slightly *expands* the data, so the zstd row
   measures nothing but encode overhead; at `1.0` it compresses to almost
   nothing. Neither resembles real scientific arrays. Whatever value is used
   must be stated alongside the numbers.

### The concurrency caveat (read before interpreting any result)

**The S3 PUT blocks a runtime worker thread.** `WriteBlocks` is a coroutine body
in the bdev, and the signed PUT runs to completion inside it. So the effective
concurrency ceiling is `clio_runtime.num_threads`, not the requested `K`.

This is structurally the same ceiling the read benchmark hit — there because the
assimilator did `fork()` + blocking `waitpid()` on a worker, here because the
HTTP request itself is synchronous. Different cause, identical consequence:
**always sweep `runtime.num_threads` alongside `concurrency`**, and compare
`requested_concurrency` against `effective_concurrency` and measured scaling
before concluding anything.

The smoke pipeline does exactly this: `K=1` with 4 workers vs `K=32` with 48.

---

### What the first real run measured (2026-08-26, 4 MiB blobs, 64 blobs)

| | K=1 agg | K=1 wire | K=32 agg | K=32 wire | % of floor @ K=32 |
|---|---|---|---|---|---|
| raw PUT floor | 4.72 | 4.72 | 10.75 | **10.75** | — (is the floor) |
| zarr (none) | 5.06 | 5.06 | 10.62 | 10.62 | 98.8% |
| zarr (zstd) | 7.14 | 3.71 | **19.90** | 10.34 | 96.2% |
| CLIO → S3 bdev | 6.08 | 6.03 | 12.17 | 10.11 | 94.0% |

All MB/s. Both rows `status: success`, `objects_measured` = 64 = `num_blobs`,
`bytes_measured` = 268435456 exactly, `put_count` = `objects_measured` (no
allocator fragmentation), `--verify` clean on both.

**The finding is the convergence, not any single number.** Four independent
write paths landing within 6% of each other at K=32 is one shared constraint:
roughly 85 Mbit/s of egress from an Ares compute node to S3. CLIO is not slow
here — the link is. Report **ratio to floor**, which is a property of CLIO;
the absolute MB/s is a property of the night you ran it.

The corollary is that CLIO's apparently poor concurrency scaling (1.7× for 32×
the concurrency) is *not* a CLIO property either. Zarr and the raw floor scale
by the same amount and stop at the same place.

Note the zstd row: **19.90 is the largest number in the file and it is the one
that will get misquoted.** It moved 133 MiB of wire bytes to CLIO's 256 MiB for
the same logical payload; on the wire it is marginally *slower* than CLIO.

---

### What the full 36-row sweep measured (2026-08-26)

2 sizes × 6 concurrencies × 3 repeats, all 36 rows `success`. Wire MB/s, mean of
the three repeats:

| K | \| | CLIO 1M | rawput 1M | zarr 1M | zstd 1M | \| | CLIO 4M | rawput 4M | zarr 4M | zstd 4M |
|---|---|---|---|---|---|---|---|---|---|---|
| 1  | | 2.51 | 1.72 | 4.14 | 2.50 | | 6.13 | 4.76 | 4.84 | 3.61 |
| 4  | | 2.60 | 4.04 | 9.49 | 7.74 | | 6.33 | 7.38 | 10.73 | 9.73 |
| 8  | | 3.41 | 5.46 | 10.75 | 9.94 | | 7.64 | 9.18 | 11.08 | 10.81 |
| 16 | | 3.92 | 7.75 | 10.87 | 10.68 | | 9.27 | 10.46 | 11.10 | 10.99 |
| 32 | | 4.76 | 9.91 | 10.90 | 10.68 | | 9.91 | 10.83 | 11.06 | 10.91 |
| 64 | | 4.87 | 9.95 | 10.83 | 10.51 | | 10.57 | 11.06 | 10.93 | 10.82 |

**The K=64 rawput point settles the ceiling question.** rawput moves +0.3% (1
MiB) and +2.2% (4 MiB) from K=32 to K=64 — flat. rawput forks K processes and
uses no runtime worker, so a per-connection concurrency limit would still be
climbing there. It is the **link**, ~11.1 MB/s, and nothing in CLIO can beat it.

**At 4 MiB CLIO converges on the floor: 0.96× at K=64**, having climbed 0.86 →
0.83 → 0.89 → 0.92 → 0.96. Report that ratio, not the MB/s.

**At 1 MiB it does not.** CLIO plateaus at 4.87 MB/s — **0.49× the floor** —
while rawput and zarr both reach ~10.9. Per the decision rule in the smoke
YAML's header, *CLIO well below rawput ⇒ CLIO's own ceiling*, and this is that
case. It is a **per-object** ceiling, not a bandwidth one: CLIO saturates at
~5.5 objects/s, worth 5.5 MB/s at 1 MiB but 22 MB/s at 4 MiB — above the link,
which is exactly why the 4 MiB rows look healthy and hide it.

The K=1 latency fit says the fixed cost is not the problem. Fitting
`latency = fixed + size/rate` through the two K=1 points:

| stack | fixed | marginal rate |
|---|---|---|
| CLIO | 313 ms | 11.96 MB/s |
| rawput | 497 ms | 11.64 MB/s |

Both see the same ~12 MB/s link, and CLIO's *fixed* per-object cost is the
**lower** of the two — 313 ms against the floor's 497 ms of fork+exec plus temp
file. That is why CLIO beats the floor at K=1 (1.47× at 1 MiB, 1.29× at 4 MiB).
CLIO's problem is that ~180 ms of that per-object work does not pipeline across
concurrency, where the floor's does. Compare the scaling K=1→64: rawput 5.8×,
CLIO 2.2×.

The oversubscription check the full-sweep YAML calls for comes back **clean**:
at K=64 (`runtime.num_threads: 64` on `cpus_per_task: 40`) CLIO does not dip
below K=32 at either size — 4.76 → 4.87 and 9.91 → 10.57. No need to re-run
that point at 48 threads.

**zstd compresses 1.93× and is link-bound like everything else.** Its
`agg_bw_mbps` reaches 20.6 — above the 11.1 MB/s link — purely because logical
bytes exceed wire bytes. Quoting that as a throughput win over CLIO is the
single easiest misread of this sweep; compare `wire_bw_mbps`.

**Client memory is a clear CLIO win, by ~6×.** At 4 MiB / K=64: CLIO 266 MB,
zarr 1664 MB, zstd 1724 MB. CLIO's K-slot SHM window grows as K × blob_size and
nothing else; zarr materializes the whole 1 GiB array in-process. rawput is flat
at 21 MB only because its bytes live in a temp file — `temp_file_bytes` reaches
256 MiB at K=64, so it moved the cost to disk rather than avoiding it.

Run-to-run spread over the 3 repeats: zarr is the steadiest (median 0.3% CV),
CLIO and rawput median ~3% with occasional 16% outliers — shared-uplink weather,
which is what `repeat: 3` is for.

## Addressing: the key prefix is mandatory

CTE registers each target as `device.path_ + "_node<N>"`. For a cloud device
that suffix lands on the **path string**, so:

```
s3://bucket/clio-s3-write-bench/bdev   ->  s3://bucket/clio-s3-write-bench/bdev_node0
```

which is exactly right — it gives free per-node key isolation. But:

```
s3://bucket                            ->  s3://bucket_node0
```

**corrupts the bucket name.** A bare bucket with no prefix will fail against a
bucket that does not exist, or worse, silently target one that does. Always
configure a prefix.

---

## One-time setup

### 0. Build IOWarp with the S3 bdev and expose it as a view

The write path needs the **`+s3_bdev`** variant (Poco + SigV4). This is a
different feature from `+s3`, which gates the CAE assimilator and `cae_s3_tool`
— you need **both**, because the raw-PUT floor uses `cae_s3_tool`:

```bash
spack install iowarp@<branch> +cae +cte +s3 +s3_bdev
spack view --dependencies no symlink /mnt/common/$USER/iowarp-s3-view iowarp@<branch>
```

Two traps worth knowing before you spend a build on them:

- **`spack view symlink` never overwrites existing links.** With another
  `iowarp` already in the view it logs conflicts and *skips* them, so the
  refresh looks successful while the view keeps serving the OLD install.
  `spack view rm` first, then symlink, then assert the view's `.so` resolves
  into the prefix you just built.
- **Branch versions do not rebuild on new commits.** Spack's hash for a git
  branch does not change when the branch moves, so `spack install` reports it
  already installed and silently skips the compile. Use
  `spack uninstall -y <spec> && spack clean -s && spack install <spec>` — the
  `spack clean -s` is required, or it re-clones the old commit.

The pipeline asserts the build for you in `pre_cmds`: it fails fast if
`libclio_bdev_runtime.so` lacks Poco NetSSL, or if it still links the AWS SDK.

### 1. Zarr venv

Same venv the read benchmark uses (`zarr`, `s3fs`, `numpy`). The pipeline also
uses its `s3fs` for the post-run purge, so it must be importable.

### 2. AWS credentials

**Credentials reach the runtime differently than in the read benchmark.** There,
`cae_s3_tool` resolved `AWS_PROFILE` through the AWS SDK's credential chain.
Here the process that signs is the `clio_run` daemon, and the Poco SigV4 signer
reads **raw environment variables only** — it has no profile support at all.

The pipeline's `pre_cmds` therefore resolve the profile to keys at job time.
Ares has **no AWS CLI**, so `aws configure export-credentials` is unavailable;
the credentials are parsed out of `~/.aws/credentials` (mode 600) with stdlib
`configparser`.

Exporting them is **not** enough on its own — the daemon does not inherit the
job script's environment. The `clio_runtime` package's `forward_env` option
carries the names listed in the pipeline into the runtime's environment. See the
troubleshooting entry for the full mechanism; get this wrong and every `PutBlob`
fails with `rc=11`.

**No secrets are stored in the YAML** — only profile and region names. Set:

```bash
export S3_BENCH_BUCKET=my-bucket
export S3_BENCH_PROFILE=clio-bench
export S3_BENCH_REGION=us-east-2
```

`S3_BENCH_REGION` is **mandatory and must be the bucket's real region** — there
is deliberately no `us-east-1` default. **SigV4 is region-scoped**, and a
mismatch is an HTTP **301/400**, not a 403 — an unhelpful error to debug from
the runtime log. `pre_cmds` verifies it against `GetBucketLocation` rather than
trusting it, because botocore silently follows the redirect and the bdev's
signer does not.

### 3. No dataset staging

Unlike the read benchmark, nothing needs staging: this pipeline creates the data
it writes. The bucket only needs to exist and be writable.

---

## Running

### Smoke test (2 rows, ~10 min)

```bash
export IOWARP_VIEW=/mnt/common/$USER/iowarp-s3-view ZARR_VENV=$HOME/zarr-venv
jarvis ppl submit $PWD/s3_write_bench.yaml
```

Output: `${HOME}/s3_write_bench_results/results.csv`, 2 rows.

Run this first on any new machine, bucket, or build. It is the cheapest thing
that proves credentials reach the daemon and bytes reach the bucket.

### Full sweep (36 rows, ~3.6 h)

```bash
export IOWARP_VIEW=/mnt/common/$USER/iowarp-s3-view ZARR_VENV=$HOME/zarr-venv
jarvis ppl submit $PWD/s3_write_bench_full.yaml
```

Output: `${HOME}/s3_write_bench_full_results/results.csv`, 36 rows —
2 granularities (1 MiB, 4 MiB) × 6 concurrencies (1, 4, 8, 16, 32, 64) ×
3 repeats. Sized for overnight; `time: "08:00:00"` in the scheduler block.

Every gate and credential path is carried over from the smoke verbatim, so a
green smoke is a strong predictor of a green sweep. Two deliberate differences:
`verify` is **off** (the smoke settled byte-fidelity; `objects_measured` is the
per-row guard and costs no egress), and `num_blobs` is 256 rather than 64 so
that K=64 has several windows of work behind it instead of one.

The grid is built around what the smoke found — see the header comment in
`s3_write_bench_full.yaml` for why there is no 16 MiB axis and what the K=64
`rawput` point is there to settle.

### Targeted diagnostic (18 rows, ~45 min)

```bash
jarvis ppl submit $PWD/jarvis_clio_core/pipelines/ares/s3_write_bench_diag.yaml
```

Scaled down from the full grid to resolve one open defect: CLIO's ~5.5
objects/s ceiling. It is **not** a comparison sweep and should be deleted or
ignored once the defect is fixed — scale back up with
`s3_write_bench_full.yaml`.

What it changes, and why each change is load-bearing:

| | full sweep | diagnostic | why |
|---|---|---|---|
| blob size | 1m, 4m | **256k**, 1m, 4m | 256k is where the two hypotheses are 7× apart |
| concurrency | swept 1→64 | **pinned 32** | the knee is known; sit past it |
| worker pool | **zipped to K** | **16 vs 48, un-zipped** | removes the confound — see below |
| baselines | zarr + zstd + rawput | **rawput only** | zarr's question is closed; it cost ~40% of each row |
| rows | 36 (~3.6 h) | 18 (~45 min) | |

**The confound this removes.** The full sweep zipped `clio_s3.concurrency`,
`runtime.num_threads` and `clio_s3.worker_threads` onto one axis. That was
right for finding the knee, but it means "CLIO stops improving past K=32" and
"CLIO stops improving past 48 workers" are *the same data points* — nothing in
those 36 rows separates them. Pinning K and moving only the pool separates them
in one step.

**`objects/s`, not MB/s, is the measurement.** `num_blobs` is held at 256 at
every size so the object count is constant and obj/s is comparable down the
axis. (This inverts the full sweep's reasoning for the same knob, which held
blob count constant to keep the K=64 window meaningful.)

**Exclude link-bound rows before concluding anything.** A row at ratio ≈ 1.0 is
doing what the network allows, not what CLIO allows, so its obj/s says nothing
about a per-object cost. 4 MiB is exactly such a row — 5.5 obj/s there would be
22 MB/s, twice the link — and including it makes a perfectly flat series read as
2× variable. That is the same trap that made 4 MiB look healthy in the 36-row
sweep. `post_cmds` does this exclusion for you and prints the verdict directly
in the job log, so the answer arrives without opening the CSV.

How to read the two verdicts:

* **Axis 1** — obj/s flat across sizes *below the link* ⇒ a fixed per-object
  cost is real, and the printout names its size in ms. Varying ⇒ it is not a
  per-object quantum and the 1 MiB plateau needs another explanation.
* **Axis 2** — obj/s rising 16→48 workers ⇒ **pool-bound**: effective
  concurrency was capped below K all along, and the fix is worker accounting.
  Flat ⇒ the pool is innocent and the serialization is in the DPE, the
  allocator, or the bdev's per-block path — profile the runtime, do not add
  threads.

Cost is ~9k PUTs, under $0.10.

### Cost

Writes are cheap: ingress to S3 is free and PUTs run ~$0.005/1000, so the smoke
is effectively free — unlike the read grid's ~155 GiB of egress (~$14). The full
36-row sweep issues roughly 37k PUTs, about **$0.18**. Only leftover object
storage accrues, and `post_cmds` purges the write prefix.

The one thing that is *not* free on the write side is `verify`: it re-reads
every blob, which is GET egress. That is why the smoke has it on (128 blobs,
cents) and the full sweep has it off (~19 GiB, ~$1.70, to re-answer a settled
question).

Object keys are deterministic (`block_<offset>`, `raw_%06d.bin`, zarr chunk
paths), so re-runs overwrite rather than accumulate. Storage does not grow
without bound even if a purge is skipped.

---

## Plotting the results

```bash
python3 jarvis_clio_core/scripts/plot_s3_write_bench.py \
    ~/s3_write_bench_full_results/results.csv [output_dir]
```

Needs `pandas` + `matplotlib` (the zarr venv has both). Prints the wire-MB/s
table above, then writes five PNGs:

| file | what it answers |
|---|---|
| `s3_write_wire_bw.png` | the cross-stack comparison — bytes actually on the wire |
| `s3_write_agg_bw.png` | logical MB/s; the gap against the wire figure *is* compression |
| `s3_write_ratio.png` | **the headline** — each stack ÷ the raw-PUT floor of the same row |
| `s3_write_ops.png` | objects/s, where a per-object ceiling shows up as a flat bar |
| `s3_write_max_rss.png` | client peak memory (log scale) |

One subplot per blob size, x-axis concurrency, one bar per stack, error bars =
repeat stddev. The transpose of the read bench's layout, because on the write
side the question is scaling with K and the answer differs by size.

**Read `s3_write_ratio.png` first.** When four stacks are all pinned to the same
link, absolute MB/s says nothing about any of them; only the ratio to the floor
separates "CLIO is slow" from "S3 is slow". Ratios are computed **per row**, not
from column means, so numerator and denominator saw the same link weather and
the error bars on the ratio are real.

Bars are hatched where a cell is not measuring what it claims: K=1 in the ratio
figure (the floor is fork+exec-bound there, not a floor), and any cell where
requested K exceeds the object count (`effective_concurrency` caps at the object
count, silently duplicating a lower-K cell — cannot happen at `num_blobs: 256`,
but a smaller smoke can trip it).

`share_y` is per-metric. MB/s, the ratio, and RSS are comparable between panels
and share an axis; objects/s does **not**, because a 4 MiB object at the same
bandwidth is a quarter the object rate.

---

## Verifying a run

1. **Every row `status: success`** — 2 for the smoke, 36 for the full sweep.
   `post_cmds` prints the count; a short count means rows failed silently.
2. **No blank throughput columns.** `post_cmds` asserts this, because a green
   row with a blank throughput column is a **failure**, not a success. The
   required columns are `clio_s3.write.agg_bw_mbps`, `zarr_s3.write.agg_bw_mbps`,
   `zarr_s3.writezstd.agg_bw_mbps`, and `raw_put.rawput.agg_bw_mbps`.
3. **`objects_written` and `put_count` > 0** on every stack.
4. **`clio_s3.write.objects_measured` equals `num_blobs`.** This one is a `list`
   of the bucket prefix rather than a number the benchmark computed, so it is
   the only column a run that wrote nothing cannot fabricate. Zero means the
   row is fiction regardless of what the throughput columns say.

   **More than `num_blobs` does not necessarily mean the allocator
   fragmented.** Check `put_count` first: if `put_count == objects_written ==
   num_blobs` and only `objects_measured` is high, the allocator was fine and
   the listing picked up **stale objects from an earlier row**. That is what
   the 2026-08-26 sweep hit: all 36 rows share one key prefix
   (`clio-s3-write-bench/bdev`), and every 4 MiB row reported
   `objects_measured: 448` against `num_blobs: 256`. The excess was exactly 192
   objects / 201326592 bytes = 192 × 1 MiB — orphaned blocks from the 1 MiB
   rows that teardown's `FreeBlocks` never deleted, constant across all
   eighteen 4 MiB rows rather than accumulating.

   It contaminated nothing but the two `*_measured` columns — every throughput
   figure comes from `logical_bytes` and `wall_time_us`, which the benchmark
   owns — but it blunted this guard from an equality into a lower bound,
   because a row that wrote nothing would still have listed its predecessors'
   objects.

   **Fixed by `purge_prefix` (default on):** the package empties the bdev
   prefix at the start of every row, so the count is exact again. Only that
   prefix is in range — zarr's store sits one level up and rawput's keys in a
   sibling — and an empty prefix is refused outright rather than widening to
   the whole bucket.

   Two consequences worth knowing:

   * **`objects_purged` is a new per-row column.** Nonzero means the *previous*
     row leaked that many objects. It is there because the purge fixes the
     measurement, **not the leak**: something in the bdev teardown path is
     still failing to issue a `DeleteObject` per block, and this column is what
     keeps that visible instead of papering over it. A sweep where
     `objects_purged` is 0 everywhere but the first row means the leak is gone.
   * **If the purge fails, it says so and the row still runs.** The log line
     names it, and `objects_measured` reverts to a lower bound for that row —
     compare it against `put_count` rather than `num_blobs`.

   To check the prefix by hand (Ares has no AWS CLI):
   `python3 scripts/s3_cli.py ls "$S3_BENCH_BUCKET" --prefix clio-s3-write-bench/bdev`
5. **`rawput` is the fastest row *at K ≥ 8*, compared on `wire_bw_mbps`.**
   Two qualifications, both learned the hard way on 2026-08-26:

   * **Not at K=1.** The floor forks one `cae_s3_tool` per object and stages
     each through a temp file, so at concurrency 1 it pays `num_blobs`
     serialized `fork+exec` calls in the critical path and came back *slower*
     than CLIO (4.72 vs 6.03 MB/s). Its own fairness columns show why —
     `subprocess_spawns: 64`, `temp_file_bytes: 4194304`. At K ≥ 8 that
     overhead pipelines across the concurrent processes and the floor becomes
     honest. It is a floor for **sustained throughput**, not for single-op
     latency.
   * **Compare `wire_bw_mbps`, not `agg_bw_mbps`.** `zarr_s3.writezstd`
     legitimately beats every other stack on `agg_bw_mbps` because it moves
     roughly half the bytes for the same logical payload. On the wire it is
     no faster. See "Read the fairness columns" above.

   If CLIO beats the floor on **wire** bandwidth at high K, *then* something
   is not reaching S3 — check that the CTE tier really is the S3 device and
   not a local fallback, and check `objects_measured`.
6. **Run once with `clio_s3.verify: true`** to prove bytes round-tripped: it
   re-reads every blob through CTE and compares content byte-for-byte. Leave it
   off for timed rows.

---

## Troubleshooting

**"Failed to initialize Clio" from the benchmark.** Ares compute nodes run
`ptrace_scope=1`, which blocks the SHM attach path for a detached client. The
pipeline sets `ipc_mode: "ipc"` (unix socket) for this reason — do not change
it.

**HTTP 301 in the runtime log.** Region mismatch. `S3_BENCH_REGION` must be the
bucket's actual region; SigV4 signatures are scoped to it.

**HTTP 403 in the runtime log.** The credential export did not reach the daemon.
Confirm `pre_cmds` printed `credentials exported from [...]`, and that the
profile exists in `~/.aws/credentials`.

**Throughput identical to a RAM tier / suspiciously fast.** The DPE placed blobs
somewhere other than S3. The CTE package must configure **exactly one** device
and it must be the `s3://` one — any local tier present gives the DPE an
alternative.

**`mkdir: cannot create directory 's3:'`.** An older `clio_cte` package that
does not skip cloud paths in its `Mkdir` loop. Pull the branch.

**The bdev link assert fails in `pre_cmds`.** Either the view is stale (see the
`spack view symlink` trap above) or the build lacked `+s3_bdev`.

**Every `PutBlob` fails with `rc=11`.** CTE has no target to place on. `rc` in the
range 11–19 is `10 + alloc_result` from `PlaceBlobBytes`; 11 means allocation
found no viable device. Scroll **up** in the runtime log — the cause is printed
minutes earlier and looks like this:

```
core_config.cc:534 ERROR ParseStorageConfig Config error: Invalid bdev_type 's3'
                     (must be 'file', 'ram', 'hbm', 'pinned', or 'noop')
core_runtime.cc:743 WARNING Create Warning: No storage devices configured
```

That error message is the *old* one — the current build names `'s3', or 'gcs'` in
the same list. So the runtime library predates the s3/gcs allowlist, the S3 tier
was dropped at config-parse time, and CTE came up with zero devices. Note that
this is only a `WARNING`: the pool is created successfully and the failure does
not surface until the first write.

The `pre_cmds` gate greps the compiled-in literal out of
`libclio_cte_core_runtime.so` to catch this before the allocation is spent.

**"mixed IOWarp installs on PATH".** Two different `iowarp` prefixes were
reachable at once — typically a stale `IOWARP_VIEW` supplying `clio_run` while a
freshly built spack prefix supplies `clio_s3_write_bench`. Because
`spack view symlink` never overwrites existing links, refreshing a view that
already has an `iowarp` in it silently keeps serving the old one. The reliable
fix is to skip the view entirely and point `IOWARP_VIEW` straight at the prefix:

```bash
export IOWARP_VIEW=$(spack find --format '{prefix}' iowarp@968-s3-bench | tail -1)
```

RPATH makes the symlink farm unnecessary, and a prefix has the `bin/` and `lib/`
layout the pipeline expects.

**`S3 bdev: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are not set`, even though
the job script exported them.** This is the same `rc=11` cascade as above, one
layer down: the bdev fails to initialize, `core_runtime.cc` logs
`Failed to register target ... (error code: 1)` as a **warning**, CTE comes up
with zero devices, and every `PutBlob` returns 11.

Exporting credentials in `pre_cmds` is not enough. Jarvis launches the daemon as
`PsshExecInfo(env=self.env, ...)`, and `self.env` is a dict it builds itself from
`EnvironmentManager.COMMON_ENV_VARS` — a fixed toolchain list (`PATH`,
`LD_LIBRARY_PATH`, `HOME`, `CC`, …) with **no `AWS_*` entry**. The job script's
exports therefore reach jarvis and every benchmark process but never `clio_run`.
The job log names the mechanism:

```
Auto-built environment with N variables (no 'env' field in pipeline)
```

The `clio_runtime` package's `forward_env` option copies named variables from
the submitting shell into the runtime's environment, and this pipeline lists the
AWS names there. Values are never logged — only names, and only whether each was
set. A top-level `env:` dict in the pipeline would also work, but it would put
the secret in a file on disk; `forward_env` reads it from the live shell.

`forward_env` refuses to forward a value containing `$`, a backtick, or a
backslash. The ssh transport emits each variable as an inline `KEY="value"`
prefix and escapes only the double quote, so those characters would reach the
daemon altered — and a corrupted secret is indistinguishable from a permissions
problem at the far end. AWS keys are base64 (`A–Za–z0–9+/=`), so this should
never trigger; if it does, regenerate the credential.

**`clio_s3_write_bench is STALE -- it predates <marker>`.** `spack develop`
builds compile from the working tree, so pulling the branch does **not** rebuild
them, and nothing about the spec, the hash or the view changes to show it. Run:

```bash
spack install iowarp@968-s3-bench      # dev spec: rebuilds in place
```

For a non-`develop` spec, branch versions never rehash when the branch moves, so
`spack install` reports "already installed" and skips the compile entirely:

```bash
spack uninstall -y iowarp@968-s3-bench && spack clean -s \
  && spack install iowarp@968-s3-bench +cae +cte +s3 +s3_bdev
```

The gate greps a build stamp (`kBuildMarker` in `clio_s3_write_bench.cc`) out of
the installed binary rather than trusting the spec.

**`S3_BENCH_REGION=... but bucket ... lives in ...`.** SigV4 is region-scoped and
the bdev's signer does not follow redirects, so a wrong region is an HTTP 400/301
on every PUT, from inside a runtime worker. This is easy to miss because
**botocore hides it**: it transparently retries against the correct region, so an
`aws`-style check or a `HeadBucket` preflight goes green while the daemon fails.
The preflight therefore asks S3 for the authoritative answer with
`GetBucketLocation` and refuses to run on a mismatch, naming the export to fix.

**A row is green but `clio_s3.write.objects_measured` is 0.** Nothing reached the
bucket and the throughput columns are fiction. `objects_measured` is a `list`
of the bdev's key prefix taken right after the timed loop — the one column a run
that wrote nothing cannot fabricate. It runs before teardown on purpose:
`FreeBlocks` issues a `DeleteObject` per block, so a count taken later reads
zero even on a healthy run.

**`objects_measured` disagrees with `put_count`.** Not a failure. One `PutBlob`
normally becomes exactly one S3 object: `AllocateFromTarget` hands the whole
request to the allocator, `WriteBlocks` issues one `PutObject` per returned
block, and an unfragmented request gets a single block. More objects than blobs
means the allocator fragmented and split the request. (An earlier version of this
benchmark derived `PUT count` as `ceil(blob_size / block_size)` from a
`--block-size` flag that configured nothing, which overstated it 4× at the smoke
test's 4 MiB blobs. The flag is gone.)

**`TaskStatModel: failed to open /tmp/clio/models/...`.** Harmless. The runtime
persists a perf model there and logs an error per attempt if it cannot. `/tmp` is
node-local, so creating the directory on the login node does nothing — `pre_cmds`
creates it on the compute node. It does not gate bdev init or `PutBlob`.
