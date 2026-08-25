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

The pipeline's `pre_cmds` therefore resolve the profile to keys at job time and
export them, so the daemon inherits them. Ares has **no AWS CLI**, so
`aws configure export-credentials` is unavailable; the credentials are parsed
out of `~/.aws/credentials` (mode 600) with stdlib `configparser`.

**No secrets are stored in the YAML** — only profile and region names. Set:

```bash
export S3_BENCH_BUCKET=my-bucket
export S3_BENCH_PROFILE=clio-bench
export S3_BENCH_REGION=us-east-2
```

`S3_BENCH_REGION` must match the bucket's real region. **SigV4 is
region-scoped**, and a mismatch is an HTTP **301**, not a 403 — an unhelpful
error to debug from the runtime log.

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

There is deliberately **no full grid yet**. Achievable write concurrency and
latency are unknown until this has been measured once; sizing a 36-row grid
before that would be guessing. Widen the sweep after reading the smoke.

### Cost

Writes are cheap: ingress to S3 is free and PUTs run ~$0.005/1000, so the smoke
is effectively free — unlike the read grid's ~155 GiB of egress (~$14). Only
leftover object storage accrues, and `post_cmds` purges the write prefix.

Object keys are deterministic (`block_<offset>`, `raw_%06d.bin`, zarr chunk
paths), so re-runs overwrite rather than accumulate. Storage does not grow
without bound even if a purge is skipped.

---

## Verifying a run

1. **2 rows, both `status: success`.**
2. **No blank throughput columns.** `post_cmds` asserts this, because a green
   row with a blank throughput column is a **failure**, not a success. The
   required columns are `clio_s3.write.agg_bw_mbps`, `zarr_s3.write.agg_bw_mbps`,
   `zarr_s3.writezstd.agg_bw_mbps`, and `raw_put.rawput.agg_bw_mbps`.
3. **`objects_written` and `put_count` > 0** on every stack.
4. **`rawput` is the fastest row.** If CLIO or Zarr beats the floor, something
   is not actually reaching S3 — check that the CTE tier really is the S3 device
   and not a local fallback.
5. **Run once with `clio_s3.verify: true`** to prove bytes round-tripped: it
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
