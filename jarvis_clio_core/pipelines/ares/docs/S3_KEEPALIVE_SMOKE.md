# Runbook: S3 bdev keep-alive smoke on Ares

Proves that the S3 bdev transport reuses HTTP connections against real AWS,
in about 15 minutes of wall clock instead of the 3.6 hours the full write
sweep takes. Run this **before** `clio_s3_write.yaml`, not instead of it: the
smoke answers "does the mechanism work", the sweep answers "did it move the
number".

- `clio_s3_write_smoke.yaml` — PUT path (`WriteBlocks` → `PutObject`)
- `clio_s3_read_smoke.yaml` — GET path (`ReadBlocks` → `GetObject`), by writing
  and then reading back through the same S3-backed bdev tier

> **`clio_s3_read.yaml` is not a keep-alive test.** Its read path is the CAE
> assimilator (`fork+exec cae_s3_tool`) into a RAM tier — the AWS SDK in a
> short-lived child process, with `S3RestClient` nowhere in it. That is why the
> read smoke is built on the *write* bench with `--verify` rather than on the
> read sweep.

---

## 0. What you are proving

`S3RestClient` used to open a fresh `Poco::Net::HTTPClientSession` per call,
paying TCP+TLS on every object. It now takes a caller-owned `S3Connection`, and
`S3BdevTransport` holds one per runtime worker — lock-free, because S3 ops run
synchronously inside a worker's task body, exactly like
`FsBdevTransport::io_contexts_`.

At teardown the transport prints a per-worker tally to the runtime log:

```
S3 keepalive worker=3 sockets=1 requests=64 reuses=63
S3 keepalive TOTAL sockets=4 requests=256 reuse_ratio=64.00
```

`sockets` is TCP connections opened, `requests` is S3 operations sent over
them. **`sockets == 1` with `requests >> 1` is reuse; `sockets == requests`
means the mechanism is dead.** Both pipelines grep this out of the job log and
**exit nonzero** if reuse did not happen — a green benchmark row with no reuse
fails the job.

This is logged at `kInfo`, not `kDebug`, on purpose: `HLOG` compiles out
anything below `CTP_LOG_LEVEL`, which defaults to `kInfo`
(`CLIO_CTP_LOG_LEVEL=1`), so a `kDebug` line does not exist in a spack build
and no env var brings it back.

---

## 1. Environment (once per login session)

```bash
# Toolchain — PATH prepends only. Never `spack load` / `conda activate` in a
# job script: they install shell functions that misbehave under `set -euo pipefail`.
export PATH="$HOME/jarvis-venv/bin:$PATH"
export IOWARP_VIEW=/mnt/common/$USER/iowarp-s3-view
export ZARR_VENV=$HOME/zarr-venv          # only botocore is needed here
export CLIO_REPO=$HOME/clio-core-fork     # auto-detected if omitted

# Bucket + credentials. Secrets stay in ~/.aws/credentials (mode 600);
# only names go in the environment.
export S3_BENCH_BUCKET=<your-bucket>
export S3_BENCH_PROFILE=clio-bench
export S3_BENCH_REGION=us-east-2          # MANDATORY, the bucket's REAL region
```

`S3_BENCH_REGION` has no default and is verified against `GetBucketLocation` in
`pre_cmds`. SigV4 is region-scoped and the bdev's signer does not follow
redirects, so a wrong region is a 301 on every request from inside a worker —
whereas botocore would silently follow the redirect and look fine.

The key pair needs `s3:ListBucket` **on the bucket** (not just object-level
Get/Put): `EnsureBucket` HEADs the bucket before any block is written. It also
needs `s3:GetBucketLocation` for the region check.

---

## 2. Build — use `spack develop`, and verify what you got

The keep-alive code is in `libclio_bdev_runtime.so`. **Build it as a develop
spec**, in the `clio-s3` environment. A develop spec builds in place from your
checkout, so an edit is an incremental recompile of a few objects; a
non-develop branch spec pins `commit=` at concretization time, so every `git
pull` needs a `spack concretize -f` *and* a full rebuild before it will produce
anything new — slower, and it fails silently by rebuilding the old commit.

### 2a. One-time: put the environment on develop

```bash
spack env activate clio-s3

spack develop -p $CLIO_REPO iowarp@968-s3-bench   # idempotent

# exactly one iowarp spec, carrying the variants the smoke needs
spack remove iowarp || true
spack add iowarp@968-s3-bench +cae +cte +s3 +s3_bdev
spack concretize -f
spack install
```

Confirm develop actually took — the spec must carry `dev_path`, and must
**not** carry a `commit=`:

```bash
spack spec -l iowarp@968-s3-bench | grep -E 'dev_path|commit='
```

### 2b. Every time after that

```bash
cd $CLIO_REPO && git pull        # branch: 968-clio-s3-benchmark
spack env activate clio-s3       # if not already active
spack install                    # REQUIRED — the pull alone rebuilds nothing
```

If `spack install` reports the spec is already installed and skips the build
(develop-spec change detection is not always reliable), force it:

```bash
spack install --overwrite -y iowarp@968-s3-bench
```

### 2c. Confirm the library before submitting

This is exactly what the pipeline's build gate checks, and it is cheap to check
yourself first:

```bash
LIB=$IOWARP_VIEW/lib/libclio_bdev_runtime.so   # or lib64/
strings "$LIB" | grep -c "S3 keepalive worker="   # must be >= 1
ldd "$LIB" | grep PocoNetSSL                      # must match
ldd "$LIB" | grep aws-cpp-sdk                     # must print NOTHING
```

If the `strings` count is 0 you have the old transport installed, and the smoke
would pass meaninglessly. The AWS SDK must stay absent **from this library's
link line** — linking it into the runtime process stack-smashes runtime init.
Note that `aws-sdk-cpp` legitimately appears in `spack find -d` output: `+cae`
needs it for the out-of-process `cae_s3_tool`. The dependency tree is not the
gate; the `ldd` line is.

### 2d. If `spack uninstall` says "matches multiple packages"

Old builds accumulate — a pre-`s3_bdev` one, a pinned non-develop one, the
develop one. Do **not** reach for `spack uninstall --force`: it orphans the
environment's view, which is what `$IOWARP_VIEW` resolves through. The
environment owns those references, so let garbage collection do it:

```bash
spack env activate clio-s3
spack gc -y        # reaps only installs nothing references
```

If a spec survives `gc`, another environment still lists it — find it with
`spack env list` and `spack -e <env> find`, and remove it there.

> If `spack install` fails on Poco with `GLIBCXX_3.4.31` / `CXXABI_1.3.15`
> undefined references, that is the recurring `.bashrc` contamination: remove
> the `envs/iowarp/bin` exports from `~/.bashrc`, then `spack clean -s` and
> retry. It is not a problem with the package.

---

## 3. Run

```bash
cd $CLIO_REPO/jarvis_clio_core/pipelines/ares
jarvis ppl submit $PWD/clio_s3_write_smoke.yaml     # PUT path,  ~5 min
jarvis ppl submit $PWD/clio_s3_read_smoke.yaml      # GET path,  ~8 min
```

Submit them one at a time — both bind port 9413 and both `pkill` stray
`clio_run` processes in `pre_cmds`, so running them concurrently on the same
node will have them tear down each other's runtime.

Each is 2 rows: `K=4` (deep reuse, ~32–64 requests per socket) and `K=16`
(isolation — reuse must hold with many workers, and sockets must scale with
workers rather than collapsing to one or exploding).

---

## 4. Read the result

```bash
tail -40 ~/clio_s3_ka_smoke_w-<jobid>.out          # write smoke
tail -40 ~/clio_s3_ka_smoke_r-<jobid>.out          # read smoke
grep "S3 keepalive" ~/clio_s3_ka_smoke_*-<jobid>.{out,err}
```

**PASS** ends with:

```
PASS: sockets are being reused across S3 operations (worst per-worker ratio 32.00)
```

**FAIL** modes and what each means:

| Message | Cause |
|---|---|
| `no 'S3 keepalive' tally in the job log` | No S3 I/O happened, or the wrong library is installed (the build gate should have caught the latter) |
| `reuse_ratio <= 1.00` | Every operation opened its own socket — the mechanism is not working |
| `a worker reused nothing` | One worker's slot is reconnecting even though others are not — suspect slot indexing |
| `requests < 2 x num_objects` (read smoke) | The verify GETs bypassed the bdev. Check `cte_core` still has exactly one device and it is the S3 tier |
| `no 'S3 keepalive TOTAL' line` | The transport never reached `Destroy` — the run did not tear down cleanly |

Note for the read smoke: a successful verify pass opens **no** new socket, so
it prints no new "opened socket" line. Absence there is the success case, which
is why the gate asserts on the request count instead.

**Ignore the throughput columns in `results.csv`.** 32 MiB per row is far too
little to say anything about bandwidth, and reporting a number from it would be
misleading.

---

## 5. Then, and only then, the real question

The smoke proves reuse happened. It does not prove reuse *helped*. That needs
the full sweep:

```bash
jarvis ppl submit $PWD/clio_s3_write.yaml     # 36 rows, ~3.6 h, overnight
```

The number that matters is **ratio-to-floor, not absolute MB/s** — on a shared
uplink the absolute value is a property of the night you ran it. The baseline
to beat, from the 2026-08-26 sweep:

- **~5.5 obj/s**, which is **0.49× the ~11.1 MB/s link floor at 1 MiB**
- large objects hide the defect: the same 5.5 obj/s is 0.96× the floor at 4 MiB

**Both outcomes are results, and report whichever you get plainly:**

- *Ratio improves* → the per-object handshake was the ~180 ms that failed to
  pipeline, and keep-alive fixed it.
- *Ratio does not move* → still useful. The keepalive tallies prove reuse
  actually happened, which **eliminates** handshake latency as the ceiling and
  re-points at worker concurrency and task routing — each S3 PUT occupies a
  whole runtime worker thread, so the concurrency ceiling is the worker pool.
  That is a clean elimination of the leading suspect, not a failure.

Do not declare victory on a green smoke. The smoke is a mechanism check.

---

## 6. Gotchas worth knowing before they cost you an allocation

- **`ipc_mode: "ipc"`, never `shm`.** The bench is a detached client and Ares
  compute nodes run `ptrace_scope=1`, which denies the `/proc` reopen SHM mode
  needs (`shm_open` → `EACCES`, surfacing as "Failed to initialize Clio").
- **`forward_env` is mandatory.** Jarvis builds the daemon's environment from
  `EnvironmentManager.COMMON_ENV_VARS`, which has no `AWS_*` entry, so
  exporting credentials in `pre_cmds` reaches jarvis and the benchmark but
  **not** the runtime that actually signs. Without it every PutBlob fails
  `rc=11`.
- **`cpus_per_task: 40`, not more.** Ares compute nodes have 40 allocatable
  CPUs; sbatch rejects the job at submit time above that.
- **Exactly one CTE device, and it is S3.** Any local tier and the DPE may place
  blobs there — the smoke would then prove nothing about S3 at all.
- **Stale jarvis package copies.** `~/.ppi-jarvis/builtin/clio_*` shadows the
  checkout; `pre_cmds` refuses to run if one exists. Remove it rather than
  working around it.
- **No AWS CLI on Ares.** Use botocore from `$ZARR_VENV`, or
  `scripts/s3_cli.py` in the repo.
