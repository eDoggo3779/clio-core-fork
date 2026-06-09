# Mofka Benchmark — Architecture Decisions

A dated log of the specific design decisions made while reimplementing the
Mofka single-/multi-node benchmarks in `jarvis_clio_core`. The framing
throughout is **comparison against CLIO Core's CTE** — Mofka and CTE are both
benchmarked with Jarvis on Ares, and these notes record where their benchmark
harnesses differ and why, so the resulting numbers are interpreted fairly.

Source of the reimplementation: archived branch `claude/mofka-multinode`
(old `jarvis_iowarp/` layout). Each decision is timestamped; entries from the
reimplementation session are dated **2026-06-08**.

---

## 2026-06-08 — Launch pattern: Jarvis `scheduler:` block + `jarvis ppl submit` (Pattern B)

**Decision.** Mofka pipelines are single YAML files carrying a top-level
`scheduler:` block plus a `config:`-wrapped sweep (`vars`/`loop`/`repeat`/
`output`), submitted with `jarvis ppl submit <file>`. The generated sbatch
runs `pre_cmds` (conda + spack activation), builds the hostfile from
`$SLURM_JOB_NODELIST`, then runs `jarvis ppl run yaml <file>` so the whole
45-run sweep executes inside one allocation.

**Why.** This matches the convention the current repo's **CTE** pipelines
already use (`pipelines/ares/clio_cte_bench_baremetal_sbatch_2n.yaml`, the
`ior/*_sbatch_*.yaml`, `xnode_bdev_bench_4n.yaml`), so Mofka and CTE are
driven the same way. The archived Mofka work used "Pattern A" (standalone
`.sbatch` + `run_bench.sh`); that remains a documented fallback (see below)
but is not the primary path.

**Mofka-vs-CTE note.** Both now submit via the same jarvis scheduler
mechanism, so allocation shape, env capture, and hostfile construction are
identical across the two benchmarks — differences in results are attributable
to the systems, not the launchers.

## 2026-06-08 — Jarvis MUST be the `dev` branch (scheduler support is dev-only)

**Decision / constraint.** Ares must run `jarvis-cd` from the **`dev`** branch
of `grc-iit/jarvis-cd`. `jarvis ppl submit` + `scheduler:` parsing
(`jarvis_cd/core/scheduler.py`, `PipelineTest.submit()`) exist **only on
`dev`** — not on `main`, and not on the pinned `6a580b8` that Ares and the
devcontainer both had installed. Confirmed by reading all remote branches:
only `origin/dev` mentions scheduler/submit/sbatch.

**Why it matters for CTE too.** The repo's CTE scheduler-block pipelines have
the same requirement; before this work Ares had a jarvis that silently could
not run them. Standardizing on `dev` unblocks both.

**Reproducibility.** Record the exact `dev` commit used
(`git -C ~/jarvis-cd log --oneline -1`) alongside any published results.

**Incident (2026-06-08).** jarvis-cd had been editable-installed from
`clio-core-fork/external/jarvis-cd`; updating the fork removed that path and
broke `import jarvis_cd` while `pip show` still reported it. Reinstalled from a
standalone `~/jarvis-cd` checkout. Lesson: do not vendor jarvis-cd inside the
benchmarked repo as an editable install.

## 2026-06-08 — Scope: single-node + 2-node TCP only; 4-node documented, not shipped

**Decision.** Ship validated **1-node** and **2-node** TCP sweeps. Do **not**
ship a 4-node sweep.

**Why.** The archived multi-node analysis found 4n TCP failed `status != success`
on 45/45 rows: an `NA_TIMEOUT` inside `mofka-server ProviderImpl::requestData`
during bulk transfer, with 3 client nodes concurrently pulling from one
bedrock server. It persisted after raising Margo/Mercury timeouts to 60 s, so
it is **structural single-server fan-in saturation**, not a timeout-tuning
issue. 2n (one client) never trips it. RDMA (which might raise the ceiling) is
out of scope.

**Mofka-vs-CTE note.** This is the crux of any scaling comparison: Mofka funnels
all client traffic into **one central bedrock broker** (one `warabi` data
provider), so it saturates at the server. CTE is **symmetric** — every node
runs its own runtime and serves its own local data — so CTE scales differently
by construction. A future fix for Mofka 4n would shard data across multiple
warabi providers/partitions or multiple bedrock servers, or move to RDMA.

## 2026-06-08 — Producer workaround: `use_progress_thread=False`

**Decision.** `scripts/producer.py` constructs `MofkaDriver(..., use_progress_thread=False)`.

**Why.** With the progress thread enabled, the C++ `mofka::Data` destructor
calls `_Py_Dealloc` on the wrapped Python `bytes` payload from an Argobots
worker thread **without holding the GIL**, racing the main push loop and
SIGSEGV-ing (exit 139), worst at small payloads. Disabling the progress thread
moves RPC progress onto the GIL-holding main thread and eliminates the race
(archived single-node producer throughput rose ~316 → ~17,800 evt/s once the
crash path was gone).

**Caveat for comparison.** This is a **benchmark workaround**, not the upstream
fix (which belongs in the mofka Python binding: a `PyGILState_Ensure/Release`
guard in the destructor). Consequences: (1) production Python producers running
the default `use_progress_thread=True` remain exposed; (2) the producer
throughput here is an upper bound for the no-overlap case — a correctly patched
binding with the progress thread on would land lower. Cite Mofka producer
numbers with this caveat. The consumer keeps `use_progress_thread=True` (its
no-callback path is unaffected).

## 2026-06-09 — Topic/partition setup uses the MofkaDriver API, not `mofkactl`

**Decision.** `mofka_server` creates the topic and adds the memory partition via
the `MofkaDriver` Python API (`scripts/setup_topic.py`: `create_topic` +
`add_memory_partition`, guarded by `topic_exists`), **not** the `mofkactl` CLI
the archived server shelled out to.

**Why.** On the current mochi build (built fresh on Ares 2026-06-09), `mofkactl`
is broken by a typer/Click version conflict: Click 8.2.0 changed
`Parameter.make_metavar()` to require a `ctx` argument, but the bundled `typer`
calls it the old way, so any mofkactl invocation that renders options crashes
with `TypeError: Parameter.make_metavar() missing 1 required positional
argument: 'ctx'`. The `mochi.mofka.client` bindings are unaffected, so routing
topic/partition creation through `MofkaDriver` sidesteps the broken CLI layer
entirely and removes a fragile cross-dependency. Chosen over pinning
`click<8.2` in the spack env because the fix lives in code we own and doesn't
require rebuilding the env.

**Mofka-vs-CTE note.** Pure plumbing — does not affect measured throughput.
Worth recording because it reflects real friction in the mochi Python tooling
on current Spack (package rename `mochi-mofka`→`mofka`, `py-mochi-mofka` folded
into `mofka +python`, and this mofkactl/typer breakage) that a CTE comparison
write-up should mention when describing how much harder Mofka was to stand up.

## 2026-06-08 — Bedrock provider topology: flock + yokan (map) + warabi (memory)

**Decision.** `mofka_server/config/config.json` runs one bedrock daemon with a
flock group manager, yokan `master` + `metadata` providers (in-memory `map`),
and a single warabi `data` provider with a `memory` target.

**Mofka-vs-CTE note.** This is an **in-memory, non-persistent, single-data-
provider** broker. It is roughly comparable to a CTE configuration with a RAM
bdev tier, but with a fundamentally different data path: CTE clients issue
PutBlob/GetBlob into a local runtime + bdev, whereas Mofka clients push/pull
events through the central broker's warabi target over Mercury RPC. No disk
persistence and no compression are modeled on either side here. The single
warabi provider is the 4n bottleneck (above).

## 2026-06-08 — Head-node exclusion lives in `mofka_bench` (idempotent server-host filter)

**Decision.** Under the scheduler block the hostfile contains **all** allocated
nodes. `mofka_bench._client_hostfile()` removes `MOFKA_SERVER_HOST` (published
by `mofka_server`, compared on short hostnames) so producers/consumers run only
on the N-1 client nodes; bedrock owns the head node alone. The filter is
**idempotent** — a no-op if the server host is already absent (e.g. a Pattern-A
pre-stripped hostfile), so the same package works under either launch pattern.
`_is_multinode()` additionally requires `len(hosts) > 1`, so a 1-node Slurm
allocation runs locally (server + client co-located), matching the archived
single-node flow.

**Mofka-vs-CTE note.** Mofka deliberately **dedicates** the head node to the
broker and runs clients elsewhere (1 server + N-1 clients). CTE runs a runtime
on **every** node (symmetric, N servers + N clients). When comparing
"N-node" results, remember Mofka's effective client count is N-1 and all of it
funnels to one server, while CTE's is N with local service.

**Known limitation.** If a NIC `suffix:` (e.g. `-40g`) is added to the
scheduler block, hostfile entries won't short-name-match `socket.gethostname()`
and the head won't be excluded. Not an issue for the shipped plain-TCP YAMLs
(no suffix); reconcile suffix + exclusion before using a secondary NIC.

## 2026-06-08 — Loud-failure detection retained (no silent empty CSV cells)

**Decision.** Keep the archived `_run_role` exit-code propagation +
`_check_output_freshness` (requires the `RESULTS` header both scripts print on
success). A crash that leaves a banner-only output raises `RuntimeError`, so the
row is marked `status=failed` instead of silently emitting empty stat columns.

**Why / Mofka-vs-CTE note.** The original Mofka sweep reported 45/45
`status=success` while 11 rows had empty producer columns — a false-positive
that would have corrupted any Mofka-vs-CTE comparison. "Fully populated with
logical data" is the completion bar precisely to avoid this.

## 2026-06-08 — Metrics & CSV schema

**Decision.** `_parse_output` emits, per role, columns
`<pkg_id>.{producer,consumer}.{throughput_mbps,events_per_sec,elapsed_ms,
total_data_mb,events_count}`. Multi-node folds across client hosts: **SUM**
throughput/events/data, **MAX** elapsed (wall time ends with the slowest host).
The sweep CSV (jarvis `PipelineTest`) adds `run_idx,combination_idx,repeat_idx,
status,runtime,<vars...>,error`.

**Mofka-vs-CTE note.** Mofka's headline metric is **events/sec** (and derived
MB/s from `data_size + metadata_size`); CTE's is IO MB/s / IOPs at a given
blob/IO size. Align comparisons on payload size (`mofka_bench.data_size` vs the
CTE `io_size`) and on aggregate vs per-node throughput (Mofka multi-node sums
client throughput; account for the single-server ceiling).

---

## Retained fallback — Pattern A (only if `dev` proves unusable on Ares)

If the `dev` scheduler path is unstable on Ares, fall back to the archived flow
**without changing the packages** (they are launch-pattern agnostic):
standalone `.sbatch` scripts that conda/spack-activate, build a **client-only**
hostfile (`scontrol show hostnames | grep -vx "$HEAD_NODE"`), `jarvis hostfile
set` it, and call a `run_bench.sh` wrapper around `jarvis ppl run yaml` against
**`config:`-wrapper sweep YAMLs without** a `scheduler:` block. The in-package
head-exclusion is idempotent, so it remains correct against the pre-stripped
hostfile. This fallback works with baseline jarvis (no scheduler support).
