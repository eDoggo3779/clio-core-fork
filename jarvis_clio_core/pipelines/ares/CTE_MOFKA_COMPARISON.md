# CTE ↔ Mofka benchmark comparison — design decisions

How the CLIO Core (CTE) sweeps in this directory (`cte_sweep_1n.yaml`,
`cte_sweep_2n.yaml`, launched by `../../scripts/run_cte_sweep_{1n,2n}_ares.sbatch`)
are made comparable to the Mofka sweeps in `../mofka/`, and how to read the two
side by side. Companion to `../mofka/architecture_decisions.md` (the Mofka side
of the same comparison). Entries dated **2026-06-19**.

The Mofka sweeps were validated on Ares (TCP 1n, TCP 2n, RDMA 1n, RDMA 2n —
45/45 each; RDMA-2n ~1.6–2× TCP-2n). These CTE sweeps are their counterpart so a
CTE-vs-Mofka write-up has matched axes.

> **Where the Mofka files live.** The Mofka sweep YAMLs/sbatch and the
> `mofka_bench` package live on the **`mofka-benchmarking`** branch (under
> `jarvis_clio_core/pipelines/mofka/`). On this `cte-benchmarking` branch
> `pipelines/mofka/` carries only `OPERATOR_PROTOCOL.md`, so the `../mofka/`
> references below resolve against that branch (or a future merge). This doc is
> self-contained for interpreting the CTE results without them.

---

## Matched knob grid (45 runs, identical to Mofka)

**Decision.** Both CTE sweeps use the EXACT Mofka grid:
- payload size `cte_bench.io_size` = `[256, 1024, 4096, 16384, 65536]` bytes,
  expressed as raw bytes so the column literally equals `mofka_bench.data_size`.
- threads `cte_bench.num_threads` = `[1, 2, 4]` == `mofka_bench.num_threads`.
- `repeat: 3`. ⇒ 5 × 3 × 3 = **45 runs**, the same shape as every Mofka sweep.

**Why.** Row-for-row comparability. `architecture_decisions.md` (Mofka) says to
"align comparisons on payload size (`mofka_bench.data_size` vs the CTE
`io_size`)"; using identical lists makes the join trivial. CTE's headline metric
is Put MB/s & IOPS at a payload size; Mofka's is events/sec & derived MB/s from
`data_size + metadata_size` — compare at matched payload size and on aggregate
throughput.

## Transport: TCP only (no RDMA twin)

**Decision.** `ipc_mode: tcp` on the runtime; no RDMA variant.

**Why.** CTE has no RDMA transport (`AGENTS.md`: "RDMA … Mofka supports it, CTE
does not yet"). TCP is the CTE analog of Mofka's `protocol: "tcp"`: the bench
client serializes each PutBlob over a ZeroMQ TCP socket to its **local** daemon
(`CLIO_SERVER_ADDR` defaults to `127.0.0.1`) instead of attaching shared memory.
So the comparison covers {CTE-TCP-1n, CTE-TCP-2n} vs {Mofka-TCP-1n,
Mofka-TCP-2n}; Mofka's two RDMA sweeps have no CTE counterpart and are simply
out of scope here. `ipc_mode` is set on the `clio_runtime` package and
propagates to the bench via the jarvis pipeline env (`CLIO_IPC_MODE=TCP`), the
same mechanism the existing CTE pipelines rely on.

(Note: the pre-existing JuiceFS-comparable CTE sweep — the other
`cte_sweep_ares_headless_prompt.md` — used **shm** transport and a different
grid {4k,16k,64k,128k}×{1,2,4,8}. That is a separate comparison; these files are
TCP + the Mofka grid and live alongside it without collision via distinct
`output:` dirs.)

## Topology: symmetric 2n — the load-bearing comparison caveat

**Decision.** CTE-2n is **symmetric**: every node runs its own runtime + CTE
pool + RAM tier and serves its own data. The runtime PSSH-launches a daemon on
both nodes; `clio_cte` PSSH-composes the pool on both (`pool_query: local`,
`neighborhood: 1`); `clio_cte_bench` runs `nprocs: 2 / ppn: 1` → one Put rank per
node, each issuing `query_type: local` PutBlobs to ITS OWN tier over TCP
loopback. Aggregate 2-node throughput is the SUM of two independent
local-service nodes.

**Why / how it differs from Mofka-2n.** Mofka-2n is **asymmetric**: 1 bedrock
broker on the head node + 1 producer/consumer client on the other node, all
client traffic funneling into the single warabi data provider. So "2 nodes"
means **2 local-serving CTE nodes** vs **(1 server + 1 client)** for Mofka —
CTE's effective client count is N, Mofka's is N-1, and Mofka's single-broker
fan-in is the structural ceiling (the reason Mofka 4n was never shipped) that
CTE's symmetric model does not have. This is the crux of any scaling comparison:
read CTE-2n as "2× the local-service capacity" and Mofka-2n as "one client's
throughput against one broker." We deliberately did NOT force CTE into a
funnel-to-one-node config (`query_type: direct0`) to mimic Mofka — that would
measure CTE in a non-native, more fragile cross-node path; the fair comparison
showcases each system's natural topology and annotates the difference.

## Workload magnitude: 60 s time-limited, constant 256 MiB working set

**Decision.** `test_case: Put`, `depth: 1`, `time_limit: 60`, and
`--max-total-blobs` set so the working set = **256 MiB per io_size**, held
constant (zip: 256→1048576, 1024→262144, 4096→65536, 16384→16384, 65536→4096).

**Why.**
- **Put** mirrors Mofka's data-producing path (push events → write blobs).
- **time_limit (not a fixed op count)** gives a stable throughput *rate* — Mofka
  ran a fixed `num_events: 1000`, which at 256 B is ~256 KB and finishes in
  milliseconds (too short/noisy for CTE). Throughput is a rate, so duration need
  not match for the rates to compare; 60 s mirrors the validated CTE sweep.
- **constant working set** isolates the thread-scaling effect (held fixed across
  thread counts at a given size), and 256 MiB keeps the small-payload key count
  bounded — 256 B → 1 Mi blobs (~150 MB of blob metadata), well within the
  runtime main segment and the 16 GiB RAM tier, at any thread count. (1 GiB, as
  the JuiceFS-comparable sweep used for its ≥4k payloads, would be 4.2 Mi blobs
  at 256 B — untested metadata pressure; 256 MiB is the conservative choice for
  the smaller Mofka payloads, and is also closer to Mofka's own ≤64 MiB
  per-run data volume.) `depth: 1` matches the validated CTE sweep and is the
  fair no-overlap analog of serial event push.

## Metrics & the multi-node fold (mirrors mofka_bench)

**Decision.** `clio_cte_bench._get_stat` writes `cte_bench.put.{metric}` columns
from the HLOG report (`bench_common.h::PrintResults`). For 2n, each rank writes a
per-host `bench_output.txt.<hostname>` and `_get_stat`/`_fold_host_stats` folds
across hosts: **SUM** the cumulative system totals (`agg_bw_mbps`,
`agg_ops_per_sec`, `total_data_mb`, `total_ops`), **MAX** the wall-clock
(`time_max_us`), **mean** the per-thread/latency descriptive stats. A single
host folds to itself, so the 1n path is unchanged.

**Why.** This is exactly `mofka_bench`'s fold (SUM throughput/events/data, MAX
elapsed), so the CTE-2n and Mofka-2n CSVs aggregate identically — the 2n row
carries the true 2-node aggregate, not one rank's numbers clobbered by a shared
file. Without per-host capture, two ranks redirecting to one NFS file would race
and corrupt the metrics. Expect CTE-2n aggregate ≈ 2× CTE-1n at the same
(io_size, threads); 2n ≈ 1n is a yellow flag (a rank didn't run).

## Launch: Pattern A (manual sbatch + in-process `jarvis ppl run yaml`)

**Decision.** Each sweep is launched by a standalone `.sbatch` that takes one
allocation (1n: 1 node; 2n: 2 nodes), activates conda, (2n) sets the jarvis
hostfile to all nodes, clears any stale `results.csv`, then runs the whole
45-run sweep in-process via `jarvis ppl run yaml`. No jarvis `scheduler:` block /
no `ppl submit`.

**Why.** Same Pattern A the Mofka launchers use (see
`../mofka/architecture_decisions.md`): the scheduler block runs one Slurm job per
iteration, which fails on Ares (compute nodes can't `sbatch`, node-local temp
YAML). One allocation for all 45 runs sidesteps both. The `rm -f results.csv`
guard matters because jarvis sweep-resume counts *failed* rows as completed and
skips them — a stale CSV would silently no-op a re-run.

## Scope

Ship validated **1-node** and **2-node** TCP CTE sweeps. No RDMA (CTE lacks it).
4-node is out of scope this round (matches the Mofka scope); CTE's symmetric
model has no single-broker fan-in ceiling, so 4n is a future scaling study, not
a blocker. Completion bar (per the headless prompt): both `results.csv` at 45/45
`status=success` with populated, logical metrics.
