# Single-Node vs Multi-Node CTE Benchmarks on Ares

This guide explains how to run the CTE timed benchmark in either single-node or
multi-node configurations on Illinois Tech's **Ares** cluster, and how to swap
between them. It assumes you have already done the one-time setup (cloned the
fork onto Ares, installed the iowarp conda environment, run `pip install -e
external/jarvis-cd`, and `jarvis repo add jarvis_iowarp`).

## Why two modes?

Single-node mode characterizes raw CTE put/get performance with the
lowest-overhead transport (shared memory) on one host. Multi-node mode adds
cross-node coordination and uses TCP between the runtime instances, so it
captures distributed throughput and the network cost of fan-out. Side-by-side
they tell you what scales and what gets paid as overhead.

## What changes between single-node and multi-node

| Layer | Single-node | Multi-node | Why |
|---|---|---|---|
| `wrp_runtime.ipc_mode` (in pipeline YAML) | `"shm"` | `"tcp"` | `shm` is in-process only; cross-node clients/servers must go over the network. |
| `wrp_cte_bench_timed.nprocs` | `1` | `nodes × ppn` | Drives how many parallel benchmark clients launch. `> 1` triggers the `PsshExecInfo` (parallel-SSH) path. |
| `wrp_cte_bench_timed.ppn` | `1` | typically `1` for now | Processes per node. CTE is async; more procs ≠ more throughput beyond a point. Tune later. |
| Jarvis hostfile | not needed | required, lists every compute node | Without it, `wrp_runtime.start()`, `wrp_cte.start()`, and the benchmark all stay local instead of fanning out. |
| Slurm `--nodes` (in sbatch) | `1` | `N` | Allocates the compute fleet that the hostfile points at. |
| `wrp_cte.devices` paths | `/mnt/nvme/$USER/storage1.bin` | same | Each Ares node provisions its own `/mnt/nvme/$USER`. The path is identical per-node; jarvis `Mkdir`s it on each node via PSSH. |

What **doesn't** change: the benchmark code, results format, the timestamping
wrapper, the sbatch launch model, and the `output:` directory (NFS — visible
from all nodes after the run).

## How to switch

### Single → Multi-node

```bash
sbatch run_ares_cte_multinode.sbatch
```

That's it from the user's perspective. The script:
1. Allocates 4 compute nodes (edit `#SBATCH --nodes=N` to change).
2. Generates a per-allocation Jarvis hostfile from `$SLURM_JOB_NODELIST` and
   registers it via `jarvis hostfile set`.
3. Calls `./run_bench.sh` with the multi-node pipeline YAML, which produces
   timestamped `results_<TS>.csv` / `results_<TS>.yaml` in
   `results/cte_bench_timed_ares_multinode_results/`.
4. Cleans up the per-job hostfile on exit.

If you change `--nodes` in the sbatch, also update `nprocs` in the pipeline
YAML so it equals `nodes × ppn`. They must match — if `nprocs < nodes`, some
nodes sit idle; if `nprocs > nodes × ppn`, jarvis will pack extra procs onto
hosts and you lose the clean per-node mapping.

### Multi-node → Single-node

```bash
sbatch run_ares_cte.sbatch
```

No hostfile, no `nprocs` calculation. The single-node sbatch allocates one
node, runs the `shm`-mode pipeline, and writes results to
`results/cte_bench_timed_ares_results/`.

## Files involved

| File | Mode | Purpose |
|---|---|---|
| `jarvis_iowarp/pipelines/performance/microbench_cte_timed_ares.yaml` | single | Pipeline: `ipc_mode: shm`, `nprocs: 1`. |
| `jarvis_iowarp/pipelines/performance/microbench_cte_timed_ares_multinode.yaml` | multi | Pipeline: `ipc_mode: tcp`, `nprocs: 4`, `ppn: 1`. |
| `run_ares_cte.sbatch` | single | Slurm driver for single node. |
| `run_ares_cte_multinode.sbatch` | multi | Slurm driver: allocates N nodes, builds hostfile, runs the multi-node pipeline. |
| `run_bench.sh` | both | Shared wrapper that timestamps `results.{csv,yaml}`. |

## Things to remember

- **Hostfile is per-allocation.** The compute nodes Slurm hands you are not
  fixed; the multi-node sbatch regenerates the hostfile from
  `$SLURM_JOB_NODELIST` each run. Don't rely on a stale `~/hostfile.txt`.
- **`ipc_mode` mismatch is silent.** Setting `shm` while running multi-node
  doesn't error — it just keeps everything local on the head node and your
  throughput numbers will look wrong. If multi-node aggregate throughput is
  *lower* than single-node, double-check `ipc_mode: "tcp"`.
- **`nprocs` must equal `nodes × ppn`.** Mismatch leads to either idle nodes
  or hosts running more procs than intended. Treat it as a hard contract.
- **Results dirs are split.** Single-node writes to
  `cte_bench_timed_ares_results/` and multi-node to
  `cte_bench_timed_ares_multinode_results/`. Don't merge them — they aren't
  directly comparable per-row, only in aggregate.
- **`-40g` hostname suffix exists for the Mellanox NIC** (40Gbps RoCE). The
  default hostfile uses the standard hostnames (1Gbps Ethernet). Routing
  benchmark traffic over the Mellanox NIC is the next milestone (see below).

## Sanity checks after a multi-node run

Open the timestamped CSV in
`results/cte_bench_timed_ares_multinode_results/`. Expectations vs the
single-node baseline:

- **Aggregate throughput** at large I/O sizes (`256k`, `1m`) should be higher
  than single-node — more procs putting/getting in parallel.
- **Per-thread latency** at small sizes (`4k`, `16k`) should be *worse* than
  single-node `shm` — TCP adds a round-trip cost.
- If aggregate is *lower* than single-node, that's a red flag. Likely
  suspects: TCP saturation on the 1Gbps interface, contention on a shared NIC,
  or a misconfigured `nprocs`/hostfile. Re-check the sbatch log for the
  "Allocated nodes" block and the runtime/CTE "started successfully on all
  nodes" lines.

Also confirm in the sbatch log that you see `PsshExecInfo` lines (multi-node
fan-out) rather than `LocalExecInfo` (which would mean the hostfile didn't
take effect).

## Future extensions (out of scope here)

- **RDMA / Mellanox 40g routing.** Suffixing each hostname in the hostfile
  with `-40g` should route traffic over the Mellanox NIC. The wrp_runtime
  currently exposes only `shm`/`ipc`/`tcp`; a true RDMA transport
  (`ofi+verbs`-style) would need code changes.
- **Mofka multi-node.** Different transport stack (Mofka has explicit
  `protocol: ofi+verbs` support). Related in spirit but a separate plan.
- **Sweep tuning.** Choosing `nprocs`/`ppn` ratios that actually saturate
  the network, varying node counts (4 → 8 → 16) to plot scaling curves.
