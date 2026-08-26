# Collective latency: Clio `PoolQuery::AllToOne` vs MPI

A 4-node Docker benchmark that times our collectives head-to-head against the
implementations they are modelled on, so "how slow are we?" has a number
attached to it instead of an intuition.

## What it measures

Four arms, run by the same four ranks, over the same cluster and the same TCP
network, in one process each:

| arm              | what it is                                        |
|------------------|---------------------------------------------------|
| `mpi_barrier`    | `MPI_Barrier` — the reference barrier              |
| `mpi_allreduce`  | `MPI_Allreduce(1 × u64, MPI_SUM)` — the reference reduce |
| `clio_barrier`   | MOD_NAME `BarrierTask`, routed `PoolQuery::AllToOne` |
| `clio_allreduce` | MOD_NAME `AllReduceTask`, routed `PoolQuery::AllToOne` |

One rank per physical node (`mpi_hostfile`), each attached as a client to its
**own** local clio daemon (`CLIO_WITH_RUNTIME=0`). MPI is used for the two MPI
arms and, in the clio arms, only to align the start of a phase and to reduce
per-rank statistics at the end — never inside a timed clio region, so no MPI
cost leaks into the clio numbers.

**Why `AllToOne` is the allreduce analogue.** An `AllToOne` task parks at the
neighborhood leader until a task from every container in the pool has arrived
(the pool has one container per node); the batch is then folded into a single
aggregate via `AggregateIn`, that aggregate runs once, and its OUT is broadcast
1→N back to every participant. All contribute, all block until the last one
has, and all observe the same combined result — the defining properties of
`MPI_Allreduce`. The barrier arm is the same path with a payload-free task, so
the gap between the two clio arms isolates the cost of the reduction from the
cost of the synchronization.

**The allreduce arm self-checks.** Rank *r*'s contribution at iteration *i* is
`(i+1)*1000 + (r+1)`, so the expected total encodes both the iteration and the
full membership. A batch that mixed two iterations, dropped a member, or
double-counted one cannot match the closed form by accident, and the run exits
non-zero. Without that check the benchmark could happily time a collective that
had silently stopped being collective (which is exactly what it caught — see
below).

## Results

4 nodes, 1 rank each, Docker bridge network on one host, 1000 iterations after
100 warmup, Release build. Latency in microseconds, **max across ranks** (a
collective is not done until its slowest participant returns):

| arm              |    mean |     p50 |     p99 |     max |
|------------------|--------:|--------:|--------:|--------:|
| `mpi_barrier`    |   27.85 |   26.34 |   60.33 |   97.83 |
| `mpi_allreduce`  |   24.64 |   20.84 |   46.99 |   91.05 |
| `clio_barrier`   | 1096.05 | 1165.49 | 1747.42 | 2226.20 |
| `clio_allreduce` |  963.53 | 1143.89 | 1580.17 | 2095.91 |

- `clio_barrier` / `mpi_barrier`: **39.4×**
- `clio_allreduce` / `mpi_allreduce`: **39.1×**

A second run of the same configuration gave 37.7× and 41.3×, so treat the
ratio as "roughly 40×", not as three significant figures. Run-to-run spread on
the clio arms is a few percent at the mean and considerably more at p99.

Two things worth reading off this table:

1. **The reduction is free; the machinery is everything.** `clio_allreduce` and
   `clio_barrier` land within run-to-run noise of each other (the reduce came
   out marginally *faster* here), so summing four `u64`s costs nothing
   measurable. The whole ~1 ms is task routing, the forward to the
   leader, parking in the `BatchManager`, the flush poll, and the 1→N
   completion broadcast. Optimizing the aggregate is pointless; the round trip
   is the target.
2. **MPI's barrier and allreduce cost the same too** (~25 µs), and that is
   roughly one docker-bridge RTT times the tree depth. We are ~1 ms against
   ~25 µs, i.e. the gap is not a constant-factor inefficiency in one place but
   several full round trips plus a polling flush.

These are latency numbers on a single host's docker bridge; on real hardware
with a real fabric both columns move, and the ratio is the portable part.

## Running it

```bash
# Requires MPI and Docker CI:
cmake -S . -B build -DCLIO_CORE_ENABLE_MPI=ON -DCLIO_CORE_ENABLE_DOCKER_CI=ON ...
cmake --build build --target clio_run clio_collective_bench

# Full run (defaults: 1000 iterations, 100 warmup)
./run_tests.sh all

# Quick run
COLL_BENCH_ITERS=100 COLL_BENCH_WARMUP=10 ./run_tests.sh all

# Or via ctest (short: 200 iterations, so it works as a regression gate)
ctest -R cr_collective_bench_docker
```

Sub-commands `setup` / `run` / `clean` bring the cluster up, wait on it, and
tear it down separately — useful when iterating, since `all` always tears down.

Results are written to `results.csv` in this directory. Exit codes: `2`
CLIO_INIT, `3` pool create, `4` allreduce mismatch, `5` failed iterations.

## The bug this found

Cross-node collectives did not work at all, and failed *silently*. Two defects,
both fixed on this branch:

1. **`SendIn` overwrote the forwarded member's `pool_query_`** with the
   `Physical(leader)` envelope it was wrapped in
   (`context-runtime/src/ipc/ipc_run2run.cc`). That erased the collective
   routing mode *and* the `container_hash` / `batch_key` that decide which
   group a member joins, so a member forwarded from a non-leader node arrived
   at the leader looking like an ordinary task.
2. **`RouteTask`'s `IsRouted()` early-return preceded the collective check**
   (`context-runtime/src/ipc_manager.cc`). `RecvIn` marks every net-received
   task routed, so even with its query intact a forwarded member would have
   returned `ExecHere` instead of reaching the `BatchManager`.

The combined effect: a collective whose members did not all originate on the
leader node did not happen. Remote members each ran standalone and returned an
**un-combined result with `rc=0`** — an `AllReduce` handed every caller back its
own value — while any leader-local member waited forever for a count that could
never be reached. A hang and a silent wrong answer, depending on where you
stood.

The pre-existing `alltoone` integration test cannot see this: it issues all four
`AllToOne` requests from a *single* client on the leader node, so every member
is leader-local and neither defect is on the path. This benchmark is the first
thing in the tree that submits one member per node, which is both what MPI does
and what a collective is for.

A third fix accompanies them: `BatchManager::OnAggregateComplete` now restores
an owning `RunFuture` handle before `EndTask` on a remote member, because
completing one is an asynchronous `SendOut` and the batch's owning reference
drops as soon as the broadcast loop ends.

## Notes

- Node 1 is the mpirun launcher and sets up passwordless SSH; nodes 2-4 serve
  `sshd` and wait for node 1's "done" flag. A worker must not exit while a peer
  is still in a collective — its daemon would vanish mid-barrier and the run
  would hang rather than fail, which is the harder failure to read.
- The daemons log to `/tmp/clio_daemon.log` inside each container rather than
  stdout; at `info` level the periodic scheduler report buries the harness's own
  progress in `docker logs`.
- Containers carry `SYS_PTRACE` so a hung rank can be inspected with `gdb` via
  `docker exec` — which is how the bug above was found.
