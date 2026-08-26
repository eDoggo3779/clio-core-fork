/*
 * Copyright (c) 2024, Gnosis Research Center, Illinois Institute of Technology
 * All rights reserved. BSD 3-Clause license.
 */

/**
 * Collective latency comparison: Clio PoolQuery::AllToOne vs MPI.
 *
 * Four arms, all run by the same ranks over the same 4-node Docker cluster and
 * the same TCP network, so the numbers differ only in the collective machinery:
 *
 *   mpi_barrier      MPI_Barrier                       -- the reference barrier
 *   mpi_allreduce    MPI_Allreduce(1 x u64, MPI_SUM)   -- the reference reduce
 *   clio_barrier     MOD_NAME BarrierTask, AllToOne    -- our barrier
 *   clio_allreduce   MOD_NAME AllReduceTask, AllToOne  -- our reduce
 *
 * One rank per physical node (see mpi_hostfile), each attached as a client to
 * its OWN local clio daemon (CLIO_WITH_RUNTIME=0). MPI is used for the two MPI
 * arms and, in the clio arms, ONLY to align the start of a measurement phase
 * and to reduce the per-rank statistics at the end -- never inside a timed clio
 * region, so no MPI cost leaks into the clio numbers.
 *
 * Why AllToOne is the right analogue of an allreduce: routed AllToOne, a task
 * parks at the neighborhood leader until a task from EVERY container in the
 * pool has arrived (the pool has one container per node), at which point the
 * batch is folded into a single aggregate via AggregateIn, that aggregate runs
 * once, and its OUT is broadcast 1->N back to every participant. All
 * contribute, all block until the last one has, and all observe the same
 * combined result -- the defining properties of MPI_Allreduce. The barrier arm
 * is the same path with an empty task, so the difference between the two clio
 * arms isolates the cost of the reduction from the cost of the synchronization.
 *
 * The clio_allreduce arm also self-checks: every iteration's contribution is
 * keyed to the iteration number, so a batch that mixed two iterations, dropped
 * a member, or double-counted one would produce a sum that does not match the
 * closed form and is reported as a mismatch. A benchmark of a collective that
 * silently failed to be collective would just be measuring a fast no-op.
 *
 * Reported per arm: mean, p50, p99 and max of per-iteration latency. Each rank
 * computes its own statistics over its own samples; the printed value is the
 * MAX across ranks (the standard way to report a collective -- the operation is
 * not complete until the slowest participant returns).
 *
 * Env: COLL_BENCH_ITERS (default 1000), COLL_BENCH_WARMUP (default 100),
 *      COLL_BENCH_CSV (optional path for a machine-readable dump).
 * Exit code 0 == every arm ran and the allreduce self-check passed.
 */
#include <mpi.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <clio_runtime/clio_runtime.h>
#include <clio_runtime/pool_query.h>
#include <clio_runtime/MOD_NAME/MOD_NAME_client.h>
#include <clio_runtime/MOD_NAME/MOD_NAME_tasks.h>

namespace {

using Clock = std::chrono::steady_clock;

/** Pool used by the clio arms. Fixed so every rank names the same pool. */
constexpr clio::run::u32 kBenchPoolMajor = 9100;
/** Collective identity shared by all participants of a given arm. */
constexpr clio::run::u32 kContainerHash = 0;
constexpr clio::run::u64 kBarrierBatchKey = 1;
constexpr clio::run::u64 kAllReduceBatchKey = 2;

int EnvInt(const char *name, int def) {
  const char *e = std::getenv(name);
  if (e == nullptr || *e == '\0') return def;
  int v = std::atoi(e);
  // Accept 0 (a legitimate warmup count); only a malformed/negative value
  // falls back to the default.
  return v >= 0 ? v : def;
}

void Log(int rank, const std::string &msg) {
  std::fprintf(stderr, "[coll-bench rank%d] %s\n", rank, msg.c_str());
  std::fflush(stderr);
}

/** Per-rank latency statistics for one arm, in microseconds. */
struct Stats {
  double mean_us = 0.0;
  double p50_us = 0.0;
  double p99_us = 0.0;
  double max_us = 0.0;
};

/** Summarize a rank's per-iteration samples. Consumes (sorts) the vector. */
Stats Summarize(std::vector<double> &samples_us) {
  Stats s;
  if (samples_us.empty()) return s;
  double total = 0.0;
  for (double v : samples_us) total += v;
  s.mean_us = total / static_cast<double>(samples_us.size());
  std::sort(samples_us.begin(), samples_us.end());
  const size_t n = samples_us.size();
  // Nearest-rank percentiles; n>=1 so both indices are in range.
  s.p50_us = samples_us[(n * 50) / 100 < n ? (n * 50) / 100 : n - 1];
  s.p99_us = samples_us[(n * 99) / 100 < n ? (n * 99) / 100 : n - 1];
  s.max_us = samples_us[n - 1];
  return s;
}

/**
 * Reduce per-rank statistics to rank 0 by MAX. A collective's latency is the
 * slowest participant's latency, so max (not mean) is the honest summary.
 */
Stats ReduceMax(const Stats &local) {
  double in[4] = {local.mean_us, local.p50_us, local.p99_us, local.max_us};
  double out[4] = {0, 0, 0, 0};
  MPI_Reduce(in, out, 4, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  Stats s;
  s.mean_us = out[0];
  s.p50_us = out[1];
  s.p99_us = out[2];
  s.max_us = out[3];
  return s;
}

/**
 * Run one arm: `warmup` untimed iterations, an alignment barrier, then `iters`
 * timed iterations. `op` receives the iteration index and returns false if that
 * iteration failed a correctness check.
 *
 * The alignment barrier is outside the timed region; nothing inside it touches
 * MPI, so an arm measures only the collective under test.
 */
template <typename Op>
Stats RunArm(int warmup, int iters, Op &&op, int *failures) {
  for (int i = 0; i < warmup; ++i) {
    if (!op(-1 - i)) ++(*failures);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  std::vector<double> samples;
  samples.reserve(static_cast<size_t>(iters));
  for (int i = 0; i < iters; ++i) {
    auto t0 = Clock::now();
    const bool ok = op(i);
    auto t1 = Clock::now();
    if (!ok) ++(*failures);
    samples.push_back(
        std::chrono::duration<double, std::micro>(t1 - t0).count());
  }
  return Summarize(samples);
}

/** Expected allreduce total for iteration `iter` over `size` ranks. */
std::uint64_t ExpectedSum(int iter, int size) {
  // Contribution of rank r at iteration i is (i+1)*1000 + (r+1), so the total
  // encodes BOTH the iteration and the full membership. A batch that mixed
  // iterations or lost a member cannot match this by accident.
  const std::uint64_t base =
      static_cast<std::uint64_t>(iter + 1) * 1000ull * static_cast<std::uint64_t>(size);
  const std::uint64_t members =
      static_cast<std::uint64_t>(size) * static_cast<std::uint64_t>(size + 1) / 2ull;
  return base + members;
}

std::uint64_t Contribution(int iter, int rank) {
  return static_cast<std::uint64_t>(iter + 1) * 1000ull +
         static_cast<std::uint64_t>(rank + 1);
}

void PrintHeader(int size, int iters, int warmup) {
  std::printf("\n");
  std::printf("=== Collective latency: Clio PoolQuery::AllToOne vs MPI ===\n");
  std::printf("ranks (1 per node): %d   iterations: %d   warmup: %d\n", size,
              iters, warmup);
  std::printf("latency in microseconds, max across ranks\n\n");
  std::printf("%-16s %12s %12s %12s %12s\n", "arm", "mean", "p50", "p99",
              "max");
  std::printf("%-16s %12s %12s %12s %12s\n", "----------------", "-----------",
              "-----------", "-----------", "-----------");
}

void PrintRow(const char *name, const Stats &s) {
  std::printf("%-16s %12.2f %12.2f %12.2f %12.2f\n", name, s.mean_us, s.p50_us,
              s.p99_us, s.max_us);
}

void PrintRatio(const char *label, const Stats &ours, const Stats &theirs) {
  if (theirs.mean_us <= 0.0) {
    std::printf("%s: n/a (reference measured 0)\n", label);
    return;
  }
  std::printf("%s: %.1fx  (%.2f us vs %.2f us mean)\n", label,
              ours.mean_us / theirs.mean_us, ours.mean_us, theirs.mean_us);
}

void WriteCsv(const char *path, int size, int iters, const Stats &mpi_bar,
              const Stats &mpi_ar, const Stats &clio_bar,
              const Stats &clio_ar) {
  std::FILE *f = std::fopen(path, "w");
  if (f == nullptr) {
    std::fprintf(stderr, "warning: could not open CSV path %s\n", path);
    return;
  }
  std::fprintf(f, "arm,ranks,iters,mean_us,p50_us,p99_us,max_us\n");
  const struct {
    const char *name;
    const Stats *s;
  } rows[] = {{"mpi_barrier", &mpi_bar},
              {"mpi_allreduce", &mpi_ar},
              {"clio_barrier", &clio_bar},
              {"clio_allreduce", &clio_ar}};
  for (const auto &r : rows) {
    std::fprintf(f, "%s,%d,%d,%.3f,%.3f,%.3f,%.3f\n", r.name, size, iters,
                 r.s->mean_us, r.s->p50_us, r.s->p99_us, r.s->max_us);
  }
  std::fclose(f);
  std::printf("\nCSV written to %s\n", path);
}

}  // namespace

int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);
  int rank = 0;
  int size = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  const int iters = EnvInt("COLL_BENCH_ITERS", 1000);
  const int warmup = EnvInt("COLL_BENCH_WARMUP", 100);
  int failures = 0;
  // Per-PHASE progress (never per-iteration): with four ranks blocking on each
  // other, a hang is only diagnosable if each rank says where it stopped.
  Log(rank, "start: " + std::to_string(size) + " ranks, iters=" +
                std::to_string(iters));

  // ---- MPI arms ------------------------------------------------------------
  // Run first: they need no runtime, so if the clio side cannot come up we
  // still have the reference numbers in the log.
  Stats mpi_barrier = RunArm(warmup, iters, [](int) {
    MPI_Barrier(MPI_COMM_WORLD);
    return true;
  }, &failures);

  Log(rank, "mpi_barrier done");

  Stats mpi_allreduce = RunArm(warmup, iters, [rank, size](int iter) {
    std::uint64_t in = Contribution(iter, rank);
    std::uint64_t out = 0;
    MPI_Allreduce(&in, &out, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    // Warmup iterations use negative indices; only check the timed ones, whose
    // expected value is the same closed form the clio arm is held to.
    return iter < 0 || out == ExpectedSum(iter, size);
  }, &failures);

  Log(rank, "mpi_allreduce done");

  // ---- Attach to the local daemon -----------------------------------------
  setenv("CLIO_WITH_RUNTIME", "0", 1);
  if (!clio::run::CLIO_INIT(clio::run::RuntimeMode::kClient, false)) {
    Log(rank, "FAIL: CLIO_INIT(kClient) failed");
    MPI_Abort(MPI_COMM_WORLD, 2);
    return 2;
  }

  // Rank 0 creates the pool; it is created across the whole cluster with one
  // container per node, which is exactly the membership the AllToOne barrier
  // counts. Every other rank waits for that to land before attaching.
  Log(rank, "attached to local daemon");

  const clio::run::PoolId pool_id(kBenchPoolMajor, 0);
  int create_rc = 0;
  if (rank == 0) {
    clio::run::MOD_NAME::Client creator(pool_id);
    auto create = creator.AsyncCreate(clio::run::PoolQuery::Dynamic(),
                                      "collective_bench_pool", pool_id);
    create.Wait();
    create_rc = static_cast<int>(create->return_code_);
    if (create_rc != 0) {
      Log(0, "FAIL: pool create rc=" + std::to_string(create_rc));
    }
  }
  if (rank == 0) Log(0, "pool create returned");
  MPI_Bcast(&create_rc, 1, MPI_INT, 0, MPI_COMM_WORLD);
  if (create_rc != 0) {
    MPI_Finalize();
    return 3;
  }
  // Let the new pool's metadata settle on every node before any rank routes to
  // it; the barrier alone only orders rank 0's create, not its propagation.
  std::this_thread::sleep_for(std::chrono::seconds(2));
  MPI_Barrier(MPI_COMM_WORLD);

  clio::run::MOD_NAME::Client client(pool_id);
  Log(rank, "pool ready; starting clio_barrier arm");

  // ---- Clio arms -----------------------------------------------------------
  // Every participant uses the same (container_hash, batch_key), which is what
  // makes their tasks one collective. A rank never has more than one request
  // outstanding (it waits before issuing the next), so a group can never
  // accumulate more members than the pool has containers and successive
  // iterations cannot merge into one batch.
  Stats clio_barrier = RunArm(warmup, iters, [&client](int) {
    auto q = clio::run::PoolQuery::AllToOne(kContainerHash, kBarrierBatchKey);
    auto f = client.AsyncBarrier(q);
    f.Wait();
    return f->return_code_ == 0;
  }, &failures);

  Log(rank, "clio_barrier done; starting clio_allreduce arm");

  int mismatches = 0;
  Stats clio_allreduce = RunArm(warmup, iters,
                                [&client, rank, size, &mismatches](int iter) {
    auto q = clio::run::PoolQuery::AllToOne(kContainerHash, kAllReduceBatchKey);
    auto f = client.AsyncAllReduce(q, Contribution(iter, rank));
    f.Wait();
    if (f->return_code_ != 0) return false;
    if (iter >= 0 && f->sum_ != ExpectedSum(iter, size)) {
      if (mismatches < 5) {
        Log(rank, "allreduce mismatch at iter " + std::to_string(iter) +
                      ": got " + std::to_string(f->sum_) + " expected " +
                      std::to_string(ExpectedSum(iter, size)));
      }
      ++mismatches;
      return false;
    }
    return true;
  }, &failures);

  Log(rank, "clio_allreduce done");

  // ---- Report --------------------------------------------------------------
  Stats r_mpi_barrier = ReduceMax(mpi_barrier);
  Stats r_mpi_allreduce = ReduceMax(mpi_allreduce);
  Stats r_clio_barrier = ReduceMax(clio_barrier);
  Stats r_clio_allreduce = ReduceMax(clio_allreduce);

  int total_failures = 0;
  MPI_Reduce(&failures, &total_failures, 1, MPI_INT, MPI_SUM, 0,
             MPI_COMM_WORLD);
  int total_mismatches = 0;
  MPI_Reduce(&mismatches, &total_mismatches, 1, MPI_INT, MPI_SUM, 0,
             MPI_COMM_WORLD);

  int exit_code = 0;
  if (rank == 0) {
    PrintHeader(size, iters, warmup);
    PrintRow("mpi_barrier", r_mpi_barrier);
    PrintRow("mpi_allreduce", r_mpi_allreduce);
    PrintRow("clio_barrier", r_clio_barrier);
    PrintRow("clio_allreduce", r_clio_allreduce);
    std::printf("\n");
    PrintRatio("clio_barrier   / mpi_barrier  ", r_clio_barrier,
               r_mpi_barrier);
    PrintRatio("clio_allreduce / mpi_allreduce", r_clio_allreduce,
               r_mpi_allreduce);
    std::printf("\n");
    if (total_mismatches > 0) {
      std::printf("FAIL: %d allreduce result mismatches -- the collective did "
                  "not combine correctly, so its timings are not meaningful\n",
                  total_mismatches);
      exit_code = 4;
    } else if (total_failures > 0) {
      std::printf("FAIL: %d failed iterations\n", total_failures);
      exit_code = 5;
    } else {
      std::printf("OK: all arms completed; allreduce results verified\n");
    }
    const char *csv = std::getenv("COLL_BENCH_CSV");
    if (csv != nullptr && *csv != '\0') {
      WriteCsv(csv, size, iters, r_mpi_barrier, r_mpi_allreduce,
               r_clio_barrier, r_clio_allreduce);
    }
    std::fflush(stdout);
  }

  MPI_Bcast(&exit_code, 1, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Finalize();
  return exit_code;
}
