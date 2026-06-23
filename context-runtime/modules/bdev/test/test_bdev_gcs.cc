/*
 * Copyright (c) 2024, Gnosis Research Center, Illinois Institute of Technology
 * All rights reserved.
 *
 * This file is part of IOWarp Core.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

// Prototype roundtrip test for the kGcs bdev backend. Validates read/write
// traces against a GCS-compatible endpoint (e.g. a local fake-gcs-server). The
// test self-skips when GCS_ENDPOINT is not set so it is safe to run without one.
//
// Build: requires -DCLIO_CORE_ENABLE_GCS=ON (this file is only compiled then).
// Run (against fake-gcs-server, anonymous):
//   export GCS_ENDPOINT=http://127.0.0.1:4443
//   export GCS_PROJECT_ID=clio-prototype
//   ./chimaera_bdev_gcs_tests
// For real GCS, also set GOOGLE_APPLICATION_CREDENTIALS (service-account JSON)
// or GCS_ACCESS_TOKEN.

#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "simple_test.h"

using namespace std::chrono_literals;

#include <clio_runtime/clio_runtime.h>
#include <clio_runtime/pool_query.h>
#include <clio_runtime/singletons.h>
#include <clio_runtime/types.h>

#include <clio_runtime/bdev/bdev_client.h>
#include <clio_runtime/bdev/bdev_tasks.h>

namespace {

bool g_initialized = false;
const chi::u64 k4KB = 4096;

/** Wrap a single block in the chi::priv::vector the client API expects. */
inline chi::priv::vector<clio::run::bdev::Block> WrapBlock(
    const clio::run::bdev::Block& block) {
  chi::priv::vector<clio::run::bdev::Block> blocks(CTP_MALLOC);
  blocks.push_back(block);
  return blocks;
}

/** Initialize the runtime once for the whole suite. */
void EnsureInit() {
  if (g_initialized) return;
  bool success = chi::CHIMAERA_INIT(chi::ChimaeraMode::kClient, true);
  if (success) {
    g_initialized = true;
    SimpleTest::g_test_finalize = chi::CHIMAERA_FINALIZE;
    std::this_thread::sleep_for(500ms);
  }
}

/** Bucket for the trace test; override with GCS_TEST_BUCKET for real GCS.
 *  Defaults to the local fake-gcs-server bucket name used in CI. */
inline std::string TestBucket() {
  const char* b = std::getenv("GCS_TEST_BUCKET");
  return (b && *b) ? std::string(b) : std::string("clio-gcs-test");
}

/** @return true if no GCS endpoint is configured (test should skip). */
bool ShouldSkip() {
  const char* ep = std::getenv("GCS_ENDPOINT");
  bool have = (ep && *ep);
  if (!have) {
    HLOG(kInfo, "GCS_ENDPOINT not set — skipping GCS bdev test");
  }
  return !have;
}

}  // namespace

TEST_CASE("bdev_gcs_allocation_and_io", "[bdev][gcs][io]") {
  if (ShouldSkip()) return;
  EnsureInit();
  REQUIRE(g_initialized);
  std::this_thread::sleep_for(100ms);

  chi::PoolId custom_pool_id(8105, 0);
  clio::run::bdev::Client bdev_client(custom_pool_id);

  // 4 MiB logical capacity; bucket/prefix encoded in pool_name.
  const chi::u64 gcs_capacity = 4 * 1024 * 1024;
  std::string pool_name =
      "gcs://" + TestBucket() + "/clio-validation/pool_" + std::to_string(getpid());
  auto create_task = bdev_client.AsyncCreate(
      chi::PoolQuery::Dynamic(), pool_name, custom_pool_id,
      clio::run::bdev::BdevType::kGcs, gcs_capacity);
  create_task.Wait();
  bdev_client.pool_id_ = create_task->new_pool_id_;
  REQUIRE(create_task->GetReturnCode() == 0);
  std::this_thread::sleep_for(100ms);

  // Roundtrip several blocks: allocate -> write -> read -> verify -> free.
  for (int i = 0; i < 8; ++i) {
    auto pool_query = chi::PoolQuery::DirectHash(i);

    auto alloc_task = bdev_client.AsyncAllocateBlocks(pool_query, k4KB);
    alloc_task.Wait();
    REQUIRE(alloc_task->return_code_ == 0);
    REQUIRE(alloc_task->blocks_.size() > 0);
    clio::run::bdev::Block block = alloc_task->blocks_[0];
    REQUIRE(block.size_ == k4KB);

    // Distinct payload per iteration.
    std::vector<ctp::u8> write_data(k4KB);
    for (size_t j = 0; j < write_data.size(); ++j) {
      write_data[j] = static_cast<ctp::u8>((j + 0x5A + i) % 256);
    }

    auto write_buffer = CLIO_IPC->AllocateBuffer(write_data.size());
    REQUIRE_FALSE(write_buffer.IsNull());
    memcpy(write_buffer.ptr_, write_data.data(), write_data.size());
    auto write_task = bdev_client.AsyncWrite(
        pool_query, WrapBlock(block),
        write_buffer.shm_.template Cast<void>().template Cast<void>(),
        write_data.size());
    write_task.Wait();
    REQUIRE(write_task->return_code_ == 0);
    REQUIRE(write_task->bytes_written_ == k4KB);

    auto read_buffer = CLIO_IPC->AllocateBuffer(k4KB);
    REQUIRE_FALSE(read_buffer.IsNull());
    auto read_task = bdev_client.AsyncRead(
        pool_query, WrapBlock(block),
        read_buffer.shm_.template Cast<void>().template Cast<void>(), k4KB);
    read_task.Wait();
    REQUIRE(read_task->return_code_ == 0);
    REQUIRE(read_task->bytes_read_ == k4KB);

    std::vector<ctp::u8> read_data(read_task->bytes_read_);
    memcpy(read_data.data(), read_buffer.ptr_, read_task->bytes_read_);
    bool data_matches =
        std::equal(write_data.begin(), write_data.end(), read_data.begin());
    REQUIRE(data_matches);

    CLIO_IPC->FreeBuffer(write_buffer);
    CLIO_IPC->FreeBuffer(read_buffer);

    std::vector<clio::run::bdev::Block> free_blocks;
    free_blocks.push_back(block);
    auto free_task = bdev_client.AsyncFreeBlocks(pool_query, free_blocks);
    free_task.Wait();
    REQUIRE(free_task->return_code_ == 0);

    HLOG(kInfo, "GCS iteration {}: roundtrip OK", i);
  }
}

TEST_CASE("bdev_gcs_sparse_read_zeros", "[bdev][gcs][sparse]") {
  if (ShouldSkip()) return;
  EnsureInit();
  REQUIRE(g_initialized);

  chi::PoolId custom_pool_id(8106, 0);
  clio::run::bdev::Client bdev_client(custom_pool_id);
  const chi::u64 gcs_capacity = 4 * 1024 * 1024;
  std::string pool_name =
      "gcs://" + TestBucket() + "/clio-validation/sparse_" + std::to_string(getpid());
  auto create_task = bdev_client.AsyncCreate(
      chi::PoolQuery::Dynamic(), pool_name, custom_pool_id,
      clio::run::bdev::BdevType::kGcs, gcs_capacity);
  create_task.Wait();
  bdev_client.pool_id_ = create_task->new_pool_id_;
  REQUIRE(create_task->GetReturnCode() == 0);

  auto pool_query = chi::PoolQuery::DirectHash(0);
  auto alloc_task = bdev_client.AsyncAllocateBlocks(pool_query, k4KB);
  alloc_task.Wait();
  REQUIRE(alloc_task->return_code_ == 0);
  clio::run::bdev::Block block = alloc_task->blocks_[0];

  // Read a never-written block: GCS download 404 -> zero-filled buffer.
  auto read_buffer = CLIO_IPC->AllocateBuffer(k4KB);
  REQUIRE_FALSE(read_buffer.IsNull());
  memset(read_buffer.ptr_, 0xFF, k4KB);  // poison so zeros are meaningful
  auto read_task = bdev_client.AsyncRead(
      pool_query, WrapBlock(block),
      read_buffer.shm_.template Cast<void>().template Cast<void>(), k4KB);
  read_task.Wait();
  REQUIRE(read_task->return_code_ == 0);
  REQUIRE(read_task->bytes_read_ == k4KB);

  std::vector<ctp::u8> read_data(k4KB);
  memcpy(read_data.data(), read_buffer.ptr_, k4KB);
  bool all_zero = true;
  for (ctp::u8 b : read_data) {
    if (b != 0) {
      all_zero = false;
      break;
    }
  }
  REQUIRE(all_zero);
  CLIO_IPC->FreeBuffer(read_buffer);
  HLOG(kInfo, "GCS sparse read returned zeros as expected");
}

SIMPLE_TEST_MAIN()
