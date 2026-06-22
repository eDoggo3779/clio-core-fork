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

// Prototype tests for the kOss (Alibaba Cloud OSS) bdev backend.
//
// Two layers of validation:
//   1. oss_v1_signature_vector — validates the native OSS V1 (HMAC-SHA1)
//      signer against Alibaba's published worked example. Deterministic, no
//      network, always runs. This is the OSS-specific correctness proof.
//   2. bdev_oss_* — full bdev read/write/sparse roundtrip against a live
//      S3-compatible endpoint (e.g. local MinIO), exercising OSS's
//      S3-compatibility mode. Self-skips when OSS_ENDPOINT is unset.
//
// Build: requires -DCLIO_CORE_ENABLE_OSS=ON (this file is only compiled then).
// Data-plane run (against MinIO, which speaks S3 — so use s3-compat signing):
//   export OSS_ENDPOINT=http://127.0.0.1:9000 OSS_SIGNATURE=s3 OSS_REGION=us-east-1
//   export OSS_ACCESS_KEY_ID=minioadmin OSS_ACCESS_KEY_SECRET=minioadmin
//   ./chimaera_bdev_oss_tests

#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "simple_test.h"

using namespace std::chrono_literals;

#include <clio_runtime/clio_runtime.h>
#include <clio_runtime/pool_query.h>
#include <clio_runtime/singletons.h>
#include <clio_runtime/types.h>

#include <clio_runtime/bdev/bdev_client.h>
#include <clio_runtime/bdev/bdev_tasks.h>
#include <clio_runtime/bdev/oss_client.h>

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

/** @return true if no OSS endpoint is configured (data-plane test skips). */
bool ShouldSkip() {
  const char* ep = std::getenv("OSS_ENDPOINT");
  const char* ep2 = std::getenv("AWS_ENDPOINT_URL");
  bool have = (ep && *ep) || (ep2 && *ep2);
  if (!have) {
    HLOG(kInfo, "OSS_ENDPOINT not set — skipping OSS data-plane test");
  }
  return !have;
}

}  // namespace

// Validates the native OSS V1 (HMAC-SHA1) signer against Alibaba's documented
// string-to-sign formula:
//   Signature = base64(hmac-sha1(AccessKeySecret,
//       VERB \n Content-MD5 \n Content-Type \n Date \n
//       CanonicalizedOSSHeaders + CanonicalizedResource))
// The two x-oss-* headers are passed OUT of sorted order and in mixed case to
// exercise the signer's lowercase+sort canonicalization. The expected value is
// reproducible with no network via the standard tooling (and was generated
// that way), so it is an independent oracle rather than a memorized constant.
// Reproduce by piping the exact string-to-sign (the six fields joined by '\n',
// then the canonical resource) into:
//   openssl dgst -sha1 -hmac OtxrzxIsfpFjA7SwPzILwy8Bw21TLhquhboDYROV -binary
//     | openssl base64
//   -> YELtlzINxe5oZaStCVfFpHqAsig=
//
// This is the OSS-specific correctness proof for this prototype (the data-plane
// cases below run only against a live endpoint).
TEST_CASE("oss_v1_signature_vector", "[bdev][oss][sign]") {
  const std::string secret = "OtxrzxIsfpFjA7SwPzILwy8Bw21TLhquhboDYROV";
  const std::string expected = "YELtlzINxe5oZaStCVfFpHqAsig=";
  // Intentionally out of sorted order + mixed case to test canonicalization.
  std::vector<std::pair<std::string, std::string>> oss_headers = {
      {"X-OSS-Meta-Author", "foo@bar.com"},
      {"X-OSS-Magic", "abracadabra"},
  };
  std::string sig = clio::run::bdev::OssClient::SignV1ForTest(
      secret, "PUT", "eB5eJF1ptWaXm4bijSPyxw==", "text/html",
      "Wed, 28 Oct 2015 00:56:21 GMT", oss_headers, "/oss-example/nelson");
  HLOG(kInfo, "OSS V1 signature computed='{}' expected='{}'", sig, expected);
  REQUIRE(sig == expected);
}

TEST_CASE("bdev_oss_allocation_and_io", "[bdev][oss][io]") {
  if (ShouldSkip()) return;
  EnsureInit();
  REQUIRE(g_initialized);
  std::this_thread::sleep_for(100ms);

  chi::PoolId custom_pool_id(8009, 0);
  clio::run::bdev::Client bdev_client(custom_pool_id);

  // 4 MiB logical capacity; bucket/prefix encoded in pool_name.
  const chi::u64 oss_capacity = 4 * 1024 * 1024;
  std::string pool_name =
      "oss://clio-bdev-test/pool_" + std::to_string(getpid());
  auto create_task = bdev_client.AsyncCreate(
      chi::PoolQuery::Dynamic(), pool_name, custom_pool_id,
      clio::run::bdev::BdevType::kOss, oss_capacity);
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

    HLOG(kInfo, "OSS iteration {}: roundtrip OK", i);
  }
}

TEST_CASE("bdev_oss_sparse_read_zeros", "[bdev][oss][sparse]") {
  if (ShouldSkip()) return;
  EnsureInit();
  REQUIRE(g_initialized);

  chi::PoolId custom_pool_id(8010, 0);
  clio::run::bdev::Client bdev_client(custom_pool_id);
  const chi::u64 oss_capacity = 4 * 1024 * 1024;
  std::string pool_name =
      "oss://clio-bdev-test/sparse_" + std::to_string(getpid());
  auto create_task = bdev_client.AsyncCreate(
      chi::PoolQuery::Dynamic(), pool_name, custom_pool_id,
      clio::run::bdev::BdevType::kOss, oss_capacity);
  create_task.Wait();
  bdev_client.pool_id_ = create_task->new_pool_id_;
  REQUIRE(create_task->GetReturnCode() == 0);

  auto pool_query = chi::PoolQuery::DirectHash(0);
  auto alloc_task = bdev_client.AsyncAllocateBlocks(pool_query, k4KB);
  alloc_task.Wait();
  REQUIRE(alloc_task->return_code_ == 0);
  clio::run::bdev::Block block = alloc_task->blocks_[0];

  // Read a never-written block: OSS GET 404 -> zero-filled buffer.
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
  HLOG(kInfo, "OSS sparse read returned zeros as expected");
}

SIMPLE_TEST_MAIN()
