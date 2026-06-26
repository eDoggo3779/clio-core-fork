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

/**
 * test_gcs_assim.cc - End-to-end test for GCS (gs://) assimilation into CTE.
 *
 * The test:
 *   1. Self-skips (exit 0) unless GCS_ENDPOINT is set (so default CI passes
 *      without a GCS endpoint / fake-gcs-server).
 *   2. Seeds a known, patterned object into GCS using google-cloud-cpp.
 *   3. Runs ParseOmni with src="gs://<bucket>/<object>", format="binary".
 *   4. Verifies the object's bytes landed in CTE (tag size == object size).
 *   5. Tears down the seeded object.
 *
 * Environment:
 *   GCS_ENDPOINT     GCS-compatible endpoint (e.g. http://127.0.0.1:4443 for
 *                    fake-gcs-server). REQUIRED — the test self-skips unset.
 *   GCS_TEST_BUCKET  Bucket to use (default clio-cae-test).
 *   GCS_PROJECT_ID   Project used only when creating the bucket (default
 *                    clio-prototype).
 */

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <unistd.h>

#include <google/cloud/credentials.h>
#include <google/cloud/options.h>
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/options.h>

#include <clio_ctp/introspect/system_info.h>
#include <clio_ctp/util/logging.h>
#include <clio_runtime/clio_runtime.h>
#include <clio_cae/core/core_client.h>
#include <clio_cae/core/constants.h>
#include <clio_cae/core/factory/assimilation_ctx.h>
#include <clio_cte/core/core_client.h>

namespace gcs = google::cloud::storage;
namespace gc = google::cloud;

namespace {

constexpr size_t kObjectSize = 3 * 1024 * 1024;  // 3 MB -> exercises chunking
const std::string kTagName = "test_gcs_assim_tag";

/** Build a deterministic, verifiable byte pattern (4-byte block index LE). */
std::string MakePatternedData(size_t size_bytes) {
  std::string data(size_bytes, '\0');
  for (size_t i = 0; i + 4 <= size_bytes; i += 4) {
    uint32_t value = static_cast<uint32_t>(i / 4);
    std::memcpy(&data[i], &value, 4);
  }
  return data;
}

}  // namespace

int main(int /*argc*/, char* /*argv*/[]) {
  HLOG(kInfo, "========================================");
  HLOG(kInfo, "GCS (gs://) Assimilation Test");
  HLOG(kInfo, "========================================");

  // Self-skip when no GCS endpoint is configured.
  const char* endpoint = std::getenv("GCS_ENDPOINT");
  if (!endpoint || !*endpoint) {
    HLOG(kInfo, "GCS_ENDPOINT not set -> skipping GCS assimilation test");
    return 0;
  }

  std::string bucket = "clio-cae-test";
  if (const char* b = std::getenv("GCS_TEST_BUCKET"); b && *b) {
    bucket = b;
  }
  std::string project = "clio-prototype";
  if (const char* p = std::getenv("GCS_PROJECT_ID"); p && *p) {
    project = p;
  }
  const std::string object =
      "cae_gcs_test/obj_" + std::to_string(static_cast<long>(::getpid()));
  const std::string data = MakePatternedData(kObjectSize);

  // Build a GCS client pointed at the (emulator) endpoint with anonymous creds.
  gc::Options options;
  options.set<gcs::RestEndpointOption>(endpoint);
  options.set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials());
  auto client = gcs::Client(std::move(options));

  // Ensure the bucket exists (best effort; already-exists is fine).
  auto made = client.CreateBucketForProject(bucket, project, gcs::BucketMetadata());
  HLOG(kInfo, "GCS ensure bucket '{}' ({})", bucket,
       made ? "created" : made.status().message());

  // Seed the object.
  auto inserted = client.InsertObject(bucket, object, data);
  if (!inserted) {
    HLOG(kError, "Failed to seed gs://{}/{}: {}", bucket, object,
         inserted.status().message());
    return 1;
  }
  HLOG(kSuccess, "Seeded gs://{}/{} ({} bytes)", bucket, object, data.size());

  int exit_code = 0;
  try {
    // Bring up CLIO + CTE + CAE.
    if (!clio::run::CLIO_INIT(clio::run::RuntimeMode::kClient, true)) {
      HLOG(kError, "Failed to initialize Clio");
      return 1;
    }
    clio::cte::core::CLIO_CTE_CLIENT_INIT();
    CLIO_CAE_CLIENT_INIT();

    clio::cae::core::Client cae_client;
    clio::cae::core::CreateParams params;
    auto create_task = cae_client.AsyncCreate(clio::run::PoolQuery::Local(),
                                              "test_cae_pool",
                                              clio::cae::core::kCaePoolId,
                                              params);
    create_task.Wait();

    // Assimilate the GCS object.
    clio::cae::core::AssimilationCtx ctx;
    ctx.src = "gs://" + bucket + "/" + object;
    ctx.dst = "iowarp::" + kTagName;
    ctx.format = "binary";
    std::vector<clio::cae::core::AssimilationCtx> contexts{ctx};

    HLOG(kInfo, "Calling ParseOmni for {}", ctx.src);
    auto parse_task = cae_client.AsyncParseOmni(contexts);
    parse_task.Wait();
    clio::run::u32 result_code = parse_task->GetReturnCode();
    clio::run::u32 num_scheduled = parse_task->num_tasks_scheduled_;
    HLOG(kInfo, "ParseOmni result_code={} num_tasks_scheduled={}", result_code,
         num_scheduled);
    if (result_code != 0 || num_scheduled == 0) {
      HLOG(kError, "ParseOmni failed for GCS source");
      exit_code = 1;
    }

    // Verify the bytes landed in CTE.
    auto cte_client = CLIO_CTE_CLIENT;
    auto tag_task = cte_client->AsyncGetOrCreateTag(kTagName);
    tag_task.Wait();
    clio::cte::core::TagId tag_id = tag_task->tag_id_;
    if (tag_id.IsNull()) {
      HLOG(kError, "Tag not found in CTE: {}", kTagName);
      exit_code = 1;
    } else {
      auto size_task = cte_client->AsyncGetTagSize(tag_id);
      size_task.Wait();
      size_t tag_size = size_task->tag_size_;
      HLOG(kInfo, "CTE tag size={} (expected {})", tag_size, kObjectSize);
      if (tag_size != kObjectSize) {
        HLOG(kError, "Tag size mismatch: got {}, expected {}", tag_size,
             kObjectSize);
        exit_code = 1;
      } else {
        HLOG(kSuccess, "GCS object bytes verified in CTE");
      }
    }
  } catch (const std::exception& e) {
    HLOG(kError, "Exception: {}", e.what());
    exit_code = 1;
  }

  // Teardown the seeded object (best effort).
  client.DeleteObject(bucket, object);

  HLOG(kInfo, "========================================");
  HLOG(kInfo, exit_code == 0 ? "TEST PASSED" : "TEST FAILED");
  HLOG(kInfo, "========================================");
  ctp::SystemInfo::TerminateProcessNow(exit_code);
  return exit_code;
}
