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
 * test_s3_assim.cc - End-to-end test for S3 (s3://) assimilation into CTE.
 *
 * The test:
 *   1. Self-skips (exit 0) unless S3_ENDPOINT is set (so default CI, which has
 *      no object store, passes without an endpoint).
 *   2. Seeds a known, patterned object into the S3-compatible store (MinIO)
 *      using the AWS SDK directly.
 *   3. Runs ParseOmni with src="s3://<bucket>/<key>", format="binary".
 *   4. Verifies the object's bytes landed in CTE (tag size == object size).
 *   5. Tears down the seeded object.
 *
 * Environment:
 *   S3_ENDPOINT        S3-compatible endpoint (e.g. http://127.0.0.1:9000).
 *                      REQUIRED — the test self-skips when unset.
 *   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY   Credentials (read by the SDK).
 *   AWS_DEFAULT_REGION Region (default us-east-1 for MinIO).
 *   S3_TEST_BUCKET     Bucket to use (default clio-cae-test).
 */

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <unistd.h>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <clio_ctp/introspect/system_info.h>
#include <clio_ctp/util/logging.h>
#include <clio_runtime/clio_runtime.h>
#include <clio_cae/core/core_client.h>
#include <clio_cae/core/constants.h>
#include <clio_cae/core/factory/assimilation_ctx.h>
#include <clio_cte/core/core_client.h>

namespace {

constexpr size_t kObjectSize = 3 * 1024 * 1024;  // 3 MB -> exercises chunking
const std::string kTagName = "test_s3_assim_tag";

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
  HLOG(kInfo, "S3 (s3://) Assimilation Test");
  HLOG(kInfo, "========================================");

  // Self-skip when no S3 endpoint is configured.
  const char* endpoint = std::getenv("S3_ENDPOINT");
  if (!endpoint || !*endpoint) {
    HLOG(kInfo, "S3_ENDPOINT not set -> skipping S3 assimilation test");
    return 0;
  }

  std::string bucket = "clio-cae-test";
  if (const char* b = std::getenv("S3_TEST_BUCKET"); b && *b) {
    bucket = b;
  }
  const std::string key =
      "cae_s3_test/obj_" + std::to_string(static_cast<long>(::getpid()));
  const std::string data = MakePatternedData(kObjectSize);

  int exit_code = 0;

  // Bring up CLIO + CTE + CAE BEFORE touching the AWS SDK. The assimilator
  // initializes the AWS SDK inside the runtime worker on first use, so the
  // runtime must come up first — this is the production ordering. (Initializing
  // the AWS SDK before the runtime starts corrupts runtime startup.)
  if (!clio::run::CLIO_INIT(clio::run::RuntimeMode::kClient, true)) {
    HLOG(kError, "Failed to initialize Clio");
    return 1;
  }
  clio::cte::core::CLIO_CTE_CLIENT_INIT();
  CLIO_CAE_CLIENT_INIT();
  clio::cae::core::Client cae_client;
  {
    clio::cae::core::CreateParams params;
    auto create_task = cae_client.AsyncCreate(clio::run::PoolQuery::Local(),
                                              "test_cae_pool",
                                              clio::cae::core::kCaePoolId, params);
    create_task.Wait();
  }

  Aws::SDKOptions options;
  Aws::InitAPI(options);
  {
    // Build an S3 client pointed at the endpoint (path-style for MinIO).
    Aws::Client::ClientConfiguration cfg;
    if (const char* region = std::getenv("AWS_DEFAULT_REGION");
        region && *region) {
      cfg.region = region;
    } else {
      cfg.region = "us-east-1";
    }
    cfg.endpointOverride = endpoint;
    Aws::S3::S3Client s3(
        cfg, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        /*useVirtualAddressing=*/false);

    // Ensure the bucket exists (best effort; already-exists is fine).
    Aws::S3::Model::CreateBucketRequest create_bucket;
    create_bucket.SetBucket(Aws::String(bucket.c_str()));
    auto cb = s3.CreateBucket(create_bucket);
    HLOG(kInfo, "S3 ensure bucket '{}' ({})", bucket,
         cb.IsSuccess() ? "created" : cb.GetError().GetMessage().c_str());

    // Seed the object.
    Aws::S3::Model::PutObjectRequest put;
    put.SetBucket(Aws::String(bucket.c_str()));
    put.SetKey(Aws::String(key.c_str()));
    auto body = std::make_shared<Aws::StringStream>();
    body->write(data.data(), static_cast<std::streamsize>(data.size()));
    body->seekg(0);
    put.SetBody(body);
    auto put_outcome = s3.PutObject(put);
    if (!put_outcome.IsSuccess()) {
      HLOG(kError, "Failed to seed s3://{}/{}: {}", bucket, key,
           put_outcome.GetError().GetMessage().c_str());
      Aws::ShutdownAPI(options);
      return 1;
    }
    HLOG(kSuccess, "Seeded s3://{}/{} ({} bytes)", bucket, key, data.size());

    try {
      // Assimilate the S3 object.
      clio::cae::core::AssimilationCtx ctx;
      ctx.src = "s3://" + bucket + "/" + key;
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
        HLOG(kError, "ParseOmni failed for S3 source");
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
          HLOG(kSuccess, "S3 object bytes verified in CTE");
        }
      }
    } catch (const std::exception& e) {
      HLOG(kError, "Exception: {}", e.what());
      exit_code = 1;
    }

    // Teardown the seeded object (best effort).
    Aws::S3::Model::DeleteObjectRequest del;
    del.SetBucket(Aws::String(bucket.c_str()));
    del.SetKey(Aws::String(key.c_str()));
    s3.DeleteObject(del);
  }
  Aws::ShutdownAPI(options);

  HLOG(kInfo, "========================================");
  HLOG(kInfo, exit_code == 0 ? "TEST PASSED" : "TEST FAILED");
  HLOG(kInfo, "========================================");
  ctp::SystemInfo::TerminateProcessNow(exit_code);
  return exit_code;
}
