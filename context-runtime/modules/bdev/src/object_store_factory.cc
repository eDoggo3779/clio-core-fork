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

#include <clio_runtime/bdev/object_store_factory.h>

#if defined(CLIO_BDEV_S3_ENABLED)
#include <clio_runtime/bdev/s3_client.h>
#endif
#if defined(CLIO_BDEV_GCS_ENABLED)
#include <clio_runtime/bdev/gcs_client.h>
#endif
#if defined(CLIO_BDEV_AZURE_ENABLED)
#include <clio_runtime/bdev/azure_blob_client.h>
#endif
#if defined(CLIO_BDEV_OSS_ENABLED)
#include <clio_runtime/bdev/oss_client.h>
#endif

namespace clio::run::bdev {

std::unique_ptr<ObjectStoreClient> MakeObjectStoreClient(
    BdevType type, const std::string &pool_name) {
  // Guarded switch: a disabled provider's case (and its header above) is
  // preprocessed away, so its symbols are never referenced or linked. This is
  // the only site in the bdev module that needs the per-provider guards.
  switch (type) {
#if defined(CLIO_BDEV_S3_ENABLED)
    case BdevType::kS3:
      return std::make_unique<S3Client>(
          S3Config::FromEnvAndPoolName(pool_name));
#endif
#if defined(CLIO_BDEV_GCS_ENABLED)
    case BdevType::kGcs:
      return std::make_unique<GcsClient>(
          GcsConfig::FromEnvAndPoolName(pool_name));
#endif
#if defined(CLIO_BDEV_AZURE_ENABLED)
    case BdevType::kAzure:
      return std::make_unique<AzureBlobClient>(
          AzureConfig::FromEnvAndPoolName(pool_name), /*is_page=*/false);
    case BdevType::kAzurePage:
      return std::make_unique<AzureBlobClient>(
          AzureConfig::FromEnvAndPoolName(pool_name), /*is_page=*/true);
#endif
#if defined(CLIO_BDEV_OSS_ENABLED)
    case BdevType::kOss:
      return std::make_unique<OssClient>(
          OssConfig::FromEnvAndPoolName(pool_name));
#endif
    default:
      return nullptr;
  }
}

}  // namespace clio::run::bdev
