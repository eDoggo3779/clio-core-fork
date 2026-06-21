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

#ifndef CLIO_BDEV_GCS_CLIENT_H_
#define CLIO_BDEV_GCS_CLIENT_H_

#include <curl/curl.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "gcs_credentials.h"
#include "http_object_store_client.h"

namespace clio::run::bdev {

/**
 * Connection + addressing + auth configuration for one GCS endpoint.
 *
 * Bucket and key prefix are parsed from the bdev pool name
 * (`gcs://bucket/prefix`). The endpoint and project come from the environment;
 * the credential *source* is resolved at Create time (see FromEnvAndPoolName)
 * so no secrets travel through serialized task fields.
 *
 * Unlike S3's per-request SigV4 signing, GCS's JSON API authenticates with a
 * single `Authorization: Bearer <token>` header. The token is supplied by a
 * shared, TTL-aware GcsTokenProvider (see gcs_credentials.h): the curl
 * transport stays per-worker, but the token cache is shared across workers and
 * refreshed before expiry — a fundamentally different auth model that the bdev
 * seam accommodates alongside request-signing.
 */
struct GcsConfig {
  std::string endpoint;      /**< Base endpoint, e.g. "https://storage.googleapis.com" */
  std::string scheme;        /**< "http" or "https" */
  std::string host;          /**< host[:port] portion of the endpoint */
  std::string bucket;        /**< Target bucket name */
  std::string prefix;        /**< Optional key prefix (no leading/trailing '/') */
  std::string project_id;    /**< GCP project id (needed only to create buckets) */
  /** Shared OAuth2 token provider (auth state is shared; transport per-worker). */
  std::shared_ptr<GcsTokenProvider> token_provider;
  size_t resumable_threshold = 8ull * 1024 * 1024; /**< Writes >= this go resumable */
  long connect_timeout_s = 10;  /**< curl connect timeout in seconds */
  bool http2 = true;            /**< Prefer HTTP/2 (false => force HTTP/1.1) */
  std::string ca_bundle;        /**< Optional CA bundle path (else curl default) */
  bool valid = false;        /**< True when all required fields were resolved */

  /**
   * Build a config from a `gcs://bucket/prefix` pool name and the environment.
   *
   * Reads GCS_ENDPOINT (default "https://storage.googleapis.com") and
   * GCS_PROJECT_ID, and binds a shared GcsTokenProvider for the credential
   * source resolved by the ADC precedence chain (ResolveGcsCredentials():
   * explicit token -> service-account JWT -> gcloud ADC -> GCE/GKE metadata ->
   * anonymous). See gcs_credentials.h.
   *
   * @param pool_name The bdev pool name (expected form "gcs://bucket/prefix").
   * @return A populated GcsConfig (check `.valid`).
   */
  static GcsConfig FromEnvAndPoolName(const std::string &pool_name);
};

/** Backward-compatible alias for the unified result type. */
using GcsResult = ObjectStoreResult;

/**
 * Minimal async Google Cloud Storage client over libcurl's multi interface,
 * speaking the GCS JSON API. The libcurl-multi transport and poll loop live in
 * the HttpObjectStoreClient base; this subclass adds only GCS's request
 * construction and OAuth2 bearer auth. One instance is owned per bdev worker
 * because a CURLM handle is not safe to share across threads.
 *
 * Object mapping is the s3backer model (same as kS3): each allocator block
 * maps to one object keyed by its byte offset (see KeyForOffset). Writes use a
 * simple media upload (POST .../o?uploadType=media&name=KEY); reads use
 * GET .../o/KEY?alt=media; a 404 read is treated as a zero (sparse) block.
 */
class GcsClient : public HttpObjectStoreClient {
 public:
  /**
   * @param config Resolved endpoint/bucket/token for this client.
   */
  explicit GcsClient(const GcsConfig &config);

  /** @return true when the config is valid and the curl multi handle exists. */
  bool IsValid() const override { return config_.valid && MultiOk(); }

  /** Create the target bucket (idempotent). @param capacity Unused. */
  bool Bootstrap(uint64_t capacity) override;

  /** Submit an async media upload for the block at `offset`. */
  void *WriteAsync(uint64_t offset, const void *buf, size_t len) override;

  /** Submit an async media download for the block at `offset`. */
  void *ReadAsync(uint64_t offset, void *buf, size_t len) override;

  /** GCS objects are immutable/whole-object; never-written blocks 404. */
  ReadFill GetReadFill() const override { return ReadFill::kSparse404; }

  /**
   * Synchronously create the target bucket (idempotent).
   * @return true on HTTP 200 (created) or 409 (already exists).
   */
  bool EnsureBucket();

  /**
   * Map a block byte offset to a stable object name (prefix + "blk_<offset>").
   * @param offset Block offset within the logical device.
   * @return The full (unencoded) object name for that block.
   */
  std::string KeyForOffset(uint64_t offset) const;

 protected:
  const char *LogTag() const override { return "Gcs"; }

  /** Invalidate the shared token so an auth-retry carries a fresh bearer. */
  void OnAuthError() override;

 private:
  /**
   * Build the request header list (bearer auth + given content type).
   * @param content_type Value for the Content-Type header (may be empty).
   * @return A curl_slist the caller owns and must free after the transfer.
   */
  curl_slist *BuildHeaders(const std::string &content_type) const;

  /**
   * Apply the common production transport options to an easy handle:
   * HTTP/2 (or forced 1.1), TCP keep-alive, a connect timeout, a stall guard,
   * and TLS peer/host verification (with an optional CA bundle override).
   * @param easy The curl easy handle to configure.
   */
  void ApplyTransportOpts(CURL *easy) const;

  /**
   * Begin a resumable upload session and return its session URI (synchronous;
   * one short round-trip, used only for writes >= resumable_threshold).
   * @param key The object name.
   * @param len Total bytes that will be uploaded.
   * @return The session URI, or "" on failure (caller falls back to media).
   */
  std::string InitiateResumable(const std::string &key, size_t len) const;

  /**
   * (Re)build the curl easy handle + headers for a media-upload write. Used for
   * the initial submit and for each retry re-arm, so a retried request always
   * carries a freshly-minted bearer token. @param op The op to configure.
   */
  void ConfigureWriteEasy(Op *op) const;

  /**
   * (Re)build the curl easy handle + headers for an alt=media download. Used for
   * the initial submit and for each retry re-arm. @param op The op to configure.
   */
  void ConfigureReadEasy(Op *op) const;

  GcsConfig config_;
};

}  // namespace clio::run::bdev

#endif  // CLIO_BDEV_GCS_CLIENT_H_
