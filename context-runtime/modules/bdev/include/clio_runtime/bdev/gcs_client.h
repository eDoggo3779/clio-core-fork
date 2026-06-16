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
#include <string>
#include <unordered_map>

namespace clio::run::bdev {

/**
 * Connection + addressing + auth configuration for one GCS endpoint.
 *
 * Bucket and key prefix are parsed from the bdev pool name
 * (`gcs://bucket/prefix`). The endpoint and project come from the environment;
 * the OAuth2 bearer token is resolved once at Create time (see
 * FromEnvAndPoolName) so no secrets travel through serialized task fields.
 *
 * Unlike S3's per-request SigV4 signing, GCS's JSON API authenticates with a
 * single `Authorization: Bearer <token>` header. The token is acquired
 * out-of-band (service-account JWT exchange, an injected token, or anonymous
 * for an emulator) and reused across every request — a fundamentally different
 * auth model that the bdev seam must accommodate alongside request-signing.
 */
struct GcsConfig {
  std::string endpoint;      /**< Base endpoint, e.g. "https://storage.googleapis.com" */
  std::string scheme;        /**< "http" or "https" */
  std::string host;          /**< host[:port] portion of the endpoint */
  std::string bucket;        /**< Target bucket name */
  std::string prefix;        /**< Optional key prefix (no leading/trailing '/') */
  std::string project_id;    /**< GCP project id (needed only to create buckets) */
  std::string access_token;  /**< OAuth2 bearer token ("" => anonymous/emulator) */
  bool valid = false;        /**< True when all required fields were resolved */

  /**
   * Build a config from a `gcs://bucket/prefix` pool name and the environment.
   *
   * Reads GCS_ENDPOINT (default "https://storage.googleapis.com"),
   * GCS_PROJECT_ID, and resolves a bearer token by precedence:
   *   1. GCS_ACCESS_TOKEN (used verbatim),
   *   2. GOOGLE_APPLICATION_CREDENTIALS (service-account JSON -> JWT exchange),
   *   3. anonymous (empty token, for fake-gcs-server / public objects).
   *
   * @param pool_name The bdev pool name (expected form "gcs://bucket/prefix").
   * @return A populated GcsConfig (check `.valid`).
   */
  static GcsConfig FromEnvAndPoolName(const std::string &pool_name);
};

/** Outcome of a completed async GCS operation. */
struct GcsResult {
  long http_status = 0;    /**< HTTP status code (0 if the transport failed) */
  size_t bytes = 0;        /**< Bytes transferred (upload body or download payload) */
  bool not_found = false;  /**< True when a download returned HTTP 404 */
  bool ok = false;         /**< True on a 2xx response with no transport error */
};

/**
 * Minimal async Google Cloud Storage client over libcurl's multi interface,
 * speaking the GCS JSON API. One instance is owned per bdev worker because a
 * CURLM handle is not safe to share across threads. The submit/poll surface
 * mirrors ctp::AsyncIO (and S3Client) so the bdev coroutines yield-while-poll
 * exactly as the file backend does.
 *
 * Object mapping is the s3backer model (same as kS3): each allocator block
 * maps to one object keyed by its byte offset (see KeyForOffset). Writes use a
 * simple media upload (POST .../o?uploadType=media&name=KEY); reads use
 * GET .../o/KEY?alt=media; a 404 read is treated as a zero (sparse) block.
 */
class GcsClient {
 public:
  /**
   * @param config Resolved endpoint/bucket/token for this client.
   */
  explicit GcsClient(const GcsConfig &config);
  ~GcsClient();
  GcsClient(const GcsClient &) = delete;
  GcsClient &operator=(const GcsClient &) = delete;

  /** @return true when the config is valid and the curl multi handle exists. */
  bool IsValid() const { return config_.valid && multi_ != nullptr; }

  /**
   * Synchronously create the target bucket (idempotent).
   * @return true on HTTP 200 (created) or 409 (already exists).
   */
  bool EnsureBucket();

  /**
   * Submit an async media upload of `len` bytes from `buf` to object `key`.
   * The caller must keep `buf` alive until IsComplete() returns true.
   * @return Opaque op token to poll with IsComplete(), or nullptr on failure.
   */
  void *PutAsync(const std::string &key, const void *buf, size_t len);

  /**
   * Submit an async download of object `key` into `buf` (capacity `cap` bytes).
   * @return Opaque op token to poll with IsComplete(), or nullptr on failure.
   */
  void *GetAsync(const std::string &key, void *buf, size_t cap);

  /**
   * Drive in-flight transfers and report whether `token`'s op has finished.
   * On a true return, `out` is populated and the op is freed (token invalid).
   * @param token An op token from PutAsync/GetAsync.
   * @param out Filled with the result when the op completes.
   * @return true if the op completed this call; false if still in flight.
   */
  bool IsComplete(void *token, GcsResult &out);

  /**
   * Map a block byte offset to a stable object name (prefix + "blk_<offset>").
   * @param offset Block offset within the logical device.
   * @return The full (unencoded) object name for that block.
   */
  std::string KeyForOffset(uint64_t offset) const;

  /** Per-request op state (defined in the .cc; public so the libcurl
   *  read/write trampolines can reach its fields). Treat as opaque. */
  struct GcsOp;

 private:
  /**
   * Build the request header list (bearer auth + given content type).
   * @param content_type Value for the Content-Type header (may be empty).
   * @return A curl_slist the caller owns and must free after the transfer.
   */
  curl_slist *BuildHeaders(const std::string &content_type) const;

  GcsConfig config_;
  CURLM *multi_ = nullptr;
  std::unordered_map<CURL *, GcsOp *> inflight_;  /**< easy handle -> op */
};

}  // namespace clio::run::bdev

#endif  // CLIO_BDEV_GCS_CLIENT_H_
