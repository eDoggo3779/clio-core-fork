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

#ifndef CLIO_BDEV_S3_CLIENT_H_
#define CLIO_BDEV_S3_CLIENT_H_

#include <curl/curl.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>

namespace clio::run::bdev {

/**
 * Connection + addressing configuration for one S3 endpoint.
 *
 * Bucket and key prefix are parsed from the bdev pool name
 * (`s3://bucket/prefix`); the endpoint, region, and credentials are read from
 * the process environment, mirroring the AWS credential-chain convention so no
 * secrets travel through serialized task fields.
 */
struct S3Config {
  std::string endpoint;    /**< Full base endpoint, e.g. "http://127.0.0.1:9000" */
  std::string scheme;      /**< "http" or "https" */
  std::string host;        /**< host[:port], used as the signed Host header */
  std::string region;      /**< AWS region label, e.g. "us-east-1" */
  std::string bucket;      /**< Target bucket name */
  std::string prefix;      /**< Optional key prefix (no leading/trailing '/') */
  std::string access_key;  /**< AWS access key id */
  std::string secret_key;  /**< AWS secret access key */
  bool valid = false;      /**< True when all required fields were resolved */

  /**
   * Build a config from an `s3://bucket/prefix` pool name and the environment.
   *
   * Reads S3_ENDPOINT (or AWS_ENDPOINT_URL), AWS_REGION (default "us-east-1"),
   * AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY. Sets `valid=false` if the
   * pool name is malformed or required credentials/endpoint are missing.
   *
   * @param pool_name The bdev pool name (expected form "s3://bucket/prefix").
   * @return A populated S3Config (check `.valid`).
   */
  static S3Config FromEnvAndPoolName(const std::string &pool_name);
};

/** Outcome of a completed async S3 operation. */
struct S3Result {
  long http_status = 0;    /**< HTTP status code (0 if the transport failed) */
  size_t bytes = 0;        /**< Bytes transferred (PUT body or GET payload) */
  bool not_found = false;  /**< True when a GET returned HTTP 404 */
  bool ok = false;         /**< True on a 2xx response with no transport error */
};

/**
 * Minimal async S3 client built on libcurl's multi interface + a SigV4 signer
 * (OpenSSL HMAC-SHA256). One instance is owned per bdev worker because a CURLM
 * handle is not safe to share across threads. The submit/poll surface
 * intentionally mirrors ctp::AsyncIO so the bdev coroutines can yield-while
 * polling exactly as the file backend does.
 *
 * Object mapping is the s3backer model: each allocator block maps to one
 * object keyed by its byte offset (see KeyForOffset).
 */
class S3Client {
 public:
  /**
   * @param config Resolved endpoint/bucket/credentials for this client.
   */
  explicit S3Client(const S3Config &config);
  ~S3Client();
  S3Client(const S3Client &) = delete;
  S3Client &operator=(const S3Client &) = delete;

  /** @return true when the config is valid and the curl multi handle exists. */
  bool IsValid() const { return config_.valid && multi_ != nullptr; }

  /**
   * Synchronously create the target bucket (idempotent).
   * @return true on HTTP 200 (created) or 409 (already owned by us).
   */
  bool EnsureBucket();

  /**
   * Submit an async PUT of `len` bytes from `buf` to object `key`.
   * The caller must keep `buf` alive until IsComplete() returns true.
   * @return Opaque op token to poll with IsComplete(), or nullptr on failure.
   */
  void *PutAsync(const std::string &key, const void *buf, size_t len);

  /**
   * Submit an async GET of object `key` into `buf` (capacity `cap` bytes).
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
  bool IsComplete(void *token, S3Result &out);

  /**
   * Map a block byte offset to a stable object key (prefix + "blk_<offset>").
   * @param offset Block offset within the logical device.
   * @return The full object key for that block.
   */
  std::string KeyForOffset(uint64_t offset) const;

  /** Per-request op state (defined in the .cc; public so the libcurl
   *  read/write trampolines can reach its fields). Treat as opaque. */
  struct S3Op;

 private:
  /**
   * Build the SigV4-signed header list for one request.
   * @param method HTTP verb ("PUT" or "GET").
   * @param canonical_uri URI-encoded path beginning with '/'.
   * @param payload_hash Lowercase hex SHA-256 of the request body.
   * @return A curl_slist the caller owns and must free after the transfer.
   */
  curl_slist *BuildSignedHeaders(const std::string &method,
                                 const std::string &canonical_uri,
                                 const std::string &payload_hash);

  /**
   * Create + sign a curl easy handle for `op` (sets URL + headers).
   * @param op The op whose key/handle/headers are populated.
   * @param method HTTP verb.
   * @param payload_hash Lowercase hex SHA-256 of the request body.
   * @return true on success.
   */
  bool NewSignedHandle(S3Op *op, const std::string &method,
                       const std::string &payload_hash);

  S3Config config_;
  CURLM *multi_ = nullptr;
  std::unordered_map<CURL *, S3Op *> inflight_;  /**< easy handle -> op */
};

}  // namespace clio::run::bdev

#endif  // CLIO_BDEV_S3_CLIENT_H_
