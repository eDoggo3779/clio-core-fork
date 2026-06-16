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

#ifndef CLIO_BDEV_OSS_CLIENT_H_
#define CLIO_BDEV_OSS_CLIENT_H_

#include <curl/curl.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace clio::run::bdev {

/** Request-signing scheme used by the OSS client. */
enum class OssSignatureVersion {
  kV1,  /**< Native OSS signature: HMAC-SHA1, "Authorization: OSS key:sig" */
  kS3   /**< AWS SigV4, for OSS's S3-compatible endpoint or S3 emulators */
};

/**
 * Connection + addressing configuration for one Alibaba Cloud OSS endpoint.
 *
 * Bucket and key prefix are parsed from the bdev pool name
 * (`oss://bucket/prefix`); the endpoint, region, and credentials are read from
 * the process environment so no secrets travel through serialized task fields.
 * The native OSS V1 (HMAC-SHA1) signer is the default; OSS_SIGNATURE=s3 selects
 * AWS SigV4 so the same data path can be validated against an S3 emulator
 * (e.g. MinIO) or OSS's own S3-compatible endpoint.
 */
struct OssConfig {
  std::string endpoint;    /**< Full base endpoint, e.g. "http://127.0.0.1:9000" */
  std::string scheme;      /**< "http" or "https" */
  std::string host;        /**< host[:port], used as the signed Host header */
  std::string region;      /**< OSS region label, e.g. "cn-hangzhou" */
  std::string bucket;      /**< Target bucket name */
  std::string prefix;      /**< Optional key prefix (no leading/trailing '/') */
  std::string access_key;  /**< OSS AccessKeyId */
  std::string secret_key;  /**< OSS AccessKeySecret */
  OssSignatureVersion signature = OssSignatureVersion::kV1; /**< Signing scheme */
  bool valid = false;      /**< True when all required fields were resolved */

  /**
   * Build a config from an `oss://bucket/prefix` pool name and the environment.
   *
   * Reads OSS_ENDPOINT (or AWS_ENDPOINT_URL), OSS_REGION (default
   * "cn-hangzhou"), OSS_ACCESS_KEY_ID (or AWS_ACCESS_KEY_ID),
   * OSS_ACCESS_KEY_SECRET (or AWS_SECRET_ACCESS_KEY), and OSS_SIGNATURE
   * ("v1" default, or "s3"). Sets `valid=false` if the pool name is malformed
   * or required credentials/endpoint are missing.
   *
   * @param pool_name The bdev pool name (expected form "oss://bucket/prefix").
   * @return A populated OssConfig (check `.valid`).
   */
  static OssConfig FromEnvAndPoolName(const std::string &pool_name);
};

/** Outcome of a completed async OSS operation. */
struct OssResult {
  long http_status = 0;    /**< HTTP status code (0 if the transport failed) */
  size_t bytes = 0;        /**< Bytes transferred (PUT body or GET payload) */
  bool not_found = false;  /**< True when a GET returned HTTP 404 */
  bool ok = false;         /**< True on a 2xx response with no transport error */
};

/**
 * Minimal async Alibaba Cloud OSS client built on libcurl's multi interface.
 *
 * The genuinely OSS-specific part is the native V1 signer (HMAC-SHA1 over a
 * canonicalized request, "Authorization: OSS AccessKeyId:Signature"), which is
 * structurally different from AWS SigV4 (SHA-1 not SHA-256, an RFC-1123 Date
 * header instead of a credential scope, and `x-oss-*` canonical headers). A
 * SigV4 path is also provided for OSS's S3-compatible endpoint and for local
 * emulators. One instance is owned per bdev worker because a CURLM handle is
 * not safe to share across threads. The submit/poll surface mirrors
 * ctp::AsyncIO so the bdev coroutines yield-while-polling exactly as the file
 * backend does.
 *
 * Object mapping is the s3backer model: each allocator block maps to one
 * object keyed by its byte offset (see KeyForOffset).
 */
class OssClient {
 public:
  /**
   * @param config Resolved endpoint/bucket/credentials for this client.
   */
  explicit OssClient(const OssConfig &config);
  ~OssClient();
  OssClient(const OssClient &) = delete;
  OssClient &operator=(const OssClient &) = delete;

  /** @return true when the config is valid and the curl multi handle exists. */
  bool IsValid() const { return config_.valid && multi_ != nullptr; }

  /**
   * Synchronously create the target bucket (idempotent).
   * @return true on a created/already-exists response.
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
  bool IsComplete(void *token, OssResult &out);

  /**
   * Map a block byte offset to a stable object key (prefix + "blk_<offset>").
   * @param offset Block offset within the logical device.
   * @return The full object key for that block.
   */
  std::string KeyForOffset(uint64_t offset) const;

  /**
   * Compute a native OSS V1 signature for an arbitrary canonicalized request.
   *
   * Exposed so the signer can be validated against Alibaba's published test
   * vector without any network. Builds the OSS string-to-sign
   * (VERB\nContent-MD5\nContent-Type\nDate\nCanonicalizedOSSHeaders +
   * CanonicalizedResource) and returns the base64 HMAC-SHA1 over it.
   *
   * @param secret OSS AccessKeySecret.
   * @param method HTTP verb (e.g. "PUT").
   * @param content_md5 Value of the Content-MD5 header (may be empty).
   * @param content_type Value of the Content-Type header (may be empty).
   * @param date RFC-1123 GMT date matching the request's Date header.
   * @param oss_headers x-oss-* headers as (name, value) pairs (any case/order).
   * @param canonical_resource "/bucket/object" plus any sub-resources.
   * @return The base64-encoded signature string.
   */
  static std::string SignV1ForTest(
      const std::string &secret, const std::string &method,
      const std::string &content_md5, const std::string &content_type,
      const std::string &date,
      const std::vector<std::pair<std::string, std::string>> &oss_headers,
      const std::string &canonical_resource);

  /** Per-request op state (defined in the .cc; public so the libcurl
   *  read/write trampolines can reach its fields). Treat as opaque. */
  struct OssOp;

 private:
  /**
   * Build the native OSS V1 signed header list for one request.
   * @param method HTTP verb ("PUT" or "GET").
   * @param canonical_resource "/bucket/key" used in the string-to-sign.
   * @return A curl_slist the caller owns and must free after the transfer.
   */
  curl_slist *BuildHeadersV1(const std::string &method,
                             const std::string &canonical_resource);

  /**
   * Build the AWS SigV4 signed header list (S3-compat mode).
   * @param method HTTP verb ("PUT" or "GET").
   * @param canonical_uri URI-encoded path beginning with '/'.
   * @param payload_hash Lowercase hex SHA-256 of the request body.
   * @return A curl_slist the caller owns and must free after the transfer.
   */
  curl_slist *BuildHeadersS3(const std::string &method,
                             const std::string &canonical_uri,
                             const std::string &payload_hash);

  /**
   * Create + sign a curl easy handle for `op` (sets URL + headers), dispatching
   * on the configured signature version.
   * @param op The op whose key/handle/headers are populated.
   * @param method HTTP verb.
   * @param payload_hash Lowercase hex SHA-256 of the body (S3 mode only).
   * @return true on success.
   */
  bool NewSignedHandle(OssOp *op, const std::string &method,
                       const std::string &payload_hash);

  OssConfig config_;
  CURLM *multi_ = nullptr;
  std::unordered_map<CURL *, OssOp *> inflight_;  /**< easy handle -> op */
};

}  // namespace clio::run::bdev

#endif  // CLIO_BDEV_OSS_CLIENT_H_
