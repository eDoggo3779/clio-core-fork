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

#include <clio_runtime/bdev/s3_client.h>

#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <vector>

#include "clio_ctp/util/logging.h"

namespace clio::run::bdev {

/** SHA-256 of empty content — the payload hash for body-less requests. */
static const char *kEmptyPayloadSha256 =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

//===========================================================================
// Crypto + encoding helpers (OpenSSL)
//===========================================================================

/**
 * Hex-encode raw bytes in lowercase.
 * @param data Input bytes.
 * @param len Number of input bytes.
 * @return Lowercase hex string of length 2*len.
 */
static std::string HexEncode(const unsigned char *data, size_t len) {
  static const char *hex = "0123456789abcdef";
  std::string out;
  out.resize(len * 2);
  for (size_t i = 0; i < len; ++i) {
    out[2 * i] = hex[(data[i] >> 4) & 0xF];
    out[2 * i + 1] = hex[data[i] & 0xF];
  }
  return out;
}

/**
 * Compute the lowercase hex SHA-256 of a buffer.
 * @param data Input bytes (may be null when len==0).
 * @param len Number of input bytes.
 * @return 64-character lowercase hex digest.
 */
static std::string Sha256Hex(const void *data, size_t len) {
  unsigned char digest[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char *>(data), len, digest);
  return HexEncode(digest, SHA256_DIGEST_LENGTH);
}

/**
 * Compute a raw HMAC-SHA256.
 * @param key HMAC key bytes.
 * @param key_len Key length.
 * @param data Message bytes.
 * @param data_len Message length.
 * @return Raw 32-byte MAC.
 */
static std::vector<unsigned char> HmacSha256(const unsigned char *key,
                                             size_t key_len, const void *data,
                                             size_t data_len) {
  unsigned char out[EVP_MAX_MD_SIZE];
  unsigned int out_len = 0;
  HMAC(EVP_sha256(), key, static_cast<int>(key_len),
       reinterpret_cast<const unsigned char *>(data), data_len, out, &out_len);
  return std::vector<unsigned char>(out, out + out_len);
}

/**
 * RFC 3986 URI-encode per AWS rules (unreserved chars pass through).
 * @param value String to encode.
 * @param encode_slash When false, '/' is left literal (used for object paths).
 * @return The percent-encoded string.
 */
static std::string AwsUriEncode(const std::string &value, bool encode_slash) {
  static const char *hex = "0123456789ABCDEF";
  std::string out;
  out.reserve(value.size() * 3);
  for (unsigned char c : value) {
    bool unreserved = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                      (c >= '0' && c <= '9') || c == '-' || c == '_' ||
                      c == '.' || c == '~';
    if (unreserved || (c == '/' && !encode_slash)) {
      out.push_back(static_cast<char>(c));
    } else {
      out.push_back('%');
      out.push_back(hex[(c >> 4) & 0xF]);
      out.push_back(hex[c & 0xF]);
    }
  }
  return out;
}

//===========================================================================
// S3Config
//===========================================================================

/**
 * Read an environment variable, returning a default when unset/empty.
 * @param name Variable name.
 * @param def Fallback value.
 * @return The value or `def`.
 */
static std::string EnvOr(const char *name, const std::string &def) {
  const char *v = std::getenv(name);
  return (v && *v) ? std::string(v) : def;
}

S3Config S3Config::FromEnvAndPoolName(const std::string &pool_name) {
  S3Config cfg;
  // Parse "s3://bucket/prefix..."
  const std::string scheme_pfx = "s3://";
  if (pool_name.rfind(scheme_pfx, 0) != 0) {
    HLOG(kError, "S3 pool_name '{}' must start with 's3://'", pool_name);
    return cfg;
  }
  std::string rest = pool_name.substr(scheme_pfx.size());
  size_t slash = rest.find('/');
  cfg.bucket = (slash == std::string::npos) ? rest : rest.substr(0, slash);
  cfg.prefix = (slash == std::string::npos) ? "" : rest.substr(slash + 1);
  // Trim any trailing '/' from the prefix for clean key joins.
  while (!cfg.prefix.empty() && cfg.prefix.back() == '/') cfg.prefix.pop_back();
  if (cfg.bucket.empty()) {
    HLOG(kError, "S3 pool_name '{}' has an empty bucket", pool_name);
    return cfg;
  }

  cfg.endpoint = EnvOr("S3_ENDPOINT", EnvOr("AWS_ENDPOINT_URL", ""));
  cfg.region = EnvOr("AWS_REGION", "us-east-1");
  cfg.access_key = EnvOr("AWS_ACCESS_KEY_ID", "");
  cfg.secret_key = EnvOr("AWS_SECRET_ACCESS_KEY", "");
  if (cfg.endpoint.empty() || cfg.access_key.empty() ||
      cfg.secret_key.empty()) {
    HLOG(kError,
         "S3 backend needs S3_ENDPOINT, AWS_ACCESS_KEY_ID, and "
         "AWS_SECRET_ACCESS_KEY in the environment");
    return cfg;
  }

  // Split endpoint into scheme + host[:port].
  size_t pos = cfg.endpoint.find("://");
  if (pos == std::string::npos) {
    cfg.scheme = "http";
    cfg.host = cfg.endpoint;
  } else {
    cfg.scheme = cfg.endpoint.substr(0, pos);
    cfg.host = cfg.endpoint.substr(pos + 3);
  }
  while (!cfg.host.empty() && cfg.host.back() == '/') cfg.host.pop_back();
  // Normalize endpoint to scheme://host (no trailing slash).
  cfg.endpoint = cfg.scheme + "://" + cfg.host;
  cfg.valid = true;
  return cfg;
}

//===========================================================================
// S3Op + libcurl callbacks
//===========================================================================

/** Per-request state for one in-flight async operation. */
struct S3Client::S3Op {
  CURL *easy = nullptr;             /**< Owning curl easy handle */
  curl_slist *headers = nullptr;    /**< Signed request headers (owned) */
  bool is_get = false;             /**< true for GET, false for PUT */
  bool finished = false;           /**< Set when the transfer completes */
  CURLcode curl_code = CURLE_OK;   /**< Transport-level result */
  std::string key;                 /**< Object key (for logging) */
  // PUT source
  const char *src = nullptr;
  size_t len = 0;
  size_t sent = 0;
  // GET destination
  char *dst = nullptr;
  size_t cap = 0;
  size_t written = 0;
  std::chrono::high_resolution_clock::time_point start;
};

/** libcurl read callback: stream the PUT body out of op->src. */
static size_t S3ReadCb(char *ptr, size_t size, size_t nmemb, void *userdata) {
  auto *op = static_cast<S3Client::S3Op *>(userdata);
  size_t want = size * nmemb;
  size_t remaining = op->len - op->sent;
  size_t n = std::min(want, remaining);
  if (n > 0) {
    std::memcpy(ptr, op->src + op->sent, n);
    op->sent += n;
  }
  return n;
}

/** libcurl write callback: capture the GET body into op->dst (bounded). */
static size_t S3WriteCb(char *ptr, size_t size, size_t nmemb, void *userdata) {
  auto *op = static_cast<S3Client::S3Op *>(userdata);
  size_t n = size * nmemb;
  size_t space = (op->written < op->cap) ? (op->cap - op->written) : 0;
  size_t c = std::min(n, space);
  if (c > 0) {
    std::memcpy(op->dst + op->written, ptr, c);
    op->written += c;
  }
  return n;  // Always consume everything so curl doesn't abort on overflow.
}

//===========================================================================
// S3Client lifecycle
//===========================================================================

S3Client::S3Client(const S3Config &config) : config_(config) {
  if (!config_.valid) return;
  multi_ = curl_multi_init();
  if (multi_ == nullptr) {
    HLOG(kError, "Failed to create curl multi handle for S3 client");
  }
}

S3Client::~S3Client() {
  for (auto &kv : inflight_) {
    S3Op *op = kv.second;
    if (multi_ && op->easy) curl_multi_remove_handle(multi_, op->easy);
    if (op->easy) curl_easy_cleanup(op->easy);
    if (op->headers) curl_slist_free_all(op->headers);
    delete op;
  }
  inflight_.clear();
  if (multi_) curl_multi_cleanup(multi_);
}

std::string S3Client::KeyForOffset(uint64_t offset) const {
  std::string key = "blk_" + std::to_string(offset);
  return config_.prefix.empty() ? key : (config_.prefix + "/" + key);
}

//===========================================================================
// SigV4 signing
//===========================================================================

curl_slist *S3Client::BuildSignedHeaders(const std::string &method,
                                         const std::string &canonical_uri,
                                         const std::string &payload_hash) {
  // Timestamps in UTC: amzdate=YYYYMMDDTHHMMSSZ, datestamp=YYYYMMDD.
  std::time_t now = std::time(nullptr);
  std::tm tmv{};
  gmtime_r(&now, &tmv);
  char amzdate[32];
  char datestamp[16];
  std::strftime(amzdate, sizeof(amzdate), "%Y%m%dT%H%M%SZ", &tmv);
  std::strftime(datestamp, sizeof(datestamp), "%Y%m%d", &tmv);

  // Canonical headers (sorted): host, x-amz-content-sha256, x-amz-date.
  std::string signed_headers = "host;x-amz-content-sha256;x-amz-date";
  std::string canonical_headers = "host:" + config_.host + "\n" +
                                  "x-amz-content-sha256:" + payload_hash +
                                  "\n" + "x-amz-date:" + amzdate + "\n";
  std::string canonical_request = method + "\n" + canonical_uri + "\n" + "" +
                                  "\n" + canonical_headers + "\n" +
                                  signed_headers + "\n" + payload_hash;

  std::string scope = std::string(datestamp) + "/" + config_.region + "/s3/" +
                      "aws4_request";
  std::string string_to_sign =
      std::string("AWS4-HMAC-SHA256\n") + amzdate + "\n" + scope + "\n" +
      Sha256Hex(canonical_request.data(), canonical_request.size());

  // Derive the signing key, then sign the string-to-sign.
  std::string k0 = "AWS4" + config_.secret_key;
  std::vector<unsigned char> k_date = HmacSha256(
      reinterpret_cast<const unsigned char *>(k0.data()), k0.size(), datestamp,
      std::strlen(datestamp));
  std::vector<unsigned char> k_region = HmacSha256(
      k_date.data(), k_date.size(), config_.region.data(), config_.region.size());
  std::vector<unsigned char> k_service =
      HmacSha256(k_region.data(), k_region.size(), "s3", 2);
  std::vector<unsigned char> k_signing =
      HmacSha256(k_service.data(), k_service.size(), "aws4_request", 12);
  std::vector<unsigned char> sig = HmacSha256(
      k_signing.data(), k_signing.size(), string_to_sign.data(),
      string_to_sign.size());
  std::string signature = HexEncode(sig.data(), sig.size());

  std::string authorization =
      "AWS4-HMAC-SHA256 Credential=" + config_.access_key + "/" + scope +
      ", SignedHeaders=" + signed_headers + ", Signature=" + signature;

  curl_slist *headers = nullptr;
  headers = curl_slist_append(headers, ("Host: " + config_.host).c_str());
  headers = curl_slist_append(headers, ("x-amz-date: " + std::string(amzdate)).c_str());
  headers = curl_slist_append(
      headers, ("x-amz-content-sha256: " + payload_hash).c_str());
  headers = curl_slist_append(headers, ("Authorization: " + authorization).c_str());
  headers = curl_slist_append(headers, "Expect:");  // suppress 100-continue
  return headers;
}

bool S3Client::NewSignedHandle(S3Op *op, const std::string &method,
                               const std::string &payload_hash) {
  op->easy = curl_easy_init();
  if (op->easy == nullptr) return false;
  std::string enc_key = AwsUriEncode(op->key, /*encode_slash=*/false);
  std::string canonical_uri = "/" + config_.bucket + "/" + enc_key;
  std::string url = config_.scheme + "://" + config_.host + canonical_uri;
  op->headers = BuildSignedHeaders(method, canonical_uri, payload_hash);
  curl_easy_setopt(op->easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(op->easy, CURLOPT_HTTPHEADER, op->headers);
  return true;
}

//===========================================================================
// Bucket creation (synchronous)
//===========================================================================

bool S3Client::EnsureBucket() {
  if (!IsValid()) return false;
  CURL *easy = curl_easy_init();
  if (easy == nullptr) return false;
  std::string canonical_uri = "/" + config_.bucket;
  std::string url = config_.scheme + "://" + config_.host + canonical_uri;
  curl_slist *headers =
      BuildSignedHeaders("PUT", canonical_uri, kEmptyPayloadSha256);
  curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, "PUT");
  curl_easy_setopt(easy, CURLOPT_NOBODY, 0L);

  CURLcode rc = curl_easy_perform(easy);
  long status = 0;
  curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &status);
  curl_slist_free_all(headers);
  curl_easy_cleanup(easy);

  // 200 = created; 409 BucketAlreadyOwnedByYou; 200/204 on re-create.
  bool ok = (rc == CURLE_OK) && (status == 200 || status == 409 || status == 204);
  if (!ok) {
    HLOG(kError, "S3 EnsureBucket '{}' failed: curl={} http={}", config_.bucket,
         static_cast<int>(rc), status);
  } else {
    HLOG(kInfo, "S3 bucket '{}' ready (http={})", config_.bucket, status);
  }
  return ok;
}

//===========================================================================
// Async submit + poll
//===========================================================================

void *S3Client::PutAsync(const std::string &key, const void *buf, size_t len) {
  if (!IsValid()) return nullptr;
  auto *op = new S3Op();
  op->is_get = false;
  op->key = key;
  op->src = static_cast<const char *>(buf);
  op->len = len;
  op->start = std::chrono::high_resolution_clock::now();
  std::string payload_hash = Sha256Hex(buf, len);
  if (!NewSignedHandle(op, "PUT", payload_hash)) {
    delete op;
    return nullptr;
  }
  curl_easy_setopt(op->easy, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(op->easy, CURLOPT_READFUNCTION, S3ReadCb);
  curl_easy_setopt(op->easy, CURLOPT_READDATA, op);
  curl_easy_setopt(op->easy, CURLOPT_INFILESIZE_LARGE,
                   static_cast<curl_off_t>(len));
  curl_multi_add_handle(multi_, op->easy);
  inflight_[op->easy] = op;
  return op;
}

void *S3Client::GetAsync(const std::string &key, void *buf, size_t cap) {
  if (!IsValid()) return nullptr;
  auto *op = new S3Op();
  op->is_get = true;
  op->key = key;
  op->dst = static_cast<char *>(buf);
  op->cap = cap;
  op->start = std::chrono::high_resolution_clock::now();
  if (!NewSignedHandle(op, "GET", kEmptyPayloadSha256)) {
    delete op;
    return nullptr;
  }
  curl_easy_setopt(op->easy, CURLOPT_HTTPGET, 1L);
  curl_easy_setopt(op->easy, CURLOPT_WRITEFUNCTION, S3WriteCb);
  curl_easy_setopt(op->easy, CURLOPT_WRITEDATA, op);
  curl_multi_add_handle(multi_, op->easy);
  inflight_[op->easy] = op;
  return op;
}

bool S3Client::IsComplete(void *token, S3Result &out) {
  auto *target = static_cast<S3Op *>(token);
  if (target == nullptr || multi_ == nullptr) return true;

  int running = 0;
  curl_multi_perform(multi_, &running);

  // Drain completion messages and mark the matching ops finished.
  CURLMsg *msg = nullptr;
  int in_queue = 0;
  while ((msg = curl_multi_info_read(multi_, &in_queue)) != nullptr) {
    if (msg->msg != CURLMSG_DONE) continue;
    auto it = inflight_.find(msg->easy_handle);
    if (it != inflight_.end()) {
      it->second->finished = true;
      it->second->curl_code = msg->data.result;
    }
  }

  if (!target->finished) return false;

  long status = 0;
  curl_easy_getinfo(target->easy, CURLINFO_RESPONSE_CODE, &status);
  double ms = std::chrono::duration<double, std::milli>(
                  std::chrono::high_resolution_clock::now() - target->start)
                  .count();
  out.http_status = status;
  out.not_found = (status == 404);
  out.bytes = target->is_get ? target->written : target->sent;
  out.ok = (target->curl_code == CURLE_OK) && (status >= 200 && status < 300);

  HLOG(kInfo, "S3 op={} key={} bytes={} http={} latency_ms={}",
       target->is_get ? "GET" : "PUT", target->key, out.bytes, status, ms);
  if (target->curl_code != CURLE_OK) {
    HLOG(kError, "S3 op={} key={} transport error curl={}",
         target->is_get ? "GET" : "PUT", target->key,
         static_cast<int>(target->curl_code));
  }

  curl_multi_remove_handle(multi_, target->easy);
  curl_easy_cleanup(target->easy);
  if (target->headers) curl_slist_free_all(target->headers);
  inflight_.erase(target->easy);
  delete target;
  return true;
}

}  // namespace clio::run::bdev
