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

#include <clio_runtime/bdev/oss_client.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <vector>

#include "clio_ctp/util/logging.h"

namespace clio::run::bdev {

/** SHA-256 of empty content — the payload hash for body-less SigV4 requests. */
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
 * Base64-encode raw bytes (standard alphabet, padded) — used by OSS V1.
 * @param data Input bytes.
 * @param len Number of input bytes.
 * @return Base64 string (empty when len==0).
 */
static std::string Base64Encode(const unsigned char *data, size_t len) {
  if (len == 0) return "";
  std::string out;
  out.resize(4 * ((len + 2) / 3));
  int n = EVP_EncodeBlock(reinterpret_cast<unsigned char *>(&out[0]), data,
                          static_cast<int>(len));
  if (n < 0) return "";
  out.resize(static_cast<size_t>(n));
  return out;
}

/**
 * Compute the lowercase hex SHA-256 of a buffer (SigV4 payload hash).
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
 * Compute a raw HMAC-SHA256 (SigV4 signing-key derivation).
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
 * Compute a raw HMAC-SHA1 (the native OSS V1 signing primitive).
 * @param key HMAC key bytes (the AccessKeySecret).
 * @param key_len Key length.
 * @param data Message bytes (the string-to-sign).
 * @param data_len Message length.
 * @return Raw 20-byte MAC.
 */
static std::vector<unsigned char> HmacSha1(const unsigned char *key,
                                           size_t key_len, const void *data,
                                           size_t data_len) {
  unsigned char out[EVP_MAX_MD_SIZE];
  unsigned int out_len = 0;
  HMAC(EVP_sha1(), key, static_cast<int>(key_len),
       reinterpret_cast<const unsigned char *>(data), data_len, out, &out_len);
  return std::vector<unsigned char>(out, out + out_len);
}

/**
 * RFC 3986 URI-encode per AWS/OSS rules (unreserved chars pass through).
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

/** ASCII-lowercase a string in place-free fashion. */
static std::string ToLower(const std::string &s) {
  std::string out = s;
  std::transform(out.begin(), out.end(), out.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return out;
}

//===========================================================================
// OSS V1 string-to-sign (the genuinely OSS-specific signing logic)
//===========================================================================

/**
 * Build the native OSS V1 string-to-sign.
 *
 * Layout (per Alibaba OSS "Add a signature to the Authorization header"):
 *   VERB\nContent-MD5\nContent-Type\nDate\nCanonicalizedOSSHeaders +
 *   CanonicalizedResource.
 * CanonicalizedOSSHeaders is every `x-oss-*` header lowercased, sorted by name,
 * each rendered as "name:value\n".
 *
 * @param method HTTP verb.
 * @param content_md5 Content-MD5 header value (may be empty).
 * @param content_type Content-Type header value (may be empty).
 * @param date RFC-1123 GMT date matching the Date header.
 * @param oss_headers x-oss-* headers as (name,value) pairs.
 * @param canonical_resource "/bucket/object" plus sub-resources.
 * @return The exact string to HMAC-SHA1.
 */
static std::string OssV1StringToSign(
    const std::string &method, const std::string &content_md5,
    const std::string &content_type, const std::string &date,
    const std::vector<std::pair<std::string, std::string>> &oss_headers,
    const std::string &canonical_resource) {
  std::vector<std::pair<std::string, std::string>> hs;
  for (const auto &kv : oss_headers) {
    std::string name = ToLower(kv.first);
    if (name.rfind("x-oss-", 0) == 0) hs.emplace_back(name, kv.second);
  }
  std::sort(hs.begin(), hs.end(),
            [](const auto &a, const auto &b) { return a.first < b.first; });
  std::string canon_headers;
  for (const auto &kv : hs) canon_headers += kv.first + ":" + kv.second + "\n";
  return method + "\n" + content_md5 + "\n" + content_type + "\n" + date +
         "\n" + canon_headers + canonical_resource;
}

std::string OssClient::SignV1ForTest(
    const std::string &secret, const std::string &method,
    const std::string &content_md5, const std::string &content_type,
    const std::string &date,
    const std::vector<std::pair<std::string, std::string>> &oss_headers,
    const std::string &canonical_resource) {
  std::string sts = OssV1StringToSign(method, content_md5, content_type, date,
                                      oss_headers, canonical_resource);
  std::vector<unsigned char> sig =
      HmacSha1(reinterpret_cast<const unsigned char *>(secret.data()),
               secret.size(), sts.data(), sts.size());
  return Base64Encode(sig.data(), sig.size());
}

//===========================================================================
// OssConfig
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

OssConfig OssConfig::FromEnvAndPoolName(const std::string &pool_name) {
  OssConfig cfg;
  // Parse "oss://bucket/prefix..."
  const std::string scheme_pfx = "oss://";
  if (pool_name.rfind(scheme_pfx, 0) != 0) {
    HLOG(kError, "OSS pool_name '{}' must start with 'oss://'", pool_name);
    return cfg;
  }
  std::string rest = pool_name.substr(scheme_pfx.size());
  size_t slash = rest.find('/');
  cfg.bucket = (slash == std::string::npos) ? rest : rest.substr(0, slash);
  cfg.prefix = (slash == std::string::npos) ? "" : rest.substr(slash + 1);
  while (!cfg.prefix.empty() && cfg.prefix.back() == '/') cfg.prefix.pop_back();
  if (cfg.bucket.empty()) {
    HLOG(kError, "OSS pool_name '{}' has an empty bucket", pool_name);
    return cfg;
  }

  cfg.endpoint = EnvOr("OSS_ENDPOINT", EnvOr("AWS_ENDPOINT_URL", ""));
  cfg.region = EnvOr("OSS_REGION", "cn-hangzhou");
  cfg.access_key = EnvOr("OSS_ACCESS_KEY_ID", EnvOr("AWS_ACCESS_KEY_ID", ""));
  cfg.secret_key =
      EnvOr("OSS_ACCESS_KEY_SECRET", EnvOr("AWS_SECRET_ACCESS_KEY", ""));
  std::string sig = ToLower(EnvOr("OSS_SIGNATURE", "v1"));
  cfg.signature =
      (sig == "s3") ? OssSignatureVersion::kS3 : OssSignatureVersion::kV1;
  if (cfg.endpoint.empty() || cfg.access_key.empty() ||
      cfg.secret_key.empty()) {
    HLOG(kError,
         "OSS backend needs OSS_ENDPOINT, OSS_ACCESS_KEY_ID, and "
         "OSS_ACCESS_KEY_SECRET in the environment");
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
  cfg.endpoint = cfg.scheme + "://" + cfg.host;
  cfg.valid = true;
  return cfg;
}

//===========================================================================
// OssOp + libcurl callbacks
//===========================================================================

/** Per-request state for one in-flight async operation. */
struct OssClient::OssOp {
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
static size_t OssReadCb(char *ptr, size_t size, size_t nmemb, void *userdata) {
  auto *op = static_cast<OssClient::OssOp *>(userdata);
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
static size_t OssWriteCb(char *ptr, size_t size, size_t nmemb, void *userdata) {
  auto *op = static_cast<OssClient::OssOp *>(userdata);
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
// OssClient lifecycle
//===========================================================================

OssClient::OssClient(const OssConfig &config) : config_(config) {
  if (!config_.valid) return;
  multi_ = curl_multi_init();
  if (multi_ == nullptr) {
    HLOG(kError, "Failed to create curl multi handle for OSS client");
  }
}

OssClient::~OssClient() {
  for (auto &kv : inflight_) {
    OssOp *op = kv.second;
    if (multi_ && op->easy) curl_multi_remove_handle(multi_, op->easy);
    if (op->easy) curl_easy_cleanup(op->easy);
    if (op->headers) curl_slist_free_all(op->headers);
    delete op;
  }
  inflight_.clear();
  if (multi_) curl_multi_cleanup(multi_);
}

std::string OssClient::KeyForOffset(uint64_t offset) const {
  std::string key = "blk_" + std::to_string(offset);
  return config_.prefix.empty() ? key : (config_.prefix + "/" + key);
}

//===========================================================================
// OSS V1 signing (native) + SigV4 signing (S3-compat)
//===========================================================================

/** Format the current UTC time as an RFC-1123 GMT date (the OSS Date header). */
static std::string Rfc1123DateNow() {
  std::time_t now = std::time(nullptr);
  std::tm tmv{};
  gmtime_r(&now, &tmv);
  char buf[40];
  // Locale-independent weekday/month abbreviations are required by OSS; the
  // process runs in the C locale so strftime emits English abbreviations.
  std::strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tmv);
  return std::string(buf);
}

curl_slist *OssClient::BuildHeadersV1(const std::string &method,
                                      const std::string &canonical_resource) {
  std::string date = Rfc1123DateNow();
  // Object PUT/GET here send no Content-MD5, no Content-Type, no x-oss-* hdrs.
  std::string sts =
      OssV1StringToSign(method, "", "", date, {}, canonical_resource);
  std::vector<unsigned char> sig =
      HmacSha1(reinterpret_cast<const unsigned char *>(config_.secret_key.data()),
               config_.secret_key.size(), sts.data(), sts.size());
  std::string signature = Base64Encode(sig.data(), sig.size());
  std::string authorization =
      "OSS " + config_.access_key + ":" + signature;

  curl_slist *headers = nullptr;
  headers = curl_slist_append(headers, ("Host: " + config_.host).c_str());
  headers = curl_slist_append(headers, ("Date: " + date).c_str());
  headers =
      curl_slist_append(headers, ("Authorization: " + authorization).c_str());
  headers = curl_slist_append(headers, "Expect:");  // suppress 100-continue
  return headers;
}

curl_slist *OssClient::BuildHeadersS3(const std::string &method,
                                      const std::string &canonical_uri,
                                      const std::string &payload_hash) {
  std::time_t now = std::time(nullptr);
  std::tm tmv{};
  gmtime_r(&now, &tmv);
  char amzdate[32];
  char datestamp[16];
  std::strftime(amzdate, sizeof(amzdate), "%Y%m%dT%H%M%SZ", &tmv);
  std::strftime(datestamp, sizeof(datestamp), "%Y%m%d", &tmv);

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

  std::string k0 = "AWS4" + config_.secret_key;
  std::vector<unsigned char> k_date = HmacSha256(
      reinterpret_cast<const unsigned char *>(k0.data()), k0.size(), datestamp,
      std::strlen(datestamp));
  std::vector<unsigned char> k_region =
      HmacSha256(k_date.data(), k_date.size(), config_.region.data(),
                 config_.region.size());
  std::vector<unsigned char> k_service =
      HmacSha256(k_region.data(), k_region.size(), "s3", 2);
  std::vector<unsigned char> k_signing =
      HmacSha256(k_service.data(), k_service.size(), "aws4_request", 12);
  std::vector<unsigned char> sig =
      HmacSha256(k_signing.data(), k_signing.size(), string_to_sign.data(),
                 string_to_sign.size());
  std::string signature = HexEncode(sig.data(), sig.size());

  std::string authorization =
      "AWS4-HMAC-SHA256 Credential=" + config_.access_key + "/" + scope +
      ", SignedHeaders=" + signed_headers + ", Signature=" + signature;

  curl_slist *headers = nullptr;
  headers = curl_slist_append(headers, ("Host: " + config_.host).c_str());
  headers = curl_slist_append(
      headers, ("x-amz-date: " + std::string(amzdate)).c_str());
  headers = curl_slist_append(
      headers, ("x-amz-content-sha256: " + payload_hash).c_str());
  headers =
      curl_slist_append(headers, ("Authorization: " + authorization).c_str());
  headers = curl_slist_append(headers, "Expect:");  // suppress 100-continue
  return headers;
}

bool OssClient::NewSignedHandle(OssOp *op, const std::string &method,
                                const std::string &payload_hash) {
  op->easy = curl_easy_init();
  if (op->easy == nullptr) return false;
  // Path-style addressing ({endpoint}/{bucket}/{key}) — required by S3
  // emulators and accepted by OSS; virtual-hosted is OSS's production default.
  std::string enc_key = AwsUriEncode(op->key, /*encode_slash=*/false);
  std::string url =
      config_.scheme + "://" + config_.host + "/" + config_.bucket + "/" +
      enc_key;
  if (config_.signature == OssSignatureVersion::kS3) {
    std::string canonical_uri = "/" + config_.bucket + "/" + enc_key;
    op->headers = BuildHeadersS3(method, canonical_uri, payload_hash);
  } else {
    // V1 canonical resource uses the raw (un-encoded) object path.
    std::string canonical_resource = "/" + config_.bucket + "/" + op->key;
    op->headers = BuildHeadersV1(method, canonical_resource);
  }
  curl_easy_setopt(op->easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(op->easy, CURLOPT_HTTPHEADER, op->headers);
  return true;
}

//===========================================================================
// Bucket creation (synchronous)
//===========================================================================

bool OssClient::EnsureBucket() {
  if (!IsValid()) return false;
  CURL *easy = curl_easy_init();
  if (easy == nullptr) return false;
  std::string url = config_.scheme + "://" + config_.host + "/" + config_.bucket;
  curl_slist *headers = nullptr;
  if (config_.signature == OssSignatureVersion::kS3) {
    headers = BuildHeadersS3("PUT", "/" + config_.bucket, kEmptyPayloadSha256);
  } else {
    headers = BuildHeadersV1("PUT", "/" + config_.bucket + "/");
  }
  curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, "PUT");
  curl_easy_setopt(easy, CURLOPT_NOBODY, 0L);

  CURLcode rc = curl_easy_perform(easy);
  long status = 0;
  curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &status);
  curl_slist_free_all(headers);
  curl_easy_cleanup(easy);

  // 200 = created; 409 already exists/owned; 204 on idempotent re-create.
  bool ok = (rc == CURLE_OK) && (status == 200 || status == 409 || status == 204);
  if (!ok) {
    HLOG(kError, "OSS EnsureBucket '{}' failed: curl={} http={}", config_.bucket,
         static_cast<int>(rc), status);
  } else {
    HLOG(kInfo, "OSS bucket '{}' ready (http={})", config_.bucket, status);
  }
  return ok;
}

//===========================================================================
// Async submit + poll
//===========================================================================

void *OssClient::PutAsync(const std::string &key, const void *buf, size_t len) {
  if (!IsValid()) return nullptr;
  auto *op = new OssOp();
  op->is_get = false;
  op->key = key;
  op->src = static_cast<const char *>(buf);
  op->len = len;
  op->start = std::chrono::high_resolution_clock::now();
  // SigV4 needs the body hash; V1 does not sign the body.
  std::string payload_hash =
      (config_.signature == OssSignatureVersion::kS3) ? Sha256Hex(buf, len) : "";
  if (!NewSignedHandle(op, "PUT", payload_hash)) {
    delete op;
    return nullptr;
  }
  curl_easy_setopt(op->easy, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(op->easy, CURLOPT_READFUNCTION, OssReadCb);
  curl_easy_setopt(op->easy, CURLOPT_READDATA, op);
  curl_easy_setopt(op->easy, CURLOPT_INFILESIZE_LARGE,
                   static_cast<curl_off_t>(len));
  curl_multi_add_handle(multi_, op->easy);
  inflight_[op->easy] = op;
  return op;
}

void *OssClient::GetAsync(const std::string &key, void *buf, size_t cap) {
  if (!IsValid()) return nullptr;
  auto *op = new OssOp();
  op->is_get = true;
  op->key = key;
  op->dst = static_cast<char *>(buf);
  op->cap = cap;
  op->start = std::chrono::high_resolution_clock::now();
  std::string payload_hash =
      (config_.signature == OssSignatureVersion::kS3) ? kEmptyPayloadSha256 : "";
  if (!NewSignedHandle(op, "GET", payload_hash)) {
    delete op;
    return nullptr;
  }
  curl_easy_setopt(op->easy, CURLOPT_HTTPGET, 1L);
  curl_easy_setopt(op->easy, CURLOPT_WRITEFUNCTION, OssWriteCb);
  curl_easy_setopt(op->easy, CURLOPT_WRITEDATA, op);
  curl_multi_add_handle(multi_, op->easy);
  inflight_[op->easy] = op;
  return op;
}

bool OssClient::IsComplete(void *token, OssResult &out) {
  auto *target = static_cast<OssOp *>(token);
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

  HLOG(kInfo, "OSS op={} key={} bytes={} http={} latency_ms={}",
       target->is_get ? "GET" : "PUT", target->key, out.bytes, status, ms);
  if (target->curl_code != CURLE_OK) {
    HLOG(kError, "OSS op={} key={} transport error curl={}",
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
