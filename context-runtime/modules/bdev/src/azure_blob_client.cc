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

#include <clio_runtime/bdev/azure_blob_client.h>

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

/** Azure Storage REST API version targeted by the signer (Azurite-compatible). */
static const char *kAzureApiVersion = "2021-08-06";

//===========================================================================
// Crypto + encoding helpers (OpenSSL + base64)
//===========================================================================

/**
 * Compute a raw HMAC-SHA256.
 * @param key HMAC key bytes (the base64-decoded account key).
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

/** Standard base64 alphabet. */
static const char *kB64Chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/**
 * Base64-encode raw bytes (standard alphabet, '=' padding).
 * @param data Input bytes.
 * @param len Number of input bytes.
 * @return The base64 text.
 */
static std::string Base64Encode(const unsigned char *data, size_t len) {
  std::string out;
  out.reserve(((len + 2) / 3) * 4);
  size_t i = 0;
  for (; i + 3 <= len; i += 3) {
    uint32_t n = (data[i] << 16) | (data[i + 1] << 8) | data[i + 2];
    out.push_back(kB64Chars[(n >> 18) & 0x3F]);
    out.push_back(kB64Chars[(n >> 12) & 0x3F]);
    out.push_back(kB64Chars[(n >> 6) & 0x3F]);
    out.push_back(kB64Chars[n & 0x3F]);
  }
  if (len - i == 1) {
    uint32_t n = data[i] << 16;
    out.push_back(kB64Chars[(n >> 18) & 0x3F]);
    out.push_back(kB64Chars[(n >> 12) & 0x3F]);
    out.append("==");
  } else if (len - i == 2) {
    uint32_t n = (data[i] << 16) | (data[i + 1] << 8);
    out.push_back(kB64Chars[(n >> 18) & 0x3F]);
    out.push_back(kB64Chars[(n >> 12) & 0x3F]);
    out.push_back(kB64Chars[(n >> 6) & 0x3F]);
    out.push_back('=');
  }
  return out;
}

/**
 * Base64-decode text into raw bytes (ignores '=' padding and whitespace).
 * @param in Base64 text (e.g. an Azure account key).
 * @return The decoded bytes (empty on malformed input).
 */
static std::vector<unsigned char> Base64Decode(const std::string &in) {
  int rev[256];
  for (int i = 0; i < 256; ++i) rev[i] = -1;
  for (int i = 0; i < 64; ++i) rev[static_cast<unsigned char>(kB64Chars[i])] = i;
  std::vector<unsigned char> out;
  out.reserve((in.size() / 4) * 3);
  uint32_t buf = 0;
  int bits = 0;
  for (unsigned char c : in) {
    if (c == '=' || c == '\n' || c == '\r' || c == ' ') continue;
    int v = rev[c];
    if (v < 0) continue;
    buf = (buf << 6) | static_cast<uint32_t>(v);
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out.push_back(static_cast<unsigned char>((buf >> bits) & 0xFF));
    }
  }
  return out;
}

/**
 * Format the current UTC time as an RFC 1123 GMT timestamp for x-ms-date.
 * @return e.g. "Mon, 15 Jun 2026 16:11:17 GMT".
 */
static std::string Rfc1123Now() {
  std::time_t now = std::time(nullptr);
  std::tm tmv{};
  gmtime_r(&now, &tmv);
  char buf[40];
  std::strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tmv);
  return std::string(buf);
}

//===========================================================================
// AzureConfig
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

AzureConfig AzureConfig::FromEnvAndPoolName(const std::string &pool_name) {
  AzureConfig cfg;
  // Parse "azure://container/prefix...".
  const std::string scheme_pfx = "azure://";
  if (pool_name.rfind(scheme_pfx, 0) != 0) {
    HLOG(kError, "Azure pool_name '{}' must start with 'azure://'", pool_name);
    return cfg;
  }
  std::string rest = pool_name.substr(scheme_pfx.size());
  size_t slash = rest.find('/');
  cfg.container = (slash == std::string::npos) ? rest : rest.substr(0, slash);
  cfg.prefix = (slash == std::string::npos) ? "" : rest.substr(slash + 1);
  while (!cfg.prefix.empty() && cfg.prefix.back() == '/') cfg.prefix.pop_back();
  if (cfg.container.empty()) {
    HLOG(kError, "Azure pool_name '{}' has an empty container", pool_name);
    return cfg;
  }

  cfg.endpoint = EnvOr("AZURE_BLOB_ENDPOINT", EnvOr("AZURE_STORAGE_ENDPOINT",
                                                    ""));
  cfg.account = EnvOr("AZURE_STORAGE_ACCOUNT", "");
  cfg.key_b64 = EnvOr("AZURE_STORAGE_KEY", "");
  if (cfg.endpoint.empty() || cfg.account.empty() || cfg.key_b64.empty()) {
    HLOG(kError,
         "Azure backend needs AZURE_BLOB_ENDPOINT, AZURE_STORAGE_ACCOUNT, and "
         "AZURE_STORAGE_KEY in the environment");
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
  // Virtual-hosted addressing when the host is account-prefixed; otherwise the
  // account is the first URL path segment (Azurite emulator convention).
  cfg.path_style = cfg.host.rfind(cfg.account + ".", 0) != 0;
  cfg.valid = true;
  return cfg;
}

//===========================================================================
// AzureOp + libcurl callbacks
//===========================================================================

/** Per-request state for one in-flight async operation. */
struct AzureBlobClient::AzureOp {
  CURL *easy = nullptr;             /**< Owning curl easy handle */
  curl_slist *headers = nullptr;    /**< Signed request headers (owned) */
  bool is_get = false;             /**< true for GET, false for PUT */
  bool finished = false;           /**< Set when the transfer completes */
  CURLcode curl_code = CURLE_OK;   /**< Transport-level result */
  std::string key;                 /**< Object key (for logging) */
  std::string op_label;            /**< "PUT"/"GET"/"PUT-PAGE" for logging */
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
static size_t AzReadCb(char *ptr, size_t size, size_t nmemb, void *userdata) {
  auto *op = static_cast<AzureBlobClient::AzureOp *>(userdata);
  size_t want = size * nmemb;
  size_t remaining = op->len - op->sent;
  size_t n = std::min(want, remaining);
  if (n > 0) {
    std::memcpy(ptr, op->src + op->sent, n);
    op->sent += n;
  }
  return n;
}

/** libcurl read callback for an empty body: signal EOF immediately so curl
 *  still emits a "Content-Length: 0" header (required by Azure's Put Blob to
 *  dispatch a zero-body page-blob create). */
static size_t AzEmptyReadCb(char *, size_t, size_t, void *) { return 0; }

/** libcurl write callback: capture the GET body into op->dst (bounded). */
static size_t AzWriteCb(char *ptr, size_t size, size_t nmemb, void *userdata) {
  auto *op = static_cast<AzureBlobClient::AzureOp *>(userdata);
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
// AzureBlobClient lifecycle
//===========================================================================

AzureBlobClient::AzureBlobClient(const AzureConfig &config) : config_(config) {
  if (!config_.valid) return;
  multi_ = curl_multi_init();
  if (multi_ == nullptr) {
    HLOG(kError, "Failed to create curl multi handle for Azure client");
  }
}

AzureBlobClient::~AzureBlobClient() {
  for (auto &kv : inflight_) {
    AzureOp *op = kv.second;
    if (multi_ && op->easy) curl_multi_remove_handle(multi_, op->easy);
    if (op->easy) curl_easy_cleanup(op->easy);
    if (op->headers) curl_slist_free_all(op->headers);
    delete op;
  }
  inflight_.clear();
  if (multi_) curl_multi_cleanup(multi_);
}

std::string AzureBlobClient::KeyForOffset(uint64_t offset) const {
  std::string key = "blk_" + std::to_string(offset);
  return config_.prefix.empty() ? key : (config_.prefix + "/" + key);
}

std::string AzureBlobClient::DeviceKey() const {
  return config_.prefix.empty() ? "device" : (config_.prefix + "/device");
}

//===========================================================================
// URL composition + Shared Key signing
//===========================================================================

std::string AzureBlobClient::BuildUrl(
    const std::string &res_path, const std::vector<QueryParam> &query) const {
  // res_path is container-rooted, e.g. "/container/key". Path-style prepends
  // the account as the first segment; virtual-hosted leaves it in the host.
  std::string url = config_.scheme + "://" + config_.host;
  if (config_.path_style) url += "/" + config_.account;
  url += res_path;
  for (size_t i = 0; i < query.size(); ++i) {
    url += (i == 0 ? "?" : "&") + query[i].first + "=" + query[i].second;
  }
  return url;
}

curl_slist *AzureBlobClient::BuildSignedHeaders(
    const std::string &method, const std::string &res_path,
    const std::vector<QueryParam> &query, const std::string &content_length,
    const std::vector<XmsHeader> &xms) {
  // Assemble the full x-ms header set (caller extras + date + version), then
  // sort by name for the canonical headers block.
  std::vector<XmsHeader> all = xms;
  std::string date = Rfc1123Now();
  all.emplace_back("x-ms-date", date);
  all.emplace_back("x-ms-version", kAzureApiVersion);
  std::sort(all.begin(), all.end());
  std::string canon_headers;
  for (const auto &h : all) canon_headers += h.first + ":" + h.second + "\n";

  // CanonicalizedResource = "/" + account + <the exact URL path after the
  // host>. In path-style addressing the account is also the first URL path
  // segment, so it legitimately appears twice (this is what Azure/Azurite
  // sign against — the canonical resource mirrors the wire path, it is NOT
  // rebuilt from logical components). Query params follow, sorted.
  std::string canon_resource = "/" + config_.account;
  if (config_.path_style) canon_resource += "/" + config_.account;
  canon_resource += res_path;
  for (const auto &q : query) canon_resource += "\n" + q.first + ":" + q.second;

  // StringToSign: fixed positional headers, then canonical headers/resource.
  // Empty fields for the headers we never set (Date is empty since we sign
  // x-ms-date; Range is empty since we use the x-ms-range header).
  std::string string_to_sign = method + "\n" +  // VERB
                               "\n" +            // Content-Encoding
                               "\n" +            // Content-Language
                               content_length + "\n" +  // Content-Length
                               "\n" +            // Content-MD5
                               "\n" +            // Content-Type
                               "\n" +            // Date
                               "\n" +            // If-Modified-Since
                               "\n" +            // If-Match
                               "\n" +            // If-None-Match
                               "\n" +            // If-Unmodified-Since
                               "\n" +            // Range
                               canon_headers + canon_resource;

  std::vector<unsigned char> key = Base64Decode(config_.key_b64);
  std::vector<unsigned char> sig =
      HmacSha256(key.data(), key.size(), string_to_sign.data(),
                 string_to_sign.size());
  std::string signature = Base64Encode(sig.data(), sig.size());
  std::string authorization =
      "SharedKey " + config_.account + ":" + signature;

  curl_slist *headers = nullptr;
  for (const auto &h : all) {
    headers = curl_slist_append(headers, (h.first + ": " + h.second).c_str());
  }
  headers = curl_slist_append(headers, ("Authorization: " + authorization).c_str());
  headers = curl_slist_append(headers, "Content-Type:");  // sign as empty
  headers = curl_slist_append(headers, "Expect:");          // no 100-continue
  return headers;
}

//===========================================================================
// Container + page-blob creation (synchronous)
//===========================================================================

bool AzureBlobClient::EnsureContainer() {
  if (!IsValid()) return false;
  std::string res_path = "/" + config_.container;
  std::vector<QueryParam> query = {{"restype", "container"}};
  std::string url = BuildUrl(res_path, query);
  curl_slist *headers = BuildSignedHeaders("PUT", res_path, query, "", {});

  CURL *easy = curl_easy_init();
  if (easy == nullptr) {
    curl_slist_free_all(headers);
    return false;
  }
  curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, "PUT");
  CURLcode rc = curl_easy_perform(easy);
  long status = 0;
  curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &status);
  curl_slist_free_all(headers);
  curl_easy_cleanup(easy);

  bool ok = (rc == CURLE_OK) && (status == 201 || status == 409);
  if (!ok) {
    HLOG(kError, "Azure EnsureContainer '{}' failed: curl={} http={}",
         config_.container, static_cast<int>(rc), status);
  } else {
    HLOG(kInfo, "Azure container '{}' ready (http={})", config_.container,
         status);
  }
  return ok;
}

bool AzureBlobClient::CreatePageBlob(const std::string &key, uint64_t size) {
  if (!IsValid()) return false;
  // Page blobs must have a 512-aligned virtual size.
  uint64_t aligned = (size + kAzurePageSize - 1) / kAzurePageSize *
                     kAzurePageSize;
  std::string res_path = "/" + config_.container + "/" + key;
  std::vector<XmsHeader> xms = {
      {"x-ms-blob-content-length", std::to_string(aligned)},
      {"x-ms-blob-type", "PageBlob"}};
  std::string url = BuildUrl(res_path, {});
  curl_slist *headers = BuildSignedHeaders("PUT", res_path, {}, "", xms);

  CURL *easy = curl_easy_init();
  if (easy == nullptr) {
    curl_slist_free_all(headers);
    return false;
  }
  // Zero-length upload (UPLOAD + INFILESIZE 0) so curl emits "Content-Length:
  // 0"; Azure's Put Blob needs that header present to dispatch a page-blob
  // create. The signed Content-Length stays "" (0 is signed as empty).
  curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(easy, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(easy, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(0));
  curl_easy_setopt(easy, CURLOPT_READFUNCTION, AzEmptyReadCb);
  CURLcode rc = curl_easy_perform(easy);
  long status = 0;
  curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &status);
  curl_slist_free_all(headers);
  curl_easy_cleanup(easy);

  bool ok = (rc == CURLE_OK) && (status == 201);
  if (!ok) {
    HLOG(kError, "Azure CreatePageBlob '{}' failed: curl={} http={}", key,
         static_cast<int>(rc), status);
  } else {
    HLOG(kInfo, "Azure page blob '{}' created ({} bytes, http={})", key,
         aligned, status);
  }
  return ok;
}

//===========================================================================
// Async submit + poll
//===========================================================================

void *AzureBlobClient::PutBlockBlobAsync(const std::string &key,
                                         const void *buf, size_t len) {
  if (!IsValid()) return nullptr;
  auto *op = new AzureOp();
  op->is_get = false;
  op->key = key;
  op->op_label = "PUT";
  op->src = static_cast<const char *>(buf);
  op->len = len;
  op->start = std::chrono::high_resolution_clock::now();

  std::string res_path = "/" + config_.container + "/" + key;
  std::vector<XmsHeader> xms = {{"x-ms-blob-type", "BlockBlob"}};
  std::string url = BuildUrl(res_path, {});
  op->headers = BuildSignedHeaders("PUT", res_path, {}, std::to_string(len),
                                   xms);
  op->easy = curl_easy_init();
  if (op->easy == nullptr) {
    if (op->headers) curl_slist_free_all(op->headers);
    delete op;
    return nullptr;
  }
  curl_easy_setopt(op->easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(op->easy, CURLOPT_HTTPHEADER, op->headers);
  curl_easy_setopt(op->easy, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(op->easy, CURLOPT_READFUNCTION, AzReadCb);
  curl_easy_setopt(op->easy, CURLOPT_READDATA, op);
  curl_easy_setopt(op->easy, CURLOPT_INFILESIZE_LARGE,
                   static_cast<curl_off_t>(len));
  curl_multi_add_handle(multi_, op->easy);
  inflight_[op->easy] = op;
  return op;
}

void *AzureBlobClient::PutPageAsync(const std::string &key, uint64_t offset,
                                    const void *buf, size_t len) {
  if (!IsValid()) return nullptr;
  auto *op = new AzureOp();
  op->is_get = false;
  op->key = key;
  op->op_label = "PUT-PAGE";
  op->src = static_cast<const char *>(buf);
  op->len = len;
  op->start = std::chrono::high_resolution_clock::now();

  std::string res_path = "/" + config_.container + "/" + key;
  std::vector<QueryParam> query = {{"comp", "page"}};
  std::string range = "bytes=" + std::to_string(offset) + "-" +
                      std::to_string(offset + len - 1);
  std::vector<XmsHeader> xms = {{"x-ms-page-write", "update"},
                                {"x-ms-range", range}};
  std::string url = BuildUrl(res_path, query);
  op->headers = BuildSignedHeaders("PUT", res_path, query,
                                   std::to_string(len), xms);
  op->easy = curl_easy_init();
  if (op->easy == nullptr) {
    if (op->headers) curl_slist_free_all(op->headers);
    delete op;
    return nullptr;
  }
  curl_easy_setopt(op->easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(op->easy, CURLOPT_HTTPHEADER, op->headers);
  curl_easy_setopt(op->easy, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(op->easy, CURLOPT_READFUNCTION, AzReadCb);
  curl_easy_setopt(op->easy, CURLOPT_READDATA, op);
  curl_easy_setopt(op->easy, CURLOPT_INFILESIZE_LARGE,
                   static_cast<curl_off_t>(len));
  curl_multi_add_handle(multi_, op->easy);
  inflight_[op->easy] = op;
  return op;
}

void *AzureBlobClient::GetAsync(const std::string &key, void *buf, size_t cap,
                                uint64_t offset, size_t range_len,
                                bool use_range) {
  if (!IsValid()) return nullptr;
  auto *op = new AzureOp();
  op->is_get = true;
  op->key = key;
  op->op_label = "GET";
  op->dst = static_cast<char *>(buf);
  op->cap = cap;
  op->start = std::chrono::high_resolution_clock::now();

  std::string res_path = "/" + config_.container + "/" + key;
  std::vector<XmsHeader> xms;
  if (use_range) {
    std::string range = "bytes=" + std::to_string(offset) + "-" +
                        std::to_string(offset + range_len - 1);
    xms.emplace_back("x-ms-range", range);
  }
  std::string url = BuildUrl(res_path, {});
  op->headers = BuildSignedHeaders("GET", res_path, {}, "", xms);
  op->easy = curl_easy_init();
  if (op->easy == nullptr) {
    if (op->headers) curl_slist_free_all(op->headers);
    delete op;
    return nullptr;
  }
  curl_easy_setopt(op->easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(op->easy, CURLOPT_HTTPHEADER, op->headers);
  curl_easy_setopt(op->easy, CURLOPT_HTTPGET, 1L);
  curl_easy_setopt(op->easy, CURLOPT_WRITEFUNCTION, AzWriteCb);
  curl_easy_setopt(op->easy, CURLOPT_WRITEDATA, op);
  curl_multi_add_handle(multi_, op->easy);
  inflight_[op->easy] = op;
  return op;
}

bool AzureBlobClient::IsComplete(void *token, AzureResult &out) {
  auto *target = static_cast<AzureOp *>(token);
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

  HLOG(kInfo, "Azure op={} key={} bytes={} http={} latency_ms={}",
       target->op_label, target->key, out.bytes, status, ms);
  if (target->curl_code != CURLE_OK) {
    HLOG(kError, "Azure op={} key={} transport error curl={}",
         target->op_label, target->key, static_cast<int>(target->curl_code));
  }

  curl_multi_remove_handle(multi_, target->easy);
  curl_easy_cleanup(target->easy);
  if (target->headers) curl_slist_free_all(target->headers);
  inflight_.erase(target->easy);
  delete target;
  return true;
}

}  // namespace clio::run::bdev
