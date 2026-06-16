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

#include <clio_runtime/bdev/gcs_client.h>

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <sstream>
#include <vector>

#include "clio_ctp/util/logging.h"

namespace clio::run::bdev {

//===========================================================================
// Encoding helpers
//===========================================================================

/**
 * Percent-encode every character that is not RFC 3986 "unreserved".
 * Used for object names that become a single URL path segment / query value,
 * so embedded '/' is encoded as %2F (GCS treats the object name as opaque).
 * @param value String to encode.
 * @return The fully percent-encoded string.
 */
static std::string UrlEncode(const std::string &value) {
  static const char *hex = "0123456789ABCDEF";
  std::string out;
  out.reserve(value.size() * 3);
  for (unsigned char c : value) {
    bool unreserved = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                      (c >= '0' && c <= '9') || c == '-' || c == '_' ||
                      c == '.' || c == '~';
    if (unreserved) {
      out.push_back(static_cast<char>(c));
    } else {
      out.push_back('%');
      out.push_back(hex[(c >> 4) & 0xF]);
      out.push_back(hex[c & 0xF]);
    }
  }
  return out;
}

/**
 * Base64url-encode raw bytes (RFC 7515: '+'->'-', '/'->'_', no padding).
 * @param data Input bytes.
 * @param len Number of input bytes.
 * @return The base64url string without '=' padding.
 */
static std::string Base64Url(const unsigned char *data, size_t len) {
  static const char *tbl =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  std::string out;
  out.reserve((len + 2) / 3 * 4);
  for (size_t i = 0; i < len; i += 3) {
    unsigned v = data[i] << 16;
    int n = 1;
    if (i + 1 < len) { v |= data[i + 1] << 8; n = 2; }
    if (i + 2 < len) { v |= data[i + 2]; n = 3; }
    out.push_back(tbl[(v >> 18) & 0x3F]);
    out.push_back(tbl[(v >> 12) & 0x3F]);
    if (n >= 2) out.push_back(tbl[(v >> 6) & 0x3F]);
    if (n >= 3) out.push_back(tbl[v & 0x3F]);
  }
  return out;
}

/** Convenience overload taking a std::string payload. */
static std::string Base64Url(const std::string &s) {
  return Base64Url(reinterpret_cast<const unsigned char *>(s.data()), s.size());
}

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

/** libcurl write callback that appends the response body to a std::string. */
static size_t StringWriteCb(char *ptr, size_t size, size_t nmemb, void *ud) {
  auto *s = static_cast<std::string *>(ud);
  s->append(ptr, size * nmemb);
  return size * nmemb;
}

//===========================================================================
// OAuth2 service-account JWT exchange (real GCS auth path)
//===========================================================================

/**
 * Extract a top-level string field from a (well-formed) JSON object and
 * un-escape the common sequences (\n \" \\ \/). Prototype-grade scan tailored
 * to service-account key files and the token endpoint response — not a general
 * JSON parser.
 * @param json The JSON text.
 * @param key The field name to extract.
 * @return The un-escaped value, or "" if not found.
 */
static std::string ExtractJsonString(const std::string &json,
                                     const std::string &key) {
  std::string needle = "\"" + key + "\"";
  size_t k = json.find(needle);
  if (k == std::string::npos) return "";
  size_t colon = json.find(':', k + needle.size());
  if (colon == std::string::npos) return "";
  size_t q = json.find('"', colon + 1);
  if (q == std::string::npos) return "";
  std::string out;
  for (size_t i = q + 1; i < json.size(); ++i) {
    char c = json[i];
    if (c == '\\' && i + 1 < json.size()) {
      char n = json[++i];
      if (n == 'n') out.push_back('\n');
      else if (n == 't') out.push_back('\t');
      else out.push_back(n);  // covers \" \\ \/
    } else if (c == '"') {
      break;
    } else {
      out.push_back(c);
    }
  }
  return out;
}

/**
 * RSA-SHA256-sign a message with a PEM private key (RS256 for the JWT).
 * @param pem PEM-encoded RSA private key.
 * @param msg Message bytes to sign (the JWT signing input).
 * @return Raw signature bytes, or empty on failure.
 */
static std::string RsaSignSha256(const std::string &pem,
                                 const std::string &msg) {
  std::string sig;
  BIO *bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
  if (bio == nullptr) return sig;
  EVP_PKEY *pkey = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
  BIO_free(bio);
  if (pkey == nullptr) {
    HLOG(kError, "GCS auth: failed to parse service-account private key");
    return sig;
  }
  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  size_t slen = 0;
  if (ctx != nullptr &&
      EVP_DigestSignInit(ctx, nullptr, EVP_sha256(), nullptr, pkey) == 1 &&
      EVP_DigestSign(ctx, nullptr, &slen, reinterpret_cast<const unsigned char *>(
                                              msg.data()), msg.size()) == 1) {
    std::vector<unsigned char> buf(slen);
    if (EVP_DigestSign(ctx, buf.data(), &slen,
                       reinterpret_cast<const unsigned char *>(msg.data()),
                       msg.size()) == 1) {
      sig.assign(reinterpret_cast<char *>(buf.data()), slen);
    }
  }
  if (ctx != nullptr) EVP_MD_CTX_free(ctx);
  EVP_PKEY_free(pkey);
  return sig;
}

/**
 * Perform the service-account OAuth2 JWT-bearer flow to obtain an access token.
 *
 * Builds a signed RS256 JWT asserting the devstorage.read_write scope, POSTs it
 * to the token endpoint, and returns the access_token. This is the real GCS
 * auth path; the prototype validates anonymously, so this runs only when
 * GOOGLE_APPLICATION_CREDENTIALS points at a key file.
 *
 * @param sa_json_path Path to the service-account JSON key file.
 * @return The OAuth2 access token, or "" on any failure.
 */
static std::string FetchTokenViaJwt(const std::string &sa_json_path) {
  std::ifstream f(sa_json_path);
  if (!f) {
    HLOG(kError, "GCS auth: cannot open credentials file '{}'", sa_json_path);
    return "";
  }
  std::stringstream ss;
  ss << f.rdbuf();
  std::string json = ss.str();
  std::string client_email = ExtractJsonString(json, "client_email");
  std::string private_key = ExtractJsonString(json, "private_key");
  std::string token_uri = ExtractJsonString(json, "token_uri");
  if (token_uri.empty()) token_uri = "https://oauth2.googleapis.com/token";
  if (client_email.empty() || private_key.empty()) {
    HLOG(kError, "GCS auth: credentials file missing client_email/private_key");
    return "";
  }

  std::time_t now = std::time(nullptr);
  std::string header = R"({"alg":"RS256","typ":"JWT"})";
  std::ostringstream claims;
  claims << "{\"iss\":\"" << client_email
         << "\",\"scope\":\"https://www.googleapis.com/auth/devstorage.read_write"
         << "\",\"aud\":\"" << token_uri << "\",\"iat\":" << now
         << ",\"exp\":" << (now + 3600) << "}";
  std::string signing_input = Base64Url(header) + "." + Base64Url(claims.str());
  std::string sig = RsaSignSha256(private_key, signing_input);
  if (sig.empty()) return "";
  std::string assertion = signing_input + "." + Base64Url(sig);

  std::string body = "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer"
                     "&assertion=" + assertion;
  CURL *easy = curl_easy_init();
  if (easy == nullptr) return "";
  std::string resp;
  curl_easy_setopt(easy, CURLOPT_URL, token_uri.c_str());
  curl_easy_setopt(easy, CURLOPT_POSTFIELDS, body.c_str());
  curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, StringWriteCb);
  curl_easy_setopt(easy, CURLOPT_WRITEDATA, &resp);
  CURLcode rc = curl_easy_perform(easy);
  long status = 0;
  curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &status);
  curl_easy_cleanup(easy);
  if (rc != CURLE_OK || status != 200) {
    HLOG(kError, "GCS auth: token exchange failed (curl={} http={})",
         static_cast<int>(rc), status);
    return "";
  }
  std::string token = ExtractJsonString(resp, "access_token");
  if (!token.empty()) HLOG(kInfo, "GCS auth: obtained OAuth2 access token");
  return token;
}

/**
 * Resolve a bearer token by precedence: explicit env token, then a
 * service-account JWT exchange, then anonymous ("").
 * @return The bearer token, or "" for anonymous access.
 */
static std::string ResolveAccessToken() {
  std::string explicit_tok = EnvOr("GCS_ACCESS_TOKEN", "");
  if (!explicit_tok.empty()) {
    HLOG(kInfo, "GCS auth: using GCS_ACCESS_TOKEN from environment");
    return explicit_tok;
  }
  std::string sa = EnvOr("GOOGLE_APPLICATION_CREDENTIALS", "");
  if (!sa.empty()) return FetchTokenViaJwt(sa);
  HLOG(kInfo, "GCS auth: no credentials set — using anonymous access");
  return "";
}

//===========================================================================
// GcsConfig
//===========================================================================

GcsConfig GcsConfig::FromEnvAndPoolName(const std::string &pool_name) {
  GcsConfig cfg;
  const std::string scheme_pfx = "gcs://";
  if (pool_name.rfind(scheme_pfx, 0) != 0) {
    HLOG(kError, "GCS pool_name '{}' must start with 'gcs://'", pool_name);
    return cfg;
  }
  std::string rest = pool_name.substr(scheme_pfx.size());
  size_t slash = rest.find('/');
  cfg.bucket = (slash == std::string::npos) ? rest : rest.substr(0, slash);
  cfg.prefix = (slash == std::string::npos) ? "" : rest.substr(slash + 1);
  while (!cfg.prefix.empty() && cfg.prefix.back() == '/') cfg.prefix.pop_back();
  if (cfg.bucket.empty()) {
    HLOG(kError, "GCS pool_name '{}' has an empty bucket", pool_name);
    return cfg;
  }

  cfg.endpoint = EnvOr("GCS_ENDPOINT", "https://storage.googleapis.com");
  cfg.project_id = EnvOr("GCS_PROJECT_ID", "clio-prototype");
  cfg.access_token = ResolveAccessToken();

  // Split endpoint into scheme + host[:port].
  size_t pos = cfg.endpoint.find("://");
  if (pos == std::string::npos) {
    cfg.scheme = "https";
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
// GcsOp + libcurl callbacks
//===========================================================================

/** Per-request state for one in-flight async operation. */
struct GcsClient::GcsOp {
  CURL *easy = nullptr;            /**< Owning curl easy handle */
  curl_slist *headers = nullptr;   /**< Request headers (owned) */
  bool is_get = false;            /**< true for download, false for upload */
  bool finished = false;          /**< Set when the transfer completes */
  CURLcode curl_code = CURLE_OK;  /**< Transport-level result */
  std::string key;                /**< Object name (for logging) */
  // Upload source
  const char *src = nullptr;
  size_t len = 0;
  size_t sent = 0;
  // Download destination
  char *dst = nullptr;
  size_t cap = 0;
  size_t written = 0;
  std::chrono::high_resolution_clock::time_point start;
};

/** libcurl read callback: stream the upload body out of op->src. */
static size_t GcsReadCb(char *ptr, size_t size, size_t nmemb, void *userdata) {
  auto *op = static_cast<GcsClient::GcsOp *>(userdata);
  size_t want = size * nmemb;
  size_t remaining = op->len - op->sent;
  size_t n = std::min(want, remaining);
  if (n > 0) {
    std::memcpy(ptr, op->src + op->sent, n);
    op->sent += n;
  }
  return n;
}

/** libcurl write callback: capture the download body into op->dst (bounded). */
static size_t GcsWriteCb(char *ptr, size_t size, size_t nmemb, void *userdata) {
  auto *op = static_cast<GcsClient::GcsOp *>(userdata);
  size_t n = size * nmemb;
  size_t space = (op->written < op->cap) ? (op->cap - op->written) : 0;
  size_t c = std::min(n, space);
  if (c > 0) {
    std::memcpy(op->dst + op->written, ptr, c);
    op->written += c;
  }
  return n;  // Always consume so curl doesn't abort on overflow.
}

/** libcurl write callback that discards the body (upload JSON response). */
static size_t GcsDiscardCb(char *, size_t size, size_t nmemb, void *) {
  return size * nmemb;
}

//===========================================================================
// GcsClient lifecycle
//===========================================================================

GcsClient::GcsClient(const GcsConfig &config) : config_(config) {
  if (!config_.valid) return;
  multi_ = curl_multi_init();
  if (multi_ == nullptr) {
    HLOG(kError, "Failed to create curl multi handle for GCS client");
  }
}

GcsClient::~GcsClient() {
  for (auto &kv : inflight_) {
    GcsOp *op = kv.second;
    if (multi_ && op->easy) curl_multi_remove_handle(multi_, op->easy);
    if (op->easy) curl_easy_cleanup(op->easy);
    if (op->headers) curl_slist_free_all(op->headers);
    delete op;
  }
  inflight_.clear();
  if (multi_) curl_multi_cleanup(multi_);
}

std::string GcsClient::KeyForOffset(uint64_t offset) const {
  std::string key = "blk_" + std::to_string(offset);
  return config_.prefix.empty() ? key : (config_.prefix + "/" + key);
}

curl_slist *GcsClient::BuildHeaders(const std::string &content_type) const {
  curl_slist *headers = nullptr;
  if (!config_.access_token.empty()) {
    headers = curl_slist_append(
        headers, ("Authorization: Bearer " + config_.access_token).c_str());
  }
  if (!content_type.empty()) {
    headers =
        curl_slist_append(headers, ("Content-Type: " + content_type).c_str());
  }
  headers = curl_slist_append(headers, "Expect:");  // suppress 100-continue
  return headers;
}

//===========================================================================
// Bucket creation (synchronous)
//===========================================================================

bool GcsClient::EnsureBucket() {
  if (!IsValid()) return false;
  CURL *easy = curl_easy_init();
  if (easy == nullptr) return false;
  std::string url = config_.endpoint + "/storage/v1/b?project=" +
                    UrlEncode(config_.project_id);
  std::string body = "{\"name\":\"" + config_.bucket + "\"}";
  curl_slist *headers = BuildHeaders("application/json");
  curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(easy, CURLOPT_POSTFIELDS, body.c_str());
  curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, GcsDiscardCb);

  CURLcode rc = curl_easy_perform(easy);
  long status = 0;
  curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &status);
  curl_slist_free_all(headers);
  curl_easy_cleanup(easy);

  bool ok = (rc == CURLE_OK) && (status == 200 || status == 409);
  if (!ok) {
    HLOG(kError, "GCS EnsureBucket '{}' failed: curl={} http={}", config_.bucket,
         static_cast<int>(rc), status);
  } else {
    HLOG(kInfo, "GCS bucket '{}' ready (http={})", config_.bucket, status);
  }
  return ok;
}

//===========================================================================
// Async submit + poll
//===========================================================================

void *GcsClient::PutAsync(const std::string &key, const void *buf, size_t len) {
  if (!IsValid()) return nullptr;
  auto *op = new GcsOp();
  op->is_get = false;
  op->key = key;
  op->src = static_cast<const char *>(buf);
  op->len = len;
  op->start = std::chrono::high_resolution_clock::now();
  op->easy = curl_easy_init();
  if (op->easy == nullptr) {
    delete op;
    return nullptr;
  }
  // Simple media upload: POST .../o?uploadType=media&name=<encoded key>.
  std::string url = config_.endpoint + "/upload/storage/v1/b/" + config_.bucket +
                    "/o?uploadType=media&name=" + UrlEncode(key);
  op->headers = BuildHeaders("application/octet-stream");
  curl_easy_setopt(op->easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(op->easy, CURLOPT_HTTPHEADER, op->headers);
  curl_easy_setopt(op->easy, CURLOPT_POST, 1L);
  curl_easy_setopt(op->easy, CURLOPT_POSTFIELDSIZE_LARGE,
                   static_cast<curl_off_t>(len));
  curl_easy_setopt(op->easy, CURLOPT_READFUNCTION, GcsReadCb);
  curl_easy_setopt(op->easy, CURLOPT_READDATA, op);
  curl_easy_setopt(op->easy, CURLOPT_WRITEFUNCTION, GcsDiscardCb);
  curl_multi_add_handle(multi_, op->easy);
  inflight_[op->easy] = op;
  return op;
}

void *GcsClient::GetAsync(const std::string &key, void *buf, size_t cap) {
  if (!IsValid()) return nullptr;
  auto *op = new GcsOp();
  op->is_get = true;
  op->key = key;
  op->dst = static_cast<char *>(buf);
  op->cap = cap;
  op->start = std::chrono::high_resolution_clock::now();
  op->easy = curl_easy_init();
  if (op->easy == nullptr) {
    delete op;
    return nullptr;
  }
  // Media download: GET .../o/<encoded key>?alt=media.
  std::string url = config_.endpoint + "/storage/v1/b/" + config_.bucket +
                    "/o/" + UrlEncode(key) + "?alt=media";
  op->headers = BuildHeaders("");
  curl_easy_setopt(op->easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(op->easy, CURLOPT_HTTPHEADER, op->headers);
  curl_easy_setopt(op->easy, CURLOPT_HTTPGET, 1L);
  curl_easy_setopt(op->easy, CURLOPT_WRITEFUNCTION, GcsWriteCb);
  curl_easy_setopt(op->easy, CURLOPT_WRITEDATA, op);
  curl_multi_add_handle(multi_, op->easy);
  inflight_[op->easy] = op;
  return op;
}

bool GcsClient::IsComplete(void *token, GcsResult &out) {
  auto *target = static_cast<GcsOp *>(token);
  if (target == nullptr || multi_ == nullptr) return true;

  int running = 0;
  curl_multi_perform(multi_, &running);

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

  HLOG(kInfo, "Gcs op={} key={} bytes={} http={} latency_ms={}",
       target->is_get ? "GET" : "PUT", target->key, out.bytes, status, ms);
  if (target->curl_code != CURLE_OK) {
    HLOG(kError, "Gcs op={} key={} transport error curl={}",
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
