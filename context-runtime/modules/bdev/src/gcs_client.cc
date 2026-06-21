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

#include <cctype>
#include <chrono>
#include <cstdlib>
#include <string>

#include "clio_ctp/util/logging.h"
#include "clio_runtime/bdev/cloud_crypto.h"
#include "clio_runtime/bdev/gcs_credentials.h"
#include "clio_runtime/bdev/gcs_retry.h"

namespace clio::run::bdev {

//===========================================================================
// Encoding helpers (GCS-specific; shared crypto lives in cloud_crypto.h, and
// the OAuth2/JWT auth path lives in gcs_credentials.h)
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

/** libcurl write callback that discards the body (upload JSON response). */
static size_t GcsDiscardCb(char *, size_t size, size_t nmemb, void *) {
  return size * nmemb;
}

/** libcurl header callback that captures the `Location:` value (session URI). */
static size_t CaptureLocationCb(char *ptr, size_t size, size_t nmemb,
                                void *ud) {
  size_t n = size * nmemb;
  auto *out = static_cast<std::string *>(ud);
  const std::string key = "location:";
  if (n >= key.size()) {
    std::string head(ptr, key.size());
    for (char &c : head) c = static_cast<char>(std::tolower((unsigned char)c));
    if (head == key) {
      std::string val(ptr + key.size(), n - key.size());
      size_t b = val.find_first_not_of(" \t");
      size_t e = val.find_last_not_of("\r\n \t");
      if (b != std::string::npos && e != std::string::npos && e >= b) {
        *out = val.substr(b, e - b + 1);
      }
    }
  }
  return n;
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

  cfg.endpoint = cloud::EnvOr("GCS_ENDPOINT", "https://storage.googleapis.com");
  cfg.project_id = cloud::EnvOr("GCS_PROJECT_ID", "clio-prototype");
  // Resolve the credential source once and bind the process-wide shared token
  // provider for it (token state is shared across workers; see X8).
  cfg.token_provider = GetSharedGcsTokenProvider(ResolveGcsCredentials());

  // Transport + resumable knobs (resolved once at Create).
  cfg.resumable_threshold = static_cast<size_t>(std::strtoull(
      cloud::EnvOr("GCS_RESUMABLE_THRESHOLD", "8388608").c_str(), nullptr, 10));
  cfg.connect_timeout_s = std::strtol(
      cloud::EnvOr("GCS_CONNECT_TIMEOUT_S", "10").c_str(), nullptr, 10);
  cfg.http2 = cloud::EnvOr("GCS_FORCE_HTTP11", "").empty();
  cfg.ca_bundle = cloud::EnvOr("GCS_CA_BUNDLE", "");

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
// GcsClient lifecycle
//===========================================================================

GcsClient::GcsClient(const GcsConfig &config) : config_(config) {
  // Opt into the transport base's transient-failure retry loop. The base
  // default is a single attempt (no-op), so only GCS gets backoff retries.
  retry_policy_ = RetryPolicy::FromEnv();
}

void GcsClient::OnAuthError() {
  // A 401/403 means the bearer token expired or was revoked; drop it so the
  // shared provider re-mints one for the retried request (theme X8).
  if (config_.token_provider) config_.token_provider->Invalidate();
}

std::string GcsClient::KeyForOffset(uint64_t offset) const {
  std::string key = "blk_" + std::to_string(offset);
  return config_.prefix.empty() ? key : (config_.prefix + "/" + key);
}

curl_slist *GcsClient::BuildHeaders(const std::string &content_type) const {
  curl_slist *headers = nullptr;
  if (config_.token_provider) {
    std::string token = config_.token_provider->GetToken();
    if (!token.empty()) {
      headers = curl_slist_append(
          headers, ("Authorization: Bearer " + token).c_str());
    }
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

bool GcsClient::Bootstrap(uint64_t /*capacity*/) { return EnsureBucket(); }

bool GcsClient::EnsureBucket() {
  if (!IsValid()) return false;
  CURL *easy = curl_easy_init();
  if (easy == nullptr) return false;
  std::string url = config_.endpoint + "/storage/v1/b?project=" +
                    UrlEncode(config_.project_id);
  std::string body = "{\"name\":\"" + config_.bucket + "\"}";
  curl_slist *headers = BuildHeaders("application/json");
  ApplyTransportOpts(easy);
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
// Async submit (offset-oriented; poll via the shared base IsComplete)
//===========================================================================

void GcsClient::ApplyTransportOpts(CURL *easy) const {
  // Prefer HTTP/2 over TLS (curl transparently falls back to 1.1 if the server
  // declines); allow forcing 1.1 for misbehaving proxies.
  curl_easy_setopt(easy, CURLOPT_HTTP_VERSION,
                   config_.http2 ? CURL_HTTP_VERSION_2TLS
                                 : CURL_HTTP_VERSION_1_1);
  curl_easy_setopt(easy, CURLOPT_TCP_KEEPALIVE, 1L);
  curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT, config_.connect_timeout_s);
  // Stall guard: abort a transfer stuck below 1 B/s for 60 s (catches hangs
  // without killing slow-but-progressing transfers).
  curl_easy_setopt(easy, CURLOPT_LOW_SPEED_LIMIT, 1L);
  curl_easy_setopt(easy, CURLOPT_LOW_SPEED_TIME, 60L);
  // TLS peer + host verification on (curl's default; set explicitly).
  curl_easy_setopt(easy, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(easy, CURLOPT_SSL_VERIFYHOST, 2L);
  if (!config_.ca_bundle.empty()) {
    curl_easy_setopt(easy, CURLOPT_CAINFO, config_.ca_bundle.c_str());
  }
}

std::string GcsClient::InitiateResumable(const std::string &key,
                                         size_t len) const {
  CURL *easy = curl_easy_init();
  if (easy == nullptr) return "";
  std::string url = config_.endpoint + "/upload/storage/v1/b/" + config_.bucket +
                    "/o?uploadType=resumable&name=" + UrlEncode(key);
  curl_slist *headers = BuildHeaders("application/octet-stream");
  headers = curl_slist_append(
      headers, ("X-Upload-Content-Length: " + std::to_string(len)).c_str());
  headers = curl_slist_append(headers,
                              "X-Upload-Content-Type: application/octet-stream");
  std::string location;
  ApplyTransportOpts(easy);
  curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(easy, CURLOPT_POST, 1L);
  curl_easy_setopt(easy, CURLOPT_POSTFIELDS, "");
  curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE, 0L);
  curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, CaptureLocationCb);
  curl_easy_setopt(easy, CURLOPT_HEADERDATA, &location);
  curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, GcsDiscardCb);
  CURLcode rc = curl_easy_perform(easy);
  long status = 0;
  curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &status);
  curl_slist_free_all(headers);
  curl_easy_cleanup(easy);
  if (rc != CURLE_OK || status != 200 || location.empty()) {
    HLOG(kError, "GCS resumable initiate '{}' failed: curl={} http={}", key,
         static_cast<int>(rc), status);
    return "";
  }
  return location;
}

void GcsClient::ConfigureWriteEasy(Op *op) const {
  op->easy = curl_easy_init();
  if (op->easy == nullptr) return;
  // Large writes use a resumable session (one extra round-trip); small/block
  // writes (the common bdev case) use a single simple media upload.
  if (op->len >= config_.resumable_threshold) {
    std::string uri = InitiateResumable(op->key, op->len);
    if (!uri.empty()) {
      op->op_label = "PUT-RESUMABLE";
      op->headers = BuildHeaders("application/octet-stream");
      std::string range = "Content-Range: bytes 0-" +
                          std::to_string(op->len - 1) + "/" +
                          std::to_string(op->len);
      op->headers = curl_slist_append(op->headers, range.c_str());
      ApplyTransportOpts(op->easy);
      curl_easy_setopt(op->easy, CURLOPT_URL, uri.c_str());
      curl_easy_setopt(op->easy, CURLOPT_HTTPHEADER, op->headers);
      curl_easy_setopt(op->easy, CURLOPT_UPLOAD, 1L);
      curl_easy_setopt(op->easy, CURLOPT_INFILESIZE_LARGE,
                       static_cast<curl_off_t>(op->len));
      curl_easy_setopt(op->easy, CURLOPT_READFUNCTION,
                       &HttpObjectStoreClient::ReadCb);
      curl_easy_setopt(op->easy, CURLOPT_READDATA, op);
      curl_easy_setopt(op->easy, CURLOPT_WRITEFUNCTION, GcsDiscardCb);
      return;
    }
    HLOG(kWarning, "GCS resumable initiate failed for {}; using media upload",
         op->key);
  }
  // Simple media upload: POST .../o?uploadType=media&name=<encoded key>.
  op->op_label = "PUT";
  std::string url = config_.endpoint + "/upload/storage/v1/b/" + config_.bucket +
                    "/o?uploadType=media&name=" + UrlEncode(op->key);
  op->headers = BuildHeaders("application/octet-stream");
  ApplyTransportOpts(op->easy);
  curl_easy_setopt(op->easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(op->easy, CURLOPT_HTTPHEADER, op->headers);
  curl_easy_setopt(op->easy, CURLOPT_POST, 1L);
  curl_easy_setopt(op->easy, CURLOPT_POSTFIELDSIZE_LARGE,
                   static_cast<curl_off_t>(op->len));
  curl_easy_setopt(op->easy, CURLOPT_READFUNCTION,
                   &HttpObjectStoreClient::ReadCb);
  curl_easy_setopt(op->easy, CURLOPT_READDATA, op);
  curl_easy_setopt(op->easy, CURLOPT_WRITEFUNCTION, GcsDiscardCb);
}

void GcsClient::ConfigureReadEasy(Op *op) const {
  op->easy = curl_easy_init();
  if (op->easy == nullptr) return;
  // Media download: GET .../o/<encoded key>?alt=media.
  std::string url = config_.endpoint + "/storage/v1/b/" + config_.bucket +
                    "/o/" + UrlEncode(op->key) + "?alt=media";
  op->headers = BuildHeaders("");
  ApplyTransportOpts(op->easy);
  curl_easy_setopt(op->easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(op->easy, CURLOPT_HTTPHEADER, op->headers);
  curl_easy_setopt(op->easy, CURLOPT_HTTPGET, 1L);
  curl_easy_setopt(op->easy, CURLOPT_WRITEFUNCTION,
                   &HttpObjectStoreClient::WriteCb);
  curl_easy_setopt(op->easy, CURLOPT_WRITEDATA, op);
}

void *GcsClient::WriteAsync(uint64_t offset, const void *buf, size_t len) {
  if (!IsValid()) return nullptr;
  auto *op = new Op();
  op->is_get = false;
  op->op_label = "PUT";
  op->key = KeyForOffset(offset);
  op->src = static_cast<const char *>(buf);
  op->len = len;
  op->start = std::chrono::high_resolution_clock::now();
  // Closure re-runs on every retry so a re-issued request gets a fresh token.
  op->rebuild = [this](Op *o) { ConfigureWriteEasy(o); };
  op->rebuild(op);
  if (op->easy == nullptr) {
    delete op;
    return nullptr;
  }
  return Submit(op);
}

void *GcsClient::ReadAsync(uint64_t offset, void *buf, size_t len) {
  if (!IsValid()) return nullptr;
  auto *op = new Op();
  op->is_get = true;
  op->op_label = "GET";
  op->key = KeyForOffset(offset);
  op->dst = static_cast<char *>(buf);
  op->cap = len;
  op->start = std::chrono::high_resolution_clock::now();
  op->rebuild = [this](Op *o) { ConfigureReadEasy(o); };
  op->rebuild(op);
  if (op->easy == nullptr) {
    delete op;
    return nullptr;
  }
  return Submit(op);
}

}  // namespace clio::run::bdev
