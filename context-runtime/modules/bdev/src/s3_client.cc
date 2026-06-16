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

#include <chrono>
#include <cstring>
#include <ctime>
#include <string>
#include <vector>

#include "clio_ctp/util/logging.h"
#include "clio_runtime/bdev/cloud_crypto.h"

namespace clio::run::bdev {

//===========================================================================
// S3Config
//===========================================================================

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

  cfg.endpoint = cloud::EnvOr("S3_ENDPOINT", cloud::EnvOr("AWS_ENDPOINT_URL", ""));
  cfg.region = cloud::EnvOr("AWS_REGION", "us-east-1");
  cfg.access_key = cloud::EnvOr("AWS_ACCESS_KEY_ID", "");
  cfg.secret_key = cloud::EnvOr("AWS_SECRET_ACCESS_KEY", "");
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
// S3Client lifecycle
//===========================================================================

S3Client::S3Client(const S3Config &config) : config_(config) {}

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
      cloud::Sha256Hex(canonical_request.data(), canonical_request.size());

  // Derive the signing key, then sign the string-to-sign.
  std::string k0 = "AWS4" + config_.secret_key;
  std::vector<unsigned char> k_date = cloud::HmacSha256(
      reinterpret_cast<const unsigned char *>(k0.data()), k0.size(), datestamp,
      std::strlen(datestamp));
  std::vector<unsigned char> k_region = cloud::HmacSha256(
      k_date.data(), k_date.size(), config_.region.data(), config_.region.size());
  std::vector<unsigned char> k_service =
      cloud::HmacSha256(k_region.data(), k_region.size(), "s3", 2);
  std::vector<unsigned char> k_signing =
      cloud::HmacSha256(k_service.data(), k_service.size(), "aws4_request", 12);
  std::vector<unsigned char> sig = cloud::HmacSha256(
      k_signing.data(), k_signing.size(), string_to_sign.data(),
      string_to_sign.size());
  std::string signature = cloud::HexEncode(sig.data(), sig.size());

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

bool S3Client::NewSignedHandle(Op *op, const std::string &method,
                               const std::string &payload_hash) {
  op->easy = curl_easy_init();
  if (op->easy == nullptr) return false;
  std::string enc_key = cloud::AwsUriEncode(op->key, /*encode_slash=*/false);
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

bool S3Client::Bootstrap(uint64_t /*capacity*/) { return EnsureBucket(); }

bool S3Client::EnsureBucket() {
  if (!IsValid()) return false;
  CURL *easy = curl_easy_init();
  if (easy == nullptr) return false;
  std::string canonical_uri = "/" + config_.bucket;
  std::string url = config_.scheme + "://" + config_.host + canonical_uri;
  curl_slist *headers =
      BuildSignedHeaders("PUT", canonical_uri, cloud::kEmptyPayloadSha256);
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
// Async submit (offset-oriented; poll via the shared base IsComplete)
//===========================================================================

void *S3Client::WriteAsync(uint64_t offset, const void *buf, size_t len) {
  if (!IsValid()) return nullptr;
  auto *op = new Op();
  op->is_get = false;
  op->op_label = "PUT";
  op->key = KeyForOffset(offset);
  op->src = static_cast<const char *>(buf);
  op->len = len;
  op->start = std::chrono::high_resolution_clock::now();
  std::string payload_hash = cloud::Sha256Hex(buf, len);
  if (!NewSignedHandle(op, "PUT", payload_hash)) {
    delete op;
    return nullptr;
  }
  curl_easy_setopt(op->easy, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(op->easy, CURLOPT_READFUNCTION, &HttpObjectStoreClient::ReadCb);
  curl_easy_setopt(op->easy, CURLOPT_READDATA, op);
  curl_easy_setopt(op->easy, CURLOPT_INFILESIZE_LARGE,
                   static_cast<curl_off_t>(len));
  return Submit(op);
}

void *S3Client::ReadAsync(uint64_t offset, void *buf, size_t len) {
  if (!IsValid()) return nullptr;
  auto *op = new Op();
  op->is_get = true;
  op->op_label = "GET";
  op->key = KeyForOffset(offset);
  op->dst = static_cast<char *>(buf);
  op->cap = len;
  op->start = std::chrono::high_resolution_clock::now();
  if (!NewSignedHandle(op, "GET", cloud::kEmptyPayloadSha256)) {
    delete op;
    return nullptr;
  }
  curl_easy_setopt(op->easy, CURLOPT_HTTPGET, 1L);
  curl_easy_setopt(op->easy, CURLOPT_WRITEFUNCTION, &HttpObjectStoreClient::WriteCb);
  curl_easy_setopt(op->easy, CURLOPT_WRITEDATA, op);
  return Submit(op);
}

}  // namespace clio::run::bdev
