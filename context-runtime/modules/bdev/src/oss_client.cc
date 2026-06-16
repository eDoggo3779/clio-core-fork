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

#include <algorithm>
#include <chrono>
#include <cstring>
#include <ctime>
#include <vector>

#include "clio_ctp/util/logging.h"
#include "clio_runtime/bdev/cloud_crypto.h"

namespace clio::run::bdev {

//===========================================================================
// OSS V1 string-to-sign (the genuinely OSS-specific signing logic)
// Shared crypto + encoding helpers live in cloud_crypto.h.
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
    std::string name = cloud::ToLower(kv.first);
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
      cloud::HmacSha1(reinterpret_cast<const unsigned char *>(secret.data()),
                      secret.size(), sts.data(), sts.size());
  return cloud::Base64Encode(sig.data(), sig.size());
}

//===========================================================================
// OssConfig
//===========================================================================

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

  cfg.endpoint = cloud::EnvOr("OSS_ENDPOINT", cloud::EnvOr("AWS_ENDPOINT_URL", ""));
  cfg.region = cloud::EnvOr("OSS_REGION", "cn-hangzhou");
  cfg.access_key =
      cloud::EnvOr("OSS_ACCESS_KEY_ID", cloud::EnvOr("AWS_ACCESS_KEY_ID", ""));
  cfg.secret_key =
      cloud::EnvOr("OSS_ACCESS_KEY_SECRET", cloud::EnvOr("AWS_SECRET_ACCESS_KEY", ""));
  std::string sig = cloud::ToLower(cloud::EnvOr("OSS_SIGNATURE", "v1"));
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
// OssClient lifecycle
//===========================================================================

OssClient::OssClient(const OssConfig &config) : config_(config) {}

std::string OssClient::KeyForOffset(uint64_t offset) const {
  std::string key = "blk_" + std::to_string(offset);
  return config_.prefix.empty() ? key : (config_.prefix + "/" + key);
}

//===========================================================================
// OSS V1 signing (native) + SigV4 signing (S3-compat)
//===========================================================================

curl_slist *OssClient::BuildHeadersV1(const std::string &method,
                                      const std::string &canonical_resource) {
  std::string date = cloud::Rfc1123GmtNow();
  // Object PUT/GET here send no Content-MD5, no Content-Type, no x-oss-* hdrs.
  std::string sts =
      OssV1StringToSign(method, "", "", date, {}, canonical_resource);
  std::vector<unsigned char> sig =
      cloud::HmacSha1(reinterpret_cast<const unsigned char *>(config_.secret_key.data()),
                      config_.secret_key.size(), sts.data(), sts.size());
  std::string signature = cloud::Base64Encode(sig.data(), sig.size());
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
      cloud::Sha256Hex(canonical_request.data(), canonical_request.size());

  std::string k0 = "AWS4" + config_.secret_key;
  std::vector<unsigned char> k_date = cloud::HmacSha256(
      reinterpret_cast<const unsigned char *>(k0.data()), k0.size(), datestamp,
      std::strlen(datestamp));
  std::vector<unsigned char> k_region =
      cloud::HmacSha256(k_date.data(), k_date.size(), config_.region.data(),
                        config_.region.size());
  std::vector<unsigned char> k_service =
      cloud::HmacSha256(k_region.data(), k_region.size(), "s3", 2);
  std::vector<unsigned char> k_signing =
      cloud::HmacSha256(k_service.data(), k_service.size(), "aws4_request", 12);
  std::vector<unsigned char> sig =
      cloud::HmacSha256(k_signing.data(), k_signing.size(), string_to_sign.data(),
                        string_to_sign.size());
  std::string signature = cloud::HexEncode(sig.data(), sig.size());

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

bool OssClient::NewSignedHandle(Op *op, const std::string &method,
                                const std::string &payload_hash) {
  op->easy = curl_easy_init();
  if (op->easy == nullptr) return false;
  // Path-style addressing ({endpoint}/{bucket}/{key}) — required by S3
  // emulators and accepted by OSS; virtual-hosted is OSS's production default.
  std::string enc_key = cloud::AwsUriEncode(op->key, /*encode_slash=*/false);
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

bool OssClient::Bootstrap(uint64_t /*capacity*/) { return EnsureBucket(); }

bool OssClient::EnsureBucket() {
  if (!IsValid()) return false;
  CURL *easy = curl_easy_init();
  if (easy == nullptr) return false;
  std::string url = config_.scheme + "://" + config_.host + "/" + config_.bucket;
  curl_slist *headers = nullptr;
  if (config_.signature == OssSignatureVersion::kS3) {
    headers =
        BuildHeadersS3("PUT", "/" + config_.bucket, cloud::kEmptyPayloadSha256);
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
// Async submit (offset-oriented; poll via the shared base IsComplete)
//===========================================================================

void *OssClient::WriteAsync(uint64_t offset, const void *buf, size_t len) {
  if (!IsValid()) return nullptr;
  auto *op = new Op();
  op->is_get = false;
  op->op_label = "PUT";
  op->key = KeyForOffset(offset);
  op->src = static_cast<const char *>(buf);
  op->len = len;
  op->start = std::chrono::high_resolution_clock::now();
  // SigV4 needs the body hash; V1 does not sign the body.
  std::string payload_hash = (config_.signature == OssSignatureVersion::kS3)
                                 ? cloud::Sha256Hex(buf, len)
                                 : "";
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

void *OssClient::ReadAsync(uint64_t offset, void *buf, size_t len) {
  if (!IsValid()) return nullptr;
  auto *op = new Op();
  op->is_get = true;
  op->op_label = "GET";
  op->key = KeyForOffset(offset);
  op->dst = static_cast<char *>(buf);
  op->cap = len;
  op->start = std::chrono::high_resolution_clock::now();
  std::string payload_hash = (config_.signature == OssSignatureVersion::kS3)
                                 ? cloud::kEmptyPayloadSha256
                                 : "";
  if (!NewSignedHandle(op, "GET", payload_hash)) {
    delete op;
    return nullptr;
  }
  curl_easy_setopt(op->easy, CURLOPT_HTTPGET, 1L);
  curl_easy_setopt(op->easy, CURLOPT_WRITEFUNCTION, &HttpObjectStoreClient::WriteCb);
  curl_easy_setopt(op->easy, CURLOPT_WRITEDATA, op);
  return Submit(op);
}

}  // namespace clio::run::bdev
