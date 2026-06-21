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

#include <clio_runtime/bdev/gcs_credentials.h>

#include <curl/curl.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

#include <ctime>
#include <fstream>
#include <map>
#include <nlohmann/json.hpp>
#include <sstream>
#include <utility>
#include <vector>

#include "clio_ctp/util/logging.h"
#include "clio_runtime/bdev/cloud_crypto.h"

namespace clio::run::bdev {

namespace {

constexpr const char *kDefaultTokenUri = "https://oauth2.googleapis.com/token";

/** libcurl write callback that appends the response body to a std::string. */
size_t StringWriteCb(char *ptr, size_t size, size_t nmemb, void *ud) {
  auto *s = static_cast<std::string *>(ud);
  s->append(ptr, size * nmemb);
  return size * nmemb;
}

/** Read a whole file into a string ("" if it cannot be opened). */
std::string ReadFile(const std::string &path) {
  std::ifstream f(path);
  if (!f) return "";
  std::stringstream ss;
  ss << f.rdbuf();
  return ss.str();
}

/** Extract a string field from a parsed JSON object (type-checked, safe). */
std::string JStr(const nlohmann::json &j, const char *key) {
  auto it = j.find(key);
  if (it != j.end() && it->is_string()) return it->get<std::string>();
  return "";
}

/** Extract an integer field from a parsed JSON object (0 if absent/non-num). */
long JLong(const nlohmann::json &j, const char *key) {
  auto it = j.find(key);
  if (it != j.end() && it->is_number()) return it->get<long>();
  return 0;
}

/**
 * POST an x-www-form-urlencoded body and capture the response body + status.
 * @return true if the transfer completed at the transport level (any status).
 */
bool HttpPostForm(const std::string &url, const std::string &body,
                  std::string &resp, long &status) {
  CURL *easy = curl_easy_init();
  if (easy == nullptr) return false;
  curl_slist *hdr = curl_slist_append(
      nullptr, "Content-Type: application/x-www-form-urlencoded");
  curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(easy, CURLOPT_HTTPHEADER, hdr);
  curl_easy_setopt(easy, CURLOPT_POSTFIELDS, body.c_str());
  curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
  curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, StringWriteCb);
  curl_easy_setopt(easy, CURLOPT_WRITEDATA, &resp);
  curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT, 10L);
  curl_easy_setopt(easy, CURLOPT_TIMEOUT, 30L);
  CURLcode rc = curl_easy_perform(easy);
  status = 0;
  curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &status);
  curl_slist_free_all(hdr);
  curl_easy_cleanup(easy);
  return rc == CURLE_OK;
}

/**
 * GET a metadata-server URL (adds the required `Metadata-Flavor: Google`).
 * @param timeout_ms Both connect and total timeout (keeps probes from hanging).
 * @return true if the transfer completed at the transport level.
 */
bool HttpGetMetadata(const std::string &url, std::string &resp, long &status,
                     long timeout_ms) {
  CURL *easy = curl_easy_init();
  if (easy == nullptr) return false;
  curl_slist *hdr = curl_slist_append(nullptr, "Metadata-Flavor: Google");
  curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
  curl_easy_setopt(easy, CURLOPT_HTTPHEADER, hdr);
  curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, StringWriteCb);
  curl_easy_setopt(easy, CURLOPT_WRITEDATA, &resp);
  curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT_MS, timeout_ms);
  curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS, timeout_ms);
  CURLcode rc = curl_easy_perform(easy);
  status = 0;
  curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &status);
  curl_slist_free_all(hdr);
  curl_easy_cleanup(easy);
  return rc == CURLE_OK;
}

/** @return The metadata server base URL (honoring GCE_METADATA_HOST / test). */
std::string MetadataBase() {
  std::string host = cloud::EnvOr("GCE_METADATA_HOST", "");
  if (!host.empty()) return "http://" + host;
  std::string base = cloud::EnvOr("GCS_METADATA_BASE", "");
  if (!base.empty()) return base;
  return "http://metadata.google.internal";
}

/** @return true if the metadata server answers a short, bounded probe. */
bool MetadataReachable(const std::string &base) {
  std::string resp;
  long status = 0;
  bool ok = HttpGetMetadata(base + "/computeMetadata/v1/", resp, status, 300);
  return ok && status == 200;
}

/** @return The gcloud ADC well-known credentials file path ("" if no HOME). */
std::string GcloudAdcPath() {
  std::string cfg = cloud::EnvOr("CLOUDSDK_CONFIG", "");
  if (!cfg.empty()) return cfg + "/application_default_credentials.json";
  std::string home = cloud::EnvOr("HOME", "");
  if (home.empty()) return "";
  return home + "/.config/gcloud/application_default_credentials.json";
}

/**
 * Populate a GcsCredentials from a key-file JSON (service-account first, then
 * authorized_user). @return true if either form parsed successfully.
 */
bool FillFromKeyJson(const std::string &json, GcsCredentials &c) {
  GcsServiceAccountKey sa = ParseServiceAccountKey(json);
  if (sa.ok) {
    c.kind = GcsCredKind::kServiceAccount;
    c.client_email = sa.client_email;
    c.private_key = sa.private_key;
    c.token_uri = sa.token_uri;
    HLOG(kInfo, "GCS auth: service-account JWT flow for {}", sa.client_email);
    return true;
  }
  GcsAuthorizedUserKey au = ParseAuthorizedUserKey(json);
  if (au.ok) {
    c.kind = GcsCredKind::kAuthorizedUser;
    c.client_id = au.client_id;
    c.client_secret = au.client_secret;
    c.refresh_token = au.refresh_token;
    c.token_uri = au.token_uri;
    HLOG(kInfo, "GCS auth: authorized_user refresh-token flow");
    return true;
  }
  return false;
}

}  // namespace

//===========================================================================
// Pure helpers
//===========================================================================

std::string Base64UrlEncode(const unsigned char *data, size_t len) {
  static const char *tbl =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  std::string out;
  out.reserve((len + 2) / 3 * 4);
  for (size_t i = 0; i < len; i += 3) {
    unsigned v = data[i] << 16;
    int n = 1;
    if (i + 1 < len) {
      v |= data[i + 1] << 8;
      n = 2;
    }
    if (i + 2 < len) {
      v |= data[i + 2];
      n = 3;
    }
    out.push_back(tbl[(v >> 18) & 0x3F]);
    out.push_back(tbl[(v >> 12) & 0x3F]);
    if (n >= 2) out.push_back(tbl[(v >> 6) & 0x3F]);
    if (n >= 3) out.push_back(tbl[v & 0x3F]);
  }
  return out;
}

std::string Base64UrlEncode(const std::string &s) {
  return Base64UrlEncode(reinterpret_cast<const unsigned char *>(s.data()),
                         s.size());
}

std::string RsaSignSha256(const std::string &pem, const std::string &msg) {
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
      EVP_DigestSign(ctx, nullptr, &slen,
                     reinterpret_cast<const unsigned char *>(msg.data()),
                     msg.size()) == 1) {
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

std::string BuildJwtAssertion(const std::string &client_email,
                              const std::string &scope,
                              const std::string &token_uri, int64_t iat,
                              int64_t exp, const std::string &private_key_pem) {
  std::string header = R"({"alg":"RS256","typ":"JWT"})";
  std::ostringstream claims;
  claims << "{\"iss\":\"" << client_email << "\",\"scope\":\"" << scope
         << "\",\"aud\":\"" << token_uri << "\",\"iat\":" << iat
         << ",\"exp\":" << exp << "}";
  std::string signing_input =
      Base64UrlEncode(header) + "." + Base64UrlEncode(claims.str());
  std::string sig = RsaSignSha256(private_key_pem, signing_input);
  if (sig.empty()) return "";
  return signing_input + "." + Base64UrlEncode(sig);
}

GcsTokenResponse ParseTokenResponse(const std::string &json) {
  GcsTokenResponse r;
  auto j = nlohmann::json::parse(json, nullptr, /*allow_exceptions=*/false);
  if (j.is_discarded() || !j.is_object()) return r;
  r.access_token = JStr(j, "access_token");
  r.expires_in = JLong(j, "expires_in");
  r.ok = !r.access_token.empty();
  return r;
}

GcsServiceAccountKey ParseServiceAccountKey(const std::string &json) {
  GcsServiceAccountKey k;
  auto j = nlohmann::json::parse(json, nullptr, /*allow_exceptions=*/false);
  if (j.is_discarded() || !j.is_object()) return k;
  k.type = JStr(j, "type");
  k.client_email = JStr(j, "client_email");
  k.private_key = JStr(j, "private_key");
  k.token_uri = JStr(j, "token_uri");
  if (k.token_uri.empty()) k.token_uri = kDefaultTokenUri;
  k.ok = !k.client_email.empty() && !k.private_key.empty();
  return k;
}

GcsAuthorizedUserKey ParseAuthorizedUserKey(const std::string &json) {
  GcsAuthorizedUserKey k;
  auto j = nlohmann::json::parse(json, nullptr, /*allow_exceptions=*/false);
  if (j.is_discarded() || !j.is_object()) return k;
  k.client_id = JStr(j, "client_id");
  k.client_secret = JStr(j, "client_secret");
  k.refresh_token = JStr(j, "refresh_token");
  k.token_uri = JStr(j, "token_uri");
  if (k.token_uri.empty()) k.token_uri = kDefaultTokenUri;
  k.ok = !k.client_id.empty() && !k.client_secret.empty() &&
         !k.refresh_token.empty();
  return k;
}

//===========================================================================
// Credential resolution (ADC chain) + identity
//===========================================================================

std::string GcsCredentials::Identity() const {
  switch (kind) {
    case GcsCredKind::kExplicitToken:
      return "explicit";  // only one GCS_ACCESS_TOKEN per process
    case GcsCredKind::kServiceAccount:
      return "sa:" + client_email + ":" + token_uri + ":" + scope;
    case GcsCredKind::kAuthorizedUser:
      return "au:" + client_id + ":" + token_uri;
    case GcsCredKind::kMetadata:
      return "md:" + metadata_base + ":" + scope;
    case GcsCredKind::kAnonymous:
    default:
      return "anon";
  }
}

GcsCredentials ResolveGcsCredentials() {
  GcsCredentials c;
  // 1. Explicit bearer token.
  std::string tok = cloud::EnvOr("GCS_ACCESS_TOKEN", "");
  if (!tok.empty()) {
    c.kind = GcsCredKind::kExplicitToken;
    c.explicit_token = tok;
    HLOG(kInfo, "GCS auth: using GCS_ACCESS_TOKEN (explicit bearer)");
    return c;
  }
  // 2. GOOGLE_APPLICATION_CREDENTIALS key file.
  std::string gac = cloud::EnvOr("GOOGLE_APPLICATION_CREDENTIALS", "");
  if (!gac.empty()) {
    std::string j = ReadFile(gac);
    if (j.empty()) {
      HLOG(kError, "GCS auth: cannot read GOOGLE_APPLICATION_CREDENTIALS '{}'",
           gac);
    } else if (FillFromKeyJson(j, c)) {
      return c;
    }
  }
  // 3. gcloud ADC well-known file.
  std::string adc_path = GcloudAdcPath();
  if (!adc_path.empty()) {
    std::string j = ReadFile(adc_path);
    if (!j.empty() && FillFromKeyJson(j, c)) {
      HLOG(kInfo, "GCS auth: using gcloud ADC file '{}'", adc_path);
      return c;
    }
  }
  // 4. GCE/GKE metadata server. Selected when explicitly pointed at one (env),
  //    else via a short reachability probe so this never hangs off-GCP.
  //    GCS_DISABLE_METADATA skips the probe (but honors an explicit selection).
  std::string base = MetadataBase();
  bool forced = !cloud::EnvOr("GCE_METADATA_HOST", "").empty() ||
                !cloud::EnvOr("GCS_METADATA_BASE", "").empty();
  bool probe_disabled = !cloud::EnvOr("GCS_DISABLE_METADATA", "").empty();
  if (forced || (!probe_disabled && MetadataReachable(base))) {
    c.kind = GcsCredKind::kMetadata;
    c.metadata_base = base;
    HLOG(kInfo, "GCS auth: using GCE/GKE metadata server at {}", base);
    return c;
  }
  // 5. Anonymous.
  c.kind = GcsCredKind::kAnonymous;
  HLOG(kInfo, "GCS auth: no credentials resolved — anonymous access");
  return c;
}

//===========================================================================
// GcsTokenProvider
//===========================================================================

GcsTokenProvider::GcsTokenProvider(GcsCredentials creds)
    : creds_(std::move(creds)) {}

std::string GcsTokenProvider::GetToken() {
  if (creds_.kind == GcsCredKind::kAnonymous) return "";
  std::lock_guard<std::mutex> lock(mu_);
  auto now = std::chrono::steady_clock::now();
  const auto skew = std::chrono::seconds(60);  // refresh this long before expiry
  if (have_token_ && now + skew < expiry_) return token_;
  if (!Refresh()) {
    HLOG(kError, "GCS auth: token refresh failed (kind={})",
         static_cast<int>(creds_.kind));
    return have_token_ ? token_ : "";  // best effort: reuse a stale token
  }
  return token_;
}

void GcsTokenProvider::Invalidate() {
  std::lock_guard<std::mutex> lock(mu_);
  have_token_ = false;
  expiry_ = std::chrono::steady_clock::time_point{};
}

bool GcsTokenProvider::Refresh() {
  auto store = [&](const GcsTokenResponse &r) {
    token_ = r.access_token;
    long ttl = r.expires_in > 0 ? r.expires_in : 3600;
    expiry_ = std::chrono::steady_clock::now() + std::chrono::seconds(ttl);
    have_token_ = true;
  };
  switch (creds_.kind) {
    case GcsCredKind::kExplicitToken: {
      token_ = cloud::EnvOr("GCS_ACCESS_TOKEN", creds_.explicit_token);
      have_token_ = !token_.empty();
      // Lifetime unknown; assume ~1 h and let a 401 force re-read via Invalidate.
      expiry_ = std::chrono::steady_clock::now() + std::chrono::seconds(3600);
      return have_token_;
    }
    case GcsCredKind::kServiceAccount: {
      auto now = static_cast<int64_t>(std::time(nullptr));
      std::string jwt = BuildJwtAssertion(creds_.client_email, creds_.scope,
                                          creds_.token_uri, now, now + 3600,
                                          creds_.private_key);
      if (jwt.empty()) return false;
      std::string body =
          "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=" +
          jwt;
      std::string resp;
      long status = 0;
      if (!HttpPostForm(creds_.token_uri, body, resp, status) || status != 200) {
        HLOG(kError, "GCS auth: SA token exchange failed (http={})", status);
        return false;
      }
      GcsTokenResponse r = ParseTokenResponse(resp);
      if (!r.ok) return false;
      store(r);
      HLOG(kInfo, "GCS auth: obtained SA access token (expires_in={}s)",
           r.expires_in);
      return true;
    }
    case GcsCredKind::kAuthorizedUser: {
      std::string body =
          "client_id=" + cloud::AwsUriEncode(creds_.client_id, true) +
          "&client_secret=" + cloud::AwsUriEncode(creds_.client_secret, true) +
          "&refresh_token=" + cloud::AwsUriEncode(creds_.refresh_token, true) +
          "&grant_type=refresh_token";
      std::string resp;
      long status = 0;
      if (!HttpPostForm(creds_.token_uri, body, resp, status) || status != 200) {
        HLOG(kError, "GCS auth: refresh_token exchange failed (http={})", status);
        return false;
      }
      GcsTokenResponse r = ParseTokenResponse(resp);
      if (!r.ok) return false;
      store(r);
      HLOG(kInfo, "GCS auth: refreshed user access token (expires_in={}s)",
           r.expires_in);
      return true;
    }
    case GcsCredKind::kMetadata: {
      std::string url =
          creds_.metadata_base +
          "/computeMetadata/v1/instance/service-accounts/default/token";
      std::string resp;
      long status = 0;
      if (!HttpGetMetadata(url, resp, status, 3000) || status != 200) {
        HLOG(kError, "GCS auth: metadata token fetch failed (http={})", status);
        return false;
      }
      GcsTokenResponse r = ParseTokenResponse(resp);
      if (!r.ok) return false;
      store(r);
      HLOG(kInfo, "GCS auth: obtained metadata access token (expires_in={}s)",
           r.expires_in);
      return true;
    }
    case GcsCredKind::kAnonymous:
    default:
      token_.clear();
      have_token_ = true;
      expiry_ = std::chrono::steady_clock::now() + std::chrono::hours(24 * 365);
      return true;
  }
}

std::shared_ptr<GcsTokenProvider> GetSharedGcsTokenProvider(
    const GcsCredentials &creds) {
  // Process-wide registry: one provider per credential Identity, shared across
  // all per-worker clients so the token is acquired/refreshed exactly once.
  // Entries are weak_ptr so the provider — and the credential material it
  // copies (e.g. a PEM private key) — is released once the last GcsClient using
  // it is destroyed, rather than accumulating for the life of the process.
  static std::mutex reg_mu;
  static std::map<std::string, std::weak_ptr<GcsTokenProvider>> registry;
  std::lock_guard<std::mutex> lock(reg_mu);
  std::string id = creds.Identity();
  auto it = registry.find(id);
  if (it != registry.end()) {
    if (auto sp = it->second.lock()) return sp;
    registry.erase(it);  // stale entry: the last user was destroyed
  }
  auto provider = std::make_shared<GcsTokenProvider>(creds);
  registry[id] = provider;
  return provider;
}

}  // namespace clio::run::bdev
