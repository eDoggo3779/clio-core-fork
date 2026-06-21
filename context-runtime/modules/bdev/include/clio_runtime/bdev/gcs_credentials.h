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

#ifndef CLIO_BDEV_GCS_CREDENTIALS_H_
#define CLIO_BDEV_GCS_CREDENTIALS_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace clio::run::bdev {

/** Which credential source backs a GcsTokenProvider (ADC precedence order). */
enum class GcsCredKind {
  kAnonymous,       /**< No auth (fake-gcs-server / public objects) */
  kExplicitToken,   /**< GCS_ACCESS_TOKEN used verbatim */
  kServiceAccount,  /**< Service-account JSON key -> RS256 JWT-bearer exchange */
  kAuthorizedUser,  /**< gcloud user ADC -> refresh_token grant */
  kMetadata         /**< GCE/GKE metadata server (also GKE Workload Identity) */
};

/**
 * Resolved credential descriptor — *what* source to authenticate with and the
 * material needed to do so, but not a live token. Produced by
 * ResolveGcsCredentials() once at pool-create time; the actual token is fetched
 * (and refreshed) lazily by GcsTokenProvider. Holding only the source here keeps
 * secrets out of serialized task fields (theme X3) and lets one provider be
 * shared by all per-worker clients that resolve to the same Identity().
 */
struct GcsCredentials {
  GcsCredKind kind = GcsCredKind::kAnonymous;
  std::string explicit_token;  /**< kExplicitToken: the bearer token */
  std::string client_email;    /**< kServiceAccount: SA email (JWT `iss`) */
  std::string private_key;     /**< kServiceAccount: PEM RSA private key */
  std::string token_uri;       /**< SA/AuthorizedUser: OAuth2 token endpoint */
  std::string client_id;       /**< kAuthorizedUser: OAuth client id */
  std::string client_secret;   /**< kAuthorizedUser: OAuth client secret */
  std::string refresh_token;   /**< kAuthorizedUser: long-lived refresh token */
  std::string metadata_base;   /**< kMetadata: metadata server base URL */
  std::string scope =
      "https://www.googleapis.com/auth/devstorage.read_write"; /**< OAuth scope */

  /** @return A stable key identifying this source (for provider sharing). */
  std::string Identity() const;
};

//===========================================================================
// Pure helpers (no network / no shared state) — unit-tested offline.
//===========================================================================

/**
 * Base64url-encode raw bytes (RFC 7515: '+'->'-', '/'->'_', no '=' padding).
 * @param data Input bytes.
 * @param len Number of input bytes.
 * @return The base64url string.
 */
std::string Base64UrlEncode(const unsigned char *data, size_t len);

/** Convenience overload taking a std::string payload. */
std::string Base64UrlEncode(const std::string &s);

/**
 * RSA-SHA256-sign a message with a PEM private key (the RS256 JWT signature).
 * @param pem PEM-encoded RSA private key.
 * @param msg Message bytes to sign (the JWT signing input).
 * @return Raw signature bytes, or "" on failure (bad key / OpenSSL error).
 */
std::string RsaSignSha256(const std::string &pem, const std::string &msg);

/**
 * Build a complete signed RS256 JWT bearer assertion (`hdr.claims.sig`).
 * @param client_email Service-account email (JWT `iss`/`sub`).
 * @param scope Space-delimited OAuth scope(s).
 * @param token_uri Token endpoint (JWT `aud`).
 * @param iat Issued-at epoch seconds.
 * @param exp Expiry epoch seconds.
 * @param private_key_pem PEM RSA private key used to sign.
 * @return The compact JWS, or "" if signing failed.
 */
std::string BuildJwtAssertion(const std::string &client_email,
                              const std::string &scope,
                              const std::string &token_uri, int64_t iat,
                              int64_t exp, const std::string &private_key_pem);

/** Parsed OAuth2 token-endpoint response. */
struct GcsTokenResponse {
  std::string access_token;  /**< The bearer token */
  long expires_in = 0;       /**< Lifetime in seconds (0 if absent) */
  bool ok = false;           /**< True when a non-empty access_token was found */
};

/**
 * Parse a token-endpoint JSON response (`access_token` + `expires_in`).
 * @param json Response body.
 * @return The parsed response (check `.ok`).
 */
GcsTokenResponse ParseTokenResponse(const std::string &json);

/** Parsed service-account JSON key fields used by the JWT-bearer flow. */
struct GcsServiceAccountKey {
  std::string type;          /**< Should be "service_account" */
  std::string client_email;  /**< SA email */
  std::string private_key;   /**< PEM RSA private key (un-escaped) */
  std::string token_uri;     /**< Token endpoint (defaulted if absent) */
  bool ok = false;           /**< True when email + private_key were present */
};

/**
 * Parse a service-account JSON key file's contents.
 * @param json The key file contents.
 * @return The parsed key (check `.ok`).
 */
GcsServiceAccountKey ParseServiceAccountKey(const std::string &json);

/** Parsed gcloud "authorized_user" application-default-credentials JSON. */
struct GcsAuthorizedUserKey {
  std::string client_id;     /**< OAuth client id */
  std::string client_secret; /**< OAuth client secret */
  std::string refresh_token; /**< Long-lived refresh token */
  std::string token_uri;     /**< Token endpoint (defaulted if absent) */
  bool ok = false;           /**< True when all three OAuth fields were present */
};

/**
 * Parse a gcloud ADC "authorized_user" JSON document.
 * @param json The ADC file contents.
 * @return The parsed credential (check `.ok`).
 */
GcsAuthorizedUserKey ParseAuthorizedUserKey(const std::string &json);

/**
 * Resolve the credential source via the Application Default Credentials
 * precedence chain, WITHOUT fetching a token:
 *   1. GCS_ACCESS_TOKEN (verbatim)            -> kExplicitToken
 *   2. GOOGLE_APPLICATION_CREDENTIALS file     -> kServiceAccount / kAuthorizedUser
 *   3. gcloud ADC well-known file              -> kAuthorizedUser / kServiceAccount
 *   4. GCE/GKE metadata server (if reachable)  -> kMetadata
 *   else                                       -> kAnonymous
 * Reads env + files only; the metadata arm does a short reachability probe and
 * is also selectable directly via GCE_METADATA_HOST / GCS_METADATA_BASE so it
 * never hangs off-GCP.
 * @return The resolved credential descriptor.
 */
GcsCredentials ResolveGcsCredentials();

//===========================================================================
// Token provider — shared, TTL-aware, refreshable.
//===========================================================================

/**
 * Caches and refreshes one OAuth2 bearer token for a single credential source.
 *
 * Production resolution of theme X8: a bearer token has a ~1 h TTL and must be
 * acquired once and refreshed before expiry, so a *shared* provider (one per
 * credential Identity) feeds every per-worker GcsClient. The curl transport
 * stays per-worker (theme X4); only this token state is shared and mutex-guarded.
 * GetToken() returns a still-valid token (refreshing under the lock if it is
 * within the skew margin of expiry), and Invalidate() forces a refresh after a
 * 401/403 so an expired token self-heals on the next request.
 */
class GcsTokenProvider {
 public:
  /** @param creds The resolved credential source this provider serves. */
  explicit GcsTokenProvider(GcsCredentials creds);

  /**
   * @return A valid bearer token ("" for anonymous), refreshing if near expiry.
   */
  std::string GetToken();

  /** Mark the cached token stale so the next GetToken() refreshes (on 401). */
  void Invalidate();

  /** @return The credential source descriptor. */
  const GcsCredentials &credentials() const { return creds_; }

 private:
  /** Fetch a fresh token for creds_.kind. Caller must hold mu_. @return ok. */
  bool Refresh();

  GcsCredentials creds_;
  std::mutex mu_;
  std::string token_;
  std::chrono::steady_clock::time_point expiry_{};
  bool have_token_ = false;
};

/**
 * Return the process-wide shared provider for a credential source, creating it
 * on first use. Providers are keyed by GcsCredentials::Identity() so all workers
 * (and all pools) that resolve to the same credentials share one token cache.
 * @param creds The resolved credential descriptor.
 * @return A shared provider (never null).
 */
std::shared_ptr<GcsTokenProvider> GetSharedGcsTokenProvider(
    const GcsCredentials &creds);

}  // namespace clio::run::bdev

#endif  // CLIO_BDEV_GCS_CREDENTIALS_H_
