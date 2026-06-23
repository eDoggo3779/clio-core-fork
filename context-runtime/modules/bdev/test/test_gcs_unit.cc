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

// Offline unit tests for the GCS adapter's pure logic — the JWT/RS256 signer,
// the JSON parsers, the ADC credential-resolution precedence, and the retry
// policy. These run with NO network and NO runtime init (unlike the trace test
// in test_bdev_gcs.cc), so they always execute and are the primary correctness
// proof for the auth/retry code that real GCS exercises.

#include <arpa/inet.h>
#include <curl/curl.h>
#include <netinet/in.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "simple_test.h"

#include <clio_runtime/bdev/gcs_client.h>
#include <clio_runtime/bdev/gcs_credentials.h>
#include <clio_runtime/bdev/gcs_retry.h>

using namespace clio::run::bdev;

namespace {

/** Decode a base64url (no-padding) string back to raw bytes (test-only). */
std::vector<unsigned char> B64UrlDecode(const std::string &in) {
  int rev[256];
  for (int i = 0; i < 256; ++i) rev[i] = -1;
  const char *tbl =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  for (int i = 0; i < 64; ++i) rev[static_cast<unsigned char>(tbl[i])] = i;
  std::vector<unsigned char> out;
  uint32_t buf = 0;
  int bits = 0;
  for (unsigned char c : in) {
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

/** Generate a throwaway 2048-bit RSA key; @return the private-key PEM. */
std::string GenRsaPrivatePem(EVP_PKEY **out_pkey) {
  EVP_PKEY *pkey = nullptr;
  EVP_PKEY_CTX *pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
  EVP_PKEY_keygen_init(pctx);
  EVP_PKEY_CTX_set_rsa_keygen_bits(pctx, 2048);
  EVP_PKEY_keygen(pctx, &pkey);
  EVP_PKEY_CTX_free(pctx);
  BIO *bio = BIO_new(BIO_s_mem());
  PEM_write_bio_PrivateKey(bio, pkey, nullptr, nullptr, 0, nullptr, nullptr);
  char *data = nullptr;
  long n = BIO_get_mem_data(bio, &data);
  std::string pem(data, static_cast<size_t>(n));
  BIO_free(bio);
  *out_pkey = pkey;
  return pem;
}

/** Verify an RS256 signature over `msg` with the given key. */
bool VerifyRs256(EVP_PKEY *pkey, const std::string &msg,
                 const std::vector<unsigned char> &sig) {
  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  bool ok = EVP_DigestVerifyInit(ctx, nullptr, EVP_sha256(), nullptr, pkey) ==
                1 &&
            EVP_DigestVerify(ctx, sig.data(), sig.size(),
                             reinterpret_cast<const unsigned char *>(msg.data()),
                             msg.size()) == 1;
  EVP_MD_CTX_free(ctx);
  return ok;
}

/** Write `content` to a temp file and return its path. */
std::string WriteTmp(const std::string &name, const std::string &content) {
  std::string path = "/tmp/" + name;
  std::ofstream f(path);
  f << content;
  f.close();
  return path;
}

/** Clear every env var the ADC resolver consults, for hermetic precedence. */
void ClearGcsEnv() {
  unsetenv("GCS_ACCESS_TOKEN");
  unsetenv("GOOGLE_APPLICATION_CREDENTIALS");
  unsetenv("GCE_METADATA_HOST");
  unsetenv("GCS_METADATA_BASE");
  unsetenv("GCS_DISABLE_METADATA");
  unsetenv("CLOUDSDK_CONFIG");
  // Point HOME at a dir with no gcloud ADC file so step 3 is a clean miss.
  setenv("HOME", "/tmp/gcs_unit_no_home", 1);
}

}  // namespace

TEST_CASE("gcs_base64url_vectors", "[gcs][unit][b64url]") {
  // RFC 4648 §10 vectors, base64url with no padding.
  REQUIRE(Base64UrlEncode(std::string("")) == "");
  REQUIRE(Base64UrlEncode(std::string("f")) == "Zg");
  REQUIRE(Base64UrlEncode(std::string("fo")) == "Zm8");
  REQUIRE(Base64UrlEncode(std::string("foo")) == "Zm9v");
  REQUIRE(Base64UrlEncode(std::string("foob")) == "Zm9vYg");
  REQUIRE(Base64UrlEncode(std::string("fooba")) == "Zm9vYmE");
  REQUIRE(Base64UrlEncode(std::string("foobar")) == "Zm9vYmFy");
  // URL-safe alphabet: bytes that map to '+' and '/' in std base64 -> '-','_'.
  const unsigned char raw[] = {0xfb, 0xff, 0xbf};
  REQUIRE(Base64UrlEncode(raw, sizeof(raw)) == "-_-_");
}

TEST_CASE("gcs_jwt_assertion_signs_and_verifies", "[gcs][unit][jwt]") {
  EVP_PKEY *pkey = nullptr;
  std::string pem = GenRsaPrivatePem(&pkey);
  REQUIRE(!pem.empty());

  std::string jwt = BuildJwtAssertion(
      "sa@proj.iam.gserviceaccount.com",
      "https://www.googleapis.com/auth/devstorage.read_write",
      "https://oauth2.googleapis.com/token", 1700000000, 1700003600, pem);
  REQUIRE(!jwt.empty());

  // Three dot-separated segments: header.claims.signature.
  size_t d1 = jwt.find('.');
  size_t d2 = jwt.find('.', d1 + 1);
  REQUIRE(d1 != std::string::npos);
  REQUIRE(d2 != std::string::npos);
  std::string signing_input = jwt.substr(0, d2);
  std::string hdr_b64 = jwt.substr(0, d1);
  std::string claims_b64 = jwt.substr(d1 + 1, d2 - d1 - 1);
  std::string sig_b64 = jwt.substr(d2 + 1);

  // Header decodes to the canonical RS256 JOSE header.
  auto hdr = B64UrlDecode(hdr_b64);
  std::string hdr_str(hdr.begin(), hdr.end());
  REQUIRE(hdr_str == R"({"alg":"RS256","typ":"JWT"})");

  // Claims carry the issuer + audience we asked for.
  auto claims = B64UrlDecode(claims_b64);
  std::string claims_str(claims.begin(), claims.end());
  REQUIRE(claims_str.find("sa@proj.iam.gserviceaccount.com") !=
          std::string::npos);
  REQUIRE(claims_str.find("oauth2.googleapis.com/token") != std::string::npos);

  // Signature verifies against the signing input with the matching key.
  REQUIRE(VerifyRs256(pkey, signing_input, B64UrlDecode(sig_b64)));
  EVP_PKEY_free(pkey);
}

TEST_CASE("gcs_parse_token_response", "[gcs][unit][json]") {
  auto good = ParseTokenResponse(
      R"({"access_token":"ya29.abcDEF","expires_in":3599,"token_type":"Bearer"})");
  REQUIRE(good.ok);
  REQUIRE(good.access_token == "ya29.abcDEF");
  REQUIRE(good.expires_in == 3599);

  auto err = ParseTokenResponse(R"({"error":"invalid_grant"})");
  REQUIRE(!err.ok);

  auto malformed = ParseTokenResponse("{not json");
  REQUIRE(!malformed.ok);
}

TEST_CASE("gcs_parse_service_account_key", "[gcs][unit][json]") {
  // private_key carries escaped newlines, as a real SA key file does.
  std::string json =
      R"({"type":"service_account","client_email":"sa@p.iam.gserviceaccount.com",)"
      R"("private_key":"-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n",)"
      R"("token_uri":"https://oauth2.googleapis.com/token"})";
  auto k = ParseServiceAccountKey(json);
  REQUIRE(k.ok);
  REQUIRE(k.client_email == "sa@p.iam.gserviceaccount.com");
  REQUIRE(k.token_uri == "https://oauth2.googleapis.com/token");
  // The escaped \n must be un-escaped to real newlines for PEM parsing.
  REQUIRE(k.private_key.find('\n') != std::string::npos);
  REQUIRE(k.private_key.find("\\n") == std::string::npos);

  // Missing private_key => not a usable service-account key.
  auto bad = ParseServiceAccountKey(
      R"({"type":"service_account","client_email":"x@y.com"})");
  REQUIRE(!bad.ok);

  // Absent token_uri defaults to the Google endpoint.
  auto defaulted = ParseServiceAccountKey(
      R"({"client_email":"a@b.com","private_key":"PEM"})");
  REQUIRE(defaulted.ok);
  REQUIRE(defaulted.token_uri == "https://oauth2.googleapis.com/token");
}

TEST_CASE("gcs_parse_authorized_user_key", "[gcs][unit][json]") {
  auto k = ParseAuthorizedUserKey(
      R"({"type":"authorized_user","client_id":"cid.apps.googleusercontent.com",)"
      R"("client_secret":"secret","refresh_token":"1//refresh"})");
  REQUIRE(k.ok);
  REQUIRE(k.client_id == "cid.apps.googleusercontent.com");
  REQUIRE(k.refresh_token == "1//refresh");
  REQUIRE(k.token_uri == "https://oauth2.googleapis.com/token");

  auto bad = ParseAuthorizedUserKey(
      R"({"client_id":"cid","client_secret":"s"})");  // no refresh_token
  REQUIRE(!bad.ok);
}

TEST_CASE("gcs_retry_should_retry_truth_table", "[gcs][unit][retry]") {
  RetryPolicy p;
  p.max_attempts = 5;
  // Success / sparse / non-transient client errors are never retried.
  REQUIRE(!p.ShouldRetry(200, CURLE_OK, 0));
  REQUIRE(!p.ShouldRetry(404, CURLE_OK, 0));
  REQUIRE(!p.ShouldRetry(400, CURLE_OK, 0));
  REQUIRE(!p.ShouldRetry(401, CURLE_OK, 0));  // auth handled separately
  REQUIRE(!p.ShouldRetry(403, CURLE_OK, 0));
  // Transient HTTP statuses are retried.
  REQUIRE(p.ShouldRetry(429, CURLE_OK, 0));
  REQUIRE(p.ShouldRetry(500, CURLE_OK, 0));
  REQUIRE(p.ShouldRetry(502, CURLE_OK, 0));
  REQUIRE(p.ShouldRetry(503, CURLE_OK, 0));
  REQUIRE(p.ShouldRetry(504, CURLE_OK, 0));
  // Transient transport errors are retried; permanent ones are not.
  REQUIRE(p.ShouldRetry(0, CURLE_COULDNT_CONNECT, 0));
  REQUIRE(p.ShouldRetry(0, CURLE_OPERATION_TIMEDOUT, 0));
  REQUIRE(!p.ShouldRetry(0, CURLE_UNSUPPORTED_PROTOCOL, 0));
  // Attempt budget is honored (0-based attempt index).
  REQUIRE(p.ShouldRetry(503, CURLE_OK, 3));
  REQUIRE(!p.ShouldRetry(503, CURLE_OK, 4));
  // Default policy is a no-op: a single attempt, never retried.
  RetryPolicy noop;
  REQUIRE(!noop.ShouldRetry(503, CURLE_OK, 0));
}

TEST_CASE("gcs_retry_backoff_ceiling_and_jitter", "[gcs][unit][retry]") {
  RetryPolicy p;
  p.base_ms = 100.0;
  p.max_ms = 30000.0;
  REQUIRE(p.BackoffCeilingMs(1) == 100.0);
  REQUIRE(p.BackoffCeilingMs(2) == 200.0);
  REQUIRE(p.BackoffCeilingMs(3) == 400.0);
  REQUIRE(p.BackoffCeilingMs(4) == 800.0);
  REQUIRE(p.BackoffCeilingMs(40) == 30000.0);  // saturates at the cap

  // Full jitter: every draw lies within [0, ceiling]; spread is non-trivial.
  double ceiling = p.BackoffCeilingMs(5);  // 1600 ms
  double lo = ceiling, hi = 0.0;
  for (int i = 0; i < 2000; ++i) {
    double d = p.NextDelayMs(5);
    REQUIRE(d >= 0.0);
    REQUIRE(d <= ceiling);
    if (d < lo) lo = d;
    if (d > hi) hi = d;
  }
  REQUIRE(hi > lo);          // jitter actually varies
  REQUIRE(hi > ceiling / 2);  // and reaches the upper half
}

TEST_CASE("gcs_transient_curl_classification", "[gcs][unit][retry]") {
  REQUIRE(IsTransientCurlError(CURLE_COULDNT_CONNECT));
  REQUIRE(IsTransientCurlError(CURLE_OPERATION_TIMEDOUT));
  REQUIRE(IsTransientCurlError(CURLE_RECV_ERROR));
  REQUIRE(!IsTransientCurlError(CURLE_OK));
  REQUIRE(!IsTransientCurlError(CURLE_UNSUPPORTED_PROTOCOL));
}

TEST_CASE("gcs_adc_precedence", "[gcs][unit][adc]") {
  // 1. Explicit token wins outright.
  ClearGcsEnv();
  setenv("GCS_ACCESS_TOKEN", "ya29.explicit", 1);
  REQUIRE(ResolveGcsCredentials().kind == GcsCredKind::kExplicitToken);

  // 2. Service-account key file.
  ClearGcsEnv();
  std::string sa = WriteTmp(
      "gcs_unit_sa.json",
      R"({"type":"service_account","client_email":"sa@p.iam.gserviceaccount.com",)"
      R"("private_key":"-----BEGIN PRIVATE KEY-----\nXX\n-----END PRIVATE KEY-----\n"})");
  setenv("GOOGLE_APPLICATION_CREDENTIALS", sa.c_str(), 1);
  {
    auto c = ResolveGcsCredentials();
    REQUIRE(c.kind == GcsCredKind::kServiceAccount);
    REQUIRE(c.client_email == "sa@p.iam.gserviceaccount.com");
  }

  // 3. Authorized-user (gcloud) ADC via GOOGLE_APPLICATION_CREDENTIALS.
  ClearGcsEnv();
  std::string au = WriteTmp(
      "gcs_unit_au.json",
      R"({"type":"authorized_user","client_id":"cid","client_secret":"s",)"
      R"("refresh_token":"1//r"})");
  setenv("GOOGLE_APPLICATION_CREDENTIALS", au.c_str(), 1);
  REQUIRE(ResolveGcsCredentials().kind == GcsCredKind::kAuthorizedUser);

  // 4. Metadata server, forced via the injection knob (no network/probe).
  ClearGcsEnv();
  setenv("GCS_METADATA_BASE", "http://169.254.169.254", 1);
  {
    auto c = ResolveGcsCredentials();
    REQUIRE(c.kind == GcsCredKind::kMetadata);
    REQUIRE(c.metadata_base == "http://169.254.169.254");
  }

  // 5. Nothing configured + metadata probe disabled => anonymous (hermetic).
  ClearGcsEnv();
  setenv("GCS_DISABLE_METADATA", "1", 1);
  REQUIRE(ResolveGcsCredentials().kind == GcsCredKind::kAnonymous);

  // 6. Precedence: explicit token beats a present credentials file.
  ClearGcsEnv();
  setenv("GCS_ACCESS_TOKEN", "ya29.explicit", 1);
  setenv("GOOGLE_APPLICATION_CREDENTIALS", sa.c_str(), 1);
  REQUIRE(ResolveGcsCredentials().kind == GcsCredKind::kExplicitToken);

  ClearGcsEnv();
  std::remove(sa.c_str());
  std::remove(au.c_str());
}

TEST_CASE("gcs_token_provider_anonymous_is_empty", "[gcs][unit][token]") {
  GcsCredentials creds;
  creds.kind = GcsCredKind::kAnonymous;
  GcsTokenProvider provider(creds);
  // Anonymous never blocks on a network refresh and yields an empty token.
  REQUIRE(provider.GetToken().empty());
  provider.Invalidate();
  REQUIRE(provider.GetToken().empty());
}

namespace {

// Minimal loopback HTTP/1.1 server returning a scripted status per connection.
// Drives the transport retry state machine with no external network: each curl
// attempt opens a fresh connection (responses set Connection: close), so the
// Nth attempt receives statuses_[N-1] (then 200 once the script is exhausted).
class FaultServer {
 public:
  explicit FaultServer(std::vector<int> statuses)
      : statuses_(std::move(statuses)) {}

  /** Bind an ephemeral loopback port and start serving. @return true on ok. */
  bool Start() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return false;
    int yes = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) <
        0) {
      return false;
    }
    socklen_t len = sizeof(addr);
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&addr), &len);
    port_ = ntohs(addr.sin_port);
    ::listen(listen_fd_, 8);
    thread_ = std::thread([this] { Serve(); });
    return true;
  }

  /** Stop serving and join the worker thread. */
  void Stop() {
    stop_.store(true);
    if (listen_fd_ >= 0) ::shutdown(listen_fd_, SHUT_RDWR);
    if (thread_.joinable()) thread_.join();
    if (listen_fd_ >= 0) ::close(listen_fd_);
    listen_fd_ = -1;
  }

  int port() const { return port_; }
  int connections() const { return conns_.load(); }

 private:
  void Serve() {
    size_t idx = 0;
    while (!stop_.load()) {
      int c = ::accept(listen_fd_, nullptr, nullptr);
      if (c < 0) {
        if (stop_.load()) break;
        continue;
      }
      // Drain the request so curl's body send completes (avoids RST on close).
      timeval tv{0, 100000};  // 100 ms
      ::setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      char buf[4096];
      while (::recv(c, buf, sizeof(buf), 0) > 0) {
      }
      int status = (idx < statuses_.size()) ? statuses_[idx] : 200;
      ++idx;
      const char *reason = (status / 100 == 2) ? "OK" : "Service Unavailable";
      std::string resp = "HTTP/1.1 " + std::to_string(status) + " " + reason +
                         "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
      (void)::send(c, resp.data(), resp.size(), 0);
      ::shutdown(c, SHUT_WR);
      while (::recv(c, buf, sizeof(buf), 0) > 0) {
      }
      ::close(c);
      ++conns_;
    }
  }

  std::vector<int> statuses_;
  int listen_fd_ = -1;
  int port_ = 0;
  std::atomic<int> conns_{0};
  std::atomic<bool> stop_{false};
  std::thread thread_;
};

/** Build an anonymous GcsClient pointed at a local endpoint for retry tests. */
GcsClient MakeClientForEndpoint(const std::string &endpoint) {
  ClearGcsEnv();
  setenv("GCS_DISABLE_METADATA", "1", 1);  // anonymous, no token network
  setenv("GCS_ENDPOINT", endpoint.c_str(), 1);
  setenv("GCS_MAX_RETRIES", "4", 1);
  setenv("GCS_RETRY_BASE_MS", "1", 1);
  setenv("GCS_RETRY_MAX_MS", "5", 1);
  return GcsClient(GcsConfig::FromEnvAndPoolName("gcs://retry-bucket/p"));
}

/** Poll IsComplete with a wall-clock deadline. @return true if it settled. */
bool PollUntilDone(GcsClient &client, void *tok, ObjectStoreResult &out,
                   int timeout_s) {
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
  while (std::chrono::steady_clock::now() < deadline) {
    if (client.IsComplete(tok, out)) return true;
    std::this_thread::sleep_for(std::chrono::microseconds(200));
  }
  return false;
}

}  // namespace

TEST_CASE("gcs_retry_exhausts_on_connect_refused", "[gcs][unit][retry]") {
  // Port 1 refuses instantly -> every attempt hits CURLE_COULDNT_CONNECT (a
  // transient error). The loop must re-arm up to the budget then terminate with
  // a failure (proves the state machine is bounded and never spins forever).
  GcsClient client = MakeClientForEndpoint("http://127.0.0.1:1");
  REQUIRE(client.IsValid());
  const char data[8] = "abc";
  void *tok = client.WriteAsync(0, data, 3);
  REQUIRE(tok != nullptr);
  ObjectStoreResult out;
  REQUIRE(PollUntilDone(client, tok, out, 10));
  REQUIRE(!out.ok);                 // exhausted retries on a refused connection
  REQUIRE_FALSE(out.not_found);     // a connect failure is not a 404
  ClearGcsEnv();
}

TEST_CASE("gcs_retry_succeeds_after_transient_503", "[gcs][unit][retry]") {
  // Server answers 503, 503, then 200: the write must transparently succeed on
  // the third attempt (proves rebuild re-issues the request and the backoff
  // window resolves to a terminal success).
  FaultServer server({503, 503, 200});
  REQUIRE(server.Start());
  GcsClient client =
      MakeClientForEndpoint("http://127.0.0.1:" + std::to_string(server.port()));
  REQUIRE(client.IsValid());
  const char data[8] = "hello";
  void *tok = client.WriteAsync(0, data, 5);
  REQUIRE(tok != nullptr);
  ObjectStoreResult out;
  REQUIRE(PollUntilDone(client, tok, out, 15));
  REQUIRE(out.ok);                   // reached the 200 after two 503s
  REQUIRE(out.http_status == 200);
  server.Stop();
  REQUIRE(server.connections() == 3);  // exactly three attempts were made
  ClearGcsEnv();
}

TEST_CASE("gcs_retry_auth_refresh_then_success", "[gcs][unit][retry]") {
  // Server answers 401 then 200: the write must invalidate-and-retry once
  // (proves the OnAuthError hook fires and the auth-retry re-issues the
  // request). Anonymous creds keep the retried request token-free; the point is
  // the control flow, not a new token.
  FaultServer server({401, 200});
  REQUIRE(server.Start());
  GcsClient client =
      MakeClientForEndpoint("http://127.0.0.1:" + std::to_string(server.port()));
  REQUIRE(client.IsValid());
  const char data[8] = "world";
  void *tok = client.WriteAsync(0, data, 5);
  REQUIRE(tok != nullptr);
  ObjectStoreResult out;
  REQUIRE(PollUntilDone(client, tok, out, 15));
  REQUIRE(out.ok);
  REQUIRE(out.http_status == 200);
  server.Stop();
  REQUIRE(server.connections() == 2);  // 401 -> one auth-retry -> 200
  ClearGcsEnv();
}

SIMPLE_TEST_MAIN()
