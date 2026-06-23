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

#include <clio_runtime/bdev/gcs_retry.h>

#include <curl/curl.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <random>

#include "clio_runtime/bdev/cloud_crypto.h"

namespace clio::run::bdev {

bool IsTransientCurlError(int curl_code) {
  switch (curl_code) {
    case CURLE_COULDNT_CONNECT:
    case CURLE_OPERATION_TIMEDOUT:
    case CURLE_RECV_ERROR:
    case CURLE_SEND_ERROR:
    case CURLE_GOT_NOTHING:
    case CURLE_COULDNT_RESOLVE_HOST:
    case CURLE_PARTIAL_FILE:
    case CURLE_SSL_CONNECT_ERROR:
      return true;
    default:
      return false;
  }
}

/**
 * Parse a positive integer environment variable, falling back to a default.
 * @param name Variable name.
 * @param def Fallback value when unset/empty/non-numeric.
 * @return The parsed value, or `def`.
 */
static double EnvNum(const char *name, double def) {
  std::string v = cloud::EnvOr(name, "");
  if (v.empty()) return def;
  char *end = nullptr;
  double parsed = std::strtod(v.c_str(), &end);
  return (end != v.c_str() && parsed >= 0.0) ? parsed : def;
}

RetryPolicy RetryPolicy::FromEnv() {
  RetryPolicy p;
  p.max_attempts = std::max(1, static_cast<int>(EnvNum("GCS_MAX_RETRIES", 5)));
  p.base_ms = EnvNum("GCS_RETRY_BASE_MS", 100.0);
  p.max_ms = EnvNum("GCS_RETRY_MAX_MS", 30000.0);
  p.retry_on_auth_err = true;
  return p;
}

bool RetryPolicy::ShouldRetry(long http_status, int curl_code,
                              int attempt) const {
  // No attempts left after the one that just finished (0-based `attempt`).
  if (attempt + 1 >= max_attempts) return false;
  // Transport-level failure: retry the transient class only.
  if (curl_code != CURLE_OK) return IsTransientCurlError(curl_code);
  // HTTP-level transient failures (rate limit + server/gateway errors).
  switch (http_status) {
    case 429:
    case 500:
    case 502:
    case 503:
    case 504:
      return true;
    default:
      return false;
  }
}

double RetryPolicy::BackoffCeilingMs(int attempt) const {
  if (attempt < 1) attempt = 1;
  // base * 2^(attempt-1), saturating at max_ms (and guarding overflow).
  double ceiling = base_ms * std::ldexp(1.0, attempt - 1);
  if (!std::isfinite(ceiling) || ceiling > max_ms) ceiling = max_ms;
  return ceiling < 0.0 ? 0.0 : ceiling;
}

double RetryPolicy::NextDelayMs(int attempt) const {
  double ceiling = BackoffCeilingMs(attempt);
  if (ceiling <= 0.0) return 0.0;
  // Full jitter: uniform in [0, ceiling]. A thread-local engine keeps this
  // cheap and lock-free across the per-worker clients.
  static thread_local std::mt19937_64 rng(std::random_device{}());
  std::uniform_real_distribution<double> dist(0.0, ceiling);
  return dist(rng);
}

}  // namespace clio::run::bdev
