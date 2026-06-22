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

#ifndef CLIO_BDEV_GCS_RETRY_H_
#define CLIO_BDEV_GCS_RETRY_H_

#include <cstdint>
#include <string>

namespace clio::run::bdev {

/**
 * Transient-failure retry policy for HTTP object-store operations.
 *
 * Cloud object stores return transient failures — HTTP 429 (rate limited) and
 * 5xx (server/gateway), plus network-level curl errors — that a well-behaved
 * client retries with exponential backoff + jitter rather than surfacing as a
 * hard I/O error. This policy is a small, pure value object so its decisions are
 * unit-testable with no network: ShouldRetry() classifies an outcome and
 * BackoffCeilingMs()/NextDelayMs() compute the wait.
 *
 * The transport base (HttpObjectStoreClient) owns one of these and consults it
 * inside its poll loop, so retry is invisible to the bdev coroutine — it only
 * changes how long IsComplete() keeps reporting "not done yet". The default
 * (max_attempts == 1) is a no-op: every operation is attempted exactly once,
 * which preserves the behavior of providers that do not opt in.
 *
 * 401/403 authentication failures are handled separately (see retry_on_auth_err
 * and HttpObjectStoreClient::OnAuthError); they are NOT classified by
 * ShouldRetry, and HTTP 404 (the sparse-read sentinel) is never retried.
 */
struct RetryPolicy {
  int max_attempts = 1;        /**< Total attempts incl. the first (1 = no retry) */
  double base_ms = 100.0;      /**< Backoff base; ceiling = base * 2^(attempt-1) */
  double max_ms = 30000.0;     /**< Upper cap on the backoff ceiling (ms) */
  bool retry_on_auth_err = false; /**< Retry once on 401/403 after re-auth */

  /**
   * Build a policy from the GCS_MAX_RETRIES / GCS_RETRY_BASE_MS /
   * GCS_RETRY_MAX_MS environment variables, falling back to production
   * defaults (5 attempts, 100 ms base, 30 s cap, auth-retry on).
   * @return A populated RetryPolicy.
   */
  static RetryPolicy FromEnv();

  /**
   * Decide whether a completed-but-failed operation should be retried.
   * Considers only transient signals: retryable curl transport errors and
   * HTTP 429/500/502/503/504. Returns false once attempts are exhausted, and
   * never retries success, 404, or non-transient 4xx (incl. 401/403, which the
   * caller routes through the auth-error path instead).
   * @param http_status HTTP status code (0 if the transport failed first).
   * @param curl_code The libcurl CURLcode for the transfer (0 == CURLE_OK).
   * @param attempt Zero-based index of the attempt that just finished.
   * @return true if another attempt is warranted.
   */
  bool ShouldRetry(long http_status, int curl_code, int attempt) const;

  /**
   * Deterministic (un-jittered) backoff ceiling for a given attempt.
   * @param attempt One-based index of the upcoming attempt (>=1).
   * @return min(max_ms, base_ms * 2^(attempt-1)); >= 0.
   */
  double BackoffCeilingMs(int attempt) const;

  /**
   * Jittered backoff delay in [0, BackoffCeilingMs(attempt)] (full jitter).
   * Uses a process-local RNG; for the upcoming attempt index (>=1).
   * @param attempt One-based index of the upcoming attempt.
   * @return A randomized delay in milliseconds within the ceiling.
   */
  double NextDelayMs(int attempt) const;
};

/**
 * @return true if `curl_code` (a CURLcode value) is a transient transport
 * error worth retrying (connect/timeout/recv/send/resolve/partial). Exposed
 * for unit testing without pulling in <curl/curl.h> at the call site.
 * @param curl_code The integer CURLcode.
 */
bool IsTransientCurlError(int curl_code);

}  // namespace clio::run::bdev

#endif  // CLIO_BDEV_GCS_RETRY_H_
