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

#ifndef CLIO_BDEV_HTTP_OBJECT_STORE_CLIENT_H_
#define CLIO_BDEV_HTTP_OBJECT_STORE_CLIENT_H_

#include <curl/curl.h>

#include <chrono>
#include <cstddef>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "gcs_retry.h"  // RetryPolicy (provider-agnostic, despite the file name)
#include "object_store_client.h"

namespace clio::run::bdev {

/**
 * Shared libcurl-multi transport for every HTTP object-store client.
 *
 * The four cloud prototypes (S3, GCS, Azure, OSS) differ only in how they build
 * and sign a request; the transport machinery — a per-worker curl multi handle,
 * the in-flight op bookkeeping, the streaming read/write callbacks, and the
 * drive/drain/cleanup poll loop in IsComplete() — was byte-for-byte identical
 * across all four (theme X2 in the prototype notes). This base owns that
 * machinery exactly once. A concrete client only has to: construct an Op,
 * configure the request on op->easy (URL + verb + signed headers), wire the
 * shared callbacks, and call Submit(). The provider-specific signing/addressing
 * stays in the subclass so the signed bytes are unchanged.
 *
 * The base also owns a transient-failure retry loop (RetryPolicy + the Op
 * `rebuild` closure + the OnAuthError() hook). Retry is opt-in: the default
 * policy is a single attempt and the default OnAuthError() is a no-op, so a
 * subclass that sets neither (S3/Azure/OSS) behaves exactly as before. A
 * subclass that opts in (GCS) inherits exponential-backoff retries and a
 * 401-driven token refresh with no change to the bdev coroutine, which only
 * sees IsComplete() keep returning false until the operation truly settles.
 */
class HttpObjectStoreClient : public ObjectStoreClient {
 public:
  HttpObjectStoreClient();
  ~HttpObjectStoreClient() override;
  HttpObjectStoreClient(const HttpObjectStoreClient &) = delete;
  HttpObjectStoreClient &operator=(const HttpObjectStoreClient &) = delete;

  /**
   * Drive in-flight transfers and report whether `token`'s op has finished.
   * Provider-agnostic: HTTP status, latency, byte count, and 404 handling are
   * identical across providers; the only per-provider inputs are LogTag(),
   * OnAuthError(), and the op's `rebuild` closure used for retries.
   */
  bool IsComplete(void *token, ObjectStoreResult &out) override;

 protected:
  /** Per-request state for one in-flight async operation. */
  struct Op {
    CURL *easy = nullptr;            /**< Owning curl easy handle */
    curl_slist *headers = nullptr;   /**< Signed request headers (owned) */
    bool is_get = false;            /**< true for GET, false for PUT */
    bool finished = false;          /**< Set when the transfer completes */
    CURLcode curl_code = CURLE_OK;  /**< Transport-level result */
    std::string key;                /**< Object key (for logging) */
    std::string op_label;           /**< "PUT"/"GET"/"PUT-PAGE" for logging */
    // PUT source
    const char *src = nullptr;
    size_t len = 0;
    size_t sent = 0;
    // GET destination
    char *dst = nullptr;
    size_t cap = 0;
    size_t written = 0;
    std::chrono::high_resolution_clock::time_point start;
    // Retry bookkeeping (inert unless the subclass installs `rebuild`).
    int attempt = 0;                /**< 0-based count of attempts already made */
    bool pending_retry = false;     /**< Parked in a backoff window */
    bool auth_retried = false;      /**< A 401/403 auth-retry was already spent */
    std::chrono::high_resolution_clock::time_point next_earliest; /**< Re-arm at */
    /** Reconfigure easy + headers for a fresh attempt (subclass-supplied). */
    std::function<void(Op *)> rebuild;
  };

  /** libcurl read callback: stream the PUT body out of op->src. */
  static size_t ReadCb(char *ptr, size_t size, size_t nmemb, void *userdata);
  /** libcurl write callback: capture the GET body into op->dst (bounded). */
  static size_t WriteCb(char *ptr, size_t size, size_t nmemb, void *userdata);

  /**
   * Register a fully-configured op on the multi handle and start it.
   * @param op An op whose easy handle has its URL/verb/headers/callbacks set.
   * @return The op token (== op), to be polled with IsComplete().
   */
  void *Submit(Op *op);

  /** @return true when the curl multi handle was created successfully. */
  bool MultiOk() const { return multi_ != nullptr; }

  /** @return A short provider tag for log lines ("S3"/"Gcs"/"Azure"/"OSS"). */
  virtual const char *LogTag() const = 0;

  /**
   * Invoked once before an auth-retry when a request returns 401/403. The base
   * default is a no-op; a subclass (GCS) overrides it to invalidate/refresh its
   * cached credential so the retried request carries a fresh token.
   */
  virtual void OnAuthError() {}

  RetryPolicy retry_policy_;  /**< Default: 1 attempt (retries disabled) */

 private:
  /** Tear down a finished transfer but keep the op parked for another attempt. */
  void ScheduleRetry(Op *op, bool immediate, long status);
  /** Rebuild a parked op's easy/headers and re-submit. @return false on failure. */
  bool RearmOp(Op *op);
  /** Populate `out`, log, free the op's resources, and delete it. */
  void FinalizeOp(Op *op, ObjectStoreResult &out, long status);

  CURLM *multi_ = nullptr;
  std::unordered_map<CURL *, Op *> inflight_;  /**< easy handle -> op */
  std::unordered_set<Op *> parked_;            /**< ops in a backoff window */
};

}  // namespace clio::run::bdev

#endif  // CLIO_BDEV_HTTP_OBJECT_STORE_CLIENT_H_
