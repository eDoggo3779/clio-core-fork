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

#include <clio_runtime/bdev/http_object_store_client.h>

#include <algorithm>
#include <cstring>

#include "clio_ctp/util/logging.h"

namespace clio::run::bdev {

size_t HttpObjectStoreClient::ReadCb(char *ptr, size_t size, size_t nmemb,
                                     void *userdata) {
  auto *op = static_cast<Op *>(userdata);
  size_t want = size * nmemb;
  size_t remaining = op->len - op->sent;
  size_t n = std::min(want, remaining);
  if (n > 0) {
    std::memcpy(ptr, op->src + op->sent, n);
    op->sent += n;
  }
  return n;
}

size_t HttpObjectStoreClient::WriteCb(char *ptr, size_t size, size_t nmemb,
                                      void *userdata) {
  auto *op = static_cast<Op *>(userdata);
  size_t n = size * nmemb;
  size_t space = (op->written < op->cap) ? (op->cap - op->written) : 0;
  size_t c = std::min(n, space);
  if (c > 0) {
    std::memcpy(op->dst + op->written, ptr, c);
    op->written += c;
  }
  return n;  // Always consume everything so curl doesn't abort on overflow.
}

HttpObjectStoreClient::HttpObjectStoreClient() {
  multi_ = curl_multi_init();
  if (multi_ == nullptr) {
    HLOG(kError, "Failed to create curl multi handle for object-store client");
  }
}

HttpObjectStoreClient::~HttpObjectStoreClient() {
  for (auto &kv : inflight_) {
    Op *op = kv.second;
    if (multi_ && op->easy) curl_multi_remove_handle(multi_, op->easy);
    if (op->easy) curl_easy_cleanup(op->easy);
    if (op->headers) curl_slist_free_all(op->headers);
    delete op;
  }
  inflight_.clear();
  // Ops parked in a backoff window are not in inflight_ (their easy handle was
  // already torn down); free them here so shutdown leaks nothing.
  for (Op *op : parked_) {
    if (op->headers) curl_slist_free_all(op->headers);
    delete op;
  }
  parked_.clear();
  if (multi_) curl_multi_cleanup(multi_);
}

void *HttpObjectStoreClient::Submit(Op *op) {
  curl_multi_add_handle(multi_, op->easy);
  inflight_[op->easy] = op;
  return op;
}

void HttpObjectStoreClient::ScheduleRetry(Op *op, bool immediate, long status) {
  // Tear down the just-finished transfer but keep the op for another attempt.
  CURL *old = op->easy;
  if (old != nullptr) {
    curl_multi_remove_handle(multi_, old);
    inflight_.erase(old);
    curl_easy_cleanup(old);
  }
  op->easy = nullptr;
  if (op->headers != nullptr) {
    curl_slist_free_all(op->headers);
    op->headers = nullptr;
  }
  double delay_ms = immediate ? 0.0 : retry_policy_.NextDelayMs(op->attempt);
  op->next_earliest = std::chrono::high_resolution_clock::now() +
                      std::chrono::duration_cast<
                          std::chrono::high_resolution_clock::duration>(
                          std::chrono::duration<double, std::milli>(delay_ms));
  op->pending_retry = true;
  parked_.insert(op);
  HLOG(kWarning, "{} op={} key={} http={} curl={} — retry {}/{} in {}ms",
       LogTag(), op->op_label, op->key, status, static_cast<int>(op->curl_code),
       op->attempt, retry_policy_.max_attempts - 1, delay_ms);
}

bool HttpObjectStoreClient::RearmOp(Op *op) {
  parked_.erase(op);
  op->sent = 0;
  op->written = 0;
  op->finished = false;
  op->curl_code = CURLE_OK;
  op->pending_retry = false;
  if (op->rebuild) op->rebuild(op);  // builds a fresh easy + headers
  if (op->easy == nullptr) return false;
  curl_multi_add_handle(multi_, op->easy);
  inflight_[op->easy] = op;
  return true;
}

void HttpObjectStoreClient::FinalizeOp(Op *op, ObjectStoreResult &out,
                                       long status) {
  double ms = std::chrono::duration<double, std::milli>(
                  std::chrono::high_resolution_clock::now() - op->start)
                  .count();
  out.http_status = status;
  out.not_found = (status == 404);
  out.bytes = op->is_get ? op->written : op->sent;
  out.ok = (op->curl_code == CURLE_OK) && (status >= 200 && status < 300);

  HLOG(kInfo, "{} op={} key={} bytes={} http={} latency_ms={}", LogTag(),
       op->op_label, op->key, out.bytes, status, ms);
  if (op->curl_code != CURLE_OK) {
    HLOG(kError, "{} op={} key={} transport error curl={}", LogTag(),
         op->op_label, op->key, static_cast<int>(op->curl_code));
  }

  if (op->easy != nullptr) {
    curl_multi_remove_handle(multi_, op->easy);
    inflight_.erase(op->easy);  // erase while the pointer key is still valid
    curl_easy_cleanup(op->easy);
  }
  if (op->headers != nullptr) curl_slist_free_all(op->headers);
  delete op;
}

bool HttpObjectStoreClient::IsComplete(void *token, ObjectStoreResult &out) {
  auto *target = static_cast<Op *>(token);
  if (target == nullptr || multi_ == nullptr) return true;

  int running = 0;
  curl_multi_perform(multi_, &running);

  // Drain completion messages and mark the matching ops finished.
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

  // Parked in a backoff window: re-arm once the delay elapses.
  if (target->pending_retry) {
    if (std::chrono::high_resolution_clock::now() < target->next_earliest) {
      return false;
    }
    if (!RearmOp(target)) {
      FinalizeOp(target, out, 0);  // rebuild failed -> terminal error
      return true;
    }
    return false;
  }

  if (!target->finished) return false;

  long status = 0;
  curl_easy_getinfo(target->easy, CURLINFO_RESPONSE_CODE, &status);

  // Auth failure: refresh the credential and retry once (opt-in).
  bool is_auth = (status == 401 || status == 403);
  if (is_auth && retry_policy_.retry_on_auth_err && !target->auth_retried &&
      target->rebuild) {
    OnAuthError();
    target->auth_retried = true;
    ScheduleRetry(target, /*immediate=*/true, status);
    return false;
  }

  // Transient failure: exponential-backoff retry (opt-in via policy + rebuild).
  if (target->rebuild &&
      retry_policy_.ShouldRetry(status, static_cast<int>(target->curl_code),
                                target->attempt)) {
    target->attempt += 1;
    ScheduleRetry(target, /*immediate=*/false, status);
    return false;
  }

  FinalizeOp(target, out, status);
  return true;
}

}  // namespace clio::run::bdev
