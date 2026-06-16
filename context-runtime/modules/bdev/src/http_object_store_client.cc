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
  if (multi_) curl_multi_cleanup(multi_);
}

void *HttpObjectStoreClient::Submit(Op *op) {
  curl_multi_add_handle(multi_, op->easy);
  inflight_[op->easy] = op;
  return op;
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

  if (!target->finished) return false;

  long status = 0;
  curl_easy_getinfo(target->easy, CURLINFO_RESPONSE_CODE, &status);
  double ms = std::chrono::duration<double, std::milli>(
                  std::chrono::high_resolution_clock::now() - target->start)
                  .count();
  out.http_status = status;
  out.not_found = (status == 404);
  out.bytes = target->is_get ? target->written : target->sent;
  out.ok = (target->curl_code == CURLE_OK) && (status >= 200 && status < 300);

  HLOG(kInfo, "{} op={} key={} bytes={} http={} latency_ms={}", LogTag(),
       target->op_label, target->key, out.bytes, status, ms);
  if (target->curl_code != CURLE_OK) {
    HLOG(kError, "{} op={} key={} transport error curl={}", LogTag(),
         target->op_label, target->key, static_cast<int>(target->curl_code));
  }

  curl_multi_remove_handle(multi_, target->easy);
  curl_easy_cleanup(target->easy);
  if (target->headers) curl_slist_free_all(target->headers);
  inflight_.erase(target->easy);
  delete target;
  return true;
}

}  // namespace clio::run::bdev
