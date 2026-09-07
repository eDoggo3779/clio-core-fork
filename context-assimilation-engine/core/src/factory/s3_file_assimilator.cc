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

#include <clio_runtime/clio_runtime.h>
#include <clio_cae/core/factory/s3_file_assimilator.h>
#include <clio_cae/core/factory/aws_creds.h>
#include <clio_cae/core/factory/s3_conn_pool.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

// The in-process S3 REST client (Poco::Net + SigV4). Loading the AWS SDK into
// this runtime process stack-smashes CLIO_INIT, so the read path signs and
// streams over Poco instead -- the same transport the kS3 bdev tier proved.
// cae_s3_tool (the SDK) still exists, but only as a standalone helper for the
// benchmark floors and test seeding; the assimilator no longer forks it.
#include "clio_runtime/bdev/transports/s3_rest.h"

// Include clio_cte headers after the clio_cae includes to avoid Method
// namespace collision (same ordering as BinaryFileAssimilator).
#include <clio_cte/core/core_client.h>
#include <clio_cte/core/core_tasks.h>

namespace clio::cae::core {

namespace {

namespace s3 = clio::run::bdev::s3;

/**
 * Build the S3 client config for one import: endpoint (and its addressing
 * style) from the environment, credentials + region from the resolver. The
 * signer reads no environment itself, so the resolved values are injected here.
 * `prefix` is left empty -- the assimilator passes whole object keys, so the
 * bdev's KeyForOffset prefixing does not apply.
 *
 * @param bucket Target bucket.
 * @param creds  Resolved access/secret/token + region.
 * @return A ready S3Config.
 */
s3::S3Config MakeS3Config(const std::string& bucket,
                          const AwsCredentials& creds) {
  // ConfigFromEnv gives us S3_ENDPOINT (+ trailing-slash stripping and the
  // path-style decision); the credential fields it reads from the environment
  // are then overridden by the resolver's result, which itself preferred the
  // environment when present, so the two agree.
  s3::S3Config cfg = s3::S3RestClient::ConfigFromEnv(bucket, /*prefix=*/"");
  cfg.region = creds.region;
  cfg.access_key = creds.access_key;
  cfg.secret_key = creds.secret_key;
  cfg.session_token = creds.session_token;
  cfg.allow_bucket_create = false;  // a reader must never create a bucket
  return cfg;
}

/**
 * Fill exactly `want` bytes into `buf` from the live GET stream, resuming with a
 * ranged GET whenever the response body ends short of `want` before the whole
 * transfer is exhausted (e.g. a keep-alive socket dropped across a co_await).
 * Mirrors what the old fork+exec path got for free by staging to a whole file.
 *
 * @param client   The S3 client.
 * @param conn     The leased connection (kept across resumes).
 * @param st       In/out: the current GET stream; replaced on a resume.
 * @param key      Object key, for the resume request.
 * @param buf      Destination buffer of capacity >= want.
 * @param want     Bytes to place into buf (<= bytes remaining in the transfer).
 * @param abs_start Object offset of buf[0] (absolute, for the Range header).
 * @param max_resumes Cap on resume attempts before giving up.
 * @param resumes  In/out: running count of resumes across the whole object.
 * @param filled   Output: bytes actually placed into buf.
 * @return An S3Result: ok when want bytes were filled, else error set.
 */
s3::S3Result FillChunk(s3::S3RestClient& client, s3::S3Connection& conn,
                       s3::S3RestClient::S3GetStream& st, const std::string& key,
                       char* buf, size_t want, uint64_t abs_start,
                       int max_resumes, int* resumes, size_t* filled) {
  size_t got_total = 0;
  while (got_total < want) {
    size_t got = 0;
    s3::S3Result rr = client.ReadBody(st, buf + got_total, want - got_total,
                                      &got);
    got_total += got;
    if (got_total >= want) break;
    // The body ended (clean EOF or a mid-stream error) before this chunk was
    // full, yet the transfer is not complete. Retire the response and resume
    // from where we stopped with a ranged GET for the remaining bytes.
    if (*resumes >= max_resumes) {
      if (rr.error.empty()) {
        rr.error = "S3 GET " + key + " ended short after " +
                   std::to_string(max_resumes) + " resumes at offset " +
                   std::to_string(abs_start + got_total);
      }
      *filled = got_total;
      return rr;
    }
    ++(*resumes);
    client.EndGetObject(conn, st);
    const uint64_t resume_off = abs_start + got_total;
    const uint64_t resume_len = static_cast<uint64_t>(want - got_total);
    s3::S3Result rb = client.BeginGetObject(conn, key, resume_off, resume_len,
                                            &st);
    if (!rb.ok()) {
      *filled = got_total;
      return rb;
    }
  }
  *filled = got_total;
  return s3::S3Result{};
}

}  // namespace

S3FileAssimilator::S3FileAssimilator(
    std::shared_ptr<clio::cte::core::Client> cte_client,
    S3ConnectionPool* s3_pool)
    : cte_client_(cte_client), s3_pool_(s3_pool) {}

clio::run::TaskResume S3FileAssimilator::Schedule(const AssimilationCtx& ctx,
                                                  int& error_code) {
#ifdef __NVCOMPILER
  thread_local clio::run::RunContext _fb_rctx;
  clio::run::RunContext* _fp = clio::run::GetCurrentRunContextFromWorker();
  clio::run::RunContext& rctx = _fp ? *_fp : _fb_rctx;
#endif
  CLIO_TASK_BODY_BEGIN
  HLOG(kDebug,
       "S3FileAssimilator::Schedule ENTRY: src='{}', dst='{}', range_off={}, "
       "range_size={}",
       ctx.src, ctx.dst, ctx.range_off, ctx.range_size);

  // Validate destination protocol
  std::string dst_protocol = GetUrlProtocol(ctx.dst);
  if (dst_protocol != "iowarp") {
    HLOG(kError,
         "S3FileAssimilator: Destination protocol must be 'iowarp', got '{}'",
         dst_protocol);
    error_code = -1;
    CLIO_CO_RETURN;
  }

  // Extract tag name from destination URL
  std::string tag_name = GetUrlPath(ctx.dst);
  if (tag_name.empty()) {
    HLOG(kError,
         "S3FileAssimilator: Invalid destination URL, no tag name found");
    error_code = -2;
    CLIO_CO_RETURN;
  }

  // Get or create the tag in CTE
  auto tag_task = cte_client_->AsyncGetOrCreateTag(tag_name);
  CLIO_CO_AWAIT(tag_task);
  clio::cte::core::TagId tag_id = tag_task->tag_id_;
  if (tag_id.IsNull()) {
    HLOG(kError, "S3FileAssimilator: Failed to get or create tag '{}'",
         tag_name);
    error_code = -3;
    CLIO_CO_RETURN;
  }

  // Dependency-based scheduling is not yet supported (mirrors binary backend).
  if (!ctx.depends_on.empty()) {
    HLOG(kDebug,
         "S3FileAssimilator: Dependency handling not yet implemented "
         "(depends_on: {})",
         ctx.depends_on);
    error_code = 0;
    CLIO_CO_RETURN;
  }

  // Parse the S3 source URL into bucket + key
  std::string bucket;
  std::string key;
  if (!ParseS3Url(ctx.src, bucket, key)) {
    HLOG(kError, "S3FileAssimilator: Invalid S3 source URL '{}'", ctx.src);
    error_code = -4;
    CLIO_CO_RETURN;
  }

  // Resolve credentials + region in-process (no AWS SDK): environment keys
  // first, else the named profile from ~/.aws/credentials. Only the profile
  // NAME (ctx.s3_profile) ever travels in the task payload -- never a secret.
  AwsCredResult cred = ResolveAwsCredentials(ctx.s3_profile, ctx.s3_region);
  if (!cred.ok) {
    HLOG(kError, "S3FileAssimilator: {}", cred.error);
    error_code = -6;
    CLIO_CO_RETURN;
  }

  // In-process S3 client (Poco + SigV4). The connection is leased from the
  // runtime-owned pool for this object's whole lifetime, so a keep-alive socket
  // is reused across objects and is never shared with another worker even if
  // this task is migrated across a co_await (issue #785).
  s3::S3RestClient client(MakeS3Config(bucket, cred.creds));
  const std::string conn_key = client.ConnectionKey(key);
  std::unique_ptr<s3::S3Connection> conn_owned =
      s3_pool_ ? s3_pool_->Acquire(conn_key)
               : std::make_unique<s3::S3Connection>();
  s3::S3Connection& conn = *conn_owned;
  s3::S3RestClient::S3GetStream stream;
  // Return the connection on every exit path (success, error, co_return). In
  // both coroutine backends locals are destroyed at CLIO_CO_RETURN but preserved
  // across a suspend, which is exactly this lifetime. A socket whose body is
  // still open (an error bailed mid-stream) must not be pooled -- it carries
  // unconsumed bytes -- so it is retired; a clean finish nulls stream.body via
  // EndGetObject, leaving a reusable socket to pool.
  struct ConnReturn {
    S3ConnectionPool* pool;
    const std::string& key;
    std::unique_ptr<s3::S3Connection>* conn;
    s3::S3RestClient::S3GetStream* stream;
    ~ConnReturn() {
      if (!conn || !*conn) return;
      if (stream && stream->body != nullptr) (*conn)->Retire();
      if (pool) pool->Release(key, std::move(*conn));
    }
  } conn_guard{s3_pool_, conn_key, &conn_owned, &stream};

  // Whole object unless a bounded range was requested (a bare range_off with no
  // range_size means "whole object", matching the old fork+exec tool contract).
  const uint64_t req_off = (ctx.range_size > 0) ? ctx.range_off : 0;
  const uint64_t req_size = static_cast<uint64_t>(ctx.range_size);

  s3::S3Result begin =
      client.BeginGetObject(conn, key, req_off, req_size, &stream);
  if (begin.not_found) {
    HLOG(kError, "S3FileAssimilator: object not found: s3://{}/{}", bucket, key);
    error_code = -7;
    CLIO_CO_RETURN;
  }
  if (!begin.ok()) {
    HLOG(kError, "S3FileAssimilator: GET failed for s3://{}/{}: {}", bucket, key,
         begin.error);
    error_code = -7;
    CLIO_CO_RETURN;
  }

  // Bytes to ingest in THIS transfer (the range length, or the whole object).
  size_t total_size = static_cast<size_t>(stream.content_length);
  size_t chunk_offset = (ctx.range_size > 0) ? ctx.range_off : 0;
  HLOG(kDebug, "S3FileAssimilator: s3://{}/{} -> {} bytes (offset {})", bucket,
       key, total_size, chunk_offset);

  // Store object metadata as the "description" blob (mirrors binary backend).
  std::string description = "binary<size=" + std::to_string(total_size) +
                            ", offset=" + std::to_string(chunk_offset) + ">";
  size_t desc_size = description.size();
  auto desc_buffer = CLIO_IPC->AllocateBuffer(desc_size);
  std::memcpy(desc_buffer.ptr_, description.c_str(), desc_size);
  auto desc_task =
      cte_client_->AsyncPutBlob(tag_id, "description", 0, desc_size,
                                desc_buffer.shm_.template Cast<void>(), 1.0f,
                                clio::cte::core::Context(), 0);
  CLIO_CO_AWAIT(desc_task);
  if (desc_task->return_code_ != 0) {
    HLOG(kError,
         "S3FileAssimilator: Failed to store description for tag '{}' (code {})",
         tag_name, desc_task->return_code_);
    error_code = -9;
    CLIO_CO_RETURN;
  }

  // Stream the body into CTE in chunks, keeping up to kMaxParallelTasks PutBlob
  // tasks in flight (identical wait-and-drain shape to the binary backend). The
  // body now comes off the socket instead of a staged file; FillChunk resumes a
  // dropped keep-alive mid-object so a suspend across a PutBlob cannot truncate.
  static constexpr size_t kMaxChunkSize = 1024 * 1024;  // 1 MB
  static constexpr size_t kMaxParallelTasks = 32;
  static constexpr int kMaxResumes = 16;
  size_t chunk_idx = 0;
  size_t bytes_processed = 0;
  int resumes = 0;
  std::vector<clio::run::Future<clio::cte::core::PutBlobTask>> active_tasks;

  while (bytes_processed < total_size) {
    while (active_tasks.size() < kMaxParallelTasks &&
           bytes_processed < total_size) {
      size_t current_chunk_size =
          std::min(kMaxChunkSize, total_size - bytes_processed);
      auto buffer_ptr = CLIO_IPC->AllocateBuffer(current_chunk_size);
      char* buffer = buffer_ptr.ptr_;

      size_t filled = 0;
      s3::S3Result fr =
          FillChunk(client, conn, stream, key, buffer, current_chunk_size,
                    req_off + bytes_processed, kMaxResumes, &resumes, &filled);
      if (!fr.error.empty() || filled != current_chunk_size) {
        HLOG(kError,
             "S3FileAssimilator: short/failed read on chunk {} from s3://{}/{} "
             "(filled={}, want={}, err='{}')",
             chunk_idx, bucket, key, filled, current_chunk_size, fr.error);
        CLIO_IPC->FreeBuffer(buffer_ptr);
        error_code = -8;
        CLIO_CO_RETURN;
      }

      std::string blob_name = "chunk_" + std::to_string(chunk_idx);
      auto task =
          cte_client_->AsyncPutBlob(tag_id, blob_name, 0, current_chunk_size,
                                    buffer_ptr.shm_.template Cast<void>(), 1.0f,
                                    clio::cte::core::Context(), 0);
      active_tasks.push_back(task);
      bytes_processed += current_chunk_size;
      chunk_idx++;
    }

    if (!active_tasks.empty()) {
      auto& first_task = active_tasks.front();
      CLIO_CO_AWAIT(first_task);
      if (first_task->return_code_ != 0) {
        HLOG(kError, "S3FileAssimilator: PutBlob task failed with code {}",
             first_task->return_code_);
        CLIO_IPC->FreeBuffer(first_task->blob_data_.template Cast<char>());
        error_code = -10;
        CLIO_CO_RETURN;
      }
      CLIO_IPC->FreeBuffer(first_task->blob_data_.template Cast<char>());
      active_tasks.erase(active_tasks.begin());
    }
  }

  // Drain any remaining in-flight tasks.
  for (auto& task : active_tasks) {
    CLIO_CO_AWAIT(task);
    if (task->return_code_ != 0) {
      HLOG(kError, "S3FileAssimilator: PutBlob task failed with code {}",
           task->return_code_);
      CLIO_IPC->FreeBuffer(task->blob_data_.template Cast<char>());
      error_code = -10;
      CLIO_CO_RETURN;
    }
    CLIO_IPC->FreeBuffer(task->blob_data_.template Cast<char>());
  }

  // Drain any unread tail so the socket is reusable, then the guard pools it.
  client.EndGetObject(conn, stream);

  HLOG(kDebug,
       "S3FileAssimilator: Imported s3://{}/{} ({} chunks, {} resumes) into "
       "tag '{}'",
       bucket, key, chunk_idx, resumes, tag_name);
  error_code = 0;
  CLIO_CO_RETURN;
  CLIO_TASK_BODY_END
}

std::string S3FileAssimilator::GetUrlProtocol(const std::string& url) {
  size_t pos_standard = url.find("://");
  if (pos_standard != std::string::npos) {
    return url.substr(0, pos_standard);
  }
  size_t pos_custom = url.find("::");
  if (pos_custom != std::string::npos) {
    return url.substr(0, pos_custom);
  }
  return "";
}

std::string S3FileAssimilator::GetUrlPath(const std::string& url) {
  size_t pos_standard = url.find("://");
  if (pos_standard != std::string::npos) {
    return url.substr(pos_standard + 3);
  }
  size_t pos_custom = url.find("::");
  if (pos_custom != std::string::npos) {
    return url.substr(pos_custom + 2);
  }
  return "";
}

bool S3FileAssimilator::ParseS3Url(const std::string& url, std::string& bucket,
                                   std::string& key) {
  // Strip the scheme (`s3://` or `s3::`) -> "bucket/key".
  std::string path = GetUrlPath(url);
  if (path.empty()) {
    return false;
  }
  size_t slash = path.find('/');
  if (slash == std::string::npos || slash == 0 || slash + 1 >= path.size()) {
    return false;  // need both a bucket and a non-empty key
  }
  bucket = path.substr(0, slash);
  key = path.substr(slash + 1);
  return true;
}

}  // namespace clio::cae::core
