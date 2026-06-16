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

#ifndef CLIO_BDEV_OBJECT_STORE_CLIENT_H_
#define CLIO_BDEV_OBJECT_STORE_CLIENT_H_

#include <cstddef>
#include <cstdint>

namespace clio::run::bdev {

/** Outcome of a completed async object-store operation (provider-agnostic). */
struct ObjectStoreResult {
  long http_status = 0;    /**< HTTP status code (0 if the transport failed) */
  size_t bytes = 0;        /**< Bytes transferred (PUT body or GET payload) */
  bool not_found = false;  /**< True when a GET returned HTTP 404 */
  bool ok = false;         /**< True on a 2xx response with no transport error */
};

/**
 * Provider-agnostic async object-store client.
 *
 * This is the seam that unifies the cloud bdev backends (S3, GCS, Azure Blob,
 * Alibaba OSS) behind one interface. It is deliberately **offset-oriented**:
 * the bdev allocator hands out byte offsets into a flat logical device, and
 * each concrete client maps an offset onto its own object model internally —
 * either one object per block keyed by offset (Mapping A; S3/GCS/OSS, Azure
 * Block Blob) or an in-place write into a single device blob at that offset
 * (Mapping D; Azure Page Blob). Pushing that mapping behind the vtable lets the
 * bdev runtime drive every cloud backend with a single Write/Read coroutine
 * pair, with no per-provider branching.
 *
 * The submit/poll surface mirrors ctp::AsyncIO so the bdev coroutines can
 * yield-while-polling exactly as the file backend does: WriteAsync/ReadAsync
 * return an opaque token, and IsComplete() drives the transport and reports
 * completion. Concrete clients are produced by MakeObjectStoreClient() and are
 * owned one-per-worker (a curl multi handle is not safe to share across
 * threads).
 */
class ObjectStoreClient {
 public:
  /**
   * How the bdev read coroutine should treat a successful but short read.
   *  - kSparse404 : never-written blocks 404 (handled via not_found); a short
   *                 successful read advances by the bytes actually returned.
   *                 Used by whole-object backends (S3/GCS/OSS, Azure Block).
   *  - kZeroFilled: the backing object always exists (e.g. an Azure page blob),
   *                 so unwritten ranges read back as zeros; a short read has its
   *                 tail client-zero-filled and advances by the full block size.
   */
  enum class ReadFill { kSparse404, kZeroFilled };

  virtual ~ObjectStoreClient() = default;

  /** @return true when the client is fully configured and ready for I/O. */
  virtual bool IsValid() const = 0;

  /**
   * Synchronously prepare the backing store (idempotent). Covers bucket /
   * container creation and, where applicable, pre-creating a device blob of
   * `capacity` bytes (Azure Page Blob). Providers that need no capacity ignore
   * the argument.
   * @param capacity Logical device capacity in bytes.
   * @return true on success.
   */
  virtual bool Bootstrap(uint64_t capacity) = 0;

  /**
   * Submit an async write of `len` bytes from `buf` to byte `offset`.
   * The caller must keep `buf` alive until IsComplete() returns true.
   * @return Opaque op token to poll with IsComplete(), or nullptr on failure.
   */
  virtual void *WriteAsync(uint64_t offset, const void *buf, size_t len) = 0;

  /**
   * Submit an async read of `len` bytes at byte `offset` into `buf`.
   * @return Opaque op token to poll with IsComplete(), or nullptr on failure.
   */
  virtual void *ReadAsync(uint64_t offset, void *buf, size_t len) = 0;

  /**
   * Drive in-flight transfers and report whether `token`'s op has finished.
   * On a true return, `out` is populated and the op is freed (token invalid).
   * @param token An op token from WriteAsync/ReadAsync.
   * @param out Filled with the result when the op completes.
   * @return true if the op completed this call; false if still in flight.
   */
  virtual bool IsComplete(void *token, ObjectStoreResult &out) = 0;

  /** @return The short-read discipline the bdev read coroutine must apply. */
  virtual ReadFill GetReadFill() const = 0;
};

}  // namespace clio::run::bdev

#endif  // CLIO_BDEV_OBJECT_STORE_CLIENT_H_
