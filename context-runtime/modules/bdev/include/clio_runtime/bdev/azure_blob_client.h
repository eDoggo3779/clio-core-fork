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

#ifndef CLIO_BDEV_AZURE_BLOB_CLIENT_H_
#define CLIO_BDEV_AZURE_BLOB_CLIENT_H_

#include <curl/curl.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "http_object_store_client.h"

namespace clio::run::bdev {

/** Page-blob pages are always aligned to and sized in 512-byte units. */
static constexpr uint64_t kAzurePageSize = 512;

/**
 * Connection + addressing configuration for one Azure Blob storage account.
 *
 * Container and key prefix are parsed from the bdev pool name
 * (`azure://container/prefix`); the endpoint, account, and access key are read
 * from the process environment so no secrets travel through serialized task
 * fields.
 *
 * Addressing style is inferred from the endpoint host: an account-prefixed host
 * (`<account>.blob.core.windows.net`) uses virtual-hosted addressing, while a
 * bare host (e.g. an Azurite emulator on `127.0.0.1:10000`) uses path-style
 * addressing where the account name is the first URL path segment.
 */
struct AzureConfig {
  std::string endpoint;    /**< Base authority, e.g. "http://127.0.0.1:10000" */
  std::string scheme;      /**< "http" or "https" */
  std::string host;        /**< host[:port] */
  std::string account;     /**< Storage account name */
  std::string container;   /**< Target container name */
  std::string prefix;      /**< Optional key prefix (no leading/trailing '/') */
  std::string key_b64;     /**< Account access key (base64, as provided) */
  bool path_style = true;  /**< True: account in URL path; false: in host */
  bool valid = false;      /**< True when all required fields were resolved */

  /**
   * Build a config from an `azure://container/prefix` pool name and the env.
   *
   * Reads AZURE_BLOB_ENDPOINT (or AZURE_STORAGE_ENDPOINT),
   * AZURE_STORAGE_ACCOUNT, and AZURE_STORAGE_KEY. Sets `valid=false` if the
   * pool name is malformed or required credentials/endpoint are missing.
   *
   * @param pool_name The bdev pool name (expected "azure://container/prefix").
   * @return A populated AzureConfig (check `.valid`).
   */
  static AzureConfig FromEnvAndPoolName(const std::string &pool_name);
};

/** Backward-compatible alias for the unified result type. */
using AzureResult = ObjectStoreResult;

/**
 * Minimal async Azure Blob client over libcurl's multi interface + a Shared Key
 * signer (OpenSSL HMAC-SHA256 over a base64-decoded account key). The
 * libcurl-multi transport and poll loop live in the HttpObjectStoreClient base;
 * this subclass adds Azure's request construction, Shared Key signing, and the
 * Block-Blob-vs-Page-Blob offset mapping. One instance is owned per bdev worker
 * because a CURLM handle is not safe to share across threads.
 *
 * Two object mappings are supported (selected by the `is_page` ctor argument):
 *  - Block Blob: each allocator block maps to one immutable blob keyed by its
 *    byte offset (s3backer model; see KeyForOffset). Sub-block writes would RMW.
 *  - Page Blob: the whole device maps to ONE page blob (see DeviceKey) that
 *    supports 512-byte-aligned in-place writes at arbitrary offsets (Put Page).
 * The offset->mapping decision lives entirely here, so the bdev runtime drives
 * both modes through the common WriteAsync/ReadAsync interface.
 */
class AzureBlobClient : public HttpObjectStoreClient {
 public:
  /**
   * @param config Resolved endpoint/account/container/credentials.
   * @param is_page True for the Page Blob mapping (kAzurePage), false for the
   *               Block Blob mapping (kAzure).
   */
  explicit AzureBlobClient(const AzureConfig &config, bool is_page = false);

  /** @return true when the config is valid and the curl multi handle exists. */
  bool IsValid() const override { return config_.valid && MultiOk(); }

  /**
   * Prepare the container and, for Page Blob, the device blob of `capacity`.
   * @param capacity Logical device capacity in bytes (used by Page Blob only).
   * @return true on success.
   */
  bool Bootstrap(uint64_t capacity) override;

  /**
   * Submit an async write of the block at `offset`. Block Blob: a whole-object
   * PUT keyed by offset. Page Blob: an in-place 512-aligned Put Page into the
   * device blob (a sub-512 tail is zero-padded).
   */
  void *WriteAsync(uint64_t offset, const void *buf, size_t len) override;

  /**
   * Submit an async read of the block at `offset`. Block Blob: a whole-object
   * GET. Page Blob: a ranged GET from the device blob.
   */
  void *ReadAsync(uint64_t offset, void *buf, size_t len) override;

  /** Both mappings client-zero-fill short reads (page blobs pre-exist). */
  ReadFill GetReadFill() const override { return ReadFill::kZeroFilled; }

  /**
   * Synchronously create the target container (idempotent).
   * @return true on HTTP 201 (created) or 409 (already exists).
   */
  bool EnsureContainer();

  /**
   * Synchronously create an (empty, all-zero) page blob of `size` bytes.
   * Used once at Create for the kAzurePage device blob. `size` is rounded up
   * to a 512-byte multiple as Azure requires.
   * @param key The blob key (typically DeviceKey()).
   * @param size Virtual size of the page blob in bytes.
   * @return true on HTTP 201.
   */
  bool CreatePageBlob(const std::string &key, uint64_t size);

  /**
   * Submit an async whole-object PUT of a block blob (`len` bytes from `buf`).
   * The caller must keep `buf` alive until IsComplete() returns true.
   * @return Opaque op token to poll with IsComplete(), or nullptr on failure.
   */
  void *PutBlockBlobAsync(const std::string &key, const void *buf, size_t len);

  /**
   * Submit an async Put Page (512-aligned in-place write) into the page blob
   * `key` at byte `offset`. `offset` and `len` must be 512-byte aligned.
   * @return Opaque op token to poll with IsComplete(), or nullptr on failure.
   */
  void *PutPageAsync(const std::string &key, uint64_t offset, const void *buf,
                     size_t len);

  /**
   * Submit an async GET of object `key` into `buf` (capacity `cap` bytes).
   * When `use_range` is true a 512-aligned `x-ms-range` of `range_len` bytes
   * starting at `offset` is requested (page-blob reads); otherwise the whole
   * blob is fetched (block-blob reads).
   * @return Opaque op token to poll with IsComplete(), or nullptr on failure.
   */
  void *GetAsync(const std::string &key, void *buf, size_t cap, uint64_t offset,
                 size_t range_len, bool use_range);

  /**
   * Map a block byte offset to a stable block-blob key (prefix + "blk_<off>").
   * @param offset Block offset within the logical device.
   * @return The full blob key for that block.
   */
  std::string KeyForOffset(uint64_t offset) const;

  /**
   * The single page-blob key representing the whole device (prefix+"device").
   * @return The device blob key.
   */
  std::string DeviceKey() const;

 protected:
  const char *LogTag() const override { return "Azure"; }

 private:
  /** A canonicalized query parameter (lowercase name, value). */
  using QueryParam = std::pair<std::string, std::string>;
  /** An x-ms-* header (lowercase name, value). */
  using XmsHeader = std::pair<std::string, std::string>;

  /**
   * Build the Shared Key-signed header list for one request.
   * Adds x-ms-date and x-ms-version internally, sorts the x-ms headers for the
   * canonical headers block, signs, and appends the Authorization header.
   * @param method HTTP verb ("PUT" or "GET").
   * @param res_path Container-rooted resource path, e.g. "/container/key".
   * @param query Sorted canonical query params (may be empty).
   * @param content_length Body length string ("" when zero, per Azure spec).
   * @param xms Extra x-ms-* headers (date/version are added automatically).
   * @return A curl_slist the caller owns and must free after the transfer.
   */
  curl_slist *BuildSignedHeaders(const std::string &method,
                                 const std::string &res_path,
                                 const std::vector<QueryParam> &query,
                                 const std::string &content_length,
                                 const std::vector<XmsHeader> &xms);

  /**
   * Compose the request URL for a resource path + query, honoring addressing.
   * @param res_path Container-rooted resource path, e.g. "/container/key".
   * @param query Query params to append (may be empty).
   * @return The full request URL.
   */
  std::string BuildUrl(const std::string &res_path,
                       const std::vector<QueryParam> &query) const;

  AzureConfig config_;
  bool is_page_ = false;     /**< Page Blob mapping (kAzurePage) vs Block Blob */
  // Scratch buffer reused to zero-pad a sub-512 page-write tail. Safe to reuse
  // per call because the bdev drives one op to completion per worker before the
  // next block (single in-flight per worker); it must outlive the async op.
  std::vector<char> pad_;
};

}  // namespace clio::run::bdev

#endif  // CLIO_BDEV_AZURE_BLOB_CLIENT_H_
