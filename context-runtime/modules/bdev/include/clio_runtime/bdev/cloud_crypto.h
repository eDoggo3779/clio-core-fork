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

#ifndef CLIO_BDEV_CLOUD_CRYPTO_H_
#define CLIO_BDEV_CLOUD_CRYPTO_H_

#include <cstddef>
#include <string>
#include <vector>

namespace clio::run::bdev::cloud {

/**
 * Shared crypto + encoding helpers for the cloud object-store adapters.
 *
 * Every HTTP object-store client (S3 SigV4, OSS V1/SigV4, Azure Shared Key)
 * needs the same handful of primitives — hex/base64 encoding, HMAC, SHA-256,
 * RFC-3986 URI encoding, and an RFC-1123 GMT clock. Each prototype originally
 * carried its own `static` copy of these (themes X1/X5 in the prototype notes);
 * factoring them here is the concrete de-duplication those notes called for.
 * The functions are pure (no shared state) so they stay reusable across the
 * per-worker clients without any synchronization.
 */

/** SHA-256 of empty content — the payload hash for body-less SigV4 requests. */
extern const char *kEmptyPayloadSha256;

/**
 * Hex-encode raw bytes in lowercase.
 * @param data Input bytes.
 * @param len Number of input bytes.
 * @return Lowercase hex string of length 2*len.
 */
std::string HexEncode(const unsigned char *data, size_t len);

/**
 * Compute the lowercase hex SHA-256 of a buffer (the SigV4 payload hash).
 * @param data Input bytes (may be null when len==0).
 * @param len Number of input bytes.
 * @return 64-character lowercase hex digest.
 */
std::string Sha256Hex(const void *data, size_t len);

/**
 * Compute a raw HMAC-SHA256 (SigV4 key derivation, Azure Shared Key).
 * @param key HMAC key bytes.
 * @param key_len Key length.
 * @param data Message bytes.
 * @param data_len Message length.
 * @return Raw 32-byte MAC.
 */
std::vector<unsigned char> HmacSha256(const unsigned char *key, size_t key_len,
                                      const void *data, size_t data_len);

/**
 * Compute a raw HMAC-SHA1 (the native OSS V1 signing primitive).
 * @param key HMAC key bytes (the AccessKeySecret).
 * @param key_len Key length.
 * @param data Message bytes (the string-to-sign).
 * @param data_len Message length.
 * @return Raw 20-byte MAC.
 */
std::vector<unsigned char> HmacSha1(const unsigned char *key, size_t key_len,
                                    const void *data, size_t data_len);

/**
 * Base64-encode raw bytes (standard alphabet, '=' padding).
 * @param data Input bytes.
 * @param len Number of input bytes.
 * @return The base64 text ("" when len==0).
 */
std::string Base64Encode(const unsigned char *data, size_t len);

/**
 * Base64-decode text into raw bytes (ignores '=' padding and whitespace).
 * @param in Base64 text (e.g. an Azure account key).
 * @return The decoded bytes (empty on malformed input).
 */
std::vector<unsigned char> Base64Decode(const std::string &in);

/**
 * RFC 3986 URI-encode per AWS/OSS rules (unreserved chars pass through).
 * @param value String to encode.
 * @param encode_slash When false, '/' is left literal (used for object paths).
 * @return The percent-encoded string.
 */
std::string AwsUriEncode(const std::string &value, bool encode_slash);

/**
 * Format the current UTC time as an RFC 1123 GMT timestamp.
 * Used for the OSS `Date` header and Azure `x-ms-date` header.
 * @return e.g. "Mon, 15 Jun 2026 16:11:17 GMT".
 */
std::string Rfc1123GmtNow();

/** ASCII-lowercase a string. */
std::string ToLower(const std::string &s);

/**
 * Read an environment variable, returning a default when unset/empty.
 * @param name Variable name.
 * @param def Fallback value.
 * @return The value or `def`.
 */
std::string EnvOr(const char *name, const std::string &def);

}  // namespace clio::run::bdev::cloud

#endif  // CLIO_BDEV_CLOUD_CRYPTO_H_
