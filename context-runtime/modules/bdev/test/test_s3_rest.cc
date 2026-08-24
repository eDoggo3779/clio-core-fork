/*
 * Copyright (c) 2024, Gnosis Research Center, Illinois Institute of Technology
 * All rights reserved.
 *
 * This file is part of IOWarp Core.
 */

/**
 * Round-trip tests for S3RestClient against a local S3 stand-in.
 *
 * s3_stub_server.py starts an in-memory endpoint on an ephemeral port, exports
 * S3_ENDPOINT (which selects path-style addressing) plus credentials, and runs
 * this binary as its child. No AWS account, no network, no Docker.
 *
 * What this covers that test_sigv4.cc cannot: the signer is exercised through
 * the client's own wiring -- the Host header, the canonical URI derived from
 * path-style addressing, a live timestamp -- and the stub verifies the
 * resulting signature independently. A client that signed a virtual-hosted URI
 * while sending a path-style request would pass every frozen vector and fail
 * here, which is the failure this pairing exists to catch.
 *
 * Run standalone (without the wrapper) and every case self-skips.
 */

#include "clio_runtime/bdev/transports/s3_rest.h"

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "simple_test.h"

namespace s3 = clio::run::bdev::s3;

namespace {

/** Read an env var, "" when unset. */
std::string Env(const char *name) {
  const char *v = std::getenv(name);
  return (v && *v) ? std::string(v) : std::string();
}

/** True when s3_stub_server.py is driving us. */
bool StubAvailable() { return !Env("S3_ENDPOINT").empty(); }

/** A client pointed at the stub bucket with the given key prefix. */
s3::S3RestClient MakeClient(const std::string &prefix) {
  return s3::S3RestClient(
      s3::S3RestClient::ConfigFromEnv(Env("S3_STUB_BUCKET"), prefix));
}

/** Deterministic, offset-dependent bytes, so a misplaced block is visible. */
std::vector<char> Pattern(size_t len, unsigned seed) {
  std::vector<char> v(len);
  for (size_t i = 0; i < len; ++i) {
    v[i] = static_cast<char>((i * 31u + seed * 7u + (i >> 8)) & 0xFF);
  }
  return v;
}

}  // namespace

TEST_CASE("s3_rest_key_for_offset", "[s3_rest]") {
  // Pure addressing: no endpoint needed, so this runs even standalone.
  s3::S3Config bare;
  bare.bucket = "b";
  REQUIRE(s3::S3RestClient(bare).KeyForOffset(0) == "block_0");
  REQUIRE(s3::S3RestClient(bare).KeyForOffset(1048576) == "block_1048576");

  s3::S3Config pre = bare;
  pre.prefix = "clio/run7";
  REQUIRE(s3::S3RestClient(pre).KeyForOffset(4096) == "clio/run7/block_4096");
}

TEST_CASE("s3_rest_addressing_mode_follows_endpoint", "[s3_rest]") {
  s3::S3Config aws;
  aws.bucket = "b";
  REQUIRE_FALSE(aws.path_style());

  s3::S3Config local;
  local.bucket = "b";
  local.endpoint = "http://127.0.0.1:9000";
  REQUIRE(local.path_style());

  // A trailing slash on the endpoint must not survive into the request path.
  s3::S3Config slashed =
      s3::S3RestClient::ConfigFromEnv("b", "p");  // reads S3_ENDPOINT
  REQUIRE(slashed.endpoint.empty() || slashed.endpoint.back() != '/');
}

TEST_CASE("s3_rest_ensure_bucket", "[s3_rest]") {
  if (!StubAvailable()) {
    INFO("S3_ENDPOINT unset; run via s3_stub_server.py. Skipping.");
    return;
  }
  s3::S3RestClient client = MakeClient("ensure");
  s3::S3Result r = client.EnsureBucket();
  REQUIRE(r.error.empty());
  REQUIRE(r.http_status == 200);
}

TEST_CASE("s3_rest_put_get_round_trip", "[s3_rest]") {
  if (!StubAvailable()) {
    INFO("S3_ENDPOINT unset; run via s3_stub_server.py. Skipping.");
    return;
  }
  s3::S3RestClient client = MakeClient("clio/roundtrip");

  SECTION("a block-sized object survives the round trip byte for byte");
  const size_t kLen = 64 * 1024;
  std::vector<char> out = Pattern(kLen, 3);
  const std::string key = client.KeyForOffset(1048576);
  s3::S3Result put = client.PutObject(key, out.data(), out.size());
  REQUIRE(put.error.empty());
  REQUIRE(put.ok());

  std::vector<char> in(kLen, 0);
  size_t got = 0;
  s3::S3Result get = client.GetObject(key, in.data(), in.size(), &got);
  REQUIRE(get.error.empty());
  REQUIRE(get.ok());
  REQUIRE_FALSE(get.not_found);
  REQUIRE(got == kLen);
  REQUIRE(std::memcmp(out.data(), in.data(), kLen) == 0);

  SECTION("a zero-length object is legal");
  const std::string empty_key = client.KeyForOffset(0);
  REQUIRE(client.PutObject(empty_key, nullptr, 0).ok());
  size_t empty_got = 1;
  s3::S3Result empty = client.GetObject(empty_key, in.data(), 0, &empty_got);
  REQUIRE(empty.ok());
  REQUIRE(empty_got == 0);

  SECTION("delete removes it, and the next read is a sparse miss");
  REQUIRE(client.DeleteObject(key).error.empty());
  size_t after = 1;
  s3::S3Result gone = client.GetObject(key, in.data(), in.size(), &after);
  REQUIRE(gone.not_found);
  REQUIRE(gone.http_status == 404);
  REQUIRE(after == 0);
}

TEST_CASE("s3_rest_missing_object_is_a_sparse_miss", "[s3_rest]") {
  if (!StubAvailable()) {
    INFO("S3_ENDPOINT unset; run via s3_stub_server.py. Skipping.");
    return;
  }
  // The bdev depends on this exact shape: not_found set, error empty, so
  // ReadBlocks zero-fills instead of failing the task.
  s3::S3RestClient client = MakeClient("clio/never-written");
  char buf[16] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
  size_t got = 99;
  s3::S3Result r = client.GetObject(client.KeyForOffset(8192), buf, sizeof(buf),
                                    &got);
  REQUIRE(r.not_found);
  REQUIRE(r.error.empty());
  REQUIRE(r.http_status == 404);
  REQUIRE(got == 0);
}

TEST_CASE("s3_rest_keys_needing_encoding", "[s3_rest]") {
  if (!StubAvailable()) {
    INFO("S3_ENDPOINT unset; run via s3_stub_server.py. Skipping.");
    return;
  }
  // The canonical URI is percent-encoded but keeps '/', and the signature is
  // computed over that encoded form. If encoding and signing disagreed, the
  // stub would reject the request rather than store it.
  s3::S3RestClient client = MakeClient("clio/odd keys+here");
  const std::string key = client.KeyForOffset(2097152);
  REQUIRE(key == "clio/odd keys+here/block_2097152");

  std::vector<char> out = Pattern(1024, 9);
  REQUIRE(client.PutObject(key, out.data(), out.size()).ok());

  std::vector<char> in(1024, 0);
  size_t got = 0;
  REQUIRE(client.GetObject(key, in.data(), in.size(), &got).ok());
  REQUIRE(got == out.size());
  REQUIRE(std::memcmp(out.data(), in.data(), out.size()) == 0);
}

TEST_CASE("s3_rest_bad_credentials_are_rejected", "[s3_rest]") {
  if (!StubAvailable()) {
    INFO("S3_ENDPOINT unset; run via s3_stub_server.py. Skipping.");
    return;
  }
  // Proves the stub actually verifies signatures -- without this, every test
  // above would pass against a server that ignored the Authorization header.
  s3::S3Config bad =
      s3::S3RestClient::ConfigFromEnv(Env("S3_STUB_BUCKET"), "clio/bad");
  bad.secret_key = "not-the-right-secret-key-not-even-close";
  s3::S3RestClient client(bad);

  std::vector<char> out = Pattern(32, 1);
  s3::S3Result r =
      client.PutObject(client.KeyForOffset(0), out.data(), out.size());
  REQUIRE_FALSE(r.ok());
  REQUIRE(r.http_status == 403);
  REQUIRE(r.error.find("403") != std::string::npos);
}

TEST_CASE("s3_rest_ensure_bucket_refuses_to_create_by_default", "[s3_rest]") {
  if (!StubAvailable()) {
    INFO("S3_ENDPOINT unset; run via s3_stub_server.py. Skipping.");
    return;
  }
  // A typo'd bucket must fail loudly rather than silently create a billed one.
  s3::S3Config missing = s3::S3RestClient::ConfigFromEnv("no-such-bucket", "p");
  missing.allow_bucket_create = false;
  s3::S3Result r = s3::S3RestClient(missing).EnsureBucket();
  REQUIRE_FALSE(r.ok());
  REQUIRE(r.error.find("no-such-bucket") != std::string::npos);
  REQUIRE(r.error.find("S3_ALLOW_BUCKET_CREATE") != std::string::npos);
}

SIMPLE_TEST_MAIN()
