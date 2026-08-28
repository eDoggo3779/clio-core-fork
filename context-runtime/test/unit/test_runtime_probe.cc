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

/**
 * Attach-or-start probe tests (issue #1015).
 *
 * These drive clio_runtime/runtime_probe.h directly: no runtime is started, no
 * IPC is touched, so they are safe to run anywhere and in any order. The
 * multi-node behaviour they underpin is covered by the docker/MPI test in
 * test/integration/with_runtime.
 */

#include "../simple_test.h"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "clio_runtime/runtime_probe.h"

using clio::run::LiveRuntimePid;
using clio::run::ProbeRuntime;
using clio::run::RemoveRuntimePidRecord;
using clio::run::RuntimePresence;
using clio::run::RuntimeStartLock;
using clio::run::TcpPortIsFree;
using clio::run::u32;
using clio::run::WriteRuntimePidRecord;

namespace {

// A port range no clio runtime uses, well above the defaults (9413 / 8080) and
// the ports the sibling unit tests pin (10500).
constexpr u32 kProbeBasePort = 24913;

/**
 * RAII pid record, standing in for a runtime that published itself on a port.
 * The record is how a same-node peer discovers a runtime it should attach to
 * rather than start a second one alongside.
 */
class PidRecord {
 public:
  PidRecord(u32 port, int pid) : port_(port) {
    WriteRuntimePidRecord(port, pid);
  }
  ~PidRecord() { RemoveRuntimePidRecord(port_); }
  PidRecord(const PidRecord &) = delete;
  PidRecord &operator=(const PidRecord &) = delete;

 private:
  u32 port_;
};

#ifndef _WIN32
/** A listening TCP socket standing in for "some other program on this port". */
class ForeignListener {
 public:
  explicit ForeignListener(u32 port) {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
      return;
    }
    // SO_REUSEADDR, as libzmq sets on the runtime's own listeners. It matters
    // for more than realism: Linux only lets a later bind reuse a TIME_WAIT
    // address when BOTH the old and the new socket carry the flag, and the
    // TIME_WAIT socket inherits it from this listener.
    const int reuse = 1;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::bind(fd_, reinterpret_cast<struct sockaddr *>(&addr),
               sizeof(addr)) != 0 ||
        ::listen(fd_, 4) != 0) {
      ::close(fd_);
      fd_ = -1;
      return;
    }
    bound_ = true;
  }
  ~ForeignListener() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }
  bool bound() const { return bound_; }
  /** Accept one pending connection; -1 if there is none. */
  int Accept() { return fd_ >= 0 ? ::accept(fd_, nullptr, nullptr) : -1; }

 private:
  int fd_ = -1;
  bool bound_ = false;
};
#endif

}  // namespace

TEST_CASE("RuntimeProbe - an unused port reads as free and empty",
          "[probe][1015]") {
  const u32 port = kProbeBasePort;
  REQUIRE(TcpPortIsFree(port));
  int pid = 12345;  // Must be overwritten with -1 when nothing is found.
  REQUIRE(ProbeRuntime(port, &pid) == RuntimePresence::kNone);
  REQUIRE(pid == -1);
}

#ifndef _WIN32
TEST_CASE("RuntimeProbe - a foreign listener is reported, not attached to",
          "[probe][1015]") {
  // THE distinction issue #1015 asks for: a busy port with no clio runtime
  // behind it must be a hard error, never a "connect to the existing runtime".
  const u32 port = kProbeBasePort + 10;
  ForeignListener squatter(port);
  REQUIRE(squatter.bound());
  REQUIRE_FALSE(TcpPortIsFree(port));
  REQUIRE(ProbeRuntime(port, nullptr) == RuntimePresence::kForeign);
}

TEST_CASE("RuntimeProbe - any of the three runtime ports counts as taken",
          "[probe][1015]") {
  // The runtime binds base, base+1 and base+3; a squatter on any one of them
  // means the whole port group is not ours to take.
  for (u32 offset : {0u, 1u, 3u}) {
    const u32 base = kProbeBasePort + 20 + offset * 10;
    ForeignListener squatter(base + offset);
    REQUIRE(squatter.bound());
    REQUIRE(ProbeRuntime(base, nullptr) == RuntimePresence::kForeign);
  }
}

TEST_CASE("RuntimeProbe - a closed connection's TIME_WAIT is not a squatter",
          "[probe][1015]") {
  // REGRESSION: the probe first bound without SO_REUSEADDR, so the TIME_WAIT
  // a just-disconnected client leaves behind read as EADDRINUSE and a
  // perfectly startable port was refused as "held by another program" — which
  // broke every test that reuses a port after a previous one released it.
  // libzmq binds with SO_REUSEADDR, so the runtime would have bound fine.
  const u32 port = kProbeBasePort + 80;
  {
    ForeignListener server(port);
    REQUIRE(server.bound());
    // Connect and let the server side close first, so the LISTEN port (not the
    // client's ephemeral port) is the one that lingers.
    int client = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(client >= 0);
    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(static_cast<uint16_t>(port));
    REQUIRE(::connect(client, reinterpret_cast<struct sockaddr *>(&addr),
                      sizeof(addr)) == 0);
    const int conn = server.Accept();
    REQUIRE(conn >= 0);
    ::close(conn);
    ::close(client);
  }  // listener closed here
  REQUIRE(TcpPortIsFree(port));
  REQUIRE(ProbeRuntime(port, nullptr) == RuntimePresence::kNone);
}

TEST_CASE("RuntimeProbe - a port the runtime does not bind is ignored",
          "[probe][1015]") {
  // base+2 is not one of the runtime's ports, so a listener there must not be
  // mistaken for a conflict (and turn every start into a spurious failure).
  const u32 base = kProbeBasePort + 60;
  ForeignListener squatter(base + 2);
  REQUIRE(squatter.bound());
  REQUIRE(ProbeRuntime(base, nullptr) == RuntimePresence::kNone);
}
#endif

TEST_CASE("RuntimeProbe - a live runtime is found, so peers attach to it",
          "[probe][1015]") {
  // THE decision this issue turns on: a peer that finds a runtime already
  // serving this port must report kRuntime, so CLIO_INIT attaches instead of
  // running ServerInit a second time (which reaps the live runtime's segments).
  // The multi-node proof is the docker/MPI suite; this pins the branch cheaply.
  const u32 port = kProbeBasePort + 200;
  const int self = static_cast<int>(ctp::SystemInfo::GetPid());
  PidRecord record(port, self);

  REQUIRE(LiveRuntimePid(port) == self);
  int found = -1;
  REQUIRE(ProbeRuntime(port, &found) == RuntimePresence::kRuntime);
  REQUIRE(found == self);
}

TEST_CASE("RuntimeProbe - a dead runtime's leftover record is not attached to",
          "[probe][1015]") {
  // A record outlives a runtime that died without its teardown. Attaching to
  // that corpse would hang the client on a response nothing can send, so a
  // record whose pid is gone must read as "no runtime" and let us start one.
  const u32 port = kProbeBasePort + 210;
  // A pid far above any plausible pid_max: reliably absent, and unlike a
  // recently-reaped pid it cannot be recycled under us mid-test.
  PidRecord record(port, 0x7FFFFFFF);

  REQUIRE(LiveRuntimePid(port) == -1);
  REQUIRE(ProbeRuntime(port, nullptr) == RuntimePresence::kNone);
}

#ifndef _WIN32
TEST_CASE("RuntimeProbe - a live runtime outranks its own busy ports",
          "[probe][1015]") {
  // Precedence matters: a running runtime's ports ARE busy, so testing ports
  // first would classify every healthy runtime as a foreign squatter and refuse
  // to attach. The runtime lookup has to win.
  const u32 port = kProbeBasePort + 220;
  PidRecord record(port, static_cast<int>(ctp::SystemInfo::GetPid()));
  ForeignListener occupied(port);
  REQUIRE(occupied.bound());
  REQUIRE_FALSE(TcpPortIsFree(port));

  REQUIRE(ProbeRuntime(port, nullptr) == RuntimePresence::kRuntime);
}
#endif

TEST_CASE("RuntimeStartLock - one holder at a time, released on destruction",
          "[probe][1015]") {
  // The mutual exclusion that keeps N simultaneously-launched ranks from all
  // deciding "the port is free" and all starting a runtime.
  const u32 port = kProbeBasePort + 100;
  std::atomic<int> concurrent{0};
  std::atomic<int> max_concurrent{0};
  std::atomic<int> acquired{0};
  constexpr int kThreads = 8;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&]() {
      RuntimeStartLock lock(port);
      if (!lock.held()) {
        return;  // Platform cannot lock; the guard degrades to probe-only.
      }
      acquired.fetch_add(1);
      const int now = concurrent.fetch_add(1) + 1;
      int prev = max_concurrent.load();
      while (now > prev && !max_concurrent.compare_exchange_weak(prev, now)) {
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      concurrent.fetch_sub(1);
    });
  }
  for (auto &t : threads) {
    t.join();
  }

  if (acquired.load() > 0) {
    // Every thread got in (so the lock is released on destruction, not leaked)
    // and never two at once.
    REQUIRE(acquired.load() == kThreads);
    REQUIRE(max_concurrent.load() == 1);
  }

  // The lock must be re-acquirable after all holders are gone.
  RuntimeStartLock again(port);
  REQUIRE(again.held() == (acquired.load() > 0));
}

SIMPLE_TEST_MAIN()
