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
 * Attach-or-start probe for CLIO_WITH_RUNTIME=1 (issue #1015). See
 * clio_runtime/runtime_probe.h for the rationale.
 */

#include "clio_runtime/runtime_probe.h"

#include <array>
#include <cerrno>
#include <cstdint>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
// windows.h after winsock2.h: the reverse order redefines the socket types.
#include <windows.h>
#else
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "clio_runtime/types.h"

namespace clio::run {

namespace {

/**
 * The three TCP ports a runtime keyed on `port` binds: the main cross-node
 * server (port), the local server (port+1) and the client ROUTER (port+3).
 * Kept in sync with IpcManager::TryStartMainServer / StartLocalServer /
 * ServerInit — if any of them is busy, this port is not ours to take.
 */
constexpr std::array<u32, 3> kRuntimePortOffsets = {{0, 1, 3}};

}  // namespace

bool TcpPortIsFree(u32 port) {
#ifdef _WIN32
  // libzmq calls WSAStartup, but the probe runs before any ZMQ socket exists,
  // so ask for the Winsock refcount ourselves and drop it on the way out.
  WSADATA wsa;
  const bool wsa_ok = WSAStartup(MAKEWORD(2, 2), &wsa) == 0;
  SOCKET sock = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sock == INVALID_SOCKET) {
    if (wsa_ok) WSACleanup();
    return true;  // Cannot test: do not manufacture a foreign-listener error.
  }
#else
  int sock = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return true;  // Cannot test: do not manufacture a foreign-listener error.
  }
#endif

  // SO_REUSEADDR, exactly as libzmq sets it on every bind: the question this
  // answers is "could the runtime's own listener bind here", not "is the port
  // mentioned anywhere in the kernel's tables". Without it a TIME_WAIT socket
  // left by a client that just disconnected reads as EADDRINUSE and a perfectly
  // startable runtime is refused as a foreign program — while the runtime, had
  // it tried, would have bound fine. SO_REUSEADDR still does NOT let us bind
  // over a live listener, which is the conflict we are actually looking for.
  const int reuse = 1;
  ::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
               reinterpret_cast<const char *>(&reuse), sizeof(reuse));

  // Bind to the wildcard address so a listener on any single local interface
  // still collides with us, and never listen() — an unlistened bind is enough
  // to detect the conflict and does not raise the Windows Defender Firewall
  // prompt.
  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(static_cast<uint16_t>(port));
  const bool free_port =
      ::bind(sock, reinterpret_cast<struct sockaddr *>(&addr),
             sizeof(addr)) == 0;

#ifdef _WIN32
  ::closesocket(sock);
  if (wsa_ok) WSACleanup();
#else
  ::close(sock);
#endif
  return free_port;
}

int LiveRuntimePid(u32 port) {
  // No exe-name check here, unlike the stop CLI's PidIsClioRun: a runtime can
  // be embedded in ANY binary (CLIO_WITH_RUNTIME=1 in an application), so the
  // executable name proves nothing. The residual pid-recycling window is
  // bounded by the record's lifetime — the runtime removes it on teardown, and
  // ClearUserIpcs reaps it on the next start if the runtime died without one.
  const int pid = DiscoverRuntimePid(port);
  if (pid > 0 && PidIsRunning(pid)) {
    return pid;
  }
  return -1;
}

RuntimePresence ProbeRuntime(u32 port, int *pid_out) {
  // A live runtime wins over the port test: the busy ports ARE its listeners.
  const int pid = LiveRuntimePid(port);
  if (pid > 0) {
    if (pid_out != nullptr) {
      *pid_out = pid;
    }
    return RuntimePresence::kRuntime;
  }
  if (pid_out != nullptr) {
    *pid_out = -1;
  }
  for (u32 offset : kRuntimePortOffsets) {
    if (!TcpPortIsFree(port + offset)) {
      return RuntimePresence::kForeign;
    }
  }
  return RuntimePresence::kNone;
}

std::string RuntimeStartLockPath(u32 port) {
  const std::string name =
      ctp::ConfigParse::ExpandPath(std::string(kRuntimeStartLockPrefix) +
                                   "${USER}") +
      "_" + std::to_string(port);
  return ctp::SystemInfo::GetMemfdPath(name);
}

RuntimeStartLock::RuntimeStartLock(u32 port) {
  ctp::SystemInfo::EnsureMemfdDir();
  const std::string path = RuntimeStartLockPath(port);
#ifdef _WIN32
  // No flock: CreateFile without FILE_SHARE_* is itself the mutual exclusion,
  // so retry until the holder closes its handle.
  for (;;) {
    HANDLE h = CreateFileA(path.c_str(), GENERIC_READ | GENERIC_WRITE, 0,
                           nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL,
                           nullptr);
    if (h != INVALID_HANDLE_VALUE) {
      handle_ = h;
      held_ = true;
      return;
    }
    if (GetLastError() != ERROR_SHARING_VIOLATION) {
      return;  // Cannot lock at all: fall back to probe-only (see header).
    }
    Sleep(20);
  }
#else
  // 0666 & ~umask: co-resident starters may run as different users on a shared
  // node, and a lock only one of them can open is no lock at all.
  fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0666);
  if (fd_ < 0) {
    return;  // Cannot lock at all: fall back to probe-only (see header).
  }
  while (::flock(fd_, LOCK_EX) != 0) {
    if (errno != EINTR) {
      ::close(fd_);
      fd_ = -1;
      return;
    }
  }
  held_ = true;
#endif
}

RuntimeStartLock::~RuntimeStartLock() {
#ifdef _WIN32
  if (handle_ != nullptr) {
    CloseHandle(static_cast<HANDLE>(handle_));
    handle_ = nullptr;
  }
#else
  if (fd_ >= 0) {
    ::flock(fd_, LOCK_UN);
    ::close(fd_);
    fd_ = -1;
  }
#endif
  held_ = false;
}

}  // namespace clio::run
