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

#ifndef CLIO_RUNTIME_RUNTIME_PROBE_H_
#define CLIO_RUNTIME_RUNTIME_PROBE_H_

/**
 * "Clio as a cache" (issue #1015): decide, before touching any global state,
 * whether this process should BE the node's runtime or merely ATTACH to one
 * that is already there.
 *
 * CLIO_WITH_RUNTIME=1 means "make sure a runtime is available", not "be the
 * runtime". Blindly running ServerInit a second time is actively destructive:
 * it reaps the per-user memfd directory, rebinds the runtime's three TCP ports
 * and republishes the port's pid record, so the second starter breaks the first
 * one's clients rather than failing cleanly. This header supplies the two
 * pieces that make the decision safe:
 *
 *   - ProbeRuntime(): is this port free, owned by a live clio runtime, or held
 *     by some unrelated program? The last case is a hard error — the caller
 *     must not pretend a foreign listener is a runtime it can talk to.
 *   - RuntimeStartLock: a node-wide, port-scoped exclusive file lock, held
 *     across the whole start. Without it, N ranks launched together (mpirun
 *     -np 4) all probe "free" in the same instant and all start a runtime. The
 *     winner holds the lock until its runtime is serving, so every loser
 *     re-probes against a runtime that is already up and attaches to it.
 */

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>

#ifdef __APPLE__
// macOS has no /proc: the process-state check goes through the KERN_PROC
// sysctl instead.
#include <sys/proc.h>
#include <sys/sysctl.h>
#endif

#include "clio_ctp/introspect/system_info.h"
#include "clio_ctp/util/config_parse.h"
#include "clio_runtime/api.h"
#include "clio_runtime/runtime_pid_record.h"
#include "clio_runtime/types.h"

namespace clio::run {

/**
 * Path of the runtime's main shared-memory segment entry in the per-user memfd
 * directory. Mirrors ConfigManager::GetSharedMemorySegmentName's default
 * port-keyed recipe so it also works when no config is loaded.
 * @param port the runtime port the segment name is keyed on
 * @return absolute path of the main segment entry
 */
inline std::string MainSegmentPath(u32 port) {
  const std::string name =
      ctp::ConfigParse::ExpandPath("chi_main_segment_${USER}") + "_" +
      std::to_string(port);
  return ctp::SystemInfo::GetMemfdPath(name);
}

/**
 * Discover the local runtime's pid: on Linux from its main segment symlink,
 * whose target is /proc/<pid>/fd/<n> (the memfd the runtime created), and
 * otherwise from the pid record the runtime writes alongside its segments
 * (see clio_runtime/runtime_pid_record.h). POSIX-only; returns -1 on Windows.
 *
 * The Windows short-circuit is deliberate and load-bearing for `clio_run stop`:
 * its kill escalation is POSIX-only, and SendStopTask treats a discovered pid
 * as "I can wait for this process to exit" — which WaitForRuntimeExit cannot do
 * on Windows. Handing it a real pid there turns a stop that used to succeed
 * into a failure. Callers that only need "is a runtime alive on this port"
 * should use LiveRuntimePid, which adds a portable record lookup on top.
 * @param port the runtime port (segment names are port-keyed)
 * @return the owning pid (may be dead), or -1 if the runtime left no trace
 */
inline int DiscoverRuntimePid(u32 port) {
#ifdef _WIN32
  (void)port;
  return -1;
#else
  std::error_code ec;
  auto target = std::filesystem::read_symlink(MainSegmentPath(port), ec);
  if (!ec) {
    const std::string t = target.string();
    constexpr const char *kProc = "/proc/";
    if (t.rfind(kProc, 0) == 0) {
      int pid = std::atoi(t.c_str() + std::string(kProc).size());
      if (pid > 0) {
        return pid;
      }
    }
  }
  return ReadRuntimePidRecord(port);
#endif
}

/**
 * Check whether a pid is a zombie (exited but not yet reaped by its parent).
 * kill(pid, 0) treats zombies as alive, but a zombie cannot run, service
 * tasks, or hold sockets — for both stop and attach purposes it is dead.
 * @param pid candidate pid
 * @return true if the process is a zombie
 */
inline bool PidIsZombie(int pid) {
#if defined(__APPLE__)
  // No /proc: ask the kernel for the process's state directly.
  struct kinfo_proc info;
  size_t len = sizeof(info);
  int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PID, pid};
  if (sysctl(mib, 4, &info, &len, nullptr, 0) != 0 || len == 0) {
    return false;
  }
  return info.kp_proc.p_stat == SZOMB;
#elif defined(__linux__)
  std::ifstream stat_file("/proc/" + std::to_string(pid) + "/stat");
  if (!stat_file.is_open()) {
    return false;
  }
  std::string stat_line;
  std::getline(stat_file, stat_line);
  // Field 3 (state) follows the parenthesized comm, which may contain spaces.
  size_t close_paren = stat_line.rfind(')');
  if (close_paren == std::string::npos ||
      close_paren + 2 >= stat_line.size()) {
    return false;
  }
  return stat_line[close_paren + 2] == 'Z';
#else
  (void)pid;
  return false;
#endif
}

/**
 * Check whether a pid is alive AND runnable (not a zombie). This is the
 * liveness test both the stop flow and the attach-or-start probe use: a
 * zombie counts as stopped.
 * @param pid candidate pid
 * @return true if the process exists and is not a zombie
 */
inline bool PidIsRunning(int pid) {
  return ctp::SystemInfo::IsProcessAlive(pid) && !PidIsZombie(pid);
}

/** What a probe of a runtime port found. */
enum class RuntimePresence {
  kNone,     /**< Nothing on the port: safe to start a runtime here. */
  kRuntime,  /**< A live clio runtime owns the port: attach as a client. */
  kForeign,  /**< An unrelated program holds the port: hard error. */
};

/**
 * Whether a TCP listener can be bound on this port right now. Probes the
 * wildcard address so a listener on ANY local interface counts as "taken"
 * (the runtime's main server binds 0.0.0.0, its local server 127.0.0.1).
 * @param port the TCP port to test
 * @return true if the port is currently free
 */
CLIO_RUN_API bool TcpPortIsFree(u32 port);

/**
 * The pid of a live clio runtime owning this port's artifacts, discovered from
 * the main segment's /proc symlink (Linux) or the port's pid record (all
 * platforms). A recorded pid that has since exited — or is a zombie, which can
 * no longer service tasks — does not count as live.
 * @param port the runtime port (artifacts are port-keyed)
 * @return the live runtime's pid, or -1 if there is none
 */
CLIO_RUN_API int LiveRuntimePid(u32 port);

/**
 * Classify what currently occupies a runtime port.
 *
 * A live runtime wins over the port test: its own listeners are what make the
 * ports busy. Only when no runtime can be found do busy ports mean a foreign
 * program.
 * @param port the runtime port to probe
 * @param pid_out if non-null, receives the live runtime's pid on kRuntime
 * @return the presence classification
 */
CLIO_RUN_API RuntimePresence ProbeRuntime(u32 port, int *pid_out);

/** Filename prefix shared by every start lock (used by the memfd-dir sweeps to
 *  tell a start lock apart from a reapable segment entry). */
inline constexpr const char *kRuntimeStartLockPrefix = "chi_runtime_lock_";

/**
 * Path of the start lock for a runtime port.
 * @param port the runtime port the lock is keyed on
 * @return absolute path of the lock file
 */
CLIO_RUN_API std::string RuntimeStartLockPath(u32 port);

/**
 * RAII node-wide exclusive lock scoping "probe, then start a runtime on this
 * port". Blocks until acquired, and releases when the object dies OR when the
 * holding process does — the kernel drops the lock on close/exit, so a starter
 * that crashes mid-bring-up cannot wedge its peers.
 *
 * Best-effort by construction: if the lock file cannot be created (read-only
 * memfd dir, unsupported platform) the guard reports !held() and the caller
 * still runs the probe, which is correct for the common single-starter case
 * and only loses the tie-break for simultaneous starters.
 */
class CLIO_RUN_API RuntimeStartLock {
 public:
  /** Acquire the start lock for this port, blocking until it is held. */
  explicit RuntimeStartLock(u32 port);

  /** Release the lock. */
  ~RuntimeStartLock();

  RuntimeStartLock(const RuntimeStartLock &) = delete;
  RuntimeStartLock &operator=(const RuntimeStartLock &) = delete;

  /** @return true if the lock is actually held (see class note). */
  bool held() const { return held_; }

 private:
  bool held_ = false;
#ifdef _WIN32
  void *handle_ = nullptr;  // HANDLE, kept opaque to avoid <windows.h> here
#else
  int fd_ = -1;
#endif
};

}  // namespace clio::run

#endif  // CLIO_RUNTIME_RUNTIME_PROBE_H_
