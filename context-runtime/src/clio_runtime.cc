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
 * Main CLIO Runtime initialization and global functions
 */

#include "clio_runtime/clio_runtime.h"
#include "clio_runtime/config_manager.h"
#include "clio_runtime/container.h"
#include "clio_runtime/runtime_probe.h"
#include "clio_runtime/work_orchestrator.h"
#include <cstdlib>
#include <cstring>
#include <memory>

namespace clio::run {

bool ClioInitImpl(RuntimeMode mode, bool default_with_runtime,
                  bool is_restart) {
  // Static guard to prevent double initialization
  static bool s_initialized = false;
  if (s_initialized) {
    return true;  // Already initialized, return success
  }

  auto* runtime_manager = CLIO_RUNTIME_MANAGER;
  runtime_manager->is_restart_ = is_restart;

  // Check environment variable CLIO_WITH_RUNTIME
  bool with_runtime = default_with_runtime;
  const char* env_val = clio::run::env::GetCompat("WITH_RUNTIME");
  if (env_val != nullptr) {
    with_runtime = (std::strcmp(env_val, "1") == 0 ||
                   std::strcmp(env_val, "true") == 0 ||
                   std::strcmp(env_val, "TRUE") == 0);
  }

  // Determine what to initialize based on mode and with_runtime flag
  bool init_runtime = false;
  bool init_client = false;

  if (mode == RuntimeMode::kServer || mode == RuntimeMode::kRuntime) {
    // Server/Runtime mode: always start runtime
    init_runtime = true;
    init_client = true;  // Runtime also needs client components
  } else {
    // Client mode
    init_client = true;
    init_runtime = with_runtime;
  }

  // "Clio as a cache" (issue #1015): a client that asks for a runtime is asking
  // for one to EXIST, not to be the one running it. Probe first, and only bring
  // a runtime up in this process if the port is genuinely unoccupied.
  std::unique_ptr<RuntimeStartLock> start_lock;
  if (init_runtime) {
    // The port lives in the config, which both ServerInit and ClientInit
    // initialize anyway; Init() is idempotent, so reading it here is free.
    auto *config_manager = CLIO_CONFIG_MANAGER;
    if (!config_manager->Init()) {
      return false;
    }
    const u32 port = config_manager->GetPort();

    // Hold the node-wide start lock across probe+start, in EVERY mode that
    // brings a runtime up. N processes starting together (mpirun -np 4) would
    // otherwise all probe "free" in the same instant; the winner keeps the lock
    // until its runtime is serving, so the losers re-probe against a live
    // runtime and take the attach path. Server mode takes the lock too — not to
    // change its semantics, but because ServerInit publishes its pid record
    // some way in, and a client probing inside that window would otherwise see
    // an empty port and start a second runtime behind the daemon's back.
    start_lock = std::make_unique<RuntimeStartLock>(port);

    // Server/runtime mode is deliberately NOT given the attach path:
    // `clio_run runtime start` is an explicit "be the runtime" and must still
    // fail loudly when one is already there, rather than silently becoming a
    // client with no daemon.
    if (mode == RuntimeMode::kClient) {
      int existing_pid = -1;
      switch (ProbeRuntime(port, &existing_pid)) {
        case RuntimePresence::kRuntime:
          HLOG(kInfo,
               "CLIO_WITH_RUNTIME=1: runtime already running on port {} "
               "(pid {}); attaching to it as a client",
               port, existing_pid);
          init_runtime = false;
          start_lock.reset();
          break;
        case RuntimePresence::kForeign:
          HLOG(kError,
               "CLIO_WITH_RUNTIME=1: port {} (or {}/{}) is held by another "
               "program and no clio runtime owns it — refusing to start",
               port, port + 1, port + 3);
          return false;
        case RuntimePresence::kNone:
          HLOG(kInfo,
               "CLIO_WITH_RUNTIME=1: no runtime on port {}; starting one in "
               "this process",
               port);
          break;
      }
    }
  }

  // Initialize runtime first if needed
  if (init_runtime) {
    if (!runtime_manager->ServerInit()) {
      return false;
    }
  }

  // Initialize client components
  if (init_client) {
    if (!runtime_manager->ClientInit()) {
      return false;
    }
  }

  // Register atexit handler so CLIO_RUNTIME_FINALIZE runs before static
  // destructors.  The CLIO Runtime singleton is heap-allocated (GetGlobalPtrVar)
  // so its destructor is never called automatically.  Without this the ZMQ
  // DEALER socket stays open and zmq_ctx_destroy blocks forever at exit.
  std::atexit(CLIO_RUNTIME_FINALIZE);

  // Mark as initialized on success
  s_initialized = true;
  return true;
}

void CLIO_RUNTIME_FINALIZE() {
  static bool s_finalized = false;
  if (s_finalized) {
    return;
  }
  s_finalized = true;
  auto *mgr = CLIO_RUNTIME_MANAGER;
  if (mgr) {
    // Server first: stop worker threads that may still be sending IPC
    mgr->ServerFinalize();
    // Client second: close DEALER socket and join recv thread
    mgr->ClientFinalize();
  }
}

}  // namespace clio::run
