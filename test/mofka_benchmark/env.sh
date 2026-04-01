#!/usr/bin/env bash
# ==============================================================================
# env.sh — Source this file in every terminal before running Mofka commands.
#
# Usage:
#   source env.sh
#
# What it does:
#   1. Activates the Mofka spack environment (/opt/spack-env) so that bedrock,
#      diaspora-producer-benchmark, diaspora-consumer-benchmark, and mofkactl
#      are all on PATH.
#   2. Sets the Mercury transport protocol (tcp by default — change if you have RDMA)
#   3. Sets a shared working directory where mofka.json and logs live
# ==============================================================================

# --- Activate the Mofka spack environment ---
# This puts bedrock, diaspora-*-benchmark, and mofkactl on PATH.
MOFKA_SPACK_ENV="/opt/spack-env"
MOFKA_SPACK_ROOT="/opt/spack"

if [ -f "${MOFKA_SPACK_ROOT}/share/spack/setup-env.sh" ]; then
    source "${MOFKA_SPACK_ROOT}/share/spack/setup-env.sh"
    spack env activate "${MOFKA_SPACK_ENV}" 2>/dev/null || true
else
    echo "WARNING: Spack not found at ${MOFKA_SPACK_ROOT}" >&2
fi

# --- Transport protocol ---
# Use "tcp" on machines without RDMA. Other options:
#   na+sm      - shared memory (same-node only, fastest)
#   ofi+tcp    - libfabric TCP provider
#   ofi+verbs  - InfiniBand RDMA
#   ofi+cxi    - Slingshot (HPE Cray)
export MOFKA_PROTOCOL="${MOFKA_PROTOCOL:-tcp}"

# --- Working directory (shared between server + client terminals) ---
export MOFKA_WORKDIR="${MOFKA_WORKDIR:-/tmp/mofka-bench}"

# --- Paths ---
# Directory containing the config and scripts
export MOFKA_BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export MOFKA_CONFIG="${MOFKA_BENCH_DIR}/config.json"

# Group file created by the server — clients read this to connect
export MOFKA_GROUP_FILE="${MOFKA_WORKDIR}/mofka.json"

# Driver config consumed by diaspora-*-benchmark binaries
export MOFKA_DRIVER_CONFIG="${MOFKA_WORKDIR}/driver_config.json"

# --- Topic defaults ---
export MOFKA_TOPIC="${MOFKA_TOPIC:-benchmark_topic}"

# --- Benchmark defaults ---
export MOFKA_NUM_EVENTS="${MOFKA_NUM_EVENTS:-1000}"
export MOFKA_DATA_SIZE="${MOFKA_DATA_SIZE:-1024}"
export MOFKA_METADATA_SIZE="${MOFKA_METADATA_SIZE:-64}"
export MOFKA_BATCH_SIZE="${MOFKA_BATCH_SIZE:-16}"
export MOFKA_FLUSH_INTERVAL="${MOFKA_FLUSH_INTERVAL:-100}"
export MOFKA_NUM_THREADS="${MOFKA_NUM_THREADS:-1}"
export MOFKA_DATA_SELECTIVITY="${MOFKA_DATA_SELECTIVITY:-1.0}"
export MOFKA_DATA_INTEREST="${MOFKA_DATA_INTEREST:-1.0}"

echo "=== Mofka benchmark environment ==="
echo "  Protocol:      ${MOFKA_PROTOCOL}"
echo "  Working dir:   ${MOFKA_WORKDIR}"
echo "  Server config: ${MOFKA_CONFIG}"
echo "  Group file:    ${MOFKA_GROUP_FILE}"
echo "  Topic:         ${MOFKA_TOPIC}"
echo "==================================="

# --- Sanity check: verify bedrock is reachable ---
if ! command -v bedrock &>/dev/null; then
    echo ""
    echo "WARNING: 'bedrock' not found on PATH."
    echo "  Mofka may not be installed yet. To install:"
    echo "    source ${MOFKA_SPACK_ROOT}/share/spack/setup-env.sh"
    echo "    spack env activate ${MOFKA_SPACK_ENV}"
    echo "    spack install"
    echo "  This takes ~15-30 min the first time."
    echo ""
fi
