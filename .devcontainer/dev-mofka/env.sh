#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────
# env.sh - Mofka Spack Environment Activation Script
# ──────────────────────────────────────────────────────────────────────
# Source this file to activate the Mofka Spack environment and make
# all Mofka/Mochi binaries, libraries, and Python packages available.
#
# Usage:
#   source /opt/spack-env/env.sh
# ──────────────────────────────────────────────────────────────────────

export SPACK_ROOT=/opt/spack

# Initialize spack shell support
if [ -f "${SPACK_ROOT}/share/spack/setup-env.sh" ]; then
    . "${SPACK_ROOT}/share/spack/setup-env.sh"
else
    echo "[env.sh] WARNING: Spack not found at ${SPACK_ROOT}" >&2
    return 1
fi

# Activate the Mofka spack environment
spack env activate /opt/spack-env

echo "[env.sh] Mofka Spack environment activated."
echo "[env.sh]   spack env: $(spack env status)"
