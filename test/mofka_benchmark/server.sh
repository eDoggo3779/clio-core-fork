#!/usr/bin/env bash
# ==============================================================================
# server.sh — Run this in TERMINAL 1.
#
# Starts the Mofka/Bedrock daemon, creates a topic and partition, then waits.
# The server keeps running until you Ctrl+C or run: kill $(cat $MOFKA_WORKDIR/mofka.pid)
#
# Usage:
#   source env.sh
#   ./server.sh
# ==============================================================================

set -euo pipefail

# Source environment if not already loaded
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -z "${MOFKA_WORKDIR:-}" ]; then
    source "${SCRIPT_DIR}/env.sh"
fi

echo ""
echo "============================================"
echo "  Step 1: Prepare working directory"
echo "============================================"
rm -rf "${MOFKA_WORKDIR}"
mkdir -p "${MOFKA_WORKDIR}"
rm -rf /tmp/mofka-logs
mkdir -p /tmp/mofka-logs
echo "  Created: ${MOFKA_WORKDIR}"

echo ""
echo "============================================"
echo "  Step 2: Start Bedrock daemon"
echo "============================================"
echo "  Protocol: ${MOFKA_PROTOCOL}"
echo "  Config:   ${MOFKA_CONFIG}"

cd "${MOFKA_WORKDIR}"
bedrock "${MOFKA_PROTOCOL}" -c "${MOFKA_CONFIG}" &
BEDROCK_PID=$!
echo "${BEDROCK_PID}" > "${MOFKA_WORKDIR}/mofka.pid"
echo "  Bedrock PID: ${BEDROCK_PID}"

# Wait for the group file (proves the server is listening)
echo "  Waiting for mofka.json..."
TIMEOUT=30
ELAPSED=0
while [ ! -f "${MOFKA_GROUP_FILE}" ]; do
    sleep 1
    ELAPSED=$((ELAPSED + 1))
    if [ "${ELAPSED}" -ge "${TIMEOUT}" ]; then
        echo "  ERROR: mofka.json did not appear after ${TIMEOUT}s"
        echo "  Check ${MOFKA_WORKDIR}/mofka.err for details"
        kill "${BEDROCK_PID}" 2>/dev/null || true
        exit 1
    fi
done
echo "  Server is ready! Group file: ${MOFKA_GROUP_FILE}"

echo ""
echo "============================================"
echo "  Step 3: Create topic '${MOFKA_TOPIC}'"
echo "============================================"
python -m mochi.mofka.mofkactl topic create "${MOFKA_TOPIC}" \
    -g "${MOFKA_GROUP_FILE}"
echo "  Topic created."

echo ""
echo "============================================"
echo "  Step 4: Add memory partition (rank 0)"
echo "============================================"
python -m mochi.mofka.mofkactl partition add "${MOFKA_TOPIC}" \
    -r 0 -t memory \
    -g "${MOFKA_GROUP_FILE}"
echo "  Partition added."

echo ""
echo "============================================"
echo "  Step 5: Write driver config for clients"
echo "============================================"
cat > "${MOFKA_DRIVER_CONFIG}" <<EOF
{
    "group_file": "${MOFKA_GROUP_FILE}",
    "margo": {
        "use_progress_thread": true
    }
}
EOF
echo "  Written to: ${MOFKA_DRIVER_CONFIG}"

echo ""
echo "============================================"
echo "  Server is ready for benchmarks!"
echo "============================================"
echo ""
echo "  In another terminal, run:"
echo "    source env.sh"
echo "    ./producer.sh"
echo "    ./consumer.sh"
echo ""
echo "  Press Ctrl+C here to stop the server."
echo ""

# Bring bedrock to foreground so Ctrl+C kills it cleanly
wait "${BEDROCK_PID}"
