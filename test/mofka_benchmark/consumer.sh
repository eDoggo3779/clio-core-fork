#!/usr/bin/env bash
# ==============================================================================
# consumer.sh — Run this in TERMINAL 2 (after the producer has run).
#
# Consumes events from Mofka and reports throughput.
#
# Usage:
#   source env.sh
#   ./consumer.sh
#
# Override defaults from env.sh:
#   MOFKA_NUM_EVENTS=5000 ./consumer.sh
# ==============================================================================

set -euo pipefail

# Source environment if not already loaded
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -z "${MOFKA_WORKDIR:-}" ]; then
    source "${SCRIPT_DIR}/env.sh"
fi

# Verify the server is running
if [ ! -f "${MOFKA_GROUP_FILE}" ]; then
    echo "ERROR: Server group file not found: ${MOFKA_GROUP_FILE}"
    echo "       Start the server first:  source env.sh && ./server.sh"
    exit 1
fi

python3 "${SCRIPT_DIR}/consumer.py" \
    --group-file "${MOFKA_GROUP_FILE}" \
    --topic "${MOFKA_TOPIC}" \
    --num-events "${MOFKA_NUM_EVENTS}" \
    --data-selectivity "${MOFKA_DATA_SELECTIVITY}" \
    --batch-size "${MOFKA_BATCH_SIZE}" \
    --num-threads "${MOFKA_NUM_THREADS}"

echo ""
echo "  Consumer benchmark complete."
echo ""
