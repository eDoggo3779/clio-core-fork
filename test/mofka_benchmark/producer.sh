#!/usr/bin/env bash
# ==============================================================================
# producer.sh — Run this in TERMINAL 2 (after the server is up).
#
# Pushes events into Mofka and reports throughput.
#
# Usage:
#   source env.sh
#   ./producer.sh
#
# Override defaults from env.sh:
#   MOFKA_NUM_EVENTS=5000 MOFKA_DATA_SIZE=4096 ./producer.sh
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

python3 "${SCRIPT_DIR}/producer.py" \
    --group-file "${MOFKA_GROUP_FILE}" \
    --topic "${MOFKA_TOPIC}" \
    --num-events "${MOFKA_NUM_EVENTS}" \
    --data-size "${MOFKA_DATA_SIZE}" \
    --metadata-size "${MOFKA_METADATA_SIZE}" \
    --batch-size "${MOFKA_BATCH_SIZE}" \
    --num-threads "${MOFKA_NUM_THREADS}"

echo ""
echo "  Producer benchmark complete."
echo "  Now run:  ./consumer.sh"
echo ""
