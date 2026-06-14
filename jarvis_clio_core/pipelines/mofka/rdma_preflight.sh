#!/bin/bash
# rdma_preflight.sh — single-node RDMA readiness checks for Mofka on Ares.
#
# Runs six checks that gate the ofi+verbs sweep. Each emits a parse-able
# PASS/FAIL marker plus full diagnostic output. The decisive one is Check 6:
# it launches bedrock over ofi+verbs and asserts the bound address contains
# 'verbs', which surfaces a missing `verbs;ofi_rxm` provider (Mercury 2.4.1
# derives `verbs;ofi_rxm` from the `verbs` protocol; if libfabric was built
# without rxm, bedrock cannot bind and falls back / fails).
#
# Designed to run via `srun` from the login node so a single shell handles the
# whole allocation. It self-activates conda + spack rather than assuming an
# interactive parent shell did it.
#
# Usage (or just use run_preflight.sh):
#   srun --nodes=1 --partition=compute --exclusive --time=0:30:00 \
#        --immediate=300 -J rdma_preflight \
#        bash ~/clio-core-fork/jarvis_clio_core/pipelines/mofka/rdma_preflight.sh
#
# Exit code: 0 iff every check PASSed.
# Parse-able markers:
#   ^RDMA_PREFLIGHT_RESULT=(PASS|FAIL)$        — overall verdict
#   ^RDMA_PREFLIGHT_CHECK_[1-6]=(PASS|FAIL)$   — per-check verdicts
#   ^RDMA_PREFLIGHT_NODE=<hostname>$           — node the checks ran on
#   ^RDMA_PREFLIGHT_NIC=<mlx[0-9]+_[0-9]+>$    — first PORT_ACTIVE HCA name
#   ^RDMA_PREFLIGHT_BOUND_ADDR=<addr-or-empty>$ — address bedrock bound

set -uo pipefail   # not -e: every check must run even if some fail

REPO_ROOT="${REPO_ROOT:-$HOME/clio-core-fork}"
CONFIG_JSON="$REPO_ROOT/jarvis_clio_core/jarvis_clio_core/mofka_server/config/config.json"

echo "============================================================"
echo "RDMA preflight — single-node Mofka on Ares"
echo "RDMA_PREFLIGHT_NODE=$(hostname)"
echo "RDMA_PREFLIGHT_DATE=$(date -Iseconds)"
echo "============================================================"

# Self-activate conda + spack inside the srun shell.
source ~/miniconda3/etc/profile.d/conda.sh
conda activate iowarp
source ~/spack/share/spack/setup-env.sh
spack env activate ~/mofka-spack-env

PREFLIGHT_FAIL=0
WORKDIR=/mnt/nvme/$USER/rdma-preflight
mkdir -p "$WORKDIR"
cd "$WORKDIR"

# ---------- Check 1: Mellanox HCA active ----------
echo ""
echo "=== Check 1: ibv_devinfo (Mellanox HCA active) ==="
ibv_devinfo > c1.out 2>&1 || true
cat c1.out
# Pass iff at least one hca_id: mlx* line AND PORT_ACTIVE somewhere.
if grep -E "hca_id:[[:space:]]+mlx" c1.out >/dev/null \
   && grep -E "state:[[:space:]]+PORT_ACTIVE" c1.out >/dev/null; then
    NIC=$(awk '/hca_id:[[:space:]]+mlx/{print $2; exit}' c1.out)
    echo "RDMA_PREFLIGHT_NIC=$NIC"
    echo "RDMA_PREFLIGHT_CHECK_1=PASS"
else
    echo "RDMA_PREFLIGHT_NIC="
    echo "RDMA_PREFLIGHT_CHECK_1=FAIL"
    PREFLIGHT_FAIL=1
fi

# ---------- Check 2: libfabric verbs provider visible ----------
echo ""
echo "=== Check 2: fi_info -p verbs ==="
fi_info -p verbs > c2.out 2>&1 || true
cat c2.out
if grep -E "provider:[[:space:]]+verbs" c2.out >/dev/null; then
    echo "RDMA_PREFLIGHT_CHECK_2=PASS"
else
    echo "RDMA_PREFLIGHT_CHECK_2=FAIL"
    PREFLIGHT_FAIL=1
fi

# ---------- Check 3: libfabric built with verbs in fabrics= ----------
echo ""
echo "=== Check 3: libfabric +verbs in spec ==="
spack spec -I libfabric > c3.out 2>&1 || true
cat c3.out
# spack 0.23+ prints fabrics:=sockets,tcp,verbs (colon, comma-joined).
if grep -E "fabrics:?=[^[:space:]]*verbs" c3.out >/dev/null; then
    echo "RDMA_PREFLIGHT_CHECK_3=PASS"
else
    echo "RDMA_PREFLIGHT_CHECK_3=FAIL"
    PREFLIGHT_FAIL=1
fi

# ---------- Check 4: mercury built with +ofi ----------
echo ""
echo "=== Check 4: mercury +ofi in spec ==="
spack spec -I mercury > c4.out 2>&1 || true
cat c4.out
# Match "+ofi" but not "~ofi" or "+ofi_xxx". spack 0.23+ concatenates variants
# without spaces (+mpi+ofi+perf), so don't require whitespace before.
if grep -E "\+ofi([^[:alnum:]_]|$)" c4.out >/dev/null; then
    echo "RDMA_PREFLIGHT_CHECK_4=PASS"
else
    echo "RDMA_PREFLIGHT_CHECK_4=FAIL"
    PREFLIGHT_FAIL=1
fi

# ---------- Check 5: bedrock + deps resolve in spack view ----------
echo ""
echo "=== Check 5: bedrock dynamic deps ==="
BEDROCK_BIN=$(command -v bedrock || true)
echo "bedrock path: $BEDROCK_BIN"
if [ -n "$BEDROCK_BIN" ] && [ -x "$BEDROCK_BIN" ]; then
    ldd "$BEDROCK_BIN" > c5.out 2>&1 || true
    cat c5.out
    # Need both libmercury and libfabric resolved (not "not found").
    MERC_OK=$(grep -E "libmercury\.so" c5.out | grep -v "not found" | head -1)
    LIBF_OK=$(grep -E "libfabric\.so"  c5.out | grep -v "not found" | head -1)
    if [ -n "$MERC_OK" ] && [ -n "$LIBF_OK" ]; then
        echo "RDMA_PREFLIGHT_CHECK_5=PASS"
    else
        echo "RDMA_PREFLIGHT_CHECK_5=FAIL"
        PREFLIGHT_FAIL=1
    fi
else
    echo "bedrock not in PATH after spack env activate"
    echo "RDMA_PREFLIGHT_CHECK_5=FAIL"
    PREFLIGHT_FAIL=1
fi

# ---------- Check 6: bedrock binds ofi+verbs end-to-end ----------
echo ""
echo "=== Check 6: bedrock binds ofi+verbs end-to-end ==="
cp "$CONFIG_JSON" . \
    || { echo "config.json copy failed from $CONFIG_JSON"; \
         echo "RDMA_PREFLIGHT_CHECK_6=FAIL"; PREFLIGHT_FAIL=1; }

if [ -f config.json ]; then
    timeout 15 bedrock 'ofi+verbs' -c config.json > bedrock.log 2>&1 &
    BPID=$!
    # 6s headroom for bedrock to write mofka.json (TCP takes ~2s; verbs adds init time).
    sleep 6
    if [ -f mofka.json ]; then
        echo "--- mofka.json contents ---"
        cat mofka.json
        echo "--- end mofka.json ---"
        BOUND=$(python3 -c "
import json, sys
try:
    d = json.load(open('mofka.json'))
    m = d.get('members', [])
    print(m[0].get('address', '') if m else '')
except Exception as e:
    print(f'<parse error: {e}>', file=sys.stderr)
    sys.exit(1)
" 2>/dev/null || echo "")
        echo "RDMA_PREFLIGHT_BOUND_ADDR=$BOUND"
        if [[ "$BOUND" == *verbs* ]]; then
            echo "RDMA_PREFLIGHT_CHECK_6=PASS"
        else
            echo "RDMA_PREFLIGHT_CHECK_6=FAIL"
            PREFLIGHT_FAIL=1
        fi
    else
        echo "mofka.json did not appear after 6s"
        echo "RDMA_PREFLIGHT_BOUND_ADDR="
        echo "RDMA_PREFLIGHT_CHECK_6=FAIL"
        PREFLIGHT_FAIL=1
    fi
    kill $BPID 2>/dev/null || true
    wait $BPID 2>/dev/null || true
    echo ""
    echo "--- bedrock.log tail (50 lines) ---"
    tail -50 bedrock.log
    echo "--- end bedrock.log tail ---"
fi

# ---------- Always cleanup, even on failure ----------
cd ~
rm -rf "$WORKDIR"

# ---------- Final summary ----------
echo ""
echo "============================================================"
echo "RDMA preflight summary"
echo "============================================================"
if [ $PREFLIGHT_FAIL -eq 0 ]; then
    echo "RDMA_PREFLIGHT_RESULT=PASS"
else
    echo "RDMA_PREFLIGHT_RESULT=FAIL"
fi
exit $PREFLIGHT_FAIL
