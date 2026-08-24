#!/usr/bin/env bash
#
# One confirmation run for the S3 (kS3) bdev tier on Ares, against real AWS.
#
# This is NOT where the S3 code is debugged. Signature correctness is settled
# offline by `ctest -R cr_bdev_sigv4` (frozen botocore vectors) and the client's
# wiring by `ctest -R cr_bdev_s3_rest` (a local stand-in that verifies SigV4
# independently). Both run in any build, with no cloud. What is left for this
# script is the one thing those cannot cover: that real AWS accepts what we
# send, through a real CLIO runtime. If it fails, fix it locally and come back
# -- do not grow this file.
#
#   Build (see the plan; no spack view -- RPATH makes one unnecessary):
#     spack env create --without-view clio-s3
#     spack env activate clio-s3
#     spack develop --path ~/clio-core-fork --no-clone iowarp@968-s3-bench
#     spack concretize --force      # REQUIRED: plain concretize omits dev_path
#     spack install
#     export PATH="$(spack location -i /<hash>)/bin:$PATH"
#
#   Run:
#     S3_BENCH_BUCKET=<bucket> ./scripts/ares_s3_bdev_smoke.sh
#
# Environment:
#   S3_BENCH_BUCKET      required; must already exist (this never creates one)
#   AWS_PROFILE          default clio-bench; read from ~/.aws/credentials
#   AWS_DEFAULT_REGION   default us-east-2. SigV4 is region-scoped: a mismatch
#                        comes back 301, not 403.
#   CAE_S3_TOOL          override the path to cae_s3_tool (not on PATH by default)
#
# Objects land at s3://$S3_BENCH_BUCKET/$KEY_PREFIX/block_<offset>. Nothing is
# purged: block keys are deterministic, so a re-run overwrites the same handful
# rather than accumulating, and the footprint stays bounded at a few MiB. To
# remove one by hand:  cae_s3_tool del <bucket> <prefix>/block_<offset>

set -euo pipefail

BUCKET="${S3_BENCH_BUCKET:-}"
PROFILE="${AWS_PROFILE:-clio-bench}"
REGION="${AWS_DEFAULT_REGION:-us-east-2}"
KEY_PREFIX="${S3_KEY_PREFIX:-clio-s3-bdev-smoke}"
BLOB_SIZE="${BLOB_SIZE:-1m}"
NUM_BLOBS="${NUM_BLOBS:-4}"
WORK="$(mktemp -d)"
RUNTIME_PID=""

pass() { printf '  PASS  %s\n' "$*"; }
fail() { printf '  FAIL  %s\n' "$*" >&2; exit 1; }
step() { printf '\n== %s\n' "$*"; }

cleanup() {
  if [[ -n "$RUNTIME_PID" ]]; then
    clio_run runtime stop >/dev/null 2>&1 || true
    sleep 2
    clio_run runtime stop --force >/dev/null 2>&1 || true
  fi
  # /dev/shm is shared with other users on Ares. Only ever our own segments.
  rm -f /dev/shm/chi_* 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

step "Preflight"
[[ -n "$BUCKET" ]] || fail "S3_BENCH_BUCKET is not set"
for tool in clio_run clio_cte_bench; do
  command -v "$tool" >/dev/null || fail "$tool is not on PATH (spack env activated?)"
done
pass "clio_run and clio_cte_bench on PATH"

PREFIX_BIN="$(dirname "$(command -v clio_run)")"
S3_TOOL="${CAE_S3_TOOL:-$PREFIX_BIN/cae_s3_tool}"
[[ -x "$S3_TOOL" ]] || fail "cae_s3_tool not found at $S3_TOOL (needs spack +s3); set CAE_S3_TOOL"
pass "cae_s3_tool at $S3_TOOL"

# The whole point of the Poco rewrite: the AWS SDK must not be in the runtime's
# bdev library. cae_s3_tool above DOES link it -- out of process, which is fine.
BDEV_SO="$(ls "$PREFIX_BIN"/../lib*/libclio_bdev_runtime.so 2>/dev/null | head -1 || true)"
[[ -n "$BDEV_SO" ]] || fail "libclio_bdev_runtime.so not found under $PREFIX_BIN/.."
if ldd "$BDEV_SO" | grep -q aws-cpp-sdk; then
  ldd "$BDEV_SO" | grep aws-cpp-sdk >&2
  fail "the AWS SDK is linked into $BDEV_SO -- this build will corrupt runtime init"
fi
pass "no aws-cpp-sdk in $(basename "$BDEV_SO")"

step "Credentials"
# No AWS CLI on Ares, so `aws configure export-credentials` is unavailable.
# Parse the profile out of ~/.aws/credentials directly. Keys stay in the
# environment of this process only; they are never written to disk here.
eval "$(python3 - "$PROFILE" <<'PY'
import configparser, os, shlex, sys
profile = sys.argv[1]
cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser("~/.aws/credentials"))
if profile not in cfg:
    # stdout is eval'd by the shell, so diagnostics must go to stderr. The
    # caller's AWS_ACCESS_KEY_ID guard turns the empty output into a clean fail.
    sys.exit(f"no profile [{profile}] in ~/.aws/credentials")
sec = cfg[profile]
for env, key in (("AWS_ACCESS_KEY_ID", "aws_access_key_id"),
                 ("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
                 ("AWS_SESSION_TOKEN", "aws_session_token")):
    if sec.get(key):
        print(f"export {env}={shlex.quote(sec[key])}")
PY
)"
[[ -n "${AWS_ACCESS_KEY_ID:-}" ]] || fail "AWS_ACCESS_KEY_ID empty after reading profile [$PROFILE]"
export AWS_DEFAULT_REGION="$REGION"
pass "credentials exported from [$PROFILE], region $REGION"

step "Compose config"
# A single S3-backed bdev pool, with CTE bound to it via existing_pool_id, so
# CTE's own storage parser is not involved. A key prefix is MANDATORY: CTE
# registers targets as <path>_node<N>, so a bare bucket would put the node
# suffix on the bucket name itself.
cat > "$WORK/compose.yaml" <<YAML
runtime:
  num_threads: 4
  queue_depth: 1024

compose:
  - mod_name: clio_bdev
    pool_name: "s3://$BUCKET/$KEY_PREFIX"
    pool_query: local
    pool_id: "360.0"
    bdev_type: s3
    capacity: "64GB"

  - mod_name: clio_cte_core
    pool_name: clio_cte_core
    pool_query: local
    pool_id: "512.0"
    targets:
      neighborhood: 1
      default_target_timeout_ms: 30000
      poll_period_ms: 5000
    storage:
      - path: "s3://$BUCKET/$KEY_PREFIX"
        existing_pool_id: "360.0"
        existing_pool_module: "clio_bdev"
        score: 1.0
        persistence_level: long_term
YAML
pass "compose written to $WORK/compose.yaml"

step "Runtime"
# ipc_mode=IPC, not SHM: Ares sets kernel.yama.ptrace_scope=1, which blocks the
# memfd /proc attach SHM needs for a client that is not a descendant.
export CLIO_IPC_MODE=IPC
rm -f /dev/shm/chi_* 2>/dev/null || true
clio_run runtime start --ephemeral > "$WORK/runtime.log" 2>&1 &
RUNTIME_PID=$!
sleep 8
kill -0 "$RUNTIME_PID" 2>/dev/null || { cat "$WORK/runtime.log" >&2; fail "runtime died during startup"; }
pass "runtime up (pid $RUNTIME_PID)"

# --ephemeral skips the compose section entirely (manager.cc), so compose must
# be applied as its own call.
clio_run compose start "$WORK/compose.yaml" > "$WORK/compose.log" 2>&1 \
  || { cat "$WORK/compose.log" >&2; fail "compose failed -- an S3 bdev Init error appears in runtime.log"; }
grep -q "S3 bdev ready" "$WORK/runtime.log" \
  || { tail -40 "$WORK/runtime.log" >&2; fail "the S3 bdev never reported ready"; }
pass "S3 bdev pool created and bound to CTE"

step "Round trip through CTE"
clio_cte_bench --op PutGet --threads 1 --depth 1 \
  --io-size "$BLOB_SIZE" --io-count "$NUM_BLOBS" > "$WORK/bench.log" 2>&1 \
  || { tail -40 "$WORK/bench.log" >&2; fail "clio_cte_bench PutGet returned non-zero"; }
pass "clio_cte_bench PutGet rc 0"

step "Independent cross-check"
# cae_s3_tool is a separate implementation on the real AWS SDK, so a successful
# GET here confirms the object exists at the key CLIO believes it wrote -- not
# merely that our own client can read back its own mistake.
KEY="$KEY_PREFIX/block_0"
"$S3_TOOL" get "$BUCKET" "$KEY" "$WORK/block_0.bin" >"$WORK/s3tool.log" 2>&1 \
  || { cat "$WORK/s3tool.log" >&2; fail "cae_s3_tool could not GET s3://$BUCKET/$KEY"; }
SIZE=$(stat -c%s "$WORK/block_0.bin")
[[ "$SIZE" -gt 0 ]] || fail "s3://$BUCKET/$KEY is zero bytes"
pass "cae_s3_tool read s3://$BUCKET/$KEY ($SIZE bytes)"

printf '\nSMOKE PASSED -- objects at s3://%s/%s/block_*\n' "$BUCKET" "$KEY_PREFIX"
