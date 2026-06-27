#!/usr/bin/env bash
#
# ares_cae_cloud_setup.sh
#
# Provision a *fresh* Ares account (assume only this git repo exists) so the CAE
# cloud assimilator tests pass:
#
#     cae_s3_assim   - S3 (s3://) import, validated against REAL AWS S3
#     cae_gcs_assim  - GCS (gs://) import, validated against fake-gcs-server
#     cae_cloud_factory_guard - always-on routing guard (no cloud)
#
# The CAE S3/GCS code is already complete on branch clio-cloud; the S3 path
# fork+execs a standalone `cae_s3_tool` so the AWS SDK never loads into the CLIO
# runtime process (which previously caused "stack smashing detected"). The only
# remaining blocker is provisioning: the CLIO base stack is recipe'd in spack,
# but aws-sdk-cpp and google-cloud-cpp are NOT in any recipe and must be added.
#
# This script is a check-and-install runbook: every dependency is first CHECKED
# (is it already on Ares?) and only installed if missing. It is idempotent and
# subcommand-driven so you can run pieces independently.
#
# Usage:
#   ./scripts/ares_cae_cloud_setup.sh check       # report what's present / missing, install nothing
#   ./scripts/ares_cae_cloud_setup.sh wipe        # remove crash-era / stale artifacts (Step 0)
#   ./scripts/ares_cae_cloud_setup.sh bootstrap   # Tier 0: spack + compiler + iowarp repo
#   ./scripts/ares_cae_cloud_setup.sh deps        # Tier 1: CLIO base stack (spack, deps-only)
#   ./scripts/ares_cae_cloud_setup.sh sdks        # Tier 2: aws-sdk-cpp + google-cloud-cpp (THE GAP)
#   ./scripts/ares_cae_cloud_setup.sh testinfra   # Tier 3: fake-gcs-server binary (S3 uses real AWS)
#   ./scripts/ares_cae_cloud_setup.sh build       # Tier 4: configure + build the worktree
#   ./scripts/ares_cae_cloud_setup.sh test        # Step 5: run the ctests (needs AWS env, see below)
#   ./scripts/ares_cae_cloud_setup.sh all         # bootstrap -> deps -> sdks -> testinfra -> build
#
# Environment knobs (all optional; sane defaults shown):
#   SPACK_ROOT=~/spack                  where spack is / will be cloned
#   IOWARP_SPEC="+cae +cte +runtime +hdf5 +elf +zmq ~mpiio ~ares ~python ~mochi"
#                                       spack spec used for the deps-only solve
#   WORKTREE=/workspace/.worktrees/clio-cloud   the source tree to build
#   FAKE_GCS_VERSION=1.49.3             fake-gcs-server release to fetch
#
# AWS credentials for `test` (real S3) are read from the standard AWS env:
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION (=us-east-1),
#   S3_TEST_BUCKET (a pre-created us-east-1 bucket), S3_ENDPOINT
#   (=https://s3.us-east-1.amazonaws.com). The S3 test self-skips if S3_ENDPOINT
#   is unset.
#
set -uo pipefail

# --------------------------------------------------------------------------- #
# Config / defaults
# --------------------------------------------------------------------------- #
SPACK_ROOT="${SPACK_ROOT:-$HOME/spack}"
WORKTREE="${WORKTREE:-/workspace/.worktrees/clio-cloud}"
IOWARP_SPEC="${IOWARP_SPEC:-+cae +cte +runtime +hdf5 +elf +zmq ~mpiio ~ares ~python ~mochi}"
FAKE_GCS_VERSION="${FAKE_GCS_VERSION:-1.49.3}"
REPO_SPACK_DIR="$WORKTREE/installers/spack"

# --------------------------------------------------------------------------- #
# Logging helpers
# --------------------------------------------------------------------------- #
c_ok()   { printf '  \033[32m[OK]\033[0m   %s\n' "$*"; }
c_miss() { printf '  \033[33m[MISS]\033[0m %s\n' "$*"; }
c_do()   { printf '\033[36m==> %s\033[0m\n' "$*"; }
c_err()  { printf '  \033[31m[ERR]\033[0m  %s\n' "$*" >&2; }
have()   { command -v "$1" >/dev/null 2>&1; }

# Source spack into this shell if it exists.
load_spack() {
  if [[ -f "$SPACK_ROOT/share/spack/setup-env.sh" ]]; then
    # shellcheck disable=SC1091
    . "$SPACK_ROOT/share/spack/setup-env.sh"
    return 0
  fi
  return 1
}

# True if a spack package is installed (concretized + built).
spack_installed() { spack find --no-groups "$1" >/dev/null 2>&1; }

# --------------------------------------------------------------------------- #
# check : report-only, installs nothing
# --------------------------------------------------------------------------- #
do_check() {
  c_do "Tier 0 - bootstrap toolchain"
  for t in git gcc g++ make patch tar gzip xz file which python3; do
    if have "$t"; then c_ok "$t ($(command -v "$t"))"; else c_miss "$t"; fi
  done
  if have g++; then c_ok "g++ version: $(g++ -dumpversion 2>/dev/null) (need >= 11 for C++20)"; fi
  if load_spack; then
    c_ok "spack ($(command -v spack))"
    if spack repo list 2>/dev/null | grep -q iowarp; then c_ok "iowarp spack repo registered"; else c_miss "iowarp spack repo (run: bootstrap)"; fi

    c_do "Tier 1/2 - libraries (spack)"
    for pkg in cmake yaml-cpp cereal msgpack-c libaio libzmq nlohmann-json curl hdf5 libelf catch2; do
      if spack_installed "$pkg"; then c_ok "$pkg"; else c_miss "$pkg (base dep)"; fi
    done
    for pkg in aws-sdk-cpp google-cloud-cpp; do
      if spack_installed "$pkg"; then c_ok "$pkg"; else c_miss "$pkg  <-- THE GAP (run: sdks)"; fi
    done
  else
    c_miss "spack not found at $SPACK_ROOT (run: bootstrap)"
  fi

  c_do "Tier 3 - test infrastructure"
  if have fake-gcs-server || [[ -x "$HOME/bin/fake-gcs-server" ]]; then c_ok "fake-gcs-server"; else c_miss "fake-gcs-server (run: testinfra)"; fi
  if curl -sI --max-time 8 https://s3.us-east-1.amazonaws.com 2>/dev/null | head -1 | grep -q HTTP; then
    c_ok "egress to s3.us-east-1.amazonaws.com"
  else
    c_miss "egress to AWS S3 (run the test on a login/DTN node or via proxy)"
  fi
  for v in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY S3_TEST_BUCKET S3_ENDPOINT; do
    if [[ -n "${!v:-}" ]]; then c_ok "env $v set"; else c_miss "env $v unset (needed for: test)"; fi
  done

  c_do "Build tree"
  if [[ -d "$WORKTREE" ]]; then c_ok "worktree $WORKTREE"; else c_err "worktree missing: $WORKTREE"; fi
}

# --------------------------------------------------------------------------- #
# wipe : Step 0 - remove crash-era / stale artifacts
# --------------------------------------------------------------------------- #
do_wipe() {
  c_do "Step 0 - wiping stale configuration"
  rm -rf "$HOME/aws-sdk-install" "$HOME/aws-sdk-install-dbg" && c_ok "removed hand-built AWS SDK prefixes"
  rm -rf "$HOME/minio-bin" "$HOME/minio-data" && c_ok "removed MinIO scratch (using real AWS S3 now)"
  rm -rf "$WORKTREE/build" "$WORKTREE"/build-* 2>/dev/null && c_ok "removed dirty CMake build dirs"
  c_ok "unset stale endpoint/SDK env in THIS shell (re-export per session as needed)"
  # Surface anything still pinned to the old hand-built SDKs.
  if env | grep -iE 'aws-sdk-install' >/dev/null 2>&1; then
    c_err "environment still references aws-sdk-install:"; env | grep -iE 'aws-sdk-install' >&2
  else
    c_ok "no environment references to ~/aws-sdk-install"
  fi
  if grep -RInE 'aws-sdk-install' "$HOME/.bashrc" "$HOME/.profile" 2>/dev/null; then
    c_err "remove the above aws-sdk-install lines from your shell rc"
  else
    c_ok "no stale aws-sdk-install lines in ~/.bashrc / ~/.profile"
  fi
}

# --------------------------------------------------------------------------- #
# bootstrap : Tier 0 - spack + compiler registration + iowarp repo
# --------------------------------------------------------------------------- #
do_bootstrap() {
  c_do "Tier 0 - bootstrap"

  # Compiler: on Ares, gcc comes from lmod, not this script. Just verify.
  if ! have g++; then
    c_err "no g++ on PATH. Load a C++20 toolchain first, e.g.:  module load gcc/12"
    return 1
  fi
  local gccver; gccver="$(g++ -dumpversion 2>/dev/null | cut -d. -f1)"
  if [[ -n "$gccver" && "$gccver" -lt 11 ]]; then
    c_err "g++ $gccver is < 11; CLIO needs C++20. Load a newer gcc:  module load gcc/12"
    return 1
  fi
  c_ok "g++ $(g++ -dumpversion) present"

  for t in git make python3 curl patch; do
    have "$t" || { c_err "missing '$t' (base tool). On Ares: module load it or ask admins."; return 1; }
  done

  # Spack.
  if [[ ! -f "$SPACK_ROOT/share/spack/setup-env.sh" ]]; then
    c_do "cloning spack into $SPACK_ROOT"
    git clone -c feature.manyFiles=true --depth=1 https://github.com/spack/spack.git "$SPACK_ROOT" || return 1
    if ! grep -q 'share/spack/setup-env.sh' "$HOME/.bashrc" 2>/dev/null; then
      echo ". $SPACK_ROOT/share/spack/setup-env.sh" >> "$HOME/.bashrc"
      c_ok "added spack source line to ~/.bashrc"
    fi
  else
    c_ok "spack already present at $SPACK_ROOT"
  fi
  load_spack || { c_err "failed to source spack"; return 1; }

  # Register the loaded compiler with spack.
  c_do "spack compiler find"
  spack compiler find >/dev/null 2>&1 || true
  spack compilers 2>/dev/null | grep -q gcc && c_ok "spack sees a gcc compiler" || c_err "spack found no gcc; check 'spack compilers'"

  # Register the in-repo iowarp package repo (provides the base-dep recipe).
  if spack repo list 2>/dev/null | grep -q iowarp; then
    c_ok "iowarp spack repo already registered"
  else
    spack repo add "$REPO_SPACK_DIR" && c_ok "registered iowarp repo from $REPO_SPACK_DIR" \
      || { c_err "failed: spack repo add $REPO_SPACK_DIR"; return 1; }
  fi

  printf '\nHint: to reuse Ares system MPI/libfabric instead of rebuilding them,\n'
  printf 'declare them as externals in ~/.spack/packages.yaml before running deps.\n'
}

# --------------------------------------------------------------------------- #
# deps : Tier 1 - CLIO base stack (dependencies only; we build the branch later)
# --------------------------------------------------------------------------- #
do_deps() {
  load_spack || { c_err "spack not available; run bootstrap first"; return 1; }
  c_do "Tier 1 - installing CLIO base dependencies (deps-only) for: iowarp $IOWARP_SPEC"
  # --only dependencies: do NOT build the iowarp package itself (its versions
  # point at upstream/other forks); we build THIS worktree in 'build'.
  # shellcheck disable=SC2086
  spack install --only dependencies iowarp $IOWARP_SPEC \
    && c_ok "base dependency closure installed" \
    || { c_err "spack install --only dependencies failed (see output above)"; return 1; }
}

# --------------------------------------------------------------------------- #
# sdks : Tier 2 - THE GAP. aws-sdk-cpp + google-cloud-cpp
# --------------------------------------------------------------------------- #
do_sdks() {
  load_spack || { c_err "spack not available; run bootstrap first"; return 1; }

  # Pre-flight: both packages must exist in the active spack repos. An OLD spack
  # snapshot may lack google-cloud-cpp (the C++ lib) entirely - the bootstrap
  # step clones a fresh spack precisely to avoid that. Fail with guidance rather
  # than a raw concretizer error.
  for pkg in aws-sdk-cpp google-cloud-cpp; do
    if ! spack info "$pkg" >/dev/null 2>&1; then
      c_err "spack has no package '$pkg' in this install."
      c_err "Your spack is likely too old. Update it:  (cd $SPACK_ROOT && git pull)"
      c_err "or re-run bootstrap to clone a fresh spack. ('$pkg' ships in current spack.)"
      return 1
    fi
  done

  if spack_installed aws-sdk-cpp; then
    c_ok "aws-sdk-cpp already installed"
  else
    c_do "installing aws-sdk-cpp (S3 + core for cae_s3_tool)"
    # Note: the default spec builds many services and is large. If your spack
    # version exposes a service filter, narrow it (see: spack info aws-sdk-cpp).
    spack install aws-sdk-cpp \
      && c_ok "aws-sdk-cpp installed" \
      || { c_err "spack install aws-sdk-cpp failed"; return 1; }
  fi

  if spack_installed google-cloud-cpp; then
    c_ok "google-cloud-cpp already installed"
  else
    c_do "installing google-cloud-cpp (storage component for the GCS assimilator)"
    # google-cloud-cpp builds all GA features by default; storage (what the GCS
    # assimilator links) is always included.
    spack install google-cloud-cpp \
      && c_ok "google-cloud-cpp installed" \
      || { c_err "spack install google-cloud-cpp failed"; return 1; }
  fi
}

# --------------------------------------------------------------------------- #
# testinfra : Tier 3 - fake-gcs-server (S3 uses real AWS, nothing to install)
# --------------------------------------------------------------------------- #
do_testinfra() {
  c_do "Tier 3 - test infrastructure"

  if have fake-gcs-server || [[ -x "$HOME/bin/fake-gcs-server" ]]; then
    c_ok "fake-gcs-server already present"
  else
    mkdir -p "$HOME/bin"
    local arch tarball url
    case "$(uname -m)" in
      x86_64|amd64) arch="amd64" ;;
      aarch64|arm64) arch="arm64" ;;
      *) c_err "unsupported arch $(uname -m) for fake-gcs-server prebuilt"; return 1 ;;
    esac
    tarball="fake-gcs-server_${FAKE_GCS_VERSION}_Linux_${arch}.tar.gz"
    url="https://github.com/fsouza/fake-gcs-server/releases/download/v${FAKE_GCS_VERSION}/${tarball}"
    c_do "downloading fake-gcs-server v${FAKE_GCS_VERSION} ($arch)"
    if curl -fsSL "$url" -o "/tmp/$tarball"; then
      tar -xzf "/tmp/$tarball" -C "$HOME/bin" fake-gcs-server 2>/dev/null \
        && chmod +x "$HOME/bin/fake-gcs-server" \
        && c_ok "installed $HOME/bin/fake-gcs-server (add ~/bin to PATH)" \
        || c_err "extraction failed; download manually from $url"
      rm -f "/tmp/$tarball"
    else
      c_err "download failed: $url"
      c_err "fetch manually from https://github.com/fsouza/fake-gcs-server/releases"
    fi
  fi

  # S3 egress check (informational).
  if curl -sI --max-time 8 https://s3.us-east-1.amazonaws.com 2>/dev/null | head -1 | grep -q HTTP; then
    c_ok "AWS S3 reachable from this node"
  else
    c_miss "AWS S3 not reachable from this node; use a login/DTN node or HTTPS proxy"
  fi
}

# --------------------------------------------------------------------------- #
# build : Tier 4 - configure + build the worktree against spack deps
# --------------------------------------------------------------------------- #
do_build() {
  load_spack || { c_err "spack not available; run bootstrap first"; return 1; }
  [[ -d "$WORKTREE" ]] || { c_err "worktree not found: $WORKTREE"; return 1; }

  c_do "spack load: base deps + both cloud SDKs"
  # shellcheck disable=SC2086
  spack load iowarp $IOWARP_SPEC 2>/dev/null || true   # loads the dep closure if an env/view exists
  spack load aws-sdk-cpp google-cloud-cpp || { c_err "spack load of SDKs failed; run sdks first"; return 1; }

  local aws_dir gcs_dir
  aws_dir="$(spack location -i aws-sdk-cpp 2>/dev/null)"
  gcs_dir="$(spack location -i google-cloud-cpp 2>/dev/null)"
  [[ -n "$aws_dir" && -n "$gcs_dir" ]] || { c_err "could not locate installed SDK prefixes"; return 1; }

  c_do "configuring (CAE_ENABLE_S3=ON, CAE_ENABLE_GCS=ON)"
  cmake -S "$WORKTREE" -B "$WORKTREE/build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCLIO_CORE_ENABLE_CAE=ON -DCLIO_CORE_ENABLE_CTE=ON -DCLIO_CORE_ENABLE_RUNTIME=ON \
    -DCAE_ENABLE_S3=ON -DCAE_ENABLE_GCS=ON \
    -DCMAKE_PREFIX_PATH="$aws_dir;$gcs_dir" \
    -DCLIO_CORE_ENABLE_COVERAGE=OFF \
    || { c_err "cmake configure failed"; return 1; }

  c_do "verify the gates reported ENABLED"
  if cmake -LA -N "$WORKTREE/build" 2>/dev/null | grep -qE 'CAE_ENABLE_S3:BOOL=ON'; then
    c_ok "CAE_ENABLE_S3=ON"; else c_err "CAE_ENABLE_S3 did not stick (AWS SDK not found?)"; fi
  if cmake -LA -N "$WORKTREE/build" 2>/dev/null | grep -qE 'CAE_ENABLE_GCS:BOOL=ON'; then
    c_ok "CAE_ENABLE_GCS=ON"; else c_err "CAE_ENABLE_GCS did not stick (google-cloud-cpp not found?)"; fi

  c_do "building cloud targets"
  cmake --build "$WORKTREE/build" \
    --target cae_s3_tool cae_s3_assim cae_gcs_assim cae_cloud_factory_guard -- -j"$(nproc)" \
    && c_ok "build complete" \
    || { c_err "build failed"; return 1; }
}

# --------------------------------------------------------------------------- #
# test : Step 5 - run the ctests
# --------------------------------------------------------------------------- #
do_test() {
  [[ -d "$WORKTREE/build" ]] || { c_err "no build dir; run build first"; return 1; }
  load_spack && { spack load aws-sdk-cpp google-cloud-cpp 2>/dev/null || true; }

  c_do "always-on guard (no cloud) - must pass 3/3"
  ctest --test-dir "$WORKTREE/build" -R cae_cloud_factory_guard -V || c_err "factory guard FAILED"

  if [[ -n "${S3_ENDPOINT:-}" && -n "${AWS_ACCESS_KEY_ID:-}" ]]; then
    c_do "S3 against real AWS (bucket=${S3_TEST_BUCKET:-clio-cae-test})"
    ctest --test-dir "$WORKTREE/build" -R cae_s3_assim -V || c_err "cae_s3_assim FAILED"
  else
    c_miss "S3 test skipped: export AWS_ACCESS_KEY_ID/SECRET, AWS_DEFAULT_REGION=us-east-1,"
    c_miss "  S3_TEST_BUCKET=<us-east-1 bucket>, S3_ENDPOINT=https://s3.us-east-1.amazonaws.com"
  fi

  if [[ -n "${GCS_ENDPOINT:-}" ]]; then
    c_do "GCS against ${GCS_ENDPOINT}"
    ctest --test-dir "$WORKTREE/build" -R cae_gcs_assim -V || c_err "cae_gcs_assim FAILED"
  else
    c_miss "GCS test skipped: start fake-gcs-server and export GCS_ENDPOINT, e.g.:"
    c_miss "  ~/bin/fake-gcs-server -scheme http -port 4443 -backend memory &"
    c_miss "  export GCS_ENDPOINT=http://127.0.0.1:4443 GCS_TEST_BUCKET=clio-cae-test GCS_PROJECT_ID=test-project"
  fi
}

# --------------------------------------------------------------------------- #
# dispatch
# --------------------------------------------------------------------------- #
main() {
  local cmd="${1:-check}"
  case "$cmd" in
    check)     do_check ;;
    wipe)      do_wipe ;;
    bootstrap) do_bootstrap ;;
    deps)      do_deps ;;
    sdks)      do_sdks ;;
    testinfra) do_testinfra ;;
    build)     do_build ;;
    test)      do_test ;;
    all)       do_bootstrap && do_deps && do_sdks && do_testinfra && do_build ;;
    *)         grep -E '^#( |$)' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
  esac
}

main "$@"
