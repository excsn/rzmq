#!/bin/sh
# Runs every test stage on Linux: default features, full-linux on the tokio and
# io_uring backends, then the pyzmq interop suite on both backends.
# Stops at the first failing stage. RUST_LOG defaults to warn.
#
#   scripts/test-linux.sh            all stages
#   scripts/test-linux.sh --soak     also the #[ignore] soak and scaling tests
set -eu
cd "$(dirname "$0")/.."

if [ "$(uname -s)" != "Linux" ]; then
  echo "test-linux.sh runs on Linux only" >&2
  exit 1
fi

if [ ! -d interop/venv ]; then
  echo "interop/venv is missing: run interop/setup_env.sh first" >&2
  exit 1
fi

soak=0
for arg in "$@"; do
  case "$arg" in
    --soak) soak=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 1 ;;
  esac
done

: "${RUST_LOG:=warn}"
export RUST_LOG

passed=""
stage() {
  name="$1"
  shift
  echo
  echo "=== $name"
  if "$@"; then
    passed="$passed
  ok      $name"
  else
    echo
    echo "Stages:$passed
  FAILED  $name"
    exit 1
  fi
}

skip_matrix="--skip every_feature_set_compiles"

stage "default features" \
  cargo test --workspace --exclude rzmq_interop --no-fail-fast
stage "full-linux, tokio" \
  cargo test -p rzmq --features full-linux --no-fail-fast -- $skip_matrix
stage "full-linux, io_uring" \
  env RZMQ_TEST_IO_URING=1 cargo test -p rzmq --features full-linux --no-fail-fast -- $skip_matrix
stage "interop, tokio" \
  sh -c 'cd interop && ./run_tests.sh --no-fail-fast'
stage "interop, io_uring" \
  sh -c 'cd interop && RZMQ_TEST_IO_URING=1 ./run_tests.sh --features io-uring --no-fail-fast'

if [ "$soak" = 1 ]; then
  stage "soak, tokio" \
    cargo test -p rzmq --features full-linux --release -- --ignored
  stage "soak, io_uring" \
    env RZMQ_TEST_IO_URING=1 cargo test -p rzmq --features full-linux --release -- --ignored
fi

echo
echo "Stages:$passed"
