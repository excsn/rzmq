#!/bin/sh
# Compiles rzmq under every feature set a workspace build never exercises on its
# own: one `cargo` invocation unifies the features every selected package asks
# of rzmq, so only separate invocations reach the no-default and single-feature
# builds. The library alone is checked per feature; the test targets need the
# defaults and are checked with `full`.
set -eu
cd "$(dirname "$0")/.."

check() {
  echo "== cargo check -p rzmq $*"
  cargo check -q -p rzmq "$@"
}

check --no-default-features --lib
for feature in ipc inproc plain curve noise_xx; do
  check --no-default-features --features "$feature" --lib
done
check --features full --all-targets
if [ "$(uname -s)" = "Linux" ]; then
  check --features full-linux --all-targets
fi
echo "all feature sets compile"
