#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

VENV_DIR="venv"

echo "--- Running rzmq interoperability tests ---"

# 1. Check that the virtual environment exists
if [ ! -d "$VENV_DIR" ]; then
    echo "ERROR: Python virtual environment not found in '$VENV_DIR'."
    echo "Please run './setup_env.sh' first to create it."
    exit 1
fi

# 2. Activate the virtual environment by prepending its bin to the PATH
# This ensures that 'python3' resolves to the one inside our venv.
echo "Activating virtual environment..."
export PATH="$(pwd)/$VENV_DIR/bin:$PATH"

# 3. Verify that we're using the correct Python and pyzmq (optional but helpful)
echo -n "Using Python from: "
which python3
python3 -c "import zmq; print(f'Using pyzmq version: {zmq.pyzmq_version()}')"
echo ""

# 4. Run the cargo tests.
#
# All arguments are forwarded verbatim to `cargo test`, so you can select what
# runs without editing this script:
#
#   ./run_tests.sh                                  # full suite
#   ./run_tests.sh --test pyzmq_req_router          # one test module
#   ./run_tests.sh --test pyzmq_req_router test_req_to_pyzmq_router   # one test
#   ./run_tests.sh some_test_name                   # name filter across all modules
#   ./run_tests.sh --test pyzmq_req_router -- --nocapture   # forward test-binary args
#
# RUST_LOG defaults to `trace` but is overridable from the environment, e.g.
#   RUST_LOG=warn ./run_tests.sh
: "${RUST_LOG:=trace}"
export RUST_LOG

echo "Executing cargo test for package 'rzmq_interop' (RUST_LOG=$RUST_LOG)..."
cargo test -p rzmq_interop "$@"

echo ""
echo "--- Interop tests completed. ---"
