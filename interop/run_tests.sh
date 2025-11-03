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

# Check if the first argument to this script is a literal "--".
# This handles the common case of users typing `script.sh -- --arg`.
if [ "$1" = "--" ]; then
    # 'shift' discards the first argument ($1) and moves all subsequent
    # arguments ($2, $3, etc.) down by one position.
    shift
fi

# 4. Run the cargo tests
# The "$@" passes along any arguments you provide to this script (e.g., -- --nocapture)
# to cargo test.
echo "Executing cargo test for package 'rzmq_interop'..."
RUST_LOG=info cargo test -p rzmq_interop -- "$@"

echo ""
echo "--- Interop tests completed. ---"