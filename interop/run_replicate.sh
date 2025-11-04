#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

VENV_DIR="venv"
ENDPOINT="tcp://127.0.0.1:5999"
SCRIPT_NAME="python_scripts/replicate_req_router.py"

echo "--- REQ -> ROUTER with pyzmq ---"

# 1. Check that the virtual environment exists
if [ ! -d "$VENV_DIR" ]; then
    echo "ERROR: Python virtual environment not found in '$VENV_DIR'."
    echo "Please run './setup_env.sh' first to create it."
    exit 1
fi

# 2. Activate the virtual environment
echo "Activating virtual environment..."
export PATH="$(pwd)/$VENV_DIR/bin:$PATH"

# 3. Run the Python replication script
echo "Executing python3 $SCRIPT_NAME $ENDPOINT"
echo "The script will exit with code 0 on SUCCESS (if the send fails as expected)."
echo "It will exit with code 1 on FAILURE (if the send unexpectedly succeeds)."
echo ""

python3 "$SCRIPT_NAME" "$ENDPOINT"

# The 'set -e' at the top ensures that if the python script exits with a non-zero
# status code (failure), this bash script will also fail immediately.

echo ""
echo "--- Replication script finished with expected outcome. ---"