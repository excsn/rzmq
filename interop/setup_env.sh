#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

VENV_DIR="venv"
REQS_FILE="requirements.txt"

echo "--- Setting up Python virtual environment for rzmq interop tests ---"

# 1. Check if python3 is available
if ! command -v python3 &> /dev/null
then
    echo "ERROR: python3 command could not be found."
    echo "Please install Python 3 and ensure it's in your PATH."
    exit 1
fi

# 2. Create the virtual environment if it doesn't exist
if [ -d "$VENV_DIR" ]; then
    echo "Virtual environment '$VENV_DIR' already exists. Skipping creation."
else
    echo "Creating virtual environment in '$VENV_DIR'..."
    python3 -m venv "$VENV_DIR"
fi

# 3. Install dependencies using pip from the virtual environment
PIP_CMD="$VENV_DIR/bin/pip"

echo "Installing/updating dependencies from '$REQS_FILE'..."
"$PIP_CMD" install --upgrade pip
"$PIP_CMD" install -r "$REQS_FILE"

echo ""
echo "--- Setup complete! ---"
echo "You can now run the interop tests using the './run_tests.sh' script."