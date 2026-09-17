#!/usr/bin/env bash
# FinOps Optimizer for BigQuery — macOS / Linux Launcher
# Usage:  ./run.sh
# See README.md for details.
set -euo pipefail

cd "$(dirname "$0")"

echo ""
echo "  ======================================================================"
echo "   FinOps Optimizer for BigQuery"
echo "  ======================================================================"
echo ""

# -------------------------------------------------------------------
# 1. Locate Python
# -------------------------------------------------------------------
PY_CMD=""
if command -v python3 &>/dev/null; then
    PY_CMD="python3"
elif command -v python &>/dev/null; then
    PY_CMD="python"
else
    echo "[ERROR] Python 3 was not found on your PATH."
    echo ""
    echo "Install Python 3.10+ from https://www.python.org/downloads/"
    echo "Or use Google Cloud Shell instead — see README Option 2."
    exit 1
fi

echo "  Using $($PY_CMD --version 2>&1)"
echo ""

# -------------------------------------------------------------------
# 2. Create virtual environment (if absent)
# -------------------------------------------------------------------
if [ ! -f "venv/bin/python" ]; then
    echo "[SETUP] Creating virtual environment..."
    if ! $PY_CMD -m venv venv 2>/dev/null; then
        echo "[INFO]  venv module unavailable — trying virtualenv..."
        $PY_CMD -m pip install --quiet virtualenv
        $PY_CMD -m virtualenv venv
    fi
    echo "[SETUP] Virtual environment created."
fi

VENV_PY="venv/bin/python"
VENV_PIP="venv/bin/pip"
VENV_UVICORN="venv/bin/uvicorn"

# -------------------------------------------------------------------
# 3. Install dependencies (skip if unchanged)
# -------------------------------------------------------------------
SENTINEL="venv/.deps.sha256"
NEED_INSTALL=0

if [ ! -f "$SENTINEL" ]; then
    NEED_INSTALL=1
else
    CURRENT_HASH=$(shasum -a 256 requirements.txt | awk '{print $1}')
    SAVED_HASH=$(tr -d '[:space:]' < "$SENTINEL")
    if [ "$CURRENT_HASH" != "$SAVED_HASH" ]; then
        NEED_INSTALL=1
    fi
fi

if [ "$NEED_INSTALL" -eq 1 ]; then
    echo "[SETUP] Installing dependencies..."
    if "$VENV_PIP" install --quiet -r requirements.txt; then
        shasum -a 256 requirements.txt | awk '{print $1}' > "$SENTINEL"
        echo "[SETUP] Dependencies installed."
    else
        # Check if uvicorn is importable despite pip failure
        if "$VENV_PY" -c "import uvicorn" 2>/dev/null; then
            echo "[WARN]  pip install had errors, but uvicorn is available — continuing."
        else
            echo "[ERROR] Dependency installation failed and uvicorn is not available."
            echo "        Check your network connection, or use Google Cloud Shell instead."
            exit 1
        fi
    fi
else
    echo "[SETUP] Dependencies up to date (cached)."
fi

# -------------------------------------------------------------------
# 4. Security notice
# -------------------------------------------------------------------
echo ""
echo "  [NOTICE] LOCAL LOOPBACK USE ONLY (127.0.0.1)."
echo "           This app has no built-in authentication. Do NOT run"
echo "           these launchers on a shared host, VM, or server."
echo "           For multi-user access, deploy to Cloud Run with"
echo "           --no-allow-unauthenticated (see README)."
echo ""

# -------------------------------------------------------------------
# 5. Start server and poll for readiness
# -------------------------------------------------------------------
echo "[START] Starting server on http://127.0.0.1:8080 ..."

"$VENV_UVICORN" src.main:app --host 127.0.0.1 --port 8080 &
SERVER_PID=$!

# Clean up on exit
trap 'kill $SERVER_PID 2>/dev/null; exit' INT TERM

READY=0
for _ in $(seq 1 30); do
    sleep 1
    if curl -sSf http://127.0.0.1:8080/ >/dev/null 2>&1; then
        READY=1
        break
    fi
done

if [ "$READY" -eq 1 ]; then
    echo "[START] Server is ready."
    echo ""
    echo "  Open: http://127.0.0.1:8080"
    echo ""
    # Auto-open browser (macOS: open, Linux: xdg-open)
    if command -v open &>/dev/null; then
        open "http://127.0.0.1:8080"
    elif command -v xdg-open &>/dev/null; then
        xdg-open "http://127.0.0.1:8080"
    fi
else
    echo "[WARN]  Server did not respond within 30 seconds."
    echo "        Check the terminal output for errors."
    echo "        Try opening http://127.0.0.1:8080 manually."
fi

echo ""
echo "Press Ctrl+C to stop the server."

# Wait for the server process
wait $SERVER_PID
