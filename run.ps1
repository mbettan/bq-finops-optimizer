# FinOps Optimizer for BigQuery — PowerShell Launcher
# Usage:  powershell -ExecutionPolicy Bypass -File .\run.ps1
# See README.md "Option 4: Windows" for details.
#
# NOTE: Do NOT add Set-ExecutionPolicy inside this script — if policy blocks
# scripts, the line never executes (chicken-and-egg).

Write-Host ""
Write-Host "  ======================================================================"
Write-Host "   FinOps Optimizer for BigQuery"
Write-Host "  ======================================================================"
Write-Host ""

# -------------------------------------------------------------------
# 1. Locate Python
# -------------------------------------------------------------------
# Test with --version, not Get-Command, because the MS Store stub
# wins PATH priority but doesn't actually run Python.

$PyCmd = $null

try {
    $null = & python --version 2>&1
    if ($LASTEXITCODE -eq 0) { $PyCmd = "python" }
} catch {}

if (-not $PyCmd) {
    try {
        $null = & py -3 --version 2>&1
        if ($LASTEXITCODE -eq 0) { $PyCmd = "py" }
    } catch {}
}

if (-not $PyCmd) {
    Write-Host "[ERROR] Python 3 was not found on your PATH." -ForegroundColor Red
    Write-Host ""
    Write-Host "Install Python 3.10+ from https://www.python.org/downloads/"
    Write-Host 'IMPORTANT: tick "Add Python to PATH" during installation.'
    Write-Host ""
    Write-Host "No install rights? Use Google Cloud Shell instead — see README Option 2."
    Write-Host "  https://shell.cloud.google.com"
    Read-Host "Press Enter to exit"
    exit 1
}

$pyVersion = & $PyCmd --version 2>&1
Write-Host "  Using $pyVersion"
Write-Host ""

# -------------------------------------------------------------------
# 2. Create virtual environment (if absent)
# -------------------------------------------------------------------
$VenvPy      = ".\venv\Scripts\python.exe"
$VenvPip     = ".\venv\Scripts\pip.exe"
$VenvUvicorn = ".\venv\Scripts\uvicorn.exe"

if (-not (Test-Path $VenvPy)) {
    Write-Host "[SETUP] Creating virtual environment..."
    & $PyCmd -m venv venv 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[INFO]  venv module unavailable — trying virtualenv..."
        & $PyCmd -m pip install --quiet virtualenv 2>&1 | Out-Null
        & $PyCmd -m virtualenv venv 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Could not create virtual environment." -ForegroundColor Red
            Write-Host "        Install virtualenv: $PyCmd -m pip install virtualenv"
            Read-Host "Press Enter to exit"
            exit 1
        }
    }
    Write-Host "[SETUP] Virtual environment created."
}

# -------------------------------------------------------------------
# 3. Install dependencies (skip if unchanged)
# -------------------------------------------------------------------
$Sentinel = "venv\.deps.sha256"
$NeedInstall = $false

if (-not (Test-Path $Sentinel)) {
    $NeedInstall = $true
} else {
    $currentHash = (Get-FileHash -Algorithm SHA256 requirements.txt).Hash
    $savedHash   = (Get-Content $Sentinel -ErrorAction SilentlyContinue).Trim()
    if ($currentHash -ne $savedHash) { $NeedInstall = $true }
}

if ($NeedInstall) {
    Write-Host "[SETUP] Installing dependencies..."
    & $VenvPip install --quiet -r requirements.txt 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        # Check if uvicorn is importable despite pip failure
        & $VenvPy -c "import uvicorn" 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Dependency installation failed and uvicorn is not available." -ForegroundColor Red
            Write-Host "        Check your network connection, or use Google Cloud Shell instead."
            Read-Host "Press Enter to exit"
            exit 1
        }
        Write-Host "[WARN]  pip install had errors, but uvicorn is available — continuing." -ForegroundColor Yellow
    } else {
        $hash = (Get-FileHash -Algorithm SHA256 requirements.txt).Hash
        Set-Content -Path $Sentinel -Value $hash
        Write-Host "[SETUP] Dependencies installed."
    }
} else {
    Write-Host "[SETUP] Dependencies up to date (cached)."
}

# -------------------------------------------------------------------
# 4. Security notice
# -------------------------------------------------------------------
Write-Host ""
Write-Host "  [NOTICE] LOCAL LOOPBACK USE ONLY (127.0.0.1)." -ForegroundColor Yellow
Write-Host "           This app has no built-in authentication. Do NOT run"
Write-Host "           these launchers on a shared host, VM, or server."
Write-Host "           For multi-user access, deploy to Cloud Run with"
Write-Host "           --no-allow-unauthenticated (see README)."
Write-Host ""

# -------------------------------------------------------------------
# 5. Start server and poll for readiness
# -------------------------------------------------------------------
# We invoke the venv Python directly — no Activate.ps1 needed.
# This avoids scope issues and ExecutionPolicy conflicts.

Write-Host "[START] Starting server on http://127.0.0.1:8080 ..."

$serverJob = Start-Process -FilePath $VenvUvicorn `
    -ArgumentList "src.main:app", "--host", "127.0.0.1", "--port", "8080" `
    -PassThru -WindowStyle Hidden

$ready = $false
for ($i = 1; $i -le 30; $i++) {
    Start-Sleep -Seconds 1
    try {
        $resp = Invoke-WebRequest -Uri "http://127.0.0.1:8080/" `
            -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($resp.StatusCode -eq 200) { $ready = $true; break }
    } catch {
        # Server not ready yet
    }
}

if ($ready) {
    Write-Host "[START] Server is ready."
    Write-Host ""
    Write-Host "  Open: http://127.0.0.1:8080"
    Write-Host ""
    Start-Process "http://127.0.0.1:8080"
} else {
    Write-Host "[WARN]  Server did not respond within 30 seconds." -ForegroundColor Yellow
    Write-Host "        Check the terminal output for errors."
    Write-Host "        Try opening http://127.0.0.1:8080 manually."
}

Write-Host ""
Write-Host "Press Ctrl+C to stop the server."

# Keep this process alive and forward to the server
try {
    Wait-Process -Id $serverJob.Id
} catch {
    # User pressed Ctrl+C
} finally {
    if ($serverJob -and -not $serverJob.HasExited) {
        Stop-Process -Id $serverJob.Id -Force -ErrorAction SilentlyContinue
    }
}
