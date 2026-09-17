@echo off
setlocal enabledelayedexpansion

:: FinOps Optimizer for BigQuery — Windows CMD Launcher
:: Double-click this file to start the application locally.
:: See README.md "Option 4: Windows" for details.

echo.
echo  ======================================================================
echo   FinOps Optimizer for BigQuery
echo  ======================================================================
echo.

:: ---------------------------------------------------------------
:: 1. Locate Python
:: ---------------------------------------------------------------
:: We test with `--version` rather than `where`, because the
:: Windows Store stub wins PATH priority but prints an error
:: instead of running Python.

set "PY_CMD="

python --version >nul 2>&1
if !errorlevel! equ 0 (
    set "PY_CMD=python"
    goto :found_python
)

py -3 --version >nul 2>&1
if !errorlevel! equ 0 (
    set "PY_CMD=py -3"
    goto :found_python
)

echo [ERROR] Python 3 was not found on your PATH.
echo.
echo Install Python 3.10+ from https://www.python.org/downloads/
echo IMPORTANT: tick "Add Python to PATH" during installation.
echo.
echo No install rights? Use Google Cloud Shell instead — see README Option 2.
echo   https://shell.cloud.google.com
echo.
pause
exit /b 1

:found_python
for /f "tokens=*" %%v in ('!PY_CMD! --version 2^>^&1') do echo   Using %%v
echo.

:: ---------------------------------------------------------------
:: 2. Create virtual environment (if absent)
:: ---------------------------------------------------------------
if not exist "venv\Scripts\python.exe" (
    echo [SETUP] Creating virtual environment...
    !PY_CMD! -m venv venv >nul 2>&1
    if !errorlevel! neq 0 (
        echo [INFO]  venv module unavailable — trying virtualenv...
        !PY_CMD! -m pip install --quiet virtualenv >nul 2>&1
        !PY_CMD! -m virtualenv venv >nul 2>&1
        if !errorlevel! neq 0 (
            echo [ERROR] Could not create virtual environment.
            echo         Install virtualenv: !PY_CMD! -m pip install virtualenv
            pause
            exit /b 1
        )
    )
    echo [SETUP] Virtual environment created.
)

set "VENV_PY=venv\Scripts\python.exe"
set "VENV_PIP=venv\Scripts\pip.exe"
set "VENV_UVICORN=venv\Scripts\uvicorn.exe"

:: ---------------------------------------------------------------
:: 3. Install dependencies (skip if unchanged)
:: ---------------------------------------------------------------
:: We hash requirements.txt and only re-run pip when it changes.
:: This saves 5-20s on every launch after the first one.

set "SENTINEL=venv\.deps.sha256"
set "NEED_INSTALL=0"

if not exist "%SENTINEL%" (
    set "NEED_INSTALL=1"
) else (
    for /f "tokens=*" %%h in ('certutil -hashfile requirements.txt SHA256 2^>nul ^| findstr /v "hash CertUtil"') do (
        set "CURRENT_HASH=%%h"
    )
    set /p SAVED_HASH=<"%SENTINEL%"
    if not "!CURRENT_HASH!"=="!SAVED_HASH!" set "NEED_INSTALL=1"
)

if !NEED_INSTALL! equ 1 (
    echo [SETUP] Installing dependencies...
    "!VENV_PIP!" install --quiet -r requirements.txt >nul 2>&1
    if !errorlevel! neq 0 (
        :: Check if uvicorn is importable despite the pip failure
        "!VENV_PY!" -c "import uvicorn" >nul 2>&1
        if !errorlevel! neq 0 (
            echo [ERROR] Dependency installation failed and uvicorn is not available.
            echo         Check your network connection, or use Google Cloud Shell instead.
            pause
            exit /b 1
        )
        echo [WARN]  pip install had errors, but uvicorn is available — continuing.
    ) else (
        :: Save the hash for next time
        for /f "tokens=*" %%h in ('certutil -hashfile requirements.txt SHA256 2^>nul ^| findstr /v "hash CertUtil"') do (
            echo %%h> "%SENTINEL%"
        )
        echo [SETUP] Dependencies installed.
    )
) else (
    echo [SETUP] Dependencies up to date ^(cached^).
)

:: ---------------------------------------------------------------
:: 4. Security notice
:: ---------------------------------------------------------------
echo.
echo  [NOTICE] LOCAL LOOPBACK USE ONLY (127.0.0.1).
echo           This app has no built-in authentication. Do NOT run
echo           these launchers on a shared host, VM, or server.
echo           For multi-user access, deploy to Cloud Run with
echo           --no-allow-unauthenticated (see README).
echo.

:: ---------------------------------------------------------------
:: 5. Start the server in the background and poll for readiness
:: ---------------------------------------------------------------
echo [START] Starting server on http://127.0.0.1:8080 ...

start "" /b "!VENV_UVICORN!" src.main:app --host 127.0.0.1 --port 8080

set "READY=0"
for /l %%i in (1,1,30) do (
    if !READY! equ 0 (
        timeout /t 1 /nobreak >nul
        "!VENV_PY!" -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/', timeout=2)" >nul 2>&1
        if !errorlevel! equ 0 (
            set "READY=1"
        )
    )
)

if !READY! equ 1 (
    echo [START] Server is ready.
    echo.
    echo   Open: http://127.0.0.1:8080
    echo.
    start "" http://127.0.0.1:8080
) else (
    echo [WARN]  Server did not respond within 30 seconds.
    echo         Check the terminal output above for errors.
    echo         You can try opening http://127.0.0.1:8080 manually.
)

:: Keep the window open so the user can see the server logs
echo.
echo Press Ctrl+C to stop the server.
"!VENV_UVICORN!" src.main:app --host 127.0.0.1 --port 8080
pause
