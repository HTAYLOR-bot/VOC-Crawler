@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

set "VENV_DIR=.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "STAMP_FILE=%VENV_DIR%\.walmart_setup_done"

if not exist "%VENV_PY%" (
  echo [1/4] Creating virtual environment...
  py -3 -m venv "%VENV_DIR%"
  if errorlevel 1 (
    echo Failed to create virtual environment.
    pause
    exit /b 1
  )
)

call "%VENV_DIR%\Scripts\activate.bat"
if errorlevel 1 (
  echo Failed to activate virtual environment.
  pause
  exit /b 1
)

if not exist "%STAMP_FILE%" (
  echo [2/4] Installing Python packages (first-time only)...
  python -m pip install --upgrade pip
  pip install -r requirements.txt
  if errorlevel 1 (
    echo Package installation failed.
    pause
    exit /b 1
  )

  echo [3/4] Installing Playwright Chromium (first-time only)...
  python -m playwright install chromium
  if errorlevel 1 (
    echo Playwright browser installation failed.
    pause
    exit /b 1
  )

  echo setup_done>"%STAMP_FILE%"
) else (
  echo Existing environment detected. Skipping install.
)

echo [4/4] Launching web app...
call 02_run_web.bat
endlocal
