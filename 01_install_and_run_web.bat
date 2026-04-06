@echo off
setlocal
cd /d "%~dp0"

set "VENV_DIR=.venv"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"
set "MARKER_FILE=%VENV_DIR%\.installed_ok"

if not exist "%VENV_DIR%\Scripts\python.exe" (
  echo [1/4] Creating virtual environment...
  py -3 -m venv "%VENV_DIR%"
  if errorlevel 1 goto :fail
)

if not exist "%MARKER_FILE%" (
  echo [2/4] Installing dependencies (first-time only)...
  "%PYTHON_EXE%" -m pip install --upgrade pip
  if errorlevel 1 goto :fail
  "%PYTHON_EXE%" -m pip install -r requirements.txt
  if errorlevel 1 goto :fail

  echo [3/4] Installing Playwright Chromium (first-time only)...
  "%PYTHON_EXE%" -m playwright install chromium
  if errorlevel 1 goto :fail

  echo installed=%date% %time%>"%MARKER_FILE%"
) else (
  echo [2/4] Dependencies already installed. Skipping install.
)

echo [4/4] Opening web UI...
start "" http://127.0.0.1:8501
"%PYTHON_EXE%" -m streamlit run app.py
exit /b 0

:fail
echo.
echo Setup failed. Please check the error above.
pause
exit /b 1
