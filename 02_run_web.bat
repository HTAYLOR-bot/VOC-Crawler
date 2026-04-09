@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo [INFO] Google Shopping Review Crawler starting...
call "%~dp0\_bootstrap_python.bat"
if errorlevel 1 goto :fail

echo [INFO] Checking Python packages and browser runtime...
"%PYTHON_EXE%" ensure_env.py
if errorlevel 1 (
  echo [ERROR] Environment check failed.
  goto :fail
)

echo [INFO] Waiting to open browser until server is actually ready...
start "BrowserWait" "%PYTHON_EXE%" open_when_ready.py

echo [INFO] Starting Flask server...
"%PYTHON_EXE%" -u launch_server.py
set "SERVER_EXIT=%errorlevel%"

if not "%SERVER_EXIT%"=="0" (
  echo [ERROR] Server stopped with an error. See server_boot.log too.
  goto :fail
)

goto :end

:fail
echo.
echo [FAIL] The launcher stopped because of an error.
echo Read the lines above and send me the full terminal text.
echo Log file: server_boot.log
pause
exit /b 1

:end
echo.
echo [INFO] Server stopped normally.
pause
exit /b 0
