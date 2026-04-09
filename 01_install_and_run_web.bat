@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo [INFO] Google Shopping Review Crawler installer starting...
call "%~dp0\_bootstrap_python.bat"
if errorlevel 1 goto :fail

echo [INFO] Preparing Python environment...
"%PYTHON_EXE%" ensure_env.py
if errorlevel 1 (
  echo [ERROR] Environment preparation failed.
  goto :fail
)

echo [INFO] Installation completed.
echo [INFO] Running the web app now...
call "%~dp0\02_run_web.bat"
exit /b %errorlevel%

:fail
echo.
echo [FAIL] The launcher stopped because of an error.
echo Read the lines above and send me the full terminal text.
pause
exit /b 1
