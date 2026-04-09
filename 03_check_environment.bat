@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo [INFO] Environment quick check...
call "%~dp0\_bootstrap_python.bat"
if errorlevel 1 goto :fail

echo [INFO] Python executable: %PYTHON_EXE%
"%PYTHON_EXE%" --version
if errorlevel 1 goto :fail

"%PYTHON_EXE%" ensure_env.py
if errorlevel 1 goto :fail

echo.
echo [INFO] Environment looks ready.
pause
exit /b 0

:fail
echo.
echo [FAIL] Environment check failed.
pause
exit /b 1
