@echo off
setlocal EnableExtensions
cd /d "%~dp0"

set "PY_BOOTSTRAP="
if exist ".venv\Scripts\python.exe" (
  endlocal & set "PYTHON_EXE=%cd%\.venv\Scripts\python.exe" & exit /b 0
)

where py >nul 2>nul
if %errorlevel%==0 (
  set "PY_BOOTSTRAP=py -3"
  goto :make_venv
)

where python >nul 2>nul
if %errorlevel%==0 (
  set "PY_BOOTSTRAP=python"
  goto :make_venv
)

echo [ERROR] Python was not found.
echo Install Python 3.10+ first, then run this BAT again.
endlocal & exit /b 1

:make_venv
echo [INFO] Creating virtual environment...
%PY_BOOTSTRAP% -m venv .venv
if errorlevel 1 (
  echo [ERROR] Failed to create virtual environment.
  endlocal & exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Virtual environment python was not created correctly.
  endlocal & exit /b 1
)

endlocal & set "PYTHON_EXE=%cd%\.venv\Scripts\python.exe" & exit /b 0
