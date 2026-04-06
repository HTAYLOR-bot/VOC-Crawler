@echo off
setlocal
cd /d "%~dp0"

set "PYTHON_EXE=.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" (
  echo Virtual environment not found.
  echo Please run 01_install_and_run_web.bat first.
  pause
  exit /b 1
)

start "" http://127.0.0.1:8501
"%PYTHON_EXE%" -m streamlit run app.py
