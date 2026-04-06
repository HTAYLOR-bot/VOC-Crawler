@echo off
setlocal
cd /d "%~dp0"

echo ==== Python ====
python --version

echo ==== Pip ====
pip --version

echo ==== Streamlit ====
python -m streamlit --version

echo ==== Playwright ====
python -m playwright --version

echo ==== Chromium Installed? ====
python -m playwright install --dry-run chromium

pause
endlocal
