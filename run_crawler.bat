@echo off
setlocal

cd /d %~dp0

if not exist .venv (
  echo [1/4] Creating virtual environment...
  python -m venv .venv
)

call .venv\Scripts\activate

echo [2/4] Installing Python packages...
python -m pip install --upgrade pip
pip install -r requirements.txt

echo [3/4] Installing Playwright Chromium...
python -m playwright install chromium

echo [4/4] Starting crawler web UI...
start http://localhost:8501
streamlit run app.py

endlocal
