@echo off
chcp 65001 >nul
setlocal

cd /d %~dp0

echo [1/6] Python 확인...
python --version >nul 2>&1
if errorlevel 1 (
  echo Python이 설치되어 있지 않습니다. Python 3.10+ 설치 후 다시 실행하세요.
  pause
  exit /b 1
)

echo [2/6] 가상환경 생성...
if not exist .venv (
  python -m venv .venv
  if errorlevel 1 (
    echo 가상환경 생성 실패
    pause
    exit /b 1
  )
)

echo [3/6] pip 업그레이드...
call .venv\Scripts\python.exe -m pip install --upgrade pip
if errorlevel 1 (
  echo pip 업그레이드 실패
  pause
  exit /b 1
)

echo [4/6] 패키지 설치...
call .venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 (
  echo requirements 설치 실패
  pause
  exit /b 1
)

echo [5/6] Playwright Chromium 설치...
call .venv\Scripts\python.exe -m playwright install chromium
if errorlevel 1 (
  echo Playwright 브라우저 설치 실패
  pause
  exit /b 1
)

echo [6/6] 웹앱 실행...
start "WalmartCrawler" cmd /k ".venv\Scripts\python.exe -m streamlit run app.py --server.address 127.0.0.1 --server.port 8501"
timeout /t 3 /nobreak >nul
start "" http://localhost:8501

echo 설치 및 실행 완료. 브라우저가 자동으로 열립니다.
endlocal
