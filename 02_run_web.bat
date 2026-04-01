@echo off
chcp 65001 >nul
setlocal

cd /d %~dp0

if not exist .venv\Scripts\python.exe (
  echo .venv 환경이 없습니다. 먼저 01_install_and_run_web.bat 를 실행하세요.
  pause
  exit /b 1
)

start "WalmartCrawler" cmd /k ".venv\Scripts\python.exe -m streamlit run app.py --server.address 127.0.0.1 --server.port 8501"
timeout /t 3 /nobreak >nul
start "" http://localhost:8501

echo 웹앱 실행 완료.
endlocal
