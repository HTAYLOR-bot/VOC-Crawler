# Walmart US Review Crawler (Brand/Product/Size + Date Range)

이 프로젝트는 Walmart 미국 사이트에서 특정 브랜드/상품명을 기반으로 상품을 찾고, 각 상품의 모든 리뷰를 수집해 CSV로 내려받을 수 있는 Streamlit 웹 UI 크롤러입니다.

## 주요 기능
- 브랜드 + 상품명으로 Walmart 검색
- 검색 결과 상품별 리뷰 수집
- `from`, `to` 날짜 범위 필터링 (`to` 미입력 시 현재까지)
- 리뷰 컬럼화 저장 (CSV)
  - 브랜드
  - 상품명
  - 사이즈
  - 업로드 날짜
  - 계정이름
  - 별점
  - 리뷰 제목
  - 리뷰

## Windows/Streamlit 호환성
- Playwright는 **`async_playwright`** 기반으로 동작합니다.
- Windows에서 Streamlit 버튼 클릭 시 발생할 수 있는 이벤트 루프 충돌(`NotImplementedError`)을 피하기 위해,
  동기 wrapper에서 **새 이벤트 루프(Windows Proactor 정책 우선)** 로 비동기 크롤러를 실행합니다.

## 실행 방법

### 1) 공통
```bash
python -m venv .venv
```

### 2) Windows (PowerShell)
```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install chromium
streamlit run app.py
```

### 3) macOS / Linux
```bash
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
streamlit run app.py
```

브라우저에서 `http://localhost:8501` 접속 후 입력값을 넣고 크롤링을 실행하세요.

## 테스트 기준
- 정적 문법 확인:
  - `python -m py_compile app.py`
- Windows 수동 확인:
  1. Streamlit 실행 후 `크롤링 시작` 클릭
  2. `NotImplementedError` 없이 크롤링 로그가 표시되는지 확인
  3. 결과 테이블/CSV 다운로드가 정상 동작하는지 확인

## 주의사항
- Walmart 페이지 구조는 수시로 변경될 수 있어, 셀렉터 조정이 필요할 수 있습니다.
- 많은 페이지를 크롤링할 경우 시간이 오래 걸릴 수 있습니다.
- 웹사이트 이용약관/robots/법적 정책을 준수하세요.
