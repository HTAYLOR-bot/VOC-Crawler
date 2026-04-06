# Walmart US Review Crawler (Brand/Product/Size + Date Range)

이 프로젝트는 Walmart 미국 사이트에서 특정 브랜드/상품명을 기반으로 상품을 찾고, 각 상품의 모든 리뷰를 수집해 CSV로 내려받을 수 있는 Streamlit 웹 UI 크롤러입니다.

## 주요 기능
- 브랜드 + 상품명(복수 입력 지원: `,` 또는 `/` 구분)으로 Walmart 검색
- 리뷰 수집 전 정렬을 **관련성순 → 최신 날짜순**으로 변경 시도
- 검색된 상품별로 리뷰 더보기/로딩 버튼을 끝까지 눌러 최대한 전체 리뷰 수집
- 리뷰 본문이 잘린 경우 `Read more/Show more/더보기` 버튼을 눌러 전체 텍스트 추출 시도
- `from`, `to` 날짜 범위 필터링
  - from 비움 + to 비움: 전체 기간
  - from 입력 + to 비움: from ~ 오늘
  - from 비움 + to 입력: 처음 ~ to
  - from/to 모두 입력: from ~ to
- CSV 컬럼
  - 브랜드, 상품명, 사이즈, 업로드 날짜, 계정이름, 별점, 리뷰 제목, 리뷰, 출처

## Windows 더블클릭 실행
### 첫 실행(설치 + 실행)
- `01_install_and_run_web.bat` 더블클릭
  - `.venv` 생성
  - 패키지 설치
  - Playwright Chromium 설치
  - Streamlit 웹 자동 실행

### 재실행(설치 생략 + 바로 실행)
- `01_install_and_run_web.bat`를 다시 더블클릭하면 설치 마커를 확인해 설치를 건너뛰고 바로 실행합니다.
- 또는 `02_run_web.bat` 더블클릭으로 즉시 실행 가능합니다.

## 수동 실행 (공통)
```bash
python -m venv .venv
# Windows
.\.venv\Scripts\Activate.ps1
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
python -m playwright install chromium
streamlit run app.py
```

브라우저에서 `http://localhost:8501` 접속 후 입력값을 넣고 크롤링을 실행하세요.

## Windows/Streamlit 호환성
- Playwright는 `async_playwright` 기반으로 동작합니다.
- Windows에서 Streamlit 버튼 클릭 시 이벤트 루프 충돌을 피하기 위해, 동기 wrapper에서 새 이벤트 루프(Windows Proactor 정책 우선)로 비동기 크롤러를 실행합니다.

## 테스트 기준
- 정적 문법 확인:
  - `python -m py_compile app.py`
- 동작 확인(Windows):
  1. `01_install_and_run_web.bat` 더블클릭
  2. 설치 완료 후 웹이 자동 오픈되는지 확인
  3. 브랜드 + 복수 상품명 입력 후 크롤링 시작
  4. 로그/테이블/CSV 다운로드 정상 동작 확인

## 주의사항
- Walmart 페이지 구조는 수시로 변경될 수 있어, 셀렉터 조정이 필요할 수 있습니다.
- 많은 페이지를 크롤링할 경우 시간이 오래 걸릴 수 있습니다.
- 웹사이트 이용약관/robots/법적 정책을 준수하세요.
