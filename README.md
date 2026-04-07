# Walmart US Review Crawler (Brand/Product/Size + Date Range)

이 프로젝트는 Walmart 미국 사이트에서 브랜드/상품명으로 상품을 찾고, 리뷰를 수집하여 CSV로 내려받는 Streamlit 웹 앱입니다.

## 핵심 기능
- 브랜드 + 상품명 검색 (복수 상품명 입력 지원: `,` 또는 `/`)
- 리뷰 정렬을 관련성순에서 최신순으로 변경 시도 후 수집
- 리뷰 더보기/Load more를 반복 클릭해 가능한 끝까지 수집
- 긴 리뷰 본문은 Read more/Show more/더보기 클릭 후 전체 텍스트 추출 시도
- 날짜 필터 (`from`, `to`) 둘 다 optional
  - 둘 다 비우면 전체
  - from만 입력하면 from~오늘
  - to만 입력하면 처음~to
- 상대 날짜(예: `3주 전`, `3 weeks ago`)를 실제 날짜로 변환
- CSV 컬럼: 브랜드, 상품명, 사이즈, 업로드 날짜, 계정이름, 별점, 리뷰 제목, 리뷰, 출처

## CAPTCHA 대응 + Resume
- Walmart 첫 진입 시 CAPTCHA(봇 체크)가 발생할 수 있습니다.
- 앱은 CAPTCHA 감지 시 자동으로 **Paused** 상태로 멈춥니다.
- 브라우저 창에서 인증을 완료한 뒤 Streamlit의 **Resume** 버튼을 눌러 크롤링을 이어서 진행할 수 있습니다.

## Windows 더블클릭 실행
### 1) 첫 실행 (설치 + 실행)
- `01_install_and_run_web.bat` 더블클릭
  - `.venv` 생성
  - 패키지 설치
  - Playwright Chromium 설치
  - 앱 실행

### 2) 재실행 (설치 생략 + 실행)
- `01_install_and_run_web.bat`를 다시 실행하면 설치 마커를 확인해 설치를 건너뜁니다.
- 또는 `02_run_web.bat` 더블클릭으로 즉시 실행할 수 있습니다.

## 수동 실행
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

## 구현 노트
- `sync_playwright`는 사용하지 않고 `async_playwright`만 사용합니다.
- Windows 이벤트 루프 정책 강제 설정(`WindowsProactorEventLoopPolicy`)은 제거했습니다.
- 새 이벤트 루프 스레드 wrapper로 Streamlit 환경에서 비동기 크롤링을 실행합니다.

## 검증
- `python -m py_compile app.py`
- `sync_playwright` 키워드 미사용 확인
