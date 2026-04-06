# Walmart US Review Crawler (Streamlit + Async Playwright)

이 프로젝트는 Walmart 미국 사이트에서 브랜드/상품명(복수 입력 가능) 기준으로 상품 리뷰를 크롤링하여 CSV로 저장하는 웹 도구입니다.

## 주요 기능
- 브랜드 + 상품명으로 Walmart 검색
- 상품명 복수 입력 지원 (`,` 또는 `/` 구분)
- 상품 클릭 후 리뷰 영역 진입
- 정렬을 최신순(최신 날짜순)으로 변경 후 추출 시도
- 리뷰 더보기/본문 더보기(See more/Read more/더보기) 반복 클릭
- 리뷰 컬럼화 CSV 저장
  - 브랜드
  - 상품명
  - 사이즈
  - 업로드 날짜
  - 계정이름
  - 별점
  - 리뷰 제목
  - 리뷰

## 날짜 필터 동작
- `from`, `to` 둘 다 비움: 전체 리뷰
- `from` 비움 + `to` 입력: 처음부터 `to` 날짜까지
- `from` 입력 + `to` 비움: `from`부터 **오늘**까지
- `from`, `to` 둘 다 입력: 해당 범위만

## Windows 더블클릭 실행
아래 BAT 파일을 사용하세요.

1) **최초 1회 설치 + 실행**
- `01_install_and_run_web.bat` 더블클릭
- 내부 동작:
  - `.venv` 생성
  - 패키지 설치
  - Playwright Chromium 설치
  - 이후 `02_run_web.bat` 실행

2) **재실행(재설치 없이 바로 실행)**
- `02_run_web.bat` 더블클릭
- 이미 설치된 `.venv`를 사용해 바로 웹 실행

3) **환경 점검(선택)**
- `03_check_environment.bat` 더블클릭

## 수동 실행 (macOS/Linux 포함)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
streamlit run app.py
```

## 테스트 기준
- 문법 확인:
  - `python -m py_compile app.py`
- 기능 수동 확인:
  1. Streamlit에서 브랜드 + 복수 상품명 입력
  2. 날짜 필터 4가지 케이스 확인
  3. 버튼 클릭 후 크롤링 로그/결과 표/CSV 다운로드 확인

## 주의사항
- Walmart 페이지 구조는 수시로 변경되어 선택자 보정이 필요할 수 있습니다.
- 리뷰 수가 많으면 시간이 오래 걸릴 수 있습니다.
- 웹사이트 이용약관/robots/법적 정책을 준수하세요.
