# Walmart 리뷰 크롤러 (Windows 원클릭 실행)

## 1) 처음 실행 (자동 설치 + 실행)
`01_install_and_run_web.bat` 더블클릭

- Python 설치 확인
- `.venv` 가상환경 생성
- `requirements.txt` 설치
- Playwright Chromium 설치
- Streamlit 웹앱 실행
- 브라우저 자동 오픈 (`http://localhost:8501`)

## 2) 두 번째부터 실행
`02_run_web.bat` 더블클릭

## 입력값
- 브랜드
- 상품명
- 리뷰 from (선택)
- 리뷰 to (선택, 비우면 오늘까지)
- 최대 상품 수

## 추출 컬럼
- brand
- product_name
- size
- upload_date
- account_name
- rating
- review_title
- review

## 참고
- Walmart 사이트 구조가 변경되면 선택자 업데이트가 필요할 수 있습니다.
- 회사/학교 PC 정책에 따라 브라우저 자동 실행이 제한될 수 있습니다.
