# Walmart Review Crawler Web App

브랜드와 상품명을 입력하면 Walmart 미국 사이트에서 관련 상품 리뷰를 수집하고 CSV로 내려받을 수 있는 Streamlit 앱입니다.

## 기능
- 입력값: `브랜드`, `상품명`, `리뷰 from`, `리뷰 to`
- `to` 미입력 시 오늘 날짜까지 자동 적용
- 검색된 상품을 순회하며 리뷰 페이지 진입
- `View all reviews`/`Load more`를 끝까지 눌러 가능한 리뷰를 최대한 수집
- 컬럼 구성:
  - brand
  - product_name
  - size (예: 215/45R17)
  - upload_date
  - account_name
  - rating
  - review_title
  - review

## 실행 방법
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
streamlit run app.py
```

## 참고
- Walmart 페이지 구조가 자주 바뀌므로 선택자 보정이 필요할 수 있습니다.
- 동적 로딩/봇 방지로 인해 일부 환경에서 수집량이 달라질 수 있습니다.


## Windows one-click 실행
- `01_install_and_run_web.bat`: 최초 1회 설치 + 실행
- `02_run_web.bat`: 이후 실행
- 웹 입력 날짜는 키인 방식이며 형식은 `YYYY-MM-DD`
- 자세한 한국어 안내: `README_KR.md`
