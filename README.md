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

## 실행 방법

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
streamlit run app.py
```

브라우저에서 `http://localhost:8501` 접속 후 입력값을 넣고 크롤링을 실행하세요.

## 주의사항
- Walmart 페이지 구조는 수시로 변경될 수 있어, 셀렉터 조정이 필요할 수 있습니다.
- 많은 페이지를 크롤링할 경우 시간이 오래 걸릴 수 있습니다.
- 웹사이트 이용약관/robots/법적 정책을 준수하세요.
