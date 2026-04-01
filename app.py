from __future__ import annotations

from datetime import date

import streamlit as st

from scraper import crawl_walmart_reviews, dataframe_to_csv_bytes


def _parse_date_input(value: str, field_name: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field_name} 날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형식으로 입력하세요.") from exc


st.set_page_config(page_title="Walmart 리뷰 크롤러", layout="wide")
st.title("Walmart 리뷰 크롤러")
st.caption("브랜드/상품명으로 Walmart 제품 리뷰를 수집하고 CSV로 다운로드합니다.")

with st.form("crawl_form"):
    col1, col2 = st.columns(2)
    with col1:
        brand = st.text_input("브랜드", placeholder="예: Goodyear")
        product_name = st.text_input("상품명", placeholder="예: Assurance WeatherReady")
    with col2:
        date_from_txt = st.text_input("리뷰 from (YYYY-MM-DD, 선택)", placeholder="예: 2025-01-01")
        date_to_txt = st.text_input("리뷰 to (YYYY-MM-DD, 비우면 오늘까지)", placeholder="예: 2026-03-31")

    max_products = st.slider("수집할 최대 상품 수", min_value=1, max_value=20, value=5)
    submitted = st.form_submit_button("크롤링 시작")

if submitted:
    if not brand.strip() or not product_name.strip():
        st.error("브랜드와 상품명을 모두 입력해 주세요.")
    else:
        try:
            from_str = _parse_date_input(date_from_txt, "from")
            to_str = _parse_date_input(date_to_txt, "to")
        except ValueError as e:
            st.error(str(e))
            st.stop()

        with st.spinner("Walmart에서 리뷰 수집 중..."):
            df = crawl_walmart_reviews(
                brand=brand,
                product_keyword=product_name,
                date_from=from_str,
                date_to=to_str,
                max_products=max_products,
            )

        if df.empty:
            st.warning("조건에 맞는 리뷰를 찾지 못했습니다.")
        else:
            st.success(f"완료: {len(df):,}개 리뷰 수집")
            st.dataframe(df, use_container_width=True)
            csv_bytes = dataframe_to_csv_bytes(df)
            st.download_button(
                label="CSV 다운로드",
                data=csv_bytes,
                file_name="walmart_reviews.csv",
                mime="text/csv",
            )

st.markdown("---")
st.markdown(
    """
#### 추출 컬럼
- 브랜드 (brand)
- 상품명 (product_name)
- 사이즈 (size)
- 업로드 날짜 (upload_date)
- 계정이름 (account_name)
- 별점 (rating)
- 리뷰 제목 (review_title)
- 리뷰 본문 (review)
"""
)
