import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from dateutil import parser as dt_parser
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

WALMART_BASE = "https://www.walmart.com"
SIZE_REGEX = re.compile(
    r"\b(\d{3}/\d{2}R\d{2}|\d{2,3}x\d{2,3}(?:\.\d+)?R\d{2}|\d{2,3}/\d{2}ZR\d{2})\b",
    re.IGNORECASE,
)


@dataclass
class ProductInfo:
    title: str
    url: str


@dataclass
class ReviewRow:
    brand: str
    product_name: str
    size: str
    upload_date: str
    account_name: str
    rating: str
    review_title: str
    review_text: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "브랜드": self.brand,
            "상품명": self.product_name,
            "사이즈": self.size,
            "업로드 날짜": self.upload_date,
            "계정이름": self.account_name,
            "별점": self.rating,
            "리뷰 제목": self.review_title,
            "리뷰": self.review_text,
        }


def parse_title_fields(raw_title: str, brand_input: str) -> Dict[str, str]:
    raw = re.sub(r"\s+", " ", raw_title).strip()
    size_match = SIZE_REGEX.search(raw)
    size = size_match.group(1) if size_match else ""

    lower_raw = raw.lower()
    lower_brand = brand_input.strip().lower()

    if lower_brand and lower_brand in lower_raw:
        brand_start = lower_raw.index(lower_brand)
        brand = raw[brand_start : brand_start + len(brand_input)].strip()
        product_name = (raw[:brand_start] + raw[brand_start + len(brand_input) :]).strip(" -,")
    else:
        brand = brand_input.strip() or ""
        product_name = raw

    if size:
        product_name = product_name.replace(size, "").strip(" -,")

    return {
        "brand": brand if brand else brand_input.strip(),
        "product_name": re.sub(r"\s+", " ", product_name).strip(),
        "size": size,
    }


def to_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    parsed = dt_parser.parse(value)
    return parsed.date()


def extract_date_from_review(raw_text: str) -> Optional[date]:
    if not raw_text:
        return None

    candidates = re.findall(
        r"([A-Z][a-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})",
        raw_text,
    )
    for c in candidates:
        try:
            return dt_parser.parse(c).date()
        except Exception:
            continue
    return None


def date_in_range(target: Optional[date], from_date: Optional[date], to_date: Optional[date]) -> bool:
    if target is None:
        return False
    if from_date and target < from_date:
        return False
    if to_date and target > to_date:
        return False
    return True


def collect_search_products(page, brand: str, keyword: str, max_products: int = 20) -> List[ProductInfo]:
    query = quote_plus(f"{brand} {keyword}".strip())
    search_url = f"{WALMART_BASE}/search?q={query}"
    page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(2500)

    products: List[ProductInfo] = []
    seen_urls = set()

    # Walmart search results are dynamic; try a few possible card selectors.
    card_selectors = [
        "[data-item-id]",
        "div[data-type='items'] > div",
        "[data-testid='list-view'] [role='listitem']",
        "[data-automation-id='product-tile']",
    ]

    cards = []
    for sel in card_selectors:
        cards = page.query_selector_all(sel)
        if cards:
            break

    for card in cards:
        link = card.query_selector("a[href*='/ip/']")
        if link is None:
            link = card.query_selector("a")
        if link is None:
            continue

        href = link.get_attribute("href") or ""
        if not href:
            continue

        if href.startswith("/"):
            url = WALMART_BASE + href.split("?")[0]
        else:
            url = href.split("?")[0]

        if "/ip/" not in url:
            continue

        title = link.inner_text().strip()
        if not title:
            title_el = card.query_selector("span[data-automation-id='product-title']")
            title = title_el.inner_text().strip() if title_el else ""

        if not title:
            continue

        if url in seen_urls:
            continue

        seen_urls.add(url)
        products.append(ProductInfo(title=title, url=url))

        if len(products) >= max_products:
            break

    return products


def click_if_exists(page, selector: str, timeout_ms: int = 1500) -> bool:
    try:
        el = page.wait_for_selector(selector, timeout=timeout_ms)
        el.click()
        page.wait_for_timeout(1200)
        return True
    except PlaywrightTimeoutError:
        return False
    except Exception:
        return False


def open_reviews_section(page) -> None:
    candidates = [
        "a:has-text('ratings')",
        "button:has-text('ratings')",
        "a[href*='#reviews']",
        "a:has-text('View all reviews')",
        "button:has-text('View all reviews')",
    ]
    for sel in candidates:
        if click_if_exists(page, sel):
            return

    # fallback scroll to review section
    page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.6)")
    page.wait_for_timeout(1000)


def expand_all_reviews(page, max_clicks: int = 100) -> None:
    for _ in range(max_clicks):
        clicked = False
        for sel in [
            "button:has-text('View all reviews')",
            "a:has-text('View all reviews')",
            "button:has-text('See more')",
            "button:has-text('Load more')",
            "button:has-text('Show more')",
        ]:
            if click_if_exists(page, sel, timeout_ms=1200):
                clicked = True
                break

        if not clicked:
            # Also try scrolling to trigger lazy load.
            page.mouse.wheel(0, 2500)
            page.wait_for_timeout(1000)
            # if no explicit button appears, break after one extra pass
            button_exists = any(
                page.query_selector(s) is not None
                for s in [
                    "button:has-text('View all reviews')",
                    "button:has-text('See more')",
                    "button:has-text('Load more')",
                    "button:has-text('Show more')",
                ]
            )
            if not button_exists:
                break


def scrape_reviews_on_product(
    page,
    product: ProductInfo,
    brand_input: str,
    from_date: Optional[date],
    to_date: Optional[date],
) -> List[ReviewRow]:
    rows: List[ReviewRow] = []
    page.goto(product.url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(2000)

    parts = parse_title_fields(product.title, brand_input)
    open_reviews_section(page)
    expand_all_reviews(page)

    review_cards = page.query_selector_all(
        "[data-testid='customer-review'], [itemprop='review'], [data-automation-id='review-card'], article"
    )

    for card in review_cards:
        title_el = card.query_selector("h3, [data-testid='review-title'], [itemprop='name']")
        text_el = card.query_selector("[data-testid='review-text'], [itemprop='reviewBody'], p")
        author_el = card.query_selector("[data-testid='review-author'], [itemprop='author'], span.f6")
        rating_el = card.query_selector("[aria-label*='out of 5 stars'], [itemprop='ratingValue']")
        date_el = card.query_selector("time, [data-testid='review-date'], [itemprop='datePublished']")

        review_title = title_el.inner_text().strip() if title_el else ""
        review_text = text_el.inner_text().strip() if text_el else ""
        account_name = author_el.inner_text().strip() if author_el else ""

        if rating_el:
            rating_raw = (rating_el.get_attribute("aria-label") or rating_el.inner_text() or "").strip()
            rating = re.search(r"(\d+(?:\.\d+)?)", rating_raw)
            rating_val = rating.group(1) if rating else rating_raw
        else:
            rating_val = ""

        upload_raw = ""
        if date_el:
            upload_raw = (
                date_el.get_attribute("datetime")
                or date_el.get_attribute("aria-label")
                or date_el.inner_text()
                or ""
            ).strip()

        upload_date = extract_date_from_review(upload_raw)
        if upload_date is None:
            upload_date = extract_date_from_review(card.inner_text())

        if not date_in_range(upload_date, from_date, to_date):
            continue

        rows.append(
            ReviewRow(
                brand=parts["brand"],
                product_name=parts["product_name"],
                size=parts["size"],
                upload_date=upload_date.isoformat() if upload_date else "",
                account_name=account_name,
                rating=rating_val,
                review_title=review_title,
                review_text=review_text,
            )
        )

    return rows


def crawl_walmart_reviews(
    brand: str,
    product_keyword: str,
    from_date_text: str,
    to_date_text: str,
    max_products: int,
) -> pd.DataFrame:
    from_date = to_iso_date(from_date_text)
    to_date = to_iso_date(to_date_text) if to_date_text else date.today()

    all_rows: List[ReviewRow] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(locale="en-US")
        page = context.new_page()

        products = collect_search_products(page, brand=brand, keyword=product_keyword, max_products=max_products)

        for idx, product in enumerate(products, 1):
            st.write(f"[{idx}/{len(products)}] Crawling: {product.title}")
            try:
                rows = scrape_reviews_on_product(
                    page=page,
                    product=product,
                    brand_input=brand,
                    from_date=from_date,
                    to_date=to_date,
                )
                all_rows.extend(rows)
            except Exception as exc:
                st.warning(f"Failed product: {product.title} ({exc})")

        context.close()
        browser.close()

    df = pd.DataFrame([r.to_dict() for r in all_rows])
    if not df.empty:
        df = df.drop_duplicates()
    return df


def main() -> None:
    st.set_page_config(page_title="Walmart 리뷰 크롤러", layout="wide")
    st.title("🛒 Walmart 미국 리뷰 크롤러")
    st.caption("브랜드/상품명 기반으로 리뷰를 크롤링하고 CSV로 다운로드하세요.")

    with st.form("crawler_form"):
        col1, col2 = st.columns(2)
        with col1:
            brand = st.text_input("브랜드", placeholder="예: Goodyear")
            product_keyword = st.text_input("상품명", placeholder="예: Assurance WeatherReady")
        with col2:
            from_date_text = st.text_input("from (YYYY-MM-DD)", placeholder="2025-01-01")
            to_date_text = st.text_input("to (YYYY-MM-DD, optional)", placeholder="비우면 현재까지")

        max_products = st.slider("최대 상품 수", min_value=1, max_value=50, value=15)
        submitted = st.form_submit_button("크롤링 시작")

    if submitted:
        if not brand.strip() or not product_keyword.strip() or not from_date_text.strip():
            st.error("브랜드, 상품명, from 날짜는 필수입니다.")
            return

        try:
            _ = to_iso_date(from_date_text)
            if to_date_text.strip():
                _ = to_iso_date(to_date_text)
        except Exception:
            st.error("날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형태로 입력해주세요.")
            return

        with st.spinner("Walmart 리뷰 크롤링 중... (상품/리뷰 수에 따라 시간이 걸릴 수 있습니다)"):
            started = time.time()
            df = crawl_walmart_reviews(
                brand=brand.strip(),
                product_keyword=product_keyword.strip(),
                from_date_text=from_date_text.strip(),
                to_date_text=to_date_text.strip(),
                max_products=max_products,
            )
            elapsed = time.time() - started

        st.success(f"완료! {len(df):,}개 리뷰 수집 (소요: {elapsed:.1f}초)")

        if df.empty:
            st.info("조건에 맞는 리뷰가 없습니다.")
            return

        st.dataframe(df, use_container_width=True)

        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        default_name = f"walmart_reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        st.download_button(
            label="CSV 다운로드",
            data=csv_bytes,
            file_name=default_name,
            mime="text/csv",
        )


if __name__ == "__main__":
    main()
