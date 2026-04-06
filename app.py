import asyncio
import platform
import re
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from dateutil import parser as dt_parser
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

WALMART_BASE = "https://www.walmart.com"
SIZE_REGEX = re.compile(
    r"\b(\d{3}/\d{2}R\d{2}|\d{2,3}x\d{2,3}(?:\.\d+)?R\d{2}|\d{2,3}/\d{2}ZR\d{2})\b",
    re.IGNORECASE,
)
MULTI_PRODUCT_SPLIT_REGEX = re.compile(r"\s*[,/]\s*")


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


def parse_relative_date(text: str) -> Optional[date]:
    if not text:
        return None

    lowered = text.lower()
    today = date.today()

    if "today" in lowered or "오늘" in text:
        return today
    if "yesterday" in lowered or "어제" in text:
        return today - timedelta(days=1)

    week_match = re.search(r"(\d+)\s*(week|weeks|주)", lowered)
    if week_match:
        return today - timedelta(weeks=int(week_match.group(1)))

    day_match = re.search(r"(\d+)\s*(day|days|일)", lowered)
    if day_match:
        return today - timedelta(days=int(day_match.group(1)))

    month_match = re.search(r"(\d+)\s*(month|months|개월)", lowered)
    if month_match:
        return today - timedelta(days=30 * int(month_match.group(1)))

    year_match = re.search(r"(\d+)\s*(year|years|년)", lowered)
    if year_match:
        return today - timedelta(days=365 * int(year_match.group(1)))

    return None


def extract_date_from_review(raw_text: str) -> Optional[date]:
    if not raw_text:
        return None

    relative = parse_relative_date(raw_text)
    if relative is not None:
        return relative

    candidates = re.findall(
        r"([A-Z][a-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})",
        raw_text,
    )
    for candidate in candidates:
        try:
            return dt_parser.parse(candidate).date()
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


def split_product_keywords(raw_keywords: str) -> List[str]:
    tokens = [token.strip() for token in MULTI_PRODUCT_SPLIT_REGEX.split(raw_keywords or "")]
    return [token for token in tokens if token]


async def collect_search_products(page: Page, brand: str, keyword: str, max_products: int = 20) -> List[ProductInfo]:
    query = quote_plus(f"{brand} {keyword}".strip())
    search_url = f"{WALMART_BASE}/search?q={query}"
    await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(2500)

    products: List[ProductInfo] = []
    seen_urls = set()

    card_selectors = [
        "[data-item-id]",
        "div[data-type='items'] > div",
        "[data-testid='list-view'] [role='listitem']",
        "[data-automation-id='product-tile']",
    ]

    cards = []
    for selector in card_selectors:
        cards = await page.query_selector_all(selector)
        if cards:
            break

    for card in cards:
        link = await card.query_selector("a[href*='/ip/']")
        if link is None:
            link = await card.query_selector("a")
        if link is None:
            continue

        href = await link.get_attribute("href") or ""
        if not href:
            continue

        if href.startswith("/"):
            url = WALMART_BASE + href.split("?")[0]
        else:
            url = href.split("?")[0]

        if "/ip/" not in url:
            continue

        title = (await link.inner_text()).strip()
        if not title:
            title_el = await card.query_selector("span[data-automation-id='product-title']")
            title = (await title_el.inner_text()).strip() if title_el else ""

        if not title or url in seen_urls:
            continue

        seen_urls.add(url)
        products.append(ProductInfo(title=title, url=url))
        if len(products) >= max_products:
            break

    return products


async def click_if_exists(page: Page, selector: str, timeout_ms: int = 1500) -> bool:
    try:
        element = await page.wait_for_selector(selector, timeout=timeout_ms)
        await element.click()
        await page.wait_for_timeout(1200)
        return True
    except PlaywrightTimeoutError:
        return False
    except Exception:
        return False


async def click_if_exists_in_scope(scope, selector: str, timeout_ms: int = 1200) -> bool:
    try:
        element = await scope.wait_for_selector(selector, timeout=timeout_ms)
        await element.click()
        return True
    except Exception:
        return False


async def set_reviews_to_newest(page: Page) -> None:
    for selector in [
        "button:has-text('Customer reviews')",
        "a:has-text('Customer reviews')",
        "button:has-text('사용자 리뷰')",
    ]:
        if await click_if_exists(page, selector, timeout_ms=1200):
            break

    # open sort menu
    opened_sort_menu = False
    for selector in [
        "button:has-text('Most relevant')",
        "button:has-text('Sort by')",
        "button:has-text('관련성순')",
        "[data-testid='reviews-sort']",
    ]:
        if await click_if_exists(page, selector, timeout_ms=1500):
            opened_sort_menu = True
            break

    # choose newest
    for selector in [
        "button:has-text('Newest')",
        "button:has-text('Newest first')",
        "li:has-text('Newest')",
        "li:has-text('Newest first')",
        "button:has-text('최신 날짜순')",
    ]:
        if await click_if_exists(page, selector, timeout_ms=1500):
            await page.wait_for_timeout(1500)
            return

    if opened_sort_menu:
        await page.keyboard.press("Escape")


async def open_reviews_section(page: Page) -> None:
    candidates = [
        "a:has-text('ratings')",
        "button:has-text('ratings')",
        "a[href*='#reviews']",
        "a:has-text('View all reviews')",
        "button:has-text('View all reviews')",
    ]

    for selector in candidates:
        if await click_if_exists(page, selector):
            return

    await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.6)")
    await page.wait_for_timeout(1000)


async def expand_visible_review_texts(page: Page) -> None:
    for _ in range(30):
        clicked_any = False
        for selector in [
            "button:has-text('See more')",
            "button:has-text('Read more')",
            "button:has-text('더보기')",
            "a:has-text('See more')",
        ]:
            buttons = await page.query_selector_all(selector)
            for button in buttons:
                try:
                    await button.click()
                    clicked_any = True
                    await page.wait_for_timeout(100)
                except Exception:
                    continue
        if not clicked_any:
            break


async def expand_all_reviews(page: Page, max_rounds: int = 300) -> None:
    no_progress_rounds = 0
    previous_card_count = 0

    for _ in range(max_rounds):
        clicked = False
        for selector in [
            "button:has-text('View all reviews')",
            "a:has-text('View all reviews')",
            "button:has-text('See more reviews')",
            "button:has-text('More reviews')",
            "button:has-text('Load more')",
            "button:has-text('Show more')",
            "button:has-text('리뷰 더보기')",
        ]:
            if await click_if_exists(page, selector, timeout_ms=1200):
                clicked = True
                break

        await expand_visible_review_texts(page)
        await page.mouse.wheel(0, 3000)
        await page.wait_for_timeout(1200)

        current_cards = await page.query_selector_all(
            "[data-testid='customer-review'], [itemprop='review'], [data-automation-id='review-card'], article"
        )
        current_count = len(current_cards)

        if clicked or current_count > previous_card_count:
            no_progress_rounds = 0
        else:
            no_progress_rounds += 1

        previous_card_count = current_count

        if no_progress_rounds >= 5:
            break


async def scrape_reviews_on_product(
    page: Page,
    product: ProductInfo,
    brand_input: str,
    from_date: Optional[date],
    to_date: Optional[date],
) -> List[ReviewRow]:
    rows: List[ReviewRow] = []
    await page.goto(product.url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(2000)

    parts = parse_title_fields(product.title, brand_input)
    await open_reviews_section(page)
    await set_reviews_to_newest(page)
    await expand_all_reviews(page)
    await expand_visible_review_texts(page)

    review_cards = await page.query_selector_all(
        "[data-testid='customer-review'], [itemprop='review'], [data-automation-id='review-card'], article"
    )

    for card in review_cards:
        await click_if_exists_in_scope(card, "button:has-text('See more')", timeout_ms=500)
        await click_if_exists_in_scope(card, "button:has-text('더보기')", timeout_ms=500)

        title_el = await card.query_selector("h3, [data-testid='review-title'], [itemprop='name']")
        text_el = await card.query_selector("[data-testid='review-text'], [itemprop='reviewBody'], p")
        author_el = await card.query_selector("[data-testid='review-author'], [itemprop='author'], span.f6")
        rating_el = await card.query_selector("[aria-label*='out of 5 stars'], [itemprop='ratingValue']")
        date_el = await card.query_selector("time, [data-testid='review-date'], [itemprop='datePublished']")

        review_title = (await title_el.inner_text()).strip() if title_el else ""
        review_text = (await text_el.inner_text()).strip() if text_el else ""
        account_name = (await author_el.inner_text()).strip() if author_el else ""

        if rating_el:
            rating_raw = ((await rating_el.get_attribute("aria-label")) or (await rating_el.inner_text()) or "").strip()
            rating_match = re.search(r"(\d+(?:\.\d+)?)", rating_raw)
            rating_value = rating_match.group(1) if rating_match else rating_raw
        else:
            rating_value = ""

        upload_raw = ""
        if date_el:
            upload_raw = (
                (await date_el.get_attribute("datetime"))
                or (await date_el.get_attribute("aria-label"))
                or (await date_el.inner_text())
                or ""
            ).strip()

        upload_date = extract_date_from_review(upload_raw)
        if upload_date is None:
            upload_date = extract_date_from_review((await card.inner_text()))

        if from_date is not None or to_date is not None:
            if not date_in_range(upload_date, from_date, to_date):
                continue

        rows.append(
            ReviewRow(
                brand=parts["brand"],
                product_name=parts["product_name"],
                size=parts["size"],
                upload_date=upload_date.isoformat() if upload_date else "",
                account_name=account_name,
                rating=rating_value,
                review_title=review_title,
                review_text=review_text,
            )
        )

    return rows


async def crawl_walmart_reviews_async(
    brand: str,
    product_keywords: List[str],
    from_date_text: str,
    to_date_text: str,
    max_products: int,
) -> Tuple[pd.DataFrame, List[str]]:
    from_date = to_iso_date(from_date_text) if from_date_text else None
    # 요구사항: 시작일 있음 + 종료일 없음 => 오늘까지
    if from_date is not None and not to_date_text:
        to_date = date.today()
    else:
        to_date = to_iso_date(to_date_text) if to_date_text else None

    all_rows: List[ReviewRow] = []
    logs: List[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(locale="en-US")
        page = await context.new_page()

        for keyword in product_keywords:
            logs.append(f"검색 키워드 시작: {keyword}")
            products = await collect_search_products(page, brand=brand, keyword=keyword, max_products=max_products)
            logs.append(f"'{keyword}' 검색 결과 상품 {len(products)}개를 수집했습니다.")

            for index, product in enumerate(products, 1):
                logs.append(f"[{keyword}] {index}/{len(products)} Crawling: {product.title}")
                try:
                    rows = await scrape_reviews_on_product(
                        page=page,
                        product=product,
                        brand_input=brand,
                        from_date=from_date,
                        to_date=to_date,
                    )
                    all_rows.extend(rows)
                except Exception as exc:
                    logs.append(f"Failed product: {product.title} ({exc})")

        await context.close()
        await browser.close()

    df = pd.DataFrame([row.to_dict() for row in all_rows])
    if not df.empty:
        df = df.drop_duplicates()
    return df, logs


def _run_coroutine_in_new_loop(coroutine):
    payload: Dict[str, object] = {}

    def _runner() -> None:
        if platform.system() == "Windows" and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            payload["result"] = loop.run_until_complete(coroutine)
        except Exception as exc:
            payload["error"] = exc
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()

    if "error" in payload:
        raise payload["error"]
    return payload.get("result")


def crawl_walmart_reviews(
    brand: str,
    product_keywords: List[str],
    from_date_text: str,
    to_date_text: str,
    max_products: int,
) -> Tuple[pd.DataFrame, List[str]]:
    coroutine = crawl_walmart_reviews_async(
        brand=brand,
        product_keywords=product_keywords,
        from_date_text=from_date_text,
        to_date_text=to_date_text,
        max_products=max_products,
    )

    if platform.system() == "Windows":
        return _run_coroutine_in_new_loop(coroutine)

    try:
        return asyncio.run(coroutine)
    except RuntimeError:
        return _run_coroutine_in_new_loop(coroutine)


def main() -> None:
    st.set_page_config(page_title="Walmart 리뷰 크롤러", layout="wide")
    st.title("🛒 Walmart 미국 리뷰 크롤러")
    st.caption("브랜드/상품명 기반으로 리뷰를 크롤링하고 CSV로 다운로드하세요.")

    with st.form("crawler_form"):
        col1, col2 = st.columns(2)
        with col1:
            brand = st.text_input("브랜드", placeholder="예: Goodyear")
            product_keywords_raw = st.text_input(
                "상품명(복수 입력 가능)",
                placeholder="예: Assurance WeatherReady, Eagle Sport / Wrangler",
            )
        with col2:
            from_date_text = st.text_input("from (YYYY-MM-DD, optional)", placeholder="비우면 처음 리뷰부터")
            to_date_text = st.text_input("to (YYYY-MM-DD, optional)", placeholder="비우면 제한 없음(단, from만 입력 시 오늘까지)")

        max_products = st.slider("키워드당 최대 상품 수", min_value=1, max_value=50, value=15)
        submitted = st.form_submit_button("크롤링 시작")

    if submitted:
        if not brand.strip() or not product_keywords_raw.strip():
            st.error("브랜드, 상품명은 필수입니다.")
            return

        product_keywords = split_product_keywords(product_keywords_raw.strip())
        if not product_keywords:
            st.error("상품명을 1개 이상 입력해주세요.")
            return

        try:
            if from_date_text.strip():
                _ = to_iso_date(from_date_text)
            if to_date_text.strip():
                _ = to_iso_date(to_date_text)
        except Exception:
            st.error("날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형태로 입력해주세요.")
            return

        with st.spinner("Walmart 리뷰 크롤링 중... (시간이 오래 걸릴 수 있습니다)"):
            started = time.time()
            df, logs = crawl_walmart_reviews(
                brand=brand.strip(),
                product_keywords=product_keywords,
                from_date_text=from_date_text.strip(),
                to_date_text=to_date_text.strip(),
                max_products=max_products,
            )
            elapsed = time.time() - started

        for log_line in logs:
            st.write(log_line)

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
