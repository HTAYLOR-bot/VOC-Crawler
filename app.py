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
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import ElementHandle, Page, async_playwright

WALMART_BASE = "https://www.walmart.com"
SIZE_REGEX = re.compile(
    r"\b(\d{3}/\d{2}R\d{2}|\d{2,3}x\d{2,3}(?:\.\d+)?R\d{2}|\d{2,3}/\d{2}ZR\d{2})\b",
    re.IGNORECASE,
)
RELATIVE_KR_REGEX = re.compile(r"(\d+)\s*(일|주|개월|달|년)\s*전")
RELATIVE_EN_REGEX = re.compile(r"(\d+)\s*(day|week|month|year)s?\s*ago", re.IGNORECASE)


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
    source: str

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
            "출처": self.source,
        }


def split_keywords(raw: str) -> List[str]:
    items = [v.strip() for v in re.split(r"[,/]+", raw) if v.strip()]
    if not items:
        return []
    # 중복 제거 (입력 순서 유지)
    deduped = []
    seen = set()
    for item in items:
        key = item.lower()
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


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


def parse_input_date(value: Optional[str]) -> Optional[date]:
    if not value or not value.strip():
        return None
    return dt_parser.parse(value).date()


def extract_relative_date(raw_text: str) -> Optional[date]:
    text = raw_text.strip()
    if not text:
        return None

    kr_match = RELATIVE_KR_REGEX.search(text)
    if kr_match:
        amount = int(kr_match.group(1))
        unit = kr_match.group(2)
        if unit == "일":
            return date.today() - timedelta(days=amount)
        if unit == "주":
            return date.today() - timedelta(weeks=amount)
        if unit in ("개월", "달"):
            return date.today() - timedelta(days=30 * amount)
        if unit == "년":
            return date.today() - timedelta(days=365 * amount)

    en_match = RELATIVE_EN_REGEX.search(text)
    if en_match:
        amount = int(en_match.group(1))
        unit = en_match.group(2).lower()
        if unit == "day":
            return date.today() - timedelta(days=amount)
        if unit == "week":
            return date.today() - timedelta(weeks=amount)
        if unit == "month":
            return date.today() - timedelta(days=30 * amount)
        if unit == "year":
            return date.today() - timedelta(days=365 * amount)

    return None


def extract_date_from_review(raw_text: str) -> Optional[date]:
    if not raw_text:
        return None

    relative = extract_relative_date(raw_text)
    if relative:
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
        await page.wait_for_timeout(1000)
        return True
    except PlaywrightTimeoutError:
        return False
    except Exception:
        return False


async def click_all_expand_buttons_in_reviews(page: Page) -> None:
    selectors = [
        "button:has-text('Read more')",
        "button:has-text('Show more')",
        "button:has-text('더보기')",
        "span:has-text('Read more')",
    ]
    for selector in selectors:
        buttons = await page.query_selector_all(selector)
        for button in buttons:
            try:
                await button.click()
                await page.wait_for_timeout(100)
            except Exception:
                continue


async def open_reviews_section_and_sort_latest(page: Page) -> None:
    # 리뷰 구역으로 이동
    moved = False
    review_entry_candidates = [
        "a:has-text('사용자 리뷰')",
        "button:has-text('사용자 리뷰')",
        "a:has-text('Customer reviews')",
        "button:has-text('Customer reviews')",
        "a:has-text('ratings')",
        "button:has-text('ratings')",
        "a[href*='#reviews']",
    ]
    for selector in review_entry_candidates:
        if await click_if_exists(page, selector):
            moved = True
            break

    if not moved:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.55)")
        await page.wait_for_timeout(1200)

    # 정렬 변경: 관련성순 -> 최신 날짜순
    sort_openers = [
        "button:has-text('관련성순')",
        "button:has-text('Relevance')",
        "button[aria-label*='Sort']",
        "select[name*='sort']",
    ]

    for opener in sort_openers:
        target = await page.query_selector(opener)
        if target is None:
            continue

        tag_name = (await target.evaluate("el => el.tagName")).lower()
        if tag_name == "select":
            try:
                await target.select_option(label="최신 날짜순")
            except Exception:
                try:
                    await target.select_option(label="Most recent")
                except Exception:
                    pass
            await page.wait_for_timeout(1000)
            return

        try:
            await target.click()
            await page.wait_for_timeout(800)
        except Exception:
            continue

        latest_options = [
            "[role='option']:has-text('최신 날짜순')",
            "[role='option']:has-text('Most recent')",
            "button:has-text('최신 날짜순')",
            "button:has-text('Most recent')",
            "li:has-text('최신 날짜순')",
            "li:has-text('Most recent')",
        ]
        for option in latest_options:
            if await click_if_exists(page, option, timeout_ms=1200):
                await page.wait_for_timeout(1500)
                return


async def get_expected_review_count(page: Page) -> Optional[int]:
    selectors = [
        "a:has-text('ratings')",
        "button:has-text('ratings')",
        "span:has-text('ratings')",
        "h2:has-text('Customer reviews')",
    ]
    for selector in selectors:
        el = await page.query_selector(selector)
        if not el:
            continue
        txt = (await el.inner_text()).strip()
        m = re.search(r"([\d,]+)", txt)
        if m:
            return int(m.group(1).replace(",", ""))
    return None


async def expand_all_reviews(page: Page, max_clicks: int = 800) -> None:
    expected_count = await get_expected_review_count(page)
    stagnant_rounds = 0
    last_review_card_count = 0

    for _ in range(max_clicks):
        await click_all_expand_buttons_in_reviews(page)

        cards = await page.query_selector_all(
            "[data-testid='customer-review'], [itemprop='review'], [data-automation-id='review-card'], article"
        )
        current_count = len(cards)

        if expected_count and current_count >= expected_count:
            break

        if current_count <= last_review_card_count:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0
            last_review_card_count = current_count

        clicked = False
        for selector in [
            "button:has-text('View all reviews')",
            "a:has-text('View all reviews')",
            "button:has-text('리뷰 더보기')",
            "button:has-text('Load more')",
            "button:has-text('Show more')",
            "button:has-text('See more')",
        ]:
            if await click_if_exists(page, selector, timeout_ms=1400):
                clicked = True
                break

        if not clicked:
            await page.mouse.wheel(0, 3200)
            await page.wait_for_timeout(1200)

        if stagnant_rounds >= 4 and not clicked:
            break


async def extract_source_from_card(card: ElementHandle) -> str:
    source_selectors = [
        "span:has-text('에서 작성된 리뷰')",
        "span:has-text('review from')",
        "[data-testid='review-source']",
    ]
    for selector in source_selectors:
        el = await card.query_selector(selector)
        if el:
            txt = (await el.inner_text()).strip()
            if txt:
                return txt

    raw = await card.inner_text()
    for line in raw.splitlines():
        if "에서 작성된 리뷰" in line or "review from" in line.lower():
            return line.strip()
    return ""


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
    await open_reviews_section_and_sort_latest(page)
    await expand_all_reviews(page)
    await click_all_expand_buttons_in_reviews(page)

    review_cards = await page.query_selector_all(
        "[data-testid='customer-review'], [itemprop='review'], [data-automation-id='review-card'], article"
    )

    for card in review_cards:
        title_el = await card.query_selector("h3, [data-testid='review-title'], [itemprop='name']")
        text_el = await card.query_selector("[data-testid='review-text'], [itemprop='reviewBody'], p")
        author_el = await card.query_selector("[data-testid='review-author'], [itemprop='author'], span.f6")
        rating_el = await card.query_selector("[aria-label*='out of 5 stars'], [itemprop='ratingValue']")
        date_el = await card.query_selector("time, [data-testid='review-date'], [itemprop='datePublished']")

        review_title = (await title_el.inner_text()).strip() if title_el else ""
        review_text = (await text_el.inner_text()).strip() if text_el else ""
        account_name = (await author_el.inner_text()).strip() if author_el else ""

        if rating_el:
            rating_raw = (
                (await rating_el.get_attribute("aria-label"))
                or (await rating_el.inner_text())
                or ""
            ).strip()
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

        card_text = await card.inner_text()
        upload_date = extract_date_from_review(upload_raw) or extract_date_from_review(card_text)

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
                source=await extract_source_from_card(card),
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
    from_date = parse_input_date(from_date_text)
    to_date = parse_input_date(to_date_text)

    if from_date and to_date and from_date > to_date:
        raise ValueError("from 날짜가 to 날짜보다 늦습니다.")

    all_rows: List[ReviewRow] = []
    logs: List[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(locale="en-US")
        page = await context.new_page()

        for keyword in product_keywords:
            products = await collect_search_products(page, brand=brand, keyword=keyword, max_products=max_products)
            logs.append(f"[{keyword}] 검색 결과 상품 {len(products)}개")

            for index, product in enumerate(products, 1):
                logs.append(f"[{keyword}] {index}/{len(products)}: {product.title}")
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
                    logs.append(f"[{keyword}] 실패: {product.title} ({exc})")

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
            product_keyword_raw = st.text_input(
                "상품명(복수 입력 가능)",
                placeholder="예: Assurance WeatherReady, Eagle Touring / Wrangler",
            )
        with col2:
            from_date_text = st.text_input("from (YYYY-MM-DD, optional)", placeholder="비우면 전체 기간")
            to_date_text = st.text_input("to (YYYY-MM-DD, optional)", placeholder="비우면 오늘까지")

        max_products = st.slider("상품명당 최대 상품 수", min_value=1, max_value=50, value=15)
        submitted = st.form_submit_button("크롤링 시작")

    if submitted:
        if not brand.strip() or not product_keyword_raw.strip():
            st.error("브랜드와 상품명은 필수입니다.")
            return

        try:
            _ = parse_input_date(from_date_text)
            _ = parse_input_date(to_date_text)
        except Exception:
            st.error("날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형태로 입력해주세요.")
            return

        keywords = split_keywords(product_keyword_raw)
        if not keywords:
            st.error("상품명을 1개 이상 입력해주세요.")
            return

        with st.spinner("Walmart 리뷰 크롤링 중... (시간이 오래 걸릴 수 있습니다)"):
            started = time.time()
            try:
                df, logs = crawl_walmart_reviews(
                    brand=brand.strip(),
                    product_keywords=keywords,
                    from_date_text=from_date_text.strip(),
                    to_date_text=to_date_text.strip(),
                    max_products=max_products,
                )
            except ValueError as exc:
                st.error(str(exc))
                return
            elapsed = time.time() - started

        for line in logs:
            st.write(line)

        st.success(f"완료! {len(df):,}개 리뷰 수집 (소요: {elapsed:.1f}초)")

        if df.empty:
            st.info("조건에 맞는 리뷰가 없습니다.")
            return

        st.dataframe(df, use_container_width=True)

        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        file_name = f"walmart_reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        st.download_button(
            label="CSV 다운로드",
            data=csv_bytes,
            file_name=file_name,
            mime="text/csv",
        )


if __name__ == "__main__":
    main()
