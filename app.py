import asyncio
import platform
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from dateutil import parser as dt_parser
from playwright.async_api import ElementHandle, Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

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


@dataclass
class CrawlProgress:
    status: str  # done | paused | error
    rows: List[Dict[str, str]] = field(default_factory=list)
    logs: List[str] = field(default_factory=list)
    keyword_index: int = 0
    product_index: int = 0
    reason: str = ""


def split_keywords(raw: str) -> List[str]:
    items = [v.strip() for v in re.split(r"[,/]+", raw) if v.strip()]
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
        start = lower_raw.index(lower_brand)
        brand = raw[start : start + len(brand_input)].strip()
        product_name = (raw[:start] + raw[start + len(brand_input) :]).strip(" -,")
    else:
        brand = brand_input.strip()
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
    text = (raw_text or "").strip()
    if not text:
        return None

    kr = RELATIVE_KR_REGEX.search(text)
    if kr:
        amount = int(kr.group(1))
        unit = kr.group(2)
        if unit == "일":
            return date.today() - timedelta(days=amount)
        if unit == "주":
            return date.today() - timedelta(weeks=amount)
        if unit in ("개월", "달"):
            return date.today() - timedelta(days=30 * amount)
        if unit == "년":
            return date.today() - timedelta(days=365 * amount)

    en = RELATIVE_EN_REGEX.search(text)
    if en:
        amount = int(en.group(1))
        unit = en.group(2).lower()
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

    rel = extract_relative_date(raw_text)
    if rel:
        return rel

    candidates = re.findall(
        r"([A-Z][a-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})",
        raw_text,
    )
    for value in candidates:
        try:
            return dt_parser.parse(value).date()
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


async def is_captcha_page(page: Page) -> bool:
    page_text = (await page.content()).lower()
    keywords = [
        "captcha",
        "verify you are human",
        "press & hold",
        "robot",
        "are you a human",
        "bot check",
    ]
    if any(k in page_text for k in keywords):
        return True

    selector_hits = [
        "iframe[src*='captcha']",
        "input[name*='captcha']",
        "text=Verify you are human",
        "text=Press & Hold",
    ]
    for selector in selector_hits:
        if await page.query_selector(selector):
            return True
    return False


async def collect_search_products(page: Page, brand: str, keyword: str, max_products: int = 20) -> List[ProductInfo]:
    query = quote_plus(f"{brand} {keyword}".strip())
    url = f"{WALMART_BASE}/search?q={query}"
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(2000)

    selectors = [
        "[data-item-id]",
        "div[data-type='items'] > div",
        "[data-testid='list-view'] [role='listitem']",
        "[data-automation-id='product-tile']",
    ]
    cards = []
    for selector in selectors:
        cards = await page.query_selector_all(selector)
        if cards:
            break

    products: List[ProductInfo] = []
    seen = set()
    for card in cards:
        link = await card.query_selector("a[href*='/ip/']") or await card.query_selector("a")
        if not link:
            continue

        href = await link.get_attribute("href") or ""
        if not href:
            continue

        url = WALMART_BASE + href.split("?")[0] if href.startswith("/") else href.split("?")[0]
        if "/ip/" not in url or url in seen:
            continue

        title = (await link.inner_text()).strip()
        if not title:
            t = await card.query_selector("span[data-automation-id='product-title']")
            title = (await t.inner_text()).strip() if t else ""
        if not title:
            continue

        seen.add(url)
        products.append(ProductInfo(title=title, url=url))
        if len(products) >= max_products:
            break

    return products


async def click_if_exists(page: Page, selector: str, timeout_ms: int = 1200) -> bool:
    try:
        el = await page.wait_for_selector(selector, timeout=timeout_ms)
        await el.click()
        await page.wait_for_timeout(800)
        return True
    except Exception:
        return False


async def click_review_expand_buttons(page: Page) -> None:
    selectors = [
        "button:has-text('Read more')",
        "button:has-text('Show more')",
        "button:has-text('더보기')",
        "span:has-text('Read more')",
    ]
    for selector in selectors:
        for button in await page.query_selector_all(selector):
            try:
                await button.click()
            except Exception:
                pass


async def open_reviews_section_and_sort_latest(page: Page) -> None:
    for selector in [
        "a:has-text('사용자 리뷰')",
        "button:has-text('사용자 리뷰')",
        "a:has-text('Customer reviews')",
        "button:has-text('Customer reviews')",
        "a:has-text('ratings')",
        "button:has-text('ratings')",
        "a[href*='#reviews']",
    ]:
        if await click_if_exists(page, selector):
            break

    for opener in [
        "button:has-text('관련성순')",
        "button:has-text('Relevance')",
        "button[aria-label*='Sort']",
        "select[name*='sort']",
    ]:
        target = await page.query_selector(opener)
        if not target:
            continue

        tag = (await target.evaluate("el => el.tagName")).lower()
        if tag == "select":
            try:
                await target.select_option(label="Most recent")
            except Exception:
                try:
                    await target.select_option(label="최신 날짜순")
                except Exception:
                    pass
            await page.wait_for_timeout(1000)
            return

        try:
            await target.click()
            await page.wait_for_timeout(700)
        except Exception:
            continue

        for option in [
            "[role='option']:has-text('Most recent')",
            "button:has-text('Most recent')",
            "li:has-text('Most recent')",
            "[role='option']:has-text('최신 날짜순')",
            "button:has-text('최신 날짜순')",
            "li:has-text('최신 날짜순')",
        ]:
            if await click_if_exists(page, option):
                await page.wait_for_timeout(1200)
                return


async def expected_review_count(page: Page) -> Optional[int]:
    for selector in [
        "a:has-text('ratings')",
        "button:has-text('ratings')",
        "span:has-text('ratings')",
        "h2:has-text('Customer reviews')",
    ]:
        el = await page.query_selector(selector)
        if not el:
            continue
        txt = (await el.inner_text()).strip()
        m = re.search(r"([\d,]+)", txt)
        if m:
            return int(m.group(1).replace(",", ""))
    return None


async def expand_all_reviews(page: Page, max_rounds: int = 900) -> None:
    expected = await expected_review_count(page)
    stagnant = 0
    prev_count = 0

    for _ in range(max_rounds):
        await click_review_expand_buttons(page)

        cards = await page.query_selector_all(
            "[data-testid='customer-review'], [itemprop='review'], [data-automation-id='review-card'], article"
        )
        curr_count = len(cards)

        if expected and curr_count >= expected:
            break

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

        if curr_count <= prev_count and not clicked:
            stagnant += 1
        else:
            stagnant = 0
            prev_count = curr_count

        if stagnant >= 4:
            break

        await page.mouse.wheel(0, 3000)
        await page.wait_for_timeout(1000)


async def extract_source_from_card(card: ElementHandle) -> str:
    for selector in [
        "span:has-text('에서 작성된 리뷰')",
        "span:has-text('review from')",
        "[data-testid='review-source']",
    ]:
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
    await page.wait_for_timeout(1200)

    if await is_captcha_page(page):
        raise RuntimeError("CAPTCHA_REQUIRED")

    parts = parse_title_fields(product.title, brand_input)
    await open_reviews_section_and_sort_latest(page)
    await expand_all_reviews(page)
    await click_review_expand_buttons(page)

    cards = await page.query_selector_all(
        "[data-testid='customer-review'], [itemprop='review'], [data-automation-id='review-card'], article"
    )

    for card in cards:
        title_el = await card.query_selector("h3, [data-testid='review-title'], [itemprop='name']")
        text_el = await card.query_selector("[data-testid='review-text'], [itemprop='reviewBody'], p")
        author_el = await card.query_selector("[data-testid='review-author'], [itemprop='author'], span.f6")
        rating_el = await card.query_selector("[aria-label*='out of 5 stars'], [itemprop='ratingValue']")
        date_el = await card.query_selector("time, [data-testid='review-date'], [itemprop='datePublished']")

        review_title = (await title_el.inner_text()).strip() if title_el else ""
        review_text = (await text_el.inner_text()).strip() if text_el else ""
        account_name = (await author_el.inner_text()).strip() if author_el else ""

        rating_val = ""
        if rating_el:
            rating_raw = ((await rating_el.get_attribute("aria-label")) or (await rating_el.inner_text()) or "").strip()
            m = re.search(r"(\d+(?:\.\d+)?)", rating_raw)
            rating_val = m.group(1) if m else rating_raw

        upload_raw = ""
        if date_el:
            upload_raw = (
                (await date_el.get_attribute("datetime"))
                or (await date_el.get_attribute("aria-label"))
                or (await date_el.inner_text())
                or ""
            ).strip()

        whole = await card.inner_text()
        upload_date = extract_date_from_review(upload_raw) or extract_date_from_review(whole)

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
    start_keyword_index: int = 0,
    start_product_index: int = 0,
    existing_rows: Optional[List[Dict[str, str]]] = None,
    existing_logs: Optional[List[str]] = None,
) -> CrawlProgress:
    from_date = parse_input_date(from_date_text)
    to_date = parse_input_date(to_date_text)

    if from_date and to_date and from_date > to_date:
        raise ValueError("from 날짜가 to 날짜보다 늦습니다.")

    rows = existing_rows[:] if existing_rows else []
    logs = existing_logs[:] if existing_logs else []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(locale="en-US")
        page = await context.new_page()

        try:
            for k_idx in range(start_keyword_index, len(product_keywords)):
                keyword = product_keywords[k_idx]
                products = await collect_search_products(page, brand=brand, keyword=keyword, max_products=max_products)

                if await is_captcha_page(page):
                    logs.append("CAPTCHA 감지: Walmart 창에서 사람 인증 완료 후 Resume을 누르세요.")
                    return CrawlProgress(
                        status="paused",
                        rows=rows,
                        logs=logs,
                        keyword_index=k_idx,
                        product_index=0,
                        reason="captcha",
                    )

                logs.append(f"[{keyword}] 검색 상품 {len(products)}개")
                p_start = start_product_index if k_idx == start_keyword_index else 0

                for p_idx in range(p_start, len(products)):
                    product = products[p_idx]
                    logs.append(f"[{keyword}] {p_idx + 1}/{len(products)} 처리: {product.title}")
                    try:
                        product_rows = await scrape_reviews_on_product(
                            page=page,
                            product=product,
                            brand_input=brand,
                            from_date=from_date,
                            to_date=to_date,
                        )
                        rows.extend([r.to_dict() for r in product_rows])
                    except RuntimeError as exc:
                        if str(exc) == "CAPTCHA_REQUIRED":
                            logs.append("CAPTCHA 감지: Walmart 창에서 사람 인증 완료 후 Resume을 누르세요.")
                            return CrawlProgress(
                                status="paused",
                                rows=rows,
                                logs=logs,
                                keyword_index=k_idx,
                                product_index=p_idx,
                                reason="captcha",
                            )
                        logs.append(f"오류(런타임): {product.title} / {exc}")
                    except Exception as exc:
                        logs.append(f"오류: {product.title} / {exc}")

            df = pd.DataFrame(rows).drop_duplicates() if rows else pd.DataFrame()
            return CrawlProgress(status="done", rows=df.to_dict("records"), logs=logs)
        finally:
            await context.close()
            await browser.close()


def _run_coroutine_in_new_loop(coroutine):
    payload: Dict[str, object] = {}

    def _runner() -> None:
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


def crawl_walmart_reviews(**kwargs) -> CrawlProgress:
    coroutine = crawl_walmart_reviews_async(**kwargs)
    try:
        return asyncio.run(coroutine)
    except RuntimeError:
        return _run_coroutine_in_new_loop(coroutine)


def show_dataframe_and_download(rows: List[Dict[str, str]]) -> None:
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates()

    st.success(f"완료! {len(df):,}개 리뷰 수집")
    if df.empty:
        st.info("조건에 맞는 리뷰가 없습니다.")
        return

    st.dataframe(df, use_container_width=True)
    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
    file_name = f"walmart_reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    st.download_button("CSV 다운로드", data=csv_bytes, file_name=file_name, mime="text/csv")


def main() -> None:
    st.set_page_config(page_title="Walmart 리뷰 크롤러", layout="wide")
    st.title("🛒 Walmart 미국 리뷰 크롤러")
    st.caption("브랜드/상품명 기반으로 리뷰를 크롤링하고 CSV로 다운로드하세요.")

    if "crawl_state" not in st.session_state:
        st.session_state.crawl_state = None

    with st.form("crawler_form"):
        col1, col2 = st.columns(2)
        with col1:
            brand = st.text_input("브랜드", placeholder="예: Goodyear")
            raw_keywords = st.text_input("상품명(복수 입력 가능)", placeholder="예: A, B / C")
        with col2:
            from_date_text = st.text_input("from (YYYY-MM-DD, optional)", placeholder="비우면 전체 기간")
            to_date_text = st.text_input("to (YYYY-MM-DD, optional)", placeholder="비우면 오늘까지")

        max_products = st.slider("상품명당 최대 상품 수", min_value=1, max_value=50, value=15)
        start_clicked = st.form_submit_button("크롤링 시작")

    if start_clicked:
        if not brand.strip() or not raw_keywords.strip():
            st.error("브랜드와 상품명은 필수입니다.")
            return

        try:
            _ = parse_input_date(from_date_text)
            _ = parse_input_date(to_date_text)
        except Exception:
            st.error("날짜 형식은 YYYY-MM-DD 로 입력하세요.")
            return

        keywords = split_keywords(raw_keywords)
        if not keywords:
            st.error("상품명을 1개 이상 입력하세요.")
            return

        st.session_state.crawl_state = {
            "brand": brand.strip(),
            "keywords": keywords,
            "from_date_text": from_date_text.strip(),
            "to_date_text": to_date_text.strip(),
            "max_products": max_products,
            "keyword_index": 0,
            "product_index": 0,
            "rows": [],
            "logs": [],
            "status": "running",
        }

    state = st.session_state.crawl_state

    if state and state.get("status") in {"running", "paused"}:
        if state.get("status") == "paused":
            st.warning("CAPTCHA 인증이 필요합니다. Walmart 브라우저 창에서 인증 완료 후 Resume을 누르세요.")

        run_now = state.get("status") == "running"
        resume_now = st.button("Resume", disabled=state.get("status") != "paused")

        if run_now or resume_now:
            with st.spinner("크롤링 진행 중... CAPTCHA가 나오면 브라우저에서 인증 후 Resume을 눌러주세요."):
                started = time.time()
                progress = crawl_walmart_reviews(
                    brand=state["brand"],
                    product_keywords=state["keywords"],
                    from_date_text=state["from_date_text"],
                    to_date_text=state["to_date_text"],
                    max_products=state["max_products"],
                    start_keyword_index=state["keyword_index"],
                    start_product_index=state["product_index"],
                    existing_rows=state["rows"],
                    existing_logs=state["logs"],
                )
                elapsed = time.time() - started

            state["rows"] = progress.rows
            state["logs"] = progress.logs
            state["keyword_index"] = progress.keyword_index
            state["product_index"] = progress.product_index
            state["status"] = progress.status

            st.info(f"최근 실행 소요: {elapsed:.1f}초")

            for line in state["logs"][-30:]:
                st.write(line)

            if progress.status == "done":
                show_dataframe_and_download(state["rows"])
            elif progress.status == "paused":
                st.warning("일시정지됨: CAPTCHA 해결 후 Resume 버튼을 눌러 재개하세요.")
            else:
                st.error("크롤링 중 오류가 발생했습니다.")


if __name__ == "__main__":
    main()
