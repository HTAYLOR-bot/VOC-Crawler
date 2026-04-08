import re
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from io import StringIO
from typing import Dict, List, Optional
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from dateutil import parser as dt_parser
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

GOOGLE_SHOPPING_URL = "https://www.google.com/search?tbm=shop&q={query}&gl=us&hl=en"
TIRE_SIZE_REGEX = re.compile(
    r"\b(\d{3}/\d{2}R\d{2}|\d{2,3}x\d{2,3}(?:\.\d+)?R\d{2}|\d{2,3}/\d{2}ZR\d{2})\b",
    re.IGNORECASE,
)


@dataclass
class CrawlConfig:
    brand: str
    product_name: str
    from_date_text: str
    to_date_text: str
    show_browser: bool
    max_cards: int = 30


@dataclass
class CrawlState:
    rows: List[Dict[str, str]] = field(default_factory=list)
    logs: List[str] = field(default_factory=list)
    status: str = "idle"
    finished: bool = False
    error: str = ""


def normalize_text(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9\s\-/]", " ", (value or "").lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def strip_sizes(value: str) -> str:
    value = TIRE_SIZE_REGEX.sub(" ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def matches_product_exact(title: str, product_name: str) -> bool:
    target = normalize_text(strip_sizes(product_name))
    text = normalize_text(strip_sizes(title))
    if not target or not text:
        return False

    if f"{target} suv" in text:
        return False

    pattern = rf"\b{re.escape(target)}\b"
    return re.search(pattern, text) is not None


def parse_review_count(raw_text: str) -> Optional[int]:
    m = re.search(r"([\d,]+)\s*(?:reviews|review|ratings|rating)", raw_text.lower())
    if not m:
        m = re.search(r"\((\d[\d,]*)\)", raw_text)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def parse_date_input(value: str) -> Optional[date]:
    value = (value or "").strip()
    if not value:
        return None
    return dt_parser.parse(value).date()


def parse_relative_or_absolute_date(raw: str, crawl_today: date) -> Optional[date]:
    text = (raw or "").strip().lower()
    if not text:
        return None

    patterns = [
        (r"(\d+)\s*day", "days"),
        (r"(\d+)\s*week", "weeks"),
        (r"(\d+)\s*month", "months"),
        (r"(\d+)\s*year", "years"),
        (r"(\d+)\s*시간", "hours"),
        (r"(\d+)\s*일", "days"),
        (r"(\d+)\s*주", "weeks"),
        (r"(\d+)\s*개월", "months"),
        (r"(\d+)\s*년", "years"),
    ]

    for pattern, unit in patterns:
        m = re.search(pattern, text)
        if not m:
            continue
        n = int(m.group(1))
        if unit == "days":
            return crawl_today - timedelta(days=n)
        if unit == "weeks":
            return crawl_today - timedelta(weeks=n)
        if unit == "months":
            return crawl_today - timedelta(days=30 * n)
        if unit == "years":
            return crawl_today - timedelta(days=365 * n)
        if unit == "hours":
            return crawl_today

    if "yesterday" in text or "어제" in text:
        return crawl_today - timedelta(days=1)
    if "today" in text or "오늘" in text:
        return crawl_today

    try:
        return dt_parser.parse(raw).date()
    except Exception:
        return None


def date_in_range(target: Optional[date], from_date: Optional[date], to_date: Optional[date]) -> bool:
    if target is None:
        return False
    if from_date and target < from_date:
        return False
    if to_date and target > to_date:
        return False
    return True


class GoogleShoppingCrawler:
    def __init__(self, config: CrawlConfig):
        self.config = config
        self.state = CrawlState(status="ready")
        self._lock = threading.Lock()
        self.pause_event = threading.Event()
        self.pause_event.set()
        self.stop_event = threading.Event()
        self.resume_after_bot_event = threading.Event()
        self.thread: Optional[threading.Thread] = None

    def log(self, msg: str) -> None:
        with self._lock:
            self.state.logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def pause(self) -> None:
        self.pause_event.clear()
        self.log("사용자 요청으로 일시중단되었습니다.")

    def resume(self) -> None:
        self.pause_event.set()
        self.log("크롤링 재개 신호를 받았습니다.")

    def resume_after_bot_check(self) -> None:
        self.resume_after_bot_event.set()
        self.log("봇 체크 인증 완료, 자동화 재개합니다.")

    def stop(self) -> None:
        self.stop_event.set()
        self.pause_event.set()
        self.resume_after_bot_event.set()
        self.log("크롤링 중단 요청을 받았습니다.")

    def _wait_if_paused(self) -> None:
        while not self.pause_event.is_set() and not self.stop_event.is_set():
            with self._lock:
                self.state.status = "paused"
            time.sleep(0.3)
        if not self.stop_event.is_set():
            with self._lock:
                self.state.status = "running"

    def _append_row(self, row: Dict[str, str]) -> None:
        with self._lock:
            row["No."] = str(len(self.state.rows) + 1)
            self.state.rows.append(row)

    def snapshot(self) -> CrawlState:
        with self._lock:
            return CrawlState(
                rows=list(self.state.rows),
                logs=list(self.state.logs[-80:]),
                status=self.state.status,
                finished=self.state.finished,
                error=self.state.error,
            )

    def _run(self) -> None:
        with self._lock:
            self.state.status = "running"

        try:
            self._crawl()
            with self._lock:
                self.state.status = "done"
                self.state.finished = True
        except Exception as exc:
            with self._lock:
                self.state.status = "error"
                self.state.finished = True
                self.state.error = str(exc)
            self.log(f"오류 발생: {exc}")

    def _crawl(self) -> None:
        from_date = parse_date_input(self.config.from_date_text)
        to_date = parse_date_input(self.config.to_date_text)
        today = datetime.utcnow().date()

        if to_date is None:
            to_date = today

        query = quote_plus(f"{self.config.brand} {self.config.product_name}".strip())
        url = GOOGLE_SHOPPING_URL.format(query=query)

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=not self.config.show_browser)
            context = browser.new_context(locale="en-US")
            page = context.new_page()

            self.log("Google Shopping 검색 페이지로 이동합니다.")
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(2500)

            if self.config.show_browser:
                self.log("봇 체크가 필요하면 브라우저에서 직접 인증한 뒤 'Resume(인증 후 재개)' 버튼을 눌러주세요.")
                with self._lock:
                    self.state.status = "waiting_bot_check"
                self.resume_after_bot_event.wait(timeout=600)
                with self._lock:
                    self.state.status = "running"

            product_cards = page.query_selector_all("div.sh-dgr__grid-result, div.sh-dlr__list-result")
            self.log(f"검색 카드 {len(product_cards)}개를 탐색합니다.")

            clicked = False
            target_review_count = None

            for idx, card in enumerate(product_cards[: self.config.max_cards], 1):
                if self.stop_event.is_set():
                    break
                self._wait_if_paused()

                title_el = card.query_selector("h3, .tAxDx")
                title = title_el.inner_text().strip() if title_el else ""
                if not title:
                    continue

                if not matches_product_exact(title, self.config.product_name):
                    continue

                self.log(f"상품 후보 발견({idx}): {title}")
                try:
                    card.click()
                    page.wait_for_timeout(1800)
                except Exception:
                    continue

                if not self._open_user_reviews(page):
                    self.log("'사용자 리뷰' 섹션을 찾지 못해 다음 상품으로 이동합니다.")
                    continue

                target_review_count = self._read_review_count_from_panel(page)
                if target_review_count:
                    self.log(f"리뷰 개수 확인: {target_review_count}")
                clicked = True
                break

            if not clicked:
                self.log("조건에 맞는 상품을 찾지 못했습니다.")
                context.close()
                browser.close()
                return

            self._sort_by_recent(page)
            self._collect_all_reviews(page, from_date, to_date, today, target_review_count)

            context.close()
            browser.close()

    def _open_user_reviews(self, page) -> bool:
        selectors = [
            "text=User reviews",
            "text=사용자 리뷰",
            "a:has-text('User reviews')",
            "button:has-text('User reviews')",
        ]
        for sel in selectors:
            try:
                el = page.wait_for_selector(sel, timeout=3000)
                el.click()
                page.wait_for_timeout(1200)
                return True
            except Exception:
                continue
        return False

    def _read_review_count_from_panel(self, page) -> Optional[int]:
        for sel in ["text=User reviews", "text=사용자 리뷰", "span:has-text('reviews')"]:
            try:
                el = page.query_selector(sel)
                if el:
                    txt = el.inner_text().strip()
                    count = parse_review_count(txt)
                    if count is not None:
                        return count
            except Exception:
                continue
        return None

    def _sort_by_recent(self, page) -> None:
        self.log("정렬을 최근 리뷰순으로 변경합니다.")
        tried = False
        sort_open_selectors = [
            "button:has-text('Most relevant')",
            "button:has-text('관련성순')",
            "div[role='button']:has-text('Most relevant')",
        ]
        for sel in sort_open_selectors:
            try:
                btn = page.wait_for_selector(sel, timeout=2500)
                btn.click()
                page.wait_for_timeout(800)
                tried = True
                break
            except Exception:
                continue

        if not tried:
            return

        for sel in ["text=Newest", "text=최근 리뷰순", "li:has-text('Newest')"]:
            try:
                opt = page.wait_for_selector(sel, timeout=2500)
                opt.click()
                page.wait_for_timeout(1200)
                return
            except Exception:
                continue

    def _collect_all_reviews(
        self,
        page,
        from_date: Optional[date],
        to_date: Optional[date],
        today: date,
        target_review_count: Optional[int],
    ) -> None:
        seen = set()
        stagnant = 0

        while not self.stop_event.is_set():
            self._wait_if_paused()
            cards = page.query_selector_all("div[data-review-id], div.z6XoBf, article")
            new_in_loop = 0

            for card in cards:
                self._wait_if_paused()
                key = (card.inner_text() or "").strip()[:220]
                if not key or key in seen:
                    continue
                seen.add(key)

                try:
                    reviewer = self._safe_text(card, [".sPPcBf", ".TSUbDb", "span"])
                    rating_raw = self._safe_attr_or_text(card, ["[aria-label*='5']", "[role='img']", "span"], "aria-label")
                    rating_match = re.search(r"(\d(?:\.\d)?)", rating_raw or "")
                    rating = rating_match.group(1) if rating_match else ""

                    date_raw = self._safe_text(card, [".ff3bE", "time", "span:has-text('ago')"])
                    upload_date = parse_relative_or_absolute_date(date_raw, today)
                    if not date_in_range(upload_date, from_date, to_date):
                        continue

                    review_text = self._safe_text(card, [".g1lvWe", ".review-full-text", "span"], longest=True)
                    source = self._safe_text(card, [".KkH0Dc", "span:has-text('on')", "span"])

                    self._append_row(
                        {
                            "브랜드명": self.config.brand,
                            "상품명": self.config.product_name,
                            "리뷰 계정명": reviewer,
                            "별점": rating,
                            "날짜": upload_date.isoformat() if upload_date else "",
                            "리뷰 내용": review_text,
                            "출처 웹사이트": source,
                        }
                    )
                    new_in_loop += 1
                except Exception:
                    continue

            self.log(f"누적 리뷰 {len(self.state.rows)}건")

            if target_review_count and len(self.state.rows) >= target_review_count:
                self.log("목표 리뷰 개수에 도달하여 종료합니다.")
                break

            if not self._click_more_reviews(page):
                page.mouse.wheel(0, 2500)
                page.wait_for_timeout(900)
                if new_in_loop == 0:
                    stagnant += 1
                else:
                    stagnant = 0
            else:
                stagnant = 0

            if stagnant >= 4:
                self.log("추가 리뷰 로딩이 없어 수집을 종료합니다.")
                break

    @staticmethod
    def _safe_text(card, selectors: List[str], longest: bool = False) -> str:
        candidates = []
        for sel in selectors:
            try:
                for el in card.query_selector_all(sel):
                    txt = (el.inner_text() or "").strip()
                    if txt:
                        candidates.append(txt)
                if candidates and not longest:
                    return candidates[0]
            except Exception:
                continue
        if not candidates:
            return ""
        return max(candidates, key=len) if longest else candidates[0]

    @staticmethod
    def _safe_attr_or_text(card, selectors: List[str], attr_name: str) -> str:
        for sel in selectors:
            try:
                el = card.query_selector(sel)
                if not el:
                    continue
                attr = (el.get_attribute(attr_name) or "").strip()
                if attr:
                    return attr
                txt = (el.inner_text() or "").strip()
                if txt:
                    return txt
            except Exception:
                continue
        return ""

    @staticmethod
    def _click_more_reviews(page) -> bool:
        for sel in [
            "button:has-text('More reviews')",
            "button:has-text('리뷰 더보기')",
            "span:has-text('More reviews')",
        ]:
            try:
                btn = page.query_selector(sel)
                if btn:
                    btn.click()
                    page.wait_for_timeout(1200)
                    return True
            except Exception:
                continue
        return False


def to_dataframe(rows: List[Dict[str, str]]) -> pd.DataFrame:
    ordered_cols = ["No.", "브랜드명", "상품명", "리뷰 계정명", "별점", "날짜", "리뷰 내용", "출처 웹사이트"]
    if not rows:
        return pd.DataFrame(columns=ordered_cols)
    df = pd.DataFrame(rows)
    df = df[ordered_cols]
    return df.drop_duplicates()


def init_state() -> None:
    if "crawler" not in st.session_state:
        st.session_state.crawler = None


def start_crawl(config: CrawlConfig) -> None:
    crawler = GoogleShoppingCrawler(config)
    st.session_state.crawler = crawler
    crawler.start()


def main() -> None:
    st.set_page_config(page_title="Google Shopping 타이어 리뷰 크롤러", layout="wide")
    st.title("🛞 Google Shopping(미국) 타이어 리뷰 크롤러")
    st.caption("브랜드 + 상품명(대소문자 무시)으로 리뷰를 수집하고 CSV로 다운로드합니다.")
    init_state()

    with st.form("crawler_form"):
        c1, c2 = st.columns(2)
        with c1:
            brand = st.text_input("브랜드명", placeholder="예: Hankook")
            product_name = st.text_input("상품명", placeholder="예: ion evo as")
        with c2:
            from_date_text = st.text_input("리뷰 시작 날짜 (선택, 예: 2025.12.01)", value="")
            to_date_text = st.text_input("리뷰 끝 날짜 (선택, 예: 2026.01.01)", value="")

        show_browser = st.checkbox("브라우저 자동화 크롬 창 띄우기", value=True)
        start = st.form_submit_button("크롤링 시작")

    if start:
        if not brand.strip() or not product_name.strip():
            st.error("브랜드명과 상품명은 필수 입력입니다.")
            return
        try:
            if from_date_text.strip():
                parse_date_input(from_date_text)
            if to_date_text.strip():
                parse_date_input(to_date_text)
        except Exception:
            st.error("날짜 형식이 올바르지 않습니다. 예: 2025.12.01 또는 2025-12-01")
            return

        config = CrawlConfig(
            brand=brand.strip(),
            product_name=product_name.strip(),
            from_date_text=from_date_text.strip(),
            to_date_text=to_date_text.strip(),
            show_browser=show_browser,
        )
        start_crawl(config)

    crawler: Optional[GoogleShoppingCrawler] = st.session_state.crawler

    if crawler:
        b1, b2, b3 = st.columns(3)
        if b1.button("Resume(인증 후 재개)"):
            crawler.resume_after_bot_check()
            crawler.resume()
        if b2.button("크롤링 일시중단"):
            crawler.pause()
        if b3.button("크롤링 중단"):
            crawler.stop()

        snapshot = crawler.snapshot()
        st.info(f"현재 상태: {snapshot.status}")

        if snapshot.error:
            st.error(snapshot.error)

        if snapshot.logs:
            st.subheader("크롤링 로그")
            st.code("\n".join(snapshot.logs[-20:]))

        df = to_dataframe(snapshot.rows)
        st.subheader("수집 데이터 미리보기")
        st.dataframe(df.head(50), width="stretch")

        csv_data = df.to_csv(index=False).encode("utf-8-sig")
        filename = f"google_shopping_reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        st.download_button("CSV 다운로드", data=csv_data, file_name=filename, mime="text/csv")

        if not snapshot.finished and snapshot.status in {"running", "paused", "waiting_bot_check"}:
            time.sleep(1)
            st.rerun()


if __name__ == "__main__":
    main()
