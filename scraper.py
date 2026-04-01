from __future__ import annotations

import re
import time
from dataclasses import dataclass, asdict
from datetime import date, datetime
from typing import Iterable, Optional

import pandas as pd
from dateutil import parser as date_parser
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


SIZE_PATTERN = re.compile(r"\b\d{3}/\d{2}R\d{2}\b", re.IGNORECASE)


@dataclass
class ReviewRow:
    brand: str
    product_name: str
    size: str
    upload_date: str
    account_name: str
    rating: float
    review_title: str
    review: str


def _parse_product_name_parts(full_name: str, input_brand: str) -> tuple[str, str, str]:
    sizes = SIZE_PATTERN.findall(full_name)
    size = ", ".join(sorted(set(s.upper() for s in sizes))) if sizes else ""

    clean = re.sub(r"\s+", " ", full_name).strip()
    brand = input_brand.strip()

    if brand and clean.lower().startswith(brand.lower()):
        product_name = clean[len(brand) :].strip(" -")
    else:
        product_name = clean

    if size:
        product_name = product_name.replace(size, "").strip(" -,")

    return brand, product_name, size


def _parse_review_date(text: str) -> Optional[date]:
    text = text.strip()
    if not text:
        return None
    try:
        dt = date_parser.parse(text, fuzzy=True)
        return dt.date()
    except Exception:
        return None


def _within_range(d: Optional[date], date_from: Optional[date], date_to: Optional[date]) -> bool:
    if d is None:
        return False
    if date_from and d < date_from:
        return False
    if date_to and d > date_to:
        return False
    return True


def _safe_inner_text(container, selector: str, default: str = "") -> str:
    try:
        el = container.locator(selector).first
        if el.count() == 0:
            return default
        return el.inner_text(timeout=1000).strip()
    except Exception:
        return default


def _extract_reviews_from_page(
    page,
    brand_input: str,
    date_from: Optional[date],
    date_to: Optional[date],
) -> list[ReviewRow]:
    rows: list[ReviewRow] = []

    title = _safe_inner_text(page, "h1")
    brand, product_name, size = _parse_product_name_parts(title, brand_input)

    review_cards = page.locator("div[data-testid='reviews-container'] article, article[aria-label*='review']")
    card_count = review_cards.count()

    for i in range(card_count):
        card = review_cards.nth(i)
        date_text = _safe_inner_text(card, "time, span:has-text('202'), span:has-text('20')")
        upload = _parse_review_date(date_text)
        if not _within_range(upload, date_from, date_to):
            continue

        account_name = _safe_inner_text(card, "[data-testid='user-name'], .f6.dark-gray")
        title_text = _safe_inner_text(card, "[data-testid='review-title'], h3")
        review_text = _safe_inner_text(card, "[data-testid='review-text'], p")
        rating_text = _safe_inner_text(card, "[aria-label*='out of 5 stars'], [data-testid='rating']")

        rating_match = re.search(r"([0-5](?:\.\d)?)", rating_text)
        rating = float(rating_match.group(1)) if rating_match else 0.0

        rows.append(
            ReviewRow(
                brand=brand,
                product_name=product_name,
                size=size,
                upload_date=upload.isoformat() if upload else "",
                account_name=account_name,
                rating=rating,
                review_title=title_text,
                review=review_text,
            )
        )

    return rows


def _open_all_reviews(page) -> None:
    for selector in ["text=Ratings", "a:has-text('ratings')", "button:has-text('ratings')"]:
        try:
            el = page.locator(selector).first
            if el.count() > 0:
                el.click(timeout=2000)
                break
        except Exception:
            continue

    for selector in ["text=View all reviews", "button:has-text('View all reviews')", "a:has-text('View all reviews')"]:
        try:
            el = page.locator(selector).first
            if el.count() > 0:
                el.click(timeout=3000)
                break
        except Exception:
            continue

    unchanged_count = 0
    last_total = 0

    while unchanged_count < 3:
        cards = page.locator("div[data-testid='reviews-container'] article, article[aria-label*='review']")
        total = cards.count()

        if total <= last_total:
            unchanged_count += 1
        else:
            unchanged_count = 0
            last_total = total

        clicked = False
        for selector in [
            "button:has-text('Load more')",
            "button:has-text('Show more')",
            "button:has-text('More reviews')",
            "button:has-text('View more')",
        ]:
            try:
                btn = page.locator(selector).first
                if btn.count() > 0 and btn.is_enabled():
                    btn.click(timeout=2000)
                    clicked = True
                    time.sleep(1.2)
                    break
            except Exception:
                continue

        if not clicked:
            page.mouse.wheel(0, 3000)
            time.sleep(0.8)


def crawl_walmart_reviews(
    brand: str,
    product_keyword: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    max_products: int = 5,
) -> pd.DataFrame:
    from_d = date.fromisoformat(date_from) if date_from else None
    to_d = date.fromisoformat(date_to) if date_to else datetime.utcnow().date()

    query = f"{brand} {product_keyword}".strip()
    rows: list[ReviewRow] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        page.goto(f"https://www.walmart.com/search?q={query}", wait_until="domcontentloaded", timeout=45000)

        products = page.locator("a[href*='/ip/']")
        seen = set()
        product_links: list[str] = []

        for i in range(products.count()):
            href = products.nth(i).get_attribute("href")
            if not href:
                continue
            if href.startswith("/"):
                href = f"https://www.walmart.com{href}"
            if href in seen:
                continue
            seen.add(href)
            product_links.append(href)
            if len(product_links) >= max_products:
                break

        for link in product_links:
            try:
                page.goto(link, wait_until="domcontentloaded", timeout=45000)
                _open_all_reviews(page)
                rows.extend(_extract_reviews_from_page(page, brand, from_d, to_d))
            except PlaywrightTimeoutError:
                continue
            except Exception:
                continue

        context.close()
        browser.close()

    df = pd.DataFrame([asdict(r) for r in rows])
    if not df.empty:
        df = df.drop_duplicates().reset_index(drop=True)
    return df


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
