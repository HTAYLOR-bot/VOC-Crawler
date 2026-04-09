from __future__ import annotations

import csv
import re
import time
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote_plus

from dateutil.relativedelta import relativedelta
from playwright.sync_api import Browser, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

FINAL_COLUMNS = [
    "No.",
    "브랜드명",
    "상품명",
    "리뷰 계정명",
    "별점",
    "날짜",
    "리뷰 내용",
    "출처 웹사이트",
]

_SHARED_BROWSER_LOCK = threading.Lock()
_SHARED_BROWSER: Dict[str, Any] = {
    "pw": None,
    "browser": None,
    "context": None,
    "page": None,
}


class CrawlCancelled(Exception):
    pass


class Logger:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, message: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {message}"
        with self.path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


def now_utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).lower()


def clean_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_match_text(value: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", " ", normalize_text(value or "")).strip()


def contains_normalized_phrase(haystack: Optional[str], needle: Optional[str]) -> bool:
    h = normalize_match_text(haystack)
    n = normalize_match_text(needle)
    return bool(h and n and n in h)


_FORBIDDEN_PRODUCT_CONTINUATION_TOKENS = {
    "suv", "crossover", "truck", "lt", "van", "sedan", "touring", "grand",
}


def _allowed_product_suffix_token(token: str) -> bool:
    t = (token or '').strip().lower().strip('.,;:()[]{}')
    if not t:
        return True
    if t in _FORBIDDEN_PRODUCT_CONTINUATION_TOKENS:
        return False
    if t.startswith('$'):
        return True
    if re.fullmatch(r"\d+(?:[.,]\d+)?", t):
        return True
    if re.fullmatch(r"\d{3}/\d{2,3}r\d{2}[a-z0-9-]*", t, re.I):
        return True
    if re.fullmatch(r"\d{2,3}/\d{2,3}", t, re.I):
        return True
    if re.fullmatch(r"r\d{2}[a-z0-9-]*", t, re.I):
        return True
    if re.fullmatch(r"(?:xl|tl|rf|zr|ev|oe|runflat|run-flat)", t, re.I):
        return True
    if re.fullmatch(r"[a-z]{0,6}\d[a-z0-9-]*", t, re.I):
        return True
    return False


def product_name_matches_strict(text: Optional[str], product_name: Optional[str]) -> bool:
    hay = normalize_text(text or '')
    target = normalize_text(product_name or '')
    if not hay or not target:
        return False
    pattern = re.compile(rf"(?<![a-z0-9]){re.escape(target)}(?![a-z0-9])", re.I)
    for m in pattern.finditer(hay):
        after = hay[m.end():].lstrip(' -_/|:,;()[]{}')
        if not after:
            return True
        after_tokens = [tok for tok in re.split(r"\s+", after) if tok]
        if not after_tokens:
            return True
        # Allow tyre sizes / item codes immediately after the exact product name,
        # but reject a different product family such as SUV / Crossover.
        bad_continuation = False
        for tok in after_tokens[:3]:
            cleaned = (tok or '').strip().lower().strip('.,;:()[]{}')
            if not cleaned:
                continue
            if cleaned.startswith('$'):
                break
            if cleaned in _FORBIDDEN_PRODUCT_CONTINUATION_TOKENS:
                bad_continuation = True
                break
            if _allowed_product_suffix_token(cleaned):
                continue
            # Stop scanning once we move past trailing size/code tokens into seller/price text.
            break
        if not bad_continuation:
            return True
    return False


def clean_multiline_text(value: Optional[str]) -> str:
    raw = (value or '').replace('\r\n', '\n').replace('\r', '\n')
    lines = [re.sub(r"[\t\f\v ]+", " ", line).strip() for line in raw.split('\n')]
    compact = [line for line in lines if line]
    return '\n'.join(compact).strip()


def parse_user_date(value: str) -> Optional[date]:
    s = clean_text(value)
    if not s:
        return None
    s = s.replace("년", ".").replace("월", ".").replace("일", "")
    s = s.replace("/", ".").replace("-", ".")
    parts = [p for p in s.split(".") if p]
    if len(parts) == 3:
        y, m, d = parts
        return date(int(y), int(m), int(d))
    raise ValueError("날짜 형식은 YYYY.MM.DD / YYYY-MM-DD / YYYY/MM/DD 중 하나여야 합니다.")


def split_product_inputs(value: str) -> List[str]:
    raw = (value or "").replace("\r", "\n")
    # 콤마는 항상 구분자로 처리합니다.
    # 슬래시는 A/S 같은 상품명 내부 표기를 해치지 않도록 " / " 형태일 때만 구분자로 처리합니다.
    tokens = re.split(r"\n+|,+|\s+/\s+", raw)
    out: List[str] = []
    seen = set()
    for token in tokens:
        cleaned = clean_text(token.strip(" ,/"))
        if not cleaned:
            continue
        norm = normalize_text(cleaned)
        if norm in seen:
            continue
        seen.add(norm)
        out.append(cleaned)
    return out


RELATIVE_PATTERNS = [
    (re.compile(r"\btoday\b", re.I), lambda base, m: base),
    (re.compile(r"\byesterday\b", re.I), lambda base, m: base - timedelta(days=1)),
    (re.compile(r"\b(\d+)\s+day[s]?\s+ago\b", re.I), lambda base, m: base - timedelta(days=int(m.group(1)))),
    (re.compile(r"\b(\d+)\s+week[s]?\s+ago\b", re.I), lambda base, m: base - timedelta(weeks=int(m.group(1)))),
    (re.compile(r"\b(\d+)\s+month[s]?\s+ago\b", re.I), lambda base, m: base - relativedelta(months=int(m.group(1)))),
    (re.compile(r"\b(\d+)\s+year[s]?\s+ago\b", re.I), lambda base, m: base - relativedelta(years=int(m.group(1)))),
    (re.compile(r"오늘"), lambda base, m: base),
    (re.compile(r"어제"), lambda base, m: base - timedelta(days=1)),
    (re.compile(r"(\d+)\s*일\s*전"), lambda base, m: base - timedelta(days=int(m.group(1)))),
    (re.compile(r"(\d+)\s*주\s*전"), lambda base, m: base - timedelta(weeks=int(m.group(1)))),
    (re.compile(r"(\d+)\s*(?:개월|달)\s*전"), lambda base, m: base - relativedelta(months=int(m.group(1)))),
    (re.compile(r"(\d+)\s*년\s*전"), lambda base, m: base - relativedelta(years=int(m.group(1)))),
]

ABSOLUTE_FORMATS = [
    "%Y-%m-%d",
    "%Y.%m.%d",
    "%Y/%m/%d",
    "%Y. %m. %d.",
    "%Y년 %m월 %d일",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%b %d, %Y",
    "%B %d, %Y",
]


def parse_review_date(raw_text: str, base_date: Optional[date] = None) -> Optional[date]:
    base = base_date or datetime.now().date()
    text = clean_text(raw_text)
    if not text:
        return None
    for pattern, builder in RELATIVE_PATTERNS:
        m = pattern.search(text)
        if m:
            return builder(base, m)

    normalized = text
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = normalized.replace("년", ".").replace("월", ".").replace("일", "")
    normalized = re.sub(r"\s*\.\s*", ".", normalized).strip(" .")

    abs_candidates = [text, normalized]
    abs_candidates.extend(re.findall(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}\b", text, re.I))
    abs_candidates.extend(re.findall(r"\b\d{4}[./-]\s*\d{1,2}[./-]\s*\d{1,2}\.?\b", text))
    abs_candidates.extend(re.findall(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{4}\b", text))
    abs_candidates.extend(re.findall(r"\b\d{4}년\s*\d{1,2}월\s*\d{1,2}일\b", text))
    seen = set()
    for candidate in abs_candidates:
        c = candidate.strip()
        if not c or c in seen:
            continue
        seen.add(c)
        for fmt in ABSOLUTE_FORMATS:
            try:
                return datetime.strptime(c, fmt).date()
            except Exception:
                continue
    return None


def format_date_yyyy_mm_dd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def product_query_patterns(brand: str, product_name: str) -> List[str]:
    tokens = [brand] + re.findall(r"[a-zA-Z0-9]+", product_name)
    out: List[str] = []
    seen = set()
    for token in tokens:
        t = normalize_text(token)
        if len(t) < 2 or t in seen:
            continue
        seen.add(t)
        out.append(re.escape(t))
    return out[:5]


def make_search_url(brand: str, product_name: str) -> str:
    q = quote_plus(f"{brand} {product_name}".strip())
    return f"https://www.google.com/search?hl=en&gl=us&udm=28&q={q}"


def save_reviews_csv(output_dir: Path, rows: List[Dict[str, Any]]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "structured_reviews.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FINAL_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in FINAL_COLUMNS})
    return csv_path


def write_outputs(output_dir: Path, rows: List[Dict[str, Any]], logger: Optional[Logger] = None) -> Path:
    csv_path = save_reviews_csv(output_dir, rows)
    if logger:
        logger.log(f"CSV 저장: {csv_path}")
    return csv_path


CLICK_CANDIDATES_JS = r"""
(args) => {
  const roots = [];
  const queue = [document];
  const seenRoots = new Set();
  while (queue.length) {
    const root = queue.shift();
    if (!root || seenRoots.has(root)) continue;
    seenRoots.add(root);
    roots.push(root);
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT);
    let node = walker.currentNode;
    while (node) {
      if (node.shadowRoot && !seenRoots.has(node.shadowRoot)) queue.push(node.shadowRoot);
      node = walker.nextNode();
    }
  }

  const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
  const visible = (el) => {
    if (!el || !el.isConnected) return false;
    const st = window.getComputedStyle(el);
    if (!st) return false;
    if (st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity || '1') === 0) return false;
    const rect = el.getBoundingClientRect();
    if (rect.width < 6 || rect.height < 6) return false;
    return true;
  };
  const clickRoot = (el) => {
    let cur = el;
    for (let i = 0; i < 6 && cur; i += 1) {
      if (
        cur.tagName === 'A' || cur.tagName === 'BUTTON' ||
        cur.getAttribute('role') === 'button' || cur.getAttribute('role') === 'link' ||
        cur.onclick || cur.tabIndex >= 0 || /pointer/i.test(window.getComputedStyle(cur).cursor || '')
      ) {
        return cur;
      }
      cur = cur.parentElement;
    }
    return null;
  };
  const regexes = (args.patterns || []).map((p) => new RegExp(p, 'i'));
  const requireAll = !!args.require_all;
  const preferReviewCount = !!args.prefer_review_count;
  const limit = Number(args.limit || 10);
  const out = [];
  const used = new Set();
  let seq = 0;

  const pushCandidate = (el) => {
    if (!el || !visible(el)) return;
    if (used.has(el)) return;
    const text = clean(el.innerText || el.textContent || '');
    if (!text || text.length < 2 || text.length > 280) return;
    const ok = regexes.length
      ? (requireAll ? regexes.every((r) => r.test(text)) : regexes.some((r) => r.test(text)))
      : true;
    if (!ok) return;
    used.add(el);
    const reviewMatch = text.match(/(\d[\d,]*)\s+reviews?/i) || text.match(/\((\d[\d,]*)\)/);
    let score = 0;
    score += el.tagName === 'A' ? 70 : 0;
    score += el.tagName === 'BUTTON' ? 65 : 0;
    score += el.getAttribute('role') === 'button' ? 55 : 0;
    score += el.getAttribute('role') === 'link' ? 50 : 0;
    score += reviewMatch ? 45 : 0;
    score += preferReviewCount && reviewMatch ? 160 : 0;
    score += Math.max(0, 160 - text.length);
    const id = `oai-click-${Date.now()}-${++seq}`;
    el.setAttribute('data-oai-click-id', id);
    const rect = el.getBoundingClientRect();
    out.push({
      id,
      tag: el.tagName,
      text,
      href: el.getAttribute('href') || '',
      review_count: reviewMatch ? Number(String(reviewMatch[1]).replace(/,/g, '')) : null,
      score,
      rect: { x: rect.x, y: rect.y, width: rect.width, height: rect.height },
    });
  };

  for (const root of roots) {
    let elements = [];
    try { elements = Array.from(root.querySelectorAll('*')); } catch (e) { elements = []; }
    for (const el of elements) {
      if (!visible(el)) continue;
      const target = clickRoot(el) || el;
      pushCandidate(target);
    }
  }

  out.sort((a, b) => b.score - a.score || (b.review_count || 0) - (a.review_count || 0));
  return out.slice(0, limit);
}
"""


EXTRACT_REVIEWS_JS = r"""
() => {
  const clean = (s) => (s || '').replace(/\u00a0/g, ' ').replace(/[\t\f\v ]+/g, ' ').trim();
  const normalizeLines = (s) => {
    const raw = (s || '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
    const lines = raw.split(/\n+/).map((line) => clean(line)).filter(Boolean);
    return lines.join('\n').trim();
  };
  const visible = (el) => {
    if (!el || !el.isConnected) return false;
    const st = window.getComputedStyle(el);
    if (!st) return false;
    if (st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity || '1') === 0) return false;
    const rect = el.getBoundingClientRect();
    return rect.width >= 6 && rect.height >= 6;
  };
  const relDate = /\b(?:today|yesterday|\d+\s+(?:day|week|month|year)s?\s+ago)\b|(?:오늘|어제|\d+\s*일\s*전|\d+\s*주\s*전|\d+\s*(?:개월|달)\s*전|\d+\s*년\s*전)/i;
  const absDate = /\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}\b|\b\d{4}[./-]\s*\d{1,2}[./-]\s*\d{1,2}\.?\b|\b\d{1,2}[./-]\d{1,2}[./-]\d{4}\b|\b\d{4}년\s*\d{1,2}월\s*\d{1,2}일\b/i;
  const ratingRe = /(?:^|\s)([0-5](?:\.\d+)?)\s*(?:out of 5|\/5|stars?|점)(?:\s|$)/i;
  const starLineNumRe = /(^|\s)([1-5])\s*[★⭐]/;
  const ratingOnlyRe = /^(?:[1-5](?:\.0)?|[1-5]\s*[★⭐]+|[★⭐]{1,5}|[1-5]\s*(?:stars?|점))$/i;
  const domainRe = /\b([a-z0-9.-]+\.[a-z]{2,})(?:\b|\/)/i;
  const skipActionRe = /^(?:helpful|see more reviews|more reviews|show more reviews|see more|show more|read more|more|by relevance|by most recent date|sort by|most relevant|most recent|all reviews|모든 리뷰|관련성순|최신순|리뷰 더보기|더보기)$/i;
  const noiseRe = /^(?:incentivized|verified purchase|추천|권장)$/i;
  const hasNameLikeText = (s) => /[a-zÀ-ɏᄀ-ᇿ㄰-㆏가-힯]/i.test(s || '');

  const isLikelyAuthor = (line) => {
    const x = clean(line);
    if (!x) return false;
    if (x.length > 80) return false;
    if (skipActionRe.test(x)) return false;
    if (noiseRe.test(x)) return false;
    if (domainRe.test(x)) return false;
    if (relDate.test(x) || absDate.test(x)) return false;
    if (ratingOnlyRe.test(x)) return false;
    if (!hasNameLikeText(x)) return false;
    return true;
  };

  const lineLooksLikeRating = (line) => {
    const x = clean(line);
    if (!x) return false;
    if (ratingOnlyRe.test(x)) return true;
    if (ratingRe.test(x)) return true;
    if (starLineNumRe.test(x)) return true;
    return (x.match(/[★⭐]/g) || []).length > 0;
  };

  const findRating = (container, lines = []) => {
    const nodes = [container, ...Array.from(container.querySelectorAll('[aria-label],[title],svg,span,div'))];
    for (const node of nodes) {
      const texts = [node.getAttribute && node.getAttribute('aria-label'), node.getAttribute && node.getAttribute('title'), node.innerText, node.textContent];
      for (const raw of texts) {
        const text = clean(raw);
        if (!text) continue;
        const m = text.match(ratingRe);
        if (m) return m[1];
        const m2 = text.match(starLineNumRe);
        if (m2) return m2[2];
        if (/^[1-5]$/.test(text)) return text;
      }
    }
    for (const line of lines) {
      const text = clean(line);
      if (!text) continue;
      const m = text.match(ratingRe);
      if (m) return m[1];
      const m2 = text.match(starLineNumRe);
      if (m2) return m2[2];
      if (/^[1-5]$/.test(text)) return text;
      const stars = (text.match(/[★⭐]/g) || []).length;
      if (stars) {
        const num = text.match(/(^|\s)([1-5])(\s|$)/);
        if (num) return num[2];
        return String(Math.min(stars, 5));
      }
    }
    return null;
  };

  const all = Array.from(document.querySelectorAll('*'));
  const candidateEls = [];
  const used = new Set();

  for (const el of all) {
    if (!visible(el)) continue;
    const txt = clean(el.innerText || el.textContent || '');
    if (!txt || txt.length > 120) continue;
    if (!(relDate.test(txt) || absDate.test(txt))) continue;

    let best = null;
    let cur = el;
    for (let depth = 0; depth < 10 && cur; depth += 1) {
      if (!visible(cur)) { cur = cur.parentElement; continue; }
      const t = normalizeLines(cur.innerText || cur.textContent || '');
      if (t.length >= 20 && t.length <= 5000) {
        const lines = t.split(/\n+/).filter(Boolean);
        const rating = findRating(cur, lines);
        const topAuthor = lines.length ? lines[0] : '';
        const authorHit = isLikelyAuthor(topAuthor) ? topAuthor : lines.find((line) => isLikelyAuthor(line));
        const lineCount = lines.length;
        let score = 0;
        score += rating ? 250 : 0;
        score += authorHit ? 220 : 0;
        score += domainRe.test(t) ? 110 : 0;
        score += /작성된 리뷰|written review|from\s+[a-z0-9.-]+\.[a-z]{2,}/i.test(t) ? 150 : 0;
        score += Math.max(0, 500 - Math.abs(t.length - 320));
        score += Math.max(0, 120 - depth * 12);
        score += Math.max(0, 120 - Math.abs(lineCount - 6) * 10);
        if (/리뷰 더보기|more reviews/i.test(t)) score -= 120;
        const item = { el: cur, score };
        if (!best || item.score > best.score) best = item;
      }
      cur = cur.parentElement;
    }
    if (best && !used.has(best.el)) {
      used.add(best.el);
      candidateEls.push(best.el);
    }
  }

  const rows = [];
  const unique = new Set();

  for (const container of candidateEls) {
    const fullText = normalizeLines(container.innerText || container.textContent || '');
    if (!fullText) continue;
    const lines = fullText.split(/\n+/).map((line) => clean(line)).filter(Boolean);
    const dateIndex = lines.findIndex((line) => relDate.test(line) || absDate.test(line));
    const dateLine = dateIndex >= 0 ? lines[dateIndex] : '';
    const sourceIndex = lines.findIndex((line) => /작성된 리뷰|written review/i.test(line) && domainRe.test(line));
    const fallbackSourceIndex = sourceIndex >= 0 ? sourceIndex : lines.findIndex((line) => domainRe.test(line));
    const sourceLine = fallbackSourceIndex >= 0 ? lines[fallbackSourceIndex] : '';
    const sourceMatch = sourceLine.match(domainRe);
    const source = sourceMatch ? sourceMatch[1] : '';

    const skipLine = (line) => {
      const x = clean(line);
      if (!x) return true;
      if (x === sourceLine) return true;
      if (skipActionRe.test(x)) return true;
      return false;
    };

    let rating = findRating(container, lines) || '';
    let author = '';

    if (lines.length && isLikelyAuthor(lines[0])) {
      author = lines[0];
    }

    if (!author && dateIndex > 0) {
      for (let i = dateIndex - 1; i >= 0; i -= 1) {
        const line = lines[i];
        if (skipLine(line)) continue;
        if (lineLooksLikeRating(line)) {
          if (!rating) {
            const m = clean(line).match(/([1-5](?:\.\d+)?)/);
            if (m) rating = m[1];
          }
          continue;
        }
        if (isLikelyAuthor(line)) {
          author = line;
          break;
        }
      }
    }

    if (!author) {
      author = lines.find((line) => isLikelyAuthor(line)) || '';
    }
    if (!rating) {
      const ratingLine = lines.find((line) => lineLooksLikeRating(line));
      if (ratingLine) {
        const m = clean(ratingLine).match(/([1-5](?:\.\d+)?)/);
        rating = m ? m[1] : '';
      }
    }

    let reviewLines = [];
    if (dateIndex >= 0) {
      for (let i = dateIndex + 1; i < lines.length; i += 1) {
        const line = lines[i];
        if (i === fallbackSourceIndex) break;
        if (skipLine(line)) continue;
        if (lineLooksLikeRating(line)) continue;
        if (noiseRe.test(line)) continue;
        reviewLines.push(line);
      }
    }

    if (!reviewLines.length) {
      const startIdx = author ? lines.findIndex((line) => line === author) + 1 : 0;
      for (let i = Math.max(startIdx, 0); i < lines.length; i += 1) {
        const line = lines[i];
        if (line == author || line == dateLine || i === fallbackSourceIndex) continue;
        if (skipLine(line)) continue;
        if (lineLooksLikeRating(line)) continue;
        if (noiseRe.test(line)) continue;
        reviewLines.push(line);
      }
    }

    reviewLines = reviewLines.filter((line) => !domainRe.test(line) || line === source);
    const reviewText = reviewLines.join('\n').trim();
    if (!author && !dateLine && !source && !reviewText) continue;

    const key = [author, rating, dateLine, reviewText, source].join('|');
    if (unique.has(key)) continue;
    unique.add(key);
    rows.push({
      author,
      rating,
      date_text: dateLine,
      review_text: reviewText,
      source,
      raw_text: fullText,
    });
  }

  return rows;
}
"""


PANEL_METRICS_JS = r"""
() => {
  const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
  const visible = (el) => {
    if (!el || !el.isConnected) return false;
    const st = window.getComputedStyle(el);
    if (!st) return false;
    if (st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity || '1') === 0) return false;
    const rect = el.getBoundingClientRect();
    return rect.width >= 6 && rect.height >= 6;
  };
  const relDateGlobal = /\b(?:today|yesterday|\d+\s+(?:day|week|month|year)s?\s+ago)\b|(?:오늘|어제|\d+\s*일\s*전|\d+\s*주\s*전|\d+\s*(?:개월|달)\s*전|\d+\s*년\s*전)/ig;
  const reviewUiRe = /user reviews|reviews?|리뷰|most relevant|most recent|by relevance|by most recent date|관련성순|최신순/i;
  const buttonRe = /(?:see more reviews|more reviews|show more reviews|리뷰 더보기)/i;
  const candidates = [];
  for (const el of Array.from(document.querySelectorAll('*'))) {
    if (!visible(el)) continue;
    if (el === document.body || el === document.documentElement) continue;
    const rect = el.getBoundingClientRect();
    const distance = Math.max((el.scrollHeight || 0) - (el.clientHeight || 0), 0);
    if (distance < 60) continue;
    const txt = clean((el.innerText || '').slice(0, 30000));
    const hits = (txt.match(relDateGlobal) || []).length;
    const rightSide = rect.left >= window.innerWidth * 0.40;
    if (hits === 0 && !reviewUiRe.test(txt)) continue;
    let score = 0;
    score += hits * 2200;
    score += distance;
    score += Math.min(el.clientHeight || 0, 1200);
    if (rightSide) score += 9000;
    if (buttonRe.test(txt)) score += 2600;
    if (/by relevance|by most recent date|most relevant|most recent|관련성순|최신순/i.test(txt)) score += 1400;
    if (/user reviews|사용자 리뷰/i.test(txt)) score += 1200;
    candidates.push({ el, score });
  }
  candidates.sort((a, b) => b.score - a.score);
  const panel = candidates[0] ? candidates[0].el : null;
  if (!panel) return { found: false };
  return {
    found: true,
    scrollTop: panel.scrollTop || 0,
    scrollHeight: panel.scrollHeight || 0,
    clientHeight: panel.clientHeight || 0,
    maxTop: Math.max((panel.scrollHeight || 0) - (panel.clientHeight || 0), 0),
    rectTop: panel.getBoundingClientRect().top,
    rectBottom: panel.getBoundingClientRect().bottom,
  };
}
"""


SET_PANEL_SCROLL_JS = r"""
(args) => {
  const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
  const visible = (el) => {
    if (!el || !el.isConnected) return false;
    const st = window.getComputedStyle(el);
    if (!st) return false;
    if (st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity || '1') === 0) return false;
    const rect = el.getBoundingClientRect();
    return rect.width >= 6 && rect.height >= 6;
  };
  const relDateGlobal = /\b(?:today|yesterday|\d+\s+(?:day|week|month|year)s?\s+ago)\b|(?:오늘|어제|\d+\s*일\s*전|\d+\s*주\s*전|\d+\s*(?:개월|달)\s*전|\d+\s*년\s*전)/ig;
  const reviewUiRe = /user reviews|reviews?|리뷰|most relevant|most recent|by relevance|by most recent date|관련성순|최신순/i;
  const buttonRe = /(?:see more reviews|more reviews|show more reviews|리뷰 더보기)/i;
  const candidates = [];
  for (const el of Array.from(document.querySelectorAll('*'))) {
    if (!visible(el)) continue;
    if (el === document.body || el === document.documentElement) continue;
    const rect = el.getBoundingClientRect();
    const distance = Math.max((el.scrollHeight || 0) - (el.clientHeight || 0), 0);
    if (distance < 60) continue;
    const txt = clean((el.innerText || '').slice(0, 30000));
    const hits = (txt.match(relDateGlobal) || []).length;
    const rightSide = rect.left >= window.innerWidth * 0.40;
    if (hits === 0 && !reviewUiRe.test(txt)) continue;
    let score = 0;
    score += hits * 2200;
    score += distance;
    score += Math.min(el.clientHeight || 0, 1200);
    if (rightSide) score += 9000;
    if (buttonRe.test(txt)) score += 2600;
    if (/by relevance|by most recent date|most relevant|most recent|관련성순|최신순/i.test(txt)) score += 1400;
    if (/user reviews|사용자 리뷰/i.test(txt)) score += 1200;
    candidates.push({ el, score });
  }
  candidates.sort((a, b) => b.score - a.score);
  const panel = candidates[0] ? candidates[0].el : null;
  if (!panel) return { found: false };
  const maxTop = Math.max((panel.scrollHeight || 0) - (panel.clientHeight || 0), 0);
  const beforeTop = panel.scrollTop || 0;
  const requested = Math.max(0, Math.min(Number(args && args.top || 0), maxTop));
  panel.scrollTop = requested;
  return {
    found: true,
    beforeTop,
    afterTop: panel.scrollTop || 0,
    maxTop,
    clientHeight: panel.clientHeight || 0,
    scrollHeight: panel.scrollHeight || 0,
  };
}
"""


EXPAND_REVIEW_TEXT_JS = r"""
() => {
  const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
  const visible = (el) => {
    if (!el || !el.isConnected) return false;
    const st = window.getComputedStyle(el);
    if (!st) return false;
    if (st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity || '1') === 0) return false;
    const rect = el.getBoundingClientRect();
    return rect.width >= 6 && rect.height >= 6;
  };
  const buttons = Array.from(document.querySelectorAll('button, [role="button"], a, [role="link"], span, div'));
  let clicked = 0;
  for (const el of buttons) {
    if (!visible(el)) continue;
    const txt = clean(el.innerText || el.textContent || '');
    if (!txt) continue;
    if (!/^(?:see more|read more|show more|more|더보기)$/i.test(txt)) continue;
    if (/리뷰 더보기|see more reviews|more reviews|show more reviews|by relevance|by most recent date|most relevant|most recent|관련성순|최신순/i.test(txt)) continue;
    const rect = el.getBoundingClientRect();
    if (rect.top < -5 || rect.bottom > (window.innerHeight + 40)) continue;
    try {
      el.click();
      clicked += 1;
    } catch (e) {}
  }
  return clicked;
}
"""


MORE_REVIEWS_STATUS_JS = r"""
() => {
  const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
  const visible = (el) => {
    if (!el || !el.isConnected) return false;
    const st = window.getComputedStyle(el);
    if (!st) return false;
    if (st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity || '1') === 0) return false;
    const rect = el.getBoundingClientRect();
    return rect.width >= 6 && rect.height >= 6;
  };
  const relDateGlobal = /(?:today|yesterday|\d+\s+(?:day|week|month|year)s?\s+ago)|(?:오늘|어제|\d+\s*일\s*전|\d+\s*주\s*전|\d+\s*(?:개월|달)\s*전|\d+\s*년\s*전)/ig;
  const reviewUiRe = /user reviews|reviews?|리뷰|most relevant|most recent|by relevance|by most recent date|관련성순|최신순/i;
  const buttonRe = /^(?:see more reviews|more reviews|show more reviews|리뷰 더보기)$/i;

  const scrollables = [];
  for (const el of Array.from(document.querySelectorAll('*'))) {
    if (!visible(el)) continue;
    if (el === document.body || el === document.documentElement) continue;
    const rect = el.getBoundingClientRect();
    const distance = Math.max((el.scrollHeight || 0) - (el.clientHeight || 0), 0);
    if (distance < 60) continue;
    const txt = clean((el.innerText || '').slice(0, 20000));
    const hits = (txt.match(relDateGlobal) || []).length;
    const rightSide = rect.left >= window.innerWidth * 0.40;
    if (hits === 0 && !reviewUiRe.test(txt)) continue;
    let score = 0;
    score += hits * 2200;
    score += distance;
    score += Math.min(el.clientHeight || 0, 1200);
    if (rightSide) score += 9000;
    if (/see more reviews|more reviews|show more reviews|리뷰 더보기/i.test(txt)) score += 2600;
    if (/by relevance|by most recent date|most relevant|most recent|관련성순|최신순/i.test(txt)) score += 1500;
    if (/user reviews|사용자 리뷰/i.test(txt)) score += 1200;
    scrollables.push({ el, score, rect });
  }
  scrollables.sort((a, b) => b.score - a.score);
  const panel = scrollables[0] ? scrollables[0].el : null;

  const matches = [];
  const seen = new Set();
  const roots = panel ? [panel] : [];
  for (const root of roots) {
    const nodes = Array.from(root.querySelectorAll('button, [role="button"], a, [role="link"], div, span'));
    for (const el of nodes) {
      if (seen.has(el) || !visible(el)) continue;
      const txt = clean(el.innerText || el.textContent || '');
      if (!buttonRe.test(txt)) continue;
      seen.add(el);
      const rect = el.getBoundingClientRect();
      const prect = panel.getBoundingClientRect();
      let score = 0;
      if (rect.left >= prect.left - 24 && rect.right <= prect.right + 24) score += 6000;
      if (rect.top >= prect.top + prect.height * 0.30) score += 5000;
      score += Math.max(0, 5200 - Math.min(Math.abs(prect.bottom - rect.bottom) * 18, 5200));
      score += Math.max(0, 1400 - Math.min(Math.abs((prect.left + prect.right) / 2 - (rect.left + rect.right) / 2) * 4, 1400));
      if (/see more reviews/i.test(txt)) score += 900;
      if (txt.toLowerCase() === 'more reviews') score += 700;
      matches.push({ txt, rect, score });
    }
  }
  matches.sort((a, b) => b.score - a.score);
  const target = matches[0] || null;
  const panelInfo = panel ? {
    scrollTop: panel.scrollTop || 0,
    scrollHeight: panel.scrollHeight || 0,
    clientHeight: panel.clientHeight || 0,
    rectTop: panel.getBoundingClientRect().top,
    rectBottom: panel.getBoundingClientRect().bottom,
    atEnd: ((panel.scrollTop || 0) + (panel.clientHeight || 0)) >= ((panel.scrollHeight || 0) - 8),
  } : null;
  return { found: !!target, text: target ? target.txt : '', visible: !!target, panelFound: !!panel, panelInfo, candidates: matches.length, targetTop: target ? target.rect.top : null };
}
"""

CLICK_MORE_REVIEWS_JS = r"""
() => {
  const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
  const visible = (el) => {
    if (!el || !el.isConnected) return false;
    const st = window.getComputedStyle(el);
    if (!st) return false;
    if (st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity || '1') === 0) return false;
    const rect = el.getBoundingClientRect();
    return rect.width >= 6 && rect.height >= 6;
  };
  const clickRoot = (el) => {
    let cur = el;
    for (let i = 0; i < 7 && cur; i += 1) {
      const role = (cur.getAttribute && cur.getAttribute('role') || '').toLowerCase();
      const tag = (cur.tagName || '').toLowerCase();
      if (tag === 'button' || tag === 'a' || role === 'button' || role === 'link' || cur.onclick || cur.tabIndex >= 0) {
        return cur;
      }
      cur = cur.parentElement;
    }
    return el;
  };
  const relDateGlobal = /\b(?:today|yesterday|\d+\s+(?:day|week|month|year)s?\s+ago)\b|(?:오늘|어제|\d+\s*일\s*전|\d+\s*주\s*전|\d+\s*(?:개월|달)\s*전|\d+\s*년\s*전)/ig;
  const reviewUiRe = /user reviews|reviews?|리뷰|most relevant|most recent|by relevance|by most recent date|관련성순|최신순/i;
  const buttonRe = /^(?:see more reviews|more reviews|show more reviews|리뷰 더보기)$/i;

  const scrollables = [];
  for (const el of Array.from(document.querySelectorAll('*'))) {
    if (!visible(el)) continue;
    if (el === document.body || el === document.documentElement) continue;
    const rect = el.getBoundingClientRect();
    const distance = Math.max((el.scrollHeight || 0) - (el.clientHeight || 0), 0);
    if (distance < 60) continue;
    const txt = clean((el.innerText || '').slice(0, 20000));
    const hits = (txt.match(relDateGlobal) || []).length;
    const rightSide = rect.left >= window.innerWidth * 0.40;
    if (hits === 0 && !reviewUiRe.test(txt)) continue;
    let score = 0;
    score += hits * 2200;
    score += distance;
    score += Math.min(el.clientHeight || 0, 1200);
    if (rightSide) score += 9000;
    if (/see more reviews|more reviews|show more reviews|리뷰 더보기/i.test(txt)) score += 2600;
    if (/by relevance|by most recent date|most relevant|most recent|관련성순|최신순/i.test(txt)) score += 1500;
    if (/user reviews|사용자 리뷰/i.test(txt)) score += 1200;
    scrollables.push({ el, score, rect });
  }
  scrollables.sort((a, b) => b.score - a.score);
  const panel = scrollables[0] ? scrollables[0].el : null;
  if (!panel) return { clicked: false, text: '', candidates: 0, found: false, reason: 'panel_not_found' };

  const matches = [];
  const seen = new Set();
  const nodes = Array.from(panel.querySelectorAll('button, [role="button"], a, [role="link"], div, span'));
  for (const node of nodes) {
    if (!visible(node)) continue;
    const txt = clean(node.innerText || node.textContent || '');
    if (!buttonRe.test(txt)) continue;
    const el = clickRoot(node);
    if (!visible(el) || seen.has(el)) continue;
    seen.add(el);
    const rect = el.getBoundingClientRect();
    const prect = panel.getBoundingClientRect();
    const distanceToBottom = Math.abs(prect.bottom - rect.bottom);
    const distanceToCenterX = Math.abs((prect.left + prect.right) / 2 - (rect.left + rect.right) / 2);
    let score = 0;
    score += 24000;
    if (rect.left >= prect.left - 24 && rect.right <= prect.right + 24) score += 4200;
    if (rect.top >= prect.top + prect.height * 0.35) score += 5200;
    score += Math.max(0, 7200 - Math.min(distanceToBottom * 22, 7200));
    score += Math.max(0, 1800 - Math.min(distanceToCenterX * 4, 1800));
    if (/see more reviews/i.test(txt)) score += 1200;
    if (txt.toLowerCase() === 'more reviews') score += 1000;
    if (el.tagName === 'BUTTON') score += 500;
    if ((el.getAttribute('role') || '').toLowerCase() === 'button') score += 320;
    score += Math.round(rect.top);
    matches.push({ el, txt, rect, score });
  }
  matches.sort((a, b) => b.score - a.score);
  const target = matches[0];
  if (!target) {
    return { clicked: false, text: '', candidates: 0, found: false, reason: 'button_not_found', panelScrollTop: panel.scrollTop || 0 };
  }

  try { target.el.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
  const rect = target.el.getBoundingClientRect();
  const cx = rect.left + rect.width / 2;
  const cy = rect.top + rect.height / 2;
  const fireMouse = (type) => target.el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true, clientX: cx, clientY: cy }));
  try { target.el.focus && target.el.focus(); } catch (e) {}
  try { target.el.click(); }
  catch (e) {
    try {
      fireMouse('pointerdown');
      fireMouse('mousedown');
      fireMouse('mouseup');
      fireMouse('click');
    } catch (err) { return { clicked: false, text: target.txt, candidates: matches.length, found: true, error: String(err || e), panelScrollTop: panel.scrollTop || 0 }; }
  }
  return { clicked: true, text: target.txt, candidates: matches.length, found: true, panelScrollTop: panel.scrollTop || 0 };
}
"""




SCROLL_REVIEWS_JS = r"""
() => {
  const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
  const visible = (el) => {
    if (!el || !el.isConnected) return false;
    const st = window.getComputedStyle(el);
    if (!st) return false;
    if (st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity || '1') === 0) return false;
    const rect = el.getBoundingClientRect();
    return rect.width >= 6 && rect.height >= 6;
  };
  const relDateGlobal = /(?:today|yesterday|\d+\s+(?:day|week|month|year)s?\s+ago)|(?:오늘|어제|\d+\s*일\s*전|\d+\s*주\s*전|\d+\s*(?:개월|달)\s*전|\d+\s*년\s*전)/ig;
  const reviewUiRe = /user reviews|reviews?|리뷰|most relevant|most recent|by relevance|by most recent date|관련성순|최신순/i;
  const buttonRe = /^(?:see more reviews|more reviews|show more reviews|리뷰 더보기)$/i;
  const candidates = [];
  for (const el of Array.from(document.querySelectorAll('*'))) {
    if (!visible(el)) continue;
    if (el === document.body || el === document.documentElement) continue;
    const rect = el.getBoundingClientRect();
    const distance = Math.max((el.scrollHeight || 0) - (el.clientHeight || 0), 0);
    if (distance < 60) continue;
    const txt = clean((el.innerText || '').slice(0, 20000));
    const hits = (txt.match(relDateGlobal) || []).length;
    const rightSide = rect.left >= window.innerWidth * 0.40;
    if (hits === 0 && !reviewUiRe.test(txt)) continue;
    let score = 0;
    score += hits * 2200;
    score += distance;
    score += Math.min(el.clientHeight || 0, 1200);
    if (rightSide) score += 9000;
    if (/see more reviews|more reviews|show more reviews|리뷰 더보기/i.test(txt)) score += 2600;
    if (/by relevance|by most recent date|most relevant|most recent|관련성순|최신순/i.test(txt)) score += 1400;
    if (/user reviews|사용자 리뷰/i.test(txt)) score += 1200;
    candidates.push({ el, score, hits, distance, clientHeight: el.clientHeight || 0 });
  }
  candidates.sort((a, b) => b.score - a.score);
  const best = candidates[0] || null;
  let beforeTop = 0;
  let afterTop = 0;
  let maxTop = 0;
  let buttonVisible = false;
  if (best) {
    beforeTop = best.el.scrollTop || 0;
    maxTop = Math.max((best.el.scrollHeight || 0) - (best.el.clientHeight || 0), 0);
    const nodes = Array.from(best.el.querySelectorAll('button, [role="button"], a, [role="link"], div, span'));
    for (const el of nodes) {
      if (!visible(el)) continue;
      const txt = clean(el.innerText || el.textContent || '');
      if (!buttonRe.test(txt)) continue;
      const rect = el.getBoundingClientRect();
      const prect = best.el.getBoundingClientRect();
      if (rect.bottom >= prect.top - 8 && rect.top <= prect.bottom + 8) {
        buttonVisible = true;
        break;
      }
    }
    if (buttonVisible) {
      afterTop = beforeTop;
    } else {
      const step = Math.max(Math.floor((best.el.clientHeight || window.innerHeight || 900) * 0.52), 260);
      try {
        best.el.scrollTop = Math.min(beforeTop + step, maxTop);
        afterTop = best.el.scrollTop || 0;
      } catch (e) {
        afterTop = beforeTop;
      }
    }
  }
  return {
    found: !!best,
    beforeTop,
    afterTop,
    moved: Math.max(0, afterTop - beforeTop),
    atEnd: best ? ((afterTop + best.clientHeight) >= ((best.el.scrollHeight || 0) - 8)) : false,
    hits: best ? best.hits : 0,
    maxTop,
    buttonVisible,
  };
}
"""








BIG_SCROLL_TO_REVIEWS_JS = r"""
() => {
  const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
  const visible = (el) => {
    if (!el || !el.isConnected) return false;
    const st = window.getComputedStyle(el);
    if (!st) return false;
    if (st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity || '1') === 0) return false;
    const rect = el.getBoundingClientRect();
    return rect.width >= 6 && rect.height >= 6;
  };
  const reviewAnchorRe = /^(?:user reviews|by relevance|by most recent date|사용자 리뷰|관련성순|최신순)$/i;
  const reviewHintRe = /user reviews|reviews written on|작성된 리뷰|by relevance|by most recent date|most recent|most relevant|(?:today|yesterday|\d+\s+(?:day|week|month|year)s?\s+ago)|(?:오늘|어제|\d+\s*일\s*전|\d+\s*주\s*전|\d+\s*(?:개월|달)\s*전|\d+\s*년\s*전)/i;
  const priceHintRe = /(?:\$\s*\d|usd|free delivery|pre-owned|used|vehicle tires|browse products|\d{2,3}\/\d{2,3}r\d{2})/i;
  const nodes = Array.from(document.querySelectorAll('button, [role="button"], a, [role="link"], div, span'));
  let chosen = null;
  let bestScore = -1;
  let userReviewsTarget = null;
  let userReviewsScore = -1;
  let reviewHits = 0;
  let priceHits = 0;

  for (const el of nodes) {
    if (!visible(el)) continue;
    const rect = el.getBoundingClientRect();
    if (rect.left < window.innerWidth * 0.40) continue;
    const txt = clean(el.innerText || el.textContent || '');
    if (!txt) continue;
    if (reviewHintRe.test(txt)) reviewHits += 1;
    if (priceHintRe.test(txt)) priceHits += 1;
    if (!reviewAnchorRe.test(txt)) continue;
    let score = 0;
    if (/user reviews|사용자 리뷰/i.test(txt)) score += 20000;
    if (/by most recent date|by relevance|관련성순|최신순/i.test(txt)) score += 12000;
    if (rect.top >= -120 && rect.top <= window.innerHeight * 0.7) score += 8000;
    score += Math.max(0, 6000 - Math.min(Math.abs(rect.top - 180) * 12, 6000));
    if (score > bestScore) {
      bestScore = score;
      chosen = { el, txt, rect };
    }
    if (/user reviews|사용자 리뷰/i.test(txt)) {
      const uScore = score + 4000;
      if (uScore > userReviewsScore) {
        userReviewsScore = uScore;
        userReviewsTarget = { el, txt, rect };
      }
    }
  }

  const beforeY = window.scrollY || window.pageYOffset || 0;
  let action = 'none';
  let clickedUserReviews = false;

  if (priceHits > reviewHits && userReviewsTarget) {
    try {
      userReviewsTarget.el.scrollIntoView({ block: 'start', inline: 'nearest' });
      userReviewsTarget.el.click();
      clickedUserReviews = true;
      action = 'click_user_reviews';
    } catch (e) {}
  } else if (chosen) {
    const rect = chosen.rect || chosen.el.getBoundingClientRect();
    if (rect.top < -80) {
      try {
        window.scrollBy({ top: Math.max(rect.top - 80, -Math.floor(window.innerHeight * 0.35)), behavior: 'auto' });
        action = 'scroll_up_to_review_anchor';
      } catch (e) {}
    } else if (rect.top > window.innerHeight * 0.82) {
      try {
        window.scrollBy({ top: Math.min(rect.top - window.innerHeight * 0.35, Math.floor(window.innerHeight * 0.30)), behavior: 'auto' });
        action = 'scroll_down_to_review_anchor';
      } catch (e) {}
    } else {
      action = 'anchor_already_visible';
    }
  }

  const afterY = window.scrollY || window.pageYOffset || 0;
  return {
    found: !!chosen,
    moved: Math.abs(afterY - beforeY),
    beforeY,
    afterY,
    text: chosen ? clean(chosen.txt) : '',
    reviewHits,
    priceHits,
    clickedUserReviews,
    action,
  };
}
"""
@dataclass
class CrawlConfig:
    brand: str
    product_name: str
    start_date: Optional[date]
    end_date: Optional[date]
    headless: bool = True
    page_timeout_ms: int = 90000
    control_hook: Optional[Callable[[], None]] = None
    partial_flush: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    verification_hook: Optional[Callable[[str], None]] = None


def maybe_checkpoint(control_hook: Optional[Callable[[], None]]):
    if control_hook:
        control_hook()


class VerificationEscalation(RuntimeError):
    """Raised when a hidden browser must be reopened visibly for manual Google verification."""


class GoogleShoppingCrawler:
    def __init__(self, cfg: CrawlConfig, output_dir: Path, logger: Logger):
        self.cfg = cfg
        self.output_dir = output_dir
        self.logger = logger
        self.rows: List[Dict[str, Any]] = []
        self.seen_keys = set()
        self.base_crawl_date = datetime.now().date()
        self.expected_review_count: Optional[int] = None
        self.start_boundary_logged = False

    def log(self, message: str):
        self.logger.log(message)

    def detect_verification(self, page: Page) -> Optional[str]:
        try:
            url = page.url or ""
        except Exception:
            url = ""
        if "google.com/sorry" in url or "/sorry/" in url:
            return "Google verification 페이지가 열렸습니다. 브라우저에서 확인을 완료한 뒤 Resume을 누르세요."
        try:
            body = clean_text((page.locator("body").inner_text(timeout=1500) or "")[:4000])
        except Exception:
            body = ""
        lowered = body.lower()
        signals = [
            "unusual traffic",
            "our systems have detected unusual traffic",
            "not a robot",
            "i'm not a robot",
            "recaptcha",
            "소화전",
            "로봇이 아닙니다",
            "비정상적인 트래픽",
        ]
        if any(sig in lowered for sig in signals):
            return "Google verification이 감지되었습니다. 브라우저에서 CAPTCHA를 푼 뒤 Resume을 누르세요."
        try:
            if page.locator("iframe[src*='recaptcha'], iframe[title*='reCAPTCHA'], #captcha, [name='captcha']").count() > 0:
                return "Google CAPTCHA가 감지되었습니다. 브라우저에서 직접 해결한 뒤 Resume을 누르세요."
        except Exception:
            pass
        return None

    def handle_verification_if_needed(self, page: Page, stage: str):
        message = self.detect_verification(page)
        if not message:
            return
        if self.cfg.headless:
            raise VerificationEscalation(f"{message} (단계: {stage})")
        notice = f"{message} (단계: {stage})"
        self.log(notice)
        if self.cfg.verification_hook:
            self.cfg.verification_hook(notice)
        while True:
            maybe_checkpoint(self.cfg.control_hook)
            page.wait_for_timeout(800)
            still_blocked = self.detect_verification(page)
            if not still_blocked:
                self.log(f"Google verification 해제 확인: {stage}")
                page.wait_for_timeout(1200)
                return
            if self.cfg.verification_hook:
                self.cfg.verification_hook(f"아직 verification 페이지가 유지되고 있습니다. 브라우저에서 해결 후 Resume을 다시 눌러주세요. (단계: {stage})")

    def _run_flow(self, page: Page) -> List[Dict[str, Any]]:
        page.set_default_timeout(self.cfg.page_timeout_ms)
        self.open_search(page)
        self.dismiss_popups(page)
        self.select_product(page)
        self.handle_verification_if_needed(page, "select_product")
        self.open_user_reviews(page)
        self.handle_verification_if_needed(page, "open_user_reviews")
        self.sort_most_recent(page)
        self.handle_verification_if_needed(page, "sort_most_recent")
        self.confirm_sort_before_extract(page)
        self.harvest_reviews(page)
        return self.rows

    def _get_or_create_shared_page(self) -> Page:
        with _SHARED_BROWSER_LOCK:
            page = _SHARED_BROWSER.get("page")
            try:
                if page is not None and not page.is_closed():
                    return page
            except Exception:
                pass

            old_context = _SHARED_BROWSER.get("context")
            old_browser = _SHARED_BROWSER.get("browser")
            old_pw = _SHARED_BROWSER.get("pw")
            for obj, closer in ((old_context, 'close'), (old_browser, 'close'), (old_pw, 'stop')):
                try:
                    if obj is not None:
                        getattr(obj, closer)()
                except Exception:
                    pass

            pw = sync_playwright().start()
            browser = pw.chromium.launch(headless=False, slow_mo=80)
            context = browser.new_context(
                locale="ko-KR",
                user_agent=USER_AGENT,
                viewport={"width": 1600, "height": 1100},
            )
            page = context.new_page()
            _SHARED_BROWSER.update({
                "pw": pw,
                "browser": browser,
                "context": context,
                "page": page,
            })
            return page

    def launch(self) -> List[Dict[str, Any]]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if not self.cfg.headless:
            page = self._get_or_create_shared_page()
            return self._run_flow(page)

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, slow_mo=0)
            context = browser.new_context(
                locale="ko-KR",
                user_agent=USER_AGENT,
                viewport={"width": 1600, "height": 1100},
            )
            page = context.new_page()
            try:
                return self._run_flow(page)
            except VerificationEscalation as exc:
                if self.cfg.verification_hook:
                    self.cfg.verification_hook(
                        "Google verification이 감지되어 브라우저 창을 자동으로 열었습니다. 브라우저에서 확인을 완료한 뒤 Resume을 누르세요."
                    )
                self.log("headless 모드에서 Google verification이 감지되어 브라우저 표시 모드로 자동 전환합니다.")
                self.cfg.headless = False
                page2 = self._get_or_create_shared_page()
                return self._run_flow(page2)
            finally:
                try:
                    context.close()
                except Exception:
                    pass
                try:
                    browser.close()
                except Exception:
                    pass

    def open_search(self, page: Page):
        url = make_search_url(self.cfg.brand, self.cfg.product_name)
        self.log(f"Google Shopping 검색 페이지 열기: {url}")
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(2500)
        self.handle_verification_if_needed(page, "after_goto")

    def dismiss_popups(self, page: Page):
        patterns = [
            [r"accept all"],
            [r"i agree"],
            [r"got it"],
            [r"continue"],
            [r"not now"],
            [r"no thanks"],
        ]
        for pats in patterns:
            try:
                item = self.find_click_candidates(page, pats, require_all=False, limit=3)
                if item:
                    self.click_marked(page, item[0]["id"])
                    page.wait_for_timeout(1000)
            except Exception:
                continue

    def find_click_candidates(self, page: Page, patterns: List[str], require_all: bool = True, prefer_review_count: bool = False, limit: int = 10) -> List[Dict[str, Any]]:
        maybe_checkpoint(self.cfg.control_hook)
        try:
            result = page.evaluate(CLICK_CANDIDATES_JS, {
                "patterns": patterns,
                "require_all": require_all,
                "prefer_review_count": prefer_review_count,
                "limit": limit,
            })
            return result or []
        except Exception:
            return []

    def click_marked(self, page: Page, mark_id: str) -> bool:
        try:
            loc = page.locator(f'[data-oai-click-id="{mark_id}"]').first
            loc.scroll_into_view_if_needed(timeout=3000)
            loc.click(timeout=4000)
            return True
        except Exception:
            try:
                page.locator(f'[data-oai-click-id="{mark_id}"]').first.click(timeout=4000, force=True)
                return True
            except Exception:
                return False

    def is_review_panel_open(self, page: Page) -> bool:
        patterns = [
            r"all reviews", r"모든 리뷰", r"by relevance", r"by most recent date", r"most relevant", r"관련성순", r"most recent", r"최신순", r"see more reviews", r"more reviews", r"리뷰 더보기"
        ]
        try:
            items = self.find_click_candidates(page, patterns, require_all=False, limit=12)
            return bool(items)
        except Exception:
            return False

    def is_customer_review_mode(self, page: Page) -> bool:
        # Be stricter than is_review_panel_open(): we only consider the crawler to be inside
        # the customer-reviews view when review-sort/review-pagination controls are visible in the
        # right-side product panel. This prevents us from trying to sort before entering User reviews.
        try:
            exact_controls = self._get_right_panel_exact_controls(
                page,
                [
                    "By relevance", "Sort by: Relevance", "By most recent date", "Most recent date",
                    "관련성순", "최신순", "More reviews", "See more reviews", "Show more reviews", "리뷰 더보기"
                ],
                limit=18,
            )
            if exact_controls:
                return True
        except Exception:
            pass
        try:
            texts = self.get_sort_state_texts(page)
            for text_value in texts:
                normalized = normalize_text(text_value)
                if (
                    "by relevance" in normalized
                    or "sort by: relevance" in normalized
                    or "by most recent date" in normalized
                    or "most recent date" in normalized
                    or "most recent" in normalized
                    or "관련성순" in text_value
                    or "최신순" in text_value
                ):
                    return True
        except Exception:
            pass
        try:
            status = self.get_more_reviews_status(page)
            if bool(status.get("visible")):
                return True
        except Exception:
            pass
        return False

    def click_by_text_regex(self, page: Page, patterns: List[str], limit: int = 12, wait_ms: int = 1500, prefer_right: bool = False) -> Optional[str]:
        regex = re.compile("|".join(f"(?:{p})" for p in patterns), re.I)
        selector_groups = [
            "button, [role='button'], a, [role='link']",
            "div, span",
        ]
        for selector in selector_groups:
            try:
                locator = page.locator(selector).filter(has_text=regex)
                count = min(locator.count(), max(limit * 2, limit))
            except Exception:
                count = 0
            ranked = []
            for idx in range(count):
                maybe_checkpoint(self.cfg.control_hook)
                item = locator.nth(idx)
                try:
                    if not item.is_visible():
                        continue
                except Exception:
                    continue
                try:
                    preview = clean_text(item.inner_text(timeout=1000))
                except Exception:
                    preview = ''
                if not preview:
                    continue
                try:
                    box = item.bounding_box() or {}
                except Exception:
                    box = {}
                x = float(box.get('x', 0) or 0)
                y = float(box.get('y', 0) or 0)
                width = float(box.get('width', 0) or 0)
                height = float(box.get('height', 0) or 0)
                right_score = x + (width * 0.5)
                ranked.append((right_score, -y, -(width * height), idx, preview, item))
            if prefer_right:
                ranked.sort(reverse=True)
            else:
                ranked.sort(key=lambda t: (t[1], t[0], t[2]), reverse=True)
            for _, _, _, _, preview, item in ranked[:limit]:
                try:
                    item.scroll_into_view_if_needed(timeout=3000)
                except Exception:
                    pass
                for force in (False, True):
                    try:
                        item.click(timeout=3000, force=force)
                        page.wait_for_timeout(wait_ms)
                        return preview[:180] if preview else regex.pattern
                    except Exception:
                        continue
        return None

    def select_product(self, page: Page):
        patterns = product_query_patterns(self.cfg.brand, self.cfg.product_name)
        self.log(f"상품 후보 검색 패턴: {patterns}")
        candidates = self.find_click_candidates(page, patterns, require_all=False, prefer_review_count=True, limit=20)
        if not candidates:
            raise RuntimeError("검색 결과에서 클릭 가능한 상품 후보를 찾지 못했습니다. headful 모드로 다시 시도해 주세요.")

        strict_candidates = [
            cand for cand in candidates
            if contains_normalized_phrase(cand.get("text", ""), self.cfg.brand)
            and product_name_matches_strict(cand.get("text", ""), self.cfg.product_name)
        ]
        if strict_candidates:
            candidates = strict_candidates
            self.log(f"상품명 엄격 일치 후보만 사용(뒤의 사이즈/코드만 허용): {len(candidates)}개")
        else:
            raise RuntimeError("입력한 상품명과 정확히 일치하는 상품 후보를 찾지 못했습니다. 비슷한 다른 상품(SUV 등)은 제외했습니다. 상품명 철자/표기를 확인해 주세요.")

        candidates = sorted(
            candidates,
            key=lambda c: (
                -int(c.get("review_count") or 0),
                len(normalize_text(c.get("text", ""))),
            ),
        )

        for cand in candidates[:8]:
            self.log(f"후보: review_count={cand.get('review_count')} | text={cand.get('text','')[:180]}")

        clicked = False
        for cand in candidates:
            if self.click_marked(page, cand["id"]):
                clicked = True
                self.expected_review_count = cand.get("review_count") if isinstance(cand.get("review_count"), int) else None
                self.log(f"상품 클릭: {cand.get('text','')[:180]}")
                if self.expected_review_count:
                    self.log(f"선택 상품 리뷰 개수(검색 카드 기준): {self.expected_review_count}개")
                page.wait_for_timeout(2500)
                break
        if not clicked:
            raise RuntimeError("상품 후보 클릭에 실패했습니다.")

    def open_user_reviews(self, page: Page):
        # All action steps must happen inside the right-side product popup/panel.
        # Always click the explicit "(N) User reviews" control in that popup first.
        patterns = [r"(?:^|\b)user\s*reviews?(?:\b|$)", r"사용자\s*리뷰"]
        for cand in self._get_right_panel_contains_controls(page, patterns, limit=24):
            preview = clean_text(cand.get("text", ""))[:160]
            normalized = normalize_text(preview)
            if "user reviews" not in normalized and "사용자 리뷰" not in preview:
                continue
            try:
                cand["locator"].click(timeout=2600)
                self.log(f"User reviews 버튼 클릭: {preview}")
                page.wait_for_timeout(2400)
                if self.is_customer_review_mode(page):
                    return
            except Exception:
                continue

        if self.is_customer_review_mode(page):
            self.log("User reviews 버튼은 보이지 않지만 고객 리뷰 화면으로는 진입한 것으로 확인했습니다.")
            return

        raise RuntimeError("오른쪽 상품 팝업 안에서 '(숫자) User reviews' 버튼을 찾지 못했습니다.")

    def get_sort_state_texts(self, page: Page) -> List[str]:
        patterns = [
            r"by\s*most\s*recent\s*date", r"most\s*recent\s*date", r"most\s*recent", r"최신순",
            r"by\s*relevance", r"sort\s*by\s*:?\s*relevance", r"most\s*relevant", r"관련성순"
        ]
        texts: List[str] = []
        seen = set()
        for cand in self._get_right_panel_contains_controls(page, patterns, limit=24):
            text_value = clean_text(cand.get("text", ""))
            normalized = normalize_text(text_value)
            if not text_value or normalized in seen:
                continue
            seen.add(normalized)
            texts.append(text_value[:160])
        return texts

    def is_most_recent_selected(self, page: Page) -> bool:
        for text_value in self.get_sort_state_texts(page):
            normalized = normalize_text(text_value)
            if (
                "by most recent date" in normalized
                or "most recent date" in normalized
                or "most recent" in normalized
                or "sort by: most recent" in normalized
                or "최신순" in text_value
            ):
                return True
        return False

    def try_click_patterns_until_confirm(self, page: Page, patterns: List[str], confirm_fn: Callable[[], bool], limit: int = 12, wait_ms: int = 1500, prefer_right: bool = False) -> Optional[str]:
        clicked = self.click_by_text_regex(page, patterns, limit=limit, wait_ms=wait_ms, prefer_right=prefer_right)
        if clicked and confirm_fn():
            return clicked
        items = self.find_click_candidates(page, patterns, require_all=False, limit=limit)
        if prefer_right:
            items = sorted(items, key=lambda c: float(c.get('rect', {}).get('x', 0) or 0), reverse=True)
        for cand in items:
            maybe_checkpoint(self.cfg.control_hook)
            if self.click_marked(page, cand["id"]):
                page.wait_for_timeout(wait_ms)
                if confirm_fn():
                    return clean_text(cand.get("text", ""))[:180]
        return None

    def _get_right_panel_sort_candidates(self, page: Page, patterns: List[str], limit: int = 24) -> List[Dict[str, Any]]:
        candidates = self.find_click_candidates(page, patterns, require_all=False, limit=limit)
        try:
            viewport = page.viewport_size or {"width": 1400, "height": 900}
        except Exception:
            viewport = {"width": 1400, "height": 900}
        width = float((viewport or {}).get("width") or 1400)
        height = float((viewport or {}).get("height") or 900)

        def score(c: Dict[str, Any]) -> tuple:
            rect = c.get("rect") or {}
            x = float(rect.get("x", 0) or 0)
            y = float(rect.get("y", 0) or 0)
            w = float(rect.get("width", 0) or 0)
            text = normalize_text(c.get("text", ""))
            right_half = x >= width * 0.55
            near_top = y <= height * 0.45
            sortish = ("sort by" in text) or ("by relevance" in text) or ("by most recent date" in text) or ("most recent date" in text)
            generic_relevance = text == "relevance"
            return (
                1 if right_half else 0,
                1 if near_top else 0,
                1 if sortish else 0,
                0 if generic_relevance else 1,
                x + w,
                -y,
            )

        filtered = [c for c in candidates if float((c.get("rect") or {}).get("x", 0) or 0) >= width * 0.45]
        ranked = filtered or candidates
        ranked = sorted(ranked, key=score, reverse=True)
        return ranked[:limit]

    def _get_right_panel_exact_controls(self, page: Page, allowed_texts: List[str], limit: int = 24) -> List[Dict[str, Any]]:
        allowed = {normalize_text(t) for t in allowed_texts if t}
        selectors = [
            "button",
            "[role='button']",
            "[role='menuitem']",
            "[role='option']",
            "li",
            "div[tabindex]",
            "span[tabindex]",
            "a",
        ]
        out: List[Dict[str, Any]] = []
        seen = set()
        try:
            viewport = page.viewport_size or {"width": 1400, "height": 900}
        except Exception:
            viewport = {"width": 1400, "height": 900}
        width = float((viewport or {}).get("width") or 1400)
        height = float((viewport or {}).get("height") or 900)
        for selector in selectors:
            try:
                locator = page.locator(selector)
                count = min(locator.count(), 120)
            except Exception:
                count = 0
            for idx in range(count):
                try:
                    item = locator.nth(idx)
                    if not item.is_visible():
                        continue
                    text_value = clean_text(item.inner_text(timeout=800))
                    normalized = normalize_text(text_value)
                    if not text_value or normalized not in allowed:
                        continue
                    box = item.bounding_box() or {}
                except Exception:
                    continue
                x = float(box.get("x") or 0)
                y = float(box.get("y") or 0)
                w = float(box.get("width") or 0)
                if x < width * 0.55:
                    continue
                key = (normalized, round(x, 1), round(y, 1), round(w, 1))
                if key in seen:
                    continue
                seen.add(key)
                out.append({
                    "locator": item,
                    "text": text_value,
                    "x": x,
                    "y": y,
                    "w": w,
                    "score": (
                        1 if ("sort by" in normalized or "by relevance" in normalized or "by most recent date" in normalized) else 0,
                        1 if y <= height * 0.45 else 0,
                        x + w,
                        -y,
                    )
                })
        out.sort(key=lambda c: c.get("score"), reverse=True)
        return out[:limit]

    def _get_right_panel_contains_controls(self, page: Page, patterns: List[str], limit: int = 24) -> List[Dict[str, Any]]:
        regex = re.compile("|".join(f"(?:{p})" for p in patterns), re.I)
        selectors = [
            "button",
            "[role='button']",
            "[role='menuitem']",
            "[role='option']",
            "li",
            "div[tabindex]",
            "span[tabindex]",
            "a",
        ]
        out: List[Dict[str, Any]] = []
        seen = set()
        try:
            viewport = page.viewport_size or {"width": 1400, "height": 900}
        except Exception:
            viewport = {"width": 1400, "height": 900}
        width = float((viewport or {}).get("width") or 1400)
        height = float((viewport or {}).get("height") or 900)
        for selector in selectors:
            try:
                locator = page.locator(selector)
                count = min(locator.count(), 160)
            except Exception:
                count = 0
            for idx in range(count):
                try:
                    item = locator.nth(idx)
                    if not item.is_visible():
                        continue
                    text_value = clean_text(item.inner_text(timeout=800))
                    if not text_value or not regex.search(text_value):
                        continue
                    box = item.bounding_box() or {}
                except Exception:
                    continue
                x = float(box.get("x") or 0)
                y = float(box.get("y") or 0)
                w = float(box.get("width") or 0)
                h = float(box.get("height") or 0)
                if x < width * 0.55:
                    continue
                normalized = normalize_text(text_value)
                key = (normalized, round(x, 1), round(y, 1), round(w, 1), round(h, 1))
                if key in seen:
                    continue
                seen.add(key)
                out.append({
                    "locator": item,
                    "text": text_value,
                    "x": x,
                    "y": y,
                    "w": w,
                    "h": h,
                    "score": (
                        1 if x >= width * 0.68 else 0,
                        1 if y <= height * 0.72 else 0,
                        1 if ("user reviews" in normalized or "사용자 리뷰" in normalized) else 0,
                        1 if ("sort by" in normalized or "by relevance" in normalized or "by most recent" in normalized) else 0,
                        x + w,
                        -y,
                    )
                })
        out.sort(key=lambda c: c.get("score"), reverse=True)
        return out[:limit]

    def click_sort_menu_button(self, page: Page) -> Optional[str]:
        # Only click the explicit review-sort opener button inside the right-side customer review panel.
        # Do NOT click generic "Sort by: Relevance" status text or other non-button text.
        preferred_norm = {"by relevance", "most relevant", "관련성순"}

        # 1) Strongest path: exact right-panel button text only.
        for cand in self._get_right_panel_exact_controls(page, ["By relevance", "Most relevant", "관련성순"], limit=18):
            preview = clean_text(cand.get("text", ""))[:180]
            normalized = normalize_text(preview)
            if normalized not in preferred_norm:
                continue
            try:
                cand["locator"].click(timeout=2200)
                page.wait_for_timeout(1300)
                return preview or "By relevance"
            except Exception:
                try:
                    box = cand["locator"].bounding_box() or {}
                    x = float(box.get("x") or 0) + max(float(box.get("width") or 0), 1.0) / 2.0
                    y = float(box.get("y") or 0) + max(float(box.get("height") or 0), 1.0) / 2.0
                    if x > 0 and y > 0:
                        page.mouse.click(x, y)
                        page.wait_for_timeout(1300)
                        return preview or "By relevance"
                except Exception:
                    pass
                continue

        # 2) Fallback: right-panel clickable candidates, but still only the actual By relevance button text.
        patterns = [
            r"^by\s*relevance$",
            r"^most\s*relevant$",
            r"^관련성순$",
        ]
        for cand in self._get_right_panel_sort_candidates(page, patterns, limit=20):
            preview = clean_text(cand.get("text", ""))[:180]
            normalized = normalize_text(preview)
            if normalized not in preferred_norm:
                continue
            if self.click_marked(page, cand["id"]):
                page.wait_for_timeout(1300)
                return preview or "By relevance"
        return None

    def click_most_recent_option(self, page: Page) -> Optional[str]:
        # Only select the explicit most-recent option from the opened sort menu.
        exact_allowed = ["By most recent date", "By most recent", "Most recent date", "Most recent", "최신순"]
        preferred_norm = {"by most recent date", "by most recent", "most recent date", "most recent", "최신순"}
        exact_controls = self._get_right_panel_exact_controls(page, exact_allowed, limit=28)
        exact_controls = sorted(
            exact_controls,
            key=lambda c: (
                1 if normalize_text(clean_text(c.get("text", ""))).startswith("by most recent") else 0,
                c.get("score"),
            ),
            reverse=True,
        )
        for cand in exact_controls:
            preview = clean_text(cand.get("text", ""))[:180]
            normalized = normalize_text(preview)
            if normalized not in preferred_norm:
                continue
            try:
                cand["locator"].click(timeout=2400)
                page.wait_for_timeout(1700)
                if self.is_most_recent_selected(page):
                    return preview or "By most recent date"
            except Exception:
                continue
        patterns = [
            r"^by\s*most\s*recent(?:\s*date)?$",
            r"^most\s*recent\s*date$",
            r"^most\s*recent$",
            r"^최신순$",
        ]
        for cand in self._get_right_panel_sort_candidates(page, patterns, limit=28):
            preview = clean_text(cand.get("text", ""))[:180]
            normalized = normalize_text(preview)
            if normalized not in preferred_norm:
                continue
            if self.click_marked(page, cand["id"]):
                page.wait_for_timeout(1700)
                if self.is_most_recent_selected(page):
                    return preview or "By most recent date"
        return None


    def confirm_sort_before_extract(self, page: Page):
        if not self.is_most_recent_selected(page):
            visible_now = self.get_sort_state_texts(page)
            if visible_now:
                self.log(f"리뷰 추출 직전 정렬 재확인 실패: {visible_now[:8]}")
            raise RuntimeError("리뷰 추출 직전에 최신순 정렬을 확인하지 못했습니다. 리뷰 추출을 중단합니다.")
        self.log("리뷰 추출 직전 정렬 재확인: 최신순 / Most recent")

    def sort_most_recent(self, page: Page):
        visible_before = self.get_sort_state_texts(page)
        if visible_before:
            self.log(f"정렬 상태 확인(변경 전): {visible_before[:6]}")

        if not self.is_customer_review_mode(page):
            raise RuntimeError("오른쪽 상품 팝업의 고객 리뷰 화면에 진입하기 전에 정렬을 시도했습니다. 먼저 User reviews를 클릭해야 합니다.")

        if self.is_most_recent_selected(page):
            self.log("정렬 상태 확인: 이미 최신순 / Most recent 입니다.")
            return

        opener_text = self.click_sort_menu_button(page)
        if not opener_text:
            visible_after = self.get_sort_state_texts(page)
            if visible_after:
                self.log(f"정렬 상태 확인(실패 시점): {visible_after[:8]}")
            raise RuntimeError("고객 리뷰 화면의 정렬 버튼(By relevance / Most relevant)을 찾지 못했습니다. 고객 리뷰 화면의 정렬 버튼만 눌러야 합니다.")

        self.log(f"정렬 버튼 클릭: {opener_text}")
        option_text = self.click_most_recent_option(page)
        if option_text:
            self.log(f"정렬 변경 확인: {option_text}")
            return

        try:
            page.keyboard.press("ArrowDown")
            page.wait_for_timeout(350)
            page.keyboard.press("Enter")
            page.wait_for_timeout(1700)
            if self.is_most_recent_selected(page):
                self.log("정렬 변경 확인: By most recent date")
                return
        except Exception:
            pass

        visible_after = self.get_sort_state_texts(page)
        if visible_after:
            self.log(f"정렬 상태 확인(실패 시점): {visible_after[:8]}")
        raise RuntimeError("By most recent date / Most recent 정렬을 확인하지 못했습니다. 리뷰 추출을 중단합니다.")


    def expand_review_bodies(self, page: Page) -> int:
        self.handle_verification_if_needed(page, "expand_review_bodies")
        try:
            clicked = int(page.evaluate(EXPAND_REVIEW_TEXT_JS) or 0)
        except Exception:
            clicked = 0
        if clicked:
            self.log(f"리뷰 본문 더보기 클릭: {clicked}건")
            page.wait_for_timeout(900)
        return clicked

    def get_more_reviews_status(self, page: Page) -> Dict[str, Any]:
        self.handle_verification_if_needed(page, "more_reviews_status")
        try:
            result = page.evaluate(MORE_REVIEWS_STATUS_JS) or {}
        except Exception:
            result = {}
        return result if isinstance(result, dict) else {}

    def pause_until_resume(self, page: Page, message: str):
        self.log(message)
        if self.cfg.verification_hook:
            self.cfg.verification_hook(message)
        maybe_checkpoint(self.cfg.control_hook)
        self.log("Resume 후 직전 지점에서 수집을 이어갑니다.")
        page.wait_for_timeout(1200)
        self.handle_verification_if_needed(page, "resume_after_pause")

    def scroll_reviews_panel_burst(self, page: Page, steps: int = 3) -> Dict[str, Any]:
        last: Dict[str, Any] = {}
        for _ in range(max(1, steps)):
            maybe_checkpoint(self.cfg.control_hook)
            try:
                info = page.evaluate(SCROLL_REVIEWS_JS) or {}
            except Exception as exc:
                self.log(f"경고: 작은 스크롤 이동 중 오류가 발생했지만 계속합니다. ({exc})")
                info = {}
            last = info if isinstance(info, dict) else {}
            page.wait_for_timeout(450)
        return last

    def get_review_panel_metrics(self, page: Page) -> Dict[str, Any]:
        self.handle_verification_if_needed(page, "panel_metrics")
        try:
            result = page.evaluate(PANEL_METRICS_JS) or {}
        except Exception:
            result = {}
        return result if isinstance(result, dict) else {}

    def set_review_panel_scroll(self, page: Page, top: int) -> Dict[str, Any]:
        self.handle_verification_if_needed(page, "panel_set_scroll")
        try:
            result = page.evaluate(SET_PANEL_SCROLL_JS, {"top": int(top)}) or {}
        except Exception:
            result = {}
        return result if isinstance(result, dict) else {}

    def sweep_loaded_reviews_in_panel(self, page: Page, reason: str = "", full: bool = False) -> int:
        metrics = self.get_review_panel_metrics(page)
        if not metrics.get("found"):
            return 0
        original_top = int(metrics.get("scrollTop") or 0)
        max_top = int(metrics.get("maxTop") or 0)
        client_height = int(metrics.get("clientHeight") or 0)
        if client_height <= 0:
            return 0
        start_top = 0 if full else max(0, original_top - int(client_height * 1.8))
        step = max(int(client_height * 0.58), 260)
        positions = []
        pos = start_top
        while pos <= max_top:
            positions.append(pos)
            pos += step
        if max_top not in positions:
            positions.append(max_top)
        added_total = 0
        if reason:
            self.log(f"리뷰 패널 스윕 시작: {reason} | 구간 {start_top}~{max_top}")
        for idx, pos in enumerate(positions):
            maybe_checkpoint(self.cfg.control_hook)
            self.set_review_panel_scroll(page, pos)
            page.wait_for_timeout(520 if idx == 0 else 420)
            self.expand_review_bodies(page)
            try:
                extracted = page.evaluate(EXTRACT_REVIEWS_JS) or []
            except Exception as exc:
                self.log(f"경고: 스윕 중 리뷰 추출 오류가 발생했지만 계속합니다. ({exc})")
                extracted = []
            added = self.merge_rows(extracted)
            if added:
                added_total += added
                self.log(f"리뷰 스윕 추가 수집: +{added}건 / 누적 {len(self.rows)}건")
                if self.cfg.partial_flush:
                    self.cfg.partial_flush(self.rows)
            if self.should_early_stop_on_start_date():
                break
        # 다음 More reviews 클릭을 위해 가능한 아래쪽으로 복귀
        self.set_review_panel_scroll(page, max_top)
        page.wait_for_timeout(350)
        return added_total

    def nudge_large_scroll_to_reviews(self, page: Page, attempts: int = 2) -> Dict[str, Any]:
        last: Dict[str, Any] = {}
        for _ in range(max(1, attempts)):
            maybe_checkpoint(self.cfg.control_hook)
            try:
                info = page.evaluate(BIG_SCROLL_TO_REVIEWS_JS) or {}
            except Exception as exc:
                self.log(f"경고: 큰 스크롤 이동 중 오류가 발생했지만 계속합니다. ({exc})")
                info = {}
            last = info if isinstance(info, dict) else {}
            if last.get('clickedUserReviews'):
                self.log('고객 리뷰 영역을 벗어나 가격/사이즈 파트로 내려간 것으로 보여 User reviews로 다시 복귀했습니다.')
                page.wait_for_timeout(1200)
            elif last.get('action') in {'scroll_up_to_review_anchor', 'scroll_down_to_review_anchor', 'anchor_already_visible'} and last.get('text'):
                self.log(f"고객 리뷰 파트로 복귀 시도: {clean_text(str(last.get('text', '')))[:120]}")
            page.wait_for_timeout(650)
            self.scroll_reviews_panel_burst(page, steps=2)
        return last

    def click_more_reviews_locator(self, page: Page) -> Optional[str]:
        regex = re.compile(r"see\s*more\s*reviews|more\s*reviews|show\s*more\s*reviews|리뷰\s*더보기", re.I)
        selectors = [
            "button, [role='button'], a, [role='link']",
            "div, span",
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector).filter(has_text=regex)
                count = min(locator.count(), 40)
            except Exception:
                count = 0
            ranked = []
            for idx in range(count):
                maybe_checkpoint(self.cfg.control_hook)
                item = locator.nth(idx)
                try:
                    if not item.is_visible():
                        continue
                    preview = clean_text(item.inner_text(timeout=800))
                    box = item.bounding_box() or {}
                except Exception:
                    continue
                if not preview:
                    continue
                x = float(box.get("x", 0) or 0)
                y = float(box.get("y", 0) or 0)
                width = float(box.get("width", 0) or 0)
                height = float(box.get("height", 0) or 0)
                score = y * 10
                if x > 700:
                    score += 6000
                score += min(width * height, 5000)
                ranked.append((score, preview, item))
            ranked.sort(reverse=True, key=lambda t: t[0])
            for _, preview, item in ranked:
                try:
                    item.scroll_into_view_if_needed(timeout=3000)
                except Exception:
                    pass
                for force in (False, True):
                    try:
                        item.click(timeout=3000, force=force)
                        page.wait_for_timeout(1800)
                        return preview
                    except Exception:
                        continue
        return None

    def exhaust_more_reviews_recovery(self, page: Page) -> Dict[str, Any]:
        status_before = self.get_more_reviews_status(page)
        self.scroll_reviews_panel_burst(page, steps=4)
        status_mid = self.get_more_reviews_status(page)
        if status_mid.get("visible"):
            return {"visible": True, "status": status_mid}
        big_info = self.nudge_large_scroll_to_reviews(page, attempts=2)
        status_after = self.get_more_reviews_status(page)
        if not status_after.get("visible"):
            clicked = self.click_by_text_regex(page, [r"user\s*reviews", r"사용자\s*리뷰"], limit=8, wait_ms=1200, prefer_right=True)
            if clicked:
                self.log(f"고객 리뷰 파트 재진입 클릭: {clicked[:120]}")
                page.wait_for_timeout(1200)
                status_after = self.get_more_reviews_status(page)
        return {"visible": bool(status_after.get("visible")), "status": status_after, "big": big_info, "before": status_before}


    def panel_only_more_reviews_recovery(self, page: Page, rounds: int = 10) -> Dict[str, Any]:
        last_status: Dict[str, Any] = {}
        for idx in range(max(1, rounds)):
            maybe_checkpoint(self.cfg.control_hook)
            try:
                self.scroll_reviews_panel_burst(page, steps=3 if idx < 4 else 4)
            except Exception as exc:
                self.log(f"경고: 패널 전용 More reviews 복구 스크롤 중 오류가 발생했지만 계속합니다. ({exc})")
            page.wait_for_timeout(900)
            last_status = self.get_more_reviews_status(page)
            if last_status.get("visible"):
                clicked_text = self.click_more_reviews_locator(page)
                if clicked_text:
                    self.log(f"추가 리뷰 클릭: {clicked_text[:120]}")
                    page.wait_for_timeout(2200)
                    return {"clicked": True, "text": clicked_text, "status": last_status, "strategy": "panel_only_recovery"}
            self.expand_review_bodies(page)
        return {"clicked": False, "status": last_status, "strategy": "panel_only_recovery"}

    def click_more_reviews(self, page: Page) -> Dict[str, Any]:
        self.handle_verification_if_needed(page, "click_more_reviews")
        status_before = self.get_more_reviews_status(page)
        patterns = [r"see\s*more\s*reviews", r"more\s*reviews", r"show\s*more\s*reviews", r"리뷰\s*더보기"]

        try:
            result = page.evaluate(CLICK_MORE_REVIEWS_JS) or {}
        except Exception as exc:
            self.log(f"경고: JS 기반 More reviews 클릭 중 오류가 발생했지만 다음 전략으로 계속합니다. ({exc})")
            result = {}
        if not isinstance(result, dict):
            result = {}
        if result.get("clicked"):
            clicked_text = clean_text(str(result.get("text", ""))) or "More reviews"
            self.log(f"추가 리뷰 클릭: {clicked_text[:120]}")
            page.wait_for_timeout(2400)
            return {"clicked": True, "text": clicked_text, "status_before": status_before, "strategy": "panel_js"}

        self.scroll_reviews_panel_burst(page, steps=3)
        clicked_text = self.click_more_reviews_locator(page)
        if clicked_text:
            self.log(f"추가 리뷰 클릭: {clicked_text[:120]}")
            return {"clicked": True, "text": clicked_text, "status_before": status_before, "strategy": "locator_after_small_scroll"}

        self.nudge_large_scroll_to_reviews(page, attempts=2)
        clicked_text = self.click_more_reviews_locator(page)
        if clicked_text:
            self.log(f"추가 리뷰 클릭: {clicked_text[:120]}")
            return {"clicked": True, "text": clicked_text, "status_before": status_before, "strategy": "locator_after_big_scroll"}

        clicked_text = self.click_by_text_regex(page, patterns, limit=20, wait_ms=1800, prefer_right=True)
        if clicked_text:
            normalized = normalize_text(clicked_text)
            if normalized not in {"by most recent date", "by relevance", "most recent", "most relevant", "sort", "최신순", "관련성순"}:
                self.log(f"추가 리뷰 클릭: {clicked_text[:120]}")
                return {"clicked": True, "text": clicked_text, "status_before": status_before, "strategy": "regex_fallback"}

        items = self.find_click_candidates(page, patterns, require_all=False, limit=20)
        items = sorted(items, key=lambda c: (float(c.get('rect', {}).get('x', 0) or 0) > 700, float(c.get('rect', {}).get('y', 0) or 0)), reverse=True)
        for cand in items:
            text_value = normalize_text(cand.get("text", ""))
            if text_value in {"by most recent date", "by relevance", "most recent", "most relevant", "sort", "최신순", "관련성순"}:
                continue
            if self.click_marked(page, cand["id"]):
                self.log(f"추가 리뷰 클릭: {cand.get('text','')[:120]}")
                page.wait_for_timeout(1800)
                return {"clicked": True, "text": cand.get("text", ""), "status_before": status_before, "strategy": "marked_fallback"}
        return {"clicked": False, "text": "", "status_before": status_before, "strategy": "not_clicked"}

    def wait_for_growth_after_more(self, page: Page) -> int:
        added_total = 0
        for attempt in range(36):
            maybe_checkpoint(self.cfg.control_hook)
            self.handle_verification_if_needed(page, f"after_more_reviews_wait_{attempt}")
            self.scroll_reviews_panel_burst(page, steps=2 if attempt < 18 else 3)
            self.expand_review_bodies(page)
            page.wait_for_timeout(1100 if attempt < 12 else 1500)
            try:
                extracted_after = page.evaluate(EXTRACT_REVIEWS_JS) or []
            except Exception as exc:
                self.log(f"경고: 리뷰 재추출 중 오류가 발생했지만 재시도합니다. ({exc})")
                extracted_after = []
            added_after = self.merge_rows(extracted_after)
            if added_after:
                added_total += added_after
                self.log(f"리뷰 추가 수집: +{added_after}건 / 누적 {len(self.rows)}건")
                if self.cfg.partial_flush:
                    self.cfg.partial_flush(self.rows)
                break
        return added_total

    def harvest_reviews(self, page: Page):
        stagnant = 0
        old_boundary_reached = False
        max_idle_cycles = 180 if self.expected_review_count else 45
        click_without_growth = 0
        auto_stall_resume_count = 0
        missing_more_confirm = 0

        # 리뷰 정렬은 고객 리뷰 화면에 처음 진입했을 때 한 번만 확인합니다.
        # 이후 루프에서는 More reviews를 누르며 순차 추출만 이어갑니다.
        maybe_checkpoint(self.cfg.control_hook)
        self.confirm_sort_before_extract(page)

        while True:
            maybe_checkpoint(self.cfg.control_hook)
            self.expand_review_bodies(page)
            try:
                extracted = page.evaluate(EXTRACT_REVIEWS_JS) or []
            except Exception as exc:
                self.log(f"경고: 리뷰 추출 중 오류가 발생했지만 재시도합니다. ({exc})")
                extracted = []
            added = self.merge_rows(extracted)
            if added:
                stagnant = 0
                click_without_growth = 0
                missing_more_confirm = 0
                self.log(f"리뷰 추가 수집: +{added}건 / 누적 {len(self.rows)}건")
                if self.cfg.partial_flush:
                    self.cfg.partial_flush(self.rows)
            else:
                stagnant += 1

            if self.expected_review_count:
                self.log(f"리뷰 수집 진행: 누적 {len(self.rows)} / 기대 {self.expected_review_count}")

            if self.should_early_stop_on_start_date():
                old_boundary_reached = True
                break

            self.handle_verification_if_needed(page, "harvest_loop")

            pre_more_status = self.get_more_reviews_status(page)
            if not pre_more_status.get("visible"):
                self.nudge_large_scroll_to_reviews(page, attempts=1)

            clicked_more = {"clicked": False, "text": ""}
            try:
                clicked_more = self.click_more_reviews(page)
            except Exception as exc:
                self.log(f"경고: More reviews 클릭 중 오류가 발생했지만 재시도합니다. ({exc})")
            self.handle_verification_if_needed(page, "after_more_reviews_click")

            try:
                expanded_after_more = self.expand_review_bodies(page)
            except Exception as exc:
                self.log(f"경고: 리뷰 본문 확장 중 오류가 발생했지만 재시도합니다. ({exc})")
                expanded_after_more = 0

            try:
                scroll_state = page.evaluate(SCROLL_REVIEWS_JS) or {}
            except Exception as exc:
                self.log(f"경고: 작은 스크롤 이동 중 오류가 발생했지만 재시도합니다. ({exc})")
                scroll_state = {}
            page.wait_for_timeout(900)
            if (not clicked_more.get("clicked")) and (not (scroll_state or {}).get("moved")):
                try:
                    self.nudge_large_scroll_to_reviews(page, attempts=1)
                except Exception as exc:
                    self.log(f"경고: 리뷰 파트 복구용 큰 스크롤 중 오류가 발생했지만 계속합니다. ({exc})")
                page.wait_for_timeout(700)

            added_after = 0
            if clicked_more.get("clicked"):
                added_after = self.wait_for_growth_after_more(page)
                if self.should_early_stop_on_start_date():
                    old_boundary_reached = True
                    break
                if added_after:
                    stagnant = 0
                    click_without_growth = 0
                    auto_stall_resume_count = 0
                    missing_more_confirm = 0
                else:
                    sweep_added = self.sweep_loaded_reviews_in_panel(page, reason="More reviews 클릭 후 무성장 구간 보완", full=False)
                    if sweep_added:
                        added_after += sweep_added
                        stagnant = 0
                        click_without_growth = 0
                        auto_stall_resume_count = 0
                        missing_more_confirm = 0
                    panel_retry = self.panel_only_more_reviews_recovery(page, rounds=12)
                    if panel_retry.get("clicked"):
                        retry_growth = self.wait_for_growth_after_more(page)
                        if retry_growth:
                            added_after += retry_growth
                            stagnant = 0
                            click_without_growth = 0
                            auto_stall_resume_count = 0
                            missing_more_confirm = 0
                    if added_after == 0:
                        for retry in range(12):
                            maybe_checkpoint(self.cfg.control_hook)
                            self.scroll_reviews_panel_burst(page, steps=3)
                            self.expand_review_bodies(page)
                            page.wait_for_timeout(1600)
                            try:
                                extracted_retry = page.evaluate(EXTRACT_REVIEWS_JS) or []
                            except Exception as exc:
                                self.log(f"경고: 리뷰 재추출 중 오류가 발생했지만 계속합니다. ({exc})")
                                extracted_retry = []
                            retry_added = self.merge_rows(extracted_retry)
                            if self.should_early_stop_on_start_date():
                                old_boundary_reached = True
                                break
                            if retry_added:
                                added_after += retry_added
                                stagnant = 0
                                click_without_growth = 0
                                auto_stall_resume_count = 0
                                missing_more_confirm = 0
                                self.log(f"리뷰 추가 수집: +{retry_added}건 / 누적 {len(self.rows)}건")
                                if self.cfg.partial_flush:
                                    self.cfg.partial_flush(self.rows)
                                break
                            if retry in {3, 7, 10}:
                                more_retry = self.panel_only_more_reviews_recovery(page, rounds=4)
                                if more_retry.get("clicked"):
                                    page.wait_for_timeout(1800)
                        if old_boundary_reached:
                            break
                        if added_after == 0:
                            click_without_growth += 1
                            stagnant += 1
            else:
                click_without_growth += 1

            if (not clicked_more.get("clicked")) and expanded_after_more == 0 and (not scroll_state.get("found")):
                stagnant += 1

            more_status_after = self.get_more_reviews_status(page)
            more_button_visible = bool(more_status_after.get("visible"))

            if self.expected_review_count:
                panel_info = more_status_after.get("panelInfo") or {}
                self.log(
                    f"More reviews 상태: visible={more_button_visible} | candidates={more_status_after.get('candidates', 0)} | "
                    f"scrollTop={panel_info.get('scrollTop', '')}"
                )

            if more_button_visible:
                missing_more_confirm = 0
            else:
                recovery = self.exhaust_more_reviews_recovery(page)
                recovered_visible = bool(recovery.get("visible"))
                more_status_after = recovery.get("status") or more_status_after
                more_button_visible = recovered_visible
                if recovered_visible:
                    missing_more_confirm = 0
                else:
                    missing_more_confirm += 1

            if self.expected_review_count and len(self.rows) < self.expected_review_count and more_button_visible:
                if clicked_more.get("clicked") and added_after == 0:
                    self.log("경고: More reviews 버튼 클릭 후 새 리뷰가 붙지 않았습니다. 고객 리뷰 영역 아래로 내려가지 않고, 같은 리뷰 파트 끝의 More reviews를 다시 찾습니다.")
                if click_without_growth >= 90:
                    auto_stall_resume_count += 1
                    self.pause_until_resume(
                        page,
                        "자동 로딩이 멈췄습니다. 브라우저에서 고객 리뷰 영역 아래로 내려가지 말고, 같은 리뷰 파트 끝의 More reviews가 다시 보이게 한 뒤 "
                        "'인증 완료 또는 일시 중단 후 Resume' 버튼을 누르면 직전 지점부터 이어집니다."
                    )
                    stagnant = 0
                    click_without_growth = 0
                    missing_more_confirm = 0
                    if auto_stall_resume_count > 50:
                        raise RuntimeError("More reviews 자동 재개가 과도하게 반복되어 중단합니다.")
                    continue
                else:
                    continue

            if self.should_finish_commanded_extraction(more_button_visible=more_button_visible, missing_more_confirm=missing_more_confirm, old_boundary_reached=(old_boundary_reached and missing_more_confirm >= 3)):
                break
            if self.expected_review_count:
                if (not more_button_visible) and missing_more_confirm >= 5 and stagnant >= 8:
                    self.log("명령에 의한 리뷰추출 종료 조건이 충족되어 현재 상품 크롤링을 종료합니다. (장시간 무성장 + More reviews 미검출)")
                    break
                if stagnant >= max_idle_cycles and missing_more_confirm >= 5:
                    self.log("명령에 의한 리뷰추출 종료 조건이 충족되어 현재 상품 크롤링을 종료합니다. (idle 한계 도달)")
                    break
            else:
                if (not more_button_visible) and missing_more_confirm >= 5 and stagnant >= 8:
                    self.log("명령에 의한 리뷰추출 종료 조건이 충족되어 현재 상품 크롤링을 종료합니다. (장시간 무성장 + More reviews 미검출)")
                    break
                if stagnant >= max_idle_cycles and missing_more_confirm >= 5:
                    self.log("명령에 의한 리뷰추출 종료 조건이 충족되어 현재 상품 크롤링을 종료합니다. (idle 한계 도달)")
                    break

        final_sweep_added = self.sweep_loaded_reviews_in_panel(page, reason="최종 전체 패널 스윕", full=True)
        if self.should_finish_commanded_extraction(
            more_button_visible=bool((self.get_more_reviews_status(page) or {}).get("visible")),
            missing_more_confirm=5,
            old_boundary_reached=old_boundary_reached,
        ):
            self.log("최종 스윕 이후 명령에 의한 리뷰추출 완료로 판단되어 크롤링을 종료합니다.")
        if final_sweep_added:
            self.log(f"최종 전체 패널 스윕으로 추가 확보: +{final_sweep_added}건 / 누적 {len(self.rows)}건")
        self.rows = self.rows_in_final_range()
        for idx, row in enumerate(self.rows, start=1):
            row["No."] = idx
        self.log(f"최종 리뷰 건수: {len(self.rows)}")

    def infer_author_from_raw_text(self, item: Dict[str, Any], current_author: str, current_rating: str) -> str:
        author = clean_text(current_author)
        raw = str(item.get("raw_text", "") or "").replace("\r\n", "\n").replace("\r", "\n")
        lines = [clean_text(line) for line in raw.split("\n") if clean_text(line)]
        date_text = clean_text(str(item.get("date_text", "")))
        source = clean_text(str(item.get("source", "")))
        review_text = clean_multiline_text(str(item.get("review_text", "")))
        if author and not re.fullmatch(r"[1-5](?:\.0)?", author):
            return author
        rating_like = re.compile(r"^(?:[1-5](?:\.0)?|[1-5]\s*[★⭐]+|[★⭐]{1,5}|[1-5]\s*(?:stars?|점))$", re.I)
        domain_like = re.compile(r"\b[a-z0-9.-]+\.[a-z]{2,}\b", re.I)
        for idx, line in enumerate(lines):
            if date_text and clean_text(line) == date_text:
                for j in range(idx - 1, -1, -1):
                    candidate = clean_text(lines[j])
                    if not candidate:
                        continue
                    if candidate == source or candidate == review_text:
                        continue
                    if rating_like.fullmatch(candidate):
                        continue
                    if domain_like.search(candidate):
                        continue
                    if parse_review_date(candidate, self.base_crawl_date):
                        continue
                    if len(candidate) <= 80 and re.search(r"[A-Za-zÀ-ɏᄀ-ᇿ㄰-㆏가-힯]", candidate):
                        return candidate
                break
        return ""

    def merge_rows(self, extracted: List[Dict[str, Any]]) -> int:
        added = 0
        for item in extracted:
            review_date = parse_review_date(str(item.get("date_text", "")), self.base_crawl_date)
            review_date_str = format_date_yyyy_mm_dd(review_date) if review_date else ""
            source = clean_text(str(item.get("source", "")))
            review_text = clean_multiline_text(str(item.get("review_text", "")))
            author = clean_text(str(item.get("author", "")))
            rating = clean_text(str(item.get("rating", "")))
            if re.fullmatch(r"[1-5](?:\.0)?", author) and not rating:
                rating = author
                author = ""
            if not author:
                author = self.infer_author_from_raw_text(item, author, rating)
            if not (review_text or author or source):
                continue
            key = (author, rating, review_date_str, normalize_text(review_text), source)
            if key in self.seen_keys:
                continue
            self.seen_keys.add(key)
            row = {
                "No.": 0,
                "브랜드명": self.cfg.brand,
                "상품명": self.cfg.product_name,
                "리뷰 계정명": author,
                "별점": rating,
                "날짜": review_date_str,
                "리뷰 내용": review_text,
                "출처 웹사이트": source,
            }
            self.rows.append(row)
            added += 1
        return added

    def reached_old_boundary(self) -> bool:
        if not self.cfg.start_date:
            return False
        for row in self.rows:
            raw = row.get("날짜", "")
            if not raw:
                continue
            try:
                d = datetime.strptime(raw, "%Y-%m-%d").date()
            except Exception:
                continue
            if d < self.cfg.start_date:
                return True
        return False

    def should_early_stop_on_start_date(self) -> bool:
        if not self.cfg.start_date:
            return False
        if not self.reached_old_boundary():
            return False
        if not self.start_boundary_logged:
            self.log("시작 날짜보다 오래된 리뷰가 나타나기 시작했습니다. Most recent 순서 기준으로 이 상품 리뷰 수집을 조기 종료합니다.")
            self.start_boundary_logged = True
        return True

    def should_finish_commanded_extraction(self, more_button_visible: bool, missing_more_confirm: int, old_boundary_reached: bool = False) -> bool:
        if old_boundary_reached:
            self.log("명령에 의한 리뷰추출 종료 조건이 충족되어 현재 상품 크롤링을 종료합니다. (시작 날짜 경계 도달)")
            return True
        if self.expected_review_count and len(self.rows) >= self.expected_review_count:
            self.log(f"명령에 의한 리뷰추출 완료로 판단하여 현재 상품 크롤링을 종료합니다. (누적 {len(self.rows)} / 기대 {self.expected_review_count})")
            return True
        if (not more_button_visible) and missing_more_confirm >= 5:
            self.log("명령에 의한 리뷰추출 완료로 판단하여 현재 상품 크롤링을 종료합니다. (More reviews 버튼 미검출 확인)")
            return True
        return False

    def rows_in_final_range(self) -> List[Dict[str, Any]]:
        out = []
        for row in self.rows:
            raw = row.get("날짜", "")
            d: Optional[date] = None
            if raw:
                try:
                    d = datetime.strptime(raw, "%Y-%m-%d").date()
                except Exception:
                    d = None
            if self.cfg.start_date and d and d < self.cfg.start_date:
                continue
            if self.cfg.end_date and d and d > self.cfg.end_date:
                continue
            out.append(row)
        out.sort(key=lambda r: (r.get("날짜", ""), r.get("No.", 0)), reverse=True)
        return out

def sort_rows_for_output(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(row: Dict[str, Any]):
        raw = row.get("날짜", "") or ""
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d")
        except Exception:
            dt = datetime.min
        return dt, row.get("브랜드명", ""), row.get("상품명", ""), row.get("리뷰 계정명", "")

    out = [dict(row) for row in rows]
    out.sort(key=sort_key, reverse=True)
    for idx, row in enumerate(out, start=1):
        row["No."] = idx
    return out


def crawl_google_shopping_reviews(
    brand: str,
    product_name: str,
    start_date_text: str,
    end_date_text: str,
    headless: bool,
    output_dir: Path,
    logger: Logger,
    control_hook: Optional[Callable[[], None]] = None,
    partial_flush: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    verification_hook: Optional[Callable[[str], None]] = None,
) -> List[Dict[str, Any]]:
    brand_clean = clean_text(brand)
    product_input_clean = clean_text(product_name)
    if not brand_clean:
        raise ValueError("브랜드명을 입력해야 합니다.")
    if not product_input_clean:
        raise ValueError("상품명을 입력해야 합니다.")

    product_targets = split_product_inputs(product_name)
    if not product_targets:
        raise ValueError("상품명을 1개 이상 입력해야 합니다.")

    start_date = parse_user_date(start_date_text) if clean_text(start_date_text) else None
    end_date = parse_user_date(end_date_text) if clean_text(end_date_text) else None
    if start_date and end_date and start_date > end_date:
        raise ValueError("리뷰 시작 날짜는 리뷰 끝 날짜보다 늦을 수 없습니다.")

    logger.log(f"브랜드명: {brand_clean}")
    logger.log(f"상품명 입력: {product_input_clean}")
    logger.log(f"상품명 분리 결과: {product_targets}")
    logger.log(f"리뷰 시작 날짜: {start_date.isoformat() if start_date else '(없음)'}")
    logger.log(f"리뷰 끝 날짜: {end_date.isoformat() if end_date else '(오늘까지)'}")
    logger.log(f"브라우저 표시 모드: {'꺼짐(headless)' if headless else '켜짐(headful)'}")

    combined_rows: List[Dict[str, Any]] = []
    combined_seen = set()

    def merge_combined(rows: List[Dict[str, Any]]):
        added = 0
        for row in rows:
            key = (
                row.get("브랜드명", ""),
                row.get("상품명", ""),
                row.get("리뷰 계정명", ""),
                row.get("별점", ""),
                row.get("날짜", ""),
                row.get("리뷰 내용", ""),
                row.get("출처 웹사이트", ""),
            )
            if key in combined_seen:
                continue
            combined_seen.add(key)
            combined_rows.append(dict(row))
            added += 1
        return added

    for idx, product_target in enumerate(product_targets, start=1):
        logger.log(f"===== 상품 {idx}/{len(product_targets)} 시작: {product_target} =====")

        def product_partial(rows: List[Dict[str, Any]]):
            if partial_flush is None:
                return
            preview_rows = combined_rows + [dict(r) for r in rows]
            partial_flush(sort_rows_for_output(preview_rows))

        crawler = GoogleShoppingCrawler(
            CrawlConfig(
                brand=brand_clean,
                product_name=product_target,
                start_date=start_date,
                end_date=end_date,
                headless=headless,
                control_hook=control_hook,
                partial_flush=product_partial if partial_flush else None,
                verification_hook=verification_hook,
            ),
            output_dir=output_dir,
            logger=logger,
        )
        rows = crawler.launch()
        added = merge_combined(rows)
        logger.log(f"===== 상품 {idx}/{len(product_targets)} 완료: {product_target} | 추가 반영 {added}건 / 누적 {len(combined_rows)}건 =====")
        if partial_flush:
            partial_flush(sort_rows_for_output(combined_rows))

    final_rows = sort_rows_for_output(combined_rows)
    write_outputs(output_dir, final_rows, logger)
    return final_rows
