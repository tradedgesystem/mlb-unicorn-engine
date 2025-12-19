"""Fetch and parse FanGraphs HTML using a real browser engine.

FanGraphs blocks requests-based scraping (Cloudflare/WAF 403). A headless browser
renders the page like a real user session, which is required to retrieve HTML reliably.
A small disk cache reduces repeated browser launches while keeping HTML fresh.
"""

from __future__ import annotations

import hashlib
import re
import time
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
from bs4 import BeautifulSoup

DEFAULT_LEADERBOARD_URL = "https://www.fangraphs.com/leaders-legacy.aspx"
DEFAULT_CACHE_TTL = 60 * 60
CACHE_DIR = Path(".cache/fangraphs")


def fetch_fangraphs_html(url: str, timeout: int = 30000, headless: bool = True) -> str:
    """Return full HTML for a FanGraphs page using a headless browser.

    Args:
        url: Target FanGraphs URL.
        timeout: Navigation timeout in milliseconds.
        headless: Whether to run the browser headless (default True).

    Raises:
        RuntimeError: If navigation fails, times out, or returns empty HTML.
    """
    if not url:
        raise RuntimeError("FanGraphs fetch failed: url is required")

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception:
        return _fetch_with_selenium(url, timeout=timeout, headless=headless)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            page = browser.new_page()
            page.set_extra_http_headers(
                {
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0 Safari/537.36"
                    ),
                    "Accept-Language": "en-US,en;q=0.9",
                }
            )
            try:
                page.goto(url, wait_until="networkidle", timeout=timeout)
            except PlaywrightTimeoutError:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            page.wait_for_selector("table", timeout=timeout, state="attached")
            html = page.content()
            browser.close()
    except PlaywrightTimeoutError as exc:
        raise RuntimeError(f"FanGraphs fetch failed: timeout after {timeout}ms") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"FanGraphs fetch failed: {exc}") from exc

    if not html or not html.strip():
        raise RuntimeError("FanGraphs fetch failed: empty HTML")
    return html


def _fetch_with_selenium(url: str, timeout: int, headless: bool) -> str:
    try:
        from selenium import webdriver
        from selenium.common.exceptions import TimeoutException as SeleniumTimeout
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "FanGraphs fetch failed: Playwright unavailable and Selenium not installed"
        ) from exc

    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = None
    try:
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(timeout / 1000)
        driver.get(url)
        WebDriverWait(driver, timeout / 1000).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        html = driver.page_source
    except SeleniumTimeout as exc:
        raise RuntimeError(f"FanGraphs fetch failed: timeout after {timeout}ms") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"FanGraphs fetch failed: {exc}") from exc
    finally:
        if driver is not None:
            driver.quit()

    if not html or not html.strip():
        raise RuntimeError("FanGraphs fetch failed: empty HTML")
    return html


def _build_url(base_url: str, params: dict | None) -> str:
    if not params:
        return base_url

    parsed = urlparse(base_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in params.items():
        if value is None:
            continue
        query[key] = value
    encoded = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=encoded))


def _cache_path(url: str, cache_dir: Path) -> Path:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return cache_dir / f"{digest}.html"


def _read_cache(cache_path: Path, cache_ttl: Optional[int]) -> Optional[str]:
    if cache_ttl is None or cache_ttl <= 0:
        return None
    if not cache_path.exists():
        return None
    age = time.time() - cache_path.stat().st_mtime
    if age > cache_ttl:
        return None
    html = cache_path.read_text(encoding="utf-8")
    if not html.strip():
        return None
    return html


def _write_cache(cache_path: Path, html: str) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(html, encoding="utf-8")


def _has_name_header(headers: list[str]) -> bool:
    normalized = [_normalize_column_name(header, "") for header in headers]
    for header in normalized:
        if header == "name" or header.startswith("name_"):
            return True
        if header == "player" or header.startswith("player_"):
            return True
    return False


def _find_leaderboard_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    if not tables:
        return None, []

    for table in tables:
        headers = _extract_headers(table)
        if not headers:
            continue
        if _has_name_header(headers):
            return table, headers

    return None, []


def _extract_headers(table) -> list[str]:
    header_rows = []
    thead = table.find("thead")
    if thead:
        header_rows = thead.find_all("tr")
    if not header_rows:
        header_rows = table.find_all("tr", limit=2)

    best = []
    for row in header_rows:
        cells = row.find_all(["th", "td"])
        values = [cell.get_text(strip=True) for cell in cells]
        if len(values) > len(best):
            best = values
    return best


def _extract_rows(table) -> list:
    tbody = table.find("tbody")
    if tbody:
        return tbody.find_all("tr")
    return table.find_all("tr")


def _normalize_column_name(name: str, fallback: str) -> str:
    cleaned = name.strip()
    if not cleaned:
        return fallback
    cleaned = cleaned.replace("%", " pct ")
    cleaned = cleaned.replace("+", " plus ")
    cleaned = cleaned.replace("/", " per ")
    cleaned = cleaned.replace("#", " num ")
    cleaned = re.sub(r"[^\w\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", "_", cleaned.strip().lower())
    return cleaned or fallback


def _normalize_headers(headers: list[str]) -> list[str]:
    normalized = [
        _normalize_column_name(header, f"col_{idx}")
        for idx, header in enumerate(headers, start=1)
    ]
    return _dedupe_headers(normalized)


def _dedupe_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    deduped = []
    for header in headers:
        count = seen.get(header, 0)
        deduped.append(f"{header}_{count}" if count else header)
        seen[header] = count + 1
    return deduped


def _extend_headers(headers: list[str], target_len: int) -> list[str]:
    if len(headers) >= target_len:
        return headers
    extended = list(headers)
    start = len(headers) + 1
    for idx in range(start, target_len + 1):
        extended.append(f"extra_col_{idx}")
    return _dedupe_headers(extended)


def _coerce_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        series = df[column]
        if series.dtype != object:
            continue
        cleaned = (
            series.astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.replace("—", "", regex=False)
            .str.replace("--", "", regex=False)
            .str.strip()
        )
        cleaned = cleaned.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        numeric = pd.to_numeric(cleaned, errors="coerce")
        non_empty = cleaned.notna().sum()
        if non_empty == 0:
            continue
        if numeric.notna().sum() >= max(1, non_empty // 2):
            df[column] = numeric
    return df


def validate_fangraphs_html(html: str) -> None:
    """Validate that the FanGraphs HTML includes the leaderboard table."""
    soup = BeautifulSoup(html, "html.parser")
    table, headers = _find_leaderboard_table(soup)
    if table is None:
        raise RuntimeError("FanGraphs HTML validation failed: leaderboard table not found")
    if not headers:
        raise RuntimeError("FanGraphs HTML validation failed: leaderboard headers missing")
    if not _has_name_header(headers):
        raise RuntimeError(
            "FanGraphs HTML validation failed: expected 'Name' header missing"
        )
    rows = [
        row for row in _extract_rows(table) if row.find_all("td")
    ]
    if not rows:
        raise RuntimeError(
            "FanGraphs HTML validation failed: leaderboard table has no data rows"
        )


def parse_fangraphs_leaderboard(html: str) -> pd.DataFrame:
    """Parse FanGraphs leaderboard HTML into a DataFrame."""
    soup = BeautifulSoup(html, "html.parser")
    table, headers = _find_leaderboard_table(soup)
    if table is None:
        raise RuntimeError("FanGraphs parse failed: leaderboard table not found")
    if not headers:
        raise RuntimeError("FanGraphs parse failed: leaderboard headers missing")

    normalized_headers = _normalize_headers(headers)
    rows = []
    for row in _extract_rows(table):
        cells = row.find_all(["td", "th"])
        if not cells or (row.find("th") and not row.find("td")):
            continue
        values = [cell.get_text(strip=True) for cell in cells]
        if not values:
            continue
        if len(values) > len(normalized_headers):
            normalized_headers = _extend_headers(normalized_headers, len(values))
        if len(values) < len(normalized_headers):
            values.extend([None] * (len(normalized_headers) - len(values)))
        rows.append(values)

    if not rows:
        raise RuntimeError("FanGraphs parse failed: no leaderboard rows found")

    df = pd.DataFrame(rows, columns=normalized_headers)
    return _coerce_numeric_columns(df)


def _get_cached_or_fetch(
    url: str,
    *,
    cache_dir: Path,
    cache_ttl: Optional[int],
    timeout: int,
    headless: bool,
) -> str:
    cache_path = _cache_path(url, cache_dir)
    cached = _read_cache(cache_path, cache_ttl)
    if cached is not None:
        return cached

    html = fetch_fangraphs_html(url, timeout=timeout, headless=headless)
    _write_cache(cache_path, html)
    return html


class FangraphsClient:
    """FanGraphs leaderboard client with caching and parsing.

    A headless browser is required because FanGraphs blocks requests-based scraping.
    A disk cache avoids repeated browser sessions while keeping HTML fresh.
    """

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_LEADERBOARD_URL,
        cache_ttl: int = DEFAULT_CACHE_TTL,
        cache_dir: Path | str = CACHE_DIR,
        headless: bool = True,
        timeout: int = 30000,
    ) -> None:
        self.base_url = base_url
        self.cache_ttl = cache_ttl
        self.cache_dir = Path(cache_dir)
        self.headless = headless
        self.timeout = timeout

    def get_leaderboard(self, params: dict | None = None) -> pd.DataFrame:
        url = _build_url(self.base_url, params)
        html = _get_cached_or_fetch(
            url,
            cache_dir=self.cache_dir,
            cache_ttl=self.cache_ttl,
            timeout=self.timeout,
            headless=self.headless,
        )
        validate_fangraphs_html(html)
        return parse_fangraphs_leaderboard(html)


if __name__ == "__main__":
    client = FangraphsClient()
    df = client.get_leaderboard()
    print(df.head())
