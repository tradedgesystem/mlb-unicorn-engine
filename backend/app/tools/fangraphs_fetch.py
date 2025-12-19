"""Fetch FanGraphs HTML using a real browser engine.

FanGraphs blocks requests-based scraping (Cloudflare/WAF 403). A headless browser
renders the page like a real user session, which is required to retrieve HTML reliably.
"""

from __future__ import annotations

from typing import Optional


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
            page.goto(url, wait_until="networkidle", timeout=timeout)
            page.wait_for_selector("table", timeout=timeout)
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


if __name__ == "__main__":
    html = fetch_fangraphs_html("https://www.fangraphs.com/leaders-legacy.aspx")
    print(len(html))
