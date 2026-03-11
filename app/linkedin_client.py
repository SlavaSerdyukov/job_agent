from __future__ import annotations

import logging
from urllib.parse import urlencode

from playwright.sync_api import Browser, BrowserContext, ElementHandle, Page, Playwright, TimeoutError, sync_playwright

from app.config import (
    LINKEDIN_EMAIL,
    LINKEDIN_JOBS_SEARCH_URL,
    LINKEDIN_LOGIN_URL,
    LINKEDIN_PASSWORD,
    SEARCH_TIME_RANGE,
    SEARCH_WORK_TYPES,
)

logger = logging.getLogger(__name__)


class LinkedInClient:
    """Playwright-based LinkedIn client."""

    def __init__(self, *, headless: bool = True) -> None:
        self.headless = headless
        self._pw: Playwright | None = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None

    def __enter__(self) -> "LinkedInClient":
        self._pw = sync_playwright().start()
        self.browser = self._pw.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context()
        self.page = self.context.new_page()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self._pw:
            self._pw.stop()

    @property
    def _page(self) -> Page:
        if not self.page:
            raise RuntimeError("LinkedInClient is not initialized. Use context manager.")
        return self.page

    def login(self) -> None:
        if not LINKEDIN_EMAIL or not LINKEDIN_PASSWORD:
            raise ValueError("Missing LinkedIn credentials in .env")

        page = self._page
        logger.info("Opening LinkedIn login page")
        page.goto(LINKEDIN_LOGIN_URL, wait_until="domcontentloaded")
        page.fill("#username", LINKEDIN_EMAIL)
        page.fill("#password", LINKEDIN_PASSWORD)
        page.click("button[type='submit']")

        try:
            page.wait_for_url("**/feed/**", timeout=20000)
        except TimeoutError as exc:
            current = page.url
            if "checkpoint" in current or "challenge" in current:
                raise RuntimeError(
                    "LinkedIn login needs manual verification (checkpoint/challenge)."
                ) from exc
            # Jobs pages can still open without feed redirect in some sessions.
            logger.warning("Feed redirect not detected, continuing with current URL: %s", current)

    def search(self, keyword: str, location: str) -> list[ElementHandle]:
        page = self._page

        params = {
            "keywords": keyword,
            "location": location,
            "f_WT": SEARCH_WORK_TYPES,
            "f_TPR": SEARCH_TIME_RANGE,
            "sortBy": "DD",
        }

        url = f"{LINKEDIN_JOBS_SEARCH_URL}?{urlencode(params)}"
        logger.info("Searching jobs: keyword=%s, location=%s", keyword, location)

        page.goto(url, wait_until="domcontentloaded")

        page.mouse.wheel(0, 5000)
        page.wait_for_timeout(2000)

        cards = page.query_selector_all("[data-job-id]")

        logger.info("Cards found: %s", len(cards))

        return cards
