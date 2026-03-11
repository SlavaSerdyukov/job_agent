from __future__ import annotations

import logging
import random
import time
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

from app.config import SEARCH_TIME_RANGE, SEARCH_WORK_TYPES
from app.retry_utils import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)


LINKEDIN_JOBS_API_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
PAGE_SIZE = 25


class LinkedInJobAPI:
    def __init__(
        self,
        *,
        timeout: int = 20,
        min_delay: float = 0.8,
        max_delay: float = 2.5,
    ) -> None:
        self.timeout = timeout
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.linkedin.com/jobs/",
            }
        )

    @staticmethod
    def _extract_job_id(raw_urn: str) -> str:
        if not raw_urn:
            return ""
        # Typical value: urn:li:jobPosting:123456789
        if ":" in raw_urn:
            return raw_urn.split(":")[-1].strip()
        return raw_urn.strip()

    @staticmethod
    def _clean_text(value: str | None) -> str:
        return (value or "").strip()

    @staticmethod
    def _normalize_link(link: str | None, fallback_job_id: str) -> str:
        raw = (link or "").strip()
        if raw:
            normalized = raw.split("?")[0]
            return urljoin("https://www.linkedin.com", normalized)

        if fallback_job_id:
            return f"https://www.linkedin.com/jobs/view/{fallback_job_id}"

        return ""

    def _build_url(self, keyword: str, location: str, start: int) -> str:
        params = {
            "keywords": keyword,
            "location": location,
            "start": start,
            "f_WT": SEARCH_WORK_TYPES,
            "f_TPR": SEARCH_TIME_RANGE,
        }
        return f"{LINKEDIN_JOBS_API_URL}?{urlencode(params)}"

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
    def _fetch_page(self, keyword: str, location: str, start: int) -> str:
        url = self._build_url(keyword, location, start)
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return response.text

    def _parse_cards(self, html: str, location: str) -> list[dict[str, str]]:
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select("div.base-card")

        parsed: list[dict[str, str]] = []
        for card in cards:
            urn = card.get("data-entity-urn", "")
            job_id = self._extract_job_id(urn)
            if not job_id:
                job_id = self._clean_text(card.get("data-job-id"))

            title_node = card.select_one("h3.base-search-card__title") or card.select_one("h3")
            company_node = card.select_one("h4.base-search-card__subtitle") or card.select_one("h4")
            link_node = card.select_one("a.base-card__full-link") or card.select_one("a[href*='/jobs/view/']")

            title = self._clean_text(title_node.get_text(" ", strip=True) if title_node else "")
            company = self._clean_text(company_node.get_text(" ", strip=True) if company_node else "")
            link = self._normalize_link(link_node.get("href") if link_node else "", job_id)

            if not title or not company or not link:
                continue

            parsed.append(
                {
                    "job_id": job_id,
                    "title": title,
                    "company": company,
                    "link": link,
                    "location": location,
                }
            )

        return parsed

    def search_jobs(self, keyword: str, location: str, limit: int = 200) -> list[dict]:
        """Collect job cards using LinkedIn jobs guest API pagination."""
        jobs: list[dict[str, str]] = []
        seen_ids: set[str] = set()

        start = 0
        stagnant_pages = 0

        while len(jobs) < max(1, limit):
            html = self._fetch_page(keyword, location, start)
            parsed = self._parse_cards(html, location)

            if not parsed:
                stagnant_pages += 1
                if stagnant_pages >= 1:
                    break
            else:
                new_in_page = 0
                for job in parsed:
                    job_id = str(job.get("job_id", "") or "").strip()
                    if not job_id:
                        job_id = str(job.get("link", "") or "").strip()
                        job["job_id"] = job_id

                    if not job_id or job_id in seen_ids:
                        continue

                    seen_ids.add(job_id)
                    jobs.append(job)
                    new_in_page += 1

                    if len(jobs) >= limit:
                        break

                if new_in_page == 0:
                    stagnant_pages += 1
                else:
                    stagnant_pages = 0

                if stagnant_pages >= 2:
                    break

            start += PAGE_SIZE
            if len(jobs) >= limit:
                break

            # Lightweight anti-blocking delay.
            time.sleep(random.uniform(self.min_delay, self.max_delay))

        logger.info(
            "metric=api_jobs_scraped keyword=%s location=%s count=%s",
            keyword,
            location,
            len(jobs),
        )
        return jobs
