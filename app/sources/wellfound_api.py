from __future__ import annotations

import asyncio
import logging
import random
import time
from datetime import datetime, timezone
from urllib.parse import quote_plus, urljoin

try:
    import aiohttp
except Exception:  # noqa: BLE001
    aiohttp = None  # type: ignore[assignment]

import requests
from bs4 import BeautifulSoup

from app.retry_utils import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)


WELLFOUND_JOBS_URL = "https://wellfound.com/jobs"
MAX_PAGES = 8


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def _fetch_html(url: str) -> str:
    response = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout=25,
    )
    response.raise_for_status()
    return response.text


def _extract_jobs(soup: BeautifulSoup, keyword: str, location: str) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []

    selectors = [
        "div[data-test='StartupResult']",
        "div[data-test='JobCard']",
        "article",
    ]
    cards = []
    for selector in selectors:
        cards = soup.select(selector)
        if cards:
            break

    for card in cards:
        link_node = card.select_one("a[href*='/jobs/']")
        title_node = card.select_one("a[href*='/jobs/']")
        company_node = card.select_one("a[href*='/company/'], div[data-test='StartupName'], span[data-test='startup-name']")
        location_node = card.select_one("div[data-test='JobLocation'], span[data-test='job-location']")

        link = ""
        if link_node and link_node.get("href"):
            link = urljoin("https://wellfound.com", link_node.get("href").split("?")[0])

        title = title_node.get_text(" ", strip=True) if title_node else ""
        company = company_node.get_text(" ", strip=True) if company_node else ""
        job_location = location_node.get_text(" ", strip=True) if location_node else location

        description_node = card.select_one("p")
        description = description_node.get_text(" ", strip=True) if description_node else ""

        if not title or not company or not link:
            continue

        job_id = link.rstrip("/").split("/")[-1]
        jobs.append(
            {
                "job_id": job_id,
                "title": title,
                "company": company,
                "location": job_location,
                "link": link,
                "description": description,
                "source": "wellfound",
                "tags": "wellfound,startup",
                "keyword": keyword,
                "searched_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    return jobs


def _fetch_jobs_sync(keywords: list[str], locations: list[str]) -> list[dict]:
    all_jobs: list[dict[str, str]] = []

    for keyword in keywords:
        normalized_keyword = keyword if keyword else "python backend"

        for location in locations or ["Remote"]:
            for page in range(1, MAX_PAGES + 1):
                q = quote_plus(normalized_keyword)
                loc = quote_plus(location)
                url = f"{WELLFOUND_JOBS_URL}?query={q}&location={loc}&page={page}"

                try:
                    html = _fetch_html(url)
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "Wellfound fetch failed keyword=%s location=%s page=%s error=%s",
                        keyword,
                        location,
                        page,
                        exc,
                    )
                    break

                soup = BeautifulSoup(html, "lxml")
                parsed = _extract_jobs(soup, normalized_keyword, location)
                if not parsed:
                    break

                all_jobs.extend(parsed)
                time.sleep(random.uniform(0.5, 2.0))

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for job in all_jobs:
        key = str(job.get("job_id", "") or job.get("link", ""))
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(job)

    logger.info("metric=source_jobs source=wellfound count=%s", len(deduped))
    return deduped


async def async_fetch_jobs(keywords: list[str], locations: list[str], session, semaphore: asyncio.Semaphore) -> list[dict]:
    # Wellfound often uses dynamic anti-bot protections; keep robust thread fallback.
    return await asyncio.to_thread(_fetch_jobs_sync, keywords, locations)


def fetch_jobs(keywords: list[str], locations: list[str]) -> list[dict]:
    return _fetch_jobs_sync(keywords, locations)
