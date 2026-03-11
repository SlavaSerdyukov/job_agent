from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from urllib.parse import urlencode, urljoin

try:
    import aiohttp
except Exception:  # noqa: BLE001
    aiohttp = None  # type: ignore[assignment]

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


INDEED_SEARCH_URL = "https://www.indeed.com/jobs"
PAGE_STEP = 10
MAX_PAGES = 20
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _extract_card(card, default_location: str, keyword: str) -> dict[str, str] | None:
    title_node = card.select_one("h2.jobTitle") or card.select_one("h2")
    company_node = card.select_one("span[data-testid='company-name']") or card.select_one("span.companyName")
    location_node = card.select_one("div[data-testid='text-location']") or card.select_one("div.companyLocation")
    link_node = card.select_one("a.jcs-JobTitle") or card.select_one("a")

    title = title_node.get_text(" ", strip=True) if title_node else ""
    company = company_node.get_text(" ", strip=True) if company_node else ""
    location = location_node.get_text(" ", strip=True) if location_node else default_location
    link = ""
    if link_node and link_node.get("href"):
        link = urljoin("https://www.indeed.com", link_node.get("href").split("?")[0])

    job_id = str(card.get("data-jk", "") or card.get("id", "")).strip()
    description_node = card.select_one("div.job-snippet")
    description = description_node.get_text(" ", strip=True) if description_node else ""

    if not title or not company or not link:
        return None

    return {
        "job_id": job_id or link,
        "title": title,
        "company": company,
        "location": location,
        "link": link,
        "description": description,
        "source": "indeed",
        "tags": "indeed",
        "keyword": keyword,
        "searched_at": datetime.now(timezone.utc).isoformat(),
    }


def _dedupe(jobs: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for job in jobs:
        key = str(job.get("job_id", "") or job.get("link", "")).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(job)
    return out


def _fetch_jobs_sync(keywords: list[str], locations: list[str]) -> list[dict]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.indeed.com/",
        }
    )

    jobs: list[dict[str, str]] = []

    for keyword in keywords:
        for location in locations:
            for start in range(0, MAX_PAGES * PAGE_STEP, PAGE_STEP):
                query = urlencode({"q": keyword, "l": location, "start": start})
                url = f"{INDEED_SEARCH_URL}?{query}"

                try:
                    response = session.get(url, timeout=25)
                    response.raise_for_status()
                    html = response.text
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "Indeed request failed keyword=%s location=%s start=%s error=%s",
                        keyword,
                        location,
                        start,
                        exc,
                    )
                    break

                soup = BeautifulSoup(html, "lxml")
                cards = soup.select("a.tapItem")
                if not cards:
                    break

                new_in_page = 0
                for card in cards:
                    parsed = _extract_card(card, location, keyword)
                    if not parsed:
                        continue
                    jobs.append(parsed)
                    new_in_page += 1

                if new_in_page == 0:
                    break

    deduped = _dedupe(jobs)
    logger.info("metric=source_jobs source=indeed count=%s", len(deduped))
    return deduped


async def _fetch_html(session, semaphore: asyncio.Semaphore, keyword: str, location: str, start: int) -> str:
    query = urlencode({"q": keyword, "l": location, "start": start})
    url = f"{INDEED_SEARCH_URL}?{query}"

    async with semaphore:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as response:
            response.raise_for_status()
            html = await response.text()

    await asyncio.sleep(random.uniform(0.5, 2.0))
    return html


async def _async_pair(session, semaphore: asyncio.Semaphore, keyword: str, location: str) -> list[dict[str, str]]:
    pair_jobs: list[dict[str, str]] = []

    for start in range(0, MAX_PAGES * PAGE_STEP, PAGE_STEP):
        try:
            html = await _fetch_html(session, semaphore, keyword, location, start)
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "Indeed request failed keyword=%s location=%s start=%s error=%s",
                keyword,
                location,
                start,
                exc,
            )
            break

        soup = BeautifulSoup(html, "lxml")
        cards = soup.select("a.tapItem")
        if not cards:
            break

        new_in_page = 0
        for card in cards:
            parsed = _extract_card(card, location, keyword)
            if not parsed:
                continue
            pair_jobs.append(parsed)
            new_in_page += 1

        if new_in_page == 0:
            break

    return pair_jobs


async def async_fetch_jobs(keywords: list[str], locations: list[str], session, semaphore: asyncio.Semaphore) -> list[dict]:
    if aiohttp is None:
        return await asyncio.to_thread(_fetch_jobs_sync, keywords, locations)

    tasks = [
        asyncio.create_task(_async_pair(session, semaphore, keyword, location))
        for keyword in keywords
        for location in locations
    ]

    jobs: list[dict[str, str]] = []
    for task in asyncio.as_completed(tasks):
        try:
            jobs.extend(await task)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Indeed async task failed: %s", exc)

    deduped = _dedupe(jobs)
    logger.info("metric=source_jobs source=indeed count=%s", len(deduped))
    return deduped


def fetch_jobs(keywords: list[str], locations: list[str]) -> list[dict]:
    if aiohttp is None:
        return _fetch_jobs_sync(keywords, locations)

    async def _run() -> list[dict]:
        timeout = aiohttp.ClientTimeout(total=25)
        connector = aiohttp.TCPConnector(limit=100, ssl=False)
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.indeed.com/",
            },
        ) as session:
            semaphore = asyncio.Semaphore(20)
            return await async_fetch_jobs(keywords, locations, session, semaphore)

    try:
        return asyncio.run(_run())
    except RuntimeError:
        return _fetch_jobs_sync(keywords, locations)
