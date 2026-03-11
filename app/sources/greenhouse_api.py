from __future__ import annotations

import asyncio
import logging
import random
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

try:
    import aiohttp
except Exception:  # noqa: BLE001
    aiohttp = None  # type: ignore[assignment]

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


GREENHOUSE_BOARD_URL = "https://boards.greenhouse.io/{company}/jobs"
GREENHOUSE_COMPANIES = [
    "stripe",
    "airbnb",
    "coinbase",
    "lyft",
    "reddit",
    "discord",
    "doordash",
    "datadog",
    "robinhood",
    "figma",
    "instacart",
    "plaid",
]
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
TIMEOUT = 25
MAX_COMPANY_WORKERS = 8


def _company_label(slug: str) -> str:
    return slug.replace("-", " ").strip().title()


def _matches_keywords(text: str, keywords: list[str]) -> bool:
    haystack = text.lower()
    baseline = ["python", "backend", "api", "django", "fastapi"]
    dynamic = [keyword.lower() for keyword in keywords if keyword]
    return any(token in haystack for token in baseline + dynamic)


def _matches_location(job_location: str, locations: list[str]) -> bool:
    if not locations:
        return True
    lowered = job_location.lower()
    if "remote" in lowered:
        return True
    return any(location.lower() in lowered for location in locations)


def _extract_job_id(link: str) -> str:
    match = re.search(r"/jobs/(\d+)", link)
    if match:
        return match.group(1)
    return link.rstrip("/").split("/")[-1]


def _parse_openings(
    html: str,
    company_slug: str,
    keywords: list[str],
    locations: list[str],
) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    jobs: list[dict[str, str]] = []
    seen_keys: set[str] = set()

    opening_nodes = soup.select("div.opening")
    if not opening_nodes:
        opening_nodes = soup.select("section#jobs li")

    for node in opening_nodes:
        link_node = node.select_one("a[href]")
        if not link_node:
            continue

        href = str(link_node.get("href", "") or "").strip()
        if not href or "/jobs/" not in href:
            continue

        title = link_node.get_text(" ", strip=True)
        location_node = node.select_one("span.location, div.location")
        job_location = location_node.get_text(" ", strip=True) if location_node else "Unknown"
        link = urljoin(f"https://boards.greenhouse.io/{company_slug}/", href.split("?")[0])
        job_id = _extract_job_id(link)

        if not title or not link:
            continue

        searchable = f"{title} {company_slug}".lower()
        if not _matches_keywords(searchable, keywords):
            continue
        if not _matches_location(job_location, locations):
            continue

        key = f"{job_id}:{link}"
        if key in seen_keys:
            continue
        seen_keys.add(key)

        jobs.append(
            {
                "job_id": job_id,
                "title": title,
                "company": _company_label(company_slug),
                "location": job_location,
                "description": "",
                "link": link,
                "source": "greenhouse",
            }
        )

    if jobs:
        return jobs

    for link_node in soup.select("a[href*='/jobs/']"):
        href = str(link_node.get("href", "") or "").strip()
        if not href:
            continue

        title = link_node.get_text(" ", strip=True)
        if not title:
            continue

        link = urljoin(f"https://boards.greenhouse.io/{company_slug}/", href.split("?")[0])
        job_id = _extract_job_id(link)
        if not _matches_keywords(title, keywords):
            continue

        key = f"{job_id}:{link}"
        if key in seen_keys:
            continue
        seen_keys.add(key)

        jobs.append(
            {
                "job_id": job_id,
                "title": title,
                "company": _company_label(company_slug),
                "location": "Unknown",
                "description": "",
                "link": link,
                "source": "greenhouse",
            }
        )

    return jobs


def _dedupe(jobs: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    deduped: list[dict[str, str]] = []
    for job in jobs:
        key = str(job.get("job_id", "") or job.get("link", "")).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(job)
    return deduped


def _fetch_company_sync(company_slug: str) -> str:
    url = GREENHOUSE_BOARD_URL.format(company=company_slug)
    response = requests.get(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.text


def fetch_jobs(keywords: list[str], locations: list[str]) -> list[dict]:
    collected: list[dict[str, str]] = []
    max_workers = min(MAX_COMPANY_WORKERS, len(GREENHOUSE_COMPANIES)) or 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_fetch_company_sync, company_slug): company_slug
            for company_slug in GREENHOUSE_COMPANIES
        }

        for future in as_completed(futures):
            company_slug = futures[future]
            try:
                html = future.result()
            except Exception as exc:  # noqa: BLE001
                logger.debug("Greenhouse fetch failed company=%s error=%s", company_slug, exc)
                continue
            collected.extend(_parse_openings(html, company_slug, keywords, locations))

    deduped = _dedupe(collected)
    logger.info("metric=source_jobs source=greenhouse count=%s", len(deduped))
    return deduped


async def _fetch_company_async(session, semaphore: asyncio.Semaphore, company_slug: str) -> tuple[str, str]:
    url = GREENHOUSE_BOARD_URL.format(company=company_slug)
    async with semaphore:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as response:
            response.raise_for_status()
            html = await response.text()
    await asyncio.sleep(random.uniform(0.5, 2.0))
    return company_slug, html


async def async_fetch_jobs(keywords: list[str], locations: list[str], session, semaphore: asyncio.Semaphore) -> list[dict]:
    if aiohttp is None:
        return await asyncio.to_thread(fetch_jobs, keywords, locations)

    tasks = [
        asyncio.create_task(_fetch_company_async(session, semaphore, company_slug))
        for company_slug in GREENHOUSE_COMPANIES
    ]

    collected: list[dict[str, str]] = []
    for task in asyncio.as_completed(tasks):
        try:
            company_slug, html = await task
        except Exception as exc:  # noqa: BLE001
            logger.debug("Greenhouse async fetch failed: %s", exc)
            continue
        collected.extend(_parse_openings(html, company_slug, keywords, locations))

    deduped = _dedupe(collected)
    logger.info("metric=source_jobs source=greenhouse count=%s", len(deduped))
    return deduped
