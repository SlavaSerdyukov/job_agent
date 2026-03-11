from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from urllib.parse import urljoin

try:
    import aiohttp
except Exception:  # noqa: BLE001
    aiohttp = None  # type: ignore[assignment]

from bs4 import BeautifulSoup

from app.config import HEADLESS, LINKEDIN_EMAIL, LINKEDIN_PASSWORD, SEARCH_MAX_JOBS_PER_QUERY, SEARCH_TIME_RANGE, SEARCH_WORK_TYPES
from app.job_collector import parse_cards
from app.linkedin_api import LinkedInJobAPI
from app.linkedin_client import LinkedInClient

logger = logging.getLogger(__name__)


LINKEDIN_JOBS_API_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
PAGE_SIZE = 25
DEFAULT_LIMIT_PER_KEYWORD = 400


def _extract_job_id(raw_urn: str) -> str:
    if not raw_urn:
        return ""
    if ":" in raw_urn:
        return raw_urn.split(":")[-1].strip()
    return raw_urn.strip()


def _parse_cards_html(html: str, keyword: str, location: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("div.base-card")

    parsed: list[dict[str, str]] = []
    for card in cards:
        urn = str(card.get("data-entity-urn", "") or "").strip()
        job_id = _extract_job_id(urn)

        title_node = card.select_one("h3.base-search-card__title") or card.select_one("h3")
        company_node = card.select_one("h4.base-search-card__subtitle") or card.select_one("h4")
        location_node = card.select_one("span.job-search-card__location")
        link_node = card.select_one("a.base-card__full-link") or card.select_one("a[href*='/jobs/view/']")

        title = title_node.get_text(" ", strip=True) if title_node else ""
        company = company_node.get_text(" ", strip=True) if company_node else ""
        card_location = location_node.get_text(" ", strip=True) if location_node else location

        link = ""
        if link_node and link_node.get("href"):
            link = urljoin("https://www.linkedin.com", str(link_node.get("href", "")).split("?")[0])

        if not job_id and link:
            job_id = link.rstrip("/").split("/")[-1]

        if not title or not company or not link:
            continue

        parsed.append(
            {
                "job_id": job_id or link,
                "title": title,
                "company": company,
                "location": card_location,
                "link": link,
                "description": "",
                "source": "linkedin",
                "tags": "linkedin",
                "keyword": keyword,
                "searched_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    return parsed


def _dedupe_jobs(jobs: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    out: list[dict[str, str]] = []

    for job in jobs:
        key = str(job.get("job_id", "") or job.get("link", "")).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(job)

    return out


def _fetch_via_requests_api(keywords: list[str], locations: list[str], *, limit: int) -> tuple[list[dict[str, str]], list[tuple[str, str]]]:
    api = LinkedInJobAPI()
    jobs: list[dict[str, str]] = []
    failed_pairs: list[tuple[str, str]] = []

    for keyword in keywords:
        for location in locations:
            try:
                result = api.search_jobs(keyword=keyword, location=location, limit=limit)
            except Exception as exc:  # noqa: BLE001
                logger.warning("LinkedIn requests API failed keyword=%s location=%s error=%s", keyword, location, exc)
                failed_pairs.append((keyword, location))
                continue

            for item in result:
                jobs.append(
                    {
                        "job_id": str(item.get("job_id", "") or item.get("link", "") or "").strip(),
                        "title": str(item.get("title", "") or "").strip(),
                        "company": str(item.get("company", "") or "").strip(),
                        "location": str(item.get("location", "") or location).strip(),
                        "link": str(item.get("link", "") or "").strip(),
                        "description": str(item.get("description", "") or item.get("text", "") or "").strip(),
                        "source": "linkedin",
                        "tags": "linkedin",
                        "keyword": keyword,
                        "searched_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

    return jobs, failed_pairs


def _fetch_via_playwright(pairs: list[tuple[str, str]], *, limit: int) -> list[dict[str, str]]:
    if not pairs:
        return []

    if not LINKEDIN_EMAIL or not LINKEDIN_PASSWORD:
        logger.warning("Skipping LinkedIn Playwright fallback: missing credentials")
        return []

    fallback_jobs: list[dict[str, str]] = []
    try:
        with LinkedInClient(headless=HEADLESS) as client:
            try:
                client.login()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Skipping LinkedIn Playwright fallback: login failed: %s", exc)
                return []

            for keyword, location in pairs:
                try:
                    cards = client.search(keyword=keyword, location=location)
                    parsed = parse_cards(cards, location)
                    for item in parsed[:limit]:
                        fallback_jobs.append(
                            {
                                "job_id": str(item.get("job_id", "") or item.get("link", "") or "").strip(),
                                "title": str(item.get("title", "") or "").strip(),
                                "company": str(item.get("company", "") or "").strip(),
                                "location": str(item.get("location", "") or location).strip(),
                                "link": str(item.get("link", "") or "").strip(),
                                "description": str(item.get("description", "") or item.get("text", "") or "").strip(),
                                "source": "linkedin",
                                "tags": "linkedin",
                                "keyword": keyword,
                                "searched_at": datetime.now(timezone.utc).isoformat(),
                            }
                        )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "LinkedIn Playwright fallback failed keyword=%s location=%s error=%s",
                        keyword,
                        location,
                        exc,
                    )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Skipping LinkedIn Playwright fallback: browser start failed: %s", exc)
        return []

    return _dedupe_jobs(fallback_jobs)


async def _fetch_page(session, semaphore: asyncio.Semaphore, keyword: str, location: str, start: int) -> str:
    params = {
        "keywords": keyword,
        "location": location,
        "start": start,
        "f_WT": SEARCH_WORK_TYPES,
        "f_TPR": SEARCH_TIME_RANGE,
    }

    async with semaphore:
        async with session.get(LINKEDIN_JOBS_API_URL, params=params, timeout=aiohttp.ClientTimeout(total=25)) as response:
            response.raise_for_status()
            html = await response.text()

    await asyncio.sleep(random.uniform(0.5, 2.0))
    return html


async def _async_pair_collect(session, semaphore: asyncio.Semaphore, keyword: str, location: str, *, limit_per_keyword: int) -> tuple[list[dict[str, str]], bool]:
    starts = list(range(0, max(limit_per_keyword, PAGE_SIZE), PAGE_SIZE))
    tasks = [
        asyncio.create_task(_fetch_page(session, semaphore, keyword, location, start))
        for start in starts
    ]

    pair_jobs: list[dict[str, str]] = []
    had_failure = False

    for task in asyncio.as_completed(tasks):
        try:
            html = await task
        except Exception as exc:  # noqa: BLE001
            had_failure = True
            logger.debug("LinkedIn page fetch failed keyword=%s location=%s error=%s", keyword, location, exc)
            continue

        parsed = _parse_cards_html(html, keyword, location)
        if not parsed:
            continue

        pair_jobs.extend(parsed)
        if len(pair_jobs) >= limit_per_keyword:
            break

    return _dedupe_jobs(pair_jobs)[:limit_per_keyword], had_failure


async def async_fetch_jobs(keywords: list[str], locations: list[str], session, semaphore: asyncio.Semaphore) -> list[dict]:
    if aiohttp is None:
        return await asyncio.to_thread(fetch_jobs, keywords, locations)

    limit_per_keyword = max(1, min(400, max(DEFAULT_LIMIT_PER_KEYWORD, SEARCH_MAX_JOBS_PER_QUERY)))

    tasks: list[tuple[str, str, asyncio.Task]] = []
    for keyword in keywords:
        for location in locations:
            task = asyncio.create_task(
                _async_pair_collect(
                    session,
                    semaphore,
                    keyword,
                    location,
                    limit_per_keyword=limit_per_keyword,
                )
            )
            tasks.append((keyword, location, task))

    jobs: list[dict[str, str]] = []
    failed_pairs: list[tuple[str, str]] = []

    for keyword, location, task in tasks:
        try:
            pair_jobs, had_failure = await task
            jobs.extend(pair_jobs)
            if had_failure and len(pair_jobs) < max(10, limit_per_keyword // 5):
                failed_pairs.append((keyword, location))
        except Exception as exc:  # noqa: BLE001
            logger.warning("LinkedIn async pair failed keyword=%s location=%s error=%s", keyword, location, exc)
            failed_pairs.append((keyword, location))

    if failed_pairs:
        jobs.extend(await asyncio.to_thread(_fetch_via_playwright, failed_pairs, limit=200))

    deduped = _dedupe_jobs(jobs)
    logger.info("metric=source_jobs source=linkedin count=%s", len(deduped))
    return deduped


def fetch_jobs(keywords: list[str], locations: list[str]) -> list[dict]:
    limit = max(1, min(400, max(DEFAULT_LIMIT_PER_KEYWORD, SEARCH_MAX_JOBS_PER_QUERY)))

    if aiohttp is None:
        jobs, failed_pairs = _fetch_via_requests_api(keywords, locations, limit=limit)
        if failed_pairs:
            jobs.extend(_fetch_via_playwright(failed_pairs, limit=200))
        deduped = _dedupe_jobs(jobs)
        logger.info("metric=source_jobs source=linkedin count=%s", len(deduped))
        return deduped

    async def _run() -> list[dict]:
        timeout = aiohttp.ClientTimeout(total=25)
        connector = aiohttp.TCPConnector(limit=100, ssl=False)
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.linkedin.com/jobs/",
            },
        ) as session:
            semaphore = asyncio.Semaphore(20)
            return await async_fetch_jobs(keywords, locations, session, semaphore)

    try:
        return asyncio.run(_run())
    except RuntimeError:
        jobs, failed_pairs = _fetch_via_requests_api(keywords, locations, limit=limit)
        if failed_pairs:
            jobs.extend(_fetch_via_playwright(failed_pairs, limit=200))
        deduped = _dedupe_jobs(jobs)
        logger.info("metric=source_jobs source=linkedin count=%s", len(deduped))
        return deduped
