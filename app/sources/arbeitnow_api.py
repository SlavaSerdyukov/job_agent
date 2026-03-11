from __future__ import annotations

import asyncio
import logging
import random

try:
    import aiohttp
except Exception:  # noqa: BLE001
    aiohttp = None  # type: ignore[assignment]

import requests

logger = logging.getLogger(__name__)


ARBEITNOW_API_URL = "https://arbeitnow.com/api/job-board-api"
TIMEOUT = 20
MAX_PAGES = 15


def _matches_keywords(text: str, keywords: list[str]) -> bool:
    haystack = text.lower()
    baseline = ["python", "backend", "api", "django", "fastapi"]
    dynamic = [keyword.lower() for keyword in keywords if keyword]
    return any(token in haystack for token in baseline + dynamic)


def _matches_location(job_location: str, locations: list[str], remote: bool) -> bool:
    if remote:
        return True
    if not locations:
        return True
    lowered = job_location.lower()
    return any(location.lower() in lowered for location in locations)


def _parse_items(items: list[dict], keywords: list[str], locations: list[str]) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []

    for item in items:
        if not isinstance(item, dict):
            continue

        title = str(item.get("title", "") or "").strip()
        company = str(item.get("company_name", "") or "").strip()
        location = str(item.get("location", "") or "Unknown").strip()
        description = str(item.get("description", "") or "").strip()
        link = str(item.get("url", "") or "").strip()
        job_id = str(item.get("slug", "") or link).strip()
        remote = bool(item.get("remote", False))
        tags = item.get("tags") if isinstance(item.get("tags"), list) else []

        searchable = f"{title} {description} {' '.join(str(tag) for tag in tags)}"
        if not _matches_keywords(searchable, keywords):
            continue
        if not _matches_location(location, locations, remote):
            continue

        if remote and "remote" not in location.lower():
            location = f"{location} (Remote)".strip()

        if not title or not company or not link:
            continue

        jobs.append(
            {
                "job_id": job_id,
                "title": title,
                "company": company,
                "location": location,
                "description": description,
                "link": link,
                "source": "arbeitnow",
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


def _fetch_page_sync(page: int) -> dict:
    response = requests.get(
        ARBEITNOW_API_URL,
        params={"page": page},
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict):
        return payload
    return {}


def fetch_jobs(keywords: list[str], locations: list[str]) -> list[dict]:
    collected: list[dict[str, str]] = []

    for page in range(1, MAX_PAGES + 1):
        try:
            payload = _fetch_page_sync(page)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Arbeitnow fetch failed page=%s error=%s", page, exc)
            continue

        items = payload.get("data")
        if not isinstance(items, list) or not items:
            break

        parsed = _parse_items(items, keywords, locations)
        if not parsed and page > 5:
            continue
        collected.extend(parsed)

    deduped = _dedupe(collected)
    logger.info("metric=source_jobs source=arbeitnow count=%s", len(deduped))
    return deduped


async def _fetch_page_async(session, semaphore: asyncio.Semaphore, page: int) -> dict:
    async with semaphore:
        async with session.get(
            ARBEITNOW_API_URL,
            params={"page": page},
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
        ) as response:
            response.raise_for_status()
            payload = await response.json()
    await asyncio.sleep(random.uniform(0.5, 2.0))
    if isinstance(payload, dict):
        return payload
    return {}


async def async_fetch_jobs(keywords: list[str], locations: list[str], session, semaphore: asyncio.Semaphore) -> list[dict]:
    if aiohttp is None:
        return await asyncio.to_thread(fetch_jobs, keywords, locations)

    tasks = [
        asyncio.create_task(_fetch_page_async(session, semaphore, page))
        for page in range(1, MAX_PAGES + 1)
    ]

    collected: list[dict[str, str]] = []
    for task in asyncio.as_completed(tasks):
        try:
            payload = await task
        except Exception as exc:  # noqa: BLE001
            logger.debug("Arbeitnow async fetch failed: %s", exc)
            continue

        items = payload.get("data")
        if not isinstance(items, list):
            continue
        collected.extend(_parse_items(items, keywords, locations))

    deduped = _dedupe(collected)
    logger.info("metric=source_jobs source=arbeitnow count=%s", len(deduped))
    return deduped
