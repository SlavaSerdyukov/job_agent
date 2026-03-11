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


LEVER_API_URL = "https://api.lever.co/v0/postings/{company}?mode=json"
LEVER_COMPANIES = [
    "asana",
    "brex",
    "gusto",
    "eventbrite",
    "samsara",
    "calendly",
    "coursera",
    "segment",
    "intercom",
    "tripactions",
    "ramp",
    "mixpanel",
]
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
TIMEOUT = 20


def _company_label(slug: str) -> str:
    return slug.replace("-", " ").title().strip()


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


def _parse_payload(
    payload: list[dict],
    company_slug: str,
    keywords: list[str],
    locations: list[str],
) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue

        title = str(item.get("text", "") or "").strip()
        location_data = item.get("categories") if isinstance(item.get("categories"), dict) else {}
        location = str(location_data.get("location", "") or "Unknown").strip()
        link = str(item.get("applyUrl", "") or "").strip()
        description = str(item.get("descriptionPlain", "") or item.get("description", "") or "").strip()
        job_id = str(item.get("id", "") or link).strip()

        if not title or not link:
            continue

        searchable = f"{title} {description}".lower()
        if not _matches_keywords(searchable, keywords):
            continue
        if not _matches_location(location, locations):
            continue

        jobs.append(
            {
                "job_id": job_id,
                "title": title,
                "company": _company_label(company_slug),
                "location": location,
                "description": description,
                "link": link,
                "source": "lever",
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


def _fetch_company_sync(company_slug: str) -> list[dict]:
    url = LEVER_API_URL.format(company=company_slug)
    response = requests.get(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        return []
    return payload


def fetch_jobs(keywords: list[str], locations: list[str]) -> list[dict]:
    collected: list[dict[str, str]] = []

    for company_slug in LEVER_COMPANIES:
        try:
            payload = _fetch_company_sync(company_slug)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Lever fetch failed company=%s error=%s", company_slug, exc)
            continue

        collected.extend(_parse_payload(payload, company_slug, keywords, locations))
        if len(collected) > 5000:
            break

    deduped = _dedupe(collected)
    logger.info("metric=source_jobs source=lever count=%s", len(deduped))
    return deduped


async def _fetch_company_async(session, semaphore: asyncio.Semaphore, company_slug: str) -> tuple[str, list[dict]]:
    url = LEVER_API_URL.format(company=company_slug)
    async with semaphore:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as response:
            response.raise_for_status()
            payload = await response.json()
    await asyncio.sleep(random.uniform(0.5, 2.0))
    if not isinstance(payload, list):
        payload = []
    return company_slug, payload


async def async_fetch_jobs(keywords: list[str], locations: list[str], session, semaphore: asyncio.Semaphore) -> list[dict]:
    if aiohttp is None:
        return await asyncio.to_thread(fetch_jobs, keywords, locations)

    tasks = [
        asyncio.create_task(_fetch_company_async(session, semaphore, company_slug))
        for company_slug in LEVER_COMPANIES
    ]

    collected: list[dict[str, str]] = []
    for task in asyncio.as_completed(tasks):
        try:
            company_slug, payload = await task
        except Exception as exc:  # noqa: BLE001
            logger.debug("Lever async fetch failed: %s", exc)
            continue
        collected.extend(_parse_payload(payload, company_slug, keywords, locations))

    deduped = _dedupe(collected)
    logger.info("metric=source_jobs source=lever count=%s", len(deduped))
    return deduped
