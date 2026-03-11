from __future__ import annotations

import asyncio
import html
import logging
import random
import re

try:
    import aiohttp
except Exception:  # noqa: BLE001
    aiohttp = None  # type: ignore[assignment]

import requests

logger = logging.getLogger(__name__)


HN_JOBS_URL = "https://hn.algolia.com/api/v1/search_by_date"
HITS_PER_PAGE = 200
MAX_PAGES = 5
TIMEOUT = 20


def _strip_html(raw: str) -> str:
    return re.sub(r"<[^>]+>", " ", html.unescape(raw or "")).strip()


def _split_company_and_title(raw_title: str) -> tuple[str, str]:
    title = (raw_title or "").strip()
    for separator in (" | ", " - ", " — ", " – ", ": "):
        if separator in title:
            left, right = title.split(separator, 1)
            if left.strip() and right.strip() and len(left.strip()) <= 48:
                return left.strip(), right.strip()
    return "HackerNews", title


def _matches_keywords(text: str, keywords: list[str]) -> bool:
    haystack = text.lower()
    baseline = ["python", "backend", "api", "django", "fastapi"]
    dynamic = [keyword.lower() for keyword in keywords if keyword]
    return any(token in haystack for token in baseline + dynamic)


def _parse_hits(hits: list[dict], keywords: list[str], locations: list[str]) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []
    for hit in hits:
        if not isinstance(hit, dict):
            continue

        raw_title = str(hit.get("title", "") or hit.get("story_title", "")).strip()
        description = _strip_html(str(hit.get("story_text", "") or hit.get("comment_text", "") or ""))
        searchable = f"{raw_title} {description}".lower()
        if not _matches_keywords(searchable, keywords):
            continue

        company, title = _split_company_and_title(raw_title)
        if not title:
            continue

        item_id = str(hit.get("objectID", "") or "").strip()
        link = str(hit.get("url", "") or "").strip()
        if not link and item_id:
            link = f"https://news.ycombinator.com/item?id={item_id}"
        if not link:
            continue

        location = "Remote"
        if locations:
            lowered = searchable.lower()
            if not any(location_name.lower() in lowered for location_name in locations) and "remote" not in lowered:
                location = "Unknown"

        jobs.append(
            {
                "job_id": item_id or link,
                "title": title,
                "company": company,
                "location": location,
                "description": description,
                "link": link,
                "source": "hackernews",
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
        HN_JOBS_URL,
        params={"tags": "job", "hitsPerPage": HITS_PER_PAGE, "page": page},
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

    for page in range(MAX_PAGES):
        try:
            payload = _fetch_page_sync(page)
        except Exception as exc:  # noqa: BLE001
            logger.debug("HackerNews fetch failed page=%s error=%s", page, exc)
            continue

        hits = payload.get("hits")
        if not isinstance(hits, list) or not hits:
            break

        parsed = _parse_hits(hits, keywords, locations)
        if not parsed and page > 1:
            break
        collected.extend(parsed)

    deduped = _dedupe(collected)
    logger.info("metric=source_jobs source=hackernews count=%s", len(deduped))
    return deduped


async def _fetch_page_async(session, semaphore: asyncio.Semaphore, page: int) -> dict:
    async with semaphore:
        async with session.get(
            HN_JOBS_URL,
            params={"tags": "job", "hitsPerPage": HITS_PER_PAGE, "page": page},
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

    tasks = [asyncio.create_task(_fetch_page_async(session, semaphore, page)) for page in range(MAX_PAGES)]
    collected: list[dict[str, str]] = []

    for task in asyncio.as_completed(tasks):
        try:
            payload = await task
        except Exception as exc:  # noqa: BLE001
            logger.debug("HackerNews async fetch failed: %s", exc)
            continue
        hits = payload.get("hits")
        if not isinstance(hits, list):
            continue
        collected.extend(_parse_hits(hits, keywords, locations))

    deduped = _dedupe(collected)
    logger.info("metric=source_jobs source=hackernews count=%s", len(deduped))
    return deduped
