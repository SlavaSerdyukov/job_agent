from __future__ import annotations

import asyncio
import json
import logging
import random
from urllib.parse import urljoin

try:
    import aiohttp
except Exception:  # noqa: BLE001
    aiohttp = None  # type: ignore[assignment]

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


YC_JOBS_URL = "https://www.ycombinator.com/jobs"
TIMEOUT = 25
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


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


def _iter_dicts(node):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _iter_dicts(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_dicts(item)


def _extract_company(value) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("name", "companyName", "title"):
            raw = value.get(key)
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
    return ""


def _extract_link(item: dict) -> str:
    for key in ("url", "jobUrl", "applyUrl", "applicationUrl", "link"):
        raw = item.get(key)
        if isinstance(raw, str) and raw.strip():
            return urljoin("https://www.ycombinator.com", raw.strip())
    return ""


def _extract_jobs_from_next_data(html: str, keywords: list[str], locations: list[str]) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    script = soup.select_one("script#__NEXT_DATA__")
    if not script or not script.string:
        return []

    try:
        payload = json.loads(script.string)
    except json.JSONDecodeError:
        return []

    jobs: list[dict[str, str]] = []
    for item in _iter_dicts(payload):
        title = ""
        for key in ("title", "jobTitle", "role", "name"):
            raw = item.get(key)
            if isinstance(raw, str) and raw.strip():
                title = raw.strip()
                break

        company = _extract_company(item.get("company")) or _extract_company(item.get("companyName"))
        link = _extract_link(item)
        location = str(item.get("location", "") or item.get("locationName", "") or "Unknown").strip()
        description = str(item.get("description", "") or item.get("snippet", "") or "").strip()
        job_id = str(item.get("id", "") or item.get("slug", "") or link).strip()

        if not title or not company or not link:
            continue
        if "/jobs" not in link:
            continue
        if not _matches_keywords(f"{title} {description}", keywords):
            continue
        if not _matches_location(location, locations):
            continue

        jobs.append(
            {
                "job_id": job_id,
                "title": title,
                "company": company,
                "location": location,
                "description": description,
                "link": link,
                "source": "ycombinator",
            }
        )

    return jobs


def _extract_jobs_from_html(html: str, keywords: list[str], locations: list[str]) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    jobs: list[dict[str, str]] = []

    for link_node in soup.select("a[href*='/jobs/']"):
        href = str(link_node.get("href", "") or "").strip()
        if not href:
            continue

        title = link_node.get_text(" ", strip=True)
        if not title:
            continue

        link = urljoin("https://www.ycombinator.com", href.split("?")[0])
        container = link_node.find_parent(["div", "li", "article"])
        context_text = container.get_text(" ", strip=True) if container else title
        company = "Y Combinator Startup"
        location = "Unknown"

        if "remote" in context_text.lower():
            location = "Remote"

        if not _matches_keywords(context_text, keywords):
            continue
        if not _matches_location(location, locations):
            continue

        jobs.append(
            {
                "job_id": link.rstrip("/").split("/")[-1],
                "title": title,
                "company": company,
                "location": location,
                "description": context_text,
                "link": link,
                "source": "ycombinator",
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


def fetch_jobs(keywords: list[str], locations: list[str]) -> list[dict]:
    try:
        response = requests.get(
            YC_JOBS_URL,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        html = response.text
    except Exception as exc:  # noqa: BLE001
        logger.warning("YC jobs fetch failed: %s", exc)
        return []

    parsed = _extract_jobs_from_next_data(html, keywords, locations)
    if not parsed:
        parsed = _extract_jobs_from_html(html, keywords, locations)

    deduped = _dedupe(parsed)
    logger.info("metric=source_jobs source=ycombinator count=%s", len(deduped))
    return deduped


async def async_fetch_jobs(keywords: list[str], locations: list[str], session, semaphore: asyncio.Semaphore) -> list[dict]:
    if aiohttp is None:
        return await asyncio.to_thread(fetch_jobs, keywords, locations)

    try:
        async with semaphore:
            async with session.get(
                YC_JOBS_URL,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT),
            ) as response:
                response.raise_for_status()
                html = await response.text()
    except Exception as exc:  # noqa: BLE001
        logger.warning("YC jobs async fetch failed: %s", exc)
        return []

    await asyncio.sleep(random.uniform(0.5, 2.0))
    parsed = _extract_jobs_from_next_data(html, keywords, locations)
    if not parsed:
        parsed = _extract_jobs_from_html(html, keywords, locations)

    deduped = _dedupe(parsed)
    logger.info("metric=source_jobs source=ycombinator count=%s", len(deduped))
    return deduped
