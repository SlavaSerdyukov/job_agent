from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone

try:
    import aiohttp
except Exception:  # noqa: BLE001
    aiohttp = None  # type: ignore[assignment]

import requests

logger = logging.getLogger(__name__)


REMOTEOK_API_URL = "https://remoteok.com/api"


def _matches_keywords(text: str, keywords: list[str]) -> bool:
    haystack = text.lower()
    wanted = ["python", "backend", "api", "django", "fastapi"] + [k.lower() for k in keywords]
    return any(token in haystack for token in wanted)


def _parse_payload(payload: list[dict], keywords: list[str], locations: list[str]) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []
    seen: set[str] = set()

    for item in payload:
        if not isinstance(item, dict):
            continue

        title = str(item.get("position", "") or item.get("title", "")).strip()
        company = str(item.get("company", "")).strip()
        description = str(item.get("description", "") or "").strip()
        link = str(item.get("url", "") or "").strip()
        tags = item.get("tags") or []
        tags_text = " ".join(str(tag) for tag in tags)
        text = f"{title} {description} {tags_text}".lower()

        if not _matches_keywords(text, keywords):
            continue

        location = str(item.get("location", "Remote")).strip() or "Remote"
        if locations:
            target_locations = [loc.lower() for loc in locations]
            if location.lower() != "remote" and not any(loc in location.lower() for loc in target_locations):
                if "python" not in text and "backend" not in text:
                    continue

        job_id = str(item.get("id", "") or link).strip()
        if not title or not company or not link:
            continue

        if job_id in seen:
            continue
        seen.add(job_id)

        jobs.append(
            {
                "job_id": job_id,
                "title": title,
                "company": company,
                "location": location,
                "link": link,
                "description": description,
                "source": "remoteok",
                "tags": ",".join(str(tag) for tag in tags) if tags else "remoteok,remote",
                "keyword": "python backend",
                "searched_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    return jobs


async def async_fetch_jobs(keywords: list[str], locations: list[str], session, semaphore: asyncio.Semaphore) -> list[dict]:
    if aiohttp is None:
        return await asyncio.to_thread(fetch_jobs, keywords, locations)

    try:
        async with semaphore:
            async with session.get(
                REMOTEOK_API_URL,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as response:
                response.raise_for_status()
                payload = await response.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("RemoteOK async fetch failed: %s", exc)
        return []

    await asyncio.sleep(random.uniform(0.5, 2.0))

    if not isinstance(payload, list):
        return []

    jobs = _parse_payload(payload, keywords, locations)
    logger.info("metric=source_jobs source=remoteok count=%s", len(jobs))
    return jobs


def fetch_jobs(keywords: list[str], locations: list[str]) -> list[dict]:
    if aiohttp is not None:
        async def _run() -> list[dict]:
            timeout = aiohttp.ClientTimeout(total=20)
            connector = aiohttp.TCPConnector(limit=30, ssl=False)
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                semaphore = asyncio.Semaphore(20)
                return await async_fetch_jobs(keywords, locations, session, semaphore)

        try:
            return asyncio.run(_run())
        except RuntimeError:
            pass

    try:
        response = requests.get(
            REMOTEOK_API_URL,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("RemoteOK sync fetch failed: %s", exc)
        return []

    if not isinstance(payload, list):
        return []

    jobs = _parse_payload(payload, keywords, locations)
    logger.info("metric=source_jobs source=remoteok count=%s", len(jobs))
    return jobs
