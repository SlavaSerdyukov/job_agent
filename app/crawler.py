from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.async_crawler import crawl_jobs as crawl_jobs_async_sync
from app.sources.arbeitnow_api import fetch_jobs as arbeitnow_jobs
from app.sources.greenhouse_api import fetch_jobs as greenhouse_jobs
from app.sources.hackernews_api import fetch_jobs as hackernews_jobs
from app.sources.indeed_api import fetch_jobs as indeed_jobs
from app.sources.lever_api import fetch_jobs as lever_jobs
from app.sources.linkedin_api import fetch_jobs as linkedin_jobs
from app.sources.remoteok_api import fetch_jobs as remoteok_jobs
from app.sources.wellfound_api import fetch_jobs as wellfound_jobs
from app.sources.yc_jobs_api import fetch_jobs as yc_jobs

logger = logging.getLogger(__name__)


CPU_WORKERS = 8
SOURCES = [
    linkedin_jobs,
    remoteok_jobs,
    indeed_jobs,
    wellfound_jobs,
    greenhouse_jobs,
    lever_jobs,
    hackernews_jobs,
    yc_jobs,
    arbeitnow_jobs,
]


def _cpu_cleanup(job: dict) -> dict:
    normalized = dict(job)
    for field in ("title", "company", "location", "description", "link", "source"):
        if field in normalized and normalized[field] is not None:
            normalized[field] = str(normalized[field]).strip()
    return normalized


def _crawl_sync_fallback(keywords: list[str], locations: list[str], max_workers: int) -> list[dict]:
    workers = max(1, min(max_workers, len(SOURCES)))
    merged: list[dict] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(source_fetcher, keywords, locations): source_fetcher
            for source_fetcher in SOURCES
        }

        for future in as_completed(futures):
            source_fetcher = futures[future]
            source_name = source_fetcher.__module__.split(".")[-1]
            try:
                source_jobs = future.result() or []
            except Exception:  # noqa: BLE001
                logger.exception("Fallback source failed source=%s", source_name)
                continue
            merged.extend(source_jobs)

    return merged


def crawl_jobs(keywords: list[str], locations: list[str], *, max_workers: int = 20) -> list[dict]:
    """Collect jobs using async crawler first, then threaded source fallback."""
    raw_jobs = crawl_jobs_async_sync(keywords, locations, concurrency=max_workers)
    if not raw_jobs:
        logger.warning("Async crawl produced no jobs; running threaded source fallback")
        raw_jobs = _crawl_sync_fallback(keywords, locations, max_workers)

    if not raw_jobs:
        return []

    workers = max(1, min(CPU_WORKERS, len(raw_jobs)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        cleaned = list(executor.map(_cpu_cleanup, raw_jobs))

    logger.info("metric=jobs_collected_total count=%s", len(cleaned))
    return cleaned
