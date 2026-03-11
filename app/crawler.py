from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from app.async_crawler import crawl_jobs as crawl_jobs_async_sync

logger = logging.getLogger(__name__)


CPU_WORKERS = 8


def _cpu_cleanup(job: dict) -> dict:
    normalized = dict(job)
    for field in ("title", "company", "location", "description", "link", "source"):
        if field in normalized and normalized[field] is not None:
            normalized[field] = str(normalized[field]).strip()
    return normalized


def crawl_jobs(keywords: list[str], locations: list[str], *, max_workers: int = 20) -> list[dict]:
    """Collect jobs using async network crawler + threadpool CPU post-processing."""
    raw_jobs = crawl_jobs_async_sync(keywords, locations, concurrency=max_workers)

    if not raw_jobs:
        return []

    workers = max(1, min(CPU_WORKERS, len(raw_jobs)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        cleaned = list(executor.map(_cpu_cleanup, raw_jobs))

    logger.info("metric=jobs_collected_total count=%s", len(cleaned))
    return cleaned
