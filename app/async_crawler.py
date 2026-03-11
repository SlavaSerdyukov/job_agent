from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

try:
    import aiohttp
except Exception:  # noqa: BLE001
    aiohttp = None  # type: ignore[assignment]

from app.sources import SOURCE_ASYNC_FETCHERS, SOURCE_FETCHERS

logger = logging.getLogger(__name__)


DEFAULT_CONCURRENCY = 20


async def _run_source_async(
    source: str,
    fetcher: Callable,
    keywords: list[str],
    locations: list[str],
    session: Any,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    try:
        jobs = await fetcher(keywords, locations, session, semaphore)
        logger.info("metric=source_completed source=%s mode=async count=%s", source, len(jobs))
        return jobs or []
    except Exception:  # noqa: BLE001
        logger.exception("Async source failed: %s", source)
        return []


async def _run_source_threaded(source: str, fetcher: Callable, keywords: list[str], locations: list[str]) -> list[dict]:
    try:
        jobs = await asyncio.to_thread(fetcher, keywords, locations)
        logger.info("metric=source_completed source=%s mode=thread count=%s", source, len(jobs))
        return jobs or []
    except Exception:  # noqa: BLE001
        logger.exception("Threaded source failed: %s", source)
        return []


async def _crawl_without_aiohttp(keywords: list[str], locations: list[str]) -> list[dict]:
    tasks = [
        asyncio.create_task(_run_source_threaded(source, fetcher, keywords, locations))
        for source, fetcher in SOURCE_FETCHERS.items()
    ]

    results = await asyncio.gather(*tasks, return_exceptions=False)
    merged: list[dict] = []
    for source_jobs in results:
        merged.extend(source_jobs or [])
    return merged


async def crawl_jobs_async(
    keywords: list[str],
    locations: list[str],
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> list[dict]:
    """Run all sources concurrently with asyncio + aiohttp.

    Async-capable sources share one aiohttp session and semaphore;
    sync-only sources run in worker threads to keep full concurrency.
    """
    if not SOURCE_FETCHERS and not SOURCE_ASYNC_FETCHERS:
        logger.warning("No source fetchers available")
        return []

    if aiohttp is None:
        logger.warning("aiohttp is unavailable, using threaded source fallback")
        merged = await _crawl_without_aiohttp(keywords, locations)
        logger.info("metric=jobs_collected_total_async count=%s", len(merged))
        return merged

    connector = aiohttp.TCPConnector(limit=max(20, concurrency * 5), ssl=False)
    timeout = aiohttp.ClientTimeout(total=30)
    semaphore = asyncio.Semaphore(max(10, concurrency))

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
    ) as session:
        tasks: list[asyncio.Task] = []

        for source, async_fetcher in SOURCE_ASYNC_FETCHERS.items():
            tasks.append(
                asyncio.create_task(
                    _run_source_async(source, async_fetcher, keywords, locations, session, semaphore)
                )
            )

        for source, sync_fetcher in SOURCE_FETCHERS.items():
            if source in SOURCE_ASYNC_FETCHERS:
                continue
            tasks.append(asyncio.create_task(_run_source_threaded(source, sync_fetcher, keywords, locations)))

        results = await asyncio.gather(*tasks, return_exceptions=False)

    merged: list[dict] = []
    for source_jobs in results:
        merged.extend(source_jobs or [])

    logger.info("metric=jobs_collected_total_async count=%s", len(merged))
    return merged


def crawl_jobs(keywords: list[str], locations: list[str], *, concurrency: int = DEFAULT_CONCURRENCY) -> list[dict]:
    try:
        return asyncio.run(crawl_jobs_async(keywords, locations, concurrency=concurrency))
    except RuntimeError:
        # Fallback if called from an already-running event loop.
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(crawl_jobs_async(keywords, locations, concurrency=concurrency))
