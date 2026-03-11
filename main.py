from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from app.ai_ranker import apply_ai_scores
from app.analytics import maybe_send_daily_report, record_metrics
from app.auto_apply import auto_apply_jobs
from app.apply_worker import start_apply_worker
from app.config import (
    CRAWLER_INTERVAL_SECONDS,
    CSV_PATH,
    ENABLE_AUTO_APPLY,
    HEADLESS,
    KEYWORDS,
    LEGACY_CSV_PATH,
    LOCATIONS,
    RESUME_PATH,
)
from app.crawler import crawl_jobs
from app.deduplicator import deduplicate_jobs
from app.job_details import fetch_job_details
from app.job_filter import filter_junior_middle
from app.job_normalizer import normalize_jobs
from app.job_ranker import rank_jobs
from app.jobs_database import append_jobs_to_database, ensure_jobs_database
from app.linkedin_client import LinkedInClient
from app.notifier import send_interactive_job
from app.scheduler import load_seen, loop, save_seen
from app.telegram_listener import start_telegram_listener

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


TOP_JOBS_TO_SEND = 10


def _bootstrap_seen_file() -> None:
    if CSV_PATH.exists():
        return

    if LEGACY_CSV_PATH.exists() and LEGACY_CSV_PATH.stat().st_size > 0:
        CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        CSV_PATH.write_text(LEGACY_CSV_PATH.read_text(encoding="utf-8"), encoding="utf-8")
        logger.info("Migrated legacy seen CSV: %s -> %s", LEGACY_CSV_PATH, CSV_PATH)


def _resume_text(path: str) -> str:
    if not path:
        return ""

    resume_path = Path(path)
    if not resume_path.exists() or not resume_path.is_file():
        return ""

    try:
        return resume_path.read_text(encoding="utf-8", errors="ignore")[:12000]
    except OSError:
        return ""


def _enrich_linkedin_details(jobs: list[dict], client: LinkedInClient | None) -> list[dict]:
    if not jobs:
        return []

    if client is None or client.page is None:
        return [dict(job) for job in jobs]

    enriched_jobs: list[dict] = []

    for job in jobs:
        enriched = dict(job)
        link = str(job.get("link", "") or "")
        source = str(job.get("source", "") or "").lower()

        if source == "linkedin" and "linkedin.com" in link:
            try:
                details = fetch_job_details(client.page, link)
                enriched.update(details)
            except Exception:  # noqa: BLE001
                logger.exception("Failed to fetch LinkedIn details for: %s", link)

        enriched_jobs.append(enriched)

    return enriched_jobs


def _send_jobs_and_collect_rows(
    jobs: list[dict],
    seen_links: set[str],
    seen_job_ids: set[str],
) -> tuple[int, list[dict[str, str | float]]]:
    sent_count = 0
    new_rows: list[dict[str, str | float]] = []

    for job in jobs[:TOP_JOBS_TO_SEND]:
        link = str(job.get("link", "") or "").strip()
        if not link:
            continue

        sent = send_interactive_job(job)
        if not sent:
            continue

        sent_count += 1
        job_id = str(job.get("job_id", "") or "").strip()
        if job_id:
            seen_job_ids.add(job_id)
        seen_links.add(link)

        new_rows.append(
            {
                "job_id": job_id,
                "link": link,
                "title": job.get("title", ""),
                "company": job.get("company", ""),
                "location": job.get("location", ""),
                "keyword": job.get("keyword", ""),
                "searched_at": datetime.now(timezone.utc).isoformat(),
                "score": float(job.get("score", 0) or 0),
                "ai_score": float(job.get("ai_score", 0) or 0),
            }
        )

    return sent_count, new_rows


def run() -> None:
    _bootstrap_seen_file()
    ensure_jobs_database()

    seen_df = load_seen(CSV_PATH)
    seen_links = set(seen_df["link"].dropna().astype(str).tolist()) if "link" in seen_df.columns else set()
    seen_job_ids = set(seen_df["job_id"].dropna().astype(str).tolist()) if "job_id" in seen_df.columns else set()

    raw_jobs = crawl_jobs(KEYWORDS, LOCATIONS, max_workers=20)
    normalized_jobs = normalize_jobs(raw_jobs)
    deduped_jobs = deduplicate_jobs(normalized_jobs, persist=True)

    record_metrics(jobs_collected=len(deduped_jobs))

    unseen_jobs: list[dict] = []
    for job in deduped_jobs:
        link = str(job.get("link", "") or "").strip()
        job_id = str(job.get("job_id", "") or "").strip()

        if link and link in seen_links:
            continue
        if job_id and job_id in seen_job_ids:
            continue
        unseen_jobs.append(job)

    requires_linkedin_session = any(
        str(job.get("source", "") or "").lower() == "linkedin" for job in unseen_jobs
    ) or ENABLE_AUTO_APPLY

    filtered_jobs: list[dict]
    scored_jobs: list[dict]
    applied_count = 0

    if requires_linkedin_session:
        with LinkedInClient(headless=HEADLESS) as client:
            logged_in = True
            try:
                client.login()
            except Exception as exc:  # noqa: BLE001
                logged_in = False
                logger.warning("LinkedIn login failed. Continuing without detail enrichment/auto-apply: %s", exc)

            detailed_jobs = _enrich_linkedin_details(unseen_jobs, client if logged_in else None)
            filtered_jobs = filter_junior_middle(detailed_jobs)
            record_metrics(jobs_filtered=len(filtered_jobs))

            ranked_jobs = rank_jobs(filtered_jobs)
            scored_jobs = apply_ai_scores(ranked_jobs)
            record_metrics(jobs_ranked=len(scored_jobs))

            append_jobs_to_database(scored_jobs)

            sent_count, new_rows = _send_jobs_and_collect_rows(scored_jobs, seen_links, seen_job_ids)
            record_metrics(jobs_sent=sent_count)
            logger.info("metric=jobs_sent count=%s", sent_count)

            if ENABLE_AUTO_APPLY and scored_jobs and logged_in and client.page:
                applied_count = auto_apply_jobs(client.page, scored_jobs)
                record_metrics(jobs_applied=applied_count)
                logger.info("metric=jobs_applied count=%s", applied_count)

    else:
        filtered_jobs = filter_junior_middle(unseen_jobs)
        record_metrics(jobs_filtered=len(filtered_jobs))

        ranked_jobs = rank_jobs(filtered_jobs)
        scored_jobs = apply_ai_scores(ranked_jobs)
        record_metrics(jobs_ranked=len(scored_jobs))

        append_jobs_to_database(scored_jobs)

        sent_count, new_rows = _send_jobs_and_collect_rows(scored_jobs, seen_links, seen_job_ids)
        record_metrics(jobs_sent=sent_count)
        logger.info("metric=jobs_sent count=%s", sent_count)

    if new_rows:
        updated_df = pd.concat([seen_df, pd.DataFrame(new_rows)], ignore_index=True)
        save_seen(updated_df, CSV_PATH)
        logger.info("Saved %s new jobs", len(new_rows))
    else:
        logger.info("No new matching jobs found")

    maybe_send_daily_report()


def main() -> None:
    threading.Thread(target=start_telegram_listener, daemon=True).start()
    threading.Thread(target=start_apply_worker, daemon=True).start()
    loop(run, CRAWLER_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
