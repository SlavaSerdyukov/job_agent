from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

import pandas as pd

from app.analytics import record_metrics
from app.auto_apply import apply_single_job
from app.config import APPLY_QUEUE_PATH, HEADLESS, LINKEDIN_EMAIL, LINKEDIN_PASSWORD, MAX_APPLICATIONS_PER_DAY
from app.jobs_database import find_job_by_id
from app.linkedin_client import LinkedInClient

logger = logging.getLogger(__name__)


WORKER_STATE_PATH = APPLY_QUEUE_PATH.parent / "apply_worker_state.json"
APPLY_WORKER_INTERVAL_SECONDS = 1800


QUEUE_COLUMNS = ["job_id", "created_at", "status", "processed_at", "error"]


def _load_queue() -> pd.DataFrame:
    if APPLY_QUEUE_PATH.exists() and APPLY_QUEUE_PATH.stat().st_size > 0:
        try:
            df = pd.read_csv(APPLY_QUEUE_PATH)
        except Exception:  # noqa: BLE001
            df = pd.DataFrame(columns=QUEUE_COLUMNS)
    else:
        df = pd.DataFrame(columns=QUEUE_COLUMNS)

    for col in QUEUE_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    return df


def _save_queue(df: pd.DataFrame) -> None:
    APPLY_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(APPLY_QUEUE_PATH, index=False)


def _load_state() -> dict:
    today = datetime.now(timezone.utc).date().isoformat()
    if WORKER_STATE_PATH.exists() and WORKER_STATE_PATH.stat().st_size > 0:
        try:
            state = json.loads(WORKER_STATE_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            state = {"date": today, "applied_today": 0}
    else:
        state = {"date": today, "applied_today": 0}

    if state.get("date") != today:
        state = {"date": today, "applied_today": 0}

    return state


def _save_state(state: dict) -> None:
    WORKER_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    WORKER_STATE_PATH.write_text(json.dumps(state), encoding="utf-8")


def process_apply_queue_once() -> int:
    if not LINKEDIN_EMAIL or not LINKEDIN_PASSWORD:
        logger.warning("Apply worker skipped: missing LinkedIn credentials")
        return 0

    queue_df = _load_queue()
    if queue_df.empty:
        return 0

    pending_idx = queue_df.index[queue_df["status"].fillna("").astype(str).str.strip().isin(["", "queued"])].tolist()
    if not pending_idx:
        return 0

    state = _load_state()
    remaining = max(0, int(MAX_APPLICATIONS_PER_DAY) - int(state.get("applied_today", 0)))
    if remaining <= 0:
        logger.info("Apply worker daily limit reached: %s", MAX_APPLICATIONS_PER_DAY)
        return 0

    applied_count = 0

    with LinkedInClient(headless=HEADLESS) as client:
        try:
            client.login()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Apply worker login failed: %s", exc)
            return 0

        for idx in pending_idx:
            if remaining <= 0:
                break

            job_id = str(queue_df.at[idx, "job_id"] or "").strip()
            if not job_id:
                queue_df.at[idx, "status"] = "failed"
                queue_df.at[idx, "processed_at"] = datetime.now(timezone.utc).isoformat()
                queue_df.at[idx, "error"] = "missing_job_id"
                continue

            job = find_job_by_id(job_id)
            if not job:
                queue_df.at[idx, "status"] = "failed"
                queue_df.at[idx, "processed_at"] = datetime.now(timezone.utc).isoformat()
                queue_df.at[idx, "error"] = "job_not_found"
                continue

            link = str(job.get("link", "") or "")
            if "linkedin.com" not in link:
                queue_df.at[idx, "status"] = "failed"
                queue_df.at[idx, "processed_at"] = datetime.now(timezone.utc).isoformat()
                queue_df.at[idx, "error"] = "non_linkedin_job"
                continue

            job_payload = {
                "link": link,
                "score": 100,
            }

            try:
                success = apply_single_job(client.page, job_payload, score_threshold=-1)
            except Exception as exc:  # noqa: BLE001
                success = False
                queue_df.at[idx, "error"] = f"{type(exc).__name__}: {exc}"

            queue_df.at[idx, "processed_at"] = datetime.now(timezone.utc).isoformat()
            if success:
                queue_df.at[idx, "status"] = "applied"
                queue_df.at[idx, "error"] = ""
                applied_count += 1
                remaining -= 1
            else:
                queue_df.at[idx, "status"] = "failed"
                if not str(queue_df.at[idx, "error"] or "").strip():
                    queue_df.at[idx, "error"] = "apply_failed"

    _save_queue(queue_df)

    if applied_count > 0:
        state["applied_today"] = int(state.get("applied_today", 0)) + applied_count
        _save_state(state)
        record_metrics(jobs_applied=applied_count)

    logger.info("metric=apply_worker_applied count=%s", applied_count)
    return applied_count


def start_apply_worker(interval_seconds: int = APPLY_WORKER_INTERVAL_SECONDS) -> None:
    logger.info("Starting apply worker")
    while True:
        try:
            process_apply_queue_once()
        except Exception:  # noqa: BLE001
            logger.exception("Apply worker iteration failed")
        time.sleep(max(60, int(interval_seconds)))


if __name__ == "__main__":
    start_apply_worker()
