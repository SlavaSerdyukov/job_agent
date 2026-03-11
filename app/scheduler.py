from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Callable

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_COLUMNS = [
    "job_id",
    "link",
    "title",
    "company",
    "location",
    "keyword",
    "searched_at",
    "score",
    "ai_score",
]


def load_seen(path: Path) -> pd.DataFrame:
    if path.exists() and path.stat().st_size > 0:
        df = pd.read_csv(path)
        for col in DEFAULT_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        return df

    return pd.DataFrame(columns=DEFAULT_COLUMNS)


def save_seen(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    cleaned = df.copy()
    if "job_id" in cleaned.columns:
        cleaned["job_id"] = cleaned["job_id"].fillna("").astype(str)
    if "link" in cleaned.columns:
        cleaned["link"] = cleaned["link"].fillna("").astype(str)

    with_job_id = cleaned[cleaned["job_id"].str.strip() != ""] if "job_id" in cleaned.columns else pd.DataFrame()
    without_job_id = cleaned[cleaned["job_id"].str.strip() == ""] if "job_id" in cleaned.columns else cleaned

    if not with_job_id.empty:
        with_job_id = with_job_id.drop_duplicates(subset=["job_id"], keep="first")

    if not without_job_id.empty and "link" in without_job_id.columns:
        without_job_id = without_job_id.drop_duplicates(subset=["link"], keep="first")

    if "job_id" in cleaned.columns:
        cleaned = pd.concat([with_job_id, without_job_id], ignore_index=True)
    else:
        cleaned = without_job_id

    cleaned.to_csv(path, index=False)


def loop(task: Callable[[], None], interval_seconds: int) -> None:
    while True:
        started_at = time.time()
        try:
            task()
        except Exception:  # noqa: BLE001
            logger.exception("Scheduled task failed")

        elapsed = time.time() - started_at
        sleep_for = max(0, interval_seconds - int(elapsed))
        logger.info("Sleeping for %s seconds", sleep_for)
        time.sleep(sleep_for)
