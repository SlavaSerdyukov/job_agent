from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from app.config import DATA_DIR

logger = logging.getLogger(__name__)


JOBS_DATABASE_PATH = DATA_DIR / "jobs_database.parquet"


def load_jobs_database(path: Path = JOBS_DATABASE_PATH) -> pd.DataFrame:
    fallback = path.with_suffix(".csv")

    if path.exists() and path.stat().st_size > 0:
        try:
            return pd.read_parquet(path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to read jobs database parquet: %s", exc)
    elif fallback.exists() and fallback.stat().st_size > 0:
        try:
            return pd.read_csv(fallback)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to read fallback jobs database CSV: %s", exc)
    return pd.DataFrame(
        columns=[
            "job_id",
            "title",
            "company",
            "location",
            "description",
            "link",
            "source",
            "keyword",
            "searched_at",
            "score",
            "ai_score",
            "sent_at",
        ]
    )


def ensure_jobs_database(path: Path = JOBS_DATABASE_PATH) -> None:
    fallback = path.with_suffix(".csv")
    if path.exists() and path.stat().st_size > 0:
        return
    if fallback.exists() and fallback.stat().st_size > 0:
        return

    empty_df = load_jobs_database(path)
    try:
        empty_df.to_parquet(path, index=False)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to initialize parquet jobs database: %s", exc)
        fallback = path.with_suffix(".csv")
        empty_df.to_csv(fallback, index=False)


def append_jobs_to_database(jobs: list[dict], path: Path = JOBS_DATABASE_PATH) -> None:
    if not jobs:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(jobs)
    existing = load_jobs_database(path)

    merged = pd.concat([existing, new_df], ignore_index=True)
    if "job_id" in merged.columns:
        merged["job_id"] = merged["job_id"].fillna("").astype(str)
    if "link" in merged.columns:
        merged["link"] = merged["link"].fillna("").astype(str)

    # Deduplicate by job_id first, then by link.
    non_empty_ids = merged[merged["job_id"].str.strip() != ""] if "job_id" in merged.columns else pd.DataFrame()
    empty_ids = merged[merged["job_id"].str.strip() == ""] if "job_id" in merged.columns else merged

    if not non_empty_ids.empty:
        non_empty_ids = non_empty_ids.drop_duplicates(subset=["job_id"], keep="last")
    if not empty_ids.empty:
        empty_ids = empty_ids.drop_duplicates(subset=["link"], keep="last")

    final_df = pd.concat([non_empty_ids, empty_ids], ignore_index=True)

    try:
        final_df.to_parquet(path, index=False)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to write jobs database parquet: %s", exc)
        fallback = path.with_suffix(".csv")
        final_df.to_csv(fallback, index=False)
        logger.warning("Wrote fallback CSV jobs database: %s", fallback)


def find_job_by_id(job_id: str, path: Path = JOBS_DATABASE_PATH) -> dict | None:
    if not job_id:
        return None

    df = load_jobs_database(path)
    if df.empty:
        return None

    candidates = df[df["job_id"].astype(str) == str(job_id)]
    if candidates.empty:
        # Fallback: sometimes callback id can be hash of link.
        candidates = df[df["link"].astype(str).str.contains(str(job_id), na=False)]

    if candidates.empty:
        return None

    row = candidates.iloc[-1].to_dict()
    return {k: ("" if pd.isna(v) else v) for k, v in row.items()}
