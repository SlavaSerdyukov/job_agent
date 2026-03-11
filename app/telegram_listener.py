from __future__ import annotations

import csv
import logging
import time
from datetime import datetime, timezone

import requests

from app.config import (
    APPLIED_JOBS_PATH,
    APPLY_QUEUE_PATH,
    SAVED_JOBS_PATH,
    SKIPPED_JOBS_PATH,
    TELEGRAM_BOT_TOKEN,
)

logger = logging.getLogger(__name__)


def _bot_api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"


def _append_row(path, row: dict[str, str], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()

    with path.open("a", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        if not exists or path.stat().st_size == 0:
            writer.writeheader()
        writer.writerow(row)


def _append_action_file(path, job_id: str) -> None:
    _append_row(
        path,
        {
            "job_id": job_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        ["job_id", "created_at"],
    )


def _append_apply_queue(job_id: str) -> None:
    _append_row(
        APPLY_QUEUE_PATH,
        {
            "job_id": job_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "queued",
        },
        ["job_id", "created_at", "status"],
    )


def _answer_callback_query(callback_query_id: str, text: str) -> None:
    try:
        requests.post(
            _bot_api_url("answerCallbackQuery"),
            json={"callback_query_id": callback_query_id, "text": text},
            timeout=20,
        )
    except requests.RequestException as exc:
        logger.warning("Failed to answer callback query: %s", exc)


def handle_apply(job_id: str) -> str:
    _append_action_file(APPLIED_JOBS_PATH, job_id)
    _append_apply_queue(job_id)
    logger.info("job_added_to_apply_queue job_id=%s", job_id)
    return "Job added to apply queue 🚀"


def handle_skip(job_id: str) -> str:
    _append_action_file(SKIPPED_JOBS_PATH, job_id)
    logger.info("job_skipped job_id=%s", job_id)
    return "Skipped ❌"


def handle_save(job_id: str) -> str:
    _append_action_file(SAVED_JOBS_PATH, job_id)
    logger.info("job_saved job_id=%s", job_id)
    return "Saved for later ⭐"


def _parse_callback_data(callback_data: str) -> tuple[str, str]:
    if callback_data.startswith("apply_"):
        return "apply", callback_data.replace("apply_", "", 1)
    if callback_data.startswith("skip_"):
        return "skip", callback_data.replace("skip_", "", 1)
    if callback_data.startswith("save_"):
        return "save", callback_data.replace("save_", "", 1)
    return "", ""


def _handle_callback_query(callback_query: dict) -> None:
    callback_query_id = str(callback_query.get("id", "") or "")
    callback_data = str(callback_query.get("data", "") or "")

    logger.info("telegram_callback_received callback_data=%s", callback_data)

    action, job_id = _parse_callback_data(callback_data)
    if not action or not job_id:
        if callback_query_id:
            _answer_callback_query(callback_query_id, "Unknown action")
        return

    if action == "apply":
        message = handle_apply(job_id)
    elif action == "save":
        message = handle_save(job_id)
    else:
        message = handle_skip(job_id)

    if callback_query_id:
        _answer_callback_query(callback_query_id, message)


def start_telegram_listener() -> None:
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("Telegram listener disabled: TELEGRAM_BOT_TOKEN is missing")
        return

    logger.info("Starting Telegram callback listener")
    offset: int | None = None

    while True:
        params: dict[str, object] = {
            "timeout": 2,
            "allowed_updates": ["callback_query"],
        }
        if offset is not None:
            params["offset"] = offset

        try:
            response = requests.get(_bot_api_url("getUpdates"), params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            logger.warning("Telegram getUpdates failed: %s", exc)
            time.sleep(2)
            continue

        if not payload.get("ok"):
            logger.warning("Telegram getUpdates returned non-ok payload: %s", payload)
            time.sleep(2)
            continue

        updates = payload.get("result", [])
        for update in updates:
            offset = int(update.get("update_id", 0)) + 1
            callback_query = update.get("callback_query")
            if callback_query:
                _handle_callback_query(callback_query)

        time.sleep(2)


if __name__ == "__main__":
    start_telegram_listener()
