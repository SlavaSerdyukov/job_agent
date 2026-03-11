from __future__ import annotations

import hashlib
import logging

import requests
from app.retry_utils import retry, stop_after_attempt, wait_fixed

from app.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


TELEGRAM_SEND_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


def _format_source_label(source: str) -> str:
    normalized = (source or "").strip().lower()
    known = {
        "linkedin": "LinkedIn",
        "remoteok": "RemoteOK",
        "indeed": "Indeed",
        "wellfound": "Wellfound",
        "hackernews": "HackerNews",
        "stackoverflow": "StackOverflow",
    }
    return known.get(normalized, normalized.title() if normalized else "N/A")


def _build_message(job: dict[str, str]) -> str:
    score_value = job.get("score")
    ai_score = job.get("ai_score")

    lines = [
        f"💼 {job.get('title', 'N/A')}",
        f"🏢 {job.get('company', 'N/A')}",
        f"📍 {job.get('location', 'N/A')}",
        f"🌐 {_format_source_label(str(job.get('source', 'N/A') or 'N/A'))}",
        "",
    ]

    if score_value is not None:
        lines.append(f"⭐ Rank: {float(score_value):.1f}")
    if ai_score is not None:
        lines.append(f"🤖 AI: {float(ai_score):.1f}")

    lines.extend(
        [
            "",
            "Apply or save this job 👇",
            str(job.get("link", "")),
        ]
    )
    return "\n".join(lines)


def _job_callback_id(job: dict[str, str]) -> str:
    raw = str(job.get("job_id", "") or "").strip()
    if raw:
        return raw[:40]

    link = str(job.get("link", "") or "")
    if not link:
        return "unknown"

    return hashlib.md5(link.encode("utf-8")).hexdigest()[:16]


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def _post_telegram(payload: dict) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise ValueError("Missing Telegram credentials in .env")

    response = requests.post(TELEGRAM_SEND_URL, json=payload, timeout=20)

    logger.info("telegram_status status_code=%s ok=%s", response.status_code, response.ok)
    logger.info("telegram_response body=%s", response.text[:1000])

    response.raise_for_status()


def send_text_message(text: str) -> bool:
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    try:
        _post_telegram(payload)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send Telegram text message: %s", exc)
        return False
    return True


def send_telegram_message(job: dict[str, str]) -> bool:
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": _build_message(job),
        "disable_web_page_preview": True,
    }

    try:
        _post_telegram(payload)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send Telegram message: %s", exc)
        return False

    return True


def send_interactive_job(job: dict[str, str]) -> bool:
    job_id = _job_callback_id(job)
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": _build_message(job),
        "disable_web_page_preview": True,
        "reply_markup": {
            "inline_keyboard": [
                [
                    {"text": "🚀 Apply", "callback_data": f"apply_{job_id}"},
                    {"text": "⭐ Save", "callback_data": f"save_{job_id}"},
                ],
                [{"text": "❌ Skip", "callback_data": f"skip_{job_id}"}],
            ]
        },
    }

    try:
        _post_telegram(payload)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send interactive Telegram message: %s", exc)
        return False

    return True


def send_job(job: dict[str, str]) -> bool:
    return send_telegram_message(job)
