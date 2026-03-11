from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from app.config import DATA_DIR
from app.notifier import send_text_message

logger = logging.getLogger(__name__)


ANALYTICS_STATE_PATH = DATA_DIR / "analytics_state.json"
METRIC_KEYS = ["jobs_collected", "jobs_filtered", "jobs_ranked", "jobs_sent", "jobs_applied"]


def _default_state(today: str) -> dict:
    return {
        "date": today,
        "metrics": {key: 0 for key in METRIC_KEYS},
        "report_sent": False,
    }


def _load_state() -> dict:
    today = datetime.now(timezone.utc).date().isoformat()

    if ANALYTICS_STATE_PATH.exists() and ANALYTICS_STATE_PATH.stat().st_size > 0:
        try:
            state = json.loads(ANALYTICS_STATE_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            state = _default_state(today)
    else:
        state = _default_state(today)

    if state.get("date") != today:
        state = _default_state(today)

    metrics = state.setdefault("metrics", {})
    for key in METRIC_KEYS:
        metrics.setdefault(key, 0)

    return state


def _save_state(state: dict) -> None:
    ANALYTICS_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    ANALYTICS_STATE_PATH.write_text(json.dumps(state), encoding="utf-8")


def record_metrics(**kwargs: int) -> None:
    state = _load_state()

    for key, value in kwargs.items():
        if key not in METRIC_KEYS:
            continue
        try:
            delta = int(value)
        except (TypeError, ValueError):
            continue
        state["metrics"][key] = int(state["metrics"].get(key, 0)) + delta

    _save_state(state)


def _build_report_text(state: dict) -> str:
    metrics = state.get("metrics", {})
    return (
        "📊 Job Hunter Report\n\n"
        f"Collected: {int(metrics.get('jobs_collected', 0))}\n"
        f"Filtered: {int(metrics.get('jobs_filtered', 0))}\n"
        f"Ranked: {int(metrics.get('jobs_ranked', 0))}\n"
        f"Sent: {int(metrics.get('jobs_sent', 0))}\n"
        f"Applied: {int(metrics.get('jobs_applied', 0))}"
    )


def maybe_send_daily_report() -> bool:
    state = _load_state()

    if bool(state.get("report_sent")):
        return False

    now_utc = datetime.now(timezone.utc)
    # Send once per day after 20:00 UTC.
    if now_utc.hour < 20:
        return False

    report = _build_report_text(state)
    sent = send_text_message(report)
    if not sent:
        logger.warning("Daily analytics report failed to send")
        return False

    state["report_sent"] = True
    _save_state(state)
    logger.info("metric=analytics_report_sent")
    return True
