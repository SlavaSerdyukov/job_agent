from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Iterable

from playwright.sync_api import Page

from app.config import (
    APPLICANT_EMAIL,
    APPLICANT_NAME,
    APPLICANT_PHONE,
    AUTO_APPLY_SCORE_THRESHOLD,
    MAX_APPLICATIONS_PER_DAY,
    RESUME_PATH,
)

logger = logging.getLogger(__name__)

_daily_applied: dict[str, int] = {}


def _today_key() -> str:
    return date.today().isoformat()


def _can_apply_more(max_per_day: int) -> bool:
    return _daily_applied.get(_today_key(), 0) < max_per_day


def _increment_applied() -> None:
    key = _today_key()
    _daily_applied[key] = _daily_applied.get(key, 0) + 1


def _first_visible(page: Page, selectors: list[str]):
    for selector in selectors:
        node = page.query_selector(selector)
        if node and node.is_visible():
            return node
    return None


def _fill_first(page: Page, selectors: list[str], value: str) -> bool:
    if not value:
        return False

    for selector in selectors:
        node = page.query_selector(selector)
        if node and node.is_visible():
            node.fill(value)
            return True
    return False


def _has_cover_letter_requirement(page: Page) -> bool:
    checks = [
        "textarea[aria-label*='cover letter' i]",
        "label:has-text('Cover letter')",
        "input[type='file'][name*='cover' i]",
        "input[type='file'][id*='cover' i]",
    ]
    return any(page.query_selector(selector) for selector in checks)


def _attach_resume(page: Page, resume_path: str) -> bool:
    if not resume_path:
        return False

    path = Path(resume_path)
    if not path.exists():
        logger.warning("Resume path does not exist: %s", resume_path)
        return False

    uploader = _first_visible(
        page,
        [
            "input[type='file']",
            "input[name='file']",
            "input[id*='upload' i]",
        ],
    )
    if not uploader:
        return False

    uploader.set_input_files(str(path))
    return True


def _try_submit(page: Page) -> bool:
    for selector in [
        "button:has-text('Submit application')",
        "button:has-text('Submit')",
    ]:
        btn = page.query_selector(selector)
        if btn and btn.is_enabled():
            btn.click()
            page.wait_for_timeout(1200)
            return True

    # Some flows require one extra review click before submit.
    review_btn = page.query_selector("button:has-text('Review')")
    if review_btn and review_btn.is_enabled():
        review_btn.click()
        page.wait_for_timeout(800)
        submit_btn = page.query_selector("button:has-text('Submit application')")
        if submit_btn and submit_btn.is_enabled():
            submit_btn.click()
            page.wait_for_timeout(1200)
            return True

    return False


def apply_single_job(page: Page, job: dict, *, score_threshold: float = AUTO_APPLY_SCORE_THRESHOLD) -> bool:
    score = float(job.get("score", 0) or 0)
    if score <= score_threshold:
        logger.info("metric=job_not_applied reason=low_score score=%s", score)
        return False

    link = str(job.get("link", "") or "")
    if not link:
        return False

    page.goto(link, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(1200)

    easy_apply_button = _first_visible(
        page,
        [
            "button:has-text('Easy Apply')",
            "button.jobs-apply-button",
            "button[aria-label*='Easy Apply' i]",
        ],
    )

    if not easy_apply_button:
        logger.info("metric=job_not_applied reason=no_easy_apply link=%s", link)
        return False

    easy_apply_button.click()
    page.wait_for_timeout(1000)

    if _has_cover_letter_requirement(page):
        dismiss = _first_visible(page, ["button[aria-label='Dismiss']", "button:has-text('Cancel')"])
        if dismiss:
            dismiss.click()
        logger.info("metric=job_not_applied reason=cover_letter_required link=%s", link)
        return False

    _fill_first(
        page,
        [
            "input[aria-label*='Full name' i]",
            "input[aria-label*='Name' i]",
            "input[name*='name' i]",
        ],
        APPLICANT_NAME,
    )
    _fill_first(
        page,
        [
            "input[aria-label*='Email' i]",
            "input[name*='email' i]",
            "input[type='email']",
        ],
        APPLICANT_EMAIL,
    )
    _fill_first(
        page,
        [
            "input[aria-label*='Phone' i]",
            "input[name*='phone' i]",
            "input[type='tel']",
        ],
        APPLICANT_PHONE,
    )

    _attach_resume(page, RESUME_PATH)

    submitted = _try_submit(page)
    if submitted:
        logger.info("metric=job_applied link=%s score=%s", link, score)
        return True

    logger.info("metric=job_not_applied reason=submit_not_available link=%s", link)
    return False


def auto_apply_jobs(
    page: Page,
    jobs: Iterable[dict],
    *,
    max_per_day: int = MAX_APPLICATIONS_PER_DAY,
    score_threshold: float = AUTO_APPLY_SCORE_THRESHOLD,
) -> int:
    applied = 0

    for job in jobs:
        if not _can_apply_more(max_per_day):
            logger.info("metric=auto_apply_limit_reached max_per_day=%s", max_per_day)
            break

        try:
            if apply_single_job(page, job, score_threshold=score_threshold):
                _increment_applied()
                applied += 1
        except Exception:  # noqa: BLE001
            logger.exception("Auto-apply failed for job: %s", job.get("link", ""))

    logger.info("metric=jobs_applied count=%s", applied)
    return applied
