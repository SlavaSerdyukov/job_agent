from __future__ import annotations

import logging
from typing import Any

from playwright.sync_api import Page
from app.retry_utils import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

_DETAILS_CACHE: dict[str, dict[str, Any]] = {}


def _first_text(page: Page, selectors: list[str]) -> str:
    for selector in selectors:
        element = page.query_selector(selector)
        if not element:
            continue
        value = element.inner_text().strip()
        if value:
            return value
    return ""


def _extract_criteria(page: Page) -> dict[str, str]:
    criteria: dict[str, str] = {}

    # Common layout on linkedin.com/jobs pages.
    for item in page.query_selector_all("li.description__job-criteria-item"):
        label = item.query_selector("h3")
        value = item.query_selector("span")
        if not label or not value:
            continue
        key = label.inner_text().strip().lower()
        val = value.inner_text().strip()
        if key and val:
            criteria[key] = val

    # Fallback layout on some signed-in pages.
    if not criteria:
        for item in page.query_selector_all(".job-details-jobs-unified-top-card__job-insight"):
            text = item.inner_text().strip()
            if not text:
                continue
            lowered = text.lower()
            if "company size" in lowered:
                criteria.setdefault("company size", text)
            if "seniority" in lowered:
                criteria.setdefault("seniority level", text)
            if "employment" in lowered:
                criteria.setdefault("employment type", text)

    return criteria


def _detect_workplace(description: str, raw_text: str, default: str = "") -> tuple[str, bool]:
    haystack = f"{description} {raw_text} {default}".lower()

    if "hybrid" in haystack:
        return "Hybrid", False
    if "remote" in haystack:
        return "Remote", True
    if "on-site" in haystack or "onsite" in haystack:
        return "On-site", False

    if default:
        return default, "remote" in default.lower()

    return "", False


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def fetch_job_details(page: Page, job_url: str) -> dict[str, Any]:
    """Open a job page and extract rich details.

    Cached in-memory by URL to avoid repeated scraping in the same run.
    """
    if not job_url:
        return {
            "description": "",
            "seniority_level": "",
            "employment_type": "",
            "workplace_type": "",
            "company_size": "",
            "remote": False,
        }

    if job_url in _DETAILS_CACHE:
        return dict(_DETAILS_CACHE[job_url])

    page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(1200)

    description = _first_text(
        page,
        [
            ".show-more-less-html__markup",
            ".jobs-description__content",
            ".description__text",
        ],
    )

    criteria = _extract_criteria(page)

    seniority_level = criteria.get("seniority level", "")
    employment_type = criteria.get("employment type", "")
    company_size = criteria.get("company size", "")

    # Some pages include a workplace type line in top insights.
    workplace_hint = _first_text(
        page,
        [
            ".job-details-jobs-unified-top-card__tertiary-description-container",
            ".jobs-unified-top-card__subtitle-primary-grouping",
        ],
    )

    page_text = ""
    body = page.query_selector("body")
    if body:
        page_text = body.inner_text().strip()

    workplace_type, remote = _detect_workplace(description, page_text, workplace_hint)

    result = {
        "description": description,
        "seniority_level": seniority_level,
        "employment_type": employment_type,
        "workplace_type": workplace_type,
        "company_size": company_size,
        "remote": remote,
    }

    _DETAILS_CACHE[job_url] = dict(result)
    logger.debug("Fetched job details for %s", job_url)

    return result
