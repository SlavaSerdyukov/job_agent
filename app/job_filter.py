from __future__ import annotations
import re

INCLUDE_TERMS = (
    "python",
    "backend",
    "api",
    "django",
    "fastapi",
)

REJECT_TERMS = (
    "data engineer",
    "qa",
    "quality assurance",
    "devops",
    "frontend",
)

EXCLUDE_TERMS = (
    "senior",
    "sr",
    "lead",
    "principal",
    "staff",
    "architect",
    "manager",
    "director",
)

LEVEL_TERMS = (
    "junior",
    "jr",
    "middle",
    "mid",
    "entry",
    "associate",
)


def _extract_years(text: str) -> int | None:
    """
    Extract years of experience requirement from text.
    """
    match = re.search(r"(\d+)\+?\s*(?:years|yrs)", text)
    if match:
        return int(match.group(1))
    return None


def is_junior_or_middle(job: dict[str, str]) -> bool:

    haystack = (
        f"{job.get('title', '')} "
        f"{job.get('description', '')} "
        f"{job.get('seniority_level', '')}"
    ).lower()

    if not any(term in haystack for term in INCLUDE_TERMS):
        return False

    if any(term in haystack for term in REJECT_TERMS):
        return False

    if any(term in haystack for term in EXCLUDE_TERMS):
        return False

    if any(term in haystack for term in LEVEL_TERMS):
        return True

    years = _extract_years(haystack)

    if years is not None:
        if years <= 3:
            return True
        if years >= 5:
            return False

    return True


def filter_junior_middle(jobs: list[dict[str, str]]) -> list[dict[str, str]]:
    return [job for job in jobs if is_junior_or_middle(job)]
