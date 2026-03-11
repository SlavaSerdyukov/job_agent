from __future__ import annotations

import hashlib
from datetime import datetime, timezone

REQUIRED_FIELDS = [
    "job_id",
    "title",
    "company",
    "location",
    "description",
    "link",
    "source",
    "tags",
]


TAG_HINTS = (
    "python",
    "backend",
    "api",
    "django",
    "fastapi",
    "microservices",
    "remote",
)


def _hash_link(link: str) -> str:
    return hashlib.md5(link.encode("utf-8")).hexdigest()


def _normalize_tags(raw: object, text: str) -> list[str]:
    tags: list[str] = []

    if isinstance(raw, (list, tuple, set)):
        tags.extend(str(item).strip().lower() for item in raw if str(item).strip())
    elif isinstance(raw, str):
        split_candidates = [part.strip().lower() for part in raw.replace(";", ",").split(",")]
        tags.extend(part for part in split_candidates if part)

    lowered = text.lower()
    for hint in TAG_HINTS:
        if hint in lowered and hint not in tags:
            tags.append(hint)

    return tags


def normalize_jobs(jobs: list[dict]) -> list[dict]:
    normalized: list[dict] = []

    for raw in jobs:
        link = str(raw.get("link", "") or "").strip()
        title = str(raw.get("title", "") or "").strip()
        company = str(raw.get("company", "") or "").strip()

        if not title or not company or not link:
            continue

        description = str(raw.get("description", "") or raw.get("text", "") or "").strip()
        source = str(raw.get("source", "") or "unknown").strip().lower()
        job_id = str(raw.get("job_id", "") or "").strip() or _hash_link(link)
        text_blob = f"{title} {description}"

        item = {
            "job_id": job_id,
            "title": title,
            "company": company,
            "location": str(raw.get("location", "") or "").strip() or "Unknown",
            "description": description,
            "link": link,
            "source": source,
            "tags": _normalize_tags(raw.get("tags"), text_blob),
            "keyword": str(raw.get("keyword", "") or "").strip(),
            "searched_at": str(raw.get("searched_at", "") or datetime.now(timezone.utc).isoformat()).strip(),
        }

        for opt in ("workplace_type", "seniority_level", "employment_type", "company_size", "remote"):
            if opt in raw:
                item[opt] = raw.get(opt)

        if all(field in item for field in REQUIRED_FIELDS):
            normalized.append(item)

    return normalized
