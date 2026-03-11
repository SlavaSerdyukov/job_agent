from __future__ import annotations

from app.config import PREFERRED_COUNTRIES


def score_job(job: dict) -> float:
    title = str(job.get("title", "") or "").lower()
    description = str(job.get("description", "") or job.get("text", "") or "").lower()
    location = str(job.get("location", "") or "").lower()
    workplace_type = str(job.get("workplace_type", "") or "").lower()
    company = str(job.get("company", "") or "").lower()
    source = str(job.get("source", "") or "").lower()

    full_text = f"{title} {description}"
    score = 0.0

    # Requested scoring weights.
    if "python" in full_text and "backend" in full_text:
        score += 40
    else:
        if "python" in full_text:
            score += 20
        if "backend" in full_text:
            score += 20

    if "django" in full_text:
        score += 20
    if "fastapi" in full_text:
        score += 20

    if "remote" in workplace_type or "remote" in location or bool(job.get("remote")):
        score += 15

    if "startup" in company or "startup" in full_text or source == "wellfound":
        score += 10

    if "senior" in full_text:
        score -= 30
    if "lead" in full_text:
        score -= 20

    # Keep existing negative relevance penalties to avoid off-target roles.
    if "data engineer" in full_text:
        score -= 20
    if "devops" in full_text:
        score -= 20
    if " qa " in f" {full_text} " or "quality assurance" in full_text or "sdet" in full_text:
        score -= 20
    if "frontend" in full_text:
        score -= 15

    # Mild location preference bias for configured countries.
    if any(country in location for country in PREFERRED_COUNTRIES):
        score += 8

    return score


def rank_jobs(jobs: list[dict]) -> list[dict]:
    scored: list[dict] = []

    for job in jobs:
        enriched = dict(job)
        enriched["score"] = score_job(enriched)
        scored.append(enriched)

    return sorted(scored, key=lambda item: item.get("score", 0), reverse=True)
