from __future__ import annotations


def ai_score_job(job: dict) -> float:
    title = str(job.get("title", "") or "").lower()
    description = str(job.get("description", "") or "").lower()
    tags = " ".join(str(tag).lower() for tag in (job.get("tags") or []))

    text = f"{title} {description} {tags}"
    score = 0.0

    if "python" in text:
        score += 40
    if "backend" in text:
        score += 20
    if "django" in text:
        score += 20
    if "fastapi" in text:
        score += 15
    if "api" in text:
        score += 10
    if "microservices" in text:
        score += 10

    if "senior" in text:
        score -= 30
    if "lead" in text:
        score -= 20

    # Keep within a readable range.
    return max(0.0, min(100.0, score))


def blend_scores(score: float, ai_score: float) -> float:
    return round((float(score) + float(ai_score)) / 2.0, 2)


def apply_ai_scores(jobs: list[dict]) -> list[dict]:
    enriched: list[dict] = []

    for job in jobs:
        item = dict(job)
        base_score = float(item.get("score", 0) or 0)
        item["ai_score"] = ai_score_job(item)
        item["final_score"] = blend_scores(base_score, float(item["ai_score"]))
        enriched.append(item)

    return sorted(enriched, key=lambda job: float(job.get("final_score", 0) or 0), reverse=True)
