from __future__ import annotations

import hashlib
from pathlib import Path

from app.config import DATA_DIR


DEDUPE_STATE_PATH = DATA_DIR / "dedupe_seen_keys.txt"


def _link_hash(link: str) -> str:
    return hashlib.md5(link.encode("utf-8")).hexdigest()


def _load_persistent_seen(path: Path) -> set[str]:
    if not path.exists() or path.stat().st_size == 0:
        return set()

    seen: set[str] = set()
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            key = line.strip()
            if key:
                seen.add(key)
    return seen


def _append_persistent_keys(path: Path, keys: set[str]) -> None:
    if not keys:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        for key in sorted(keys):
            fp.write(f"{key}\n")


def deduplicate_jobs(
    jobs: list[dict],
    *,
    persist: bool = True,
    state_path: Path = DEDUPE_STATE_PATH,
) -> list[dict]:
    persistent_seen = _load_persistent_seen(state_path) if persist else set()

    seen_job_ids: set[str] = set()
    seen_links: set[str] = set()
    deduped: list[dict] = []
    new_persistent: set[str] = set()

    for job in jobs:
        job_id = str(job.get("job_id", "") or "").strip()
        link = str(job.get("link", "") or "").strip()
        link_key = _link_hash(link) if link else ""

        if job_id and (job_id in seen_job_ids or job_id in persistent_seen):
            continue
        if link_key and (link_key in seen_links or link_key in persistent_seen):
            continue

        if job_id:
            seen_job_ids.add(job_id)
            if persist:
                new_persistent.add(job_id)

        if link_key:
            seen_links.add(link_key)
            if persist:
                new_persistent.add(link_key)

        deduped.append(job)

    if persist:
        _append_persistent_keys(state_path, new_persistent)

    return deduped
