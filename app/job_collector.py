from __future__ import annotations

from playwright.sync_api import ElementHandle


def _first_text(card: ElementHandle, selectors: list[str]) -> str:
    for selector in selectors:
        node = card.query_selector(selector)
        if node:
            value = node.inner_text().strip()
            if value:
                return value
    return ""


def _first_attr(card: ElementHandle, selectors: list[str], attr: str) -> str:
    for selector in selectors:
        node = card.query_selector(selector)
        if node:
            value = node.get_attribute(attr)
            if value:
                return value.strip()
    return ""


def parse_cards(cards: list[ElementHandle], location: str) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []

    for card in cards:
        try:
            title = _first_text(card, ["h3", ".base-search-card__title", ".job-card-list__title"])
            company = _first_text(card, ["h4", ".base-search-card__subtitle", ".artdeco-entity-lockup__subtitle"])

            link = _first_attr(card, ["a.base-card__full-link", "a.job-card-list__title", "a"], "href")
            if link:
                link = link.split("?")[0]

            job_id = card.get_attribute("data-job-id") or ""
            snippet = _first_text(
                card,
                [
                    ".job-search-card__snippet",
                    ".base-search-card__metadata",
                    ".job-card-list__snippet",
                ],
            )

            if not title or not company or not link:
                continue

            jobs.append(
                {
                    "job_id": job_id.strip(),
                    "title": title,
                    "company": company,
                    "link": link,
                    "location": location,
                    "text": snippet,
                }
            )

        except Exception:  # noqa: BLE001
            continue

    return jobs
