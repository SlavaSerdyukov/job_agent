from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"

# New default data folder (requested project structure)
DATA_DIR = BASE_DIR / "data"
# Legacy data folder kept for compatibility with existing installations
LEGACY_DATA_DIR = BASE_DIR / "app" / "data"

CSV_PATH = DATA_DIR / "jobs_seen.csv"
LEGACY_CSV_PATH = LEGACY_DATA_DIR / "jobs_seen.csv"
SCRAPED_JOBS_PATH = DATA_DIR / "scraped_jobs.csv"
APPLY_QUEUE_PATH = DATA_DIR / "apply_queue.csv"
APPLIED_JOBS_PATH = DATA_DIR / "applied_jobs.csv"
SAVED_JOBS_PATH = DATA_DIR / "saved_jobs.csv"
SKIPPED_JOBS_PATH = DATA_DIR / "skipped_jobs.csv"
DIGEST_STATE_PATH = DATA_DIR / "digest_state.json"

load_dotenv(ENV_PATH)


def _as_bool(value: str | None, *, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


LINKEDIN_EMAIL = os.getenv("LINKEDIN_EMAIL", "")
LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD", "")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

RESUME_PATH = os.getenv("RESUME_PATH", "")
APPLICANT_NAME = os.getenv("APPLICANT_NAME", "")
APPLICANT_EMAIL = os.getenv("APPLICANT_EMAIL", "")
APPLICANT_PHONE = os.getenv("APPLICANT_PHONE", "")

HEADLESS = _as_bool(os.getenv("HEADLESS"), default=True)
ENABLE_AUTO_APPLY = _as_bool(os.getenv("ENABLE_AUTO_APPLY"), default=False)

MAX_APPLICATIONS_PER_DAY = int(os.getenv("MAX_APPLICATIONS_PER_DAY", "20"))
AUTO_APPLY_SCORE_THRESHOLD = float(os.getenv("AUTO_APPLY_SCORE_THRESHOLD", "70"))

SEARCH_MAX_JOBS_PER_QUERY = int(os.getenv("SEARCH_MAX_JOBS_PER_QUERY", "400"))
SEARCH_SCROLL_ROUNDS = int(os.getenv("SEARCH_SCROLL_ROUNDS", "6"))
CRAWLER_INTERVAL_SECONDS = int(os.getenv("CRAWLER_INTERVAL_SECONDS", "1800"))

DIGEST_INTERVAL_HOURS = int(os.getenv("DIGEST_INTERVAL_HOURS", "12"))
TELEGRAM_POLL_TIMEOUT = int(os.getenv("TELEGRAM_POLL_TIMEOUT", "25"))

KEYWORDS = [
    "Backend Engineer Python",
    "Python Backend Developer",
    "Backend Engineer",
    "Backend Python",
    "Python Backend Developer",
    "Python Developer",
    "Backend Developer Python",
    "Software Engineer Python",
    "Backend Software Engineer",
    "API Developer Python",
    "Back End Developer",
    "Platform Engineer Python",
]

LOCATIONS = [
    "Belgium",
    "Netherlands",
    "Luxembourg",
]

PREFERRED_COUNTRIES = [
    country.strip().lower()
    for country in os.getenv("PREFERRED_COUNTRIES", ",".join(LOCATIONS)).split(",")
    if country.strip()
]

SEARCH_WORK_TYPES = "2,3"
SEARCH_TIME_RANGE = "r3600"

LINKEDIN_LOGIN_URL = "https://www.linkedin.com/login"
LINKEDIN_JOBS_SEARCH_URL = "https://www.linkedin.com/jobs/search/"

DATA_DIR.mkdir(parents=True, exist_ok=True)
if not APPLY_QUEUE_PATH.exists():
    APPLY_QUEUE_PATH.write_text("job_id,created_at,status\n", encoding="utf-8")
if not APPLIED_JOBS_PATH.exists():
    APPLIED_JOBS_PATH.write_text("job_id,created_at\n", encoding="utf-8")
if not SAVED_JOBS_PATH.exists():
    SAVED_JOBS_PATH.write_text("job_id,created_at\n", encoding="utf-8")
if not SKIPPED_JOBS_PATH.exists():
    SKIPPED_JOBS_PATH.write_text("job_id,created_at\n", encoding="utf-8")
