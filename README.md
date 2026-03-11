# AI Job Hunter - Automated Python Backend Job Finder

AI Job Hunter is a Python-based automation system that discovers, ranks, and delivers Python backend job opportunities from multiple sources. It combines high-throughput crawling, relevance filtering, AI-assisted ranking, Telegram interaction, and optional auto-apply workflows.

## Features

- Multi-source job crawling
- LinkedIn API scraping with fallback support
- AI job ranking with blended score model
- Telegram notifications for top opportunities
- Interactive Telegram buttons (`Apply`, `Save`, `Skip`)
- Automated job application queue processing
- Async crawling for high performance

## Architecture

```text
sources -> crawler -> filter -> ranking -> telegram
```

Pipeline overview:

1. Source connectors collect jobs from LinkedIn, Indeed, RemoteOK, and Wellfound.
2. Jobs are normalized and globally deduplicated.
3. Filtering keeps junior/middle Python backend opportunities.
4. Ranking combines rule-based and AI-assisted scoring.
5. Top jobs are sent to Telegram with action buttons.

## Tech Stack

- Python
- asyncio
- aiohttp
- Playwright
- BeautifulSoup
- pandas
- Telegram Bot API

## Installation

```bash
git clone <repo>
cd linkedin-job-agent

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

Install browser binaries for Playwright:

```bash
playwright install
```

## Configuration

Create `.env` from `config/example.env` and set values:

```env
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

You can also configure runtime behavior such as headless mode, crawler interval, and auto-apply options.

## Running the Agent

```bash
python main.py
```

Or with helper script:

```bash
./scripts/run_agent.sh
```

## Example Output

```text
💼 Python Backend Engineer
🏢 Adyen
📍 Amsterdam

⭐ Rank: 52
🤖 AI: 61

[🚀 Apply] [⭐ Save]
[❌ Skip]
```

## Future Improvements

- ML-based job ranking
- CV embeddings
- Cover letter generation
- Additional job sources
