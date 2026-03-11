from __future__ import annotations

import importlib
import logging
from collections.abc import Callable

logger = logging.getLogger(__name__)


_MODULES = {
    "linkedin": "app.sources.linkedin_api",
    "indeed": "app.sources.indeed_api",
    "remoteok": "app.sources.remoteok_api",
    "wellfound": "app.sources.wellfound_api",
}

SOURCE_FETCHERS: dict[str, Callable] = {}
SOURCE_ASYNC_FETCHERS: dict[str, Callable] = {}

for source_name, module_path in _MODULES.items():
    try:
        module = importlib.import_module(module_path)
        fetcher = getattr(module, "fetch_jobs", None)
        async_fetcher = getattr(module, "async_fetch_jobs", None)

        if callable(fetcher):
            SOURCE_FETCHERS[source_name] = fetcher
        if callable(async_fetcher):
            SOURCE_ASYNC_FETCHERS[source_name] = async_fetcher
    except Exception as exc:  # noqa: BLE001
        logger.warning("Source module unavailable source=%s module=%s error=%s", source_name, module_path, exc)

__all__ = ["SOURCE_FETCHERS", "SOURCE_ASYNC_FETCHERS"]
