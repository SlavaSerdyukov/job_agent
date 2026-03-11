from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

try:
    from tenacity import retry as _retry
    from tenacity import stop_after_attempt as _stop_after_attempt
    from tenacity import wait_fixed as _wait_fixed

    retry = _retry
    stop_after_attempt = _stop_after_attempt
    wait_fixed = _wait_fixed
except Exception:  # noqa: BLE001
    def retry(*args, **kwargs):
        def decorator(func: F) -> F:
            return func

        return decorator

    def stop_after_attempt(*args, **kwargs) -> None:
        return None

    def wait_fixed(*args, **kwargs) -> None:
        return None
