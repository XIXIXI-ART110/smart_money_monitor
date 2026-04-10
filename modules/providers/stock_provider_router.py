from __future__ import annotations

import random
import time
from typing import Any

from config import LOGGER
from modules.providers import stock_backup_provider, stock_primary_provider


def _is_usable_market_data(data: dict[str, Any]) -> bool:
    """Check whether a provider result is usable for downstream analysis."""
    if not isinstance(data, dict) or not data:
        return False
    if data.get("status") == "error":
        return False
    if data.get("latest_price") is None:
        return False
    return True


def _build_error_payload(
    code: str,
    name: str | None,
    primary_error: str,
    backup_error: str,
) -> dict[str, Any]:
    """Return the final fallback-safe payload when both providers fail."""
    LOGGER.warning(
        "Stock router failed for %s. primary_error=%s | backup_error=%s",
        code,
        primary_error,
        backup_error,
    )
    return {
        "code": code,
        "name": name or code,
        "status": "error",
        "error": "未获取到实时行情数据",
        "latest_price": None,
        "pct_change": None,
        "turnover": None,
        "main_net_inflow": None,
    }


def fetch_stock_data(code: str, name: str | None = None) -> dict[str, Any]:
    """Fetch one stock with primary, backup, and final safe fallback."""
    normalized_code = stock_primary_provider.normalize_code(code)

    time.sleep(random.uniform(0.2, 0.5))

    primary_error = ""
    try:
        primary_result = stock_primary_provider.fetch_stock_data(normalized_code, name)
        if _is_usable_market_data(primary_result):
            return primary_result
        primary_error = "primary returned empty data"
    except Exception as exc:  # pragma: no cover - runtime safety
        primary_error = str(exc)
        LOGGER.warning("Primary stock provider failed for %s: %s", normalized_code, exc)

    backup_error = ""
    try:
        backup_result = stock_backup_provider.fetch_stock_data(normalized_code, name)
        if _is_usable_market_data(backup_result):
            return backup_result
        backup_error = "backup returned empty data"
    except Exception as exc:  # pragma: no cover - runtime safety
        backup_error = str(exc)
        LOGGER.warning("Backup stock provider failed for %s: %s", normalized_code, exc)

    return _build_error_payload(normalized_code, name, primary_error, backup_error)
