from __future__ import annotations

from typing import Any

from config import LOGGER
from modules.providers import stock_primary_provider


def normalize_code(code: Any) -> str:
    """Normalize a stock code into a six-digit string."""
    return stock_primary_provider.normalize_code(code)


def fetch_stock_data(code: str, name: str | None = None) -> dict[str, Any]:
    """Backup source: use Tushare latest daily close only."""
    normalized_code = normalize_code(code)
    result = stock_primary_provider.fetch_latest_daily_data(normalized_code, name)
    if result:
        result["data_source"] = "tushare_daily_backup"
        return result

    LOGGER.warning("Tushare backup daily source returned no usable data for %s.", normalized_code)
    return {}
