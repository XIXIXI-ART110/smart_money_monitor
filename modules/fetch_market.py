from __future__ import annotations

import re
from typing import Any, Mapping

from config import DATA_PROVIDER, LOGGER
from modules.providers import stock_primary_provider, stock_provider_router, ths_market_provider


def normalize_code(code: Any) -> str:
    """Normalize a stock code into a six-digit string."""
    raw_code = str(code).strip()
    if not raw_code:
        return ""

    digit_matches = re.findall(r"\d+", raw_code)
    if digit_matches:
        digits = "".join(digit_matches)
        if len(digits) >= 6:
            return digits[-6:]
        return digits.zfill(6)

    return raw_code.zfill(6)


def _safe_float(value: Any) -> float | None:
    """Compatibility helper kept for fund-flow normalization."""
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("%", "").strip()
        if cleaned in {"", "-", "None", "nan", "NaN"}:
            return None
        value = cleaned
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_key(key: str) -> str:
    return (
        str(key)
        .strip()
        .lower()
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
        .replace("%", "")
        .replace("（", "(")
        .replace("）", ")")
    )


def _first_existing(row: Mapping[str, Any], keys: list[str]) -> Any:
    """Compatibility helper retained for existing fund-flow module."""
    for key in keys:
        if key in row:
            return row[key]

    normalized_items = {_normalize_key(str(key)): value for key, value in row.items()}
    for key in keys:
        normalized_key = _normalize_key(key)
        if normalized_key in normalized_items:
            return normalized_items[normalized_key]
    return None


def _resolve_market_provider():
    """Resolve the current market data provider from runtime config."""
    if DATA_PROVIDER == "ths":
        return ths_market_provider
    return stock_primary_provider


def get_all_spot_data() -> dict[str, dict[str, Any]]:
    """Fetch market snapshot data from the configured provider."""
    provider = _resolve_market_provider()
    try:
        return provider.get_all_spot_data()
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Market provider %s failed: %s", DATA_PROVIDER, exc)
        return {}


def get_stock_by_code(code: str) -> dict[str, Any]:
    """Fetch one stock snapshot from the configured provider."""
    if DATA_PROVIDER != "ths":
        return stock_provider_router.fetch_stock_data(code)

    provider = _resolve_market_provider()
    try:
        return provider.get_stock_by_code(code)
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Market provider %s failed for code %s: %s", DATA_PROVIDER, code, exc)
        return {}


def fetch_stock_data(code: str, name: str | None = None) -> dict[str, Any]:
    """Compatibility entry that now routes through the resilient stock router."""
    if DATA_PROVIDER != "ths":
        return stock_provider_router.fetch_stock_data(code, name)

    normalized_code = normalize_code(code)
    try:
        result = ths_market_provider.get_stock_by_code(normalized_code)
        if result:
            if name and not result.get("name"):
                result["name"] = name
            return result
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("THS market provider failed for code %s: %s", normalized_code, exc)

    return {
        "code": normalized_code,
        "name": name or normalized_code,
        "status": "error",
        "error": "未获取到实时行情数据",
        "latest_price": None,
        "pct_change": None,
        "turnover": None,
        "main_net_inflow": None,
    }


def self_test(codes: list[str] | None = None) -> None:
    """Run a smoke test against the configured provider."""
    provider = _resolve_market_provider()
    if hasattr(provider, "self_test"):
        provider.self_test(codes)
        return
    print(f"当前 provider={DATA_PROVIDER} 暂未实现自测入口。")


if __name__ == "__main__":
    self_test()
