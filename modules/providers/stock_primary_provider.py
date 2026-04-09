from __future__ import annotations

import re
from typing import Any, Mapping

import akshare as ak
import pandas as pd

from config import LOGGER


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


SPOT_API_CANDIDATES = (
    "stock_zh_a_spot_em",
    "stock_zh_a_spot",
)

MARKET_FIELD_ALIASES: dict[str, list[str]] = {
    "code": ["代码", "股票代码", "证券代码", "symbol", "code"],
    "name": ["名称", "股票名称", "证券简称", "简称", "name"],
    "latest_price": ["最新价", "现价", "最新", "最新价格", "收盘价", "price"],
    "pct_change": ["涨跌幅", "涨跌幅%", "涨跌幅(%)", "涨幅", "changepercent", "pct_change"],
    "turnover": ["成交额", "成交金额", "成交总额", "amount", "turnover"],
    "volume": ["成交量", "总手", "volume"],
    "open": ["今开", "开盘价", "open"],
    "high": ["最高", "最高价", "high"],
    "low": ["最低", "最低价", "low"],
    "close_prev": ["昨收", "昨收价", "previous_close", "close_prev"],
    "turnover_rate": ["换手率", "turnoverrate", "turnover_rate"],
    "amplitude": ["振幅", "amplitude"],
    "timestamp": ["时间戳", "时间", "更新时间", "timestamp"],
}


def _safe_float(value: Any) -> float | None:
    """Convert runtime values into float when possible."""
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
    """Normalize raw field names for alias matching."""
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
    """Return the first matching value from a row."""
    for key in keys:
        if key in row:
            return row[key]

    normalized_items = {_normalize_key(str(key)): value for key, value in row.items()}
    for key in keys:
        normalized_key = _normalize_key(key)
        if normalized_key in normalized_items:
            return normalized_items[normalized_key]
    return None


def _pick_field(row: Mapping[str, Any], field_name: str) -> Any:
    """Pick one logical field from a raw row."""
    return _first_existing(row, MARKET_FIELD_ALIASES[field_name])


def _normalize_market_row(row: Mapping[str, Any], fallback_name: str | None = None) -> dict[str, Any]:
    """Normalize one market row into the project structure."""
    raw_code = _pick_field(row, "code")
    code = normalize_code(raw_code)
    if not code:
        return {}

    normalized_name = str(_pick_field(row, "name") or fallback_name or code)
    return {
        "code": code,
        "name": normalized_name,
        "latest_price": _safe_float(_pick_field(row, "latest_price")),
        "pct_change": _safe_float(_pick_field(row, "pct_change")),
        "turnover": _safe_float(_pick_field(row, "turnover")),
        "main_net_inflow": None,
        "volume": _safe_float(_pick_field(row, "volume")),
        "open": _safe_float(_pick_field(row, "open")),
        "high": _safe_float(_pick_field(row, "high")),
        "low": _safe_float(_pick_field(row, "low")),
        "close_prev": _safe_float(_pick_field(row, "close_prev")),
        "turnover_rate": _safe_float(_pick_field(row, "turnover_rate")),
        "amplitude": _safe_float(_pick_field(row, "amplitude")),
        "timestamp": str(_pick_field(row, "timestamp") or ""),
        "data_source": "primary",
    }


def _get_spot_dataframe_with_source() -> tuple[pd.DataFrame, str]:
    """Fetch the whole-market snapshot via the current primary data source."""
    last_error: Exception | None = None
    for func_name in SPOT_API_CANDIDATES:
        func = getattr(ak, func_name, None)
        if func is None:
            continue
        try:
            dataframe = func()
            if not isinstance(dataframe, pd.DataFrame):
                LOGGER.warning("Primary stock source akshare.%s did not return a DataFrame.", func_name)
                continue
            if dataframe.empty:
                LOGGER.warning("Primary stock source akshare.%s returned an empty DataFrame.", func_name)
            return dataframe, func_name
        except Exception as exc:  # pragma: no cover - runtime safety
            last_error = exc
            LOGGER.warning("Primary stock source akshare.%s failed: %s", func_name, exc)

    if last_error is not None:
        raise last_error
    raise AttributeError("No supported primary stock data API was found.")


def get_all_spot_data() -> dict[str, dict[str, Any]]:
    """Return the primary source whole-market snapshot."""
    try:
        dataframe, source_name = _get_spot_dataframe_with_source()
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Failed to fetch primary stock snapshot: %s", exc)
        return {}

    if dataframe.empty:
        LOGGER.warning("Primary stock snapshot is empty from %s.", source_name)
        return {}

    results: dict[str, dict[str, Any]] = {}
    try:
        for _, row in dataframe.iterrows():
            normalized = _normalize_market_row(row.to_dict())
            if normalized:
                results[normalized["code"]] = normalized
        LOGGER.info("Primary stock source fetched %s rows from %s.", len(results), source_name)
        return results
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Failed to normalize primary stock snapshot: %s", exc)
        return {}


def fetch_stock_data(code: str, name: str | None = None) -> dict[str, Any]:
    """Fetch one stock from the primary whole-market snapshot."""
    normalized_code = normalize_code(code)
    snapshot = get_all_spot_data()
    result = snapshot.get(normalized_code, {})
    if not result:
        LOGGER.warning("Primary stock source returned no row for %s.", normalized_code)
        return {}

    if name and not result.get("name"):
        result["name"] = name
    return result

