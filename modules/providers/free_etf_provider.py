from __future__ import annotations

import re
from typing import Any, Mapping

import akshare as ak
import pandas as pd

from config import LOGGER


def normalize_code(code: Any) -> str:
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


ETF_SPOT_API_CANDIDATES = (
    "fund_etf_spot_em",
    "fund_etf_spot_ths",
)

ETF_FIELD_ALIASES: dict[str, list[str]] = {
    "code": ["代码", "基金代码", "证券代码", "symbol", "code"],
    "name": ["名称", "基金简称", "基金名称", "name"],
    "latest_price": [
        "最新价",
        "现价",
        "最新",
        "当前-单位净值",
        "最新-单位净值",
        "单位净值",
        "price",
    ],
    "pct_change": ["涨跌幅", "涨跌幅%", "增长率", "涨幅", "pct_change"],
    "turnover": ["成交额", "成交金额", "成交总额", "amount", "turnover"],
    "volume": ["成交量", "总手", "volume"],
    "main_net_inflow": [
        "主力净流入-净额",
        "主力净流入",
        "主力资金净流入",
        "main_net_inflow",
    ],
    "timestamp": ["更新时间", "查询日期", "数据日期", "timestamp"],
}


def _safe_float(value: Any) -> float | None:
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
    return _first_existing(row, ETF_FIELD_ALIASES[field_name])


def _normalize_etf_row(row: Mapping[str, Any]) -> dict[str, Any]:
    raw_code = _pick_field(row, "code")
    code = normalize_code(raw_code)
    if not code:
        return {}

    main_net_inflow = _safe_float(_pick_field(row, "main_net_inflow"))
    if main_net_inflow is None:
        fund_direction = "未知"
    elif main_net_inflow > 0:
        fund_direction = "流入"
    elif main_net_inflow < 0:
        fund_direction = "流出"
    else:
        fund_direction = "持平"

    return {
        "code": code,
        "name": _pick_field(row, "name") or code,
        "latest_price": _safe_float(_pick_field(row, "latest_price")),
        "pct_change": _safe_float(_pick_field(row, "pct_change")),
        "turnover": _safe_float(_pick_field(row, "turnover")),
        "volume": _safe_float(_pick_field(row, "volume")),
        "main_net_inflow": main_net_inflow,
        "fund_direction": fund_direction,
        "timestamp": str(_pick_field(row, "timestamp") or ""),
    }


def _get_etf_dataframe_with_source() -> tuple[pd.DataFrame, str]:
    last_error: Exception | None = None
    for func_name in ETF_SPOT_API_CANDIDATES:
        func = getattr(ak, func_name, None)
        if func is None:
            continue
        try:
            dataframe = func()
            if not isinstance(dataframe, pd.DataFrame):
                LOGGER.warning("akshare.%s did not return a DataFrame.", func_name)
                continue
            LOGGER.info("Fetched ETF spot data via akshare.%s.", func_name)
            LOGGER.info("ETF DataFrame columns via %s: %s", func_name, list(dataframe.columns))
            if dataframe.empty:
                LOGGER.warning("akshare.%s returned an empty DataFrame.", func_name)
            return dataframe, func_name
        except Exception as exc:  # pragma: no cover
            last_error = exc
            LOGGER.warning("akshare.%s failed: %s", func_name, exc)

    if last_error is not None:
        raise last_error
    raise AttributeError("No supported akshare ETF spot API was found.")


def get_all_etf_spot_data() -> dict[str, dict[str, Any]]:
    try:
        dataframe, source_name = _get_etf_dataframe_with_source()
    except Exception as exc:  # pragma: no cover
        LOGGER.exception("Failed to fetch ETF spot data: %s", exc)
        return {}

    if dataframe.empty:
        LOGGER.warning(
            "ETF DataFrame is empty from %s. Please check network connectivity, proxy settings, or akshare data source status.",
            source_name,
        )
        return {}

    results: dict[str, dict[str, Any]] = {}
    try:
        for _, row in dataframe.iterrows():
            normalized = _normalize_etf_row(row.to_dict())
            if normalized:
                results[normalized["code"]] = normalized
        LOGGER.info("Fetched ETF spot data for %s ETFs.", len(results))
        return results
    except Exception as exc:  # pragma: no cover
        LOGGER.exception("Failed to normalize ETF spot data: %s", exc)
        return {}


def get_etf_by_code(code: str) -> dict[str, Any]:
    normalized_code = normalize_code(code)
    etf_data = get_all_etf_spot_data().get(normalized_code)
    if etf_data:
        return etf_data

    LOGGER.warning(
        "No ETF data found for code %s. Possible reasons: interface fields changed, data source is incomplete, or the code was not present in the latest snapshot.",
        normalized_code,
    )
    return {}
