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


SPOT_API_CANDIDATES = (
    "stock_zh_a_spot_em",
    "stock_zh_a_spot",
)

MARKET_FIELD_ALIASES: dict[str, list[str]] = {
    "code": ["代码", "股票代码", "证券代码", "symbol", "code"],
    "name": ["名称", "股票名称", "证券简称", "简称", "name"],
    "latest_price": ["最新价", "现价", "最新", "最新价格", "收盘价", "price"],
    "pct_change": ["涨跌幅", "涨跌幅%", "涨跌幅(%)", "涨幅", "changepercent", "pct_change"],
    "change_amount": ["涨跌额", "涨跌", "涨跌值", "changeamount"],
    "turnover": ["成交额", "成交金额", "成交总额", "amount", "turnover"],
    "volume": ["成交量", "总手", "volume"],
    "open": ["今开", "开盘价", "open"],
    "high": ["最高", "最高价", "high"],
    "low": ["最低", "最低价", "low"],
    "close_prev": ["昨收", "昨收价", "previous_close", "close_prev"],
    "turnover_rate": ["换手率", "turnoverrate", "turnover_rate"],
    "amplitude": ["振幅", "amplitude"],
    "pe_ratio": ["市盈率-动态", "市盈率", "pe", "pe_ratio"],
    "timestamp": ["时间戳", "时间", "更新时间", "timestamp"],
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
    return _first_existing(row, MARKET_FIELD_ALIASES[field_name])


def _normalize_market_row(row: Mapping[str, Any]) -> dict[str, Any]:
    raw_code = _pick_field(row, "code")
    code = normalize_code(raw_code)
    if not code:
        return {}

    return {
        "code": code,
        "raw_code": str(raw_code).strip() if raw_code is not None else "",
        "name": _pick_field(row, "name") or code,
        "latest_price": _safe_float(_pick_field(row, "latest_price")),
        "pct_change": _safe_float(_pick_field(row, "pct_change")),
        "change_amount": _safe_float(_pick_field(row, "change_amount")),
        "turnover": _safe_float(_pick_field(row, "turnover")),
        "volume": _safe_float(_pick_field(row, "volume")),
        "open": _safe_float(_pick_field(row, "open")),
        "high": _safe_float(_pick_field(row, "high")),
        "low": _safe_float(_pick_field(row, "low")),
        "close_prev": _safe_float(_pick_field(row, "close_prev")),
        "turnover_rate": _safe_float(_pick_field(row, "turnover_rate")),
        "amplitude": _safe_float(_pick_field(row, "amplitude")),
        "pe_ratio": _safe_float(_pick_field(row, "pe_ratio")),
        "timestamp": str(_pick_field(row, "timestamp") or ""),
    }


def _get_spot_dataframe_with_source() -> tuple[pd.DataFrame, str]:
    last_error: Exception | None = None
    for func_name in SPOT_API_CANDIDATES:
        func = getattr(ak, func_name, None)
        if func is None:
            continue
        try:
            dataframe = func()
            if not isinstance(dataframe, pd.DataFrame):
                LOGGER.warning("akshare.%s did not return a DataFrame.", func_name)
                continue
            LOGGER.info("Fetched market spot data via akshare.%s.", func_name)
            LOGGER.info("Spot DataFrame columns via %s: %s", func_name, list(dataframe.columns))
            if dataframe.empty:
                LOGGER.warning("akshare.%s returned an empty DataFrame.", func_name)
            return dataframe, func_name
        except Exception as exc:  # pragma: no cover
            last_error = exc
            LOGGER.warning("akshare.%s failed: %s", func_name, exc)

    if last_error is not None:
        raise last_error
    raise AttributeError("No supported akshare A-share spot API was found.")


def get_all_spot_data() -> dict[str, dict[str, Any]]:
    try:
        dataframe, source_name = _get_spot_dataframe_with_source()
    except Exception as exc:  # pragma: no cover
        LOGGER.exception("Failed to fetch A-share spot data: %s", exc)
        return {}

    if dataframe.empty:
        LOGGER.warning(
            "Spot market DataFrame is empty from %s. Please check network connectivity, proxy settings, or akshare data source status.",
            source_name,
        )
        return {}

    results: dict[str, dict[str, Any]] = {}
    try:
        for _, row in dataframe.iterrows():
            normalized = _normalize_market_row(row.to_dict())
            if normalized:
                results[normalized["code"]] = normalized
        LOGGER.info("Fetched spot data for %s stocks.", len(results))
        return results
    except Exception as exc:  # pragma: no cover
        LOGGER.exception("Failed to normalize A-share spot data: %s", exc)
        return {}


def get_stock_by_code(code: str) -> dict[str, Any]:
    normalized_code = normalize_code(code)
    stock_data = get_all_spot_data().get(normalized_code)
    if stock_data:
        return stock_data

    LOGGER.warning(
        "No market data found for stock code %s. Possible reasons: interface fields changed, data source is incomplete, or the code was not present in the latest snapshot.",
        normalized_code,
    )
    return {}


def self_test(codes: list[str] | None = None) -> None:
    test_codes = codes or ["300750", "300308", "600519"]
    print("开始执行 free_market_provider 自测...")

    try:
        dataframe, source_name = _get_spot_dataframe_with_source()
    except Exception as exc:
        print(f"实时行情接口调用失败: {exc}")
        return

    print(f"使用接口: {source_name}")
    print(f"DataFrame 行数: {len(dataframe)}")
    print(f"DataFrame 列名: {list(dataframe.columns)}")

    code_series = dataframe.iloc[:, 0].astype(str)
    for code in test_codes:
        normalized_code = normalize_code(code)
        subset = dataframe.loc[code_series.map(normalize_code) == normalized_code]
        print("-" * 80)
        print(f"目标代码: {normalized_code}")
        print(f"匹配条数: {len(subset)}")
        if subset.empty:
            print("未匹配到行情数据。")
            continue
        print(subset.head(1).to_dict(orient="records")[0])
