from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any

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


def _safe_float(value: Any) -> float | None:
    """Convert arbitrary values into float when possible."""
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


def _get_recent_hist(code: str) -> pd.DataFrame | None:
    """Use recent daily history as a backup source when realtime snapshot is unstable."""
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=15)).strftime("%Y%m%d")

    try:
        dataframe = ak.stock_zh_a_hist(
            symbol=normalize_code(code),
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
        if isinstance(dataframe, pd.DataFrame) and not dataframe.empty:
            return dataframe
        return None
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Backup stock source failed for %s: %s", code, exc)
        return None


def fetch_stock_data(code: str, name: str | None = None) -> dict[str, Any]:
    """Fetch one stock via backup daily-history data."""
    normalized_code = normalize_code(code)
    dataframe = _get_recent_hist(normalized_code)
    if dataframe is None or dataframe.empty:
        return {}

    try:
        latest_row = dataframe.iloc[-1].to_dict()
        latest_price = _safe_float(latest_row.get("收盘"))
        pct_change = _safe_float(latest_row.get("涨跌幅"))
        turnover = _safe_float(latest_row.get("成交额"))

        return {
            "code": normalized_code,
            "name": str(name or normalized_code),
            "latest_price": latest_price,
            "pct_change": pct_change,
            "turnover": turnover,
            "main_net_inflow": None,
            "volume": _safe_float(latest_row.get("成交量")),
            "open": _safe_float(latest_row.get("开盘")),
            "high": _safe_float(latest_row.get("最高")),
            "low": _safe_float(latest_row.get("最低")),
            "close_prev": None,
            "turnover_rate": _safe_float(latest_row.get("换手率")),
            "amplitude": _safe_float(latest_row.get("振幅")),
            "timestamp": str(latest_row.get("日期") or ""),
            "data_source": "backup",
        }
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Failed to normalize backup stock data for %s: %s", normalized_code, exc)
        return {}

