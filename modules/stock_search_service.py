from __future__ import annotations

import time
from collections import Counter
from typing import Any

import pandas as pd

from config import LOGGER
from modules.providers import stock_primary_provider


_CACHE_TTL_SECONDS = 60 * 60 * 6
_STOCK_BASIC_CACHE: list[dict[str, str]] | None = None
_STOCK_BASIC_CACHE_TS = 0.0

_FALLBACK_STOCKS = [
    {"code": "300750", "name": "宁德时代"},
    {"code": "002594", "name": "比亚迪"},
    {"code": "601318", "name": "中国平安"},
    {"code": "600036", "name": "招商银行"},
    {"code": "000333", "name": "美的集团"},
    {"code": "600519", "name": "贵州茅台"},
    {"code": "300308", "name": "中际旭创"},
]


def _normalize_stock_rows(dataframe: pd.DataFrame) -> list[dict[str, str]]:
    """Normalize Tushare stock_basic rows into frontend search options."""
    if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
        return []

    rows: list[dict[str, str]] = []
    seen_codes: set[str] = set()
    for _, row in dataframe.iterrows():
        code = stock_primary_provider.normalize_code(row.get("symbol") or row.get("ts_code"))
        name = str(row.get("name") or "").strip()
        if not code or not name or code in seen_codes:
            continue
        rows.append({"code": code, "name": name})
        seen_codes.add(code)
    return rows


def _load_stock_basic() -> list[dict[str, str]]:
    """Load and cache Tushare stock_basic; fallback keeps search usable without token."""
    global _STOCK_BASIC_CACHE, _STOCK_BASIC_CACHE_TS

    now = time.time()
    if _STOCK_BASIC_CACHE is not None and now - _STOCK_BASIC_CACHE_TS < _CACHE_TTL_SECONDS:
        return _STOCK_BASIC_CACHE

    pro = stock_primary_provider._get_tushare_pro()
    if pro is None:
        _STOCK_BASIC_CACHE = list(_FALLBACK_STOCKS)
        _STOCK_BASIC_CACHE_TS = now
        return _STOCK_BASIC_CACHE

    try:
        dataframe = stock_primary_provider._call_tushare_with_retry(
            lambda: pro.stock_basic(
                exchange="",
                list_status="L",
                fields="ts_code,symbol,name",
            ),
            description="search stock_basic",
            retries=1,
            timeout_seconds=5.0,
        )
        rows = _normalize_stock_rows(dataframe)
        _STOCK_BASIC_CACHE = rows or list(_FALLBACK_STOCKS)
        _STOCK_BASIC_CACHE_TS = now
        return _STOCK_BASIC_CACHE
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Search stock_basic failed, using fallback stocks: %s", exc)
        _STOCK_BASIC_CACHE = list(_FALLBACK_STOCKS)
        _STOCK_BASIC_CACHE_TS = now
        return _STOCK_BASIC_CACHE


def _digits_match_loose(code: str, digits: str) -> bool:
    """Support short fuzzy code input, e.g. 700 can still surface 300750."""
    if not digits:
        return False
    code_counts = Counter(code)
    return all(code_counts[digit] >= count for digit, count in Counter(digits).items())


def search_stocks(keyword: str, limit: int = 12) -> list[dict[str, Any]]:
    """Search stocks by code or name for the watchlist selector."""
    query = str(keyword or "").strip()
    if not query:
        return []

    query_digits = "".join(ch for ch in query if ch.isdigit())
    normalized_query = stock_primary_provider.normalize_code(query_digits) if len(query_digits) >= 6 else ""
    query_lower = query.lower()
    candidates = _load_stock_basic()

    matched: list[tuple[int, dict[str, str]]] = []
    for item in candidates:
        code = item["code"]
        name = item["name"]
        name_lower = name.lower()

        if normalized_query and code == normalized_query:
            rank = 0
        elif query_digits and code.startswith(query_digits):
            rank = 1
        elif query_digits and query_digits in code:
            rank = 2
        elif query_lower and name_lower.startswith(query_lower):
            rank = 3
        elif query_lower and query_lower in name_lower:
            rank = 4
        elif query_digits and _digits_match_loose(code, query_digits):
            rank = 5
        else:
            continue
        matched.append((rank, item))

    matched.sort(key=lambda pair: (pair[0], pair[1]["code"]))
    return [item for _, item in matched[: max(1, min(limit, 30))]]
