from __future__ import annotations

import json
import time
import unicodedata
from typing import Any

import akshare as ak
import pandas as pd

from config import CACHE_DIR, LOGGER, ensure_runtime_directories
from modules.providers import stock_primary_provider


_CACHE_TTL_SECONDS = 60 * 60 * 6
_CACHE_FILE_PATH = CACHE_DIR / "stock_basic_a.json"
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


def _normalize_text(value: Any) -> str:
    """Normalize user-facing text for robust Chinese/code matching."""
    return (
        unicodedata.normalize("NFKC", str(value or ""))
        .strip()
        .lower()
        .replace(" ", "")
        .replace("\u3000", "")
    )


def _normalize_stock_rows(dataframe: pd.DataFrame, *, code_field: str | None = None, name_field: str | None = None) -> list[dict[str, str]]:
    """Normalize stock rows into frontend search options."""
    if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
        return []

    rows: list[dict[str, str]] = []
    seen_codes: set[str] = set()
    resolved_code_field = code_field or ("code" if "code" in dataframe.columns else "symbol")
    resolved_name_field = name_field or ("name" if "name" in dataframe.columns else "名称")
    for _, row in dataframe.iterrows():
        code = stock_primary_provider.normalize_code(
            row.get(resolved_code_field) or row.get("symbol") or row.get("ts_code")
        )
        name = str(row.get(resolved_name_field) or row.get("name") or row.get("名称") or "").strip()
        if not code or not name or code in seen_codes:
            continue
        rows.append({"code": code, "name": name})
        seen_codes.add(code)
    return rows


def _save_stock_basic_cache(rows: list[dict[str, str]]) -> None:
    """Persist the latest full A-share stock list for offline fallback."""
    if not rows:
        return
    ensure_runtime_directories()
    _CACHE_FILE_PATH.write_text(
        json.dumps(
            {
                "updated_at": int(time.time()),
                "items": rows,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _load_stock_basic_cache_file() -> list[dict[str, str]]:
    """Load the last successful full A-share stock list from disk."""
    if not _CACHE_FILE_PATH.exists():
        return []
    try:
        payload = json.loads(_CACHE_FILE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        LOGGER.warning("Failed to read stock basic cache file: %s", exc)
        return []
    items = payload.get("items", [])
    if not isinstance(items, list):
        return []
    rows: list[dict[str, str]] = []
    seen_codes: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        code = stock_primary_provider.normalize_code(item.get("code"))
        name = str(item.get("name") or "").strip()
        if not code or not name or code in seen_codes:
            continue
        rows.append({"code": code, "name": name})
        seen_codes.add(code)
    return rows


def _fetch_akshare_stock_basic() -> list[dict[str, str]]:
    """Fetch the latest full A-share code/name table via AkShare."""
    dataframe = ak.stock_info_a_code_name()
    rows = _normalize_stock_rows(dataframe, code_field="code", name_field="name")
    if not rows:
        raise ValueError("akshare stock_info_a_code_name returned no rows")
    LOGGER.info("Loaded %s A-share stock basics via akshare.stock_info_a_code_name.", len(rows))
    return rows


def _fetch_tushare_stock_basic() -> list[dict[str, str]]:
    """Fallback to Tushare stock_basic when available."""
    pro = stock_primary_provider._get_tushare_pro()
    if pro is None:
        return []
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
    rows = _normalize_stock_rows(dataframe, code_field="symbol", name_field="name")
    if rows:
        LOGGER.info("Loaded %s A-share stock basics via Tushare stock_basic.", len(rows))
    return rows


def _load_stock_basic() -> list[dict[str, str]]:
    """Load and cache the full A-share stock list with on-disk fallback."""
    global _STOCK_BASIC_CACHE, _STOCK_BASIC_CACHE_TS

    now = time.time()
    if _STOCK_BASIC_CACHE is not None and now - _STOCK_BASIC_CACHE_TS < _CACHE_TTL_SECONDS:
        return _STOCK_BASIC_CACHE

    cached_rows = _load_stock_basic_cache_file()

    try:
        rows = _fetch_akshare_stock_basic()
        _save_stock_basic_cache(rows)
        _STOCK_BASIC_CACHE_TS = now
        _STOCK_BASIC_CACHE = rows
        return _STOCK_BASIC_CACHE
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("AkShare stock basic failed, trying fallback sources: %s", exc)

    try:
        rows = _fetch_tushare_stock_basic()
        if rows:
            _save_stock_basic_cache(rows)
            _STOCK_BASIC_CACHE_TS = now
            _STOCK_BASIC_CACHE = rows
            return _STOCK_BASIC_CACHE
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Tushare stock_basic failed, trying cache fallback: %s", exc)

    if cached_rows:
        LOGGER.info("Using cached A-share stock basics from %s. row_count=%s", _CACHE_FILE_PATH, len(cached_rows))
        _STOCK_BASIC_CACHE = cached_rows
        _STOCK_BASIC_CACHE_TS = now
        return _STOCK_BASIC_CACHE

    LOGGER.warning("Stock basic sources unavailable; using minimal fallback stock list.")
    _STOCK_BASIC_CACHE = list(_FALLBACK_STOCKS)
    _STOCK_BASIC_CACHE_TS = now
    return _STOCK_BASIC_CACHE


def _is_subsequence(query: str, target: str) -> bool:
    """Support loose Chinese abbreviation matching, e.g. 亿纬 -> 亿纬锂能."""
    if not query or not target:
        return False
    index = 0
    for char in target:
        if index < len(query) and char == query[index]:
            index += 1
            if index == len(query):
                return True
    return index == len(query)


def search_stocks(keyword: str, limit: int = 12) -> list[dict[str, Any]]:
    """Search the full A-share stock list by code, Chinese full name, or fuzzy Chinese keyword."""
    query = str(keyword or "").strip()
    if not query:
        return []

    query_digits = "".join(ch for ch in query if ch.isdigit())
    normalized_query = stock_primary_provider.normalize_code(query_digits) if len(query_digits) >= 6 else ""
    normalized_text_query = _normalize_text(query)
    candidates = _load_stock_basic()

    matched: list[tuple[int, int, dict[str, str]]] = []
    for item in candidates:
        code = item["code"]
        name = item["name"]
        normalized_name = _normalize_text(name)

        if normalized_query and code == normalized_query:
            rank = 0
            tie_break = 0
        elif query_digits and code.startswith(query_digits):
            rank = 1
            tie_break = len(code) - len(query_digits)
        elif query_digits and query_digits in code:
            rank = 2
            tie_break = code.index(query_digits)
        elif normalized_text_query and normalized_name == normalized_text_query:
            rank = 3
            tie_break = 0
        elif normalized_text_query and normalized_name.startswith(normalized_text_query):
            rank = 4
            tie_break = len(normalized_name) - len(normalized_text_query)
        elif normalized_text_query and normalized_text_query in normalized_name:
            rank = 5
            tie_break = normalized_name.index(normalized_text_query)
        elif normalized_text_query and _is_subsequence(normalized_text_query, normalized_name):
            rank = 6
            tie_break = len(normalized_name)
        else:
            continue
        matched.append((rank, tie_break, item))

    matched.sort(key=lambda pair: (pair[0], pair[1], pair[2]["code"]))
    return [item for _, _, item in matched[: max(1, min(limit, 30))]]
