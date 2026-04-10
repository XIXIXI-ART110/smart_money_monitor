from __future__ import annotations

import math
import time
from datetime import datetime
from typing import Any

import akshare as ak
import pandas as pd

from config import LOGGER
from modules.etf_service import analyze_single_etf_service


DEFAULT_SELECTED_INDEX_CODES = [
    "sh000001",
    "sz399001",
    "sz399006",
    "sh000016",
    "sz399330",
    "global_ndx",
    "bj899050",
    "sz399303",
]

INDEX_LIBRARY: list[dict[str, str]] = [
    {"code": "sh000001", "name": "上证指数", "category": "宽基", "style": "大盘价值", "etf_code": "510210", "etf_name": "上证ETF", "source": "cn"},
    {"code": "sz399001", "name": "深证成指", "category": "宽基", "style": "成长均衡", "etf_code": "159903", "etf_name": "深成ETF", "source": "cn"},
    {"code": "sz399006", "name": "创业板指", "category": "成长", "style": "成长科技", "etf_code": "159915", "etf_name": "创业板ETF", "source": "cn"},
    {"code": "sh000016", "name": "上证50", "category": "宽基", "style": "核心权重", "etf_code": "510050", "etf_name": "上证50ETF", "source": "cn"},
    {"code": "sz399330", "name": "深证100", "category": "宽基", "style": "核心龙头", "etf_code": "159901", "etf_name": "深证100ETF", "source": "cn"},
    {"code": "global_ndx", "name": "纳斯达克100", "category": "海外", "style": "海外科技", "etf_code": "513100", "etf_name": "纳指ETF", "source": "global", "global_symbol": "NDX", "us_symbol": ".NDX"},
    {"code": "bj899050", "name": "北证50", "category": "北交所", "style": "专精特新", "etf_code": "920082", "etf_name": "北证50ETF", "source": "cn_daily"},
    {"code": "sz399303", "name": "国证2000", "category": "小盘", "style": "小盘成长", "etf_code": "159628", "etf_name": "国证2000ETF", "source": "cn"},
    {"code": "sh000300", "name": "沪深300", "category": "宽基", "style": "核心宽基", "etf_code": "510300", "etf_name": "沪深300ETF", "source": "cn"},
    {"code": "sh000905", "name": "中证500", "category": "小盘", "style": "中盘均衡", "etf_code": "510500", "etf_name": "中证500ETF", "source": "cn"},
    {"code": "sh000852", "name": "中证1000", "category": "小盘", "style": "中小盘弹性", "etf_code": "512100", "etf_name": "中证1000ETF", "source": "cn"},
    {"code": "sh000688", "name": "科创50", "category": "成长", "style": "硬科技", "etf_code": "588000", "etf_name": "科创50ETF", "source": "cn"},
]


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("%", "").strip()
        if cleaned in {"", "-", "None", "nan", "NaN"}:
            return None
        value = cleaned
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed):
        return None
    return parsed


def _round(value: Any, digits: int = 2) -> float | None:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return round(parsed, digits)


def _format_index_code(raw_code: Any) -> str:
    code = str(raw_code or "").strip().lower()
    if code.startswith(("sh", "sz", "bj", "global_")):
        return code
    digits = "".join(ch for ch in code if ch.isdigit())
    if len(digits) != 6:
        return code
    if digits.startswith("899"):
        return f"sh{digits}"
    return f"sh{digits}" if digits.startswith(("0", "5", "6", "9")) else f"sz{digits}"


def _calc_amplitude(snapshot: dict[str, Any]) -> str:
    high = _safe_float(snapshot.get("high"))
    low = _safe_float(snapshot.get("low"))
    prev_close = _safe_float(snapshot.get("prev_close"))
    if high is None or low is None or prev_close in {None, 0}:
        return "--"
    amplitude = (high - low) / prev_close * 100
    return f"{amplitude:.2f}%"


def _build_sparkline(snapshot: dict[str, Any], count: int = 18) -> list[float]:
    prev_close = _safe_float(snapshot.get("prev_close")) or _safe_float(snapshot.get("value")) or 1.0
    open_price = _safe_float(snapshot.get("open")) or prev_close
    high = _safe_float(snapshot.get("high")) or max(open_price, prev_close)
    low = _safe_float(snapshot.get("low")) or min(open_price, prev_close)
    latest = _safe_float(snapshot.get("value")) or open_price

    anchors = [
        prev_close,
        open_price,
        (open_price + high) / 2,
        high,
        (high + latest) / 2,
        latest,
        (latest + low) / 2,
        low,
        (low + latest) / 2,
        latest,
    ]

    points: list[float] = []
    for index in range(len(anchors) - 1):
        start = anchors[index]
        end = anchors[index + 1]
        for step in range(2):
            ratio = step / 2
            wave = math.sin(ratio * math.pi) * max(abs(end - start) * 0.12, 0.02)
            direction = 1 if index % 2 == 0 else -1
            points.append(start + (end - start) * ratio + wave * direction)
    points.append(anchors[-1])
    if len(points) > count:
        step = len(points) / count
        points = [points[min(int(index * step), len(points) - 1)] for index in range(count)]
    return [round(point, 3) for point in points]


def _derive_signal(change_pct: float | None) -> str:
    pct = float(change_pct or 0.0)
    if pct >= 1:
        return "强势修复"
    if pct > -0.8:
        return "震荡偏弱" if pct < 0 else "震荡偏强"
    return "震荡偏弱"


def _derive_style(meta: dict[str, str], change_pct: float | None) -> str:
    pct = float(change_pct or 0.0)
    base_style = meta.get("style", meta.get("category", "宽基"))
    if pct >= 1:
        return f"{base_style}走强"
    if pct <= -1:
        return f"{base_style}承压"
    return base_style


def _derive_summary(meta: dict[str, str], snapshot: dict[str, Any]) -> str:
    pct = _safe_float(snapshot.get("change_pct")) or 0.0
    style = _derive_style(meta, pct)
    if pct >= 1:
        return f"今日{meta['name']}偏强运行，{style}方向活跃，短线情绪有所修复。"
    if pct <= -1:
        return f"今日{meta['name']}承压震荡，{style}方向偏弱，资金态度相对谨慎。"
    return f"今日{meta['name']}以区间震荡为主，{style}方向暂未走出单边趋势。"


def _build_cn_snapshot_map() -> dict[str, dict[str, Any]]:
    try:
        dataframe = ak.stock_zh_index_spot_sina()
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Failed to fetch CN index spot data: %s", exc)
        return {}

    code_col, name_col, price_col, change_col, pct_col = dataframe.columns[:5]
    prev_close_col = dataframe.columns[5] if len(dataframe.columns) > 5 else None
    open_col = dataframe.columns[6] if len(dataframe.columns) > 6 else None
    high_col = dataframe.columns[7] if len(dataframe.columns) > 7 else None
    low_col = dataframe.columns[8] if len(dataframe.columns) > 8 else None
    volume_col = dataframe.columns[9] if len(dataframe.columns) > 9 else None
    amount_col = dataframe.columns[10] if len(dataframe.columns) > 10 else None

    snapshots: dict[str, dict[str, Any]] = {}
    for _, row in dataframe.iterrows():
        code = _format_index_code(row.get(code_col))
        snapshots[code] = {
            "code": code,
            "name": str(row.get(name_col) or code),
            "value": _round(row.get(price_col), 3),
            "change": _round(row.get(change_col), 3),
            "change_pct": _round(row.get(pct_col), 3),
            "prev_close": _round(row.get(prev_close_col), 3) if prev_close_col else None,
            "open": _round(row.get(open_col), 3) if open_col else None,
            "high": _round(row.get(high_col), 3) if high_col else None,
            "low": _round(row.get(low_col), 3) if low_col else None,
            "volume": _round(row.get(volume_col), 3) if volume_col else None,
            "amount": _round(row.get(amount_col), 3) if amount_col else None,
        }
    return snapshots


def _build_global_snapshot_map() -> dict[str, dict[str, Any]]:
    try:
        dataframe = ak.index_global_spot_em()
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Failed to fetch global index spot data: %s", exc)
        return {}

    code_col = "代码"
    name_col = "名称"
    snapshots: dict[str, dict[str, Any]] = {}
    for _, row in dataframe.iterrows():
        code = str(row.get(code_col) or "").strip().upper()
        snapshots[code] = {
            "code": code,
            "name": str(row.get(name_col) or code),
            "value": _round(row.get("最新价"), 3),
            "change": _round(row.get("涨跌额"), 3),
            "change_pct": _round(row.get("涨跌幅"), 3),
            "prev_close": _round(row.get("昨收价"), 3),
            "open": _round(row.get("开盘价"), 3),
            "high": _round(row.get("最高价"), 3),
            "low": _round(row.get("最低价"), 3),
            "volume": None,
            "amount": None,
        }
    return snapshots


def _build_fallback_snapshot(meta: dict[str, str], etf_data: dict[str, Any]) -> dict[str, Any]:
    price = _round(etf_data.get("price"), 3) or _round(etf_data.get("latest_price"), 3)
    change_pct = _round(etf_data.get("change_pct"), 3)
    if price is None:
        price = 0.0
    if change_pct is None:
        change_pct = 0.0
    prev_close = round(price / (1 + change_pct / 100), 3) if price and change_pct != -100 else price
    change = round(price - prev_close, 3) if price is not None and prev_close is not None else None
    high = round(max(price, prev_close) * 1.008, 3) if price is not None else None
    low = round(min(price, prev_close) * 0.992, 3) if price is not None else None
    return {
        "code": meta["code"],
        "name": meta["name"],
        "value": price,
        "change": change,
        "change_pct": change_pct,
        "prev_close": prev_close,
        "open": prev_close,
        "high": high,
        "low": low,
        "volume": None,
        "amount": None,
    }


def _build_cn_daily_snapshot(symbol: str) -> dict[str, Any]:
    try:
        dataframe = ak.stock_zh_index_daily(symbol=symbol)
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Failed to fetch daily index data for %s: %s", symbol, exc)
        return {}

    if not isinstance(dataframe, pd.DataFrame) or len(dataframe) < 2:
        return {}

    latest = dataframe.iloc[-1]
    previous = dataframe.iloc[-2]
    latest_close = _safe_float(latest.get("close"))
    previous_close = _safe_float(previous.get("close"))
    if latest_close is None or previous_close in {None, 0}:
        return {}

    change = latest_close - previous_close
    change_pct = change / previous_close * 100
    return {
        "code": symbol,
        "name": symbol,
        "value": round(latest_close, 3),
        "change": round(change, 3),
        "change_pct": round(change_pct, 3),
        "prev_close": round(previous_close, 3),
        "open": _round(latest.get("open"), 3),
        "high": _round(latest.get("high"), 3),
        "low": _round(latest.get("low"), 3),
        "volume": _round(latest.get("volume"), 3),
        "amount": None,
    }


def _build_us_daily_snapshot(symbol: str) -> dict[str, Any]:
    try:
        dataframe = ak.index_us_stock_sina(symbol=symbol)
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Failed to fetch US index data for %s: %s", symbol, exc)
        return {}

    if not isinstance(dataframe, pd.DataFrame) or len(dataframe) < 2:
        return {}

    latest = dataframe.iloc[-1]
    previous = dataframe.iloc[-2]
    latest_close = _safe_float(latest.get("close"))
    previous_close = _safe_float(previous.get("close"))
    if latest_close is None or previous_close in {None, 0}:
        return {}

    change = latest_close - previous_close
    change_pct = change / previous_close * 100
    return {
        "code": symbol,
        "name": symbol,
        "value": round(latest_close, 3),
        "change": round(change, 3),
        "change_pct": round(change_pct, 3),
        "prev_close": round(previous_close, 3),
        "open": _round(latest.get("open"), 3),
        "high": _round(latest.get("high"), 3),
        "low": _round(latest.get("low"), 3),
        "volume": _round(latest.get("volume"), 3),
        "amount": None,
    }


def _resolve_snapshot(meta: dict[str, str], cn_map: dict[str, dict[str, Any]], global_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    source = meta.get("source", "cn")
    if source == "cn":
        return cn_map.get(meta["code"], {})
    if source == "cn_daily":
        return _build_cn_daily_snapshot(meta["code"])
    if source == "global":
        snapshot = global_map.get(meta.get("global_symbol", "").upper(), {})
        if snapshot:
            return snapshot
        us_snapshot = _build_us_daily_snapshot(meta.get("us_symbol", ""))
        if us_snapshot:
            return us_snapshot
        related_result = analyze_single_etf_service(meta["etf_code"])
        if related_result.get("ok"):
            return _build_fallback_snapshot(meta, related_result.get("etf", {}))
        return {}

    related_result = analyze_single_etf_service(meta["etf_code"])
    if related_result.get("ok"):
        return _build_fallback_snapshot(meta, related_result.get("etf", {}))
    return {}


def _build_card(meta: dict[str, str], snapshot: dict[str, Any]) -> dict[str, Any]:
    pct_change = _safe_float(snapshot.get("change_pct"))
    return {
        "code": meta["code"],
        "name": meta["name"],
        "value": snapshot.get("value"),
        "change": snapshot.get("change"),
        "change_pct": snapshot.get("change_pct"),
        "sparkline": _build_sparkline(snapshot),
        "category": meta.get("category", "宽基"),
        "etf_code": meta["etf_code"],
        "etf_name": meta["etf_name"],
        "signal": _derive_signal(pct_change),
        "style": _derive_style(meta, pct_change),
    }


def _build_detail(meta: dict[str, str], snapshot: dict[str, Any]) -> dict[str, Any]:
    pct_change = _safe_float(snapshot.get("change_pct"))
    related_result = analyze_single_etf_service(meta["etf_code"])
    related_etf = related_result.get("etf", {}) if related_result.get("ok") else {}
    etf_suggestion = str(related_etf.get("suggestion") or "适合观察，不宜追高")

    return {
        "code": meta["code"],
        "name": meta["name"],
        "value": snapshot.get("value"),
        "change": snapshot.get("change"),
        "change_pct": snapshot.get("change_pct"),
        "amplitude": _calc_amplitude(snapshot),
        "signal": _derive_signal(pct_change),
        "style": _derive_style(meta, pct_change),
        "summary": _derive_summary(meta, snapshot),
        "sparkline": _build_sparkline(snapshot, count=30),
        "etf": {
            "code": meta["etf_code"],
            "name": meta["etf_name"],
            "suggestion": etf_suggestion,
        },
    }


def _load_snapshot_context() -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    return _build_cn_snapshot_map(), _build_global_snapshot_map()


def _lookup_meta(code: str) -> dict[str, str] | None:
    normalized_code = _format_index_code(code)
    return next((item for item in INDEX_LIBRARY if item["code"] == normalized_code), None)


def get_indexes_service() -> dict[str, Any]:
    start_time = time.perf_counter()
    cn_map, global_map = _load_snapshot_context()
    indexes: list[dict[str, Any]] = []

    for code in DEFAULT_SELECTED_INDEX_CODES:
        meta = _lookup_meta(code)
        if meta is None:
            continue
        snapshot = _resolve_snapshot(meta, cn_map, global_map)
        if not snapshot:
            snapshot = {
                "value": None,
                "change": None,
                "change_pct": None,
                "prev_close": None,
                "open": None,
                "high": None,
                "low": None,
                "volume": None,
                "amount": None,
            }
        indexes.append(_build_card(meta, snapshot))

    return {
        "ok": True,
        "indexes": indexes,
        "elapsed_seconds": round(time.perf_counter() - start_time, 3),
    }


def get_index_options_service() -> dict[str, Any]:
    start_time = time.perf_counter()
    cn_map, global_map = _load_snapshot_context()
    options: list[dict[str, Any]] = []

    for meta in INDEX_LIBRARY:
        snapshot = _resolve_snapshot(meta, cn_map, global_map)
        if not snapshot:
            snapshot = {
                "value": None,
                "change": None,
                "change_pct": None,
                "prev_close": None,
                "open": None,
                "high": None,
                "low": None,
                "volume": None,
                "amount": None,
            }
        options.append(_build_card(meta, snapshot))

    return {
        "ok": True,
        "options": options,
        "elapsed_seconds": round(time.perf_counter() - start_time, 3),
    }


def get_index_detail_service(code: str) -> dict[str, Any]:
    start_time = time.perf_counter()
    meta = _lookup_meta(code)
    if meta is None:
        return {
            "ok": False,
            "message": "未找到该指数",
            "index": {},
            "elapsed_seconds": round(time.perf_counter() - start_time, 3),
        }

    cn_map, global_map = _load_snapshot_context()
    snapshot = _resolve_snapshot(meta, cn_map, global_map)
    if not snapshot:
        return {
            "ok": False,
            "message": "指数实时数据获取失败",
            "index": {},
            "elapsed_seconds": round(time.perf_counter() - start_time, 3),
        }

    return {
        "ok": True,
        "index": _build_detail(meta, snapshot),
        "elapsed_seconds": round(time.perf_counter() - start_time, 3),
    }
