from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Mapping

import pandas as pd

from config import LOGGER
from modules.fetch_market import fetch_stock_data, normalize_code
from modules.opportunity_review import _get_hist_dataframe
from modules.stock_score_service import StockScoreService, safe_float as score_safe_float
from modules.watchlist_service import load_watchlist


# Fallback stock pool used when the local watchlist is empty.
DEFAULT_STOCK_POOL: list[dict[str, str]] = [
    {"code": "300750", "name": "宁德时代"},
    {"code": "002594", "name": "比亚迪"},
    {"code": "601318", "name": "中国平安"},
    {"code": "600036", "name": "招商银行"},
    {"code": "000333", "name": "美的集团"},
]

# Safe fallback data so the page still works if upstream data is unavailable.
LOW_OPPORTUNITY_FALLBACK: list[dict[str, Any]] = [
    {
        "code": "300750",
        "name": "宁德时代",
        "score": 86,
        "tag": "推荐",
        "signal": "推荐",
        "reason": "低位区间内企稳，量能改善，短线反弹适中",
        "retrace": "24.8%",
        "trend": "站上5日线并靠近10日线",
        "risk": "中",
        "signals": ["接近60日低位", "量能放大", "反弹节奏适中"],
        "summary": "当前仍处于相对低位，量能改善后修复节奏较清晰。",
        "suggestion": "适合观察低位布局机会，不建议追高。",
        "metrics": {
            "drawdown": "24.8%",
            "trend": "站上5日线并靠近10日线",
            "risk": "中",
        },
        "features": {
            "volume_spike": True,
            "trend_turn": True,
            "drawdown": "24.8%",
            "stop_falling": True,
            "bullish_break": True,
            "risk": "中",
        },
    },
    {
        "code": "002594",
        "name": "比亚迪",
        "score": 73,
        "tag": "观察",
        "signal": "观察",
        "reason": "回撤较深，短线止跌，量能温和回升",
        "retrace": "18.2%",
        "trend": "5日线拐头，仍在10日线下方",
        "risk": "中高",
        "signals": ["回撤充分", "止跌", "量能边际改善"],
        "summary": "仍在修复早期，适合继续观察确认力度。",
        "suggestion": "可继续跟踪，但更适合等进一步放量确认。",
        "metrics": {
            "drawdown": "18.2%",
            "trend": "5日线拐头，仍在10日线下方",
            "risk": "中高",
        },
        "features": {
            "volume_spike": True,
            "trend_turn": False,
            "drawdown": "18.2%",
            "stop_falling": True,
            "bullish_break": False,
            "risk": "中高",
        },
    },
    {
        "code": "601318",
        "name": "中国平安",
        "score": 61,
        "tag": "观察",
        "signal": "观察",
        "reason": "低位盘整，趋势修复有限，弹性一般",
        "retrace": "16.1%",
        "trend": "收盘贴近5日线",
        "risk": "中",
        "signals": ["位置偏低", "趋势修复有限"],
        "summary": "属于偏稳健修复型标的，可继续观察。",
        "suggestion": "适合放入观察池，不宜当作最强进攻方向。",
        "metrics": {
            "drawdown": "16.1%",
            "trend": "收盘贴近5日线",
            "risk": "中",
        },
        "features": {
            "volume_spike": False,
            "trend_turn": False,
            "drawdown": "16.1%",
            "stop_falling": True,
            "bullish_break": False,
            "risk": "中",
        },
    },
]

LOW_POOL_CACHE_SECONDS = 300
_LOW_POOL_CACHE: dict[str, Any] = {
    "timestamp": None,
    "items": None,
}
_SCORED_OPPORTUNITY_CACHE: dict[str, Any] = {}

SUPPORTED_OPPORTUNITY_BOARDS = {"all", "gem", "sz_main", "sh_main", "star"}
SUPPORTED_OPPORTUNITY_SCOPES = {"market", "watchlist"}
SCOPE_NAMES = {
    "market": "全市场",
    "watchlist": "自选股",
}
BOARD_NAMES = {
    "all": "全部",
    "gem": "创业板",
    "sz_main": "深市主板",
    "sh_main": "沪市主板",
    "star": "科创板",
    "other": "其他",
}
SUB_SCORE_MAX = {
    "low": 40.0,
    "trend": 25.0,
    "volume": 25.0,
    "capital": 10.0,
}
BOARD_STRATEGIES: dict[str, dict[str, Any]] = {
    "all": {
        "weights": {"low": 0.40, "trend": 0.25, "volume": 0.25, "capital": 0.10},
        "thresholds": {"board_total": 50, "low": 12, "trend": 8},
        "sort": ("board_total", "low", "trend", "volume", "capital"),
        "fallback_limit": 5,
        "badge": "综合低位机会",
    },
    "gem": {
        "weights": {"low": 0.30, "trend": 0.30, "volume": 0.25, "capital": 0.15},
        "thresholds": {"board_total": 48, "trend": 10, "volume": 8, "low": 10},
        "sort": ("trend", "volume", "board_total", "low", "capital"),
        "fallback_sort": ("trend", "volume", "board_total", "low", "capital"),
        "fallback_limit": 5,
        "badge": "趋势量能优先",
        "empty_mode": "watchlist",
    },
    "sz_main": {
        "weights": {"low": 0.35, "trend": 0.25, "volume": 0.20, "capital": 0.20},
        "thresholds": {"board_total": 45, "low": 12, "trend": 8},
        "sort": ("low", "trend", "board_total", "capital", "volume"),
        "fallback_sort": ("board_total", "low", "trend", "capital", "volume"),
        "fallback_limit": 5,
        "badge": "低位修复优先",
        "empty_mode": "fallback",
    },
    "sh_main": {
        "weights": {"low": 0.40, "trend": 0.20, "volume": 0.15, "capital": 0.25},
        "thresholds": {"board_total": 43, "low": 14, "capital": 6},
        "sort": ("low", "capital", "board_total", "trend", "volume"),
        "fallback_sort": ("low_capital", "low", "capital", "board_total", "trend", "volume"),
        "fallback_limit": 3,
        "badge": "低位资金优先",
        "empty_mode": "fallback",
    },
    "star": {
        "weights": {"low": 0.25, "trend": 0.35, "volume": 0.25, "capital": 0.15},
        "thresholds": {"board_total": 50, "trend": 12, "volume": 8},
        "sort": ("trend", "volume", "board_total", "low", "capital"),
        "fallback_sort": ("trend", "volume", "board_total", "low", "capital"),
        "fallback_limit": 5,
        "badge": "弹性趋势优先",
        "empty_mode": "watchlist",
    },
}


def identify_stock_board(code: str) -> str:
    """Identify the stock board by code prefix."""
    normalized_code = normalize_code(code)
    if normalized_code.startswith("300"):
        return "gem"
    if normalized_code.startswith(("000", "001", "002", "003")):
        return "sz_main"
    if normalized_code.startswith(("600", "601", "603", "605")):
        return "sh_main"
    if normalized_code.startswith("688"):
        return "star"
    return "other"


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


def _format_pct(value: float) -> str:
    """Format a percentage value for API responses."""
    return f"{round(float(value), 1)}%"


def _resolve_tag(score: int) -> str:
    """Map numeric score to UI tag."""
    if score >= 80:
        return "推荐"
    if score >= 60:
        return "观察"
    return "谨慎"


def _get_candidate_pool() -> list[dict[str, str]]:
    """Reuse the existing stock watchlist when available, otherwise fallback."""
    watchlist = load_watchlist()
    if watchlist:
        return watchlist
    return DEFAULT_STOCK_POOL


def _fetch_hist_dataframe(code: str) -> pd.DataFrame | None:
    """Fetch daily k-line data via the existing project history helper."""
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=160)).strftime("%Y%m%d")
    return _get_hist_dataframe(code, start_date, end_date)


def _prepare_kline_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame | None:
    """Normalize the historical dataframe into open/high/low/close/volume columns."""
    required_columns = ("开盘", "最高", "最低", "收盘", "成交量")
    if not all(column in dataframe.columns for column in required_columns):
        return None

    working_df = dataframe.copy()
    for column in required_columns:
        working_df[column] = pd.to_numeric(working_df[column], errors="coerce")

    working_df = working_df.dropna(subset=list(required_columns)).reset_index(drop=True)
    if len(working_df) < 60:
        return None
    return working_df


def _calculate_low_position_score(close_price: float, low_60: float, high_60: float) -> tuple[float, float]:
    """
    Low-position score, max 35 points.

    Formula:
    range_position = (close - low_60) / (high_60 - low_60)
    low_score = (1 - range_position) * 35
    """
    if high_60 <= low_60:
        return 0.0, 0.5
    range_position = (close_price - low_60) / (high_60 - low_60)
    range_position = max(0.0, min(range_position, 1.0))
    return round((1.0 - range_position) * 35.0, 2), range_position


def _calculate_volume_score(volume_ratio: float) -> float:
    """
    Volume score, max 25 points.

    Ratio is latest_volume / avg_volume_20.
    ratio <= 1 gives 0 points, ratio >= 2 gives full score.
    """
    if volume_ratio <= 1.0:
        return 0.0
    return round(min((volume_ratio - 1.0) * 25.0, 25.0), 2)


def _calculate_rebound_score(rebound_5d_pct: float) -> float:
    """
    Rebound score, max 20 points.

    A moderate rebound is preferred. The score peaks near 4%.
    """
    if rebound_5d_pct <= 0:
        return 0.0
    score = 20.0 - abs(rebound_5d_pct - 4.0) * 4.0
    return round(max(0.0, min(score, 20.0)), 2)


def _calculate_trend_score(close_price: float, ma5: float, ma10: float) -> tuple[float, str]:
    """
    Trend repair score, max 10 points.

    Preferred relation:
    close >= ma5 >= ma10
    """
    if close_price >= ma5 >= ma10:
        return 10.0, "收盘站上5日线，5日线位于10日线上方"
    if close_price >= ma5 and ma5 < ma10:
        return 6.0, "收盘站上5日线，5日线开始靠近10日线"
    if close_price >= ma10:
        return 5.0, "收盘重新回到10日线附近"
    if ma5 >= ma10:
        return 4.0, "5日线拐头但收盘尚未完全确认"
    return 0.0, "短线均线修复仍不明显"


def _calculate_risk_penalty(distance_to_high_pct: float, rebound_5d_pct: float) -> tuple[float, str]:
    """
    Risk penalty, max 10 points.

    - Too close to the 60-day high => penalize.
    - 5-day rebound too large => penalize.
    """
    high_risk_penalty = max(0.0, min((12.0 - distance_to_high_pct) * 0.7, 7.0)) if distance_to_high_pct < 12.0 else 0.0
    rebound_penalty = max(0.0, min((rebound_5d_pct - 8.0) * 1.0, 5.0)) if rebound_5d_pct > 8.0 else 0.0
    penalty = round(min(high_risk_penalty + rebound_penalty, 10.0), 2)

    if penalty >= 7.0:
        return penalty, "高"
    if penalty >= 3.5:
        return penalty, "中高"
    return penalty, "中"


def _build_reason_and_signals(
    range_position: float,
    volume_ratio: float,
    rebound_5d_pct: float,
    trend_text: str,
    risk_level: str,
) -> tuple[str, list[str]]:
    """Build concise reason text and UI signals from the component metrics."""
    signals: list[str] = []

    if range_position <= 0.3:
        signals.append("接近60日低位")
    elif range_position <= 0.45:
        signals.append("处于中低位区间")

    if volume_ratio >= 1.5:
        signals.append("明显放量")
    elif volume_ratio >= 1.2:
        signals.append("量能改善")

    if 0 < rebound_5d_pct <= 8:
        signals.append("5日反弹适中")
    elif rebound_5d_pct > 8:
        signals.append("5日反弹偏大")

    if "不明显" not in trend_text:
        signals.append("趋势修复")

    if risk_level == "高":
        signals.append("短线风险偏高")

    if not signals:
        signals.append("信号仍在形成中")

    return " + ".join(signals[:3]), signals


def _build_payload(
    *,
    code: str,
    name: str,
    score: int,
    tag: str,
    reason: str,
    retrace: str,
    trend: str,
    risk: str,
    signals: list[str],
    volume_ratio: float,
    range_position: float,
    rebound_5d_pct: float,
) -> dict[str, Any]:
    """Create one API item while preserving frontend-compatible fields."""
    summary = f"当前距60日高点回撤约{retrace}，处于低位修复观察阶段。"
    suggestion = "适合观察低位布局机会，不建议追高。" if tag == "推荐" else (
        "建议继续观察量能和均线修复情况。" if tag == "观察" else "短线风险仍偏高，建议谨慎。"
    )

    return {
        "code": code,
        "name": name,
        "score": score,
        "tag": tag,
        "signal": tag,
        "reason": reason,
        "retrace": retrace,
        "trend": trend,
        "risk": risk,
        "signals": signals,
        "summary": summary,
        "suggestion": suggestion,
        "metrics": {
            "drawdown": retrace,
            "trend": trend,
            "risk": risk,
        },
        "features": {
            "volume_spike": bool(volume_ratio >= 1.2),
            "trend_turn": bool("不明显" not in trend),
            "drawdown": retrace,
            "stop_falling": bool(range_position <= 0.45),
            "bullish_break": bool(0 < rebound_5d_pct <= 8),
            "risk": risk,
        },
    }


def _analyze_candidate(code: str, name: str) -> dict[str, Any] | None:
    """Analyze one stock using the first-version low-opportunity formula."""
    raw_df = _fetch_hist_dataframe(code)
    if raw_df is None:
        return None

    dataframe = _prepare_kline_dataframe(raw_df)
    if dataframe is None:
        LOGGER.info("Skip %s %s because k-line rows are less than 60.", code, name)
        return None

    latest = dataframe.iloc[-1]
    recent_60 = dataframe.tail(60)
    recent_20 = dataframe.tail(20)

    close_price = _safe_float(latest["收盘"])
    latest_volume = _safe_float(latest["成交量"])
    high_60 = _safe_float(recent_60["最高"].max())
    low_60 = _safe_float(recent_60["最低"].min())
    avg_volume_20 = _safe_float(recent_20["成交量"].mean())
    ma5 = _safe_float(dataframe.tail(5)["收盘"].mean())
    ma10 = _safe_float(dataframe.tail(10)["收盘"].mean())
    close_5d_ago = _safe_float(dataframe.iloc[-6]["收盘"])

    if None in {close_price, latest_volume, high_60, low_60, avg_volume_20, ma5, ma10, close_5d_ago}:
        return None
    if avg_volume_20 in {None, 0} or high_60 in {None, 0}:
        return None

    low_score, range_position = _calculate_low_position_score(close_price, low_60, high_60)
    volume_ratio = latest_volume / avg_volume_20 if avg_volume_20 else 0.0
    volume_score = _calculate_volume_score(volume_ratio)
    rebound_5d_pct = (close_price / close_5d_ago - 1.0) * 100 if close_5d_ago else 0.0
    rebound_score = _calculate_rebound_score(rebound_5d_pct)
    trend_score, trend_text = _calculate_trend_score(close_price, ma5, ma10)
    distance_to_high_pct = (high_60 - close_price) / high_60 * 100 if high_60 else 0.0
    risk_penalty, risk_level = _calculate_risk_penalty(distance_to_high_pct, rebound_5d_pct)

    # Final score = low + volume + rebound + trend - risk
    score = int(round(max(0.0, min(low_score + volume_score + rebound_score + trend_score - risk_penalty, 100.0))))
    tag = _resolve_tag(score)
    retrace = _format_pct(distance_to_high_pct)
    reason, signals = _build_reason_and_signals(
        range_position=range_position,
        volume_ratio=volume_ratio,
        rebound_5d_pct=rebound_5d_pct,
        trend_text=trend_text,
        risk_level=risk_level,
    )

    return _build_payload(
        code=code,
        name=name,
        score=score,
        tag=tag,
        reason=reason,
        retrace=retrace,
        trend=trend_text,
        risk=risk_level,
        signals=signals,
        volume_ratio=volume_ratio,
        range_position=range_position,
        rebound_5d_pct=rebound_5d_pct,
    )


def _calculate_real_low_pool() -> list[dict[str, Any]]:
    """Build the low-opportunity pool from the current candidate stock list."""
    results: list[dict[str, Any]] = []
    for item in _get_candidate_pool():
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip() or code
        if not code:
            continue
        try:
            analyzed = _analyze_candidate(code, name)
            if analyzed is not None:
                results.append(analyzed)
        except Exception as exc:  # pragma: no cover - runtime safety
            LOGGER.warning("Failed to calculate low opportunity for %s %s: %s", code, name, exc)

    results.sort(key=lambda item: int(item.get("score", 0)), reverse=True)
    return results


def _get_effective_low_pool() -> list[dict[str, Any]]:
    """
    Return the real pool when data is available.

    If all candidates fail because upstream history data is unavailable,
    fall back to a safe static list so existing pages do not break.
    """
    now = datetime.now()
    cached_at = _LOW_POOL_CACHE.get("timestamp")
    cached_items = _LOW_POOL_CACHE.get("items")
    if cached_at is not None and cached_items is not None:
        age_seconds = (now - cached_at).total_seconds()
        if age_seconds <= LOW_POOL_CACHE_SECONDS:
            return list(cached_items)

    calculated = _calculate_real_low_pool()
    effective_items = calculated if calculated else LOW_OPPORTUNITY_FALLBACK
    _LOW_POOL_CACHE["timestamp"] = now
    _LOW_POOL_CACHE["items"] = list(effective_items)
    return list(effective_items)


_STOCK_SCORE_SERVICE = StockScoreService()


def _score_first_float(*values: Any, default: float = 0.0) -> float:
    """Return the first present numeric value, keeping zero as valid."""
    for value in values:
        if value is None or value == "":
            continue
        return score_safe_float(value, default=default)
    return default


def _score_first_number(*values: Any, default: float = 0.0) -> float:
    """Return the first non-zero numeric value."""
    for value in values:
        parsed = score_safe_float(value, default=0.0)
        if parsed:
            return parsed
    return default


def _score_mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _score_numeric_series(dataframe: pd.DataFrame, *names: str) -> pd.Series | None:
    for name in names:
        if name in dataframe.columns:
            return pd.to_numeric(dataframe[name], errors="coerce")
    return None


def _normalize_stock_pool(source: list[dict[str, str]]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    seen_codes: set[str] = set()
    for item in source:
        code = normalize_code(item.get("code", ""))
        if not code or code in seen_codes:
            continue
        candidates.append({"code": code, "name": str(item.get("name") or code).strip()})
        seen_codes.add(code)
    return candidates


def _normalize_opportunity_scope(scope: str | None) -> str:
    normalized_scope = str(scope or "market").strip().lower()
    return normalized_scope if normalized_scope in SUPPORTED_OPPORTUNITY_SCOPES else "market"


def _score_candidate_pool(scope: str = "market") -> list[dict[str, str]]:
    """Select candidate stocks by opportunity scope without changing score logic."""
    normalized_scope = _normalize_opportunity_scope(scope)
    source = load_watchlist() if normalized_scope == "watchlist" else DEFAULT_STOCK_POOL
    return _normalize_stock_pool(source)


def _score_fetch_history(code: str) -> pd.DataFrame | None:
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")
    return _get_hist_dataframe(code, start_date, end_date)


def _score_history_features(dataframe: pd.DataFrame | None) -> dict[str, float]:
    if dataframe is None or dataframe.empty:
        return {}

    close_series = _score_numeric_series(dataframe, "收盘", "close")
    high_series = _score_numeric_series(dataframe, "最高", "high")
    low_series = _score_numeric_series(dataframe, "最低", "low")
    volume_series = _score_numeric_series(dataframe, "成交量", "volume")
    change_pct_series = _score_numeric_series(dataframe, "涨跌幅", "pct_change", "change_pct")
    turnover_rate_series = _score_numeric_series(dataframe, "换手率", "turnover_rate")

    if close_series is None or high_series is None or low_series is None:
        return {}

    working = pd.DataFrame(
        {
            "close": close_series,
            "high": high_series,
            "low": low_series,
        }
    )
    working["volume"] = volume_series if volume_series is not None else 0.0
    working["change_pct"] = change_pct_series if change_pct_series is not None else 0.0
    working["turnover_rate"] = turnover_rate_series if turnover_rate_series is not None else 0.0
    working = working.dropna(subset=["close", "high", "low"])
    if working.empty:
        return {}

    latest = working.iloc[-1]
    recent_60 = working.tail(60)
    recent_120 = working.tail(120)
    recent_20 = working.tail(20)
    latest_volume = _score_first_float(latest.get("volume"), default=0.0)
    avg_volume_20 = _score_first_float(recent_20["volume"].mean(), default=0.0)

    return {
        "price": _score_first_float(latest.get("close"), default=0.0),
        "change_pct": _score_first_float(latest.get("change_pct"), default=0.0),
        "turnover_rate": _score_first_float(latest.get("turnover_rate"), default=0.0),
        "volume_ratio": (latest_volume / avg_volume_20) if latest_volume and avg_volume_20 else 0.0,
        "ma5": _score_first_float(working["close"].tail(5).mean(), default=0.0),
        "ma10": _score_first_float(working["close"].tail(10).mean(), default=0.0),
        "ma20": _score_first_float(working["close"].tail(20).mean(), default=0.0),
        "ma60": _score_first_float(working["close"].tail(60).mean(), default=0.0),
        "high_60d": _score_first_float(recent_60["high"].max(), default=0.0),
        "low_60d": _score_first_float(recent_60["low"].min(), default=0.0),
        "high_120d": _score_first_float(recent_120["high"].max(), default=0.0),
        "low_120d": _score_first_float(recent_120["low"].min(), default=0.0),
    }


def _score_input(market_data: Mapping[str, Any], history_features: Mapping[str, Any]) -> dict[str, float]:
    market_data = _score_mapping(market_data)
    history_features = _score_mapping(history_features)
    price = _score_first_number(
        market_data.get("price"),
        market_data.get("latest_price"),
        market_data.get("close"),
        history_features.get("price"),
    )
    high = _score_first_number(market_data.get("high"), history_features.get("high_60d"), price)
    low = _score_first_number(market_data.get("low"), history_features.get("low_60d"), price)

    return {
        "price": price,
        "change_pct": _score_first_float(market_data.get("change_pct"), market_data.get("pct_change"), history_features.get("change_pct")),
        "volume_ratio": _score_first_float(market_data.get("volume_ratio"), history_features.get("volume_ratio")),
        "turnover_rate": _score_first_float(market_data.get("turnover_rate"), history_features.get("turnover_rate")),
        "main_net_inflow": _score_first_float(market_data.get("main_net_inflow"), default=0.0),
        "ma5": _score_first_float(market_data.get("ma5"), history_features.get("ma5"), price),
        "ma10": _score_first_float(market_data.get("ma10"), history_features.get("ma10"), price),
        "ma20": _score_first_float(market_data.get("ma20"), history_features.get("ma20"), price),
        "ma60": _score_first_float(market_data.get("ma60"), history_features.get("ma60"), price),
        "high_60d": _score_first_number(market_data.get("high_60d"), history_features.get("high_60d"), high),
        "low_60d": _score_first_number(market_data.get("low_60d"), history_features.get("low_60d"), low),
        "high_120d": _score_first_number(market_data.get("high_120d"), history_features.get("high_120d"), high),
        "low_120d": _score_first_number(market_data.get("low_120d"), history_features.get("low_120d"), low),
    }


def _passes_opportunity_filters(score: Mapping[str, Any], score_input: Mapping[str, Any]) -> bool:
    sub_scores = _score_mapping(score.get("sub_scores"))
    total_score = int(score_safe_float(score.get("total_score"), default=0.0))
    low_score = int(score_safe_float(sub_scores.get("low"), default=0.0))
    trend_score = int(score_safe_float(sub_scores.get("trend"), default=0.0))
    change_pct = score_safe_float(score_input.get("change_pct"), default=0.0)
    turnover_rate = score_safe_float(score_input.get("turnover_rate"), default=0.0)
    return (
        total_score >= 60
        and low_score >= 18
        and trend_score >= 10
        and change_pct <= 8
        and turnover_rate <= 15
    )


def _normalize_opportunity_board(board: str | None) -> str:
    normalized_board = str(board or "all").strip().lower()
    return normalized_board if normalized_board in SUPPORTED_OPPORTUNITY_BOARDS else "all"


def _strategy_for_board(board: str) -> dict[str, Any]:
    return BOARD_STRATEGIES.get(board) or BOARD_STRATEGIES["all"]


def _board_score_value(item: Mapping[str, Any], field: str) -> float:
    score = _score_mapping(item.get("score"))
    sub_scores = _score_mapping(score.get("sub_scores"))
    if field in {"board_total", "total"}:
        return score_safe_float(score.get("board_total"), default=0.0)
    if field in {"base_total", "original_total"}:
        return score_safe_float(score.get("base_total"), default=score_safe_float(score.get("total_score"), default=0.0))
    if field == "low_capital":
        return score_safe_float(sub_scores.get("low"), default=0.0) + score_safe_float(sub_scores.get("capital"), default=0.0)
    return score_safe_float(sub_scores.get(field), default=0.0)


def _calculate_board_total_score(sub_scores: Mapping[str, Any], board: str) -> int:
    strategy = _strategy_for_board(board)
    weights = _score_mapping(strategy.get("weights"))
    weighted_total = 0.0
    for key, max_score in SUB_SCORE_MAX.items():
        raw_score = score_safe_float(sub_scores.get(key), default=0.0)
        component_score = (raw_score / max_score * 100.0) if max_score else 0.0
        weighted_total += component_score * score_safe_float(weights.get(key), default=0.0)
    return int(round(max(0.0, min(weighted_total, 100.0))))


def _passes_board_filters(item: Mapping[str, Any]) -> bool:
    board = str(item.get("board") or "other")
    strategy = _strategy_for_board(board)
    thresholds = _score_mapping(strategy.get("thresholds"))
    score = _score_mapping(item.get("score"))
    sub_scores = _score_mapping(score.get("sub_scores"))

    if _board_score_value(item, "board_total") < score_safe_float(thresholds.get("board_total"), default=0.0):
        return False

    for key in ("low", "trend", "volume", "capital"):
        threshold = thresholds.get(key)
        if threshold is not None and score_safe_float(sub_scores.get(key), default=0.0) < score_safe_float(threshold, default=0.0):
            return False
    return True


def _opportunity_sort_key(item: Mapping[str, Any], fields: tuple[str, ...] | list[str]) -> tuple[float, ...]:
    values = tuple(_board_score_value(item, field) for field in fields)
    return values + (_board_score_value(item, "base_total"),)


def _mark_opportunity_scope(item: Mapping[str, Any], scope: str) -> dict[str, Any]:
    enriched = dict(item)
    normalized_scope = _normalize_opportunity_scope(scope)
    enriched["scope"] = normalized_scope
    enriched["scope_name"] = SCOPE_NAMES.get(normalized_scope, SCOPE_NAMES["market"])
    return enriched


def _mark_opportunity_mode(item: Mapping[str, Any], mode: str) -> dict[str, Any]:
    enriched = dict(item)
    board = str(enriched.get("board") or "other")
    strategy = _strategy_for_board(board)
    board_name = BOARD_NAMES.get(board, BOARD_NAMES["other"])
    enriched["board"] = board
    enriched["board_name"] = board_name
    enriched["mode"] = mode
    if mode == "normal":
        enriched["badge"] = f"{board_name}正式机会"
    elif mode == "watchlist":
        enriched["badge"] = f"{board_name}观察名单"
    else:
        enriched["badge"] = f"{board_name}保底推荐"
    return enriched


def _select_board_opportunities(items: list[dict[str, Any]], board: str, limit: int) -> list[dict[str, Any]]:
    strategy = _strategy_for_board(board)
    normal_items = [_mark_opportunity_mode(item, "normal") for item in items if _passes_board_filters(item)]
    if normal_items:
        sort_fields = tuple(strategy.get("sort", ("board_total", "low", "trend", "volume", "capital")))
        normal_items.sort(key=lambda item: _opportunity_sort_key(item, sort_fields), reverse=True)
        return normal_items[: max(0, int(limit))]

    fallback_fields = tuple(strategy.get("fallback_sort", strategy.get("sort", ("board_total",))))
    fallback_limit = min(max(0, int(limit)), int(strategy.get("fallback_limit", limit)))
    empty_mode = str(strategy.get("empty_mode") or "fallback")
    fallback_items = [_mark_opportunity_mode(item, empty_mode) for item in items]
    fallback_items.sort(key=lambda item: _opportunity_sort_key(item, fallback_fields), reverse=True)
    return fallback_items[:fallback_limit]


def _score_signal(level: str) -> str:
    if level == "A":
        return "推荐"
    if level in {"B", "C"}:
        return "观察"
    return "谨慎"


def _build_scored_opportunity(
    *,
    code: str,
    name: str,
    market_data: Mapping[str, Any],
    score_input: Mapping[str, Any],
    score: Mapping[str, Any],
) -> dict[str, Any]:
    sub_scores = _score_mapping(score.get("sub_scores"))
    board = identify_stock_board(code)
    board_name = BOARD_NAMES.get(board, BOARD_NAMES["other"])
    board_total_score = _calculate_board_total_score(sub_scores, board)
    total_score = int(score_safe_float(score.get("total_score"), default=0.0))
    low_score = int(score_safe_float(sub_scores.get("low"), default=0.0))
    trend_score = int(score_safe_float(sub_scores.get("trend"), default=0.0))
    level = str(score.get("level") or "D")
    conclusion = str(score.get("conclusion") or "")
    signal = _score_signal(level)
    price = score_safe_float(score_input.get("price"), default=0.0)
    high_60d = score_safe_float(score_input.get("high_60d"), default=0.0)
    drawdown = max(0.0, (high_60d - price) / high_60d * 100) if price and high_60d else 0.0

    return {
        "symbol": code,
        "name": str(market_data.get("name") or name or code),
        "price": price,
        "change_pct": score_safe_float(score_input.get("change_pct"), default=0.0),
        "turnover_rate": score_safe_float(score_input.get("turnover_rate"), default=0.0),
        "base_total": total_score,
        "board_total": board_total_score,
        "score": {
            "total_score": total_score,
            "base_total": total_score,
            "board_total": board_total_score,
            "level": level,
            "sub_scores": dict(sub_scores),
            "conclusion": conclusion,
            "tags": list(score.get("tags", []) or []),
            "board_weights": dict(_strategy_for_board(board).get("weights", {})),
        },
        "code": code,
        "score_value": board_total_score,
        "board": board,
        "board_name": board_name,
        "mode": "candidate",
        "badge": f"{board_name}待筛选",
        "signal": signal,
        "tag": signal,
        "reason": conclusion,
        "summary": conclusion,
        "suggestion": conclusion,
        "metrics": {
            "drawdown": f"{drawdown:.1f}%",
            "trend": f"趋势分 {trend_score}",
            "risk": "中" if total_score >= 65 else "中高",
        },
        "features": {
            "volume_spike": score_safe_float(score_input.get("volume_ratio"), default=0.0) >= 1.2,
            "trend_turn": trend_score >= 10,
            "drawdown": f"{drawdown:.1f}%",
            "stop_falling": low_score >= 18,
            "bullish_break": score_safe_float(score_input.get("change_pct"), default=0.0) > 0,
            "risk": "中" if total_score >= 65 else "中高",
        },
    }


def _score_candidate(stock: Mapping[str, Any]) -> dict[str, Any] | None:
    code = normalize_code(stock.get("code", ""))
    if not code:
        return None
    name = str(stock.get("name") or code).strip()

    market_data: dict[str, Any] = {}
    try:
        fetched = fetch_stock_data(code, name)
        if isinstance(fetched, dict) and fetched.get("status") != "error":
            market_data = fetched
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Failed to fetch market data for opportunity %s %s: %s", code, name, exc)

    history_features = _score_history_features(_score_fetch_history(code))
    score_input = _score_input(market_data, history_features)
    if not score_safe_float(score_input.get("price"), default=0.0):
        return None

    score = _STOCK_SCORE_SERVICE.score(score_input)

    return _build_scored_opportunity(
        code=code,
        name=name,
        market_data=market_data,
        score_input=score_input,
        score=score,
    )


def _calculate_scored_opportunities(limit: int = 10, board: str = "all", scope: str = "market") -> list[dict[str, Any]]:
    requested_board = _normalize_opportunity_board(board)
    requested_scope = _normalize_opportunity_scope(scope)
    candidates: list[dict[str, Any]] = []
    for stock in _score_candidate_pool(requested_scope):
        try:
            item = _score_candidate(stock)
        except Exception as exc:  # pragma: no cover - runtime safety
            LOGGER.warning("Failed to score opportunity candidate %s: %s", stock, exc)
            item = None
        if item is None:
            continue
        item = _mark_opportunity_scope(item, requested_scope)
        if requested_board != "all" and item.get("board") != requested_board:
            continue
        candidates.append(item)

    if requested_board != "all":
        return _select_board_opportunities(candidates, requested_board, limit)

    selected_by_board: dict[str, list[dict[str, Any]]] = {}
    board_order = ("gem", "sz_main", "sh_main", "star", "other")
    for board_key in board_order:
        board_items = [item for item in candidates if item.get("board") == board_key]
        if board_items:
            selected_by_board[board_key] = _select_board_opportunities(board_items, board_key, limit)

    return _merge_board_opportunities(selected_by_board, limit)


def _merge_board_opportunities(selected_by_board: Mapping[str, list[dict[str, Any]]], limit: int) -> list[dict[str, Any]]:
    """Round-robin board results so all-mode is not dominated by one board."""
    board_order = ("gem", "sz_main", "sh_main", "star", "other")
    buckets = {board: list(selected_by_board.get(board, [])) for board in board_order}
    merged: list[dict[str, Any]] = []
    while len(merged) < max(0, int(limit)) and any(buckets.values()):
        for board in board_order:
            bucket = buckets.get(board, [])
            if bucket:
                merged.append(bucket.pop(0))
                if len(merged) >= max(0, int(limit)):
                    break
    return merged


def _fallback_scored_opportunities(board: str, limit: int, scope: str = "market") -> list[dict[str, Any]]:
    requested_board = _normalize_opportunity_board(board)
    requested_scope = _normalize_opportunity_scope(scope)
    if requested_scope != "market":
        return []
    items: list[dict[str, Any]] = []
    for item in LOW_OPPORTUNITY_FALLBACK:
        code = normalize_code(item.get("code", ""))
        item_board = identify_stock_board(code)
        if requested_board != "all" and item_board != requested_board:
            continue
        enriched = dict(item)
        enriched["code"] = code
        enriched["symbol"] = code
        base_total = int(score_safe_float(enriched.get("score"), default=0.0))
        enriched["base_total"] = base_total
        enriched["board_total"] = base_total
        enriched["score_value"] = base_total
        enriched["board"] = item_board
        enriched["board_name"] = BOARD_NAMES.get(item_board, BOARD_NAMES["other"])
        enriched["scope"] = requested_scope
        enriched["scope_name"] = SCOPE_NAMES.get(requested_scope, SCOPE_NAMES["market"])
        enriched["mode"] = "fallback"
        enriched["badge"] = f"{enriched['board_name']}保底推荐"
        items.append(enriched)
    items.sort(key=lambda item: int(score_safe_float(item.get("score_value"), default=0.0)), reverse=True)
    if requested_board == "all":
        grouped: dict[str, list[dict[str, Any]]] = {}
        for item in items:
            grouped.setdefault(str(item.get("board") or "other"), []).append(item)
        return _merge_board_opportunities(grouped, limit)
    return items[: max(0, int(limit))]


def get_opportunities(limit: int = 10, board: str = "all", scope: str = "market") -> list[dict[str, Any]]:
    """Return the filtered low-position opportunity pool based on StockScoreService."""
    normalized_board = _normalize_opportunity_board(board)
    normalized_scope = _normalize_opportunity_scope(scope)
    cache_key = f"{normalized_scope}:{normalized_board}"
    use_cache = normalized_scope == "market"
    now = datetime.now()
    cached_payload = _SCORED_OPPORTUNITY_CACHE.get(cache_key) if use_cache else None
    if use_cache and isinstance(cached_payload, Mapping):
        cached_at = cached_payload.get("timestamp")
        cached_items = cached_payload.get("items")
        if cached_at is not None and cached_items is not None:
            age_seconds = (now - cached_at).total_seconds()
            if age_seconds <= LOW_POOL_CACHE_SECONDS:
                return list(cached_items)[: max(0, int(limit))]

    items = _calculate_scored_opportunities(limit=limit, board=normalized_board, scope=normalized_scope)
    if not items:
        items = _fallback_scored_opportunities(normalized_board, limit, normalized_scope)
    if use_cache:
        _SCORED_OPPORTUNITY_CACHE[cache_key] = {
            "timestamp": now,
            "items": list(items),
        }
    return items


def get_low_opportunity_pool() -> list[dict[str, Any]]:
    """Return the low-position opportunity list for existing endpoints."""
    return get_opportunities(limit=10)


def get_auto_recommendation() -> dict[str, Any]:
    """Return the highest-scored opportunity from the current pool."""
    pool = get_opportunities(limit=10)
    if not pool:
        return {}
    return max(
        pool,
        key=lambda item: int(
            score_safe_float(
                _score_mapping(item.get("score")).get("board_total"),
                default=score_safe_float(item.get("score_value"), default=0.0),
            )
        ),
    )


def get_opportunity_detail(code: str) -> dict[str, Any] | None:
    """Return one low-position opportunity item by code."""
    normalized_code = normalize_code(code)
    return next((item for item in get_opportunities(limit=10) if item["symbol"] == normalized_code), None)


def get_stock_low_opportunity_pool() -> list[dict[str, Any]]:
    """Return the same stock opportunity pool for the legacy endpoint."""
    return get_opportunities(limit=10)
