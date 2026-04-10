from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from config import LOGGER
from modules.opportunity_review import _get_hist_dataframe
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


def get_low_opportunity_pool() -> list[dict[str, Any]]:
    """Return the sorted low-position opportunity list."""
    return _get_effective_low_pool()


def get_auto_recommendation() -> dict[str, Any]:
    """Return the highest-scored opportunity from the current pool."""
    pool = _get_effective_low_pool()
    if not pool:
        return {}
    return max(pool, key=lambda item: int(item.get("score", 0)))


def get_opportunity_detail(code: str) -> dict[str, Any] | None:
    """Return one detailed low-position opportunity item by code."""
    normalized_code = str(code).strip()
    return next((item for item in _get_effective_low_pool() if item["code"] == normalized_code), None)


def get_stock_low_opportunity_pool() -> list[dict[str, Any]]:
    """Expose the same real low-position stock pool to the stock tab."""
    return _get_effective_low_pool()
