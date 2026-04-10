from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import akshare as ak
import pandas as pd

from config import LOGGER


LOW_OPPORTUNITY_ITEMS: list[dict[str, Any]] = [
    {
        "code": "510300",
        "name": "沪深300ETF",
        "score": 82,
        "signal": "推荐",
        "reason": "回撤后企稳，均线拐头，量能改善",
        "summary": "适合观察低位布局机会，但不建议追高。",
        "metrics": {
            "drawdown": "18%",
            "trend": "均线拐头",
            "risk": "中",
        },
    },
    {
        "code": "159915",
        "name": "创业板ETF",
        "score": 74,
        "signal": "观察",
        "reason": "低位震荡，短期修复中",
        "summary": "当前位置仍偏低，适合继续观察修复持续性。",
        "metrics": {
            "drawdown": "22%",
            "trend": "低位震荡",
            "risk": "中高",
        },
    },
    {
        "code": "588000",
        "name": "科创50ETF",
        "score": 67,
        "signal": "观察",
        "reason": "回撤充分，情绪回暖但波动仍大",
        "summary": "有修复预期，但短线波动仍偏大，适合轻仓跟踪。",
        "metrics": {
            "drawdown": "25%",
            "trend": "修复初期",
            "risk": "中高",
        },
    },
    {
        "code": "512100",
        "name": "中证1000ETF",
        "score": 56,
        "signal": "谨慎",
        "reason": "低位反复震荡，量能改善不明显",
        "summary": "当前位置不算高，但尚未形成清晰修复趋势。",
        "metrics": {
            "drawdown": "16%",
            "trend": "弱势震荡",
            "risk": "中",
        },
    },
]

STOCK_LOW_OPPORTUNITY_CANDIDATES: list[dict[str, str]] = [
    {"code": "300750", "name": "宁德时代"},
    {"code": "300308", "name": "中际旭创"},
    {"code": "002594", "name": "比亚迪"},
    {"code": "600519", "name": "贵州茅台"},
    {"code": "601318", "name": "中国平安"},
    {"code": "002371", "name": "北方华创"},
]

STOCK_LOW_OPPORTUNITY_FALLBACK: list[dict[str, Any]] = [
    {
        "code": "300750",
        "name": "宁德时代",
        "score": 88,
        "signal": "推荐",
        "reason": "低位放量 + 主力吸筹",
        "summary": "阶段回撤较深，短线止跌后量能放大，适合优先跟踪。",
        "features": {
            "volume_spike": True,
            "trend_turn": True,
            "drawdown": "25%",
            "stop_falling": True,
            "bullish_break": True,
            "risk": "中",
        },
    },
    {
        "code": "002594",
        "name": "比亚迪",
        "score": 76,
        "signal": "观察",
        "reason": "回撤充分，连续缩量后出现修复",
        "summary": "当前位置不高，但放量确认还需要继续观察。",
        "features": {
            "volume_spike": True,
            "trend_turn": False,
            "drawdown": "18%",
            "stop_falling": True,
            "bullish_break": True,
            "risk": "中高",
        },
    },
    {
        "code": "601318",
        "name": "中国平安",
        "score": 61,
        "signal": "观察",
        "reason": "低位盘整，量能温和改善",
        "summary": "属于偏稳健修复类型，可继续留意均线变化。",
        "features": {
            "volume_spike": False,
            "trend_turn": True,
            "drawdown": "16%",
            "stop_falling": True,
            "bullish_break": False,
            "risk": "中",
        },
    },
]


def get_low_opportunity_pool() -> list[dict[str, Any]]:
    """Return the current low-position opportunity pool."""
    return [
        {
            "code": item["code"],
            "name": item["name"],
            "score": item["score"],
            "signal": item["signal"],
            "reason": item["reason"],
        }
        for item in LOW_OPPORTUNITY_ITEMS
    ]


def get_auto_recommendation() -> dict[str, Any]:
    """Return the highest-scored current opportunity."""
    best = max(LOW_OPPORTUNITY_ITEMS, key=lambda item: int(item.get("score", 0)))
    return {
        "code": best["code"],
        "name": best["name"],
        "score": best["score"],
        "signal": best["signal"],
        "reason": best["reason"],
        "summary": best["summary"],
    }


def get_opportunity_detail(code: str) -> dict[str, Any] | None:
    """Return one detailed opportunity item by code."""
    target = next((item for item in LOW_OPPORTUNITY_ITEMS if item["code"] == str(code).strip()), None)
    if target is None:
        return None
    return target


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


def _resolve_stock_signal(score: int) -> str:
    if score >= 80:
        return "推荐"
    if score >= 60:
        return "观察"
    return "谨慎"


def _build_stock_reason(flags: list[str]) -> str:
    if flags:
        return " + ".join(flags[:3])
    return "低位信号不足，仍需继续观察"


def _format_drawdown(drawdown_pct: float) -> str:
    return f"{round(drawdown_pct, 1)}%"


def _analyze_stock_low_candidate(code: str, name: str) -> dict[str, Any] | None:
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")

    dataframe = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust="qfq",
    )
    if not isinstance(dataframe, pd.DataFrame) or dataframe.empty or len(dataframe) < 35:
        return None

    working_df = dataframe.copy()
    for column in ("开盘", "收盘", "最高", "最低", "成交量"):
        if column in working_df.columns:
            working_df[column] = pd.to_numeric(working_df[column], errors="coerce")
    working_df = working_df.dropna(subset=["收盘", "开盘", "最低", "成交量"]).reset_index(drop=True)
    if len(working_df) < 35:
        return None

    recent_30 = working_df.tail(30).reset_index(drop=True)
    recent_3 = working_df.tail(3).reset_index(drop=True)
    prev_5 = working_df.tail(8).head(5)
    latest = working_df.iloc[-1]
    prev = working_df.iloc[-2]

    max_close_30 = _safe_float(recent_30["收盘"].max())
    min_low_30 = _safe_float(recent_30["最低"].min())
    latest_close = _safe_float(latest.get("收盘"))
    latest_open = _safe_float(latest.get("开盘"))
    latest_low = _safe_float(latest.get("最低"))
    latest_volume = _safe_float(latest.get("成交量"))
    prev_volume = _safe_float(prev.get("成交量"))
    prev_low_min = _safe_float(prev_5["最低"].min()) if not prev_5.empty else None
    avg_volume_3 = _safe_float(recent_3["成交量"].mean())
    avg_volume_prev_10 = _safe_float(working_df.tail(13).head(10)["成交量"].mean())
    ma5 = _safe_float(working_df.tail(5)["收盘"].mean())
    ma10 = _safe_float(working_df.tail(10)["收盘"].mean())

    if None in {max_close_30, min_low_30, latest_close, latest_open, latest_low, latest_volume, avg_volume_3, avg_volume_prev_10}:
        return None

    drawdown_pct = (max_close_30 - latest_close) / max_close_30 * 100 if max_close_30 else 0.0
    volume_spike = avg_volume_3 > avg_volume_prev_10 * 1.2 if avg_volume_prev_10 else False
    stop_falling = latest_low >= (prev_low_min or latest_low)
    bullish_break = latest_close > latest_open and latest_volume > (prev_volume or 0)
    trend_turn = latest_close >= (ma5 or latest_close) and (ma5 or 0) >= (ma10 or 0)
    volatility_risk = abs((latest_close - latest_open) / latest_open * 100) if latest_open else 0.0

    score = 35
    reason_flags: list[str] = []
    if drawdown_pct >= 15:
        score += 20
        reason_flags.append("回撤充分")
    if volume_spike:
        score += 15
        reason_flags.append("量能放大")
    if stop_falling:
        score += 12
        reason_flags.append("止跌企稳")
    if bullish_break:
        score += 12
        reason_flags.append("放量阳线")
    if trend_turn:
        score += 14
        reason_flags.append("均线拐头")
    if volatility_risk >= 7:
        score -= 12
    elif volatility_risk >= 5:
        score -= 6

    score = max(35, min(95, int(round(score))))
    signal = _resolve_stock_signal(score)
    risk = "高" if volatility_risk >= 7 else "中高" if volatility_risk >= 5 else "中"

    return {
        "code": code,
        "name": name,
        "score": score,
        "signal": signal,
        "reason": _build_stock_reason(reason_flags),
        "summary": f"近30日回撤约{_format_drawdown(drawdown_pct)}，当前处于低位修复观察阶段。",
        "features": {
            "volume_spike": bool(volume_spike),
            "trend_turn": bool(trend_turn),
            "drawdown": _format_drawdown(drawdown_pct),
            "stop_falling": bool(stop_falling),
            "bullish_break": bool(bullish_break),
            "risk": risk,
        },
    }


def get_stock_low_opportunity_pool() -> list[dict[str, Any]]:
    """Return stock low-position opportunities using a small candidate universe."""
    analyzed_items: list[dict[str, Any]] = []
    for item in STOCK_LOW_OPPORTUNITY_CANDIDATES:
        try:
            analyzed = _analyze_stock_low_candidate(item["code"], item["name"])
            if analyzed is not None:
                analyzed_items.append(analyzed)
        except Exception as exc:  # pragma: no cover - runtime safety
            LOGGER.warning("Failed to analyze stock low opportunity %s %s: %s", item["code"], item["name"], exc)

    if not analyzed_items:
        return STOCK_LOW_OPPORTUNITY_FALLBACK

    analyzed_items.sort(key=lambda entry: int(entry.get("score", 0)), reverse=True)
    return analyzed_items[:8]
