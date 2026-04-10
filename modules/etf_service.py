from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any

import akshare as ak
import pandas as pd

from config import LOGGER
from modules.etf_watchlist_service import load_etf_watchlist
from modules.fetch_etf import get_all_etf_spot_data, get_etf_by_code


DEFAULT_RECOMMENDED_ETFS: list[dict[str, str]] = [
    {"code": "510300", "name": "沪深300ETF"},
    {"code": "510500", "name": "中证500ETF"},
    {"code": "159915", "name": "创业板ETF"},
    {"code": "512100", "name": "中证1000ETF"},
    {"code": "588000", "name": "科创50ETF"},
    {"code": "513100", "name": "纳指ETF"},
    {"code": "513500", "name": "标普500ETF"},
    {"code": "159949", "name": "创业板50ETF"},
]


def _safe_float(value: Any) -> float:
    """Convert any runtime value into float safely."""
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_round(value: Any, digits: int = 3) -> float | None:
    """Round numeric values safely when present."""
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _merge_unique_etfs(*groups: list[dict[str, str]]) -> list[dict[str, str]]:
    """Merge multiple ETF lists into one stable unique list."""
    merged: list[dict[str, str]] = []
    seen_codes: set[str] = set()

    for group in groups:
        for item in group:
            code = str(item.get("code", "")).strip()
            name = str(item.get("name", "")).strip() or code
            if not code or code in seen_codes:
                continue
            merged.append({"code": code, "name": name})
            seen_codes.add(code)

    return merged


def _get_recommended_etfs() -> list[dict[str, str]]:
    """Return default recommended ETFs plus user-added watchlist items."""
    return _merge_unique_etfs(DEFAULT_RECOMMENDED_ETFS, load_etf_watchlist())


def _calculate_etf_score(etf_data: dict[str, Any], avg_turnover: float) -> tuple[int, list[str]]:
    """Calculate a simple ETF score and summary tags."""
    score = 0
    tags: list[str] = []

    pct_change = _safe_float(etf_data.get("pct_change"))
    turnover = _safe_float(etf_data.get("turnover"))
    main_net_inflow = _safe_float(etf_data.get("main_net_inflow"))

    if pct_change > 1:
        score += 2
        tags.append("涨幅偏强")
    elif pct_change < -1.5:
        tags.append("波动偏弱")

    if avg_turnover > 0 and turnover > avg_turnover:
        score += 1
        tags.append("成交放大")

    if main_net_inflow > 0:
        score += 2
        tags.append("资金流入")

    return score, tags


def _build_etf_summary(etf_data: dict[str, Any], tags: list[str]) -> str:
    """Build a short ETF summary for the frontend."""
    if tags:
        return "、".join(tags)

    pct_change = _safe_float(etf_data.get("pct_change"))
    if pct_change > 0:
        return "走势偏稳，短线表现温和偏强"
    if pct_change < 0:
        return "走势偏弱，短线关注止跌信号"
    return "波动有限，暂未出现明显风格信号"


def _derive_signal(score: int, pct_change: float) -> str:
    """Map score and price action into a human-friendly recommendation signal."""
    if score >= 3 and pct_change >= 0:
        return "推荐"
    if score >= 1 or pct_change > -1:
        return "观察"
    return "谨慎"


def _derive_risk_level(score: int, pct_change: float) -> str:
    """Return a simple risk level label for card display."""
    volatility = abs(pct_change)
    if volatility >= 4:
        return "高"
    if pct_change < -2 or score <= 0:
        return "中高"
    if score >= 3 and pct_change >= 0:
        return "中"
    return "中"


def _fetch_etf_hist_dataframe(code: str) -> pd.DataFrame | None:
    """Fetch recent ETF daily history for indicator calculation."""
    start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")

    try:
        dataframe = ak.fund_etf_hist_em(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
        if isinstance(dataframe, pd.DataFrame) and not dataframe.empty:
            return dataframe
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Failed to fetch ETF history for %s via fund_etf_hist_em: %s", code, exc)

    try:
        dataframe = ak.fund_etf_hist_sina(symbol=code)
        if isinstance(dataframe, pd.DataFrame) and not dataframe.empty:
            return dataframe
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Failed to fetch ETF history for %s via fund_etf_hist_sina: %s", code, exc)

    return None


def _build_indicators(code: str) -> dict[str, float | None]:
    """Calculate basic moving-average indicators from recent history."""
    dataframe = _fetch_etf_hist_dataframe(code)
    if dataframe is None or dataframe.empty:
        return {"ma5": None, "ma10": None, "ma20": None}

    working_df = dataframe.copy()
    close_column = "收盘" if "收盘" in working_df.columns else None
    if close_column is None:
        close_column = "close" if "close" in working_df.columns else None
    if close_column is None:
        return {"ma5": None, "ma10": None, "ma20": None}

    try:
        working_df[close_column] = pd.to_numeric(working_df[close_column], errors="coerce")
        closes = working_df[close_column].dropna()
        if closes.empty:
            return {"ma5": None, "ma10": None, "ma20": None}

        return {
            "ma5": _safe_round(closes.tail(5).mean()),
            "ma10": _safe_round(closes.tail(10).mean()),
            "ma20": _safe_round(closes.tail(20).mean()),
        }
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Failed to calculate ETF indicators for %s: %s", code, exc)
        return {"ma5": None, "ma10": None, "ma20": None}


def _derive_trend(price: float, pct_change: float, indicators: dict[str, float | None]) -> str:
    """Build a concise trend conclusion from price and MA levels."""
    ma5 = indicators.get("ma5")
    ma10 = indicators.get("ma10")
    ma20 = indicators.get("ma20")

    if ma5 and ma10 and ma20 and price >= ma5 >= ma10 >= ma20:
        return "震荡偏强"
    if ma5 and ma10 and price < ma5 < ma10:
        return "短线偏弱"
    if pct_change > 0.8:
        return "反弹修复"
    if pct_change < -1.2:
        return "回撤整理"
    return "区间震荡"


def _build_suggestion(signal: str, trend: str, risk_level: str) -> str:
    """Generate a direct user-facing action suggestion."""
    if signal == "推荐":
        return "可分批关注，不建议追高。"
    if signal == "观察" and trend in {"震荡偏强", "反弹修复"}:
        return "建议观察放量配合，再决定是否跟进。"
    if risk_level in {"中高", "高"}:
        return "波动偏大，建议控制仓位并耐心等待更清晰信号。"
    return "建议继续观察量能与均线变化。"


def _build_card_payload(
    *,
    code: str,
    name: str,
    latest_price: Any,
    pct_change: Any,
    score: int,
    risk_level: str,
    tag: str,
) -> dict[str, Any]:
    """Return the list-card structure used by /api/etf/list."""
    return {
        "code": code,
        "name": name,
        "tag": tag,
        "change_pct": latest_price is None and pct_change is None and None or _safe_round(pct_change, 2),
        "score": score,
        "risk_level": risk_level,
    }


def _build_etf_result(
    etf: dict[str, str],
    *,
    all_etf_data: dict[str, dict[str, Any]] | None = None,
    avg_turnover: float = 0.0,
) -> dict[str, Any]:
    """Build a unified ETF analysis structure shared by list and detail endpoints."""
    code = str(etf.get("code", "")).strip()
    name = str(etf.get("name", "")).strip() or code
    LOGGER.info("Processing ETF %s %s", code, name)

    try:
        etf_data = (all_etf_data or {}).get(code) or get_etf_by_code(code)
        if not etf_data:
            raise ValueError(f"未获取到 ETF 行情数据: code={code}")

        score, tags = _calculate_etf_score(etf_data, avg_turnover)
        summary = _build_etf_summary(etf_data, tags)
        price = _safe_float(etf_data.get("latest_price"))
        pct_change = _safe_float(etf_data.get("pct_change"))
        indicators = _build_indicators(code)
        signal = _derive_signal(score, pct_change)
        risk_level = _derive_risk_level(score, pct_change)
        trend = _derive_trend(price, pct_change, indicators)
        suggestion = _build_suggestion(signal, trend, risk_level)

        return {
            "code": code,
            "name": name,
            "status": "ok",
            "latest_price": etf_data.get("latest_price"),
            "pct_change": etf_data.get("pct_change"),
            "turnover": etf_data.get("turnover"),
            "main_net_inflow": etf_data.get("main_net_inflow"),
            "fund_direction": etf_data.get("fund_direction", "未知"),
            "score": score,
            "summary": summary,
            "tags": tags,
            "signal": signal,
            "trend": trend,
            "risk_level": risk_level,
            "suggestion": suggestion,
            "indicators": indicators,
        }
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Failed to process ETF %s %s: %s", code, name, exc)
        return {
            "code": code,
            "name": name,
            "status": "error",
            "latest_price": None,
            "pct_change": None,
            "turnover": None,
            "main_net_inflow": None,
            "fund_direction": "未知",
            "score": 0,
            "summary": "数据获取失败，请稍后重试",
            "tags": [],
            "signal": "谨慎",
            "trend": "数据缺失",
            "risk_level": "高",
            "suggestion": "当前数据不完整，建议暂不操作。",
            "indicators": {"ma5": None, "ma10": None, "ma20": None},
            "error": str(exc),
        }


def process_etf(
    etf: dict[str, str],
    *,
    all_etf_data: dict[str, dict[str, Any]] | None = None,
    avg_turnover: float = 0.0,
) -> dict[str, Any]:
    """Process one ETF and return a structured result."""
    return _build_etf_result(etf, all_etf_data=all_etf_data, avg_turnover=avg_turnover)


def _get_avg_turnover(candidates: list[dict[str, str]], all_etf_data: dict[str, dict[str, Any]]) -> float:
    """Calculate average turnover across candidate ETFs."""
    turnovers = [
        _safe_float((all_etf_data.get(item["code"], {}) or {}).get("turnover"))
        for item in candidates
        if _safe_float((all_etf_data.get(item["code"], {}) or {}).get("turnover")) > 0
    ]
    return sum(turnovers) / len(turnovers) if turnovers else 0.0


def get_default_etf_list_service() -> dict[str, Any]:
    """Return the default ETF card list for the new frontend module."""
    start_time = time.perf_counter()
    candidates = _get_recommended_etfs()
    all_etf_data = get_all_etf_spot_data()
    avg_turnover = _get_avg_turnover(candidates, all_etf_data)

    analyzed_items = [
        _build_etf_result(item, all_etf_data=all_etf_data, avg_turnover=avg_turnover)
        for item in candidates
    ]
    analyzed_items.sort(
        key=lambda item: (int(item.get("score", 0)), _safe_float(item.get("pct_change"))),
        reverse=True,
    )

    etfs = [
        _build_card_payload(
            code=str(item.get("code", "")),
            name=str(item.get("name", "")),
            latest_price=item.get("latest_price"),
            pct_change=item.get("pct_change"),
            score=int(item.get("score", 0)),
            risk_level=str(item.get("risk_level", "中")),
            tag=str(item.get("signal", "观察")),
        )
        for item in analyzed_items
    ]

    return {
        "ok": True,
        "message": "etf list loaded",
        "etfs": etfs,
        "elapsed_seconds": round(time.perf_counter() - start_time, 3),
    }


def analyze_single_etf_service(code: str) -> dict[str, Any]:
    """Return detailed analysis for one ETF code."""
    start_time = time.perf_counter()
    code = str(code).strip()
    candidates = _get_recommended_etfs()
    name_map = {item["code"]: item["name"] for item in candidates}
    target = {"code": code, "name": name_map.get(code, code)}

    all_etf_data = get_all_etf_spot_data()
    avg_turnover = _get_avg_turnover(candidates, all_etf_data)
    result = _build_etf_result(target, all_etf_data=all_etf_data, avg_turnover=avg_turnover)

    if result.get("status") != "ok":
        return {
            "ok": False,
            "message": result.get("error", "ETF 分析失败"),
            "etf": {
                "code": code,
                "name": target["name"],
                "price": None,
                "change_pct": None,
                "score": 0,
                "signal": "谨慎",
                "trend": "数据缺失",
                "risk_level": "高",
                "summary": result.get("summary", "数据获取失败，请稍后重试。"),
                "suggestion": result.get("suggestion", "当前数据不完整，建议暂不操作。"),
                "indicators": {"ma5": None, "ma10": None, "ma20": None},
            },
            "elapsed_seconds": round(time.perf_counter() - start_time, 3),
        }

    return {
        "ok": True,
        "message": "etf analysis loaded",
        "etf": {
            "code": str(result.get("code", "")),
            "name": str(result.get("name", "")),
            "price": result.get("latest_price"),
            "change_pct": result.get("pct_change"),
            "score": int(result.get("score", 0)),
            "signal": str(result.get("signal", "观察")),
            "trend": str(result.get("trend", "区间震荡")),
            "risk_level": str(result.get("risk_level", "中")),
            "summary": str(result.get("summary", "")),
            "suggestion": str(result.get("suggestion", "")),
            "indicators": result.get("indicators", {"ma5": None, "ma10": None, "ma20": None}),
            "turnover": result.get("turnover"),
            "fund_direction": result.get("fund_direction", "未知"),
            "tags": result.get("tags", []),
        },
        "elapsed_seconds": round(time.perf_counter() - start_time, 3),
    }


def run_etf_once_service() -> dict[str, Any]:
    """Run the ETF monitoring workflow once and return a structured payload."""
    start_time = time.perf_counter()
    etfs = load_etf_watchlist()
    if not etfs:
        message = "请先添加 ETF 自选池"
        LOGGER.warning(message)
        return {
            "ok": False,
            "message": message,
            "etf_results": [],
            "elapsed_seconds": round(time.perf_counter() - start_time, 3),
        }

    all_etf_data = get_all_etf_spot_data()
    avg_turnover = _get_avg_turnover(etfs, all_etf_data)

    etf_results = [
        process_etf(etf, all_etf_data=all_etf_data, avg_turnover=avg_turnover)
        for etf in etfs
    ]
    etf_results.sort(
        key=lambda item: (int(item.get("score", 0)), _safe_float(item.get("pct_change"))),
        reverse=True,
    )

    elapsed_seconds = round(time.perf_counter() - start_time, 3)
    LOGGER.info("ETF run service finished in %.3f seconds.", elapsed_seconds)

    return {
        "ok": True,
        "message": "run etf once completed",
        "etf_results": etf_results,
        "elapsed_seconds": elapsed_seconds,
    }
