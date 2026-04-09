from __future__ import annotations

import time
from typing import Any

from config import LOGGER
from modules.etf_watchlist_service import load_etf_watchlist
from modules.fetch_etf import get_all_etf_spot_data, get_etf_by_code


def _safe_float(value: Any) -> float:
    """Convert any runtime value into float safely."""
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


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


def process_etf(
    etf: dict[str, str],
    *,
    all_etf_data: dict[str, dict[str, Any]] | None = None,
    avg_turnover: float = 0.0,
) -> dict[str, Any]:
    """Process one ETF and return a structured result."""
    code = str(etf.get("code", "")).strip()
    name = str(etf.get("name", "")).strip() or code
    LOGGER.info("Processing ETF %s %s", code, name)

    try:
        etf_data = (all_etf_data or {}).get(code) or get_etf_by_code(code)
        if not etf_data:
            raise ValueError(f"未获取到 ETF 行情数据: code={code}")

        score, tags = _calculate_etf_score(etf_data, avg_turnover)
        summary = _build_etf_summary(etf_data, tags)

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
            "error": str(exc),
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
    selected_snapshots = [(all_etf_data or {}).get(item["code"], {}) for item in etfs]
    turnovers = [
        _safe_float(snapshot.get("turnover"))
        for snapshot in selected_snapshots
        if _safe_float(snapshot.get("turnover")) > 0
    ]
    avg_turnover = sum(turnovers) / len(turnovers) if turnovers else 0.0

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
