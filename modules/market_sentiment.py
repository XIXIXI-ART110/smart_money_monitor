from __future__ import annotations

from typing import Any


def _to_float(value: Any) -> float:
    """Convert arbitrary input into float, defaulting to 0.0 on failure."""
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def analyze_market_sentiment(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Analyze overall market sentiment from per-stock run results."""
    valid_results = [item for item in results if isinstance(item, dict)]
    total_count = len(valid_results)

    total_inflow = 0.0
    total_change = 0.0
    positive_count = 0

    for item in valid_results:
        market_data = item.get("market_data", {}) or {}
        fund_flow = item.get("fund_flow", {}) or {}

        pct_change = _to_float(market_data.get("pct_change"))
        main_net_inflow = _to_float(fund_flow.get("main_net_inflow"))

        total_change += pct_change
        total_inflow += main_net_inflow
        if pct_change > 0:
            positive_count += 1

    avg_change = total_change / total_count if total_count else 0.0
    positive_ratio = positive_count / total_count if total_count else 0.0

    if total_inflow > 0 and avg_change > 1:
        market_status = "强势"
        summary = "市场整体资金流入，情绪偏强"
    elif total_inflow < 0 and avg_change < 0:
        market_status = "风险"
        summary = "资金流出明显，注意系统性风险"
    else:
        market_status = "中性"
        summary = "市场震荡，观望为主"

    return {
        "market_status": market_status,
        "summary": summary,
        "detail": {
            "total_inflow": total_inflow,
            "avg_change": avg_change,
            "positive_ratio": positive_ratio,
        },
    }
