from __future__ import annotations

from typing import Any


ACTIVE_TURNOVER_THRESHOLD = 1_000_000_000.0


def _to_float(value: Any) -> float | None:
    """Convert arbitrary input into a float when possible."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def analyze_stock(market_data: dict[str, Any], fund_flow: dict[str, Any] | None) -> dict[str, list[str]]:
    """Analyze a stock snapshot with simple market and fund flow rules."""
    signal: list[str] = []
    risk: list[str] = []
    summary: list[str] = []

    pct_change = _to_float(market_data.get("pct_change"))
    turnover = _to_float(market_data.get("turnover"))
    main_net_inflow = _to_float((fund_flow or {}).get("main_net_inflow"))

    if pct_change is not None:
        if pct_change > 3:
            signal.append("今日涨幅较强")
        elif pct_change < -3:
            risk.append("今日跌幅较大")

    if turnover is not None and turnover >= ACTIVE_TURNOVER_THRESHOLD:
        signal.append("成交活跃")

    if main_net_inflow is not None:
        if main_net_inflow > 0:
            signal.append("主力资金净流入")
        else:
            risk.append("主力资金净流出")

    if signal:
        summary.extend(signal)
    if risk:
        summary.extend(risk)

    if not summary:
        summary.append("暂无明显异动")

    return {
        "signal": signal,
        "risk": risk,
        "summary": summary,
    }


def self_test() -> None:
    """Run a local pure-function smoke test for analyzer rules."""
    market_data = {
        "pct_change": 4.25,
        "turnover": 2_600_000_000,
    }
    fund_flow = {
        "main_net_inflow": 58_000_000,
    }
    result = analyze_stock(market_data, fund_flow)
    print("analyzer 自测结果：")
    print(result)


if __name__ == "__main__":
    self_test()
