from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from modules.stock_score_service import StockScoreService, safe_float


ACTIVE_TURNOVER_THRESHOLD = 1_000_000_000.0
_STOCK_SCORE_SERVICE = StockScoreService()


def _as_mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _first_number(*values: Any, default: float = 0.0) -> float:
    """Return the first non-zero numeric value from available fields."""
    for value in values:
        parsed = safe_float(value, default=0.0)
        if parsed:
            return parsed
    return default


def _first_float(*values: Any, default: float = 0.0) -> float:
    """Return the first present numeric value, keeping zero as a valid value."""
    for value in values:
        if value is None or value == "":
            continue
        return safe_float(value, default=default)
    return default


def _build_score_input(market_data: Mapping[str, Any], fund_flow: Mapping[str, Any] | None) -> dict[str, Any]:
    """Adapt current market/fund fields into the StockScoreService input shape."""
    price = _first_number(
        market_data.get("price"),
        market_data.get("latest_price"),
        market_data.get("close"),
    )
    change_pct = _first_float(market_data.get("change_pct"), market_data.get("pct_change"), default=0.0)
    turnover_rate = safe_float(market_data.get("turnover_rate"), default=0.0)
    volume_ratio = safe_float(market_data.get("volume_ratio"), default=0.0)
    fund_flow = _as_mapping(fund_flow)
    main_net_inflow = safe_float(fund_flow.get("main_net_inflow"), default=0.0)

    # Historical fields may not exist yet; fall back to current high/low/price so scoring stays safe.
    high = _first_number(market_data.get("high"), price)
    low = _first_number(market_data.get("low"), price)
    high_60d = _first_number(market_data.get("high_60d"), market_data.get("high_60"), high)
    low_60d = _first_number(market_data.get("low_60d"), market_data.get("low_60"), low)
    high_120d = _first_number(market_data.get("high_120d"), market_data.get("high_120"), high_60d)
    low_120d = _first_number(market_data.get("low_120d"), market_data.get("low_120"), low_60d)

    ma5 = safe_float(market_data.get("ma5"), default=price)
    ma10 = safe_float(market_data.get("ma10"), default=ma5)
    ma20 = safe_float(market_data.get("ma20"), default=ma10)
    ma60 = safe_float(market_data.get("ma60"), default=ma20)

    return {
        "price": price,
        "change_pct": change_pct,
        "volume_ratio": volume_ratio,
        "turnover_rate": turnover_rate,
        "main_net_inflow": main_net_inflow,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma60": ma60,
        "high_60d": high_60d,
        "low_60d": low_60d,
        "high_120d": high_120d,
        "low_120d": low_120d,
    }


def analyze_stock(market_data: Mapping[str, Any] | None, fund_flow: Mapping[str, Any] | None) -> dict[str, Any]:
    """Analyze a stock snapshot and attach the stock scoring model result."""
    market_data = _as_mapping(market_data)
    fund_flow = _as_mapping(fund_flow)
    signal: list[str] = []
    risk: list[str] = []

    pct_change = _first_float(market_data.get("pct_change"), market_data.get("change_pct"), default=0.0)
    turnover = safe_float(market_data.get("turnover"), default=0.0)
    main_net_inflow = safe_float(fund_flow.get("main_net_inflow"), default=0.0)

    score = _STOCK_SCORE_SERVICE.score(_build_score_input(market_data, fund_flow))

    if pct_change > 3:
        signal.append("今日涨幅较强")
    elif pct_change < -3:
        risk.append("今日跌幅较大")

    if turnover >= ACTIVE_TURNOVER_THRESHOLD:
        signal.append("成交活跃")

    if main_net_inflow > 0:
        signal.append("主力资金净流入")
    elif main_net_inflow < 0:
        risk.append("主力资金净流出")

    for tag in score.get("tags", []):
        tag_text = str(tag).strip()
        if not tag_text:
            continue
        if any(keyword in tag_text for keyword in ("偏高", "过热", "流出", "下跌")):
            if tag_text not in risk:
                risk.append(tag_text)
        elif tag_text not in signal:
            signal.append(tag_text)

    summary = [score.get("conclusion", "当前信号一般，暂时不算理想机会")]
    summary.extend(signal)
    summary.extend(risk)

    return {
        "signal": signal,
        "risk": risk,
        "summary": summary,
        "score": score,
        "total_score": score.get("total_score", 0),
        "level": score.get("level", "D"),
        "sub_scores": score.get("sub_scores", {}),
        "conclusion": score.get("conclusion", ""),
        "tags": score.get("tags", []),
        "details": score.get("details", {}),
        "dimension_scores": {
            "low_position": (score.get("sub_scores") or {}).get("low", 0),
            "volume_change": (score.get("sub_scores") or {}).get("volume", 0),
            "trend_strength": (score.get("sub_scores") or {}).get("trend", 0),
            "fund_support": (score.get("sub_scores") or {}).get("capital", 0),
        },
    }


def self_test() -> None:
    """Run a local pure-function smoke test for analyzer rules."""
    market_data = {
        "latest_price": 1523.66,
        "pct_change": 2.38,
        "turnover": 1_860_000_000,
        "turnover_rate": 2.1,
        "volume_ratio": 1.4,
        "high": 1530.0,
        "low": 1498.0,
    }
    fund_flow = {
        "main_net_inflow": 58_000_000,
    }
    result = analyze_stock(market_data, fund_flow)
    print("analyzer 自测结果：")
    print(result)


if __name__ == "__main__":
    self_test()
