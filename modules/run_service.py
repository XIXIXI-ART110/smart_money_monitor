from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from config import BASE_DIR, LOGGER, ensure_runtime_directories
from modules.ai_summary import summarize_with_ai
from modules.analyzer import analyze_stock
from modules.fetch_fund_flow import get_individual_fund_flow
from modules.fetch_market import fetch_stock_data, normalize_code
from modules.market_sentiment import analyze_market_sentiment
from modules.notify import build_feishu_daily_report, send_feishu_text
from modules.opportunity_review import save_daily_opportunity_record
from modules.reporter import build_report, save_report_to_file
from modules.watchlist_service import load_watchlist


def _to_relative_path(path_str: str) -> str:
    """Convert an absolute path string into a project-relative path when possible."""
    path = Path(path_str)
    try:
        return str(path.relative_to(BASE_DIR)).replace("\\", "/")
    except ValueError:
        return str(path)


def _safe_float(value: Any) -> float:
    """Convert runtime values into float safely."""
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _calculate_score(analysis: dict[str, Any]) -> int:
    """Calculate a score from signals and risks."""
    score = 0
    signals = list(analysis.get("signal", []))
    risks = list(analysis.get("risk", []))

    if "主力资金净流入" in signals:
        score += 2
    if "成交活跃" in signals:
        score += 1
    if "今日涨幅较强" in signals:
        score += 2
    if "主力资金流出" in risks or "主力资金净流出" in risks:
        score -= 2

    return score


def generate_ai_advice(signals: list[str], risks: list[str]) -> str:
    """Generate a one-line paid-version opportunity advice."""
    if "主力资金净流入" in signals and "今日涨幅较强" in signals:
        return "短线强势，可关注"
    if "成交活跃" in signals:
        return "活跃标的，可跟踪"
    if risks:
        return "存在风险，建议观望"
    return "暂无明显机会"


def _build_opportunity_rank(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build the paid-version opportunity ranking."""
    rank: list[dict[str, Any]] = []

    for item in results:
        if item.get("status") != "ok":
            continue

        score = int(item.get("score", 0))
        if score >= 5:
            level = "strong"
        elif score >= 3:
            level = "medium"
        else:
            continue

        analysis = item.get("analysis", {}) or {}
        signals = list(analysis.get("signal", []))
        risks = list(analysis.get("risk", []))
        market_data = item.get("market_data", {}) or {}
        fund_flow = item.get("fund_flow", {}) or {}

        rank.append(
            {
                "code": str(item.get("code", "")),
                "name": str(item.get("name", "")),
                "score": score,
                "level": level,
                "ai_advice": generate_ai_advice(signals, risks),
                "signals": signals,
                "risks": risks,
                "pct_change": market_data.get("pct_change"),
                "latest_price": market_data.get("latest_price"),
                "turnover": market_data.get("turnover"),
                "main_net_inflow": fund_flow.get("main_net_inflow"),
            }
        )

    rank.sort(key=lambda item: int(item.get("score", 0)), reverse=True)
    return rank


def _classify_style(result: dict[str, Any]) -> str:
    """Assign a lightweight style label from current result metrics."""
    if result.get("status") != "ok":
        return "待观察"

    score = int(result.get("score", 0))
    market_data = result.get("market_data", {}) or {}
    fund_flow = result.get("fund_flow", {}) or {}
    pct_change = _safe_float(market_data.get("pct_change"))
    inflow = _safe_float(fund_flow.get("main_net_inflow"))

    if score >= 4:
        return "高弹进攻"
    if inflow > 0 and pct_change > 0:
        return "机构回流"
    if pct_change > 1:
        return "趋势强化"
    if inflow < 0 and pct_change < 0:
        return "风险释放"
    if abs(pct_change) <= 1:
        return "震荡整理"
    return "待观察"


def _build_style_distribution(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate monitored stocks into a simple style-flow distribution view."""
    buckets: dict[str, dict[str, Any]] = {}

    for item in results:
        label = _classify_style(item)
        market_data = item.get("market_data", {}) or {}
        fund_flow = item.get("fund_flow", {}) or {}
        inflow = _safe_float(fund_flow.get("main_net_inflow"))
        pct_change = _safe_float(market_data.get("pct_change"))

        bucket = buckets.setdefault(
            label,
            {
                "label": label,
                "net_inflow": 0.0,
                "stock_count": 0,
                "avg_change": 0.0,
                "leaders": [],
            },
        )
        bucket["net_inflow"] += inflow
        bucket["stock_count"] += 1
        bucket["avg_change"] += pct_change
        if item.get("name"):
            bucket["leaders"].append(str(item["name"]))

    distribution: list[dict[str, Any]] = []
    for bucket in buckets.values():
        stock_count = int(bucket["stock_count"])
        distribution.append(
            {
                "label": bucket["label"],
                "net_inflow": round(float(bucket["net_inflow"]), 3),
                "stock_count": stock_count,
                "avg_change": round(
                    float(bucket["avg_change"]) / stock_count if stock_count else 0.0,
                    3,
                ),
                "leaders": bucket["leaders"][:3],
            }
        )

    distribution.sort(key=lambda item: abs(float(item.get("net_inflow", 0.0))), reverse=True)
    return distribution


def process_stock(
    stock: dict[str, str],
    all_spot_data: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Process one stock end to end and return a structured result."""
    code = normalize_code(stock["code"])
    name = stock.get("name", code)
    LOGGER.info("Processing stock %s %s", code, name)

    try:
        market_data = fetch_stock_data(code, name)
        if not market_data or market_data.get("status") == "error":
            raise ValueError(str(market_data.get("error", f"未获取到实时行情数据: code={code}")))

        market_data["code"] = code
        market_data["name"] = name

        fund_flow = get_individual_fund_flow(code)
        analysis = analyze_stock(market_data, fund_flow)
        ai_summary = summarize_with_ai(name, market_data, analysis)
        score = _calculate_score(analysis)

        return {
            "code": code,
            "name": name,
            "status": "ok",
            "score": score,
            "market_data": market_data,
            "fund_flow": fund_flow,
            "analysis": analysis,
            "ai_summary": ai_summary,
        }
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Failed to process stock %s %s: %s", code, name, exc)
        error_analysis = {
            "signal": [],
            "risk": [f"数据抓取失败: {exc}"],
            "summary": ["本次未能完成规则分析"],
        }
        return {
            "code": code,
            "name": name,
            "status": "error",
            "score": _calculate_score(error_analysis),
            "error": str(exc),
            "market_data": {},
            "fund_flow": None,
            "analysis": error_analysis,
            "ai_summary": "本次数据获取失败，建议稍后重试并结合其他公开信息继续观察。",
        }


def run_once_service(
    *,
    push_notification: bool = True,
    print_report: bool = False,
) -> dict[str, Any]:
    """Execute the full run-once workflow and return a structured payload."""
    ensure_runtime_directories()
    start_time = time.perf_counter()
    LOGGER.info("Run-once service started.")

    stocks = load_watchlist()
    if not stocks:
        message = "请先添加自选股"
        LOGGER.warning(message)
        return {
            "ok": False,
            "message": message,
            "report_path": None,
            "report_content": "",
            "results": [],
            "opportunity_rank": [],
            "elapsed_seconds": round(time.perf_counter() - start_time, 3),
            "notification": {
                "sent": False,
                "reason": "watchlist_empty",
            },
        }

    results = [process_stock(stock) for stock in stocks]
    results = sorted(results, key=lambda item: int(item.get("score", 0)), reverse=True)

    opportunity_rank = _build_opportunity_rank(results)
    market_sentiment = analyze_market_sentiment(results)
    style_distribution = _build_style_distribution(results)
    save_daily_opportunity_record(
        market_conclusion=str(market_sentiment.get("summary", "")),
        opportunity_rank=opportunity_rank,
    )

    report_content = build_report(results)
    report_path = save_report_to_file(report_content)

    if print_report:
        print(report_content)

    elapsed_seconds = round(time.perf_counter() - start_time, 3)

    notification_result: dict[str, Any]
    if push_notification:
        feishu_message = build_feishu_daily_report(
            opportunity_rank=opportunity_rank,
            market_sentiment=market_sentiment,
            results=results,
        )
        notification_result = send_feishu_text(feishu_message)
    else:
        notification_result = {
            "sent": False,
            "reason": "notification_disabled",
        }

    LOGGER.info("Feishu notification result: %s", notification_result)
    LOGGER.info("Run-once service finished in %.3f seconds.", elapsed_seconds)

    return {
        "ok": True,
        "message": "run once completed",
        "report_path": _to_relative_path(report_path),
        "report_content": report_content,
        "results": results,
        "opportunity_rank": opportunity_rank,
        "elapsed_seconds": elapsed_seconds,
        "market_sentiment": market_sentiment,
        "style_distribution": style_distribution,
        "notification": {
            "sent": bool(notification_result.get("sent")),
            "reason": str(notification_result.get("reason", "")),
        },
    }
