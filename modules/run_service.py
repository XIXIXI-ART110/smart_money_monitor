from __future__ import annotations

import time
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError, as_completed
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


_TIMEOUT_SENTINEL = object()


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
    total_score = analysis.get("total_score")
    if total_score is not None:
        try:
            return int(total_score)
        except (TypeError, ValueError):
            pass

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
        if score >= 75:
            level = "strong"
        elif score >= 60:
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

    if score >= 75:
        return "高弹进攻"
    if score >= 60:
        return "机会跟踪"
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


def _call_with_timeout(
    func: Any,
    *args: Any,
    timeout_seconds: float,
    step_name: str,
    **kwargs: Any,
) -> Any:
    """Run one blocking step with timeout protection and timing logs."""
    started = time.perf_counter()
    result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)

    def runner() -> None:
        try:
            result_queue.put((True, func(*args, **kwargs)))
        except Exception as exc:  # pragma: no cover - runtime safety
            result_queue.put((False, exc))

    # Daemon thread keeps a stuck data-source call from holding the API response open.
    thread = threading.Thread(target=runner, name=f"run-once-step-{step_name}", daemon=True)
    thread.start()
    try:
        ok, result = result_queue.get(timeout=timeout_seconds)
        if not ok:
            raise result
        LOGGER.info("%s finished in %.3fs.", step_name, time.perf_counter() - started)
        return result
    except queue.Empty:
        LOGGER.warning("%s timed out after %.2fs.", step_name, timeout_seconds)
        return _TIMEOUT_SENTINEL
    except FutureTimeoutError:
        LOGGER.warning("%s timed out after %.2fs.", step_name, timeout_seconds)
        return _TIMEOUT_SENTINEL
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("%s failed in %.3fs: %s", step_name, time.perf_counter() - started, exc)
        return exc


def _build_fast_summary(
    name: str,
    market_data: dict[str, Any],
    analysis: dict[str, Any],
    *,
    error: str = "",
) -> str:
    """Build a fast summary without waiting on external AI services."""
    if error:
        return f"{name} 本次仅返回基础结果：{error}"

    conclusion = str(analysis.get("conclusion", "")).strip()
    if conclusion:
        return conclusion

    summary_items = [str(item).strip() for item in (analysis.get("summary", []) or []) if str(item).strip()]
    if summary_items:
        return "；".join(summary_items[:2])

    pct_change = market_data.get("pct_change")
    if pct_change is not None:
        return f"{name} 当前涨跌幅 {pct_change}%，建议继续观察量价配合。"
    return f"{name} 已返回基础分析结果，建议结合后续量价变化继续观察。"


def _build_error_result(code: str, name: str, error: str) -> dict[str, Any]:
    """Return one safe stock result when a single stock fails."""
    error_analysis = {
        "signal": [],
        "risk": [f"数据抓取失败: {error}"],
        "summary": ["本次未能完成规则分析"],
        "dimension_scores": {
            "low_position": 0,
            "volume_change": 0,
            "trend_strength": 0,
            "fund_support": 0,
        },
        "total_score": 0,
        "conclusion": "数据不足，暂时观望",
    }
    return {
        "code": code,
        "name": name,
        "status": "error",
        "score": _calculate_score(error_analysis),
        "error": error,
        "market_data": {
            "code": code,
            "name": name,
            "latest_price": None,
            "pct_change": None,
            "turnover": None,
        },
        "fund_flow": {},
        "analysis": error_analysis,
        "ai_summary": f"{name} 本次数据获取失败，建议稍后重试。",
    }


def process_stock(
    stock: dict[str, str],
    all_spot_data: dict[str, dict[str, Any]] | None = None,
    *,
    enable_ai_summary: bool = False,
    market_timeout_seconds: float = 2.5,
    fund_flow_timeout_seconds: float = 2.5,
    ai_timeout_seconds: float = 4.0,
) -> dict[str, Any]:
    """Process one stock end to end and return a structured result."""
    del all_spot_data  # compatibility placeholder; current fast path fetches per stock.
    stock_started = time.perf_counter()
    code = normalize_code(stock["code"])
    name = stock.get("name", code)
    LOGGER.info("Processing stock %s", code)

    market_error = ""
    fund_error = ""
    market_data: dict[str, Any] = {
        "code": code,
        "name": name,
        "latest_price": None,
        "pct_change": None,
        "turnover": None,
    }
    fund_flow: dict[str, Any] = {}

    try:
        market_result = _call_with_timeout(
            fetch_stock_data,
            code,
            name,
            timeout_seconds=market_timeout_seconds,
            step_name=f"[{code}] fetch_stock_data",
        )
        if market_result is _TIMEOUT_SENTINEL:
            market_error = "实时行情超时"
        elif isinstance(market_result, Exception):
            market_error = str(market_result)
        elif isinstance(market_result, dict) and market_result and market_result.get("status") != "error":
            market_data.update(market_result)
        else:
            market_error = str((market_result or {}).get("error", "未获取到实时行情数据"))

        fund_result = _call_with_timeout(
            get_individual_fund_flow,
            code,
            timeout_seconds=fund_flow_timeout_seconds,
            step_name=f"[{code}] get_individual_fund_flow",
        )
        if fund_result is _TIMEOUT_SENTINEL:
            fund_error = "资金流超时"
        elif isinstance(fund_result, Exception):
            fund_error = str(fund_result)
        elif isinstance(fund_result, dict):
            fund_flow = fund_result
        else:
            fund_error = "资金流返回为空"

        analysis_started = time.perf_counter()
        analysis = analyze_stock(market_data, fund_flow or None)
        LOGGER.info("[%s] analyze_stock finished in %.3fs.", code, time.perf_counter() - analysis_started)
        score = _calculate_score(analysis)

        has_market = any(market_data.get(key) is not None for key in ("latest_price", "pct_change", "turnover"))
        has_fund = (fund_flow or {}).get("main_net_inflow") is not None
        if not has_market and not has_fund:
            error_parts = [part for part in (market_error, fund_error) if part]
            return _build_error_result(code, name, "；".join(error_parts) or "未获取到有效分析数据")

        if enable_ai_summary:
            ai_result = _call_with_timeout(
                summarize_with_ai,
                name,
                market_data,
                analysis,
                timeout_seconds=ai_timeout_seconds,
                step_name=f"[{code}] summarize_with_ai",
            )
            if ai_result is _TIMEOUT_SENTINEL or isinstance(ai_result, Exception):
                ai_summary = _build_fast_summary(name, market_data, analysis, error="AI 摘要超时")
            else:
                ai_summary = str(ai_result)
        else:
            ai_summary = _build_fast_summary(name, market_data, analysis, error=market_error or fund_error)

        LOGGER.info(
            "[%s] stock pipeline finished in %.3fs. market_error=%s fund_error=%s score=%s",
            code,
            time.perf_counter() - stock_started,
            bool(market_error),
            bool(fund_error),
            score,
        )
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
        return _build_error_result(code, name, str(exc))


def run_once_service(
    *,
    push_notification: bool = True,
    print_report: bool = False,
    enable_ai_summary: bool = False,
    market_timeout_seconds: float = 2.5,
    fund_flow_timeout_seconds: float = 2.5,
    ai_timeout_seconds: float = 4.0,
    total_timeout_seconds: float = 10.0,
    max_workers: int = 3,
) -> dict[str, Any]:
    """Execute the full run-once workflow and return a structured payload."""
    ensure_runtime_directories()
    start_time = time.perf_counter()
    LOGGER.info(
        "Run-once service started. enable_ai_summary=%s push_notification=%s total_timeout=%.2fs",
        enable_ai_summary,
        push_notification,
        total_timeout_seconds,
    )

    watchlist_started = time.perf_counter()
    stocks = load_watchlist()
    LOGGER.info("Watchlist loaded in %.3fs. stock_count=%s", time.perf_counter() - watchlist_started, len(stocks))
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

    results: list[dict[str, Any]] = []
    stock_executor = ThreadPoolExecutor(
        max_workers=max(1, min(max_workers, len(stocks))),
        thread_name_prefix="run-once-stock",
    )
    future_map = {
        stock_executor.submit(
            process_stock,
            stock,
            None,
            enable_ai_summary=enable_ai_summary,
            market_timeout_seconds=market_timeout_seconds,
            fund_flow_timeout_seconds=fund_flow_timeout_seconds,
            ai_timeout_seconds=ai_timeout_seconds,
        ): stock
        for stock in stocks
    }
    stock_loop_started = time.perf_counter()

    try:
        try:
            for future in as_completed(future_map, timeout=total_timeout_seconds):
                stock = future_map.pop(future)
                code = normalize_code(stock["code"])
                name = stock.get("name", code)
                try:
                    result = future.result()
                except Exception as exc:  # pragma: no cover - runtime safety
                    LOGGER.exception("Unhandled stock future failure for %s %s: %s", code, name, exc)
                    result = _build_error_result(code, name, str(exc))
                results.append(result)
        except FutureTimeoutError:
            LOGGER.warning("Run-once stock loop timed out after %.2fs.", total_timeout_seconds)
    finally:
        unfinished_stocks = [stock for future, stock in future_map.items() if not future.done()]
        stock_executor.shutdown(wait=False, cancel_futures=True)

    for stock in unfinished_stocks:
        code = normalize_code(stock["code"])
        name = stock.get("name", code)
        LOGGER.warning("Stock %s %s exceeded overall run timeout and was skipped.", code, name)
        results.append(_build_error_result(code, name, "单只股票处理超时，已跳过"))

    LOGGER.info(
        "Stock processing finished in %.3fs. completed=%s total=%s",
        time.perf_counter() - stock_loop_started,
        len(results),
        len(stocks),
    )

    results = sorted(results, key=lambda item: int(item.get("score", 0)), reverse=True)

    sentiment_started = time.perf_counter()
    opportunity_rank = _build_opportunity_rank(results)
    market_sentiment = analyze_market_sentiment(results)
    style_distribution = _build_style_distribution(results)
    LOGGER.info("Post-analysis aggregation finished in %.3fs.", time.perf_counter() - sentiment_started)

    review_started = time.perf_counter()
    try:
        save_daily_opportunity_record(
            market_conclusion=str(market_sentiment.get("summary", "")),
            opportunity_rank=opportunity_rank,
        )
        LOGGER.info("Opportunity review save finished in %.3fs.", time.perf_counter() - review_started)
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Failed to save opportunity review: %s", exc)

    report_started = time.perf_counter()
    report_content = build_report(results)
    report_path = save_report_to_file(report_content)
    LOGGER.info("Report build/save finished in %.3fs.", time.perf_counter() - report_started)

    if print_report:
        print(report_content)

    notification_result: dict[str, Any]
    notification_started = time.perf_counter()
    if push_notification:
        try:
            feishu_message = build_feishu_daily_report(
                opportunity_rank=opportunity_rank,
                market_sentiment=market_sentiment,
                results=results,
            )
            notification_result = send_feishu_text(feishu_message)
        except Exception as exc:  # pragma: no cover - runtime safety
            LOGGER.exception("Failed to send notification: %s", exc)
            notification_result = {
                "sent": False,
                "reason": str(exc),
            }
    else:
        notification_result = {
            "sent": False,
            "reason": "notification_disabled",
        }
    LOGGER.info("Notification step finished in %.3fs.", time.perf_counter() - notification_started)

    elapsed_seconds = round(time.perf_counter() - start_time, 3)
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
