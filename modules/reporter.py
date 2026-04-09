from __future__ import annotations

from datetime import datetime
from typing import Any

from config import LOGGER, REPORT_DIR, ensure_runtime_directories


def _format_number(value: Any, digits: int = 2) -> str:
    """Format a numeric value into a human-readable string."""
    if value is None:
        return "N/A"
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def _format_amount(value: Any) -> str:
    """Format an amount into yuan, ten-thousand yuan, or hundred-million yuan."""
    if value is None:
        return "N/A"
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return str(value)

    absolute_amount = abs(amount)
    if absolute_amount >= 100_000_000:
        return f"{amount / 100_000_000:.2f}亿元"
    if absolute_amount >= 10_000:
        return f"{amount / 10_000:.2f}万元"
    return f"{amount:.2f}元"


def build_report(results: list[dict[str, Any]]) -> str:
    """Build a Markdown-style monitoring report."""
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: list[str] = [
        "# A股智能监控报告",
        "",
        f"- 生成时间：{generated_at}",
        f"- 覆盖股票数：{len(results)}",
        "",
    ]

    if not results:
        lines.append("- 本次没有可展示的结果，请检查自选股配置或数据源状态。")
        return "\n".join(lines)

    for item in results:
        code = item.get("code", "N/A")
        name = item.get("name", "未知股票")
        status = item.get("status", "ok")
        market_data = item.get("market_data", {}) or {}
        fund_flow = item.get("fund_flow", {}) or {}
        analysis = item.get("analysis", {}) or {}
        ai_summary = item.get("ai_summary", "本次未生成 AI 解读。")

        lines.extend(
            [
                f"## {name}（{code}）",
                f"- 执行状态：{status}",
            ]
        )

        if status != "ok":
            lines.append(f"- 错误信息：{item.get('error', '未知错误')}")
            lines.append(f"- AI总结：{ai_summary}")
            lines.append("")
            continue

        lines.extend(
            [
                f"- 最新价：{_format_number(market_data.get('latest_price'))}",
                f"- 涨跌幅：{_format_number(market_data.get('pct_change'))}%",
                f"- 成交额：{_format_amount(market_data.get('turnover'))}",
                f"- 主力净流入：{_format_amount(fund_flow.get('main_net_inflow'))}",
                f"- 规则分析：{'；'.join(analysis.get('summary', []) or ['暂无明显异动'])}",
                f"- AI总结：{ai_summary}",
                "",
            ]
        )

    return "\n".join(lines)


def save_report_to_file(content: str) -> str:
    """Save a report to the reports directory and return the path string."""
    ensure_runtime_directories()
    report_name = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    report_path = REPORT_DIR / report_name
    report_path.write_text(content, encoding="utf-8")
    LOGGER.info("Report saved to %s", report_path)
    return str(report_path)


def self_test() -> None:
    """Run a local smoke test for report rendering and saving."""
    results = [
        {
            "code": "600519",
            "name": "贵州茅台",
            "status": "ok",
            "market_data": {
                "latest_price": 1523.66,
                "pct_change": 2.38,
                "turnover": 1_860_000_000,
            },
            "fund_flow": {
                "main_net_inflow": 58_000_000,
            },
            "analysis": {
                "summary": ["成交活跃", "主力资金净流入"],
            },
            "ai_summary": "量价与主力资金表现偏活跃，适合继续跟踪资金持续性与后续公告信息。",
        }
    ]
    content = build_report(results)
    path = save_report_to_file(content)
    print("reporter 自测成功，报告已保存：")
    print(path)


if __name__ == "__main__":
    self_test()
