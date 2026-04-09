from __future__ import annotations

from typing import Any

from config import LOGGER, OPENAI_API_KEY, OPENAI_MODEL

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - depends on runtime environment
    OpenAI = None  # type: ignore[assignment]


def _truncate_text(text: str, limit: int = 100) -> str:
    """Trim text to a bounded length."""
    cleaned = " ".join(text.split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1] + "…"


def _build_fallback_summary(stock_name: str, analysis: dict[str, Any]) -> str:
    """Build a deterministic summary when AI is unavailable."""
    highlights = analysis.get("summary") or ["暂无明显异动"]
    base_text = f"{stock_name}当前观察点：{'、'.join(highlights)}。以上仅供研究参考，请结合基本面与市场环境持续跟踪。"
    return _truncate_text(base_text, 100)


def _extract_response_text(response: Any) -> str:
    """Extract plain text from different OpenAI SDK response shapes."""
    output_text = getattr(response, "output_text", "")
    if output_text:
        return str(output_text)

    output = getattr(response, "output", None)
    if not output:
        return ""

    texts: list[str] = []
    for item in output:
        content = getattr(item, "content", None) or []
        for part in content:
            text = getattr(part, "text", None)
            if text:
                texts.append(str(text))
    return "\n".join(texts).strip()


def summarize_with_ai(stock_name: str, market_data: dict[str, Any], analysis: dict[str, Any]) -> str:
    """Generate a concise Chinese research summary with the OpenAI SDK."""
    if not OPENAI_API_KEY:
        LOGGER.warning("OPENAI_API_KEY is not configured. Falling back to rule-based summary.")
        return _build_fallback_summary(stock_name, analysis)

    if OpenAI is None:
        LOGGER.error("OpenAI SDK is unavailable. Falling back to rule-based summary.")
        return _build_fallback_summary(stock_name, analysis)

    latest_price = market_data.get("latest_price")
    pct_change = market_data.get("pct_change")
    turnover = market_data.get("turnover")
    signals = "、".join(analysis.get("signal", [])) or "无明显正向信号"
    risks = "、".join(analysis.get("risk", [])) or "无明显风险提示"

    prompt = f"""
你是一名谨慎克制的A股投研助手。请根据给定数据，输出一段不超过100字的中文观察总结。

要求：
1. 只做数据观察与风险提示，不提供买卖建议，不做收益承诺。
2. 避免使用“必须买入”“肯定上涨”等绝对化表达。
3. 文风简洁、专业、克制。

股票名称：{stock_name}
最新价：{latest_price}
涨跌幅：{pct_change}%
成交额：{turnover}
规则正向信号：{signals}
规则风险提示：{risks}
规则摘要：{'、'.join(analysis.get("summary", [])) or '暂无明显异动'}
""".strip()

    try:
        client = OpenAI(api_key=OPENAI_API_KEY, timeout=20.0)

        try:
            response = client.responses.create(
                model=OPENAI_MODEL,
                input=prompt,
                max_output_tokens=180,
            )
            text = _extract_response_text(response)
        except AttributeError:
            completion = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": "你是谨慎克制的A股投研助手，只输出简洁中文观察结论，不给交易指令。",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=180,
            )
            text = completion.choices[0].message.content or ""

        if not text.strip():
            LOGGER.warning("OpenAI returned an empty summary for %s.", stock_name)
            return _build_fallback_summary(stock_name, analysis)

        return _truncate_text(text, 100)
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Failed to generate AI summary for %s: %s", stock_name, exc)
        return _build_fallback_summary(stock_name, analysis)


def self_test() -> None:
    """Run a minimal smoke test for AI summary generation."""
    market_data = {
        "latest_price": 1523.66,
        "pct_change": 2.38,
        "turnover": 1_860_000_000,
    }
    analysis = {
        "signal": ["成交活跃", "主力资金净流入"],
        "risk": [],
        "summary": ["成交活跃", "主力资金净流入"],
    }
    summary = summarize_with_ai("贵州茅台", market_data, analysis)
    print("ai_summary 自测结果：")
    print(summary)


if __name__ == "__main__":
    self_test()
