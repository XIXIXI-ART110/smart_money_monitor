from __future__ import annotations

from typing import Any

import requests

from config import FEISHU_WEBHOOK, LOGGER


def _score_stars(score: int) -> str:
    """Render paid-version star score."""
    normalized = max(1, min(int(score), 5))
    return "⭐" * normalized


def _build_risk_hint(results: list[dict[str, Any]]) -> str:
    """Build a concise risk hint from current results."""
    risk_items: list[str] = []
    for item in results:
        analysis = item.get("analysis", {}) or {}
        for risk in analysis.get("risk", []):
            risk_text = str(risk).strip()
            if risk_text and risk_text not in risk_items:
                risk_items.append(risk_text)

    if not risk_items:
        return "暂无显著风险提示"
    return "、".join(risk_items[:2])


def build_feishu_daily_report(
    *,
    opportunity_rank: list[dict[str, Any]],
    market_sentiment: dict[str, Any],
    results: list[dict[str, Any]],
) -> str:
    """Build the paid-version daily opportunity report."""
    strong_items = [item for item in opportunity_rank if item.get("level") == "strong"]
    medium_items = [item for item in opportunity_rank if item.get("level") == "medium"]
    market_summary = str((market_sentiment or {}).get("summary", "")).strip() or "市场暂无明确结论"
    risk_hint = _build_risk_hint(results)

    lines: list[str] = ["📊 今日机会榜", ""]

    if strong_items:
        lines.append("🔥 强机会")
        for index, item in enumerate(strong_items, start=1):
            lines.append(f"{index}️⃣ {item['name']}（{item['code']}） {_score_stars(int(item.get('score', 0)))}")
            lines.append(f"👉 {item.get('ai_advice', '暂无建议')}")
            lines.append("")

    if medium_items:
        lines.append("⚡ 次机会")
        start_index = len(strong_items) + 1
        for index, item in enumerate(medium_items, start=start_index):
            lines.append(f"{index}️⃣ {item['name']}（{item['code']}） {_score_stars(int(item.get('score', 0)))}")
            lines.append(f"👉 {item.get('ai_advice', '暂无建议')}")
            lines.append("")

    if not strong_items and not medium_items:
        lines.append("今日暂无达到机会榜标准的标的")
        lines.append("")

    lines.append("📉 市场结论：")
    lines.append(market_summary)
    lines.append("")
    lines.append("⚠️ 风险提示：")
    lines.append(risk_hint)

    return "\n".join(lines).strip()


def send_feishu_text(message: str) -> dict[str, Any]:
    """Send a plain text message to a Feishu webhook."""
    webhook_url = FEISHU_WEBHOOK.strip()
    if not webhook_url:
        LOGGER.info("FEISHU_WEBHOOK is not configured. Skip Feishu push.")
        return {
            "sent": False,
            "reason": "webhook_not_configured",
        }

    payload = {
        "msg_type": "text",
        "content": {
            "text": message,
        },
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        LOGGER.info("Feishu push sent successfully.")
        return {
            "sent": True,
            "reason": "",
        }
    except requests.RequestException as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Failed to send Feishu push: %s", exc)
        return {
            "sent": False,
            "reason": str(exc),
        }
