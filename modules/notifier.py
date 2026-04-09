from __future__ import annotations

from typing import Any

import requests

from config import LOGGER


def send_to_feishu(webhook_url: str, content: str) -> dict[str, Any] | None:
    """Send a plain-text message to a Feishu robot webhook."""
    if not webhook_url:
        LOGGER.info("Feishu webhook is not configured. Skip sending.")
        return None

    text_content = content
    if len(text_content) > 3500:
        text_content = f"{text_content[:3450]}\n\n[内容较长，已截断，请查看本地报告文件]"

    payload = {
        "msg_type": "text",
        "content": {
            "text": text_content,
        },
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        try:
            data: dict[str, Any] = response.json()
        except ValueError:
            data = {"status_code": response.status_code, "text": response.text}
        LOGGER.info("Feishu notification sent successfully.")
        return data
    except requests.RequestException as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Failed to send report to Feishu: %s", exc)
        return None


def self_test() -> None:
    """Run a local notifier smoke test with a dummy webhook."""
    result = send_to_feishu(
        webhook_url="https://example.invalid/feishu-webhook",
        content="这是一条本地自测消息，用于验证飞书推送失败时不会中断主流程。",
    )
    print("notifier 自测结果：")
    print(result)


if __name__ == "__main__":
    self_test()
