from __future__ import annotations

from typing import Any

from config import LOGGER, THS_API_BASE, THS_PASSWORD, THS_TOKEN, THS_USERNAME


def _ensure_ths_configured() -> None:
    has_sdk_config = bool(THS_USERNAME and THS_PASSWORD)
    has_http_config = bool(THS_API_BASE and THS_TOKEN)
    if has_sdk_config or has_http_config:
        return
    raise RuntimeError("同花顺接口未配置")


def get_all_etf_spot_data() -> dict[str, dict[str, Any]]:
    _ensure_ths_configured()

    # Future direction 1: Python SDK
    # TODO: 这里未来接 THS_iFinDLogin，并通过 iFinDPy 查询 ETF 行情。

    # Future direction 2: HTTP API
    # TODO: 这里未来接 HTTP API 请求，使用 THS_API_BASE + THS_TOKEN。

    LOGGER.warning("THS ETF provider is selected but not implemented yet.")
    raise RuntimeError("同花顺接口未配置")


def get_etf_by_code(code: str) -> dict[str, Any]:
    _ensure_ths_configured()
    LOGGER.warning("THS single-ETF provider is selected but not implemented yet: %s", code)
    raise RuntimeError("同花顺接口未配置")


def self_test(codes: list[str] | None = None) -> None:
    _ensure_ths_configured()
    print("THS ETF provider 已选中，但当前仅为占位实现。")
