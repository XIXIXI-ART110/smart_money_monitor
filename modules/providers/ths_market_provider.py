from __future__ import annotations

from typing import Any

from config import LOGGER, THS_API_BASE, THS_PASSWORD, THS_TOKEN, THS_USERNAME


def _ensure_ths_configured() -> None:
    has_sdk_config = bool(THS_USERNAME and THS_PASSWORD)
    has_http_config = bool(THS_API_BASE and THS_TOKEN)
    if has_sdk_config or has_http_config:
        return
    raise RuntimeError("同花顺接口未配置")


def get_all_spot_data() -> dict[str, dict[str, Any]]:
    _ensure_ths_configured()

    # Future direction 1: Python SDK
    # TODO: 这里未来接 THS_iFinDLogin / THS_HQ 等 iFinDPy 能力。
    # 示例思路：
    # 1. 调用 THS_iFinDLogin(THS_USERNAME, THS_PASSWORD)
    # 2. 批量获取股票代码列表行情
    # 3. 映射成当前项目统一字段结构

    # Future direction 2: HTTP API
    # TODO: 这里未来接 HTTP API 请求，使用 THS_API_BASE + THS_TOKEN。
    # 需要你提供正式接口文档后，再把返回字段映射到统一结构。

    LOGGER.warning("THS market provider is selected but not implemented yet.")
    raise RuntimeError("同花顺接口未配置")


def get_stock_by_code(code: str) -> dict[str, Any]:
    _ensure_ths_configured()

    # TODO: 这里未来接单只股票的 THS SDK / HTTP API 查询。
    LOGGER.warning("THS single-stock provider is selected but not implemented yet: %s", code)
    raise RuntimeError("同花顺接口未配置")


def self_test(codes: list[str] | None = None) -> None:
    _ensure_ths_configured()
    print("THS 市场数据 provider 已选中，但当前仅为占位实现。")

