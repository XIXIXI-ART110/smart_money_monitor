from __future__ import annotations

from typing import Any

from config import DATA_PROVIDER, LOGGER
from modules.providers import free_etf_provider, ths_etf_provider


def _resolve_etf_provider():
    """Resolve the current ETF data provider from runtime config."""
    if DATA_PROVIDER == "ths":
        return ths_etf_provider
    return free_etf_provider


def get_all_etf_spot_data() -> dict[str, dict[str, Any]]:
    """Fetch ETF snapshot data from the configured provider."""
    provider = _resolve_etf_provider()
    try:
        return provider.get_all_etf_spot_data()
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("ETF provider %s failed: %s", DATA_PROVIDER, exc)
        return {}


def get_etf_by_code(code: str) -> dict[str, Any]:
    """Fetch one ETF snapshot from the configured provider."""
    provider = _resolve_etf_provider()
    try:
        return provider.get_etf_by_code(code)
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("ETF provider %s failed for code %s: %s", DATA_PROVIDER, code, exc)
        return {}


def self_test(codes: list[str] | None = None) -> None:
    """Run a smoke test against the configured ETF provider."""
    provider = _resolve_etf_provider()
    if hasattr(provider, "self_test"):
        provider.self_test(codes)
        return
    print(f"当前 ETF provider={DATA_PROVIDER} 暂未实现自测入口。")


if __name__ == "__main__":
    self_test()
