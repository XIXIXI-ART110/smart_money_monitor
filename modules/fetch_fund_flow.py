from __future__ import annotations

from typing import Any, Mapping

import akshare as ak
import pandas as pd

from config import LOGGER
from modules.fetch_market import _first_existing, _safe_float, normalize_code


FUND_FLOW_API_CANDIDATES = (
    "stock_individual_fund_flow",
)

FUND_FLOW_FIELD_ALIASES: dict[str, list[str]] = {
    "date": ["日期", "交易日", "date"],
    "close_price": ["收盘价", "最新价", "close_price"],
    "pct_change": ["涨跌幅", "涨跌幅%", "pct_change"],
    "main_net_inflow": ["主力净流入-净额", "主力净流入", "主力净额", "main_net_inflow"],
    "main_net_inflow_ratio": ["主力净流入-净占比", "主力净流入占比", "main_net_inflow_ratio"],
    "super_large_net_inflow": ["超大单净流入-净额", "超大单净流入", "super_large_net_inflow"],
    "large_net_inflow": ["大单净流入-净额", "大单净流入", "large_net_inflow"],
    "medium_net_inflow": ["中单净流入-净额", "中单净流入", "medium_net_inflow"],
    "small_net_inflow": ["小单净流入-净额", "小单净流入", "small_net_inflow"],
}


def _infer_market(code: str) -> str | None:
    """Infer the exchange market prefix from a stock code."""
    if code.startswith(("600", "601", "603", "605", "688", "689", "900")):
        return "sh"
    if code.startswith(("000", "001", "002", "003", "300", "301", "200")):
        return "sz"
    return None


def _normalize_fund_flow_row(code: str, row: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize the latest fund flow row."""
    return {
        "code": code,
        "date": str(_first_existing(row, FUND_FLOW_FIELD_ALIASES["date"]) or ""),
        "close_price": _safe_float(_first_existing(row, FUND_FLOW_FIELD_ALIASES["close_price"])),
        "pct_change": _safe_float(_first_existing(row, FUND_FLOW_FIELD_ALIASES["pct_change"])),
        "main_net_inflow": _safe_float(_first_existing(row, FUND_FLOW_FIELD_ALIASES["main_net_inflow"])),
        "main_net_inflow_ratio": _safe_float(
            _first_existing(row, FUND_FLOW_FIELD_ALIASES["main_net_inflow_ratio"])
        ),
        "super_large_net_inflow": _safe_float(
            _first_existing(row, FUND_FLOW_FIELD_ALIASES["super_large_net_inflow"])
        ),
        "large_net_inflow": _safe_float(_first_existing(row, FUND_FLOW_FIELD_ALIASES["large_net_inflow"])),
        "medium_net_inflow": _safe_float(_first_existing(row, FUND_FLOW_FIELD_ALIASES["medium_net_inflow"])),
        "small_net_inflow": _safe_float(_first_existing(row, FUND_FLOW_FIELD_ALIASES["small_net_inflow"])),
    }


def _call_fund_flow_api(code: str, market: str) -> tuple[pd.DataFrame, str]:
    """Try multiple call signatures for akshare fund flow APIs."""
    last_error: Exception | None = None
    for func_name in FUND_FLOW_API_CANDIDATES:
        func = getattr(ak, func_name, None)
        if func is None:
            continue
        LOGGER.info("trying fund source: akshare.%s code=%s market=%s", func_name, code, market)

        call_variants: list[tuple[tuple[Any, ...], dict[str, Any]]] = [
            ((), {"stock": code, "market": market}),
            ((), {"stock": code}),
            ((), {"symbol": code, "market": market}),
            ((), {"symbol": code}),
            ((code, market), {}),
            ((code,), {}),
        ]

        for args, kwargs in call_variants:
            try:
                dataframe = func(*args, **kwargs)
                if isinstance(dataframe, pd.DataFrame):
                    LOGGER.info(
                        "Fetched fund flow via akshare.%s with args=%s kwargs=%s.",
                        func_name,
                        args,
                        kwargs,
                    )
                    LOGGER.info("fund source success: akshare.%s code=%s", func_name, code)
                    return dataframe, f"akshare.{func_name}"
            except TypeError as exc:
                last_error = exc
                continue
            except Exception as exc:  # pragma: no cover - runtime safety
                last_error = exc
                LOGGER.warning("fund source fail: akshare.%s code=%s error=%s", func_name, code, exc)
                LOGGER.warning(
                    "akshare.%s call failed for stock %s with args=%s kwargs=%s: %s",
                    func_name,
                    code,
                    args,
                    kwargs,
                    exc,
                )

    if last_error is not None:
        raise last_error
    raise AttributeError("No supported akshare fund flow API was found.")


def get_individual_fund_flow(code: str) -> dict[str, Any] | None:
    """Fetch the latest individual stock fund flow snapshot."""
    normalized_code = normalize_code(code)
    market = _infer_market(normalized_code)

    if market is None:
        LOGGER.warning("Unsupported stock code for fund flow lookup: %s", normalized_code)
        return None

    try:
        dataframe, data_source = _call_fund_flow_api(normalized_code, market)
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("fund source fail: final code=%s error=%s", normalized_code, exc)
        LOGGER.exception(
            "Failed to fetch fund flow for stock %s on market %s: %s",
            normalized_code,
            market,
            exc,
        )
        return None

    if dataframe is None or dataframe.empty:
        LOGGER.warning("fund source fail: final code=%s error=empty_dataframe", normalized_code)
        LOGGER.warning("Fund flow data is empty for stock %s.", normalized_code)
        return None

    try:
        latest_row = dataframe.iloc[-1].to_dict()
        normalized = _normalize_fund_flow_row(normalized_code, latest_row)
        normalized["data_source"] = data_source
        LOGGER.info("Fetched latest fund flow for stock %s.", normalized_code)
        return normalized
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Failed to normalize fund flow for stock %s: %s", normalized_code, exc)
        return None


def self_test() -> None:
    """Run a minimal local smoke test for fund flow fetching."""
    sample = get_individual_fund_flow("600519")
    if sample:
        print("fetch_fund_flow 自测成功：")
        print(sample)
        return
    print("fetch_fund_flow 自测未拿到数据，请检查网络、交易时段或 akshare 版本。")


if __name__ == "__main__":
    self_test()
