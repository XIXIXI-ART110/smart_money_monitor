from __future__ import annotations

import queue
import re
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Callable

import pandas as pd

from config import LOGGER, TUSHARE_TOKEN


_STOCK_NAME_CACHE: dict[str, str] | None = None


def normalize_code(code: Any) -> str:
    """Normalize a stock code into a six-digit string."""
    raw_code = str(code).strip()
    if not raw_code:
        return ""

    digit_matches = re.findall(r"\d+", raw_code)
    if digit_matches:
        digits = "".join(digit_matches)
        if len(digits) >= 6:
            return digits[-6:]
        return digits.zfill(6)

    return raw_code.zfill(6)


def to_ts_code(code: Any) -> str:
    """Convert a six-digit A-share code into Tushare ts_code format."""
    normalized_code = normalize_code(code)
    if not normalized_code:
        return ""
    if normalized_code.startswith(("4", "8")):
        return f"{normalized_code}.BJ"
    if normalized_code.startswith(("6", "9")):
        return f"{normalized_code}.SH"
    return f"{normalized_code}.SZ"


def to_xq_symbol(code: Any) -> str:
    """Convert a six-digit A-share code into Xueqiu symbol format used by AKShare."""
    normalized_code = normalize_code(code)
    if not normalized_code:
        return ""
    if normalized_code.startswith(("4", "8")):
        return f"BJ{normalized_code}"
    if normalized_code.startswith(("6", "9")):
        return f"SH{normalized_code}"
    return f"SZ{normalized_code}"


def to_market_prefixed_symbol(code: Any) -> str:
    """Convert a six-digit stock code into sh/sz/bj prefixed symbol."""
    normalized_code = normalize_code(code)
    if not normalized_code:
        return ""
    if normalized_code.startswith(("4", "8")):
        return f"bj{normalized_code}"
    if normalized_code.startswith(("6", "9")):
        return f"sh{normalized_code}"
    return f"sz{normalized_code}"


def _safe_float(value: Any) -> float | None:
    """Convert runtime values into float when possible."""
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("%", "").strip()
        if cleaned in {"", "-", "None", "nan", "NaN"}:
            return None
        value = cleaned
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_a_share_trading_time(now: datetime | None = None) -> bool:
    """Check whether the current time is within A-share trading sessions."""
    current = now or datetime.now()
    if current.weekday() >= 5:
        return False

    current_minutes = current.hour * 60 + current.minute
    morning_open = 9 * 60 + 30
    morning_close = 11 * 60 + 30
    afternoon_open = 13 * 60
    afternoon_close = 15 * 60
    return (morning_open <= current_minutes < morning_close) or (
        afternoon_open <= current_minutes < afternoon_close
    )


def _get_tushare_module() -> Any | None:
    """Import Tushare lazily so the app can still start with a clear warning."""
    try:
        import tushare as ts  # type: ignore

        return ts
    except ImportError:
        LOGGER.warning("Tushare is not installed. Please install requirements.txt.")
        return None


def _get_akshare_module() -> Any | None:
    """Import AKShare lazily so startup does not depend on provider availability."""
    try:
        import akshare as ak  # type: ignore

        return ak
    except ImportError:
        LOGGER.warning("AKShare is not installed. Please install requirements.txt.")
        return None


def _get_tushare_pro() -> Any | None:
    """Create a Tushare pro client from the configured token."""
    if not TUSHARE_TOKEN:
        LOGGER.warning("TUSHARE_TOKEN is not configured. Skip Tushare stock data fetch.")
        return None

    ts = _get_tushare_module()
    if ts is None:
        return None

    try:
        ts.set_token(TUSHARE_TOKEN)
        return ts.pro_api(TUSHARE_TOKEN)
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Failed to initialize Tushare pro client: %s", exc)
        return None


def _call_with_timeout(
    func: Callable[[], Any],
    *,
    timeout_seconds: float,
    description: str,
) -> Any:
    """Run one provider call with a hard API-facing timeout."""
    result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)

    def runner() -> None:
        try:
            result_queue.put((True, func()))
        except Exception as exc:  # pragma: no cover - runtime safety
            result_queue.put((False, exc))

    thread = threading.Thread(target=runner, name=f"market-provider-{description}", daemon=True)
    thread.start()

    try:
        ok, result = result_queue.get(timeout=timeout_seconds)
    except queue.Empty:
        raise TimeoutError(f"{description} timed out after {timeout_seconds:.1f}s")

    if not ok:
        raise result
    return result


def _call_tushare_with_retry(
    func: Callable[[], Any],
    *,
    description: str,
    retries: int = 2,
    timeout_seconds: float = 4.0,
) -> Any:
    """Call Tushare with lightweight retry and timeout protection."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        started = time.perf_counter()
        try:
            result = _call_with_timeout(
                func,
                timeout_seconds=timeout_seconds,
                description=description,
            )
            LOGGER.info(
                "Tushare %s finished in %.3fs on attempt %s.",
                description,
                time.perf_counter() - started,
                attempt,
            )
            return result
        except Exception as exc:  # pragma: no cover - runtime safety
            last_error = exc
            LOGGER.warning("Tushare %s failed on attempt %s: %s", description, attempt, exc)
            if attempt < retries:
                time.sleep(0.35 * attempt)

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Tushare {description} failed")


def _call_akshare_with_retry(
    func: Callable[[], Any],
    *,
    description: str,
    retries: int = 1,
    timeout_seconds: float = 2.0,
) -> Any:
    """Call AKShare with lightweight retry and timeout protection."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        started = time.perf_counter()
        try:
            result = _call_with_timeout(
                func,
                timeout_seconds=timeout_seconds,
                description=description,
            )
            LOGGER.info(
                "AKShare %s finished in %.3fs on attempt %s.",
                description,
                time.perf_counter() - started,
                attempt,
            )
            return result
        except Exception as exc:  # pragma: no cover - runtime safety
            last_error = exc
            LOGGER.warning("AKShare %s failed on attempt %s: %s", description, attempt, exc)
            if attempt < retries:
                time.sleep(0.25 * attempt)

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"AKShare {description} failed")


def _first_value(row: dict[str, Any], *keys: str) -> Any:
    """Return the first non-empty value from a row with flexible key casing."""
    lowered = {str(key).strip().lower(): value for key, value in row.items()}
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
        lowered_key = key.lower()
        if lowered_key in lowered and lowered[lowered_key] not in (None, ""):
            return lowered[lowered_key]
    return None


def _item_value_frame_to_dict(dataframe: pd.DataFrame) -> dict[str, Any]:
    """Convert AKShare item/value frames into a small lookup dict."""
    if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
        return {}
    if not {"item", "value"}.issubset(set(dataframe.columns)):
        return {}

    values: dict[str, Any] = {}
    for _, row in dataframe.iterrows():
        item = str(row.get("item", "")).strip()
        if item:
            values[item] = row.get("value")
    return values


def _normalize_akshare_item_values(
    row: dict[str, Any],
    code: str,
    name: str | None,
    data_source: str,
) -> dict[str, Any]:
    """Normalize AKShare item/value quote data into the project market-data shape."""
    normalized_code = normalize_code(code)
    latest_price = _safe_float(_first_value(row, "现价", "最新", "最新价", "price"))
    pct_change = _safe_float(_first_value(row, "涨幅", "涨跌幅", "pct_change", "pct_chg"))
    turnover = _safe_float(_first_value(row, "成交额", "金额", "amount"))

    if latest_price is None:
        return {}

    return {
        "code": normalized_code,
        "name": str(_first_value(row, "名称", "股票简称", "name") or name or normalized_code),
        "latest_price": latest_price,
        "pct_change": pct_change,
        "turnover": turnover,
        "main_net_inflow": None,
        "volume": _safe_float(_first_value(row, "成交量", "总手", "volume", "vol")),
        "open": _safe_float(_first_value(row, "今开", "开盘", "open")),
        "high": _safe_float(_first_value(row, "最高", "high")),
        "low": _safe_float(_first_value(row, "最低", "low")),
        "close_prev": _safe_float(_first_value(row, "昨收", "pre_close")),
        "turnover_rate": _safe_float(_first_value(row, "周转率", "换手", "换手率")),
        "amplitude": _safe_float(_first_value(row, "振幅", "amplitude")),
        "timestamp": str(_first_value(row, "时间", "TIME", "time") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        "data_source": data_source,
    }


def _get_stock_name_map(pro: Any) -> dict[str, str]:
    """Load stock code -> name mapping from Tushare stock_basic."""
    global _STOCK_NAME_CACHE
    if _STOCK_NAME_CACHE is not None:
        return _STOCK_NAME_CACHE

    try:
        dataframe = _call_tushare_with_retry(
            lambda: pro.stock_basic(
                exchange="",
                list_status="L",
                fields="ts_code,symbol,name",
            ),
            description="stock_basic",
            retries=2,
            timeout_seconds=5.0,
        )
        if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
            _STOCK_NAME_CACHE = {}
            return _STOCK_NAME_CACHE

        _STOCK_NAME_CACHE = {
            normalize_code(row.get("symbol")): str(row.get("name") or normalize_code(row.get("symbol")))
            for _, row in dataframe.iterrows()
            if normalize_code(row.get("symbol"))
        }
        return _STOCK_NAME_CACHE
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Tushare stock_basic failed: %s", exc)
        _STOCK_NAME_CACHE = {}
        return _STOCK_NAME_CACHE


def _resolve_stock_name(pro: Any, code: str, fallback_name: str | None) -> str:
    """Resolve stock display name without making a single failed name lookup fatal."""
    if fallback_name:
        return str(fallback_name)
    return _get_stock_name_map(pro).get(normalize_code(code), normalize_code(code))


def _normalize_realtime_row(row: dict[str, Any], code: str, name: str | None) -> dict[str, Any]:
    """Normalize a Tushare realtime quote row into the project market-data shape."""
    normalized_code = normalize_code(code)
    latest_price = _safe_float(_first_value(row, "PRICE", "price", "现价", "最新价"))
    pct_change = _safe_float(_first_value(row, "PCT_CHANGE", "pct_chg", "pct_change", "涨跌幅"))
    turnover = _safe_float(_first_value(row, "AMOUNT", "amount", "成交额"))

    if latest_price is None:
        return {}

    return {
        "code": normalized_code,
        "name": str(_first_value(row, "NAME", "name", "名称") or name or normalized_code),
        "latest_price": latest_price,
        "pct_change": pct_change,
        "turnover": turnover,
        "main_net_inflow": None,
        "volume": _safe_float(_first_value(row, "VOLUME", "vol", "volume", "成交量")),
        "open": _safe_float(_first_value(row, "OPEN", "open", "开盘")),
        "high": _safe_float(_first_value(row, "HIGH", "high", "最高")),
        "low": _safe_float(_first_value(row, "LOW", "low", "最低")),
        "close_prev": _safe_float(_first_value(row, "PRE_CLOSE", "pre_close", "昨收")),
        "turnover_rate": None,
        "amplitude": None,
        "timestamp": str(_first_value(row, "TIME", "time", "DATE", "date") or ""),
        "data_source": "tushare_realtime",
    }


def fetch_realtime_data(code: str, name: str | None = None) -> dict[str, Any]:
    """Try to fetch one stock via Tushare realtime quote."""
    ts = _get_tushare_module()
    if ts is None or not TUSHARE_TOKEN:
        return {}
    if not hasattr(ts, "realtime_quote"):
        LOGGER.info("Current Tushare package has no realtime_quote API. Use daily fallback.")
        return {}

    ts_code = to_ts_code(code)
    try:
        dataframe = _call_tushare_with_retry(
            lambda: ts.realtime_quote(ts_code=ts_code),
            description=f"realtime_quote {ts_code}",
            retries=2,
            timeout_seconds=3.0,
        )
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Tushare realtime quote failed for %s: %s", ts_code, exc)
        return {}

    if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
        return {}

    normalized = _normalize_realtime_row(dataframe.iloc[0].to_dict(), code, name)
    if normalized:
        return normalized
    return {}


def fetch_akshare_display_data(code: str, name: str | None = None) -> dict[str, Any]:
    """Fetch display quote fields from AKShare as the preferred quote source."""
    ak = _get_akshare_module()
    if ak is None:
        return {}

    normalized_code = normalize_code(code)
    quote_attempts: list[tuple[str, Callable[[], Any], str, float]] = [
        (
            "akshare_xq",
            lambda: ak.stock_individual_spot_xq(
                symbol=to_xq_symbol(normalized_code),
                timeout=1.8,
            ),
            f"stock_individual_spot_xq {to_xq_symbol(normalized_code)}",
            2.0,
        ),
        (
            "akshare_bid_ask",
            lambda: ak.stock_bid_ask_em(symbol=normalized_code),
            f"stock_bid_ask_em {normalized_code}",
            1.2,
        ),
    ]

    for data_source, fetcher, description, timeout_seconds in quote_attempts:
        try:
            dataframe = _call_akshare_with_retry(
                fetcher,
                description=description,
                retries=1,
                timeout_seconds=timeout_seconds,
            )
            normalized = _normalize_akshare_item_values(
                _item_value_frame_to_dict(dataframe),
                normalized_code,
                name,
                data_source,
            )
            if normalized:
                return normalized
        except TimeoutError as exc:  # pragma: no cover - runtime safety
            LOGGER.warning("AKShare %s display quote timed out for %s: %s", data_source, normalized_code, exc)
            if data_source == "akshare_xq":
                break
        except Exception as exc:  # pragma: no cover - runtime safety
            LOGGER.warning("AKShare %s display quote failed for %s: %s", data_source, normalized_code, exc)

    return {}


def fetch_akshare_hist_data(code: str, name: str | None = None) -> dict[str, Any]:
    """Fetch the latest valid daily close from AKShare daily sources."""
    ak = _get_akshare_module()
    if ak is None:
        return {}

    normalized_code = normalize_code(code)
    prefixed_symbol = to_market_prefixed_symbol(normalized_code)
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=220)).strftime("%Y%m%d")
    quote_attempts: list[tuple[str, Callable[[], Any], str, float]] = [
        (
            "akshare_sina_daily",
            lambda: ak.stock_zh_a_daily(
                symbol=prefixed_symbol,
                start_date=start_date,
                end_date=end_date,
                adjust="",
            ),
            f"stock_zh_a_daily {prefixed_symbol}",
            4.0,
        ),
        (
            "akshare_tx_hist",
            lambda: ak.stock_zh_a_hist_tx(
                symbol=prefixed_symbol,
                start_date=start_date,
                end_date=end_date,
                adjust="",
                timeout=3.0,
            ),
            f"stock_zh_a_hist_tx {prefixed_symbol}",
            4.0,
        ),
        (
            "akshare_em_hist",
            lambda: ak.stock_zh_a_hist(
                symbol=normalized_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="",
                timeout=2.0,
            ),
            f"stock_zh_a_hist {normalized_code}",
            2.6,
        ),
    ]

    for data_source, fetcher, description, timeout_seconds in quote_attempts:
        try:
            LOGGER.info(
                "Requesting recent trading-day data for %s via %s. start_date=%s end_date=%s",
                normalized_code,
                data_source,
                start_date,
                end_date,
            )
            dataframe = _call_akshare_with_retry(
                fetcher,
                description=description,
                retries=1,
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:  # pragma: no cover - runtime safety
            LOGGER.warning("AKShare recent daily fallback failed for %s via %s: %s", normalized_code, data_source, exc)
            continue

        if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
            LOGGER.warning("AKShare recent daily data is empty for %s via %s.", normalized_code, data_source)
            continue

        try:
            dataframe = dataframe.copy()
            column_map = {
                "date": "trade_date",
                "日期": "trade_date",
                "open": "open",
                "开盘": "open",
                "close": "close",
                "收盘": "close",
                "high": "high",
                "最高": "high",
                "low": "low",
                "最低": "low",
                "volume": "volume",
                "成交量": "volume",
                "amount": "amount",
                "成交额": "amount",
                "turnover": "turnover_rate",
                "换手率": "turnover_rate",
                "涨跌幅": "pct_change",
                "振幅": "amplitude",
            }
            dataframe = dataframe.rename(columns={key: value for key, value in column_map.items() if key in dataframe.columns})

            if "trade_date" not in dataframe.columns or "close" not in dataframe.columns:
                LOGGER.warning(
                    "AKShare recent daily data missing required columns for %s via %s. columns=%s",
                    normalized_code,
                    data_source,
                    list(dataframe.columns),
                )
                continue

            dataframe["trade_date"] = pd.to_datetime(dataframe["trade_date"], errors="coerce")
            dataframe = dataframe.dropna(subset=["trade_date"]).sort_values("trade_date")
            for column in ("open", "close", "high", "low", "volume", "amount", "turnover_rate", "pct_change", "amplitude"):
                if column in dataframe.columns:
                    dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")

            valid_dataframe = dataframe[dataframe["close"].notna()].reset_index(drop=True)
            if valid_dataframe.empty:
                LOGGER.warning("AKShare recent daily data has no valid close rows for %s via %s.", normalized_code, data_source)
                continue

            latest_row = valid_dataframe.iloc[-1].to_dict()
            previous_close = None
            if len(valid_dataframe) >= 2:
                previous_close = _safe_float(valid_dataframe.iloc[-2].to_dict().get("close"))

            latest_price = _safe_float(latest_row.get("close"))
            if latest_price is None:
                LOGGER.warning("AKShare recent daily latest close is empty for %s via %s.", normalized_code, data_source)
                continue

            pct_change = _safe_float(latest_row.get("pct_change"))
            if pct_change is None and previous_close not in (None, 0):
                pct_change = (latest_price - previous_close) / previous_close * 100

            turnover_rate = _safe_float(latest_row.get("turnover_rate"))
            if data_source == "akshare_sina_daily" and turnover_rate is not None and turnover_rate <= 1:
                turnover_rate *= 100

            close_series = valid_dataframe["close"]
            volume_series = valid_dataframe["volume"] if "volume" in valid_dataframe.columns else pd.Series(dtype=float)
            ma5 = _safe_float(close_series.tail(5).mean()) if len(close_series) >= 5 else None
            ma10 = _safe_float(close_series.tail(10).mean()) if len(close_series) >= 10 else None
            ma20 = _safe_float(close_series.tail(20).mean()) if len(close_series) >= 20 else None
            ma60 = _safe_float(close_series.tail(60).mean()) if len(close_series) >= 60 else None
            high_60d = _safe_float(valid_dataframe["high"].tail(60).max()) if "high" in valid_dataframe.columns and len(valid_dataframe) >= 60 else None
            low_60d = _safe_float(valid_dataframe["low"].tail(60).min()) if "low" in valid_dataframe.columns and len(valid_dataframe) >= 60 else None
            high_120d = _safe_float(valid_dataframe["high"].tail(120).max()) if "high" in valid_dataframe.columns and len(valid_dataframe) >= 120 else None
            low_120d = _safe_float(valid_dataframe["low"].tail(120).min()) if "low" in valid_dataframe.columns and len(valid_dataframe) >= 120 else None
            volume_ratio = None
            if not volume_series.empty and len(volume_series) >= 6:
                previous_avg_volume = _safe_float(volume_series.iloc[-6:-1].mean())
                current_volume = _safe_float(volume_series.iloc[-1])
                if previous_avg_volume not in (None, 0) and current_volume is not None:
                    volume_ratio = current_volume / previous_avg_volume

            normalized_result = {
                "code": normalized_code,
                "name": str(name or normalized_code),
                "latest_price": latest_price,
                "close": latest_price,
                "pct_change": pct_change,
                "turnover": _safe_float(latest_row.get("amount")),
                "main_net_inflow": None,
                "volume": _safe_float(latest_row.get("volume")),
                "open": _safe_float(latest_row.get("open")),
                "high": _safe_float(latest_row.get("high")),
                "low": _safe_float(latest_row.get("low")),
                "close_prev": previous_close,
                "turnover_rate": turnover_rate,
                "amplitude": _safe_float(latest_row.get("amplitude")),
                "ma5": ma5,
                "ma10": ma10,
                "ma20": ma20,
                "ma60": ma60,
                "high_60d": high_60d,
                "low_60d": low_60d,
                "high_120d": high_120d,
                "low_120d": low_120d,
                "volume_ratio": volume_ratio,
                "timestamp": str(latest_row.get("trade_date") or ""),
                "data_source": data_source,
                "missing_fields": [
                    field for field in ("latest_price", "pct_change", "turnover")
                    if _safe_float({"latest_price": latest_price, "pct_change": pct_change, "turnover": latest_row.get("amount")}.get(field)) is None
                ],
            }
            LOGGER.info(
                "Recent trading-day data mapped for %s via %s. columns=%s latest_price=%s pct_change=%s turnover=%s timestamp=%s missing_fields=%s",
                normalized_code,
                data_source,
                list(dataframe.columns),
                normalized_result.get("latest_price"),
                normalized_result.get("pct_change"),
                normalized_result.get("turnover"),
                normalized_result.get("timestamp"),
                normalized_result.get("missing_fields"),
            )
            return normalized_result
        except Exception as exc:  # pragma: no cover - runtime safety
            LOGGER.warning("Failed to normalize AKShare recent daily data for %s via %s: %s", normalized_code, data_source, exc)

    return {}


def fetch_latest_daily_data(code: str, name: str | None = None) -> dict[str, Any]:
    """Fetch the latest available daily close via Tushare as the stable source."""
    pro = _get_tushare_pro()
    if pro is None:
        return {}

    normalized_code = normalize_code(code)
    ts_code = to_ts_code(normalized_code)
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

    try:
        dataframe = _call_tushare_with_retry(
            lambda: pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date),
            description=f"daily {ts_code}",
            retries=2,
            timeout_seconds=4.0,
        )
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Tushare daily fallback failed for %s: %s", ts_code, exc)
        return {}

    if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
        return {}

    try:
        dataframe = dataframe.copy()
        if "trade_date" in dataframe.columns:
            dataframe = dataframe.sort_values("trade_date")
        latest_row = dataframe.iloc[-1].to_dict()
        amount = _safe_float(latest_row.get("amount"))
        turnover = amount * 1000 if amount is not None else None
        close = _safe_float(latest_row.get("close"))
        if close is None:
            return {}

        return {
            "code": normalized_code,
            "name": _resolve_stock_name(pro, normalized_code, name),
            "latest_price": close,
            "close": close,
            "pct_change": _safe_float(latest_row.get("pct_chg")),
            "turnover": turnover,
            "main_net_inflow": None,
            "volume": _safe_float(latest_row.get("vol")),
            "open": _safe_float(latest_row.get("open")),
            "high": _safe_float(latest_row.get("high")),
            "low": _safe_float(latest_row.get("low")),
            "close_prev": _safe_float(latest_row.get("pre_close")),
            "turnover_rate": None,
            "amplitude": None,
            "timestamp": str(latest_row.get("trade_date") or ""),
            "data_source": "tushare_daily",
        }
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Failed to normalize Tushare daily data for %s: %s", ts_code, exc)
        return {}


def get_all_spot_data() -> dict[str, dict[str, Any]]:
    """Return a small Tushare snapshot for configured watchlist items."""
    try:
        from modules.watchlist_service import load_watchlist

        watchlist = load_watchlist()
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Failed to load watchlist for Tushare snapshot: %s", exc)
        return {}

    results: dict[str, dict[str, Any]] = {}
    for stock in watchlist:
        code = normalize_code(stock.get("code", ""))
        if not code:
            continue
        result = fetch_stock_data(code, stock.get("name"))
        if result:
            results[code] = result
    return results


def _merge_provider_results(
    akshare_result: dict[str, Any],
    tushare_result: dict[str, Any],
    code: str,
    name: str | None,
) -> dict[str, Any]:
    """Use Tushare as structure fallback while keeping AKShare quote fields first."""
    normalized_code = normalize_code(code)
    if tushare_result:
        merged = dict(tushare_result)
    else:
        merged = {
            "code": normalized_code,
            "name": name or normalized_code,
            "latest_price": None,
            "pct_change": None,
            "turnover": None,
            "main_net_inflow": None,
        }

    if akshare_result:
        merged["code"] = normalized_code
        if not merged.get("name") or merged.get("name") == normalized_code:
            merged["name"] = akshare_result.get("name") or name or normalized_code

        # Display fields prefer AKShare; keep Tushare values only when AKShare is missing.
        for field in ("latest_price", "pct_change", "turnover"):
            if akshare_result.get(field) is not None:
                merged[field] = akshare_result[field]

        for field in (
            "volume",
            "open",
            "high",
            "low",
            "close_prev",
            "turnover_rate",
            "amplitude",
            "timestamp",
        ):
            if akshare_result.get(field) is not None:
                merged[field] = akshare_result[field]

        primary_source = str(akshare_result.get("data_source") or "akshare")
        fallback_source = str(tushare_result.get("data_source") or "fallback") if tushare_result else ""
        merged["data_source"] = f"{primary_source}+{fallback_source}" if fallback_source else primary_source
    elif tushare_result:
        merged["data_source"] = tushare_result.get("data_source", "tushare_daily")

    has_market_field = any(
        merged.get(field) is not None for field in ("latest_price", "pct_change", "turnover")
    )
    return merged if has_market_field else {}


def fetch_stock_data(code: str, name: str | None = None) -> dict[str, Any]:
    """Fetch one stock with AKShare quote first and Tushare daily as fallback."""
    normalized_code = normalize_code(code)
    is_non_trading_session = not _is_a_share_trading_time()
    if is_non_trading_session:
        LOGGER.info("Skip realtime quote for %s because current session is non-trading.", normalized_code)
        akshare_result = {}
    else:
        akshare_result = fetch_akshare_display_data(normalized_code, name)
    hist_result = fetch_akshare_hist_data(normalized_code, name)
    daily_result = fetch_latest_daily_data(normalized_code, name)
    fallback_result = hist_result or daily_result

    if is_non_trading_session and fallback_result:
        non_trading_result = dict(fallback_result)
        non_trading_result["code"] = normalized_code
        non_trading_result["name"] = non_trading_result.get("name") or name or normalized_code
        non_trading_result["is_non_trading_session"] = True
        non_trading_result["used_previous_trading_day"] = True
        non_trading_result["is_data_incomplete"] = any(
            non_trading_result.get(field) is None for field in ("latest_price", "pct_change", "turnover")
        )
        non_trading_result["data_notice"] = (
            "当前为非交易时段，展示最近交易日数据"
            if not non_trading_result["is_data_incomplete"]
            else "当前为非交易时段，但最近交易日数据不完整"
        )
        LOGGER.info(
            "Final non-trading payload for %s: data_source=%s latest_price=%s pct_change=%s turnover=%s missing_fields=%s",
            normalized_code,
            non_trading_result.get("data_source"),
            non_trading_result.get("latest_price"),
            non_trading_result.get("pct_change"),
            non_trading_result.get("turnover"),
            non_trading_result.get("missing_fields", []),
        )
        return non_trading_result

    merged_result = _merge_provider_results(akshare_result, fallback_result, normalized_code, name)
    if merged_result:
        used_previous_trading_day = (
            merged_result.get("latest_price") == (fallback_result or {}).get("latest_price")
            and (fallback_result or {}).get("latest_price") is not None
            and akshare_result.get("latest_price") is None
        )
        merged_result["is_non_trading_session"] = is_non_trading_session
        merged_result["used_previous_trading_day"] = bool(used_previous_trading_day)
        merged_result["is_data_incomplete"] = any(
            merged_result.get(field) is None for field in ("latest_price", "pct_change", "turnover")
        )
        if used_previous_trading_day:
            merged_result["data_notice"] = "实时行情不可用，已回退到最近交易日数据"
        elif merged_result["is_data_incomplete"]:
            merged_result["data_notice"] = "数据不完整"
        else:
            merged_result["data_notice"] = ""
        merged_result["missing_fields"] = [
            field for field in ("latest_price", "pct_change", "turnover") if merged_result.get(field) is None
        ]
        LOGGER.info(
            "Final market payload for %s: data_source=%s latest_price=%s pct_change=%s turnover=%s missing_fields=%s",
            normalized_code,
            merged_result.get("data_source"),
            merged_result.get("latest_price"),
            merged_result.get("pct_change"),
            merged_result.get("turnover"),
            merged_result.get("missing_fields"),
        )
        return merged_result

    LOGGER.warning("AKShare and Tushare stock sources returned no usable data for %s.", normalized_code)
    return {}
