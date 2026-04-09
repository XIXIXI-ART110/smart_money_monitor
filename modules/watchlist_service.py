from __future__ import annotations

import json
from typing import Any

from config import LOGGER, WATCHLIST_PATH, ensure_runtime_directories
from modules.fetch_market import normalize_code


DEFAULT_WATCHLIST_PAYLOAD = {"stocks": []}


def _ensure_watchlist_file() -> None:
    """Ensure the watchlist file exists with a valid default structure."""
    ensure_runtime_directories()
    WATCHLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not WATCHLIST_PATH.exists():
        WATCHLIST_PATH.write_text(
            json.dumps(DEFAULT_WATCHLIST_PAYLOAD, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        LOGGER.info("Created default watchlist file at %s.", WATCHLIST_PATH)


def _normalize_stocks(raw_stocks: Any) -> list[dict[str, str]]:
    """Normalize raw stock items into a stable list of unique stock dicts."""
    if not isinstance(raw_stocks, list):
        return []

    normalized: list[dict[str, str]] = []
    seen_codes: set[str] = set()

    for item in raw_stocks:
        if not isinstance(item, dict):
            continue
        code = normalize_code(item.get("code", ""))
        name = str(item.get("name", "")).strip() or code
        if not code or code in seen_codes:
            continue
        normalized.append({"code": code, "name": name})
        seen_codes.add(code)

    return normalized


def load_watchlist() -> list[dict[str, str]]:
    """Load the current watchlist and auto-heal file issues when possible."""
    _ensure_watchlist_file()

    try:
        raw_content = WATCHLIST_PATH.read_text(encoding="utf-8").strip()
        if not raw_content:
            LOGGER.warning("Watchlist file is empty. Resetting to default structure.")
            save_watchlist([])
            return []

        payload = json.loads(raw_content)
        stocks = _normalize_stocks(payload.get("stocks", []))

        if stocks != payload.get("stocks", []):
            save_watchlist(stocks)

        return stocks
    except json.JSONDecodeError as exc:
        LOGGER.exception("Watchlist JSON is invalid. Resetting file: %s", exc)
        save_watchlist([])
        return []
    except OSError as exc:
        LOGGER.exception("Failed to read watchlist file: %s", exc)
        return []


def save_watchlist(stocks: list[dict[str, Any]]) -> None:
    """Persist the watchlist to JSON using a normalized format."""
    _ensure_watchlist_file()
    normalized_stocks = _normalize_stocks(stocks)
    payload = {"stocks": normalized_stocks}
    WATCHLIST_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    LOGGER.info("Saved %s stocks to watchlist.", len(normalized_stocks))


def add_stock(code: str, name: str) -> list[dict[str, str]]:
    """Add one stock to the watchlist and return the updated list."""
    normalized_code = normalize_code(code)
    normalized_name = str(name).strip() or normalized_code
    stocks = load_watchlist()

    if any(item["code"] == normalized_code for item in stocks):
        LOGGER.info("Stock %s already exists in watchlist.", normalized_code)
        return stocks

    stocks.append({"code": normalized_code, "name": normalized_name})
    save_watchlist(stocks)
    return load_watchlist()


def delete_stock(code: str) -> list[dict[str, str]]:
    """Delete one stock from the watchlist and return the updated list."""
    normalized_code = normalize_code(code)
    stocks = load_watchlist()
    filtered_stocks = [item for item in stocks if item["code"] != normalized_code]
    save_watchlist(filtered_stocks)
    return load_watchlist()
