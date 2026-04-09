from __future__ import annotations

import json
from typing import Any

from config import ETF_WATCHLIST_PATH, LOGGER, ensure_runtime_directories
from modules.fetch_market import normalize_code


DEFAULT_ETF_WATCHLIST_PAYLOAD = {"etfs": []}


def _ensure_etf_watchlist_file() -> None:
    """Ensure the ETF watchlist file exists with a valid default structure."""
    ensure_runtime_directories()
    ETF_WATCHLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not ETF_WATCHLIST_PATH.exists():
        ETF_WATCHLIST_PATH.write_text(
            json.dumps(DEFAULT_ETF_WATCHLIST_PAYLOAD, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        LOGGER.info("Created default ETF watchlist file at %s.", ETF_WATCHLIST_PATH)


def _normalize_etfs(raw_etfs: Any) -> list[dict[str, str]]:
    """Normalize raw ETF items into a stable unique list."""
    if not isinstance(raw_etfs, list):
        return []

    normalized: list[dict[str, str]] = []
    seen_codes: set[str] = set()

    for item in raw_etfs:
        if not isinstance(item, dict):
            continue
        code = normalize_code(item.get("code", ""))
        name = str(item.get("name", "")).strip() or code
        if not code or code in seen_codes:
            continue
        normalized.append({"code": code, "name": name})
        seen_codes.add(code)

    return normalized


def load_etf_watchlist() -> list[dict[str, str]]:
    """Load the current ETF watchlist and auto-heal file issues when possible."""
    _ensure_etf_watchlist_file()

    try:
        raw_content = ETF_WATCHLIST_PATH.read_text(encoding="utf-8").strip()
        if not raw_content:
            LOGGER.warning("ETF watchlist file is empty. Resetting to default structure.")
            save_etf_watchlist([])
            return []

        payload = json.loads(raw_content)
        etfs = _normalize_etfs(payload.get("etfs", []))

        if etfs != payload.get("etfs", []):
            save_etf_watchlist(etfs)

        return etfs
    except json.JSONDecodeError as exc:
        LOGGER.exception("ETF watchlist JSON is invalid. Resetting file: %s", exc)
        save_etf_watchlist([])
        return []
    except OSError as exc:
        LOGGER.exception("Failed to read ETF watchlist file: %s", exc)
        return []


def save_etf_watchlist(etfs: list[dict[str, Any]]) -> None:
    """Persist the ETF watchlist to JSON using a normalized format."""
    _ensure_etf_watchlist_file()
    normalized_etfs = _normalize_etfs(etfs)
    payload = {"etfs": normalized_etfs}
    ETF_WATCHLIST_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    LOGGER.info("Saved %s ETFs to ETF watchlist.", len(normalized_etfs))


def add_etf(code: str, name: str) -> list[dict[str, str]]:
    """Add one ETF to the watchlist and return the updated list."""
    normalized_code = normalize_code(code)
    normalized_name = str(name).strip() or normalized_code
    etfs = load_etf_watchlist()

    if any(item["code"] == normalized_code for item in etfs):
        LOGGER.info("ETF %s already exists in watchlist.", normalized_code)
        return etfs

    etfs.append({"code": normalized_code, "name": normalized_name})
    save_etf_watchlist(etfs)
    return load_etf_watchlist()


def delete_etf(code: str) -> list[dict[str, str]]:
    """Delete one ETF from the watchlist and return the updated list."""
    normalized_code = normalize_code(code)
    etfs = load_etf_watchlist()
    filtered_etfs = [item for item in etfs if item["code"] != normalized_code]
    save_etf_watchlist(filtered_etfs)
    return load_etf_watchlist()
