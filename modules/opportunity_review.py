from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

import akshare as ak
import pandas as pd

from config import LOGGER, OPPORTUNITY_HISTORY_PATH, ensure_runtime_directories
from modules.fetch_market import normalize_code


DEFAULT_OPPORTUNITY_HISTORY_PAYLOAD = {"history": []}


def _safe_float(value: Any) -> float | None:
    """Convert arbitrary values into float when possible."""
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


def _ensure_history_file() -> None:
    """Ensure the opportunity history file exists."""
    ensure_runtime_directories()
    OPPORTUNITY_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not OPPORTUNITY_HISTORY_PATH.exists():
        OPPORTUNITY_HISTORY_PATH.write_text(
            json.dumps(DEFAULT_OPPORTUNITY_HISTORY_PAYLOAD, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        LOGGER.info("Created default opportunity history file at %s.", OPPORTUNITY_HISTORY_PATH)


def load_opportunity_history() -> list[dict[str, Any]]:
    """Load all opportunity history records."""
    _ensure_history_file()

    try:
        raw_content = OPPORTUNITY_HISTORY_PATH.read_text(encoding="utf-8").strip()
        if not raw_content:
            save_opportunity_history([])
            return []

        payload = json.loads(raw_content)
        history = payload.get("history", [])
        if not isinstance(history, list):
            save_opportunity_history([])
            return []
        return history
    except json.JSONDecodeError as exc:
        LOGGER.exception("Opportunity history JSON is invalid. Resetting file: %s", exc)
        save_opportunity_history([])
        return []
    except OSError as exc:
        LOGGER.exception("Failed to read opportunity history file: %s", exc)
        return []


def save_opportunity_history(history: list[dict[str, Any]]) -> None:
    """Persist full opportunity history payload."""
    _ensure_history_file()
    payload = {"history": history}
    OPPORTUNITY_HISTORY_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def save_daily_opportunity_record(
    *,
    market_conclusion: str,
    opportunity_rank: list[dict[str, Any]],
    record_date: str | None = None,
) -> list[dict[str, Any]]:
    """Save or overwrite one daily opportunity record."""
    history = load_opportunity_history()
    current_date = record_date or datetime.now().strftime("%Y-%m-%d")

    opportunities = [
        {
            "code": str(item.get("code", "")),
            "name": str(item.get("name", "")),
            "score": int(item.get("score", 0)),
            "level": str(item.get("level", "")),
            "reason": list(item.get("signals", [])) or list(item.get("risks", [])),
            "ai_advice": str(item.get("ai_advice", "")),
            "status": "pending",
        }
        for item in opportunity_rank
    ]

    daily_record = {
        "date": current_date,
        "market_conclusion": market_conclusion,
        "opportunities": opportunities,
    }

    replaced = False
    for index, record in enumerate(history):
        if str(record.get("date")) == current_date:
            history[index] = daily_record
            replaced = True
            break

    if not replaced:
        history.append(daily_record)

    history.sort(key=lambda item: str(item.get("date", "")))
    save_opportunity_history(history)
    return history


def _get_hist_dataframe(code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    """Fetch historical daily data for one stock."""
    try:
        dataframe = ak.stock_zh_a_hist(
            symbol=normalize_code(code),
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
        if isinstance(dataframe, pd.DataFrame) and not dataframe.empty:
            return dataframe
        return None
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Failed to fetch historical data for %s: %s", code, exc)
        return None


def _find_next_day_performance(code: str, record_date: str) -> dict[str, float] | None:
    """Calculate next-day close/high change from the recommendation date."""
    try:
        base_date = datetime.strptime(record_date, "%Y-%m-%d")
    except ValueError:
        return None

    start_date = base_date.strftime("%Y%m%d")
    end_date = (base_date + timedelta(days=15)).strftime("%Y%m%d")
    dataframe = _get_hist_dataframe(code, start_date, end_date)
    if dataframe is None or dataframe.empty or "日期" not in dataframe.columns:
        return None

    try:
        working_df = dataframe.copy()
        working_df["日期"] = pd.to_datetime(working_df["日期"])
        working_df = working_df.sort_values("日期").reset_index(drop=True)
        base_rows = working_df.loc[working_df["日期"] == pd.Timestamp(base_date)]
        next_rows = working_df.loc[working_df["日期"] > pd.Timestamp(base_date)]
        if base_rows.empty or next_rows.empty:
            return None

        base_row = base_rows.iloc[-1]
        next_row = next_rows.iloc[0]

        base_close = _safe_float(base_row.get("收盘"))
        next_close = _safe_float(next_row.get("收盘"))
        next_high = _safe_float(next_row.get("最高"))
        if base_close in {None, 0} or next_close is None or next_high is None:
            return None

        next_day_close_change = round((next_close - base_close) / base_close * 100, 3)
        next_day_high_change = round((next_high - base_close) / base_close * 100, 3)
        return {
            "next_day_close_change": next_day_close_change,
            "next_day_high_change": next_day_high_change,
        }
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.warning("Failed to calculate next-day performance for %s: %s", code, exc)
        return None


def review_opportunities() -> dict[str, Any]:
    """Review pending opportunities and update next-day performance when available."""
    history = load_opportunity_history()
    updated_count = 0
    pending_count = 0

    for record in history:
        record_date = str(record.get("date", ""))
        opportunities = record.get("opportunities", [])
        if not isinstance(opportunities, list):
            continue

        for item in opportunities:
            if str(item.get("status", "")) != "pending":
                continue

            pending_count += 1
            performance = _find_next_day_performance(str(item.get("code", "")), record_date)
            if performance is None:
                continue

            next_day_close_change = performance["next_day_close_change"]
            next_day_high_change = performance["next_day_high_change"]
            review_status = "hit" if (next_day_close_change > 0 or next_day_high_change > 2) else "miss"

            item["next_day_close_change"] = next_day_close_change
            item["next_day_high_change"] = next_day_high_change
            item["review_status"] = review_status
            item["status"] = "reviewed"
            updated_count += 1

    save_opportunity_history(history)
    stats = calculate_hit_stats(history)
    return {
        "ok": True,
        "message": "review completed",
        "updated": updated_count,
        "pending": pending_count - updated_count,
        "stats": stats,
        "history": history,
    }


def calculate_hit_stats(history: list[dict[str, Any]]) -> dict[str, Any]:
    """Calculate overall and per-level opportunity hit statistics."""
    reviewed_items: list[dict[str, Any]] = []
    for record in history:
        for item in record.get("opportunities", []):
            if str(item.get("review_status", "")) in {"hit", "miss"}:
                reviewed_items.append(item)

    total = len(reviewed_items)
    hit = sum(1 for item in reviewed_items if item.get("review_status") == "hit")
    miss = sum(1 for item in reviewed_items if item.get("review_status") == "miss")

    strong_items = [item for item in reviewed_items if item.get("level") == "strong"]
    medium_items = [item for item in reviewed_items if item.get("level") == "medium"]

    strong_total = len(strong_items)
    strong_hit = sum(1 for item in strong_items if item.get("review_status") == "hit")
    medium_total = len(medium_items)
    medium_hit = sum(1 for item in medium_items if item.get("review_status") == "hit")

    close_changes = [
        _safe_float(item.get("next_day_close_change"))
        for item in reviewed_items
        if _safe_float(item.get("next_day_close_change")) is not None
    ]
    high_changes = [
        _safe_float(item.get("next_day_high_change"))
        for item in reviewed_items
        if _safe_float(item.get("next_day_high_change")) is not None
    ]

    return {
        "total": total,
        "hit": hit,
        "miss": miss,
        "hit_rate": round(hit / total, 4) if total else 0.0,
        "strong_total": strong_total,
        "strong_hit": strong_hit,
        "strong_hit_rate": round(strong_hit / strong_total, 4) if strong_total else 0.0,
        "medium_total": medium_total,
        "medium_hit": medium_hit,
        "medium_hit_rate": round(medium_hit / medium_total, 4) if medium_total else 0.0,
        "avg_next_day_close_change": round(sum(close_changes) / len(close_changes), 3) if close_changes else 0.0,
        "avg_next_day_high_change": round(sum(high_changes) / len(high_changes), 3) if high_changes else 0.0,
    }
