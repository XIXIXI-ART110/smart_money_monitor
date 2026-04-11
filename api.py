from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from config import ENV_PATH, ETF_WATCHLIST_PATH, LOGGER, WATCHLIST_PATH, get_public_runtime_config, update_env_config
from modules.etf_service import analyze_single_etf_service, get_default_etf_list_service, run_etf_once_service
from modules.etf_watchlist_service import add_etf, delete_etf, load_etf_watchlist
from modules.index_service import get_index_detail_service, get_index_options_service, get_indexes_service
from modules.opportunity_service import (
    SCOPE_NAMES,
    get_auto_recommendation,
    get_low_opportunity_pool,
    get_opportunities,
    get_opportunity_detail,
    get_stock_low_opportunity_pool,
)
from modules.opportunity_review import calculate_hit_stats, load_opportunity_history, review_opportunities
from modules.report_service import get_latest_report, get_report_by_filename, list_reports
from modules.run_service import run_once_service
from modules.stock_search_service import search_stocks
from modules.watchlist_service import add_stock, delete_stock, load_watchlist


class StockPayload(BaseModel):
    """Request body for adding one stock."""

    code: str = Field(..., min_length=1, max_length=20)
    name: str = Field(..., min_length=1, max_length=50)


class ConfigUpdatePayload(BaseModel):
    """Request body for updating local config."""

    openai_api_key: str | None = None
    openai_model: str | None = None
    feishu_webhook: str | None = None
    default_schedule_time: str | None = None


app = FastAPI(
    title="Smart Money Monitor API",
    description="A-share research monitor backend service.",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:8000",
        "http://localhost:8000",
        "null",
    ],
    allow_origin_regex=r"^https?://(127\.0\.0\.1|localhost)(:\d+)?$",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR / "frontend"
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
app.mount("/frontend", StaticFiles(directory=FRONTEND_DIR), name="frontend")



def _response_items(data: dict[str, Any]) -> list[Any]:
    if isinstance(data.get("items"), list):
        return list(data["items"])

    for key in ("results", "stocks", "etfs", "indexes", "options", "indices", "history", "reports", "etf_results"):
        value = data.get(key)
        if isinstance(value, list):
            return list(value)

    for key in ("item", "index", "etf", "stats", "report"):
        value = data.get(key)
        if value:
            return [value]
    return []


def _response_errors(message: str, data: dict[str, Any], ok: bool) -> list[Any]:
    if isinstance(data.get("errors"), list):
        return list(data["errors"])

    errors: list[Any] = []
    failed_symbols = data.get("failed_symbols")
    if isinstance(failed_symbols, list) and failed_symbols:
        errors.append(
            {
                "type": "failed_symbols",
                "message": f"失败股票: {', '.join(str(symbol) for symbol in failed_symbols)}",
                "symbols": failed_symbols,
            }
        )
    if not ok and message:
        errors.append(message)
    return errors


def _normalize_response_data(message: str, data: dict[str, Any] | None, ok: bool) -> dict[str, Any]:
    payload = dict(data or {})
    payload["items"] = _response_items(payload)
    payload["errors"] = _response_errors(message, payload, ok)
    return payload


def _log_api_request_start(route: str, **context: Any) -> None:
    LOGGER.info("API request start: route=%s context=%s", route, context or {})


def _log_api_response(route: str, payload: dict[str, Any]) -> None:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    items = data.get("items", []) if isinstance(data, dict) else []
    errors = data.get("errors", []) if isinstance(data, dict) else []
    keys = sorted(data.keys()) if isinstance(data, dict) else []
    LOGGER.info(
        "API response summary: route=%s ok=%s item_count=%s error_count=%s data_keys=%s",
        route,
        payload.get("ok") if isinstance(payload, dict) else None,
        len(items) if isinstance(items, list) else 0,
        len(errors) if isinstance(errors, list) else 0,
        keys,
    )


def success_response(message: str, data: dict[str, Any] | None = None, *, route: str | None = None) -> dict[str, Any]:
    """Build a consistent success response payload."""
    payload = {
        "ok": True,
        "message": message,
        "data": _normalize_response_data(message, data, True),
    }
    if route:
        _log_api_response(route, payload)
    return payload


def error_response(message: str, data: dict[str, Any] | None = None, *, route: str | None = None) -> dict[str, Any]:
    """Build a consistent error response payload."""
    payload = {
        "ok": False,
        "message": message,
        "data": _normalize_response_data(message, data, False),
    }
    if route:
        _log_api_response(route, payload)
    return payload


@app.get("/")
def serve_index() -> FileResponse:
    """Serve the local frontend entry page."""
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/low")
def get_low_opportunities():
    _log_api_request_start("/low")
    return success_response("low opportunity pool loaded", {"items": get_low_opportunity_pool()}, route="/low")


@app.get("/recommend")
def get_recommend():
    _log_api_request_start("/recommend")
    return success_response("opportunity recommendation loaded", {"item": get_auto_recommendation()}, route="/recommend")


@app.get("/api/health")
def health_check() -> dict[str, Any]:
    """Return a simple health payload."""
    _log_api_request_start("/api/health")
    return success_response(
        "service is running",
        {
            "watchlist_exists": WATCHLIST_PATH.exists(),
            "etf_watchlist_exists": ETF_WATCHLIST_PATH.exists(),
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        route="/api/health",
    )


@app.get("/api/config")
def get_config() -> dict[str, Any]:
    """Return a safe subset of current runtime config."""
    _log_api_request_start("/api/config")
    return success_response(
        "config loaded",
        {
            **get_public_runtime_config(),
            "env_exists": ENV_PATH.exists(),
        },
        route="/api/config",
    )


@app.post("/api/config")
def update_config(payload: ConfigUpdatePayload) -> dict[str, Any]:
    """Persist basic config values into the local .env file."""
    _log_api_request_start("/api/config", method="POST")
    updates = {
        "OPENAI_API_KEY": payload.openai_api_key,
        "OPENAI_MODEL": payload.openai_model,
        "FEISHU_WEBHOOK": payload.feishu_webhook,
        "DEFAULT_SCHEDULE_TIME": payload.default_schedule_time,
    }
    result = update_env_config({key: value for key, value in updates.items() if value is not None})
    return success_response("config saved", result, route="/api/config")


@app.get("/api/stocks")
def get_stocks() -> dict[str, Any]:
    """Return the current watchlist."""
    _log_api_request_start("/api/stocks")
    stocks = load_watchlist()
    LOGGER.info("Watchlist source result count: %s", len(stocks))
    return success_response("stocks loaded", {"stocks": stocks}, route="/api/stocks")


@app.get("/api/search-stocks")
def search_stock_options(q: str = "") -> dict[str, Any]:
    """Search A-share stocks by code or name for the watchlist selector."""
    _log_api_request_start("/api/search-stocks", q=q)
    items = search_stocks(q)
    LOGGER.info("Stock search result count: %s", len(items))
    return success_response("stocks searched", {"items": items}, route="/api/search-stocks")


@app.post("/api/stocks")
def create_stock(payload: StockPayload) -> dict[str, Any]:
    """Add one stock to the watchlist."""
    _log_api_request_start("/api/stocks", method="POST", code=payload.code)
    existing_codes = {item["code"] for item in load_watchlist()}
    stocks = add_stock(payload.code, payload.name)
    normalized_code = str(payload.code).strip().zfill(6)
    message = "stock added"
    if normalized_code in existing_codes:
        message = "stock already exists"
    LOGGER.info("Watchlist source result count: %s", len(stocks))
    return success_response(message, {"stocks": stocks}, route="/api/stocks")


@app.get("/api/etfs")
def get_etfs() -> dict[str, Any]:
    """Return the current ETF watchlist."""
    _log_api_request_start("/api/etfs")
    etfs = load_etf_watchlist()
    LOGGER.info("ETF watchlist source result count: %s", len(etfs))
    return success_response("etfs loaded", {"etfs": etfs}, route="/api/etfs")


@app.post("/api/etfs")
def create_etf(payload: StockPayload) -> dict[str, Any]:
    """Add one ETF to the watchlist."""
    _log_api_request_start("/api/etfs", method="POST", code=payload.code)
    existing_codes = {item["code"] for item in load_etf_watchlist()}
    etfs = add_etf(payload.code, payload.name)
    normalized_code = str(payload.code).strip().zfill(6)
    message = "etf added"
    if normalized_code in existing_codes:
        message = "etf already exists"
    LOGGER.info("ETF watchlist source result count: %s", len(etfs))
    return success_response(message, {"etfs": etfs}, route="/api/etfs")


@app.delete("/api/stocks/{code}")
def remove_stock(code: str) -> dict[str, Any]:
    """Delete one stock from the watchlist."""
    _log_api_request_start("/api/stocks/{code}", method="DELETE", code=code)
    existing_codes = {item["code"] for item in load_watchlist()}
    normalized_code = str(code).strip().zfill(6)
    stocks = delete_stock(normalized_code)
    if normalized_code not in existing_codes:
        return success_response("stock not found, nothing deleted", {"stocks": stocks}, route="/api/stocks/{code}")
    return success_response("stock deleted", {"stocks": stocks}, route="/api/stocks/{code}")


@app.delete("/api/etfs/{code}")
def remove_etf(code: str) -> dict[str, Any]:
    """Delete one ETF from the watchlist."""
    _log_api_request_start("/api/etfs/{code}", method="DELETE", code=code)
    existing_codes = {item["code"] for item in load_etf_watchlist()}
    normalized_code = str(code).strip().zfill(6)
    etfs = delete_etf(normalized_code)
    if normalized_code not in existing_codes:
        return success_response("etf not found, nothing deleted", {"etfs": etfs}, route="/api/etfs/{code}")
    return success_response("etf deleted", {"etfs": etfs}, route="/api/etfs/{code}")


@app.get("/api/run-once")
@app.post("/api/run-once")
def api_run_once() -> dict[str, Any]:
    """Execute one full monitoring cycle and return structured results."""
    result = run_once_service(
        push_notification=False,
        print_report=False,
        enable_ai_summary=False,
        market_timeout_seconds=2.5,
        fund_flow_timeout_seconds=2.5,
        ai_timeout_seconds=4.0,
        total_timeout_seconds=8.0,
        max_workers=3,
    )
    if not result["ok"]:
        return error_response(
            result["message"],
            {
                "report_path": result.get("report_path"),
                "results": result.get("results", []),
                "failed_symbols": result.get("failed_symbols", []),
                "elapsed_seconds": result.get("elapsed_seconds"),
            },
        )

    return success_response(
        result.get("message", "run once completed"),
        {
            "report_path": result["report_path"],
            "report_content": result["report_content"],
            "results": result["results"],
            "failed_symbols": result.get("failed_symbols", []),
            "opportunity_rank": result.get("opportunity_rank", []),
            "elapsed_seconds": result["elapsed_seconds"],
            "market_sentiment": result.get("market_sentiment", {}),
            "style_distribution": result.get("style_distribution", []),
            "notification": result["notification"],
        },
    )


@app.post("/api/run-etf-once")
def api_run_etf_once() -> dict[str, Any]:
    """Execute one ETF monitoring cycle and return structured results."""
    return run_etf_once_service()


@app.get("/api/etf/list")
def get_default_etf_list_api() -> dict[str, Any]:
    """Return the default ETF card list for the ETF recommendation page."""
    result = get_default_etf_list_service()
    return success_response(
        "etf list loaded",
        {
            "etfs": result.get("etfs", []),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
        },
    )


@app.get("/etfs")
@app.get("/list")
def get_default_etf_list_compat_api() -> dict[str, Any]:
    """Compatibility aliases for older ETF list requests."""
    result = get_default_etf_list_service()
    return success_response(
        "etf list loaded",
        {
            "etfs": result.get("etfs", []),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
        },
    )


@app.get("/api/etf/analyze")
def analyze_single_etf_api(code: str) -> dict[str, Any]:
    """Return detailed analysis for one ETF code."""
    result = analyze_single_etf_service(code)
    if not result.get("ok", False):
        return error_response(
            str(result.get("message", "ETF analysis failed")),
            {
                "etf": result.get("etf", {}),
                "elapsed_seconds": result.get("elapsed_seconds", 0),
            },
        )

    return success_response(
        "etf analysis loaded",
        {
            "etf": result.get("etf", {}),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
        },
    )


@app.get("/api/indexes")
@app.get("/indexes")
def get_indexes_api() -> dict[str, Any]:
    """Return the default selected index board for the ETF page."""
    result = get_indexes_service()
    return success_response(
        "indexes loaded",
        {
            "indexes": result.get("indexes", []),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
        },
    )


@app.get("/api/indexes/options")
@app.get("/options")
def get_index_options_api() -> dict[str, Any]:
    """Return the full addable index pool for the settings modal."""
    result = get_index_options_service()
    return success_response(
        "index options loaded",
        {
            "options": result.get("options", []),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
        },
    )


@app.get("/api/indexes/detail")
@app.get("/indexes/detail")
@app.get("/detail")
def get_index_detail_api(code: str) -> dict[str, Any]:
    """Return detail data for one index card."""
    result = get_index_detail_service(code)
    if not result.get("ok", False):
        return error_response(
            str(result.get("message", "index detail failed")),
            {
                "index": result.get("index", {}),
                "elapsed_seconds": result.get("elapsed_seconds", 0),
            },
        )

    return success_response(
        "index detail loaded",
        {
            "index": result.get("index", {}),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
        },
    )


@app.get("/api/index/list")
def get_index_board_api() -> dict[str, Any]:
    """Compatibility alias for the old index board route."""
    result = get_indexes_service()
    return success_response(
        "index board loaded",
        {
            "indices": result.get("indexes", []),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
        },
    )


@app.get("/api/index/analyze")
def analyze_index_api(code: str) -> dict[str, Any]:
    """Compatibility alias for the old index detail route."""
    result = get_index_detail_service(code)
    if not result.get("ok", False):
        return error_response(
            str(result.get("message", "index analysis failed")),
            {
                "index": result.get("index", {}),
                "elapsed_seconds": result.get("elapsed_seconds", 0),
            },
        )

    return success_response(
        "index analysis loaded",
        {
            "index": result.get("index", {}),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
        },
    )


@app.get("/api/opportunity-history")
def get_opportunity_history_api() -> dict[str, Any]:
    """Return saved opportunity recommendation history."""
    history = load_opportunity_history()
    return success_response("opportunity history loaded", {"history": history})


@app.get("/api/opportunity/low")
def get_low_opportunity_api() -> dict[str, Any]:
    """Return the current low-position opportunity pool."""
    return success_response(
        "low opportunity pool loaded",
        {
            "items": get_low_opportunity_pool(),
        },
    )


@app.get("/api/opportunities")
def get_opportunities_api(board: str = "all", scope: str = "market") -> dict[str, Any]:
    """Return the StockScoreService-based low-position opportunity pool."""
    requested_board = str(board or "all").strip().lower()
    normalized_board = requested_board if requested_board in {"all", "gem", "sz_main", "sh_main", "star"} else "all"
    requested_scope = str(scope or "market").strip().lower()
    normalized_scope = requested_scope if requested_scope in {"market", "watchlist"} else "market"
    return success_response(
        "opportunities loaded",
        {
            "scope": normalized_scope,
            "scope_name": SCOPE_NAMES.get(normalized_scope, "全市场"),
            "board": normalized_board,
            "items": get_opportunities(limit=10, board=normalized_board, scope=normalized_scope),
        },
    )


@app.get("/api/opportunity/recommend")
def get_opportunity_recommend_api() -> dict[str, Any]:
    """Return today's auto recommendation from the low-position pool."""
    return success_response(
        "opportunity recommendation loaded",
        {
            "item": get_auto_recommendation(),
        },
    )


@app.get("/api/opportunity/detail")
def get_opportunity_detail_api(code: str) -> dict[str, Any]:
    """Return one low-position opportunity detail."""
    item = get_opportunity_detail(code)
    if item is None:
        return error_response(
            "opportunity not found",
            {
                "item": {},
            },
        )
    return success_response(
        "opportunity detail loaded",
        {
            "item": item,
        },
    )


@app.get("/api/opportunity/stock_low")
def get_stock_low_opportunity_api() -> dict[str, Any]:
    """Return the current stock low-position opportunity pool."""
    return success_response(
        "stock low opportunity pool loaded",
        {
            "items": get_stock_low_opportunity_pool(),
        },
    )


@app.get("/api/opportunity-stats")
def get_opportunity_stats_api() -> dict[str, Any]:
    """Return hit-rate statistics from reviewed opportunities."""
    history = load_opportunity_history()
    stats = calculate_hit_stats(history)
    return success_response("opportunity stats loaded", {"stats": stats})


@app.post("/api/review-opportunities")
def review_opportunities_api() -> dict[str, Any]:
    """Manually trigger one opportunity review pass."""
    result = review_opportunities()
    return success_response(
        "opportunity review completed",
        {
            "updated": result.get("updated", 0),
            "pending": result.get("pending", 0),
            "stats": result.get("stats", {}),
            "history": result.get("history", []),
        },
    )


@app.get("/api/reports")
def get_reports() -> dict[str, Any]:
    """Return the report file list."""
    reports = list_reports()
    return success_response("reports loaded", {"reports": reports})


@app.get("/api/reports/latest")
def get_latest_report_api() -> dict[str, Any]:
    """Return the latest report content."""
    report = get_latest_report()
    if report is None:
        raise HTTPException(status_code=404, detail="latest report not found")
    return success_response("latest report loaded", report)


@app.get("/api/reports/{filename}")
def get_report_api(filename: str) -> dict[str, Any]:
    """Return one report content by filename."""
    report = get_report_by_filename(filename)
    if report is None:
        raise HTTPException(status_code=404, detail="report not found")
    return success_response("report loaded", report)


@app.exception_handler(HTTPException)
async def http_exception_handler(_, exc: HTTPException) -> JSONResponse:
    """Return JSON responses for expected HTTP errors."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "ok": False,
            "message": str(exc.detail),
            "data": {
                "status_code": exc.status_code,
            },
        },
    )


@app.exception_handler(Exception)
async def unexpected_exception_handler(_, exc: Exception) -> JSONResponse:
    """Return JSON responses for unexpected errors."""
    LOGGER.exception("Unhandled API exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "message": "internal server error",
            "data": {},
        },
    )
