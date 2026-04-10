from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
REPORT_DIR = DATA_DIR / "reports"
CACHE_DIR = DATA_DIR / "cache"
WATCHLIST_PATH = DATA_DIR / "watchlist.json"
ETF_WATCHLIST_PATH = DATA_DIR / "etf_watchlist.json"
OPPORTUNITY_HISTORY_PATH = DATA_DIR / "opportunity_history.json"
LOG_DIR = BASE_DIR / "logs"
LOG_PATH = LOG_DIR / "app.log"
ENV_PATH = BASE_DIR / ".env"

load_dotenv(ENV_PATH)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK", "").strip() or ""
DEFAULT_SCHEDULE_TIME = os.getenv("DEFAULT_SCHEDULE_TIME", "09:00").strip() or "09:00"
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "").strip() or ""

# Data provider selection.
# free: current free data source (AkShare)
# ths: placeholder for official Tonghuashun / iFinD / QuantAPI access
DATA_PROVIDER = os.getenv("DATA_PROVIDER", "free").strip().lower() or "free"

# Future Tonghuashun official access configuration.
# Python SDK direction: iFinDPy login credentials
THS_USERNAME = os.getenv("THS_USERNAME", "").strip() or ""
THS_PASSWORD = os.getenv("THS_PASSWORD", "").strip() or ""

# HTTP API direction: base URL + token
THS_API_BASE = os.getenv("THS_API_BASE", "").strip() or ""
THS_TOKEN = os.getenv("THS_TOKEN", "").strip() or ""


def ensure_runtime_directories() -> None:
    """Ensure all runtime directories exist before file operations."""
    for directory in (DATA_DIR, REPORT_DIR, CACHE_DIR, LOG_DIR):
        directory.mkdir(parents=True, exist_ok=True)


ensure_runtime_directories()


def setup_logging() -> logging.Logger:
    """Configure the shared project logger."""
    logger = logging.getLogger("smart_money_monitor")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


LOGGER = setup_logging()


def get_runtime_warnings() -> list[str]:
    """Return user-friendly startup warnings for missing optional configuration."""
    warnings: list[str] = []
    if not ENV_PATH.exists():
        warnings.append("未检测到 .env 文件，将使用默认配置继续运行。")
    if not OPENAI_API_KEY:
        warnings.append("未配置 OPENAI_API_KEY，AI 解读将自动降级为规则兜底文本。")
    if not FEISHU_WEBHOOK:
        warnings.append("未配置 FEISHU_WEBHOOK，本次不会推送飞书消息。")
    if not TUSHARE_TOKEN:
        warnings.append("未配置 TUSHARE_TOKEN，A股个股行情将无法使用 Tushare 数据源。")
    if DATA_PROVIDER == "ths" and not any([THS_USERNAME and THS_PASSWORD, THS_API_BASE and THS_TOKEN]):
        warnings.append("已切换 DATA_PROVIDER=ths，但尚未配置同花顺接口参数。")
    return warnings


def get_public_runtime_config() -> dict[str, Any]:
    """Return a frontend-friendly snapshot of the current runtime config."""
    return {
        "openai_model": OPENAI_MODEL,
        "default_schedule_time": DEFAULT_SCHEDULE_TIME,
        "data_provider": DATA_PROVIDER,
        "has_openai_api_key": bool(OPENAI_API_KEY),
        "has_feishu_webhook": bool(FEISHU_WEBHOOK),
        "has_tushare_token": bool(TUSHARE_TOKEN),
        "has_ths_credentials": bool(THS_USERNAME and THS_PASSWORD),
        "has_ths_api_token": bool(THS_API_BASE and THS_TOKEN),
        "env_path": str(ENV_PATH),
    }


def update_env_config(updates: dict[str, str]) -> dict[str, Any]:
    """Persist selected config values into the local .env file."""
    allowed_keys = {
        "OPENAI_API_KEY",
        "OPENAI_MODEL",
        "FEISHU_WEBHOOK",
        "DEFAULT_SCHEDULE_TIME",
        "TUSHARE_TOKEN",
        "DATA_PROVIDER",
        "THS_USERNAME",
        "THS_PASSWORD",
        "THS_API_BASE",
        "THS_TOKEN",
    }
    sanitized_updates = {
        key: str(value).strip()
        for key, value in updates.items()
        if key in allowed_keys and value is not None
    }

    ensure_runtime_directories()
    existing_lines: list[str] = []
    if ENV_PATH.exists():
        existing_lines = ENV_PATH.read_text(encoding="utf-8").splitlines()

    key_to_index: dict[str, int] = {}
    for index, line in enumerate(existing_lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in allowed_keys:
            key_to_index[key] = index

    for key, value in sanitized_updates.items():
        new_line = f"{key}={value}"
        if key in key_to_index:
            existing_lines[key_to_index[key]] = new_line
        else:
            existing_lines.append(new_line)

    content = "\n".join(existing_lines).strip()
    if content:
        content += "\n"
    ENV_PATH.write_text(content, encoding="utf-8")

    return {
        "updated_keys": sorted(sanitized_updates.keys()),
        "env_path": str(ENV_PATH),
        "message": "配置已写入 .env，重启服务后生效。",
    }
