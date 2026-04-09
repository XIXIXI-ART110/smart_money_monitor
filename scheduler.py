from __future__ import annotations

import atexit
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler

from config import LOGGER
from modules.run_service import run_once_service


_scheduler: BackgroundScheduler | None = None


def run_daily_job() -> dict[str, Any]:
    """Run the daily monitoring job and log a concise summary."""
    result = run_once_service(push_notification=True, print_report=False)
    results = result.get("results", [])
    success_count = sum(1 for item in results if item.get("status") == "ok")
    error_count = sum(1 for item in results if item.get("status") != "ok")

    LOGGER.info(
        "定时任务执行完成 | ok=%s | success=%s | error=%s | elapsed_seconds=%s | report_path=%s",
        result.get("ok"),
        success_count,
        error_count,
        result.get("elapsed_seconds"),
        result.get("report_path"),
    )
    print(
        "定时任务执行完成 | "
        f"success={success_count} | "
        f"error={error_count} | "
        f"elapsed_seconds={result.get('elapsed_seconds')} | "
        f"report_path={result.get('report_path')}"
    )
    return result


def start_scheduler() -> BackgroundScheduler:
    """Start the background scheduler once and register the daily job."""
    global _scheduler

    if _scheduler is not None and _scheduler.running:
        return _scheduler

    scheduler = BackgroundScheduler(timezone="Asia/Taipei")
    scheduler.add_job(
        run_daily_job,
        trigger="cron",
        hour=17,
        minute=0,
        id="smart_money_daily_run",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False) if scheduler.running else None)

    LOGGER.info("APScheduler started. Daily job scheduled at 17:00.")
    print("APScheduler 已启动：每天 17:00 自动执行一次分析任务。")

    _scheduler = scheduler
    return scheduler
