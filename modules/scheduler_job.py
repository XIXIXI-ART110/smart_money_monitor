from __future__ import annotations

import time
from datetime import datetime

import schedule

from config import DEFAULT_SCHEDULE_TIME, LOGGER, ensure_runtime_directories
from modules.run_service import run_once_service


def _normalize_schedule_time(schedule_time: str) -> str:
    """Validate and normalize a HH:MM schedule string."""
    try:
        parsed = datetime.strptime(schedule_time, "%H:%M")
        return parsed.strftime("%H:%M")
    except ValueError:
        LOGGER.warning(
            "Invalid DEFAULT_SCHEDULE_TIME '%s'. Falling back to 09:00.",
            schedule_time,
        )
        return "09:00"


def run_once() -> None:
    """Run the full monitoring workflow once."""
    ensure_runtime_directories()
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{start_time}] 开始执行 A 股智能监控任务")
    result = run_once_service(push_notification=True, print_report=True)

    if not result["ok"]:
        print(result["message"])
        return

    if not result["notification"]["sent"] and result["notification"]["reason"] == "webhook_not_configured":
        print("未配置飞书 webhook，本次仅保存本地报告。")

    print(f"报告已保存到：{result['report_path']}")


def run_daily_scheduler() -> None:
    """Run the monitoring workflow on weekday schedule."""
    schedule.clear()
    schedule_time = _normalize_schedule_time(DEFAULT_SCHEDULE_TIME)

    schedule.every().monday.at(schedule_time).do(run_once)
    schedule.every().tuesday.at(schedule_time).do(run_once)
    schedule.every().wednesday.at(schedule_time).do(run_once)
    schedule.every().thursday.at(schedule_time).do(run_once)
    schedule.every().friday.at(schedule_time).do(run_once)

    LOGGER.info("Scheduler started for weekdays at %s.", schedule_time)
    print(f"定时任务已启动：每个工作日 {schedule_time} 执行一次。")

    heartbeat_time = 0.0
    while True:
        schedule.run_pending()

        current_timestamp = time.time()
        if current_timestamp - heartbeat_time >= 60:
            next_run = schedule.next_run()
            next_run_text = next_run.strftime("%Y-%m-%d %H:%M:%S") if next_run else "N/A"
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 等待下次任务，下一次执行时间：{next_run_text}")
            heartbeat_time = current_timestamp

        time.sleep(1)


def self_test() -> None:
    """Run a lightweight scheduler self-test without entering the infinite loop."""
    print("scheduler_job 自测：开始执行 run_once()。")
    run_once()


if __name__ == "__main__":
    self_test()
