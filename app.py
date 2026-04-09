from __future__ import annotations

import argparse
import time

from config import LOGGER, get_runtime_warnings
from modules.run_service import run_once_service
from scheduler import start_scheduler


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="A-share smart research monitor for local command-line usage."
    )
    mode_group = parser.add_mutually_exclusive_group(required=False)
    mode_group.add_argument(
        "--once",
        action="store_true",
        help="Run the monitoring workflow once immediately and exit.",
    )
    mode_group.add_argument(
        "--schedule",
        action="store_true",
        help="Start the background scheduler and keep the process alive.",
    )
    return parser.parse_args()


def _print_startup_warnings() -> None:
    """Print runtime configuration warnings once at startup."""
    for warning in get_runtime_warnings():
        LOGGER.warning(warning)
        print(f"[配置提示] {warning}")


def _run_once_and_exit() -> None:
    """Run the workflow once and print a short summary."""
    result = run_once_service(push_notification=True, print_report=True)
    print(
        "本次执行完成 | "
        f"ok={result.get('ok')} | "
        f"elapsed_seconds={result.get('elapsed_seconds')} | "
        f"report_path={result.get('report_path')}"
    )


def _keep_process_alive() -> None:
    """Keep the process alive so the background scheduler can run jobs."""
    print("定时任务进程已进入常驻模式，按 Ctrl+C 可退出。")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        LOGGER.info("Scheduler process stopped by user.")
        print("定时任务进程已停止。")


def main() -> None:
    """Application entry point."""
    args = parse_args()
    LOGGER.info("Application started.")
    _print_startup_warnings()

    # 按要求：app.py 启动时自动启动 scheduler
    start_scheduler()

    if args.once:
        _run_once_and_exit()
        return

    _keep_process_alive()


if __name__ == "__main__":
    main()
