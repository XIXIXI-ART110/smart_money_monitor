from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from config import BASE_DIR, LOGGER, REPORT_DIR, ensure_runtime_directories


def _to_relative_path(path: Path) -> str:
    """Convert a path into a project-relative string when possible."""
    try:
        return str(path.relative_to(BASE_DIR)).replace("\\", "/")
    except ValueError:
        return str(path)


def _build_report_item(path: Path, include_content: bool = False) -> dict[str, Any]:
    """Build a metadata payload for one report file."""
    item: dict[str, Any] = {
        "filename": path.name,
        "path": _to_relative_path(path),
        "modified_time": datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
    }
    if include_content:
        item["content"] = path.read_text(encoding="utf-8")
    return item


def list_reports() -> list[dict[str, Any]]:
    """List all report files in reverse chronological order."""
    ensure_runtime_directories()
    if not REPORT_DIR.exists():
        return []

    report_files = sorted(
        [path for path in REPORT_DIR.glob("*.md") if path.is_file()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return [_build_report_item(path) for path in report_files]


def get_latest_report() -> dict[str, Any] | None:
    """Return the latest report with full markdown content."""
    reports = list_reports()
    if not reports:
        return None
    return get_report_by_filename(reports[0]["filename"])


def get_report_by_filename(filename: str) -> dict[str, Any] | None:
    """Return one report by filename with content, or None when missing."""
    ensure_runtime_directories()
    safe_name = Path(filename).name
    if safe_name != filename or not safe_name:
        LOGGER.warning("Invalid report filename requested: %s", filename)
        return None

    report_path = REPORT_DIR / safe_name
    if not report_path.exists() or not report_path.is_file():
        return None

    try:
        return _build_report_item(report_path, include_content=True)
    except OSError as exc:
        LOGGER.exception("Failed to read report %s: %s", report_path, exc)
        return None
