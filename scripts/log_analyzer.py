#!/usr/bin/env python3
"""Airflow log analyzer.

This script recursively scans an Airflow log directory, counts ERROR entries,
and prints each detailed error message.

It supports both common Airflow log formats:
1. Classic text logs:
   [2020-09-26 20:15:33,479] {taskinstance.py:1150} ERROR - No columns to parse from file
2. Airflow JSON logs:
   {"timestamp":"...","level":"error","event":"Task failed with exception", ...}
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Iterable

CLASSIC_ERROR_PATTERN = re.compile(r"^\[(?P<timestamp>[^\]]+)\]\s+(?P<source>\{[^}]+\})\s+ERROR\s+-\s+(?P<message>.*)$")
LOG_START_PATTERN = re.compile(r"^\[\d{4}-\d{2}-\d{2} .*\]\s+\{[^}]+\}\s+(INFO|WARNING|ERROR|DEBUG|CRITICAL)\s+-")


def discover_log_files(log_dir: Path) -> list[Path]:
    """Return all .log files under the supplied root directory."""
    return sorted(path for path in log_dir.rglob("*.log") if path.is_file())


def _format_json_error(record: dict, file_path: Path, line_number: int) -> str:
    timestamp = record.get("timestamp", "unknown-time")
    event = record.get("event", "")
    logger = record.get("logger", "")
    filename = record.get("filename", "")
    lineno = record.get("lineno", "")
    task_id = record.get("task_id", "")
    dag_id = record.get("dag_id", "")
    run_id = record.get("run_id", "")

    parts = [f"[{timestamp}]"]
    if filename or lineno:
        parts.append(f"{{{filename}:{lineno}}}")
    parts.append("ERROR -")
    parts.append(str(event))

    context = []
    if dag_id:
        context.append(f"dag_id={dag_id}")
    if task_id:
        context.append(f"task_id={task_id}")
    if run_id:
        context.append(f"run_id={run_id}")
    if logger:
        context.append(f"logger={logger}")
    context.append(f"file={file_path}")
    context.append(f"line={line_number}")

    error_detail = record.get("error_detail")
    if error_detail:
        parts.append(f"| detail={json.dumps(error_detail, ensure_ascii=False)}")

    return " ".join(parts) + " | " + "; ".join(context)


def analyze_file(file_path: str | Path) -> tuple[int, list[str]]:
    """Analyze one Airflow log file.

    Returns:
        A tuple containing:
        - total ERROR entries found in this file
        - detailed error messages for this file
    """
    path = Path(file_path)
    errors: list[str] = []

    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        return 1, [f"ERROR - Could not read log file {path}: {exc}"]

    index = 0
    while index < len(lines):
        line = lines[index]
        stripped = line.strip()

        # Airflow 3 JSON log format.
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                record = json.loads(stripped)
            except json.JSONDecodeError:
                index += 1
                continue
            if str(record.get("level", "")).lower() == "error":
                errors.append(_format_json_error(record, path, index + 1))
            index += 1
            continue

        # Classic Airflow text format.
        match = CLASSIC_ERROR_PATTERN.match(line)
        if match:
            message_lines = [line]
            next_index = index + 1
            while next_index < len(lines) and not LOG_START_PATTERN.match(lines[next_index]):
                if lines[next_index].strip():
                    message_lines.append(lines[next_index])
                next_index += 1
            errors.append("\n".join(message_lines) + f"\n  file={path}; line={index + 1}")
            index = next_index
            continue

        index += 1

    return len(errors), errors


def analyze_directory(log_dir: str | Path) -> tuple[int, list[str], int]:
    """Analyze every .log file under the supplied log root."""
    root = Path(log_dir).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Log directory does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {root}")

    total_errors = 0
    all_errors: list[str] = []
    files = discover_log_files(root)

    for file_path in files:
        count, cur_list = analyze_file(file_path)
        total_errors += count
        all_errors.extend(cur_list)

    return total_errors, all_errors, len(files)


def print_report(total_errors: int, errors: Iterable[str], files_scanned: int) -> None:
    """Print cumulative log analysis results."""
    print(f"Total log files scanned: {files_scanned}")
    print(f"Total number of errors: {total_errors}")
    print("Here are all the errors:")
    for number, error in enumerate(errors, start=1):
        print(f"\n{number}. {error}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze Airflow logs and report ERROR messages.")
    parser.add_argument("log_dir", help="Root Airflow log directory, for example ./logs/dag_id=marketvol")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    try:
        total_errors, errors, files_scanned = analyze_directory(args.log_dir)
    except Exception as exc:
        print(f"ERROR - {exc}", file=sys.stderr)
        return 1

    print_report(total_errors, errors, files_scanned)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
