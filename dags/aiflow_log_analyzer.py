"""Airflow DAG for analyzing AAPL and TSLA task logs from the marketvol project."""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.sdk import dag, task

# Reuse the standalone analyzer from the scripts folder mounted into the Airflow container.
# If scripts is not mounted, copy scripts/log_analyzer.py into the container or run the CLI locally.
import sys
sys.path.append("/opt/airflow/scripts")

from log_analyzer import analyze_file  # noqa: E402

LOG_ROOT = Path("/opt/airflow/logs/dag_id=marketvol")


def analyze_symbol_logs(symbol: str) -> dict[str, object]:
    """Analyze log files for one stock symbol task group."""
    symbol_upper = symbol.upper()
    total = 0
    details: list[str] = []

    for file_path in sorted(LOG_ROOT.rglob("*.log")):
        path_text = str(file_path).upper()
        if symbol_upper not in path_text:
            continue
        count, cur_list = analyze_file(file_path)
        total += count
        details.extend(cur_list)

    print(f"Total number of errors for {symbol_upper}: {total}")
    print(f"Here are all {symbol_upper} errors:")
    for error in details:
        print(error)

    return {"symbol": symbol_upper, "error_count": total, "errors": details}


@dag(
    dag_id="aiflow_log_analyzer",
    description="Analyze marketvol Airflow logs and report ERROR messages for AAPL and TSLA task logs.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["mini-project", "logs", "analyzer"],
)
def aiflow_log_analyzer():
    @task(task_id="t1_analyze_aapl_logs")
    def t1() -> dict[str, object]:
        return analyze_symbol_logs("AAPL")

    @task(task_id="t2_analyze_tsla_logs")
    def t2() -> dict[str, object]:
        return analyze_symbol_logs("TSLA")

    t1()
    t2()


aiflow_log_analyzer()
