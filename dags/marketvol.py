"""Airflow mini-project DAG: marketvol with HDFS output only.

This version follows the project guideline strictly:
- t1 downloads AAPL data
- t2 downloads TSLA data
- t3 saves AAPL as CSV and uploads it to HDFS
- t4 saves TSLA as CSV and uploads it to HDFS
- t5 runs a custom query after both HDFS uploads finish

The DAG uses Chapter 2 TaskFlow style:
- @dag for the DAG definition
- @task for Python tasks
- @task.bash for Bash setup
- bitshift syntax (>>) for dependencies
"""

from __future__ import annotations

from datetime import timedelta
from io import StringIO
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import pendulum
import requests
import yfinance as yf
from airflow.sdk import dag, task

BASE_TMP_DIR = "/tmp/data"
HDFS_WEBHDFS_URL = "http://namenode:9870/webhdfs/v1"
HDFS_BASE_DIR = "/marketvol"
HDFS_USER = "root"
SYMBOLS = ["AAPL", "TSLA"]


def _flatten_yfinance_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns that yfinance can return."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]
    return df


def _webhdfs_url(hdfs_path: str, operation: str, **params: str) -> str:
    """Build a WebHDFS URL for the HDFS NameNode Docker service."""
    clean_path = "/" + hdfs_path.strip("/")
    encoded_path = quote(clean_path, safe="/")
    query = {"op": operation, "user.name": HDFS_USER, **params}
    query_string = "&".join(f"{quote(str(k))}={quote(str(v))}" for k, v in query.items())
    return f"{HDFS_WEBHDFS_URL.rstrip('/')}{encoded_path}?{query_string}"


def create_hdfs_directory(hdfs_dir: str) -> None:
    """Create an HDFS directory using WebHDFS."""
    response = requests.put(_webhdfs_url(hdfs_dir, "MKDIRS"), timeout=30)
    response.raise_for_status()
    print(f"Ensured HDFS directory exists: {hdfs_dir}")


def upload_text_to_hdfs(csv_text: str, hdfs_file: str) -> None:
    """Upload CSV text to HDFS using WebHDFS CREATE."""
    create_url = _webhdfs_url(hdfs_file, "CREATE", overwrite="true")
    first_response = requests.put(create_url, allow_redirects=False, timeout=30)

    if first_response.status_code not in {307, 201}:
        first_response.raise_for_status()

    upload_url = first_response.headers.get("Location")
    if upload_url:
        second_response = requests.put(upload_url, data=csv_text.encode("utf-8"), timeout=120)
        second_response.raise_for_status()
    else:
        first_response.raise_for_status()

    print(f"Uploaded CSV data to HDFS: {hdfs_file}")


def read_hdfs_file(hdfs_file: str) -> str:
    """Read a file from HDFS using WebHDFS OPEN."""
    open_url = _webhdfs_url(hdfs_file, "OPEN")
    response = requests.get(open_url, allow_redirects=True, timeout=120)
    response.raise_for_status()
    return response.text


@dag(
    dag_id="marketvol",
    description="Download AAPL and TSLA one-minute market data, load CSV files to HDFS, and run a volume query.",
    start_date=pendulum.datetime(2026, 1, 1, 18, 0, tz="UTC"),
    schedule="0 18 * * 1-5",  # 6 PM every weekday, Monday-Friday
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["mini-project", "stocks", "yfinance", "hdfs"],
)
def marketvol():
    """Create the marketvol DAG using TaskFlow-style operators."""

    @task.bash(task_id="t0_initialize_temp_directory")
    def initialize_temp_directory() -> str:
        # Temporary working folder only. Final output is written to HDFS.
        return f"mkdir -p {BASE_TMP_DIR}/{{{{ ds }}}}"

    @task(task_id="t1_download_aapl")
    def download_aapl(ds: str | None = None) -> str:
        return download_market_data(symbol="AAPL", ds=ds)

    @task(task_id="t2_download_tsla")
    def download_tsla(ds: str | None = None) -> str:
        return download_market_data(symbol="TSLA", ds=ds)

    @task(task_id="t3_save_aapl_csv_to_hdfs")
    def save_aapl_csv_to_hdfs(data_file: str, ds: str | None = None) -> str:
        return save_csv_and_upload_to_hdfs(symbol="AAPL", data_file=data_file, ds=ds)

    @task(task_id="t4_save_tsla_csv_to_hdfs")
    def save_tsla_csv_to_hdfs(data_file: str, ds: str | None = None) -> str:
        return save_csv_and_upload_to_hdfs(symbol="TSLA", data_file=data_file, ds=ds)

    @task(task_id="t5_query_stock_data")
    def query_stock_data(aapl_hdfs_file: str, tsla_hdfs_file: str, ds: str | None = None) -> str:
        return run_stock_query(aapl_hdfs_file=aapl_hdfs_file, tsla_hdfs_file=tsla_hdfs_file, ds=ds)

    t0 = initialize_temp_directory()
    t1 = download_aapl(ds="{{ ds }}")
    t2 = download_tsla(ds="{{ ds }}")
    t3 = save_aapl_csv_to_hdfs(t1, ds="{{ ds }}")
    t4 = save_tsla_csv_to_hdfs(t2, ds="{{ ds }}")
    t5 = query_stock_data(t3, t4, ds="{{ ds }}")

    # Required dependency order:
    # t1 and t2 run after t0 and can run in parallel.
    # t3 runs after t1, and t4 runs after t2; t3 and t4 can run in parallel.
    # t5 runs only after both t3 and t4 complete.
    t0 >> [t1, t2]
    t1 >> t3
    t2 >> t4
    [t3, t4] >> t5


def download_market_data(symbol: str, ds: str | None = None) -> str:
    """Download one-minute Yahoo Finance data for one symbol and save a temporary pickle file."""
    if not ds:
        raise ValueError("Airflow logical date ds was not provided.")

    output_dir = Path(BASE_TMP_DIR) / ds
    output_dir.mkdir(parents=True, exist_ok=True)

    start_date = pendulum.parse(ds).date()
    end_date = start_date + timedelta(days=1)

    df = yf.download(
        symbol,
        start=start_date,
        end=end_date,
        interval="1m",
        progress=False,
        auto_adjust=False,
    )

    if df.empty:
        raise ValueError(f"No market data returned for {symbol} on {ds}. Try a recent weekday trading date.")

    df = _flatten_yfinance_columns(df).reset_index()

    # This is temporary task handoff storage, not the final project output.
    output_file = output_dir / f"{symbol}.pkl"
    df.to_pickle(output_file)
    print(f"Downloaded {symbol} data and stored temporary task file: {output_file}")
    return str(output_file)


def save_csv_and_upload_to_hdfs(symbol: str, data_file: str, ds: str | None = None) -> str:
    """Save one downloaded dataset as CSV and load that CSV into HDFS."""
    if not ds:
        raise ValueError("Airflow logical date ds was not provided.")

    df = pd.read_pickle(data_file)
    csv_text = df.to_csv(index=False)

    hdfs_dir = f"{HDFS_BASE_DIR}/{ds}"
    hdfs_file = f"{hdfs_dir}/{symbol}.csv"

    create_hdfs_directory(hdfs_dir)
    upload_text_to_hdfs(csv_text=csv_text, hdfs_file=hdfs_file)
    return hdfs_file


def run_stock_query(aapl_hdfs_file: str, tsla_hdfs_file: str, ds: str | None = None) -> str:
    """Run a custom query on both HDFS CSV files and write the summary to HDFS."""
    if not ds:
        raise ValueError("Airflow logical date ds was not provided.")

    frames: list[pd.DataFrame] = []
    for symbol, hdfs_file in [("AAPL", aapl_hdfs_file), ("TSLA", tsla_hdfs_file)]:
        csv_text = read_hdfs_file(hdfs_file)
        df = pd.read_csv(StringIO(csv_text))
        df["symbol"] = symbol
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    close_col = "Close" if "Close" in combined.columns else "close"
    volume_col = "Volume" if "Volume" in combined.columns else "volume"

    result = combined.groupby("symbol").agg(
        average_close=(close_col, "mean"),
        highest_close=(close_col, "max"),
        lowest_close=(close_col, "min"),
        total_volume=(volume_col, "sum") if volume_col in combined.columns else (close_col, "count"),
        records=(close_col, "count"),
    )

    summary_text = result.to_csv()
    summary_hdfs_file = f"{HDFS_BASE_DIR}/{ds}/marketvol_summary.csv"
    upload_text_to_hdfs(csv_text=summary_text, hdfs_file=summary_hdfs_file)

    print("Market volume summary:")
    print(result)
    print(f"Summary written to HDFS: {summary_hdfs_file}")
    return summary_hdfs_file


marketvol()
