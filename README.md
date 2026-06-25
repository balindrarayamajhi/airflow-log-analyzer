# Aiflow Log Analyzer

This project builds on the previous `marketvol` Airflow mini-project. It adds a Python log analyzer that recursively reads Airflow `.log` files, counts `ERROR` entries, and prints detailed information for every error found.

> Note: The requested project name was written as **aiflow log analyzer**, so this repository uses `aiflow-log-analyzer` and the DAG id `aiflow_log_analyzer`.

## Requirement covered

The analyzer meets the Log Analyzer mini-project requirements:

- Locate and scan Airflow log files under the log root directory.
- Recursively read every `.log` file using `pathlib.Path.rglob('*.log')`.
- Provide an `analyze_file(file)` method.
- Return the error count and detailed error list for each file.
- Print the cumulative error count and all error details.


## Project structure

```text
aiflow-log-analyzer/
├── dags/
│   ├── marketvol.py
│   └── aiflow_log_analyzer.py
├── logs/
│   └── dag_id=marketvol/
├── scripts/
│   ├── log_analyzer.py
│   └── query_stock_data.py
├── validation/
│   └── execution_logs/
│       └── log_analyzer_success.txt
├── run_log_analyzer.sh
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Main files

### `scripts/log_analyzer.py`

Standalone Python command-line application. It accepts the Airflow log root directory as an argument.

Main functions:

```python
discover_log_files(log_dir)
analyze_file(file_path)
analyze_directory(log_dir)
print_report(total_errors, errors, files_scanned)
```

The script supports both classic Airflow text logs and newer JSON-formatted Airflow logs.

### `run_log_analyzer.sh`

Convenience shell script for running the analyzer.

### `dags/aiflow_log_analyzer.py`

Optional Airflow DAG version of the analyzer. It creates two Python tasks:

- `t1_analyze_aapl_logs`
- `t2_analyze_tsla_logs`

These tasks call the same `analyze_file()` function used by the command-line analyzer.


## Run with Docker/Airflow

Start the Airflow project:

```bash
docker compose build
docker compose up airflow-init
docker compose up
```

Open Airflow:

```text
http://localhost:8080
```

Default login:

```text
username: airflow
password: airflow
```

You can trigger the analyzer DAG from the CLI:

```bash
docker compose exec airflow-apiserver airflow dags trigger aiflow_log_analyzer
```

You can also run the standalone analyzer inside the Airflow container:

```bash
docker compose exec airflow-apiserver python /opt/airflow/scripts/log_analyzer.py /opt/airflow/logs/dag_id=marketvol
```

## Original marketvol DAG

The original `marketvol` DAG is still included. It downloads AAPL and TSLA data, writes CSV files to HDFS, and creates a summary file.

The new analyzer can be used after the DAG runs to inspect the generated Airflow task logs.

## Validation

The `validation` directory contains execution evidence demonstrating that the project was successfully implemented and verified.

### Execution Logs

The `validation/execution_logs` folder includes:

- A step-by-step validation guide (`README.md`)
- Screenshots showing that the `airflow_log_analyzer` DAG is successfully loaded in the Airflow UI
- Evidence that the `marketvol` DAG executed successfully and generated Airflow task logs
- The commands used to trigger the `airflow_log_analyzer` DAG
- Screenshots confirming the successful execution of the Log Analyzer DAG
- Console output showing the cumulative error count and detailed error messages collected from all analyzed log files

To review the execution evidence, navigate to:

```text
validation/
└── execution_logs/
    ├── README.md
    ├── 01_airflow_log_analyzer_dag.png
    ├── 02_marketvol_dag.png
    ├── 03_marketvol_logs.png
    ├── 04_airflow_log_analyzer_success.png
    └── 05_console_output.png
```

