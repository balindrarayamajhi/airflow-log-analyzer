# MarketVol Airflow Pipeline Validation

This document captures the steps used to build, run, trigger, and validate the **MarketVol Airflow DAG**.  
The screenshots are stored in the `images/` directory and are referenced with relative paths, so they will display correctly in the GitHub UI.

---

## 1. Build the Docker Images

Build all Docker images from scratch to ensure the latest source code and dependency changes are included.

```bash
docker compose build --no-cache
```

**Evidence**

![Docker Compose Build 1](images/01-docker-compose-build-1.png)

![Docker Compose Build 2](images/02-docker-compose-build-2.png)

---

## 2. Initialize Airflow

Run the Airflow initialization service. This prepares the Airflow metadata database and required setup.

```bash
docker compose up airflow-init
```

**Evidence**

![Airflow Init](images/03-docker-compose-up-airflow-init.png)

---

## 3. Start All Services

Start the full Docker Compose environment.

```bash
docker compose up
```

This starts Airflow, PostgreSQL, Redis, Hadoop NameNode, Hadoop DataNode, scheduler, worker, triggerer, and DAG processor services.

**Evidence**

![Docker Compose Up](images/04-docker-compose-up.png)

![Docker Compose Up Continue](images/05-docker-compose-up-continue.png)

---

## 4. Verify the `marketvol` DAG in Airflow UI

After the services are running, open the Airflow UI and verify that the `marketvol` DAG is visible.

**Expected Result**

- The `marketvol` DAG is listed in the Airflow UI.
- The DAG is available for manual or scheduled execution.

**Evidence**

![MarketVol DAG in Airflow UI](images/06-airflow-ui-marketvol-dag.png)

---

## 5. Trigger the `marketvol` DAG Manually

Trigger the DAG manually using the Airflow CLI inside the `airflow-apiserver` container.

```bash
docker compose exec airflow-apiserver airflow dags trigger marketvol
```

**Expected Result**

A new manual DAG run is created and queued.

**Evidence**

![Trigger MarketVol DAG](images/07-trigger-marketvol-dag-cli.png)

---

## 6. Verify Successful DAG Run in Airflow UI

Open the DAG run in the Airflow UI and verify that all tasks completed successfully.

**Expected Result**

- DAG run status is `Success`.
- All task instances are green.
- No failed tasks are shown.

**Evidence**

![Successful DAG Run](images/08-airflow-ui-successful-run.png)

---

## 7. Validate Files Persisted to HDFS

Use the Hadoop NameNode container to list files written under `/marketvol`.

```bash
docker compose exec namenode hdfs dfs -ls -R /marketvol
```

**Expected Files**

- `AAPL.csv`
- `TSLA.csv`
- `marketvol_summary.csv`

**Evidence**

![HDFS File Listing](images/09-hdfs-list-marketvol-files.png)

---

## 8. View the Summary File from HDFS

Read the generated summary CSV directly from HDFS.

```bash
docker compose exec namenode hdfs dfs -cat /marketvol/2026-06-24/marketvol_summary.csv
```

**Expected Result**

The summary file should show aggregated stock metrics for AAPL and TSLA.

**Evidence**

![HDFS Summary Output](images/10-hdfs-cat-marketvol-summary.png)

---

## 9. Validate HDFS Files Using Hadoop UI

Open the Hadoop NameNode UI and browse to:

```text
/marketvol/2026-06-24/
```

Verify that the generated files are visible from the Hadoop web interface.

### AAPL CSV File

![AAPL File in Hadoop UI](images/11-hadoop-ui-aapl-file.png)

### TSLA CSV File

![TSLA File in Hadoop UI](images/12-hadoop-ui-tsla-file.png)

### MarketVol Summary CSV File

![Summary File in Hadoop UI](images/13-hadoop-ui-summary-file.png)

---

## Final Validation Summary

The MarketVol pipeline was successfully validated.

- Docker images built successfully.
- Airflow initialized successfully.
- All Docker Compose services started successfully.
- The `marketvol` DAG appeared in the Airflow UI.
- The DAG was triggered manually from the command line.
- The DAG run completed successfully.
- AAPL, TSLA, and summary files were persisted to HDFS.
- The HDFS output was validated using both CLI commands and the Hadoop UI.
