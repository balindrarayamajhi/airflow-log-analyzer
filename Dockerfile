ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.2.2
FROM ${AIRFLOW_IMAGE_NAME}

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
