
FROM apache/airflow:2.8.1


ARG AIRFLOW_UID=50000


USER root


RUN apt-get update && \
    apt-get install -y --no-install-recommends docker.io && \
    pip install --no-cache-dir docker-compose

USER airflow