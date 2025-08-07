# airflow/dags/transformation_dag.py
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='daily_spark_transformation',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule='@daily',
    catchup=False,
    tags=['spark', 'transformation'],
    doc_md="DAG para orquestrar jobs de transformação do Spark."
) as dag:
    
    submit_spark_job = BashOperator(
        task_id='submit_commodities_spark_job',
        bash_command='docker-compose run --rm spark-job'
    )