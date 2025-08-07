from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='build_gold_layer_daily',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule='@daily',
    catchup=False,
    tags=['gold', 'spark', 'dwh'],
) as dag:
    
    trigger_production_job = BashOperator(
        task_id='trigger_spark_production_job',
        bash_command='docker-compose run --rm spark-gold-job-production' # <-- Nome corrigido
    )

    trigger_climate_job = BashOperator(
        task_id='trigger_spark_climate_job',
        bash_command='docker-compose run --rm spark-gold-job-climate'
    )

    trigger_prices_job = BashOperator(
        task_id='trigger_spark_prices_job',
        bash_command='docker-compose run --rm spark-gold-job-prices'
    )

    # Os jobs podem rodar em paralelo, pois não dependem um do outro.
    [trigger_production_job, trigger_climate_job, trigger_prices_job]