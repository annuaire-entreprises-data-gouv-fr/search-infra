from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from dag_datalake_sirene.config import (
    EMAIL_LIST,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_PORT,
)
from dag_datalake_sirene.helpers.flush_cache import flush_cache

DAG_NAME = "flush_cache_only"

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule="0 23 10 * *",
    start_date=days_ago(10),
    dagrun_timeout=timedelta(minutes=5),
    tags=["flush cache only"],
) as dag:
    flush_cache = PythonOperator(
        task_id="flush_cache",
        provide_context=True,
        python_callable=flush_cache,
        op_args=(
            REDIS_HOST,
            REDIS_PORT,
            REDIS_DB,
            REDIS_PASSWORD,
        ),
    )
