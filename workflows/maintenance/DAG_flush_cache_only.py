from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from helpers.flush_cache import flush_cache
from helpers.settings import Settings

DAG_NAME = "flush_cache_only"

default_args = {
    "depends_on_past": False,
    "email": Settings.EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval="0 23 10 * *",
    start_date=days_ago(10),
    dagrun_timeout=timedelta(minutes=5),
    tags=["flush cache only"],
) as dag:
    flush_cache = PythonOperator(
        task_id="flush_cache",
        provide_context=True,
        python_callable=flush_cache,
        op_args=(
            Settings.REDIS_HOST,
            Settings.REDIS_PORT,
            Settings.REDIS_DB,
            Settings.REDIS_PASSWORD,
        ),
    )
