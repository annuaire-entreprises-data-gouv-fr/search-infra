from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from dag_datalake_sirene.config import (
    EMAIL_LIST,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.maintenance.task_functions import (
    rename_old_rne_folders,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="rename_rne_folders",
    default_args=default_args,
    start_date=datetime(2023, 10, 5),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=(60 * 8)),
    tags=["rename", "rne", "files"],
    params={},
) as dag:
    rename_old_rne_folders = PythonOperator(
        task_id="rename_old_rne_folders",
        python_callable=rename_old_rne_folders,
    )
