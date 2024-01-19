from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dag_datalake_sirene.config import EMAIL_LIST, RNE_DB_TMP_FOLDER
from dag_datalake_sirene.workflows.data_pipelines.rne.temp.task_functions import (
    process_flux_json_files,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="TMP-FIX-RNE-FILES",
    default_args=default_args,
    start_date=datetime(2023, 10, 11),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=(60 * 20)),
    tags=["fix", "rne", "json"],
    params={},
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {RNE_DB_TMP_FOLDER} && mkdir -p {RNE_DB_TMP_FOLDER}",
    )

    process_flux_json_files = PythonOperator(
        task_id="process_flux_json_files", python_callable=process_flux_json_files
    )
    process_flux_json_files.set_upstream(clean_previous_outputs)
