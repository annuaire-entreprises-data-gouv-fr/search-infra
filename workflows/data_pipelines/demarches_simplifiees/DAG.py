from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.workflows.data_pipelines.demarches_simplifiees.main import (
    run_processor,
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
    dag_id="get_dossiers_demarches_simplifiees",
    default_args=default_args,
    start_date=datetime(2023, 10, 18),
    # schedule_interval="0 1 * * *",  # Run every day at 1 AM
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(days=30),
    tags=["demarches_simplifiees", "dossiers"],
    params={},
) as dag:
    process_task = PythonOperator(
        task_id="process_demarche", python_callable=run_processor
    )
