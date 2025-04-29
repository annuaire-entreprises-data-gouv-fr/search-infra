from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from dag_datalake_sirene.config import (
    AIRFLOW_SNAPSHOT_ROLLBACK_DAG_NAME,
    EMAIL_LIST,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.task_functions.downstream import (
    wait_for_downstream_rollback_import,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.task_functions.snapshot import (
    rollback_minio_current_index_version,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=AIRFLOW_SNAPSHOT_ROLLBACK_DAG_NAME,
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    dagrun_timeout=timedelta(minutes=60 * 2),
    tags=["siren"],
    catchup=False,  # False to ignore past runs
    max_active_runs=1,
) as dag:
    rollback_minio_file = PythonOperator(
        task_id="rollback_minio_current_index_version",
        provide_context=True,
        python_callable=rollback_minio_current_index_version,
    )

    wait_for_downstream = PythonOperator(
        task_id="wait_for_downstream_rollback_import",
        provide_context=True,
        python_callable=wait_for_downstream_rollback_import,
    )

    wait_for_downstream.set_upstream(rollback_minio_file)
