from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.task_functions.snapshot import (
    snapshot_elastic_index,
    delete_old_snapshots,
)

from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.task_functions.downstream import (
    wait_for_downstream_import,
)

from dag_datalake_sirene.config import (
    EMAIL_LIST,
    AIRFLOW_SNAPSHOT_DAG_NAME,
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
    dag_id=AIRFLOW_SNAPSHOT_DAG_NAME,
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    dagrun_timeout=timedelta(minutes=60 * 2),
    tags=["siren"],
    catchup=False,  # False to ignore past runs
    max_active_runs=1,
) as dag:
    snapshot_elastic_index = PythonOperator(
        task_id="snapshot_elastic_index",
        provide_context=True,
        python_callable=snapshot_elastic_index,
    )

    wait_for_downstream_import = PythonOperator(
        task_id="wait_for_downstream_import",
        provide_context=True,
        python_callable=wait_for_downstream_import,
    )

    delete_old_snapshots = PythonOperator(
        task_id="delete_old_snapshots",
        provide_context=True,
        python_callable=delete_old_snapshots,
    )

    snapshot_elastic_index.set_upstream(delete_old_snapshots)
    wait_for_downstream_import.set_upstream(snapshot_elastic_index)
