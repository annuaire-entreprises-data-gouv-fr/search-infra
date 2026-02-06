from datetime import datetime, timedelta

from airflow.sdk import dag

from data_pipelines_annuaire.config import (
    AIRFLOW_SNAPSHOT_ROLLBACK_DAG_NAME,
    EMAIL_LIST,
)
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.downstream import (
    wait_for_downstream_rollback_import,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.snapshot import (
    rollback_object_storage_current_index_version,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id=AIRFLOW_SNAPSHOT_ROLLBACK_DAG_NAME,
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    dagrun_timeout=timedelta(minutes=60 * 2),
    tags=["siren"],
    catchup=False,
    max_active_runs=1,
    on_failure_callback=Notification(),
    on_success_callback=Notification(),
)
def snapshot_index_rollback():
    return (
        rollback_object_storage_current_index_version()
        >> wait_for_downstream_rollback_import()
    )


snapshot_index_rollback()
