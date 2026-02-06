from datetime import datetime, timedelta

from airflow.sdk import dag

from data_pipelines_annuaire.config import (
    AIRFLOW_SNAPSHOT_DAG_NAME,
    EMAIL_LIST,
)
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.downstream import (
    update_downstream_alias,
    wait_for_downstream_import,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.snapshot import (
    delete_old_snapshots,
    snapshot_elastic_index,
    update_object_storage_current_index_version,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id=AIRFLOW_SNAPSHOT_DAG_NAME,
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
def snapshot_index():
    return (
        delete_old_snapshots()
        >> snapshot_elastic_index()
        >> update_object_storage_current_index_version()
        >> wait_for_downstream_import()
        >> update_downstream_alias()
    )


snapshot_index()
