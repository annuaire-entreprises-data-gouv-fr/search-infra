from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator

# fmt: on
from data_pipelines_annuaire.config import (
    AIRFLOW_SNAPSHOT_DAG_NAME,
    EMAIL_LIST,
)
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.downstream import (
    update_downstream_alias,
    wait_for_downstream_import,
)

# fmt: off
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

with DAG(
    dag_id=AIRFLOW_SNAPSHOT_DAG_NAME,
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    dagrun_timeout=timedelta(minutes=60 * 2),
    tags=["siren"],
    catchup=False,  # False to ignore past runs
    max_active_runs=1,
    on_failure_callback=Notification.send_notification_mattermost,
) as dag:
    snapshot_elastic_index = PythonOperator(
        task_id="snapshot_elastic_index",

        python_callable=snapshot_elastic_index,
    )

    update_object_storage_current_index_version = PythonOperator(
        task_id="update_object_storage_current_index_version",
        python_callable=update_object_storage_current_index_version,
    )

    wait_for_downstream_import = PythonOperator(
        task_id="wait_for_downstream_import",

        python_callable=wait_for_downstream_import,
    )

    update_downstream_alias = PythonOperator(
        task_id="update_downstream_alias",

        python_callable=update_downstream_alias,
    )

    delete_old_snapshots = PythonOperator(
        task_id="delete_old_snapshots",

        python_callable=delete_old_snapshots,
    )

    snapshot_elastic_index.set_upstream(delete_old_snapshots)
    update_object_storage_current_index_version.set_upstream(snapshot_elastic_index)
    wait_for_downstream_import.set_upstream(update_object_storage_current_index_version)
    update_downstream_alias.set_upstream(wait_for_downstream_import)
