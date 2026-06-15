from datetime import datetime, timedelta

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import dag, setup, task, teardown

from data_pipelines_annuaire.config import (
    AIRFLOW_SHARE_PUBLIC_DATASET_DAG_NAME,
    EMAIL_LIST,
    SHARE_PUBLIC_DATASET_DATA_DIR,
)
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.share_public_dataset.processor import (
    ExportFile,
    export_file,
    get_latest_sirene_database,
)
from data_pipelines_annuaire.workflows.data_pipelines.share_public_dataset.queries import (
    RADIATIONS_PM_QUERY,
    RADIATIONS_PP_QUERY,
)

default_args = {
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    dag_id=AIRFLOW_SHARE_PUBLIC_DATASET_DAG_NAME,
    tags=["bodacc", "radiations", "export", "share dataset"],
    default_args=default_args,
    schedule=None,  # Triggered by the index_elasticsearch dag
    start_date=datetime(2026, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=[Notification(), SmtpNotifier(to=EMAIL_LIST)],
    on_success_callback=Notification(),
)
def export():
    @setup
    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {SHARE_PUBLIC_DATASET_DATA_DIR} && mkdir -p {SHARE_PUBLIC_DATASET_DATA_DIR}"

    @teardown
    @task.bash
    def clean_outputs():
        return f"rm -rf {SHARE_PUBLIC_DATASET_DATA_DIR}"

    files_to_export = [
        ExportFile(
            file_name="radiations_pp.csv",
            query=RADIATIONS_PP_QUERY,
        ),
        ExportFile(
            file_name="radiations_pm.csv",
            query=RADIATIONS_PM_QUERY,
        ),
    ]

    exports = [
        export_file.override(task_id=f"export_{file.file_name.removesuffix('.csv')}")(
            file
        )
        for file in files_to_export
    ]

    return (
        clean_previous_outputs()
        >> get_latest_sirene_database()
        >> exports
        >> clean_outputs()
    )


export()
