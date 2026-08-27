from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, setup, task, teardown

from data_pipelines_annuaire.config import (
    AIRFLOW_EXPORT_DAG_NAME,
    EMAIL_LIST,
    EXPORT_DATA_DIR,
)
from data_pipelines_annuaire.helpers import EmailNotification, Notification
from data_pipelines_annuaire.workflows.data_pipelines.export_bodacc_radiations.processor import (
    ExportFile,
    export_file,
    get_latest_sirene_database,
)
from data_pipelines_annuaire.workflows.data_pipelines.export_bodacc_radiations.queries import (
    RADIATIONS_INCOHERENCES_QUERY,
)

default_args = {
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    dag_id=AIRFLOW_EXPORT_DAG_NAME,
    tags=["bodacc", "radiations", "export"],
    default_args=default_args,
    schedule=None,  # Triggered by the index_elasticsearch dag
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=[Notification(), EmailNotification(to=EMAIL_LIST)],
    on_success_callback=Notification(),
)
def export_bodacc_radiations():
    @setup
    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {EXPORT_DATA_DIR} && mkdir -p {EXPORT_DATA_DIR}"

    @teardown
    @task.bash
    def clean_outputs():
        return f"rm -rf {EXPORT_DATA_DIR}"

    files_to_export = [
        ExportFile(
            file_name="radiations_incoherences.csv",
            query=RADIATIONS_INCOHERENCES_QUERY,
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


export_bodacc_radiations()
