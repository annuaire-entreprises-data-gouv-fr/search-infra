from datetime import UTC, datetime, timedelta

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.administration.processor import (
    AdministrationProcessor,
)

default_args = {
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    tags=["administration", "juridique"],
    default_args=default_args,
    schedule="0 16 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    dagrun_timeout=timedelta(minutes=60 * 5),
    params={},
    catchup=False,
    on_failure_callback=[Notification(), SmtpNotifier(to=EMAIL_LIST)],
    on_success_callback=Notification(),
    max_active_runs=1,
)
def data_processing_administration():
    administration_processor = AdministrationProcessor()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {administration_processor.config.tmp_folder} && mkdir -p {administration_processor.config.tmp_folder}"

    @task
    def preprocess_data():
        return administration_processor.preprocess_data()

    @task
    def send_file_to_object_storage():
        return administration_processor.send_file_to_object_storage()

    @task
    def compare_files_object_storage():
        return administration_processor.compare_files_object_storage()

    return (
        clean_previous_outputs()
        >> preprocess_data()
        >> send_file_to_object_storage()
        >> compare_files_object_storage()
    )


data_processing_administration()
