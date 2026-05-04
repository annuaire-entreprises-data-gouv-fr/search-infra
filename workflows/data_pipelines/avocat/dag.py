from datetime import datetime, timedelta

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.avocat.config import AVOCAT_CONFIG
from data_pipelines_annuaire.workflows.data_pipelines.avocat.processor import (
    AvocatProcessor,
)

default_args = {
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    tags=["avocat"],
    default_args=default_args,
    schedule="0 16 * * *",
    start_date=datetime(2026, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    on_failure_callback=[Notification(), SmtpNotifier(to=EMAIL_LIST)],
    on_success_callback=Notification(),
    max_active_runs=1,
)
def data_processing_avocat():
    avocat_processor = AvocatProcessor()

    @task.bash
    def clean_previous_outputs():
        return (
            f"rm -rf {AVOCAT_CONFIG.tmp_folder} && mkdir -p {AVOCAT_CONFIG.tmp_folder}"
        )

    @task
    def download_data():
        return avocat_processor.download_data()

    @task
    def preprocess_egapro():
        return avocat_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return avocat_processor.save_date_last_modified()

    @task
    def send_file_to_object_storage():
        return avocat_processor.send_file_to_object_storage()

    @task
    def compare_files_object_storage():
        return avocat_processor.compare_files_object_storage()

    return (
        clean_previous_outputs()
        >> download_data()
        >> preprocess_egapro()
        >> save_date_last_modified()
        >> send_file_to_object_storage()
        >> compare_files_object_storage()
    )


# Instantiate the DAG
data_processing_avocat()
