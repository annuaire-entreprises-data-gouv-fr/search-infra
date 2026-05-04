from datetime import datetime, timedelta

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.rge.config import RGE_CONFIG
from data_pipelines_annuaire.workflows.data_pipelines.rge.processor import (
    RgeProcessor,
)

rge_processor = RgeProcessor()
default_args = {
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    tags=["reconnu garant de l'environnement", "label", "ademe"],
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
def data_processing_rge():
    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {RGE_CONFIG.tmp_folder} && mkdir -p {RGE_CONFIG.tmp_folder}"

    @task
    def preprocess_rge():
        return rge_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return rge_processor.save_date_last_modified()

    @task
    def send_file_to_object_storage():
        return rge_processor.send_file_to_object_storage()

    @task
    def compare_files_object_storage():
        return rge_processor.compare_files_object_storage()

    return (
        clean_previous_outputs()
        >> preprocess_rge()
        >> save_date_last_modified()
        >> send_file_to_object_storage()
        >> compare_files_object_storage()
    )


data_processing_rge()
