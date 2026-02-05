from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.finess.juridique.config import (
    FINESS_JURIDIQUE_CONFIG,
)
from data_pipelines_annuaire.workflows.data_pipelines.finess.juridique.processor import (
    FinessJuridiqueProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["finess", "domaine sanitaire et social"],
    default_args=default_args,
    schedule="0 16 * * *",
    start_date=pendulum.today("UTC").add(days=-8),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    on_failure_callback=Notification(),
    on_success_callback=Notification(),
)
def data_processing_finess_juridique():
    finess_juridique_processor = FinessJuridiqueProcessor()

    @task.bash
    def clean_outputs():
        return f"rm -rf {FINESS_JURIDIQUE_CONFIG.tmp_folder} && mkdir -p {FINESS_JURIDIQUE_CONFIG.tmp_folder}"

    @task
    def download_data():
        return finess_juridique_processor.download_data()

    @task
    def preprocess_finess_juridique():
        return finess_juridique_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return finess_juridique_processor.save_date_last_modified()

    @task
    def send_file_to_object_storage():
        return finess_juridique_processor.send_file_to_object_storage()

    @task
    def compare_files_object_storage():
        return finess_juridique_processor.compare_files_object_storage()

    return (
        clean_outputs()
        >> download_data()
        >> preprocess_finess_juridique()
        >> save_date_last_modified()
        >> send_file_to_object_storage()
        >> compare_files_object_storage()
        >> clean_outputs()
    )


data_processing_finess_juridique()
