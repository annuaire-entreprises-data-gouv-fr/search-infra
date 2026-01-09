from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.finess.geographique.config import (
    FINESS_GEOGRAPHIQUE_CONFIG,
)
from data_pipelines_annuaire.workflows.data_pipelines.finess.geographique.processor import (
    FinessGeographiqueProcessor,
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
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
)
def data_processing_finess_geographique():
    finess_geographique_processor = FinessGeographiqueProcessor()

    @task.bash
    def clean_outputs():
        return f"rm -rf {FINESS_GEOGRAPHIQUE_CONFIG.tmp_folder} && mkdir -p {FINESS_GEOGRAPHIQUE_CONFIG.tmp_folder}"

    @task
    def download_data():
        return finess_geographique_processor.download_data()

    @task
    def preprocess_finess_geographique():
        return finess_geographique_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return finess_geographique_processor.save_date_last_modified()

    @task
    def send_file_to_minio():
        return finess_geographique_processor.send_file_to_minio()

    @task
    def compare_files_minio():
        return finess_geographique_processor.compare_files_minio()

    return (
        clean_outputs()
        >> download_data()
        >> preprocess_finess_geographique()
        >> save_date_last_modified()
        >> send_file_to_minio()
        >> compare_files_minio()
        >> clean_outputs()
    )


data_processing_finess_geographique()
