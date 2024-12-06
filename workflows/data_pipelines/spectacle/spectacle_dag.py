from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.spectacle.spectacle_config import (
    SPECTACLE_CONFIG,
)
from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.workflows.data_pipelines.spectacle.spectacle_processor import (
    SpectacleProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["entrepreneur spectacle"],
    default_args=default_args,
    schedule_interval="0 16 * * *",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_tchap,
    on_success_callback=Notification.send_notification_tchap,
)
def data_processing_entrepreneur_spectacle():
    spectacle_processor = SpectacleProcessor()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {SPECTACLE_CONFIG.tmp_folder} && mkdir -p {SPECTACLE_CONFIG.tmp_folder}"

    @task
    def download_data():
        return spectacle_processor.download_data()

    @task
    def preprocess_data():
        return spectacle_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return spectacle_processor.save_date_last_modified()

    @task
    def send_file_to_minio():
        return spectacle_processor.send_file_to_minio()

    @task
    def compare_files_minio():
        return spectacle_processor.compare_files_minio()

    (
        clean_previous_outputs()
        >> download_data()
        >> preprocess_data()
        >> save_date_last_modified()
        >> send_file_to_minio()
        >> compare_files_minio()
    )


data_processing_entrepreneur_spectacle()
