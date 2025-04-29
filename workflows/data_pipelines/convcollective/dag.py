from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.convcollective.processor import (
    ConventionCollectiveProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["convention collective"],
    default_args=default_args,
    schedule="0 16 * * *",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
)
def data_processing_convention_collective():
    convention_collective_processor = ConventionCollectiveProcessor()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {convention_collective_processor.config.tmp_folder} && mkdir -p {convention_collective_processor.config.tmp_folder}"

    @task
    def download_data():
        return convention_collective_processor.download_data()

    @task
    def preprocess_data():
        return convention_collective_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return convention_collective_processor.save_date_last_modified()

    @task
    def send_file_to_minio():
        return convention_collective_processor.send_file_to_minio()

    @task
    def compare_files_minio():
        return convention_collective_processor.compare_files_minio()

    (
        clean_previous_outputs()
        >> download_data()
        >> preprocess_data()
        >> save_date_last_modified()
        >> send_file_to_minio()
        >> compare_files_minio()
    )


data_processing_convention_collective()
