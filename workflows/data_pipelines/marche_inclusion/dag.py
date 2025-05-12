from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.marche_inclusion.processor import (
    MarcheInclusionProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["marche inclusion"],
    default_args=default_args,
    schedule="0 16 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
)
def data_processing_marche_inclusion():
    marche_inclusion_processor = MarcheInclusionProcessor()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {marche_inclusion_processor.config.tmp_folder} && mkdir -p {marche_inclusion_processor.config.tmp_folder}"

    @task
    def preprocess_data():
        return marche_inclusion_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return marche_inclusion_processor.save_date_last_modified()

    @task
    def send_file_to_minio():
        return marche_inclusion_processor.send_file_to_minio()

    @task
    def compare_files_minio():
        return marche_inclusion_processor.compare_files_minio()

    (
        clean_previous_outputs()
        >> preprocess_data()
        >> save_date_last_modified()
        >> send_file_to_minio()
        >> compare_files_minio()
    )


data_processing_marche_inclusion()
