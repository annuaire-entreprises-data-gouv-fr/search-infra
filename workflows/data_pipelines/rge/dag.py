from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.rge.config import RGE_CONFIG
from dag_datalake_sirene.workflows.data_pipelines.rge.processor import (
    RgeProcessor,
)

rge_processor = RgeProcessor()
default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["reconnu garant de l'environnement", "label", "ademe"],
    default_args=default_args,
    schedule="0 16 * * *",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
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
    def send_file_to_minio():
        return rge_processor.send_file_to_minio()

    @task
    def compare_files_minio():
        return rge_processor.compare_files_minio()

    (
        clean_previous_outputs()
        >> preprocess_rge()
        >> save_date_last_modified()
        >> send_file_to_minio()
        >> compare_files_minio()
    )


data_processing_rge()
